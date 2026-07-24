package jsonrpc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"github.com/wchen99998/goaria"
)

const (
	DefaultAddr             = "127.0.0.1:6800"
	DefaultPath             = "/jsonrpc"
	DefaultMaxRequestSize   = 2 << 20
	webSocketAuthTimeout    = 10 * time.Second
	maxJSONPCallbackSize    = 256
	maxWebSocketConnections = 256
	serverShutdownTimeout   = 10 * time.Second
	forceShutdownGrace      = 250 * time.Millisecond
)

type Config struct {
	Addr string
	// Path is the route registered by Handler and ListenAndServe. The
	// default is /jsonrpc, matching aria2.
	Path           string
	MaxRequestSize int64
	Secret         string
	Logger         *zap.Logger
}

type Server struct {
	engine   *goaria.Engine
	rpc      *Handler
	addr     string
	path     string
	maxBody  int64
	log      *zap.Logger
	server   *http.Server
	upgrader websocket.Upgrader

	wsMu      sync.Mutex
	wsConns   map[*websocket.Conn]*webSocketState
	wsClosing bool
	wsActive  int
	wsWG      sync.WaitGroup
}

type webSocketState struct {
	conn *websocket.Conn

	mu       sync.Mutex
	handling bool
	closing  bool
}

func NewServer(engine *goaria.Engine, cfg Config) *Server {
	if cfg.Addr == "" {
		cfg.Addr = DefaultAddr
	}
	cfg.Path = normalizePath(cfg.Path)
	if cfg.MaxRequestSize <= 0 {
		cfg.MaxRequestSize = DefaultMaxRequestSize
	}
	log := cfg.Logger
	if log == nil {
		log = zap.NewNop()
	}
	return &Server{
		engine:  engine,
		rpc:     NewHandler(engine, cfg.Secret),
		addr:    cfg.Addr,
		path:    cfg.Path,
		maxBody: cfg.MaxRequestSize,
		log:     log,
		wsConns: make(map[*websocket.Conn]*webSocketState),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return cfg.Secret != "" || sameWebSocketOrigin(r)
			},
		},
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle(s.path, s.JSONRPCHandler())
	return mux
}

// JSONRPCHandler returns only the JSON-RPC endpoint handler, without creating
// or owning a mux. Mount it on an existing net/http, Gin, or compatible router.
func (s *Server) JSONRPCHandler() http.Handler {
	return http.HandlerFunc(s.handleJSONRPC)
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	s.server = &http.Server{
		Addr:              s.addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    requestHeaderLimit(s.maxBody),
	}
	errCh := make(chan error, 1)
	go func() {
		s.log.Info("rpc server listening", zap.String("addr", s.addr))
		errCh <- s.server.ListenAndServe()
	}()

	force := false
	select {
	case <-ctx.Done():
	case force = <-s.engine.ShutdownRequests():
		if force {
			s.log.Info("force shutdown requested")
		} else {
			s.log.Info("shutdown requested")
		}
	case err := <-errCh:
		s.initiateWebSocketClose()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), serverShutdownTimeout)
	defer cancel()
	var shutdownErr error
	if force {
		shutdownErr = s.forceShutdownHTTP()
	} else {
		shutdownDone := make(chan error, 1)
		go func() { shutdownDone <- s.server.Shutdown(shutdownCtx) }()
		for {
			select {
			case shutdownErr = <-shutdownDone:
				goto shutdownComplete
			case escalate := <-s.engine.ShutdownRequests():
				if escalate {
					var gracefulErr, closeErr error
					timer := time.NewTimer(forceShutdownGrace)
					select {
					case gracefulErr = <-shutdownDone:
						if !timer.Stop() {
							<-timer.C
						}
					case <-timer.C:
						closeErr = s.server.Close()
						gracefulErr = <-shutdownDone
					}
					if errors.Is(gracefulErr, http.ErrServerClosed) {
						gracefulErr = nil
					}
					shutdownErr = errors.Join(closeErr, gracefulErr)
					goto shutdownComplete
				}
			}
		}
	}

shutdownComplete:
	if errors.Is(shutdownErr, http.ErrServerClosed) {
		shutdownErr = nil
	}

	// Shutdown ignores hijacked connections. Mark active RPCs for close after
	// their response is written, then bound the wait with the same deadline.
	s.initiateWebSocketClose()
	if err := s.waitWebSockets(shutdownCtx); err != nil {
		s.forceCloseWebSockets()
		shutdownErr = errors.Join(shutdownErr, err)
	}
	if err := <-errCh; !errors.Is(err, http.ErrServerClosed) {
		shutdownErr = errors.Join(shutdownErr, err)
	}
	return shutdownErr
}

func (s *Server) forceShutdownHTTP() error {
	ctx, cancel := context.WithTimeout(context.Background(), forceShutdownGrace)
	defer cancel()
	err := s.server.Shutdown(ctx)
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	closeErr := s.server.Close()
	if errors.Is(err, context.DeadlineExceeded) {
		err = nil
	}
	return errors.Join(err, closeErr)
}

func normalizePath(path string) string {
	if path == "" {
		return DefaultPath
	}
	if !strings.HasPrefix(path, "/") {
		return "/" + path
	}
	return path
}

func (s *Server) handleJSONRPC(w http.ResponseWriter, r *http.Request) {
	if websocket.IsWebSocketUpgrade(r) {
		s.handleWebSocket(w, r)
		return
	}
	callback := r.URL.Query().Get("jsoncallback")
	if len(callback) > maxJSONPCallbackSize {
		http.Error(w, "jsoncallback too large", http.StatusRequestEntityTooLarge)
		return
	}
	if callback != "" && !validJSONPCallback(callback) {
		http.Error(w, "invalid jsoncallback", http.StatusBadRequest)
		return
	}
	var payload []byte
	var err error
	switch r.Method {
	case http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, s.maxBody)
		payload, err = io.ReadAll(r.Body)
	case http.MethodGet:
		if int64(len(r.URL.RawQuery)) > requestQueryLimit(s.maxBody) {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
			return
		}
		payload, err = buildGETPayload(r)
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if int64(len(payload)) > s.maxBody {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	response, ok := s.rpc.HandlePayload(payload)
	if !ok {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if callback != "" {
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
		_, _ = fmt.Fprintf(w, "%s(%s);", callback, response)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write(response)
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if !s.beginWebSocket() {
		http.Error(w, "websocket unavailable", http.StatusServiceUnavailable)
		return
	}
	defer s.endWebSocket()

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.log.Warn("websocket upgrade failed", zap.Error(err))
		return
	}
	conn.SetReadLimit(s.maxBody)
	state := &webSocketState{conn: conn}
	if !s.trackWebSocket(state) {
		return
	}

	done := make(chan struct{})
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			close(done)
			_ = conn.Close()
		})
	}

	var writeMu sync.Mutex
	write := func(payload []byte) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		return conn.WriteMessage(websocket.TextMessage, payload)
	}

	var writerWG sync.WaitGroup
	unsubscribe := func() {}
	notificationsStarted := false
	startNotifications := func() {
		if notificationsStarted {
			return
		}
		notificationsStarted = true
		notifications, stopNotifications := s.engine.Subscribe(64)
		unsubscribe = stopNotifications
		writerWG.Add(1)
		go func() {
			defer writerWG.Done()
			for {
				select {
				case <-done:
					return
				case n, ok := <-notifications:
					if !ok {
						return
					}
					payload, _ := json.Marshal(map[string]any{
						"jsonrpc": "2.0",
						"method":  n.Method,
						"params":  []map[string]string{{"gid": n.GID}},
					})
					if err := write(payload); err != nil {
						stop()
						return
					}
				}
			}
		}()
	}
	authenticated := s.rpc.secret == ""
	if authenticated {
		startNotifications()
	} else if err := conn.SetReadDeadline(time.Now().Add(webSocketAuthTimeout)); err != nil {
		return
	}
	defer func() {
		stop()
		unsubscribe()
		writerWG.Wait()
		s.untrackWebSocket(state)
	}()

	for {
		typ, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if typ != websocket.TextMessage {
			continue
		}
		if !authenticated && s.rpc.payloadProvesSecret(msg) {
			authenticated = true
			if err := conn.SetReadDeadline(time.Time{}); err != nil {
				return
			}
			// Subscribe before invoking the authenticated call so notifications
			// caused by that call cannot race ahead of registration.
			startNotifications()
		}
		if !state.beginRPC() {
			return
		}
		response, ok := s.rpc.HandlePayload(msg)
		var writeErr error
		if ok {
			writeErr = write(response)
		}
		closing := state.endRPC()
		if writeErr != nil || closing {
			return
		}
	}
}

func (s *Server) beginWebSocket() bool {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	if s.wsClosing || s.wsActive >= maxWebSocketConnections {
		return false
	}
	s.wsActive++
	s.wsWG.Add(1)
	return true
}

func (s *Server) endWebSocket() {
	s.wsMu.Lock()
	s.wsActive--
	s.wsMu.Unlock()
	s.wsWG.Done()
}

func (s *Server) trackWebSocket(state *webSocketState) bool {
	s.wsMu.Lock()
	if s.wsClosing {
		s.wsMu.Unlock()
		_ = state.conn.Close()
		return false
	}
	s.wsConns[state.conn] = state
	s.wsMu.Unlock()
	return true
}

func (s *Server) untrackWebSocket(state *webSocketState) {
	s.wsMu.Lock()
	delete(s.wsConns, state.conn)
	s.wsMu.Unlock()
}

func (state *webSocketState) beginRPC() bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closing {
		return false
	}
	state.handling = true
	return true
}

func (state *webSocketState) endRPC() bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.handling = false
	if state.closing {
		_ = state.conn.Close()
		return true
	}
	return false
}

// Close stops accepting WebSocket connections through this Server's handlers,
// closes tracked connections, and waits for their subscriptions to be released.
// It intentionally does not close an externally owned http.Server.
func (s *Server) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.initiateWebSocketClose()
	if err := s.waitWebSockets(ctx); err != nil {
		s.forceCloseWebSockets()
		return err
	}
	return nil
}

func (s *Server) initiateWebSocketClose() {
	s.wsMu.Lock()
	if s.wsClosing {
		s.wsMu.Unlock()
		return
	}
	s.wsClosing = true
	states := make([]*webSocketState, 0, len(s.wsConns))
	for _, state := range s.wsConns {
		states = append(states, state)
	}
	s.wsMu.Unlock()
	for _, state := range states {
		state.mu.Lock()
		state.closing = true
		handling := state.handling
		state.mu.Unlock()
		if !handling {
			_ = state.conn.Close()
		}
	}
}

func (s *Server) waitWebSockets(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.wsWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) forceCloseWebSockets() {
	s.wsMu.Lock()
	conns := make([]*websocket.Conn, 0, len(s.wsConns))
	for conn := range s.wsConns {
		conns = append(conns, conn)
	}
	s.wsMu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func buildGETPayload(r *http.Request) ([]byte, error) {
	q := r.URL.Query()
	encoded := q.Get("params")
	var decoded []byte
	var err error
	if encoded != "" {
		decoded, err = decodeGETParams(encoded)
		if err != nil {
			return nil, err
		}
	}
	method := q.Get("method")
	if method == "" {
		if len(decoded) == 0 {
			return nil, fmt.Errorf("missing method or params")
		}
		return decoded, nil
	}
	params := json.RawMessage("[]")
	if len(decoded) > 0 {
		params = json.RawMessage(decoded)
	}
	obj := map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
	}
	if ids, ok := q["id"]; ok && len(ids) > 0 {
		obj["id"] = ids[0]
	}
	payload, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	trimmed := bytes.TrimSpace(params)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return nil, fmt.Errorf("params must be a JSON array")
	}
	return payload, nil
}

func decodeGETParams(params string) ([]byte, error) {
	trimmed := bytes.TrimSpace([]byte(params))
	if json.Valid(trimmed) {
		return trimmed, nil
	}
	return base64.StdEncoding.DecodeString(params)
}

func sameWebSocketOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	expectedScheme := "http"
	if r.TLS != nil {
		expectedScheme = "https"
	}
	if !strings.EqualFold(u.Scheme, expectedScheme) {
		return false
	}
	originHost, ok := canonicalOriginHost(u.Host, expectedScheme)
	if !ok {
		return false
	}
	requestHost, ok := canonicalOriginHost(r.Host, expectedScheme)
	return ok && strings.EqualFold(originHost, requestHost)
}

func canonicalOriginHost(rawHost, scheme string) (string, bool) {
	u := &url.URL{Host: rawHost}
	host := u.Hostname()
	if host == "" {
		return "", false
	}
	port := u.Port()
	if port == "" {
		if scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	return net.JoinHostPort(strings.ToLower(host), port), true
}

func validJSONPCallback(callback string) bool {
	for _, part := range strings.Split(callback, ".") {
		if !validJSIdentifier(part) {
			return false
		}
	}
	return true
}

func validJSIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		if i == 0 {
			if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || r == '_' || r == '$' {
				continue
			}
			return false
		}
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '$' {
			continue
		}
		return false
	}
	return true
}

func requestQueryLimit(maxBody int64) int64 {
	// A JSON GET payload can expand through base64 and URL escaping. Bound the
	// wire representation independently while keeping every configured decoded
	// payload size reachable.
	const overhead = int64(64 << 10)
	if maxBody > (int64(^uint64(0)>>1)-overhead)/4 {
		return int64(^uint64(0) >> 1)
	}
	return maxBody*4 + overhead
}

func requestHeaderLimit(maxBody int64) int {
	limit := requestQueryLimit(maxBody)
	maxInt := int64(^uint(0) >> 1)
	if limit > maxInt {
		return int(maxInt)
	}
	if limit < 1<<20 {
		return 1 << 20
	}
	return int(limit)
}
