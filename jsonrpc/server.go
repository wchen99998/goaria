package jsonrpc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	DefaultAddr           = "127.0.0.1:6800"
	DefaultPath           = "/jsonrpc"
	DefaultMaxRequestSize = 2 << 20
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
	}
	errCh := make(chan error, 1)
	go func() {
		s.log.Info("rpc server listening", zap.String("addr", s.addr))
		errCh <- s.server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
	case force := <-s.engine.ShutdownRequests():
		if force {
			s.log.Info("force shutdown requested")
		} else {
			s.log.Info("shutdown requested")
		}
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.server.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if err := <-errCh; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
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
		payload, err = buildGETPayload(r)
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.log.Warn("websocket upgrade failed", zap.Error(err))
		return
	}
	defer conn.Close()

	notifications, unsubscribe := s.engine.Subscribe(64)
	defer unsubscribe()

	done := make(chan struct{})
	var writeMu sync.Mutex
	write := func(payload []byte) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		return conn.WriteMessage(websocket.TextMessage, payload)
	}
	go func() {
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
					return
				}
			}
		}
	}()

	for {
		typ, msg, err := conn.ReadMessage()
		if err != nil {
			close(done)
			return
		}
		if typ != websocket.TextMessage {
			continue
		}
		response, ok := s.rpc.HandlePayload(msg)
		if ok {
			if err := write(response); err != nil {
				close(done)
				return
			}
		}
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
	return strings.EqualFold(u.Host, r.Host)
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
