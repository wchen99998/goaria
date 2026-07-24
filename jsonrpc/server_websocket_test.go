package jsonrpc

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wchen99998/goaria"
)

func TestWebSocketEnforcesMaxRequestSize(t *testing.T) {
	engine, err := goaria.NewEngine(goaria.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	rpcServer := NewServer(engine, Config{MaxRequestSize: 128})
	httpServer := httptest.NewServer(rpcServer.Handler())
	defer httpServer.Close()

	conn, _, err := websocket.DefaultDialer.Dial(websocketURL(httpServer.URL), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	payload := `{"jsonrpc":"2.0","id":"large","method":"system.listMethods","padding":"` + strings.Repeat("x", 512) + `"}`
	if err := conn.WriteMessage(websocket.TextMessage, []byte(payload)); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, _, err = conn.ReadMessage()
	if err == nil {
		t.Fatal("oversized WebSocket request unexpectedly received a response")
	}
	var closeErr *websocket.CloseError
	if !errors.As(err, &closeErr) || closeErr.Code != websocket.CloseMessageTooBig {
		t.Fatalf("oversized WebSocket request error = %v, want close code %d", err, websocket.CloseMessageTooBig)
	}
}

func TestSecretWebSocketDoesNotSubscribeBeforeAuthentication(t *testing.T) {
	started := make(chan struct{}, 1)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	}))
	defer source.Close()
	engine, err := goaria.NewEngine(goaria.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	rpcServer := NewServer(engine, Config{Secret: "secret"})
	httpServer := httptest.NewServer(rpcServer.Handler())
	defer httpServer.Close()
	headers := http.Header{"Origin": []string{"https://cross-origin.invalid"}}
	conn, _, err := websocket.DefaultDialer.Dial(websocketURL(httpServer.URL), headers)
	if err != nil {
		t.Fatalf("secret-protected cross-origin WebSocket was not admitted for authentication: %v", err)
	}
	defer conn.Close()
	if _, err := engine.AddURI([]string{source.URL + "/blocked.bin"}, goaria.Options{"max-tries": "1"}, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("download did not start")
	}
	_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("unauthenticated WebSocket received a notification")
	} else {
		var netErr net.Error
		if !errors.As(err, &netErr) || !netErr.Timeout() {
			t.Fatalf("unauthenticated read error = %v, want timeout with no data", err)
		}
	}
}

func TestCloseWebSocketsStopsAndUntracksHandlers(t *testing.T) {
	engine, err := goaria.NewEngine(goaria.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	rpcServer := NewServer(engine, Config{})
	httpServer := httptest.NewServer(rpcServer.Handler())
	defer httpServer.Close()

	conn, _, err := websocket.DefaultDialer.Dial(websocketURL(httpServer.URL), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	waitForWebSocketCount(t, rpcServer, 1)

	if err := rpcServer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	rpcServer.wsMu.Lock()
	tracked := len(rpcServer.wsConns)
	rpcServer.wsMu.Unlock()
	if tracked != 0 {
		t.Fatalf("closeWebSockets returned with %d active handlers", tracked)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("tracked WebSocket remained open after server close")
	}
}

func TestListenAndServeShutdownStopsWebSockets(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	engine, err := goaria.NewEngine(goaria.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	rpcServer := NewServer(engine, Config{Addr: addr})
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- rpcServer.ListenAndServe(ctx)
	}()

	var conn *websocket.Conn
	deadline := time.Now().Add(time.Second)
	for {
		conn, _, err = websocket.DefaultDialer.Dial("ws://"+addr+DefaultPath, nil)
		if err == nil {
			break
		}
		select {
		case err := <-serveErr:
			t.Fatalf("server stopped before WebSocket dial: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("WebSocket dial did not succeed: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	defer conn.Close()
	waitForWebSocketCount(t, rpcServer, 1)

	cancel()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("ListenAndServe shutdown failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ListenAndServe did not return after cancellation")
	}
	rpcServer.wsMu.Lock()
	tracked := len(rpcServer.wsConns)
	rpcServer.wsMu.Unlock()
	if tracked != 0 {
		t.Fatalf("ListenAndServe returned with %d active WebSocket handlers", tracked)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("WebSocket remained open after ListenAndServe shutdown")
	}
}

func websocketURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http") + DefaultPath
}

func waitForWebSocketCount(t testing.TB, server *Server, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		server.wsMu.Lock()
		got := len(server.wsConns)
		server.wsMu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("tracked WebSocket count = %d, want %d", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}
