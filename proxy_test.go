package goaria

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDerivedHTTPClientPreservesConfiguredClientPolicy(t *testing.T) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	redirectErr := errors.New("redirect policy called")
	configured := &http.Client{
		Transport: http.DefaultTransport,
		Jar:       jar,
		Timeout:   17 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return redirectErr
		},
	}
	engine, err := NewEngine(Config{Dir: t.TempDir(), HTTPClient: configured})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	segmented, err := engine.clientFor("http", Options{"http-version": "1.1"})
	if err != nil {
		t.Fatal(err)
	}
	if segmented != configured {
		t.Fatal("internal segmented HTTP/1.1 preference replaced the configured client")
	}

	derived, err := engine.clientFor("http", Options{"http-version": "1.1", "timeout": "5"})
	if err != nil {
		t.Fatal(err)
	}
	if derived == configured {
		t.Fatal("clientFor returned the configured client instead of a transport-specific clone")
	}
	if derived.Jar != jar {
		t.Fatal("derived client lost the configured cookie jar")
	}
	if derived.Timeout != configured.Timeout {
		t.Fatalf("derived timeout = %v, want %v", derived.Timeout, configured.Timeout)
	}
	if derived.CheckRedirect == nil {
		t.Fatal("derived client lost the configured redirect policy")
	}
	if got := derived.CheckRedirect(nil, nil); !errors.Is(got, redirectErr) {
		t.Fatalf("derived redirect policy returned %v, want %v", got, redirectErr)
	}
}

func TestEngineOwnedDefaultTransportUsesAdvertisedTimeouts(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	transport, ok := engine.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("default transport type = %T, want *http.Transport", engine.client.Transport)
	}
	if transport.DialContext == nil {
		t.Fatal("default transport has no connect-timeout dialer")
	}
	if transport.TLSHandshakeTimeout != 60*time.Second {
		t.Fatalf("TLSHandshakeTimeout = %v, want 60s", transport.TLSHandshakeTimeout)
	}
	if transport.ResponseHeaderTimeout != 60*time.Second {
		t.Fatalf("ResponseHeaderTimeout = %v, want 60s", transport.ResponseHeaderTimeout)
	}
}

func TestCloseTransportsClosesOnlyEngineOwnedBaseClient(t *testing.T) {
	tests := []struct {
		name       string
		callerOwns bool
		wantCloses int32
	}{
		{name: "engine-owned", wantCloses: 1},
		{name: "caller-owned", callerOwns: true, wantCloses: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport := &idleCloseTrackingTransport{}
			engine := &Engine{
				client:           &http.Client{Transport: transport},
				customHTTPClient: test.callerOwns,
			}
			engine.closeTransports()
			if got := transport.closes.Load(); got != test.wantCloses {
				t.Fatalf("CloseIdleConnections calls = %d, want %d", got, test.wantCloses)
			}
		})
	}
}

func TestDerivedTransportCacheIsBounded(t *testing.T) {
	setProxyEnvironment(t, "", "")
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	for i := 0; i < maxCachedTransports+32; i++ {
		_, err := engine.clientFor("http", Options{
			"http-proxy": fmt.Sprintf("http://proxy-%d.invalid:8080", i),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	engine.transportMu.Lock()
	count := len(engine.transports) + len(engine.h2Transports) + len(engine.h3Transports)
	engine.transportMu.Unlock()
	if count > maxCachedTransports {
		t.Fatalf("cached transport count = %d, want at most %d", count, maxCachedTransports)
	}
}

func TestExplicitProxyDisableOverridesEnvironment(t *testing.T) {
	setProxyEnvironment(t, "http://environment-proxy.test:8080", "")
	cfg, err := buildProxyConfig("http", Options{"http-proxy": ""})
	if err != nil {
		t.Fatal(err)
	}
	transport := newTransport(cfg)
	if transport.Proxy != nil {
		proxy, err := transport.Proxy(mustProxyRequest(t, "http://target.test/file"))
		t.Fatalf("explicit proxy disable left proxy function active: proxy=%v err=%v", proxy, err)
	}
}

func TestConfiguredNoProxyAppliesToEnvironmentProxy(t *testing.T) {
	setProxyEnvironment(t, "http://environment-proxy.test:8080", "")
	cfg, err := buildProxyConfig("http", Options{"no-proxy": "skip.test"})
	if err != nil {
		t.Fatal(err)
	}
	transport := newTransport(cfg)
	if got := proxyURLFor(t, transport, "http://skip.test/file"); got != nil {
		t.Fatalf("configured no-proxy selected %v", got)
	}
	if got := proxyURLFor(t, transport, "http://other.test/file"); got == nil || got.Host != "environment-proxy.test:8080" {
		t.Fatalf("environment proxy = %v, want environment-proxy.test:8080", got)
	}
}

func TestSchemeSpecificProxyDisableDoesNotBypassRedirectProxy(t *testing.T) {
	setProxyEnvironment(t, "", "")
	t.Setenv("HTTPS_PROXY", "http://secure-proxy.test:8443")
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	client, err := engine.clientFor("http", Options{"http-proxy": ""})
	if err != nil {
		t.Fatal(err)
	}
	dispatch, ok := client.Transport.(*schemeRoundTripper)
	if !ok {
		t.Fatalf("transport type = %T, want per-scheme dispatcher", client.Transport)
	}
	httpTransport, ok := dispatch.http.(*http.Transport)
	if !ok {
		t.Fatalf("HTTP transport type = %T", dispatch.http)
	}
	httpsTransport, ok := dispatch.https.(*http.Transport)
	if !ok {
		t.Fatalf("HTTPS transport type = %T", dispatch.https)
	}
	if got := proxyURLFor(t, httpTransport, "http://origin.test/file"); got != nil {
		t.Fatalf("explicitly disabled HTTP proxy = %v, want direct", got)
	}
	if got := proxyURLFor(t, httpsTransport, "https://redirect.test/file"); got == nil || got.Host != "secure-proxy.test:8443" {
		t.Fatalf("redirect HTTPS proxy = %v, want secure-proxy.test:8443", got)
	}
}

func TestHTTPSProxyDisableDoesNotBypassHTTPRedirectProxy(t *testing.T) {
	setProxyEnvironment(t, "http://plain-proxy.test:8080", "")
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	client, err := engine.clientFor("https", Options{"https-proxy": ""})
	if err != nil {
		t.Fatal(err)
	}
	dispatch, ok := client.Transport.(*schemeRoundTripper)
	if !ok {
		t.Fatalf("transport type = %T, want per-scheme dispatcher", client.Transport)
	}
	httpTransport := dispatch.http.(*http.Transport)
	httpsTransport := dispatch.https.(*http.Transport)
	if got := proxyURLFor(t, httpsTransport, "https://origin.test/file"); got != nil {
		t.Fatalf("explicitly disabled HTTPS proxy = %v, want direct", got)
	}
	if got := proxyURLFor(t, httpTransport, "http://redirect.test/file"); got == nil || got.Host != "plain-proxy.test:8080" {
		t.Fatalf("redirect HTTP proxy = %v, want plain-proxy.test:8080", got)
	}
}

func TestEnvironmentNoProxyAppliesToExplicitProxy(t *testing.T) {
	setProxyEnvironment(t, "", ".skip.test")
	cfg, err := buildProxyConfig("http", Options{"http-proxy": "http://explicit-proxy.test:8080"})
	if err != nil {
		t.Fatal(err)
	}
	transport := newTransport(cfg)
	if got := proxyURLFor(t, transport, "http://child.skip.test/file"); got != nil {
		t.Fatalf("environment no-proxy selected %v", got)
	}
	if got := proxyURLFor(t, transport, "http://skip.test/file"); got == nil || got.Host != "explicit-proxy.test:8080" {
		t.Fatalf("leading-dot no-proxy incorrectly matched root domain: %v", got)
	}
	if got := proxyURLFor(t, transport, "http://other.test/file"); got == nil || got.Host != "explicit-proxy.test:8080" {
		t.Fatalf("explicit proxy = %v, want explicit-proxy.test:8080", got)
	}
}

func TestForcedHTTPProtocolsRejectEnvironmentProxy(t *testing.T) {
	setProxyEnvironment(t, "", "target.test")
	t.Setenv("HTTPS_PROXY", "http://environment-proxy.test:8080")
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	for _, version := range []string{"2", "3"} {
		t.Run("http-"+version, func(t *testing.T) {
			if _, err := engine.clientFor("https", Options{"http-version": version}); err == nil {
				t.Fatalf("forced HTTP/%s silently bypassed the environment proxy", version)
			}
		})
	}
}

func TestForcedHTTPProtocolsAllowGlobalProxyBypass(t *testing.T) {
	setProxyEnvironment(t, "", "*")
	t.Setenv("HTTPS_PROXY", "http://environment-proxy.test:8080")
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	for _, version := range []string{"2", "3"} {
		t.Run("http-"+version, func(t *testing.T) {
			if _, err := engine.clientFor("https", Options{"http-version": version}); err != nil {
				t.Fatalf("forced HTTP/%s with no-proxy=* failed: %v", version, err)
			}
		})
	}
}

func TestResponseBodyTimeoutOnlyRunsDuringRead(t *testing.T) {
	body := newIdleTimeoutBody(io.NopCloser(strings.NewReader("ab")), 20*time.Millisecond)
	defer body.Close()
	one := make([]byte, 1)
	if _, err := io.ReadFull(body, one); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := io.ReadFull(body, one); err != nil {
		t.Fatalf("consumer delay was treated as network idle time: %v", err)
	}
}

func TestSOCKSHandshakeDoesNotConsumeBufferedTargetBytes(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	payload := []byte("target-response")
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- serveSOCKSHandshake(server, payload)
	}()

	if err := client.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := socks5Handshake(client, "", "", "example.test:80"); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("target payload = %q, want %q", got, payload)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestSOCKSDialHandshakeHonorsContextCancellation(t *testing.T) {
	proxyAddr, accepted, closeProxy := startStallingSOCKSProxy(t)
	defer closeProxy()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := dialSOCKS5(ctx, &net.Dialer{Timeout: 5 * time.Second}, proxyAddr, "", "", "tcp", "example.test:80")
		done <- err
	}()
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("SOCKS proxy did not accept connection")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("dial error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("SOCKS handshake did not stop after context cancellation")
	}
}

func TestSOCKSDialHandshakeHonorsDialerTimeout(t *testing.T) {
	proxyAddr, accepted, closeProxy := startStallingSOCKSProxy(t)
	defer closeProxy()
	done := make(chan error, 1)
	go func() {
		_, err := dialSOCKS5(context.Background(), &net.Dialer{Timeout: 100 * time.Millisecond}, proxyAddr, "", "", "tcp", "example.test:80")
		done <- err
	}()
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("SOCKS proxy did not accept connection")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("SOCKS handshake unexpectedly succeeded")
		}
		var netErr net.Error
		if !errors.As(err, &netErr) || !netErr.Timeout() {
			t.Fatalf("dial error = %v, want network timeout", err)
		}
	case <-time.After(time.Second):
		t.Fatal("SOCKS handshake ignored dialer timeout")
	}
}

type idleCloseTrackingTransport struct {
	closes atomic.Int32
}

func (*idleCloseTrackingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (t *idleCloseTrackingTransport) CloseIdleConnections() {
	t.closes.Add(1)
}

func setProxyEnvironment(t *testing.T, httpProxy, noProxy string) {
	t.Helper()
	for key, value := range map[string]string{
		"HTTP_PROXY":     httpProxy,
		"http_proxy":     "",
		"HTTPS_PROXY":    "",
		"https_proxy":    "",
		"NO_PROXY":       noProxy,
		"no_proxy":       "",
		"REQUEST_METHOD": "",
	} {
		t.Setenv(key, value)
	}
}

func mustProxyRequest(t *testing.T, rawURL string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func proxyURLFor(t *testing.T, transport *http.Transport, rawURL string) *url.URL {
	t.Helper()
	if transport.Proxy == nil {
		return nil
	}
	proxy, err := transport.Proxy(mustProxyRequest(t, rawURL))
	if err != nil {
		t.Fatal(err)
	}
	return proxy
}

func serveSOCKSHandshake(conn net.Conn, payload []byte) error {
	var greeting [2]byte
	if _, err := io.ReadFull(conn, greeting[:]); err != nil {
		return err
	}
	methods := make([]byte, int(greeting[1]))
	if _, err := io.ReadFull(conn, methods); err != nil {
		return err
	}
	if err := writeAll(conn, []byte{0x05, 0x00}); err != nil {
		return err
	}
	var requestHeader [4]byte
	if _, err := io.ReadFull(conn, requestHeader[:]); err != nil {
		return err
	}
	switch requestHeader[3] {
	case 0x01:
		if _, err := io.ReadFull(conn, make([]byte, 4+2)); err != nil {
			return err
		}
	case 0x03:
		var length [1]byte
		if _, err := io.ReadFull(conn, length[:]); err != nil {
			return err
		}
		if _, err := io.ReadFull(conn, make([]byte, int(length[0])+2)); err != nil {
			return err
		}
	case 0x04:
		if _, err := io.ReadFull(conn, make([]byte, 16+2)); err != nil {
			return err
		}
	default:
		return errors.New("unexpected SOCKS address type")
	}
	reply := append([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}, payload...)
	return writeAll(conn, reply)
}

func startStallingSOCKSProxy(t *testing.T) (string, <-chan struct{}, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		close(accepted)
		defer conn.Close()
		_, _ = io.Copy(io.Discard, conn)
	}()
	return listener.Addr().String(), accepted, func() {
		_ = listener.Close()
		wg.Wait()
	}
}
