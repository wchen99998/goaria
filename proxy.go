package goaria

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"golang.org/x/net/http/httpproxy"
	"golang.org/x/net/http2"
	xproxy "golang.org/x/net/proxy"
)

const maxCachedTransports = 256

type proxyConfig struct {
	scheme           string
	raw              string
	proxyURL         *url.URL
	socksAddr        string
	socksUser        string
	socksPass        string
	noProxy          []string
	connectTimeout   time.Duration
	headerTimeout    time.Duration
	checkCertificate bool
	keepAlive        bool
	httpVersion      string
	envHTTPProxy     string
	envHTTPSProxy    string
	envCGI           bool
}

func (e *Engine) do(req *http.Request, opts Options) (*http.Response, error) {
	client, err := e.clientFor(req.URL.Scheme, opts)
	if err != nil {
		return nil, err
	}
	client = clientWithLoadedCookies(client, optionString(opts, "load-cookies"))
	client = clientWithResponseTimeouts(client, responseBodyTimeout(opts))
	client = clientWithRedirectHost(client, req.Host)
	user, pass, hasAuth := basicAuthCredentials(req, opts)
	credentialURL := cloneURL(req.URL)
	if optionBool(opts, "http-auth-challenge") && req.URL.User != nil && req.Header.Get("Authorization") == "" {
		retrySafe := req.Clone(req.Context())
		retrySafe.Header = req.Header.Clone()
		u := *req.URL
		u.User = nil
		retrySafe.URL = &u
		req = retrySafe
	}
	resp, err := client.Do(req)
	if err != nil || !shouldRetryBasicAuthChallenge(resp, opts, hasAuth) {
		return resp, err
	}
	challenged := resp.Request
	if challenged == nil {
		challenged = req
	}
	// Match net/http's redirect policy: credentials configured for one origin
	// must not be replayed to a different origin after a challenge.
	if !sameURLOrigin(credentialURL, challenged.URL) {
		return resp, nil
	}
	// The per-RoundTrip header-timeout context is canceled when the challenge
	// body closes. Reuse the caller's request context for the authenticated
	// retry while preserving the final redirected URL and headers.
	retry := challenged.Clone(req.Context())
	retry.Header = challenged.Header.Clone()
	if challenged.Body != nil {
		if challenged.GetBody == nil {
			return resp, nil
		}
		body, err := challenged.GetBody()
		if err != nil {
			return resp, nil
		}
		retry.Body = body
	}
	retry.SetBasicAuth(user, pass)
	_ = resp.Body.Close()
	resp, err = client.Do(retry)
	return resp, err
}

type responseBodyTimeoutRoundTripper struct {
	base    http.RoundTripper
	timeout time.Duration
}

func (t *responseBodyTimeoutRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if resp != nil && resp.Body != nil {
		resp.Body = newIdleTimeoutBody(resp.Body, t.timeout)
	}
	return resp, err
}

type responseHeaderTimeoutRoundTripper struct {
	base    http.RoundTripper
	timeout time.Duration
}

type responseHeaderTimeoutError struct {
	timeout time.Duration
}

func (e *responseHeaderTimeoutError) Error() string {
	return fmt.Sprintf("response header timeout after %s", e.timeout)
}

func (*responseHeaderTimeoutError) Timeout() bool   { return true }
func (*responseHeaderTimeoutError) Temporary() bool { return true }

func (t *responseHeaderTimeoutRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithCancelCause(req.Context())
	timeoutErr := &responseHeaderTimeoutError{timeout: t.timeout}
	timerDone := make(chan struct{})
	timer := time.AfterFunc(t.timeout, func() {
		cancel(timeoutErr)
		close(timerDone)
	})
	resp, err := t.base.RoundTrip(req.Clone(ctx))
	if !timer.Stop() {
		<-timerDone
	}
	if errors.Is(context.Cause(ctx), timeoutErr) {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return nil, timeoutErr
	}
	if err != nil {
		cancel(err)
		return resp, err
	}
	if resp != nil && resp.Body != nil {
		resp.Body = &cancelContextBody{ReadCloser: resp.Body, cancel: cancel}
	} else {
		cancel(nil)
	}
	return resp, nil
}

type cancelContextBody struct {
	io.ReadCloser
	cancel context.CancelCauseFunc
	once   sync.Once
}

func (b *cancelContextBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.once.Do(func() { b.cancel(err) })
	}
	return n, err
}

func (b *cancelContextBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(func() { b.cancel(err) })
	return err
}

func clientWithResponseTimeouts(client *http.Client, timeout time.Duration) *http.Client {
	if client == nil || timeout <= 0 {
		return client
	}
	clone := *client
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	base = &responseHeaderTimeoutRoundTripper{base: base, timeout: timeout}
	clone.Transport = &responseBodyTimeoutRoundTripper{base: base, timeout: timeout}
	return &clone
}

func clientWithRedirectHost(client *http.Client, host string) *http.Client {
	if client == nil || host == "" {
		return client
	}
	clone := *client
	checkRedirect := client.CheckRedirect
	clone.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		req.Host = host
		if checkRedirect != nil {
			return checkRedirect(req, via)
		}
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}
	return &clone
}

func responseBodyTimeout(opts Options) time.Duration {
	seconds := optionInt(opts, "timeout", 60)
	if seconds <= 0 {
		return 0
	}
	if int64(seconds) > math.MaxInt64/int64(time.Second) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(seconds) * time.Second
}

type responseBodyIdleTimeoutError struct {
	timeout time.Duration
}

func (e *responseBodyIdleTimeoutError) Error() string {
	return fmt.Sprintf("response body idle timeout after %s", e.timeout)
}

func (*responseBodyIdleTimeoutError) Timeout() bool   { return true }
func (*responseBodyIdleTimeoutError) Temporary() bool { return true }

type idleTimeoutBody struct {
	body    io.ReadCloser
	timeout time.Duration

	mu         sync.Mutex
	timer      *time.Timer
	generation uint64
	reading    bool
	closed     bool
	timedOut   bool
	closeErr   error
	close      sync.Once
}

func newIdleTimeoutBody(body io.ReadCloser, timeout time.Duration) *idleTimeoutBody {
	return &idleTimeoutBody{body: body, timeout: timeout}
}

func (b *idleTimeoutBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	if b.timedOut {
		b.mu.Unlock()
		return 0, &responseBodyIdleTimeoutError{timeout: b.timeout}
	}
	if b.closed {
		b.mu.Unlock()
		return 0, http.ErrBodyReadAfterClose
	}
	b.generation++
	generation := b.generation
	b.reading = true
	b.timer = time.AfterFunc(b.timeout, func() { b.expire(generation) })
	b.mu.Unlock()

	n, err := b.body.Read(p)
	b.mu.Lock()
	if generation == b.generation {
		b.reading = false
		if b.timer != nil {
			b.timer.Stop()
			b.timer = nil
		}
	}
	timedOut := b.timedOut
	b.mu.Unlock()
	if timedOut {
		return n, &responseBodyIdleTimeoutError{timeout: b.timeout}
	}
	return n, err
}

func (b *idleTimeoutBody) Close() error {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		b.reading = false
		b.generation++
		if b.timer != nil {
			b.timer.Stop()
			b.timer = nil
		}
	}
	b.mu.Unlock()
	return b.closeBody()
}

func (b *idleTimeoutBody) expire(generation uint64) {
	b.mu.Lock()
	if b.closed || !b.reading || generation != b.generation {
		b.mu.Unlock()
		return
	}
	b.closed = true
	b.reading = false
	b.timedOut = true
	b.timer = nil
	b.mu.Unlock()
	_ = b.closeBody()
}

func (b *idleTimeoutBody) closeBody() error {
	b.close.Do(func() {
		b.closeErr = b.body.Close()
	})
	return b.closeErr
}

func shouldRetryBasicAuthChallenge(resp *http.Response, opts Options, hasAuth bool) bool {
	if resp == nil || resp.StatusCode != http.StatusUnauthorized || !optionBool(opts, "http-auth-challenge") {
		return false
	}
	if resp.Request != nil && resp.Request.Header.Get("Authorization") != "" {
		return false
	}
	if !hasAuth {
		return false
	}
	return hasBasicAuthChallenge(resp)
}

func sameURLOrigin(a, b *url.URL) bool {
	if a == nil || b == nil || !strings.EqualFold(a.Scheme, b.Scheme) {
		return false
	}
	return strings.EqualFold(canonicalURLHost(a), canonicalURLHost(b))
}

func canonicalURLHost(u *url.URL) string {
	host := strings.ToLower(u.Hostname())
	port := u.Port()
	if port == "" {
		switch strings.ToLower(u.Scheme) {
		case "http":
			port = "80"
		case "https":
			port = "443"
		}
	}
	return net.JoinHostPort(host, port)
}

func (e *Engine) clientFor(scheme string, opts Options) (*http.Client, error) {
	if !hasCustomTransportOptions(opts) {
		return e.client, nil
	}
	// Segmented downloads internally prefer HTTP/1.1. A caller-supplied client
	// is authoritative when that internal preference is the only reason a
	// derived transport would otherwise be created; replacing its RoundTripper
	// would break custom in-memory, authenticated, or otherwise specialized
	// transports.
	if e.customHTTPClient && normalizeHTTPVersion(optionString(opts, "http-version")) == "1.1" && !hasCustomTransportOptionsExceptHTTPVersion(opts) {
		return e.client, nil
	}
	cfg, err := buildProxyConfig(scheme, opts)
	if err != nil {
		return nil, err
	}
	if cfg.httpVersion == "3" || cfg.httpVersion == "h3" {
		if scheme != "https" {
			return nil, fmt.Errorf("HTTP/3 requires https")
		}
		if hasPotentialProxy(cfg) {
			return nil, fmt.Errorf("HTTP/3 over configured HTTP/SOCKS proxy is not supported")
		}
		key := cfg.cacheKey()
		e.transportMu.Lock()
		tr := e.h3Transports[key]
		cached := tr != nil
		if tr == nil {
			tr = newHTTP3Transport(cfg)
			if !e.transportCacheFullLocked() {
				e.h3Transports[key] = tr
				cached = true
			}
		}
		e.transportMu.Unlock()
		return e.clientWithTransport(transportWithKeepAlivePolicy(tr, tr.CloseIdleConnections, cfg.keepAlive && cached)), nil
	}
	if cfg.httpVersion == "2" || cfg.httpVersion == "h2" {
		if scheme != "https" {
			return nil, fmt.Errorf("forced HTTP/2 requires https")
		}
		if hasPotentialProxy(cfg) {
			return nil, fmt.Errorf("forced HTTP/2 over configured HTTP/SOCKS proxy is not supported")
		}
		key := cfg.cacheKey()
		e.transportMu.Lock()
		tr := e.h2Transports[key]
		cached := tr != nil
		if tr == nil {
			tr = newHTTP2Transport(cfg)
			if !e.transportCacheFullLocked() {
				e.h2Transports[key] = tr
				cached = true
			}
		}
		e.transportMu.Unlock()
		return e.clientWithTransport(transportWithKeepAlivePolicy(tr, tr.CloseIdleConnections, cfg.keepAlive && cached)), nil
	}
	tr := e.cachedHTTPTransport(cfg)
	if !hasSchemeSpecificProxyOptions(opts) {
		return e.clientWithTransport(tr), nil
	}
	otherScheme := "http"
	if scheme == "http" {
		otherScheme = "https"
	}
	otherCfg, err := buildProxyConfig(otherScheme, opts)
	if err != nil {
		return nil, err
	}
	other := e.cachedHTTPTransport(otherCfg)
	dispatch := &schemeRoundTripper{http: tr, https: other}
	if scheme == "https" {
		dispatch.http, dispatch.https = other, tr
	}
	return e.clientWithTransport(dispatch), nil
}

func (e *Engine) cachedHTTPTransport(cfg proxyConfig) *http.Transport {
	key := cfg.cacheKey()
	e.transportMu.Lock()
	defer e.transportMu.Unlock()
	tr := e.transports[key]
	if tr == nil {
		tr = newTransport(cfg)
		if e.transportCacheFullLocked() {
			// An uncached one-shot transport must not retain idle connections
			// after the response body is closed.
			tr.DisableKeepAlives = true
		} else {
			e.transports[key] = tr
		}
	}
	return tr
}

func (e *Engine) transportCacheFullLocked() bool {
	return len(e.transports)+len(e.h2Transports)+len(e.h3Transports) >= maxCachedTransports
}

type schemeRoundTripper struct {
	http  http.RoundTripper
	https http.RoundTripper
}

func (t *schemeRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil || req.URL == nil {
		return nil, fmt.Errorf("HTTP request has no URL")
	}
	switch req.URL.Scheme {
	case "http":
		return t.http.RoundTrip(req)
	case "https":
		return t.https.RoundTrip(req)
	default:
		return nil, fmt.Errorf("unsupported protocol scheme %q", req.URL.Scheme)
	}
}

func (e *Engine) closeTransports() {
	if !e.customHTTPClient && e.client != nil {
		e.client.CloseIdleConnections()
	}
	e.transportMu.Lock()
	defer e.transportMu.Unlock()
	for _, tr := range e.transports {
		tr.CloseIdleConnections()
	}
	for _, tr := range e.h2Transports {
		tr.CloseIdleConnections()
	}
	for _, tr := range e.h3Transports {
		_ = tr.Close()
	}
}

func (e *Engine) clientWithTransport(transport http.RoundTripper) *http.Client {
	if e.client == nil {
		return &http.Client{Transport: transport}
	}
	client := *e.client
	client.Transport = transport
	return &client
}

func hasCustomTransportOptions(opts Options) bool {
	if hasCustomTransportOptionsExceptHTTPVersion(opts) {
		return true
	}
	return normalizeHTTPVersion(optionString(opts, "http-version")) != "auto"
}

func hasCustomTransportOptionsExceptHTTPVersion(opts Options) bool {
	for _, key := range []string{"all-proxy", "http-proxy", "https-proxy", "no-proxy"} {
		if optionPresent(opts, key) {
			return true
		}
	}
	for _, key := range []string{"all-proxy-user", "all-proxy-passwd", "http-proxy-user", "http-proxy-passwd", "https-proxy-user", "https-proxy-passwd"} {
		if optionString(opts, key) != "" {
			return true
		}
	}
	if optionInt(opts, "connect-timeout", 60) != 60 || optionInt(opts, "timeout", 60) != 60 {
		return true
	}
	if !optionBoolDefault(opts, "check-certificate", true) || !optionBoolDefault(opts, "enable-http-keep-alive", true) {
		return true
	}
	return false
}

func hasSchemeSpecificProxyOptions(opts Options) bool {
	for _, scheme := range []string{"http", "https"} {
		if optionPresent(opts, scheme+"-proxy") ||
			optionString(opts, scheme+"-proxy-user") != "" ||
			optionString(opts, scheme+"-proxy-passwd") != "" {
			return true
		}
	}
	return false
}

func buildProxyConfig(scheme string, opts Options) (proxyConfig, error) {
	env := httpproxy.FromEnvironment()
	noProxy := splitList(env.NoProxy)
	if optionPresent(opts, "no-proxy") {
		noProxy = splitList(optionString(opts, "no-proxy"))
	}
	cfg := proxyConfig{
		scheme:           scheme,
		noProxy:          noProxy,
		connectTimeout:   time.Duration(optionInt(opts, "connect-timeout", 60)) * time.Second,
		headerTimeout:    time.Duration(optionInt(opts, "timeout", 60)) * time.Second,
		checkCertificate: optionBoolDefault(opts, "check-certificate", true),
		keepAlive:        optionBoolDefault(opts, "enable-http-keep-alive", true),
		httpVersion:      normalizeHTTPVersion(optionString(opts, "http-version")),
		envHTTPProxy:     env.HTTPProxy,
		envHTTPSProxy:    env.HTTPSProxy,
		envCGI:           env.CGI,
	}
	raw, explicitNone := proxySpecForScheme(scheme, opts)
	if explicitNone {
		cfg.raw = "-"
		return cfg, nil
	}
	cfg.raw = raw
	if raw == "" {
		return cfg, nil
	}
	u, err := normalizeProxyURL(raw)
	if err != nil {
		return cfg, err
	}
	user, pass := proxyCredentialsForScheme(scheme, opts)
	if user != "" {
		u.User = url.UserPassword(user, pass)
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
		cfg.proxyURL = u
	case "socks5", "socks5h":
		cfg.socksAddr = u.Host
		if cfg.socksAddr == "" {
			return cfg, fmt.Errorf("invalid socks proxy")
		}
		if u.User != nil {
			cfg.socksUser = u.User.Username()
			cfg.socksPass, _ = u.User.Password()
		}
	default:
		return cfg, fmt.Errorf("unsupported proxy scheme %q", u.Scheme)
	}
	return cfg, nil
}

func newTransport(cfg proxyConfig) *http.Transport {
	dialer := &net.Dialer{
		Timeout:   cfg.connectTimeout,
		KeepAlive: 30 * time.Second,
	}
	bypassProxy := explicitProxyBypassFunc(cfg.noProxy)
	dialContext := dialer.DialContext
	if cfg.socksAddr != "" {
		var auth *xproxy.Auth
		if cfg.socksUser != "" {
			auth = &xproxy.Auth{User: cfg.socksUser, Password: cfg.socksPass}
		}
		socksDialer, _ := xproxy.SOCKS5("tcp", cfg.socksAddr, auth, dialer)
		dialSOCKS := socksDialer.(xproxy.ContextDialer).DialContext
		dialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			if bypassProxy(&url.URL{Scheme: cfg.scheme, Host: addr}) {
				return dialer.DialContext(ctx, network, addr)
			}
			if dialer.Timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, dialer.Timeout)
				defer cancel()
			}
			conn, err := dialSOCKS(ctx, network, addr)
			if ctxErr := ctx.Err(); ctxErr != nil {
				if conn != nil {
					_ = conn.Close()
				}
				return nil, ctxErr
			}
			return conn, err
		}
	}
	tr := &http.Transport{
		Proxy:                 environmentProxyFunc(cfg),
		DialContext:           dialContext,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   128,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   cfg.connectTimeout,
		ResponseHeaderTimeout: cfg.headerTimeout,
		ForceAttemptHTTP2:     true,
		DisableCompression:    true,
		DisableKeepAlives:     !cfg.keepAlive,
	}
	if cfg.httpVersion == "1.1" || cfg.httpVersion == "1" || cfg.httpVersion == "h1" {
		tr.ForceAttemptHTTP2 = false
		tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	}
	if cfg.raw == "-" {
		tr.Proxy = nil
	} else if cfg.proxyURL != nil {
		proxyURL := cloneURL(cfg.proxyURL)
		tr.Proxy = func(req *http.Request) (*url.URL, error) {
			if bypassProxy(req.URL) {
				return nil, nil
			}
			return proxyURL, nil
		}
	}
	if cfg.socksAddr != "" {
		tr.Proxy = nil
	}
	if !cfg.checkCertificate {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec
	}
	return tr
}

func newHTTP2Transport(cfg proxyConfig) *http2.Transport {
	tlsConfig := &tls.Config{
		NextProtos: []string{"h2"},
	}
	if !cfg.checkCertificate {
		tlsConfig.InsecureSkipVerify = true //nolint:gosec
	}
	dialer := &net.Dialer{Timeout: cfg.connectTimeout, KeepAlive: 30 * time.Second}
	return &http2.Transport{
		TLSClientConfig:    tlsConfig,
		DisableCompression: true,
		IdleConnTimeout:    90 * time.Second,
		WriteByteTimeout:   cfg.headerTimeout,
		DialTLSContext: func(ctx context.Context, network, addr string, tlsCfg *tls.Config) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			tlsConn := tls.Client(conn, tlsCfg)
			handshakeCtx := ctx
			cancel := func() {}
			if cfg.connectTimeout > 0 {
				handshakeCtx, cancel = context.WithTimeout(ctx, cfg.connectTimeout)
			}
			defer cancel()
			if err := tlsConn.HandshakeContext(handshakeCtx); err != nil {
				_ = conn.Close()
				return nil, err
			}
			return tlsConn, nil
		},
	}
}

func newHTTP3Transport(cfg proxyConfig) *http3.Transport {
	tlsConfig := &tls.Config{
		NextProtos: []string{http3.NextProtoH3},
	}
	if !cfg.checkCertificate {
		tlsConfig.InsecureSkipVerify = true //nolint:gosec
	}
	return &http3.Transport{
		TLSClientConfig: tlsConfig,
		QUICConfig: &quic.Config{
			HandshakeIdleTimeout: cfg.connectTimeout,
			MaxIdleTimeout:       90 * time.Second,
		},
		MaxResponseHeaderBytes: 10 << 20,
		DisableCompression:     true,
	}
}

type closeIdleRoundTripper struct {
	base      http.RoundTripper
	closeIdle func()
}

func transportWithKeepAlivePolicy(base http.RoundTripper, closeIdle func(), keepAlive bool) http.RoundTripper {
	if keepAlive {
		return base
	}
	return &closeIdleRoundTripper{base: base, closeIdle: closeIdle}
}

func (t *closeIdleRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if resp != nil && resp.Body != nil {
		resp.Body = &closeIdleBody{ReadCloser: resp.Body, closeIdle: t.closeIdle}
	} else {
		t.closeIdle()
	}
	return resp, err
}

type closeIdleBody struct {
	io.ReadCloser
	closeIdle func()
	once      sync.Once
}

func (b *closeIdleBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.once.Do(b.closeIdle)
	}
	return n, err
}

func (b *closeIdleBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.closeIdle)
	return err
}

func (cfg proxyConfig) cacheKey() string {
	parts := []string{
		cfg.scheme,
		cfg.raw,
		cfg.socksAddr,
		cfg.socksUser,
		cfg.socksPass,
		strings.Join(cfg.noProxy, ","),
		cfg.connectTimeout.String(),
		cfg.headerTimeout.String(),
		strconv.FormatBool(cfg.checkCertificate),
		strconv.FormatBool(cfg.keepAlive),
		cfg.httpVersion,
		cfg.envHTTPProxy,
		cfg.envHTTPSProxy,
		strconv.FormatBool(cfg.envCGI),
	}
	if cfg.proxyURL != nil {
		parts = append(parts, cfg.proxyURL.String())
	}
	return strings.Join(parts, "|")
}

func environmentProxyFunc(cfg proxyConfig) func(*http.Request) (*url.URL, error) {
	proxyForURL := (&httpproxy.Config{
		HTTPProxy:  cfg.envHTTPProxy,
		HTTPSProxy: cfg.envHTTPSProxy,
		NoProxy:    strings.Join(cfg.noProxy, ","),
		CGI:        cfg.envCGI,
	}).ProxyFunc()
	return func(req *http.Request) (*url.URL, error) {
		return proxyForURL(req.URL)
	}
}

func hasPotentialProxy(cfg proxyConfig) bool {
	// An explicit empty proxy disables environment proxy discovery. Likewise,
	// no-proxy=* guarantees that redirects cannot leave the bypass set.
	if cfg.raw == "-" {
		return false
	}
	for _, rule := range cfg.noProxy {
		if strings.TrimSpace(rule) == "*" {
			return false
		}
	}
	if cfg.proxyURL != nil || cfg.socksAddr != "" {
		return true
	}
	if cfg.scheme == "https" {
		return strings.TrimSpace(cfg.envHTTPSProxy) != ""
	}
	return strings.TrimSpace(cfg.envHTTPProxy) != ""
}

func explicitProxyBypassFunc(noProxy []string) func(*url.URL) bool {
	return func(target *url.URL) bool {
		return shouldBypassProxyURL(target, noProxy)
	}
}

func normalizeHTTPVersion(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "auto":
		return "auto"
	case "1", "1.1", "http/1.1", "h1":
		return "1.1"
	case "2", "2.0", "http/2", "h2":
		return "2"
	case "3", "3.0", "http/3", "h3":
		return "3"
	default:
		return raw
	}
}

func proxySpecForScheme(scheme string, opts Options) (string, bool) {
	key := scheme + "-proxy"
	if optionPresent(opts, key) {
		raw := optionString(opts, key)
		return raw, raw == ""
	}
	if optionPresent(opts, "all-proxy") {
		raw := optionString(opts, "all-proxy")
		return raw, raw == ""
	}
	return "", false
}

func proxyCredentialsForScheme(scheme string, opts Options) (string, string) {
	user := optionString(opts, scheme+"-proxy-user")
	pass := optionString(opts, scheme+"-proxy-passwd")
	if user == "" {
		user = optionString(opts, "all-proxy-user")
		pass = optionString(opts, "all-proxy-passwd")
	}
	return user, pass
}

func normalizeProxyURL(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty proxy URL")
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if u.Host == "" {
		return nil, fmt.Errorf("invalid proxy URL")
	}
	return u, nil
}

func shouldBypassProxy(host string, noProxy []string) bool {
	target := &url.URL{Host: host}
	if !strings.Contains(host, ":") {
		target.Host = net.JoinHostPort(host, "")
	}
	return shouldBypassProxyURL(target, noProxy)
}

func shouldBypassProxyURL(target *url.URL, noProxy []string) bool {
	if target == nil {
		return false
	}
	host := strings.TrimSuffix(strings.ToLower(target.Hostname()), ".")
	if host == "" {
		return false
	}
	port := target.Port()
	if port == "" {
		switch strings.ToLower(target.Scheme) {
		case "http":
			port = "80"
		case "https":
			port = "443"
		}
	}
	ip := net.ParseIP(host)
	for _, rawRule := range noProxy {
		rule := strings.TrimSpace(strings.ToLower(rawRule))
		if rule == "" {
			continue
		}
		if rule == "*" {
			return true
		}
		if _, network, err := net.ParseCIDR(rule); err == nil && ip != nil {
			if network.Contains(ip) {
				return true
			}
			continue
		}
		ruleHost, rulePort := splitNoProxyHostPort(rule)
		if rulePort != "" && port != rulePort {
			continue
		}
		ruleHost = strings.TrimSuffix(strings.Trim(ruleHost, "[]"), ".")
		if parsed := net.ParseIP(ruleHost); parsed != nil && ip != nil {
			if parsed.Equal(ip) {
				return true
			}
			continue
		}
		subdomainsOnly := strings.HasPrefix(ruleHost, ".") || strings.HasPrefix(ruleHost, "*.")
		ruleHost = strings.TrimPrefix(strings.TrimPrefix(ruleHost, "*."), ".")
		if ruleHost == "" {
			continue
		}
		if (!subdomainsOnly && host == ruleHost) || strings.HasSuffix(host, "."+ruleHost) {
			return true
		}
	}
	return false
}

func splitNoProxyHostPort(rule string) (string, string) {
	if host, port, err := net.SplitHostPort(rule); err == nil {
		return host, port
	}
	return rule, ""
}

func splitList(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func cloneURL(in *url.URL) *url.URL {
	cp := *in
	return &cp
}

func optionBoolDefault(opts Options, key string, fallback bool) bool {
	if !optionPresent(opts, key) {
		return fallback
	}
	return optionBool(opts, key)
}
