package goaria

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/quic-go/quic-go/http3"
	"golang.org/x/net/http2"
)

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
}

func (e *Engine) do(req *http.Request, opts Options) (*http.Response, error) {
	client, err := e.clientFor(req.URL.Scheme, opts)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (e *Engine) clientFor(scheme string, opts Options) (*http.Client, error) {
	if e.customHTTPClient && !hasCustomTransportOptions(opts) {
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
		if cfg.proxyURL != nil || cfg.socksAddr != "" {
			return nil, fmt.Errorf("HTTP/3 over configured HTTP/SOCKS proxy is not supported")
		}
		key := cfg.cacheKey()
		e.transportMu.Lock()
		tr := e.h3Transports[key]
		if tr == nil {
			tr = newHTTP3Transport(cfg)
			e.h3Transports[key] = tr
		}
		e.transportMu.Unlock()
		return &http.Client{Transport: tr}, nil
	}
	if cfg.httpVersion == "2" || cfg.httpVersion == "h2" {
		if scheme != "https" {
			return nil, fmt.Errorf("forced HTTP/2 requires https")
		}
		if cfg.proxyURL != nil || cfg.socksAddr != "" {
			return nil, fmt.Errorf("forced HTTP/2 over configured HTTP/SOCKS proxy is not supported")
		}
		key := cfg.cacheKey()
		e.transportMu.Lock()
		tr := e.h2Transports[key]
		if tr == nil {
			tr = newHTTP2Transport(cfg)
			e.h2Transports[key] = tr
		}
		e.transportMu.Unlock()
		return &http.Client{Transport: tr}, nil
	}
	key := cfg.cacheKey()
	e.transportMu.Lock()
	tr := e.transports[key]
	if tr == nil {
		tr = newTransport(cfg)
		e.transports[key] = tr
	}
	e.transportMu.Unlock()
	return &http.Client{Transport: tr}, nil
}

func (e *Engine) closeTransports() {
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

func hasCustomTransportOptions(opts Options) bool {
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
	if normalizeHTTPVersion(optionString(opts, "http-version")) != "auto" {
		return true
	}
	return false
}

func buildProxyConfig(scheme string, opts Options) (proxyConfig, error) {
	cfg := proxyConfig{
		scheme:           scheme,
		noProxy:          splitList(optionString(opts, "no-proxy")),
		connectTimeout:   time.Duration(optionInt(opts, "connect-timeout", 60)) * time.Second,
		headerTimeout:    time.Duration(optionInt(opts, "timeout", 60)) * time.Second,
		checkCertificate: optionBoolDefault(opts, "check-certificate", true),
		keepAlive:        optionBoolDefault(opts, "enable-http-keep-alive", true),
		httpVersion:      normalizeHTTPVersion(optionString(opts, "http-version")),
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
	dialContext := dialer.DialContext
	if cfg.socksAddr != "" {
		dialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			if shouldBypassProxy(hostOnly(addr), cfg.noProxy) {
				return dialer.DialContext(ctx, network, addr)
			}
			return dialSOCKS5(ctx, dialer, cfg.socksAddr, cfg.socksUser, cfg.socksPass, network, addr)
		}
	}
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
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
	if cfg.proxyURL != nil {
		proxyURL := cloneURL(cfg.proxyURL)
		tr.Proxy = func(req *http.Request) (*url.URL, error) {
			if shouldBypassProxy(req.URL.Hostname(), cfg.noProxy) {
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
	return &http2.Transport{
		TLSClientConfig:    tlsConfig,
		DisableCompression: true,
		IdleConnTimeout:    90 * time.Second,
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
		TLSClientConfig:    tlsConfig,
		DisableCompression: true,
	}
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
	}
	if cfg.proxyURL != nil {
		parts = append(parts, cfg.proxyURL.String())
	}
	return strings.Join(parts, "|")
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
	host = strings.Trim(strings.ToLower(host), "[]")
	if host == "" {
		return false
	}
	ip := net.ParseIP(host)
	for _, rule := range noProxy {
		rule = strings.TrimSpace(strings.ToLower(rule))
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
		if parsed := net.ParseIP(rule); parsed != nil && ip != nil {
			if parsed.Equal(ip) {
				return true
			}
			continue
		}
		rule = strings.TrimPrefix(rule, ".")
		if host == rule || strings.HasSuffix(host, "."+rule) {
			return true
		}
	}
	return false
}

func hostOnly(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err == nil {
		return host
	}
	return addr
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
