package goaria

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
)

func TestHTTP1ForcedAgainstHTTP2CapableServer(t *testing.T) {
	data := []byte("http1")
	seen := protocolRecorder{}
	src := newTLSServer(t, true, protocolHandler(data, &seen))

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/h1.txt"}, Options{
		"out":               "h1.txt",
		"check-certificate": "false",
		"http-version":      "1.1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "h1.txt"), data)
	if seen.saw("HTTP/2.0") {
		t.Fatalf("forced HTTP/1.1 used HTTP/2: %#v", seen.snapshot())
	}
	if !seen.sawPrefix("HTTP/1.") {
		t.Fatalf("expected HTTP/1.x, saw %#v", seen.snapshot())
	}
}

func TestHTTP2Download(t *testing.T) {
	data := bytes.Repeat([]byte("http2-"), 4096)
	seen := protocolRecorder{}
	src := newTLSServer(t, true, protocolHandler(data, &seen))

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/h2.bin"}, Options{
		"out":                       "h2.bin",
		"check-certificate":         "false",
		"http-version":              "2",
		"split":                     "2",
		"max-connection-per-server": "2",
		"min-split-size":            "1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "h2.bin"), data)
	if !seen.saw("HTTP/2.0") {
		t.Fatalf("expected HTTP/2.0, saw %#v", seen.snapshot())
	}
}

func TestHTTP3Download(t *testing.T) {
	data := bytes.Repeat([]byte("http3-"), 4096)
	seen := protocolRecorder{}
	url, closeServer := newHTTP3Server(t, protocolHandler(data, &seen))
	defer closeServer()

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{url + "/h3.bin"}, Options{
		"out":                       "h3.bin",
		"check-certificate":         "false",
		"http-version":              "3",
		"split":                     "2",
		"max-connection-per-server": "2",
		"min-split-size":            "1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "h3.bin"), data)
	if !seen.sawPrefix("HTTP/3") {
		t.Fatalf("expected HTTP/3, saw %#v", seen.snapshot())
	}
}

func newTLSServer(t *testing.T, enableHTTP2 bool, handler http.Handler) *httptest.Server {
	t.Helper()
	src := httptest.NewUnstartedServer(handler)
	src.EnableHTTP2 = enableHTTP2
	src.StartTLS()
	t.Cleanup(src.Close)
	return src
}

func newHTTP3Server(t *testing.T, handler http.Handler) (string, func()) {
	t.Helper()
	cert := selfSignedCertificate(t)
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http3.Server{
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{http3.NextProtoH3},
		},
		Handler: handler,
	}
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(packetConn)
	}()
	closeFn := func() {
		_ = server.Close()
		_ = packetConn.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
	}
	return "https://" + packetConn.LocalAddr().String(), closeFn
}

func protocolHandler(data []byte, seen *protocolRecorder) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.add(r.Proto)
		setDownloadHeaders(w, data)
		http.ServeContent(w, r, "protocol.bin", time.Now(), bytes.NewReader(data))
	})
}

type protocolRecorder struct {
	mu     sync.Mutex
	protos []string
}

func (r *protocolRecorder) add(proto string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.protos = append(r.protos, proto)
}

func (r *protocolRecorder) saw(proto string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.protos {
		if p == proto {
			return true
		}
	}
	return false
}

func (r *protocolRecorder) sawPrefix(prefix string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.protos {
		if len(p) >= len(prefix) && p[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}

func (r *protocolRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.protos...)
}

func selfSignedCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  key,
		Leaf:        cert,
	}
}

func TestHTTP3ProxyConfigurationFailsClearly(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{"https://127.0.0.1:1/file"}, Options{
		"http-version": "3",
		"all-proxy":    "http://127.0.0.1:1",
		"max-tries":    "1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusError)
	status, err := engine.TellStatus(gid, []string{"errorMessage"})
	if err != nil {
		t.Fatal(err)
	}
	if status["errorMessage"] == "" {
		t.Fatal("expected clear HTTP/3 proxy error")
	}
}
