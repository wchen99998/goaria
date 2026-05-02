package goaria

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestFailureRecoveryResumesSequentialPartialDownload(t *testing.T) {
	data := bytes.Repeat([]byte("resume-"), 128*1024)
	cut := len(data) / 3
	var fullGETs atomic.Int32
	var sawResume atomic.Bool
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setDownloadHeaders(w, data)
		if r.Method == http.MethodHead {
			return
		}
		if rng := r.Header.Get("Range"); rng != "" {
			start, end, ok := parseTestRange(t, rng, int64(len(data)))
			if ok && start == int64(cut) {
				sawResume.Store(true)
			}
			writeRange(w, data, start, end)
			return
		}
		if fullGETs.Add(1) == 1 {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data[:cut])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			return
		}
		_, _ = w.Write(data)
	}))
	defer src.Close()

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/resume.bin"}, Options{
		"out":       "resume.bin",
		"split":     "1",
		"max-tries": "3",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "resume.bin"), data)
	if !sawResume.Load() {
		t.Fatal("expected retry to resume with a Range request")
	}
}

func TestChaosSegmentedRetryAndParallelRanges(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789abcdef"), 128*1024)
	var failedRange atomic.Bool
	var activeRanges atomic.Int32
	var maxActiveRanges atomic.Int32
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setDownloadHeaders(w, data)
		if r.Method == http.MethodHead {
			return
		}
		rng := r.Header.Get("Range")
		if rng == "" {
			_, _ = w.Write(data)
			return
		}
		current := activeRanges.Add(1)
		for {
			prev := maxActiveRanges.Load()
			if current <= prev || maxActiveRanges.CompareAndSwap(prev, current) {
				break
			}
		}
		defer activeRanges.Add(-1)
		if failedRange.CompareAndSwap(false, true) {
			http.Error(w, "planned chaos failure", http.StatusServiceUnavailable)
			return
		}
		time.Sleep(20 * time.Millisecond)
		start, end, _ := parseTestRange(t, rng, int64(len(data)))
		writeRange(w, data, start, end)
	}))
	defer src.Close()

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/segmented.bin"}, Options{
		"out":                       "segmented.bin",
		"split":                     "4",
		"max-connection-per-server": "4",
		"min-split-size":            "1",
		"max-tries":                 "3",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "segmented.bin"), data)
	if maxActiveRanges.Load() < 2 {
		t.Fatalf("expected parallel range requests, max active = %d", maxActiveRanges.Load())
	}
}

func TestHTTPProxyOptionRoutesRequests(t *testing.T) {
	data := []byte("proxied content")
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setDownloadHeaders(w, data)
		if r.Method != http.MethodHead {
			_, _ = w.Write(data)
		}
	}))
	defer target.Close()

	var proxyRequests atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyRequests.Add(1)
		if !r.URL.IsAbs() {
			http.Error(w, "proxy expected absolute-form URL", http.StatusBadGateway)
			return
		}
		forwardProxyRequest(t, w, r)
	}))
	defer proxy.Close()

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{target.URL + "/proxy.txt"}, Options{
		"out":        "proxy.txt",
		"http-proxy": proxy.URL,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "proxy.txt"), data)
	if proxyRequests.Load() == 0 {
		t.Fatal("expected HTTP proxy to receive requests")
	}
}

func TestSOCKS5ProxyOptionRoutesRequests(t *testing.T) {
	data := []byte("socks content")
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setDownloadHeaders(w, data)
		if r.Method != http.MethodHead {
			_, _ = w.Write(data)
		}
	}))
	defer target.Close()
	socksAddr, socksCount, closeSocks := startTestSOCKS5Proxy(t)
	defer closeSocks()

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{target.URL + "/socks.txt"}, Options{
		"out":       "socks.txt",
		"all-proxy": "socks5://" + socksAddr,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "socks.txt"), data)
	if socksCount.Load() == 0 {
		t.Fatal("expected SOCKS5 proxy to receive connections")
	}
}

func TestNoProxyBypassesConfiguredProxy(t *testing.T) {
	data := []byte("direct content")
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setDownloadHeaders(w, data)
		if r.Method != http.MethodHead {
			_, _ = w.Write(data)
		}
	}))
	defer target.Close()
	targetURL, err := url.Parse(target.URL)
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{target.URL + "/direct.txt"}, Options{
		"out":       "direct.txt",
		"all-proxy": "http://127.0.0.1:1",
		"no-proxy":  targetURL.Hostname(),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "direct.txt"), data)
}

func setDownloadHeaders(w http.ResponseWriter, data []byte) {
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
}

func parseTestRange(t *testing.T, header string, total int64) (int64, int64, bool) {
	t.Helper()
	if !strings.HasPrefix(header, "bytes=") {
		t.Fatalf("unexpected range header %q", header)
	}
	parts := strings.SplitN(strings.TrimPrefix(header, "bytes="), "-", 2)
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	end := total - 1
	if parts[1] != "" {
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
	}
	return start, end, true
}

func writeRange(w http.ResponseWriter, data []byte, start, end int64) {
	if end >= int64(len(data)) {
		end = int64(len(data)) - 1
	}
	if start < 0 || start > end {
		http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(data[start : end+1])
}

func assertFileEquals(t testing.TB, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("file mismatch: got %d bytes want %d", len(got), len(want))
	}
}

func forwardProxyRequest(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	req, err := http.NewRequestWithContext(r.Context(), r.Method, r.URL.String(), nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	req.Header = r.Header.Clone()
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for k, values := range resp.Header {
		for _, v := range values {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func startTestSOCKS5Proxy(t *testing.T) (string, *atomic.Int32, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var count atomic.Int32
	done := make(chan struct{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-done:
					return
				default:
					return
				}
			}
			count.Add(1)
			go handleTestSOCKS5Conn(conn)
		}
	}()
	return ln.Addr().String(), &count, func() {
		close(done)
		_ = ln.Close()
	}
}

func handleTestSOCKS5Conn(conn net.Conn) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	header := make([]byte, 2)
	if _, err := io.ReadFull(br, header); err != nil {
		return
	}
	methods := make([]byte, int(header[1]))
	if _, err := io.ReadFull(br, methods); err != nil {
		return
	}
	if _, err := conn.Write([]byte{0x05, 0x00}); err != nil {
		return
	}
	reqHead := make([]byte, 4)
	if _, err := io.ReadFull(br, reqHead); err != nil {
		return
	}
	host, ok := readSOCKS5Addr(br, reqHead[3])
	if !ok {
		return
	}
	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(br, portBytes); err != nil {
		return
	}
	target := net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(portBytes))))
	upstream, err := net.DialTimeout("tcp", target, 2*time.Second)
	if err != nil {
		_, _ = conn.Write([]byte{0x05, 0x04, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}
	defer upstream.Close()
	if _, err := conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}); err != nil {
		return
	}
	go func() {
		_, _ = io.Copy(upstream, br)
		_ = upstream.(*net.TCPConn).CloseWrite()
	}()
	_, _ = io.Copy(conn, upstream)
}

func readSOCKS5Addr(r io.Reader, atyp byte) (string, bool) {
	switch atyp {
	case 0x01:
		buf := make([]byte, 4)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", false
		}
		return net.IP(buf).String(), true
	case 0x03:
		var lenBuf [1]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return "", false
		}
		buf := make([]byte, int(lenBuf[0]))
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", false
		}
		return string(buf), true
	case 0x04:
		buf := make([]byte, 16)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", false
		}
		return net.IP(buf).String(), true
	default:
		return "", false
	}
}
