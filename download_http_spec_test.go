package goaria

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPRequestOptionsAndRemoteTime(t *testing.T) {
	data := []byte("option content")
	modTime := time.Date(2024, 7, 8, 9, 10, 11, 0, time.UTC)
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("User-Agent"); got != "goaria-test" {
			t.Errorf("User-Agent = %q", got)
		}
		if got := r.Header.Get("Referer"); !strings.HasSuffix(got, r.URL.String()) {
			t.Errorf("Referer = %q", got)
		}
		if got := r.Header.Get("Cache-Control"); got != "no-cache" {
			t.Errorf("Cache-Control = %q", got)
		}
		if got := r.Header.Get("X-Goaria-Test"); got != "ok" {
			t.Errorf("X-Goaria-Test = %q", got)
		}
		user, pass, ok := r.BasicAuth()
		if !ok || user != "user" || pass != "pass" {
			t.Errorf("BasicAuth = %q/%q ok=%v", user, pass, ok)
		}
		setDownloadHeaders(w, data)
		w.Header().Set("Last-Modified", modTime.Format(http.TimeFormat))
		if r.Method != http.MethodHead {
			_, _ = w.Write(data)
		}
	}))
	defer src.Close()

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/opts.txt"}, Options{
		"out":           "opts.txt",
		"user-agent":    "goaria-test",
		"referer":       "*",
		"http-no-cache": "true",
		"http-user":     "user",
		"http-passwd":   "pass",
		"header":        []string{"X-Goaria-Test: ok"},
		"remote-time":   "true",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	path := filepath.Join(dir, "opts.txt")
	assertFileEquals(t, path, data)
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !st.ModTime().Equal(modTime) {
		t.Fatalf("modtime = %s, want %s", st.ModTime(), modTime)
	}
}

func TestFallbackURIAfterHTTP404(t *testing.T) {
	data := []byte("fallback")
	missing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer missing.Close()
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setDownloadHeaders(w, data)
		if r.Method != http.MethodHead {
			_, _ = w.Write(data)
		}
	}))
	defer ok.Close()

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{missing.URL + "/file.txt", ok.URL + "/file.txt"}, Options{
		"out":       "fallback.txt",
		"max-tries": "2",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "fallback.txt"), data)
}

func TestMaxFileNotFoundFailsBeforeFallback(t *testing.T) {
	data := []byte("should not be reached")
	missing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer missing.Close()
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setDownloadHeaders(w, data)
		if r.Method != http.MethodHead {
			_, _ = w.Write(data)
		}
	}))
	defer ok.Close()

	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{missing.URL + "/file.txt", ok.URL + "/file.txt"}, Options{
		"max-file-not-found": "1",
		"max-tries":          "2",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusError)
	status, err := engine.TellStatus(gid, []string{"errorMessage"})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains([]byte(status["errorMessage"].(string)), []byte("404")) {
		t.Fatalf("unexpected error: %#v", status)
	}
}

func TestChecksumOptionVerifiesDownloadedFile(t *testing.T) {
	data := []byte("checksum content")
	sum := sha256.Sum256(data)
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setDownloadHeaders(w, data)
		if r.Method != http.MethodHead {
			_, _ = w.Write(data)
		}
	}))
	defer src.Close()

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/checksum.txt"}, Options{
		"out":      "checksum.txt",
		"checksum": fmt.Sprintf("sha-256=%x", sum),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "checksum.txt"), data)
}

func TestConditionalGetSkipsUnmodifiedDownloadWithoutHEADRequirement(t *testing.T) {
	data := []byte("cached")
	dir := t.TempDir()
	path := filepath.Join(dir, "cached.txt")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	modTime := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatal(err)
	}
	var getCount atomic.Int32
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-Modified-Since") != "" {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		if r.Method == http.MethodHead {
			http.Error(w, "HEAD should not be used", http.StatusInternalServerError)
			return
		}
		getCount.Add(1)
		setDownloadHeaders(w, data)
		_, _ = w.Write(data)
	}))
	defer src.Close()

	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/cached.txt"}, Options{
		"out":             "cached.txt",
		"conditional-get": "true",
		"use-head":        "false",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, path, data)
	if getCount.Load() != 0 {
		t.Fatalf("unexpected body download count: %d", getCount.Load())
	}
}

func TestHTTPAcceptGzipDecompressesBody(t *testing.T) {
	data := bytes.Repeat([]byte("gzip-data"), 1024)
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	if _, err := zw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept-Encoding") != "gzip" {
			http.Error(w, "missing gzip request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Length", strconv.Itoa(compressed.Len()))
		if r.Method != http.MethodHead {
			_, _ = w.Write(compressed.Bytes())
		}
	}))
	defer src.Close()

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/gzip.txt"}, Options{
		"out":              "gzip.txt",
		"http-accept-gzip": "true",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "gzip.txt"), data)
}

func TestLowestSpeedLimitFailsSlowTransfer(t *testing.T) {
	data := []byte("slow")
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		if r.Method == http.MethodHead {
			return
		}
		_, _ = w.Write(data[:1])
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(1100 * time.Millisecond)
		_, _ = w.Write(data[1:])
	}))
	defer src.Close()

	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/slow.txt"}, Options{
		"lowest-speed-limit": "1000",
		"max-tries":          "1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusError)
}
