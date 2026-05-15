package goaria

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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

func TestHTTPAuthChallengeRetriesWithBasicAuth(t *testing.T) {
	data := []byte("challenge auth")
	var unauthenticated atomic.Int32
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok {
			unauthenticated.Add(1)
			w.Header().Set("WWW-Authenticate", `Basic realm="goaria"`)
			http.Error(w, "auth required", http.StatusUnauthorized)
			return
		}
		if user != "user" || pass != "pass" {
			t.Errorf("BasicAuth = %q/%q", user, pass)
			http.Error(w, "bad auth", http.StatusForbidden)
			return
		}
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
	gid, err := engine.AddURI([]string{src.URL + "/auth.txt"}, Options{
		"out":                 "auth.txt",
		"split":               "1",
		"http-user":           "user",
		"http-passwd":         "pass",
		"http-auth-challenge": "true",
		"use-head":            "false",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "auth.txt"), data)
	if unauthenticated.Load() == 0 {
		t.Fatal("expected an initial unauthenticated challenge request")
	}
}

func TestHTTPAuthChallengeDoesNotPreemptivelySendURLCredentials(t *testing.T) {
	data := []byte("url challenge auth")
	var unauthenticated atomic.Int32
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok {
			unauthenticated.Add(1)
			w.Header().Set("WWW-Authenticate", `Basic realm="goaria"`)
			http.Error(w, "auth required", http.StatusUnauthorized)
			return
		}
		if user != "user" || pass != "pass" {
			t.Errorf("BasicAuth = %q/%q", user, pass)
			http.Error(w, "bad auth", http.StatusForbidden)
			return
		}
		setDownloadHeaders(w, data)
		if r.Method != http.MethodHead {
			_, _ = w.Write(data)
		}
	}))
	defer src.Close()

	u, err := url.Parse(src.URL + "/auth-url.txt")
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword("user", "pass")

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{u.String()}, Options{
		"out":                 "auth-url.txt",
		"split":               "1",
		"http-auth-challenge": "true",
		"use-head":            "false",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "auth-url.txt"), data)
	if unauthenticated.Load() == 0 {
		t.Fatal("expected URL credentials to wait for an initial challenge")
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

func TestHTTPProbeFallsBackToRangeWhenHeadLengthIsUnknown(t *testing.T) {
	data := bytes.Repeat([]byte("range-data"), 512)
	var sawRange atomic.Bool
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Accept-Ranges", "bytes")
			return
		}
		if got := r.Header.Get("Range"); got != "bytes=0-1023" {
			http.Error(w, "unexpected range "+got, http.StatusBadRequest)
			return
		}
		sawRange.Store(true)
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-1023/%d", len(data)))
		w.Header().Set("Content-Length", "1024")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[:1024])
	}))
	defer src.Close()

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/range.bin"}, Options{
		"out":     "range.bin",
		"dry-run": "true",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	waitForStatus(t, engine, gid, StatusComplete)
	if !sawRange.Load() {
		t.Fatal("range fallback was not used")
	}
	status, err := engine.TellStatus(gid, []string{"totalLength", "files"})
	if err != nil {
		t.Fatal(err)
	}
	want := strconv.Itoa(len(data))
	if status["totalLength"] != want {
		t.Fatalf("totalLength = %v, want %s", status["totalLength"], want)
	}
	files := status["files"].([]FileInfo)
	if files[0].Length != want {
		t.Fatalf("file length = %v, want %s", files[0].Length, want)
	}
}

func TestSegmentedRangeRequestsUseIdentityEncodingAndSingleRange(t *testing.T) {
	data := bytes.Repeat([]byte("identity-range-"), 64*1024)
	var sawRange atomic.Int32
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			if got := r.Header.Get("Accept-Encoding"); got != "identity" {
				http.Error(w, "HEAD Accept-Encoding = "+got, http.StatusBadRequest)
				return
			}
			if got := r.Header.Get("Range"); got != "" {
				http.Error(w, "HEAD sent Range "+got, http.StatusBadRequest)
				return
			}
			w.Header().Set("ETag", `"range-etag"`)
			setDownloadHeaders(w, data)
			return
		}
		ranges := r.Header.Values("Range")
		if len(ranges) != 1 {
			http.Error(w, fmt.Sprintf("Range field count = %d", len(ranges)), http.StatusBadRequest)
			return
		}
		if got := r.Header.Get("Accept-Encoding"); got != "identity" {
			http.Error(w, "range Accept-Encoding = "+got, http.StatusBadRequest)
			return
		}
		if got := r.Header.Get("If-Range"); got != `"range-etag"` {
			http.Error(w, "range If-Range = "+got, http.StatusBadRequest)
			return
		}
		start, end, _ := parseTestRange(t, ranges[0], int64(len(data)))
		sawRange.Add(1)
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
		"http-accept-gzip":          "true",
		"header":                    []string{"Range: bytes=1-1", "Accept-Encoding: gzip"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "segmented.bin"), data)
	if sawRange.Load() < 2 {
		t.Fatalf("expected segmented range requests, got %d", sawRange.Load())
	}
}

func TestSegmentedRangeRequestsDoNotUseWeakETagAsIfRange(t *testing.T) {
	data := bytes.Repeat([]byte("weak-if-range-"), 64*1024)
	var sawRange atomic.Bool
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("ETag", `W/"weak-range-etag"`)
			setDownloadHeaders(w, data)
			return
		}
		if got := r.Header.Get("If-Range"); got != "" {
			http.Error(w, "weak ETag used as If-Range "+got, http.StatusBadRequest)
			return
		}
		start, end, _ := parseTestRange(t, r.Header.Get("Range"), int64(len(data)))
		sawRange.Store(true)
		writeRange(w, data, start, end)
	}))
	defer src.Close()

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/weak-etag.bin"}, Options{
		"out":                       "weak-etag.bin",
		"split":                     "4",
		"max-connection-per-server": "4",
		"min-split-size":            "1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "weak-etag.bin"), data)
	if !sawRange.Load() {
		t.Fatal("expected segmented range request")
	}
}

func TestSegmentedDownloadRejectsMismatchedContentRange(t *testing.T) {
	data := bytes.Repeat([]byte("bad-content-range-"), 64*1024)
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setDownloadHeaders(w, data)
		if r.Method == http.MethodHead {
			return
		}
		rng := r.Header.Get("Range")
		if rng == "" {
			http.Error(w, "expected Range", http.StatusBadRequest)
			return
		}
		start, end, _ := parseTestRange(t, rng, int64(len(data)))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start+1, end+1, len(data)+1))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[start : end+1])
	}))
	defer src.Close()

	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/bad-range.bin"}, Options{
		"out":                       "bad-range.bin",
		"split":                     "4",
		"max-connection-per-server": "4",
		"min-split-size":            "1",
		"max-tries":                 "1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusError)
	status, err := engine.TellStatus(gid, []string{"errorMessage"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status["errorMessage"].(string), "Content-Range") {
		t.Fatalf("errorMessage = %#v, want Content-Range", status["errorMessage"])
	}
}

func TestHTTPRedirectWithoutFollowIsNotSavedAsDownload(t *testing.T) {
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/elsewhere", http.StatusFound)
	}))
	defer src.Close()

	dir := t.TempDir()
	noRedirect := &http.Client{
		Transport: http.DefaultTransport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	engine, err := NewEngine(Config{Dir: dir, HTTPClient: noRedirect})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/redirect.bin"}, Options{"out": "redirect.bin"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusError)
	if _, err := os.Stat(filepath.Join(dir, "redirect.bin")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("redirect response was saved or stat failed: %v", err)
	}
}

func TestContinueTreats416AtExistingLengthAsComplete(t *testing.T) {
	data := bytes.Repeat([]byte("already-complete-"), 1024)
	dir := t.TempDir()
	path := filepath.Join(dir, "complete.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	var sawRange atomic.Bool
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Range"); got == fmt.Sprintf("bytes=%d-", len(data)) {
			sawRange.Store(true)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(data)))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		http.Error(w, "unexpected request", http.StatusBadRequest)
	}))
	defer src.Close()

	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/complete.bin"}, Options{
		"out":      "complete.bin",
		"continue": "true",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, path, data)
	if !sawRange.Load() {
		t.Fatal("expected a resume Range request")
	}
}

func TestUnknownLengthHTTPDownloadPublishesObservedSize(t *testing.T) {
	data := bytes.Repeat([]byte("chunked-data"), 128*1024)
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		startedOnce.Do(func() { close(started) })
		<-release
		_, _ = w.Write(data)
	}))
	defer src.Close()

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/chunked.bin"}, Options{"out": "chunked.bin"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for chunked response to start")
	}
	status, err := engine.TellStatus(gid, []string{"status", "totalLength", "files"})
	if err != nil {
		t.Fatal(err)
	}
	if status["status"] != string(StatusActive) {
		t.Fatalf("status = %v, want active", status["status"])
	}
	if status["totalLength"] != "0" {
		t.Fatalf("active totalLength = %v, want 0 for unknown length", status["totalLength"])
	}
	files := status["files"].([]FileInfo)
	if files[0].Length != "0" {
		t.Fatalf("active file length = %v, want 0 for unknown length", files[0].Length)
	}

	close(release)
	waitForStatus(t, engine, gid, StatusComplete)
	status, err = engine.TellStatus(gid, []string{"totalLength", "completedLength", "files"})
	if err != nil {
		t.Fatal(err)
	}
	want := strconv.Itoa(len(data))
	if status["totalLength"] != want || status["completedLength"] != want {
		t.Fatalf("final lengths = total %v completed %v, want %s", status["totalLength"], status["completedLength"], want)
	}
	files = status["files"].([]FileInfo)
	if files[0].Length != want || files[0].CompletedLength != want {
		t.Fatalf("final file lengths = length %v completed %v, want %s", files[0].Length, files[0].CompletedLength, want)
	}
	assertFileEquals(t, filepath.Join(dir, "chunked.bin"), data)
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
