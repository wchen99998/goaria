package goaria

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"mime"
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

type closeTrackingResponseBody struct {
	io.Reader
	closed atomic.Bool
}

func (b *closeTrackingResponseBody) Close() error {
	b.closed.Store(true)
	return nil
}

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

func TestHeaderUserAgentOverridesOptionUserAgent(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "https://example.test/file", nil)
	if err != nil {
		t.Fatal(err)
	}
	applyRequestOptions(req, Options{
		"user-agent": "goaria/default",
		"header": []string{
			"User-Agent: Mozilla/5.0",
			"X-Goaria-Test: ok",
		},
	})
	if got := req.Header.Values("User-Agent"); len(got) != 1 || got[0] != "Mozilla/5.0" {
		t.Fatalf("User-Agent values = %#v, want header override only", got)
	}
	if got := req.Header.Get("X-Goaria-Test"); got != "ok" {
		t.Fatalf("X-Goaria-Test = %q", got)
	}
}

func TestHeaderHostOverridesRequestHost(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "https://origin.example/file", nil)
	if err != nil {
		t.Fatal(err)
	}
	applyRequestOptions(req, Options{
		"header": []string{"Host: mirror.example:8443"},
	})
	if req.Host != "mirror.example:8443" {
		t.Fatalf("Request.Host = %q, want mirror.example:8443", req.Host)
	}
	if got := req.Header.Get("Host"); got != "" {
		t.Fatalf("ordinary Host header = %q, want empty", got)
	}
}

func TestHostOverrideSurvivesRedirects(t *testing.T) {
	data := []byte("redirect host")
	var seen atomic.Int32
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "mirror.example" {
			t.Errorf("request %s Host = %q, want mirror.example", r.URL.Path, r.Host)
		}
		seen.Add(1)
		if r.URL.Path == "/start" {
			http.Redirect(w, r, "/final", http.StatusFound)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		_, _ = w.Write(data)
	}))
	defer src.Close()
	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/start"}, Options{
		"out":    "host.bin",
		"split":  "1",
		"header": []string{"Host: mirror.example"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	if seen.Load() < 2 {
		t.Fatalf("server saw %d requests, want redirect and final request", seen.Load())
	}
	assertFileEquals(t, filepath.Join(dir, "host.bin"), data)
}

func TestWildcardRefererStripsURLCredentials(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "https://user:secret@example.test/file", nil)
	if err != nil {
		t.Fatal(err)
	}
	applyRequestOptions(req, Options{"referer": "*"})
	if got := req.Header.Get("Referer"); got != "https://example.test/file" {
		t.Fatalf("Referer = %q, want credentials stripped", got)
	}
}

func TestFilenameFromResponseSanitizesContentDisposition(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{name: "safe.bin", want: "safe.bin"},
		{name: "../../outside.bin", want: "outside.bin"},
		{name: "..", want: "index.html"},
		{name: ".", want: "index.html"},
		{name: "/", want: "index.html"},
		{name: `\`, want: "index.html"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &http.Response{Header: make(http.Header)}
			resp.Header.Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": tt.name}))
			if got := filenameFromResponse(resp, "https://example.test/fallback.bin"); got != tt.want {
				t.Fatalf("filenameFromResponse = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFilenameFromResponseSanitizesRedirectURLPath(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "https://example.test/files/%2e%2e", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp := &http.Response{Header: make(http.Header), Request: req}
	if got := filenameFromResponse(resp, "https://example.test/fallback.bin"); got != "index.html" {
		t.Fatalf("filenameFromResponse = %q, want index.html", got)
	}
}

func TestFilenameFromURIUsesDecodedURLPath(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{raw: "https://example.test/", want: "index.html"},
		{raw: "https://example.test/files/", want: "index.html"},
		{raw: "https://example.test?token=secret#fragment", want: "index.html"},
		{raw: "https://example.test/files/report%20one.txt?token=secret#fragment", want: "report one.txt"},
		{raw: "ftp://example.test/pub/archive.tar.gz?mirror=1", want: "archive.tar.gz"},
		{raw: "https://example.test/files/a%2Fb.txt", want: "b.txt"},
		{raw: "https://example.test/files/%2e%2e", want: "index.html"},
		{raw: "https://example.test/%zz?token=secret#fragment", want: "%zz"},
		{raw: "", want: "index.html"},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			if got := filenameFromURI(tt.raw); got != tt.want {
				t.Fatalf("filenameFromURI(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
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

func TestHTTPAuthChallengeRetriesRedirectedRequest(t *testing.T) {
	data := []byte("redirected challenge")
	var protectedRequests atomic.Int32
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			http.Redirect(w, r, "/protected", http.StatusFound)
			return
		}
		protectedRequests.Add(1)
		user, pass, ok := r.BasicAuth()
		if !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="redirect"`)
			http.Error(w, "auth required", http.StatusUnauthorized)
			return
		}
		if user != "user" || pass != "pass" {
			http.Error(w, "bad auth", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		_, _ = w.Write(data)
	}))
	defer src.Close()
	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/start"}, Options{
		"out":                 "redirect-auth.bin",
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
	if protectedRequests.Load() != 2 {
		t.Fatalf("protected request count = %d, want challenge plus retry", protectedRequests.Load())
	}
	assertFileEquals(t, filepath.Join(dir, "redirect-auth.bin"), data)
}

func TestHTTPAuthChallengeDoesNotCrossRedirectOrigins(t *testing.T) {
	var authenticated atomic.Int32
	protected := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, _, ok := r.BasicAuth(); ok {
			authenticated.Add(1)
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="cross-origin"`)
		http.Error(w, "auth required", http.StatusUnauthorized)
	}))
	defer protected.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, protected.URL+"/protected", http.StatusFound)
	}))
	defer origin.Close()

	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{origin.URL + "/start"}, Options{
		"out":                 "cross-origin-auth.bin",
		"split":               "1",
		"max-tries":           "1",
		"http-user":           "user",
		"http-passwd":         "pass",
		"http-auth-challenge": "true",
		"use-head":            "false",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusError)
	if got := authenticated.Load(); got != 0 {
		t.Fatalf("credentials were sent %d times to a cross-origin redirect target", got)
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

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
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

func TestContinueDoesNotTreatPartialFileAsConditionallyComplete(t *testing.T) {
	data := []byte("complete resumed representation")
	const prefixLength = 9
	var sawRange atomic.Bool
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-Modified-Since") != "" || r.Header.Get("If-None-Match") != "" {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Accept-Ranges", "bytes")
			return
		}
		if got := r.Header.Get("Range"); got != fmt.Sprintf("bytes=%d-", prefixLength) {
			t.Errorf("Range = %q, want resume from %d", got, prefixLength)
		} else {
			sawRange.Store(true)
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", prefixLength, len(data)-1, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(len(data)-prefixLength))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[prefixLength:])
	}))
	defer src.Close()
	dir := t.TempDir()
	path := filepath.Join(dir, "resume.bin")
	if err := os.WriteFile(path, data[:prefixLength], 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/resume.bin"}, Options{
		"out":             "resume.bin",
		"split":           "1",
		"continue":        "true",
		"conditional-get": "true",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	if !sawRange.Load() {
		t.Fatal("resume request was replaced by a conditional 304")
	}
	assertFileEquals(t, path, data)
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
	status, err := engine.TellStatus(gid, []string{"totalLength", "completedLength", "files"})
	if err != nil {
		t.Fatal(err)
	}
	want := strconv.Itoa(len(data))
	if status["totalLength"] != want {
		t.Fatalf("totalLength = %v, want %s", status["totalLength"], want)
	}
	if status["completedLength"] != "0" {
		t.Fatalf("dry-run completedLength = %v, want 0", status["completedLength"])
	}
	files := status["files"].([]FileInfo)
	if files[0].Length != want {
		t.Fatalf("file length = %v, want %s", files[0].Length, want)
	}
	if files[0].CompletedLength != "0" {
		t.Fatalf("dry-run file completedLength = %v, want 0", files[0].CompletedLength)
	}
}

func TestHTTPProbeClosesHEADBodyBeforeRangeFallback(t *testing.T) {
	headBody := &closeTrackingResponseBody{Reader: strings.NewReader("")}
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.Method {
		case http.MethodHead:
			return &http.Response{
				StatusCode:    http.StatusOK,
				Status:        "200 OK",
				Header:        http.Header{"Accept-Ranges": []string{"bytes"}},
				Body:          headBody,
				ContentLength: -1,
				Request:       req,
			}, nil
		case http.MethodGet:
			if !headBody.closed.Load() {
				return nil, errors.New("HEAD body still open during range fallback")
			}
			return &http.Response{
				StatusCode:    http.StatusPartialContent,
				Status:        "206 Partial Content",
				Header:        http.Header{"Content-Range": []string{"bytes 0-0/1"}},
				Body:          io.NopCloser(strings.NewReader("x")),
				ContentLength: 1,
				Request:       req,
			}, nil
		default:
			return nil, fmt.Errorf("unexpected method %s", req.Method)
		}
	})}
	engine := &Engine{client: client}
	meta, err := engine.probe(context.Background(), "https://example.test/file", Options{}, "")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Length != 1 || !meta.AcceptRange {
		t.Fatalf("probe metadata = %#v, want one-byte ranged response", meta)
	}
}

func TestHTTPResponseBodyIdleTimeout(t *testing.T) {
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "4")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer src.Close()

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	started := time.Now()
	gid, err := engine.AddURI([]string{src.URL + "/stall.bin"}, Options{
		"out":       "stall.bin",
		"split":     "1",
		"use-head":  "false",
		"timeout":   "1",
		"max-tries": "1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusError)
	if elapsed := time.Since(started); elapsed > 4*time.Second {
		t.Fatalf("idle response timed out after %v, want under 4s", elapsed)
	}
	status, err := engine.TellStatus(gid, []string{"errorMessage"})
	if err != nil {
		t.Fatal(err)
	}
	if message := status["errorMessage"].(string); !strings.Contains(message, "response body idle timeout") {
		t.Fatalf("errorMessage = %q, want idle timeout", message)
	}
}

func TestHTTPResponseHeaderTimeout(t *testing.T) {
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer src.Close()
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	started := time.Now()
	gid, err := engine.AddURI([]string{src.URL + "/headers.bin"}, Options{
		"out":       "headers.bin",
		"split":     "1",
		"use-head":  "false",
		"timeout":   "1",
		"max-tries": "1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusError)
	if elapsed := time.Since(started); elapsed > 4*time.Second {
		t.Fatalf("response header timeout took %v", elapsed)
	}
	status, err := engine.TellStatus(gid, []string{"errorMessage"})
	if err != nil {
		t.Fatal(err)
	}
	if message := status["errorMessage"].(string); !strings.Contains(message, "response header timeout") {
		t.Fatalf("errorMessage = %q, want response header timeout", message)
	}
}

func TestContinueReusesExplicitZeroLengthOutput(t *testing.T) {
	data := []byte("restarted in place")
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Range"); got != "" {
			t.Errorf("zero-length resume sent Range %q", got)
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		_, _ = w.Write(data)
	}))
	defer src.Close()

	dir := t.TempDir()
	path := filepath.Join(dir, "zero.bin")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/zero.bin"}, Options{
		"out":       "zero.bin",
		"split":     "1",
		"continue":  "true",
		"use-head":  "false",
		"max-tries": "1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, path, data)
	if _, err := os.Stat(path + ".1"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("auto-renamed output exists or stat failed: %v", err)
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
		"goaria-http-segment-size":  "16K",
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

func TestSegmentedRetryRollsBackFailedAttemptProgress(t *testing.T) {
	const segmentSize = 16 * 1024
	data := bytes.Repeat([]byte("r"), 4*segmentSize)
	var firstRangeAttempts atomic.Int32
	var otherRanges atomic.Int32
	retryStarted := make(chan struct{})
	otherRangesDone := make(chan struct{})
	releaseRetry := make(chan struct{})
	var retryStartedOnce, otherRangesOnce, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseRetry) })

	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			setDownloadHeaders(w, data)
			return
		}
		start, end, _ := parseTestRange(t, r.Header.Get("Range"), int64(len(data)))
		if start == 0 {
			if firstRangeAttempts.Add(1) == 1 {
				length := end - start + 1
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
				w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(data[:length/2])
				return
			}
			retryStartedOnce.Do(func() { close(retryStarted) })
			<-releaseRetry
			writeRange(w, data, start, end)
			return
		}
		writeRange(w, data, start, end)
		if otherRanges.Add(1) == 3 {
			otherRangesOnce.Do(func() { close(otherRangesDone) })
		}
	}))
	defer src.Close()

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{src.URL + "/retry-progress.bin"}, Options{
		"out":                       "retry-progress.bin",
		"split":                     "4",
		"max-connection-per-server": "4",
		"min-split-size":            "1",
		"goaria-http-segment-size":  "16K",
		"max-tries":                 "2",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, ready := range map[string]<-chan struct{}{
		"range retry":  retryStarted,
		"other ranges": otherRangesDone,
	} {
		select {
		case <-ready:
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for %s", name)
		}
	}

	wantCompleted := int64(len(data) - segmentSize)
	deadline := time.Now().Add(2 * time.Second)
	for {
		status, err := engine.TellStatus(gid, []string{"completedLength"})
		if err != nil {
			t.Fatal(err)
		}
		completed, err := strconv.ParseInt(status["completedLength"].(string), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		if completed > wantCompleted {
			t.Fatalf("completedLength = %d, includes bytes from failed range attempt; want at most %d", completed, wantCompleted)
		}
		if completed == wantCompleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("completedLength = %d, want %d after successful ranges", completed, wantCompleted)
		}
		time.Sleep(10 * time.Millisecond)
	}
	releaseOnce.Do(func() { close(releaseRetry) })
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "retry-progress.bin"), data)
	if firstRangeAttempts.Load() != 2 {
		t.Fatalf("first range attempts = %d, want 2", firstRangeAttempts.Load())
	}
}

func TestSegmentedRangeRequestsDoNotUseWeakETagAsIfRange(t *testing.T) {
	data := bytes.Repeat([]byte("weak-if-range-"), 64*1024)
	lastModified := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC).Format(http.TimeFormat)
	var sawRange atomic.Bool
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Last-Modified", lastModified)
		if r.Method == http.MethodHead {
			w.Header().Set("ETag", `W/"weak-range-etag"`)
			setDownloadHeaders(w, data)
			w.Header().Set("Last-Modified", lastModified)
			return
		}
		if got := r.Header.Get("If-Range"); got != lastModified {
			http.Error(w, "range If-Range = "+got, http.StatusBadRequest)
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
		"goaria-http-segment-size":  "16K",
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
	tests := map[string]func(start, end, total int64) string{
		"bounds": func(start, end, total int64) string {
			return fmt.Sprintf("bytes %d-%d/%d", start+1, end+1, total+1)
		},
		"total": func(start, end, total int64) string {
			return fmt.Sprintf("bytes %d-%d/%d", start, end, total+1)
		},
	}
	for name, contentRange := range tests {
		t.Run(name, func(t *testing.T) {
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
				w.Header().Set("Content-Range", contentRange(start, end, int64(len(data))))
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
				"goaria-http-segment-size":  "16K",
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
		})
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
