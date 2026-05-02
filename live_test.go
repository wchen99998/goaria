package goaria

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestLiveRealDownloadTargets(t *testing.T) {
	if os.Getenv("GOARIA_LIVE_TESTS") != "1" {
		t.Skip("set GOARIA_LIVE_TESTS=1 to run public internet download checks")
	}
	cases := []struct {
		name string
		env  string
		url  string
		opts Options
	}{
		{
			name: "http1",
			env:  "GOARIA_LIVE_HTTP1_URL",
			url:  "http://example.com/",
			opts: Options{"http-version": "1.1", "out": "live-http1.bin", "max-tries": "2"},
		},
		{
			name: "http2",
			env:  "GOARIA_LIVE_HTTP2_URL",
			url:  "https://www.rfc-editor.org/rfc/rfc9114.txt",
			opts: Options{"http-version": "2", "out": "live-http2.bin", "max-tries": "2"},
		},
		{
			name: "http3",
			env:  "GOARIA_LIVE_HTTP3_URL",
			url:  "https://cloudflare-quic.com/",
			opts: Options{"http-version": "3", "out": "live-http3.bin", "max-tries": "2"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rawURL := os.Getenv(tc.env)
			if rawURL == "" {
				rawURL = tc.url
			}
			dir := t.TempDir()
			engine, err := NewEngine(Config{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			defer engine.Close(context.Background())
			gid, err := engine.AddURI([]string{rawURL}, tc.opts, nil)
			if err != nil {
				t.Fatal(err)
			}
			waitForStatus(t, engine, gid, StatusComplete)
			path := filepath.Join(dir, optionString(tc.opts, "out"))
			st, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if st.Size() == 0 {
				t.Fatalf("downloaded empty file from %s", rawURL)
			}
		})
	}
}

func TestLiveChaosProxyWithRealHTTPDownloadTarget(t *testing.T) {
	if os.Getenv("GOARIA_LIVE_TESTS") != "1" {
		t.Skip("set GOARIA_LIVE_TESTS=1 to run public internet chaos checks")
	}
	target := os.Getenv("GOARIA_LIVE_CHAOS_HTTP_URL")
	if target == "" {
		target = "http://example.com/"
	}
	var attempts atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			http.Error(w, "planned live chaos failure", http.StatusServiceUnavailable)
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
	gid, err := engine.AddURI([]string{target}, Options{
		"out":        "live-chaos.bin",
		"http-proxy": proxy.URL,
		"max-tries":  "3",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	st, err := os.Stat(filepath.Join(dir, "live-chaos.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() == 0 {
		t.Fatal("downloaded empty live chaos file")
	}
	if attempts.Load() < 2 {
		t.Fatalf("expected chaos retry through proxy, attempts=%d", attempts.Load())
	}
}
