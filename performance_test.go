package goaria

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkDownloadSmallFile(b *testing.B) {
	benchmarkHTTPDownload(b, 4<<10, Options{"split": "1"}, 1)
}

func BenchmarkDownloadMediumFile(b *testing.B) {
	benchmarkHTTPDownload(b, 2<<20, Options{}, 1)
}

func BenchmarkDownloadLargeFile(b *testing.B) {
	benchmarkHTTPDownload(b, 32<<20, Options{
		"split":                     "8",
		"max-connection-per-server": "8",
		"min-split-size":            "1",
	}, 8)
}

func benchmarkHTTPDownload(b *testing.B, size int, opts Options, maxConcurrent int) {
	b.Helper()
	data := bytes.Repeat([]byte("0123456789abcdef"), (size+15)/16)
	data = data[:size]
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "bench.bin", time.Now(), bytes.NewReader(data))
	}))
	defer src.Close()

	dir := b.TempDir()
	engine, err := NewEngine(Config{Dir: dir, MaxConcurrentDownloads: maxConcurrent, MaxDownloadResult: b.N + 1})
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close(context.Background())
	events, cancel := engine.Subscribe(maxConcurrent + 4)
	defer cancel()

	b.ReportAllocs()
	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runBenchDownload(b, engine, events, src.URL+"/bench.bin", filepath.Join("bench", fmt.Sprintf("%06d.bin", i)), opts)
	}
}

func BenchmarkConcurrentHTTPDownloads1750(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		runConcurrentHTTPDownloads(b, 1750)
	}
}

func runBenchDownload(b *testing.B, engine *Engine, events <-chan Notification, uri, out string, opts Options) {
	b.Helper()
	downloadOpts := cloneOptions(opts)
	downloadOpts["out"] = out
	downloadOpts["max-tries"] = "1"
	gid, err := engine.AddURI([]string{uri}, downloadOpts, nil)
	if err != nil {
		b.Fatal(err)
	}
	deadline := time.After(30 * time.Second)
	for {
		select {
		case n := <-events:
			if n.GID != gid {
				continue
			}
			switch n.Method {
			case "aria2.onDownloadComplete":
				return
			case "aria2.onDownloadError":
				status, err := engine.TellStatus(gid, []string{"errorMessage"})
				if err != nil {
					b.Fatal(err)
				}
				b.Fatalf("download errored: %v", status["errorMessage"])
			}
		case <-deadline:
			b.Fatalf("timed out waiting for download %s", gid)
		}
	}
}
