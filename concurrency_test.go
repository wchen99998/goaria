package goaria

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

const scaleDownloads = 1750

func TestScaleConcurrentHTTPDownloads(t *testing.T) {
	runConcurrentHTTPDownloads(t, scaleDownloads)
}

func BenchmarkScaleConcurrentHTTPDownloads(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		runConcurrentHTTPDownloads(b, scaleDownloads)
	}
}

func BenchmarkThousandConcurrentHTTPDownloads(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		runConcurrentHTTPDownloads(b, 1000)
	}
}

func runConcurrentHTTPDownloads(tb testing.TB, downloads int) {
	data := []byte("scale-ok")
	var active atomic.Int32
	var maxActive atomic.Int32
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			setDownloadHeaders(w, data)
			return
		}
		current := active.Add(1)
		for {
			prev := maxActive.Load()
			if current <= prev || maxActive.CompareAndSwap(prev, current) {
				break
			}
		}
		defer active.Add(-1)
		time.Sleep(5 * time.Millisecond)
		setDownloadHeaders(w, data)
		_, _ = w.Write(data)
	}))
	defer src.Close()

	dir := tb.TempDir()
	engine, err := NewEngine(Config{Dir: dir, MaxConcurrentDownloads: downloads, MaxDownloadResult: downloads})
	if err != nil {
		tb.Fatal(err)
	}
	defer engine.Close(context.Background())
	if _, err := engine.ChangeGlobalOption(Options{"max-concurrent-downloads": fmt.Sprint(downloads)}); err != nil {
		tb.Fatal(err)
	}
	events, cancelEvents := engine.Subscribe(downloads * 2)
	defer cancelEvents()

	pending := make(map[string]struct{}, downloads)
	for i := 0; i < downloads; i++ {
		gid, err := engine.AddURI([]string{src.URL + "/scale"}, Options{
			"out":       fmt.Sprintf("scale-%04d.bin", i),
			"max-tries": "1",
			"split":     "1",
		}, nil)
		if err != nil {
			tb.Fatalf("add %d: %v", i, err)
		}
		pending[gid] = struct{}{}
	}

	deadline := time.After(20 * time.Second)
	for len(pending) > 0 {
		select {
		case n := <-events:
			if _, ok := pending[n.GID]; !ok {
				continue
			}
			switch n.Method {
			case "aria2.onDownloadComplete":
				delete(pending, n.GID)
			case "aria2.onDownloadError":
				status, err := engine.TellStatus(n.GID, []string{"errorMessage"})
				if err != nil {
					tb.Fatal(err)
				}
				tb.Fatalf("download %s errored: %v", n.GID, status["errorMessage"])
			}
		case <-deadline:
			tb.Fatalf("timed out waiting for %d downloads; %d pending; max active observed %d", downloads, len(pending), maxActive.Load())
		}
	}

	for i := 0; i < downloads; i++ {
		assertFileEquals(tb, filepath.Join(dir, fmt.Sprintf("scale-%04d.bin", i)), data)
	}
	if maxActive.Load() < 125 {
		tb.Fatalf("expected substantial parallelism, max active observed %d", maxActive.Load())
	}
}
