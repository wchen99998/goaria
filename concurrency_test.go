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

func TestThousandConcurrentHTTPDownloads(t *testing.T) {
	runThousandConcurrentHTTPDownloads(t)
}

func BenchmarkThousandConcurrentHTTPDownloads(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		runThousandConcurrentHTTPDownloads(b)
	}
}

func runThousandConcurrentHTTPDownloads(tb testing.TB) {
	const downloads = 1000
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
	engine, err := NewEngine(Config{Dir: dir, MaxConcurrentDownloads: downloads})
	if err != nil {
		tb.Fatal(err)
	}
	defer engine.Close(context.Background())
	if _, err := engine.ChangeGlobalOption(Options{"max-concurrent-downloads": fmt.Sprint(downloads)}); err != nil {
		tb.Fatal(err)
	}

	gids := make([]string, 0, downloads)
	for i := 0; i < downloads; i++ {
		gid, err := engine.AddURI([]string{src.URL + "/scale"}, Options{
			"out":       fmt.Sprintf("scale-%04d.bin", i),
			"max-tries": "1",
			"split":     "1",
		}, nil)
		if err != nil {
			tb.Fatalf("add %d: %v", i, err)
		}
		gids = append(gids, gid)
	}

	deadline := time.Now().Add(20 * time.Second)
	for _, gid := range gids {
		for {
			if time.Now().After(deadline) {
				tb.Fatalf("timed out waiting for 1000 downloads; max active observed %d", maxActive.Load())
			}
			status, err := engine.TellStatus(gid, []string{"status", "errorMessage"})
			if err != nil {
				tb.Fatal(err)
			}
			switch status["status"] {
			case string(StatusComplete):
				goto next
			case string(StatusError):
				tb.Fatalf("download %s errored: %v", gid, status["errorMessage"])
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}
	next:
	}

	for i := 0; i < downloads; i++ {
		assertFileEquals(tb, filepath.Join(dir, fmt.Sprintf("scale-%04d.bin", i)), data)
	}
	if maxActive.Load() < 100 {
		tb.Fatalf("expected substantial parallelism, max active observed %d", maxActive.Load())
	}
}
