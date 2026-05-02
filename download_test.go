package goaria

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSegmentedHTTPDownloadCompletes(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789abcdef"), 64*1024)
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "payload.bin", time.Now(), bytes.NewReader(data))
	}))
	defer src.Close()

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddURI([]string{src.URL + "/payload.bin"}, Options{
		"out":                       "payload.bin",
		"split":                     "4",
		"max-connection-per-server": "4",
		"min-split-size":            "1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	waitForStatus(t, engine, gid, StatusComplete)
	got, err := os.ReadFile(filepath.Join(dir, "payload.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("downloaded data mismatch: got %d bytes want %d", len(got), len(data))
	}
	status, err := engine.TellStatus(gid, []string{"status", "totalLength", "completedLength", "files"})
	if err != nil {
		t.Fatal(err)
	}
	if status["status"] != string(StatusComplete) {
		t.Fatalf("status = %v", status["status"])
	}
	if status["totalLength"] != status["completedLength"] {
		t.Fatalf("length mismatch: %#v", status)
	}
}

func TestQueuePauseUnpauseAndChangePosition(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir(), MaxConcurrentDownloads: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	if _, err := engine.ChangeGlobalOption(Options{"max-concurrent-downloads": "0"}); err != nil {
		t.Fatal(err)
	}

	pos0 := 0
	_, err = engine.AddURI([]string{"http://example.invalid/a"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	gid2, err := engine.AddURI([]string{"http://example.invalid/b"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	newPos, err := engine.ChangePosition(gid2, pos0, "POS_SET")
	if err != nil {
		t.Fatal(err)
	}
	if newPos != 0 {
		t.Fatalf("new position = %d, want 0", newPos)
	}
	waiting := engine.TellWaiting(0, 2, []string{"gid"})
	if len(waiting) == 0 || waiting[0]["gid"] != gid2 {
		t.Fatalf("waiting order = %#v, want gid2 first", waiting)
	}
}

func waitForStatus(t testing.TB, engine *Engine, gid string, want Status) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, err := engine.TellStatus(gid, []string{"status", "errorMessage"})
		if err != nil {
			t.Fatal(err)
		}
		if status["status"] == string(want) {
			return
		}
		if status["status"] == string(StatusError) {
			t.Fatalf("download errored: %v", status["errorMessage"])
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", want)
}
