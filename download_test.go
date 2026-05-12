package goaria

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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

func TestDownloadSpeedTrackerAggregatesConcurrentWorkers(t *testing.T) {
	d := &Download{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tracker := startDownloadSpeedTrackerWithInterval(ctx, d, 100*time.Millisecond)
	defer tracker.stopAndWait()

	for i := 0; i < 4; i++ {
		tracker.add(256 * 1024)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		d.mu.RLock()
		speed := d.downloadBPS
		d.mu.RUnlock()
		if speed > 0 {
			if speed < 5*1024*1024 {
				t.Fatalf("downloadBPS = %d, expected aggregate worker speed", speed)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for aggregate download speed")
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

func TestControlMethodsReturnGIDAndStoppedRemoveRequiresResultRemoval(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir(), MaxConcurrentDownloads: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	if _, err := engine.ChangeGlobalOption(Options{"max-concurrent-downloads": "0"}); err != nil {
		t.Fatal(err)
	}

	gid, err := engine.AddURI([]string{"http://example.invalid/a"}, Options{"pause": "true"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := engine.Unpause(gid); err != nil || got != gid {
		t.Fatalf("Unpause = %q, %v; want %q, nil", got, err, gid)
	}
	if got, err := engine.Pause(gid); err != nil || got != gid {
		t.Fatalf("Pause = %q, %v; want %q, nil", got, err, gid)
	}
	if got, err := engine.Remove(gid); err != nil || got != gid {
		t.Fatalf("Remove = %q, %v; want %q, nil", got, err, gid)
	}
	if _, err := engine.Remove(gid); err == nil {
		t.Fatal("Remove on stopped download succeeded; want error")
	}
	if got, err := engine.RemoveDownloadResult(gid); err != nil || got != "OK" {
		t.Fatalf("RemoveDownloadResult = %q, %v; want OK, nil", got, err)
	}

	forceGID, err := engine.AddURI([]string{"http://example.invalid/b"}, Options{"pause": "true"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := engine.ForceRemove(forceGID); err != nil || got != forceGID {
		t.Fatalf("ForceRemove = %q, %v; want %q, nil", got, err, forceGID)
	}
}

func TestDefaultOptionsPreferAria2Compatibility(t *testing.T) {
	opts := defaultOptions(t.TempDir(), 0, 0)
	for key, want := range map[string]string{
		"split":                     "5",
		"max-connection-per-server": "1",
		"min-split-size":            "20M",
		"allow-overwrite":           "false",
		"auto-file-renaming":        "true",
		"continue":                  "false",
	} {
		if got := optionString(opts, key); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
}

func TestExistingOutputFileAutoRenamedByDefault(t *testing.T) {
	data := []byte("replacement")
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "file.txt", time.Now(), bytes.NewReader(data))
	}))
	defer src.Close()

	dir := t.TempDir()
	original := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(original, []byte("existing"), 0o644); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddURI([]string{src.URL + "/file.txt"}, Options{"out": "file.txt", "split": "1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, original, []byte("existing"))
	assertFileEquals(t, filepath.Join(dir, "file.txt.1"), data)
}

func TestExistingOutputFileFailsWhenRenamingAndOverwriteDisabled(t *testing.T) {
	data := []byte("replacement")
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "file.txt", time.Now(), bytes.NewReader(data))
	}))
	defer src.Close()

	dir := t.TempDir()
	original := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(original, []byte("existing"), 0o644); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddURI([]string{src.URL + "/file.txt"}, Options{
		"out":                "file.txt",
		"split":              "1",
		"auto-file-renaming": "false",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusError)
	status, err := engine.TellStatus(gid, []string{"errorMessage"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status["errorMessage"].(string), "file already exists") {
		t.Fatalf("errorMessage = %#v, want file already exists", status["errorMessage"])
	}
	assertFileEquals(t, original, []byte("existing"))
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
