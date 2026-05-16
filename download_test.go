package goaria

import (
	"bytes"
	"context"
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

func TestChangeOptionScalesActiveSegmentedDownload(t *testing.T) {
	runActiveSegmentedConnectionScaleTest(t, func(engine *Engine, gid string) error {
		_, err := engine.ChangeOption(gid, Options{"max-connection-per-server": "4"})
		return err
	})
}

func TestGlobalMaxConnectionOptionScalesActiveSegmentedDownload(t *testing.T) {
	runActiveSegmentedConnectionScaleTest(t, func(engine *Engine, gid string) error {
		_, err := engine.ChangeGlobalOption(Options{"max-connection-per-server": "4"})
		return err
	})
}

func TestChangeGlobalOptionDoesNotRebaseExistingDownloadDerivedState(t *testing.T) {
	root := t.TempDir()
	oldDir := filepath.Join(root, "old")
	newDir := filepath.Join(root, "new")
	sessionPath := filepath.Join(root, "session.txt")
	engine, err := NewEngine(Config{Dir: oldDir, SaveSession: sessionPath})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddURI([]string{"http://example.invalid/file.bin"}, Options{
		"pause": "true",
		"out":   "file.bin",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ChangeGlobalOption(Options{"dir": newDir}); err != nil {
		t.Fatal(err)
	}

	global := engine.GetGlobalOption()
	if global["dir"] != newDir {
		t.Fatalf("global dir = %v, want %s", global["dir"], newDir)
	}
	opts, err := engine.GetOption(gid)
	if err != nil {
		t.Fatal(err)
	}
	if opts["dir"] != oldDir {
		t.Fatalf("download option dir = %v, want original dir %s", opts["dir"], oldDir)
	}
	status, err := engine.TellStatus(gid, []string{"dir", "files"})
	if err != nil {
		t.Fatal(err)
	}
	if status["dir"] != oldDir {
		t.Fatalf("status dir = %v, want original dir %s", status["dir"], oldDir)
	}
	files := status["files"].([]FileInfo)
	if got, want := files[0].Path, filepath.Join(oldDir, "file.bin"); got != want {
		t.Fatalf("file path = %s, want %s", got, want)
	}
	if _, err := engine.SaveSession(); err != nil {
		t.Fatal(err)
	}
	session, err := os.ReadFile(sessionPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(session), newDir) {
		t.Fatalf("saved session unexpectedly contains rebased dir %s:\n%s", newDir, string(session))
	}
	if !strings.Contains(string(session), oldDir) {
		t.Fatalf("saved session missing original dir %s:\n%s", oldDir, string(session))
	}
}

func runActiveSegmentedConnectionScaleTest(t *testing.T, adjust func(*Engine, string) error) {
	t.Helper()
	data := bytes.Repeat([]byte("active-scale-"), 64*1024)
	var activeRanges atomic.Int64
	var activeFull atomic.Int64
	var maxActiveRanges atomic.Int64

	rememberMax := func(n int64) {
		for {
			old := maxActiveRanges.Load()
			if n <= old || maxActiveRanges.CompareAndSwap(old, n) {
				return
			}
		}
	}
	slowWrite := func(w http.ResponseWriter, payload []byte) {
		flusher, _ := w.(http.Flusher)
		for len(payload) > 0 {
			n := 1024
			if len(payload) < n {
				n = len(payload)
			}
			if _, err := w.Write(payload[:n]); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			payload = payload[n:]
			time.Sleep(time.Millisecond)
		}
	}

	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setDownloadHeaders(w, data)
		if r.Method == http.MethodHead {
			return
		}
		rng := r.Header.Get("Range")
		if rng == "" {
			activeFull.Add(1)
			defer activeFull.Add(-1)
			w.WriteHeader(http.StatusOK)
			slowWrite(w, data)
			return
		}
		start, end, _ := parseTestRange(t, rng, int64(len(data)))
		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}
		current := activeRanges.Add(1)
		rememberMax(current)
		defer activeRanges.Add(-1)
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.WriteHeader(http.StatusPartialContent)
		slowWrite(w, data[start:end+1])
	}))
	defer src.Close()

	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddURI([]string{src.URL + "/active-scale.bin"}, Options{
		"out":            "active-scale.bin",
		"min-split-size": "1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && activeRanges.Load()+activeFull.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if activeRanges.Load()+activeFull.Load() == 0 {
		t.Fatal("timed out waiting for initial HTTP transfer")
	}

	if err := adjust(engine, gid); err != nil {
		t.Fatal(err)
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && maxActiveRanges.Load() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := maxActiveRanges.Load(); got < 2 {
		status, err := engine.TellStatus(gid, []string{"connections", "status"})
		if err != nil {
			t.Fatal(err)
		}
		t.Fatalf("active range connections peaked at %d, want at least 2 after max-connection-per-server change; status=%#v", got, status)
	}

	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(dir, "active-scale.bin"), data)
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
