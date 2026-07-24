package goaria

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFinishDownloadIgnoresStaleCanceledRun(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	oldCtx, oldCancel := context.WithCancel(context.Background())
	defer oldCancel()
	currentCtx, currentCancel := context.WithCancel(context.Background())
	defer currentCancel()

	d := &Download{
		gid:     "stale",
		status:  StatusActive,
		ctx:     currentCtx,
		torrent: &torrentRuntime{},
	}
	engine.mu.Lock()
	engine.downloads[d.gid] = d
	engine.active[d.gid] = struct{}{}
	engine.mu.Unlock()

	engine.finishDownload(d, oldCtx, context.Canceled)

	d.mu.RLock()
	status := d.status
	runtime := d.torrent
	d.mu.RUnlock()
	if status != StatusActive {
		t.Fatalf("status = %s, want %s", status, StatusActive)
	}
	if runtime == nil {
		t.Fatal("stale run cleared current torrent runtime")
	}

	engine.mu.RLock()
	_, active := engine.active[d.gid]
	engine.mu.RUnlock()
	if !active {
		t.Fatal("stale run removed current active marker")
	}
}

func TestFinishDownloadTreatsCurrentCancellationAsPaused(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := &Download{
		gid:     "current",
		status:  StatusActive,
		ctx:     runCtx,
		torrent: &torrentRuntime{},
	}
	engine.mu.Lock()
	engine.downloads[d.gid] = d
	engine.active[d.gid] = struct{}{}
	engine.mu.Unlock()

	engine.finishDownload(d, runCtx, context.Canceled)

	d.mu.RLock()
	status := d.status
	errorCode := d.errorCode
	errorMessage := d.errorMessage
	runtime := d.torrent
	d.mu.RUnlock()
	if status != StatusPaused {
		t.Fatalf("status = %s, want %s", status, StatusPaused)
	}
	if errorCode != "" || errorMessage != "" {
		t.Fatalf("unexpected error fields: code=%q message=%q", errorCode, errorMessage)
	}
	if runtime != nil {
		t.Fatal("current finished run kept torrent runtime")
	}

	engine.mu.RLock()
	_, active := engine.active[d.gid]
	engine.mu.RUnlock()
	if active {
		t.Fatal("current finished run kept active marker")
	}
}

func TestFinishDownloadCleansRuntimeAfterPause(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := &Download{
		gid:     "paused",
		status:  StatusPaused,
		ctx:     nil,
		torrent: &torrentRuntime{},
	}
	engine.mu.Lock()
	engine.downloads[d.gid] = d
	engine.mu.Unlock()

	engine.finishDownload(d, runCtx, context.Canceled)

	d.mu.RLock()
	status := d.status
	runtime := d.torrent
	d.mu.RUnlock()
	if status != StatusPaused {
		t.Fatalf("status = %s, want %s", status, StatusPaused)
	}
	if runtime != nil {
		t.Fatal("paused stale run kept old torrent runtime")
	}
}

func TestChangePositionInvalidModePreservesWaitingQueue(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	if _, err := engine.ChangeGlobalOption(Options{"max-concurrent-downloads": "0"}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.AddURI([]string{"http://example.invalid/one"}, nil, nil); err != nil {
		t.Fatal(err)
	}
	gid, err := engine.AddURI([]string{"http://example.invalid/two"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	engine.mu.RLock()
	before := append([]string(nil), engine.waiting...)
	engine.mu.RUnlock()
	if _, err := engine.ChangePosition(gid, 0, "INVALID"); err == nil {
		t.Fatal("ChangePosition accepted an invalid mode")
	}
	engine.mu.RLock()
	after := append([]string(nil), engine.waiting...)
	engine.mu.RUnlock()
	if !slices.Equal(after, before) {
		t.Fatalf("waiting queue changed after rejected position: got %v want %v", after, before)
	}
}

func TestChangeGlobalOptionPrunesStoppedResultsImmediately(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir(), MaxDownloadResult: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gids := []string{"0000000000000001", "0000000000000002", "0000000000000003"}
	engine.mu.Lock()
	for _, gid := range gids {
		d := newDownload(gid, []string{"https://example.invalid/file"}, layerOptions(engine.global, nil))
		d.status = StatusComplete
		engine.downloads[gid] = d
		engine.appendStoppedLocked(gid)
	}
	engine.mu.Unlock()

	if _, err := engine.ChangeGlobalOption(Options{"max-download-result": "1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.TellStatus(gids[0], nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("oldest result lookup error = %v, want ErrNotFound", err)
	}
	if _, err := engine.TellStatus(gids[1], nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("middle result lookup error = %v, want ErrNotFound", err)
	}
	if _, err := engine.TellStatus(gids[2], nil); err != nil {
		t.Fatalf("newest result was pruned: %v", err)
	}

	if _, err := engine.ChangeGlobalOption(Options{"max-download-result": "0"}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.TellStatus(gids[2], nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("max-download-result=0 retained completed result: %v", err)
	}
	engine.mu.Lock()
	for _, gid := range []string{"0000000000000004", "0000000000000005"} {
		d := newDownload(gid, []string{"https://example.invalid/file"}, layerOptions(engine.global, nil))
		d.status = StatusComplete
		engine.downloads[gid] = d
		engine.appendStoppedLocked(gid)
	}
	got := len(engine.stopped)
	engine.mu.Unlock()
	if got != 0 {
		t.Fatalf("disabled stopped result count = %d, want 0", got)
	}

	errorGID := "0000000000000006"
	completeGID := "0000000000000007"
	engine.mu.Lock()
	errorDownload := newDownload(errorGID, []string{"https://example.invalid/file"}, layerOptions(engine.global, nil))
	errorDownload.status = StatusError
	engine.downloads[errorGID] = errorDownload
	engine.appendStoppedLocked(errorGID)
	completeDownload := newDownload(completeGID, []string{"https://example.invalid/file"}, layerOptions(engine.global, nil))
	completeDownload.status = StatusComplete
	engine.downloads[completeGID] = completeDownload
	engine.appendStoppedLocked(completeGID)
	engine.mu.Unlock()
	if _, err := engine.TellStatus(errorGID, nil); err != nil {
		t.Fatalf("unfinished error result was pruned: %v", err)
	}
	if _, err := engine.TellStatus(completeGID, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("completed result survived disabled storage: %v", err)
	}
	if _, err := engine.ChangeGlobalOption(Options{"keep-unfinished-download-result": "false"}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.TellStatus(errorGID, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("disabled unfinished retention kept error result: %v", err)
	}
}

func TestKeepUnfinishedResultsDoNotConsumeCompletedQuota(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir(), MaxDownloadResult: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	errorGID := "0000000000000010"
	oldCompleteGID := "0000000000000011"
	newCompleteGID := "0000000000000012"
	engine.mu.Lock()
	for _, item := range []struct {
		gid    string
		status Status
	}{
		{gid: errorGID, status: StatusError},
		{gid: oldCompleteGID, status: StatusComplete},
	} {
		d := newDownload(item.gid, []string{"https://example.invalid/file"}, layerOptions(engine.global, nil))
		d.status = item.status
		engine.downloads[item.gid] = d
		engine.appendStoppedLocked(item.gid)
	}
	d := newDownload(newCompleteGID, []string{"https://example.invalid/file"}, layerOptions(engine.global, nil))
	d.status = StatusComplete
	engine.downloads[newCompleteGID] = d
	engine.appendStoppedLocked(newCompleteGID)
	engine.mu.Unlock()

	if _, err := engine.TellStatus(errorGID, nil); err != nil {
		t.Fatalf("unfinished result consumed the completed quota: %v", err)
	}
	if _, err := engine.TellStatus(oldCompleteGID, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("oldest completed result lookup error = %v, want ErrNotFound", err)
	}
	if _, err := engine.TellStatus(newCompleteGID, nil); err != nil {
		t.Fatalf("newest completed result was pruned: %v", err)
	}
	if got := engine.GetGlobalOption()["keep-unfinished-download-result"]; got != "true" {
		t.Fatalf("keep-unfinished-download-result default = %v, want true", got)
	}
}

func TestInvalidOptionChangesAreAtomic(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{"http://example.invalid/file"}, Options{
		"out":   "original.bin",
		"pause": "true",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := engine.ChangeOption(gid, Options{
		"out":                "mutated.bin",
		"max-download-limit": "not-a-size",
	}); err == nil {
		t.Fatal("ChangeOption accepted an invalid size")
	}
	opts, err := engine.GetOption(gid)
	if err != nil {
		t.Fatal(err)
	}
	if opts["out"] != "original.bin" {
		t.Fatalf("rejected ChangeOption partially changed out to %v", opts["out"])
	}

	oldDir := engine.GetGlobalOption()["dir"]
	if _, err := engine.ChangeGlobalOption(Options{
		"dir":                 t.TempDir(),
		"max-download-result": "invalid",
	}); err == nil {
		t.Fatal("ChangeGlobalOption accepted an invalid result limit")
	}
	if got := engine.GetGlobalOption()["dir"]; got != oldDir {
		t.Fatalf("rejected ChangeGlobalOption partially changed dir to %v", got)
	}
	for key, value := range map[string]any{
		"gid":                             "1111111111111111",
		"keep-unfinished-download-result": "sometimes",
		"max-concurrent-downloads":        "-1",
		"http-version":                    "http/9",
	} {
		if _, err := engine.ChangeGlobalOption(Options{key: value}); err == nil {
			t.Fatalf("ChangeGlobalOption accepted %s=%v", key, value)
		}
	}
	if _, err := engine.AddURI([]string{"http://example.invalid/invalid"}, Options{"split": "many"}, nil); err == nil {
		t.Fatal("AddURI accepted an invalid integer option")
	}
}

func TestChangeURIReplacesActiveDownloadSource(t *testing.T) {
	oldStarted := make(chan struct{}, 1)
	newStarted := make(chan struct{}, 1)
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host == "old.example" {
			select {
			case oldStarted <- struct{}{}:
			default:
			}
			<-req.Context().Done()
			return nil, req.Context().Err()
		}
		select {
		case newStarted <- struct{}{}:
		default:
		}
		return &http.Response{
			StatusCode:    http.StatusOK,
			Status:        "200 OK",
			Header:        make(http.Header),
			Body:          io.NopCloser(bytes.NewReader([]byte("replacement"))),
			ContentLength: int64(len("replacement")),
			Request:       req,
		}, nil
	})}
	engine, err := NewEngine(Config{Dir: t.TempDir(), HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	const oldURI = "http://old.example/file"
	const newURI = "http://new.example/file"
	gid, err := engine.AddURI([]string{oldURI}, Options{"out": "changed-uri.bin", "split": "1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-oldStarted:
	case <-time.After(time.Second):
		t.Fatal("original URI did not start")
	}
	changed, err := engine.ChangeURI(gid, 1, []string{oldURI}, []string{newURI}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(changed, []int{1, 1}) {
		t.Fatalf("ChangeURI result = %v, want [1 1]", changed)
	}
	select {
	case <-newStarted:
	case <-time.After(time.Second):
		t.Fatal("replacement URI did not start")
	}
	waitForStatus(t, engine, gid, StatusComplete)
	uris, err := engine.GetURIs(gid)
	if err != nil {
		t.Fatal(err)
	}
	if len(uris) != 1 || uris[0].URI != newURI {
		t.Fatalf("changed URIs = %#v, want only replacement", uris)
	}
}

func TestChangeURIRejectsRemovingLastSourceTransactionally(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	const source = "http://example.invalid/file.bin"
	gid, err := engine.AddURI([]string{source}, Options{"pause": "true"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ChangeURI(gid, 1, []string{source}, nil, nil); err == nil {
		t.Fatal("ChangeURI removed the last URI")
	}
	uris, err := engine.GetURIs(gid)
	if err != nil {
		t.Fatal(err)
	}
	if len(uris) != 1 || uris[0].URI != source {
		t.Fatalf("URIs changed after rejected mutation: %#v", uris)
	}
}

func TestEngineCloseContinuesCleanupAfterCallerTimeout(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-release
		return nil, req.Context().Err()
	})}
	engine, err := NewEngine(Config{Dir: t.TempDir(), HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.AddURI([]string{"http://example.invalid/block.bin"}, Options{"max-tries": "1"}, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("download did not start")
	}
	shortCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := engine.Close(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Close error = %v, want deadline exceeded", err)
	}
	close(release)
	longCtx, longCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer longCancel()
	if err := engine.Close(longCtx); err != nil {
		t.Fatalf("second Close did not observe completed cleanup: %v", err)
	}
}

func TestForceShutdownUpgradesQueuedGracefulRequest(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	engine.RequestShutdown(false)
	engine.RequestShutdown(true)
	select {
	case force := <-engine.ShutdownRequests():
		if !force {
			t.Fatal("force shutdown did not replace queued graceful request")
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown request missing")
	}
}

func TestChangeOptionRestartsActiveDownloadWithNewRequestOptions(t *testing.T) {
	firstStarted := make(chan struct{})
	secondHost := make(chan string, 1)
	var calls atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			close(firstStarted)
			<-req.Context().Done()
			return nil, req.Context().Err()
		}
		secondHost <- req.Header.Get("X-Restarted")
		return &http.Response{
			StatusCode:    http.StatusOK,
			Status:        "200 OK",
			Header:        make(http.Header),
			Body:          io.NopCloser(bytes.NewReader([]byte("ok"))),
			ContentLength: 2,
			Request:       req,
		}, nil
	})}
	engine, err := NewEngine(Config{Dir: t.TempDir(), HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{"http://example.test/restart"}, Options{"out": "restart.bin", "split": "1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("initial request did not start")
	}
	if _, err := engine.ChangeOption(gid, Options{"header": []string{"X-Restarted: yes"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-secondHost:
		if got != "yes" {
			t.Fatalf("restarted request header = %q, want yes", got)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement request did not start")
	}
	waitForStatus(t, engine, gid, StatusComplete)
	if got := calls.Load(); got != 2 {
		t.Fatalf("request count = %d, want 2", got)
	}
}

func TestChangeOptionPathChangeDropsLateMetadataFromCanceledRun(t *testing.T) {
	firstStarted := make(chan struct{})
	allowFirstResponse := make(chan struct{})
	firstBodyClosing := make(chan struct{})
	allowFirstClose := make(chan struct{})
	var responseOnce sync.Once
	releaseFirstResponse := func() { responseOnce.Do(func() { close(allowFirstResponse) }) }
	defer releaseFirstResponse()
	var closeOnce sync.Once
	releaseFirstClose := func() { closeOnce.Do(func() { close(allowFirstClose) }) }
	defer releaseFirstClose()
	var calls atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body := io.NopCloser(bytes.NewReader([]byte("replacement")))
		if calls.Add(1) == 1 {
			close(firstStarted)
			<-allowFirstResponse
			body = &blockingCloseBody{
				Reader:  bytes.NewReader([]byte("stale")),
				closing: firstBodyClosing,
				release: allowFirstClose,
			}
		}
		return &http.Response{
			StatusCode:    http.StatusOK,
			Status:        "200 OK",
			Header:        make(http.Header),
			Body:          body,
			ContentLength: int64(len("replacement")),
			Request:       req,
		}, nil
	})}
	root := t.TempDir()
	oldDir := filepath.Join(root, "old")
	newDir := filepath.Join(root, "new")
	engine, err := NewEngine(Config{Dir: root, HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		releaseFirstResponse()
		releaseFirstClose()
		_ = engine.Close(context.Background())
	}()
	gid, err := engine.AddURI([]string{"http://example.test/path-change"}, Options{
		"dir":   oldDir,
		"out":   "old.bin",
		"split": "1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("initial request did not start")
	}
	if _, err := engine.ChangeOption(gid, Options{"dir": newDir, "out": "new.bin"}); err != nil {
		t.Fatal(err)
	}
	releaseFirstResponse()
	select {
	case <-firstBodyClosing:
	case <-time.After(time.Second):
		t.Fatal("canceled response did not publish late metadata")
	}
	engine.mu.RLock()
	d := engine.downloads[gid]
	engine.mu.RUnlock()
	d.mu.RLock()
	latePath := d.path
	d.mu.RUnlock()
	if latePath != filepath.Join(oldDir, "old.bin") {
		t.Fatalf("late canceled-run path = %q, want old destination", latePath)
	}
	releaseFirstClose()
	waitForStatus(t, engine, gid, StatusComplete)
	newPath := filepath.Join(newDir, "new.bin")
	got, err := os.ReadFile(newPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "replacement" {
		t.Fatalf("replacement file = %q, want replacement", got)
	}
	d.mu.RLock()
	finalPath := d.path
	d.mu.RUnlock()
	if finalPath != newPath {
		t.Fatalf("final path = %q, want %q", finalPath, newPath)
	}
}

type blockingCloseBody struct {
	*bytes.Reader
	closing chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (b *blockingCloseBody) Close() error {
	b.once.Do(func() { close(b.closing) })
	<-b.release
	return nil
}

func TestChangeOptionRejectsImmutableOptions(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{"http://example.invalid/file"}, Options{"pause": "true"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"dry-run", "gid", "pause", "piece-length"} {
		if _, err := engine.ChangeOption(gid, Options{key: "true"}); err == nil {
			t.Fatalf("ChangeOption accepted immutable option %q", key)
		}
	}
}

func TestPauseUnpauseWaitsForPreviousRunToExit(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var calls atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		started <- struct{}{}
		<-release
		return &http.Response{
			StatusCode:    http.StatusOK,
			Status:        "200 OK",
			Header:        make(http.Header),
			Body:          io.NopCloser(bytes.NewReader([]byte("x"))),
			ContentLength: 1,
			Request:       req,
		}, nil
	})}
	engine, err := NewEngine(Config{Dir: t.TempDir(), HTTPClient: client, MaxConcurrentDownloads: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{"http://example.test/overlap"}, Options{"out": "overlap.bin", "split": "1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first run did not start")
	}
	if _, err := engine.Pause(gid); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Unpause(gid); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
		t.Fatal("replacement run started before the canceled run exited")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	waitForStatus(t, engine, gid, StatusComplete)
	if got := calls.Load(); got != 2 {
		t.Fatalf("RoundTrip calls = %d, want canceled run plus replacement", got)
	}
}
