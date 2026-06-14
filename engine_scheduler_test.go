package goaria

import (
	"context"
	"testing"
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
