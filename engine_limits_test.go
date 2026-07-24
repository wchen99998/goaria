package goaria

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPublicURIListsAreBounded(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	tooMany := make([]string, maxDownloadURIs+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("https://example.invalid/%d", i)
	}
	if _, err := engine.AddURI(tooMany, Options{"pause": "true"}, nil); err == nil {
		t.Fatal("AddURI accepted an unbounded URI list")
	}
	if _, err := engine.AddTorrent("not-a-torrent", tooMany, nil, nil); err == nil {
		t.Fatal("AddTorrent accepted an unbounded webseed list")
	}

	gid, err := engine.AddURI([]string{tooMany[0]}, Options{"pause": "true"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ChangeURI(gid, 1, nil, tooMany[:maxDownloadURIs], nil); err == nil {
		t.Fatal("ChangeURI allowed the resulting URI list to exceed its limit")
	}
	got, err := engine.GetURIs(gid)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].URI != tooMany[0] {
		t.Fatalf("failed ChangeURI mutated the list: %#v", got)
	}
}

func TestSessionRejectsTooManyOrdinaryURIs(t *testing.T) {
	uris := make([]string, maxDownloadURIs+1)
	for i := range uris {
		uris[i] = fmt.Sprintf("https://example.invalid/%d", i)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "session.txt")
	if err := os.WriteFile(path, []byte(strings.Join(uris, " ")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewEngine(Config{Dir: dir, InputFile: path}); err == nil {
		t.Fatal("NewEngine accepted a session entry with too many ordinary URIs")
	}
}

func TestSubscriptionsAreBoundedAndClosedWithEngine(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	events, cancel := engine.Subscribe(int(^uint(0) >> 1))
	if got := cap(events); got != maxNotificationBuffer {
		t.Fatalf("subscription capacity = %d, want %d", got, maxNotificationBuffer)
	}
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-events; ok {
		t.Fatal("subscription remained open after Engine.Close")
	}
	// Cancellation remains safe after shutdown has already closed the channel.
	cancel()

	afterClose, cancelAfterClose := engine.Subscribe(1)
	defer cancelAfterClose()
	if _, ok := <-afterClose; ok {
		t.Fatal("Subscribe returned an open channel after Engine.Close")
	}
}

func TestRepeatedOptionChangesCannotGrowLayersWithoutBound(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddURI([]string{"https://example.invalid/file"}, Options{"pause": "true"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	first := make(Options, maxDownloadOptionKeys)
	for i := 0; i < maxDownloadOptionKeys; i++ {
		first[fmt.Sprintf("x-option-%d", i)] = "value"
	}
	if _, err := engine.ChangeOption(gid, first); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ChangeOption(gid, Options{"overflow-option": "value"}); err == nil {
		t.Fatal("ChangeOption allowed the cumulative overlay to exceed its limit")
	}
	opts, err := engine.GetOption(gid)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := opts["overflow-option"]; exists {
		t.Fatal("failed ChangeOption mutated the option overlay")
	}
}

func TestMaximumOptionLayersRoundTripThroughSession(t *testing.T) {
	dir := t.TempDir()
	sessionPath := filepath.Join(dir, "session.txt")
	engine, err := NewEngine(Config{Dir: dir, SaveSession: sessionPath})
	if err != nil {
		t.Fatal(err)
	}

	engine.mu.RLock()
	globalCount := len(engine.global)
	engine.mu.RUnlock()
	globalAdditions := make(Options, maxDownloadOptionKeys-globalCount)
	for i := 0; i < maxDownloadOptionKeys-globalCount; i++ {
		globalAdditions[fmt.Sprintf("x-global-%d", i)] = "value"
	}
	if _, err := engine.ChangeGlobalOption(globalAdditions); err != nil {
		t.Fatal(err)
	}

	downloadOpts := make(Options, maxDownloadOptionKeys)
	downloadOpts["pause"] = "true"
	for i := 0; i < maxDownloadOptionKeys-1; i++ {
		downloadOpts[fmt.Sprintf("x-download-%d", i)] = "value"
	}
	gid, err := engine.AddURI([]string{"https://example.invalid/file"}, downloadOpts, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ChangeOption(gid, Options{"x-download-last": "value"}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	restored, err := NewEngine(Config{Dir: dir, InputFile: sessionPath})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close(context.Background())
	opts, err := restored.GetOption(gid)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"x-download-last", "x-download-0", "x-global-0"} {
		if opts[key] != "value" {
			t.Fatalf("restored option %q = %v, want value", key, opts[key])
		}
	}
}

func TestEngineCloseDrainsAndDisablesQueuedSessionSaves(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir, SaveSession: filepath.Join(dir, "session.txt")})
	if err != nil {
		t.Fatal(err)
	}
	engine.queueSessionSaveBestEffort()
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	engine.queueSessionSaveBestEffort()
	engine.sessionSaveMu.Lock()
	pending := engine.sessionSavePending
	timer := engine.sessionSaveTimer
	closed := engine.sessionSaveClosed
	engine.sessionSaveMu.Unlock()
	if !closed || pending || timer != nil {
		t.Fatalf("session save state after close: closed=%v pending=%v timer=%v", closed, pending, timer != nil)
	}
}

func TestChangePositionSaturatesAtIntegerExtremes(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	if _, err := engine.ChangeGlobalOption(Options{"max-concurrent-downloads": "0"}); err != nil {
		t.Fatal(err)
	}
	var gids []string
	for i := 0; i < 3; i++ {
		gid, err := engine.AddURI([]string{fmt.Sprintf("https://example.invalid/%d", i)}, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		gids = append(gids, gid)
	}
	maxInt := int(^uint(0) >> 1)
	minInt := -maxInt - 1
	if got, err := engine.ChangePosition(gids[0], maxInt, "POS_CUR"); err != nil || got != 2 {
		t.Fatalf("POS_CUR MaxInt = %d, %v; want 2, nil", got, err)
	}
	if got, err := engine.ChangePosition(gids[0], minInt, "POS_CUR"); err != nil || got != 0 {
		t.Fatalf("POS_CUR MinInt = %d, %v; want 0, nil", got, err)
	}
	if got, err := engine.ChangePosition(gids[0], maxInt, "POS_END"); err != nil || got != 2 {
		t.Fatalf("POS_END MaxInt = %d, %v; want 2, nil", got, err)
	}
	if got, err := engine.ChangePosition(gids[0], -1, "POS_END"); err != nil || got != 2 {
		t.Fatalf("POS_END -1 = %d, %v; want 2, nil", got, err)
	}
	if got, err := engine.ChangePosition(gids[0], minInt, "POS_END"); err != nil || got != 0 {
		t.Fatalf("POS_END MinInt = %d, %v; want 0, nil", got, err)
	}
}
