package goaria

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSessionAutoSaveAndLoadPausedDownload(t *testing.T) {
	dir := t.TempDir()
	sessionPath := filepath.Join(dir, "goaria.session")
	engine, err := NewEngine(Config{Dir: dir, SaveSession: sessionPath})
	if err != nil {
		t.Fatal(err)
	}

	gid, err := engine.AddURI([]string{
		"http://example.invalid/file.bin",
		"http://mirror.example.invalid/file.bin",
	}, Options{
		"pause":  "true",
		"out":    "file.bin",
		"header": []string{"X-One: 1", "X-Two: 2"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	sessionData, err := os.ReadFile(sessionPath)
	if err != nil {
		t.Fatal(err)
	}
	session := string(sessionData)
	if !strings.Contains(session, "http://example.invalid/file.bin\thttp://mirror.example.invalid/file.bin") {
		t.Fatalf("session did not preserve all URIs:\n%s", session)
	}
	if !strings.Contains(session, "gid="+gid) {
		t.Fatalf("session did not preserve gid %s:\n%s", gid, session)
	}
	if !strings.Contains(session, "pause=true") {
		t.Fatalf("session did not preserve paused state:\n%s", session)
	}
	if !strings.Contains(session, "header=X-One: 1\n") || !strings.Contains(session, "header=X-Two: 2\n") {
		t.Fatalf("session did not preserve repeated header options:\n%s", session)
	}

	restored, err := NewEngine(Config{Dir: dir, InputFile: sessionPath})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close(context.Background())

	status, err := restored.TellStatus(gid, []string{"gid", "status"})
	if err != nil {
		t.Fatal(err)
	}
	if status["status"] != string(StatusPaused) {
		t.Fatalf("restored status = %v, want paused", status["status"])
	}
	uris, err := restored.GetURIs(gid)
	if err != nil {
		t.Fatal(err)
	}
	if len(uris) != 2 || uris[0].URI != "http://example.invalid/file.bin" || uris[1].URI != "http://mirror.example.invalid/file.bin" {
		t.Fatalf("restored URIs = %#v", uris)
	}
	opts, err := restored.GetOption(gid)
	if err != nil {
		t.Fatal(err)
	}
	headers, ok := opts["header"].([]string)
	if !ok || len(headers) != 2 || headers[0] != "X-One: 1" || headers[1] != "X-Two: 2" {
		t.Fatalf("restored headers = %#v", opts["header"])
	}
}

func TestSessionPreservesTorrentDownloads(t *testing.T) {
	data, err := os.ReadFile("test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	encoded := base64.StdEncoding.EncodeToString(data)
	mi, _, err := torrentMetaInfo(data)
	if err != nil {
		t.Fatal(err)
	}
	wantInfoHash := mi.HashInfoBytes().HexString()
	dir := t.TempDir()
	sessionPath := filepath.Join(dir, "goaria.session")
	engine, err := NewEngine(Config{Dir: dir, SaveSession: sessionPath})
	if err != nil {
		t.Fatal(err)
	}

	gidWithoutWebseed, err := engine.AddTorrent(encoded, nil, Options{"pause": "true"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	gidWithWebseed, err := engine.AddTorrent(encoded, []string{"http://example.invalid/webseed"}, Options{"pause": "true"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	sessionData, err := os.ReadFile(sessionPath)
	if err != nil {
		t.Fatal(err)
	}
	session := string(sessionData)
	if strings.Count(session, sessionTorrentDataURIPrefix) != 2 {
		t.Fatalf("session did not preserve torrent payloads:\n%s", session)
	}
	if !strings.Contains(session, "http://example.invalid/webseed") {
		t.Fatalf("session did not preserve torrent webseed:\n%s", session)
	}

	restored, err := NewEngine(Config{Dir: dir, InputFile: sessionPath})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close(context.Background())
	for _, gid := range []string{gidWithoutWebseed, gidWithWebseed} {
		status, err := restored.TellStatus(gid, []string{"status", "infoHash", "bittorrent"})
		if err != nil {
			t.Fatal(err)
		}
		if status["status"] != string(StatusPaused) {
			t.Fatalf("restored torrent %s status = %v, want paused", gid, status["status"])
		}
		if status["infoHash"] != wantInfoHash {
			t.Fatalf("restored torrent %s infoHash = %v", gid, status["infoHash"])
		}
		if _, ok := status["bittorrent"].(*BittorrentInfo); !ok {
			t.Fatalf("restored torrent %s lost bittorrent metadata: %#v", gid, status)
		}
	}
	uris, err := restored.GetURIs(gidWithWebseed)
	if err != nil {
		t.Fatal(err)
	}
	if len(uris) != 1 || uris[0].URI != "http://example.invalid/webseed" {
		t.Fatalf("restored torrent webseeds = %#v", uris)
	}
}

func TestSessionSaveUsesCurrentPausedState(t *testing.T) {
	dir := t.TempDir()
	sessionPath := filepath.Join(dir, "goaria.session")
	engine, err := NewEngine(Config{Dir: dir, SaveSession: sessionPath})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	if _, err := engine.ChangeGlobalOption(Options{"max-concurrent-downloads": "0"}); err != nil {
		t.Fatal(err)
	}

	gid, err := engine.AddURI([]string{"http://example.invalid/file.bin"}, Options{"pause": "true"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Unpause(gid); err != nil {
		t.Fatal(err)
	}

	sessionData, err := os.ReadFile(sessionPath)
	if err != nil {
		t.Fatal(err)
	}
	session := string(sessionData)
	if !strings.Contains(session, "pause=false") {
		t.Fatalf("session did not persist current waiting state:\n%s", session)
	}
	if strings.Contains(session, "pause=true") {
		t.Fatalf("session kept stale paused option:\n%s", session)
	}
}

func TestSessionAutoSaveExcludesCompletedDownloads(t *testing.T) {
	data := []byte("session complete")
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "complete.txt", time.Now(), bytes.NewReader(data))
	}))
	defer src.Close()

	dir := t.TempDir()
	sessionPath := filepath.Join(dir, "goaria.session")
	engine, err := NewEngine(Config{Dir: dir, SaveSession: sessionPath})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddURI([]string{src.URL + "/complete.txt"}, Options{"out": "complete.txt"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)

	deadline := time.Now().Add(time.Second)
	for {
		sessionData, err := os.ReadFile(sessionPath)
		if err != nil {
			t.Fatal(err)
		}
		if strings.TrimSpace(string(sessionData)) == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("completed download remained in session:\n%s", string(sessionData))
		}
		time.Sleep(10 * time.Millisecond)
	}
}
