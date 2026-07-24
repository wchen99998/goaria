package goaria

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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

func TestLoadSessionRejectsInvalidOptionValues(t *testing.T) {
	for name, option := range map[string]string{
		"boolean":    "pause=maybe",
		"integer":    "max-tries=-1",
		"option key": "bad key=value",
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "invalid.session")
			contents := "http://example.invalid/file\n  " + option + "\n"
			if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
				t.Fatal(err)
			}
			engine, err := NewEngine(Config{Dir: dir, InputFile: path})
			if engine != nil {
				_ = engine.Close(context.Background())
			}
			if err == nil || !strings.Contains(err.Error(), "invalid") {
				t.Fatalf("NewEngine error = %v, want invalid session option", err)
			}
		})
	}
}

func TestSessionRoundTripsTorrentAboveDefaultMaxSize(t *testing.T) {
	data, err := os.ReadFile("test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	mi, _, err := torrentMetaInfo(data)
	if err != nil {
		t.Fatal(err)
	}
	mi.Comment = strings.Repeat("x", defaultMaxTorrentSize+(2<<20))
	encoded, err := encodeMetaInfo(*mi)
	if err != nil {
		t.Fatal(err)
	}
	largeData, err := decodeTorrentParam(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(largeData) <= defaultMaxTorrentSize {
		t.Fatalf("large torrent size = %d, want more than %d", len(largeData), defaultMaxTorrentSize)
	}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(largeData)
	}))
	defer source.Close()

	dir := t.TempDir()
	sessionPath := filepath.Join(dir, "large.session")
	engine, err := NewEngine(Config{Dir: dir, SaveSession: sessionPath})
	if err != nil {
		t.Fatal(err)
	}
	gid, err := engine.AddTorrent(source.URL, nil, Options{
		"pause":                   "true",
		"goaria-max-torrent-size": "32M",
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
	legacyMaxLineSize := len(sessionTorrentDataURIPrefix) + ((defaultMaxTorrentSize+2)/3)*4 + 1<<20
	firstLineEnd := bytes.IndexByte(sessionData, '\n')
	if firstLineEnd <= legacyMaxLineSize {
		t.Fatalf("large torrent session line = %d bytes, want more than legacy limit %d", firstLineEnd, legacyMaxLineSize)
	}

	restored, err := NewEngine(Config{Dir: dir, InputFile: sessionPath})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close(context.Background())
	status, err := restored.TellStatus(gid, []string{"status"})
	if err != nil {
		t.Fatal(err)
	}
	if status["status"] != string(StatusPaused) {
		t.Fatalf("restored status = %v, want paused", status["status"])
	}
	opts, err := restored.GetOption(gid)
	if err != nil {
		t.Fatal(err)
	}
	if opts["goaria-max-torrent-size"] != "32M" {
		t.Fatalf("restored max torrent size = %v, want 32M", opts["goaria-max-torrent-size"])
	}
}

func TestSessionPreservesErroredDownloadForRetry(t *testing.T) {
	dir := t.TempDir()
	sessionPath := filepath.Join(dir, "retry.session")
	failingClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("temporary transport failure")
	})}
	engine, err := NewEngine(Config{Dir: dir, SaveSession: sessionPath, HTTPClient: failingClient})
	if err != nil {
		t.Fatal(err)
	}
	gid, err := engine.AddURI([]string{"http://example.invalid/retry.bin"}, Options{"out": "retry.bin"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusError)
	if _, err := engine.SaveSession(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	sessionData, err := os.ReadFile(sessionPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(sessionData), "gid="+gid) {
		t.Fatalf("errored download was omitted from session:\n%s", sessionData)
	}

	started := make(chan struct{}, 1)
	blockingClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	restored, err := NewEngine(Config{Dir: dir, InputFile: sessionPath, HTTPClient: blockingClient})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close(context.Background())
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("restored errored download did not retry")
	}
	status, err := restored.TellStatus(gid, []string{"status"})
	if err != nil {
		t.Fatal(err)
	}
	if status["status"] != string(StatusActive) {
		t.Fatalf("restored status = %v, want active retry", status["status"])
	}
}

func TestEngineShutdownPersistsActiveDownloadAsResumable(t *testing.T) {
	dir := t.TempDir()
	sessionPath := filepath.Join(dir, "active.session")
	started := make(chan struct{}, 1)
	blockingClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	engine, err := NewEngine(Config{Dir: dir, SaveSession: sessionPath, HTTPClient: blockingClient})
	if err != nil {
		t.Fatal(err)
	}
	gid, err := engine.AddURI([]string{"http://example.invalid/resume.bin"}, Options{
		"out":       "resume.bin",
		"max-tries": "1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("download did not become active")
	}
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	restarted := make(chan struct{}, 1)
	restoreClient := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		select {
		case restarted <- struct{}{}:
		default:
		}
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	restored, err := NewEngine(Config{Dir: dir, InputFile: sessionPath, HTTPClient: restoreClient})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close(context.Background())
	select {
	case <-restarted:
	case <-time.After(time.Second):
		status, _ := restored.TellStatus(gid, []string{"status"})
		t.Fatalf("restored active download did not resume: %#v", status)
	}
}

func TestWriteSessionFileRejectsUnrepresentableDataAtomically(t *testing.T) {
	tests := []struct {
		name    string
		entry   sessionDownload
		wantErr string
	}{
		{
			name:    "URI whitespace",
			entry:   sessionDownload{gid: "uri", uris: []string{"http://example.invalid/a\tb"}},
			wantErr: "URI 1 contains whitespace",
		},
		{
			name:    "comment URI",
			entry:   sessionDownload{gid: "comment", uris: []string{"#not-a-uri"}},
			wantErr: "first URI is parsed as a comment",
		},
		{
			name:    "option key separator",
			entry:   sessionDownload{gid: "key", uris: []string{"http://example.invalid/file"}, options: map[string]any{"bad=key": "value"}},
			wantErr: "option key",
		},
		{
			name:    "option value newline",
			entry:   sessionDownload{gid: "newline", uris: []string{"http://example.invalid/file"}, options: map[string]any{"header": "safe\npause=true"}},
			wantErr: "value contains a line break",
		},
		{
			name:    "option value edge whitespace",
			entry:   sessionDownload{gid: "space", uris: []string{"http://example.invalid/file"}, options: map[string]any{"out": " leading"}},
			wantErr: "value has leading or trailing whitespace",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "session.txt")
			const original = "existing session\n"
			if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
				t.Fatal(err)
			}
			err := writeSessionFile(path, []sessionDownload{tt.entry})
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("writeSessionFile error = %v, want containing %q", err, tt.wantErr)
			}
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != original {
				t.Fatalf("failed serialization replaced session with %q", got)
			}
		})
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

func TestSessionPreservesInheritedOptionProvenance(t *testing.T) {
	dir := t.TempDir()
	sessionPath := filepath.Join(dir, "provenance.session")
	engine, err := NewEngine(Config{Dir: dir, SaveSession: sessionPath})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ChangeGlobalOption(Options{
		"split":              "2",
		"goaria-disable-dht": "false",
	}); err != nil {
		t.Fatal(err)
	}
	gid, err := engine.AddURI([]string{"http://example.invalid/file.bin"}, Options{
		"pause": "true",
		"out":   "file.bin",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	restored, err := NewEngine(Config{Dir: t.TempDir(), InputFile: sessionPath})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close(context.Background())
	restored.mu.RLock()
	d := restored.downloads[gid]
	restored.mu.RUnlock()
	if d == nil {
		t.Fatalf("restored download %s missing", gid)
	}
	d.mu.RLock()
	if optionExplicit(d.options, "split") || optionExplicit(d.options, "goaria-disable-dht") {
		d.mu.RUnlock()
		t.Fatal("inherited options became explicit after session restore")
	}
	if !optionExplicit(d.options, "out") {
		d.mu.RUnlock()
		t.Fatal("explicit output option lost provenance after session restore")
	}
	if got := optionString(d.options, "split"); got != "2" {
		d.mu.RUnlock()
		t.Fatalf("restored inherited split = %q, want frozen value 2", got)
	}
	d.mu.RUnlock()

	if _, err := restored.ChangeGlobalOption(Options{"split": "4"}); err != nil {
		t.Fatal(err)
	}
	if got := optionInt(restored.dynamicSegmentedOptionSnapshot(d), "split", 0); got != 4 {
		t.Fatalf("dynamic inherited split = %d, want updated global value 4", got)
	}
	gotOptions, err := restored.GetOption(gid)
	if err != nil {
		t.Fatal(err)
	}
	if _, leaked := gotOptions[sessionExplicitOptionsOption]; leaked {
		t.Fatalf("private provenance option leaked through GetOption: %#v", gotOptions)
	}
}

func TestSessionParserBoundsRepeatedOptionValues(t *testing.T) {
	var input strings.Builder
	input.WriteString("http://example.invalid/file\n")
	for i := 0; i <= maxSessionOptionValues; i++ {
		input.WriteString("  header=X-Test: value\n")
	}
	if _, err := parseSession(strings.NewReader(input.String())); err == nil || !strings.Contains(err.Error(), "too many values") {
		t.Fatalf("parseSession error = %v, want repeated-option bound", err)
	}
}

func TestSessionParserBoundsPrivateAndOrdinaryOptionValues(t *testing.T) {
	tooLarge := strings.Repeat("x", maxDownloadOptionValueSize+1)
	input := "http://example.invalid/file\n  out=" + tooLarge + "\n"
	if _, err := parseSession(strings.NewReader(input)); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("parseSession oversized option error = %v", err)
	}

	keys := make([]string, maxDownloadOptionKeys+1)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	encoded, err := json.Marshal(keys)
	if err != nil {
		t.Fatal(err)
	}
	opts := Options{sessionExplicitOptionsOption: base64.RawURLEncoding.EncodeToString(encoded)}
	if _, _, err := parseSessionExplicitOptions(opts); err == nil || !strings.Contains(err.Error(), "too many") {
		t.Fatalf("parseSessionExplicitOptions error = %v, want key-count bound", err)
	}
}

func TestDecodeSessionTorrentDataURIHonorsEntryLimit(t *testing.T) {
	raw := encodeSessionTorrentDataURI([]byte{1, 2})
	if _, ok, err := decodeSessionTorrentDataURI(raw, 1); !ok || err == nil {
		t.Fatalf("decodeSessionTorrentDataURI = ok:%v err:%v, want size error", ok, err)
	}
}

func TestRemoveDownloadResultUpdatesSavedSession(t *testing.T) {
	dir := t.TempDir()
	sessionPath := filepath.Join(dir, "remove-result.session")
	failingClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("permanent test failure")
	})}
	engine, err := NewEngine(Config{Dir: dir, SaveSession: sessionPath, HTTPClient: failingClient})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{"http://example.invalid/fail.bin"}, Options{
		"max-tries": "1",
		"out":       "fail.bin",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusError)
	if _, err := engine.RemoveDownloadResult(gid); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(sessionPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), gid) {
		t.Fatalf("removed result remained in saved session:\n%s", data)
	}
}

func TestMagnetWebseedsRoundTripThroughSession(t *testing.T) {
	data, err := os.ReadFile("test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	mi, _, err := torrentMetaInfo(data)
	if err != nil {
		t.Fatal(err)
	}
	magnet := fmt.Sprintf("magnet:?xt=urn:btih:%s", mi.HashInfoBytes().HexString())
	dir := t.TempDir()
	sessionPath := filepath.Join(dir, "magnet.session")
	engine, err := NewEngine(Config{Dir: dir, SaveSession: sessionPath})
	if err != nil {
		t.Fatal(err)
	}
	gid, err := engine.AddURI([]string{magnet}, Options{"pause": "true"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	webseed := "https://webseed.example.invalid/content"
	if _, err := engine.ChangeURI(gid, 1, nil, []string{webseed}, nil); err != nil {
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
	uris, err := restored.GetURIs(gid)
	if err != nil {
		t.Fatal(err)
	}
	if len(uris) != 1 || uris[0].URI != webseed {
		t.Fatalf("restored magnet webseeds = %#v, want %q", uris, webseed)
	}
}
