package goaria

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wchen99998/torrent"
	"github.com/wchen99998/torrent/bencode"
	"github.com/wchen99998/torrent/metainfo"
	"github.com/wchen99998/torrent/storage"
)

func TestTorrentSpeedReportsUnverifiedPayloadAndReturnsToZeroWhenIdle(t *testing.T) {
	const partialLength = 64 << 10
	payload := bytes.Repeat([]byte("payload-"), 1<<17)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, end := 0, len(payload)-1
		if r.Header.Get("Range") != "" {
			if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || start < 0 || end >= len(payload) || end < start {
				http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
				return
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
		}
		w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
		if r.Header.Get("Range") != "" {
			w.WriteHeader(http.StatusPartialContent)
		}
		if start < partialLength {
			until := min(end+1, partialLength)
			_, _ = w.Write(payload[start:until])
			w.(http.Flusher).Flush()
			start = until
		}
		select {
		case <-r.Context().Done():
			return
		case <-release:
			_, _ = w.Write(payload[start : end+1])
		}
	}))
	defer func() {
		unblock()
		source.Close()
	}()

	path := filepath.Join(t.TempDir(), "payload.bin")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	info := metainfo.Info{PieceLength: int64(len(payload))}
	if err := info.BuildFromFilePath(path); err != nil {
		t.Fatal(err)
	}
	infoBytes, err := bencode.Marshal(&info)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeMetaInfo(metainfo.MetaInfo{InfoBytes: infoBytes, UrlList: []string{source.URL}})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddTorrent(encoded, nil, Options{
		"goaria-disable-dht":      "true",
		"goaria-disable-trackers": "true",
		"goaria-disable-utp":      "true",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	waitForTorrentSpeed(t, engine, gid, func(speed int64) bool { return speed > 0 })
	status, err := engine.TellStatus(gid, []string{"completedLength"})
	if err != nil {
		t.Fatal(err)
	}
	if status["completedLength"] != "0" {
		t.Fatalf("partially received piece must remain unverified: %v", status)
	}
	active := engine.TellActive([]string{"downloadSpeed"})
	if len(active) != 1 || active[0]["downloadSpeed"] == "0" {
		t.Fatalf("active download speed does not include received payload: %v", active)
	}
	if stat := engine.GetGlobalStat(); stat.DownloadSpeed == "0" {
		t.Fatalf("global download speed does not include received payload: %+v", stat)
	}
	waitForTorrentSpeed(t, engine, gid, func(speed int64) bool { return speed == 0 })
	unblock()
	waitForStatus(t, engine, gid, StatusComplete)
	assertFileEquals(t, filepath.Join(engine.cfg.Dir, "payload.bin"), payload)
}

type gatedVerifiedPiece struct {
	payload  []byte
	release  <-chan struct{}
	complete atomic.Bool
}

func (p *gatedVerifiedPiece) OpenTorrent(context.Context, *metainfo.Info, metainfo.Hash) (storage.TorrentImpl, error) {
	return storage.TorrentImpl{Piece: func(metainfo.Piece) storage.PieceImpl { return p }}, nil
}

func (p *gatedVerifiedPiece) ReadAt(dst []byte, off int64) (int, error) {
	<-p.release
	return bytes.NewReader(p.payload).ReadAt(dst, off)
}

func (*gatedVerifiedPiece) WriteAt([]byte, int64) (int, error) { return 0, io.ErrClosedPipe }
func (p *gatedVerifiedPiece) MarkComplete() error              { p.complete.Store(true); return nil }
func (p *gatedVerifiedPiece) MarkNotComplete() error           { p.complete.Store(false); return nil }
func (p *gatedVerifiedPiece) Completion() storage.Completion {
	return storage.Completion{Ok: true, Complete: p.complete.Load()}
}

func TestTorrentSpeedDoesNotCountLocalVerification(t *testing.T) {
	payload := bytes.Repeat([]byte("on-disk-data-"), 4096)
	path := filepath.Join(t.TempDir(), "payload.bin")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	info := metainfo.Info{PieceLength: 1 << 20}
	if err := info.BuildFromFilePath(path); err != nil {
		t.Fatal(err)
	}
	infoBytes, err := bencode.Marshal(&info)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = t.TempDir()
	cfg.DefaultStorage = &gatedVerifiedPiece{payload: payload, release: release}
	cfg.ListenHost = torrent.LoopbackListenHost
	cfg.ListenPort = 0
	cfg.NoDHT = true
	cfg.DisableTrackers = true
	cfg.DisableUTP = true
	cfg.NoDefaultPortForwarding = true
	cfg.Slogger = slog.New(slog.NewTextHandler(io.Discard, nil))
	client, err := torrent.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { unblock(); client.Close() }()
	tor, err := client.AddTorrent(&metainfo.MetaInfo{InfoBytes: infoBytes})
	if err != nil {
		t.Fatal(err)
	}
	d := &Download{
		status:       StatusActive,
		completedLen: -1,
		torrentFiles: []torrentFileState{{
			Index: 1, Length: int64(len(payload)), Selected: true,
		}},
	}
	engine := &Engine{}
	stop := engine.startTorrentProgress(t.Context(), d, tor)
	defer stop()
	deadline := time.Now().Add(5 * time.Second)
	for d.snapshot([]string{"completedLength"})["completedLength"] != "0" {
		if time.Now().After(deadline) {
			t.Fatal("torrent sampler did not establish its initial baseline")
		}
		time.Sleep(time.Millisecond)
	}
	unblock()
	if err := tor.VerifyDataContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status := d.snapshot([]string{"completedLength", "downloadSpeed", "uploadSpeed"})
		if status["completedLength"] == strconv.Itoa(len(payload)) {
			if status["downloadSpeed"] != "0" || status["uploadSpeed"] != "0" {
				t.Fatalf("local verification generated network speed: %v", status)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("local verification did not update completed length")
}

func waitForTorrentSpeed(t *testing.T, engine *Engine, gid string, accept func(int64) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, err := engine.TellStatus(gid, []string{"downloadSpeed", "status", "errorMessage"})
		if err != nil {
			t.Fatal(err)
		}
		if status["status"] == string(StatusError) {
			t.Fatalf("torrent failed: %v", status)
		}
		speed, err := strconv.ParseInt(status["downloadSpeed"].(string), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		if accept(speed) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("torrent speed did not reach expected state")
}
