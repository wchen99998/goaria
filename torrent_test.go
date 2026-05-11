package goaria

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wchen99998/torrent"
	"github.com/wchen99998/torrent/bencode"
	"github.com/wchen99998/torrent/metainfo"
	"github.com/wchen99998/torrent/storage"
)

func TestAddTorrentParsesRealTestTorrent(t *testing.T) {
	data, err := os.ReadFile("test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddTorrent(base64.StdEncoding.EncodeToString(data), nil, Options{"pause": "true"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	status, err := engine.TellStatus(gid, []string{"status", "infoHash", "bittorrent", "files", "totalLength", "pieceLength", "numPieces"})
	if err != nil {
		t.Fatal(err)
	}
	if status["status"] != string(StatusPaused) {
		t.Fatalf("status = %v, want paused", status["status"])
	}
	if got := status["infoHash"].(string); len(got) != 40 {
		t.Fatalf("infoHash length = %d, want 40", len(got))
	}
	files := status["files"].([]FileInfo)
	if len(files) == 0 {
		t.Fatal("test.torrent produced no files")
	}
	bt := status["bittorrent"].(*BittorrentInfo)
	if bt.Mode != "single" && bt.Mode != "multi" {
		t.Fatalf("unexpected bittorrent mode %q", bt.Mode)
	}
	if status["totalLength"] == "0" || status["pieceLength"] == "0" || status["numPieces"] == "0" {
		t.Fatalf("incomplete torrent metadata: %#v", status)
	}
}

func TestAddURIAcceptsBitTorrentMagnet(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddURI([]string{"magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=fixture"}, Options{"pause": "true"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	status, err := engine.TellStatus(gid, []string{"status", "infoHash", "bittorrent"})
	if err != nil {
		t.Fatal(err)
	}
	if status["status"] != string(StatusPaused) {
		t.Fatalf("status = %v, want paused", status["status"])
	}
	if status["infoHash"] != "0123456789abcdef0123456789abcdef01234567" {
		t.Fatalf("infoHash = %v", status["infoHash"])
	}
}

func TestTorrentStreamHandlerWithTestTorrent(t *testing.T) {
	data, err := os.ReadFile("test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	mi, info, err := torrentMetaInfo(data)
	if err != nil {
		t.Fatal(err)
	}
	files := info.UpvertedFiles()
	if len(files) == 0 {
		t.Fatal("test.torrent has no files")
	}
	payloadDir := t.TempDir()
	payloads := writeTestTorrentPayload(t, payloadDir, info)
	selectedIndex := 1
	selectedFile := files[0]
	for i, f := range files[1:] {
		if f.Length < selectedFile.Length {
			selectedIndex = i + 2
			selectedFile = f
		}
	}
	expected, ok := payloads[selectedIndex]
	if !ok {
		t.Fatalf("test fixture did not generate payload for selected file %d", selectedIndex)
	}

	peerAddrs, stopSeeder := startSeederForMetaInfo(t, payloadDir, mi)
	defer stopSeeder()

	streamed := make(chan []byte, 1)
	engine, err := NewEngine(Config{
		Dir: t.TempDir(),
		TorrentFileHandler: func(ctx context.Context, tf TorrentFileLease) error {
			got, err := io.ReadAll(tf.Reader)
			if err != nil {
				return err
			}
			streamed <- got
			return tf.Release(ctx)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddTorrent(base64.StdEncoding.EncodeToString(data), nil, Options{
		"select-file":             strconv.Itoa(selectedIndex),
		"goaria-disable-dht":      "true",
		"goaria-disable-trackers": "true",
		"goaria-disable-utp":      "true",
		"goaria-peer-addrs":       peerAddrs,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	select {
	case got := <-streamed:
		if !bytes.Equal(got, expected) {
			t.Fatalf("streamed payload mismatch for file %d: got %d bytes, want %d", selectedIndex, len(got), len(expected))
		}
	default:
		t.Fatal("stream handler did not receive selected file")
	}
}

func TestTorrentDownloadWithoutStreamHandlerUsesLocalSeeder(t *testing.T) {
	data, err := os.ReadFile("test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	mi, info, err := torrentMetaInfo(data)
	if err != nil {
		t.Fatal(err)
	}
	payloadDir := t.TempDir()
	payloads := writeTestTorrentPayload(t, payloadDir, info)
	peerAddrs, stopSeeder := startSeederForMetaInfo(t, payloadDir, mi)
	defer stopSeeder()

	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddTorrent(base64.StdEncoding.EncodeToString(data), nil, Options{
		"select-file":             "2",
		"goaria-disable-dht":      "true",
		"goaria-disable-trackers": "true",
		"goaria-disable-utp":      "true",
		"goaria-peer-addrs":       peerAddrs,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)

	files, err := engine.GetFiles(gid)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("GetFiles returned %d files, want 2", len(files))
	}
	if files[1].CompletedLength != files[1].Length || files[1].Selected != "true" {
		t.Fatalf("selected file did not complete: %#v", files[1])
	}
	got, err := os.ReadFile(files[1].Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payloads[2]) {
		t.Fatalf("downloaded payload mismatch: got %q want %q", got, payloads[2])
	}
}

func TestTorrentCompletesWhenNoFilesAreSelected(t *testing.T) {
	data, err := os.ReadFile("test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddTorrent(base64.StdEncoding.EncodeToString(data), nil, Options{
		"select-file":             "not-a-number",
		"goaria-disable-dht":      "true",
		"goaria-disable-trackers": "true",
		"goaria-disable-utp":      "true",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	status, err := engine.TellStatus(gid, []string{"totalLength", "completedLength"})
	if err != nil {
		t.Fatal(err)
	}
	if status["totalLength"] != "0" || status["completedLength"] != "0" {
		t.Fatalf("empty selection status = %#v", status)
	}
}

func TestTorrentOptionParsingAndHelpers(t *testing.T) {
	data, err := os.ReadFile("test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	mi, info, err := torrentMetaInfo(data)
	if err != nil {
		t.Fatal(err)
	}
	encoded := base64.StdEncoding.EncodeToString(data)
	rawEncoded := strings.TrimRight(encoded, "=")
	for _, input := range []string{encoded, rawEncoded} {
		got, err := decodeTorrentParam(input)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, data) {
			t.Fatal("decoded torrent did not round-trip")
		}
	}
	if _, err := decodeTorrentParam("not base64!"); err == nil {
		t.Fatal("decodeTorrentParam accepted invalid base64")
	}
	if _, _, err := torrentMetaInfo([]byte("d4:info3:bade")); err == nil {
		t.Fatal("torrentMetaInfo accepted invalid info payload")
	}
	reencoded, err := encodeMetaInfo(*mi)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip, err := decodeTorrentParam(reencoded)
	if err != nil {
		t.Fatal(err)
	}
	roundTripMI, _, err := torrentMetaInfo(roundTrip)
	if err != nil {
		t.Fatal(err)
	}
	if roundTripMI.HashInfoBytes() != mi.HashInfoBytes() {
		t.Fatalf("encoded metainfo hash = %s, want %s", roundTripMI.HashInfoBytes(), mi.HashInfoBytes())
	}

	for input, want := range map[string]string{
		"":                        "torrent",
		"../escape/./file.txt":    filepath.Join("escape", "file.txt"),
		"root//nested/../../file": filepath.Join("root", "nested", "file"),
		"/absolute/path/file.bin": filepath.Join("absolute", "path", "file.bin"),
	} {
		if got := safeTorrentRelPath(input); got != want {
			t.Fatalf("safeTorrentRelPath(%q) = %q, want %q", input, got, want)
		}
	}

	selected := selectedFileSet(5, "2, 4-5, bad, 7-6,")
	for _, index := range []int{2, 4, 5} {
		if !selected[index] {
			t.Fatalf("selectedFileSet missing index %d: %#v", index, selected)
		}
	}
	if selected[1] || selected[3] || selected[6] {
		t.Fatalf("selectedFileSet selected unexpected indexes: %#v", selected)
	}
	if all := selectedFileSet(1, "999"); !all[1] || len(all) != 1 {
		t.Fatalf("single-file selection should select the only file: %#v", all)
	}

	indexOut, err := parseIndexOut(Options{"index-out": []string{"2=renamed.txt", "3= nested/ok.txt "}})
	if err != nil {
		t.Fatal(err)
	}
	if indexOut[2] != "renamed.txt" || indexOut[3] != "nested/ok.txt" {
		t.Fatalf("parseIndexOut = %#v", indexOut)
	}
	for _, opts := range []Options{
		{"index-out": "missing-separator"},
		{"index-out": "0=file.txt"},
		{"index-out": "x=file.txt"},
		{"index-out": "1= "},
	} {
		if _, err := parseIndexOut(opts); err == nil {
			t.Fatalf("parseIndexOut(%#v) succeeded, want error", opts)
		}
	}
	if _, _, err := torrentSelectionAndIndexOut(info, Options{"select-file": "1-2", "index-out": "2=renamed.txt"}); err != nil {
		t.Fatal(err)
	}
	for _, opts := range []Options{
		{"select-file": "999"},
		{"index-out": "999=missing.txt"},
	} {
		if _, _, err := torrentSelectionAndIndexOut(info, opts); err == nil {
			t.Fatalf("torrentSelectionAndIndexOut(%#v) succeeded, want error", opts)
		}
	}

	spec := &torrent.TorrentSpec{Trackers: [][]string{{"udp://one", "udp://two"}, {"http://keep"}}}
	applyTorrentTrackerOptions(spec, Options{
		"bt-exclude-tracker": "udp://one",
		"bt-tracker":         "udp://new, http://new2",
	})
	if len(spec.Trackers) != 3 || len(spec.Trackers[0]) != 1 || spec.Trackers[0][0] != "udp://two" || spec.Trackers[2][0] != "udp://new" || spec.Trackers[2][1] != "http://new2" {
		t.Fatalf("tracker options produced %#v", spec.Trackers)
	}
	spec = &torrent.TorrentSpec{Trackers: [][]string{{"udp://one"}, {"http://keep"}}}
	applyTorrentTrackerOptions(spec, Options{"bt-exclude-tracker": "*"})
	if len(spec.Trackers) != 0 {
		t.Fatalf("exclude all trackers produced %#v", spec.Trackers)
	}

	if maker := torrentFilePathMaker(nil); maker != nil {
		t.Fatal("nil index-out should not install a custom path maker")
	}
	maker := torrentFilePathMaker(Options{"index-out": []string{"1=../custom-alpha.txt"}})
	if maker == nil {
		t.Fatal("index-out should install a custom path maker")
	}
	upverted := info.UpvertedFiles()
	gotPath := maker(storage.FilePathMakerOpts{Info: &info, File: &upverted[0]})
	if gotPath != "custom-alpha.txt" {
		t.Fatalf("custom path = %q, want custom-alpha.txt", gotPath)
	}
	gotPath = maker(storage.FilePathMakerOpts{Info: &info, File: &upverted[1]})
	if gotPath != filepath.Join(info.BestName(), "nested", "beta.txt") {
		t.Fatalf("default path = %q", gotPath)
	}
	states := torrentFileStates(info, Options{"index-out": "2=custom-beta.txt"}, t.TempDir(), nil)
	if filepath.Base(states[1].Path) != "custom-beta.txt" {
		t.Fatalf("index-out torrent state path = %q", states[1].Path)
	}

	if got := peerBitfield(0, 8); got != "" {
		t.Fatalf("empty peer bitfield = %q", got)
	}
	if got := peerBitfield(3, 10); got != "e000" {
		t.Fatalf("partial peer bitfield = %q, want e000", got)
	}
	if got := peerBitfield(99, 10); got != "ffc0" {
		t.Fatalf("capped peer bitfield = %q, want ffc0", got)
	}
	if _, err := newMagnetDownload("gid", "magnet:?xt=urn:btih:not-a-hash", nil); err == nil {
		t.Fatal("newMagnetDownload accepted invalid magnet")
	}
	if d := newTorrentDownload("gid", nil, "", nil, Options{}, nil, metainfo.Info{}); d.dir != "." {
		t.Fatalf("empty torrent options dir = %q, want .", d.dir)
	}
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	if err := engine.runTorrentDownload(&Download{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("runTorrentDownload with nil context = %v, want context.Canceled", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	canceledMagnet := newTorrentDownload("gid", nil, magnetFixtureURI(), nil, Options{
		"goaria-disable-dht":      "true",
		"goaria-disable-trackers": "true",
		"goaria-disable-utp":      "true",
	}, nil, metainfo.Info{Name: "fixture"})
	canceledMagnet.ctx = ctx
	if err := engine.runTorrentDownload(canceledMagnet); !errors.Is(err, context.Canceled) {
		t.Fatalf("runTorrentDownload with canceled magnet = %v, want context.Canceled", err)
	}
	invalidMagnet := newTorrentDownload("gid", nil, "magnet:?xt=urn:btih:not-a-hash", nil, Options{}, nil, metainfo.Info{})
	invalidMagnet.ctx = context.Background()
	if err := engine.runTorrentDownload(invalidMagnet); err == nil {
		t.Fatal("runTorrentDownload accepted invalid magnet")
	}
	invalidTorrent := newTorrentDownload("gid", []byte("not a torrent"), "", nil, Options{}, nil, metainfo.Info{})
	invalidTorrent.ctx = context.Background()
	if err := engine.runTorrentDownload(invalidTorrent); err == nil {
		t.Fatal("runTorrentDownload accepted invalid torrent data")
	}
	badListenHost := newTorrentDownload("gid", data, "", nil, Options{"goaria-listen-host": "bad host"}, mi, info)
	badListenHost.ctx = context.Background()
	if err := engine.runTorrentDownload(badListenHost); err == nil {
		t.Fatal("runTorrentDownload accepted invalid listen host")
	}
	if err := engine.processCompletedTorrentFiles(context.Background(), &Download{}, nil); err != nil {
		t.Fatalf("processCompletedTorrentFiles with no files = %v", err)
	}
	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = t.TempDir()
	cfg.NoDHT = true
	cfg.DisableTrackers = true
	cfg.DisableUTP = true
	cfg.NoDefaultPortForwarding = true
	cfg.Slogger = slog.New(slog.NewTextHandler(io.Discard, nil))
	cl, err := torrent.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	tor, err := cl.AddTorrent(mi)
	if err != nil {
		t.Fatal(err)
	}
	<-tor.GotInfo()
	progressDownload := newTorrentDownload("gid", data, "", nil, Options{"dir": cfg.DataDir}, mi, info)
	progressDownload.torrentFiles = append(progressDownload.torrentFiles, torrentFileState{Length: 1, Selected: true})
	engine.updateTorrentProgress(progressDownload, tor)
}

func TestAddTorrentAndMagnetRejectInvalidInputs(t *testing.T) {
	data, err := os.ReadFile("test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	encoded := base64.StdEncoding.EncodeToString(data)
	magnet := magnetFixtureURI()

	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	for name, fn := range map[string]func() error{
		"bad base64": func() error {
			_, err := engine.AddTorrent("not base64!", nil, Options{"pause": "true"}, nil)
			return err
		},
		"bad metainfo": func() error {
			_, err := engine.AddTorrent(base64.StdEncoding.EncodeToString([]byte("not a torrent")), nil, Options{"pause": "true"}, nil)
			return err
		},
		"bad webseed": func() error {
			_, err := engine.AddTorrent(encoded, []string{"ftp://example.invalid/file"}, Options{"pause": "true"}, nil)
			return err
		},
		"bad gid": func() error {
			_, err := engine.AddTorrent(encoded, nil, Options{"pause": "true", "gid": "short"}, nil)
			return err
		},
		"bad select-file": func() error {
			_, err := engine.AddTorrent(encoded, nil, Options{"pause": "true", "select-file": "999"}, nil)
			return err
		},
		"bad index-out": func() error {
			_, err := engine.AddTorrent(encoded, nil, Options{"pause": "true", "index-out": "999=missing.txt"}, nil)
			return err
		},
		"bad magnet": func() error {
			_, err := engine.AddURI([]string{"magnet:?xt=urn:btih:not-a-hash"}, Options{"pause": "true"}, nil)
			return err
		},
		"mixed magnet": func() error {
			_, err := engine.AddURI([]string{magnet, "http://example.invalid/file"}, Options{"pause": "true"}, nil)
			return err
		},
		"bad magnet gid": func() error {
			_, err := engine.AddURI([]string{magnet}, Options{"pause": "true", "gid": "short"}, nil)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := fn(); err == nil {
				t.Fatal("expected error")
			}
		})
	}

	if _, err := engine.AddTorrent(encoded, nil, Options{"pause": "true", "gid": "0123456789abcdef"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.AddTorrent(encoded, nil, Options{"pause": "true", "gid": "0123456789abcdef"}, nil); err == nil {
		t.Fatal("duplicate torrent gid succeeded")
	}
	if _, err := engine.AddURI([]string{magnet}, Options{"pause": "true", "gid": "1111111111111111"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.AddURI([]string{magnet}, Options{"pause": "true", "gid": "1111111111111111"}, nil); err == nil {
		t.Fatal("duplicate magnet gid succeeded")
	}
	peers, err := engine.GetPeers("1111111111111111")
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 0 {
		t.Fatalf("paused magnet peers = %#v, want empty", peers)
	}
	activeMagnet, err := engine.addMagnet(magnet, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ForceRemove(activeMagnet); err != nil {
		t.Fatal(err)
	}

	closed, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := closed.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := closed.AddTorrent(encoded, nil, Options{"pause": "true"}, nil); !errors.Is(err, ErrShutdown) {
		t.Fatalf("AddTorrent after Close error = %v, want ErrShutdown", err)
	}
	if _, err := closed.AddURI([]string{magnet}, Options{"pause": "true"}, nil); !errors.Is(err, ErrShutdown) {
		t.Fatalf("AddURI magnet after Close error = %v, want ErrShutdown", err)
	}
}

func magnetFixtureURI() string {
	return "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=fixture"
}

func writeTestTorrentPayload(t *testing.T, dir string, info metainfo.Info) map[int][]byte {
	t.Helper()
	payloads := make(map[int][]byte)
	for i, file := range info.UpvertedFiles() {
		index := i + 1
		rel := strings.Join(file.BestPath(), "/")
		var data []byte
		switch rel {
		case "alpha.txt":
			data = []byte("alpha-alpha-alpha-alpha-alpha-alpha-alpha-alpha-")
		case "nested/beta.txt":
			data = []byte("beta-beta-beta-beta-beta-beta-")
		default:
			t.Fatalf("unknown test.torrent payload path %q", rel)
		}
		if int64(len(data)) != file.Length {
			t.Fatalf("test.torrent payload %q length = %d, want %d", rel, len(data), file.Length)
		}
		path := filepath.Join(append([]string{dir, info.BestName()}, file.BestPath()...)...)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
		payloads[index] = data
	}
	return payloads
}

func TestMagnetValidatesFileSelectionAfterMetadataLoads(t *testing.T) {
	_, encoded, peerAddrs, stopSeeder, _ := startLocalTorrentSeeder(t)
	defer stopSeeder()
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	mi, _, err := torrentMetaInfo(data)
	if err != nil {
		t.Fatal(err)
	}
	magnet := fmt.Sprintf("magnet:?xt=urn:btih:%s&dn=fixture", mi.HashInfoBytes().HexString())

	for name, extra := range map[string]Options{
		"select-file": {"select-file": "999"},
		"index-out":   {"index-out": "999=missing.txt"},
	} {
		t.Run(name, func(t *testing.T) {
			engine, err := NewEngine(Config{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer engine.Close(context.Background())
			opts := Options{
				"bt-metadata-only":         "true",
				"goaria-disable-dht":       "true",
				"goaria-disable-trackers":  "true",
				"goaria-disable-utp":       "true",
				"goaria-peer-addrs":        peerAddrs,
				"max-concurrent-downloads": "1",
			}
			for k, v := range extra {
				opts[k] = v
			}
			gid, err := engine.AddURI([]string{magnet}, opts, nil)
			if err != nil {
				t.Fatal(err)
			}
			waitForStatus(t, engine, gid, StatusError)
		})
	}
}

func TestTorrentMetadataOnlyAppliesTrackerOptions(t *testing.T) {
	data, err := os.ReadFile("test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddTorrent(base64.StdEncoding.EncodeToString(data), []string{"http://example.invalid/webseed"}, Options{
		"bt-exclude-tracker": "http://plab.site/ann?uk=R841G0WjJy",
		"bt-max-peers":       "8",
		"bt-metadata-only":   "true",
		"bt-tracker":         "http://127.0.0.1:1/announce",
		"max-upload-limit":   "16K",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	status, err := engine.TellStatus(gid, []string{"bittorrent"})
	if err != nil {
		t.Fatal(err)
	}
	bt := status["bittorrent"].(*BittorrentInfo)
	if len(bt.AnnounceList) == 0 || len(bt.AnnounceList[len(bt.AnnounceList)-1]) != 1 || bt.AnnounceList[len(bt.AnnounceList)-1][0] != "http://127.0.0.1:1/announce" {
		t.Fatalf("tracker override not reflected: %#v", bt.AnnounceList)
	}
}

func TestTorrentStreamHandlerProcessesAndReleasesFiles(t *testing.T) {
	seedDir, encoded, peerAddrs, stopSeeder, want := startLocalTorrentSeeder(t)
	defer stopSeeder()

	var seenMu sync.Mutex
	var seenBuf bytes.Buffer
	var seenFiles []string
	var engine *Engine
	engine, err := NewEngine(Config{
		Dir: t.TempDir(),
		TorrentFileHandler: func(ctx context.Context, tf TorrentFileLease) error {
			files, err := engine.GetFiles(tf.GID)
			if err != nil {
				return err
			}
			gotComplete := false
			for _, f := range files {
				if f.Index == strconv.Itoa(tf.Index) {
					gotComplete = f.CompletedLength == f.Length
					break
				}
			}
			if !gotComplete {
				return fmt.Errorf("handler called before file %d was complete", tf.Index)
			}
			got, err := io.ReadAll(tf.Reader)
			if err != nil {
				return err
			}
			seenMu.Lock()
			defer seenMu.Unlock()
			seenFiles = append(seenFiles, filepath.Base(tf.Path))
			seenBuf.WriteString(filepath.Base(tf.Path))
			seenBuf.WriteByte('=')
			seenBuf.Write(got)
			seenBuf.WriteByte('\n')
			return tf.Release(ctx)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddTorrent(encoded, nil, Options{
		"goaria-disable-dht":              "true",
		"goaria-disable-trackers":         "true",
		"goaria-disable-utp":              "true",
		"goaria-peer-addrs":               peerAddrs,
		"goaria-torrent-max-active-files": "99",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)

	seenMu.Lock()
	sort.Strings(seenFiles)
	seenFileNames := strings.Join(seenFiles, ",")
	got := seenBuf.String()
	seenMu.Unlock()
	if seenFileNames != "alpha.txt,beta.txt" {
		t.Fatalf("streamed files = %v", seenFiles)
	}
	for name, data := range want {
		if !strings.Contains(got, filepath.Base(name)+"="+data+"\n") {
			t.Fatalf("stream output missing %s=%q in:\n%s", name, data, got)
		}
	}
	files, err := engine.GetFiles(gid)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("GetFiles returned %d files, want 2", len(files))
	}
	for _, f := range files {
		if f.CompletedLength != f.Length {
			t.Fatalf("file %s completedLength=%s length=%s", f.Path, f.CompletedLength, f.Length)
		}
		if _, err := os.Stat(f.Path); !os.IsNotExist(err) {
			t.Fatalf("streamed file %s still exists or stat failed unexpectedly: %v", f.Path, err)
		}
	}
	if _, err := os.Stat(seedDir); err != nil {
		t.Fatalf("seeder data disappeared: %v", err)
	}
}

func TestTorrentStreamHandlerStartsCompletedFilesWhileEarlierHandlerRuns(t *testing.T) {
	_, encoded, peerAddrs, stopSeeder, _ := startLocalTorrentSeeder(t)
	defer stopSeeder()

	firstStarted := make(chan string, 1)
	secondStarted := make(chan string, 1)
	releaseFirst := make(chan struct{})
	var firstOnce sync.Once
	engine, err := NewEngine(Config{
		Dir: t.TempDir(),
		TorrentFileHandler: func(ctx context.Context, tf TorrentFileLease) error {
			name := filepath.Base(tf.Path)
			isFirst := false
			firstOnce.Do(func() {
				isFirst = true
				firstStarted <- name
			})
			if !isFirst {
				select {
				case secondStarted <- name:
				default:
				}
			}
			if _, err := io.ReadAll(tf.Reader); err != nil {
				return err
			}
			if !isFirst {
				return tf.Release(ctx)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-releaseFirst:
				return tf.Release(ctx)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddTorrent(encoded, nil, Options{
		"goaria-disable-dht":      "true",
		"goaria-disable-trackers": "true",
		"goaria-disable-utp":      "true",
		"goaria-peer-addrs":       peerAddrs,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("first stream handler did not start")
	}
	select {
	case <-secondStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("second stream handler did not start while first handler was still running")
	}
	close(releaseFirst)
	waitForStatus(t, engine, gid, StatusComplete)
}

func TestTorrentStreamLeaseWaitsForExplicitReleaseAndBoundsActiveFiles(t *testing.T) {
	_, encoded, peerAddrs, stopSeeder, _ := startLocalTorrentSeeder(t)
	defer stopSeeder()

	leases := make(chan TorrentFileLease, 2)
	engine, err := NewEngine(Config{
		Dir: t.TempDir(),
		TorrentFileHandler: func(ctx context.Context, tf TorrentFileLease) error {
			leases <- tf
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddTorrent(encoded, nil, Options{
		"goaria-disable-dht":              "true",
		"goaria-disable-trackers":         "true",
		"goaria-disable-utp":              "true",
		"goaria-peer-addrs":               peerAddrs,
		"goaria-torrent-max-active-files": "1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	first := receiveLease(t, leases, "first")
	assertStatus(t, engine, gid, StatusActive)
	peers, err := engine.GetPeers(gid)
	if err != nil {
		t.Fatal(err)
	}
	if peers == nil {
		t.Fatal("GetPeers returned nil for active torrent")
	}
	select {
	case second := <-leases:
		t.Fatalf("second lease %s started before first lease was released", second.Path)
	case <-time.After(250 * time.Millisecond):
	}
	time.Sleep(1100 * time.Millisecond)
	if err := first.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if pathExists(first.Path) || pathExists(first.Path+".part") {
		t.Fatalf("released file storage still exists: %q", first.Path)
	}

	second := receiveLease(t, leases, "second")
	if err := second.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
}

func TestTorrentStreamHandlerFailureReleasesFileStorage(t *testing.T) {
	_, encoded, peerAddrs, stopSeeder, _ := startLocalTorrentSeeder(t)
	defer stopSeeder()

	streamedPaths := make(chan string, 2)
	engine, err := NewEngine(Config{
		Dir: t.TempDir(),
		TorrentFileHandler: func(ctx context.Context, tf TorrentFileLease) error {
			if _, err := io.ReadAll(tf.Reader); err != nil {
				return err
			}
			streamedPaths <- tf.Path
			return io.ErrUnexpectedEOF
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddTorrent(encoded, nil, Options{
		"goaria-disable-dht":      "true",
		"goaria-disable-trackers": "true",
		"goaria-disable-utp":      "true",
		"goaria-peer-addrs":       peerAddrs,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusError)
	paths := drainStringChannel(streamedPaths)
	if len(paths) == 0 {
		t.Fatal("handler did not stream any files")
	}
	for _, streamedPath := range paths {
		if pathExists(streamedPath) || pathExists(streamedPath+".part") {
			t.Fatalf("handler failure should release file storage: %q", streamedPath)
		}
	}
}

func TestTorrentStreamHandlerDiscardRemovesStorageOnFailure(t *testing.T) {
	_, encoded, peerAddrs, stopSeeder, _ := startLocalTorrentSeeder(t)
	defer stopSeeder()

	streamedPaths := make(chan string, 2)
	engine, err := NewEngine(Config{
		Dir: t.TempDir(),
		TorrentFileHandler: func(ctx context.Context, tf TorrentFileLease) error {
			if _, err := io.ReadAll(tf.Reader); err != nil {
				return err
			}
			streamedPaths <- tf.Path
			if err := tf.Discard(ctx); err != nil {
				return err
			}
			return io.ErrUnexpectedEOF
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddTorrent(encoded, nil, Options{
		"goaria-disable-dht":      "true",
		"goaria-disable-trackers": "true",
		"goaria-disable-utp":      "true",
		"goaria-peer-addrs":       peerAddrs,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusError)
	paths := drainStringChannel(streamedPaths)
	if len(paths) == 0 {
		t.Fatal("handler did not stream any files")
	}
	for _, streamedPath := range paths {
		if pathExists(streamedPath) || pathExists(streamedPath+".part") {
			t.Fatalf("discarded file storage still exists: %q", streamedPath)
		}
	}
}

func drainStringChannel(ch <-chan string) []string {
	var out []string
	for {
		select {
		case s := <-ch:
			out = append(out, s)
		default:
			return out
		}
	}
}

func receiveLease(t *testing.T, leases <-chan TorrentFileLease, label string) TorrentFileLease {
	t.Helper()
	select {
	case lease := <-leases:
		return lease
	case <-time.After(3 * time.Second):
		t.Fatalf("%s lease did not start", label)
		return TorrentFileLease{}
	}
}

func assertStatus(t testing.TB, engine *Engine, gid string, want Status) {
	t.Helper()
	status, err := engine.TellStatus(gid, []string{"status", "errorMessage"})
	if err != nil {
		t.Fatal(err)
	}
	if status["status"] != string(want) {
		t.Fatalf("status = %v, want %s; error=%v", status["status"], want, status["errorMessage"])
	}
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func startSeederForMetaInfo(t *testing.T, dataDir string, mi *metainfo.MetaInfo) (peerAddrs []string, stop func()) {
	t.Helper()
	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = dataDir
	cfg.ListenHost = torrent.LoopbackListenHost
	cfg.ListenPort = 0
	cfg.NoDHT = true
	cfg.DisableTrackers = true
	cfg.DisableUTP = true
	cfg.Seed = true
	cfg.NoDefaultPortForwarding = true
	cfg.Slogger = slog.New(slog.NewTextHandler(io.Discard, nil))
	seeder, err := torrent.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	stop = func() { seeder.Close() }
	seederTorrent, err := seeder.AddTorrent(mi)
	if err != nil {
		stop()
		t.Fatal(err)
	}
	<-seederTorrent.GotInfo()
	if err := seederTorrent.VerifyDataContext(context.Background()); err != nil {
		stop()
		t.Fatal(err)
	}
	seederTorrent.DownloadAll()
	for _, addr := range seeder.ListenAddrs() {
		if addr.Network() != "tcp" {
			continue
		}
		peerAddrs = append(peerAddrs, addr.String())
	}
	if len(peerAddrs) == 0 {
		stop()
		t.Fatal("seeder has no listen addresses")
	}
	return peerAddrs, stop
}

func startLocalTorrentSeeder(t *testing.T) (seedDir string, encoded string, peerAddrs []string, stop func(), want map[string]string) {
	t.Helper()
	seedDir = t.TempDir()
	root := filepath.Join(seedDir, "fixture")
	want = map[string]string{
		"alpha.txt": strings.Repeat("alpha-", 32),
		"beta.txt":  strings.Repeat("beta-", 40),
	}
	for name, data := range want {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	info := metainfo.Info{PieceLength: 32}
	if err := info.BuildFromFilePath(root); err != nil {
		t.Fatal(err)
	}
	var mi metainfo.MetaInfo
	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	mi.InfoBytes = infoBytes
	var buf bytes.Buffer
	if err := mi.Write(&buf); err != nil {
		t.Fatal(err)
	}

	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = seedDir
	cfg.ListenHost = torrent.LoopbackListenHost
	cfg.ListenPort = 0
	cfg.NoDHT = true
	cfg.DisableTrackers = true
	cfg.DisableUTP = true
	cfg.Seed = true
	cfg.NoDefaultPortForwarding = true
	cfg.Slogger = slog.New(slog.NewTextHandler(io.Discard, nil))
	seeder, err := torrent.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	stop = func() { seeder.Close() }
	seederTorrent, err := seeder.AddTorrent(&mi)
	if err != nil {
		stop()
		t.Fatal(err)
	}
	<-seederTorrent.GotInfo()
	seederTorrent.DownloadAll()
	deadline := time.After(5 * time.Second)
	for seederTorrent.BytesCompleted() < seederTorrent.Length() {
		select {
		case <-deadline:
			stop()
			t.Fatalf("seed torrent did not verify local data: %d/%d", seederTorrent.BytesCompleted(), seederTorrent.Length())
		case <-time.After(25 * time.Millisecond):
		}
	}
	for _, addr := range seeder.ListenAddrs() {
		if addr.Network() != "tcp" {
			continue
		}
		peerAddrs = append(peerAddrs, addr.String())
	}
	if len(peerAddrs) == 0 {
		stop()
		t.Fatal("seeder has no listen addresses")
	}
	return seedDir, base64.StdEncoding.EncodeToString(buf.Bytes()), peerAddrs, stop, want
}
