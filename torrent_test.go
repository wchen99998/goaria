package goaria

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	infohash_v2 "github.com/wchen99998/torrent/types/infohash-v2"
)

func encodeMetaInfo(mi metainfo.MetaInfo) (string, error) {
	var buf bytes.Buffer
	if err := bencode.NewEncoder(&buf).Encode(mi); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

type recordingTorrentStorage struct {
	opened bool
}

func (s *recordingTorrentStorage) OpenTorrent(context.Context, *metainfo.Info, metainfo.Hash) (storage.TorrentImpl, error) {
	s.opened = true
	return storage.TorrentImpl{}, nil
}

func TestValidatingTorrentStorageRejectsMetadataBeforeDelegating(t *testing.T) {
	delegate := &recordingTorrentStorage{}
	wrapped := validatingTorrentStorage{delegate: delegate}
	invalid := &metainfo.Info{
		Name:        "unsafe",
		Length:      1,
		PieceLength: maxTorrentProtocolPieceLength + 1,
		Pieces:      make([]byte, metainfo.HashSize),
	}
	if _, err := wrapped.OpenTorrent(context.Background(), invalid, metainfo.Hash{}); err == nil {
		t.Fatal("unsafe metadata reached torrent storage")
	}
	if delegate.opened {
		t.Fatal("delegate storage opened before metadata validation")
	}

	valid := &metainfo.Info{Name: "empty"}
	if _, err := wrapped.OpenTorrent(context.Background(), valid, metainfo.Hash{}); err != nil {
		t.Fatalf("valid metadata rejected: %v", err)
	}
	if !delegate.opened {
		t.Fatal("valid metadata was not delegated")
	}
}

func TestTorrentFileStatesRejectSymlinkEscape(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(t.TempDir(), "outside.bin")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "payload.bin")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	info := metainfo.Info{Name: "payload.bin"}
	if _, err := torrentFileStates(info, metainfo.Hash{}, nil, dir); err == nil {
		t.Fatal("torrent output symlink escaped the download directory")
	}
}

func TestFetchTorrentURLDoesNotSleepAfterFinalAttempt(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("temporary network failure")
	})}
	engine := &Engine{client: client}
	started := time.Now()
	_, err := engine.fetchTorrentURL(context.Background(), "http://example.invalid/file.torrent", Options{
		"max-tries":  "1",
		"retry-wait": "1",
	})
	if err == nil {
		t.Fatal("fetchTorrentURL unexpectedly succeeded")
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("single final attempt slept for retry-wait: %v", elapsed)
	}
}

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

func TestAddTorrentRejectsMalformedV2InfoBeforeLayout(t *testing.T) {
	shortRoot := append([]byte("d9:file treed1:ad0:d6:lengthi1e11:pieces root5:shorteee6:lengthi1e12:meta versioni2e4:name1:x12:piece lengthi16384e6:pieces20:"), make([]byte, metainfo.HashSize)...)
	shortRoot = append(shortRoot, 'e')
	zeroPieceLength := append([]byte("d9:file treed1:ad0:d6:lengthi1e11:pieces root32:"), make([]byte, 32)...)
	zeroPieceLength = append(zeroPieceLength, []byte("eee6:lengthi1e12:meta versioni2e4:name1:x12:piece lengthi0e6:pieces20:")...)
	zeroPieceLength = append(zeroPieceLength, make([]byte, metainfo.HashSize)...)
	zeroPieceLength = append(zeroPieceLength, 'e')

	for name, info := range map[string][]byte{
		"short pieces root": shortRoot,
		"zero piece length": zeroPieceLength,
	} {
		t.Run(name, func(t *testing.T) {
			meta := append([]byte("d4:info"), info...)
			meta = append(meta, 'e')
			engine, err := NewEngine(Config{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer engine.Close(context.Background())

			if _, err := engine.AddTorrent(base64.StdEncoding.EncodeToString(meta), nil, Options{"pause": "true"}, nil); err == nil {
				t.Fatal("AddTorrent accepted malformed v2 metainfo")
			}
			if got := len(engine.downloadsSnapshot()); got != 0 {
				t.Fatalf("malformed torrent registered %d downloads", got)
			}
		})
	}
}

func TestTorrentMetadataValidationBoundsV2Geometry(t *testing.T) {
	root := string(bytes.Repeat([]byte{0x51}, 32))
	fileTree := func(length int64) metainfo.FileTree {
		return metainfo.FileTree{Dir: map[string]metainfo.FileTree{
			"payload.bin": {File: metainfo.FileTreeFile{Length: length, PiecesRoot: root}},
		}}
	}
	for name, tc := range map[string]struct {
		info    metainfo.Info
		wantErr string
	}{
		"piece length exceeds protocol integer": {
			info: metainfo.Info{
				MetaVersion: 2,
				PieceLength: 1 << 32,
				Name:        "oversized-piece",
				FileTree:    fileTree(1),
			},
			wantErr: "exceeds peer protocol limit",
		},
		"piece count exceeds allocation bound": {
			info: metainfo.Info{
				MetaVersion: 2,
				PieceLength: 16 << 10,
				Name:        "too-many-pieces",
				FileTree:    fileTree((maxTorrentPieceCount + 1) * (16 << 10)),
			},
			wantErr: "more than 1048576 pieces",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateTorrentInfo(&tc.info); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("validateTorrentInfo error = %v, want substring %q", err, tc.wantErr)
			}
		})
	}

	safeLargePiece := metainfo.Info{
		MetaVersion: 2,
		PieceLength: 128 << 20,
		Name:        "bounded-large-piece",
		FileTree:    fileTree(1),
	}
	if err := validateTorrentInfo(&safeLargePiece); err != nil {
		t.Fatalf("largest safe v2 piece length rejected: %v", err)
	}
}

func TestTorrentMetadataValidationChecksHybridLayout(t *testing.T) {
	const pieceLength = int64(16 << 10)
	root := string(bytes.Repeat([]byte{0x63}, 32))
	valid := metainfo.Info{
		MetaVersion: 2,
		PieceLength: pieceLength,
		Name:        "hybrid",
		Pieces:      make([]byte, 2*metainfo.HashSize),
		Files: []metainfo.FileInfo{
			{Length: 1, Path: []string{"a.bin"}},
			{
				Length:            pieceLength - 1,
				Path:              []string{".pad", strconv.FormatInt(pieceLength-1, 10)},
				ExtendedFileAttrs: metainfo.ExtendedFileAttrs{Attr: "p"},
			},
			{Length: 1, Path: []string{"b.bin"}},
		},
		FileTree: metainfo.FileTree{Dir: map[string]metainfo.FileTree{
			"a.bin": {File: metainfo.FileTreeFile{Length: 1, PiecesRoot: root}},
			"b.bin": {File: metainfo.FileTreeFile{Length: 1, PiecesRoot: root}},
		}},
	}
	if err := validateTorrentInfo(&valid); err != nil {
		t.Fatalf("valid aligned hybrid rejected: %v", err)
	}

	badPath := valid
	badPath.Files = append([]metainfo.FileInfo(nil), valid.Files...)
	badPath.Files[0].Path = []string{"different.bin"}
	if err := validateTorrentInfo(&badPath); err == nil || !strings.Contains(err.Error(), "paths differ") {
		t.Fatalf("hybrid path mismatch error = %v", err)
	}

	badOffset := valid
	badOffset.Files = append([]metainfo.FileInfo(nil), valid.Files...)
	badOffset.Files[1].Length++
	if err := validateTorrentInfo(&badOffset); err == nil || !strings.Contains(err.Error(), "offsets differ") {
		t.Fatalf("hybrid offset mismatch error = %v", err)
	}

	badPieceCount := valid
	badPieceCount.Pieces = badPieceCount.Pieces[:metainfo.HashSize]
	if err := validateTorrentInfo(&badPieceCount); err == nil || !strings.Contains(err.Error(), "piece count") {
		t.Fatalf("hybrid piece-count mismatch error = %v", err)
	}
}

func TestTorrentPieceLayerValidationRejectsUnexpectedHashCount(t *testing.T) {
	const pieceLength = int64(16 << 10)
	root := string(bytes.Repeat([]byte{0x72}, 32))
	info := metainfo.Info{
		MetaVersion: 2,
		PieceLength: pieceLength,
		Name:        "piece-layer-shape",
		FileTree: metainfo.FileTree{Dir: map[string]metainfo.FileTree{
			"payload.bin": {File: metainfo.FileTreeFile{Length: 2*pieceLength + 1, PiecesRoot: root}},
		}},
	}
	if err := validateTorrentInfo(&info); err != nil {
		t.Fatal(err)
	}
	layers := map[string]string{root: string(make([]byte, 4*32))}
	if err := validateTorrentPieceLayers(layers, &info); err == nil || !strings.Contains(err.Error(), "128 bytes, expected 96") {
		t.Fatalf("piece-layer shape error = %v", err)
	}
}

func TestTorrentPiecePadHashMatchesFork(t *testing.T) {
	for pieceLength := int64(16 << 10); pieceLength <= 2<<30; pieceLength <<= 1 {
		if got, want := torrentPiecePadHash(pieceLength), metainfo.HashForPiecePad(pieceLength); got != want {
			t.Fatalf("piece pad hash for %d bytes = %x, want %x", pieceLength, got, want)
		}
	}
}

func TestPausedSelectedTorrentBitfieldUsesWholeTorrentWidth(t *testing.T) {
	data, err := os.ReadFile("test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	_, info, err := torrentMetaInfo(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.UpvertedFiles()) < 2 {
		t.Fatal("test torrent needs at least two files")
	}
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddTorrent(base64.StdEncoding.EncodeToString(data), nil, Options{
		"pause":       "true",
		"select-file": "2",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantBitfield := unknownTorrentBitfield(int64(info.NumPieces()))
	assertStatus := func(label string) {
		t.Helper()
		status, err := engine.TellStatus(gid, []string{"numPieces", "bitfield"})
		if err != nil {
			t.Fatal(err)
		}
		if status["numPieces"] != strconv.Itoa(info.NumPieces()) || status["bitfield"] != wantBitfield {
			t.Fatalf("%s status = %#v, want whole-torrent zero bitfield %q", label, status, wantBitfield)
		}
		engine.mu.RLock()
		download := engine.downloads[gid]
		engine.mu.RUnlock()
		download.mu.RLock()
		donePieces := download.donePieces
		download.mu.RUnlock()
		if donePieces != 0 {
			t.Fatalf("%s donePieces = %d, want 0 without verified runtime state", label, donePieces)
		}
	}
	assertStatus("paused selection")

	engine.mu.RLock()
	download := engine.downloads[gid]
	engine.mu.RUnlock()
	if err := download.restoreTorrentFinalized(map[int]struct{}{2: {}}); err != nil {
		t.Fatal(err)
	}
	assertStatus("restored finalized selection")
	if _, err := engine.ChangeOption(gid, Options{"select-file": "1"}); err != nil {
		t.Fatal(err)
	}
	assertStatus("changed selection")
}

func TestV2OnlyTorrentUsesCanonicalShortInfoHash(t *testing.T) {
	mi, info, data := v2OnlyTestMetaInfo(t)
	v2Hash := infohash_v2.HashBytes(mi.InfoBytes)
	want := v2Hash.ToShort().HexString()
	if want == mi.HashInfoBytes().HexString() {
		t.Fatal("v2 fixture does not distinguish canonical and v1-style hashes")
	}

	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	directGID, err := engine.AddTorrent(base64.StdEncoding.EncodeToString(data), nil, Options{"pause": "true"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	directStatus, err := engine.TellStatus(directGID, []string{"infoHash"})
	if err != nil {
		t.Fatal(err)
	}
	if directStatus["infoHash"] != want {
		t.Fatalf("v2 torrent infoHash = %v, want canonical short hash %s", directStatus["infoHash"], want)
	}

	magnet := fmt.Sprintf("magnet:?xt=urn:btmh:1220%s&dn=v2-only", v2Hash.HexString())
	magnetGID, err := engine.AddURI([]string{magnet}, Options{"pause": "true"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	magnetStatus, err := engine.TellStatus(magnetGID, []string{"infoHash"})
	if err != nil {
		t.Fatal(err)
	}
	if magnetStatus["infoHash"] != want {
		t.Fatalf("v2 magnet infoHash = %v, want canonical short hash %s", magnetStatus["infoHash"], want)
	}

	engine.mu.RLock()
	magnetDownload := engine.downloads[magnetGID]
	engine.mu.RUnlock()
	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = t.TempDir()
	cfg.ListenHost = torrent.LoopbackListenHost
	cfg.ListenPort = 0
	cfg.NoDHT = true
	cfg.DisableTrackers = true
	cfg.DisableUTP = true
	cfg.NoUpload = true
	cfg.NoDefaultPortForwarding = true
	cfg.Slogger = slog.New(slog.NewTextHandler(io.Discard, nil))
	client, err := torrent.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	spec, err := torrent.TorrentSpecFromMetaInfo(mi)
	if err != nil {
		t.Fatal(err)
	}
	ensureTorrentSpecShortInfoHash(spec)
	tor, _, err := client.AddTorrentSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	<-tor.GotInfo()
	if err := engine.refreshTorrentMetadata(magnetDownload, tor); err != nil {
		t.Fatal(err)
	}
	refreshed, err := engine.TellStatus(magnetGID, []string{"infoHash", "numPieces", "bitfield"})
	if err != nil {
		t.Fatal(err)
	}
	if refreshed["infoHash"] != want {
		t.Fatalf("refreshed v2 magnet infoHash = %v, want stable hash %s", refreshed["infoHash"], want)
	}
	if refreshed["numPieces"] != strconv.Itoa(info.NumPieces()) {
		t.Fatalf("refreshed v2 numPieces = %v, want %d", refreshed["numPieces"], info.NumPieces())
	}

	hybrid := info
	hybrid.Pieces = make([]byte, metainfo.HashSize)
	hybridBytes, err := bencode.Marshal(&hybrid)
	if err != nil {
		t.Fatal(err)
	}
	hybridMI := &metainfo.MetaInfo{InfoBytes: hybridBytes}
	if got := canonicalTorrentMetaInfoHash(hybridMI, hybrid); got != hybridMI.HashInfoBytes() {
		t.Fatalf("hybrid canonical hash = %s, want v1 hash %s", got, hybridMI.HashInfoBytes())
	}
}

func TestMagnetMetadataRefreshAllowsDeferredV2PieceLayers(t *testing.T) {
	const pieceLength = int64(16 << 10)
	info := metainfo.Info{
		MetaVersion: 2,
		PieceLength: pieceLength,
		Name:        "deferred-layers",
		FileTree: metainfo.FileTree{Dir: map[string]metainfo.FileTree{
			"payload.bin": {File: metainfo.FileTreeFile{
				Length:     2*pieceLength + 1,
				PiecesRoot: string(bytes.Repeat([]byte{0x73}, 32)),
			}},
		}},
	}
	infoBytes, err := bencode.Marshal(&info)
	if err != nil {
		t.Fatal(err)
	}
	mi := &metainfo.MetaInfo{InfoBytes: infoBytes}
	if err := validateRefreshedTorrentMetadata(*mi, &info, true); err != nil {
		t.Fatalf("magnet metadata without immediate piece layers was rejected: %v", err)
	}
	if err := validateRefreshedTorrentMetadata(*mi, &info, false); err == nil || !strings.Contains(err.Error(), "piece layers") {
		t.Fatalf("direct v2 metadata without piece layers error = %v, want piece-layer rejection", err)
	}

	var encoded bytes.Buffer
	if err := bencode.NewEncoder(&encoded).Encode(mi); err != nil {
		t.Fatal(err)
	}
	d := &Download{torrentMagnet: "magnet:?xt=urn:btmh:1220deferred", torrentData: encoded.Bytes()}
	d.mu.Lock()
	cached, err := d.metadataInfoLocked()
	d.mu.Unlock()
	if err != nil {
		t.Fatalf("cached magnet metadata became unusable: %v", err)
	}
	if cached == nil || cached.PieceLength != pieceLength {
		t.Fatalf("cached magnet info = %#v, want piece length %d", cached, pieceLength)
	}
}

func TestEmbeddedTorrentWebseedUsesConfiguredHTTPOptions(t *testing.T) {
	payload := bytes.Repeat([]byte("configured-webseed-"), 2048)
	requests := make(chan http.Header, 32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case requests <- r.Header.Clone():
		default:
		}
		if r.Header.Get("X-Goaria-Webseed") != "configured" {
			http.Error(w, "missing configured header", http.StatusUnauthorized)
			return
		}
		http.ServeContent(w, r, "payload.bin", time.Unix(1, 0), bytes.NewReader(payload))
	}))
	defer server.Close()

	source := filepath.Join(t.TempDir(), "payload.bin")
	if err := os.WriteFile(source, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	info := metainfo.Info{PieceLength: 16 << 10}
	if err := info.BuildFromFilePath(source); err != nil {
		t.Fatal(err)
	}
	infoBytes, err := bencode.Marshal(&info)
	if err != nil {
		t.Fatal(err)
	}
	mi := metainfo.MetaInfo{
		InfoBytes: infoBytes,
		UrlList:   []string{server.URL, server.URL},
	}
	var encoded bytes.Buffer
	if err := mi.Write(&encoded); err != nil {
		t.Fatal(err)
	}

	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddTorrent(base64.StdEncoding.EncodeToString(encoded.Bytes()), []string{server.URL}, Options{
		"header":                  []string{"X-Goaria-Webseed: configured"},
		"goaria-disable-dht":      "true",
		"goaria-disable-trackers": "true",
		"goaria-disable-utp":      "true",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	select {
	case header := <-requests:
		if got := header.Get("X-Goaria-Webseed"); got != "configured" {
			t.Fatalf("embedded webseed header = %q, want configured", got)
		}
	default:
		t.Fatal("embedded webseed was not requested")
	}

	spec := &torrent.TorrentSpec{Webseeds: []string{"https://one.invalid", "https://two.invalid", "https://one.invalid"}}
	got, err := extractTorrentWebSeedURLs(spec, []URIInfo{
		{URI: "https://two.invalid"},
		{URI: "https://three.invalid"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "https://one.invalid,https://two.invalid,https://three.invalid" {
		t.Fatalf("combined webseeds = %#v", got)
	}
	if spec.Webseeds != nil {
		t.Fatalf("TorrentSpec retained unconfigured webseeds: %#v", spec.Webseeds)
	}
	overLimit := make([]string, maxTorrentWebSeeds+1)
	for i := range overLimit {
		overLimit[i] = fmt.Sprintf("https://webseed-%d.invalid", i)
	}
	if _, err := mergeTorrentWebSeedURLs(overLimit, nil); err == nil {
		t.Fatal("accepted an unbounded embedded webseed list")
	}
}

func TestTorrentPieceProgressUsesVerifiedPieceRuns(t *testing.T) {
	complete := torrent.PieceState{Completion: storage.Completion{Ok: true, Complete: true}}
	incomplete := torrent.PieceState{Completion: storage.Completion{Ok: true}}
	unknownComplete := torrent.PieceState{Completion: storage.Completion{Complete: true}}
	runs := torrent.PieceStateRuns{
		{PieceState: complete, Length: 1},
		{PieceState: incomplete, Length: 1},
		{PieceState: complete, Length: 2},
		{PieceState: unknownComplete, Length: 1},
		{PieceState: complete, Length: 1},
		{PieceState: incomplete, Length: 4},
	}
	if done, bitfield := torrentPieceProgress(runs); done != 4 || bitfield != "b400" {
		t.Fatalf("piece progress = (%d, %q), want (4, %q)", done, bitfield, "b400")
	}
	if done, bitfield := torrentPieceProgress(torrent.PieceStateRuns{{PieceState: unknownComplete, Length: 2}}); done != 0 || bitfield != "00" {
		t.Fatalf("unknown piece progress = (%d, %q), want (0, %q)", done, bitfield, "00")
	}
	if done, bitfield := torrentPieceProgress(nil); done != 0 || bitfield != "" {
		t.Fatalf("empty piece progress = (%d, %q), want zero values", done, bitfield)
	}
}

func TestTorrentDownloadRateLimitUsesDefaultBoundedBurst(t *testing.T) {
	cfg := torrent.NewDefaultClientConfig()
	applyTorrentRateLimitOptions(cfg, Options{"max-download-limit": "2K", "max-upload-limit": "3K"})
	if cfg.DownloadRateLimiter == nil {
		t.Fatal("max-download-limit did not configure the torrent client")
	}
	if got := int64(cfg.DownloadRateLimiter.Limit()); got != 2<<10 {
		t.Fatalf("torrent download limit = %d, want %d", got, 2<<10)
	}
	if got := cfg.DownloadRateLimiter.Burst(); got != 0 {
		t.Fatalf("torrent download limiter burst = %d, want zero for fork defaulting", got)
	}
	if cfg.UploadRateLimiter == nil || int64(cfg.UploadRateLimiter.Limit()) != 3<<10 || cfg.UploadRateLimiter.Burst() != 0 {
		t.Fatalf("torrent upload limiter = %#v, want 3K with zero burst", cfg.UploadRateLimiter)
	}
}

func TestTorrentOverallDownloadLimitAppliesAsCeiling(t *testing.T) {
	cfg := torrent.NewDefaultClientConfig()
	applyTorrentRateLimitOptions(cfg, Options{
		"max-download-limit":         "8K",
		"max-overall-download-limit": "2K",
	})
	if cfg.DownloadRateLimiter == nil || int64(cfg.DownloadRateLimiter.Limit()) != 2<<10 {
		t.Fatalf("torrent download limiter = %#v, want overall 2K ceiling", cfg.DownloadRateLimiter)
	}
}

func TestAddTorrentFetchesHTTPURLWithWebseedsAndPersistsBytes(t *testing.T) {
	data, err := os.ReadFile("test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	mi, _, err := torrentMetaInfo(data)
	if err != nil {
		t.Fatal(err)
	}
	wantInfoHash := mi.HashInfoBytes().HexString()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/redirect.torrent":
			cookie := r.Header.Get("Cookie")
			if !strings.Contains(cookie, "redirect=only") {
				http.Error(w, "missing redirect cookie", http.StatusUnauthorized)
				return
			}
			if strings.Contains(cookie, "session=ok") {
				http.Error(w, "final-path cookie leaked to redirect", http.StatusBadRequest)
				return
			}
			http.Redirect(w, r, "/file.torrent", http.StatusFound)
		case "/file.torrent":
			if got := r.Header.Get("Authorization"); got != "Bearer token" {
				http.Error(w, "missing authorization", http.StatusUnauthorized)
				return
			}
			if got := r.UserAgent(); got != "goaria-client/1.0" {
				http.Error(w, "missing user agent", http.StatusBadRequest)
				return
			}
			if got := r.Header.Get("Cookie"); !strings.Contains(got, "session=ok") {
				http.Error(w, "missing cookie", http.StatusUnauthorized)
				return
			} else if strings.Contains(got, "redirect=only") {
				http.Error(w, "redirect cookie leaked to final URL", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/x-bittorrent")
			_, _ = w.Write(data)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	cookieDomain := parsed.Hostname()
	dir := t.TempDir()
	cookiePath := filepath.Join(dir, "cookies.txt")
	cookieData := cookieDomain + "\tFALSE\t/redirect.torrent\tFALSE\t0\tredirect\tonly\n" +
		cookieDomain + "\tFALSE\t/file.torrent\tFALSE\t0\tsession\tok\n"
	if err := os.WriteFile(cookiePath, []byte(cookieData), 0o644); err != nil {
		t.Fatal(err)
	}
	sessionPath := filepath.Join(dir, "goaria.session")
	engine, err := NewEngine(Config{Dir: dir, SaveSession: sessionPath})
	if err != nil {
		t.Fatal(err)
	}

	sourceURL := server.URL + "/redirect.torrent"
	webseed := "https://cdn.example.com/payload-file"
	gid, err := engine.AddTorrent(sourceURL, []string{webseed}, Options{
		"pause":        "true",
		"header":       []string{"Authorization: Bearer token"},
		"user-agent":   "goaria-client/1.0",
		"load-cookies": cookiePath,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	status, err := engine.TellStatus(gid, []string{"status", "infoHash", "bittorrent"})
	if err != nil {
		t.Fatal(err)
	}
	if status["status"] != string(StatusPaused) || status["infoHash"] != wantInfoHash {
		t.Fatalf("status = %#v, want paused torrent %s", status, wantInfoHash)
	}
	uris, err := engine.GetURIs(gid)
	if err != nil {
		t.Fatal(err)
	}
	if len(uris) != 1 || uris[0].URI != webseed {
		t.Fatalf("torrent webseeds = %#v, want only %q", uris, webseed)
	}
	opts, err := engine.GetOption(gid)
	if err != nil {
		t.Fatal(err)
	}
	if opts["goaria-torrent-source-url"] != sourceURL {
		t.Fatalf("source URL option = %#v, want %q", opts["goaria-torrent-source-url"], sourceURL)
	}
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	sessionData, err := os.ReadFile(sessionPath)
	if err != nil {
		t.Fatal(err)
	}
	session := string(sessionData)
	if !strings.Contains(session, sessionTorrentDataURIPrefix) {
		t.Fatalf("session did not store fetched torrent bytes:\n%s", session)
	}
	if strings.Contains(strings.SplitN(session, "\n", 2)[0], sourceURL) {
		t.Fatalf("session URI line depends on source URL:\n%s", session)
	}

	server.Close()
	restored, err := NewEngine(Config{Dir: dir, InputFile: sessionPath})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close(context.Background())
	restoredStatus, err := restored.TellStatus(gid, []string{"status", "infoHash"})
	if err != nil {
		t.Fatal(err)
	}
	if restoredStatus["status"] != string(StatusPaused) || restoredStatus["infoHash"] != wantInfoHash {
		t.Fatalf("restored status = %#v, want paused torrent %s", restoredStatus, wantInfoHash)
	}
}

func TestAddTorrentDirectHonorsMaxTorrentSize(t *testing.T) {
	data, err := os.ReadFile("test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	limit := int64(len(data) - 1)
	if _, err := engine.AddTorrent(base64.StdEncoding.EncodeToString(data), nil, Options{
		"goaria-max-torrent-size": strconv.FormatInt(limit, 10),
		"pause":                   "true",
	}, nil); err == nil {
		t.Fatal("oversized direct torrent was accepted")
	} else {
		var sizeErr *torrentSizeError
		if !errors.As(err, &sizeErr) || sizeErr.max != limit {
			t.Fatalf("oversized direct torrent error = %v, want limit %d", err, limit)
		}
	}
	if got := len(engine.downloadsSnapshot()); got != 0 {
		t.Fatalf("oversized direct torrent registered %d downloads", got)
	}
	if _, err := engine.AddTorrent(base64.StdEncoding.EncodeToString(data), nil, Options{
		"goaria-max-torrent-size": strconv.Itoa(len(data)),
		"pause":                   "true",
	}, nil); err != nil {
		t.Fatalf("torrent at exact configured size rejected: %v", err)
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
		TorrentFileHandler: func(ctx context.Context, tf TorrentFile) error {
			got, err := io.ReadAll(tf.Lease.Reader)
			if err != nil {
				return err
			}
			streamed <- got
			return tf.Lease.Release(ctx)
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
	for name, extra := range map[string]Options{
		"extension selection": {"goaria-select-file-ext": ".missing"},
	} {
		t.Run(name, func(t *testing.T) {
			engine, err := NewEngine(Config{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer engine.Close(context.Background())

			opts := Options{
				"goaria-disable-dht":      "true",
				"goaria-disable-trackers": "true",
				"goaria-disable-utp":      "true",
			}
			for k, v := range extra {
				opts[k] = v
			}
			gid, err := engine.AddTorrent(base64.StdEncoding.EncodeToString(data), nil, opts, nil)
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
		})
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

	selected, err := selectedFileSet(5, "2,4-5")
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range []int{2, 4, 5} {
		if !selected[index] {
			t.Fatalf("selectedFileSet missing index %d: %#v", index, selected)
		}
	}
	if selected[1] || selected[3] || selected[6] {
		t.Fatalf("selectedFileSet selected unexpected indexes: %#v", selected)
	}
	if _, err := selectedFileSet(1, "999"); err == nil {
		t.Fatal("out-of-range single-file selection unexpectedly succeeded")
	}
	if _, err := selectedFileSet(5, "2,bad"); err == nil {
		t.Fatal("malformed selection unexpectedly succeeded")
	}

	extInfo := metainfo.Info{
		Name:        "bundle",
		PieceLength: 16,
		Files: []metainfo.FileInfo{
			{Path: []string{"movie.MKV"}, Length: 10},
			{Path: []string{"clip.mp4"}, Length: 20},
			{Path: []string{"archive.tar.gz"}, Length: 30},
			{Path: []string{"poster.JPG"}, Length: 40},
			{Path: []string{"notes.nfo"}, Length: 50},
		},
	}
	selected, _, err = torrentSelectionAndIndexOut(extInfo, Options{"goaria-select-file-ext": []string{"mkv", ".tar.gz"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range []int{1, 3} {
		if !selected[index] {
			t.Fatalf("extension selection missing index %d: %#v", index, selected)
		}
	}
	if selected[2] || selected[4] || selected[5] {
		t.Fatalf("extension selection selected unexpected indexes: %#v", selected)
	}
	selected, _, err = torrentSelectionAndIndexOut(extInfo, Options{
		"select-file":             "1-2,4",
		"goaria-select-file-ext":  ".mkv,.jpg",
		"goaria-exclude-file-ext": "jpg",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !selected[1] || len(selected) != 1 {
		t.Fatalf("composed extension selection = %#v, want only index 1", selected)
	}
	selected, _, err = torrentSelectionAndIndexOut(extInfo, Options{"goaria-select-file-ext": ".flac"})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 0 {
		t.Fatalf("no-match extension selection = %#v, want no files", selected)
	}
	for _, opts := range []Options{
		{"goaria-select-file-ext": "bad/name"},
		{"goaria-select-file-ext": "."},
		{"goaria-exclude-file-ext": []string{"ok", `bad\name`}},
	} {
		if _, _, err := torrentSelectionAndIndexOut(extInfo, opts); err == nil {
			t.Fatalf("torrentSelectionAndIndexOut(%#v) succeeded, want error", opts)
		}
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
		{"select-file": "1-9223372036854775807"},
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
	gotPath, err := maker(storage.FilePathMakerOpts{Info: &info, File: &upverted[0], FileIndex: 0})
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "custom-alpha.txt" {
		t.Fatalf("custom path = %q, want custom-alpha.txt", gotPath)
	}
	gotPath, err = maker(storage.FilePathMakerOpts{Info: &info, File: &upverted[1], FileIndex: 1})
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != filepath.Join(info.BestName(), "nested", "beta.txt") {
		t.Fatalf("default path = %q", gotPath)
	}
	states, err := torrentFileStates(info, metainfo.Hash{}, Options{"index-out": "2=custom-beta.txt"}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(states[1].Path) != "custom-beta.txt" {
		t.Fatalf("index-out torrent state path = %q", states[1].Path)
	}

	if _, err := newMagnetDownload("gid", "magnet:?xt=urn:btih:not-a-hash", nil, nil); err == nil {
		t.Fatal("newMagnetDownload accepted invalid magnet")
	}
	d, err := newTorrentDownload("gid", nil, "", nil, Options{}, nil, metainfo.Info{})
	if err != nil {
		t.Fatal(err)
	}
	if d.dir != "." {
		t.Fatalf("empty torrent options dir = %q, want .", d.dir)
	}
	identityCfg := torrent.NewDefaultClientConfig()
	if err := applyTorrentClientIdentityOptions(identityCfg, Options{}); err != nil {
		t.Fatal(err)
	}
	if identityCfg.Bep20 != qBittorrentBep20Prefix {
		t.Fatalf("default torrent Bep20 = %q, want %q", identityCfg.Bep20, qBittorrentBep20Prefix)
	}
	if identityCfg.HTTPUserAgent != qBittorrentPeerVisibleVersion {
		t.Fatalf("default torrent user agent = %q, want %q", identityCfg.HTTPUserAgent, qBittorrentPeerVisibleVersion)
	}
	if identityCfg.ExtendedHandshakeClientVersion != qBittorrentPeerVisibleVersion {
		t.Fatalf("default torrent client version = %q, want %q", identityCfg.ExtendedHandshakeClientVersion, qBittorrentPeerVisibleVersion)
	}
	identityCfg = torrent.NewDefaultClientConfig()
	if err := applyTorrentClientIdentityOptions(identityCfg, Options{
		"goaria-torrent-bep20-prefix":   "-XX0100-",
		"goaria-torrent-client-version": "Client/1.0",
		"goaria-torrent-user-agent":     "Client/1.0",
	}); err != nil {
		t.Fatal(err)
	}
	if identityCfg.Bep20 != "-XX0100-" || identityCfg.HTTPUserAgent != "Client/1.0" || identityCfg.ExtendedHandshakeClientVersion != "Client/1.0" {
		t.Fatalf("torrent identity overrides not applied: prefix=%q ua=%q version=%q", identityCfg.Bep20, identityCfg.HTTPUserAgent, identityCfg.ExtendedHandshakeClientVersion)
	}
	if err := applyTorrentClientIdentityOptions(torrent.NewDefaultClientConfig(), Options{"goaria-torrent-bep20-prefix": strings.Repeat("x", 21)}); err == nil {
		t.Fatal("accepted too-long torrent Bep20 prefix")
	}
	if err := applyTorrentClientIdentityOptions(torrent.NewDefaultClientConfig(), Options{"goaria-torrent-peer-profile": "bad"}); err == nil {
		t.Fatal("accepted invalid torrent peer profile")
	}
	discoveryCfg := torrent.NewDefaultClientConfig()
	applyTorrentPeerDiscoveryOptions(discoveryCfg, Options{}, true)
	if !discoveryCfg.NoDHT || !discoveryCfg.DisablePEX || discoveryCfg.DisableTrackers {
		t.Fatalf("private torrent discovery defaults = NoDHT:%v DisablePEX:%v DisableTrackers:%v, want true true false", discoveryCfg.NoDHT, discoveryCfg.DisablePEX, discoveryCfg.DisableTrackers)
	}
	discoveryCfg = torrent.NewDefaultClientConfig()
	applyTorrentPeerDiscoveryOptions(discoveryCfg, Options{
		"goaria-disable-dht": "false",
		"goaria-disable-pex": "false",
	}, true)
	if discoveryCfg.NoDHT || discoveryCfg.DisablePEX {
		t.Fatalf("explicit private discovery overrides not applied: NoDHT:%v DisablePEX:%v", discoveryCfg.NoDHT, discoveryCfg.DisablePEX)
	}
	discoveryCfg = torrent.NewDefaultClientConfig()
	applyTorrentPeerDiscoveryOptions(discoveryCfg, Options{"goaria-disable-pex": "true"}, false)
	if !discoveryCfg.DisablePEX {
		t.Fatal("goaria-disable-pex was not applied")
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
	canceledMagnet, err := newTorrentDownload("gid", nil, magnetFixtureURI(), nil, Options{
		"goaria-disable-dht":      "true",
		"goaria-disable-trackers": "true",
		"goaria-disable-utp":      "true",
	}, nil, metainfo.Info{Name: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	canceledMagnet.ctx = ctx
	if err := engine.runTorrentDownload(canceledMagnet); !errors.Is(err, context.Canceled) {
		t.Fatalf("runTorrentDownload with canceled magnet = %v, want context.Canceled", err)
	}
	invalidMagnet, err := newTorrentDownload("gid", nil, "magnet:?xt=urn:btih:not-a-hash", nil, Options{}, nil, metainfo.Info{})
	if err != nil {
		t.Fatal(err)
	}
	invalidMagnet.ctx = context.Background()
	if err := engine.runTorrentDownload(invalidMagnet); err == nil {
		t.Fatal("runTorrentDownload accepted invalid magnet")
	}
	invalidTorrent, err := newTorrentDownload("gid", []byte("not a torrent"), "", nil, Options{}, nil, metainfo.Info{})
	if err != nil {
		t.Fatal(err)
	}
	invalidTorrent.ctx = context.Background()
	if err := engine.runTorrentDownload(invalidTorrent); err == nil {
		t.Fatal("runTorrentDownload accepted invalid torrent data")
	}
	badListenHost, err := newTorrentDownload("gid", data, "", nil, Options{"goaria-listen-host": "bad host"}, mi, info)
	if err != nil {
		t.Fatal(err)
	}
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
	progressDownload, err := newTorrentDownload("gid", data, "", nil, Options{"dir": cfg.DataDir}, mi, info)
	if err != nil {
		t.Fatal(err)
	}
	engine.updateTorrentProgress(progressDownload, tor)
	if progressDownload.torrentFiles[0].Completed < 0 || progressDownload.completedLen < 0 {
		t.Fatalf("torrent progress should not be negative: file=%d total=%d", progressDownload.torrentFiles[0].Completed, progressDownload.completedLen)
	}
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
		"bad torrent extension selection": func() error {
			_, err := engine.AddTorrent(encoded, nil, Options{"pause": "true", "goaria-select-file-ext": "bad/name"}, nil)
			return err
		},
		"bad magnet": func() error {
			_, err := engine.AddURI([]string{"magnet:?xt=urn:btih:not-a-hash"}, Options{"pause": "true"}, nil)
			return err
		},
		"bad magnet extension selection": func() error {
			_, err := engine.AddURI([]string{magnet}, Options{"pause": "true", "goaria-exclude-file-ext": "."}, nil)
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

func TestTorrentChangeOptionAppliesExtensionSelection(t *testing.T) {
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
	if _, err := engine.ChangeOption(gid, Options{"goaria-select-file-ext": ".missing"}); err != nil {
		t.Fatal(err)
	}
	files, err := engine.GetFiles(gid)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if file.Selected != "false" {
			t.Fatalf("file %s selected = %s, want false after no-match extension filter", file.Index, file.Selected)
		}
	}
	status, err := engine.TellStatus(gid, []string{"totalLength"})
	if err != nil {
		t.Fatal(err)
	}
	if status["totalLength"] != "0" {
		t.Fatalf("totalLength = %v, want 0", status["totalLength"])
	}
	if _, err := engine.ChangeOption(gid, Options{"goaria-select-file-ext": ".txt"}); err != nil {
		t.Fatal(err)
	}
	files, err = engine.GetFiles(gid)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if file.Selected != "true" {
			t.Fatalf("file %s selected = %s, want true after .txt extension filter", file.Index, file.Selected)
		}
	}
	gotOpts, err := engine.GetOption(gid)
	if err != nil {
		t.Fatal(err)
	}
	if gotOpts["goaria-select-file-ext"] != ".txt" {
		t.Fatalf("goaria-select-file-ext option = %v, want .txt", gotOpts["goaria-select-file-ext"])
	}
	if _, ok := gotOpts["select-file"]; ok {
		t.Fatalf("GetOption synthesized select-file: %#v", gotOpts["select-file"])
	}
	if _, err := engine.ChangeOption(gid, Options{"goaria-exclude-file-ext": "bad/name"}); err == nil {
		t.Fatal("ChangeOption accepted invalid extension filter")
	}
	if _, err := engine.ChangeGlobalOption(Options{"goaria-select-file-ext": "bad/name"}); err == nil {
		t.Fatal("ChangeGlobalOption accepted invalid extension filter")
	}
}

func TestActiveTorrentSelectionChangeRestartsFixedSelection(t *testing.T) {
	_, encoded, peerAddrs, stopSeeder, _ := startLocalTorrentSeeder(t)
	defer stopSeeder()
	firstStarted := make(chan struct{}, 1)
	secondHandled := make(chan struct{}, 1)
	releaseCanceledHandler := make(chan struct{})
	var releaseOnce sync.Once
	releaseCanceled := func() { releaseOnce.Do(func() { close(releaseCanceledHandler) }) }
	defer releaseCanceled()
	engine, err := NewEngine(Config{
		Dir: t.TempDir(),
		TorrentFileHandler: func(ctx context.Context, tf TorrentFile) error {
			switch tf.Lease.Index {
			case 0:
				select {
				case firstStarted <- struct{}{}:
				default:
				}
				<-ctx.Done()
				<-releaseCanceledHandler
				return ctx.Err()
			case 1:
				if err := tf.Lease.Release(ctx); err != nil {
					return err
				}
				select {
				case secondHandled <- struct{}{}:
				default:
				}
				return nil
			default:
				return fmt.Errorf("unexpected file index %d", tf.Lease.Index)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddTorrent(encoded, nil, Options{
		"select-file":                     "1",
		"goaria-peer-addrs":               peerAddrs,
		"goaria-disable-dht":              "true",
		"goaria-disable-trackers":         "true",
		"goaria-disable-utp":              "true",
		"goaria-torrent-max-active-files": "1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first selected file did not reach the handler")
	}
	before, err := engine.GetFiles(gid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ChangeOption(gid, Options{"select-file": "2"}); err != nil {
		t.Fatal(err)
	}
	after, err := engine.GetFiles(gid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.reserveOutputPath(before[0].Path, "competing-old", false); err == nil {
		t.Fatal("old torrent output path was released while its canceled runtime could still write")
	}
	if _, err := engine.reserveOutputPath(after[1].Path, "competing-new", false); err == nil {
		t.Fatal("replacement torrent output path was not reserved before restart")
	}
	releaseCanceled()
	select {
	case <-secondHandled:
	case <-time.After(5 * time.Second):
		t.Fatal("replacement run did not process the newly selected file")
	}
	waitForStatus(t, engine, gid, StatusComplete)
	files, err := engine.GetFiles(gid)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files[0].Selected != "false" || files[1].Selected != "true" {
		t.Fatalf("selection after active restart = %#v", files)
	}
}

func TestTorrentChangeOptionIsAtomicOnLayoutError(t *testing.T) {
	data, err := os.ReadFile("test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	engine, err := NewEngine(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddTorrent(base64.StdEncoding.EncodeToString(data), nil, Options{"pause": "true"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ChangeOption(gid, Options{
		"dir":       t.TempDir(),
		"index-out": "999=missing.txt",
	}); err == nil {
		t.Fatal("ChangeOption accepted an out-of-range index-out")
	}

	opts, err := engine.GetOption(gid)
	if err != nil {
		t.Fatal(err)
	}
	if got := opts["dir"]; got != dir {
		t.Fatalf("dir = %v after failed ChangeOption, want %s", got, dir)
	}
	if _, ok := opts["index-out"]; ok {
		t.Fatalf("failed ChangeOption persisted index-out: %#v", opts)
	}
}

func TestChangeOptionActiveMagnetBeforeMetadata(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddURI([]string{magnetFixtureURI()}, Options{
		"goaria-disable-dht":      "true",
		"goaria-disable-trackers": "true",
		"goaria-disable-utp":      "true",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForActiveMagnetWithoutMetadata(t, engine, gid)

	if _, err := engine.ChangeOption(gid, Options{"max-download-limit": "1K"}); err != nil {
		t.Fatalf("ChangeOption before magnet metadata = %v, want nil", err)
	}
}

func TestMagnetChangeOptionBeforeMetadataAffectsLoadedSelection(t *testing.T) {
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

	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{magnet}, Options{
		"bt-metadata-only":         "true",
		"goaria-disable-dht":       "true",
		"goaria-disable-trackers":  "true",
		"goaria-disable-utp":       "true",
		"max-concurrent-downloads": "1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tor := waitForActiveMagnetWithoutMetadata(t, engine, gid)
	if _, err := engine.ChangeOption(gid, Options{"goaria-select-file-ext": ".missing"}); err != nil {
		t.Fatal(err)
	}
	if err := tor.MergeSpec(&torrent.TorrentSpec{PeerAddrs: peerAddrs}); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	files, err := engine.GetFiles(gid)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if file.Selected != "false" {
			t.Fatalf("file %s selected = %s, want false from pre-metadata ChangeOption", file.Index, file.Selected)
		}
	}
}

func waitForActiveMagnetWithoutMetadata(t *testing.T, engine *Engine, gid string) *torrent.Torrent {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		engine.mu.RLock()
		d := engine.downloads[gid]
		engine.mu.RUnlock()
		if d != nil {
			d.mu.RLock()
			tor := d.torrent
			var activeWithoutMetadata bool
			if tor != nil && tor.torrent != nil {
				activeWithoutMetadata = len(tor.torrent.Metainfo().InfoBytes) == 0
			}
			d.mu.RUnlock()
			if activeWithoutMetadata {
				return tor.torrent
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("magnet did not become active without metadata")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestTorrentChangeOptionPreservesProgressWhenRebuildingFiles(t *testing.T) {
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
	engine.mu.RLock()
	d := engine.downloads[gid]
	engine.mu.RUnlock()
	d.mu.Lock()
	firstCompleted := d.torrentFiles[0].Length
	secondCompleted := int64(7)
	d.torrentFiles[0].Completed = firstCompleted
	d.torrentFiles[1].Completed = secondCompleted
	d.completedLen = firstCompleted + secondCompleted
	d.mu.Unlock()

	if _, err := engine.ChangeOption(gid, Options{"goaria-select-file-ext": ".missing"}); err != nil {
		t.Fatal(err)
	}
	files, err := engine.GetFiles(gid)
	if err != nil {
		t.Fatal(err)
	}
	if files[0].CompletedLength != files[0].Length || files[0].Selected != "false" {
		t.Fatalf("completed unselected file was not preserved: %#v", files[0])
	}
	status, err := engine.TellStatus(gid, []string{"totalLength", "completedLength"})
	if err != nil {
		t.Fatal(err)
	}
	if status["totalLength"] != "0" || status["completedLength"] != "0" {
		t.Fatalf("unselected progress status = %#v, want zero selected lengths", status)
	}
	if _, err := engine.ChangeOption(gid, Options{"goaria-select-file-ext": ".txt"}); err != nil {
		t.Fatal(err)
	}
	files, err = engine.GetFiles(gid)
	if err != nil {
		t.Fatal(err)
	}
	if files[0].CompletedLength != files[0].Length || files[1].CompletedLength != strconv.FormatInt(secondCompleted, 10) {
		t.Fatalf("progress after reselection = %#v", files)
	}
	status, err = engine.TellStatus(gid, []string{"completedLength"})
	if err != nil {
		t.Fatal(err)
	}
	if status["completedLength"] != strconv.FormatInt(firstCompleted+secondCompleted, 10) {
		t.Fatalf("completedLength after reselection = %v, want %d", status["completedLength"], firstCompleted+secondCompleted)
	}
}

func TestTorrentLayoutChangeKeepsFinalizedFileIdentity(t *testing.T) {
	data, err := os.ReadFile("test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	mi, info, err := torrentMetaInfo(data)
	if err != nil {
		t.Fatal(err)
	}
	oldDir := t.TempDir()
	oldStates, err := torrentFileStates(info, mi.HashInfoBytes(), Options{}, oldDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(oldStates) < 2 {
		t.Fatal("torrent fixture needs at least two files")
	}
	oldStates[0].Completed = oldStates[0].Length
	oldStates[1].Completed = min(oldStates[1].Length, 7)
	finalized := map[int]struct{}{oldStates[0].Index: {}}
	newDir := t.TempDir()
	next, _, _, err := rebuildTorrentFileStatesPreservingProgress(info, Options{
		"index-out": []string{fmt.Sprintf("%d=moved/final.bin", oldStates[0].Index)},
	}, newDir, oldStates, finalized)
	if err != nil {
		t.Fatal(err)
	}
	if next[0].Path != oldStates[0].Path || next[0].Completed != next[0].Length {
		t.Fatalf("finalized file identity changed: old=%#v new=%#v", oldStates[0], next[0])
	}
	if next[1].Path == oldStates[1].Path {
		t.Fatalf("unfinished file did not adopt new layout: old=%#v new=%#v", oldStates[1], next[1])
	}
	if next[1].Completed != oldStates[1].Completed {
		t.Fatalf("unfinished progress = %d, want %d", next[1].Completed, oldStates[1].Completed)
	}
}

func TestAddTorrentURLRejectsInvalidMetadataWithoutAddingDownload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("not a torrent"))
	}))
	defer server.Close()

	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	const gid = "2222222222222222"
	if _, err := engine.AddTorrent(server.URL+"/file.torrent", nil, Options{"gid": gid, "pause": "true"}, nil); err == nil {
		t.Fatal("AddTorrent accepted URL with invalid torrent metadata")
	}
	if _, err := engine.TellStatus(gid, []string{"status"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("TellStatus after rejected torrent = %v, want ErrNotFound", err)
	}
}

func TestAddTorrentURLEnforcesMaxTorrentSize(t *testing.T) {
	data, err := os.ReadFile("test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(data)
	}))
	defer server.Close()

	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	const gid = "3333333333333333"
	if _, err := engine.AddTorrent(server.URL+"/file.torrent", nil, Options{
		"gid":                     gid,
		"pause":                   "true",
		"goaria-max-torrent-size": "8",
	}, nil); err == nil {
		t.Fatal("AddTorrent accepted oversized torrent metadata response")
	}
	if _, err := engine.TellStatus(gid, []string{"status"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("TellStatus after oversized torrent = %v, want ErrNotFound", err)
	}
}

func TestAddTorrentURLRejectsPartialHTTPResponse(t *testing.T) {
	payload := []byte("partial torrent bytes")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(payload)-1, len(payload)+10))
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	const gid = "4444444444444444"
	_, err = engine.AddTorrent(server.URL+"/file.torrent", nil, Options{"gid": gid, "pause": "true"}, nil)
	if err == nil {
		t.Fatal("AddTorrent accepted partial HTTP torrent metadata response")
	}
	var statusErr *httpStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusPartialContent {
		t.Fatalf("AddTorrent error = %v, want 206 status error", err)
	}
	if _, err := engine.TellStatus(gid, []string{"status"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("TellStatus after partial torrent = %v, want ErrNotFound", err)
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

func TestMagnetAppliesExtensionSelectionAfterMetadataLoads(t *testing.T) {
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

	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	gid, err := engine.AddURI([]string{magnet}, Options{
		"bt-metadata-only":         "true",
		"goaria-select-file-ext":   ".missing",
		"goaria-disable-dht":       "true",
		"goaria-disable-trackers":  "true",
		"goaria-disable-utp":       "true",
		"goaria-peer-addrs":        peerAddrs,
		"max-concurrent-downloads": "1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)
	files, err := engine.GetFiles(gid)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if file.Selected != "false" {
			t.Fatalf("magnet file %s selected = %s, want false", file.Index, file.Selected)
		}
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
	status, err := engine.TellStatus(gid, []string{"completedLength"})
	if err != nil {
		t.Fatal(err)
	}
	if status["completedLength"] != "0" {
		t.Fatalf("metadata-only completedLength = %v, want 0", status["completedLength"])
	}
	status, err = engine.TellStatus(gid, []string{"bittorrent"})
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
		TorrentFileHandler: func(ctx context.Context, tf TorrentFile) error {
			files, err := engine.GetFiles(tf.GID)
			if err != nil {
				return err
			}
			ariaIndex := tf.Lease.Index + 1
			gotComplete := false
			for _, f := range files {
				if f.Index == strconv.Itoa(ariaIndex) {
					gotComplete = f.CompletedLength == f.Length
					break
				}
			}
			if !gotComplete {
				return fmt.Errorf("handler called before file %d was complete", ariaIndex)
			}
			got, err := io.ReadAll(tf.Lease.Reader)
			if err != nil {
				return err
			}
			seenMu.Lock()
			defer seenMu.Unlock()
			seenFiles = append(seenFiles, filepath.Base(tf.Lease.Path))
			seenBuf.WriteString(filepath.Base(tf.Lease.Path))
			seenBuf.WriteByte('=')
			seenBuf.Write(got)
			seenBuf.WriteByte('\n')
			return tf.Lease.Release(ctx)
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
	status, err := engine.TellStatus(gid, []string{"numPieces", "bitfield"})
	if err != nil {
		t.Fatal(err)
	}
	numPieces, err := strconv.Atoi(status["numPieces"].(string))
	if err != nil {
		t.Fatal(err)
	}
	wantDone, wantBitfield := torrentPieceProgress(torrent.PieceStateRuns{{
		PieceState: torrent.PieceState{Completion: storage.Completion{Ok: true, Complete: true}},
		Length:     numPieces,
	}})
	engine.mu.RLock()
	download := engine.downloads[gid]
	engine.mu.RUnlock()
	download.mu.RLock()
	gotDone := download.donePieces
	download.mu.RUnlock()
	if gotDone != wantDone || status["bitfield"] != wantBitfield {
		t.Fatalf("released torrent piece state = done %d bitfield %v, want %d %s", gotDone, status["bitfield"], wantDone, wantBitfield)
	}

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

func TestTorrentStreamFinalizedFilesSurvivePauseUnpause(t *testing.T) {
	_, encoded, peerAddrs, stopSeeder, _ := startLocalTorrentSeeder(t)
	defer stopSeeder()

	var countsMu sync.Mutex
	counts := make(map[int]int)
	started := make(chan int, 4)
	engine, err := NewEngine(Config{
		Dir: t.TempDir(),
		TorrentFileHandler: func(ctx context.Context, tf TorrentFile) error {
			countsMu.Lock()
			counts[tf.Lease.Index]++
			invocation := counts[tf.Lease.Index]
			countsMu.Unlock()
			if tf.Lease.Index == 0 {
				if err := tf.Lease.Release(ctx); err != nil {
					return err
				}
				started <- tf.Lease.Index
				return nil
			}
			started <- tf.Lease.Index
			if invocation == 1 {
				<-ctx.Done()
				if err := tf.Lease.Discard(context.Background()); err != nil {
					return err
				}
				return ctx.Err()
			}
			return tf.Lease.Release(ctx)
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
	if got := receiveTorrentFileIndex(t, started, "first finalized file"); got != 0 {
		t.Fatalf("first handled file index = %d, want 0", got)
	}
	if got := receiveTorrentFileIndex(t, started, "second file before pause"); got != 1 {
		t.Fatalf("second handled file index = %d, want 1", got)
	}
	if _, err := engine.Pause(gid); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Unpause(gid); err != nil {
		t.Fatal(err)
	}
	if got := receiveTorrentFileIndex(t, started, "second file after unpause"); got != 1 {
		t.Fatalf("resumed handled file index = %d, want 1", got)
	}
	waitForStatus(t, engine, gid, StatusComplete)

	countsMu.Lock()
	firstCount, secondCount := counts[0], counts[1]
	countsMu.Unlock()
	if firstCount != 1 || secondCount != 2 {
		t.Fatalf("handler calls by zero-based index = %#v, want map[0:1 1:2]", counts)
	}
	files, err := engine.GetFiles(gid)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if file.CompletedLength != file.Length {
			t.Fatalf("final file progress = %#v, want completed", file)
		}
	}
}

func TestTorrentStreamFinalizedFilesSurviveSessionRoundTrip(t *testing.T) {
	_, encoded, peerAddrs, stopSeeder, _ := startLocalTorrentSeeder(t)
	defer stopSeeder()

	dir := t.TempDir()
	sessionPath := filepath.Join(dir, "goaria.session")
	started := make(chan int, 2)
	engine, err := NewEngine(Config{
		Dir:         dir,
		SaveSession: sessionPath,
		TorrentFileHandler: func(ctx context.Context, tf TorrentFile) error {
			if tf.Lease.Index == 0 {
				if err := tf.Lease.Release(ctx); err != nil {
					return err
				}
				started <- tf.Lease.Index
				return nil
			}
			started <- tf.Lease.Index
			<-ctx.Done()
			if err := tf.Lease.Discard(context.Background()); err != nil {
				return err
			}
			return ctx.Err()
		},
	})
	if err != nil {
		t.Fatal(err)
	}

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
	if got := receiveTorrentFileIndex(t, started, "first finalized file"); got != 0 {
		t.Fatalf("first handled file index = %d, want 0", got)
	}
	if got := receiveTorrentFileIndex(t, started, "second file before save"); got != 1 {
		t.Fatalf("second handled file index = %d, want 1", got)
	}
	if _, err := engine.Pause(gid); err != nil {
		t.Fatal(err)
	}
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
	if !strings.Contains(string(sessionData), "  "+sessionTorrentFinalizedOption+"=1\n") {
		t.Fatalf("session omitted finalized torrent file state:\n%s", sessionData)
	}

	restoredIndexes := make(chan int, 2)
	restored, err := NewEngine(Config{
		Dir:       dir,
		InputFile: sessionPath,
		TorrentFileHandler: func(ctx context.Context, tf TorrentFile) error {
			restoredIndexes <- tf.Lease.Index
			return tf.Lease.Release(ctx)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close(context.Background())

	files, err := restored.GetFiles(gid)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files[0].CompletedLength != files[0].Length || files[1].CompletedLength == files[1].Length {
		t.Fatalf("restored per-file progress = %#v, want only first file finalized", files)
	}
	status, err := restored.TellStatus(gid, []string{"completedLength"})
	if err != nil {
		t.Fatal(err)
	}
	if status["completedLength"] != files[0].Length {
		t.Fatalf("restored completedLength = %v, want %s", status["completedLength"], files[0].Length)
	}
	opts, err := restored.GetOption(gid)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := opts[sessionTorrentFinalizedOption]; ok {
		t.Fatalf("private session state leaked through GetOption: %#v", opts)
	}
	if _, err := restored.Unpause(gid); err != nil {
		t.Fatal(err)
	}
	if got := receiveTorrentFileIndex(t, restoredIndexes, "restored unfinished file"); got != 1 {
		t.Fatalf("restored handler received file index %d, want 1", got)
	}
	waitForStatus(t, restored, gid, StatusComplete)
	select {
	case index := <-restoredIndexes:
		t.Fatalf("restored handler received extra file index %d", index)
	case <-time.After(100 * time.Millisecond):
	}
}

func receiveTorrentFileIndex(t *testing.T, indexes <-chan int, label string) int {
	t.Helper()
	select {
	case index := <-indexes:
		return index
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
		return -1
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
		TorrentFileHandler: func(ctx context.Context, tf TorrentFile) error {
			name := filepath.Base(tf.Lease.Path)
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
			if _, err := io.ReadAll(tf.Lease.Reader); err != nil {
				return err
			}
			if !isFirst {
				return tf.Lease.Release(ctx)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-releaseFirst:
				return tf.Lease.Release(ctx)
			}
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
		"goaria-torrent-max-active-files": "2",
		"goaria-peer-addrs":               peerAddrs,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("first stream handler did not start")
	}
	select {
	case <-secondStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("second stream handler did not start while first handler was still running")
	}
	close(releaseFirst)
	waitForStatus(t, engine, gid, StatusComplete)
}

func TestConcurrentTorrentsDoNotShareSelectedOutputPaths(t *testing.T) {
	_, encoded, peerAddrs, stopSeeder, _ := startLocalTorrentSeeder(t)
	defer stopSeeder()

	handlerStarted := make(chan string, 4)
	releaseHandlers := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseHandlers) }) }
	defer release()

	engine, err := NewEngine(Config{
		Dir:                    t.TempDir(),
		MaxConcurrentDownloads: 2,
		TorrentFileHandler: func(ctx context.Context, tf TorrentFile) error {
			select {
			case handlerStarted <- tf.GID:
			case <-ctx.Done():
				return ctx.Err()
			}
			select {
			case <-releaseHandlers:
				return tf.Lease.Release(ctx)
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	opts := Options{
		"goaria-disable-dht":      "true",
		"goaria-disable-trackers": "true",
		"goaria-disable-utp":      "true",
		"goaria-peer-addrs":       peerAddrs,
	}
	first, err := engine.AddTorrent(encoded, nil, opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case startedGID := <-handlerStarted:
		if startedGID != first {
			t.Fatalf("first handler gid = %s, want %s", startedGID, first)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("first torrent handler did not start")
	}

	second, err := engine.AddTorrent(encoded, nil, opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, second, StatusError)
	status, err := engine.TellStatus(second, []string{"errorMessage"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status["errorMessage"].(string), "output path is already in use") {
		t.Fatalf("second torrent error = %q", status["errorMessage"])
	}
	for {
		select {
		case startedGID := <-handlerStarted:
			if startedGID == second {
				t.Fatal("conflicting torrent reached the file handler")
			}
		default:
			goto handlersChecked
		}
	}

handlersChecked:
	release()
	waitForStatus(t, engine, first, StatusComplete)
}

func TestTorrentStreamLeaseWaitsForExplicitReleaseAndBoundsActiveFiles(t *testing.T) {
	_, encoded, peerAddrs, stopSeeder, _ := startLocalTorrentSeeder(t)
	defer stopSeeder()

	leases := make(chan TorrentFile, 2)
	engine, err := NewEngine(Config{
		Dir: t.TempDir(),
		TorrentFileHandler: func(ctx context.Context, tf TorrentFile) error {
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
		t.Fatalf("second lease %s started before first lease was released", second.Lease.Path)
	case <-time.After(250 * time.Millisecond):
	}
	time.Sleep(1100 * time.Millisecond)
	if err := first.Lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if pathExists(first.Lease.Path) || pathExists(first.Lease.Path+".part") {
		t.Fatalf("released file storage still exists: %q", first.Lease.Path)
	}

	second := receiveLease(t, leases, "second")
	if err := second.Lease.Release(context.Background()); err != nil {
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
		TorrentFileHandler: func(ctx context.Context, tf TorrentFile) error {
			if _, err := io.ReadAll(tf.Lease.Reader); err != nil {
				return err
			}
			streamedPaths <- tf.Lease.Path
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
		TorrentFileHandler: func(ctx context.Context, tf TorrentFile) error {
			if _, err := io.ReadAll(tf.Lease.Reader); err != nil {
				return err
			}
			streamedPaths <- tf.Lease.Path
			if err := tf.Lease.Discard(ctx); err != nil {
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

func receiveLease(t *testing.T, leases <-chan TorrentFile, label string) TorrentFile {
	t.Helper()
	select {
	case lease := <-leases:
		return lease
	case <-time.After(3 * time.Second):
		t.Fatalf("%s lease did not start", label)
		return TorrentFile{}
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

func v2OnlyTestMetaInfo(t *testing.T) (*metainfo.MetaInfo, metainfo.Info, []byte) {
	t.Helper()
	info := metainfo.Info{
		MetaVersion: 2,
		PieceLength: 16 << 10,
		Name:        "v2-only",
		FileTree: metainfo.FileTree{Dir: map[string]metainfo.FileTree{
			"payload.bin": {File: metainfo.FileTreeFile{
				Length:     1,
				PiecesRoot: string(bytes.Repeat([]byte{0x42}, 32)),
			}},
		}},
	}
	infoBytes, err := bencode.Marshal(&info)
	if err != nil {
		t.Fatal(err)
	}
	mi := &metainfo.MetaInfo{InfoBytes: infoBytes}
	var encoded bytes.Buffer
	if err := mi.Write(&encoded); err != nil {
		t.Fatal(err)
	}
	return mi, info, encoded.Bytes()
}

func TestTorrentRuntimeReleasedAfterCompletion(t *testing.T) {
	data, err := os.ReadFile("test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	mi, info, err := torrentMetaInfo(data)
	if err != nil {
		t.Fatal(err)
	}
	payloadDir := t.TempDir()
	writeTestTorrentPayload(t, payloadDir, info)
	peerAddrs, stopSeeder := startSeederForMetaInfo(t, payloadDir, mi)
	defer stopSeeder()

	engine, err := NewEngine(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	gid, err := engine.AddTorrent(base64.StdEncoding.EncodeToString(data), nil, Options{
		"goaria-disable-dht":      "true",
		"goaria-disable-trackers": "true",
		"goaria-disable-utp":      "true",
		"goaria-peer-addrs":       peerAddrs,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, StatusComplete)

	engine.mu.RLock()
	d := engine.downloads[gid]
	engine.mu.RUnlock()
	d.mu.RLock()
	runtimeReleased := d.torrent == nil
	d.mu.RUnlock()
	if !runtimeReleased {
		t.Fatal("torrent runtime retained after completion")
	}

	d.mu.Lock()
	metaInfo, err := d.metadataInfoLocked()
	d.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if metaInfo == nil {
		t.Fatal("metadata unavailable after runtime release")
	}
}
