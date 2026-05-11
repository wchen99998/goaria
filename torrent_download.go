package goaria

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	g "github.com/anacrolix/generics"
	"github.com/wchen99998/torrent"
	"github.com/wchen99998/torrent/bencode"
	"github.com/wchen99998/torrent/metainfo"
	"github.com/wchen99998/torrent/storage"
	"golang.org/x/time/rate"
)

type torrentRuntime struct {
	client  *torrent.Client
	torrent *torrent.Torrent
}

type torrentFileState struct {
	Index     int
	Path      string
	Length    int64
	Completed int64
	Released  bool
	Selected  bool
	URIs      []URIInfo
}

const (
	torrentPeerProfileQBitTorrent = "qbittorrent"
	torrentPeerProfileNative      = "native"

	qBittorrentVersion            = "5.2.0"
	qBittorrentBep20Prefix        = "-qB5200-"
	qBittorrentPeerVisibleVersion = "qBittorrent/" + qBittorrentVersion
)

func isMagnetURI(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && strings.EqualFold(u.Scheme, "magnet")
}

func isHTTPTorrentURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Host != "" && (u.Scheme == "http" || u.Scheme == "https")
}

func (e *Engine) addTorrent(torrentData string, uris []string, opts Options, position *int) (string, error) {
	for _, raw := range uris {
		if err := validateHTTPURI(raw); err != nil {
			return "", err
		}
	}
	if opts == nil {
		opts = Options{}
	}

	e.mu.RLock()
	if err := e.ctx.Err(); err != nil {
		e.mu.RUnlock()
		return "", ErrShutdown
	}
	merged := layerOptions(cloneOptions(e.global), opts)
	e.mu.RUnlock()

	data, mi, info, sourceURL, err := e.resolveTorrentSource(torrentData, merged)
	if err != nil {
		return "", err
	}
	if sourceURL != "" {
		merged["goaria-torrent-source-url"] = sourceURL
	}
	if _, _, err := torrentSelectionAndIndexOut(info, merged); err != nil {
		return "", err
	}

	e.mu.Lock()
	if err := e.ctx.Err(); err != nil {
		e.mu.Unlock()
		return "", ErrShutdown
	}
	gid := optionString(merged, "gid")
	if gid == "" {
		gid = randomHex(8)
	} else if !validGID(gid) {
		e.mu.Unlock()
		return "", ErrInvalidGID
	}
	if _, exists := e.downloads[gid]; exists {
		e.mu.Unlock()
		return "", fmt.Errorf("gid already exists")
	}
	d := newTorrentDownload(gid, data, "", uris, merged, mi, info)
	if optionBool(merged, "pause") {
		d.status = StatusPaused
	} else {
		e.insertWaitingLocked(gid, position)
	}
	e.downloads[gid] = d
	e.mu.Unlock()
	e.signal()
	e.saveSessionBestEffort()
	return gid, nil
}

func (e *Engine) resolveTorrentSource(raw string, opts Options) ([]byte, *metainfo.MetaInfo, metainfo.Info, string, error) {
	data, decodeErr := decodeTorrentParam(raw)
	if decodeErr == nil {
		mi, info, metaErr := torrentMetaInfo(data)
		if metaErr == nil {
			return data, mi, info, "", nil
		}
		if !isHTTPTorrentURL(raw) {
			return nil, nil, metainfo.Info{}, "", metaErr
		}
	} else if !isHTTPTorrentURL(raw) {
		return nil, nil, metainfo.Info{}, "", decodeErr
	}

	data, err := e.fetchTorrentURL(e.ctx, raw, opts)
	if err != nil {
		return nil, nil, metainfo.Info{}, "", err
	}
	mi, info, err := torrentMetaInfo(data)
	if err != nil {
		return nil, nil, metainfo.Info{}, "", err
	}
	return data, mi, info, raw, nil
}

const defaultMaxTorrentSize = 16 << 20

type torrentResponseSizeError struct {
	max int64
}

func (e *torrentResponseSizeError) Error() string {
	return fmt.Sprintf("torrent response exceeds goaria-max-torrent-size %d", e.max)
}

func (e *Engine) fetchTorrentURL(ctx context.Context, raw string, opts Options) ([]byte, error) {
	if err := validateHTTPURI(raw); err != nil {
		return nil, err
	}
	maxSize := optionBytes(opts, "goaria-max-torrent-size", defaultMaxTorrentSize)
	if maxSize <= 0 {
		maxSize = defaultMaxTorrentSize
	}
	maxTries := optionInt(opts, "max-tries", 5)
	if maxTries < 0 {
		maxTries = 1
	}
	retryWait := time.Duration(optionInt(opts, "retry-wait", 0)) * time.Second
	var lastErr error
	for attempt := 0; maxTries == 0 || attempt < maxTries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, err := e.fetchTorrentURLOnce(ctx, raw, opts, maxSize)
		if err == nil {
			return data, nil
		}
		lastErr = err
		var sizeErr *torrentResponseSizeError
		if errors.As(err, &sizeErr) {
			break
		}
		if !isRetryableDownloadError(err) {
			break
		}
		if retryWait > 0 {
			if err := sleepContext(ctx, retryWait); err != nil {
				return nil, err
			}
		}
	}
	return nil, lastErr
}

func (e *Engine) fetchTorrentURLOnce(ctx context.Context, raw string, opts Options, maxSize int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return nil, err
	}
	applyRequestOptions(req, opts)
	resp, err := e.do(req, opts)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, &httpStatusError{Method: http.MethodGet, URL: raw, StatusCode: resp.StatusCode, Status: resp.Status}
	}
	if resp.ContentLength > maxSize {
		return nil, &torrentResponseSizeError{max: maxSize}
	}
	body, closeBody, err := responseReader(resp, opts)
	if err != nil {
		return nil, err
	}
	defer closeBody()
	data, err := io.ReadAll(io.LimitReader(body, maxSize+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxSize {
		return nil, &torrentResponseSizeError{max: maxSize}
	}
	return data, nil
}

func (e *Engine) addMagnet(raw string, opts Options, position *int) (string, error) {
	if _, err := torrent.TorrentSpecFromMagnetUri(raw); err != nil {
		return "", err
	}
	if opts == nil {
		opts = Options{}
	}
	e.mu.Lock()
	if err := e.ctx.Err(); err != nil {
		e.mu.Unlock()
		return "", ErrShutdown
	}
	merged := layerOptions(e.global, opts)
	gid := optionString(merged, "gid")
	if gid == "" {
		gid = randomHex(8)
	} else if !validGID(gid) {
		e.mu.Unlock()
		return "", ErrInvalidGID
	}
	if _, exists := e.downloads[gid]; exists {
		e.mu.Unlock()
		return "", fmt.Errorf("gid already exists")
	}
	d, err := newMagnetDownload(gid, raw, merged)
	if err != nil {
		e.mu.Unlock()
		return "", err
	}
	if optionBool(merged, "pause") {
		d.status = StatusPaused
	} else {
		e.insertWaitingLocked(gid, position)
	}
	e.downloads[gid] = d
	e.mu.Unlock()
	e.signal()
	e.saveSessionBestEffort()
	return gid, nil
}

func torrentMetaInfo(data []byte) (*metainfo.MetaInfo, metainfo.Info, error) {
	mi, err := metainfo.Load(bytes.NewReader(data))
	if err != nil {
		return nil, metainfo.Info{}, fmt.Errorf("invalid torrent metainfo: %w", err)
	}
	info, err := mi.UnmarshalInfo()
	if err != nil {
		return nil, metainfo.Info{}, fmt.Errorf("invalid torrent info: %w", err)
	}
	return mi, info, nil
}

func decodeTorrentParam(s string) ([]byte, error) {
	data, err := base64.StdEncoding.DecodeString(s)
	if err == nil {
		return data, nil
	}
	data, rawErr := base64.RawStdEncoding.DecodeString(s)
	if rawErr == nil {
		return data, nil
	}
	return nil, fmt.Errorf("invalid base64 torrent: %w", err)
}

func newMagnetDownload(gid, raw string, opts Options) (*Download, error) {
	spec, err := torrent.TorrentSpecFromMagnetUri(raw)
	if err != nil {
		return nil, err
	}
	d := newTorrentDownload(gid, nil, raw, nil, opts, nil, metainfo.Info{
		Name: spec.DisplayName,
	})
	d.infoHash = spec.InfoHash.HexString()
	return d, nil
}

func newTorrentDownload(gid string, data []byte, magnet string, uris []string, opts Options, mi *metainfo.MetaInfo, info metainfo.Info) *Download {
	infos := make([]URIInfo, 0, len(uris))
	for i, raw := range uris {
		status := URIStatusWaiting
		if i == 0 {
			status = URIStatusUsed
		}
		infos = append(infos, URIInfo{URI: raw, Status: status})
	}
	dir := optionString(opts, "dir")
	if dir == "" {
		dir = "."
	}
	files := torrentFileStates(info, opts, dir, infos)
	total := torrentSelectedLength(files)
	d := &Download{
		kind:          downloadKindTorrent,
		gid:           gid,
		uris:          infos,
		options:       opts,
		status:        StatusWaiting,
		dir:           dir,
		totalLength:   total,
		pieceLength:   info.PieceLength,
		numPieces:     int64(info.NumPieces()),
		torrentFiles:  files,
		torrentData:   append([]byte(nil), data...),
		torrentMagnet: magnet,
		bittorrent:    bittorrentInfo(mi, info),
		createdAt:     time.Now(),
	}
	if mi != nil {
		d.infoHash = mi.HashInfoBytes().HexString()
	}
	d.bitfield = bitfieldFor(d.totalLength, 0, d.pieceLength)
	return d
}

func bittorrentInfo(mi *metainfo.MetaInfo, info metainfo.Info) *BittorrentInfo {
	mode := "single"
	if len(info.Files) > 0 || info.HasV2() && info.IsDir() {
		mode = "multi"
	}
	bt := &BittorrentInfo{
		Mode: mode,
		Info: map[string]interface{}{
			"name": info.BestName(),
		},
	}
	if mi != nil {
		bt.AnnounceList = mi.UpvertedAnnounceList()
		bt.Comment = mi.Comment
		bt.CreationDate = mi.CreationDate
	}
	return bt
}

func torrentFileStates(info metainfo.Info, opts Options, dir string, uris []URIInfo) []torrentFileState {
	files := info.UpvertedFiles()
	selected := selectedFileSet(len(files), optionString(opts, "select-file"))
	indexOut, _ := parseIndexOut(opts)
	out := make([]torrentFileState, 0, len(files))
	for i, fi := range files {
		index := i + 1
		path := filepath.Join(dir, safeTorrentRelPath(torrentDisplayPath(info, fi)))
		if override := indexOut[index]; override != "" {
			path = resolveOutputPath(dir, override, "", "")
		}
		out = append(out, torrentFileState{
			Index:    index,
			Path:     path,
			Length:   fi.Length,
			Selected: selected[index],
			URIs:     cloneURIs(uris),
		})
	}
	return out
}

func torrentDisplayPath(info metainfo.Info, fi metainfo.FileInfo) string {
	if info.IsDir() {
		parts := append([]string{info.BestName()}, fi.BestPath()...)
		return strings.Join(parts, "/")
	}
	return info.BestName()
}

func safeTorrentRelPath(path string) string {
	parts := strings.FieldsFunc(filepath.ToSlash(path), func(r rune) bool {
		return r == '/'
	})
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			continue
		}
		clean = append(clean, part)
	}
	if len(clean) == 0 {
		return "torrent"
	}
	return filepath.Join(clean...)
}

func torrentSelectedLength(files []torrentFileState) int64 {
	var total int64
	for _, f := range files {
		if f.Selected {
			total += f.Length
		}
	}
	return total
}

func torrentSelectionAndIndexOut(info metainfo.Info, opts Options) (map[int]bool, map[int]string, error) {
	files := info.UpvertedFiles()
	selected := selectedFileSet(len(files), optionString(opts, "select-file"))
	indexOut, err := parseIndexOut(opts)
	if err != nil {
		return nil, nil, err
	}
	for index := range indexOut {
		if index < 1 || index > len(files) {
			return nil, nil, fmt.Errorf("index-out file index %d out of range", index)
		}
	}
	for index := range selected {
		if index < 1 || index > len(files) {
			return nil, nil, fmt.Errorf("select-file index %d out of range", index)
		}
	}
	return selected, indexOut, nil
}

func selectedFileSet(numFiles int, spec string) map[int]bool {
	selected := make(map[int]bool, numFiles)
	if strings.TrimSpace(spec) == "" || numFiles <= 1 {
		for i := 1; i <= numFiles; i++ {
			selected[i] = true
		}
		return selected
	}
	for _, item := range strings.Split(spec, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		first, last, ok := strings.Cut(item, "-")
		if !ok {
			n, err := strconv.Atoi(strings.TrimSpace(item))
			if err == nil {
				selected[n] = true
			}
			continue
		}
		start, err1 := strconv.Atoi(strings.TrimSpace(first))
		end, err2 := strconv.Atoi(strings.TrimSpace(last))
		if err1 != nil || err2 != nil || start > end {
			continue
		}
		for i := start; i <= end; i++ {
			selected[i] = true
		}
	}
	return selected
}

func parseIndexOut(opts Options) (map[int]string, error) {
	out := make(map[int]string)
	for _, raw := range optionStringList(opts, "index-out") {
		left, right, ok := strings.Cut(raw, "=")
		if !ok {
			return nil, fmt.Errorf("invalid index-out %q", raw)
		}
		index, err := strconv.Atoi(strings.TrimSpace(left))
		if err != nil || index <= 0 {
			return nil, fmt.Errorf("invalid index-out index %q", left)
		}
		path := strings.TrimSpace(right)
		if path == "" {
			return nil, fmt.Errorf("invalid empty index-out path")
		}
		out[index] = path
	}
	return out, nil
}

func applyTorrentClientIdentityOptions(cfg *torrent.ClientConfig, opts Options) error {
	profile := strings.ToLower(strings.TrimSpace(optionString(opts, "goaria-torrent-peer-profile")))
	if profile == "" {
		profile = torrentPeerProfileQBitTorrent
	}
	switch profile {
	case torrentPeerProfileQBitTorrent, "qbt", "qbit":
		cfg.Bep20 = qBittorrentBep20Prefix
		cfg.HTTPUserAgent = qBittorrentPeerVisibleVersion
		cfg.ExtendedHandshakeClientVersion = qBittorrentPeerVisibleVersion
	case torrentPeerProfileNative, "anacrolix", "goaria":
		if userAgent := optionString(opts, "user-agent"); userAgent != "" {
			cfg.HTTPUserAgent = userAgent
		}
	default:
		return fmt.Errorf("invalid goaria-torrent-peer-profile %q", profile)
	}
	if prefix := optionString(opts, "goaria-torrent-bep20-prefix"); prefix != "" {
		if len(prefix) > 20 {
			return fmt.Errorf("goaria-torrent-bep20-prefix too long: %d bytes", len(prefix))
		}
		cfg.Bep20 = prefix
	}
	if userAgent := optionString(opts, "goaria-torrent-user-agent"); userAgent != "" {
		cfg.HTTPUserAgent = userAgent
	}
	if version := optionString(opts, "goaria-torrent-client-version"); version != "" {
		cfg.ExtendedHandshakeClientVersion = version
	}
	return nil
}

func torrentInfoPrivate(info metainfo.Info) bool {
	return info.Private != nil && *info.Private
}

func applyTorrentPeerDiscoveryOptions(cfg *torrent.ClientConfig, opts Options, privateTorrent bool) {
	cfg.NoDHT = optionBool(opts, "goaria-disable-dht")
	cfg.DisableTrackers = optionBool(opts, "goaria-disable-trackers")
	cfg.DisablePEX = optionBool(opts, "goaria-disable-pex")
	if !privateTorrent {
		return
	}
	if !optionExplicit(opts, "goaria-disable-dht") {
		cfg.NoDHT = true
	}
	if !optionExplicit(opts, "goaria-disable-pex") {
		cfg.DisablePEX = true
	}
}

func (e *Engine) runTorrentDownload(d *Download) error {
	d.mu.RLock()
	ctx := d.ctx
	opts := d.options
	data := append([]byte(nil), d.torrentData...)
	magnet := d.torrentMagnet
	webseeds := cloneURIs(d.uris)
	dir := d.dir
	d.mu.RUnlock()
	if ctx == nil {
		return context.Canceled
	}
	var mi *metainfo.MetaInfo
	privateTorrent := false
	if magnet == "" {
		var err error
		mi, err = metainfo.Load(bytes.NewReader(data))
		if err != nil {
			return err
		}
		info, err := mi.UnmarshalInfo()
		if err != nil {
			return err
		}
		privateTorrent = torrentInfoPrivate(info)
	}
	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = dir
	cfg.NoDefaultPortForwarding = true
	if err := applyTorrentClientIdentityOptions(cfg, opts); err != nil {
		return err
	}
	cfg.DefaultStorage = storage.NewFileOpts(storage.NewFileClientOpts{
		ClientBaseDir:      dir,
		PieceCompletion:    storage.NewMapPieceCompletion(),
		UsePartFiles:       g.Some(false),
		ForceClassicFileIO: true,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		FilePathMaker:      torrentFilePathMaker(opts),
	})
	if optionBool(opts, "goaria-torrent-no-upload") {
		cfg.NoUpload = true
	}
	cfg.Slogger = slog.New(slog.NewTextHandler(io.Discard, nil))
	applyTorrentPeerDiscoveryOptions(cfg, opts, privateTorrent)
	cfg.DisableUTP = optionBool(opts, "disable-utp") || optionBool(opts, "goaria-disable-utp")
	cfg.DisableIPv6 = optionBool(opts, "disable-ipv6")
	if maxPeers := optionInt(opts, "bt-max-peers", 0); maxPeers > 0 {
		cfg.EstablishedConnsPerTorrent = maxPeers
		cfg.TorrentPeersHighWater = maxPeers
		cfg.TorrentPeersLowWater = maxPeers / 2
	}
	if uploadLimit := optionBytes(opts, "max-upload-limit", 0); uploadLimit > 0 {
		cfg.UploadRateLimiter = rate.NewLimiter(rate.Limit(uploadLimit), int(uploadLimit))
	}
	cfg.ListenPort = optionInt(opts, "listen-port", 0)
	if host := optionString(opts, "goaria-listen-host"); host != "" {
		cfg.ListenHost = func(string) string { return host }
	}
	cl, err := torrent.NewClient(cfg)
	if err != nil {
		return err
	}
	defer cl.Close()

	var tor *torrent.Torrent
	if magnet != "" {
		spec, err := torrent.TorrentSpecFromMagnetUri(magnet)
		if err != nil {
			return err
		}
		applyTorrentTrackerOptions(spec, opts)
		tor, _, err = cl.AddTorrentSpec(spec)
	} else {
		spec, err := torrent.TorrentSpecFromMetaInfoErr(mi)
		if err != nil {
			return err
		}
		for _, u := range webseeds {
			spec.Webseeds = append(spec.Webseeds, u.URI)
		}
		applyTorrentTrackerOptions(spec, opts)
		tor, _, err = cl.AddTorrentSpec(spec)
	}
	if err != nil {
		return err
	}
	if peers := optionStringList(opts, "goaria-peer-addrs"); len(peers) > 0 {
		spec := torrent.TorrentSpec{PeerAddrs: peers}
		if err := tor.MergeSpec(&spec); err != nil {
			return err
		}
	}
	d.mu.Lock()
	d.torrent = &torrentRuntime{client: cl, torrent: tor}
	d.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-tor.GotInfo():
	}
	if err := e.refreshTorrentMetadata(d, tor, opts, dir, webseeds); err != nil {
		return err
	}
	if optionBool(opts, "bt-metadata-only") {
		e.updateTorrentProgress(d, tor)
		e.notify("aria2.onBtDownloadComplete", d.gid)
		return nil
	}

	stopProgress := e.startTorrentProgress(ctx, d, tor)
	defer stopProgress()

	files := tor.Files()
	selected := selectedTorrentFiles(d, files)
	if e.cfg.TorrentFileHandler != nil {
		if err := e.processCompletedTorrentFiles(ctx, d, selected); err != nil {
			return err
		}
		e.notify("aria2.onBtDownloadComplete", d.gid)
		return nil
	}
	for _, item := range selected {
		item.file.Download()
	}
	if len(selected) == 0 {
		e.notify("aria2.onBtDownloadComplete", d.gid)
		return nil
	}
	for _, item := range selected {
		if err := item.file.WaitComplete(ctx); err != nil {
			item.file.SetPriority(torrent.PiecePriorityNone)
			return err
		}
		e.updateTorrentProgress(d, tor)
	}
	e.notify("aria2.onBtDownloadComplete", d.gid)
	return nil
}

func applyTorrentTrackerOptions(spec *torrent.TorrentSpec, opts Options) {
	if exclude := splitCSVOptions(opts, "bt-exclude-tracker"); len(exclude) > 0 {
		excluded := make(map[string]struct{}, len(exclude))
		removeAll := false
		for _, tracker := range exclude {
			if tracker == "*" {
				removeAll = true
				break
			}
			excluded[tracker] = struct{}{}
		}
		filtered := spec.Trackers[:0]
		if !removeAll {
			for _, tier := range spec.Trackers {
				outTier := tier[:0]
				for _, tracker := range tier {
					if _, ok := excluded[tracker]; !ok {
						outTier = append(outTier, tracker)
					}
				}
				if len(outTier) > 0 {
					filtered = append(filtered, outTier)
				}
			}
		}
		spec.Trackers = filtered
	}
	if trackers := splitCSVOptions(opts, "bt-tracker"); len(trackers) > 0 {
		spec.Trackers = append(spec.Trackers, trackers)
	}
}

func splitCSVOptions(opts Options, key string) []string {
	var out []string
	for _, raw := range optionStringList(opts, key) {
		for _, item := range strings.Split(raw, ",") {
			item = strings.TrimSpace(item)
			if item != "" {
				out = append(out, item)
			}
		}
	}
	return out
}

func torrentFilePathMaker(opts Options) storage.FilePathMaker {
	indexOut, _ := parseIndexOut(opts)
	if len(indexOut) == 0 {
		return nil
	}
	return func(pathOpts storage.FilePathMakerOpts) (string, error) {
		if override := indexOut[pathOpts.FileIndex+1]; override != "" {
			return safeTorrentRelPath(override), nil
		}
		if pathOpts.DefaultPath != "" {
			return pathOpts.DefaultPath, nil
		}
		var parts []string
		if pathOpts.Info.BestName() != metainfo.NoName {
			parts = append(parts, pathOpts.Info.BestName())
		}
		return filepath.Join(append(parts, pathOpts.File.BestPath()...)...), nil
	}
}

func (e *Engine) refreshTorrentMetadata(d *Download, tor *torrent.Torrent, opts Options, dir string, uris []URIInfo) error {
	mi := tor.Metainfo()
	info, err := mi.UnmarshalInfo()
	if err != nil {
		return err
	}
	if _, _, err := torrentSelectionAndIndexOut(info, opts); err != nil {
		return err
	}
	d.mu.Lock()
	d.infoHash = tor.InfoHash().HexString()
	d.pieceLength = info.PieceLength
	d.numPieces = int64(info.NumPieces())
	d.bittorrent = bittorrentInfo(&mi, info)
	d.torrentFiles = torrentFileStates(info, opts, dir, uris)
	d.totalLength = torrentSelectedLength(d.torrentFiles)
	d.bitfield = bitfieldFor(d.totalLength, d.completedLen, d.pieceLength)
	d.mu.Unlock()
	return nil
}

type selectedTorrentFile struct {
	index int
	state torrentFileState
	file  *torrent.File
}

func selectedTorrentFiles(d *Download, files []*torrent.File) []selectedTorrentFile {
	d.mu.RLock()
	states := append([]torrentFileState(nil), d.torrentFiles...)
	d.mu.RUnlock()
	out := make([]selectedTorrentFile, 0, len(files))
	for i, f := range files {
		if i >= len(states) || !states[i].Selected {
			continue
		}
		out = append(out, selectedTorrentFile{index: i, state: states[i], file: f})
	}
	return out
}

func (e *Engine) processCompletedTorrentFiles(ctx context.Context, d *Download, files []selectedTorrentFile) error {
	if len(files) == 0 {
		return nil
	}
	maxActive := optionInt(d.options, "goaria-torrent-max-active-files", len(files))
	if maxActive <= 0 || maxActive > len(files) {
		maxActive = len(files)
	}
	slots := make(chan struct{}, maxActive)
	results := make(chan error, len(files))
	var resultErr error
	var wg sync.WaitGroup
queue:
	for _, item := range files {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			resultErr = errors.Join(resultErr, ctx.Err())
			break queue
		}
		wg.Add(1)
		go func(item selectedTorrentFile) {
			defer wg.Done()
			defer func() { <-slots }()
			item.file.Download()
			if err := waitTorrentFileVerifiedComplete(ctx, item.file); err != nil {
				item.file.SetPriority(torrent.PiecePriorityNone)
				results <- err
				return
			}
			e.updateTorrentProgress(d, item.file.Torrent())
			results <- e.handleCompletedTorrentFile(ctx, d, item)
		}(item)
	}
	wg.Wait()
	close(results)
	for err := range results {
		resultErr = errors.Join(resultErr, err)
	}
	return resultErr
}

func (e *Engine) handleCompletedTorrentFile(ctx context.Context, d *Download, item selectedTorrentFile) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	reader := item.file.NewReader()
	reader.SetContext(ctx)
	reader.SetReadahead(optionBytes(d.options, "goaria-torrent-readahead", 4<<20))
	fileReader := newLimitedReadCloser(reader, item.file.Length())
	done := make(chan error, 1)
	var once sync.Once
	var finalizeErr error
	finalize := func(discard bool) error {
		once.Do(func() {
			finalizeErr = fileReader.Close()
			if discard {
				finalizeErr = errors.Join(finalizeErr, item.file.DiscardStorage())
			} else {
				finalizeErr = errors.Join(finalizeErr, item.file.ReleaseStorage())
			}
			d.mu.Lock()
			if item.index >= 0 && item.index < len(d.torrentFiles) {
				if discard {
					d.torrentFiles[item.index].Released = false
				} else if finalizeErr == nil {
					d.torrentFiles[item.index].Released = true
					d.torrentFiles[item.index].Completed = d.torrentFiles[item.index].Length
				}
			}
			d.mu.Unlock()
			if removeErr := os.Remove(item.state.Path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				finalizeErr = errors.Join(finalizeErr, removeErr)
			}
			item.file.SetPriority(torrent.PiecePriorityNone)
			e.updateTorrentProgress(d, item.file.Torrent())
			done <- finalizeErr
			close(done)
		})
		return finalizeErr
	}
	tf := TorrentFileLease{
		GID:     d.gid,
		Index:   item.state.Index,
		Path:    item.state.Path,
		Length:  item.state.Length,
		Reader:  fileReader,
		Release: func(context.Context) error { return finalize(false) },
		Discard: func(context.Context) error { return finalize(true) },
	}
	handlerErr := e.cfg.TorrentFileHandler(ctx, tf)
	if handlerErr != nil {
		releaseErr := finalize(false)
		return errors.Join(handlerErr, releaseErr)
	}
	select {
	case releaseErr := <-done:
		return releaseErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

type limitedReadCloser struct {
	reader io.Reader
	closer io.Closer
	once   sync.Once
	err    error
}

func newLimitedReadCloser(r io.ReadCloser, n int64) *limitedReadCloser {
	return &limitedReadCloser{
		reader: io.LimitReader(r, n),
		closer: r,
	}
}

func waitTorrentFileVerifiedComplete(ctx context.Context, file *torrent.File) error {
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if torrentFileVerifiedComplete(file) {
			return nil
		}
		if err := file.WaitComplete(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func torrentFileVerifiedComplete(file *torrent.File) bool {
	if file.Length() == 0 {
		return true
	}
	for _, state := range file.State() {
		if !state.Ok || !state.Complete {
			return false
		}
	}
	return true
}

func (r *limitedReadCloser) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *limitedReadCloser) Close() error {
	r.once.Do(func() {
		r.err = r.closer.Close()
	})
	return r.err
}

func (e *Engine) startTorrentProgress(ctx context.Context, d *Download, tor *torrent.Torrent) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var lastCompleted int64
		var lastAt = time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case now := <-ticker.C:
				completed := e.updateTorrentProgress(d, tor)
				elapsed := now.Sub(lastAt)
				if elapsed > 0 {
					d.setDownloadBPS((completed - lastCompleted) * int64(time.Second) / int64(elapsed))
				}
				lastCompleted = completed
				lastAt = now
			}
		}
	}()
	return func() {
		close(stop)
		<-done
		e.updateTorrentProgress(d, tor)
	}
}

func (e *Engine) updateTorrentProgress(d *Download, tor *torrent.Torrent) int64 {
	stats := tor.Stats()
	files := tor.Files()
	d.mu.Lock()
	defer d.mu.Unlock()
	var completed int64
	for i := range d.torrentFiles {
		if i >= len(files) {
			continue
		}
		var n int64
		if d.torrentFiles[i].Released {
			n = d.torrentFiles[i].Length
		} else {
			n = files[i].BytesCompleted()
			if n > d.torrentFiles[i].Length {
				n = d.torrentFiles[i].Length
			}
		}
		d.torrentFiles[i].Completed = n
		if d.torrentFiles[i].Selected {
			completed += n
		}
	}
	d.completedLen = completed
	d.connections = stats.ActivePeers + stats.HalfOpenPeers
	d.numSeeders = stats.ConnectedSeeders
	d.seeder = tor.Seeding()
	if d.pieceLength > 0 {
		d.donePieces = completedPieces(d.totalLength, completed, d.pieceLength)
		d.bitfield = bitfieldFor(d.totalLength, completed, d.pieceLength)
	}
	return completed
}

func (d *Download) torrentPeersLocked() []map[string]string {
	if d.torrent == nil || d.torrent.torrent == nil {
		return []map[string]string{}
	}
	tor := d.torrent.torrent
	snapshots := tor.PeerSnapshots()
	out := make([]map[string]string, 0, len(snapshots))
	for _, peer := range snapshots {
		host, port, err := net.SplitHostPort(peer.RemoteAddr)
		if err != nil {
			host = peer.RemoteAddr
			port = "0"
		}
		stats := peer.Stats
		bitfield := ""
		if len(peer.RemoteBitfield) > 0 {
			bitfield = hex.EncodeToString(peer.RemoteBitfield)
		}
		out = append(out, map[string]string{
			"peerId":        url.QueryEscape(peer.PeerID.String()),
			"ip":            host,
			"port":          port,
			"bitfield":      bitfield,
			"amChoking":     strconv.FormatBool(peer.LocalChoking),
			"peerChoking":   strconv.FormatBool(peer.RemoteChoking),
			"downloadSpeed": strconv.FormatInt(int64(stats.DownloadRate), 10),
			"uploadSpeed":   strconv.FormatInt(int64(stats.LastWriteUploadRate), 10),
			"seeder":        strconv.FormatBool(peer.Seeder),
		})
	}
	return out
}

func peerBitfield(remotePieces int, numPieces int64) string {
	if remotePieces <= 0 || numPieces <= 0 {
		return ""
	}
	pieces := int64(remotePieces)
	if pieces > numPieces {
		pieces = numPieces
	}
	bytesLen := (numPieces + 7) / 8
	bits := make([]byte, bytesLen)
	for i := int64(0); i < pieces; i++ {
		byteIndex := i / 8
		bit := uint(7 - (i % 8))
		bits[byteIndex] |= 1 << bit
	}
	return hex.EncodeToString(bits)
}

func encodeMetaInfo(mi metainfo.MetaInfo) (string, error) {
	var buf bytes.Buffer
	if err := bencode.NewEncoder(&buf).Encode(mi); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}
