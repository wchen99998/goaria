package goaria

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/quic-go/quic-go/http3"
	"github.com/wchen99998/torrent"
	"github.com/wchen99998/torrent/metainfo"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
)

type downloadKind string

const (
	downloadKindURI     downloadKind = "uri"
	downloadKindTorrent downloadKind = "torrent"
	maxDownloadURIs                  = 64
)

type Engine struct {
	cfg              Config
	log              *zap.Logger
	client           *http.Client
	customHTTPClient bool

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeDone chan struct{}
	closeMu   sync.Mutex
	closeErr  error

	mu                 sync.RWMutex
	downloads          map[string]*Download
	waiting            []string
	active             map[string]struct{}
	stopped            []string
	stoppedSet         map[string]struct{}
	stoppedTotal       int
	global             Options
	sessionID          string
	sessionMu          sync.Mutex
	sessionSaveMu      sync.Mutex
	sessionSaveTimer   *time.Timer
	sessionSavePending bool
	sessionSaveClosed  bool
	sessionSaveWG      sync.WaitGroup

	wake        chan struct{}
	shutdownReq chan bool

	transportMu  sync.Mutex
	transports   map[string]*http.Transport
	h2Transports map[string]*http2.Transport
	h3Transports map[string]*http3.Transport

	pathMu             sync.Mutex
	pathReservations   map[string]string
	reservedPathsByGID map[string]map[string]struct{}
	overallLimiter     *rateLimiter

	subMu       sync.Mutex
	subscribers map[chan Notification]struct{}
}

type Download struct {
	mu sync.RWMutex

	kind    downloadKind
	gid     string
	uris    []URIInfo
	options Options

	status       Status
	dir          string
	out          string
	path         string
	currentURI   string
	totalLength  int64
	completedLen int64
	downloadBPS  int64
	connections  int
	pieceLength  int64
	numPieces    int64
	donePieces   int64
	bitfield     string
	infoHash     string
	numSeeders   int
	seeder       bool
	bittorrent   *BittorrentInfo
	torrent      *torrentRuntime
	torrentFiles []torrentFileState
	// torrentFinalized contains one-based torrent file indexes successfully
	// handed off to TorrentFileHandler. It survives runtime teardown so a
	// paused or restored download never hands the same file off twice.
	torrentFinalized map[int]struct{}
	torrentData      []byte
	torrentMagnet    string

	errorCode    string
	errorMessage string
	createdAt    time.Time
	stoppedAt    time.Time

	ctx    context.Context
	cancel context.CancelFunc
	// pathResetAfterRun identifies a canceled URI run that may still publish
	// stale output metadata after a dynamic dir/out change. Its finisher clears
	// that metadata before the queued replacement can start.
	pathResetAfterRun context.Context
	// retainReservationsAfterRun keeps an unchanged or atomically replaced
	// output reservation set across a cancel-and-requeue restart.
	retainReservationsAfterRun context.Context
	// reservationTransitionAfterRun identifies a torrent restart whose old
	// runtime may still write the previous selection. During cancellation the
	// engine holds the union of old and new paths, then collapses it to this
	// desired set only after the old runtime has exited.
	reservationTransitionAfterRun context.Context
	reservationPathsAfterRun      []string
}

func NewEngine(cfg Config) (*Engine, error) {
	if cfg.Dir == "" {
		cfg.Dir = "."
	}
	abs, err := filepath.Abs(cfg.Dir)
	if err != nil {
		return nil, err
	}
	cfg.Dir = abs
	if cfg.MaxConcurrentDownloads <= 0 {
		cfg.MaxConcurrentDownloads = 5
	}
	if cfg.MaxDownloadResult <= 0 {
		cfg.MaxDownloadResult = 1000
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = "goaria/" + Version
	}
	log := cfg.Logger
	if log == nil {
		log = zap.NewNop()
	}
	client := cfg.HTTPClient
	customHTTPClient := client != nil
	if client == nil {
		dialer := &net.Dialer{
			Timeout:   60 * time.Second,
			KeepAlive: 30 * time.Second,
		}
		client = &http.Client{
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				DialContext:           dialer.DialContext,
				MaxIdleConns:          1024,
				MaxIdleConnsPerHost:   128,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   60 * time.Second,
				ResponseHeaderTimeout: 60 * time.Second,
				ForceAttemptHTTP2:     true,
				DisableCompression:    true,
			},
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		cfg:                cfg,
		log:                log,
		client:             client,
		customHTTPClient:   customHTTPClient,
		ctx:                ctx,
		cancel:             cancel,
		downloads:          make(map[string]*Download),
		active:             make(map[string]struct{}),
		stoppedSet:         make(map[string]struct{}),
		global:             defaultOptions(cfg.Dir, cfg.MaxConcurrentDownloads, cfg.MaxDownloadResult),
		sessionID:          randomHex(20),
		wake:               make(chan struct{}, 1),
		shutdownReq:        make(chan bool, 1),
		transports:         make(map[string]*http.Transport),
		h2Transports:       make(map[string]*http2.Transport),
		h3Transports:       make(map[string]*http3.Transport),
		pathReservations:   make(map[string]string),
		reservedPathsByGID: make(map[string]map[string]struct{}),
		subscribers:        make(map[chan Notification]struct{}),
		closeDone:          make(chan struct{}),
	}
	e.global["user-agent"] = cfg.UserAgent
	e.overallLimiter = newDynamicRateLimiter(func() int64 {
		e.mu.RLock()
		defer e.mu.RUnlock()
		return optionBytes(e.global, "max-overall-download-limit", 0)
	})
	if cfg.SaveSession != "" {
		e.global["save-session"] = cfg.SaveSession
	}
	if cfg.InputFile != "" {
		if err := e.loadSession(cfg.InputFile); err != nil {
			cancel()
			return nil, err
		}
	}
	e.wg.Add(1)
	go e.scheduler()
	e.signal()
	return e, nil
}

func (e *Engine) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	e.cancel()
	e.signal()
	e.closeOnce.Do(func() {
		go func() {
			e.wg.Wait()
			e.closeQueuedSessionSaves()
			saveErr := e.saveSessionIfConfigured()
			e.closeTransports()
			e.closeSubscribers()
			e.closeMu.Lock()
			e.closeErr = saveErr
			e.closeMu.Unlock()
			close(e.closeDone)
		}()
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.closeDone:
		e.closeMu.Lock()
		defer e.closeMu.Unlock()
		return e.closeErr
	}
}

func (e *Engine) ShutdownRequests() <-chan bool {
	return e.shutdownReq
}

func (e *Engine) RequestShutdown(force bool) {
	if force {
		select {
		case e.shutdownReq <- true:
			return
		default:
		}
		// Replace a queued graceful request so forceShutdown can escalate it.
		select {
		case <-e.shutdownReq:
		default:
		}
		select {
		case e.shutdownReq <- true:
		default:
		}
		return
	}
	select {
	case e.shutdownReq <- false:
	default:
	}
}

func (e *Engine) AddURI(uris []string, opts Options, position *int) (string, error) {
	if err := validateOptionValues(opts); err != nil {
		return "", err
	}
	if len(uris) == 0 {
		return "", fmt.Errorf("no URI to download")
	}
	if len(uris) > maxDownloadURIs {
		return "", fmt.Errorf("too many URIs: maximum is %d", maxDownloadURIs)
	}
	if len(uris) == 1 && isMagnetURI(uris[0]) {
		return e.addMagnet(uris[0], opts, position)
	}
	for _, raw := range uris {
		if isMagnetURI(raw) {
			return "", ErrUnsupportedProtocol
		}
		if err := validateHTTPURI(raw); err != nil {
			return "", err
		}
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
	d := newDownload(gid, uris, merged)
	if optionBool(merged, "pause") {
		d.status = StatusPaused
	} else {
		e.insertWaitingLocked(gid, position)
	}
	e.downloads[gid] = d
	if ce := e.log.Check(zap.InfoLevel, "download added"); ce != nil {
		ce.Write(zap.String("gid", gid), zap.Strings("uris", uris))
	}
	e.mu.Unlock()
	e.signal()
	e.saveSessionBestEffort()
	return gid, nil
}

func (e *Engine) AddTorrent(torrent string, uris []string, opts Options, position *int) (string, error) {
	if err := validateOptionValues(opts); err != nil {
		return "", err
	}
	if len(uris) > maxDownloadURIs {
		return "", fmt.Errorf("too many webseed URIs: maximum is %d", maxDownloadURIs)
	}
	return e.addTorrent(torrent, uris, opts, position)
}

func (e *Engine) AddMetalink(string, Options, *int) ([]string, error) {
	return nil, ErrUnsupportedMethod
}

func (e *Engine) Remove(gid string) (string, error) {
	return e.remove(gid)
}

func (e *Engine) ForceRemove(gid string) (string, error) {
	return e.remove(gid)
}

func (e *Engine) Pause(gid string) (string, error) {
	return e.pause(gid)
}

func (e *Engine) ForcePause(gid string) (string, error) {
	return e.pause(gid)
}

func (e *Engine) PauseAll() (string, error) {
	for _, gid := range e.gidsByStatus(StatusActive, StatusWaiting) {
		_, _ = e.pause(gid)
	}
	return "OK", nil
}

func (e *Engine) ForcePauseAll() (string, error) {
	for _, gid := range e.gidsByStatus(StatusActive, StatusWaiting) {
		_, _ = e.pause(gid)
	}
	return "OK", nil
}

func (e *Engine) Unpause(gid string) (string, error) {
	e.mu.Lock()
	d, err := e.findDownloadLocked(gid)
	if err != nil {
		e.mu.Unlock()
		return "", err
	}
	d.mu.Lock()
	if d.status != StatusPaused {
		d.mu.Unlock()
		e.mu.Unlock()
		return d.gid, nil
	}
	d.status = StatusWaiting
	d.errorCode = ""
	d.errorMessage = ""
	e.waiting = append(e.waiting, d.gid)
	d.mu.Unlock()
	e.mu.Unlock()
	e.signal()
	e.saveSessionBestEffort()
	return d.gid, nil
}

func (e *Engine) UnpauseAll() (string, error) {
	for _, gid := range e.gidsByStatus(StatusPaused) {
		_, _ = e.Unpause(gid)
	}
	return "OK", nil
}

func (e *Engine) ChangePosition(gid string, pos int, how string) (int, error) {
	e.mu.Lock()
	d, err := e.findDownloadLocked(gid)
	if err != nil {
		e.mu.Unlock()
		return 0, err
	}
	idx := slices.Index(e.waiting, d.gid)
	if idx < 0 {
		e.mu.Unlock()
		return 0, fmt.Errorf("download is not in waiting queue")
	}
	switch how {
	case "POS_SET", "POS_CUR", "POS_END":
	default:
		e.mu.Unlock()
		return 0, fmt.Errorf("invalid position mode")
	}
	e.waiting = append(e.waiting[:idx], e.waiting[idx+1:]...)
	var dst int
	switch how {
	case "POS_SET":
		dst = pos
	case "POS_CUR":
		dst = clampedQueuePosition(idx, pos, len(e.waiting))
	case "POS_END":
		dst = clampedQueuePosition(len(e.waiting)+1, pos, len(e.waiting))
	}
	if dst < 0 {
		dst = 0
	}
	if dst > len(e.waiting) {
		dst = len(e.waiting)
	}
	e.waiting = append(e.waiting, "")
	copy(e.waiting[dst+1:], e.waiting[dst:])
	e.waiting[dst] = d.gid
	e.mu.Unlock()
	e.saveSessionBestEffort()
	return dst, nil
}

func clampedQueuePosition(base, delta, maximum int) int {
	if base < 0 {
		base = 0
	}
	if delta >= 0 {
		if base >= maximum || delta > maximum-base {
			return maximum
		}
		return base + delta
	}
	if delta < -base {
		return 0
	}
	position := base + delta
	if position > maximum {
		return maximum
	}
	return position
}

func (e *Engine) ChangeURI(gid string, fileIndex int, delURIs []string, addURIs []string, position *int) ([]int, error) {
	if fileIndex != 1 {
		return nil, fmt.Errorf("fileIndex out of range")
	}
	if len(delURIs) > maxDownloadURIs || len(addURIs) > maxDownloadURIs {
		return nil, fmt.Errorf("too many URI changes: maximum is %d", maxDownloadURIs)
	}
	for _, raw := range addURIs {
		if err := validateHTTPURI(raw); err != nil {
			return nil, err
		}
	}
	e.mu.Lock()
	d, err := e.findDownloadLocked(gid)
	if err != nil {
		e.mu.Unlock()
		return nil, err
	}
	d.mu.Lock()
	nextURIs := append([]URIInfo(nil), d.uris...)
	deleted := 0
	for _, raw := range delURIs {
		for i, u := range nextURIs {
			if u.URI == raw {
				nextURIs = append(nextURIs[:i], nextURIs[i+1:]...)
				deleted++
				break
			}
		}
	}
	added := len(addURIs)
	insertAt := len(nextURIs)
	if position != nil {
		insertAt = *position
		if insertAt < 0 {
			insertAt = 0
		}
		if insertAt > len(nextURIs) {
			insertAt = len(nextURIs)
		}
	}
	newInfos := make([]URIInfo, 0, len(addURIs))
	for _, raw := range addURIs {
		newInfos = append(newInfos, URIInfo{URI: raw, Status: URIStatusWaiting})
	}
	nextURIs = append(nextURIs, make([]URIInfo, len(newInfos))...)
	copy(nextURIs[insertAt+len(newInfos):], nextURIs[insertAt:])
	copy(nextURIs[insertAt:], newInfos)
	if len(nextURIs) > maxDownloadURIs {
		d.mu.Unlock()
		e.mu.Unlock()
		return nil, fmt.Errorf("too many URIs: maximum is %d", maxDownloadURIs)
	}
	if d.kind == downloadKindURI && len(nextURIs) == 0 {
		d.mu.Unlock()
		e.mu.Unlock()
		return nil, fmt.Errorf("URI download must retain at least one URI")
	}
	d.uris = nextURIs
	var cancel context.CancelFunc
	if deleted+added > 0 {
		if d.status == StatusActive {
			d.retainReservationsAfterRun = d.ctx
		}
		cancel = e.restartActiveDownloadLocked(d)
	}
	d.mu.Unlock()
	e.mu.Unlock()
	if cancel != nil {
		cancel()
		e.signal()
	}
	e.saveSessionBestEffort()
	return []int{deleted, added}, nil
}

func (e *Engine) GetOption(gid string) (map[string]any, error) {
	e.mu.RLock()
	d, err := e.findDownloadLocked(gid)
	e.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	return optionsForRPC(d.options), nil
}

func (e *Engine) ChangeOption(gid string, opts Options) (string, error) {
	for _, key := range []string{"dry-run", "gid", "metalink-base-uri", "parameterized-uri", "pause", "piece-length", "rpc-save-upload-metadata"} {
		if optionExplicit(opts, key) {
			return "", fmt.Errorf("option %q cannot be changed dynamically", key)
		}
	}
	if err := validateOptionValues(opts); err != nil {
		return "", err
	}

	e.mu.Lock()
	d, err := e.findDownloadLocked(gid)
	if err != nil {
		e.mu.Unlock()
		return "", err
	}
	d.mu.Lock()
	merged := mergeOptions(d.options, opts)
	if err := validateOptionLayerSize(merged); err != nil {
		d.mu.Unlock()
		e.mu.Unlock()
		return "", err
	}
	var torrentInfo *metainfo.Info
	if d.kind == downloadKindTorrent {
		if err := validateTorrentExtensionOptions(merged); err != nil {
			d.mu.Unlock()
			e.mu.Unlock()
			return "", err
		}
		torrentInfo, err = d.metadataInfoLocked()
		if err != nil {
			d.mu.Unlock()
			e.mu.Unlock()
			return "", err
		}
	}
	nextDir := optionString(merged, "dir")
	nextOut := optionString(merged, "out")
	var nextFiles []torrentFileState
	var nextTotal, nextCompleted int64
	if torrentInfo != nil {
		nextFiles, nextTotal, nextCompleted, err = rebuildTorrentFileStatesPreservingProgress(*torrentInfo, merged, nextDir, d.torrentFiles, d.torrentFinalized)
		if err != nil {
			d.mu.Unlock()
			e.mu.Unlock()
			return "", err
		}
	}
	requiresRestart := activeOptionChangeRequiresRestart(d.kind, opts)
	if d.kind == downloadKindURI && d.status == StatusActive && d.numPieces > 1 && uriSegmentedLiveOptionChange(opts) {
		requiresRestart = false
	}
	// A magnet waiting for metadata re-reads selection options after GotInfo,
	// so a selection-only update is already effective without tearing down the
	// peer session. Once metadata exists, restart to rebuild the fixed file
	// selection and atomically reserve all newly selected output paths.
	if d.kind == downloadKindTorrent && torrentInfo == nil && torrentSelectionOnlyChange(opts) {
		requiresRestart = false
	}
	transitioningTorrentRun := torrentInfo != nil && requiresRestart && d.ctx != nil &&
		(d.status == StatusActive || (d.status == StatusWaiting && d.retainReservationsAfterRun == d.ctx))
	if transitioningTorrentRun {
		nextPaths := selectedTorrentOutputPaths(nextFiles, d.torrentFinalized)
		// Keep paths used by the old runtime reserved until it has fully exited.
		// reserveOutputPaths adds the new set without removing the old set.
		if err := e.reserveOutputPaths(nextPaths, d.gid); err != nil {
			d.mu.Unlock()
			e.mu.Unlock()
			return "", err
		}
		d.reservationTransitionAfterRun = d.ctx
		d.reservationPathsAfterRun = append(d.reservationPathsAfterRun[:0], nextPaths...)
	}
	oldDir, oldOut := d.dir, d.out
	d.options = merged
	d.dir = nextDir
	d.out = nextOut
	pathChanged := d.kind == downloadKindURI &&
		((optionExplicit(opts, "dir") && oldDir != nextDir) || (optionExplicit(opts, "out") && oldOut != nextOut))
	if pathChanged {
		d.path = ""
		if d.ctx != nil {
			d.pathResetAfterRun = d.ctx
		}
	}
	if torrentInfo != nil {
		d.torrentFiles = nextFiles
		d.totalLength = nextTotal
		d.completedLen = nextCompleted
	}
	cancel := context.CancelFunc(nil)
	if requiresRestart {
		if d.status == StatusActive && (d.kind == downloadKindTorrent || !pathChanged) {
			d.retainReservationsAfterRun = d.ctx
		}
		cancel = e.restartActiveDownloadLocked(d)
	}
	d.mu.Unlock()
	e.mu.Unlock()
	if cancel != nil {
		cancel()
		e.signal()
	}
	e.saveSessionBestEffort()
	return "OK", nil
}

func activeOptionChangeRequiresRestart(kind downloadKind, opts Options) bool {
	if len(opts) == 0 {
		return false
	}
	for key := range opts {
		if key == optionBaseKey || key == sessionTorrentFinalizedOption || key == sessionExplicitOptionsOption {
			continue
		}
		if kind == downloadKindURI {
			switch key {
			case "max-download-limit":
				continue
			}
		}
		return true
	}
	return false
}

func torrentSelectionOnlyChange(opts Options) bool {
	hasOption := false
	for key := range opts {
		if key == optionBaseKey || key == sessionTorrentFinalizedOption || key == sessionExplicitOptionsOption {
			continue
		}
		hasOption = true
		switch key {
		case "select-file", torrentSelectFileExtOption, torrentExcludeFileExtOption:
		default:
			return false
		}
	}
	return hasOption
}

func uriSegmentedLiveOptionChange(opts Options) bool {
	hasOption := false
	for key := range opts {
		if key == optionBaseKey || key == sessionTorrentFinalizedOption || key == sessionExplicitOptionsOption {
			continue
		}
		hasOption = true
		switch key {
		case "max-download-limit", "max-connection-per-server", "split":
		default:
			return false
		}
	}
	return hasOption
}

// restartActiveDownloadLocked queues an active item behind its current run.
// e.mu and d.mu must both be held. The scheduler keeps the old active marker
// until its goroutine exits, so the replacement cannot overlap it.
func (e *Engine) restartActiveDownloadLocked(d *Download) context.CancelFunc {
	if d.status != StatusActive {
		return nil
	}
	d.status = StatusWaiting
	d.connections = 0
	d.downloadBPS = 0
	if slices.Index(e.waiting, d.gid) < 0 {
		e.waiting = append(e.waiting, d.gid)
	}
	cancel := d.cancel
	d.cancel = nil
	return cancel
}

func (d *Download) metadataInfoLocked() (*metainfo.Info, error) {
	if len(d.torrentData) > 0 {
		var info metainfo.Info
		var err error
		if d.torrentMagnet != "" {
			_, parsed, _, parseErr := torrent.ParseMetaInfoBytes(d.torrentData)
			if parseErr != nil {
				err = fmt.Errorf("invalid cached magnet metadata: %w", parseErr)
			} else if validateErr := validateTorrentInfo(parsed); validateErr != nil {
				err = fmt.Errorf("invalid cached magnet metadata: %w", validateErr)
			} else {
				info = *parsed
			}
		} else {
			_, info, err = torrentMetaInfo(d.torrentData)
		}
		if err != nil {
			return nil, err
		}
		return &info, nil
	}
	if d.torrent != nil && d.torrent.torrent != nil {
		mi := d.torrent.torrent.Metainfo()
		if len(mi.InfoBytes) == 0 {
			return nil, nil
		}
		info, err := mi.UnmarshalInfo()
		if err != nil {
			return nil, err
		}
		return &info, nil
	}
	return nil, nil
}

func rebuildTorrentFileStatesPreservingProgress(info metainfo.Info, opts Options, dir string, previous []torrentFileState, finalized map[int]struct{}) ([]torrentFileState, int64, int64, error) {
	next, err := torrentFileStates(info, metainfo.Hash{}, opts, dir)
	if err != nil {
		return nil, 0, 0, err
	}
	byIndex := make(map[int]torrentFileState, len(previous))
	for _, state := range previous {
		byIndex[state.Index] = state
	}
	var total int64
	var completed int64
	for i := range next {
		if old, ok := byIndex[next[i].Index]; ok {
			next[i].Completed = old.Completed
			if _, finalized := finalized[next[i].Index]; finalized {
				// A finalized file has already been handed to the caller. Keep its
				// durable path identity rather than claiming a newly configured path
				// was completed without downloading or handing it off.
				next[i].Path = old.Path
			}
			if next[i].Completed > next[i].Length {
				next[i].Completed = next[i].Length
			}
		}
		if _, ok := finalized[next[i].Index]; ok {
			next[i].Completed = next[i].Length
		}
		if next[i].Selected {
			total += next[i].Length
			completed += next[i].Completed
		}
	}
	return next, total, completed, nil
}

func (e *Engine) GetGlobalOption() map[string]any {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return optionsForRPC(e.global)
}

func (e *Engine) ChangeGlobalOption(opts Options) (string, error) {
	if optionExplicit(opts, "gid") {
		return "", fmt.Errorf("option %q cannot be changed globally", "gid")
	}
	if err := validateOptionValues(opts); err != nil {
		return "", err
	}
	e.mu.Lock()
	merged := mergeOptions(e.global, opts)
	if err := validateOptionLayerSize(merged); err != nil {
		e.mu.Unlock()
		return "", err
	}
	if err := validateTorrentExtensionOptions(merged); err != nil {
		e.mu.Unlock()
		return "", err
	}
	e.global = merged
	e.cfg.MaxConcurrentDownloads = optionInt(e.global, "max-concurrent-downloads", e.cfg.MaxConcurrentDownloads)
	e.cfg.MaxDownloadResult = optionInt(e.global, "max-download-result", e.cfg.MaxDownloadResult)
	e.pruneStoppedLocked()
	e.mu.Unlock()
	e.signal()
	e.saveSessionBestEffort()
	return "OK", nil
}

func (e *Engine) PurgeDownloadResult() (string, error) {
	e.mu.Lock()
	for _, gid := range e.stopped {
		delete(e.downloads, gid)
		delete(e.stoppedSet, gid)
	}
	e.stopped = nil
	e.mu.Unlock()
	e.saveSessionBestEffort()
	return "OK", nil
}

func (e *Engine) RemoveDownloadResult(gid string) (string, error) {
	e.mu.Lock()
	d, err := e.findDownloadLocked(gid)
	if err != nil {
		e.mu.Unlock()
		return "", err
	}
	d.mu.RLock()
	terminal := isTerminal(d.status)
	d.mu.RUnlock()
	if !terminal {
		e.mu.Unlock()
		return "", fmt.Errorf("download result is not stopped")
	}
	delete(e.downloads, d.gid)
	delete(e.stoppedSet, d.gid)
	e.stopped = slices.DeleteFunc(e.stopped, func(gid string) bool { return gid == d.gid })
	e.mu.Unlock()
	e.saveSessionBestEffort()
	return "OK", nil
}

func (e *Engine) GetVersion() VersionInfo {
	return VersionInfo{
		Version:         Aria2CompatVersion,
		EnabledFeatures: []string{"BitTorrent", "HTTP", "HTTPS", "JSON-RPC", "WebSocket"},
	}
}

func (e *Engine) GetSessionInfo() SessionInfo {
	return SessionInfo{SessionID: e.sessionID}
}
