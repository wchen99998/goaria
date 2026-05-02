package goaria

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/quic-go/quic-go/http3"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
)

type Engine struct {
	cfg              Config
	log              *zap.Logger
	client           *http.Client
	customHTTPClient bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu           sync.RWMutex
	downloads    map[string]*Download
	waiting      []string
	active       map[string]struct{}
	stopped      []string
	stoppedSet   map[string]struct{}
	stoppedTotal int
	global       Options
	sessionID    string

	wake        chan struct{}
	shutdownReq chan bool

	transportMu  sync.Mutex
	transports   map[string]*http.Transport
	h2Transports map[string]*http2.Transport
	h3Transports map[string]*http3.Transport

	dirMu      sync.Mutex
	createdDir map[string]struct{}

	subMu       sync.Mutex
	subscribers map[chan Notification]struct{}
}

type Download struct {
	mu sync.RWMutex

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

	errorCode    string
	errorMessage string
	createdAt    time.Time
	stoppedAt    time.Time

	ctx    context.Context
	cancel context.CancelFunc
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
		client = &http.Client{
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				MaxIdleConns:          1024,
				MaxIdleConnsPerHost:   128,
				IdleConnTimeout:       90 * time.Second,
				ResponseHeaderTimeout: 30 * time.Second,
				ForceAttemptHTTP2:     true,
				DisableCompression:    true,
			},
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		cfg:              cfg,
		log:              log,
		client:           client,
		customHTTPClient: customHTTPClient,
		ctx:              ctx,
		cancel:           cancel,
		downloads:        make(map[string]*Download),
		active:           make(map[string]struct{}),
		stoppedSet:       make(map[string]struct{}),
		global:           defaultOptions(cfg.Dir, cfg.MaxConcurrentDownloads, cfg.MaxDownloadResult),
		sessionID:        randomHex(20),
		wake:             make(chan struct{}, 1),
		shutdownReq:      make(chan bool, 1),
		transports:       make(map[string]*http.Transport),
		h2Transports:     make(map[string]*http2.Transport),
		h3Transports:     make(map[string]*http3.Transport),
		createdDir:       make(map[string]struct{}),
		subscribers:      make(map[chan Notification]struct{}),
	}
	e.global["user-agent"] = cfg.UserAgent
	e.wg.Add(1)
	go e.scheduler()
	return e, nil
}

func (e *Engine) Close(ctx context.Context) error {
	e.cancel()
	e.signal()
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		e.closeTransports()
		return nil
	}
}

func (e *Engine) ShutdownRequests() <-chan bool {
	return e.shutdownReq
}

func (e *Engine) RequestShutdown(force bool) {
	select {
	case e.shutdownReq <- force:
	default:
	}
}

func (e *Engine) AddURI(uris []string, opts Options, position *int) (string, error) {
	if len(uris) == 0 {
		return "", fmt.Errorf("no URI to download")
	}
	for _, raw := range uris {
		if err := validateHTTPURI(raw); err != nil {
			return "", err
		}
	}
	if opts == nil {
		opts = Options{}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.ctx.Err(); err != nil {
		return "", ErrShutdown
	}
	merged := layerOptions(e.global, opts)
	gid := optionString(merged, "gid")
	if gid == "" {
		gid = randomHex(8)
	} else if !validGID(gid) {
		return "", ErrInvalidGID
	}
	if _, exists := e.downloads[gid]; exists {
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
	e.signal()
	return gid, nil
}

func (e *Engine) AddTorrent(string, []string, Options, *int) (string, error) {
	return "", ErrUnsupportedMethod
}

func (e *Engine) AddMetalink(string, Options, *int) ([]string, error) {
	return nil, ErrUnsupportedMethod
}

func (e *Engine) Remove(gid string) (string, error) {
	return e.remove(gid, false)
}

func (e *Engine) ForceRemove(gid string) (string, error) {
	return e.remove(gid, true)
}

func (e *Engine) Pause(gid string) (string, error) {
	return e.pause(gid, false)
}

func (e *Engine) ForcePause(gid string) (string, error) {
	return e.pause(gid, true)
}

func (e *Engine) PauseAll() (string, error) {
	for _, gid := range e.gidsByStatus(StatusActive, StatusWaiting) {
		_, _ = e.pause(gid, false)
	}
	return "OK", nil
}

func (e *Engine) ForcePauseAll() (string, error) {
	for _, gid := range e.gidsByStatus(StatusActive, StatusWaiting) {
		_, _ = e.pause(gid, true)
	}
	return "OK", nil
}

func (e *Engine) Unpause(gid string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	d, err := e.findDownloadLocked(gid)
	if err != nil {
		return "", err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.status != StatusPaused {
		return "OK", nil
	}
	d.status = StatusWaiting
	d.errorCode = ""
	d.errorMessage = ""
	e.waiting = append(e.waiting, d.gid)
	e.signal()
	return "OK", nil
}

func (e *Engine) UnpauseAll() (string, error) {
	for _, gid := range e.gidsByStatus(StatusPaused) {
		_, _ = e.Unpause(gid)
	}
	return "OK", nil
}

func (e *Engine) ChangePosition(gid string, pos int, how string) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	d, err := e.findDownloadLocked(gid)
	if err != nil {
		return 0, err
	}
	idx := indexOf(e.waiting, d.gid)
	if idx < 0 {
		return 0, fmt.Errorf("download is not in waiting queue")
	}
	e.waiting = append(e.waiting[:idx], e.waiting[idx+1:]...)
	var dst int
	switch how {
	case "POS_SET":
		dst = pos
	case "POS_CUR":
		dst = idx + pos
	case "POS_END":
		dst = len(e.waiting) + pos + 1
	default:
		return 0, fmt.Errorf("invalid position mode")
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
	return dst, nil
}

func (e *Engine) ChangeURI(gid string, fileIndex int, delURIs []string, addURIs []string, position *int) ([]int, error) {
	if fileIndex != 1 {
		return nil, fmt.Errorf("fileIndex out of range")
	}
	for _, raw := range addURIs {
		if err := validateHTTPURI(raw); err != nil {
			return nil, err
		}
	}
	e.mu.RLock()
	d, err := e.findDownloadLocked(gid)
	e.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	deleted := 0
	for _, raw := range delURIs {
		for i, u := range d.uris {
			if u.URI == raw {
				d.uris = append(d.uris[:i], d.uris[i+1:]...)
				deleted++
				break
			}
		}
	}
	added := len(addURIs)
	insertAt := len(d.uris)
	if position != nil {
		insertAt = *position
		if insertAt < 0 {
			insertAt = 0
		}
		if insertAt > len(d.uris) {
			insertAt = len(d.uris)
		}
	}
	newInfos := make([]URIInfo, 0, len(addURIs))
	for _, raw := range addURIs {
		newInfos = append(newInfos, URIInfo{URI: raw, Status: URIStatusWaiting})
	}
	d.uris = append(d.uris, make([]URIInfo, len(newInfos))...)
	copy(d.uris[insertAt+len(newInfos):], d.uris[insertAt:])
	copy(d.uris[insertAt:], newInfos)
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
	e.mu.RLock()
	d, err := e.findDownloadLocked(gid)
	e.mu.RUnlock()
	if err != nil {
		return "", err
	}
	d.mu.Lock()
	d.options = mergeOptions(d.options, opts)
	d.dir = optionString(d.options, "dir")
	d.out = optionString(d.options, "out")
	d.mu.Unlock()
	return "OK", nil
}

func (e *Engine) GetGlobalOption() map[string]any {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return optionsForRPC(e.global)
}

func (e *Engine) ChangeGlobalOption(opts Options) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.global = mergeOptions(e.global, opts)
	e.cfg.MaxConcurrentDownloads = optionInt(e.global, "max-concurrent-downloads", e.cfg.MaxConcurrentDownloads)
	e.cfg.MaxDownloadResult = optionInt(e.global, "max-download-result", e.cfg.MaxDownloadResult)
	e.signal()
	return "OK", nil
}

func (e *Engine) PurgeDownloadResult() (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, gid := range e.stopped {
		delete(e.downloads, gid)
		delete(e.stoppedSet, gid)
	}
	e.stopped = nil
	return "OK", nil
}

func (e *Engine) RemoveDownloadResult(gid string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	d, err := e.findDownloadLocked(gid)
	if err != nil {
		return "", err
	}
	d.mu.RLock()
	terminal := isTerminal(d.status)
	d.mu.RUnlock()
	if !terminal {
		return "", fmt.Errorf("download result is not stopped")
	}
	delete(e.downloads, d.gid)
	delete(e.stoppedSet, d.gid)
	e.stopped = removeString(e.stopped, d.gid)
	return "OK", nil
}

func (e *Engine) GetVersion() VersionInfo {
	return VersionInfo{
		Version:         Aria2CompatVersion,
		EnabledFeatures: []string{"HTTP", "HTTPS", "JSON-RPC", "WebSocket"},
	}
}

func (e *Engine) GetSessionInfo() SessionInfo {
	return SessionInfo{SessionID: e.sessionID}
}

func (e *Engine) SaveSession() (string, error) {
	e.mu.RLock()
	path := optionString(e.global, "save-session")
	e.mu.RUnlock()
	if path == "" {
		return "OK", nil
	}
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	for _, d := range e.downloadsSnapshot() {
		d.mu.RLock()
		if isTerminal(d.status) {
			d.mu.RUnlock()
			continue
		}
		if len(d.uris) > 0 {
			_, _ = fmt.Fprintln(f, d.uris[0].URI)
			for k, v := range optionsForRPC(d.options) {
				if k == "gid" {
					continue
				}
				_, _ = fmt.Fprintf(f, "  %s=%v\n", k, v)
			}
		}
		d.mu.RUnlock()
	}
	return "OK", nil
}
