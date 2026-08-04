package goaria

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

func (e *Engine) scheduler() {
	defer e.wg.Done()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-e.wake:
			e.schedule()
		}
	}
}

func (e *Engine) schedule() {
	type startDownload struct {
		download *Download
		ctx      context.Context
	}
	var starts []startDownload
	e.mu.Lock()
	maxActive := optionInt(e.global, "max-concurrent-downloads", e.cfg.MaxConcurrentDownloads)
	candidates := len(e.waiting)
	for e.ctx.Err() == nil && len(e.active) < maxActive && len(e.waiting) > 0 && candidates > 0 {
		gid := e.waiting[0]
		e.waiting = e.waiting[1:]
		candidates--
		if _, running := e.active[gid]; running {
			e.waiting = append(e.waiting, gid)
			continue
		}
		d := e.downloads[gid]
		if d == nil {
			continue
		}
		d.mu.Lock()
		if d.status != StatusWaiting {
			d.mu.Unlock()
			continue
		}
		ctx, cancel := context.WithCancel(e.ctx)
		d.ctx = ctx
		d.cancel = cancel
		d.status = StatusActive
		d.errorCode = ""
		d.errorMessage = ""
		d.downloadBPS = 0
		d.connections = 0
		d.mu.Unlock()
		e.active[gid] = struct{}{}
		starts = append(starts, startDownload{download: d, ctx: ctx})
	}
	e.mu.Unlock()

	for _, start := range starts {
		e.notify("aria2.onDownloadStart", start.download.gid)
		e.wg.Add(1)
		go func(download *Download, runCtx context.Context) {
			defer e.wg.Done()
			err := e.runDownload(download)
			e.finishDownload(download, runCtx, err)
		}(start.download, start.ctx)
	}
}

func (e *Engine) finishDownload(d *Download, runCtx context.Context, err error) {
	var notification string
	var cancel context.CancelFunc
	terminal := false
	e.mu.Lock()
	d.mu.Lock()
	if d.ctx != runCtx {
		if d.status != StatusActive {
			d.torrent = nil
		}
		d.mu.Unlock()
		e.mu.Unlock()
		return
	}
	delete(e.active, d.gid)
	retainReservations := d.status == StatusWaiting && d.retainReservationsAfterRun == runCtx
	if d.retainReservationsAfterRun == runCtx {
		d.retainReservationsAfterRun = nil
	}
	if d.pathResetAfterRun == runCtx {
		d.path = ""
		d.dir = optionString(d.options, "dir")
		d.out = optionString(d.options, "out")
		d.pathResetAfterRun = nil
	}
	if d.reservationTransitionAfterRun == runCtx {
		// The old torrent runtime can no longer write. Drop its obsolete paths
		// while retaining the replacement run's complete reservation set.
		if replaceErr := e.replaceOutputPaths(d.reservationPathsAfterRun, d.gid); replaceErr != nil {
			e.log.Error("failed to finalize torrent output reservation transition",
				zap.String("gid", d.gid),
				zap.Error(replaceErr),
			)
			retainReservations = false
		}
		d.reservationTransitionAfterRun = nil
		d.reservationPathsAfterRun = nil
	}
	d.ctx = nil
	// The torrent client is closed once runTorrentDownload returns. Drop the
	// runtime so the client's per-piece and per-peer state can be collected;
	// metadata queries fall back to d.torrentData.
	d.torrent = nil
	if d.status == StatusActive {
		cancel = d.cancel
		d.cancel = nil
		d.connections = 0
		d.downloadBPS = 0
		d.stoppedAt = time.Now()
		if err == nil {
			d.status = StatusComplete
			metadataOnly := d.kind == downloadKindURI && optionBool(d.options, "dry-run") ||
				d.kind == downloadKindTorrent && optionBool(d.options, "bt-metadata-only")
			if d.totalLength > 0 && !metadataOnly {
				d.completedLen = d.totalLength
			} else if d.completedLen > 0 {
				d.totalLength = d.completedLen
			}
			notification = "aria2.onDownloadComplete"
		} else if e.ctx.Err() != nil {
			// Engine shutdown is not a user pause. Persist the interrupted item as
			// waiting so a saved session resumes it on the next process start.
			d.status = StatusWaiting
		} else if errors.Is(err, context.Canceled) {
			d.status = StatusPaused
		} else {
			d.status = StatusError
			d.errorCode = "1"
			d.errorMessage = err.Error()
			notification = "aria2.onDownloadError"
		}
		terminal = isTerminal(d.status)
	}
	d.mu.Unlock()
	if terminal {
		e.appendStoppedLocked(d.gid)
	}
	if !retainReservations {
		e.releaseOutputPaths(d.gid)
	}
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if notification != "" {
		e.notify(notification, d.gid)
	}
	e.signal()
	e.saveSessionBestEffort()
}

func (e *Engine) remove(gid string) (string, error) {
	e.mu.Lock()
	d, err := e.findDownloadLocked(gid)
	if err != nil {
		e.mu.Unlock()
		return "", err
	}
	e.waiting = slices.DeleteFunc(e.waiting, func(gid string) bool { return gid == d.gid })
	d.mu.Lock()
	if isTerminal(d.status) {
		d.mu.Unlock()
		e.mu.Unlock()
		return "", fmt.Errorf("cannot remove stopped download")
	}
	cancel := d.cancel
	d.cancel = nil
	d.status = StatusRemoved
	d.connections = 0
	d.downloadBPS = 0
	d.stoppedAt = time.Now()
	d.mu.Unlock()
	e.appendStoppedLocked(d.gid)
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	e.notify("aria2.onDownloadStop", d.gid)
	e.signal()
	e.saveSessionBestEffort()
	return d.gid, nil
}

func (e *Engine) pause(gid string) (string, error) {
	e.mu.Lock()
	d, err := e.findDownloadLocked(gid)
	if err != nil {
		e.mu.Unlock()
		return "", err
	}
	d.mu.Lock()
	if isTerminal(d.status) {
		d.mu.Unlock()
		e.mu.Unlock()
		return "", fmt.Errorf("cannot pause stopped download")
	}
	e.waiting = slices.DeleteFunc(e.waiting, func(gid string) bool { return gid == d.gid })
	cancel := d.cancel
	d.cancel = nil
	d.status = StatusPaused
	d.connections = 0
	d.downloadBPS = 0
	d.mu.Unlock()
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	e.notify("aria2.onDownloadPause", d.gid)
	e.signal()
	e.saveSessionBestEffort()
	return d.gid, nil
}

func (e *Engine) insertWaitingLocked(gid string, position *int) {
	if position == nil || *position >= len(e.waiting) {
		e.waiting = append(e.waiting, gid)
		return
	}
	pos := *position
	if pos < 0 {
		pos = 0
	}
	e.waiting = append(e.waiting, "")
	copy(e.waiting[pos+1:], e.waiting[pos:])
	e.waiting[pos] = gid
}

func (e *Engine) appendStoppedLocked(gid string) {
	if _, ok := e.stoppedSet[gid]; !ok {
		e.stopped = append(e.stopped, gid)
		e.stoppedSet[gid] = struct{}{}
		e.stoppedTotal++
	}
	e.pruneStoppedLocked()
}

func (e *Engine) pruneStoppedLocked() {
	maxResult := optionInt(e.global, "max-download-result", e.cfg.MaxDownloadResult)
	if maxResult < 0 {
		maxResult = 0
	}
	keepUnfinished := optionBoolDefault(e.global, "keep-unfinished-download-result", true)
	isProtected := func(gid string) bool {
		if !keepUnfinished {
			return false
		}
		d := e.downloads[gid]
		if d == nil {
			return false
		}
		d.mu.RLock()
		protected := d.status == StatusError
		d.mu.RUnlock()
		return protected
	}
	prunable := 0
	for _, gid := range e.stopped {
		if !isProtected(gid) {
			prunable++
		}
	}
	excess := prunable - maxResult
	if excess <= 0 {
		return
	}
	kept := e.stopped[:0]
	for _, gid := range e.stopped {
		if excess > 0 && !isProtected(gid) {
			delete(e.stoppedSet, gid)
			delete(e.downloads, gid)
			excess--
			continue
		}
		kept = append(kept, gid)
	}
	e.stopped = kept
}

func (e *Engine) signal() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

func (e *Engine) findDownloadLocked(gid string) (*Download, error) {
	if gid == "" {
		return nil, ErrNotFound
	}
	if d := e.downloads[gid]; d != nil {
		return d, nil
	}
	var found *Download
	for id, d := range e.downloads {
		if strings.HasPrefix(id, gid) {
			if found != nil {
				return nil, fmt.Errorf("gid prefix is not unique")
			}
			found = d
		}
	}
	if found == nil {
		return nil, ErrNotFound
	}
	return found, nil
}

func (e *Engine) gidsByStatus(statuses ...Status) []string {
	want := make(map[Status]struct{}, len(statuses))
	for _, s := range statuses {
		want[s] = struct{}{}
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	var gids []string
	for gid, d := range e.downloads {
		d.mu.RLock()
		_, ok := want[d.status]
		d.mu.RUnlock()
		if ok {
			gids = append(gids, gid)
		}
	}
	return gids
}

func newDownload(gid string, uris []string, opts Options) *Download {
	infos := make([]URIInfo, 0, len(uris))
	for i, raw := range uris {
		status := URIStatusWaiting
		if i == 0 {
			status = URIStatusUsed
		}
		infos = append(infos, URIInfo{URI: raw, Status: status})
	}
	return &Download{
		kind:      downloadKindURI,
		gid:       gid,
		uris:      infos,
		options:   opts,
		status:    StatusWaiting,
		dir:       optionString(opts, "dir"),
		out:       optionString(opts, "out"),
		createdAt: time.Now(),
	}
}

func validateHTTPURI(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return ErrUnsupportedProtocol
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return ErrUnsupportedProtocol
	}
	return nil
}

func validGID(gid string) bool {
	if len(gid) != 16 {
		return false
	}
	_, err := hex.DecodeString(gid)
	return err == nil
}

func randomHex(bytesLen int) string {
	b := make([]byte, bytesLen)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b)
}

func isTerminal(s Status) bool {
	return s == StatusComplete || s == StatusError || s == StatusRemoved
}
