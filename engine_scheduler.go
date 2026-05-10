package goaria

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
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
	var starts []*Download
	e.mu.Lock()
	maxActive := optionInt(e.global, "max-concurrent-downloads", e.cfg.MaxConcurrentDownloads)
	for e.ctx.Err() == nil && len(e.active) < maxActive && len(e.waiting) > 0 {
		gid := e.waiting[0]
		e.waiting = e.waiting[1:]
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
		starts = append(starts, d)
	}
	e.mu.Unlock()

	for _, d := range starts {
		e.notify("aria2.onDownloadStart", d.gid)
		e.wg.Add(1)
		go func(download *Download) {
			defer e.wg.Done()
			err := e.runDownload(download)
			e.finishDownload(download, err)
		}(d)
	}
}

func (e *Engine) finishDownload(d *Download, err error) {
	var notification string
	e.mu.Lock()
	delete(e.active, d.gid)
	d.mu.Lock()
	if d.status == StatusActive {
		d.cancel = nil
		d.connections = 0
		d.downloadBPS = 0
		d.stoppedAt = time.Now()
		if err == nil {
			d.status = StatusComplete
			if d.totalLength > 0 {
				d.completedLen = d.totalLength
			} else if d.completedLen > 0 {
				d.totalLength = d.completedLen
			}
			notification = "aria2.onDownloadComplete"
		} else if e.ctx.Err() != nil {
			d.status = StatusPaused
		} else {
			d.status = StatusError
			d.errorCode = "1"
			d.errorMessage = err.Error()
			notification = "aria2.onDownloadError"
		}
		if isTerminal(d.status) {
			e.appendStoppedLocked(d.gid)
		}
	}
	d.mu.Unlock()
	e.mu.Unlock()
	if notification != "" {
		e.notify(notification, d.gid)
	}
	e.signal()
	e.saveSessionBestEffort()
}

func (e *Engine) remove(gid string, force bool) (string, error) {
	e.mu.Lock()
	d, err := e.findDownloadLocked(gid)
	if err != nil {
		e.mu.Unlock()
		return "", err
	}
	e.waiting = removeString(e.waiting, d.gid)
	delete(e.active, d.gid)
	d.mu.Lock()
	cancel := d.cancel
	d.cancel = nil
	d.status = StatusRemoved
	d.connections = 0
	d.downloadBPS = 0
	d.stoppedAt = time.Now()
	e.appendStoppedLocked(d.gid)
	d.mu.Unlock()
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	e.notify("aria2.onDownloadStop", d.gid)
	e.signal()
	e.saveSessionBestEffort()
	return "OK", nil
}

func (e *Engine) pause(gid string, force bool) (string, error) {
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
	e.waiting = removeString(e.waiting, d.gid)
	delete(e.active, d.gid)
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
	return "OK", nil
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
	maxResult := optionInt(e.global, "max-download-result", e.cfg.MaxDownloadResult)
	for maxResult > 0 && len(e.stopped) > maxResult {
		old := e.stopped[0]
		e.stopped = e.stopped[1:]
		delete(e.stoppedSet, old)
		delete(e.downloads, old)
	}
}

func (e *Engine) signal() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

func (e *Engine) findDownloadLocked(gid string) (*Download, error) {
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

func (e *Engine) downloadsSnapshot() []*Download {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]*Download, 0, len(e.downloads))
	for _, d := range e.downloads {
		out = append(out, d)
	}
	return out
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

func indexOf(items []string, s string) int {
	for i, item := range items {
		if item == s {
			return i
		}
	}
	return -1
}

func removeString(items []string, s string) []string {
	for i := 0; i < len(items); i++ {
		if items[i] == s {
			items = append(items[:i], items[i+1:]...)
			i--
		}
	}
	return items
}
