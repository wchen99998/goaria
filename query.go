package goaria

import (
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func (e *Engine) TellStatus(gid string, keys []string) (map[string]any, error) {
	e.mu.RLock()
	d, err := e.findDownloadLocked(gid)
	e.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	return d.snapshot(keys), nil
}

func (e *Engine) GetURIs(gid string) ([]URIInfo, error) {
	e.mu.RLock()
	d, err := e.findDownloadLocked(gid)
	e.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	return cloneURIs(d.uris), nil
}

func (e *Engine) GetFiles(gid string) ([]FileInfo, error) {
	e.mu.RLock()
	d, err := e.findDownloadLocked(gid)
	e.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.filesLocked(), nil
}

func (e *Engine) GetPeers(gid string) ([]map[string]string, error) {
	e.mu.RLock()
	_, err := e.findDownloadLocked(gid)
	e.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	return []map[string]string{}, nil
}

func (e *Engine) GetServers(gid string) ([]map[string]any, error) {
	e.mu.RLock()
	d, err := e.findDownloadLocked(gid)
	e.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	servers := make([]ServerInfo, 0, len(d.uris))
	for _, u := range d.uris {
		servers = append(servers, ServerInfo{
			URI:           u.URI,
			CurrentURI:    currentOrOriginal(d.currentURI, u.URI),
			DownloadSpeed: strconv.FormatInt(d.downloadBPS, 10),
		})
	}
	return []map[string]any{{
		"index":   "1",
		"servers": servers,
	}}, nil
}

func (e *Engine) TellActive(keys []string) []map[string]any {
	downloads := e.downloadsByStatus(StatusActive)
	out := make([]map[string]any, 0, len(downloads))
	for _, d := range downloads {
		out = append(out, d.snapshot(keys))
	}
	return out
}

func (e *Engine) TellWaiting(offset, num int, keys []string) []map[string]any {
	items := e.waitingDownloads()
	items = sliceDownloads(items, offset, num)
	out := make([]map[string]any, 0, len(items))
	for _, d := range items {
		out = append(out, d.snapshot(keys))
	}
	return out
}

func (e *Engine) TellStopped(offset, num int, keys []string) []map[string]any {
	e.mu.RLock()
	items := make([]*Download, 0, len(e.stopped))
	for _, gid := range e.stopped {
		if d := e.downloads[gid]; d != nil {
			items = append(items, d)
		}
	}
	e.mu.RUnlock()
	items = sliceDownloads(items, offset, num)
	out := make([]map[string]any, 0, len(items))
	for _, d := range items {
		out = append(out, d.snapshot(keys))
	}
	return out
}

func (e *Engine) GetGlobalStat() GlobalStat {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var speed int64
	active := 0
	waiting := 0
	for _, d := range e.downloads {
		d.mu.RLock()
		switch d.status {
		case StatusActive:
			active++
			speed += d.downloadBPS
		case StatusWaiting, StatusPaused:
			waiting++
		}
		d.mu.RUnlock()
	}
	return GlobalStat{
		DownloadSpeed:   strconv.FormatInt(speed, 10),
		UploadSpeed:     "0",
		NumActive:       strconv.Itoa(active),
		NumWaiting:      strconv.Itoa(waiting),
		NumStopped:      strconv.Itoa(len(e.stopped)),
		NumStoppedTotal: strconv.Itoa(e.stoppedTotal),
	}
}

func (e *Engine) downloadsByStatus(status Status) []*Download {
	e.mu.RLock()
	defer e.mu.RUnlock()
	items := make([]*Download, 0)
	for _, d := range e.downloads {
		d.mu.RLock()
		ok := d.status == status
		d.mu.RUnlock()
		if ok {
			items = append(items, d)
		}
	}
	sort.Slice(items, func(i, j int) bool {
		items[i].mu.RLock()
		ci := items[i].createdAt
		items[i].mu.RUnlock()
		items[j].mu.RLock()
		cj := items[j].createdAt
		items[j].mu.RUnlock()
		return ci.Before(cj)
	})
	return items
}

func (e *Engine) waitingDownloads() []*Download {
	e.mu.RLock()
	defer e.mu.RUnlock()
	items := make([]*Download, 0, len(e.waiting))
	seen := make(map[string]struct{}, len(e.waiting))
	for _, gid := range e.waiting {
		if d := e.downloads[gid]; d != nil {
			items = append(items, d)
			seen[gid] = struct{}{}
		}
	}
	var paused []*Download
	for gid, d := range e.downloads {
		if _, ok := seen[gid]; ok {
			continue
		}
		d.mu.RLock()
		isPaused := d.status == StatusPaused
		d.mu.RUnlock()
		if isPaused {
			paused = append(paused, d)
		}
	}
	sort.Slice(paused, func(i, j int) bool {
		paused[i].mu.RLock()
		ci := paused[i].createdAt
		paused[i].mu.RUnlock()
		paused[j].mu.RLock()
		cj := paused[j].createdAt
		paused[j].mu.RUnlock()
		return ci.Before(cj)
	})
	items = append(items, paused...)
	return items
}

func (d *Download) snapshot(keys []string) map[string]any {
	d.mu.RLock()
	defer d.mu.RUnlock()
	all := map[string]any{
		"gid":             d.gid,
		"status":          string(d.status),
		"totalLength":     strconv.FormatInt(d.totalLength, 10),
		"completedLength": strconv.FormatInt(d.completedLen, 10),
		"uploadLength":    "0",
		"downloadSpeed":   strconv.FormatInt(d.downloadBPS, 10),
		"uploadSpeed":     "0",
		"connections":     strconv.Itoa(d.connections),
		"numPieces":       strconv.FormatInt(d.numPieces, 10),
		"pieceLength":     strconv.FormatInt(d.pieceLength, 10),
		"bitfield":        d.bitfield,
		"dir":             d.dir,
		"files":           d.filesLocked(),
	}
	if d.errorCode != "" {
		all["errorCode"] = d.errorCode
	}
	if d.errorMessage != "" {
		all["errorMessage"] = d.errorMessage
	}
	if len(keys) == 0 {
		return all
	}
	filtered := make(map[string]any, len(keys))
	for _, key := range keys {
		if v, ok := all[key]; ok {
			filtered[key] = v
		}
	}
	return filtered
}

func (d *Download) filesLocked() []FileInfo {
	path := d.path
	if path == "" {
		name := d.out
		if name == "" && len(d.uris) > 0 {
			name = filenameFromURI(d.uris[0].URI)
		}
		if name == "" {
			name = d.gid
		}
		path = filepath.Join(d.dir, name)
	}
	return []FileInfo{{
		Index:           "1",
		Path:            path,
		Length:          strconv.FormatInt(d.totalLength, 10),
		CompletedLength: strconv.FormatInt(d.completedLen, 10),
		Selected:        "true",
		URIs:            cloneURIs(d.uris),
	}}
}

func sliceDownloads(items []*Download, offset, num int) []*Download {
	if num < 0 {
		num = 0
	}
	if num == 0 || len(items) == 0 {
		return []*Download{}
	}
	if offset >= 0 {
		if offset >= len(items) {
			return []*Download{}
		}
		end := offset + num
		if end > len(items) {
			end = len(items)
		}
		return items[offset:end]
	}
	start := len(items) + offset
	if start < 0 {
		start = 0
	}
	out := make([]*Download, 0, num)
	for i := start; i >= 0 && len(out) < num; i-- {
		out = append(out, items[i])
	}
	return out
}

func cloneURIs(in []URIInfo) []URIInfo {
	out := make([]URIInfo, len(in))
	copy(out, in)
	return out
}

func currentOrOriginal(current, original string) string {
	if current != "" {
		return current
	}
	return original
}

func filenameFromURI(raw string) string {
	beforeQuery := strings.Split(raw, "?")[0]
	name := filepath.Base(beforeQuery)
	if name == "." || name == "/" || name == "" {
		return "index.html"
	}
	return name
}
