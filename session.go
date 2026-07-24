package goaria

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"go.uber.org/zap"
)

const (
	sessionTorrentDataURIPrefix = "data:application/x-bittorrent;base64,"
	// sessionTorrentFinalizedOption is private session state, not an aria2
	// download option. Normal option layering deliberately strips it.
	sessionTorrentFinalizedOption = "goaria-internal-torrent-finalized-files"
	// sessionExplicitOptionsOption preserves which effective values originated
	// in the per-download overlay. This keeps inherited options dynamically
	// inherited after a restore without losing the frozen base snapshot.
	sessionExplicitOptionsOption = "goaria-internal-explicit-options"
	maxSessionFileSize           = 256 << 20
	maxSessionDownloads          = 10_000
	maxSessionURIsPerDownload    = 65
	maxSessionOptionKeys         = maxDownloadOptionKeys*2 + 4
	maxSessionEffectiveOptions   = maxDownloadOptionKeys*2 + 2
	maxSessionOptionValues       = 64
)

type sessionDownload struct {
	gid       string
	uris      []string
	options   map[string]any
	createdAt time.Time
	order     int
}

type sessionItem struct {
	line    int
	uris    []string
	options Options
}

func (e *Engine) loadSession(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	items, err := parseSession(f)
	if err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	for _, item := range items {
		if len(item.uris) == 0 {
			continue
		}
		finalized, hasFinalized, err := parseSessionTorrentFinalizedIndexes(item.options)
		if err != nil {
			return fmt.Errorf("%s:%d: %w", path, item.line, err)
		}
		delete(item.options, sessionTorrentFinalizedOption)
		explicit, hasExplicit, err := parseSessionExplicitOptions(item.options)
		if err != nil {
			return fmt.Errorf("%s:%d: %w", path, item.line, err)
		}
		delete(item.options, sessionExplicitOptionsOption)
		optionLimit := maxDownloadOptionKeys
		if hasExplicit {
			optionLimit = maxSessionEffectiveOptions
		}
		if err := validateOptionValuesWithLimit(item.options, optionLimit); err != nil {
			return fmt.Errorf("%s:%d: %w", path, item.line, err)
		}
		merged := layerOptions(e.global, item.options)
		if hasExplicit {
			merged = restoreSessionOptionLayers(item.options, explicit)
			if err := validateRestoredOptionLayers(merged); err != nil {
				return fmt.Errorf("%s:%d: %w", path, item.line, err)
			}
		}
		if isMagnetURI(item.uris[0]) {
			if err := validateTorrentExtensionOptions(merged); err != nil {
				return fmt.Errorf("%s:%d: %w", path, item.line, err)
			}
		}
		gid := optionString(merged, "gid")
		if gid == "" {
			gid = randomHex(8)
		} else if !validGID(gid) {
			return fmt.Errorf("%s:%d: %w", path, item.line, ErrInvalidGID)
		}
		if _, exists := e.downloads[gid]; exists {
			return fmt.Errorf("%s:%d: gid already exists", path, item.line)
		}

		var d *Download
		if data, ok, err := decodeSessionTorrentDataURI(item.uris[0], torrentSizeLimit(merged)); err != nil {
			return fmt.Errorf("%s:%d: %w", path, item.line, err)
		} else if ok {
			webseeds := item.uris[1:]
			for _, raw := range webseeds {
				if err := validateHTTPURI(raw); err != nil {
					return fmt.Errorf("%s:%d: %w", path, item.line, err)
				}
			}
			mi, info, err := torrentMetaInfo(data)
			if err != nil {
				return fmt.Errorf("%s:%d: %w", path, item.line, err)
			}
			if _, _, err := torrentSelectionAndIndexOut(info, merged); err != nil {
				return fmt.Errorf("%s:%d: %w", path, item.line, err)
			}
			d, err = newTorrentDownload(gid, data, "", webseeds, merged, mi, info)
			if err != nil {
				return fmt.Errorf("%s:%d: %w", path, item.line, err)
			}
		} else if isMagnetURI(item.uris[0]) {
			webseeds := item.uris[1:]
			if len(webseeds) > maxDownloadURIs {
				return fmt.Errorf("%s:%d: too many magnet webseeds", path, item.line)
			}
			for _, raw := range webseeds {
				if err := validateHTTPURI(raw); err != nil {
					return fmt.Errorf("%s:%d: %w", path, item.line, err)
				}
			}
			var err error
			d, err = newMagnetDownload(gid, item.uris[0], webseeds, merged)
			if err != nil {
				return fmt.Errorf("%s:%d: %w", path, item.line, err)
			}
		} else {
			if len(item.uris) > maxDownloadURIs {
				return fmt.Errorf("%s:%d: too many URIs", path, item.line)
			}
			for _, raw := range item.uris {
				if isMagnetURI(raw) {
					return fmt.Errorf("%s:%d: %w", path, item.line, ErrUnsupportedProtocol)
				}
				if err := validateHTTPURI(raw); err != nil {
					return fmt.Errorf("%s:%d: %w", path, item.line, err)
				}
			}
			d = newDownload(gid, item.uris, merged)
		}
		if hasFinalized {
			if d.kind != downloadKindTorrent {
				return fmt.Errorf("%s:%d: %s is only valid for torrent session entries", path, item.line, sessionTorrentFinalizedOption)
			}
			if err := d.restoreTorrentFinalized(finalized); err != nil {
				return fmt.Errorf("%s:%d: %w", path, item.line, err)
			}
		}
		if optionBool(merged, "pause") {
			d.status = StatusPaused
		} else {
			e.insertWaitingLocked(gid, nil)
		}
		e.downloads[gid] = d
	}
	return nil
}

func parseSession(r io.Reader) ([]sessionItem, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64<<10), sessionMaxLineSize())
	var items []sessionItem
	var current *sessionItem
	lineNo := 0
	totalBytes := int64(0)
	flush := func() {
		if current != nil && len(current.uris) > 0 {
			items = append(items, *current)
		}
		current = nil
	}
	for scanner.Scan() {
		lineNo++
		line := scanner.Text()
		totalBytes += int64(len(line)) + 1
		if totalBytes > maxSessionFileSize {
			return nil, fmt.Errorf("session exceeds maximum size %d", maxSessionFileSize)
		}
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			if isSessionOptionLine(line) {
				if current == nil {
					return nil, fmt.Errorf("line %d: option without URI", lineNo)
				}
				key, value, ok := strings.Cut(trimmed, "=")
				if !ok {
					return nil, fmt.Errorf("line %d: invalid option", lineNo)
				}
				key = strings.TrimSpace(key)
				if key == "" {
					return nil, fmt.Errorf("line %d: invalid option", lineNo)
				}
				if err := current.addOption(key, normalizeOptionValue(strings.TrimSpace(value))); err != nil {
					return nil, fmt.Errorf("line %d: %w", lineNo, err)
				}
			} else {
				flush()
				uris := strings.Fields(trimmed)
				if len(uris) > maxSessionURIsPerDownload {
					return nil, fmt.Errorf("line %d: too many URIs", lineNo)
				}
				if len(items) >= maxSessionDownloads {
					return nil, fmt.Errorf("line %d: too many session downloads", lineNo)
				}
				current = &sessionItem{line: lineNo, uris: uris, options: Options{}}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("line %d: %w", lineNo+1, err)
	}
	flush()
	return items, nil
}

func encodeSessionTorrentDataURI(data []byte) string {
	return sessionTorrentDataURIPrefix + base64.StdEncoding.EncodeToString(data)
}

func sessionMaxLineSize() int {
	return base64.StdEncoding.EncodedLen(maxSupportedTorrentSize) + len(sessionTorrentDataURIPrefix) + 1<<20
}

func decodeSessionTorrentDataURI(raw string, maxSize int64) ([]byte, bool, error) {
	encoded, ok := strings.CutPrefix(raw, sessionTorrentDataURIPrefix)
	if !ok {
		return nil, false, nil
	}
	if maxSize <= 0 || maxSize > maxSupportedTorrentSize {
		maxSize = maxSupportedTorrentSize
	}
	decoder := base64.NewDecoder(base64.StdEncoding, strings.NewReader(encoded))
	data, err := io.ReadAll(io.LimitReader(decoder, maxSize+1))
	if err != nil {
		return nil, true, fmt.Errorf("invalid session torrent data URI: %w", err)
	}
	if int64(len(data)) > maxSize {
		return nil, true, &torrentSizeError{max: maxSize}
	}
	return data, true, nil
}

func parseSessionTorrentFinalizedIndexes(opts Options) (map[int]struct{}, bool, error) {
	if !optionExplicit(opts, sessionTorrentFinalizedOption) {
		return nil, false, nil
	}
	raw := strings.TrimSpace(optionString(opts, sessionTorrentFinalizedOption))
	if raw == "" {
		return nil, true, nil
	}
	indexes := make(map[int]struct{})
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		index, err := strconv.Atoi(item)
		if err != nil || index < 1 {
			return nil, true, fmt.Errorf("invalid %s index %q", sessionTorrentFinalizedOption, item)
		}
		indexes[index] = struct{}{}
	}
	return indexes, true, nil
}

func encodeSessionTorrentFinalizedIndexes(indexes map[int]struct{}) string {
	ordered := make([]int, 0, len(indexes))
	for index := range indexes {
		ordered = append(ordered, index)
	}
	sort.Ints(ordered)
	encoded := make([]string, 0, len(ordered))
	for _, index := range ordered {
		encoded = append(encoded, strconv.Itoa(index))
	}
	return strings.Join(encoded, ",")
}

func parseSessionExplicitOptions(opts Options) (map[string]struct{}, bool, error) {
	if !optionExplicit(opts, sessionExplicitOptionsOption) {
		return nil, false, nil
	}
	raw := optionString(opts, sessionExplicitOptionsOption)
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, true, fmt.Errorf("invalid %s: %w", sessionExplicitOptionsOption, err)
	}
	var keys []string
	if err := json.Unmarshal(decoded, &keys); err != nil {
		return nil, true, fmt.Errorf("invalid %s: %w", sessionExplicitOptionsOption, err)
	}
	if len(keys) > maxDownloadOptionKeys {
		return nil, true, fmt.Errorf("invalid %s: too many option keys", sessionExplicitOptionsOption)
	}
	explicit := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if key == "" || len(key) > 256 || key == optionBaseKey || key == sessionTorrentFinalizedOption || key == sessionExplicitOptionsOption || strings.ContainsRune(key, '=') || strings.IndexFunc(key, unicode.IsSpace) >= 0 {
			return nil, true, fmt.Errorf("invalid explicit session option key %q", key)
		}
		explicit[key] = struct{}{}
	}
	return explicit, true, nil
}

func encodeSessionExplicitOptions(opts Options) string {
	keys := make([]string, 0, len(opts))
	for key := range opts {
		if key == optionBaseKey || key == sessionTorrentFinalizedOption || key == sessionExplicitOptionsOption || key == "gid" || key == "pause" {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	encoded, _ := json.Marshal(keys)
	return base64.RawURLEncoding.EncodeToString(encoded)
}

func restoreSessionOptionLayers(effective Options, explicit map[string]struct{}) Options {
	base := make(Options, len(effective))
	overlay := make(Options, len(explicit)+2)
	for key, value := range effective {
		if key == "gid" || key == "pause" {
			overlay[key] = normalizeOptionValue(value)
			continue
		}
		if _, ok := explicit[key]; ok {
			overlay[key] = normalizeOptionValue(value)
		} else {
			base[key] = normalizeOptionValue(value)
		}
	}
	return layerOptions(base, overlay)
}

func validateRestoredOptionLayers(opts Options) error {
	if err := validateOptionLayerSize(opts); err != nil {
		return fmt.Errorf("too many explicit session options: %w", err)
	}
	if base, ok := opts[optionBaseKey].(Options); ok {
		if err := validateOptionLayerSize(base); err != nil {
			return fmt.Errorf("too many inherited session options: %w", err)
		}
	}
	return nil
}

func (i *sessionItem) addOption(key string, value any) error {
	if key == "" || len(key) > 256 || strings.ContainsRune(key, '=') || strings.IndexFunc(key, unicode.IsSpace) >= 0 {
		return fmt.Errorf("invalid option key %q", key)
	}
	valueText := fmt.Sprint(value)
	if len(valueText) > maxDownloadOptionValueSize {
		return fmt.Errorf("value for option %q is too large", key)
	}
	if i.options == nil {
		i.options = Options{}
	}
	if previous, ok := i.options[key]; ok {
		switch p := previous.(type) {
		case []string:
			if len(p) >= maxSessionOptionValues {
				return fmt.Errorf("too many values for option %q", key)
			}
			i.options[key] = append(p, valueText)
		default:
			i.options[key] = []string{fmt.Sprint(p), valueText}
		}
		return nil
	}
	if len(i.options) >= maxSessionOptionKeys {
		return fmt.Errorf("too many options")
	}
	i.options[key] = value
	return nil
}

func isSessionOptionLine(line string) bool {
	if line == "" {
		return false
	}
	switch line[0] {
	case ' ', '\t':
		return true
	default:
		return false
	}
}

func (e *Engine) SaveSession() (string, error) {
	e.sessionMu.Lock()
	defer e.sessionMu.Unlock()

	e.mu.RLock()
	path := optionString(e.global, "save-session")
	e.mu.RUnlock()
	if path == "" {
		return "OK", nil
	}
	entries := e.sessionSnapshot()
	if err := writeSessionFile(path, entries); err != nil {
		return "", err
	}
	return "OK", nil
}

func (e *Engine) saveSessionIfConfigured() error {
	e.mu.RLock()
	configured := optionString(e.global, "save-session") != ""
	e.mu.RUnlock()
	if !configured {
		return nil
	}
	_, err := e.SaveSession()
	return err
}

func (e *Engine) saveSessionBestEffort() {
	e.cancelQueuedSessionSave()
	if err := e.saveSessionIfConfigured(); err != nil {
		e.log.Warn("save session failed", zap.Error(err))
	}
}

func (e *Engine) queueSessionSaveBestEffort() {
	e.mu.RLock()
	configured := optionString(e.global, "save-session") != ""
	e.mu.RUnlock()
	if !configured {
		return
	}
	e.sessionSaveMu.Lock()
	if e.sessionSaveClosed || e.sessionSavePending {
		e.sessionSaveMu.Unlock()
		return
	}
	e.sessionSavePending = true
	e.sessionSaveWG.Add(1)
	var timer *time.Timer
	timer = time.AfterFunc(250*time.Millisecond, func() {
		defer e.sessionSaveWG.Done()
		e.sessionSaveMu.Lock()
		if e.sessionSaveTimer == timer {
			e.sessionSavePending = false
			e.sessionSaveTimer = nil
		}
		closed := e.sessionSaveClosed
		e.sessionSaveMu.Unlock()
		if closed {
			return
		}
		if err := e.saveSessionIfConfigured(); err != nil {
			e.log.Warn("save session failed", zap.Error(err))
		}
	})
	e.sessionSaveTimer = timer
	e.sessionSaveMu.Unlock()
}

func (e *Engine) cancelQueuedSessionSave() {
	e.sessionSaveMu.Lock()
	if e.sessionSaveTimer != nil {
		if e.sessionSaveTimer.Stop() {
			e.sessionSaveWG.Done()
		}
		e.sessionSaveTimer = nil
	}
	e.sessionSavePending = false
	e.sessionSaveMu.Unlock()
}

func (e *Engine) closeQueuedSessionSaves() {
	e.sessionSaveMu.Lock()
	e.sessionSaveClosed = true
	if e.sessionSaveTimer != nil {
		if e.sessionSaveTimer.Stop() {
			e.sessionSaveWG.Done()
		}
		e.sessionSaveTimer = nil
	}
	e.sessionSavePending = false
	e.sessionSaveMu.Unlock()
	e.sessionSaveWG.Wait()
}

func (e *Engine) sessionSnapshot() []sessionDownload {
	e.mu.RLock()
	defer e.mu.RUnlock()

	waitingOrder := make(map[string]int, len(e.waiting))
	for i, gid := range e.waiting {
		waitingOrder[gid] = i
	}
	entries := make([]sessionDownload, 0, len(e.downloads))
	for gid, d := range e.downloads {
		d.mu.RLock()
		if d.status == StatusComplete || d.status == StatusRemoved {
			d.mu.RUnlock()
			continue
		}
		var uris []string
		if d.kind == downloadKindTorrent {
			switch {
			case d.torrentMagnet != "":
				uris = []string{d.torrentMagnet}
				for _, u := range d.uris {
					uris = append(uris, u.URI)
				}
			case len(d.torrentData) > 0:
				uris = []string{encodeSessionTorrentDataURI(d.torrentData)}
				for _, u := range d.uris {
					uris = append(uris, u.URI)
				}
			default:
				uris = make([]string, 0, len(d.uris))
				for _, u := range d.uris {
					uris = append(uris, u.URI)
				}
			}
		} else {
			uris = make([]string, 0, len(d.uris))
			for _, u := range d.uris {
				uris = append(uris, u.URI)
			}
		}
		opts := optionsForRPC(d.options)
		delete(opts, sessionTorrentFinalizedOption)
		delete(opts, sessionExplicitOptionsOption)
		opts[sessionExplicitOptionsOption] = encodeSessionExplicitOptions(d.options)
		if d.kind == downloadKindTorrent && len(d.torrentFinalized) != 0 {
			opts[sessionTorrentFinalizedOption] = encodeSessionTorrentFinalizedIndexes(d.torrentFinalized)
		}
		opts["gid"] = d.gid
		delete(opts, "save-session")
		if d.status == StatusPaused {
			opts["pause"] = "true"
		} else {
			opts["pause"] = "false"
		}
		order, ok := waitingOrder[gid]
		if !ok {
			order = len(waitingOrder)
		}
		entries = append(entries, sessionDownload{
			gid:       d.gid,
			uris:      uris,
			options:   opts,
			createdAt: d.createdAt,
			order:     order,
		})
		d.mu.RUnlock()
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].order != entries[j].order {
			return entries[i].order < entries[j].order
		}
		if !entries[i].createdAt.Equal(entries[j].createdAt) {
			return entries[i].createdAt.Before(entries[j].createdAt)
		}
		return entries[i].gid < entries[j].gid
	})
	return entries
}

func writeSessionFile(path string, entries []sessionDownload) error {
	serializableEntries := 0
	for _, entry := range entries {
		if len(entry.uris) == 0 {
			continue
		}
		if err := validateSessionDownload(entry); err != nil {
			return err
		}
		serializableEntries++
		if serializableEntries > maxSessionDownloads {
			return fmt.Errorf("cannot serialize more than %d session downloads", maxSessionDownloads)
		}
	}

	dir := filepath.Dir(path)
	if dir == "" {
		dir = "."
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	w := bufio.NewWriter(&sessionSizeWriter{writer: tmp, remaining: maxSessionFileSize})
	for _, entry := range entries {
		if len(entry.uris) == 0 {
			continue
		}
		if _, err := fmt.Fprintln(w, strings.Join(entry.uris, "\t")); err != nil {
			_ = tmp.Close()
			return err
		}
		keys := make([]string, 0, len(entry.options))
		for k := range entry.options {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			switch v := entry.options[k].(type) {
			case []string:
				for _, item := range v {
					if _, err := fmt.Fprintf(w, "  %s=%s\n", k, item); err != nil {
						_ = tmp.Close()
						return err
					}
				}
			default:
				if _, err := fmt.Fprintf(w, "  %s=%v\n", k, v); err != nil {
					_ = tmp.Close()
					return err
				}
			}
		}
	}
	if err := w.Flush(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	cleanup = false
	return syncDir(dir)
}

func validateSessionDownload(entry sessionDownload) error {
	uriLimit := maxDownloadURIs
	if len(entry.uris) > 0 && (strings.HasPrefix(entry.uris[0], sessionTorrentDataURIPrefix) || isMagnetURI(entry.uris[0])) {
		uriLimit++
	}
	if len(entry.uris) > uriLimit {
		return fmt.Errorf("cannot serialize session download %s: too many URIs", entry.gid)
	}
	for i, raw := range entry.uris {
		if raw == "" {
			return fmt.Errorf("cannot serialize session download %s: URI %d is empty", entry.gid, i+1)
		}
		if strings.IndexFunc(raw, unicode.IsSpace) >= 0 {
			return fmt.Errorf("cannot serialize session download %s: URI %d contains whitespace", entry.gid, i+1)
		}
		if i == 0 && strings.HasPrefix(raw, "#") {
			return fmt.Errorf("cannot serialize session download %s: first URI is parsed as a comment", entry.gid)
		}
	}
	if len(entry.options) > maxSessionOptionKeys {
		return fmt.Errorf("cannot serialize session download %s: too many options", entry.gid)
	}
	for key, value := range entry.options {
		if key == "" {
			return fmt.Errorf("cannot serialize session download %s: option key is empty", entry.gid)
		}
		if len(key) > 256 || strings.ContainsRune(key, '=') || strings.IndexFunc(key, unicode.IsSpace) >= 0 {
			return fmt.Errorf("cannot serialize session download %s: option key %q contains whitespace or '='", entry.gid, key)
		}
		switch value := value.(type) {
		case []string:
			if len(value) > maxSessionOptionValues {
				return fmt.Errorf("cannot serialize session download %s: too many values for option %q", entry.gid, key)
			}
			for _, item := range value {
				if err := validateSessionOptionValue(entry.gid, key, item); err != nil {
					return err
				}
			}
		default:
			if err := validateSessionOptionValue(entry.gid, key, fmt.Sprint(value)); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateSessionOptionValue(gid, key, value string) error {
	if len(value) > maxDownloadOptionValueSize {
		return fmt.Errorf("cannot serialize session download %s: option %q value is too large", gid, key)
	}
	if strings.ContainsAny(value, "\r\n") {
		return fmt.Errorf("cannot serialize session download %s: option %q value contains a line break", gid, key)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("cannot serialize session download %s: option %q value has leading or trailing whitespace", gid, key)
	}
	return nil
}

type sessionSizeWriter struct {
	writer    io.Writer
	remaining int64
}

func (w *sessionSizeWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, fmt.Errorf("session exceeds maximum size %d", maxSessionFileSize)
	}
	n, err := w.writer.Write(p)
	w.remaining -= int64(n)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return n, err
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
