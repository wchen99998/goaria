package goaria

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"
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
		for _, raw := range item.uris {
			if err := validateHTTPURI(raw); err != nil {
				return fmt.Errorf("%s:%d: %w", path, item.line, err)
			}
		}
		merged := layerOptions(e.global, item.options)
		gid := optionString(merged, "gid")
		if gid == "" {
			gid = randomHex(8)
		} else if !validGID(gid) {
			return fmt.Errorf("%s:%d: %w", path, item.line, ErrInvalidGID)
		}
		if _, exists := e.downloads[gid]; exists {
			return fmt.Errorf("%s:%d: gid already exists", path, item.line)
		}
		d := newDownload(gid, item.uris, merged)
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
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var items []sessionItem
	var current *sessionItem
	lineNo := 0
	flush := func() {
		if current != nil && len(current.uris) > 0 {
			items = append(items, *current)
		}
		current = nil
	}
	for scanner.Scan() {
		lineNo++
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
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
			current.addOption(key, normalizeOptionValue(strings.TrimSpace(value)))
			continue
		}
		flush()
		uris := strings.Fields(trimmed)
		current = &sessionItem{line: lineNo, uris: uris, options: Options{}}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	flush()
	return items, nil
}

func (i *sessionItem) addOption(key string, value any) {
	if i.options == nil {
		i.options = Options{}
	}
	if previous, ok := i.options[key]; ok {
		switch p := previous.(type) {
		case []string:
			i.options[key] = append(p, fmt.Sprint(value))
		default:
			i.options[key] = []string{fmt.Sprint(p), fmt.Sprint(value)}
		}
		return
	}
	i.options[key] = value
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
	if err := e.saveSessionIfConfigured(); err != nil {
		e.log.Warn("save session failed", zap.Error(err))
	}
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
		if isTerminal(d.status) {
			d.mu.RUnlock()
			continue
		}
		uris := make([]string, 0, len(d.uris))
		for _, u := range d.uris {
			uris = append(uris, u.URI)
		}
		opts := optionsForRPC(d.options)
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

	w := bufio.NewWriter(tmp)
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

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
