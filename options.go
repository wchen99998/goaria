package goaria

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// Options is the aria2 option struct shape. Values are normally strings; for
// options like "header", aria2 also accepts an array of strings.
type Options map[string]any

const optionBaseKey = "\x00goaria.base-options"

const (
	maxDownloadOptionKeys      = 128
	maxDownloadOptionListItems = 64
	maxDownloadOptionValueSize = 1 << 20
)

func defaultOptions(dir string, maxConcurrent, maxResult int) Options {
	if maxConcurrent <= 0 {
		maxConcurrent = 5
	}
	if maxResult <= 0 {
		maxResult = 1000
	}
	return Options{
		"dir":                             dir,
		"split":                           "5",
		"max-connection-per-server":       "1",
		"min-split-size":                  "20M",
		"max-concurrent-downloads":        strconv.Itoa(maxConcurrent),
		"max-download-result":             strconv.Itoa(maxResult),
		"keep-unfinished-download-result": "true",
		"max-download-limit":              "0",
		"max-overall-download-limit":      "0",
		"allow-overwrite":                 "false",
		"auto-file-renaming":              "true",
		"continue":                        "false",
		"follow-metalink":                 "false",
		"follow-torrent":                  "false",
		"lowest-speed-limit":              "0",
		"max-file-not-found":              "0",
		"goaria-http-segment-size":        "50M",
		"goaria-max-torrent-size":         "16M",
		"max-tries":                       "5",
		"retry-wait":                      "0",
		"connect-timeout":                 "60",
		"timeout":                         "60",
		"check-certificate":               "true",
		"enable-http-keep-alive":          "true",
		"http-version":                    "auto",
		"remote-time":                     "false",
		"user-agent":                      "goaria",
	}
}

func cloneOptions(in Options) Options {
	out := make(Options, len(in))
	for k, v := range in {
		if k == optionBaseKey {
			out[k] = v
			continue
		}
		switch vv := v.(type) {
		case []string:
			cp := append([]string(nil), vv...)
			out[k] = cp
		case []any:
			cp := append([]any(nil), vv...)
			out[k] = cp
		default:
			out[k] = v
		}
	}
	return out
}

func layerOptions(base Options, overlay Options) Options {
	out := make(Options, len(overlay)+1)
	out[optionBaseKey] = base
	for k, v := range overlay {
		if k == optionBaseKey || k == sessionTorrentFinalizedOption || k == sessionExplicitOptionsOption {
			continue
		}
		out[k] = normalizeOptionValue(v)
	}
	return out
}

func mergeOptions(base Options, overlay Options) Options {
	out := cloneOptions(base)
	for k, v := range overlay {
		if k == optionBaseKey || k == sessionTorrentFinalizedOption || k == sessionExplicitOptionsOption {
			continue
		}
		out[k] = normalizeOptionValue(v)
	}
	return out
}

func normalizeOptionValue(v any) any {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []string:
		return append([]string(nil), x...)
	case []any:
		items := make([]string, 0, len(x))
		for _, item := range x {
			items = append(items, fmt.Sprint(item))
		}
		return items
	case json.Number:
		return x.String()
	case float64:
		if math.Trunc(x) == x {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case bool:
		if x {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprint(x)
	}
}

func (d *Download) optionSnapshot() Options {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return cloneOptions(d.options)
}

func optionValue(opts Options, key string) (any, bool) {
	if opts == nil {
		return nil, false
	}
	v, ok := opts[key]
	if ok {
		return v, v != nil
	}
	if base, ok := opts[optionBaseKey].(Options); ok {
		return optionValue(base, key)
	}
	return nil, false
}

func optionString(opts Options, key string) string {
	v, ok := optionValue(opts, key)
	if !ok {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case []string:
		return strings.Join(x, "\n")
	default:
		return fmt.Sprint(x)
	}
}

func optionPresent(opts Options, key string) bool {
	_, ok := optionValue(opts, key)
	return ok
}

func optionExplicit(opts Options, key string) bool {
	if opts == nil {
		return false
	}
	_, ok := opts[key]
	return ok
}

func optionStringList(opts Options, key string) []string {
	v, ok := optionValue(opts, key)
	if !ok {
		return nil
	}
	switch x := v.(type) {
	case []string:
		return append([]string(nil), x...)
	case []any:
		out := make([]string, 0, len(x))
		for _, item := range x {
			out = append(out, fmt.Sprint(item))
		}
		return out
	case string:
		if x == "" {
			return nil
		}
		return []string{x}
	default:
		return []string{fmt.Sprint(x)}
	}
}

func optionBool(opts Options, key string) bool {
	switch strings.ToLower(optionString(opts, key)) {
	case "true", "yes", "1":
		return true
	default:
		return false
	}
}

func optionInt(opts Options, key string, fallback int) int {
	s := strings.TrimSpace(optionString(opts, key))
	if s == "" {
		return fallback
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return fallback
	}
	return n
}

func optionBytes(opts Options, key string, fallback int64) int64 {
	return parseSize(optionString(opts, key), fallback)
}

func parseSize(s string, fallback int64) int64 {
	n, err := parseSizeValue(s)
	if err != nil {
		return fallback
	}
	return n
}

func parseSizeValue(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	mul := int64(1)
	last := s[len(s)-1]
	switch last {
	case 'K', 'k':
		mul = 1024
		s = s[:len(s)-1]
	case 'M', 'm':
		mul = 1024 * 1024
		s = s[:len(s)-1]
	case 'G', 'g':
		mul = 1024 * 1024 * 1024
		s = s[:len(s)-1]
	}
	s = strings.TrimSpace(s)
	if !validDecimalSizeNumber(s) {
		return 0, fmt.Errorf("invalid size")
	}
	// ParseFloat is used only as a cheap exponent/range guard. The final
	// conversion is rational so values near MaxInt64 are not rounded to 2^63.
	if _, err := strconv.ParseFloat(s, 64); err != nil {
		return 0, fmt.Errorf("invalid size")
	}
	n, ok := new(big.Rat).SetString(s)
	if !ok || n.Sign() < 0 {
		return 0, fmt.Errorf("invalid size")
	}
	n.Mul(n, big.NewRat(mul, 1))
	integer := new(big.Int).Quo(n.Num(), n.Denom())
	if !integer.IsInt64() {
		return 0, fmt.Errorf("invalid size")
	}
	return integer.Int64(), nil
}

func validDecimalSizeNumber(s string) bool {
	if s == "" {
		return false
	}
	digits := 0
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
		digits++
	}
	if i < len(s) && s[i] == '.' {
		i++
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
			digits++
		}
	}
	if digits == 0 {
		return false
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			i++
		}
		exponentDigits := 0
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
			exponentDigits++
		}
		if exponentDigits == 0 {
			return false
		}
	}
	return i == len(s)
}

func validateOptionValues(opts Options) error {
	return validateOptionValuesWithLimit(opts, maxDownloadOptionKeys)
}

func validateOptionValuesWithLimit(opts Options, maxKeys int) error {
	if err := validateOptionCount(opts, maxKeys); err != nil {
		return err
	}
	for key := range opts {
		if key == "" || len(key) > 256 || key == optionBaseKey || key == sessionTorrentFinalizedOption || key == sessionExplicitOptionsOption || strings.ContainsRune(key, '=') || strings.IndexFunc(key, unicode.IsSpace) >= 0 {
			return fmt.Errorf("invalid option key %q", key)
		}
		values := []string{optionString(opts, key)}
		switch raw := opts[key].(type) {
		case []string:
			if len(raw) > maxDownloadOptionListItems {
				return fmt.Errorf("too many values for option %q: %d", key, len(raw))
			}
			values = raw
		case []any:
			if len(raw) > maxDownloadOptionListItems {
				return fmt.Errorf("too many values for option %q: %d", key, len(raw))
			}
			values = make([]string, len(raw))
			for i := range raw {
				values[i] = fmt.Sprint(raw[i])
			}
		}
		for _, item := range values {
			if len(item) > maxDownloadOptionValueSize {
				return fmt.Errorf("value for option %q is too large", key)
			}
			if strings.ContainsAny(item, "\r\n") || strings.TrimSpace(item) != item {
				return fmt.Errorf("invalid value for option %q: leading/trailing whitespace and line breaks are not supported", key)
			}
		}
		value := optionString(opts, key)
		switch key {
		case "allow-overwrite", "auto-file-renaming", "bt-metadata-only", "check-certificate", "conditional-get", "continue", "disable-ipv6", "disable-utp", "dry-run", "enable-http-keep-alive", "follow-metalink", "follow-torrent", "goaria-disable-dht", "goaria-disable-pex", "goaria-disable-trackers", "goaria-disable-utp", "goaria-torrent-disable-smart-ban", "goaria-torrent-no-upload", "http-accept-gzip", "http-auth-challenge", "http-no-cache", "keep-unfinished-download-result", "parameterized-uri", "pause", "remote-time", "rpc-save-upload-metadata", "use-head":
			if strings.TrimSpace(value) != value {
				return fmt.Errorf("invalid value for option %q: %q", key, value)
			}
			switch strings.ToLower(value) {
			case "true", "false", "yes", "no", "1", "0":
			default:
				return fmt.Errorf("invalid boolean value for option %q: %q", key, value)
			}
		case "bt-max-peers", "connect-timeout", "goaria-torrent-max-active-files", "listen-port", "max-concurrent-downloads", "max-connection-per-server", "max-download-result", "max-file-not-found", "max-tries", "retry-wait", "split", "timeout":
			if strings.TrimSpace(value) != value {
				return fmt.Errorf("invalid value for option %q: %q", key, value)
			}
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 {
				return fmt.Errorf("invalid non-negative integer for option %q: %q", key, value)
			}
			if (key == "connect-timeout" || key == "retry-wait" || key == "timeout") && int64(n) > int64(math.MaxInt64)/int64(time.Second) {
				return fmt.Errorf("duration for option %q is too large: %q", key, value)
			}
			if key == "listen-port" && n > 65535 {
				return fmt.Errorf("invalid port for option %q: %q", key, value)
			}
		case "goaria-http-segment-size", "goaria-max-torrent-size", "goaria-torrent-readahead", "lowest-speed-limit", "max-download-limit", "max-overall-download-limit", "max-upload-limit", "min-split-size":
			if strings.TrimSpace(value) != value {
				return fmt.Errorf("invalid value for option %q: %q", key, value)
			}
			size, err := parseSizeValue(value)
			if err != nil {
				return fmt.Errorf("invalid size for option %q: %q", key, value)
			}
			if key == "goaria-max-torrent-size" && size > maxSupportedTorrentSize {
				return fmt.Errorf("option %q exceeds supported maximum %d", key, maxSupportedTorrentSize)
			}
		case "http-version":
			if strings.TrimSpace(value) != value {
				return fmt.Errorf("invalid value for option %q: %q", key, value)
			}
			switch strings.ToLower(value) {
			case "", "auto", "1", "1.1", "2", "2.0", "3", "3.0", "http/1.1", "http/2", "http/3", "h1", "h2", "h3":
			default:
				return fmt.Errorf("invalid HTTP version %q", value)
			}
		}
	}
	return nil
}

func validateOptionLayerSize(opts Options) error {
	count := 0
	for key := range opts {
		switch key {
		case optionBaseKey, sessionTorrentFinalizedOption, sessionExplicitOptionsOption, "gid", "pause":
			continue
		}
		count++
	}
	if count > maxDownloadOptionKeys {
		return fmt.Errorf("too many download options: %d", count)
	}
	return nil
}

func validateOptionCount(opts Options, maxKeys int) error {
	count := 0
	for key := range opts {
		switch key {
		case optionBaseKey, sessionTorrentFinalizedOption, sessionExplicitOptionsOption:
			continue
		}
		count++
	}
	if count > maxKeys {
		return fmt.Errorf("too many download options: %d", count)
	}
	return nil
}

func optionsForRPC(opts Options) map[string]any {
	out := make(map[string]any, len(opts))
	if base, ok := opts[optionBaseKey].(Options); ok {
		out = optionsForRPC(base)
	}
	for k, v := range opts {
		if k == optionBaseKey || k == sessionTorrentFinalizedOption || k == sessionExplicitOptionsOption {
			continue
		}
		switch x := v.(type) {
		case []string:
			out[k] = append([]string(nil), x...)
		default:
			out[k] = fmt.Sprint(x)
		}
	}
	return out
}
