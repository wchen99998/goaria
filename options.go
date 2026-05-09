package goaria

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Options is the aria2 option struct shape. Values are normally strings; for
// options like "header", aria2 also accepts an array of strings.
type Options map[string]any

const optionBaseKey = "\x00goaria.base-options"

func defaultOptions(dir string, maxConcurrent, maxResult int) Options {
	if maxConcurrent <= 0 {
		maxConcurrent = 5
	}
	if maxResult <= 0 {
		maxResult = 1000
	}
	return Options{
		"dir":                        dir,
		"split":                      "4",
		"max-connection-per-server":  "4",
		"min-split-size":             "1M",
		"max-concurrent-downloads":   strconv.Itoa(maxConcurrent),
		"max-download-result":        strconv.Itoa(maxResult),
		"max-download-limit":         "0",
		"max-overall-download-limit": "0",
		"allow-overwrite":            "true",
		"auto-file-renaming":         "false",
		"continue":                   "true",
		"follow-metalink":            "false",
		"follow-torrent":             "false",
		"lowest-speed-limit":         "0",
		"max-file-not-found":         "0",
		"max-tries":                  "5",
		"retry-wait":                 "0",
		"connect-timeout":            "60",
		"timeout":                    "60",
		"check-certificate":          "true",
		"enable-http-keep-alive":     "true",
		"http-version":               "auto",
		"remote-time":                "false",
		"user-agent":                 "goaria",
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
		if k == optionBaseKey {
			continue
		}
		out[k] = normalizeOptionValue(v)
	}
	return out
}

func mergeOptions(base Options, overlay Options) Options {
	out := cloneOptions(base)
	for k, v := range overlay {
		if k == optionBaseKey {
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

func normalizeOptions(in map[string]any) Options {
	out := make(Options, len(in))
	for k, v := range in {
		if k == optionBaseKey {
			continue
		}
		out[k] = normalizeOptionValue(v)
	}
	return out
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
	s = strings.TrimSpace(s)
	if s == "" {
		return fallback
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
	n, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	maxInt64PlusOne := float64(uint64(1) << 63)
	if err != nil || n < 0 || math.IsNaN(n) || math.IsInf(n, 0) || n >= maxInt64PlusOne/float64(mul) {
		return fallback
	}
	return int64(n * float64(mul))
}

func optionsForRPC(opts Options) map[string]any {
	out := make(map[string]any, len(opts))
	if base, ok := opts[optionBaseKey].(Options); ok {
		out = optionsForRPC(base)
	}
	for k, v := range opts {
		if k == optionBaseKey {
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
