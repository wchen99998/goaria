package goaria

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func FuzzParseContentRangeTotal(f *testing.F) {
	for _, seed := range []string{
		"bytes 0-0/1",
		"bytes 0-1023/4096",
		"bytes */4096",
		"/-2",
		"invalid",
		"",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, s string) {
		total := parseContentRangeTotal(s)
		if total < -1 {
			t.Fatalf("parseContentRangeTotal(%q) = %d", s, total)
		}
	})
}

func FuzzMakeChunks(f *testing.F) {
	f.Add(int64(1), 1, int64(1))
	f.Add(int64(1024), 4, int64(1))
	f.Add(int64(1024*1024+1), 16, int64(1024))
	f.Fuzz(func(t *testing.T, total int64, concurrency int, minSplit int64) {
		if total <= 0 || total > 64<<20 {
			t.Skip()
		}
		chunks := makeChunks(total, concurrency, minSplit)
		if len(chunks) == 0 {
			t.Fatal("no chunks")
		}
		wantStart := int64(0)
		for _, ch := range chunks {
			if ch.start != wantStart {
				t.Fatalf("gap or overlap: got start %d want %d in %#v", ch.start, wantStart, chunks)
			}
			if ch.end < ch.start {
				t.Fatalf("invalid chunk %#v", ch)
			}
			wantStart = ch.end + 1
		}
		if wantStart != total {
			t.Fatalf("chunks cover %d bytes, want %d: %#v", wantStart, total, chunks)
		}
	})
}

func FuzzProxyParsingAndBypass(f *testing.F) {
	for _, seed := range []string{
		"http://127.0.0.1:8080",
		"socks5://user:pass@localhost:1080",
		"proxy.internal:3128",
		"",
		"://bad",
	} {
		f.Add(seed, "localhost,127.0.0.0/8", "127.0.0.1")
	}
	f.Fuzz(func(t *testing.T, proxyURL, noProxy, host string) {
		if proxyURL != "" {
			_, _ = normalizeProxyURL(proxyURL)
		}
		_ = shouldBypassProxy(host, splitList(noProxy))
	})
}

func FuzzOptionsAndSizes(f *testing.F) {
	for _, seed := range []struct {
		size    string
		boolVal string
		version string
	}{
		{"1K", "true", "h2"},
		{"1.5M", "false", "HTTP/3"},
		{"-1", "yes", "auto"},
		{"9223372036854775808G", "0", "weird"},
		{"", "", ""},
	} {
		f.Add(seed.size, seed.boolVal, seed.version)
	}

	f.Fuzz(func(t *testing.T, sizeRaw, boolRaw, versionRaw string) {
		if len(sizeRaw) > 256 || len(boolRaw) > 256 || len(versionRaw) > 256 {
			t.Skip()
		}
		opts := normalizeOptions(map[string]any{
			"size":         sizeRaw,
			"enabled":      boolRaw,
			"http-version": versionRaw,
			"header":       []any{sizeRaw, boolRaw},
		})
		if got := parseSize(sizeRaw, 17); got < 0 {
			t.Fatalf("parseSize(%q) = %d", sizeRaw, got)
		}
		_ = optionBool(opts, "enabled")
		_ = optionBytes(opts, "size", 17)
		_ = optionStringList(opts, "header")
		_ = optionsForRPC(opts)
		_ = normalizeHTTPVersion(versionRaw)
	})
}

func FuzzPathsAndBitfield(f *testing.F) {
	for _, seed := range []struct {
		dir       string
		out       string
		uri       string
		total     int64
		completed int64
		piece     int64
	}{
		{"/tmp/goaria", "file.txt", "https://example.com/file.txt", 10, 5, 2},
		{"", "", "https://example.com/", 1, 1, 1},
		{"downloads", "../escape", "not a uri", 1024, 1024, 128},
		{".", "", "https://example.com/a?b=c", 0, 0, 0},
	} {
		f.Add(seed.dir, seed.out, seed.uri, seed.total, seed.completed, seed.piece)
	}

	f.Fuzz(func(t *testing.T, dir, out, rawURI string, total, completed, piece int64) {
		if len(dir) > 512 || len(out) > 512 || len(rawURI) > 2048 {
			t.Skip()
		}
		name := filenameFromURI(rawURI)
		if name == "" || strings.ContainsRune(name, filepath.Separator) {
			t.Fatalf("filenameFromURI(%q) = %q", rawURI, name)
		}
		path := resolveOutputPath(dir, out, name, rawURI)
		if path == "" {
			t.Fatalf("resolveOutputPath(%q, %q, %q, %q) returned empty", dir, out, name, rawURI)
		}
		if total < 0 || total > 1<<20 || piece < 0 || piece > 1<<20 {
			t.Skip()
		}
		if completed < 0 {
			completed = 0
		}
		if completed > total && total > 0 {
			completed = total
		}
		got := bitfieldFor(total, completed, piece)
		if got != "" && len(got)%2 != 0 {
			t.Fatalf("bitfield has odd hex length: %q", got)
		}
	})
}

func FuzzAddTorrentMetadata(f *testing.F) {
	f.Add([]byte("not a torrent"))
	if data, err := os.ReadFile("test.torrent"); err == nil {
		f.Add(data)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 || len(data) > 256<<10 {
			t.Skip()
		}
		engine, err := NewEngine(Config{Dir: t.TempDir(), MaxConcurrentDownloads: 1})
		if err != nil {
			t.Fatal(err)
		}
		defer engine.Close(context.Background())
		_, _ = engine.AddTorrent(base64.StdEncoding.EncodeToString(data), nil, Options{"pause": "true"}, nil)
	})
}
