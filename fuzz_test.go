package goaria

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func FuzzParseJSONRPCCall(f *testing.F) {
	for _, seed := range []string{
		`{"jsonrpc":"2.0","id":"1","method":"system.listMethods"}`,
		`{"jsonrpc":"2.0","id":null,"method":"aria2.tellStatus","params":["0123456789abcdef"]}`,
		`[]`,
		`{"params":{}}`,
		`{"method":"0"}0`,
		`not-json`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, payload string) {
		call, err := parseCall([]byte(payload))
		if err == nil {
			if call.Method != "" && !json.Valid([]byte(payload)) {
				t.Fatalf("parsed invalid JSON payload: %q", payload)
			}
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

func FuzzJSONRPCOverHTTPChunks(f *testing.F) {
	for _, seed := range []string{
		`{"jsonrpc":"2.0","id":"m","method":"system.listMethods","params":[]}`,
		`{"jsonrpc":"2.0","id":"n","method":"system.listNotifications","params":[]}`,
		`{"jsonrpc":"2.0","id":"v","method":"aria2.getVersion","params":["token:fuzz-secret"]}`,
		`[{"jsonrpc":"2.0","id":"a","method":"system.listMethods","params":[]}]`,
		`{"jsonrpc":"2.0","id":"bad","method":"aria2.tellStatus","params":{}}`,
		`not-json`,
	} {
		f.Add(seed, uint8(1))
	}

	engine, err := NewEngine(Config{Dir: f.TempDir(), RPCSecret: "fuzz-secret"})
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { _ = engine.Close(context.Background()) })
	server := httptest.NewServer(NewServer(engine, ServerConfig{RPCSecret: "fuzz-secret", MaxRequestSize: 1 << 16}).Handler())
	f.Cleanup(server.Close)

	f.Fuzz(func(t *testing.T, payload string, chunkByte uint8) {
		if len(payload) > 1<<14 {
			t.Skip()
		}
		chunk := int(chunkByte%31) + 1
		req, err := http.NewRequest(http.MethodPost, server.URL+"/jsonrpc", newChunkedReader([]byte(payload), chunk))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode >= 500 {
			t.Fatalf("server returned %d for chunk %d and payload %q: %s", resp.StatusCode, chunk, payload, body)
		}
		if len(body) > 0 && resp.StatusCode != http.StatusBadRequest && !json.Valid(body) {
			t.Fatalf("invalid JSON response for chunk %d and payload %q: %s", chunk, payload, body)
		}
	})
}

func FuzzBuildGETPayload(f *testing.F) {
	for _, seed := range []struct {
		method string
		id     string
		params string
	}{
		{"aria2.tellStatus", "foo", `["0123456789abcdef"]`},
		{"system.listMethods", "methods", `[]`},
		{"", "", `{"jsonrpc":"2.0","id":"raw","method":"system.listMethods"}`},
		{"aria2.addUri", "add", `[[ "https://example.com/file" ],{"out":"file"}]`},
		{"aria2.tellStatus", "bad", `{}`},
		{"", "", `not-json`},
	} {
		f.Add(seed.method, seed.id, seed.params)
	}

	f.Fuzz(func(t *testing.T, method, id, params string) {
		if len(method) > 256 || len(id) > 256 || len(params) > 1<<14 {
			t.Skip()
		}
		q := url.Values{}
		if method != "" {
			q.Set("method", method)
		}
		if id != "" {
			q.Set("id", id)
		}
		if params != "" {
			q.Set("params", base64.StdEncoding.EncodeToString([]byte(params)))
		}
		req := httptest.NewRequest(http.MethodGet, "/jsonrpc?"+q.Encode(), nil)
		payload, err := buildGETPayload(req)
		if err != nil {
			return
		}
		if method != "" && !json.Valid(payload) {
			t.Fatalf("method GET payload is invalid JSON: %s", payload)
		}
		if method == "" && len(payload) == 0 {
			t.Fatal("raw GET payload should not be empty on success")
		}
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
