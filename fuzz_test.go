package goaria

import (
	"encoding/json"
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
