package jsonrpc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"goaria"
)

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

	engine, err := goaria.NewEngine(goaria.Config{Dir: f.TempDir()})
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { _ = engine.Close(context.Background()) })
	server := httptest.NewServer(NewServer(engine, Config{Secret: "fuzz-secret", MaxRequestSize: 1 << 16}).Handler())
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
