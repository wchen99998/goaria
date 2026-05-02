package goaria

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestJSONRPCPostBodyChunks(t *testing.T) {
	engine, err := NewEngine(Config{Dir: t.TempDir(), RPCSecret: "chunk-secret"})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	server := httptest.NewServer(NewServer(engine, ServerConfig{RPCSecret: "chunk-secret"}).Handler())
	defer server.Close()

	payloads := [][]byte{
		[]byte(`{"jsonrpc":"2.0","id":"methods","method":"system.listMethods","params":[]}`),
		[]byte(`{"jsonrpc":"2.0","id":"version","method":"aria2.getVersion","params":["token:chunk-secret"]}`),
		[]byte(`[{"jsonrpc":"2.0","id":"n","method":"system.listNotifications","params":[]},{"jsonrpc":"2.0","id":"s","method":"aria2.getGlobalStat","params":["token:chunk-secret"]}]`),
		bytes.Repeat([]byte(" "), 17),
		[]byte(`{"jsonrpc":"2.0","id":"bad","method":"aria2.tellStatus","params":{}}`),
	}
	for _, payload := range payloads {
		for _, chunk := range []int{1, 2, 3, 5, 8, 13, 64} {
			req, err := http.NewRequest(http.MethodPost, server.URL+"/jsonrpc", newChunkedReader(payload, chunk))
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("POST chunk=%d payload=%q: %v", chunk, payload, err)
			}
			body, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if readErr != nil {
				t.Fatal(readErr)
			}
			if resp.StatusCode >= 500 {
				t.Fatalf("server error for chunk=%d payload=%q: status=%d body=%s", chunk, payload, resp.StatusCode, body)
			}
			if len(body) > 0 && resp.StatusCode != http.StatusBadRequest && !json.Valid(body) {
				t.Fatalf("non-JSON RPC body for chunk=%d payload=%q: %s", chunk, payload, body)
			}
		}
	}
}

type chunkedReader struct {
	data  []byte
	chunk int
}

func newChunkedReader(data []byte, chunk int) io.Reader {
	if chunk <= 0 {
		chunk = 1
	}
	return &chunkedReader{data: append([]byte(nil), data...), chunk: chunk}
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := r.chunk
	if n > len(p) {
		n = len(p)
	}
	if n > len(r.data) {
		n = len(r.data)
	}
	copy(p, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}
