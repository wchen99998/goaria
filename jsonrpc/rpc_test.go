package jsonrpc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wchen99998/goaria"
)

func TestRPCMethodSurfaceAndToken(t *testing.T) {
	engine, err := goaria.NewEngine(goaria.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	rpc := NewHandler(engine, "secret")

	resp := invokeRPC(t, rpc, `{"jsonrpc":"2.0","id":"m","method":"system.listMethods"}`)
	methods := stringSet(resp.Result)
	for _, method := range rpcMethods {
		if !methods[method] {
			t.Fatalf("method %s missing from listMethods", method)
		}
	}
	if methods["aria2.addMetalink"] {
		t.Fatal("listMethods advertised unsupported aria2.addMetalink")
	}

	resp = invokeRPC(t, rpc, `{"jsonrpc":"2.0","id":"v","method":"aria2.getVersion"}`)
	if resp.Error == nil || !strings.Contains(resp.Error.Message, "Unauthorized") {
		t.Fatalf("expected Unauthorized error, got %#v", resp)
	}

	resp = invokeRPC(t, rpc, `{"jsonrpc":"2.0","id":"v","method":"aria2.getVersion","params":["token:secret"]}`)
	if resp.Error != nil {
		t.Fatalf("getVersion with token failed: %#v", resp.Error)
	}

	resp = invokeRPC(t, rpc, `{"jsonrpc":"2.0","id":"m","method":"aria2.addMetalink","params":["token:secret",""]}`)
	if resp.Error == nil || !strings.Contains(resp.Error.Message, "unsupported") {
		t.Fatalf("expected unsupported addMetalink error, got %#v", resp)
	}
}

func TestRPCMalformedSinglePayloadReturnsParseError(t *testing.T) {
	engine, err := goaria.NewEngine(goaria.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	rpc := NewHandler(engine, "")

	for _, payload := range []string{
		`not-json`,
		`{"jsonrpc":"2.0","id":"bad","method":"system.listMethods"} trailing`,
		`{"jsonrpc":"2.0","id":"bad","method":`,
	} {
		data, ok := rpc.HandlePayload([]byte(payload))
		if !ok {
			t.Fatalf("no response for %q", payload)
		}
		var resp rpcResponse
		if err := json.Unmarshal(data, &resp); err != nil {
			t.Fatal(err)
		}
		if resp.Error == nil || resp.Error.Code != rpcParseError {
			t.Fatalf("payload %q returned %#v, want parse error", payload, resp)
		}
	}

	resp := invokeRPC(t, rpc, `{}`)
	if resp.Error == nil || resp.Error.Code != rpcInvalidRequest {
		t.Fatalf("empty object returned %#v, want invalid request", resp)
	}
}

func TestRPCPostGetBatchAndMulticall(t *testing.T) {
	data := []byte("hello rpc")
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "rpc.txt", time.Now(), bytes.NewReader(data))
	}))
	defer src.Close()

	engine, err := goaria.NewEngine(goaria.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	server := httptest.NewServer(NewServer(engine, Config{}).Handler())
	defer server.Close()

	postBody := map[string]any{
		"jsonrpc": "2.0",
		"id":      "add",
		"method":  "aria2.addUri",
		"params":  []any{[]string{src.URL + "/rpc.txt"}, map[string]string{"out": "rpc.txt"}},
	}
	body, _ := json.Marshal(postBody)
	httpResp, err := http.Post(server.URL+"/jsonrpc", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer httpResp.Body.Close()
	var add rpcResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&add); err != nil {
		t.Fatal(err)
	}
	if add.Error != nil {
		t.Fatalf("addUri failed: %#v", add.Error)
	}
	var gid string
	raw, _ := json.Marshal(add.Result)
	if err := json.Unmarshal(raw, &gid); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, engine, gid, goaria.StatusComplete)

	params := base64.StdEncoding.EncodeToString([]byte(`["` + gid + `",["gid","status"]]`))
	httpResp, err = http.Get(server.URL + "/jsonrpc?method=aria2.tellStatus&id=get&params=" + params)
	if err != nil {
		t.Fatal(err)
	}
	defer httpResp.Body.Close()
	var status rpcResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	if status.Error != nil {
		t.Fatalf("tellStatus over GET failed: %#v", status.Error)
	}

	rawParams := url.QueryEscape(`["` + gid + `",["gid","status"]]`)
	httpResp, err = http.Get(server.URL + "/jsonrpc?method=aria2.tellStatus&id=get-raw&params=" + rawParams)
	if err != nil {
		t.Fatal(err)
	}
	defer httpResp.Body.Close()
	status = rpcResponse{}
	if err := json.NewDecoder(httpResp.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	if status.Error != nil {
		t.Fatalf("tellStatus over raw GET params failed: %#v", status.Error)
	}

	multi := map[string]any{
		"jsonrpc": "2.0",
		"id":      "multi",
		"method":  "system.multicall",
		"params": []any{[]any{
			map[string]any{"methodName": "system.listMethods", "params": []any{}},
			map[string]any{"methodName": "aria2.getGlobalStat", "params": []any{}},
		}},
	}
	body, _ = json.Marshal(multi)
	httpResp, err = http.Post(server.URL+"/jsonrpc", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer httpResp.Body.Close()
	var mc rpcResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&mc); err != nil {
		t.Fatal(err)
	}
	if mc.Error != nil {
		t.Fatalf("multicall failed: %#v", mc.Error)
	}
}

func TestRPCAddTorrentWithRealTorrentMetadata(t *testing.T) {
	data, err := os.ReadFile("../test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	engine, err := goaria.NewEngine(goaria.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	rpc := NewHandler(engine, "")

	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      "torrent",
		"method":  "aria2.addTorrent",
		"params": []any{
			base64.StdEncoding.EncodeToString(data),
			map[string]any{"pause": "true"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp := invokeRPC(t, rpc, string(payload))
	if resp.Error != nil {
		t.Fatalf("addTorrent failed: %#v", resp.Error)
	}
	var gid string
	raw, _ := json.Marshal(resp.Result)
	if err := json.Unmarshal(raw, &gid); err != nil {
		t.Fatal(err)
	}

	payload, err = json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      "status",
		"method":  "aria2.tellStatus",
		"params":  []any{gid, []string{"status", "infoHash", "bittorrent", "files"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp = invokeRPC(t, rpc, string(payload))
	if resp.Error != nil {
		t.Fatalf("tellStatus failed: %#v", resp.Error)
	}
	var status map[string]any
	raw, _ = json.Marshal(resp.Result)
	if err := json.Unmarshal(raw, &status); err != nil {
		t.Fatal(err)
	}
	if status["status"] != string(goaria.StatusPaused) {
		t.Fatalf("status = %v, want paused", status["status"])
	}
	if len(status["infoHash"].(string)) != 40 {
		t.Fatalf("bad infoHash: %#v", status["infoHash"])
	}
	if _, ok := status["bittorrent"].(map[string]any); !ok {
		t.Fatalf("missing bittorrent metadata: %#v", status)
	}
	if files, ok := status["files"].([]any); !ok || len(files) == 0 {
		t.Fatalf("missing files: %#v", status["files"])
	}
}

func TestRPCAddTorrentAcceptsTorrentURLSource(t *testing.T) {
	data, err := os.ReadFile("../test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-bittorrent")
		_, _ = w.Write(data)
	}))
	defer server.Close()
	engine, err := goaria.NewEngine(goaria.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	rpc := NewHandler(engine, "")

	webseed := "https://cdn.example.com/payload-file"
	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      "torrent-url",
		"method":  "aria2.addTorrent",
		"params": []any{
			server.URL + "/file.torrent",
			[]string{webseed},
			map[string]any{"pause": "true", "gid": "4444444444444444"},
			0,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp := invokeRPC(t, rpc, string(payload))
	if resp.Error != nil {
		t.Fatalf("addTorrent URL failed: %#v", resp.Error)
	}
	if resp.Result != "4444444444444444" {
		t.Fatalf("gid = %#v, want 4444444444444444", resp.Result)
	}
	uris, err := engine.GetURIs("4444444444444444")
	if err != nil {
		t.Fatal(err)
	}
	if len(uris) != 1 || uris[0].URI != webseed {
		t.Fatalf("torrent webseeds = %#v, want only %q", uris, webseed)
	}
}

func TestRPCAddTorrentURLAutoSavedSessionRestoresWithoutGracefulClose(t *testing.T) {
	data, err := os.ReadFile("../test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-bittorrent")
		_, _ = w.Write(data)
	}))
	defer source.Close()

	dir := t.TempDir()
	sessionPath := filepath.Join(dir, "goaria.session")
	engine, err := goaria.NewEngine(goaria.Config{Dir: dir, SaveSession: sessionPath})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	if _, err := engine.ChangeGlobalOption(goaria.Options{"max-concurrent-downloads": "0"}); err != nil {
		t.Fatal(err)
	}
	rpc := NewHandler(engine, "")

	const gid = "5555555555555555"
	webseed := "https://cdn.example.com/payload-file"
	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      "torrent-url-session",
		"method":  "aria2.addTorrent",
		"params": []any{
			source.URL + "/file.torrent",
			[]string{webseed},
			map[string]any{"gid": gid},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp := invokeRPC(t, rpc, string(payload))
	if resp.Error != nil {
		t.Fatalf("addTorrent URL failed: %#v", resp.Error)
	}
	if resp.Result != gid {
		t.Fatalf("gid = %#v, want %s", resp.Result, gid)
	}

	sessionData, err := os.ReadFile(sessionPath)
	if err != nil {
		t.Fatal(err)
	}
	session := string(sessionData)
	firstLine, _, _ := strings.Cut(session, "\n")
	if !strings.Contains(session, "gid="+gid) || !strings.Contains(session, "pause=false") {
		t.Fatalf("auto-saved session did not keep queued torrent state:\n%s", session)
	}
	if strings.Contains(firstLine, source.URL) {
		t.Fatalf("session URI line still depends on source torrent URL:\n%s", session)
	}
	if !strings.Contains(firstLine, webseed) {
		t.Fatalf("session URI line did not preserve webseed:\n%s", session)
	}

	source.Close()
	restored, err := goaria.NewEngine(goaria.Config{Dir: dir, InputFile: sessionPath})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close(context.Background())
	status, err := restored.TellStatus(gid, []string{"status", "infoHash", "bittorrent"})
	if err != nil {
		t.Fatal(err)
	}
	if status["status"] != string(goaria.StatusWaiting) && status["status"] != string(goaria.StatusActive) {
		t.Fatalf("restored status = %v, want waiting or active", status["status"])
	}
	if len(status["infoHash"].(string)) != 40 {
		t.Fatalf("bad restored infoHash: %#v", status["infoHash"])
	}
	if _, ok := status["bittorrent"].(*goaria.BittorrentInfo); !ok {
		t.Fatalf("restored torrent lost bittorrent metadata: %#v", status)
	}
}

func TestRPCAddTorrentAria2CompatibleParamShapes(t *testing.T) {
	data, err := os.ReadFile("../test.torrent")
	if err != nil {
		t.Fatal(err)
	}
	encoded := base64.StdEncoding.EncodeToString(data)
	webseed := "http://example.invalid/webseed"

	for _, tt := range []struct {
		name   string
		secret string
		params []any
		gid    string
	}{
		{
			name:   "torrent and options",
			params: []any{encoded, map[string]any{"pause": "true", "gid": "1000000000000001"}},
			gid:    "1000000000000001",
		},
		{
			name:   "torrent uris options position",
			params: []any{encoded, []string{webseed}, map[string]any{"pause": "true", "gid": "1000000000000002"}, 0},
			gid:    "1000000000000002",
		},
		{
			name:   "token torrent options position",
			secret: "secret",
			params: []any{"token:secret", encoded, map[string]any{"pause": "true", "gid": "1000000000000003"}, 0},
			gid:    "1000000000000003",
		},
		{
			name:   "token torrent uris options position",
			secret: "secret",
			params: []any{"token:secret", encoded, []string{webseed}, map[string]any{"pause": "true", "gid": "1000000000000004"}, 0},
			gid:    "1000000000000004",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			engine, err := goaria.NewEngine(goaria.Config{Dir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer engine.Close(context.Background())
			rpc := NewHandler(engine, tt.secret)

			payload, err := json.Marshal(map[string]any{
				"jsonrpc": "2.0",
				"id":      "add",
				"method":  "aria2.addTorrent",
				"params":  tt.params,
			})
			if err != nil {
				t.Fatal(err)
			}
			resp := invokeRPC(t, rpc, string(payload))
			if resp.Error != nil {
				t.Fatalf("addTorrent failed: %#v", resp.Error)
			}
			var gotGID string
			raw, _ := json.Marshal(resp.Result)
			if err := json.Unmarshal(raw, &gotGID); err != nil {
				t.Fatal(err)
			}
			if gotGID != tt.gid {
				t.Fatalf("gid = %q, want %q", gotGID, tt.gid)
			}
			status, err := engine.TellStatus(gotGID, []string{"status"})
			if err != nil {
				t.Fatal(err)
			}
			if status["status"] != string(goaria.StatusPaused) {
				t.Fatalf("status = %v, want paused", status["status"])
			}
		})
	}

	engine, err := goaria.NewEngine(goaria.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	rpc := NewHandler(engine, "")
	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      "bad",
		"method":  "aria2.addTorrent",
		"params":  []any{encoded, "not-uri-list-or-options"},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp := invokeRPC(t, rpc, string(payload))
	if resp.Error == nil || resp.Error.Code != rpcInvalidParams {
		t.Fatalf("bad addTorrent params response = %#v", resp)
	}
}

func TestServerCustomPathAndMountableHandler(t *testing.T) {
	engine, err := goaria.NewEngine(goaria.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	customPathServer := httptest.NewServer(NewServer(engine, Config{Path: "rpc"}).Handler())
	defer customPathServer.Close()

	resp := postListMethods(t, customPathServer.URL+"/rpc")
	if resp.Error != nil || resp.Result == nil {
		t.Fatalf("listMethods on custom path failed: %#v", resp)
	}

	httpResp, err := http.Post(customPathServer.URL+"/jsonrpc", "application/json", strings.NewReader(`{"jsonrpc":"2.0","id":"miss","method":"system.listMethods"}`))
	if err != nil {
		t.Fatal(err)
	}
	httpResp.Body.Close()
	if httpResp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected default path to be unregistered, got %s", httpResp.Status)
	}

	mounted := NewServer(engine, Config{}).JSONRPCHandler()
	mux := http.NewServeMux()
	mux.Handle("/downloads/rpc", mounted)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	existingServer := httptest.NewServer(mux)
	defer existingServer.Close()

	resp = postListMethods(t, existingServer.URL+"/downloads/rpc")
	if resp.Error != nil || resp.Result == nil {
		t.Fatalf("listMethods on mounted handler failed: %#v", resp)
	}
	health, err := http.Get(existingServer.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	health.Body.Close()
	if health.StatusCode != http.StatusNoContent {
		t.Fatalf("expected existing route to keep working, got %s", health.Status)
	}
}

func TestJSONPCallbackValidation(t *testing.T) {
	engine, err := goaria.NewEngine(goaria.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	server := httptest.NewServer(NewServer(engine, Config{}).Handler())
	defer server.Close()

	resp, err := http.Get(server.URL + "/jsonrpc?method=system.listMethods&id=cb&jsoncallback=goaria.cb_$1")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("valid JSONP callback got %s", resp.Status)
	}

	resp, err = http.Get(server.URL + "/jsonrpc?method=system.listMethods&id=cb&jsoncallback=alert%281%29")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid JSONP callback got %s", resp.Status)
	}
}

func TestInvalidJSONPCallbackRejectedBeforeRPCDispatch(t *testing.T) {
	engine, err := goaria.NewEngine(goaria.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	if _, err := engine.ChangeGlobalOption(goaria.Options{"max-concurrent-downloads": "0"}); err != nil {
		t.Fatal(err)
	}
	gid, err := engine.AddURI([]string{"http://example.invalid/remove-me"}, goaria.Options{"pause": "true"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewServer(engine, Config{}).Handler())
	defer server.Close()

	params := url.QueryEscape(`["` + gid + `"]`)
	resp, err := http.Get(server.URL + "/jsonrpc?method=aria2.remove&id=remove&params=" + params + "&jsoncallback=alert%281%29")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid JSONP callback got %s", resp.Status)
	}

	status, err := engine.TellStatus(gid, []string{"status"})
	if err != nil {
		t.Fatal(err)
	}
	if status["status"] != string(goaria.StatusPaused) {
		t.Fatalf("invalid JSONP request mutated status to %v", status["status"])
	}
}

func TestWebSocketJSONRPCAndNotification(t *testing.T) {
	data := []byte("hello websocket")
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "ws.txt", time.Now(), bytes.NewReader(data))
	}))
	defer src.Close()

	engine, err := goaria.NewEngine(goaria.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())

	server := httptest.NewServer(NewServer(engine, Config{}).Handler())
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/jsonrpc"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": "methods", "method": "system.listMethods"}); err != nil {
		t.Fatal(err)
	}
	var response map[string]any
	if err := conn.ReadJSON(&response); err != nil {
		t.Fatal(err)
	}
	if response["id"] != "methods" || response["result"] == nil {
		t.Fatalf("unexpected websocket response: %#v", response)
	}

	_ = conn.SetReadDeadline(time.Now().Add(time.Second))

	if _, err := engine.AddURI([]string{src.URL + "/ws.txt"}, goaria.Options{"out": "ws.txt"}, nil); err != nil {
		t.Fatal(err)
	}
	for {
		var notification map[string]any
		if err := conn.ReadJSON(&notification); err != nil {
			t.Fatal(err)
		}
		if notification["method"] == "aria2.onDownloadStart" || notification["method"] == "aria2.onDownloadComplete" {
			return
		}
	}
}

func TestWebSocketRejectsCrossOriginWithoutSecret(t *testing.T) {
	engine, err := goaria.NewEngine(goaria.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	server := httptest.NewServer(NewServer(engine, Config{}).Handler())
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/jsonrpc"

	headers := http.Header{}
	headers.Set("Origin", "https://example.invalid")
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, headers)
	if conn != nil {
		conn.Close()
	}
	if err == nil {
		t.Fatal("cross-origin websocket dial unexpectedly succeeded")
	}
	if resp != nil {
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("cross-origin websocket status = %s, want 403", resp.Status)
		}
	}
}

func postListMethods(t *testing.T, url string) rpcResponse {
	t.Helper()
	httpResp, err := http.Post(url, "application/json", strings.NewReader(`{"jsonrpc":"2.0","id":"methods","method":"system.listMethods"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer httpResp.Body.Close()
	var resp rpcResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func invokeRPC(t *testing.T, rpc *RPCHandler, payload string) rpcResponse {
	t.Helper()
	data, ok := rpc.HandlePayload([]byte(payload))
	if !ok {
		t.Fatal("no RPC response")
	}
	var resp rpcResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func stringSet(v any) map[string]bool {
	out := map[string]bool{}
	items, ok := v.([]any)
	if !ok {
		return out
	}
	for _, item := range items {
		if s, ok := item.(string); ok {
			out[s] = true
		}
	}
	return out
}

func waitForStatus(t testing.TB, engine *goaria.Engine, gid string, want goaria.Status) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, err := engine.TellStatus(gid, []string{"status", "errorMessage"})
		if err != nil {
			t.Fatal(err)
		}
		if status["status"] == string(want) {
			return
		}
		if status["status"] == string(goaria.StatusError) {
			t.Fatalf("download errored: %v", status["errorMessage"])
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", want)
}
