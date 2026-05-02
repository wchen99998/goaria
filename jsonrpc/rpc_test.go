package jsonrpc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

	resp = invokeRPC(t, rpc, `{"jsonrpc":"2.0","id":"v","method":"aria2.getVersion"}`)
	if resp.Error == nil || !strings.Contains(resp.Error.Message, "Unauthorized") {
		t.Fatalf("expected Unauthorized error, got %#v", resp)
	}

	resp = invokeRPC(t, rpc, `{"jsonrpc":"2.0","id":"v","method":"aria2.getVersion","params":["token:secret"]}`)
	if resp.Error != nil {
		t.Fatalf("getVersion with token failed: %#v", resp.Error)
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
