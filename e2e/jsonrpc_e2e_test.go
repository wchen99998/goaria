package e2e

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const e2eSecret = "goaria-e2e-secret"

var builtBinary struct {
	once sync.Once
	path string
	dir  string
	err  error
}

func TestMain(m *testing.M) {
	code := m.Run()
	if builtBinary.dir != "" {
		_ = os.RemoveAll(builtBinary.dir)
	}
	os.Exit(code)
}

func TestJSONRPCAPIMethodCoverage(t *testing.T) {
	manifest := loadManifest(t)
	fixtures := newFixtureServer(t)

	covered := map[string]int{}
	daemon := startDaemon(t, e2eSecret, "")
	client := daemon.client.withCoverage(covered)

	advertised := stringSliceResult(t, client.Call(t, "system.listMethods"))
	assertSameStrings(t, "advertised methods vs manifest", advertised, manifest.PublicMethods)

	notifications := stringSliceResult(t, client.Call(t, "system.listNotifications"))
	assertSameStrings(t, "advertised notifications vs manifest", notifications, manifest.Notifications)

	sessionFile := filepath.Join(t.TempDir(), "goaria.session")
	expectOK(t, client.Call(t, "aria2.changeGlobalOption", map[string]any{
		"max-concurrent-downloads": "0",
		"max-download-result":      "1000",
		"save-session":             sessionFile,
	}))
	global := objectResult(t, client.Call(t, "aria2.getGlobalOption"))
	assertMapValue(t, global, "max-concurrent-downloads", "0")
	assertMapValue(t, global, "save-session", sessionFile)
	expectOK(t, client.Call(t, "aria2.saveSession"))
	if _, err := os.Stat(sessionFile); err != nil {
		t.Fatalf("saveSession did not create %s: %v", sessionFile, err)
	}

	version := objectResult(t, client.Call(t, "aria2.getVersion"))
	if version["version"] == "" {
		t.Fatalf("getVersion returned no version: %#v", version)
	}
	sessionInfo := objectResult(t, client.Call(t, "aria2.getSessionInfo"))
	if sessionInfo["sessionId"] == "" {
		t.Fatalf("getSessionInfo returned no sessionId: %#v", sessionInfo)
	}
	_ = objectResult(t, client.Call(t, "aria2.getGlobalStat"))
	active := arrayResult(t, client.Call(t, "aria2.tellActive", []string{"gid", "status"}))
	if len(active) != 0 {
		t.Fatalf("tellActive with max-concurrent-downloads=0 = %#v, want empty", active)
	}

	gid1 := stringResult(t, client.Call(t, "aria2.addUri", []string{fixtures.URL + "/a.bin"}, map[string]any{"gid": "1000000000000001"}, 0))
	gid2 := stringResult(t, client.Call(t, "aria2.addUri", []string{fixtures.URL + "/b.bin"}, map[string]any{"gid": "1000000000000002"}, 0))
	gid3 := stringResult(t, client.Call(t, "aria2.addUri", []string{fixtures.URL + "/c.bin"}, map[string]any{"gid": "1000000000000003"}))
	if gid1 != "1000000000000001" || gid2 != "1000000000000002" || gid3 != "1000000000000003" {
		t.Fatalf("unexpected gids: %s %s %s", gid1, gid2, gid3)
	}

	waiting := arrayResult(t, client.Call(t, "aria2.tellWaiting", 0, 10, []string{"gid", "status"}))
	if len(waiting) < 3 {
		t.Fatalf("tellWaiting returned %#v, want at least three queued downloads", waiting)
	}
	_ = numberResult(t, client.Call(t, "aria2.changePosition", gid3, 0, "POS_SET"))
	_ = numberResult(t, client.Call(t, "aria2.changePosition", gid2, 1, "POS_CUR"))
	_ = numberResult(t, client.Call(t, "aria2.changePosition", gid2, -1, "POS_END"))

	status := objectResult(t, client.Call(t, "aria2.tellStatus", gid1, []string{"gid", "status", "files"}))
	assertMapValue(t, status, "gid", gid1)
	uris := arrayResult(t, client.Call(t, "aria2.getUris", gid1))
	if len(uris) != 1 {
		t.Fatalf("getUris returned %#v", uris)
	}
	files := arrayResult(t, client.Call(t, "aria2.getFiles", gid1))
	if len(files) != 1 {
		t.Fatalf("getFiles returned %#v", files)
	}
	peers := arrayResult(t, client.Call(t, "aria2.getPeers", gid1))
	if len(peers) != 0 {
		t.Fatalf("HTTP getPeers returned %#v, want empty", peers)
	}
	servers := arrayResult(t, client.Call(t, "aria2.getServers", gid1))
	if len(servers) != 1 {
		t.Fatalf("getServers returned %#v", servers)
	}
	opts := objectResult(t, client.Call(t, "aria2.getOption", gid1))
	assertMapValue(t, opts, "gid", gid1)
	expectOK(t, client.Call(t, "aria2.changeOption", gid1, map[string]any{"max-download-limit": "1K"}))
	opts = objectResult(t, client.Call(t, "aria2.getOption", gid1))
	assertMapValue(t, opts, "max-download-limit", "1K")
	changeURI := arrayResult(t, client.Call(t, "aria2.changeUri", gid1, 1, []string{fixtures.URL + "/a.bin"}, []string{fixtures.URL + "/d.bin"}, 0))
	if !reflect.DeepEqual(changeURI, []any{float64(1), float64(1)}) {
		t.Fatalf("changeUri returned %#v, want [1,1]", changeURI)
	}

	assertStringResult(t, client.Call(t, "aria2.pause", gid1), gid1)
	assertStringResult(t, client.Call(t, "aria2.unpause", gid1), gid1)
	assertStringResult(t, client.Call(t, "aria2.forcePause", gid1), gid1)
	assertStringResult(t, client.Call(t, "aria2.unpause", gid1), gid1)
	expectOK(t, client.Call(t, "aria2.pauseAll"))
	expectOK(t, client.Call(t, "aria2.unpauseAll"))
	expectOK(t, client.Call(t, "aria2.forcePauseAll"))
	expectOK(t, client.Call(t, "aria2.unpauseAll"))

	assertStringResult(t, client.Call(t, "aria2.remove", gid2), gid2)
	assertStringResult(t, client.Call(t, "aria2.forceRemove", gid3), gid3)
	stopped := arrayResult(t, client.Call(t, "aria2.tellStopped", 0, 10, []string{"gid", "status"}))
	if len(stopped) < 2 {
		t.Fatalf("tellStopped returned %#v, want removed downloads", stopped)
	}
	expectOK(t, client.Call(t, "aria2.removeDownloadResult", gid2))
	expectOK(t, client.Call(t, "aria2.purgeDownloadResult"))

	torrentData, err := os.ReadFile(filepath.Join(repoRoot(t), "test.torrent"))
	if err != nil {
		t.Fatal(err)
	}
	torrentGID := stringResult(t, client.Call(t, "aria2.addTorrent",
		base64.StdEncoding.EncodeToString(torrentData),
		[]string{fixtures.URL + "/webseed.bin"},
		map[string]any{"pause": "true", "gid": "10000000000000aa"},
		0,
	))
	if torrentGID != "10000000000000aa" {
		t.Fatalf("addTorrent gid = %s, want 10000000000000aa", torrentGID)
	}
	torrentStatus := objectResult(t, client.Call(t, "aria2.tellStatus", torrentGID, []string{"status", "infoHash", "bittorrent", "files"}))
	assertMapValue(t, torrentStatus, "status", "paused")
	if infoHash, _ := torrentStatus["infoHash"].(string); len(infoHash) != 40 {
		t.Fatalf("torrent infoHash = %#v", torrentStatus["infoHash"])
	}

	multicall := arrayResult(t, client.Call(t, "system.multicall", []any{
		map[string]any{"methodName": "system.listMethods", "params": []any{}},
		map[string]any{"methodName": "aria2.getGlobalStat", "params": []any{"token:" + e2eSecret}},
	}))
	if len(multicall) != 2 {
		t.Fatalf("system.multicall returned %#v", multicall)
	}

	expectOK(t, client.Call(t, "aria2.shutdown"))
	if err, ok := daemon.wait(5 * time.Second); !ok {
		t.Fatal("daemon did not exit after aria2.shutdown")
	} else if err != nil {
		t.Fatalf("daemon exited after aria2.shutdown with error: %v\n%s", err, daemon.output.String())
	}

	forceDaemon := startDaemon(t, e2eSecret, "")
	forceClient := forceDaemon.client.withCoverage(covered)
	expectOK(t, forceClient.Call(t, "aria2.forceShutdown"))
	if err, ok := forceDaemon.wait(5 * time.Second); !ok {
		t.Fatal("daemon did not exit after aria2.forceShutdown")
	} else if err != nil {
		t.Fatalf("daemon exited after aria2.forceShutdown with error: %v\n%s", err, forceDaemon.output.String())
	}

	var missing []string
	for _, method := range advertised {
		if covered[method] == 0 {
			missing = append(missing, method)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("advertised methods not exercised by E2E coverage test: %v", missing)
	}
}

func TestJSONRPCOptionCoverage(t *testing.T) {
	manifest := loadManifest(t)
	optionSchemas := schemaOptionProperties(t)
	if len(optionSchemas) == 0 {
		t.Fatal("schema exposes no JSON-RPC options")
	}

	manifestKeys := sortedKeys(manifest.OptionCases)
	schemaKeys := sortedKeys(optionSchemas)
	assertSameStrings(t, "schema options vs E2E option manifest", manifestKeys, schemaKeys)

	temp := t.TempDir()
	if err := os.WriteFile(filepath.Join(temp, "cookies.txt"), []byte("# Netscape HTTP Cookie File\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	daemon := startDaemon(t, e2eSecret, "")
	client := daemon.client
	fixtures := newFixtureServer(t)

	for i, key := range manifestKeys {
		value := materializeOptionValue(t, manifest.OptionCases[key], temp)
		t.Run("global/"+key, func(t *testing.T) {
			expectOK(t, client.Call(t, "aria2.changeGlobalOption", map[string]any{key: value}))
			got := objectResult(t, client.Call(t, "aria2.getGlobalOption"))
			assertOptionValue(t, got, key, value)
		})
		t.Run("download/"+key, func(t *testing.T) {
			gid := fmt.Sprintf("200000000000%04x", i+1)
			opts := map[string]any{"pause": "true", "gid": gid}
			if key == "gid" {
				gid = fmt.Sprint(value)
				opts = map[string]any{"pause": "true", "gid": gid}
			} else {
				opts[key] = value
			}
			gotGID := stringResult(t, client.Call(t, "aria2.addUri", []string{fixtures.URL + "/option-" + url.PathEscape(key)}, opts))
			if gotGID != gid {
				t.Fatalf("addUri gid = %q, want %q", gotGID, gid)
			}
			got := objectResult(t, client.Call(t, "aria2.getOption", gotGID))
			assertOptionValue(t, got, key, value)
		})
	}
}

func TestJSONRPCFiniteOptionValueVariants(t *testing.T) {
	optionSchemas := schemaOptionProperties(t)
	defs := schemaDefs(t)
	daemon := startDaemon(t, e2eSecret, "")
	client := daemon.client

	for _, key := range sortedKeys(optionSchemas) {
		values := finiteEnumValues(optionSchemas[key], defs)
		if len(values) == 0 {
			continue
		}
		t.Run(key, func(t *testing.T) {
			for _, value := range values {
				name := strings.NewReplacer("/", "_", ".", "_").Replace(fmt.Sprint(value))
				t.Run(name, func(t *testing.T) {
					expectOK(t, client.Call(t, "aria2.changeGlobalOption", map[string]any{key: value}))
					got := objectResult(t, client.Call(t, "aria2.getGlobalOption"))
					if _, ok := got[key]; !ok {
						t.Fatalf("getGlobalOption missing %q after setting finite value %#v: %#v", key, value, got)
					}
				})
			}
		})
	}
}

func TestJSONRPCParameterShapesAndAuth(t *testing.T) {
	secretDaemon := startDaemon(t, e2eSecret, "")
	secretClient := secretDaemon.client
	fixtures := newFixtureServer(t)

	unauthorized := secretClient.CallNoToken(t, "aria2.getVersion")
	assertRPCError(t, unauthorized, 1, "Unauthorized")
	wrongToken := secretClient.CallNoToken(t, "aria2.getVersion", "token:wrong")
	assertRPCError(t, wrongToken, 1, "Unauthorized")
	if resp := secretClient.CallNoToken(t, "system.listMethods"); resp.Error != nil {
		t.Fatalf("system.listMethods should not require token: %#v", resp.Error)
	}
	assertRPCError(t, secretClient.Call(t, "aria2.noSuchMethod"), -32601, "Method not found")
	assertRPCError(t, secretClient.Call(t, "aria2.addMetalink", "not-supported"), 1, "unsupported")

	status, body := secretClient.PostRaw(t, `{"jsonrpc":"2.0","id":"bad","method":`)
	if status != http.StatusOK {
		t.Fatalf("malformed JSON status = %d body=%s", status, body)
	}
	var parseErr rpcResponse
	if err := json.Unmarshal(body, &parseErr); err != nil {
		t.Fatal(err)
	}
	assertRPCError(t, parseErr, -32700, "Parse error")

	status, body = secretClient.PostRaw(t, `{"jsonrpc":"2.0","id":"bad","method":"aria2.tellStatus","params":{}}`)
	if status != http.StatusOK {
		t.Fatalf("invalid params status = %d body=%s", status, body)
	}
	var invalidParams rpcResponse
	if err := json.Unmarshal(body, &invalidParams); err != nil {
		t.Fatal(err)
	}
	assertRPCError(t, invalidParams, -32602, "Invalid params")

	torrentData, err := os.ReadFile(filepath.Join(repoRoot(t), "test.torrent"))
	if err != nil {
		t.Fatal(err)
	}
	encodedTorrent := base64.StdEncoding.EncodeToString(torrentData)
	assertStringResult(t, secretClient.Call(t, "aria2.addTorrent", encodedTorrent, map[string]any{"pause": "true", "gid": "3000000000000001"}, 0), "3000000000000001")
	assertStringResult(t, secretClient.Call(t, "aria2.addTorrent", encodedTorrent, []string{fixtures.URL + "/seed-one.bin"}, map[string]any{"pause": "true", "gid": "3000000000000002"}, 0), "3000000000000002")
	assertStringResult(t, secretClient.Call(t, "aria2.addTorrent", fixtures.URL+"/fixture.torrent", map[string]any{"pause": "true", "gid": "3000000000000006"}, 0), "3000000000000006")

	openDaemon := startDaemon(t, "", "")
	openClient := openDaemon.client
	assertStringResult(t, openClient.Call(t, "aria2.addUri", []string{fixtures.URL + "/open-uri.bin"}, map[string]any{"pause": "true", "gid": "3000000000000003"}, 0), "3000000000000003")
	assertStringResult(t, openClient.Call(t, "aria2.addTorrent", encodedTorrent, map[string]any{"pause": "true", "gid": "3000000000000004"}, 0), "3000000000000004")
	assertStringResult(t, openClient.Call(t, "aria2.addTorrent", encodedTorrent, []string{fixtures.URL + "/seed-two.bin"}, map[string]any{"pause": "true", "gid": "3000000000000005"}, 0), "3000000000000005")
	assertStringResult(t, openClient.Call(t, "aria2.addTorrent", fixtures.URL+"/open-fixture.torrent", map[string]any{"pause": "true", "gid": "3000000000000007"}, 0), "3000000000000007")
}

func TestJSONRPCTransports(t *testing.T) {
	daemon := startDaemon(t, e2eSecret, "")
	client := daemon.client
	fixtures := newFixtureServer(t)

	params := base64.StdEncoding.EncodeToString([]byte(`[]`))
	resp, body := client.Get(t, url.Values{"method": {"system.listMethods"}, "id": {"get-base64"}, "params": {params}})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET base64 status = %s body=%s", resp.Status, body)
	}
	var getBase64 rpcResponse
	if err := json.Unmarshal(body, &getBase64); err != nil {
		t.Fatal(err)
	}
	if getBase64.Error != nil {
		t.Fatalf("GET base64 listMethods failed: %#v", getBase64.Error)
	}

	rawParams := `["token:` + e2eSecret + `"]`
	resp, body = client.Get(t, url.Values{"method": {"aria2.getVersion"}, "id": {"get-raw"}, "params": {rawParams}})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET raw status = %s body=%s", resp.Status, body)
	}
	var getRaw rpcResponse
	if err := json.Unmarshal(body, &getRaw); err != nil {
		t.Fatal(err)
	}
	if getRaw.Error != nil {
		t.Fatalf("GET raw getVersion failed: %#v", getRaw.Error)
	}

	resp, body = client.Get(t, url.Values{"method": {"system.listNotifications"}, "id": {"jsonp"}, "params": {"[]"}, "jsoncallback": {"goaria.e2e_cb"}})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("JSONP status = %s body=%s", resp.Status, body)
	}
	if !bytes.HasPrefix(body, []byte("goaria.e2e_cb(")) || !bytes.HasSuffix(bytes.TrimSpace(body), []byte(");")) {
		t.Fatalf("invalid JSONP body: %s", body)
	}

	status, body := client.PostPayload(t, []any{
		map[string]any{"jsonrpc": "2.0", "id": "batch-methods", "method": "system.listMethods", "params": []any{}},
		map[string]any{"jsonrpc": "2.0", "id": "batch-stat", "method": "aria2.getGlobalStat", "params": []any{"token:" + e2eSecret}},
	})
	if status != http.StatusOK {
		t.Fatalf("batch status = %d body=%s", status, body)
	}
	var batch []rpcResponse
	if err := json.Unmarshal(body, &batch); err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 || batch[0].Error != nil || batch[1].Error != nil {
		t.Fatalf("batch response = %#v", batch)
	}

	wsURL := "ws" + strings.TrimPrefix(daemon.endpoint, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{
		"jsonrpc": "2.0",
		"id":      "ws-add",
		"method":  "aria2.addUri",
		"params":  []any{"token:" + e2eSecret, []string{fixtures.URL + "/ws.bin"}, map[string]any{"out": "ws.bin"}},
	}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	sawResponse := false
	sawNotification := false
	for time.Now().Before(deadline) && (!sawResponse || !sawNotification) {
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		var msg map[string]any
		if err := conn.ReadJSON(&msg); err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
				continue
			}
			t.Fatal(err)
		}
		if msg["id"] == "ws-add" && msg["result"] != nil {
			sawResponse = true
		}
		switch msg["method"] {
		case "aria2.onDownloadStart", "aria2.onDownloadComplete":
			sawNotification = true
		}
	}
	if !sawResponse || !sawNotification {
		t.Fatalf("websocket saw response=%v notification=%v", sawResponse, sawNotification)
	}
}

func TestJSONRPCDownloadBehaviorSideEffects(t *testing.T) {
	fixtures := newBehaviorFixture(t)
	daemon := startDaemon(t, e2eSecret, "")
	client := daemon.client

	gid := stringResult(t, client.Call(t, "aria2.addUri",
		[]string{fixtures.URL + "/side-effect.bin"},
		map[string]any{
			"out":           "side-effect.bin",
			"user-agent":    "goaria-e2e-side-effect/1.0",
			"referer":       "http://e2e.local/ref",
			"header":        []string{"X-Goaria-E2E: side-effect"},
			"http-no-cache": "true",
			"remote-time":   "true",
			"use-head":      "false",
		},
	))
	status := waitForDownloadStatus(t, client, gid, "complete")
	assertMapValue(t, status, "completedLength", fmt.Sprint(len(fixtures.payload("/side-effect.bin"))))
	assertMapValue(t, status, "totalLength", fmt.Sprint(len(fixtures.payload("/side-effect.bin"))))

	path := filepath.Join(daemon.downloadDir, "side-effect.bin")
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, fixtures.payload("/side-effect.bin")) {
		t.Fatalf("downloaded bytes = %q, want %q", got, fixtures.payload("/side-effect.bin"))
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !st.ModTime().UTC().Equal(fixtures.lastModified) {
		t.Fatalf("downloaded mtime = %s, want remote Last-Modified %s", st.ModTime().UTC(), fixtures.lastModified)
	}
	if !fixtures.sawHeader("/side-effect.bin", "User-Agent", "goaria-e2e-side-effect/1.0") {
		t.Fatalf("source server did not see configured User-Agent; records=%#v", fixtures.records("/side-effect.bin"))
	}
	if !fixtures.sawHeader("/side-effect.bin", "Referer", "http://e2e.local/ref") {
		t.Fatalf("source server did not see configured Referer; records=%#v", fixtures.records("/side-effect.bin"))
	}
	if !fixtures.sawHeader("/side-effect.bin", "X-Goaria-E2E", "side-effect") {
		t.Fatalf("source server did not see custom header; records=%#v", fixtures.records("/side-effect.bin"))
	}
	if !fixtures.sawHeader("/side-effect.bin", "Cache-Control", "no-cache") {
		t.Fatalf("source server did not see no-cache header; records=%#v", fixtures.records("/side-effect.bin"))
	}
}

func TestJSONRPCHTTPRFCDownloadScenarios(t *testing.T) {
	segmentedData := bytes.Repeat([]byte("e2e-range-"), 64*1024)
	weakETagData := bytes.Repeat([]byte("e2e-weak-etag-"), 64*1024)
	completeData := bytes.Repeat([]byte("e2e-complete-"), 1024)
	torrentPartial := []byte("partial torrent metadata")

	var mu sync.Mutex
	var records []requestRecord
	record := func(r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		records = append(records, requestRecord{
			Method: r.Method,
			Path:   r.URL.Path,
			Header: r.Header.Clone(),
		})
	}
	countGET := func(path string) int {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		for _, r := range records {
			if r.Path == path && r.Method == http.MethodGet {
				n++
			}
		}
		return n
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record(r)
		switch r.URL.Path {
		case "/segmented.bin":
			if r.Method == http.MethodHead {
				if got := r.Header.Get("Accept-Encoding"); got != "identity" {
					http.Error(w, "HEAD Accept-Encoding = "+got, http.StatusBadRequest)
					return
				}
				if got := r.Header.Get("Range"); got != "" {
					http.Error(w, "HEAD Range = "+got, http.StatusBadRequest)
					return
				}
				setE2ERangeHeaders(w, segmentedData)
				w.Header().Set("ETag", `"e2e-range-etag"`)
				return
			}
			ranges := r.Header.Values("Range")
			if len(ranges) != 1 {
				http.Error(w, fmt.Sprintf("Range field count = %d", len(ranges)), http.StatusBadRequest)
				return
			}
			if got := r.Header.Get("Accept-Encoding"); got != "identity" {
				http.Error(w, "range Accept-Encoding = "+got, http.StatusBadRequest)
				return
			}
			if got := r.Header.Get("If-Range"); got != `"e2e-range-etag"` {
				http.Error(w, "range If-Range = "+got, http.StatusBadRequest)
				return
			}
			start, end, ok := parseE2ERange(ranges[0], int64(len(segmentedData)))
			if !ok {
				http.Error(w, "invalid Range "+ranges[0], http.StatusBadRequest)
				return
			}
			writeE2ERange(w, segmentedData, start, end)
		case "/weak-etag.bin":
			if r.Method == http.MethodHead {
				setE2ERangeHeaders(w, weakETagData)
				w.Header().Set("ETag", `W/"e2e-weak-etag"`)
				return
			}
			if got := r.Header.Get("If-Range"); got != "" {
				http.Error(w, "weak ETag used as If-Range "+got, http.StatusBadRequest)
				return
			}
			start, end, ok := parseE2ERange(r.Header.Get("Range"), int64(len(weakETagData)))
			if !ok {
				http.Error(w, "invalid Range "+r.Header.Get("Range"), http.StatusBadRequest)
				return
			}
			writeE2ERange(w, weakETagData, start, end)
		case "/bad-range.bin":
			if r.Method == http.MethodHead {
				setE2ERangeHeaders(w, segmentedData)
				return
			}
			start, end, ok := parseE2ERange(r.Header.Get("Range"), int64(len(segmentedData)))
			if !ok {
				http.Error(w, "invalid Range "+r.Header.Get("Range"), http.StatusBadRequest)
				return
			}
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start+1, end+1, len(segmentedData)+1))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(segmentedData[start : end+1])
		case "/bare-redirect.bin":
			w.WriteHeader(http.StatusFound)
			_, _ = w.Write([]byte("redirect body must not be saved"))
		case "/complete.bin":
			if got := r.Header.Get("Range"); got == fmt.Sprintf("bytes=%d-", len(completeData)) {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(completeData)))
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			http.Error(w, "unexpected complete request", http.StatusBadRequest)
		case "/partial.torrent":
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(torrentPartial)-1, len(torrentPartial)+10))
			w.Header().Set("Content-Length", strconv.Itoa(len(torrentPartial)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(torrentPartial)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	daemon := startDaemon(t, e2eSecret, "")
	client := daemon.client

	segmentedGID := stringResult(t, client.Call(t, "aria2.addUri",
		[]string{server.URL + "/segmented.bin"},
		map[string]any{
			"out":                       "segmented.bin",
			"split":                     "4",
			"max-connection-per-server": "4",
			"min-split-size":            "1",
			"http-accept-gzip":          "true",
			"header":                    []string{"Range: bytes=1-1", "Accept-Encoding: gzip"},
		},
	))
	waitForDownloadStatus(t, client, segmentedGID, "complete")
	assertFileBytes(t, filepath.Join(daemon.downloadDir, "segmented.bin"), segmentedData)
	if n := countGET("/segmented.bin"); n < 2 {
		t.Fatalf("segmented download used %d GET requests, want range segmentation", n)
	}

	weakETagGID := stringResult(t, client.Call(t, "aria2.addUri",
		[]string{server.URL + "/weak-etag.bin"},
		map[string]any{
			"out":                       "weak-etag.bin",
			"split":                     "4",
			"max-connection-per-server": "4",
			"min-split-size":            "1",
		},
	))
	waitForDownloadStatus(t, client, weakETagGID, "complete")
	assertFileBytes(t, filepath.Join(daemon.downloadDir, "weak-etag.bin"), weakETagData)

	badRangeGID := stringResult(t, client.Call(t, "aria2.addUri",
		[]string{server.URL + "/bad-range.bin"},
		map[string]any{
			"out":                       "bad-range.bin",
			"split":                     "4",
			"max-connection-per-server": "4",
			"min-split-size":            "1",
			"max-tries":                 "1",
		},
	))
	badStatus := waitForDownloadStatus(t, client, badRangeGID, "error")
	if msg, _ := badStatus["errorMessage"].(string); !strings.Contains(msg, "Content-Range") {
		t.Fatalf("bad range errorMessage = %q, want Content-Range", msg)
	}

	redirectGID := stringResult(t, client.Call(t, "aria2.addUri",
		[]string{server.URL + "/bare-redirect.bin"},
		map[string]any{"out": "bare-redirect.bin", "max-tries": "1"},
	))
	waitForDownloadStatus(t, client, redirectGID, "error")
	if _, err := os.Stat(filepath.Join(daemon.downloadDir, "bare-redirect.bin")); !os.IsNotExist(err) {
		t.Fatalf("bare redirect response was saved or stat failed: %v", err)
	}

	completePath := filepath.Join(daemon.downloadDir, "complete.bin")
	if err := os.WriteFile(completePath, completeData, 0o644); err != nil {
		t.Fatal(err)
	}
	completeGID := stringResult(t, client.Call(t, "aria2.addUri",
		[]string{server.URL + "/complete.bin"},
		map[string]any{"out": "complete.bin", "continue": "true"},
	))
	waitForDownloadStatus(t, client, completeGID, "complete")
	assertFileBytes(t, completePath, completeData)

	assertRPCError(t, client.Call(t, "aria2.addTorrent",
		server.URL+"/partial.torrent",
		map[string]any{"pause": "true", "gid": "6000000000000001", "max-tries": "1"},
		0,
	), 1, "206")
	assertRPCError(t, client.Call(t, "aria2.tellStatus", "6000000000000001", []string{"status"}), 1, "not found")
}

func TestJSONRPCURIFallbackAndChangeURISideEffects(t *testing.T) {
	fixtures := newBehaviorFixture(t)
	daemon := startDaemon(t, e2eSecret, "")
	client := daemon.client

	fallbackGID := stringResult(t, client.Call(t, "aria2.addUri",
		[]string{fixtures.URL + "/always-503.bin", fixtures.URL + "/fallback.bin"},
		map[string]any{"out": "fallback.bin", "max-tries": "2", "use-head": "false"},
	))
	waitForDownloadStatus(t, client, fallbackGID, "complete")
	assertFileBytes(t, filepath.Join(daemon.downloadDir, "fallback.bin"), fixtures.payload("/fallback.bin"))
	if fixtures.count("/always-503.bin") == 0 || fixtures.count("/fallback.bin") == 0 {
		t.Fatalf("fallback did not exercise both URIs; failed=%d fallback=%d", fixtures.count("/always-503.bin"), fixtures.count("/fallback.bin"))
	}

	changeGID := stringResult(t, client.Call(t, "aria2.addUri",
		[]string{fixtures.URL + "/never-used.bin"},
		map[string]any{"pause": "true", "out": "changed.bin"},
	))
	change := arrayResult(t, client.Call(t, "aria2.changeUri", changeGID, 1, []string{fixtures.URL + "/never-used.bin"}, []string{fixtures.URL + "/changed.bin"}, 0))
	if !reflect.DeepEqual(change, []any{float64(1), float64(1)}) {
		t.Fatalf("changeUri returned %#v, want [1,1]", change)
	}
	assertStringResult(t, client.Call(t, "aria2.unpause", changeGID), changeGID)
	waitForDownloadStatus(t, client, changeGID, "complete")
	assertFileBytes(t, filepath.Join(daemon.downloadDir, "changed.bin"), fixtures.payload("/changed.bin"))
	if fixtures.count("/never-used.bin") != 0 {
		t.Fatalf("deleted URI was requested %d times", fixtures.count("/never-used.bin"))
	}
	if fixtures.count("/changed.bin") == 0 {
		t.Fatal("replacement URI was never requested")
	}
}

func TestJSONRPCQueueAndRemovalBehaviorSideEffects(t *testing.T) {
	fixtures := newBehaviorFixture(t)
	daemon := startDaemon(t, e2eSecret, "")
	client := daemon.client

	expectOK(t, client.Call(t, "aria2.changeGlobalOption", map[string]any{"max-concurrent-downloads": "0"}))
	gid1 := stringResult(t, client.Call(t, "aria2.addUri", []string{fixtures.URL + "/queue-1.bin"}, map[string]any{"gid": "4000000000000001"}))
	gid2 := stringResult(t, client.Call(t, "aria2.addUri", []string{fixtures.URL + "/queue-2.bin"}, map[string]any{"gid": "4000000000000002"}))
	gid3 := stringResult(t, client.Call(t, "aria2.addUri", []string{fixtures.URL + "/queue-3.bin"}, map[string]any{"gid": "4000000000000003"}))

	assertGIDOrder(t, arrayResult(t, client.Call(t, "aria2.tellWaiting", 0, 10, []string{"gid"})), []string{gid1, gid2, gid3})
	_ = numberResult(t, client.Call(t, "aria2.changePosition", gid3, 0, "POS_SET"))
	assertGIDOrder(t, arrayResult(t, client.Call(t, "aria2.tellWaiting", 0, 10, []string{"gid"})), []string{gid3, gid1, gid2})

	assertStringResult(t, client.Call(t, "aria2.pause", gid1), gid1)
	assertMapValue(t, objectResult(t, client.Call(t, "aria2.tellStatus", gid1, []string{"status"})), "status", "paused")
	assertStringResult(t, client.Call(t, "aria2.unpause", gid1), gid1)
	assertMapValue(t, objectResult(t, client.Call(t, "aria2.tellStatus", gid1, []string{"status"})), "status", "waiting")

	assertStringResult(t, client.Call(t, "aria2.remove", gid2), gid2)
	assertMapValue(t, objectResult(t, client.Call(t, "aria2.tellStatus", gid2, []string{"status"})), "status", "removed")
	assertGIDOrder(t, arrayResult(t, client.Call(t, "aria2.tellStopped", 0, 10, []string{"gid"})), []string{gid2})
	expectOK(t, client.Call(t, "aria2.removeDownloadResult", gid2))
	assertRPCError(t, client.Call(t, "aria2.tellStatus", gid2, []string{"status"}), 1, "not found")

	assertStringResult(t, client.Call(t, "aria2.forceRemove", gid3), gid3)
	expectOK(t, client.Call(t, "aria2.purgeDownloadResult"))
	stopped := arrayResult(t, client.Call(t, "aria2.tellStopped", 0, 10, []string{"gid"}))
	if len(stopped) != 0 {
		t.Fatalf("tellStopped after purge = %#v, want empty", stopped)
	}
}

func TestJSONRPCSessionPersistenceSideEffects(t *testing.T) {
	fixtures := newBehaviorFixture(t)
	sessionPath := filepath.Join(t.TempDir(), "goaria.session")

	first := startDaemonWithConfig(t, daemonConfig{secret: e2eSecret, saveSession: sessionPath})
	firstClient := first.client
	gid := stringResult(t, firstClient.Call(t, "aria2.addUri",
		[]string{fixtures.URL + "/persisted.bin"},
		map[string]any{"pause": "true", "gid": "5000000000000001", "out": "persisted.bin"},
	))
	expectOK(t, firstClient.Call(t, "aria2.saveSession"))
	sessionData, err := os.ReadFile(sessionPath)
	if err != nil {
		t.Fatal(err)
	}
	sessionText := string(sessionData)
	for _, want := range []string{fixtures.URL + "/persisted.bin", "gid=" + gid, "out=persisted.bin", "pause=true"} {
		if !strings.Contains(sessionText, want) {
			t.Fatalf("session file missing %q:\n%s", want, sessionText)
		}
	}
	expectOK(t, firstClient.Call(t, "aria2.shutdown"))
	if err, ok := first.wait(5 * time.Second); !ok {
		t.Fatal("daemon did not exit after save-session shutdown")
	} else if err != nil {
		t.Fatalf("daemon exited with error: %v\n%s", err, first.output.String())
	}

	restored := startDaemonWithConfig(t, daemonConfig{secret: e2eSecret, inputFile: sessionPath})
	restoredClient := restored.client
	status := objectResult(t, restoredClient.Call(t, "aria2.tellStatus", gid, []string{"gid", "status"}))
	assertMapValue(t, status, "gid", gid)
	assertMapValue(t, status, "status", "paused")
	opts := objectResult(t, restoredClient.Call(t, "aria2.getOption", gid))
	assertMapValue(t, opts, "out", "persisted.bin")
	uris := arrayResult(t, restoredClient.Call(t, "aria2.getUris", gid))
	if len(uris) != 1 {
		t.Fatalf("restored getUris = %#v, want one URI", uris)
	}
	uriObj, ok := uris[0].(map[string]any)
	if !ok || uriObj["uri"] != fixtures.URL+"/persisted.bin" {
		t.Fatalf("restored URI = %#v, want %s", uris, fixtures.URL+"/persisted.bin")
	}
}

type coverageManifest struct {
	PublicMethods []string                   `json:"publicMethods"`
	Notifications []string                   `json:"notifications"`
	OptionCases   map[string]json.RawMessage `json:"optionCases"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcClient struct {
	endpoint string
	secret   string
	http     *http.Client
	covered  map[string]int
}

func (c *rpcClient) withCoverage(covered map[string]int) *rpcClient {
	cp := *c
	cp.covered = covered
	return &cp
}

func (c *rpcClient) Call(t testing.TB, method string, params ...any) rpcResponse {
	t.Helper()
	if c.secret != "" && strings.HasPrefix(method, "aria2.") {
		params = append([]any{"token:" + c.secret}, params...)
	}
	return c.call(t, method, params)
}

func (c *rpcClient) CallNoToken(t testing.TB, method string, params ...any) rpcResponse {
	t.Helper()
	return c.call(t, method, params)
}

func (c *rpcClient) call(t testing.TB, method string, params []any) rpcResponse {
	t.Helper()
	if c.covered != nil {
		c.covered[method]++
	}
	if params == nil {
		params = []any{}
	}
	status, body := c.PostPayload(t, map[string]any{
		"jsonrpc": "2.0",
		"id":      method,
		"method":  method,
		"params":  params,
	})
	if status != http.StatusOK && status != http.StatusNoContent {
		t.Fatalf("%s HTTP status = %d body=%s", method, status, body)
	}
	if len(body) == 0 {
		t.Fatalf("%s returned empty response", method)
	}
	var resp rpcResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("%s invalid JSON response %s: %v", method, body, err)
	}
	return resp
}

func (c *rpcClient) PostPayload(t testing.TB, payload any) (int, []byte) {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return c.PostRaw(t, string(body))
}

func (c *rpcClient) PostRaw(t testing.TB, payload string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, c.endpoint, strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, body
}

func (c *rpcClient) Get(t testing.TB, q url.Values) (*http.Response, []byte) {
	t.Helper()
	resp, err := c.http.Get(c.endpoint + "?" + q.Encode())
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return resp, body
}

type daemonProcess struct {
	endpoint    string
	secret      string
	downloadDir string
	client      *rpcClient
	cmd         *exec.Cmd
	done        chan error
	output      *lockedBuffer
}

func startDaemon(t testing.TB, secret, saveSession string) *daemonProcess {
	t.Helper()
	return startDaemonWithConfig(t, daemonConfig{secret: secret, saveSession: saveSession})
}

type daemonConfig struct {
	secret      string
	inputFile   string
	saveSession string
}

func startDaemonWithConfig(t testing.TB, cfg daemonConfig) *daemonProcess {
	t.Helper()
	addr := freeAddr(t)
	downloadDir := filepath.Join(t.TempDir(), "downloads")
	if err := os.MkdirAll(downloadDir, 0o755); err != nil {
		t.Fatal(err)
	}

	args := []string{"daemon", "-listen", addr, "-dir", downloadDir, "-log-level", "error"}
	if cfg.secret != "" {
		args = append(args, "-rpc-secret", cfg.secret)
	}
	if cfg.inputFile != "" {
		args = append(args, "-input-file", cfg.inputFile)
	}
	if cfg.saveSession != "" {
		args = append(args, "-save-session", cfg.saveSession)
	}

	output := &lockedBuffer{}
	cmd := exec.Command(goariaBinary(t), args...)
	cmd.Dir = repoRoot(t)
	cmd.Stdout = output
	cmd.Stderr = output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start goaria daemon: %v\n%s", err, output.String())
	}

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
		close(done)
	}()

	d := &daemonProcess{
		endpoint:    "http://" + addr + "/jsonrpc",
		secret:      cfg.secret,
		downloadDir: downloadDir,
		cmd:         cmd,
		done:        done,
		output:      output,
	}
	d.client = &rpcClient{
		endpoint: d.endpoint,
		secret:   cfg.secret,
		http:     &http.Client{Timeout: 5 * time.Second},
	}
	t.Cleanup(func() { d.stop(t) })
	d.waitReady(t)
	return d
}

func (d *daemonProcess) waitReady(t testing.TB) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	payload := []byte(`{"jsonrpc":"2.0","id":"ready","method":"system.listMethods","params":[]}`)
	for time.Now().Before(deadline) {
		select {
		case err, ok := <-d.done:
			if !ok {
				t.Fatalf("goaria daemon exited before readiness\n%s", d.output.String())
			}
			t.Fatalf("goaria daemon exited before readiness: %v\n%s", err, d.output.String())
		default:
		}
		resp, err := d.client.http.Post(d.endpoint, "application/json", bytes.NewReader(payload))
		if err == nil {
			body, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if readErr == nil && resp.StatusCode == http.StatusOK && json.Valid(body) {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("goaria daemon did not become ready at %s\n%s", d.endpoint, d.output.String())
}

func (d *daemonProcess) stop(t testing.TB) {
	t.Helper()
	if _, ok := d.wait(0); ok {
		return
	}
	payload := map[string]any{
		"jsonrpc": "2.0",
		"id":      "cleanup",
		"method":  "aria2.forceShutdown",
		"params":  []any{},
	}
	if d.secret != "" {
		payload["params"] = []any{"token:" + d.secret}
	}
	d.postCleanup(payload)
	if _, ok := d.wait(2 * time.Second); ok {
		return
	}
	_ = d.cmd.Process.Kill()
	_, _ = d.wait(2 * time.Second)
}

func (d *daemonProcess) postCleanup(payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	req, err := http.NewRequest(http.MethodPost, d.endpoint, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.http.Do(req)
	if err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func (d *daemonProcess) wait(timeout time.Duration) (error, bool) {
	if timeout == 0 {
		select {
		case err, ok := <-d.done:
			if !ok {
				return nil, true
			}
			return err, true
		default:
			return nil, false
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err, ok := <-d.done:
		if !ok {
			return nil, true
		}
		return err, true
	case <-timer.C:
		return nil, false
	}
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func goariaBinary(t testing.TB) string {
	t.Helper()
	if path := os.Getenv("GOARIA_BIN"); path != "" {
		return path
	}
	builtBinary.once.Do(func() {
		builtBinary.dir, builtBinary.err = os.MkdirTemp("", "goaria-e2e-bin-*")
		if builtBinary.err != nil {
			return
		}
		builtBinary.path = filepath.Join(builtBinary.dir, "goaria")
		cmd := exec.Command("go", "build", "-o", builtBinary.path, "./cmd/goaria")
		cmd.Dir = repoRoot(t)
		out, err := cmd.CombinedOutput()
		if err != nil {
			builtBinary.err = fmt.Errorf("go build ./cmd/goaria: %w\n%s", err, out)
		}
	})
	if builtBinary.err != nil {
		t.Fatal(builtBinary.err)
	}
	return builtBinary.path
}

func freeAddr(t testing.TB) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func repoRoot(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate e2e test file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), ".."))
}

func newFixtureServer(t testing.TB) *httptest.Server {
	t.Helper()
	payload := bytes.Repeat([]byte("goaria-e2e\n"), 32)
	torrentData, err := os.ReadFile(filepath.Join(repoRoot(t), "test.torrent"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".torrent") {
			w.Header().Set("Content-Type", "application/x-bittorrent")
			_, _ = w.Write(torrentData)
			return
		}
		http.ServeContent(w, r, strings.TrimPrefix(r.URL.Path, "/"), time.Unix(1700000000, 0), bytes.NewReader(payload))
	}))
	t.Cleanup(server.Close)
	return server
}

type behaviorFixture struct {
	*httptest.Server
	lastModified time.Time
	payloads     map[string][]byte

	mu       sync.Mutex
	requests []requestRecord
}

type requestRecord struct {
	Method string
	Path   string
	Header http.Header
}

func newBehaviorFixture(t testing.TB) *behaviorFixture {
	t.Helper()
	torrentData, err := os.ReadFile(filepath.Join(repoRoot(t), "test.torrent"))
	if err != nil {
		t.Fatal(err)
	}
	f := &behaviorFixture{
		lastModified: time.Unix(1700000123, 0).UTC(),
		payloads: map[string][]byte{
			"/side-effect.bin": []byte("side-effect-payload\n"),
			"/fallback.bin":    []byte("fallback-payload\n"),
			"/changed.bin":     []byte("changed-payload\n"),
			"/persisted.bin":   []byte("persisted-payload\n"),
			"/queue-1.bin":     []byte("queue-1-payload\n"),
			"/queue-2.bin":     []byte("queue-2-payload\n"),
			"/queue-3.bin":     []byte("queue-3-payload\n"),
		},
	}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.record(r)
		if strings.HasSuffix(r.URL.Path, ".torrent") {
			w.Header().Set("Content-Type", "application/x-bittorrent")
			_, _ = w.Write(torrentData)
			return
		}
		if r.URL.Path == "/always-503.bin" {
			http.Error(w, "temporary fixture failure", http.StatusServiceUnavailable)
			return
		}
		http.ServeContent(w, r, strings.TrimPrefix(r.URL.Path, "/"), f.lastModified, bytes.NewReader(f.payload(r.URL.Path)))
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *behaviorFixture) payload(path string) []byte {
	if payload, ok := f.payloads[path]; ok {
		return append([]byte(nil), payload...)
	}
	return []byte("generated payload for " + strings.TrimPrefix(path, "/") + "\n")
}

func (f *behaviorFixture) record(r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, requestRecord{
		Method: r.Method,
		Path:   r.URL.Path,
		Header: r.Header.Clone(),
	})
}

func (f *behaviorFixture) records(path string) []requestRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []requestRecord
	for _, r := range f.requests {
		if r.Path == path {
			out = append(out, r)
		}
	}
	return out
}

func (f *behaviorFixture) count(path string) int {
	return len(f.records(path))
}

func (f *behaviorFixture) sawHeader(path, key, value string) bool {
	for _, r := range f.records(path) {
		if r.Header.Get(key) == value {
			return true
		}
	}
	return false
}

func setE2ERangeHeaders(w http.ResponseWriter, data []byte) {
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
}

func parseE2ERange(header string, total int64) (int64, int64, bool) {
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false
	}
	parts := strings.SplitN(strings.TrimPrefix(header, "bytes="), "-", 2)
	if len(parts) != 2 || parts[0] == "" {
		return 0, 0, false
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	end := total - 1
	if parts[1] != "" {
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, false
		}
	}
	if start < 0 || end < start {
		return 0, 0, false
	}
	if end >= total {
		end = total - 1
	}
	return start, end, true
}

func writeE2ERange(w http.ResponseWriter, data []byte, start, end int64) {
	if start < 0 || end < start || start >= int64(len(data)) {
		http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if end >= int64(len(data)) {
		end = int64(len(data)) - 1
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(data[start : end+1])
}

func loadManifest(t testing.TB) coverageManifest {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repoRoot(t), "e2e", "coverage_manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest coverageManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func loadSchema(t testing.TB) map[string]any {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repoRoot(t), "schemas", "aria2-jsonrpc.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatal(err)
	}
	return schema
}

func schemaDefs(t testing.TB) map[string]any {
	t.Helper()
	defs, ok := loadSchema(t)["$defs"].(map[string]any)
	if !ok {
		t.Fatal("schema has no $defs object")
	}
	return defs
}

func schemaOptionProperties(t testing.TB) map[string]any {
	t.Helper()
	defs := schemaDefs(t)
	options, ok := defs["options"].(map[string]any)
	if !ok {
		t.Fatal("schema has no $defs.options object")
	}
	props, ok := options["properties"].(map[string]any)
	if !ok {
		t.Fatal("schema has no $defs.options.properties object")
	}
	return props
}

func finiteEnumValues(node any, defs map[string]any) []any {
	obj, ok := node.(map[string]any)
	if !ok {
		return nil
	}
	if values, ok := obj["enum"].([]any); ok {
		return values
	}
	if ref, ok := obj["$ref"].(string); ok {
		prefix := "#/$defs/"
		if !strings.HasPrefix(ref, prefix) {
			return nil
		}
		return finiteEnumValues(defs[strings.TrimPrefix(ref, prefix)], defs)
	}
	return nil
}

func materializeOptionValue(t testing.TB, raw json.RawMessage, temp string) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return replaceTemp(v, temp)
}

func replaceTemp(v any, temp string) any {
	switch x := v.(type) {
	case string:
		return strings.ReplaceAll(x, "${TEMP}", temp)
	case []any:
		out := make([]any, len(x))
		for i, item := range x {
			out[i] = replaceTemp(item, temp)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, item := range x {
			out[k] = replaceTemp(item, temp)
		}
		return out
	default:
		return v
	}
}

func expectOK(t testing.TB, resp rpcResponse) {
	t.Helper()
	assertStringResult(t, resp, "OK")
}

func assertStringResult(t testing.TB, resp rpcResponse, want string) {
	t.Helper()
	got := stringResult(t, resp)
	if got != want {
		t.Fatalf("result = %q, want %q", got, want)
	}
}

func stringResult(t testing.TB, resp rpcResponse) string {
	t.Helper()
	if resp.Error != nil {
		t.Fatalf("RPC error: %#v", resp.Error)
	}
	var out string
	if err := json.Unmarshal(resp.Result, &out); err != nil {
		t.Fatalf("result is not string: %s: %v", resp.Result, err)
	}
	return out
}

func numberResult(t testing.TB, resp rpcResponse) float64 {
	t.Helper()
	if resp.Error != nil {
		t.Fatalf("RPC error: %#v", resp.Error)
	}
	var out float64
	if err := json.Unmarshal(resp.Result, &out); err != nil {
		t.Fatalf("result is not number: %s: %v", resp.Result, err)
	}
	return out
}

func stringSliceResult(t testing.TB, resp rpcResponse) []string {
	t.Helper()
	if resp.Error != nil {
		t.Fatalf("RPC error: %#v", resp.Error)
	}
	var out []string
	if err := json.Unmarshal(resp.Result, &out); err != nil {
		t.Fatalf("result is not []string: %s: %v", resp.Result, err)
	}
	return out
}

func objectResult(t testing.TB, resp rpcResponse) map[string]any {
	t.Helper()
	if resp.Error != nil {
		t.Fatalf("RPC error: %#v", resp.Error)
	}
	var out map[string]any
	if err := json.Unmarshal(resp.Result, &out); err != nil {
		t.Fatalf("result is not object: %s: %v", resp.Result, err)
	}
	return out
}

func arrayResult(t testing.TB, resp rpcResponse) []any {
	t.Helper()
	if resp.Error != nil {
		t.Fatalf("RPC error: %#v", resp.Error)
	}
	var out []any
	if err := json.Unmarshal(resp.Result, &out); err != nil {
		t.Fatalf("result is not array: %s: %v", resp.Result, err)
	}
	return out
}

func assertRPCError(t testing.TB, resp rpcResponse, code int, contains string) {
	t.Helper()
	if resp.Error == nil {
		t.Fatalf("expected RPC error %d containing %q, got success %s", code, contains, resp.Result)
	}
	if resp.Error.Code != code || !strings.Contains(resp.Error.Message, contains) {
		t.Fatalf("RPC error = %#v, want code %d containing %q", resp.Error, code, contains)
	}
}

func waitForDownloadStatus(t testing.TB, client *rpcClient, gid, want string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last map[string]any
	for time.Now().Before(deadline) {
		resp := client.Call(t, "aria2.tellStatus", gid, []string{"gid", "status", "completedLength", "totalLength", "errorMessage", "files"})
		last = objectResult(t, resp)
		if last["status"] == want {
			return last
		}
		if last["status"] == "error" && want != "error" {
			t.Fatalf("download %s errored while waiting for %s: %#v", gid, want, last)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to become %s; last status=%#v", gid, want, last)
	return nil
}

func assertFileBytes(t testing.TB, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s bytes = %q, want %q", path, got, want)
	}
}

func assertGIDOrder(t testing.TB, rows []any, want []string) {
	t.Helper()
	got := make([]string, 0, len(rows))
	for _, row := range rows {
		obj, ok := row.(map[string]any)
		if !ok {
			t.Fatalf("row is not object: %#v", row)
		}
		gid, ok := obj["gid"].(string)
		if !ok {
			t.Fatalf("row has no gid string: %#v", row)
		}
		got = append(got, gid)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("gid order = %v, want %v", got, want)
	}
}

func assertMapValue(t testing.TB, got map[string]any, key string, want any) {
	t.Helper()
	if !reflect.DeepEqual(got[key], want) {
		t.Fatalf("%s = %#v, want %#v in %#v", key, got[key], want, got)
	}
}

func assertOptionValue(t testing.TB, got map[string]any, key string, want any) {
	t.Helper()
	normalized := normalizeOptionWant(want)
	if !reflect.DeepEqual(got[key], normalized) {
		t.Fatalf("option %s = %#v, want %#v in %#v", key, got[key], normalized, got)
	}
}

func normalizeOptionWant(v any) any {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case float64:
		return fmt.Sprintf("%g", x)
	case []any:
		out := make([]any, len(x))
		for i, item := range x {
			out[i] = fmt.Sprint(item)
		}
		return out
	default:
		return fmt.Sprint(x)
	}
}

func assertSameStrings(t testing.TB, label string, got, want []string) {
	t.Helper()
	got = append([]string(nil), got...)
	want = append([]string(nil), want...)
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s mismatch\nmissing: %v\nextra: %v\ngot: %v\nwant: %v", label, stringSetDiff(want, got), stringSetDiff(got, want), got, want)
	}
}

func stringSetDiff(a, b []string) []string {
	bs := make(map[string]struct{}, len(b))
	for _, s := range b {
		bs[s] = struct{}{}
	}
	var out []string
	for _, s := range a {
		if _, ok := bs[s]; !ok {
			out = append(out, s)
		}
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
