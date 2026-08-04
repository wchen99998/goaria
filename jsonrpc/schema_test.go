package jsonrpc

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestJSONRPCSchemaAcceptsKnownRequests(t *testing.T) {
	payload, err := os.ReadFile("../schemas/aria2-jsonrpc.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(payload) {
		t.Fatal("schema is not valid JSON")
	}
	compiled, err := jsonschema.NewCompiler().Compile("../schemas/aria2-jsonrpc.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, example := range []string{
		`{"jsonrpc":"2.0","id":"1","method":"aria2.addUri","params":[["https://example.com/file"],{"out":"file"}]}`,
		`{"jsonrpc":"2.0","id":"torrent","method":"aria2.addTorrent","params":["ZHVtbXk",{"pause":"true"},0]}`,
		`{"jsonrpc":"2.0","id":"torrent-ext-filter","method":"aria2.addTorrent","params":["ZHVtbXk",{"goaria-select-file-ext":".mp4,.mkv","goaria-exclude-file-ext":["nfo","txt"]},0]}`,
		`{"jsonrpc":"2.0","id":"torrent-url","method":"aria2.addTorrent","params":["https://example.com/file.torrent",{"pause":"true","header":["Authorization: Bearer token"],"goaria-max-torrent-size":"16M"},0]}`,
		`{"jsonrpc":"2.0","id":"torrent-webseed","method":"aria2.addTorrent","params":["ZHVtbXk",["https://example.com/webseed"],{"pause":"true"},0]}`,
		`{"jsonrpc":"2.0","id":"torrent-url-webseed","method":"aria2.addTorrent","params":["https://example.com/file.torrent",["https://cdn.example.com/payload-file"],{"pause":"true","user-agent":"goaria-client/1.0","load-cookies":"/tmp/cookies.txt"},0]}`,
		`{"jsonrpc":"2.0","id":"torrent-token-options","method":"aria2.addTorrent","params":["token:secret","ZHVtbXk",{"pause":"true"},0]}`,
		`{"jsonrpc":"2.0","id":"2","method":"aria2.tellStatus","params":["token:secret","0123456789abcdef",["gid","status"]]}`,
		`{"jsonrpc":"2.0","id":"active","method":"aria2.tellActive","params":[["gid","status"]]}`,
		`{"jsonrpc":"2.0","id":"waiting","method":"aria2.tellWaiting","params":[0,10,["gid","status"]]}`,
		`{"jsonrpc":"2.0","id":"stopped","method":"aria2.tellStopped","params":["token:secret",-1,10,["gid","status"]]}`,
		`{"jsonrpc":"2.0","id":"uri","method":"aria2.changeUri","params":["0123456789abcdef",1,[],["https://example.com/file"],0]}`,
		`{"jsonrpc":"2.0","id":"uri-token","method":"aria2.changeUri","params":["token:secret","0123456789abcdef",1,[],["https://example.com/file"],0]}`,
		`{"jsonrpc":"2.0","id":"3","method":"system.multicall","params":[[{"methodName":"system.listMethods","params":[]}]]}`,
		`[{"jsonrpc":"2.0","id":"4","method":"system.listMethods","params":[]}]`,
	} {
		var value any
		if err := json.Unmarshal([]byte(example), &value); err != nil {
			t.Fatal(err)
		}
		if err := compiled.Validate(value); err != nil {
			t.Fatalf("example should validate: %v\n%s", err, example)
		}
	}
}

func TestJSONRPCSchemaRejectsInvalidListAndChangeURIShapes(t *testing.T) {
	compiled, err := jsonschema.NewCompiler().Compile("../schemas/aria2-jsonrpc.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, example := range []string{
		`{"jsonrpc":"2.0","id":"waiting","method":"aria2.tellWaiting","params":[]}`,
		`{"jsonrpc":"2.0","id":"active","method":"aria2.tellActive","params":[0,10]}`,
		`{"jsonrpc":"2.0","id":"add","method":"aria2.addUri"}`,
		`{"jsonrpc":"2.0","id":"remove","method":"aria2.remove"}`,
		`{"jsonrpc":"2.0","id":"option","method":"aria2.changeOption"}`,
		`{"jsonrpc":"2.0","id":"uri","method":"aria2.changeUri","params":["0123456789abcdef",-1,[],[]]}`,
		`{"jsonrpc":"2.0","id":"uri","method":"aria2.changeUri","params":[1,"0123456789abcdef",[],[]]}`,
		`{"jsonrpc":"2.0","id":"uri","method":"aria2.changeUri"}`,
		`{"jsonrpc":"2.0","id":"multi","method":"system.multicall","params":[[{"methodName":"system.multicall","params":[[]]}]]}`,
	} {
		var value any
		if err := json.Unmarshal([]byte(example), &value); err != nil {
			t.Fatal(err)
		}
		if err := compiled.Validate(value); err == nil {
			t.Fatalf("invalid parameter shape should not validate:\n%s", example)
		}
	}
	batch := make([]any, maxBatchCalls+1)
	for i := range batch {
		batch[i] = map[string]any{"jsonrpc": "2.0", "id": i, "method": "system.listMethods"}
	}
	if err := compiled.Validate(batch); err == nil {
		t.Fatalf("batch with %d calls should not validate", len(batch))
	}
}
