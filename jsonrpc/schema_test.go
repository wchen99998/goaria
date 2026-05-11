package jsonrpc

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	chaff "github.com/ryanolee/go-chaff"
	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/wchen99998/goaria"
)

func TestJSONRPCSchemaWithGoChaff(t *testing.T) {
	generator, err := chaff.ParseSchemaFileWithDefaults("../schemas/aria2-jsonrpc.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	engine, err := goaria.NewEngine(goaria.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	rpc := NewHandler(engine, "schema-secret")

	opts := &chaff.GeneratorOptions{
		DefaultStringMaxLength:     64,
		DefaultArrayMaxItems:       3,
		DefaultObjectMaxProperties: 8,
		MaximumGenerationSteps:     500,
		CutoffGenerationSteps:      1000,
	}
	for i := 0; i < 64; i++ {
		value := generator.Generate(opts)
		payload, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal generated payload %d: %v", i, err)
		}
		if !json.Valid(payload) {
			t.Fatalf("go-chaff generated invalid JSON %d: %s", i, payload)
		}
		response, ok := rpc.HandlePayload(payload)
		if ok && !json.Valid(response) {
			t.Fatalf("RPC response to generated payload %d is invalid JSON: %s", i, response)
		}
	}
}

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
		`{"jsonrpc":"2.0","id":"torrent-webseed","method":"aria2.addTorrent","params":["ZHVtbXk",["https://example.com/webseed"],{"pause":"true"},0]}`,
		`{"jsonrpc":"2.0","id":"torrent-token-options","method":"aria2.addTorrent","params":["token:secret","ZHVtbXk",{"pause":"true"},0]}`,
		`{"jsonrpc":"2.0","id":"2","method":"aria2.tellStatus","params":["token:secret","0123456789abcdef",["gid","status"]]}`,
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
