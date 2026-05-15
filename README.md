# goaria

`goaria` is an aria2-compatible download engine written in Go. It can be embedded directly as a library or run as a CLI daemon, and JSON-RPC is provided by an optional `github.com/wchen99998/goaria/jsonrpc` package.

## Requirements

- Go 1.26.1

## CLI

Run an aria2-style JSON-RPC daemon:

```sh
go run ./cmd/goaria daemon -dir ./downloads -rpc-secret secret
```

Persist and restore the daemon queue with an aria2-style session file:

```sh
go run ./cmd/goaria daemon -dir ./downloads -input-file ./goaria.session -save-session ./goaria.session
```

Download directly from the CLI:

```sh
go run ./cmd/goaria -dir ./downloads -split 8 -max-connection-per-server 8 https://example.com/file.bin
```

For CDN compatibility testing, the direct CLI also accepts `-http-version` (`auto`, `1.1`, `2`, or `3`) and `-user-agent`.

The JSON-RPC endpoint is `/jsonrpc`, matching aria2. HTTP POST, HTTP GET with base64 `params` plus raw URL-encoded JSON `params`, JSONP, batch requests, `system.multicall`, and JSON-RPC over WebSocket are implemented.

## Library

Use the root package when embedding the downloader without an RPC server:

```go
import "github.com/wchen99998/goaria"

engine, err := goaria.NewEngine(goaria.Config{Dir: "./downloads", Logger: logger})
if err != nil {
    return err
}
defer engine.Close(context.Background())

gid, err := engine.AddURI([]string{"https://example.com/file.bin"}, goaria.Options{
    "split": "8",
    "max-connection-per-server": "8",
}, nil)
```

Torrent downloads can be processed file-by-file by setting `Config.TorrentFileHandler`. The handler receives a `TorrentFileLease` for each selected torrent file as soon as that file is complete, while the rest of the torrent can continue downloading. Handlers for different files can run concurrently. If the handler returns nil, it must eventually call `TorrentFileLease.Release(ctx)` or `TorrentFileLease.Discard(ctx)`; goaria keeps the torrent download active until the lease is finalized. If the handler returns an error before finalizing the lease, goaria releases the file storage and reports the handler error as the download error. Set the goaria extension option `goaria-torrent-max-active-files` to keep only that many files downloading or leased at a time.

To expose JSON-RPC, import the optional adapter package:

```go
import "github.com/wchen99998/goaria/jsonrpc"

srv := jsonrpc.NewServer(engine, jsonrpc.Config{Addr: "127.0.0.1:6800", Secret: "secret"})
err := srv.ListenAndServe(ctx)
```

If you already own the HTTP server or router, mount only the JSON-RPC endpoint:

```go
srv := jsonrpc.NewServer(engine, jsonrpc.Config{Secret: "secret"})
mux.Handle("/downloads/rpc", srv.JSONRPCHandler())
```

With Gin, wrap the same handler:

```go
router.Any("/downloads/rpc", gin.WrapH(srv.JSONRPCHandler()))
```

For a server owned by `jsonrpc.Server`, use `Config.Path` to change the route:

```go
srv := jsonrpc.NewServer(engine, jsonrpc.Config{Addr: "127.0.0.1:6800", Path: "/downloads/rpc", Secret: "secret"})
```

## RPC Coverage

Most of the aria2 JSON-RPC method surface is implemented. Unsupported methods are not advertised through `system.listMethods`:

- `aria2.addUri`
- `aria2.addTorrent`
- `aria2.remove`
- `aria2.forceRemove`
- `aria2.pause`
- `aria2.pauseAll`
- `aria2.forcePause`
- `aria2.forcePauseAll`
- `aria2.unpause`
- `aria2.unpauseAll`
- `aria2.tellStatus`
- `aria2.getUris`
- `aria2.getFiles`
- `aria2.getPeers`
- `aria2.getServers`
- `aria2.tellActive`
- `aria2.tellWaiting`
- `aria2.tellStopped`
- `aria2.changePosition`
- `aria2.changeUri`
- `aria2.getOption`
- `aria2.changeOption`
- `aria2.getGlobalOption`
- `aria2.changeGlobalOption`
- `aria2.getGlobalStat`
- `aria2.purgeDownloadResult`
- `aria2.removeDownloadResult`
- `aria2.getVersion`
- `aria2.getSessionInfo`
- `aria2.shutdown`
- `aria2.forceShutdown`
- `aria2.saveSession`
- `system.multicall`
- `system.listMethods`
- `system.listNotifications`

`aria2.addTorrent` accepts base64 `.torrent` payloads and HTTP/S `.torrent` URLs, `aria2.addUri` accepts BitTorrent magnet URIs, and torrent status exposes `infoHash`, `numSeeders`, `seeder`, `bittorrent`, torrent file metadata, and peer data. HTTP and HTTPS transfers use ranged segmented downloads when the server supports byte ranges. `aria2.addMetalink` remains explicitly unsupported.

## BitTorrent Coverage

Implemented BitTorrent behaviors include:

- base64 `.torrent` uploads via `aria2.addTorrent`
- HTTP/S `.torrent` URL fetching via `aria2.addTorrent`, persisted as fetched torrent bytes in sessions
- magnet registration through `aria2.addUri`
- webseed URI forwarding from the `aria2.addTorrent` URI parameter
- selected file downloads through `select-file`
- extension-based selected file downloads through `goaria-select-file-ext` and `goaria-exclude-file-ext`
- output path overrides through `index-out`
- qBittorrent-like peer-visible identity by default for tracker user-agent, BEP20 peer ID prefix, and extended handshake version
- `aria2.getFiles`, `aria2.getPeers`, `aria2.tellStatus`, and `aria2.getVersion` BitTorrent fields
- embedded streaming with `Config.TorrentFileHandler`, explicit `TorrentFileLease.Release`/`Discard`, and completed file removal
- test-only direct peer injection through the goaria extension option `goaria-peer-addrs`

## HTTP Download Coverage

Implemented HTTP/S behaviors include:

- segmented range downloads through `split`, `max-connection-per-server`, and `min-split-size`
- retry and recovery through `max-tries`, `retry-wait`, and `max-file-not-found`
- resume through `continue=true` when servers support byte ranges
- queue/session persistence through `save-session`, `aria2.saveSession`, and daemon `-input-file` / `-save-session`
- fallback across multiple HTTP/S URIs for the same resource
- per-download HTTP headers, `user-agent`, `referer`, `http-no-cache`, and Basic auth through `http-user` / `http-passwd`
- conditional requests through `conditional-get`, optional `use-head=false`, `http-accept-gzip`, and `http-auth-challenge`
- `remote-time` using the server `Last-Modified` header
- checksum validation for `md5`, `sha-1`, `sha-224`, `sha-256`, `sha-384`, and `sha-512`
- HTTP proxies via `http-proxy`, `https-proxy`, `all-proxy`, proxy credentials, and `no-proxy`
- SOCKS5 proxies through `socks5://` or `socks5h://` proxy URLs

## Test Harness

The test suite includes normal unit/integration tests, race tests, deterministic chaos tests, proxy tests, and fuzz targets:

```sh
make test
make e2e
make race
make chaos
make chaos10x
make protocol
make scale
make profile-scale
make fuzz
make fuzz-hour
```

`make e2e` runs the standalone JSON-RPC end-to-end suite from the nested `e2e` module. It builds and spawns the `goaria` daemon as a child process, drives the public JSON-RPC endpoint over HTTP GET, HTTP POST, JSONP, batch requests, `system.multicall`, and WebSocket, and audits the E2E method and option coverage against `schemas/aria2-jsonrpc.schema.json` plus `e2e/coverage_manifest.json`. The suite also checks observable behavior and side effects, including downloaded file bytes and mtimes, source-server request headers, URI mutation, queue ordering, removal/purge state, and session-file restoration. To test a prebuilt daemon binary instead of building from source, set `GOARIA_BIN=/path/to/goaria` before running `cd e2e && go test ./...`.

Chaos coverage intentionally injects mid-stream connection drops, transient 503 range failures, retries over multipart downloads, and proxy routing scenarios.

Protocol coverage includes local HTTP/1.1, HTTP/2, and HTTP/3 servers. HTTP/3 support uses quic-go's `http3.Transport`, which implements RFC 9114 HTTP/3 over QUIC.

Scale coverage includes a local 1250-download concurrency test through `make scale`.
`make profile-scale` profiles the 1250-download benchmark and writes CPU/memory profiles under `profiles/`.

Live public-internet checks are opt-in to keep the default suite deterministic:

```sh
GOARIA_LIVE_TESTS=1 make live-chaos
```

The live targets can be overridden with `GOARIA_LIVE_HTTP1_URL`, `GOARIA_LIVE_HTTP2_URL`, `GOARIA_LIVE_HTTP3_URL`, and `GOARIA_LIVE_CHAOS_HTTP_URL`.
