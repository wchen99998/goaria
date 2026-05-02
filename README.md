# goaria

`goaria` is an aria2-compatible download engine written in Go. It can be embedded directly as a library or run as a CLI daemon, and JSON-RPC is provided by an optional `goaria/jsonrpc` package.

## Requirements

- Go 1.26.1

## CLI

Run an aria2-style JSON-RPC daemon:

```sh
go run ./cmd/goaria daemon -listen :6800 -dir ./downloads -rpc-secret secret
```

Download directly from the CLI:

```sh
go run ./cmd/goaria -dir ./downloads -split 8 -max-connection-per-server 8 https://example.com/file.bin
```

The JSON-RPC endpoint is `/jsonrpc`, matching aria2. HTTP POST, HTTP GET with base64 `params`, JSONP, batch requests, `system.multicall`, and JSON-RPC over WebSocket are implemented.

## Library

Use the root package when embedding the downloader without an RPC server:

```go
import "goaria"

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

To expose JSON-RPC, import the optional adapter package:

```go
import "goaria/jsonrpc"

srv := jsonrpc.NewServer(engine, jsonrpc.Config{Addr: ":6800", Secret: "secret"})
err := srv.ListenAndServe(ctx)
```

## RPC Coverage

The aria2 JSON-RPC method surface is implemented:

- `aria2.addUri`
- `aria2.addTorrent`
- `aria2.addMetalink`
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

`aria2.addTorrent`, `aria2.addMetalink`, and BitTorrent-only peer data return explicit unsupported or empty HTTP/S-only results. HTTP and HTTPS transfers use ranged segmented downloads when the server supports byte ranges.

## HTTP Download Coverage

Implemented HTTP/S behaviors include:

- segmented range downloads through `split`, `max-connection-per-server`, and `min-split-size`
- retry and recovery through `max-tries`, `retry-wait`, and `max-file-not-found`
- resume through `continue=true` when servers support byte ranges
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
make race
make chaos
make chaos10x
make protocol
make scale
make profile-scale
make fuzz
make fuzz-hour
```

Chaos coverage intentionally injects mid-stream connection drops, transient 503 range failures, retries over multipart downloads, and proxy routing scenarios.

Protocol coverage includes local HTTP/1.1, HTTP/2, and HTTP/3 servers. HTTP/3 support uses quic-go's `http3.Transport`, which implements RFC 9114 HTTP/3 over QUIC.

Scale coverage includes a local 1250-download concurrency test through `make scale`.
`make profile-scale` profiles the 1250-download benchmark and writes CPU/memory profiles under `profiles/`.

Live public-internet checks are opt-in to keep the default suite deterministic:

```sh
GOARIA_LIVE_TESTS=1 make live-chaos
```

The live targets can be overridden with `GOARIA_LIVE_HTTP1_URL`, `GOARIA_LIVE_HTTP2_URL`, `GOARIA_LIVE_HTTP3_URL`, and `GOARIA_LIVE_CHAOS_HTTP_URL`.
