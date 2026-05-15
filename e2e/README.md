# goaria JSON-RPC E2E

This module is intentionally separate from the main `goaria` module. Tests in
this directory do not import `github.com/wchen99998/goaria` or its `jsonrpc`
package. They build or use a daemon binary, spawn it as a child process, and
exercise only the public JSON-RPC endpoint.

Run from the repository root:

```sh
make e2e
```

Run directly:

```sh
cd e2e
go test ./...
```

Run against a prebuilt binary:

```sh
cd e2e
GOARIA_BIN=/path/to/goaria go test ./...
```

`coverage_manifest.json` is the E2E contract manifest. The tests compare it to
the daemon's `system.listMethods`, `system.listNotifications`, and the
repository JSON-RPC schema so newly added methods or options fail E2E until
they have black-box coverage.

The tests are expected to prove behavior, not just response shape. They assert
side effects that an external client can observe, such as files written to the
daemon download directory, remote `Last-Modified` mtimes, request headers seen
by a source server, queue ordering, result removal, and session restoration
from a saved file.
