# libraft

[![Go Reference](https://pkg.go.dev/badge/github.com/cnuss/libraft.svg)](https://pkg.go.dev/github.com/cnuss/libraft)
[![Go Report Card](https://goreportcard.com/badge/github.com/cnuss/libraft)](https://goreportcard.com/report/github.com/cnuss/libraft)
[![CI](https://github.com/cnuss/libraft/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/cnuss/libraft/actions/workflows/ci.yml)
[![CodeQL](https://github.com/cnuss/libraft/actions/workflows/codeql.yml/badge.svg?branch=main)](https://github.com/cnuss/libraft/actions/workflows/codeql.yml)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/cnuss/libraft/badge)](https://scorecard.dev/viewer/?uri=github.com/cnuss/libraft)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

`libraft` (**s3raft**) replaces etcd's raft consensus with an
**S3-compatible object store**. S3 conditional writes (`If-None-Match` / CAS)
build a shared, totally-ordered append log — and that log *is* the raft log.
Every node runs its own etcd apply loop over the same log, so anything computed
at apply time (revision, consistent index, auth state) is a deterministic
function of the log and stays identical across nodes. Ships with CI, CodeQL,
OpenSSF Scorecard, cosign-signed releases, Dependabot, an example, and an e2e
harness.

It installs into etcd by machine-code monkey-patch — **no edits to etcd
source** — triggered by a blank import plus the `ETCD_S3LOG_URL` environment
variable. See [`v3/DEVNOTES.md`](./v3/DEVNOTES.md) for the mechanism and
[`v3/LIMITATIONS.md`](./v3/LIMITATIONS.md) for the behavioral edges.

## Quick Start

```sh
go get github.com/cnuss/libraft/v3
```

Blank-import the installer in the etcd binary's `main`; it patches
`raft.StartNode`, `raft.RestartNode` and `serverstorage.OpenBackend` at `init`
when `ETCD_S3LOG_URL` is set (a no-op otherwise):

```go
package main

import (
	_ "github.com/cnuss/libraft/v3/reflect"
)

// ... build/run etcd as usual ...
```

```sh
export ETCD_S3LOG_URL=s3://my-bucket/my-prefix   # bucket name must be lowercase
export AWS_ACCESS_KEY_ID=…  AWS_SECRET_ACCESS_KEY=…
etcd --data-dir /var/lib/etcd
```

(Minimal install call site: [`examples/basic/main.go`](./examples/basic/main.go).)

## Layout

Two packages:

```
github.com/cnuss/libraft/v3          — s3raft core: S3 CAS log, raft.Node over
                                     the log, bbolt checkpoint/restore, notify,
                                     batch, metrics.
github.com/cnuss/libraft/v3/reflect  — the installer: blank-import to monkey-patch
                                     etcd's raft entry points into the core.
```

Hosts blank-import `v3/reflect`; the core exports the seam it calls
(`Start`, `S3OpenBackend`, `ActiveNS`, `EnvURL`, `Logger`). For the
file-by-file map, see
[CONTRIBUTING.md → Where to find things](./CONTRIBUTING.md#where-to-find-things).

## The install seam

The installer rewrites the machine-code prologue of three exported functions
with an unconditional jump into the core:

| Patched target              | Replacement                    |
| --------------------------- | ------------------------------ |
| `raft.StartNode`            | `s3StartNode` → `v3.Start`     |
| `raft.RestartNode`          | `s3RestartNode` → `v3.Start`   |
| `serverstorage.OpenBackend` | `v3.S3OpenBackend`             |

Only exported far-module entry points are patched (never etcd's unexported
`newRaftNode`), so the replacements are expressible with exported types alone
and never collide with the patcher's own text pages. Platform primitives:
`mach_vm_protect` on darwin, `mprotect` on linux, `VirtualProtect` on windows;
amd64 and arm64.

## Example

Self-contained program in [`./examples`](./examples):

| Example | Demonstrates                                                |
| ------- | ---------------------------------------------------------- |
| `basic` | Blank-import the installer; print the activation env var.  |

Run it locally:

```sh
make run basic
```

## Testing

```sh
make test   # library unit + fuzz tests (fast, in-package)
make e2e    # builds and runs every example binary, asserts its output
```

`make e2e` runs `go test -count=1 -v ./e2e`. The `-count=1` defeats the test
cache, since the harness builds the example binaries at runtime and the cache
key wouldn't otherwise pick up example source changes.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the local dev loop, release
process, and what makes a good example.

## License

[MIT](./LICENSE)
