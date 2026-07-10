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
variable. See [The install seam](#the-install-seam) below for the mechanism
and [`v3/LIMITATIONS.md`](./v3/LIMITATIONS.md) for the behavioral edges.

## Quick Start

```sh
go get github.com/cnuss/libraft
```

Blank-import libraft in the etcd binary's `main` (or any program embedding
etcd); it patches `(*bootstrappedRaft).newRaftNode` and
`serverstorage.OpenBackend` at `init` when `ETCD_S3LOG_URL` is set (a no-op
otherwise):

```go
package main

import (
	_ "github.com/cnuss/libraft"
)

// ... build/run etcd as usual ...
```

`ETCD_S3LOG_URL` is the http(s) endpoint of any S3-compatible store, followed
by the bucket (lowercase) and an optional prefix:

```sh
export ETCD_S3LOG_URL=https://s3.us-east-1.amazonaws.com/my-bucket/my-prefix
export AWS_ACCESS_KEY_ID=…  AWS_SECRET_ACCESS_KEY=…
etcd --data-dir /var/lib/etcd
```

Or try it locally against MinIO with the bundled example (credentials default
to `minioadmin`/`minioadmin`):

```sh
docker run -d -p 9000:9000 minio/minio server /data
cd examples/basic
ETCD_S3LOG_URL=http://127.0.0.1:9000/libraft-demo/basic go run .
```

Run it twice: the second run starts from a brand-new data directory yet reads
back the first run's value, restored from S3.

## Layout

Three packages:

```
github.com/cnuss/libraft             — the import seam: blank-import this to
                                     install s3raft (re-exports EnvURL).
github.com/cnuss/libraft/v3          — s3raft core: S3 CAS log, raft.Node over
                                     the log, bbolt checkpoint/restore, notify,
                                     batch, metrics.
github.com/cnuss/libraft/v3/reflect  — the installer: monkey-patches etcd's
                                     raft construction into the core.
```

Hosts blank-import the root package (which pulls in `v3/reflect`); the core
exports the seam the installer calls
(`Start`, `NewRaftNode`, `S3OpenBackend`, `ActiveNS`, `EnvURL`, `Logger`). For
the file-by-file map, see
[CONTRIBUTING.md → Where to find things](./CONTRIBUTING.md#where-to-find-things).

## The install seam

The installer rewrites the machine-code prologue of two functions with an
unconditional jump into the core:

| Patched target                     | Replacement                     |
| ---------------------------------- | ------------------------------- |
| `(*bootstrappedRaft).newRaftNode`  | `v3.NewRaftNode` → `v3.Start`   |
| `serverstorage.OpenBackend`        | `v3.S3OpenBackend`              |

`newRaftNode` is etcd's sole raft-construction site — the one call that also
carries the snapshotter, WAL, and `*membership.RaftCluster` (whose `ID()` is
the etcd cluster ID). It is unexported, so its code address is reached with
`//go:linkname` and its unexported argument/return types are reconstructed as
byte-identical layout mirrors in `v3.NewRaftNode`; etcd calls its own methods
on the returned pointer, so only the memory layout must match. Platform
primitives: `mach_vm_protect` on darwin, `mprotect` on linux, `VirtualProtect`
on windows; amd64 and arm64.

Patching an unexported symbol inside `etcdserver` was once reverted for a
build-layout-dependent SIGBUS (the target's text page could share with the
patcher's own helpers). It is safe here because the patcher lives in a separate
package that the linker places pages away from `etcdserver`; verify with
`go tool nm` when touching the installer (see CONTRIBUTING).

## When to use it

s3raft trades write latency for operational simplicity: every write is an S3
round-trip, so the latency floor is object-store RTT — physics, not tuning.
That makes it a fit for **low-write control planes** (configuration, service
discovery, CI locks) that want etcd's API without managing raft quorum, disks,
and membership. It is not an etcd replacement for kube-apiserver-class write
loads, and it is a mode with documented semantic differences
([`v3/LIMITATIONS.md`](./v3/LIMITATIONS.md)) — not a silent drop-in. The CAS
ordering that makes the shared log safe is modeled in TLA+
([`v3/tla/`](./v3/tla)).

## Example

Self-contained program in [`./examples`](./examples):

| Example | Demonstrates                                                |
| ------- | ---------------------------------------------------------- |
| `basic` | Embedded etcd + the blank import; with `ETCD_S3LOG_URL` set, the raft log lives in S3 and state survives a wiped data dir. |

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
