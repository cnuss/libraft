# libraft

[![Go Reference](https://pkg.go.dev/badge/github.com/cnuss/libraft.svg)](https://pkg.go.dev/github.com/cnuss/libraft)
[![Go Report Card](https://goreportcard.com/badge/github.com/cnuss/libraft)](https://goreportcard.com/report/github.com/cnuss/libraft)
[![CI](https://github.com/cnuss/libraft/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/cnuss/libraft/actions/workflows/ci.yml)
[![CodeQL](https://github.com/cnuss/libraft/actions/workflows/codeql.yml/badge.svg?branch=main)](https://github.com/cnuss/libraft/actions/workflows/codeql.yml)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/cnuss/libraft/badge)](https://scorecard.dev/viewer/?uri=github.com/cnuss/libraft)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

`libraft` is a thin, stable façade over stable/alpha versioned packages
(`v1` stable contract, `v1alpha1` mutable implementation), with CI, CodeQL,
OpenSSF Scorecard, cosign-signed releases, Dependabot, examples, and an e2e
harness.

The API is a builder around [etcd raft](https://github.com/etcd-io/raft):
`New()` configures with `With*` methods and finalizes with the terminal
`Node()`, producing a `Node` (a wrapper around `raft.Node`).

## Quick Start

```sh
go get github.com/cnuss/libraft
```

```go
package main

import (
	"fmt"

	"github.com/cnuss/libraft"
)

func main() {
	node := libraft.New().Node()

	fmt.Printf("node: %T\n", node)
}
```

(Full source: [`examples/basic/main.go`](./examples/basic/main.go).)

## Layout

Three packages, stable/alpha versioning:

```
github.com/cnuss/libraft           — root façade. Stable surface (New).
github.com/cnuss/libraft/v1        — stable Builder interface.
github.com/cnuss/libraft/v1alpha1  — current implementation. May change
                                   between alpha revisions.
```

Application code imports the root (`libraft.New()…`). Code that needs to
declare types against the interface imports `v1`. Direct access to the
`BuilderImpl` struct lives in `v1alpha1`.

For the file-by-file map, see
[CONTRIBUTING.md → Where to find things](./CONTRIBUTING.md#where-to-find-things).

## API at a glance

```go
type Builder interface {
    WithContext(ctx context.Context) Builder   // ties the builder's lifetime to ctx

    // one chainable setter per raft.Config field; see v1/v1.go for the full set
    WithID(id uint64) Builder
    WithElectionTick(ticks int) Builder
    WithHeartbeatTick(ticks int) Builder
    WithStorage(storage raft.Storage) Builder
    WithLogger(logger raft.Logger) Builder
    // …

    Node() Node                                // terminal: assembles and returns the node
}

type Node interface {
    raft.Node   // wraps the upstream interface so the surface can grow

    WithPeers(peers []raft.Peer) Node   // start: bootstrap a new cluster from peers
    WithoutPeers() Node                 // restart: recover peers from storage
}

func New() Builder   // unconfigured builder
```

The terminal `Node()` carries the assembled config; the underlying raft node
starts when `WithPeers` (bootstrap, `raft.StartNode`) or `WithoutPeers`
(restart from storage, `raft.RestartNode`) is called on it, and stops when the
context given via `WithContext` is done.

## Examples

Self-contained programs in [`./examples`](./examples):

| Example | Demonstrates                                          |
| ------- | ----------------------------------------------------- |
| `basic` | Smallest wiring — `New` + terminal `Node`.            |

Run one locally:

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
