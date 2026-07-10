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
`New()` configures with `With*` methods and finalizes with `Build()`, producing
a `raft.Node`.

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
	node := libraft.New().Build()

	fmt.Printf("node: %v\n", node)
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
    Build() raft.Node   // terminal: assembles and returns the node
}

func New() Builder   // unconfigured builder
```

Configuration (`With*` methods) and node assembly land in upcoming revisions;
until then `Build()` returns nil.

## Examples

Self-contained programs in [`./examples`](./examples):

| Example | Demonstrates                                          |
| ------- | ----------------------------------------------------- |
| `basic` | Smallest wiring — `New` + `Build`.                    |

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
