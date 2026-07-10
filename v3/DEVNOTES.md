# DEVNOTES — s3raft (libraft/v3)

Working notes for the next session. Read this **and** `LIMITATIONS.md` before touching
anything. `LIMITATIONS.md` documents the behavioral edges (force-new-cluster, single-node
assumptions, etc.); this file documents the _port_ state and the traps.

## What this is

s3raft replaces etcd's raft consensus with an **S3-compatible object store**. S3 conditional
writes (CAS / `If-None-Match`) build a shared, totally-ordered append log; that log IS the raft
log. `tla/S3RaftCAS.tla` is the TLA+ model of the CAS ordering.

## Provenance

Copied from the etcd tree, then reorganized into two packages:

- Source: `~/cnuss/etcd` → `server/etcdserver/s3raft/*`
- Source commit: `0ab3790793d54f7709d9c9b62175d200aadb483e` (2026-07-09)
- Copied + reorganized: 2026-07-10

Layout (differs from the flat etcd `s3raft` package):

- **`github.com/cnuss/libraft/v3`** — `package v3`, the s3raft **core** (S3 log, node, checkpoint,
  notify, batch, metrics). Exports the seam the installer calls: `Start`, `S3OpenBackend`,
  `ActiveNS`, `EnvURL`, `Logger` (logger extracted to `logger.go`).
- **`github.com/cnuss/libraft/v3/reflect`** — `package reflect`, the **installer** (monkey-patch).
  The old `hijack*.go` files, renamed `init*.go`. Blank-import this to install; it patches the raft
  entry points to jump into `v3.*`.

**This is a copy, not a move.** The etcd tree still has its own flat `s3raft` package (with the
hijack files inline). If this becomes the source of truth, delete the etcd copy and have etcd
`import _ "github.com/cnuss/libraft/v3/reflect"`. Nothing here compiles yet — go.mod deps unwired
(below), and the core↔installer split is fresh; expect loose ends.

## THE install mechanism is a monkey-patch — do not "fix" it

s3raft installs itself by **machine-code monkey-patch**, in the `v3/reflect` package, triggered by
a blank import (`import _ "github.com/cnuss/libraft/v3/reflect"`) plus the `ETCD_S3LOG_URL` env var.
There are NO edits to etcd source. This is intentional and load-bearing (see the persistent memory
note "s3raft hijack is intentional" — never suggest replacing the monkey-patch install mechanism).

`reflect/init.go:init()` (fires only when `ETCD_S3LOG_URL` is set) rewrites the machine-code prologue
of three functions with an unconditional jump to our replacements:

| Patched target              | Module (why safe)         | Replacement                    |
| --------------------------- | ------------------------- | ------------------------------ |
| `raft.StartNode`            | `go.etcd.io/raft` (far)   | `s3StartNode` (in `reflect`)   |
| `raft.RestartNode`          | `go.etcd.io/raft` (far)   | `s3RestartNode` (in `reflect`) |
| `serverstorage.OpenBackend` | `server/v3/storage` (far) | `v3.S3OpenBackend`             |

`s3StartNode`/`s3RestartNode` are local to `reflect`; both funnel through `s3Node`, which calls
`v3.Start(...)` with `v3.ActiveNS` (the namespace `S3OpenBackend` resolved). Files in `v3/reflect/`:

- `init.go` — patch install + the `s3*` replacements (was `hijack.go`).
- `init_darwin.go` / `init_darwin_{amd64,arm64}.s` — darwin patcher: `mach_vm_protect` with
  `VM_PROT_COPY` (COW-break for W^X on Apple Silicon), `sys_icache_invalidate`, asm trampolines.
- `init_other.go` — linux/other via `mprotect`.

Arch support: amd64 + arm64. Darwin + linux.

### ⚠️ Do NOT try to hijack `bootstrappedRaft.newRaftNode` again

This was attempted and **reverted** on 2026-07-10. It crashes (SIGBUS in `flushICache`), and the
crash is **build-layout-dependent** — smoke passes, e2e SIGBUSes, same source. Root cause:

- `newRaftNode` lives in `etcdserver`; the patcher (now `v3/reflect`) is still linked into the same
  binary at an uncontrolled offset.
- `newRaftNode`'s 16 KB text page can contain the patcher's own mach helpers (`flushICache`/
  `setExecutable`/trampolines), depending on link layout, which **varies between builds**. (Moving
  the patcher into its own package does NOT fix this — same binary, same uncontrolled layout.)
- `writeText`→`setWritable` drops execute on that page → de-executes the running patcher
  mid-patch → SIGBUS on the next helper call.

The three shipped targets are all in **far modules** (pages away from the patcher), so they never
collide — that is _the whole reason_ the design patches exported far-module entry points instead
of the convenient unexported `newRaftNode`.

If capturing the etcd **cluster ID** (the ideal S3 namespace key — see `LIMITATIONS.md`) is still
wanted, do NOT reintroduce a write-in-place patch of an in-`etcdserver` symbol. Two safe paths:

1. Robust patcher: write patches through a separate RW alias (`mach_vm_remap`) so the executing
   mapping never loses execute. Fixes the whole same-page hazard class. Real work; test both arches.
2. Derive the cluster ID inside the already-safe `s3OpenBackend` seam from `ServerConfig` / WAL
   metadata — no adjacent-text patch, no new hazard. **Recommended.**

## Dependencies — the blocker to `go build`

`libraft/go.mod` currently requires only `go.etcd.io/raft/v3`. Both new packages additionally import
etcd-server-internal packages, so **neither compiles in libraft until go.mod grows these**:

- `v3` (core): `server/v3/config`, `server/v3/storage`, `server/v3/storage/backend`,
  `client/pkg/v3/logutil`, `raft/v3` (+ `/raftpb`, `/tracker`), `go.uber.org/zap`,
  `google.golang.org/protobuf/proto`
- `v3/reflect` (installer): `server/v3/storage`, `raft/v3`, `go.uber.org/zap`,
  `github.com/cnuss/libraft/v3`, `unsafe`, `syscall`

Pulling `go.etcd.io/etcd/server/v3` drags a heavy tree (bbolt, grpc, etc.) and pins etcd's Go
toolchain (`go 1.26` / `toolchain go1.26.4`). Expect version-alignment churn between libraft's
`go.mod` and etcd's. Do the go.mod surgery deliberately; run `go mod tidy` and reconcile.

## File map

`v3/` (package `v3`, core):

| File                         | Role                                                                                              |
| ---------------------------- | ------------------------------------------------------------------------------------------------- |
| `client.go`                  | S3 client: CAS append, list, get/put/del, `purgeNamespace`, etag-chain vs conditional-write modes |
| `node.go`                    | raft.Node impl over the S3 log; `Start`; seed/publish/reconcile; force-new republish + heal; `EnvURL` |
| `checkpoint.go`              | `S3OpenBackend`, `ActiveNS`, bbolt checkpoint/restore via bucket; `forceNewClusterPurge`; guards  |
| `batch.go`                   | batched appends                                                                                    |
| `notify.go`, `notify_sqs.go` | change-notification (poll / SQS)                                                                   |
| `metrics.go`                 | prometheus metrics                                                                                 |
| `logger.go`                  | exported `Logger()` (named "s3raft")                                                               |
| `*_test.go`                  | `cas_linearizability_test.go`, `notify_sqs_test.go`                                                |
| `tla/`                       | TLA+ spec of the CAS log ordering                                                                  |
| `LIMITATIONS.md`             | behavioral limits + force-new-cluster semantics                                                    |

`v3/reflect/` (package `reflect`, installer):

| File                                 | Role                                                    |
| ------------------------------------ | ------------------------------------------------------- |
| `init.go`                            | patch install + `s3StartNode`/`s3RestartNode`/`s3Node`  |
| `init_darwin.go`, `init_darwin_*.s`  | darwin patcher (mach_vm_protect + icache + trampolines) |
| `init_other.go`                      | linux/other patcher (mprotect)                          |

## Force-new-cluster (already implemented, on real S3)

`--force-new-cluster` under s3raft = **purge the cluster's namespace + re-genesis from local WAL**
(not etcd's in-place surgery). Flow: `s3OpenBackend` → `forceNewClusterPurge` (synchronous
`purgeNamespace`) → one-shot marker `s3raft-force-new-done` → node republishes local WAL entries
verbatim (`publishLocalEntries`, with `seedHead`+`uploadSnapshot` when a snapshot base exists) →
reconcile heal-tail on subsequent boots (`forceNewBoot` flag guards the fatal-on-mismatch path).
All `TestForceNewCluster_*` pass on real S3. See `LIMITATIONS.md` for the full writeup.

## Build / test

```sh
# unit (needs the go.mod deps wired first)
go test ./v3/ -run TestCAS -count=1

# install seam: host blank-imports the installer, not the core:
#   import _ "github.com/cnuss/libraft/v3/reflect"
# e2e lives in the etcd tree today: builds bin/etcd (with that blank import) and spawns it
# against a REAL S3 bucket. Env: ETCD_S3LOG_URL=s3://<bucket>/<prefix>, AWS creds.
# bucket names MUST be lowercase (S3 400s otherwise).
```

Verifying a patch actually lands (inlining / same-page traps):

```sh
go tool nm bin/etcd   | grep <symbol>     # link addresses
go tool objdump bin/etcd -s <symbol>      # confirm not inlined away
```

## What's left (reconciled 2026-07-10 vs the 2026-07-09 screenshots)

The two `Screenshot 2026-07-09 *.png` in the etcd repo root captured an earlier gap list; most of
that "productionize" list has since landed (fencing epochs, lessor fencing, snapshot→bucket + log
truncation, batching, SQS notification, 429/503 backoff, force-new-cluster). What remains, reconciled
against current code:

1. **Sealed writes / integrity (#6)** — NOT done. `sha256` in `client.go` is only sigv4 signing.
   No per-entry checksum, and no real **cluster-ID stamp / bucket-mismatch guard**. The namespace
   key today is a proxy: `root|InitialClusterToken|minPeerURL` (`checkpoint.go:284`), not the etcd
   cluster ID — so two different clusters aimed at the same bucket/prefix are not hard-fenced.
   Ties to the reverted `newRaftNode` cluster-ID capture; **derive the cluster ID in the safe
   `S3OpenBackend` seam** (see the ⚠️ section) and stamp it into `meta/`.
2. **Multipart (rest of #7)** — NOT done (`grep multipart` empty). Snapshots and entries >5 MB use a
   single PUT; real S3 needs multipart. Also: request-pricing telemetry, IAM-scoped creds, TLS review.
3. **Config surface (#8)** — NOT done. Still the `ETCD_S3LOG_URL` env var; wants a proper
   `--experimental-s3-log-url` flag + feature-gate per etcd conventions, and e2e moved into a
   `tests/` harness (today it's ad-hoc + needs a real bucket).
4. **Concurrency correctness pass (#9)** — NOT done. Jepsen-style: `kill -9` loops, partition S3
   access, clock skew; validate no lost/duplicated acks. CAS core should survive; leases are the risk.
5. **ETag-chain heal gap** — reconcile heal-tail was added for force-new; confirm the _general_
   case (writer crash between HEAD CAS and `log/N` write) backfills for a reader-only node without a
   stall. (`node.go` reconcile path)
6. **Membership semantics** — add/remove-member still untested; rejoin-after-wipe needs
   `--initial-cluster-state existing` (a lone node with `existing` tries to fetch cluster info and dies).
7. **Cosmetic lints** (low priority, carried from the screenshots) — `proto.Uint64` → `new()` (many
   sites in `node.go`), `sort.Slice` → `slices.Sort`, and `go vet` flags the `unsafe.Pointer` patch
   address in `hijack.go`. These are expected in the patcher and can be `//nolint`-annotated.

**Framing (unchanged):** the landed work makes s3raft credible for low-write control planes (config,
service discovery, CI locks). Nothing makes it an etcd replacement for kube-apiserver-class write
loads — the S3 RTT floor is physics. Realistic upstream path is a separate "etcd-on-object-storage"
mode with documented semantics diff, not a silent drop-in.

## Open decisions for the next session

1. **go.mod**: add the etcd-server deps, `go mod tidy`, reconcile toolchain. Blocker for compile.
   First make both packages build (the core↔installer split is fresh — check `v3.Start` /
   `v3.S3OpenBackend` / `v3.ActiveNS` / `v3.EnvURL` / `v3.Logger` are all exported and referenced
   correctly from `v3/reflect`).
2. **`ActiveNS` as cross-package global**: `S3OpenBackend` sets `v3.ActiveNS`, `s3Node` (in
   `reflect`) reads it — a package-level global mutated across the patch boundary. Works because
   OpenBackend always fires before StartNode, single-threaded at boot; worth revisiting if the seam
   grows (pass it explicitly, or stash on a context).
3. **libraft integration**: `reflect` is the raw hijack installer. Whether/how the core plugs into
   the `v1`/`v1alpha1` Fluent Builder (e.g. a `WithNodeProvider`-style seam) was discussed but
   reverted; revisit if a non-hijack install path is wanted.
4. **Cluster-ID namespace**: see the ⚠️ section — derive from `S3OpenBackend`, not a newRaftNode patch.
