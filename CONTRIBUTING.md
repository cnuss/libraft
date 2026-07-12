# Contributing

This document is for everyone working on `libraft` — humans and AI agents alike.
It covers the layout, the local dev loop, the conventions that bite, and how a
change gets from an issue to a release.

## Where to find things

Deep-link by filename; line numbers will drift.

| Topic                                          | Source                                                           |
| ---------------------------------------------- | ---------------------------------------------------------------- |
| Monkey-patch mechanism (overview)              | [README → The install seam](./README.md#the-install-seam)        |
| Behavioral limits + force-new-cluster          | [`v3/LIMITATIONS.md`](./v3/LIMITATIONS.md)                       |
| TLA+ model of the CAS log ordering             | [`v3/tla/`](./v3/tla)                                            |
| Installer (`init`, `patchFunc`, linkname target) | [`v3/reflect/init.go`](./v3/reflect/init.go), [`v3/reflect/newraftnode.go`](./v3/reflect/newraftnode.go) |
| `NewRaftNode` replacement + layout mirrors     | [`v3/newraftnode.go`](./v3/newraftnode.go)                       |
| Layout guard (mirror vs imported etcd)         | [`v3/reflect/init_test.go`](./v3/reflect/init_test.go)           |
| Platform patch primitives                      | [`v3/reflect/init_darwin.go`](./v3/reflect/init_darwin.go), [`init_windows.go`](./v3/reflect/init_windows.go), [`init_other.go`](./v3/reflect/init_other.go) |
| S3 CAS client                                  | [`v3/client.go`](./v3/client.go)                                 |
| raft.Node over the S3 log + `Start`            | [`v3/node.go`](./v3/node.go)                                     |
| `S3OpenBackend`, checkpoint/restore, guards    | [`v3/checkpoint.go`](./v3/checkpoint.go)                         |
| Core unit tests                                | [`v3/cas_linearizability_test.go`](./v3/cas_linearizability_test.go) |
| e2e harness + runner                           | [`e2e/e2e_test.go`](./e2e/e2e_test.go)                           |
| e2e MinIO/S3 store harness                     | [`e2e/s3raft_test.go`](./e2e/s3raft_test.go)                     |
| s3raft-enabled etcd binary (drives etcd's e2e) | [`e2e/main.go`](./e2e/main.go)                                   |
| etcd-e2e CI job (baseline vs reflect)          | [`.github/workflows/ci.yml`](./.github/workflows/ci.yml)         |
| Worked examples                                | [`examples/`](./examples)                                        |
| Build / lint / test commands                   | [`Makefile`](./Makefile)                                         |
| Release + skip release regex                   | [`.github/workflows/ci.yml`](./.github/workflows/ci.yml)         |
| CodeQL scan                                    | [`.github/workflows/codeql.yml`](./.github/workflows/codeql.yml) |
| OpenSSF Scorecard scan                         | [`.github/workflows/scorecard.yml`](./.github/workflows/scorecard.yml) |
| Dependabot config                              | [`.github/dependabot.yml`](./.github/dependabot.yml)             |
| Cosign verification recipe                     | [`SECURITY.md`](./SECURITY.md)                                   |
| Orientation for AI agents                      | [`CLAUDE.md`](./CLAUDE.md)                                       |

## Module layout

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

[`e2e/`](./e2e) is a **separate Go module** (`github.com/cnuss/libraft/e2e`,
`replace`d onto the checkout): it carries the deps only the harness needs —
the docker SDK for the MinIO container and etcd's `etcdmain` tree for the
`etcd-s3raft` binary — so they never enter the library's dependency graph.
`./...` from the root skips it; the Makefile's `-C e2e` legs cover it.

A host blank-imports the root package (which pulls in `v3/reflect`); the
installer's `init` (when `ETCD_S3LOG_URL` is set) rewrites the prologue of
`(*bootstrappedRaft).newRaftNode` and `serverstorage.OpenBackend` to jump into
the core. The core exports the seam the installer calls: `Start`,
`NewRaftNode`, `S3OpenBackend`, `ActiveNS`, `EnvURL`, `Logger`. The
reconstruction of etcd's unexported `raftNode` lives in the core
(`v3/newraftnode.go`), since it is node-construction logic; `v3/reflect` only
supplies the patch-target address (via `//go:linkname`) and does the overwrite.
The monkey-patch is intentional and load-bearing — never replace it with a
source edit (see the patch-target rules under Conventions that bite).

## Provenance

The `v3` core was ported out of an etcd fork — `server/etcdserver/s3raft/*` at
commit `0ab3790793d54f7709d9c9b62175d200aadb483e` (2026-07-09) — and split into
the core/installer layout here on 2026-07-10. It was a **copy, not a move**:
that etcd tree still carries its own flat `s3raft` package (installer files
inline) plus an e2e harness that builds `bin/etcd` with the blank import and
drives it against a real bucket. If libraft becomes the source of truth,
delete the etcd copy and have that tree blank-import
`github.com/cnuss/libraft/v3/reflect`. The bucket-backed pieces already live
here: the `e2e/` module carries the s3raft-enabled etcd binary (`e2e/main.go`,
driven by the etcd-e2e CI job against etcd's own suite) and a MinIO-backed
harness (`e2e/s3raft_test.go`).

## Local development

Requires Go 1.26 or later (pinned by `go.etcd.io/etcd/server/v3`, which also
drags a heavy dependency tree — bbolt, grpc, otel. Expect version-alignment
churn between this `go.mod` and etcd's on bumps; run `go mod tidy` and
reconcile deliberately).

```sh
git clone https://github.com/cnuss/libraft.git
cd libraft
make test   # library unit + fuzz tests (fast, in-package)
make e2e    # builds and runs every example binary; without AWS_REGION it
            # starts a throwaway MinIO container (docker) for the s3raft legs
```

Run a specific example locally:

```sh
make run basic
```

## Test layout

Three tiers, each with a distinct job — don't blur them:

- **`*_test.go` next to the code** — unit tests: anything with fabricated
  inputs or fakes, however elaborate (e.g. the CAS linearizability and SQS
  notify tests in [`v3/`](./v3)).
- **`examples/`** — real-world, simple-ish API usage written for humans. An
  example demonstrates; it never asserts. Assertion logic belongs in `e2e/`.
- **`e2e/`** — the harness builds and runs the example binaries and asserts
  on their output. If a check can pass without running an example binary, it
  is a unit test, not e2e. Its own module (see Module layout); the s3raft legs
  run against a real store — ambient AWS env when `AWS_REGION` is set, a
  docker-launched MinIO otherwise (skipped where no linux-container daemon is
  reachable). The same module also builds `etcd-s3raft` (`e2e/main.go`), the
  binary the etcd-e2e CI job substitutes into etcd's own e2e suite.

## Before you push

- `gofmt -w .`
- `make vet` (covers the root and `e2e/` modules)
- `make test`
- `make e2e`

CI runs the same on every PR.

## Conventions that bite

Easy to get wrong from the diff alone:

- **`examples/` is intentionally duplicated.** Each `main.go` is a
  copy-pasteable starter; no shared internal package. Don't refactor it into
  one.
- **`go vet` runs with `-unsafeptr=false`.** The installer converts a
  text-segment code address (`uintptr`) to `unsafe.Pointer` to overwrite a
  function prologue — a legitimate monkey-patch that `vet`'s `unsafeptr`
  analyzer flags as misuse. The Makefile's `vet` and `windows` targets disable
  that one analyzer; keep it off, don't rewrite the patcher to appease it (the
  mechanism is load-bearing).
- **The patch target is `(*bootstrappedRaft).newRaftNode`, an unexported
  `etcdserver` symbol — and that co-location is a live hazard.** The patcher
  makes the target's text page writable mid-install; if the linker places any
  of the patcher's own helpers (`flushICache`/`setExecutable`/trampolines) on
  that same 16 KB page, the running patcher de-executes itself → SIGBUS. This
  exact crash was hit and reverted once (2026-07-10). It works now only because
  the patcher lives in its own `v3/reflect` package that the linker places
  pages away from `etcdserver` — in the current binaries `newRaftNode` and
  `reflect.writeText` sit ~1.3 MB apart. This is **not guaranteed** for every
  host binary; a different link layout could re-collide. So:
  - **Verify the gap holds** whenever you touch the installer, its target, or
    bump etcd — inlining and same-page traps are silent:
    ```sh
    go tool nm bin/etcd | grep -E 'newRaftNode|reflect\.writeText'  # must be pages apart
    go tool objdump bin/etcd -s newRaftNode                         # confirm not inlined
    ```
  - **The real fix, if this ever collides**, is a robust patcher that writes
    through a separate RW alias (`mach_vm_remap`) so the executing mapping never
    loses execute — eliminating the whole same-page class. Don't paper over a
    collision by relocating code and hoping.
- **The layout mirrors in `v3/newraftnode.go` reconstruct etcd's unexported
  `raftNode` / `raftNodeConfig` / `toApply` / `bootstrappedRaft` by
  field-for-field memory layout**; a plain field reorder or addition upstream
  silently corrupts state. `v3/reflect/init_test.go` guards this: it reflects
  etcd's *actually imported* `raftNode` (via the `EtcdServer.r` field) and
  asserts the mirror matches offset-for-offset, so an etcd bump that shifts the
  layout fails `make test` instead of corrupting at runtime. `bootstrappedRaft`
  has no exported reflect anchor, so its size + read-field offsets are pinned by
  hand in the same test — update those against `etcdserver/bootstrap.go` when
  the pin trips, then re-verify end-to-end.
- **`//go:linkname` reaches the target's address across the module boundary.**
  It needs `import _ "unsafe"` and a blank import of `etcdserver` so the symbol
  is linked (see `v3/reflect/newraftnode.go`). The linkname'd handle is never
  called — only its `.Pointer()` is taken as the patch source.
- **The patcher is platform- and arch-specific.** `//go:build` tags partition
  `init_darwin.go` (mach_vm_protect) / `init_windows.go` (VirtualProtect) /
  `init_other.go` (mprotect); amd64 + arm64. Adding a platform means adding a
  primitives file, not editing the shared `init.go`.
- **e2e builds binaries at runtime**, so the test cache can't see example
  source changes — `make e2e` passes `-count=1` to force a rebuild.
- **Skip-release token must be line-anchored.** The regex in
  [`ci.yml`](./.github/workflows/ci.yml) (`resolve tag` step) is
  `^[[:space:]]*\[skip release\][[:space:]]*$`. Inline prose mentions are safe;
  a standalone line in the commit body opts out.
- **Cosign / Scorecard tags are annotated.** `ossf/scorecard-action` publishes
  annotated tags; pinning the tag-object SHA fails Sigstore verification
  ("imposter commit"). Pin to the commit underneath (see existing entries in
  [`scorecard.yml`](./.github/workflows/scorecard.yml)).

## Adding an example

Examples live in `./examples/<name>/main.go`. Keep each example self-contained
(there's no shared internal package — the duplication is intentional, so each
example is copy-pasteable on its own).

Print a single recognizable line so the e2e harness can assert on it, then add
a row to the `cases` table in `e2e/e2e_test.go` (name + expected substring) and
to the README's example table.

## Roadmap & known gaps

Reconciled 2026-07-10. The landed work (fencing epochs, lessor fencing,
snapshot→bucket + log truncation, batching, SQS notification, 429/503 backoff,
force-new-cluster) makes s3raft credible for low-write control planes (see
[README → When to use it](./README.md#when-to-use-it)); these gaps are what's
left:

1. **Sealed writes / integrity** — no per-entry checksum, and no cluster-ID
   stamp / bucket-mismatch guard. The namespace key is still a proxy —
   `root|InitialClusterToken|minPeerURL` (`nsFromConfig`, checkpoint.go) — not
   the etcd cluster ID, so two different clusters aimed at the same
   bucket/prefix are not hard-fenced. The `newRaftNode` seam now *reaches* the
   real cluster ID (`cl.ID()` in `v3.NewRaftNode`, currently only logged); wire
   it into the namespace and stamp it into `meta/`. Note the ordering: the
   namespace is resolved in `S3OpenBackend`, which runs *before* `newRaftNode`,
   so either move ID derivation into `S3OpenBackend` (compute it from
   `ServerConfig`) or defer namespace finalization until the ID is in hand.
2. **Multipart uploads** — snapshots and entries >5 MB use a single PUT; real
   S3 needs multipart. Related: request-pricing telemetry, IAM-scoped creds,
   TLS review.
3. **Config surface** — `ETCD_S3LOG_URL` env var only; wants a proper
   `--experimental-s3-log-url` flag + feature gate per etcd conventions. (The
   bucket-backed e2e harness has landed in the `e2e/` module — MinIO via the
   docker SDK plus the `etcd-s3raft` binary for etcd's own suite.)
4. **Concurrency correctness pass** — Jepsen-style: `kill -9` loops,
   partitioned S3 access, clock skew; validate no lost/duplicated acks. The
   CAS core should survive; leases are the risk.
5. **ETag-chain heal, general case** — reconcile heal-tail exists for
   force-new; confirm a writer crash between the HEAD CAS and the `log/N`
   write backfills for a reader-only node without a stall (node.go reconcile
   path).
6. **Membership semantics** — add/remove-member untested; rejoin-after-wipe
   needs `--initial-cluster-state existing` (a lone node with `existing` tries
   to fetch cluster info and dies). Behavioral edges in
   [`v3/LIMITATIONS.md`](./v3/LIMITATIONS.md).
7. **`ActiveNS` cross-package global** — `S3OpenBackend` sets it, `NewRaftNode`
   reads it. Safe today (OpenBackend always runs before newRaftNode,
   single-threaded at boot); make it an explicit parameter if the seam grows.
8. **Housekeeping** — `node.go` (~1300 lines) wants splitting (run loop /
   reconcile / force-new); extract sigv4 signing from `client.go`;
   `checkpoint.go` mixes backend-open, namespace derivation, checkpoint and
   guards; `package reflect` shadows the stdlib package it imports (a rename
   breaks the import path — decide before tagging v3); the boot logger is
   double-named `s3raft.s3raft`; `proto.Uint64` → `new()`, `sort.Slice` →
   `slices.Sort`; scope the vet `-unsafeptr` exception to `v3/reflect` instead
   of tree-wide.

## Branch / PR flow

**Every change starts with an issue** — no exceptions, including retroactive
cleanups. The PR body always carries a `Closes #<n>` line so the merge
auto-closes the tracking issue and leaves a paper trail.

```sh
gh issue create --title "…" --body "…"                    # 1. issue first
git switch -c <type>/<topic>                              # 2. branch
# ... edits, commit ...
git push -u origin <type>/<topic>
gh pr create --title "<type>: …" --body "Closes #<n>. …"  # 3. PR refs the issue
# CI green ⇒
gh pr merge <pr#> --squash --delete-branch
```

`main` is protected (`ci` required; no force-push). Don't push directly to it
for routine work — PR flow gives CI + auto-release a clean audit trail. Pushing
to `main` auto-bumps a patch tag and signs the release (see Releasing below).

Don't commit secrets. [`.gitignore`](./.gitignore) covers `.env*`, `.claude/`,
etc.

## Pull requests

- Keep PRs focused. One feature or fix per PR.
- Include test coverage for behavior changes — core tests (`v3/`) for the S3
  log / node behavior, e2e tests (`e2e/e2e_test.go`) for example-visible changes.
- **Keep the README in sync with the seam.** The README mirrors the public
  surface, so any change to it must update the README in the same PR:
  - a new/changed/removed exported core symbol or patched target → update the
    **The install seam** section and the **Quick Start** snippet;
  - a new example → add a row to the **Example** table;
  - a renamed package → update the **Layout** tree.
  Treat the README's code blocks as documentation that must compile against the
  current API — stale snippets are a review blocker.
- Signed commits preferred. The repo enables commit signing locally; CI does
  not enforce signatures.

## Commit messages

Short subject (≤ 72 chars), imperative mood ("Add X", not "Added X").
Wrap body at ~72 cols. Explain the *why*; the diff covers the *what*.

## Releasing

Patch releases are automatic. Every push to `main` runs the `Release`
workflow, which bumps the patch component of the latest `v*` tag,
re-runs `go vet`, `go build`, `make test`, and `make e2e` against that
ref, then:

- pushes the new tag,
- creates a GitHub Release with auto-generated notes, and
- warms `proxy.golang.org` so [pkg.go.dev](https://pkg.go.dev/github.com/cnuss/libraft)
  surfaces the new version without manual prodding.

To opt a commit out of the auto-bump, put `[skip release]` on its own
line in the commit body. (It must be the only thing on its line, so
prose mentioning the token inline doesn't accidentally suppress.)

For a minor or major bump, tag locally and push the tag — the workflow
treats a manual tag as the version of record and skips the bump:

```sh
git tag v0.2.0
git push --tags
```

Tags must follow `vMAJOR.MINOR.PATCH` (Go module semver).

## License

By contributing you agree your contributions are licensed under the
[MIT License](./LICENSE).
