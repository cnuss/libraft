# Contributing

This document is for everyone working on `libraft` — humans and AI agents alike.
It covers the layout, the local dev loop, the conventions that bite, and how a
change gets from an issue to a release.

## Where to find things

Deep-link by filename; line numbers will drift.

| Topic                                          | Source                                                           |
| ---------------------------------------------- | ---------------------------------------------------------------- |
| Port state + monkey-patch mechanism            | [`v3/DEVNOTES.md`](./v3/DEVNOTES.md)                             |
| Behavioral limits + force-new-cluster          | [`v3/LIMITATIONS.md`](./v3/LIMITATIONS.md)                       |
| Installer (`init`, `s3StartNode`, `patchFunc`) | [`v3/reflect/init.go`](./v3/reflect/init.go)                     |
| Platform patch primitives                      | [`v3/reflect/init_darwin.go`](./v3/reflect/init_darwin.go), [`init_windows.go`](./v3/reflect/init_windows.go), [`init_other.go`](./v3/reflect/init_other.go) |
| S3 CAS client                                  | [`v3/client.go`](./v3/client.go)                                 |
| raft.Node over the S3 log + `Start`            | [`v3/node.go`](./v3/node.go)                                     |
| `S3OpenBackend`, checkpoint/restore, guards    | [`v3/checkpoint.go`](./v3/checkpoint.go)                         |
| Core unit tests                                | [`v3/cas_linearizability_test.go`](./v3/cas_linearizability_test.go) |
| e2e harness + runner                           | [`e2e/e2e_test.go`](./e2e/e2e_test.go)                           |
| Worked examples                                | [`examples/`](./examples)                                        |
| Build / lint / test commands                   | [`Makefile`](./Makefile)                                         |
| Release + skip release regex                   | [`.github/workflows/ci.yml`](./.github/workflows/ci.yml)         |
| CodeQL scan                                    | [`.github/workflows/codeql.yml`](./.github/workflows/codeql.yml) |
| OpenSSF Scorecard scan                         | [`.github/workflows/scorecard.yml`](./.github/workflows/scorecard.yml) |
| Dependabot config                              | [`.github/dependabot.yml`](./.github/dependabot.yml)             |
| Cosign verification recipe                     | [`SECURITY.md`](./SECURITY.md)                                   |
| Orientation for AI agents                      | [`CLAUDE.md`](./CLAUDE.md)                                       |

## Module layout

Two packages:

```
github.com/cnuss/libraft/v3          — s3raft core: S3 CAS log, raft.Node over
                                     the log, bbolt checkpoint/restore, notify,
                                     batch, metrics.
github.com/cnuss/libraft/v3/reflect  — the installer: blank-import to monkey-patch
                                     etcd's raft entry points into the core.
```

A host blank-imports `v3/reflect`; its `init` (when `ETCD_S3LOG_URL` is set)
rewrites the prologue of `raft.StartNode`, `raft.RestartNode` and
`serverstorage.OpenBackend` to jump into the core. The core exports the seam
the installer calls: `Start`, `S3OpenBackend`, `ActiveNS`, `EnvURL`, `Logger`.
The monkey-patch is intentional and load-bearing — see
[`v3/DEVNOTES.md`](./v3/DEVNOTES.md); never replace it with a source edit.

## Local development

Requires Go 1.26 or later (pinned by `go.etcd.io/etcd/server/v3`).

```sh
git clone https://github.com/cnuss/libraft.git
cd libraft
make test   # library unit + fuzz tests (fast, in-package)
make e2e    # builds and runs every example binary
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
  is a unit test, not e2e.

## Before you push

- `gofmt -w .`
- `go vet ./...`
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
  mechanism is load-bearing — see [`v3/DEVNOTES.md`](./v3/DEVNOTES.md)).
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
