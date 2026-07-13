# libraft known limitations & safety notes

libraft replaces raft with a shared S3 CAS log: every node runs its own etcd
apply loop over the same totally-ordered log, so anything computed at apply time
(revision, compactRevision, consistentIndex, auth revision, alarm state) is a
deterministic function of the log and stays identical across nodes. The items
below are where that model — one fencing-epoch owner leads, every node applies the
same S3-CAS-ordered log — needs care or falls short.

## Guarded by startup checks

libraft validates these at boot (`guardConfig`, checkpoint.go), so correctness no
longer depends on operator discipline:

- **Periodic corruption check is INCOMPATIBLE — libraft refuses to start with it on.**
  `corrupt.go`'s `PeriodicCheck` asserts the leader's revision/compactRevision is
  `>=` every follower's — a hard single-leader raft invariant. Under libraft the epoch
  owner is routinely *behind* a peer that just wrote and applied locally, so the
  assertion fails and raises a false `CORRUPT` alarm (member, or whole-cluster, write
  outage). It is off by default (`corrupt-check-time-interval: 0s`); if
  `--corrupt-check-time` > 0, libraft fails fast with a clear error rather than boot into
  that time bomb. The cross-member KV *hash* comparison itself is fine (identical apply
  order ⇒ identical hash at identical revision); only the revision-ordering assertion
  breaks.

- **Lease TTLs must exceed the fencing-detection window** (`fenceCheckInterval`, 1s).
  Lease renewals apply to the local lessor and are not replicated through the log, so a
  keepalive delivered to a node during its pre-demotion window extends the lease only
  there while the true epoch owner may expire it. etcd already clamps every grant up to
  `MinLeaseTTL = ceil((3*ElectionTicks/2) · TickMs)` (≈2s at defaults, clearing the 1s
  window with margin); libraft **warns** at startup if that floor does not exceed the
  window (e.g. under an unusually short election timeout). Direct keepalives at the
  current epoch owner.

## Efficiency (correct, but wasteful) — not yet addressed

- **A hung S3 call on the propose path blocks the single-threaded run loop** for
  the S3 retry budget (`s3RetryBudget`, ~12s; per-attempt deadline 5s):
  `appendBatch` writes the log CAS synchronously on the loop. Reads no longer do
  — the epoch check, linearizable read, and tail sync now run their S3 GETs in
  goroutines and deliver results back for the loop to fold (only state mutation
  stays on-loop), so a slow store no longer stalls proposals behind a read.
  Moving the write CAS off the loop the same way, and a caller-supplied context
  to cut the retry budget shorter, are further refinements.

## Membership & restore

Each cluster's objects live under a bucket namespace
(`<bucket>/<prefix>/c-<cluster-id>-<root-hash>/…`, `nsFromConfig` / `rebindNamespace`,
checkpoint.go) — cached on first boot in `<member>/libraft-ns`. Two orthogonal inputs
key it:

- The **real etcd cluster ID** — globally unique per cluster and frozen at genesis
  (etcd hashes the founding member set once, folding in `--initial-cluster-token`, and
  never regenerates it on member add). It is identical across members and stable across
  membership changes and disk-wiped restarts, and it *subsumes* the old token +
  lowest-peer-URL proxy.
- The **parent of the member's data directory** (the deployment root — `/var/lib/etcd`
  per host, or one temp root per test). The cluster ID is *not* unique across clusters
  that reuse the same member URLs and token: the e2e suite runs dozens of single-node
  clusters all on `localhost:2380` + token `new`, which hash to one cluster ID. The
  data-dir root discriminates them so they don't share one log. Identical across a
  cluster's members (they share a deployment root), so it doesn't perturb agreement.

The cluster ID reaches the namespace by two paths. In `S3OpenBackend` — which runs
before the raft node exists — it is reconstructed from `ServerConfig` by reusing etcd's
own `membership.NewClusterFromURLsMap` over `--initial-cluster-token` +
`--initial-cluster` (the identical computation etcd's bootstrap performs). This is
authoritative for the founding members and drives the disk-wiped-restore probe. In
`newRaftNode` — the single seam that carries the authoritative `cl.ID()` — the namespace
is **rebound** to it and re-cached: a no-op for founding members, and the correction for
a member that *joined* later (whose `--initial-cluster` describes the grown set, so the
config reconstruction would hash the wrong set). Because a fresh joiner's restore probe
is a no-op, the transient config-derived value it uses before the rebind is harmless.

- **Clusters that share one bucket are isolated by cluster ID *and* data-dir root.**
  Two clusters collide only if they share both (same founding member set, token, and
  deployment root) *and* the same bucket/prefix — give one a distinct token,
  initial-cluster, data-dir root, or bucket. A single cluster per bucket/prefix needs no
  coordination.

- **A joining member finds the log via the frozen cluster ID**, which it learns from
  the running cluster and has rebound in `newRaftNode` — independent of `--initial-cluster`
  ordering or which members remain. This closes the old proxy-hash gap where removing
  the lowest-peer-URL member and then adding a brand-new one shifted the derived key.

- **`ETCD_S3LOG_NS` pins the namespace explicitly**, overriding both the on-disk cache
  and the cluster-ID derivation (and suppressing the rebind). Use it to point a new
  process at an existing cluster's log — e.g. disk-wiped recovery or migration. Set it
  uniformly across every member; a mismatch splits them onto separate logs.

- **Leadership is pinned to the founder; it does not follow membership changes.**
  The genesis member claims the fencing epoch and stays leader; a member *joining* an
  existing cluster (voter or learner) boots as a follower of the current epoch owner
  rather than seizing leadership — mirroring raft, where adding a member does not
  trigger a leader change. A **learner never claims the epoch** (it cannot serve
  client RPCs, so leading it would wedge the cluster). Leadership advances only when a
  *voting* member restarts or recovers from a disk-wiped backend, which re-claims the
  epoch. Consequence: if the owner crashes and does not restart, leadership does **not**
  fail over on its own — restart a surviving voter to move the epoch. There is no
  liveness/lease on the epoch object yet; "genesis, or the last voter to (re)boot,
  leads" is the whole election.

- **A member add adds ~`pollInterval`+0.5s of latency** (`memberChangePropagationDelay`,
  node.go). The add is committed the instant its log object is CAS-written, but peers
  learn of it only by *pulling* the log (libraft sends no raft traffic), so the serving
  node holds the ConfChange for one poll interval before `MemberAdd` returns — giving
  every peer time to apply it first. Without this a member started immediately after the
  add can query a peer that has not caught up and fail bootstrap validation ("member
  count is unequal" / "could not retrieve cluster information"). Removes and promotes
  are not delayed.

- **V3 discovery bootstrap is unsupported.** Discovery builds the peer set
  dynamically; use a static `--initial-cluster`.

- **Restoring a local snapshot into a non-empty, behind log is rejected.** The
  shared log is authoritative; `etcdutl snapshot restore` then start only works when
  the bucket log is empty (the node bootstraps it from the restored state). To
  recover an existing cluster, point a new cluster at the existing bucket namespace
  instead of restoring a local snapshot over it.

- **`--force-new-cluster` wipes this cluster's shared log and re-genesises from the
  local backend.** etcd's force-new rewrites the *local* WAL to a single-member
  config; under libraft the authority is the shared log, so the equivalent recovery is
  destructive: `forceNewClusterPurge` (checkpoint.go) synchronously deletes every
  object under this cluster's namespace (scoped strictly to the namespace prefix —
  never the whole bucket, so clusters sharing a bucket are untouched), then start()
  republishes the local committed WAL entries (`publishLocalEntries`) as the new
  genesis history and this node claims the epoch. If etcd had taken a raft snapshot,
  the republish starts at the snapshot boundary and also publishes a bucket snapshot at
  the current index so a future member can restore the compacted-away prefix. Use it
  **only when quorum is permanently lost** — libraft cannot verify the other members are
  dead, and any surviving member still pointed at this namespace will break (split
  brain). A one-shot marker (`libraft-force-new-done` in the member dir) stops a flag
  left in a boot unit from re-wiping the log on every restart; because etcd's force-new
  surgery still appends a fresh conf-change on each such restart, start() then heals the
  one-entry-ahead local WAL by publishing its tail rather than failing. To force a fresh
  recovery again, delete the marker.

## Unsupported

- **Explicit leader transfer** (`MoveLeader`) is a no-op — leadership is pinned to the
  epoch owner (see "Leadership is pinned to the founder" above), not movable by this
  call. It reports success immediately without moving anything (which also lets
  graceful-shutdown transfer complete instead of blocking).

- **The `raft.status` expvar is absent.** etcd's own `newRaftNode` also stores the node
  in the package-level `etcdserver.raftStatus` indirection that backs the `/debug/vars`
  `raft.status` entry. That symbol is unexported and unreachable from the `v3.NewRaftNode`
  reconstruction, so the expvar is missing under libraft. Debug-only; no functional impact.

## Configuration (security & notifier)

Credentials resolve from `ETCD_S3LOG_{ACCESS,SECRET}_KEY`, then
`AWS_{ACCESS_KEY_ID,SECRET_ACCESS_KEY}`, then the minioadmin default.

- **Credential rotation is not implemented.** Credentials are read once at startup;
  `ETCD_S3LOG_SESSION_TOKEN` / `AWS_SESSION_TOKEN` (assumed-role creds, signed as
  `X-Amz-Security-Token`) work, but short-lived creds are not refreshed from the
  instance/role provider — use creds whose lifetime exceeds the process, or restart
  on rotation.
- **Encryption at rest** — set `ETCD_S3LOG_SSE=aws:kms` (with
  `ETCD_S3LOG_SSE_KMS_KEY_ID`) or `ETCD_S3LOG_SSE=AES256`; enforce with a bucket
  policy that denies unencrypted PUTs.
- **Encryption in transit** — use an `https://` endpoint; do not disable Go's
  default server-certificate verification.
- **Least privilege** — the node needs only `s3:GetObject`, `s3:PutObject`,
  `s3:DeleteObject`, `s3:ListBucket` (and `s3:CreateBucket` on first run) scoped to
  the bucket, plus `sqs:{ReceiveMessage,DeleteMessage}` on the notify queue if used.
- **AWS change notifications** — set `ETCD_S3LOG_SQS_URL` to an SQS queue subscribed
  to the bucket's S3 Event Notifications (directly or via SNS); the node long-polls
  it (`SQSNotifier`) instead of MinIO's streaming extension. The Lambda push path
  (`PushNotifier`, `POST /s3`) remains available. Live-queue wiring is
  deployment-specific; only the event parsing is unit tested.
