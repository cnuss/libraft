# s3raft known limitations & safety notes

s3raft replaces raft with a shared S3 CAS log: every node runs its own etcd
apply loop over the same totally-ordered log, so anything computed at apply time
(revision, compactRevision, consistentIndex, auth revision, alarm state) is a
deterministic function of the log and stays identical across nodes. The items
below are where that model — one fencing-epoch owner leads, every node applies the
same S3-CAS-ordered log — needs care or falls short.

## Guarded by startup checks

s3raft validates these at boot (`guardConfig`, checkpoint.go), so correctness no
longer depends on operator discipline:

- **Periodic corruption check is INCOMPATIBLE — s3raft refuses to start with it on.**
  `corrupt.go`'s `PeriodicCheck` asserts the leader's revision/compactRevision is
  `>=` every follower's — a hard single-leader raft invariant. Under s3raft the epoch
  owner is routinely *behind* a peer that just wrote and applied locally, so the
  assertion fails and raises a false `CORRUPT` alarm (member, or whole-cluster, write
  outage). It is off by default (`corrupt-check-time-interval: 0s`); if
  `--corrupt-check-time` > 0, s3raft fails fast with a clear error rather than boot into
  that time bomb. The cross-member KV *hash* comparison itself is fine (identical apply
  order ⇒ identical hash at identical revision); only the revision-ordering assertion
  breaks.

- **Lease TTLs must exceed the fencing-detection window** (`fenceCheckInterval`, 1s).
  Lease renewals apply to the local lessor and are not replicated through the log, so a
  keepalive delivered to a node during its pre-demotion window extends the lease only
  there while the true epoch owner may expire it. etcd already clamps every grant up to
  `MinLeaseTTL = ceil((3*ElectionTicks/2) · TickMs)` (≈2s at defaults, clearing the 1s
  window with margin); s3raft **warns** at startup if that floor does not exceed the
  window (e.g. under an unusually short election timeout). Direct keepalives at the
  current epoch owner.

## Efficiency (correct, but wasteful) — not yet addressed

- **A hung S3 call blocks the single-threaded run loop** for the S3 retry budget
  (`s3RetryBudget`, ~12s; per-attempt deadline 5s) since confirmRead/checkTail/
  checkEpoch do synchronous S3 I/O on the loop. Bounded and correct (reads fail
  closed), but a fully-hung store still stalls the loop for that budget; a
  caller-supplied context to cut the read path shorter is a further refinement.

## Membership & restore

Each cluster's objects live under a bucket namespace (`<bucket>/<prefix>/c-<hash>/…`)
derived from three inputs — the **cluster token**, the **parent of the member's data
directory**, and the **lowest `--initial-cluster` peer URL** (`nsFromConfig`,
checkpoint.go) — cached on first boot in `<member>/s3raft-ns`. All three are identical
across members (incl. those added later), stable across membership changes, and only
read — never mutated — so they don't perturb etcd's cluster ID. The etcd cluster ID
itself would be the ideal key but is unreachable without editing `bootstrap.go`
(it's computed after the backend is opened and never handed to the raft node).

- **Clusters that share one bucket need a distinct token, data-dir parent, or lowest
  peer URL.** Any one differing isolates them. Only clusters matching on all three
  *and* the same bucket collide — give one a distinct token, path, peer URL, or
  bucket. A single cluster per bucket/prefix needs no coordination.

- **A joining member must pass the same token, share the same data-dir parent, and
  keep the same lowest peer URL** as the cluster it joins, or it derives a different
  namespace and won't find the log. All hold for a normal member add (a new member
  has a higher URL, so the minimum is unchanged). The one gap: permanently removing
  the lowest-peer-URL member and then adding a brand-new member shifts the minimum,
  so that new member can't find the log (existing members keep their cached namespace).

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
  learn of it only by *pulling* the log (s3raft sends no raft traffic), so the serving
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
  config; under s3raft the authority is the shared log, so the equivalent recovery is
  destructive: `forceNewClusterPurge` (checkpoint.go) synchronously deletes every
  object under this cluster's namespace (scoped strictly to the namespace prefix —
  never the whole bucket, so clusters sharing a bucket are untouched), then start()
  republishes the local committed WAL entries (`publishLocalEntries`) as the new
  genesis history and this node claims the epoch. If etcd had taken a raft snapshot,
  the republish starts at the snapshot boundary and also publishes a bucket snapshot at
  the current index so a future member can restore the compacted-away prefix. Use it
  **only when quorum is permanently lost** — s3raft cannot verify the other members are
  dead, and any surviving member still pointed at this namespace will break (split
  brain). A one-shot marker (`s3raft-force-new-done` in the member dir) stops a flag
  left in a boot unit from re-wiping the log on every restart; because etcd's force-new
  surgery still appends a fresh conf-change on each such restart, start() then heals the
  one-entry-ahead local WAL by publishing its tail rather than failing. To force a fresh
  recovery again, delete the marker.

## Unsupported

- **Explicit leader transfer** (`MoveLeader`) is a no-op — leadership is pinned to the
  epoch owner (see "Leadership is pinned to the founder" above), not movable by this
  call. It reports success immediately without moving anything (which also lets
  graceful-shutdown transfer complete instead of blocking).

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
