// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v3

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	serverstorage "go.etcd.io/etcd/server/v3/storage"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/raft/v3/raftpb"
)

// Checkpointing bounds two otherwise-unbounded quantities:
//
//   - Bucket storage: the log grows one object per proposal forever.
//   - Wipe-recovery time: a disk-wiped node replays the entire log from
//     index 1 to rebuild its bbolt backend.
//
// The fix mirrors raft snapshotting. Periodically the leader writes a
// consistent bbolt snapshot to <ns>/snap/<index>.db and records the index
// in <ns>/snap/meta. Log objects at or below a safe floor can then be
// deleted: a fresh node downloads the latest snapshot (restoring its
// backend to that index) and replays only the surviving log tail.
//
// The truncation floor is the minimum, across all members, of the raft
// index each has locally snapshotted+compacted (published to
// <ns>/progress/<memberID>). Deleting only below that floor guarantees no
// member — however far behind — loses an entry it still needs, minus a
// catch-up margin for safety.

const (
	checkpointInterval = 10 * time.Second
	// truncateMargin keeps this many entries below the safe floor as slack
	// for slow members (raft's SnapshotCatchUpEntries plays the same role).
	// Kept small for the PoC; production would tie it to
	// cfg.SnapshotCatchUpEntries.
	truncateMargin = 10
)

// backend capture: OpenBackend (hijacked in hijack.go) runs during
// bootstrap, before the raft node is constructed, so it stashes the opened
// backend here for the checkpointer to snapshot. activeNS is the bucket
// namespace derived from the (identical-across-members, restart-stable)
// initial-cluster configuration; the node reuses it so log and snapshot
// objects share one prefix.
// backendSnapshotter is the slice of the bbolt backend the checkpointer needs:
// a consistent point-in-time snapshot. *backend.Backend satisfies it, and tests
// substitute a fake through snapshotSource without standing up a real backend.
type backendSnapshotter interface {
	Snapshot() backend.Snapshot
}

// snapshotSource returns the current snapshot source — the backend captured by
// the OpenBackend hijack. Indirected through a var so tests can inject a fake.
var snapshotSource = func() backendSnapshotter {
	if capturedBackend == nil {
		return nil
	}
	return capturedBackend
}

var (
	capturedBackend backend.Backend
	ActiveNS        string
	// activeNSPath is the on-disk cache file for ActiveNS (nsFile(cfg)), and
	// activeNSRoot is the data-dir parent — both captured in S3OpenBackend so
	// newRaftNode, which has no ServerConfig but does have the authoritative
	// cluster ID, can recompute and rewrite the namespace (see rebindNamespace).
	activeNSPath string
	activeNSRoot string
	// restoredIndex is the snapshot index the backend was restored from
	// during disk-wiped recovery (0 if no restore happened). The node reads
	// it to resume log replay past the snapshot.
	restoredIndex uint64
	// pendingSnapshot is armed by prepareRestore on a disk-wiped start. The
	// node emits it as its first Ready.Snapshot so etcd's snapshot-apply
	// path restores the backend, applied index and MemoryStorage together —
	// exactly as a follower catching up from a leader snapshot.
	pendingSnapshot *raftpb.Snapshot
	// forceNewApplied is set by forceNewClusterPurge after it wipes the shared
	// log for --force-new-cluster. The node reads it to republish the local WAL
	// entries as the new genesis history (see start()).
	forceNewApplied bool
	// forceNewBoot is set whenever --force-new-cluster is present, even when the
	// one-shot marker suppresses the purge. The node reads it to heal a local WAL
	// that etcd's per-boot force-new surgery left ahead of the shared log,
	// instead of failing the "local ahead of shared" guard (see start()).
	forceNewBoot bool
)

type checkpointMeta struct {
	Index  uint64   `json:"index"`
	Term   uint64   `json:"term"`
	Voters []uint64 `json:"voters"`
}

type memberProgress struct {
	// SnapshotIndex is the raft index this member's state is durable
	// through (its latest local snapshot). The truncation floor is the
	// minimum across all members, so no live member loses an entry it has
	// not yet snapshotted past.
	SnapshotIndex uint64 `json:"snapshot_index"`
}

// s3OpenBackend replaces serverstorage.OpenBackend. It reproduces the stock
// backend configuration (via exported APIs), restores the bbolt file from
// the bucket when the local one is missing (disk-wiped recovery), opens the
// backend, and captures it for the checkpointer.
func S3OpenBackend(cfg config.ServerConfig, hooks backend.Hooks) backend.Backend {
	lg := Logger().Named("libraft")
	guardConfig(lg, cfg)
	activeNSPath = nsFile(cfg)
	activeNSRoot = dataDirParent(cfg)
	ActiveNS = resolveNS(lg, cfg)
	forceNewClusterPurge(lg, cfg)

	path := cfg.BackendPath()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if rerr := prepareRestore(lg, cfg); rerr != nil {
			lg.Warn("libraft: backend restore from bucket failed; starting empty", zap.Error(rerr))
		}
	}

	be := backend.New(backendConfig(cfg, hooks))
	capturedBackend = be
	return be
}

// guardConfig turns settings libraft cannot honor into automatic startup
// checks, so correctness no longer depends on operator discipline. It runs once,
// from the OpenBackend hijack, where the full server config is available.
func guardConfig(lg *zap.Logger, cfg config.ServerConfig) {
	// The periodic corruption check asserts the leader's revision is >= every
	// follower's — a single-leader raft invariant libraft breaks (a fenced peer
	// that just applied is routinely ahead of the epoch owner), so it would raise
	// a false CORRUPT alarm and halt writes cluster-wide. It is off by default;
	// refuse to start when it is enabled rather than boot into that time bomb.
	if cfg.CorruptCheckTime > 0 {
		lg.Fatal("libraft: periodic corruption check is incompatible with libraft "+
			"(it asserts a single-leader revision ordering that the shared log violates, "+
			"raising a false CORRUPT alarm); set --corrupt-check-time to 0",
			zap.Duration("corrupt-check-time", cfg.CorruptCheckTime))
	}

	// A lease whose TTL is below the fencing-detection window can be extended by a
	// keepalive delivered to a node during its pre-demotion window while the true
	// epoch owner expires it. etcd clamps every grant up to
	// MinLeaseTTL = ceil((3*ElectionTicks/2) * TickMs) (server.go); warn when that
	// floor does not clear fenceCheckInterval, since libraft cannot raise it here.
	heartbeat := time.Duration(cfg.TickMs) * time.Millisecond
	minLeaseTTL := time.Duration((3*cfg.ElectionTicks)/2) * heartbeat
	if minLeaseTTL <= fenceCheckInterval {
		lg.Warn("libraft: minimum lease TTL is at or below the fencing-detection window; "+
			"a keepalive to a fenced node can extend a lease the epoch owner has expired — "+
			"raise --heartbeat-interval and/or --election-timeout so the floor exceeds it",
			zap.Duration("min-lease-ttl", minLeaseTTL),
			zap.Duration("fence-window", fenceCheckInterval))
	}
}

// forceNewClusterMarker is the sentinel file recording that a --force-new-cluster
// purge has already run for this member, so leaving the flag in a boot unit does
// not re-wipe the shared log on every restart.
const forceNewClusterMarker = "libraft-force-new-done"

// forceNewClusterPurge implements --force-new-cluster for libraft. etcd's own
// force-new rewrites the local WAL to a single-member config; under libraft the
// authority is the shared S3 log, so the equivalent recovery is to WIPE this
// cluster's namespace and let the local backend re-seed it as a fresh genesis
// (start() sees an empty log, claims the epoch, and seedLogFromLocal rebuilds
// the log from local state).
//
// This is destructive and irreversible, and libraft cannot verify the other
// members are actually dead — only run it when quorum is permanently lost. Two
// guards keep the footguns in check: the purge is scoped strictly to this
// cluster's namespace (never the whole bucket), and a one-shot marker in the
// member dir stops a stale flag from re-wiping the log on subsequent boots.
func forceNewClusterPurge(lg *zap.Logger, cfg config.ServerConfig) {
	if !cfg.ForceNewCluster {
		return
	}
	forceNewBoot = true
	marker := filepath.Join(cfg.MemberDir(), forceNewClusterMarker)
	if _, err := os.Stat(marker); err == nil {
		lg.Warn("libraft: --force-new-cluster already applied for this member; skipping purge " +
			"(remove the force-new-cluster flag from your boot config; to force again, delete " +
			marker + ")")
		return
	}

	cli, err := checkpointClient()
	if err != nil {
		lg.Fatal("libraft: --force-new-cluster: cannot reach the shared log to purge it", zap.Error(err))
	}
	lg.Warn("libraft: --force-new-cluster: WIPING this cluster's shared log namespace; " +
		"the local backend will re-seed it as a fresh genesis — only valid when quorum is " +
		"permanently lost (surviving members pointed at this namespace will break)")
	n, err := cli.purgeNamespace()
	if err != nil {
		lg.Fatal("libraft: --force-new-cluster: purge failed", zap.Error(err))
	}
	if werr := os.WriteFile(marker, []byte("force-new-cluster applied\n"), 0o600); werr != nil {
		lg.Fatal("libraft: --force-new-cluster: could not write one-shot marker", zap.Error(werr))
	}
	forceNewApplied = true
	lg.Warn("libraft: --force-new-cluster: shared log purged",
		zap.Int("objects-deleted", n),
		zap.String("marker", marker))
}

// backendConfig mirrors serverstorage.newBackend using exported APIs so the
// hijack does not need the unexported original.
func backendConfig(cfg config.ServerConfig, hooks backend.Hooks) backend.BackendConfig {
	bcfg := backend.DefaultBackendConfig(cfg.Logger)
	bcfg.Path = cfg.BackendPath()
	bcfg.UnsafeNoFsync = cfg.UnsafeNoFsync
	if cfg.BackendBatchLimit != 0 {
		bcfg.BatchLimit = cfg.BackendBatchLimit
	}
	if cfg.BackendBatchInterval != 0 {
		bcfg.BatchInterval = cfg.BackendBatchInterval
	}
	bcfg.BackendFreelistType = cfg.BackendFreelistType
	bcfg.Logger = cfg.Logger
	if cfg.QuotaBackendBytes > 0 && cfg.QuotaBackendBytes != serverstorage.DefaultQuotaBytes {
		bcfg.MmapSize = uint64(cfg.QuotaBackendBytes + cfg.QuotaBackendBytes/10)
	}
	bcfg.Mlock = cfg.MemoryMlock
	bcfg.Hooks = hooks
	return bcfg
}

// nsFile is where the resolved namespace is cached inside the member dir.
func nsFile(cfg config.ServerConfig) string {
	return filepath.Join(cfg.MemberDir(), "libraft-ns")
}

// resolveNS returns the bucket namespace for this member, cached in the data
// dir so it is stable across every kind of restart. Once written, the cache is
// authoritative: a member reuses the exact prefix its cluster first wrote
// under regardless of any later config drift.
func resolveNS(lg *zap.Logger, cfg config.ServerConfig) (ns string) {
	// An explicit operator override wins over both the cache and derivation, and
	// is re-read each boot (deterministic), so it is not persisted.
	if env := strings.TrimSpace(os.Getenv(EnvNS)); env != "" {
		return env
	}

	f := nsFile(cfg)
	// Whatever we resolve, leave it cached: MemberDir exists by the time
	// OpenBackend runs (etcd created snap/ under it), so this best-effort write
	// lets the next restart reuse the exact prefix (and re-seats a short cache).
	defer func() { persistNS(lg, f, ns) }()

	if b, err := os.ReadFile(f); err == nil {
		if cached := strings.TrimSpace(string(b)); cached != "" {
			return cached
		}
	}
	return nsFromConfig(lg, cfg)
}

// persistNS best-effort caches the resolved namespace to path.
func persistNS(lg *zap.Logger, path, ns string) {
	if err := os.WriteFile(path, []byte(ns), 0o600); err != nil {
		lg.Warn("libraft: persist namespace failed", zap.Error(err), zap.String("path", path))
	}
}

// rebindNamespace switches ActiveNS to the authoritative etcd cluster ID once
// newRaftNode can observe it via cl.ID(). For original bootstrap members this
// equals the ID S3OpenBackend already derived from config, so it is a no-op. For
// a member that JOINED an existing cluster it corrects the namespace: a joiner's
// --initial-cluster describes the grown set, not the genesis set etcd froze the
// cluster ID over, so config derivation alone would point it at an empty log.
// The corrected value is persisted so the next restart short-circuits via the
// nsFile cache.
func rebindNamespace(lg *zap.Logger, cl *membership.RaftCluster) {
	// An explicit operator override (EnvNS) is authoritative — never clobber it
	// with the derived cluster ID.
	if strings.TrimSpace(os.Getenv(EnvNS)) != "" {
		return
	}
	ns := nsKey(activeNSRoot, cl.ID())
	if ns == ActiveNS {
		return
	}
	lg.Info("libraft: rebinding namespace to cluster ID",
		zap.String("from", ActiveNS), zap.String("to", ns),
		zap.String("cluster-id", cl.ID().String()))
	ActiveNS = ns
	if activeNSPath != "" {
		persistNS(lg, activeNSPath, ns)
	}
}

// nsFromConfig derives the per-cluster bucket namespace from two orthogonal
// inputs: the real etcd cluster ID and the parent of the member's data
// directory.
//
//   - The cluster ID (see cidFromConfig) is globally unique per cluster and
//     frozen at genesis. It already folds in --initial-cluster-token and the
//     member set (etcd hashes both), so it replaces the old token + lowest-peer-URL
//     proxy AND is stable across membership changes — unlike the lowest-peer-URL
//     projection, which shifted when the lowest member was removed.
//   - The data-dir parent (the deployment's common root — /var/lib/etcd per host,
//     or one temp root per test) discriminates otherwise-identical clusters that
//     share a bucket. etcd's cluster ID is NOT unique across such clusters: the
//     e2e suite runs dozens of single-node clusters all reusing peer URL
//     localhost:20001 + token "new", which hash to one cluster ID — only their
//     data-dir roots differ. Without this, they collide on one shared log.
//
// The cluster ID is authoritative ONLY for original bootstrap members: etcd
// freezes it over the genesis member set and never regenerates it on AddMember.
// A member that joins LATER starts with --initial-cluster describing the grown
// set, so cidFromConfig would hash the wrong set — that member instead learns
// the frozen ID from the running cluster and has it rebound in newRaftNode (see
// rebindNamespace). For such a joiner the ID derived here is only used for the
// disk-wiped-restore probe, a no-op on a fresh member, so the mismatch is
// harmless. The data-dir parent is identical across all members of a cluster
// (they share a deployment root), so it does not perturb this agreement.
func nsFromConfig(lg *zap.Logger, cfg config.ServerConfig) string {
	return nsKey(dataDirParent(cfg), cidFromConfig(lg, cfg))
}

// dataDirParent is the deployment root shared by every member of a cluster (the
// parent of --data-dir), used to isolate clusters that share a bucket and a
// cluster ID (see nsFromConfig).
func dataDirParent(cfg config.ServerConfig) string {
	return filepath.Dir(strings.TrimRight(cfg.DataDir, string(filepath.Separator)))
}

// cidFromConfig reconstructs the etcd cluster ID from ServerConfig by reusing
// etcd's own membership hashing — the identical code path bootstrap takes
// (NewClusterFromURLsMap over --initial-cluster-token + --initial-cluster).
// Reusing the upstream function rather than reimplementing the SHA-1 keeps the
// derivation from silently drifting on an etcd bump.
func cidFromConfig(lg *zap.Logger, cfg config.ServerConfig) types.ID {
	cl, err := membership.NewClusterFromURLsMap(lg, cfg.InitialClusterToken, cfg.InitialPeerURLsMap)
	if err != nil {
		// Only fires on a malformed initial-cluster (duplicate/zero member ID),
		// which etcd would itself reject moments later. Deterministic across
		// members, so a zero fallback stays consistent rather than split-brained.
		lg.Warn("libraft: cluster ID derivation failed", zap.Error(err))
		return types.ID(0)
	}
	return cl.ID()
}

// nsKey formats the bucket namespace prefix from the data-dir root and the etcd
// cluster ID. The cluster ID stays legible (`c-<id>`); the root is folded in as
// a hash so filesystem-path characters can't leak into the S3 key.
func nsKey(root string, id types.ID) string {
	return fmt.Sprintf("c-%016x-%016x", uint64(id), fnv64a(root))
}

// fnv64a hashes s with the standard 64-bit FNV-1a. It folds the data-dir root
// into the namespace key (see nsKey); the value only needs to be stable and
// well-distributed, not any particular basis.
func fnv64a(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

// prepareRestore downloads the latest bucket snapshot and stages it for
// etcd's snapshot-apply path: the bbolt bytes are written to the snap
// directory as <index>.snap.db (where OpenSnapshotBackend looks for them),
// and a raftpb.Snapshot is armed for the node to emit. This fast-forwards
// the backend, applied index and MemoryStorage in one atomic step, which a
// bare backend-file copy cannot do.
func prepareRestore(lg *zap.Logger, cfg config.ServerConfig) error {
	cli, err := checkpointClient()
	if err != nil {
		return err
	}
	// Resolve meta -> db atomically-by-retry: the leader's checkpoint tick can
	// GC snap/<index>.db (uploadSnapshot's GC loop) between our meta read and the
	// db fetch, 404ing an in-flight download. On errNotFound re-read meta — which
	// now points at the newer, still-present snapshot — and retry. Without this a
	// 404 would surface as "start empty" and open a truncation gap.
	var meta checkpointMeta
	var db []byte
	for attempt := 0; ; attempt++ {
		meta, err = readCheckpointMeta(cli)
		if err == errNotFound {
			return nil // no snapshot yet; fresh cluster
		}
		if err != nil {
			return err
		}
		db, err = cli.get(snapDBKey(meta.Index))
		if err == nil {
			break
		}
		if err == errNotFound && attempt < 4 {
			continue // GC raced us between meta and db; re-resolve
		}
		return err
	}

	snapDBPath := filepath.Join(cfg.SnapDir(), fmt.Sprintf("%016x.snap.db", meta.Index))
	if err := os.MkdirAll(cfg.SnapDir(), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(snapDBPath, db, 0o600); err != nil {
		return err
	}

	restoredIndex = meta.Index
	pendingSnapshot = &raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{
			Index:     proto.Uint64(meta.Index),
			Term:      proto.Uint64(meta.Term),
			ConfState: &raftpb.ConfState{Voters: meta.Voters},
		},
	}
	lg.Info("libraft: staged bucket snapshot for restore",
		zap.Uint64("snapshot-index", meta.Index),
		zap.Uint64("snapshot-term", meta.Term),
		zap.Int("bytes", len(db)),
		zap.String("snap-db-path", snapDBPath))
	return nil
}

// checkpointClient builds an S3 client scoped to the active namespace.
func checkpointClient() (*client, error) {
	if ActiveNS == "" {
		return nil, fmt.Errorf("libraft: checkpoint namespace not set")
	}
	return openStore(os.Getenv(EnvURL), ActiveNS)
}

func snapDBKey(index uint64) string { return fmt.Sprintf("snap/%020d.db", index) }

func readCheckpointMeta(cli store) (checkpointMeta, error) {
	var m checkpointMeta
	raw, err := cli.get("snap/meta")
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return m, fmt.Errorf("libraft: corrupt checkpoint meta: %w", err)
	}
	return m, nil
}

// runCheckpointer periodically snapshots the captured backend to the bucket
// and truncates the log. It runs on every node but only the current leader
// writes snapshots (avoiding redundant uploads); every node publishes its
// own progress so the truncation floor respects the slowest member.
func (n *node) runCheckpointer() {
	if capturedBackend == nil {
		n.lg.Warn("libraft: no captured backend; checkpointer disabled")
		return
	}
	cli, err := checkpointClient()
	if err != nil {
		n.lg.Warn("libraft: checkpointer disabled", zap.Error(err))
		return
	}
	cli.baseCtx = n.bgCtx // abort in-flight snapshot upload on shutdown
	t := time.NewTicker(checkpointInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			n.checkpoint(cli)
		case <-n.done:
			return
		}
	}
}

func (n *node) checkpoint(cli *client) {
	// The MemoryStorage snapshot index is the raft index etcd last
	// snapshotted at (it calls CreateSnapshot every SnapshotCount applied
	// entries). etcd commits the backend at/before that index, so the
	// backend is durable through it — a safe index to advertise and, for
	// the leader, to snapshot the bbolt state at. (FirstIndex lags by
	// SnapshotCatchUpEntries and is too coarse.)
	snap, err := n.ms.Snapshot()
	if err != nil {
		return
	}
	localSnap := snap.Metadata.GetIndex()

	// Publish this member's progress for the shared truncation floor.
	if body, merr := json.Marshal(memberProgress{SnapshotIndex: localSnap}); merr == nil {
		if perr := cli.put(progressKey(n.id), body); perr != nil {
			n.lg.Warn("libraft: publish progress failed", zap.Error(perr))
		}
	}

	// Only the leader writes snapshots + truncates. Racy read of isLeader is
	// fine: a stale reader at worst skips or does a redundant checkpoint.
	if !n.isLeader || localSnap == 0 {
		return
	}
	if localSnap <= n.lastCheckpoint {
		return // nothing new since last checkpoint
	}

	if err := n.uploadSnapshot(cli, localSnap); err != nil {
		n.lg.Warn("libraft: snapshot upload failed", zap.Error(err))
		return
	}
	n.lastCheckpoint = localSnap
	checkpointIndex.Set(float64(localSnap))
	// pruneProgress lists progress/ once and returns the surviving keys, which
	// truncateLog reuses so the floor computation need not list again.
	survivors := n.pruneProgress(cli)
	n.truncateLog(cli, survivors)
}

// pruneProgress removes progress records for members no longer in the cluster
// and returns the surviving progress/ keys (for truncationFloor to reuse without
// re-listing). Without pruning, a departed member's last-published snapshot index
// would pin the truncation floor forever (truncationFloor takes the min across
// all progress objects), so the log could never be truncated after any removal.
// Leader-only, and only when membership is known so a transient empty voter set
// cannot delete live members' progress. Returns nil when membership is unknown or
// the list fails, signalling truncationFloor to list for itself.
func (n *node) pruneProgress(cli store) []string {
	// Snapshot the voter set under membMu; the run goroutine may mutate it
	// (applyConf) concurrently, and an unsynchronized map read would crash.
	voters := n.voterSet()
	if len(voters) == 0 {
		return nil
	}
	keys, err := cli.list("progress/")
	if err != nil {
		return nil
	}
	survivors := make([]string, 0, len(keys))
	for _, k := range keys {
		if id, ok := parseProgressKey(k); ok {
			if _, member := voters[id]; !member {
				if derr := cli.del(k); derr == nil {
					n.lg.Info("libraft: pruned departed member progress",
						zap.String("member-id", fmt.Sprintf("%x", id)))
					continue // dropped; not a survivor
				}
				// delete failed: keep it so the floor stays conservative
			}
		}
		survivors = append(survivors, k)
	}
	return survivors
}

// uploadSnapshot writes a consistent bbolt snapshot to the bucket, then
// points the checkpoint meta at it and garbage-collects older db objects.
func (n *node) uploadSnapshot(cli store, index uint64) error {
	src := snapshotSource()
	if src == nil {
		return fmt.Errorf("libraft: no captured backend to snapshot")
	}
	snap := src.Snapshot()
	defer snap.Close()
	var buf bytes.Buffer
	if _, err := snap.WriteTo(&buf); err != nil {
		return fmt.Errorf("bbolt snapshot: %w", err)
	}
	if err := cli.put(snapDBKey(index), buf.Bytes()); err != nil {
		return err
	}
	// getMaxTerm/voterList take membMu: this runs on the checkpointer goroutine,
	// off the run goroutine that writes maxTerm and voters.
	meta, err := json.Marshal(checkpointMeta{Index: index, Term: n.getMaxTerm(), Voters: n.voterList()})
	if err != nil {
		return err
	}
	if err := cli.put("snap/meta", meta); err != nil {
		return err
	}
	n.lg.Info("libraft: wrote bucket snapshot",
		zap.Uint64("index", index), zap.Int("bytes", buf.Len()))

	// GC older snapshot db objects, but retain the immediately-previous one:
	// a restorer that already read an older meta must still be able to fetch its
	// db. Keeping the last two means a single concurrent checkpoint never orphans
	// an in-flight reader; prepareRestore's retry covers the rarer case of two
	// checkpoints landing during one restore.
	if keys, lerr := cli.list("snap/"); lerr == nil {
		prev := uint64(0)
		for _, k := range keys {
			if idx, ok := parseSnapDBKey(k); ok && idx < index && idx > prev {
				prev = idx // highest index strictly below the new one
			}
		}
		for _, k := range keys {
			if idx, ok := parseSnapDBKey(k); ok && idx < prev {
				_ = cli.del(k)
			}
		}
	}
	return nil
}

// truncateLog deletes log objects at or below the cluster-wide safe floor
// (min published progress minus a catch-up margin).
func (n *node) truncateLog(cli store, progressKeys []string) {
	floor := n.truncationFloor(cli, progressKeys)
	if floor <= truncateMargin {
		return
	}
	cutoff := floor - truncateMargin
	keys, err := cli.list("log/")
	if err != nil {
		return
	}
	deleted := 0
	for _, k := range keys {
		if idx, ok := parseLogKey(k); ok && idx <= cutoff {
			if derr := cli.del(k); derr == nil {
				deleted++
			}
		}
	}
	if deleted > 0 {
		n.lg.Info("libraft: truncated log",
			zap.Uint64("cutoff-index", cutoff), zap.Int("deleted", deleted))
	}
}

// truncationFloor is the minimum FirstIndex across all members' published
// progress. Truncating below it is safe: no member still needs those
// entries (each has snapshotted past them locally).
func (n *node) truncationFloor(cli store, keys []string) uint64 {
	// keys == nil means the caller has no cached listing (membership unknown or
	// pruneProgress's list failed); list here. A non-nil empty slice means the
	// caller listed and found nothing, so the floor is 0.
	if keys == nil {
		var err error
		if keys, err = cli.list("progress/"); err != nil {
			return 0
		}
	}
	if len(keys) == 0 {
		return 0
	}
	min := uint64(math.MaxUint64)
	for _, k := range keys {
		raw, gerr := cli.get(k)
		if gerr != nil {
			return 0 // be conservative if any member's progress is unreadable
		}
		var p memberProgress
		if json.Unmarshal(raw, &p) != nil {
			return 0
		}
		if p.SnapshotIndex < min {
			min = p.SnapshotIndex
		}
	}
	if min == math.MaxUint64 {
		return 0
	}
	return min
}

func progressKey(id uint64) string { return fmt.Sprintf("progress/%016x", id) }

func parseProgressKey(key string) (uint64, bool) {
	base := strings.TrimPrefix(key, "progress/")
	if base == key {
		return 0, false
	}
	id, err := strconv.ParseUint(base, 16, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

func parseSnapDBKey(key string) (uint64, bool) {
	base := strings.TrimPrefix(key, "snap/")
	base = strings.TrimSuffix(base, ".db")
	if base == key {
		return 0, false
	}
	n, err := strconv.ParseUint(base, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}
