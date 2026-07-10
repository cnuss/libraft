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

	"go.etcd.io/etcd/server/v3/config"
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
var (
	capturedBackend backend.Backend
	ActiveNS        string
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
	lg := Logger()
	guardConfig(lg, cfg)
	ActiveNS = resolveNS(lg, cfg)
	forceNewClusterPurge(lg, cfg)

	path := cfg.BackendPath()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if rerr := prepareRestore(lg, cfg); rerr != nil {
			lg.Warn("s3raft: backend restore from bucket failed; starting empty", zap.Error(rerr))
		}
	}

	be := backend.New(backendConfig(cfg, hooks))
	capturedBackend = be
	return be
}

// guardConfig turns settings s3raft cannot honor into automatic startup
// checks, so correctness no longer depends on operator discipline. It runs once,
// from the OpenBackend hijack, where the full server config is available.
func guardConfig(lg *zap.Logger, cfg config.ServerConfig) {
	// The periodic corruption check asserts the leader's revision is >= every
	// follower's — a single-leader raft invariant s3raft breaks (a fenced peer
	// that just applied is routinely ahead of the epoch owner), so it would raise
	// a false CORRUPT alarm and halt writes cluster-wide. It is off by default;
	// refuse to start when it is enabled rather than boot into that time bomb.
	if cfg.CorruptCheckTime > 0 {
		lg.Fatal("s3raft: periodic corruption check is incompatible with s3raft "+
			"(it asserts a single-leader revision ordering that the shared log violates, "+
			"raising a false CORRUPT alarm); set --corrupt-check-time to 0",
			zap.Duration("corrupt-check-time", cfg.CorruptCheckTime))
	}

	// A lease whose TTL is below the fencing-detection window can be extended by a
	// keepalive delivered to a node during its pre-demotion window while the true
	// epoch owner expires it. etcd clamps every grant up to
	// MinLeaseTTL = ceil((3*ElectionTicks/2) * TickMs) (server.go); warn when that
	// floor does not clear fenceCheckInterval, since s3raft cannot raise it here.
	heartbeat := time.Duration(cfg.TickMs) * time.Millisecond
	minLeaseTTL := time.Duration((3*cfg.ElectionTicks)/2) * heartbeat
	if minLeaseTTL <= fenceCheckInterval {
		lg.Warn("s3raft: minimum lease TTL is at or below the fencing-detection window; "+
			"a keepalive to a fenced node can extend a lease the epoch owner has expired — "+
			"raise --heartbeat-interval and/or --election-timeout so the floor exceeds it",
			zap.Duration("min-lease-ttl", minLeaseTTL),
			zap.Duration("fence-window", fenceCheckInterval))
	}
}

// forceNewClusterMarker is the sentinel file recording that a --force-new-cluster
// purge has already run for this member, so leaving the flag in a boot unit does
// not re-wipe the shared log on every restart.
const forceNewClusterMarker = "s3raft-force-new-done"

// forceNewClusterPurge implements --force-new-cluster for s3raft. etcd's own
// force-new rewrites the local WAL to a single-member config; under s3raft the
// authority is the shared S3 log, so the equivalent recovery is to WIPE this
// cluster's namespace and let the local backend re-seed it as a fresh genesis
// (start() sees an empty log, claims the epoch, and seedLogFromLocal rebuilds
// the log from local state).
//
// This is destructive and irreversible, and s3raft cannot verify the other
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
		lg.Warn("s3raft: --force-new-cluster already applied for this member; skipping purge " +
			"(remove the force-new-cluster flag from your boot config; to force again, delete " +
			marker + ")")
		return
	}

	cli, err := checkpointClient()
	if err != nil {
		lg.Fatal("s3raft: --force-new-cluster: cannot reach the shared log to purge it", zap.Error(err))
	}
	lg.Warn("s3raft: --force-new-cluster: WIPING this cluster's shared log namespace; " +
		"the local backend will re-seed it as a fresh genesis — only valid when quorum is " +
		"permanently lost (surviving members pointed at this namespace will break)")
	n, err := cli.purgeNamespace()
	if err != nil {
		lg.Fatal("s3raft: --force-new-cluster: purge failed", zap.Error(err))
	}
	if werr := os.WriteFile(marker, []byte("force-new-cluster applied\n"), 0o600); werr != nil {
		lg.Fatal("s3raft: --force-new-cluster: could not write one-shot marker", zap.Error(werr))
	}
	forceNewApplied = true
	lg.Warn("s3raft: --force-new-cluster: shared log purged",
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
	return filepath.Join(cfg.MemberDir(), "s3raft-ns")
}

// resolveNS returns the bucket namespace for this member, cached in the data
// dir so it is stable across every kind of restart. Once written, the cache is
// authoritative: a member reuses the exact prefix its cluster first wrote
// under regardless of any later config drift.
func resolveNS(lg *zap.Logger, cfg config.ServerConfig) string {
	f := nsFile(cfg)
	if b, err := os.ReadFile(f); err == nil {
		if ns := strings.TrimSpace(string(b)); ns != "" {
			return ns
		}
	}
	ns := nsFromConfig(cfg)
	// MemberDir exists by the time OpenBackend runs (etcd created snap/ under
	// it); persist best-effort so the next restart reuses this exact prefix.
	if err := os.WriteFile(f, []byte(ns), 0o600); err != nil {
		lg.Warn("s3raft: persist namespace failed", zap.Error(err), zap.String("path", f))
	}
	return ns
}

// nsFromConfig derives the per-cluster bucket namespace from three inputs that
// are each identical across every member of a cluster (including members added
// later) and stable across membership changes: the cluster token, the parent of
// the member's data directory, and the lowest initial-cluster peer URL.
//
//   - The cluster token (--initial-cluster-token) is the natural per-cluster
//     identity; it is only *read* here, never mutated, so it does not perturb
//     etcd's own cluster/member IDs (giving etcd a unique token would).
//   - The data-dir parent (the deployment's common root — /var/lib/etcd per host,
//     or one temp root per test) discriminates clusters that reuse the same token
//     (notably the e2e suite, where every cluster uses "new") but run under
//     different roots.
//   - The lowest peer URL in --initial-cluster discriminates distinct clusters
//     that share a token *and* a data-dir parent — e.g. two clusters started
//     under one process/test (make-mirror's source and dest). Every member passes
//     the same --initial-cluster, so all agree on the minimum; a member added
//     later keeps the same minimum as long as the lowest member stays, so it is
//     stable across membership (unlike the full member-set hash, which shifts on
//     any join). The minimum is a stable projection of the member set.
//
// Residual: permanently removing the lowest-peer-URL member and then adding a
// brand-new member shifts the minimum, so that new member derives a different
// namespace (existing members keep their cached one). Rare; not exercised by the
// e2e suite (MemberReplace re-adds the same URL). Two clusters collide only when
// token, data-dir parent, lowest peer URL, and bucket all match.
func nsFromConfig(cfg config.ServerConfig) string {
	root := filepath.Dir(strings.TrimRight(cfg.DataDir, string(filepath.Separator)))
	key := root + "|" + cfg.InitialClusterToken + "|" + minPeerURL(cfg)
	return fmt.Sprintf("c-%016x", fnv64a(key))
}

// minPeerURL returns the lexicographically smallest advertised peer URL across
// the initial-cluster configuration (empty if none). Identical across all
// members of a cluster because they share --initial-cluster.
func minPeerURL(cfg config.ServerConfig) string {
	min := ""
	for _, urls := range cfg.InitialPeerURLsMap {
		for _, u := range urls {
			s := u.String()
			if min == "" || s < min {
				min = s
			}
		}
	}
	return min
}

// fnv64a hashes s with the standard 64-bit FNV-1a. It backs the per-cluster
// namespace key (nsFromConfig); the value only needs to be stable and
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
	meta, err := readCheckpointMeta(cli)
	if err == errNotFound {
		return nil // no snapshot yet; fresh cluster
	}
	if err != nil {
		return err
	}
	db, err := cli.get(snapDBKey(meta.Index))
	if err != nil {
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
	lg.Info("s3raft: staged bucket snapshot for restore",
		zap.Uint64("snapshot-index", meta.Index),
		zap.Uint64("snapshot-term", meta.Term),
		zap.Int("bytes", len(db)),
		zap.String("snap-db-path", snapDBPath))
	return nil
}

// checkpointClient builds an S3 client scoped to the active namespace.
func checkpointClient() (*client, error) {
	if ActiveNS == "" {
		return nil, fmt.Errorf("s3raft: checkpoint namespace not set")
	}
	return openStore(os.Getenv(EnvURL), ActiveNS)
}

func snapDBKey(index uint64) string { return fmt.Sprintf("snap/%020d.db", index) }

func readCheckpointMeta(cli *client) (checkpointMeta, error) {
	var m checkpointMeta
	raw, err := cli.get("snap/meta")
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return m, fmt.Errorf("s3raft: corrupt checkpoint meta: %w", err)
	}
	return m, nil
}

// runCheckpointer periodically snapshots the captured backend to the bucket
// and truncates the log. It runs on every node but only the current leader
// writes snapshots (avoiding redundant uploads); every node publishes its
// own progress so the truncation floor respects the slowest member.
func (n *node) runCheckpointer() {
	if capturedBackend == nil {
		n.lg.Warn("s3raft: no captured backend; checkpointer disabled")
		return
	}
	cli, err := checkpointClient()
	if err != nil {
		n.lg.Warn("s3raft: checkpointer disabled", zap.Error(err))
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
			n.lg.Warn("s3raft: publish progress failed", zap.Error(perr))
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
		n.lg.Warn("s3raft: snapshot upload failed", zap.Error(err))
		return
	}
	n.lastCheckpoint = localSnap
	checkpointIndex.Set(float64(localSnap))
	n.pruneProgress(cli)
	n.truncateLog(cli)
}

// pruneProgress removes progress records for members no longer in the cluster.
// Without this, a departed member's last-published snapshot index would pin the
// truncation floor forever (truncationFloor takes the min across all progress
// objects), so the log could never be truncated after any membership removal.
// Leader-only, and only when membership is known so a transient empty voter set
// cannot delete live members' progress.
func (n *node) pruneProgress(cli *client) {
	if len(n.voters) == 0 {
		return
	}
	keys, err := cli.list("progress/")
	if err != nil {
		return
	}
	for _, k := range keys {
		id, ok := parseProgressKey(k)
		if !ok {
			continue
		}
		if _, member := n.voters[id]; !member {
			if derr := cli.del(k); derr == nil {
				n.lg.Info("s3raft: pruned departed member progress",
					zap.String("member-id", fmt.Sprintf("%x", id)))
			}
		}
	}
}

// uploadSnapshot writes a consistent bbolt snapshot to the bucket, then
// points the checkpoint meta at it and garbage-collects older db objects.
func (n *node) uploadSnapshot(cli *client, index uint64) error {
	snap := capturedBackend.Snapshot()
	defer snap.Close()
	var buf bytes.Buffer
	if _, err := snap.WriteTo(&buf); err != nil {
		return fmt.Errorf("bbolt snapshot: %w", err)
	}
	if err := cli.put(snapDBKey(index), buf.Bytes()); err != nil {
		return err
	}
	meta, err := json.Marshal(checkpointMeta{Index: index, Term: n.maxTerm, Voters: n.voterList()})
	if err != nil {
		return err
	}
	if err := cli.put("snap/meta", meta); err != nil {
		return err
	}
	n.lg.Info("s3raft: wrote bucket snapshot",
		zap.Uint64("index", index), zap.Int("bytes", buf.Len()))

	// GC older snapshot db objects.
	if keys, lerr := cli.list("snap/"); lerr == nil {
		for _, k := range keys {
			if idx, ok := parseSnapDBKey(k); ok && idx < index {
				_ = cli.del(k)
			}
		}
	}
	return nil
}

// truncateLog deletes log objects at or below the cluster-wide safe floor
// (min published progress minus a catch-up margin).
func (n *node) truncateLog(cli *client) {
	floor := n.truncationFloor(cli)
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
		n.lg.Info("s3raft: truncated log",
			zap.Uint64("cutoff-index", cutoff), zap.Int("deleted", deleted))
	}
}

// truncationFloor is the minimum FirstIndex across all members' published
// progress. Truncating below it is safe: no member still needs those
// entries (each has snapshotted past them locally).
func (n *node) truncationFloor(cli *client) uint64 {
	keys, err := cli.list("progress/")
	if err != nil || len(keys) == 0 {
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
