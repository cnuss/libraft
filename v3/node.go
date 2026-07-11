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
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

// envURL is the switch: when non-empty, s3raft takes over consensus.
const EnvURL = "ETCD_S3LOG_URL"

var _ raft.Node = &node{}

// node implements raft.Node on top of a shared S3 CAS log.
//
// Consensus model: there is no election, no quorum and no replication —
// S3 itself is the single strongly-consistent, internally replicated
// authority. A proposal is "committed" the moment its log object is
// successfully created with `If-None-Match: *`: exactly one writer can win
// each log index, which yields a total order across any number of
// concurrent etcd processes sharing the bucket. Every node considers
// itself "leader" so the local etcd server accepts writes; conflicting
// appends simply lose the CAS and retry at the next index after ingesting
// the winner's entry.
//
// Terms are fencing epochs: each node CAS-bumps meta/epoch at boot and
// stamps entries with the highest term it has seen. The most recent
// booter is the leader (lessor primary); everyone else demotes on
// observing its higher term in the log. Multi-writer safety of the log
// itself comes purely from CAS and needs no fencing; the epoch fences
// leader-derived authority. Residual window: a fenced node notices its
// demotion only when it next reads the log (bounded by the poll
// interval), so lease TTLs must comfortably exceed that.
type node struct {
	lg  *zap.Logger
	id  uint64
	cli store
	clk clock // wall-clock seam for the sync-debounce cooldown + CAS backoff

	proposec  chan proposal
	readc     chan []byte
	confc     chan confReq
	statusc   chan chan raft.Status
	pokec     chan struct{}
	advancec  chan struct{}
	readyc    chan raft.Ready
	transferc chan uint64 // shutdown leadership-transfer target (see TransferLeadership)
	stopc     chan struct{}
	done      chan struct{}

	// notifyCancel stops the log-change notifier goroutine (see notify.go) and,
	// via bgCtx, aborts in-flight background S3 calls on shutdown.
	notifyCancel context.CancelFunc
	bgCtx        context.Context

	// state below is owned by run()
	lastIndex      uint64          // last log index known committed in S3
	stableIndex    uint64          // last index already in the local MemoryStorage/WAL
	pendingEntries []*raftpb.Entry // to hand out via Ready.Entries (WAL append)
	pendingCommit  []*raftpb.Entry // to hand out via Ready.CommittedEntries
	pendingReads   []raft.ReadState
	pendingSoft    *raft.SoftState   // leadership announcement for the next Ready
	pendingMsgs    []*raftpb.Message // heartbeats to hand out via Ready.Messages

	// voters and maxTerm are written only on the run goroutine, but the
	// checkpointer goroutine (runCheckpointer -> pruneProgress/uploadSnapshot)
	// reads them concurrently. membMu guards those writes against the
	// checkpointer's reads: an unsynchronized map read racing applyConf's write
	// is a fatal "concurrent map read and map write" that crashes the process.
	// Run-goroutine reads stay lockless (ordered against the writes by being on
	// the same goroutine); only the run-side writes take Lock and the off-run
	// checkpointer reads take RLock.
	membMu sync.RWMutex
	voters map[uint64]struct{}

	// Fencing epoch state (also owned by run()). myEpoch is the epoch this
	// node claimed at boot by CAS-bumping meta/epoch — the s3raft analog
	// of winning a raft election; every entry we write is stamped with the
	// highest term we have seen. Observing any entry with a term above
	// myEpoch means a newer node has since claimed the epoch: we demote
	// (SoftState follower) so etcdserver demotes the lessor and forwards
	// leader-only work to the epoch owner. Plain KV appends stay unfenced
	// on purpose — the CAS log is safe from any writer; the epoch fences
	// authority (lease primary), not writes.
	myEpoch  uint64
	maxTerm  uint64
	isLeader bool
	// lead is the member id this node currently announces as leader (the epoch
	// owner) while demoted. Kept current by checkEpoch so a follower tracks the
	// owner as it advances across successive joins, instead of freezing on the
	// first owner it fenced to.
	lead uint64

	// ms is etcd's raft MemoryStorage (owned by etcd, shared with us).
	// FirstIndex advances as etcd locally snapshots and compacts, which the
	// checkpointer observes to bound bucket storage. lastCheckpoint is the
	// raft index of the most recent snapshot this node uploaded.
	ms             *raft.MemoryStorage
	lastCheckpoint uint64

	// syncTail debounce state (owned by run()): cap blocking log reads so a
	// notification storm cannot starve the run loop.
	lastSync    time.Time
	resyncArmed bool

	// bootSnap, when set, is emitted as the first Ready.Snapshot to
	// fast-forward etcd through a bucket-restored snapshot on a disk-wiped
	// start (see checkpoint.go).
	bootSnap *raftpb.Snapshot
}

type proposal struct {
	typ  raftpb.EntryType
	data []byte
	resc chan error
}

type confReq struct {
	cc   raftpb.ConfChangeI
	resc chan *raftpb.ConfState
}

const pollInterval = time.Second

// minSyncInterval caps how often the run loop performs a blocking log read in
// response to notification wakeups, bounding notification-driven latency while
// leaving the loop free to service proposals.
const minSyncInterval = 50 * time.Millisecond

// fenceCheckInterval is how often a node acting as leader re-confirms its
// fencing epoch is still current (see checkEpoch). It bounds the worst-case
// window during which a superseded node keeps acting as leader; lease TTLs must
// comfortably exceed it (etcd's default min lease TTL of 5s does, with margin).
const fenceCheckInterval = time.Second

// genesisEpoch is the fencing epoch of a freshly bootstrapped cluster: the
// first bumpEpoch on an absent meta/epoch yields 1. Genesis ConfChange entries
// are stamped with it so every founder writes identical entries.
const genesisEpoch = 1

// heartbeatInterval is how often the node emits no-op MsgHeartbeat to its peers.
// s3raft exchanges no real raft traffic, so without these etcd's rafthttp
// transport never marks peers "active" and reconfiguration health gates
// (isConnectedToQuorumAfterAddingNewMemberSince, gated on HealthInterval) reject
// member changes with "unhealthy cluster". Kept well under etcd's HealthInterval
// so a quorum always looks freshly connected. The heartbeats carry no state; the
// receiver's Step is a no-op.
const heartbeatInterval = 500 * time.Millisecond

func logKey(index uint64) string { return fmt.Sprintf("log/%020d", index) }

func parseLogKey(key string) (uint64, bool) {
	s := strings.TrimPrefix(key, "log/")
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// isLogKey reports whether an object key (namespaced or bare) names a log
// object — i.e. its last "log/" segment is followed by a numeric index. It is
// the single predicate the notifiers use, deferring the numeric check to
// parseLogKey instead of matching "/log/" as a loose substring.
func isLogKey(key string) bool {
	if i := strings.LastIndex(key, "log/"); i >= 0 {
		key = key[i:]
	}
	_, ok := parseLogKey(key)
	return ok
}

// seedGenesis bootstraps a fresh cluster (peers = --initial-cluster-state=new):
// it CAS-writes the initial ConfChange entries into the empty log and sets this
// node's initial leadership, before Start's replay reads the entries back.
// Deterministic leadership: the lowest member id is the epoch owner and the
// other founders boot as its followers, avoiding a boot-order race where every
// founder claims the epoch and then fences down to the winner (a settle window
// that surfaces transient "leader changed" errors on a high-latency store).
func (n *node) seedGenesis(peers []raft.Peer) error {
	designated := n.id
	for _, p := range peers {
		if p.ID < designated {
			designated = p.ID
		}
	}

	// Seed the initial ConfChange entries. Idempotent: an existing log wins the
	// CAS and is adopted by the replay in Start. Stamped with the genesis epoch
	// so every founder writes identical entries regardless of which leads or
	// wins each index.
	for i, p := range peers {
		cc := &raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode.Enum(),
			NodeId:  proto.Uint64(p.ID),
			Context: p.Context,
		}
		data, merr := proto.Marshal(cc)
		if merr != nil {
			return merr
		}
		ent := &raftpb.Entry{
			Term:  proto.Uint64(genesisEpoch),
			Index: proto.Uint64(uint64(i + 1)),
			Type:  raftpb.EntryConfChange.Enum(),
			Data:  data,
		}
		body, merr := encodeEntries([]*raftpb.Entry{ent})
		if merr != nil {
			return merr
		}
		if perr := n.cli.appendCAS(uint64(i+1), uint64(i+1), body); perr != nil && perr != errConflict {
			return perr
		}
	}

	if n.id == designated {
		return n.claimEpoch()
	}
	n.lead = designated
	n.isLeader = false
	n.pendingSoft = &raft.SoftState{Lead: designated, RaftState: raft.StateFollower}
	n.lg.Info("s3raft: genesis follower of designated (lowest-id) leader",
		zap.String("leader", fmt.Sprintf("%x", designated)))
	return nil
}

// start creates (or joins) the shared S3 log and returns a raft.Node
// backed by it. peers is non-empty when bootstrapping a new cluster, in
// which case the initial ConfChange entries are CAS-written to the log
// (idempotently: an existing log wins and is adopted). ms is the
// WAL-seeded MemoryStorage; entries already present there are not
// re-emitted through Ready.Entries.
//
// nsKey namespaces all objects (`<nsKey>/...`) so multiple etcd clusters
// can share one bucket without seeing each other's logs. The hijack layer
// derives it (see nsFromConfig) identically across members and stably across
// membership changes and disk-wiped restarts.
func Start(lg *zap.Logger, rawurl string, id uint64, nsKey string, peers []raft.Peer, ms *raft.MemoryStorage) (raft.Node, error) {
	// The per-cluster namespace isolates distinct clusters that share one
	// bucket (see nsFromConfig): identical across all members (including those
	// added later) and stable across membership changes, so a joining member
	// finds the same log. Empty means the bucket/prefix root is the log.
	cli, err := openStore(rawurl, nsKey)
	if err != nil {
		return nil, err
	}

	n := &node{
		lg:        lg.Named("s3raft"),
		id:        id,
		cli:       cli,
		clk:       realClock{},
		proposec:  make(chan proposal),
		readc:     make(chan []byte),
		confc:     make(chan confReq),
		statusc:   make(chan chan raft.Status),
		pokec:     make(chan struct{}, 1),
		advancec:  make(chan struct{}),
		readyc:    make(chan raft.Ready),
		transferc: make(chan uint64),
		stopc:     make(chan struct{}),
		done:      make(chan struct{}),
		voters:    make(map[uint64]struct{}),
		ms:        ms,
	}

	// Local durable state: entries the WAL already holds. base is the
	// local snapshot index (0 on a fresh start).
	fi, err := ms.FirstIndex()
	if err != nil {
		return nil, err
	}
	li, err := ms.LastIndex()
	if err != nil {
		return nil, err
	}
	base := fi - 1
	n.stableIndex = li
	n.lastIndex = li

	// Disk-wiped recovery: s3OpenBackend restored the bbolt backend from a
	// bucket snapshot at restoredIndex, but the local WAL (and hence ms) is
	// empty. The log below the snapshot has been truncated, so replay must
	// resume at the snapshot index — not index 0, which would hit the
	// truncation gap. The restored backend already holds all state through
	// restoredIndex, so emitting from restoredIndex+1 is correct.
	if restoredIndex > base {
		base = restoredIndex
		n.stableIndex = restoredIndex
		n.lastIndex = restoredIndex
		n.bootSnap = pendingSnapshot // emitted first to fast-forward etcd
		n.lg.Info("s3raft: resuming replay past restored snapshot",
			zap.Uint64("restored-index", restoredIndex))
	}

	// --force-new-cluster re-genesis. forceNewClusterPurge (checkpoint.go) has
	// already wiped this cluster's shared log; etcd's own force-new surgery left
	// the local WAL holding the committed entries after its last raft snapshot
	// (base), with etcd's apply index resuming from base. Republish those local
	// entries (base+1..li) into the empty log so the replay below re-emits them
	// and etcd applies them contiguously from base — the re-genesis history. A
	// plain snapshot seed (seedLogFromLocal) would instead compact the log to a
	// single boundary at li and leave etcd with an unfillable base+1..li gap
	// ("unexpected committed entry index"). When base>0 (etcd took a snapshot),
	// also publish a bucket snapshot at li so a future member can restore the
	// compacted-away prefix (base=0 keeps the full history in the log).
	if forceNewApplied && li > 0 {
		if perr := n.publishLocalEntries(ms, fi, li); perr != nil {
			return nil, fmt.Errorf("s3raft: force-new-cluster: %w", perr)
		}
		if base > 0 {
			if serr := n.uploadSnapshot(cli, li); serr != nil {
				return nil, fmt.Errorf("s3raft: force-new-cluster: publish restore snapshot at %d: %w", li, serr)
			}
			n.lastCheckpoint = li
		}
		n.lg.Warn("s3raft: --force-new-cluster: republished local WAL as new genesis history",
			zap.Uint64("entries", li), zap.Uint64("from-index", base))
	}

	// Genesis (peers non-empty = --initial-cluster-state=new) seeds the initial
	// ConfChange entries into the empty log *before* the replay below reads them
	// back and hands them to etcd. A member joining an existing cluster (peers
	// empty, the RestartNode path) instead replays first and decides leadership
	// afterwards — because it may be a learner, which must never claim the epoch
	// (see below).
	genesis := len(peers) > 0
	if genesis {
		if err := n.seedGenesis(peers); err != nil {
			return nil, err
		}
	}

	// Replay the shared log from base: everything in S3 beyond the local
	// snapshot is (re-)emitted as committed; the etcd apply loop drops
	// entries at or below its consistent index, so replay is idempotent.
	replay, err := n.fetchTail(base)
	if err != nil {
		return nil, err
	}
	n.ingest(replay)

	// Leadership for a joining member. Two rules:
	//   - A learner must never claim the epoch — it cannot serve client traffic
	//     (learners reject client RPCs), so fencing the real leader to it would
	//     wedge the cluster.
	//   - A brand-new member joining a running cluster follows the existing
	//     owner rather than seizing leadership, mirroring raft (adding a member
	//     does not trigger a leader change) and keeping leadership on the
	//     founder. Otherwise the newest voter would become leader, and the two
	//     TestCtlV3MemberPromoteWithAuth tests — which pick the "follower" as
	//     (leaderIdx+1) — would land on the just-added learner.
	// Only genesis (handled above) and a *restart/recovery* of a voting member
	// claim the epoch; the restart path is how leadership advances after the
	// previous owner is gone (li>0 from the WAL, or a disk-wiped restore).
	if !genesis {
		learner := bootIsLearner(id, ms, replay)
		freshJoin := li == 0 && restoredIndex == 0
		if learner || freshJoin {
			d, eerr := cli.currentEpoch()
			if eerr != nil {
				return nil, eerr
			}
			n.lead = d.Owner
			n.isLeader = false
			n.pendingSoft = &raft.SoftState{Lead: d.Owner, RaftState: raft.StateFollower}
			n.lg.Info("s3raft: joined existing cluster — following epoch owner",
				zap.Bool("learner", learner),
				zap.Uint64("epoch", d.Epoch),
				zap.String("owner", fmt.Sprintf("%x", d.Owner)))
		} else if err := n.claimEpoch(); err != nil {
			return nil, err
		}
	}

	// Reconcile local durable state with the shared log before proceeding.
	// After replay, n.lastIndex reflects the higher of local state and the
	// shared log. Three cases matter:
	//   - shared log >= local: normal; replay already caught us up.
	//   - local ahead of an EMPTY log: the backend was restored (e.g. via
	//     `etcdutl snapshot restore`) but no shared log exists for this
	//     namespace — bootstrap the log from the restored state (#2).
	//   - no local state AND empty log AND not a genesis member: a member
	//     trying to join a running cluster, whose log lives under a different
	//     namespace. Unsupported — fail fast with a clear message (#3).
	s3idx, err := n.committedLogIndex()
	if err != nil {
		return nil, err
	}
	if s3idx < n.lastIndex {
		if s3idx != 0 && !forceNewBoot {
			return nil, fmt.Errorf("s3raft: local state (index %d) is ahead of a non-empty shared log (index %d); the shared log is authoritative — restore the bucket, not a local snapshot", n.lastIndex, s3idx)
		}
		if s3idx == 0 {
			if serr := n.seedLogFromLocal(n.lastIndex); serr != nil {
				return nil, fmt.Errorf("s3raft: bootstrap shared log from restored state: %w", serr)
			}
		} else {
			// --force-new-cluster boot after the initial purge: this member's
			// local WAL is the authority, but --force-new-cluster (still set in
			// the boot args) makes etcd's bootstrap append a fresh conf-change
			// each restart, leaving the local WAL one ahead of the shared log.
			// The one-shot marker suppressed a re-purge, so heal the divergence
			// by publishing the local tail rather than failing.
			if serr := n.publishLocalEntries(ms, s3idx+1, n.lastIndex); serr != nil {
				return nil, fmt.Errorf("s3raft: force-new-cluster: heal shared log: %w", serr)
			}
			n.lg.Warn("s3raft: --force-new-cluster: healed shared log from local tail",
				zap.Uint64("from", s3idx+1), zap.Uint64("to", n.lastIndex))
		}
	} else if s3idx == 0 && n.lastIndex == 0 && len(peers) == 0 {
		return nil, fmt.Errorf("s3raft: no shared log for this namespace and no local state — adding a member to a running s3raft cluster is unsupported; provision all members at cluster creation (see LIMITATIONS.md)")
	}

	// Append the leader no-op entry, mirroring what a freshly elected raft
	// leader does. etcdserver promotes the lessor (lease primary) only when
	// it applies an empty entry — without this, leases never renew. Only a
	// node that actually claimed leadership does this; a learner-follower must
	// not (it holds no epoch to stamp the entry and does not lead).
	// (run() has not started yet, so calling appendBatch directly is safe.)
	if n.isLeader {
		if err := n.appendBatch([]proposal{{typ: raftpb.EntryNormal}}); err != nil {
			return nil, fmt.Errorf("s3raft: leader no-op append: %w", err)
		}
	}

	n.lg.Info("s3raft node started",
		zap.String("member-id", fmt.Sprintf("%x", id)),
		zap.String("bucket", cli.bucket),
		zap.Bool("leader", n.isLeader),
		zap.Uint64("my-epoch", n.myEpoch),
		zap.Uint64("local-stable-index", li),
		zap.Uint64("log-tail-index", n.lastIndex),
		zap.Duration("fence-detection-window", fenceCheckInterval),
		zap.String("safety-note", "lease TTLs must exceed fence-detection-window"),
	)

	// One context drives all background S3 work. Cancelling it on Stop aborts
	// any in-flight run-loop S3 call (checkEpoch/confirmRead/syncTail), so the
	// loop returns to its select and services stopc promptly instead of
	// blocking shutdown for the S3 retry budget.
	notifyCtx, cancel := context.WithCancel(context.Background())
	n.notifyCancel = cancel
	n.bgCtx = notifyCtx
	cli.baseCtx = notifyCtx

	go n.run()
	go n.poll()
	go n.runCheckpointer()
	go n.watchLog(notifyCtx)
	return n, nil
}

// claimEpoch CAS-bumps meta/epoch and takes leadership — the s3raft "election".
// This node is leader until a node with a higher epoch announces itself through
// the log. Only voting members (genesis or a promoted/voter join) may claim;
// learners must not (see start()).
func (n *node) claimEpoch() error {
	epoch, err := n.cli.bumpEpoch(n.id)
	if err != nil {
		return err
	}
	n.myEpoch = epoch
	n.bumpMaxTerm(epoch)
	n.lead = n.id
	n.isLeader = true
	n.pendingSoft = &raft.SoftState{Lead: n.id, RaftState: raft.StateLeader}
	return nil
}

// bootIsLearner reports whether this member boots as a learner and therefore
// must not claim the fencing epoch. A WAL restart carries the answer in the
// seeded storage ConfState; a fresh join has an empty ConfState, so fold the
// shared log's configuration changes instead.
func bootIsLearner(id uint64, ms *raft.MemoryStorage, replay []*raftpb.Entry) bool {
	if _, cs, err := ms.InitialState(); err == nil {
		for _, l := range cs.GetLearners() {
			if l == id {
				return true
			}
		}
		for _, v := range cs.GetVoters() {
			if v == id {
				return false
			}
		}
	}
	_, learners := foldMembership(replay)
	_, ok := learners[id]
	return ok
}

// foldMembership replays the shared log's configuration changes into the
// current voter and learner sets, so a joining member can tell whether it is a
// learner before it touches meta/epoch.
func foldMembership(entries []*raftpb.Entry) (voters, learners map[uint64]struct{}) {
	voters = make(map[uint64]struct{})
	learners = make(map[uint64]struct{})
	apply := func(typ raftpb.ConfChangeType, id uint64) {
		switch typ {
		case raftpb.ConfChangeAddNode:
			delete(learners, id)
			voters[id] = struct{}{}
		case raftpb.ConfChangeAddLearnerNode:
			delete(voters, id)
			learners[id] = struct{}{}
		case raftpb.ConfChangeRemoveNode:
			delete(voters, id)
			delete(learners, id)
		}
	}
	for _, e := range entries {
		switch e.GetType() {
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := proto.Unmarshal(e.GetData(), &cc); err != nil {
				continue
			}
			apply(cc.GetType(), cc.GetNodeId())
		case raftpb.EntryConfChangeV2:
			var cc raftpb.ConfChangeV2
			if err := proto.Unmarshal(e.GetData(), &cc); err != nil {
				continue
			}
			for _, s := range cc.GetChanges() {
				apply(s.GetType(), s.GetNodeId())
			}
		}
	}
	return voters, learners
}

// fetchTail lists and fetches all log entries with index > after.
func (n *node) fetchTail(after uint64) ([]*raftpb.Entry, error) {
	keys, err := n.cli.listAfter("log/", logKey(after))
	if err != nil {
		return nil, err
	}
	sort.Strings(keys) // zero-padded keys sort numerically
	var ents []*raftpb.Entry
	for _, k := range keys {
		if _, ok := parseLogKey(k); !ok {
			continue
		}
		body, gerr := n.cli.get(k)
		if gerr != nil {
			return nil, gerr
		}
		batch, derr := decodeEntries(body)
		if derr != nil {
			return nil, fmt.Errorf("s3raft: corrupt log object %s: %w", k, derr)
		}
		ents = append(ents, batch...)
	}
	return ents, nil
}

// committedLogIndex returns the highest committed index in the shared log: the
// HEAD pointer in etagChain mode, or the max published log-object index in
// conditional mode. Zero means the log is empty (genesis).
func (n *node) committedLogIndex() (uint64, error) {
	if n.cli.chainMode() {
		h, err := n.cli.readHead()
		if err != nil {
			return 0, err
		}
		return h.Index, nil
	}
	keys, err := n.cli.listAfter("log/", "")
	if err != nil {
		return 0, err
	}
	var max uint64
	for _, k := range keys {
		if idx, ok := parseLogKey(k); ok && idx > max {
			max = idx
		}
	}
	return max, nil
}

// seedLogFromLocal bootstraps an empty shared log from restored local state at
// `index`: it publishes a bucket snapshot of the restored backend at that index
// (so future members restore from it) and advances the log's committed pointer
// to index. With s3raft the log is the source of truth, but a local snapshot
// restore leaves the backend ahead of an empty log; this reconciles them for a
// genuine fresh cluster (single member, empty log). A non-empty log is rejected
// by the caller instead of seeded, to avoid diverging from agreed history.
func (n *node) seedLogFromLocal(index uint64) error {
	if capturedBackend == nil {
		return fmt.Errorf("backend not captured; cannot bootstrap log from local state")
	}
	if err := n.uploadSnapshot(n.cli, index); err != nil {
		return err
	}
	n.lastCheckpoint = index
	if n.cli.chainMode() {
		if err := n.cli.seedHead(index); err != nil {
			return err
		}
	}
	n.lg.Info("s3raft: bootstrapped shared log from restored local state",
		zap.Uint64("index", index))
	return nil
}

// publishLocalEntries republishes local WAL entries [from,to] into the shared
// log, re-stamped with the genesis epoch, for --force-new-cluster recovery. In
// etagChain mode it first seeds the commit HEAD to from-1 so the first entry
// chains contiguously from that predecessor. Missing objects are the caller's
// responsibility (from must be <= to). Existing objects (errConflict) are left
// as-is, so re-running is idempotent.
func (n *node) publishLocalEntries(ms *raft.MemoryStorage, from, to uint64) error {
	if from > to {
		return nil
	}
	if n.cli.chainMode() && from > 1 {
		if err := n.cli.seedHead(from - 1); err != nil {
			return fmt.Errorf("seed head at %d: %w", from-1, err)
		}
	}
	ents, err := ms.Entries(from, to+1, math.MaxUint64)
	if err != nil {
		return fmt.Errorf("read local entries [%d,%d]: %w", from, to, err)
	}
	for _, e := range ents {
		e.Term = proto.Uint64(genesisEpoch)
		body, merr := encodeEntries([]*raftpb.Entry{e})
		if merr != nil {
			return merr
		}
		if perr := n.cli.appendCAS(e.GetIndex(), e.GetIndex(), body); perr != nil && perr != errConflict {
			return fmt.Errorf("republish entry %d: %w", e.GetIndex(), perr)
		}
	}
	return nil
}

// ingest appends fetched log entries to the pending Ready state, keeping
// the emitted stream contiguous. Called only from run() (or before run()
// starts).
func (n *node) ingest(ents []*raftpb.Entry) {
	for _, ent := range ents {
		idx := ent.GetIndex()
		if idx <= n.lastIndex && idx <= n.stableIndex {
			// already known locally, only needs (re-)commit emission
			if idx > n.committedEmittedThrough() {
				n.pendingCommit = append(n.pendingCommit, ent)
			}
			continue
		}
		if idx != n.lastIndex+1 && idx > n.lastIndex {
			n.lg.Warn("s3raft: gap in log, refetching later",
				zap.Uint64("expected", n.lastIndex+1), zap.Uint64("got", idx))
			return
		}
		if idx <= n.lastIndex {
			continue
		}
		n.lastIndex = idx
		n.pendingEntries = append(n.pendingEntries, ent)
		n.pendingCommit = append(n.pendingCommit, ent)
		n.bumpMaxTerm(ent.GetTerm())
	}
	n.checkFenced()
}

// checkEpoch proactively demotes this node the instant a newer node claims the
// fencing epoch, without waiting to observe that node's first log entry. This
// closes two windows that observing-a-higher-term-entry alone leaves open:
//   - the new leader bumps meta/epoch at boot before it writes its no-op, so
//     its authority is visible in the epoch object earlier than in the log; and
//   - a new leader that bumps the epoch and then stalls/crashes before writing
//     anything would otherwise never fence the old leader, which would keep
//     renewing leases forever.
//
// The detection window is therefore bounded by fenceCheckInterval regardless of
// the new leader's write timing — this is the window lease TTLs must exceed.
func (n *node) checkEpoch() {
	d, err := n.cli.currentEpoch()
	if err != nil {
		n.lg.Warn("s3raft: epoch freshness check failed", zap.Error(err))
		return
	}
	n.bumpMaxTerm(d.Epoch)
	if n.isLeader {
		if d.Epoch > n.myEpoch {
			n.checkFenced() // a newer node claimed the epoch — demote
		}
		return
	}
	// Already a follower: keep the announced leader pinned to the *current*
	// epoch owner. The owner advances on every new member (each bumps the
	// epoch), so a node that fenced to an earlier owner must follow the change
	// or the members never agree on a single leader (WaitLeader would hang).
	// d.Owner == n.id means we still hold the epoch but are reporting follower
	// (a graceful-shutdown transfer announcement) — leave that pendingSoft
	// alone rather than announcing ourselves as our own leader.
	if d.Owner != raft.None && d.Owner != n.id && d.Owner != n.lead {
		n.lead = d.Owner
		n.pendingSoft = &raft.SoftState{Lead: d.Owner, RaftState: raft.StateFollower}
		n.lg.Info("s3raft: follower tracking new epoch owner",
			zap.Uint64("epoch", d.Epoch),
			zap.String("leader", fmt.Sprintf("%x", d.Owner)))
	}
}

// checkFenced demotes this node once any entry carries a term above the
// epoch we claimed at boot: a newer node has taken over leadership. The
// SoftState announcement makes etcdserver demote the lessor and forward
// leader-only work (lease renews, member promotion checks) to the owner.
func (n *node) checkFenced() {
	if !n.isLeader || n.maxTerm <= n.myEpoch {
		return
	}
	n.isLeader = false
	fenceDemotions.Inc()
	lead := raft.None
	if d, err := n.cli.currentEpoch(); err == nil {
		lead = d.Owner
	} else {
		n.lg.Warn("s3raft: could not resolve epoch owner on demotion", zap.Error(err))
	}
	n.lead = lead
	n.pendingSoft = &raft.SoftState{Lead: lead, RaftState: raft.StateFollower}
	n.lg.Info("s3raft: fenced — higher epoch observed, demoting to follower",
		zap.Uint64("my-epoch", n.myEpoch),
		zap.Uint64("observed-term", n.maxTerm),
		zap.String("new-leader", fmt.Sprintf("%x", lead)),
	)
}

func (n *node) committedEmittedThrough() uint64 {
	if len(n.pendingCommit) == 0 {
		return 0
	}
	return n.pendingCommit[len(n.pendingCommit)-1].GetIndex()
}

func (n *node) run() {
	defer close(n.done)
	// awaitingAdvance is set after a Ready is handed out and cleared when etcd
	// calls Advance. While set, no new Ready is offered — but every other
	// operation (crucially confc/statusc) is still serviced. Advance MUST be a
	// select case, not a blocking receive: etcd applies a ConfChange inside the
	// same apply pass that precedes Advance, and that apply calls back into
	// ApplyConfChange (-> confc). Blocking on advancec here would wedge confc
	// and deadlock the whole node.
	awaitingAdvance := false
	fenceTicker := time.NewTicker(fenceCheckInterval)
	defer fenceTicker.Stop()
	heartbeatTicker := time.NewTicker(heartbeatInterval)
	defer heartbeatTicker.Stop()
	for {
		var out chan raft.Ready
		var rd raft.Ready
		if !awaitingAdvance && (n.bootSnap != nil || len(n.pendingEntries) > 0 || len(n.pendingCommit) > 0 || len(n.pendingReads) > 0 || n.pendingSoft != nil || len(n.pendingMsgs) > 0) {
			rd = n.makeReady()
			out = n.readyc
		}

		select {
		case p := <-n.proposec:
			n.handleProposals(p)
		case rctx := <-n.readc:
			// Linearizable read: establish an authoritative read index from the
			// store's commit point, or fail closed. Dropping the ReadState on
			// failure makes the read time out rather than return stale data.
			if idx, ok := n.confirmRead(); ok {
				n.pendingReads = append(n.pendingReads, raft.ReadState{Index: idx, RequestCtx: rctx})
			}
		case req := <-n.confc:
			req.resc <- n.applyConf(req.cc)
		case sc := <-n.statusc:
			sc <- n.status()
		case <-n.pokec:
			n.syncTail()
		case t := <-n.transferc:
			// Graceful-shutdown leadership transfer: announce the transferee as
			// leader so etcdserver's MoveLeader wait loop (for s.Lead() ==
			// transferee) completes at once instead of blocking on ReqTimeout.
			// There is no real transfer — authority is the fencing epoch — but
			// reporting follower lets a leader shut down without stalling.
			n.isLeader = false
			n.lead = t
			n.pendingSoft = &raft.SoftState{Lead: t, RaftState: raft.StateFollower}
		case <-fenceTicker.C:
			n.checkEpoch()
		case <-heartbeatTicker.C:
			n.buildHeartbeats()
		case out <- rd:
			// hand out at most one Ready until the next Advance, like raft does
			n.bootSnap = nil
			n.pendingSoft = nil
			n.pendingEntries = nil
			n.pendingCommit = nil
			n.pendingReads = nil
			n.pendingMsgs = nil
			awaitingAdvance = true
		case <-n.advancec:
			awaitingAdvance = false
		case <-n.stopc:
			return
		}
	}
}

// hardState returns the HardState this node advertises: the highest term seen,
// a self-vote, and the committed index. Shared by makeReady and status. Run
// goroutine only (reads maxTerm without membMu).
func (n *node) hardState() *raftpb.HardState {
	return &raftpb.HardState{
		Term:   proto.Uint64(n.maxTerm),
		Vote:   proto.Uint64(n.id),
		Commit: proto.Uint64(n.lastIndex),
	}
}

func (n *node) makeReady() raft.Ready {
	rd := raft.Ready{
		HardState:        n.hardState(),
		Entries:          n.pendingEntries,
		CommittedEntries: n.pendingCommit,
		ReadStates:       n.pendingReads,
		Messages:         n.pendingMsgs,
		MustSync:         len(n.pendingEntries) > 0,
	}
	rd.SoftState = n.pendingSoft
	if n.bootSnap != nil {
		rd.Snapshot = n.bootSnap
	}
	return rd
}

// buildHeartbeats stages a no-op MsgHeartbeat to every peer (all known members
// except self), so etcdserver relays them over rafthttp and the transport keeps
// marking peers active — the liveness reconfig health gates depend on. Rebuilt
// fresh each tick; a departed member simply stops being pinged. Runs on the run
// goroutine, so it shares n.voters/n.maxTerm with the rest of the loop safely.
func (n *node) buildHeartbeats() {
	msgs := make([]*raftpb.Message, 0, len(n.voters))
	for id := range n.voters {
		if id == n.id {
			continue
		}
		msgs = append(msgs, &raftpb.Message{
			Type: raftpb.MessageType_MsgHeartbeat.Enum(),
			To:   proto.Uint64(id),
			From: proto.Uint64(n.id),
			Term: proto.Uint64(n.maxTerm),
		})
	}
	n.pendingMsgs = msgs
}

// bumpMaxTerm raises maxTerm to t if t is higher. Run-goroutine only; takes
// membMu so the checkpointer's concurrent read cannot tear.
func (n *node) bumpMaxTerm(t uint64) {
	n.membMu.Lock()
	if t > n.maxTerm {
		n.maxTerm = t
	}
	n.membMu.Unlock()
}

// getMaxTerm reads maxTerm under membMu, for the off-run checkpointer.
func (n *node) getMaxTerm() uint64 {
	n.membMu.RLock()
	defer n.membMu.RUnlock()
	return n.maxTerm
}

// voterSet returns a copy of the voter set under membMu, for the off-run
// checkpointer to test membership without racing applyConf.
func (n *node) voterSet() map[uint64]struct{} {
	n.membMu.RLock()
	defer n.membMu.RUnlock()
	m := make(map[uint64]struct{}, len(n.voters))
	for id := range n.voters {
		m[id] = struct{}{}
	}
	return m
}

// sortedVotersLocked returns the voter member IDs sorted ascending. Caller must
// hold membMu (RLock or Lock).
func (n *node) sortedVotersLocked() []uint64 {
	return slices.Sorted(maps.Keys(n.voters))
}

// voterList returns the current voter member IDs (sorted), for stamping
// into checkpoint metadata so a restoring node can build the snapshot's
// ConfState. Falls back to this node alone if membership is not yet known.
// Takes membMu (RLock) because the checkpointer calls it off the run goroutine.
func (n *node) voterList() []uint64 {
	n.membMu.RLock()
	defer n.membMu.RUnlock()
	if len(n.voters) == 0 {
		return []uint64{n.id}
	}
	return n.sortedVotersLocked()
}

// maxBatchEntries caps how many queued proposals coalesce into one log object,
// bounding object size and worst-case per-append latency.
const maxBatchEntries = 256

// handleProposals coalesces the triggering proposal with any others already
// queued on proposec into a single CAS append (one S3 round-trip for the whole
// group), then replies to every proposer with the shared result. This is the
// write-throughput analog of raft batching entries into one Ready: under
// concurrent load N proposals cost one round-trip instead of N. Runs on run().
func (n *node) handleProposals(first proposal) {
	batch := []proposal{first}
drain:
	for len(batch) < maxBatchEntries {
		select {
		case p := <-n.proposec:
			batch = append(batch, p)
		default:
			break drain
		}
	}
	err := n.appendBatch(batch)
	if err == nil && addsMember(batch) {
		// Propagation barrier. A member add is committed the instant its log
		// object is CAS-written, but peers learn of it only by *pulling* the
		// shared log (s3raft sends no raft traffic), so an existing member can
		// still be serving a membership view that predates this add. etcd's
		// MemberAdd returns as soon as *this* node applies the ConfChange, and
		// the caller immediately starts the new member, which validates its
		// config against an existing peer's /members — if that peer has not yet
		// pulled the add, bootstrap fails ("member count is unequal" /
		// "could not retrieve cluster information"). Holding the ConfChange here
		// (before run() emits it as a committed entry, which is what unblocks
		// MemberAdd) gives every peer a full poll interval to catch up first.
		n.clk.Sleep(memberChangePropagationDelay)
	}
	for _, p := range batch {
		if p.resc != nil {
			p.resc <- err
		}
	}
}

// memberChangePropagationDelay bounds how long a member add is held on the
// serving node before MemberAdd returns, so peers pull the change first. It
// exceeds pollInterval so a peer whose change-notification stream is degraded
// still catches up via the poll fallback within the window.
const memberChangePropagationDelay = pollInterval + 500*time.Millisecond

// addsMember reports whether the batch contains a ConfChange that adds a member
// (voter or learner) — the case that a freshly started member's bootstrap must
// see propagated (see the propagation barrier in handleProposals).
func addsMember(batch []proposal) bool {
	adds := func(typ raftpb.ConfChangeType) bool {
		return typ == raftpb.ConfChangeAddNode || typ == raftpb.ConfChangeAddLearnerNode
	}
	for _, p := range batch {
		switch p.typ {
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if proto.Unmarshal(p.data, &cc) == nil && adds(cc.GetType()) {
				return true
			}
		case raftpb.EntryConfChangeV2:
			var cc raftpb.ConfChangeV2
			if proto.Unmarshal(p.data, &cc) == nil {
				for _, s := range cc.GetChanges() {
					if adds(s.GetType()) {
						return true
					}
				}
			}
		}
	}
	return false
}

// appendBatch CAS-appends all proposals as one contiguous run of entries in a
// single log object keyed by the batch's last index. On a lost race it ingests
// the winning tail and retries the whole batch at the new next index.
func (n *node) appendBatch(batch []proposal) error {
	started := time.Now()
	appendBatchEntries.Observe(float64(len(batch)))
	defer func() { appendSeconds.Observe(time.Since(started).Seconds()) }()

	backoff := conflictBaseBackoff
	for attempt := 0; attempt < 64; attempt++ {
		firstIdx := n.lastIndex + 1
		ents := make([]*raftpb.Entry, len(batch))
		for i, p := range batch {
			ents[i] = &raftpb.Entry{
				Term:  proto.Uint64(n.maxTerm),
				Index: proto.Uint64(firstIdx + uint64(i)),
				Type:  p.typ.Enum(),
				Data:  p.data,
			}
		}
		lastIdx := firstIdx + uint64(len(batch)) - 1
		body, err := encodeEntries(ents)
		if err != nil {
			return err
		}
		err = n.cli.appendCAS(firstIdx, lastIdx, body)
		if err == nil {
			n.lastIndex = lastIdx
			n.pendingEntries = append(n.pendingEntries, ents...)
			n.pendingCommit = append(n.pendingCommit, ents...)
			return nil
		}
		if err != errConflict {
			return err
		}
		// Lost the CAS: another writer owns our range. Adopt the winner's
		// entries and retry the whole batch at the next free index.
		casConflicts.Inc()
		tail, ferr := n.fetchTail(n.lastIndex)
		if ferr != nil {
			return ferr
		}
		if len(tail) == 0 {
			// 409 race where the winner's object is not visible yet: back off
			// with jitter, ramping up so a hot multi-writer key does not thrash.
			n.clk.Sleep(backoff + jitter(backoff))
			if backoff *= 2; backoff > conflictMaxBackoff {
				backoff = conflictMaxBackoff
			}
			continue
		}
		n.ingest(tail)
	}
	return fmt.Errorf("s3raft: propose: too many CAS conflicts")
}

// Conflict backoff bounds for the CAS retry loops (appendBatch's lost-race
// loop and retryCAS).
const (
	conflictBaseBackoff = 10 * time.Millisecond
	conflictMaxBackoff  = 500 * time.Millisecond
	// casMaxAttempts caps a single read-modify-CAS before giving up.
	casMaxAttempts = 64
)

// syncTail runs checkTail but no more often than minSyncInterval, so a burst
// of notification wakeups (every cross-node write pokes us) collapses to one
// S3 read per interval instead of saturating the single-threaded run loop with
// blocking I/O and starving proposals. A poke arriving during the cooldown
// re-arms a single deferred poke so no change is missed. Owned by run().
func (n *node) syncTail() {
	if d := n.clk.Since(n.lastSync); d < minSyncInterval {
		if !n.resyncArmed {
			n.resyncArmed = true
			time.AfterFunc(minSyncInterval-d, n.poke)
		}
		return
	}
	n.resyncArmed = false
	n.checkTail()
	n.lastSync = n.clk.Now()
}

// confirmRead establishes an authoritative, linearizable read index by
// consulting the object store directly, rather than trusting this node's
// possibly-stale local view. It returns ok=false — and the caller then drops
// the read so it fails closed — only when that authority cannot be positively
// confirmed: an S3 error/partition leaves us unable to prove freshness.
//
// This is the s3raft analog of raft's ReadIndex, but simpler. Real raft must
// route a linearizable read through the leader, because only the leader knows
// the commit index; a follower has to forward. s3raft externalizes the commit
// point to S3, so *any* reachable node can serve a linearizable read: it proves
// (a) it can still reach the store and (b) advances to the store's true
// committed index, then answers. Leadership (the fencing epoch) governs lease
// authority, not read consistency — a fenced/demoted node is still fully
// consistent with the shared log and may serve reads. That is why a client can
// issue `member list` (a linearizable read) against any member, not just the
// epoch owner. Each read costs a small number of S3 GETs — the price of
// linearizability without a replicated quorum.
func (n *node) confirmRead() (uint64, bool) {
	// Reachability gate: reading the epoch proves we can reach the store
	// (closing the silent-partition stale-read hole) and surfaces whether a
	// newer node has superseded our lease authority. Supersession demotes us
	// for lease purposes but does not deny the read — the commit point is in
	// S3, not in a leader, so we can still answer linearizably below.
	d, err := n.cli.currentEpoch()
	if err != nil {
		readsDenied.Inc()
		n.lg.Warn("s3raft: linearizable read denied — epoch check failed", zap.Error(err))
		return 0, false
	}
	if d.Epoch > n.myEpoch {
		n.bumpMaxTerm(d.Epoch)
		n.checkFenced()
	}
	// Freshness gate: advance local state to the store's authoritative committed
	// tail (including the commit point that lives only in HEAD, in etagChain
	// mode, before the log object is published).
	if err := n.syncToHead(); err != nil {
		readsDenied.Inc()
		n.lg.Warn("s3raft: linearizable read denied — tail sync failed", zap.Error(err))
		return 0, false
	}
	return n.lastIndex, true
}

// syncToHead advances lastIndex to the store's authoritative committed index.
// In etagChain mode the commit point is the HEAD pointer, which is advanced by
// the If-Match CAS *before* the per-index log object is written (client.go); a
// reader that only lists log/ objects would miss a committed-but-not-yet-
// published (or crashed-mid-write) entry. So we read HEAD and, if it is ahead of
// the published tail, recover the missing entries from HEAD.body. If we still
// cannot reach HEAD.Index, we report an error so the read fails closed.
func (n *node) syncToHead() error {
	tail, err := n.fetchTail(n.lastIndex)
	if err != nil {
		return err
	}
	n.ingest(tail)
	if !n.cli.chainMode() {
		return nil // conditional mode: the log object IS the commit point
	}
	h, err := n.cli.readHead()
	if err != nil {
		return err
	}
	if h.Index > n.lastIndex && len(h.Entry) > 0 {
		ents, derr := decodeEntries(h.Entry)
		if derr != nil {
			return derr
		}
		n.ingest(ents)
	}
	if n.lastIndex < h.Index {
		return fmt.Errorf("s3raft: could not reach committed index %d (local tail %d)", h.Index, n.lastIndex)
	}
	return nil
}

// checkTail ingests any entries other writers appended to the shared log.
func (n *node) checkTail() {
	tail, err := n.fetchTail(n.lastIndex)
	if err != nil {
		n.lg.Warn("s3raft: tail check failed", zap.Error(err))
		return
	}
	if len(tail) > 0 {
		n.ingest(tail)
	}
}

func (n *node) poll() {
	t := time.NewTicker(pollInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			n.poke()
		case <-n.done:
			return
		}
	}
}

// poke asks the run loop to re-read the log tail (non-blocking; coalesced).
func (n *node) poke() {
	select {
	case n.pokec <- struct{}{}:
	default:
	}
}

// watchLog reacts to log changes the instant they are notified, instead of
// waiting for the poll fallback — cutting cross-node visibility, fenced-node
// demotion, and forwarded lease-renew latency from ~pollInterval to network
// round-trip. The notifier is pluggable (see LogNotifier); if it is
// unavailable the poll ticker still guarantees eventual progress.
func (n *node) watchLog(ctx context.Context) {
	notifier := notifierFor(n.cli, "log/")
	if err := notifier.Watch(ctx, n.poke); err != nil && ctx.Err() == nil {
		n.lg.Info("s3raft: log-change notifications unavailable; using poll only",
			zap.Error(err))
	}
}

func (n *node) applyConf(cc raftpb.ConfChangeI) *raftpb.ConfState {
	ccv2 := cc.AsV2()
	// Lock across the mutation and the read-back so the off-run checkpointer
	// never observes the map mid-write.
	n.membMu.Lock()
	for _, ch := range ccv2.GetChanges() {
		switch ch.GetType() {
		case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
			n.voters[ch.GetNodeId()] = struct{}{}
		case raftpb.ConfChangeRemoveNode:
			delete(n.voters, ch.GetNodeId())
		}
	}
	ids := n.sortedVotersLocked()
	n.membMu.Unlock()
	return &raftpb.ConfState{Voters: ids}
}

func (n *node) status() raft.Status {
	soft := raft.SoftState{Lead: n.id, RaftState: raft.StateLeader}
	if !n.isLeader {
		// n.lead is kept current by checkEpoch; report it directly rather than
		// paying a live S3 read on every status poll.
		soft = raft.SoftState{Lead: n.lead, RaftState: raft.StateFollower}
	}
	st := raft.Status{
		BasicStatus: raft.BasicStatus{
			ID:        n.id,
			HardState: n.hardState(),
			SoftState: soft,
			Applied:   n.lastIndex,
		},
	}
	// Report every known member in Progress, unconditionally — not gated on
	// being the epoch owner. etcd's learner-promotion readiness check
	// (isLearnerReady) scans this Progress map for the candidate and errors if
	// it is nil (ErrNotLeader) or missing the member (ErrIDNotFound). Progress
	// measures log catch-up, not lease authority: because all members apply the
	// same shared S3 log they are equally caught up, so every node can honestly
	// advertise Match = lastIndex for all members — and a member-promote request
	// served by a fenced (non-owner) node still succeeds. (SoftState above still
	// reflects fencing, so leadership-gated work is unaffected.)
	st.Progress = make(map[uint64]tracker.Progress, len(n.voters)+1)
	st.Progress[n.id] = tracker.Progress{Match: n.lastIndex, Next: n.lastIndex + 1, State: tracker.StateReplicate}
	for id := range n.voters {
		st.Progress[id] = tracker.Progress{Match: n.lastIndex, Next: n.lastIndex + 1, State: tracker.StateReplicate}
	}
	return st
}

// --- raft.Node interface ---

func (n *node) Tick() {}

func (n *node) Campaign(ctx context.Context) error { return nil }

func (n *node) Propose(ctx context.Context, data []byte) error {
	return n.propose(ctx, proposal{typ: raftpb.EntryNormal, data: data, resc: make(chan error, 1)})
}

func (n *node) ProposeConfChange(ctx context.Context, cc raftpb.ConfChangeI) error {
	typ, data, err := raftpb.MarshalConfChange(cc)
	if err != nil {
		return err
	}
	return n.propose(ctx, proposal{typ: typ, data: data, resc: make(chan error, 1)})
}

func (n *node) propose(ctx context.Context, p proposal) error {
	select {
	case n.proposec <- p:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return raft.ErrStopped
	}
	select {
	case err := <-p.resc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return raft.ErrStopped
	}
}

func (n *node) Step(ctx context.Context, m *raftpb.Message) error { return nil }

func (n *node) Ready() <-chan raft.Ready { return n.readyc }

func (n *node) Advance() {
	select {
	case n.advancec <- struct{}{}:
	case <-n.done:
	}
}

func (n *node) ApplyConfChange(cc raftpb.ConfChangeI) *raftpb.ConfState {
	req := confReq{cc: cc, resc: make(chan *raftpb.ConfState, 1)}
	select {
	case n.confc <- req:
		return <-req.resc
	case <-n.done:
		return &raftpb.ConfState{}
	}
}

func (n *node) TransferLeadership(ctx context.Context, lead, transferee uint64) {
	select {
	case n.transferc <- transferee:
	case <-n.stopc:
	case <-ctx.Done():
	}
}

func (n *node) ForgetLeader(ctx context.Context) error { return nil }

func (n *node) ReadIndex(ctx context.Context, rctx []byte) error {
	select {
	case n.readc <- rctx:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return raft.ErrStopped
	}
}

func (n *node) Status() raft.Status {
	sc := make(chan raft.Status, 1)
	select {
	case n.statusc <- sc:
		return <-sc
	case <-n.done:
		return raft.Status{}
	}
}

func (n *node) ReportUnreachable(id uint64) {}

func (n *node) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}

func (n *node) Stop() {
	if n.notifyCancel != nil {
		n.notifyCancel()
	}
	select {
	case n.stopc <- struct{}{}:
	case <-n.done:
		return
	}
	<-n.done
}
