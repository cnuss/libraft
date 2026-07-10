// NewRaftNode is the s3raft replacement for etcd's unexported
// (*bootstrappedRaft).newRaftNode — the sole raft-construction site. The
// installer (v3/reflect) monkey-patches newRaftNode to jump here; this file
// holds the reconstruction because it is core logic (building the raft.Node
// from the S3 log), not part of the patch mechanism.
//
// newRaftNode returns the unexported type *raftNode built off the unexported
// receiver *bootstrappedRaft, so no typed replacement is expressible. The
// structs below are byte-identical layout mirrors of etcd v3.7.0
// (etcdserver/raft.go + bootstrap.go); etcd calls its OWN methods on the
// returned pointer, so only the memory layout must match. An etcd bump can
// silently shift these offsets — keep them pinned to the required version.

package v3

import (
	"fmt"
	"os"
	"sync"
	"time"
	"unsafe"

	"go.uber.org/zap"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/contention"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	serverstorage "go.etcd.io/etcd/server/v3/storage"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// --- layout mirrors of etcd's unexported types (v3.7.0) ---
// Field order and types MUST match etcdserver/raft.go + bootstrap.go exactly;
// etcd reads these at compile-time offsets from its own definitions.

type bootstrappedRaft struct {
	lg        *zap.Logger
	heartbeat time.Duration
	peers     []raft.Peer
	config    *raft.Config
	storage   *raft.MemoryStorage
}

type raftNodeConfig struct {
	lg          *zap.Logger
	isIDRemoved func(id uint64) bool
	raft.Node
	raftStorage *raft.MemoryStorage
	storage     serverstorage.Storage
	heartbeat   time.Duration
	transport   rafthttp.Transporter
}

type toApply struct {
	entries       []*raftpb.Entry
	snapshot      *raftpb.Snapshot
	notifyc       chan struct{}
	raftAdvancedC <-chan struct{}
}

type raftNode struct {
	lg           *zap.Logger
	tickMu       *sync.RWMutex
	latestTickTs time.Time
	raftNodeConfig
	msgSnapC   chan *raftpb.Message
	applyc     chan toApply
	readStateC chan raft.ReadState
	ticker     *time.Ticker
	td         *contention.TimeoutDetector
	stopped    chan struct{}
	done       chan struct{}
}

const maxInFlightMsgSnap = 16

// NewRaftNode replaces (*bootstrappedRaft).newRaftNode. It mirrors etcd's body
// but builds the raft.Node from the S3 log via Start (instead of
// raft.StartNode/RestartNode). The arguments arrive as opaque pointer words so
// the signature is expressible without etcd's unexported types; the real ABI is
// receiver + 3 pointer args → 1 pointer result, all word-sized, so the register
// layout lines up. This call site — unlike the raft.StartNode seam — carries the
// *membership.RaftCluster, whose ID() is the etcd cluster ID.
func NewRaftNode(bp, ssp, walp, clp unsafe.Pointer) unsafe.Pointer {
	b := (*bootstrappedRaft)(bp)
	ss := (*snap.Snapshotter)(ssp)
	w := (*wal.WAL)(walp)
	cl := (*membership.RaftCluster)(clp)

	// The etcd cluster ID, unavailable to the StartNode seam. Logged for now; a
	// productionized version would thread it into ActiveNS as the namespace key.
	Logger().Info("s3raft: newRaftNode has cluster ID",
		zap.String("cluster-id", cl.ID().String()),
		zap.String("active-ns", ActiveNS))

	n, err := Start(Logger(), os.Getenv(EnvURL), b.config.ID, ActiveNS, b.peers, b.storage)
	if err != nil {
		panic(fmt.Sprintf("s3raft: start: %v", err))
	}

	r := &raftNode{
		lg:           b.lg,
		tickMu:       new(sync.RWMutex),
		latestTickTs: time.Now(),
		raftNodeConfig: raftNodeConfig{
			lg:          b.lg,
			isIDRemoved: func(id uint64) bool { return cl.IsIDRemoved(types.ID(id)) },
			Node:        n,
			heartbeat:   b.heartbeat,
			raftStorage: b.storage,
			storage:     serverstorage.NewStorage(b.lg, w, ss),
		},
		readStateC: make(chan raft.ReadState, 1),
		msgSnapC:   make(chan *raftpb.Message, maxInFlightMsgSnap),
		applyc:     make(chan toApply),
		stopped:    make(chan struct{}),
		done:       make(chan struct{}),
	}
	if b.heartbeat == 0 {
		r.ticker = &time.Ticker{}
	} else {
		r.ticker = time.NewTicker(b.heartbeat)
	}
	r.td = contention.NewTimeoutDetector(2 * b.heartbeat)
	return unsafe.Pointer(r)
}
