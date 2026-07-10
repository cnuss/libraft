// Package v1 is the stable public surface for libraft. The Builder interface
// here is the contract callers depend on across releases; the implementation
// lives in v1alpha1 and may change between alpha revisions.
package v1

import (
	"context"

	"go.etcd.io/raft/v3"
)

// Builder assembles a Node from optional configuration. Configure it with the
// With* methods (each returns the Builder for chaining), then call the
// terminal Node to produce it. Obtain one from libraft.New.
type Builder interface {
	// WithContext ties the builder's lifetime to ctx. The builder runs on its
	// own background-derived context; when ctx is done, its cause is
	// propagated to that internal context. Unset, the internal context is
	// cancelled only by the builder itself.
	WithContext(ctx context.Context) Builder

	// The setters below map one-to-one onto raft.Config fields; see that
	// type's documentation for the full semantics of each.

	// WithID sets the identity of the local raft node. It cannot be 0.
	WithID(id uint64) Builder
	// WithElectionTick sets the number of Node.Tick invocations that must pass
	// between elections. Must be greater than the heartbeat tick.
	WithElectionTick(ticks int) Builder
	// WithHeartbeatTick sets the number of Node.Tick invocations that must
	// pass between heartbeats.
	WithHeartbeatTick(ticks int) Builder
	// WithStorage sets the storage the raft node reads persisted entries and
	// states from.
	WithStorage(storage raft.Storage) Builder
	// WithApplied sets the last applied index, restarting from a snapshot.
	// Only set this when restarting raft.
	WithApplied(index uint64) Builder
	// WithAsyncStorageWrites toggles asynchronous storage write messages on
	// the Ready channel.
	WithAsyncStorageWrites(async bool) Builder
	// WithMaxSizePerMsg caps the byte size of each append message.
	WithMaxSizePerMsg(size uint64) Builder
	// WithMaxCommittedSizePerReady caps the byte size of committed entries
	// returned in a single Ready.
	WithMaxCommittedSizePerReady(size uint64) Builder
	// WithMaxUncommittedEntriesSize caps the aggregate byte size of
	// uncommitted entries in the leader's log.
	WithMaxUncommittedEntriesSize(size uint64) Builder
	// WithMaxInflightMsgs caps the number of in-flight append messages during
	// optimistic replication.
	WithMaxInflightMsgs(count int) Builder
	// WithMaxInflightBytes caps the aggregate byte size of in-flight append
	// messages.
	WithMaxInflightBytes(size uint64) Builder
	// WithCheckQuorum makes the leader step down when it cannot reach quorum
	// for an election timeout.
	WithCheckQuorum(check bool) Builder
	// WithPreVote enables the Pre-Vote algorithm (raft thesis §9.6), reducing
	// disruption from rejoining partitioned nodes.
	WithPreVote(preVote bool) Builder
	// WithReadOnlyOption sets how read-only requests are served (safe via
	// quorum, or lease-based; lease-based requires check quorum).
	WithReadOnlyOption(option raft.ReadOnlyOption) Builder
	// WithLogger sets the logger the built node logs through. Unset, logs are
	// discarded.
	WithLogger(logger raft.Logger) Builder
	// WithDisableProposalForwarding stops followers from forwarding proposals
	// to the leader.
	WithDisableProposalForwarding(disable bool) Builder
	// WithDisableConfChangeValidation turns off propose-time validation of
	// configuration changes. Use with caution; see raft.Config for the
	// invariants this skips.
	WithDisableConfChangeValidation(disable bool) Builder
	// WithStepDownOnRemoval makes the leader step down when it is removed or
	// demoted.
	WithStepDownOnRemoval(stepDown bool) Builder
	// WithTraceLogger sets the logger used for raft state machine tracing.
	WithTraceLogger(logger raft.TraceLogger) Builder

	// Node assembles the configured node and returns it. It is the terminal
	// step.
	Node() Node
}

// Node is the raft node the Builder produces. It wraps raft.Node so the stable
// surface can grow beyond the upstream interface without breaking callers.
type Node interface {
	raft.Node

	// WithPeers starts the underlying raft node, bootstrapping a new cluster
	// from the given initial peer set (raft.StartNode). It is a no-op if the
	// node is already started. It returns the Node for chaining.
	WithPeers(peers []raft.Peer) Node
	// WithoutPeers restarts the underlying raft node from previously
	// persisted state, with no bootstrap peers (raft.RestartNode) — the peer
	// set is recovered from storage. It is a no-op if the node is already
	// started. It returns the Node for chaining.
	WithoutPeers() Node
}
