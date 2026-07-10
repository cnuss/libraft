package v1alpha1

import (
	"context"

	v1 "github.com/cnuss/libraft/v1"
	"go.etcd.io/raft/v3"
)

// WithContext ties the builder's lifetime to ctx: a goroutine waits for ctx to
// finish and propagates its cause to the builder's internal context. The
// goroutine exits once either context is done, so an unfinished caller context
// does not leak it past the builder's own cancellation.
func (b *BuilderImpl) WithContext(ctx context.Context) v1.Builder {
	go func() {
		select {
		case <-ctx.Done():
			b.cancel(context.Cause(ctx))
		case <-b.ctx.Done():
		}
	}()
	return b
}

// The setters below map one-to-one onto the embedded raft.Config fields (see
// raft.Config for full semantics), each guarded by configMu so concurrent
// configuration is safe.

// WithID sets the identity of the local raft node.
func (b *BuilderImpl) WithID(id uint64) v1.Builder {
	b.configMu.Lock()
	defer b.configMu.Unlock()
	b.ID = id
	return b
}

// WithElectionTick sets the number of Node.Tick invocations between elections.
func (b *BuilderImpl) WithElectionTick(ticks int) v1.Builder {
	b.configMu.Lock()
	defer b.configMu.Unlock()
	b.ElectionTick = ticks
	return b
}

// WithHeartbeatTick sets the number of Node.Tick invocations between
// heartbeats.
func (b *BuilderImpl) WithHeartbeatTick(ticks int) v1.Builder {
	b.configMu.Lock()
	defer b.configMu.Unlock()
	b.HeartbeatTick = ticks
	return b
}

// WithStorage sets the storage the raft node reads persisted entries and
// states from.
func (b *BuilderImpl) WithStorage(storage raft.Storage) v1.Builder {
	b.configMu.Lock()
	defer b.configMu.Unlock()
	b.Storage = storage
	return b
}

// WithApplied sets the last applied index when restarting from a snapshot.
func (b *BuilderImpl) WithApplied(index uint64) v1.Builder {
	b.configMu.Lock()
	defer b.configMu.Unlock()
	b.Applied = index
	return b
}

// WithAsyncStorageWrites toggles asynchronous storage write messages on the
// Ready channel.
func (b *BuilderImpl) WithAsyncStorageWrites(async bool) v1.Builder {
	b.configMu.Lock()
	defer b.configMu.Unlock()
	b.AsyncStorageWrites = async
	return b
}

// WithMaxSizePerMsg caps the byte size of each append message.
func (b *BuilderImpl) WithMaxSizePerMsg(size uint64) v1.Builder {
	b.configMu.Lock()
	defer b.configMu.Unlock()
	b.MaxSizePerMsg = size
	return b
}

// WithMaxCommittedSizePerReady caps the byte size of committed entries
// returned in a single Ready.
func (b *BuilderImpl) WithMaxCommittedSizePerReady(size uint64) v1.Builder {
	b.configMu.Lock()
	defer b.configMu.Unlock()
	b.MaxCommittedSizePerReady = size
	return b
}

// WithMaxUncommittedEntriesSize caps the aggregate byte size of uncommitted
// entries in the leader's log.
func (b *BuilderImpl) WithMaxUncommittedEntriesSize(size uint64) v1.Builder {
	b.configMu.Lock()
	defer b.configMu.Unlock()
	b.MaxUncommittedEntriesSize = size
	return b
}

// WithMaxInflightMsgs caps the number of in-flight append messages during
// optimistic replication.
func (b *BuilderImpl) WithMaxInflightMsgs(count int) v1.Builder {
	b.configMu.Lock()
	defer b.configMu.Unlock()
	b.MaxInflightMsgs = count
	return b
}

// WithMaxInflightBytes caps the aggregate byte size of in-flight append
// messages.
func (b *BuilderImpl) WithMaxInflightBytes(size uint64) v1.Builder {
	b.configMu.Lock()
	defer b.configMu.Unlock()
	b.MaxInflightBytes = size
	return b
}

// WithCheckQuorum makes the leader step down when it cannot reach quorum for
// an election timeout.
func (b *BuilderImpl) WithCheckQuorum(check bool) v1.Builder {
	b.configMu.Lock()
	defer b.configMu.Unlock()
	b.CheckQuorum = check
	return b
}

// WithPreVote enables the Pre-Vote algorithm.
func (b *BuilderImpl) WithPreVote(preVote bool) v1.Builder {
	b.configMu.Lock()
	defer b.configMu.Unlock()
	b.PreVote = preVote
	return b
}

// WithReadOnlyOption sets how read-only requests are served.
func (b *BuilderImpl) WithReadOnlyOption(option raft.ReadOnlyOption) v1.Builder {
	b.configMu.Lock()
	defer b.configMu.Unlock()
	b.ReadOnlyOption = option
	return b
}

// WithLogger sets the logger the built node logs through.
func (b *BuilderImpl) WithLogger(logger raft.Logger) v1.Builder {
	b.configMu.Lock()
	defer b.configMu.Unlock()
	b.Logger = logger
	return b
}

// WithDisableProposalForwarding stops followers from forwarding proposals to
// the leader.
func (b *BuilderImpl) WithDisableProposalForwarding(disable bool) v1.Builder {
	b.configMu.Lock()
	defer b.configMu.Unlock()
	b.DisableProposalForwarding = disable
	return b
}

// WithDisableConfChangeValidation turns off propose-time validation of
// configuration changes.
func (b *BuilderImpl) WithDisableConfChangeValidation(disable bool) v1.Builder {
	b.configMu.Lock()
	defer b.configMu.Unlock()
	b.DisableConfChangeValidation = disable
	return b
}

// WithStepDownOnRemoval makes the leader step down when it is removed or
// demoted.
func (b *BuilderImpl) WithStepDownOnRemoval(stepDown bool) v1.Builder {
	b.configMu.Lock()
	defer b.configMu.Unlock()
	b.StepDownOnRemoval = stepDown
	return b
}

// WithTraceLogger sets the logger used for raft state machine tracing.
func (b *BuilderImpl) WithTraceLogger(logger raft.TraceLogger) v1.Builder {
	b.configMu.Lock()
	defer b.configMu.Unlock()
	b.TraceLogger = logger
	return b
}

// Node assembles the configured node and returns it. It is the terminal step.
// The node carries the builder's lifetime context and a copy of the assembled
// config; the underlying raft node starts when WithPeers or WithoutPeers is
// called on it.
func (b *BuilderImpl) Node() v1.Node {
	b.configMu.Lock()
	defer b.configMu.Unlock()
	return NewNode(b.ctx, b.Config)
}
