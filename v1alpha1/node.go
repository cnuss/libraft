package v1alpha1

import (
	"context"

	v1 "github.com/cnuss/libraft/v1"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// WithPeers starts the underlying raft node, bootstrapping a new cluster from
// the given initial peer set (raft.StartNode). It is a no-op if the node is
// already started. Like raft.StartNode, it panics if the config is invalid or
// peers is empty. It returns the Node for chaining.
func (n *NodeImpl) WithPeers(peers []raft.Peer) v1.Node {
	if n.node != nil {
		return n
	}
	n.node = raft.StartNode(&n.config, peers)

	// Tie the started node to the NodeImpl's lifetime: stop it when ctx is done.
	go func(node raft.Node) {
		<-n.ctx.Done()
		node.Stop()
	}(n.node)

	return n
}

// WithoutPeers restarts the underlying raft node from previously persisted
// state, with no bootstrap peers (raft.RestartNode) — the peer set is
// recovered from storage. It is a no-op if the node is already started. Like
// raft.RestartNode, it panics if the config is invalid. It returns the Node
// for chaining.
func (n *NodeImpl) WithoutPeers() v1.Node {
	if n.node != nil {
		return n
	}
	n.node = raft.RestartNode(&n.config)

	// Tie the restarted node to the NodeImpl's lifetime: stop it when ctx is done.
	go func(node raft.Node) {
		<-n.ctx.Done()
		node.Stop()
	}(n.node)

	return n
}

// The methods below satisfy raft.Node by delegating to the wrapped node; see
// raft.Node for the full semantics of each. The wrapped node is nil until
// WithPeers or WithoutPeers starts it — calling a delegating method before
// then panics.

// Tick increments the node's internal logical clock.
func (n *NodeImpl) Tick() {
	n.node.Tick()
}

// Campaign causes the node to transition to candidate state and start
// campaigning to become leader.
func (n *NodeImpl) Campaign(ctx context.Context) error {
	return n.node.Campaign(ctx)
}

// Propose proposes appending data to the log.
func (n *NodeImpl) Propose(ctx context.Context, data []byte) error {
	return n.node.Propose(ctx, data)
}

// ProposeConfChange proposes a configuration change.
func (n *NodeImpl) ProposeConfChange(ctx context.Context, cc pb.ConfChangeI) error {
	return n.node.ProposeConfChange(ctx, cc)
}

// Step advances the state machine using the given message.
func (n *NodeImpl) Step(ctx context.Context, msg *pb.Message) error {
	return n.node.Step(ctx, msg)
}

// Ready returns the channel that surfaces entries and messages the caller
// must handle.
func (n *NodeImpl) Ready() <-chan raft.Ready {
	return n.node.Ready()
}

// Advance notifies the node that the application has saved progress up to the
// last Ready.
func (n *NodeImpl) Advance() {
	n.node.Advance()
}

// ApplyConfChange applies a config change to the local node.
func (n *NodeImpl) ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState {
	return n.node.ApplyConfChange(cc)
}

// TransferLeadership attempts to transfer leadership to the given transferee.
func (n *NodeImpl) TransferLeadership(ctx context.Context, lead, transferee uint64) {
	n.node.TransferLeadership(ctx, lead, transferee)
}

// ForgetLeader forgets a follower's current leader, changing it to None.
func (n *NodeImpl) ForgetLeader(ctx context.Context) error {
	return n.node.ForgetLeader(ctx)
}

// ReadIndex requests a read state, for linearizable reads.
func (n *NodeImpl) ReadIndex(ctx context.Context, rctx []byte) error {
	return n.node.ReadIndex(ctx, rctx)
}

// Status returns the current status of the raft state machine.
func (n *NodeImpl) Status() raft.Status {
	return n.node.Status()
}

// ReportUnreachable reports that the given node is not reachable for the last
// send.
func (n *NodeImpl) ReportUnreachable(id uint64) {
	n.node.ReportUnreachable(id)
}

// ReportSnapshot reports the status of the sent snapshot.
func (n *NodeImpl) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	n.node.ReportSnapshot(id, status)
}

// Stop performs any necessary termination of the node.
func (n *NodeImpl) Stop() {
	n.node.Stop()
}
