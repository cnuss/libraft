package v1alpha1

import (
	"context"
	"errors"
	"io"
	"log"
	"slices"
	"testing"
	"time"

	v1 "github.com/cnuss/libraft/v1"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// testNodeConfig returns the smallest config raft's validation accepts, with
// logging discarded.
func testNodeConfig() raft.Config {
	return raft.Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         raft.NewMemoryStorage(),
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,
		Logger:          &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)},
	}
}

// NewNode carries the given context as the node's lifetime and the config the
// node will start with; the wrapped raft node stays nil until WithPeers or
// WithoutPeers starts it.
func TestNewNode(t *testing.T) {
	ctx := context.Background()
	n := NewNode(ctx, raft.Config{ID: 7})

	if n.ctx != ctx {
		t.Errorf("ctx = %v, want the context passed to NewNode", n.ctx)
	}
	if n.config.ID != 7 {
		t.Errorf("config.ID = %d, want 7", n.config.ID)
	}
	if n.node != nil {
		t.Errorf("node = %v, want nil before WithPeers/WithoutPeers", n.node)
	}
}

// WithPeers bootstraps and starts the underlying raft node and returns the
// receiver for chaining.
func TestWithPeersStartsNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // stops the node

	n := NewNode(ctx, testNodeConfig())
	if got := n.WithPeers([]raft.Peer{{ID: 1}}); got != v1.Node(n) {
		t.Errorf("WithPeers() = %v, want the receiver %v", got, n)
	}
	if n.node == nil {
		t.Fatal("wrapped raft node not started")
	}
	if got := n.Status().ID; got != 1 {
		t.Errorf("Status().ID = %d, want 1", got)
	}
}

// WithoutPeers restarts the underlying raft node from storage and returns the
// receiver for chaining.
func TestWithoutPeersRestartsNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // stops the node

	n := NewNode(ctx, testNodeConfig())
	if got := n.WithoutPeers(); got != v1.Node(n) {
		t.Errorf("WithoutPeers() = %v, want the receiver %v", got, n)
	}
	if n.node == nil {
		t.Fatal("wrapped raft node not started")
	}
	if got := n.Status().ID; got != 1 {
		t.Errorf("Status().ID = %d, want 1", got)
	}
}

// A second start call is a no-op: the already-started node is kept.
func TestStartIsNoOpWhenStarted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // stops the node

	n := NewNode(ctx, testNodeConfig())
	n.WithPeers([]raft.Peer{{ID: 1}})

	started := n.node
	n.WithPeers([]raft.Peer{{ID: 1}})
	n.WithoutPeers()
	if n.node != started {
		t.Error("second start call replaced the running node")
	}
}

// Cancelling the lifetime context stops the underlying raft node: calls reject
// with raft.ErrStopped once the stop lands.
func TestCtxCancelStopsNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	n := NewNode(ctx, testNodeConfig())
	n.WithPeers([]raft.Peer{{ID: 1}})
	cancel()

	deadline := time.After(5 * time.Second)
	for {
		if err := n.Campaign(context.Background()); errors.Is(err, raft.ErrStopped) {
			return
		}
		select {
		case <-deadline:
			t.Fatal("node not stopped after context cancel")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// fakeRaftNode records which raft.Node methods were called and returns the
// canned err so the delegation test can watch values pass through both ways.
type fakeRaftNode struct {
	calls []string
	err   error
}

var _ raft.Node = (*fakeRaftNode)(nil)

func (f *fakeRaftNode) record(name string) { f.calls = append(f.calls, name) }

func (f *fakeRaftNode) Tick()                          { f.record("Tick") }
func (f *fakeRaftNode) Campaign(context.Context) error { f.record("Campaign"); return f.err }
func (f *fakeRaftNode) Propose(context.Context, []byte) error {
	f.record("Propose")
	return f.err
}
func (f *fakeRaftNode) ProposeConfChange(context.Context, pb.ConfChangeI) error {
	f.record("ProposeConfChange")
	return f.err
}
func (f *fakeRaftNode) Step(context.Context, *pb.Message) error { f.record("Step"); return f.err }
func (f *fakeRaftNode) Ready() <-chan raft.Ready                { f.record("Ready"); return nil }
func (f *fakeRaftNode) Advance()                                { f.record("Advance") }
func (f *fakeRaftNode) ApplyConfChange(pb.ConfChangeI) *pb.ConfState {
	f.record("ApplyConfChange")
	return &pb.ConfState{}
}
func (f *fakeRaftNode) TransferLeadership(context.Context, uint64, uint64) {
	f.record("TransferLeadership")
}
func (f *fakeRaftNode) ForgetLeader(context.Context) error { f.record("ForgetLeader"); return f.err }
func (f *fakeRaftNode) ReadIndex(context.Context, []byte) error {
	f.record("ReadIndex")
	return f.err
}
func (f *fakeRaftNode) Status() raft.Status      { f.record("Status"); return raft.Status{} }
func (f *fakeRaftNode) ReportUnreachable(uint64) { f.record("ReportUnreachable") }
func (f *fakeRaftNode) ReportSnapshot(uint64, raft.SnapshotStatus) {
	f.record("ReportSnapshot")
}
func (f *fakeRaftNode) Stop() { f.record("Stop") }

// Every raft.Node method on NodeImpl delegates to the wrapped node, passing
// return values through.
func TestRaftNodeDelegation(t *testing.T) {
	wantErr := errors.New("passthrough")
	fake := &fakeRaftNode{err: wantErr}
	n := &NodeImpl{node: fake}
	ctx := context.Background()

	cases := []struct {
		name string
		call func() error // nil result for void methods
	}{
		{"Tick", func() error { n.Tick(); return wantErr }},
		{"Campaign", func() error { return n.Campaign(ctx) }},
		{"Propose", func() error { return n.Propose(ctx, []byte("data")) }},
		{"ProposeConfChange", func() error { return n.ProposeConfChange(ctx, &pb.ConfChange{}) }},
		{"Step", func() error { return n.Step(ctx, &pb.Message{}) }},
		{"Ready", func() error { n.Ready(); return wantErr }},
		{"Advance", func() error { n.Advance(); return wantErr }},
		{"ApplyConfChange", func() error {
			if n.ApplyConfChange(&pb.ConfChange{}) == nil {
				return errors.New("ApplyConfChange returned nil ConfState")
			}
			return wantErr
		}},
		{"TransferLeadership", func() error { n.TransferLeadership(ctx, 1, 2); return wantErr }},
		{"ForgetLeader", func() error { return n.ForgetLeader(ctx) }},
		{"ReadIndex", func() error { return n.ReadIndex(ctx, []byte("rctx")) }},
		{"Status", func() error { n.Status(); return wantErr }},
		{"ReportUnreachable", func() error { n.ReportUnreachable(1); return wantErr }},
		{"ReportSnapshot", func() error { n.ReportSnapshot(1, raft.SnapshotFinish); return wantErr }},
		{"Stop", func() error { n.Stop(); return wantErr }},
	}
	for _, tc := range cases {
		if err := tc.call(); !errors.Is(err, wantErr) {
			t.Errorf("%s error = %v, want %v", tc.name, err, wantErr)
		}
		if !slices.Contains(fake.calls, tc.name) {
			t.Errorf("%s did not reach the wrapped node", tc.name)
		}
	}
	if len(fake.calls) != len(cases) {
		t.Errorf("wrapped node saw %d calls, want %d", len(fake.calls), len(cases))
	}
}
