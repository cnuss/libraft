package v1alpha1

import (
	"context"
	"errors"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	v1 "github.com/cnuss/libraft/v1"
	"go.etcd.io/raft/v3"
)

// The terminal Node wraps the builder's lifetime context and a copy of the
// assembled config in a NodeImpl; the underlying raft node is not started.
func TestNodeCarriesContextAndConfig(t *testing.T) {
	b := NewBuilder()
	b.WithID(7)

	n, ok := b.Node().(*NodeImpl)
	if !ok {
		t.Fatalf("Node() is %T, want *NodeImpl", b.Node())
	}
	if n.ctx != b.ctx {
		t.Errorf("node ctx = %v, want the builder's internal context", n.ctx)
	}
	if n.config.ID != 7 {
		t.Errorf("node config.ID = %d, want 7", n.config.ID)
	}
	if n.node != nil {
		t.Errorf("wrapped raft node = %v, want nil before WithPeers/WithoutPeers", n.node)
	}

	// The node holds a copy: builder mutations after Node must not leak in.
	b.WithID(8)
	if n.config.ID != 7 {
		t.Errorf("node config.ID = %d after builder mutation, want still 7", n.config.ID)
	}
}

// WithContext returns the builder for chaining through to the terminal Node.
func TestWithContextChains(t *testing.T) {
	b := NewBuilder()
	if got := b.WithContext(context.Background()); got != b {
		t.Errorf("WithContext() = %v, want the receiver %v", got, b)
	}
	if node := b.WithContext(context.Background()).Node(); node == nil {
		t.Error("Node() = nil, want a node")
	}
}

// New defaults the config logger to io.Discard so an unconfigured node is
// silent.
func TestDefaultLoggerDiscards(t *testing.T) {
	b := NewBuilder()

	dl, ok := b.Logger.(*raft.DefaultLogger)
	if !ok {
		t.Fatalf("Logger is %T, want *raft.DefaultLogger", b.Logger)
	}
	if dl.Writer() != io.Discard {
		t.Errorf("Logger writes to %v, want io.Discard", dl.Writer())
	}
}

// Every raft.Config setter stores its value on the embedded Config and returns
// the receiver for chaining.
func TestConfigSetters(t *testing.T) {
	storage := raft.NewMemoryStorage()
	logger := &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)}
	type traceLogger struct{} // raft.TraceLogger is interface{} without the with_trace build tag

	b := NewBuilder()
	cases := []struct {
		name string
		call func() v1.Builder
		got  func() any
		want any
	}{
		{"WithID", func() v1.Builder { return b.WithID(7) }, func() any { return b.ID }, uint64(7)},
		{"WithElectionTick", func() v1.Builder { return b.WithElectionTick(10) }, func() any { return b.ElectionTick }, 10},
		{"WithHeartbeatTick", func() v1.Builder { return b.WithHeartbeatTick(1) }, func() any { return b.HeartbeatTick }, 1},
		{"WithStorage", func() v1.Builder { return b.WithStorage(storage) }, func() any { return b.Storage }, raft.Storage(storage)},
		{"WithApplied", func() v1.Builder { return b.WithApplied(42) }, func() any { return b.Applied }, uint64(42)},
		{"WithAsyncStorageWrites", func() v1.Builder { return b.WithAsyncStorageWrites(true) }, func() any { return b.AsyncStorageWrites }, true},
		{"WithMaxSizePerMsg", func() v1.Builder { return b.WithMaxSizePerMsg(1 << 20) }, func() any { return b.MaxSizePerMsg }, uint64(1 << 20)},
		{"WithMaxCommittedSizePerReady", func() v1.Builder { return b.WithMaxCommittedSizePerReady(1 << 21) }, func() any { return b.MaxCommittedSizePerReady }, uint64(1 << 21)},
		{"WithMaxUncommittedEntriesSize", func() v1.Builder { return b.WithMaxUncommittedEntriesSize(1 << 22) }, func() any { return b.MaxUncommittedEntriesSize }, uint64(1 << 22)},
		{"WithMaxInflightMsgs", func() v1.Builder { return b.WithMaxInflightMsgs(256) }, func() any { return b.MaxInflightMsgs }, 256},
		{"WithMaxInflightBytes", func() v1.Builder { return b.WithMaxInflightBytes(1 << 23) }, func() any { return b.MaxInflightBytes }, uint64(1 << 23)},
		{"WithCheckQuorum", func() v1.Builder { return b.WithCheckQuorum(true) }, func() any { return b.CheckQuorum }, true},
		{"WithPreVote", func() v1.Builder { return b.WithPreVote(true) }, func() any { return b.PreVote }, true},
		{"WithReadOnlyOption", func() v1.Builder { return b.WithReadOnlyOption(raft.ReadOnlyLeaseBased) }, func() any { return b.ReadOnlyOption }, raft.ReadOnlyLeaseBased},
		{"WithLogger", func() v1.Builder { return b.WithLogger(logger) }, func() any { return b.Logger }, raft.Logger(logger)},
		{"WithDisableProposalForwarding", func() v1.Builder { return b.WithDisableProposalForwarding(true) }, func() any { return b.DisableProposalForwarding }, true},
		{"WithDisableConfChangeValidation", func() v1.Builder { return b.WithDisableConfChangeValidation(true) }, func() any { return b.DisableConfChangeValidation }, true},
		{"WithStepDownOnRemoval", func() v1.Builder { return b.WithStepDownOnRemoval(true) }, func() any { return b.StepDownOnRemoval }, true},
		{"WithTraceLogger", func() v1.Builder { return b.WithTraceLogger(traceLogger{}) }, func() any { return b.TraceLogger }, raft.TraceLogger(traceLogger{})},
	}
	for _, tc := range cases {
		if got := tc.call(); got != v1.Builder(b) {
			t.Errorf("%s did not return the receiver", tc.name)
		}
		if got := tc.got(); got != tc.want {
			t.Errorf("%s stored %v, want %v", tc.name, got, tc.want)
		}
	}
}

// configMu makes concurrent setter calls safe; run under -race this catches a
// missing lock.
func TestConfigSettersConcurrent(t *testing.T) {
	b := NewBuilder()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func(n uint64) {
			defer wg.Done()
			b.WithID(n)
		}(uint64(i + 1))
		go func(n int) {
			defer wg.Done()
			b.WithElectionTick(n)
		}(i + 1)
	}
	wg.Wait()

	if b.ID == 0 || b.ElectionTick == 0 {
		t.Errorf("ID = %d, ElectionTick = %d; want both set", b.ID, b.ElectionTick)
	}
}

// waitDone waits for ctx to finish and returns its cause, failing the test on
// timeout. The propagation crosses a goroutine, so Done is not immediate.
func waitDone(t *testing.T, ctx context.Context) error {
	t.Helper()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-time.After(5 * time.Second):
		t.Fatal("internal context not cancelled")
		return nil
	}
}

// Cancelling the context passed to WithContext propagates its cause to the
// builder's internal context.
func TestWithContextPropagatesCause(t *testing.T) {
	b := NewBuilder()
	ctx, cancel := context.WithCancelCause(context.Background())
	b.WithContext(ctx)

	cause := errors.New("caller gave up")
	cancel(cause)

	if got := waitDone(t, b.ctx); !errors.Is(got, cause) {
		t.Errorf("Cause(b.ctx) = %v, want %v", got, cause)
	}
}

// A plain context.CancelFunc has no explicit cause; context.Canceled is what
// must arrive on the internal context.
func TestWithContextPropagatesCanceled(t *testing.T) {
	b := NewBuilder()
	ctx, cancel := context.WithCancel(context.Background())
	b.WithContext(ctx)

	cancel()

	if got := waitDone(t, b.ctx); !errors.Is(got, context.Canceled) {
		t.Errorf("Cause(b.ctx) = %v, want context.Canceled", got)
	}
}

// Without WithContext the internal context stays live: only the builder's own
// cancel finishes it.
func TestInternalContextLiveUntilOwnCancel(t *testing.T) {
	b := NewBuilder()

	select {
	case <-b.ctx.Done():
		t.Fatal("internal context done before any cancel")
	default:
	}

	cause := errors.New("builder shut down")
	b.cancel(cause)

	if got := waitDone(t, b.ctx); !errors.Is(got, cause) {
		t.Errorf("Cause(b.ctx) = %v, want %v", got, cause)
	}
}
