// Package v1alpha1 is the current implementation behind the v1.Builder
// interface. The root libraft façade wraps this; callers reaching directly into
// v1alpha1 use it for the concrete struct. Anything here may change between
// alpha revisions — depend on the v1 contract, not these internals.
package v1alpha1

import (
	"context"
	"io"
	"log"
	"sync"

	v1 "github.com/cnuss/libraft/v1"
	"go.etcd.io/raft/v3"
)

// NewBuilder returns an unconfigured BuilderImpl. The root libraft.New façade
// wraps this and returns the v1.Builder interface. The config's logger
// defaults to io.Discard so an unconfigured node is silent; override with
// WithLogger.
func NewBuilder() *BuilderImpl {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &BuilderImpl{
		Config: raft.Config{
			Logger: &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)},
		},
		ctx:    ctx,
		cancel: cancel,
	}
}

// BuilderImpl is the default Builder implementation. The terminal Node wraps
// the assembled config in a NodeImpl; starting the underlying raft node is the
// NodeImpl's job (WithPeers / WithoutPeers).
type BuilderImpl struct {
	// Config is the raft configuration the With* setters populate and Build
	// hands to raft. Guarded by configMu.
	raft.Config

	// configMu guards the embedded Config against concurrent With* calls.
	configMu sync.Mutex

	// ctx is the builder's own lifetime, derived from context.Background at
	// New. WithContext propagates a caller context's cause into it via cancel.
	ctx    context.Context
	cancel context.CancelCauseFunc
}

// NewNode returns a NodeImpl whose lifetime is tied to ctx, carrying the raft
// configuration the underlying node starts with (the builder passes its
// internal context and assembled config here). The wrapped raft node is nil
// until WithPeers or WithoutPeers starts it.
func NewNode(ctx context.Context, config raft.Config) *NodeImpl {
	return &NodeImpl{ctx: ctx, config: config}
}

// NodeImpl is the default Node implementation: the raft.Node the builder
// assembles, wrapped so the v1.Node surface can grow beyond the upstream
// interface. Anything here may change between alpha revisions — depend on the
// v1 contract, not these internals.
type NodeImpl struct {
	node raft.Node

	// config is the raft configuration WithPeers (raft.StartNode) and
	// WithoutPeers (raft.RestartNode) will start the wrapped node with.
	config raft.Config

	// ctx is the node's lifetime, handed in by NewNode: once WithPeers or
	// WithoutPeers starts the wrapped node, it is stopped when ctx is done.
	ctx context.Context
}

var _ v1.Node = (*NodeImpl)(nil)
