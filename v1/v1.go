// Package v1 is the stable public surface for libraft. The Builder interface
// here is the contract callers depend on across releases; the implementation
// lives in v1alpha1 and may change between alpha revisions.
package v1

import "go.etcd.io/raft/v3"

// Builder assembles a raft.Node from optional configuration. Configure it with
// the With* methods (each returns the Builder for chaining), then call the
// terminal Build to produce the node. Obtain one from libraft.New.
type Builder interface {
	// Build assembles the configured raft.Node and returns it. It is the
	// terminal step.
	Build() raft.Node
}
