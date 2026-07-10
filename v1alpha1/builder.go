package v1alpha1

import "go.etcd.io/raft/v3"

// Build assembles the configured raft.Node and returns it. It is the terminal
// step. Node assembly is not implemented yet, so Build currently returns nil.
func (b *BuilderImpl) Build() raft.Node {
	return nil
}
