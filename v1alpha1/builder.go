package v1alpha1

import (
	"log/slog"

	v1 "github.com/cnuss/libraft/v1"
	"go.etcd.io/raft/v3"
)

// WithLog sets the logger the built node logs through. Unset, Build falls back
// to slog.Default.
func (b *BuilderImpl) WithLog(logger *slog.Logger) v1.Builder {
	b.log = logger
	return b
}

// Build assembles the configured raft.Node and returns it. It is the terminal
// step. Node assembly is not implemented yet, so Build currently returns nil.
func (b *BuilderImpl) Build() raft.Node {
	return nil
}
