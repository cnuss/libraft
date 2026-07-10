// Package v1alpha1 is the current implementation behind the v1.Builder
// interface. The root libraft façade wraps this; callers reaching directly into
// v1alpha1 use it for the concrete struct. Anything here may change between
// alpha revisions — depend on the v1 contract, not these internals.
package v1alpha1

import "log/slog"

// New returns an unconfigured BuilderImpl. The root libraft.New façade wraps
// this and returns the v1.Builder interface.
func New() *BuilderImpl {
	return &BuilderImpl{}
}

// BuilderImpl is the default Builder implementation. Node assembly lands in
// upcoming revisions; until then Build returns nil.
type BuilderImpl struct {
	log *slog.Logger // nil until WithLog is called; Build falls back to slog.Default
}
