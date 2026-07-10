// Package v1alpha1 is the current implementation behind the v1.Builder
// interface. The root libraft façade wraps this; callers reaching directly into
// v1alpha1 use it for the concrete struct. Anything here may change between
// alpha revisions — depend on the v1 contract, not these internals.
package v1alpha1

// New returns an unconfigured BuilderImpl. The root libraft.New façade wraps
// this and returns the v1.Builder interface.
func New() *BuilderImpl {
	return &BuilderImpl{}
}

// BuilderImpl is the default Builder implementation. Configuration fields and
// node assembly land in upcoming revisions; until then Build returns nil.
type BuilderImpl struct{}
