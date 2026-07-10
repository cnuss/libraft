// Package libraft is a thin, stable façade over stable/alpha versioned packages.
//
// The package is split into three pieces:
//
//   - libraft (this package) — thin façade exposing New. Stable surface for
//     application code.
//   - github.com/cnuss/libraft/v1 — the stable Builder interface. Application
//     code that wants to declare types against the interface imports this.
//   - github.com/cnuss/libraft/v1alpha1 — the current implementation. Internals
//     (BuilderImpl, helpers) may change between alpha revisions; pin only if
//     you need direct access to the struct.
//
// New() returns a Builder you configure with With* methods and finalize with
// Build(), producing a raft.Node.
package libraft

import (
	v1 "github.com/cnuss/libraft/v1"
	"github.com/cnuss/libraft/v1alpha1"
)

// New returns an unconfigured Builder. Configure it with the With* methods,
// then call Build to produce the raft.Node.
func New() v1.Builder {
	return v1alpha1.New()
}
