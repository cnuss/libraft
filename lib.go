// Package libraft is a thin, stable façade over stable/alpha versioned packages.
//
// The package is split into three pieces:
//
//   - libraft (this package) — thin façade exposing New. Stable surface for
//     application code.
//   - github.com/cnuss/libraft/v1 — the stable Builder[T] interface and Result
//     type. Application code that wants to declare types against the interface
//     imports this.
//   - github.com/cnuss/libraft/v1alpha1 — the current implementation. Internals
//     (BuilderImpl, helpers) may change between alpha revisions; pin only if
//     you need direct access to the struct.
//
// New[T]() returns a Builder[T] you configure with With* methods and finalize
// with Build().
package libraft

import (
	v1 "github.com/cnuss/libraft/v1"
	"github.com/cnuss/libraft/v1alpha1"
)

// New returns an unconfigured Builder for values of type T. Configure it with
// the With* methods, then call Build.
//
//	res := libraft.New[string]().WithName("greeting").WithValue("hello").Build()
func New[T any]() v1.Builder[T] {
	return v1alpha1.New[T]()
}
