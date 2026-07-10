package v1alpha1_test

import (
	"testing"

	"github.com/cnuss/libraft/v1alpha1"
)

// Node assembly is not implemented yet: Build returns nil until the builder
// grows configuration and assembly in upcoming revisions.
func TestBuildReturnsNilForNow(t *testing.T) {
	if node := v1alpha1.New().Build(); node != nil {
		t.Errorf("Build() = %v, want nil", node)
	}
}
