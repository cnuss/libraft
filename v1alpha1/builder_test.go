package v1alpha1_test

import (
	"io"
	"log/slog"
	"testing"

	"github.com/cnuss/libraft/v1alpha1"
)

// Node assembly is not implemented yet: Build returns nil until the builder
// grows assembly in upcoming revisions.
func TestBuildReturnsNilForNow(t *testing.T) {
	if node := v1alpha1.New().Build(); node != nil {
		t.Errorf("Build() = %v, want nil", node)
	}
}

// WithLog returns the builder for chaining through to the terminal Build.
func TestWithLogChains(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	b := v1alpha1.New()
	if got := b.WithLog(logger); got != b {
		t.Errorf("WithLog() = %v, want the receiver %v", got, b)
	}
	if node := b.WithLog(logger).Build(); node != nil {
		t.Errorf("Build() = %v, want nil", node)
	}
}
