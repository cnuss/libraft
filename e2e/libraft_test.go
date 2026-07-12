package main

import (
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/cnuss/libraft/e2e/internal/harness"
)

// The libraft e2e legs need a real S3-compatible store; harness.StartS3 brings
// one up (ambient AWS or a throwaway MinIO container) — see that package.
var (
	s3store harness.Store
	s3once  sync.Once
)

func TestMain(m *testing.M) {
	code := m.Run()
	if s3store.Cleanup != nil {
		s3store.Cleanup()
	}
	os.Exit(code)
}

// s3Env returns the env vars that point an example at the S3 store, standing it
// up on first use. It skips the calling test when no store can exist here and
// fails it when one should but is misconfigured.
func s3Env(t *testing.T) []string {
	t.Helper()
	s3once.Do(func() { s3store = harness.StartS3() })
	if s3store.Err != nil {
		t.Fatalf("s3 store setup: %v", s3store.Err)
	}
	if s3store.Skip != "" {
		t.Skip(s3store.Skip)
	}
	return s3store.Env
}

// TestLibraftBasic runs the basic example twice against the store: the first
// run writes through libraft; the second starts from a brand-new data dir and
// must read the value back out of the S3 log alone.
func TestLibraftBasic(t *testing.T) {
	env := s3Env(t)
	r := newRunner(t, "basic")

	out, code := r.run(t, env)
	if code != 0 {
		t.Fatalf("first run exited %d, want 0", code)
	}
	for _, want := range []string{
		"libraft active",
		`wrote and read back: message="hello from libraft"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("first run output does not contain %q", want)
		}
	}

	out, code = r.run(t, env)
	if code != 0 {
		t.Fatalf("second run exited %d, want 0", code)
	}
	for _, want := range []string{
		`restored from a previous run: message="hello from libraft"`,
		`wrote and read back: message="hello from libraft"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("second run output does not contain %q", want)
		}
	}
}
