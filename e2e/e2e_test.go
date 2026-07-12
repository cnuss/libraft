package main

import (
	"os"
	"strings"
	"testing"

	"github.com/cnuss/libraft/e2e/internal/harness"
)

// The libraft e2e legs need a real S3-compatible store; harness.S3Env brings
// one up (ambient AWS or a throwaway MinIO container) and Cleanup tears it down.
func TestMain(m *testing.M) {
	code := m.Run()
	harness.Cleanup()
	os.Exit(code)
}

func TestExamples(t *testing.T) {
	cases := []struct {
		name string
		want string
	}{
		{"basic", `wrote and read back: message="hello from libraft"`},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			harness.AssertExample(t, tc.name, tc.want)
		})
	}
}

// TestRestartRecoversStateFromS3Log runs the basic example twice against the
// store: the first run writes through libraft; the second starts from a
// brand-new data dir (local disk wiped) and must reconstruct the value from the
// S3 log alone — exercising durability/recovery across a restart.
func TestRestartRecoversStateFromS3Log(t *testing.T) {
	env := harness.S3Env(t)
	r := harness.NewRunner(t, "basic")

	out, code := r.Run(t, env)
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

	out, code = r.Run(t, env)
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
