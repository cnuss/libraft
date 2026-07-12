package harness

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Runner builds one example binary, then runs it. The harness builds at test
// time (not via `go build ./...`) so example source changes are always picked
// up — that's why `make e2e` passes -count=1 to defeat the test cache.
type Runner struct {
	name string
	bin  string
}

// NewRunner builds the example named name (from ../examples/<name>, relative to
// the e2e module the caller's test runs in) and returns a Runner for it.
func NewRunner(t *testing.T, name string) *Runner {
	t.Helper()
	bin := filepath.Join(t.TempDir(), name)
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	// Build from inside the example directory: the examples live in the root
	// module, not this one, so a relative package path from here would be
	// rejected as outside the main module.
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = filepath.Join("..", "examples", name)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", name, err, out)
	}
	return &Runner{name: name, bin: bin}
}

// Run executes the built example with extraEnv appended and returns
// (output, exitCode). exitCode is -1 if the process could not be started.
func (r *Runner) Run(t *testing.T, extraEnv []string, args ...string) (string, int) {
	t.Helper()
	cmd := exec.Command(r.bin, args...)
	// Hermetic: strip ETCD_S3LOG_URL so an ambient value (e.g. exported by the
	// etcd-e2e job) can't flip an example into libraft mode behind a test's
	// back. Tests that want libraft mode pass the URL via extraEnv, which is
	// appended after the filter and therefore wins.
	env := os.Environ()
	filtered := env[:0]
	for _, kv := range env {
		if strings.HasPrefix(kv, "ETCD_S3LOG_URL=") {
			continue
		}
		filtered = append(filtered, kv)
	}
	cmd.Env = append(filtered, extraEnv...)
	out, err := cmd.CombinedOutput()
	code := 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else {
			code = -1
		}
	}
	t.Logf("$ %s %s (exit %d)\n%s", r.name, strings.Join(args, " "), code, out)
	return string(out), code
}

// AssertExample builds an example, runs it with no extra env, and checks the
// exit code is 0 and stdout contains want. Each example added under examples/
// should get a row in TestExamples' table.
func AssertExample(t *testing.T, name, want string) {
	t.Helper()
	r := NewRunner(t, name)
	out, code := r.Run(t, nil)
	if code != 0 {
		t.Errorf("%s exited %d, want 0", name, code)
	}
	if !strings.Contains(out, want) {
		t.Errorf("%s output %q does not contain %q", name, out, want)
	}
}
