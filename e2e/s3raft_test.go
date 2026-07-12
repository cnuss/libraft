package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

// The s3raft e2e legs need a real S3-compatible store. Two ways to get one:
//
//   - AWS_REGION set: the environment brings its own store; ETCD_S3LOG_URL
//     must point at it and the ambient AWS credentials are used as-is.
//   - AWS_REGION unset: the harness starts a throwaway MinIO container via
//     the docker SDK, torn down in TestMain. Where no linux-container docker
//     daemon is reachable (macOS/windows CI cells) the s3raft tests skip.
const minioImage = "minio/minio:latest"

var s3store struct {
	once    sync.Once
	env     []string // extra env for example runs (URL + credentials)
	skip    string   // non-empty: skip s3raft tests with this reason
	err     error    // non-nil: fail s3raft tests (misconfiguration, not absence)
	cleanup func()
}

func TestMain(m *testing.M) {
	code := m.Run()
	if s3store.cleanup != nil {
		s3store.cleanup()
	}
	os.Exit(code)
}

// s3Env returns the env vars that point an example at the S3 store, starting
// MinIO on first use. It skips the calling test when no store can exist here
// and fails it when one should but is misconfigured.
func s3Env(t *testing.T) []string {
	t.Helper()
	s3store.once.Do(setupS3)
	if s3store.err != nil {
		t.Fatalf("s3 store setup: %v", s3store.err)
	}
	if s3store.skip != "" {
		t.Skip(s3store.skip)
	}
	return s3store.env
}

func setupS3() {
	if os.Getenv("AWS_REGION") != "" {
		url := os.Getenv("ETCD_S3LOG_URL")
		if url == "" {
			s3store.err = fmt.Errorf("AWS_REGION is set but ETCD_S3LOG_URL is not; point it at the store the s3raft e2e should use")
			return
		}
		s3store.env = []string{"ETCD_S3LOG_URL=" + url}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		s3store.skip = fmt.Sprintf("no AWS_REGION and no docker client: %v", err)
		return
	}
	info, err := cli.Info(ctx)
	if err != nil {
		cli.Close()
		s3store.skip = fmt.Sprintf("no AWS_REGION and docker daemon unreachable: %v", err)
		return
	}
	if info.OSType != "linux" {
		cli.Close()
		s3store.skip = fmt.Sprintf("no AWS_REGION and docker daemon runs %s containers; MinIO needs linux", info.OSType)
		return
	}

	// Daemon is present and can run the image: from here on, failures are
	// real infrastructure errors, not absence — fail rather than skip so CI
	// can't go green by silently losing the s3raft legs.
	rc, err := cli.ImagePull(ctx, minioImage, image.PullOptions{})
	if err != nil {
		cli.Close()
		s3store.err = fmt.Errorf("pull %s: %w", minioImage, err)
		return
	}
	_, _ = io.Copy(io.Discard, rc) // drain: the pull completes when the stream ends
	rc.Close()

	port := nat.Port("9000/tcp")
	created, err := cli.ContainerCreate(ctx,
		&container.Config{
			Image:        minioImage,
			Cmd:          []string{"server", "/data"},
			Env:          []string{"MINIO_ROOT_USER=minioadmin", "MINIO_ROOT_PASSWORD=minioadmin"},
			ExposedPorts: nat.PortSet{port: struct{}{}},
		},
		&container.HostConfig{
			// Empty HostPort: the daemon picks a free one, so parallel test
			// invocations never collide.
			PortBindings: nat.PortMap{port: []nat.PortBinding{{HostIP: "127.0.0.1"}}},
		},
		nil, nil, "")
	if err != nil {
		cli.Close()
		s3store.err = fmt.Errorf("create minio container: %w", err)
		return
	}
	s3store.cleanup = func() {
		_ = cli.ContainerRemove(context.Background(), created.ID,
			container.RemoveOptions{Force: true, RemoveVolumes: true})
		cli.Close()
	}
	if err := cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		s3store.err = fmt.Errorf("start minio container: %w", err)
		return
	}
	inspected, err := cli.ContainerInspect(ctx, created.ID)
	if err != nil {
		s3store.err = fmt.Errorf("inspect minio container: %w", err)
		return
	}
	bindings := inspected.NetworkSettings.Ports[port]
	if len(bindings) == 0 {
		s3store.err = fmt.Errorf("minio container has no host binding for %s", port)
		return
	}
	endpoint := "http://127.0.0.1:" + bindings[0].HostPort

	for {
		resp, err := http.Get(endpoint + "/minio/health/live")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case <-ctx.Done():
			s3store.err = fmt.Errorf("minio never became healthy at %s: %w", endpoint, ctx.Err())
			return
		case <-time.After(500 * time.Millisecond):
		}
	}

	s3store.env = []string{
		"ETCD_S3LOG_URL=" + endpoint + "/libraft-e2e/basic",
		"AWS_ACCESS_KEY_ID=minioadmin",
		"AWS_SECRET_ACCESS_KEY=minioadmin",
	}
}

// TestS3raftBasic runs the basic example twice against the store: the first
// run writes through s3raft; the second starts from a brand-new data dir and
// must read the value back out of the S3 log alone.
func TestS3raftBasic(t *testing.T) {
	env := s3Env(t)
	r := newRunner(t, "basic")

	out, code := r.run(t, env)
	if code != 0 {
		t.Fatalf("first run exited %d, want 0", code)
	}
	for _, want := range []string{
		"s3raft active",
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
