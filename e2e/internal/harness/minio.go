// Package harness provides the e2e module's reusable test infrastructure.
// It is deliberately a normal (non-main) package so its dependencies — notably
// the docker SDK used to run MinIO — stay off the etcd-libraft binary and out
// of the test files that only need the resulting store.
package harness

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

const minioImage = "minio/minio:latest"

// storeSetupTimeout bounds pulling the image, starting the container, and
// waiting for MinIO to report healthy.
const storeSetupTimeout = 3 * time.Minute

// Store describes an S3-compatible store the e2e examples can target. Exactly
// one of Env/Skip/Err is meaningful:
//
//   - Env set: a store is ready; the vars point an example at it (URL + creds).
//   - Skip set: no store can exist in this environment (skip the caller's test).
//   - Err set: a store should exist but is misconfigured (fail, don't skip) —
//     so CI can't go green by silently losing the libraft legs.
//
// Cleanup, when non-nil, tears the store down and must be called once the tests
// that use the store have finished (typically from TestMain).
type Store struct {
	Env     []string
	Skip    string
	Err     error
	Cleanup func()
}

// StartS3 returns an S3 store for the e2e run. When AWS_REGION is set the
// environment brings its own store (ETCD_S3LOG_URL must point at it and the
// ambient AWS credentials are used as-is); otherwise it starts a throwaway
// MinIO container via the docker SDK. Where no linux-container docker daemon is
// reachable (macOS/windows CI cells) it returns a Store with Skip set.
func StartS3() Store {
	if os.Getenv("AWS_REGION") != "" {
		url := os.Getenv("ETCD_S3LOG_URL")
		if url == "" {
			return Store{Err: fmt.Errorf("AWS_REGION is set but ETCD_S3LOG_URL is not; point it at the store the libraft e2e should use")}
		}
		return Store{Env: []string{"ETCD_S3LOG_URL=" + url}}
	}
	return startMinIO()
}

func startMinIO() Store {
	ctx, cancel := context.WithTimeout(context.Background(), storeSetupTimeout)
	defer cancel()

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return Store{Skip: fmt.Sprintf("no AWS_REGION and no docker client: %v", err)}
	}
	info, err := cli.Info(ctx)
	if err != nil {
		cli.Close()
		return Store{Skip: fmt.Sprintf("no AWS_REGION and docker daemon unreachable: %v", err)}
	}
	if info.OSType != "linux" {
		cli.Close()
		return Store{Skip: fmt.Sprintf("no AWS_REGION and docker daemon runs %s containers; MinIO needs linux", info.OSType)}
	}

	// Daemon is present and can run the image: from here on, failures are real
	// infrastructure errors, not absence — return Err (fail) rather than Skip.
	rc, err := cli.ImagePull(ctx, minioImage, image.PullOptions{})
	if err != nil {
		cli.Close()
		return Store{Err: fmt.Errorf("pull %s: %w", minioImage, err)}
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
		return Store{Err: fmt.Errorf("create minio container: %w", err)}
	}
	cleanup := func() {
		_ = cli.ContainerRemove(context.Background(), created.ID,
			container.RemoveOptions{Force: true, RemoveVolumes: true})
		cli.Close()
	}
	if err := cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return Store{Err: fmt.Errorf("start minio container: %w", err), Cleanup: cleanup}
	}
	inspected, err := cli.ContainerInspect(ctx, created.ID)
	if err != nil {
		return Store{Err: fmt.Errorf("inspect minio container: %w", err), Cleanup: cleanup}
	}
	bindings := inspected.NetworkSettings.Ports[port]
	if len(bindings) == 0 {
		return Store{Err: fmt.Errorf("minio container has no host binding for %s", port), Cleanup: cleanup}
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
			return Store{Err: fmt.Errorf("minio never became healthy at %s: %w", endpoint, ctx.Err()), Cleanup: cleanup}
		case <-time.After(500 * time.Millisecond):
		}
	}

	return Store{
		Env: []string{
			"ETCD_S3LOG_URL=" + endpoint + "/libraft-e2e/basic",
			"AWS_ACCESS_KEY_ID=minioadmin",
			"AWS_SECRET_ACCESS_KEY=minioadmin",
		},
		Cleanup: cleanup,
	}
}
