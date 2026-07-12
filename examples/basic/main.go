// Command basic runs an out-of-the-box embedded etcd with libraft installed.
//
// The blank import of github.com/cnuss/libraft below is the entire
// integration: when ETCD_S3LOG_URL is set it monkey-patches etcd's raft entry
// points at init, so the embedded server stores its raft log in S3 instead of
// running raft. With the variable unset the installer is a no-op and this is
// a plain single-node embedded etcd — safe to run as a smoke test.
//
// The URL is the http(s) endpoint of any S3-compatible store, followed by the
// bucket (lowercase) and an optional prefix:
//
//	export ETCD_S3LOG_URL=https://s3.us-east-1.amazonaws.com/my-bucket/my-prefix
//	export AWS_ACCESS_KEY_ID=…  AWS_SECRET_ACCESS_KEY=…
//	go run .
//
// Or against a local MinIO (credentials default to minioadmin/minioadmin):
//
//	docker run -d -p 9000:9000 minio/minio server /data
//	ETCD_S3LOG_URL=http://127.0.0.1:9000/libraft-demo/basic go run .
//
// Run it twice with ETCD_S3LOG_URL set: the second run starts from a brand-new
// data directory yet reads back the first run's value, restored from S3.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"

	// Installs libraft into etcd; activated by ETCD_S3LOG_URL.
	_ "github.com/cnuss/libraft"
)

func main() {
	// Thin wrapper so every defer in run() (temp-dir cleanup, server/client
	// close) unwinds before the process exits — a bare log.Fatal in the body
	// would skip them.
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	if url := os.Getenv("ETCD_S3LOG_URL"); url != "" {
		fmt.Printf("libraft active: raft log stored at %s\n", url)
	} else {
		fmt.Println("libraft inactive; set ETCD_S3LOG_URL to store the raft log in S3")
	}

	// A fresh data directory every run: with libraft active the server restores
	// its state from the S3 log, so nothing needs to survive locally.
	dir, err := os.MkdirTemp("", "libraft-basic-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	cfg := embed.NewConfig()
	cfg.Dir = dir
	cfg.LogLevel = "fatal" // silence etcd's own logging so the demo output is readable

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return err
	}
	defer e.Close()

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(time.Minute):
		return fmt.Errorf("etcd took too long to start")
	}

	cli := v3client.New(e.Server)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// A value put by a previous run shows up here — with libraft active it was
	// restored from S3, since the data directory above is brand new.
	if prev, err := cli.Get(ctx, "message"); err == nil && len(prev.Kvs) > 0 {
		fmt.Printf("restored from a previous run: message=%q\n", prev.Kvs[0].Value)
	}

	if _, err := cli.Put(ctx, "message", "hello from libraft"); err != nil {
		return err
	}
	resp, err := cli.Get(ctx, "message")
	if err != nil {
		return err
	}
	fmt.Printf("wrote and read back: message=%q\n", resp.Kvs[0].Value)
	return nil
}
