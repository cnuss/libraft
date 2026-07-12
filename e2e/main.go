// Command etcd-libraft is a stock etcd server binary with libraft installed:
// it blank-imports the reflect installer and delegates to etcd's own main
// (etcdmain.Main). When ETCD_S3LOG_URL is set the installer hijacks etcd's
// raft construction; unset, it is a byte-for-byte-behaving stock etcd.
//
// It exists only so etcd's e2e suite can drive a libraft-enabled binary
// without editing etcd source. It shares this module with the e2e harness
// (see go.mod) so etcdmain's dependency tree — and the harness's docker SDK —
// never enter the root libraft module.
//
// The package's _test files are the e2e harness itself: they build each
// example binary and run it, asserting it exits 0 and prints what that
// example demonstrates. Run with: make e2e
package main

import (
	"os"

	// Installs libraft into etcd; activated by ETCD_S3LOG_URL.
	_ "github.com/cnuss/libraft/v3/reflect"

	"go.etcd.io/etcd/server/v3/etcdmain"
)

func main() { etcdmain.Main(os.Args) }
