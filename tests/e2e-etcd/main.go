// Command etcd-s3raft is a stock etcd server binary with s3raft installed:
// it blank-imports the reflect installer and delegates to etcd's own main
// (etcdmain.Main). When ETCD_S3LOG_URL is set the installer hijacks etcd's
// raft construction; unset, it is a byte-for-byte-behaving stock etcd.
//
// It exists only so etcd's e2e suite can drive an s3raft-enabled binary
// without editing etcd source. It lives in its own module (see go.mod) so
// etcdmain's dependency tree never enters the root libraft module.
package main

import (
	"os"

	// Installs s3raft into etcd; activated by ETCD_S3LOG_URL.
	_ "github.com/cnuss/libraft/v3/reflect"

	"go.etcd.io/etcd/server/v3/etcdmain"
)

func main() { etcdmain.Main(os.Args) }
