// Package libraft installs s3raft — etcd's raft consensus replaced by an
// S3-compatible object store — into the importing binary.
//
// Blank-import it from the main package of any binary that embeds or builds
// etcd:
//
//	import _ "github.com/cnuss/libraft"
//
// The installer ([github.com/cnuss/libraft/v3/reflect]) monkey-patches etcd's
// raft entry points at init when the ETCD_S3LOG_URL environment variable is
// set, and is a no-op otherwise. See the README for the install seam and
// v3/LIMITATIONS.md for the behavioral edges.
package libraft

import (
	v3 "github.com/cnuss/libraft/v3"
	_ "github.com/cnuss/libraft/v3/reflect"
)

// EnvURL is the environment variable that activates s3raft. Its value is the
// http(s) endpoint of an S3-compatible store followed by the bucket and an
// optional prefix, e.g. https://s3.us-east-1.amazonaws.com/my-bucket/my-prefix
// (bucket names must be lowercase).
const EnvURL = v3.EnvURL
