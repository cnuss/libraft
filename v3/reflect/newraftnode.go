package reflect

import (
	_ "unsafe" // for go:linkname

	// Link the etcdserver package so the //go:linkname pull below resolves;
	// without an importer the symbol is absent at link time.
	_ "go.etcd.io/etcd/server/v3/etcdserver"
)

// etcdNewRaftNode is a //go:linkname handle to the unexported method
// (*bootstrappedRaft).newRaftNode. We never CALL it — init() only takes its
// code address (via reflect) as the patch target. The replacement lives in the
// core: v3.NewRaftNode.
//
//go:linkname etcdNewRaftNode go.etcd.io/etcd/server/v3/etcdserver.(*bootstrappedRaft).newRaftNode
func etcdNewRaftNode()
