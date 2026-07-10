package reflect

import (
	"reflect"
	"testing"

	v3 "github.com/cnuss/libraft/v3"
	"go.etcd.io/etcd/server/v3/etcdserver"
)

// The structs in v3/newraftnode.go are byte-for-byte layout mirrors of etcd's
// unexported raft types; the installer here patches v3.NewRaftNode into etcd's
// newRaftNode, so a reorder or resize on an etcd bump would silently corrupt
// state. These tests live in the installer package (the code that depends on
// the layout) and check the mirrors against the *actually imported* etcd, so
// they track whatever version go.mod resolves — no golden constants for the
// reachable types.

// etcdRaftNodeType returns etcd's real (unexported) raftNode type, reached via
// the exported EtcdServer struct's `r raftNode` field. reflect can read the
// declared type of an unexported field even though it cannot read its value —
// that is the whole anchor. raftNodeConfig (embedded) and toApply (the applyc
// channel's element) hang off it, so one anchor validates all three.
func etcdRaftNodeType(t *testing.T) reflect.Type {
	t.Helper()
	f, ok := reflect.TypeOf(etcdserver.EtcdServer{}).FieldByName("r")
	if !ok {
		t.Fatal("etcdserver.EtcdServer has no field `r`: the reflect anchor for raftNode is gone; " +
			"find another exported type holding a raftNode, or pin the layout by hand")
	}
	return f.Type
}

func TestRaftNodeLayoutMatchesEtcd(t *testing.T) {
	assertSameLayout(t, "raftNode", etcdRaftNodeType(t), reflect.TypeOf(v3.RaftNode{}))
}

// assertSameLayout fails if a and b do not have identical memory layout: same
// size, and for structs the same field count with matching per-field offsets
// and recursively-matching field types. Types that are the same reflect.Type on
// both sides (e.g. *raft.MemoryStorage, shared by mirror and original) short-
// circuit, so the recursion only descends into the renamed mirror structs:
// raftNode → raftNodeConfig (embedded) and raftNode → toApply (applyc chan elem).
func assertSameLayout(t *testing.T, path string, a, b reflect.Type) {
	t.Helper()
	if a == b {
		return
	}
	if a.Kind() != b.Kind() {
		t.Fatalf("%s: kind %s != %s", path, a.Kind(), b.Kind())
	}
	if a.Size() != b.Size() {
		t.Fatalf("%s: size %d != %d (etcd vs mirror)", path, a.Size(), b.Size())
	}
	switch a.Kind() {
	case reflect.Struct:
		if a.NumField() != b.NumField() {
			t.Fatalf("%s: field count %d != %d (etcd vs mirror)", path, a.NumField(), b.NumField())
		}
		for i := 0; i < a.NumField(); i++ {
			fa, fb := a.Field(i), b.Field(i)
			if fa.Offset != fb.Offset {
				t.Fatalf("%s.%s: offset %d != %d (etcd vs mirror)", path, fb.Name, fa.Offset, fb.Offset)
			}
			assertSameLayout(t, path+"."+fb.Name, fa.Type, fb.Type)
		}
	case reflect.Pointer, reflect.Chan, reflect.Slice, reflect.Array:
		assertSameLayout(t, path+"[elem]", a.Elem(), b.Elem())
	}
}

// v3.BootstrappedRaft has no exported reflect anchor (etcd's bootstrappedRaft
// lives only in the unexported bootstrappedServer), so it cannot auto-track like
// the types above. Pin the size and the offsets of the fields NewRaftNode reads;
// a reorder or resize on an etcd bump trips this. Offsets are read by reflect
// (which sees unexported fields), so no field access is needed. Update
// deliberately against etcdserver/bootstrap.go when it fails, then re-verify
// end-to-end.
func TestBootstrappedRaftLayoutPinned(t *testing.T) {
	rt := reflect.TypeOf(v3.BootstrappedRaft{})
	const wantSize = 56
	if got := rt.Size(); got != wantSize {
		t.Fatalf("BootstrappedRaft size = %d, pinned %d (etcd bootstrappedRaft changed?)", got, wantSize)
	}
	wantOffset := map[string]uintptr{
		"heartbeat": 8,  // time.Duration
		"peers":     16, // []raft.Peer
		"config":    40, // *raft.Config
		"storage":   48, // *raft.MemoryStorage
	}
	for name, want := range wantOffset {
		f, ok := rt.FieldByName(name)
		if !ok {
			t.Fatalf("BootstrappedRaft has no field %q (etcd bootstrappedRaft changed?)", name)
		}
		if f.Offset != want {
			t.Fatalf("BootstrappedRaft.%s offset = %d, pinned %d", name, f.Offset, want)
		}
	}
}
