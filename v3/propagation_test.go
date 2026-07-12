// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v3

import (
	"encoding/json"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// The propagation barrier in handleProposals holds the proposing call until
// peers pull the commit whenever addsMember or mutatesAuth reports true. These
// tables pin which proposals trip each detector.

func confChange(t *testing.T, typ raftpb.ConfChangeType) []byte {
	t.Helper()
	b, err := proto.Marshal(&raftpb.ConfChange{Type: typ.Enum()})
	if err != nil {
		t.Fatalf("marshal ConfChange: %v", err)
	}
	return b
}

func internalReq(t *testing.T, r *pb.InternalRaftRequest) []byte {
	t.Helper()
	b, err := proto.Marshal(r)
	if err != nil {
		t.Fatalf("marshal InternalRaftRequest: %v", err)
	}
	return b
}

func TestAddsMember(t *testing.T) {
	tests := []struct {
		name  string
		batch []proposal
		want  bool
	}{
		{"empty", nil, false},
		{"add voter", []proposal{{typ: raftpb.EntryConfChange, data: confChange(t, raftpb.ConfChangeAddNode)}}, true},
		{"add learner", []proposal{{typ: raftpb.EntryConfChange, data: confChange(t, raftpb.ConfChangeAddLearnerNode)}}, true},
		{"remove", []proposal{{typ: raftpb.EntryConfChange, data: confChange(t, raftpb.ConfChangeRemoveNode)}}, false},
		{"update", []proposal{{typ: raftpb.EntryConfChange, data: confChange(t, raftpb.ConfChangeUpdateNode)}}, false},
		{"normal entry", []proposal{{typ: raftpb.EntryNormal, data: []byte("kv")}}, false},
		{"v2 adds", []proposal{{typ: raftpb.EntryConfChangeV2, data: func() []byte {
			b, err := proto.Marshal(&raftpb.ConfChangeV2{Changes: []*raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddNode.Enum()}}})
			if err != nil {
				t.Fatalf("marshal ConfChangeV2: %v", err)
			}
			return b
		}()}}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := addsMember(tc.batch); got != tc.want {
				t.Errorf("addsMember = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMutatesAuth(t *testing.T) {
	tests := []struct {
		name string
		req  *pb.InternalRaftRequest
		want bool
	}{
		{"authenticate", &pb.InternalRaftRequest{Authenticate: &pb.InternalAuthenticateRequest{Name: "root"}}, true},
		{"enable", &pb.InternalRaftRequest{AuthEnable: &pb.AuthEnableRequest{}}, true},
		{"disable", &pb.InternalRaftRequest{AuthDisable: &pb.AuthDisableRequest{}}, true},
		{"user add", &pb.InternalRaftRequest{AuthUserAdd: &pb.AuthUserAddRequest{Name: "u"}}, true},
		{"role grant perm", &pb.InternalRaftRequest{AuthRoleGrantPermission: &pb.AuthRoleGrantPermissionRequest{Name: "r"}}, true},
		// Reads mutate nothing — no peer has to catch up, so no barrier.
		{"user get (read)", &pb.InternalRaftRequest{AuthUserGet: &pb.AuthUserGetRequest{Name: "u"}}, false},
		{"role list (read)", &pb.InternalRaftRequest{AuthRoleList: &pb.AuthRoleListRequest{}}, false},
		{"plain kv put", &pb.InternalRaftRequest{Put: &pb.PutRequest{Key: []byte("k")}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			batch := []proposal{{typ: raftpb.EntryNormal, data: internalReq(t, tc.req)}}
			if got := mutatesAuth(batch); got != tc.want {
				t.Errorf("mutatesAuth = %v, want %v", got, tc.want)
			}
		})
	}

	// A ConfChange payload is not an InternalRaftRequest; mutatesAuth must skip
	// non-normal entries rather than mis-decode them.
	cc := []proposal{{typ: raftpb.EntryConfChange, data: confChange(t, raftpb.ConfChangeAddNode)}}
	if mutatesAuth(cc) {
		t.Error("mutatesAuth(ConfChange batch) = true, want false")
	}
}

// writeMarker publishes an applied marker for id into the store, as
// publishApplied would.
func writeMarker(t *testing.T, ms *memStore, id, applied uint64) {
	t.Helper()
	b, err := json.Marshal(appliedMarker{Applied: applied, UnixNs: 1})
	if err != nil {
		t.Fatalf("marshal marker: %v", err)
	}
	if err := ms.put(memberKey(id), b); err != nil {
		t.Fatalf("put marker: %v", err)
	}
}

func newBarrierNode(id uint64, ms *memStore, clk clock, voters ...uint64) *node {
	vs := map[uint64]struct{}{}
	for _, v := range voters {
		vs[v] = struct{}{}
	}
	return &node{id: id, cli: ms, lg: zap.NewNop(), voters: vs, clk: clk}
}

func TestPeersAppliedAtLeast(t *testing.T) {
	ms := newMemStore()
	n := newBarrierNode(1, ms, &fakeClock{}, 1, 2, 3)

	if n.peersAppliedAtLeast(10) {
		t.Fatal("no markers present: want not-confirmed")
	}
	writeMarker(t, ms, 2, 10)
	if n.peersAppliedAtLeast(10) {
		t.Fatal("only peer 2 present: want not-confirmed (peer 3 missing)")
	}
	writeMarker(t, ms, 3, 9)
	if n.peersAppliedAtLeast(10) {
		t.Fatal("peer 3 behind (9<10): want not-confirmed")
	}
	writeMarker(t, ms, 3, 10)
	if !n.peersAppliedAtLeast(10) {
		t.Fatal("both peers at target: want confirmed")
	}
	// A higher published index still confirms a lower target.
	if !n.peersAppliedAtLeast(5) {
		t.Fatal("both peers past target: want confirmed")
	}
}

func TestPeersAppliedAtLeastSingleMember(t *testing.T) {
	// Only self is a voter: no peer to wait on, so any target is trivially met.
	n := newBarrierNode(1, newMemStore(), &fakeClock{}, 1)
	if !n.peersAppliedAtLeast(42) {
		t.Fatal("single-member cluster: want confirmed with no peers")
	}
}

func TestAwaitPropagationConfirmsWithoutWaiting(t *testing.T) {
	ms := newMemStore()
	fc := &fakeClock{now: time.Unix(0, 0)}
	n := newBarrierNode(1, ms, fc, 1, 2)
	writeMarker(t, ms, 2, 100)

	n.awaitPropagation(100) // peer already caught up
	if len(fc.slept) != 0 {
		t.Fatalf("confirmed immediately: want 0 sleeps, got %v", fc.slept)
	}
}

func TestAwaitPropagationFallsBackToDeadline(t *testing.T) {
	ms := newMemStore()
	fc := &fakeClock{now: time.Unix(0, 0)}
	n := newBarrierNode(1, ms, fc, 1, 2)
	// Peer 2 never reaches the target (simulates a down/lagging member); the
	// barrier must give up at the deadline rather than block forever.
	writeMarker(t, ms, 2, 5)

	n.awaitPropagation(100)

	var total time.Duration
	for _, d := range fc.slept {
		total += d
	}
	if total < propagationDelay {
		t.Fatalf("fallback should wait ~propagationDelay before giving up, waited %v", total)
	}
	// And it must stop near the deadline, not spin far past it.
	if total > propagationDelay+propagationCheckInterval {
		t.Fatalf("overran the deadline: waited %v (ceiling %v)", total, propagationDelay)
	}
}
