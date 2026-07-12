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
	"testing"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/raft/v3/raftpb"
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
