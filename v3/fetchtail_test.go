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

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"go.etcd.io/raft/v3/raftpb"
)

// TestFetchTailConcurrentOrder seeds many log objects and checks fetchTail
// returns every entry exactly once in ascending index order despite fetching
// the objects concurrently. Run under -race, it also guards the parallel
// download against data races. N exceeds fetchLogWorkers so the bounded pool
// actually queues.
func TestFetchTailConcurrentOrder(t *testing.T) {
	ms := newMemStore()
	const N = 50
	for i := 1; i <= N; i++ {
		body, err := encodeEntries([]*raftpb.Entry{{
			Index: proto.Uint64(uint64(i)),
			Term:  proto.Uint64(1),
			Data:  []byte{byte(i)},
		}})
		if err != nil {
			t.Fatalf("encode %d: %v", i, err)
		}
		if err := ms.put(logKey(uint64(i)), body); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	n := &node{cli: ms, lg: zap.NewNop()}
	ents, err := n.fetchTail(0)
	if err != nil {
		t.Fatalf("fetchTail: %v", err)
	}
	if len(ents) != N {
		t.Fatalf("got %d entries, want %d", len(ents), N)
	}
	for i, e := range ents {
		if e.GetIndex() != uint64(i+1) {
			t.Fatalf("entry at position %d has index %d, want %d (order not preserved)", i, e.GetIndex(), i+1)
		}
	}
}
