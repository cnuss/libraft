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
	"io"
	"testing"

	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.uber.org/zap"
)

// fakeSnapshot / fakeBackend implement just the snapshot slice of the bbolt
// backend (backend.Snapshot / backendSnapshotter) so uploadSnapshot can be
// exercised without a real backend, via the snapshotSource seam.
type fakeSnapshot struct{ data []byte }

func (f fakeSnapshot) Size() int64 { return int64(len(f.data)) }
func (f fakeSnapshot) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(f.data)
	return int64(n), err
}
func (f fakeSnapshot) Close() error { return nil }

type fakeBackend struct{ data []byte }

func (f fakeBackend) Snapshot() backend.Snapshot { return fakeSnapshot{data: f.data} }

// TestUploadSnapshotWritesDBAndMeta drives the checkpoint upload path against an
// in-memory store and a fake backend: it must write the snapshot bytes to
// snap/<index>.db and point snap/meta at that index.
func TestUploadSnapshotWritesDBAndMeta(t *testing.T) {
	ms := newMemStore()

	prev := snapshotSource
	snapshotSource = func() backendSnapshotter { return fakeBackend{data: []byte("bolt-bytes")} }
	defer func() { snapshotSource = prev }()

	n := &node{id: 1, lg: zap.NewNop(), voters: map[uint64]struct{}{1: {}}, maxTerm: 7}

	if err := n.uploadSnapshot(ms, 42); err != nil {
		t.Fatalf("uploadSnapshot: %v", err)
	}

	db, err := ms.get(snapDBKey(42))
	if err != nil || string(db) != "bolt-bytes" {
		t.Fatalf("snap db = %q err=%v, want \"bolt-bytes\"/nil", db, err)
	}

	metaB, err := ms.get("snap/meta")
	if err != nil {
		t.Fatalf("snap/meta: %v", err)
	}
	var m checkpointMeta
	if err := json.Unmarshal(metaB, &m); err != nil {
		t.Fatalf("unmarshal meta: %v", err)
	}
	if m.Index != 42 || m.Term != 7 {
		t.Errorf("meta = {index:%d term:%d}, want {42 7}", m.Index, m.Term)
	}
}
