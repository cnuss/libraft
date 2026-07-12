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
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"testing"

	"go.uber.org/zap"
)

// memStore is an in-memory store implementation for tests: it emulates the
// conditional-write ("If-None-Match: *") mode of the real S3 client — one
// winner per log index — plus a monotonic fencing epoch. It lets the consensus
// core be exercised without a live object store, which is the point of the
// store seam (see store.go).
type memStore struct {
	mu    sync.Mutex
	objs  map[string][]byte
	epoch epochDoc
}

func newMemStore() *memStore {
	return &memStore{objs: map[string][]byte{}}
}

func (m *memStore) chainMode() bool { return false }

func (m *memStore) appendCAS(firstIdx, lastIdx uint64, body []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := logKey(lastIdx)
	if _, ok := m.objs[k]; ok {
		return errConflict // index already claimed
	}
	m.objs[k] = append([]byte(nil), body...)
	return nil
}

func (m *memStore) bumpEpoch(owner uint64) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.epoch.Epoch++
	m.epoch.Owner = owner
	return m.epoch.Epoch, nil
}

func (m *memStore) currentEpoch() (epochDoc, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.epoch, nil
}

func (m *memStore) get(key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objs[key]
	if !ok {
		return nil, errNotFound
	}
	return append([]byte(nil), b...), nil
}

func (m *memStore) getOnce(key string) ([]byte, error) { return m.get(key) }

func (m *memStore) put(key string, body []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[key] = append([]byte(nil), body...)
	return nil
}

func (m *memStore) del(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objs, key)
	return nil
}

func (m *memStore) list(keyPrefix string) ([]string, error) {
	return m.listAfter(keyPrefix, "")
}

func (m *memStore) listAfter(keyPrefix, after string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for k := range m.objs {
		if strings.HasPrefix(k, keyPrefix) && k > after {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (m *memStore) purgeNamespace() (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := len(m.objs)
	m.objs = map[string][]byte{}
	return n, nil
}

func (m *memStore) readHead() (head, error) {
	b, err := m.get(headKey)
	if err != nil {
		return head{}, err
	}
	var h head
	if err := json.Unmarshal(b, &h); err != nil {
		return head{}, err
	}
	return h, nil
}

func (m *memStore) seedHead(index uint64) error {
	b, err := json.Marshal(head{Index: index})
	if err != nil {
		return err
	}
	return m.put(headKey, b)
}

func (m *memStore) listenBucketNotifications(ctx context.Context, keyPrefix string, onEvent func(key string)) error {
	<-ctx.Done() // no push events in the fake; block until shutdown like the real one
	return nil
}

// TestEpochFencingWithMemStore exercises the fencing path — claimEpoch ->
// startFenceRead -> applyEpoch -> checkFenced — against the in-memory store. Two nodes share one
// store: the second to claim wins a higher epoch, and the first must demote to
// follower and track the new owner on its next epoch check.
func TestEpochFencingWithMemStore(t *testing.T) {
	ms := newMemStore()
	a := &node{id: 1, cli: ms, lg: zap.NewNop(), voters: map[uint64]struct{}{},
		stopc: make(chan struct{}), readResultC: make(chan readResult, 1)}
	b := &node{id: 2, cli: ms, lg: zap.NewNop(), voters: map[uint64]struct{}{}}

	if err := a.claimEpoch(); err != nil {
		t.Fatalf("a.claimEpoch: %v", err)
	}
	if !a.isLeader || a.myEpoch != 1 {
		t.Fatalf("A after claim: isLeader=%v myEpoch=%d, want true/1", a.isLeader, a.myEpoch)
	}

	if err := b.claimEpoch(); err != nil {
		t.Fatalf("b.claimEpoch: %v", err)
	}
	if !b.isLeader || b.myEpoch != 2 {
		t.Fatalf("B after claim: isLeader=%v myEpoch=%d, want true/2", b.isLeader, b.myEpoch)
	}

	// A re-confirms its epoch off the loop and must find it superseded → demote
	// to follower, tracking B (owner of epoch 2) as leader.
	a.startFenceRead()
	a.applyReadResult(<-a.readResultC)
	if a.isLeader {
		t.Errorf("A still leader after B claimed a higher epoch; want demoted")
	}
	if a.lead != b.id {
		t.Errorf("A.lead = %d after fencing, want %d (the new epoch owner)", a.lead, b.id)
	}
}

// TestMemStoreAppendCASOneWinner locks in the fake's core invariant: exactly one
// append can claim a given log index.
func TestMemStoreAppendCASOneWinner(t *testing.T) {
	ms := newMemStore()
	if err := ms.appendCAS(1, 1, []byte("first")); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if err := ms.appendCAS(1, 1, []byte("second")); err != errConflict {
		t.Fatalf("second append at same index: err=%v, want errConflict", err)
	}
	got, err := ms.get(logKey(1))
	if err != nil || string(got) != "first" {
		t.Fatalf("stored=%q err=%v, want \"first\"/nil", got, err)
	}
}
