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

//go:build linearizability

// This is the empirical companion to the TLA+ model (tla/S3RaftCAS.tla): it
// hammers the real appendCAS code from many concurrent writers against a live
// S3-compatible store and then checks the resulting log is a valid total order.
// The TLA+ model proves the protocol correct under all interleavings; this test
// proves the Go implementation and the actual store honor it. Full
// Jepsen-against-real-AWS (fault injection, clock skew) remains the external
// step this does not cover.
//
// Run against a running MinIO:
//   ETCD_S3RAFT_MINIO_URL=http://localhost:9000/lintest \
//     go test -tags linearizability -run TestCASLinearizability \
//     -v ./server/etcdserver/s3raft/

package v3

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"
	"testing"
	"time"
)

// appendOne appends one opaque payload at the current log tail using the same
// read-tail / CAS / retry-on-conflict protocol the node uses, returning the log
// index it won. It exercises appendCAS (etagChain or conditional) directly.
func appendOne(cli *client, body []byte) (uint64, error) {
	for attempt := 0; attempt < 500; attempt++ {
		next, err := tailIndex(cli)
		if err != nil {
			time.Sleep(5 * time.Millisecond)
			continue // transient read error; re-read and retry
		}
		next++
		switch err := cli.appendCAS(next, next, body); err {
		case nil:
			return next, nil
		case errConflict:
			continue // lost the race; re-read tail and retry
		default:
			// Ambiguous transient error: the CAS PUT may have committed before
			// the response was lost. Blindly retrying would append our body a
			// second time at a later index (a false duplicate). So check whether
			// our write actually landed at `next` before retrying.
			if idx, ok := landedAt(cli, next, body); ok {
				return idx, nil
			}
			time.Sleep(5 * time.Millisecond)
			continue
		}
	}
	return 0, fmt.Errorf("appendOne: too many conflicts")
}

// landedAt reports whether body is the committed entry at index idx (used to
// disambiguate a lost response from a genuinely-failed append).
func landedAt(cli *client, idx uint64, body []byte) (uint64, bool) {
	if cli.etagChain {
		if h, err := cli.readHead(); err == nil && h.Index == idx && bytes.Equal(h.Entry, body) {
			_ = cli.put(logKey(idx), body) // heal the log object the lost PUT skipped
			return idx, true
		}
		return 0, false
	}
	if b, err := cli.get(logKey(idx)); err == nil && bytes.Equal(b, body) {
		return idx, true
	}
	return 0, false
}

// tailIndex returns the current last committed index.
func tailIndex(cli *client) (uint64, error) {
	if cli.etagChain {
		h, err := cli.readHead()
		if err != nil {
			return 0, err
		}
		return h.Index, nil
	}
	keys, err := cli.listAfter("log/", "")
	if err != nil {
		return 0, err
	}
	var max uint64
	for _, k := range keys {
		if idx, ok := parseLogKey(k); ok && idx > max {
			max = idx
		}
	}
	return max, nil
}

func TestCASLinearizability(t *testing.T) {
	url := os.Getenv("ETCD_S3RAFT_MINIO_URL")
	if url == "" {
		t.Skip("set ETCD_S3RAFT_MINIO_URL (e.g. http://localhost:9000/lintest) to run")
	}

	cli, err := newClient(url)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	// Disable HTTP keep-alives: under heavy concurrency the reuse of an idle
	// connection the server has just closed surfaces as a spurious transport
	// error. That is an HTTP-client artifact, not a store-consistency property;
	// eliminating it keeps this test focused on ordering. (Real S3 fault
	// tolerance — throttling, retries — is separate client hardening.)
	cli.hc = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 30 * time.Second}
	// Isolate this run in its own namespace so reruns don't collide.
	cli.prefix += fmt.Sprintf("lin-%d/", time.Now().UnixNano())
	if err := cli.ensureBucket(); err != nil {
		t.Fatalf("ensureBucket (also runs the consistency probe): %v", err)
	}
	t.Logf("mode: etagChain=%v", cli.etagChain)

	const (
		writers   = 8
		perWriter = 25
	)

	type ack struct {
		idx  uint64
		body []byte
	}
	var (
		mu   sync.Mutex
		acks []ack
		wg   sync.WaitGroup
	)

	start := time.Now()
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				// Unique, self-describing payload: (writer, seq). If the store
				// ever lets two writers win one index, the collision surfaces
				// as a mismatched/duplicated body below.
				body := []byte(fmt.Sprintf("w%02d-seq%03d", w, i))
				idx, err := appendOne(cli, body)
				if err != nil {
					t.Errorf("writer %d append %d: %v", w, i, err)
					return
				}
				mu.Lock()
				acks = append(acks, ack{idx: idx, body: body})
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	total := writers * perWriter
	if len(acks) != total {
		t.Fatalf("expected %d acked appends, got %d", total, len(acks))
	}
	t.Logf("%d concurrent appends in %v (%.0f/s)", total, elapsed, float64(total)/elapsed.Seconds())

	// Read back the whole log.
	keys, err := cli.listAfter("log/", "")
	if err != nil {
		t.Fatalf("list log: %v", err)
	}
	logMap := make(map[uint64][]byte)
	for _, k := range keys {
		idx, ok := parseLogKey(k)
		if !ok {
			continue
		}
		b, gerr := cli.get(k)
		if gerr != nil {
			t.Fatalf("get %s: %v", k, gerr)
		}
		logMap[idx] = b
	}

	// Property 1 — no acked commit was lost or overwritten with a different
	// body. Two writers winning the same index would trip this.
	for _, a := range acks {
		got, ok := logMap[a.idx]
		if !ok {
			t.Errorf("acked index %d missing from log", a.idx)
			continue
		}
		if !bytes.Equal(got, a.body) {
			t.Errorf("index %d: acked %q but log holds %q (two winners / lost commit)", a.idx, a.body, got)
		}
	}

	// Property 2 — every distinct payload appears at exactly one index, and no
	// index holds two payloads (map guarantees the latter). Detects duplication.
	seen := make(map[string]uint64)
	for idx, body := range logMap {
		s := string(body)
		if prev, dup := seen[s]; dup {
			t.Errorf("payload %q at two indices: %d and %d", s, prev, idx)
		}
		seen[s] = idx
	}

	// Property 3 — indices form a gapless prefix. Every append advances the tail
	// by exactly one, so the union of committed indices must be 1..N contiguous
	// (index 0 is the genesis HEAD; real entries start at 1).
	idxs := make([]uint64, 0, len(logMap))
	for idx := range logMap {
		idxs = append(idxs, idx)
	}
	sort.Slice(idxs, func(i, j int) bool { return idxs[i] < idxs[j] })
	for i, idx := range idxs {
		if want := uint64(i + 1); idx != want {
			t.Errorf("gap/duplication in log: position %d has index %d, want %d", i, idx, want)
			break
		}
	}
	if uint64(len(idxs)) != uint64(total) {
		t.Errorf("expected %d distinct log indices, got %d", total, len(idxs))
	}

	t.Logf("linearizability OK: %d appends, %d contiguous indices, no lost/duplicated/diverged entries", total, len(idxs))
}
