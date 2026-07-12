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
	"encoding/binary"
	"fmt"

	"google.golang.org/protobuf/proto"

	"go.etcd.io/raft/v3/raftpb"
)

// A log object holds a *batch* of one or more contiguous entries so that many
// concurrent proposals coalesce into a single CAS append (one S3 round-trip for
// the whole group) — the write-throughput analog of raft batching entries into
// one Ready. The object is keyed by the batch's LAST index (see logKey) so a
// `list-after(N)` still returns every batch whose tail is beyond N.
//
// Wire format: a length-prefixed sequence of marshaled raftpb.Entry records,
// each `[uint32 big-endian length][marshaled entry]`. A single-entry batch is
// just one such record — the format subsumes the old one-entry-per-object log.

func encodeEntries(ents []*raftpb.Entry) ([]byte, error) {
	var buf []byte
	var lenhdr [4]byte
	for _, e := range ents {
		b, err := proto.Marshal(e)
		if err != nil {
			return nil, err
		}
		binary.BigEndian.PutUint32(lenhdr[:], uint32(len(b)))
		buf = append(buf, lenhdr[:]...)
		buf = append(buf, b...)
	}
	return buf, nil
}

func decodeEntries(body []byte) ([]*raftpb.Entry, error) {
	var out []*raftpb.Entry
	for len(body) > 0 {
		if len(body) < 4 {
			return nil, fmt.Errorf("libraft: truncated batch header")
		}
		n := binary.BigEndian.Uint32(body[:4])
		body = body[4:]
		if uint32(len(body)) < n {
			return nil, fmt.Errorf("libraft: truncated batch entry (want %d, have %d)", n, len(body))
		}
		e := &raftpb.Entry{}
		if err := proto.Unmarshal(body[:n], e); err != nil {
			return nil, err
		}
		out = append(out, e)
		body = body[n:]
	}
	return out, nil
}
