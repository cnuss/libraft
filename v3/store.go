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

import "context"

// store is the S3 CAS surface the consensus core (node), the shared
// checkpoint helpers, and the notifiers depend on. The production
// implementation is *client (client.go); tests substitute an in-memory fake
// (store_test.go). Extracting this seam decouples the raft logic from the
// HTTP/S3 details and makes the core exercisable without a live object store.
//
// Methods stay unexported: this is an internal seam, not a public API, so only
// same-package types (the real client, the test fake) implement it. It captures
// exactly the high-level log/epoch/object operations callers reach through —
// the request plumbing (do/doOnce/signedRequest/withSSE) stays private to
// *client.
type store interface {
	// Log CAS + fencing epoch.
	appendCAS(firstIdx, lastIdx uint64, body []byte) error
	readHead() (head, error)
	seedHead(index uint64) error
	bumpEpoch(owner uint64) (uint64, error)
	currentEpoch() (epochDoc, error)

	// Object primitives.
	get(key string) ([]byte, error)
	put(key string, body []byte) error
	del(key string) error
	list(keyPrefix string) ([]string, error)
	listAfter(keyPrefix, after string) ([]string, error)
	purgeNamespace() (int, error)

	// Mode flag + change notifications.
	chainMode() bool
	listenBucketNotifications(ctx context.Context, keyPrefix string, onEvent func(key string)) error
}

// Compile-time assertion that the production client satisfies the seam.
var _ store = (*client)(nil)
