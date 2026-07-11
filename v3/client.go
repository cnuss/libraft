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

// package v3 is a proof-of-concept replacement for the raft consensus
// algorithm backed by an S3-compatible object store. It relies on S3
// conditional writes (`If-None-Match: *`) to implement compare-and-swap
// appends to a shared log, which provides a total order of proposals
// without leader election or quorum replication: the object store itself is
// the single strongly-consistent authority.
//
// This file contains a minimal, dependency-free S3 REST client (SigV4).
package v3

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

var (
	// errConflict is returned by putIfAbsent when the object already exists
	// (the CAS lost the race).
	errConflict = errors.New("s3raft: conditional write conflict")
	errNotFound = errors.New("s3raft: object not found")
)

type client struct {
	endpoint     *url.URL // scheme://host:port
	bucket       string
	prefix       string
	region       string
	accessKey    string
	secretKey    string
	sessionToken string          // STS/IAM-role temporary credentials (X-Amz-Security-Token)
	sse          string          // server-side encryption: "AES256" or "aws:kms" (empty = off)
	sseKMSKey    string          // KMS key id when sse == "aws:kms"
	baseCtx      context.Context // cancelled on node shutdown to abort in-flight S3 calls
	hc           *http.Client
	clk          clock // wall-clock seam for the retry budget (realClock in prod)

	// etagChain is enabled when the store does not support
	// `If-None-Match: *` create-if-absent (community MinIO, see
	// https://github.com/minio/minio/issues/20346). In this mode the
	// ordering primitive is exact-ETag `If-Match` compare-and-swap on a
	// single HEAD pointer object instead: HEAD holds {index, entry} and
	// every append must transition HEAD from the exact ETag the appender
	// read — atomic, one winner per index. The per-index log object is
	// written after winning; a crash between the two heals because the
	// next appender backfills log/N from HEAD before advancing it.
	etagChain bool
}

// chainMode reports whether the store runs in etag-chain mode (see the
// etagChain field). Exposed as a method so consumers can hold the store
// interface rather than the concrete *client.
func (c *client) chainMode() bool { return c.etagChain }

// headKey is the HEAD pointer object used in etagChain mode.
const headKey = "meta/head"

// head is the JSON content of the HEAD pointer object.
type head struct {
	Index uint64 `json:"index"`
	Entry []byte `json:"entry,omitempty"` // batch body of the object keyed at Index
}

// epochKey is the fencing-epoch object: the s3raft analog of a raft term.
const epochKey = "meta/epoch"

// epochDoc is the JSON content of the epoch object. Owner is the member ID
// of the node that performed the latest bump; it is the cluster's leader
// (lessor primary) until a higher epoch appears.
type epochDoc struct {
	Epoch uint64 `json:"epoch"`
	Owner uint64 `json:"owner"`
}

// newClient parses a URL of the form http(s)://host:port/bucket[/prefix].
// Credentials come from ETCD_S3LOG_ACCESS_KEY / ETCD_S3LOG_SECRET_KEY,
// falling back to AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, falling back to
// minioadmin/minioadmin (the MinIO default).
func newClient(rawurl string) (*client, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, fmt.Errorf("s3raft: bad url %q: %w", rawurl, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("s3raft: unsupported scheme %q (use http/https endpoint of an S3-compatible store)", u.Scheme)
	}
	parts := strings.SplitN(strings.Trim(u.Path, "/"), "/", 2)
	if parts[0] == "" {
		return nil, fmt.Errorf("s3raft: url %q missing bucket path", rawurl)
	}
	cr := awsCredsFromEnv()
	c := &client{
		endpoint:     &url.URL{Scheme: u.Scheme, Host: u.Host},
		bucket:       parts[0],
		region:       cr.region,
		accessKey:    cr.accessKey,
		secretKey:    cr.secretKey,
		sessionToken: cr.sessionToken,
		// Optional server-side encryption for log/snapshot objects.
		sse:       firstEnv("", "ETCD_S3LOG_SSE"),
		sseKMSKey: firstEnv("", "ETCD_S3LOG_SSE_KMS_KEY_ID"),
		hc:        &http.Client{Timeout: 30 * time.Second},
		clk:       realClock{},
	}
	if len(parts) == 2 && parts[1] != "" {
		c.prefix = parts[1] + "/"
	}
	return c, nil
}

// openStore builds an S3 client for rawurl, scopes it to the per-cluster
// namespace ns (empty = the bucket/prefix root), and verifies the store can
// back the log (ensureBucket, which also probes CAS support). Both the node
// (Start) and the checkpointer open the store this one way.
func openStore(rawurl, ns string) (*client, error) {
	cli, err := newClient(rawurl)
	if err != nil {
		return nil, err
	}
	if ns != "" {
		cli.prefix += ns + "/"
	}
	if err := cli.ensureBucket(); err != nil {
		return nil, err
	}
	return cli, nil
}

// firstEnv returns the first non-empty value among the named environment
// variables, or def if none is set.
func firstEnv(def string, keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return def
}

// awsCreds holds the resolved credential + region environment shared by the S3
// client and the SQS notifier, so both derive them one way.
type awsCreds struct {
	accessKey, secretKey, sessionToken, region string
}

// awsCredsFromEnv resolves credentials from ETCD_S3LOG_* then AWS_* then the
// minioadmin default. A session token (from an assumed IAM role — EKS/EC2/IRSA)
// is signed as X-Amz-Security-Token when present.
func awsCredsFromEnv() awsCreds {
	return awsCreds{
		accessKey:    firstEnv("minioadmin", "ETCD_S3LOG_ACCESS_KEY", "AWS_ACCESS_KEY_ID"),
		secretKey:    firstEnv("minioadmin", "ETCD_S3LOG_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"),
		sessionToken: firstEnv("", "ETCD_S3LOG_SESSION_TOKEN", "AWS_SESSION_TOKEN"),
		// AWS_REGION / AWS_DEFAULT_REGION let a standard AWS env work unchanged.
		region: firstEnv("us-east-1", "ETCD_S3LOG_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"),
	}
}

// sigV4Authorization computes the SigV4 signature over canonicalRequest and
// returns the Authorization header value. Shared by the S3 client and the SQS
// notifier; service is "s3" or "sqs".
func sigV4Authorization(accessKey, secretKey, region, service, amzDate, dateStamp, signedHeaders, canonicalRequest string) string {
	scope := dateStamp + "/" + region + "/" + service + "/aws4_request"
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256", amzDate, scope, sha256Hex([]byte(canonicalRequest)),
	}, "\n")
	kDate := hmacSHA256([]byte("AWS4"+secretKey), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))
	return fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		accessKey, scope, signedHeaders, signature)
}

// ensureBucket creates the bucket (tolerating "already exists") and probes
// whether the store supports `If-None-Match: *` create-if-absent. If not,
// it switches to ETag-chain mode and initializes the HEAD pointer.
func (c *client) ensureBucket() error {
	status, _, _, err := c.do(http.MethodPut, "", nil, nil, nil)
	if err != nil {
		return err
	}
	// 200 created, 409 BucketAlreadyOwnedByYou / BucketAlreadyExists
	if status != http.StatusOK && status != http.StatusConflict {
		return fmt.Errorf("s3raft: create bucket: unexpected status %d", status)
	}

	// Probe conditional-write support with a throwaway key.
	probe := c.prefix + "meta/cas-probe"
	status, _, _, err = c.do(http.MethodPut, probe, nil, []byte("probe"), map[string]string{"If-None-Match": "*"})
	if err != nil {
		return err
	}
	switch status {
	case http.StatusOK, http.StatusPreconditionFailed, http.StatusConflict:
		// conditional (If-None-Match:*) writes supported
	case http.StatusNotFound:
		// MinIO community: If-None-Match wants an exact ETag and answers
		// NoSuchKey for a missing object. Fall back to If-Match CAS on a
		// HEAD pointer object.
		c.etagChain = true
		if err := c.initHead(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("s3raft: conditional write probe: unexpected status %d", status)
	}

	// Actively verify the guarantees the log's safety depends on, so a store
	// that silently lacks them fails fast instead of corrupting the log.
	return c.verifyConsistency()
}

// verifyConsistency confirms, at startup, the two properties s3raft's
// correctness rests on: strong read-after-write visibility and atomic
// exact-precondition conditional writes. A store that fails either cannot
// safely back a consensus log, so this returns a hard error rather than
// letting the node run and diverge.
func (c *client) verifyConsistency() error {
	// Per-invocation key: members of one cluster run this concurrently at boot
	// against the shared bucket, so a fixed key would let one member's write
	// clobber another's mid-probe and fail the CAS spuriously.
	key := fmt.Sprintf("meta/consistency-probe-%016x", rand.Uint64())

	// 1. Read-after-write: a freshly written object must be immediately
	//    visible with its exact bytes.
	want := []byte("s3raft-probe")
	if err := c.put(key, want); err != nil {
		return fmt.Errorf("s3raft: consistency probe write: %w", err)
	}
	got, etag, err := c.getWithETag(key)
	if err != nil {
		return fmt.Errorf("s3raft: consistency probe read-after-write: %w", err)
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("s3raft: store violates read-after-write (wrote %q, read %q) — unsafe as a consensus log", want, got)
	}

	// 2. Conditional-write atomicity, tested in whichever mode is active. The
	//    decisive check: a write carrying a STALE precondition must be refused.
	//    If the store accepts it, two racing appenders could both win the same
	//    log index and the total order is broken.
	if c.etagChain {
		if err := c.putIfMatch(key, []byte("cas-1"), etag); err != nil {
			return fmt.Errorf("s3raft: If-Match CAS rejected a fresh etag: %w", err)
		}
		switch err := c.putIfMatch(key, []byte("cas-2"), etag); err {
		case errConflict:
			// correct: the now-stale etag is refused
		case nil:
			return fmt.Errorf("s3raft: store does NOT enforce If-Match CAS (accepted a stale etag) — unsafe as a consensus log")
		default:
			return fmt.Errorf("s3raft: If-Match CAS probe: %w", err)
		}
	} else {
		hdr := map[string]string{"If-None-Match": "*"}
		status, _, _, err := c.do(http.MethodPut, c.prefix+key, nil, []byte("cas-x"), hdr)
		if err != nil {
			return fmt.Errorf("s3raft: If-None-Match probe: %w", err)
		}
		switch status {
		case http.StatusPreconditionFailed, http.StatusConflict:
			// correct: create-if-absent refused an overwrite of the existing key
		case http.StatusOK:
			return fmt.Errorf("s3raft: store does NOT enforce If-None-Match:* (overwrote an existing key) — unsafe as a consensus log")
		default:
			return fmt.Errorf("s3raft: If-None-Match probe: unexpected status %d", status)
		}
	}

	_ = c.del(key) // best-effort cleanup
	return nil
}

// initHead creates the HEAD pointer if it does not exist. Concurrent
// initializers write byte-identical content, so the unconditional PUT race
// is harmless (identical object, identical ETag).
func (c *client) initHead() error {
	if _, _, err := c.getWithETag(headKey); err == nil {
		return nil
	} else if err != errNotFound {
		return err
	}
	body, err := json.Marshal(head{Index: 0})
	if err != nil {
		return err
	}
	return c.put(headKey, body)
}

// appendCAS atomically claims the contiguous index range [firstIdx, lastIdx]
// for body (a batch of marshaled raftpb.Entry records; see batch.go). The log
// object is keyed by lastIdx. Returns errConflict if another writer claimed the
// range first. A single-entry append passes firstIdx == lastIdx.
//
// Conditional mode (AWS S3, LocalStack): one `If-None-Match: *` PUT of the
// log object itself.
//
// ETag-chain mode (MinIO): CAS the HEAD pointer from the exact ETag we
// read. Protocol per append:
//  1. GET HEAD -> {index N, batch E_N}, etag T. If N != firstIdx-1 we are
//     stale or racing.
//  2. Backfill log/N with E_N (idempotent: all writers write identical
//     bytes), healing any predecessor that crashed after step 3.
//  3. PUT HEAD {lastIdx, body} with If-Match: T — the atomic claim.
//  4. Write log/lastIdx (crash here is healed by the next appender's step 2).
func (c *client) appendCAS(firstIdx, lastIdx uint64, body []byte) error {
	if !c.etagChain {
		hdr := map[string]string{"If-None-Match": "*"}
		status, _, _, err := c.do(http.MethodPut, c.prefix+logKey(lastIdx), nil, body, hdr)
		if err != nil {
			return err
		}
		switch status {
		case http.StatusOK:
			return nil
		case http.StatusPreconditionFailed, http.StatusConflict:
			// 412: object exists. 409: concurrent conditional writers, one won.
			// But do() transparently retries transient 5xx/429/network faults, so
			// a PUT that actually committed server-side can resurface here on the
			// retry (the object now exists → 412). Distinguish "our own write won"
			// from "another writer won" by content: a different writer's batch at
			// this index has different bytes. Without this, a committed-then-
			// ambiguously-failed append is misreported as errConflict, the caller
			// re-ingests its own batch and re-proposes it, and non-idempotent ops
			// (txn, lease grant) apply twice — an exactly-once violation.
			if existing, gerr := c.get(logKey(lastIdx)); gerr == nil && bytes.Equal(existing, body) {
				return nil
			}
			return errConflict
		default:
			return fmt.Errorf("s3raft: put %s: unexpected status %d", logKey(lastIdx), status)
		}
	}

	h, etag, err := c.loadHead()
	if err != nil {
		return err
	}
	if h.Index >= firstIdx {
		return errConflict // someone already claimed our range
	}
	if h.Index != firstIdx-1 {
		return errConflict // we are behind; caller re-syncs and retries
	}
	if h.Index > 0 && len(h.Entry) > 0 {
		// heal a predecessor that crashed between HEAD CAS and log write
		if err := c.put(logKey(h.Index), h.Entry); err != nil {
			return err
		}
	}
	newHead, err := json.Marshal(head{Index: lastIdx, Entry: body})
	if err != nil {
		return err
	}
	if err := c.putIfMatch(headKey, newHead, etag); err != nil {
		return err
	}
	// we own the range; publish the log object for readers
	return c.put(logKey(lastIdx), body)
}

// loadHead reads and decodes the HEAD pointer, returning its ETag so callers
// that CAS on it (appendCAS, seedHead) can pass the ETag to putIfMatch.
func (c *client) loadHead() (head, string, error) {
	var h head
	raw, etag, err := c.getWithETag(headKey)
	if err != nil {
		return h, "", err
	}
	if err := json.Unmarshal(raw, &h); err != nil {
		return h, "", fmt.Errorf("s3raft: corrupt HEAD object: %w", err)
	}
	return h, etag, nil
}

// readHead returns the current HEAD pointer (etagChain mode). HEAD is the
// authoritative commit point: a batch is linearizably committed the instant its
// If-Match CAS advances HEAD, which happens before the per-index log object is
// published. Readers must consult HEAD (not just log/ objects) to observe the
// true committed index.
func (c *client) readHead() (head, error) {
	h, _, err := c.loadHead()
	return h, err
}

// seedHead advances the HEAD pointer to index (etagChain mode) when the log is
// being bootstrapped from restored local state — see node.seedLogFromLocal.
// No-op if HEAD already reached index (a peer seeded concurrently).
func (c *client) seedHead(index uint64) error {
	return c.retryCAS(func() error {
		h, etag, err := c.loadHead()
		if err != nil {
			return err
		}
		if h.Index >= index {
			return nil // already at/past index (a peer seeded concurrently)
		}
		body, err := json.Marshal(head{Index: index})
		if err != nil {
			return err
		}
		return c.putIfMatch(headKey, body, etag)
	})
}

// retryCAS runs fn until it stops returning errConflict, backing off with
// jitter between conflicting attempts so concurrent writers to one hot object
// (e.g. every founder bumping meta/epoch at genesis) decorrelate instead of
// thrashing. It gives up after casMaxAttempts.
func (c *client) retryCAS(fn func() error) error {
	backoff := conflictBaseBackoff
	for attempt := 0; attempt < casMaxAttempts; attempt++ {
		err := fn()
		if err != errConflict {
			return err // nil (won), or a real error
		}
		c.clk.Sleep(backoff + jitter(backoff))
		if backoff *= 2; backoff > conflictMaxBackoff {
			backoff = conflictMaxBackoff
		}
	}
	return fmt.Errorf("s3raft: too many CAS conflicts")
}

// bumpEpoch atomically increments the fencing epoch and installs owner as
// the new leader. Returns the claimed epoch. Works in both backend modes:
// exact-ETag If-Match CAS is supported by AWS S3 (since Nov 2024), MinIO
// and LocalStack.
func (c *client) bumpEpoch(owner uint64) (uint64, error) {
	var claimed uint64
	err := c.retryCAS(func() error {
		raw, etag, err := c.getWithETag(epochKey)
		if err == errNotFound {
			// Genesis: unconditional PUT of byte-identical content; racers
			// converge on the same object and settle in the CAS on retry.
			body, merr := json.Marshal(epochDoc{})
			if merr != nil {
				return merr
			}
			if perr := c.put(epochKey, body); perr != nil {
				return perr
			}
			return errConflict // created; re-read and CAS on top
		}
		if err != nil {
			return err
		}
		var d epochDoc
		if err := json.Unmarshal(raw, &d); err != nil {
			return fmt.Errorf("s3raft: corrupt epoch object: %w", err)
		}
		d.Epoch++
		d.Owner = owner
		body, err := json.Marshal(d)
		if err != nil {
			return err
		}
		if err := c.putIfMatch(epochKey, body, etag); err != nil {
			return err // errConflict → retry on top; else propagate
		}
		claimed = d.Epoch
		return nil
	})
	return claimed, err
}

// currentEpoch reads the epoch object without modifying it.
func (c *client) currentEpoch() (epochDoc, error) {
	var d epochDoc
	raw, _, err := c.getWithETag(epochKey)
	if err != nil {
		return d, err
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return d, fmt.Errorf("s3raft: corrupt epoch object: %w", err)
	}
	return d, nil
}

func (c *client) put(key string, body []byte) error {
	status, _, _, err := c.do(http.MethodPut, c.prefix+key, nil, body, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("s3raft: put %s: unexpected status %d", key, status)
	}
	return nil
}

// del removes an object. A missing object is not an error (idempotent).
func (c *client) del(key string) error {
	status, _, _, err := c.do(http.MethodDelete, c.prefix+key, nil, nil, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK && status != http.StatusNoContent && status != http.StatusNotFound {
		return fmt.Errorf("s3raft: delete %s: unexpected status %d", key, status)
	}
	return nil
}

// list returns all object keys (prefix-stripped) under keyPrefix.
func (c *client) list(keyPrefix string) ([]string, error) {
	return c.listAfter(keyPrefix, "")
}

// purgeNamespace deletes every object under this client's namespace prefix
// (log entries, meta/epoch, meta/head, snapshots). It is the destructive core
// of --force-new-cluster recovery: it wipes the shared log so the caller's
// local backend can re-seed it as a fresh genesis. Scoped strictly to c.prefix,
// so it never touches other clusters sharing the same bucket. Returns the count
// deleted.
func (c *client) purgeNamespace() (int, error) {
	keys, err := c.list("")
	if err != nil {
		return 0, err
	}
	for _, k := range keys {
		if derr := c.del(k); derr != nil {
			return 0, derr
		}
	}
	return len(keys), nil
}

// putIfMatch overwrites key only if its current ETag matches (atomic CAS).
func (c *client) putIfMatch(key string, body []byte, etag string) error {
	hdr := map[string]string{"If-Match": etag}
	status, _, _, err := c.do(http.MethodPut, c.prefix+key, nil, body, hdr)
	if err != nil {
		return err
	}
	switch status {
	case http.StatusOK:
		return nil
	case http.StatusPreconditionFailed, http.StatusConflict, http.StatusNotFound:
		// Same committed-then-retried hazard as appendCAS: do() may re-issue this
		// If-Match PUT after it already committed (our write changed the ETag, so
		// the retry's If-Match now fails → 412). Read back and compare: if the
		// stored object is byte-identical to what we wrote, our write won. This
		// keeps a retried epoch bump / head seed from being mistaken for a lost
		// CAS (which would consume an extra epoch or stall the etag chain).
		if existing, gerr := c.get(key); gerr == nil && bytes.Equal(existing, body) {
			return nil
		}
		return errConflict
	default:
		return fmt.Errorf("s3raft: conditional put %s: unexpected status %d", key, status)
	}
}

func (c *client) getWithETag(key string) ([]byte, string, error) {
	status, body, hdr, err := c.do(http.MethodGet, c.prefix+key, nil, nil, nil)
	if err != nil {
		return nil, "", err
	}
	switch status {
	case http.StatusOK:
		return body, hdr.Get("ETag"), nil
	case http.StatusNotFound:
		return nil, "", errNotFound
	default:
		return nil, "", fmt.Errorf("s3raft: get %s: unexpected status %d", key, status)
	}
}

func (c *client) get(key string) ([]byte, error) {
	body, _, err := c.getWithETag(key)
	return body, err
}

type listResult struct {
	Keys        []string `xml:"Contents>Key"`
	IsTruncated bool     `xml:"IsTruncated"`
}

// listAfter returns object keys (prefix-stripped) lexicographically after
// `after` under the given key prefix.
func (c *client) listAfter(keyPrefix, after string) ([]string, error) {
	q := url.Values{}
	q.Set("list-type", "2")
	q.Set("prefix", c.prefix+keyPrefix)
	if after != "" {
		q.Set("start-after", c.prefix+after)
	}
	var out []string
	for {
		status, body, _, err := c.do(http.MethodGet, "", q, nil, nil)
		if err != nil {
			return nil, err
		}
		if status != http.StatusOK {
			return nil, fmt.Errorf("s3raft: list: unexpected status %d", status)
		}
		var lr listResult
		if err := xml.Unmarshal(body, &lr); err != nil {
			return nil, fmt.Errorf("s3raft: list: bad xml: %w", err)
		}
		for _, k := range lr.Keys {
			out = append(out, strings.TrimPrefix(k, c.prefix))
		}
		if !lr.IsTruncated || len(lr.Keys) == 0 {
			return out, nil
		}
		q.Set("start-after", lr.Keys[len(lr.Keys)-1])
	}
}

const (
	s3MaxAttempts       = 5
	s3BaseBackoff       = 50 * time.Millisecond
	s3MaxBackoff        = 2 * time.Second
	s3PerAttemptTimeout = 5 * time.Second
	// s3RetryBudget bounds total wall time across all retries — including each
	// attempt's own execution, not just the inter-attempt sleeps — so a hung
	// store cannot block a caller (notably the single-threaded run loop) past
	// roughly this bound.
	s3RetryBudget = 12 * time.Second
)

// do performs a SigV4-signed request with bounded retries on transient faults —
// network errors, throttling (429/503 SlowDown) and 5xx — using exponential
// backoff with jitter, a per-attempt deadline, and an overall time budget.
// Definitive statuses, including the 409/412 conditional-write outcomes callers
// rely on for CAS, are returned immediately and never retried. key=="" targets
// the bucket itself.
func (c *client) do(method, key string, query url.Values, body []byte, extraHdr map[string]string) (int, []byte, http.Header, error) {
	deadline := c.clk.Now().Add(s3RetryBudget)
	backoff := s3BaseBackoff
	var (
		status   int
		respBody []byte
		hdr      http.Header
		err      error
	)
	for attempt := 0; attempt < s3MaxAttempts; attempt++ {
		if attempt > 0 && !c.clk.Now().Before(deadline) {
			break // budget exhausted before this attempt could start
		}
		status, respBody, hdr, err = c.doOnce(method, key, query, body, extraHdr, deadline)
		if err == nil && !retryableStatus(status) {
			return status, respBody, hdr, nil
		}
		if c.baseCtx != nil && c.baseCtx.Err() != nil {
			break // node is shutting down; stop retrying
		}
		sleep := backoff + jitter(backoff)
		if attempt == s3MaxAttempts-1 || c.clk.Now().Add(sleep).After(deadline) {
			break
		}
		s3Retries.Inc()
		c.clk.Sleep(sleep)
		if backoff *= 2; backoff > s3MaxBackoff {
			backoff = s3MaxBackoff
		}
	}
	return status, respBody, hdr, err
}

// doOnce issues a single signed request under a per-attempt deadline, further
// clamped by budgetDeadline so a hung attempt cannot overrun the overall retry
// budget (see do).
func (c *client) doOnce(method, key string, query url.Values, body []byte, extraHdr map[string]string, budgetDeadline time.Time) (int, []byte, http.Header, error) {
	if method == http.MethodPut && key != "" {
		extraHdr = c.withSSE(extraHdr) // encrypt object writes if configured
	}
	req, err := c.signedRequest(method, key, query, body, extraHdr)
	if err != nil {
		return 0, nil, nil, err
	}
	base := c.baseCtx
	if base == nil {
		base = context.Background()
	}
	timeout := s3PerAttemptTimeout
	if rem := budgetDeadline.Sub(c.clk.Now()); rem < timeout {
		timeout = rem // don't let a single attempt exceed the remaining budget
	}
	ctx, cancel := context.WithTimeout(base, timeout)
	defer cancel()
	started := time.Now()
	resp, err := c.hc.Do(req.WithContext(ctx))
	s3RequestSeconds.WithLabelValues(method).Observe(time.Since(started).Seconds())
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, respBody, resp.Header, nil
}

// retryableStatus reports whether an HTTP status is a transient fault worth
// retrying. It deliberately excludes 409/412 (definitive conditional-write
// outcomes) and all other 4xx.
func retryableStatus(status int) bool {
	switch status {
	case http.StatusInternalServerError, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout,
		http.StatusTooManyRequests:
		return true
	default:
		return false
	}
}

// jitter returns a random duration in [0, d) to decorrelate concurrent retries.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(d)))
}

// withSSE merges server-side-encryption headers into a copy of hdr when SSE is
// configured, leaving the caller's map untouched.
func (c *client) withSSE(hdr map[string]string) map[string]string {
	if c.sse == "" {
		return hdr
	}
	out := make(map[string]string, len(hdr)+2)
	for k, v := range hdr {
		out[k] = v
	}
	out["x-amz-server-side-encryption"] = c.sse
	if c.sse == "aws:kms" && c.sseKMSKey != "" {
		out["x-amz-server-side-encryption-aws-kms-key-id"] = c.sseKMSKey
	}
	return out
}

// signedRequest builds a SigV4-signed *http.Request without executing it, so
// callers that need the streaming response body (e.g. notification listeners)
// can drive it themselves. key=="" targets the bucket itself.
func (c *client) signedRequest(method, key string, query url.Values, body []byte, extraHdr map[string]string) (*http.Request, error) {
	path := "/" + c.bucket
	if key != "" {
		path += "/" + key
	}
	u := *c.endpoint
	u.Path = path
	u.RawQuery = canonicalQuery(query)

	req, err := http.NewRequest(method, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	payloadHash := sha256Hex(body)
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	req.Header.Set("Host", u.Host)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	req.Header.Set("X-Amz-Date", amzDate)
	if c.sessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", c.sessionToken)
	}
	for k, v := range extraHdr {
		req.Header.Set(k, v)
	}

	// Sign host plus every x-amz-* header present (session token, SSE, content
	// hash, date), lowercased and sorted per SigV4. Non-x-amz conditional
	// headers (If-Match/If-None-Match) are sent unsigned, as before.
	signed := map[string]string{"host": u.Host}
	for k := range req.Header {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "x-amz-") {
			signed[lk] = strings.TrimSpace(req.Header.Get(k))
		}
	}
	names := make([]string, 0, len(signed))
	for k := range signed {
		names = append(names, k)
	}
	sort.Strings(names)
	var chb strings.Builder
	for _, k := range names {
		chb.WriteString(k + ":" + signed[k] + "\n")
	}
	canonicalHeaders := chb.String()
	signedHeaders := strings.Join(names, ";")

	canonicalRequest := strings.Join([]string{
		method,
		uriEncodePath(path),
		u.RawQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	req.Header.Set("Authorization", sigV4Authorization(
		c.accessKey, c.secretKey, c.region, "s3", amzDate, dateStamp, signedHeaders, canonicalRequest))
	return req, nil
}

func canonicalQuery(q url.Values) string {
	if len(q) == 0 {
		return ""
	}
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(uriEncode(k) + "=" + uriEncode(q.Get(k)))
	}
	return b.String()
}

// uriEncode implements AWS URI encoding (RFC 3986 unreserved set).
func uriEncode(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
			(ch >= '0' && ch <= '9') || ch == '-' || ch == '.' || ch == '_' || ch == '~' {
			b.WriteByte(ch)
		} else {
			fmt.Fprintf(&b, "%%%02X", ch)
		}
	}
	return b.String()
}

// uriEncodePath encodes each path segment but preserves "/" separators.
func uriEncodePath(p string) string {
	segs := strings.Split(p, "/")
	for i, s := range segs {
		segs[i] = uriEncode(s)
	}
	return strings.Join(segs, "/")
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func hmacSHA256(key []byte, msg string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(msg))
	return m.Sum(nil)
}
