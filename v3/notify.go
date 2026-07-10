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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// LogNotifier delivers "the shared log changed" signals so the node can sync
// its tail immediately instead of waiting for the poll fallback. This is the
// seam that decouples *where* change notifications come from:
//
//   - minioNotifier (default): MinIO's ListenBucketNotification streaming GET,
//     for local/self-hosted clusters.
//   - An SQS-backed implementation long-polling a queue fed by S3 Event
//     Notifications, for AWS.
//   - A Lambda entrypoint subscribed to S3 object-created events that holds a
//     node reference and calls wake() per delivered event — no long-poll at
//     all.
//
// Watch blocks until ctx is cancelled, invoking wake once per detected change
// (coalescing is fine: the node re-reads the whole tail). It returns an error
// if the source is unavailable, and the node falls back to polling.
type LogNotifier interface {
	Watch(ctx context.Context, wake func()) error
}

// customNotifier, when set via SetNotifier, replaces the default MinIO
// streaming notifier — e.g. an SQS-backed or Lambda-push implementation.
var customNotifier LogNotifier

// SetNotifier installs a custom log-change notifier. Call before the node
// starts (e.g. from a deployment's init). A nil argument restores the default.
func SetNotifier(n LogNotifier) { customNotifier = n }

// notifierFor returns the notifier the node should use for its log prefix, in
// priority order: an injected custom notifier, else an SQS notifier if a queue
// is configured (the AWS pull path), else the default MinIO streaming notifier.
func notifierFor(cli *client, keyPrefix string) LogNotifier {
	if customNotifier != nil {
		return customNotifier
	}
	if sqs := newSQSNotifierFromEnv(); sqs != nil {
		return sqs
	}
	return &minioNotifier{cli: cli, keyPrefix: keyPrefix}
}

// streamHTTPClient has no timeout: notification listens are long-lived and
// kept alive by the store's periodic ping, not by request deadlines.
var streamHTTPClient = &http.Client{}

// notifyReconnectDelay is the pause before re-establishing a dropped stream.
const notifyReconnectDelay = 500 * time.Millisecond

// minioNotifier implements LogNotifier over MinIO's ListenBucketNotification
// extension.
type minioNotifier struct {
	cli       *client
	keyPrefix string
}

// Watch streams object-creation events and calls wake for each new log object.
// The first connection failure is returned so the node can fall back to
// polling; once connected, dropped streams are reconnected until ctx is done.
func (m *minioNotifier) Watch(ctx context.Context, wake func()) error {
	connectedOnce := false
	for {
		err := m.cli.listenBucketNotifications(ctx, m.keyPrefix, func(key string) {
			if _, ok := parseLogKey(key); ok {
				wake()
			}
		})
		if ctx.Err() != nil {
			return nil
		}
		if !connectedOnce && err != nil {
			return err // source unsupported/unreachable: caller falls back
		}
		connectedOnce = true
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(notifyReconnectDelay):
		}
	}
}

// PushNotifier is a LogNotifier fed by inbound S3 event deliveries instead of
// polling — the shape a Lambda deployment uses, where object notifications
// arrive as an HTTP POST. Wire its ServeHTTP onto the "POST /s3" route and
// install it with SetNotifier before the node starts:
//
//	pn := &s3raft.PushNotifier{}
//	s3raft.SetNotifier(pn)
//	http.Handle("POST /s3", pn)
//
// Watch parks until ctx is done, holding the node's wake callback; each
// delivered S3 event that creates a log object invokes it.
type PushNotifier struct {
	mu   sync.Mutex
	wake func()
}

func (p *PushNotifier) Watch(ctx context.Context, wake func()) error {
	p.mu.Lock()
	p.wake = wake
	p.mu.Unlock()
	<-ctx.Done()
	return nil
}

// ServeHTTP parses an S3 event notification body (POST /s3) and wakes the node
// if any record creates a log object. It coalesces to a single wake per
// delivery: the node re-reads the whole tail regardless.
func (p *PushNotifier) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var ev s3Event
	if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
		http.Error(w, "bad s3 event", http.StatusBadRequest)
		return
	}
	p.mu.Lock()
	wake := p.wake
	p.mu.Unlock()

	if s3EventCreatesLogObject(ev) && wake != nil {
		wake()
	}
	w.WriteHeader(http.StatusOK)
}

// s3EventCreatesLogObject reports whether any record in an S3 event notification
// creates a log object. The push and SQS notifiers share it so they agree on
// what counts as a log key (see isLogKey).
func s3EventCreatesLogObject(ev s3Event) bool {
	for _, rec := range ev.Records {
		key := rec.S3.Object.Key
		if dec, derr := url.QueryUnescape(key); derr == nil {
			key = dec // S3 notification keys are URL-encoded
		}
		if isLogKey(key) {
			return true
		}
	}
	return false
}

// s3Event is the subset of an S3 event notification record we consume.
type s3Event struct {
	Records []struct {
		EventName string `json:"eventName"`
		S3        struct {
			Object struct {
				Key string `json:"key"`
			} `json:"object"`
		} `json:"s3"`
	} `json:"Records"`
}

// listenBucketNotifications streams object-creation notifications under
// keyPrefix, invoking onEvent(strippedKey) for each. It blocks until ctx is
// cancelled or the stream ends/errors. Uses MinIO's ListenBucketNotification
// extension (a streaming GET on the bucket).
func (c *client) listenBucketNotifications(ctx context.Context, keyPrefix string, onEvent func(key string)) error {
	q := url.Values{}
	q.Set("prefix", c.prefix+keyPrefix)
	q.Set("events", "s3:ObjectCreated:*")
	q.Set("ping", "10")

	req, err := c.signedRequest(http.MethodGet, "", q, nil, nil)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)

	resp, err := streamHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("s3raft: listen notifications: unexpected status %d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue // whitespace keep-alive ping
		}
		var ev s3Event
		if json.Unmarshal([]byte(line), &ev) != nil {
			continue
		}
		for _, r := range ev.Records {
			key := r.S3.Object.Key
			if dec, derr := url.QueryUnescape(key); derr == nil {
				key = dec // S3 notification keys are URL-encoded
			}
			onEvent(strings.TrimPrefix(key, c.prefix))
		}
	}
	return scanner.Err()
}
