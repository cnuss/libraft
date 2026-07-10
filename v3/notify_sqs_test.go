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
)

// snsWrap builds an SNS-envelope SQS body carrying an S3 event JSON string.
func snsWrap(t *testing.T, s3event string) string {
	t.Helper()
	b, err := json.Marshal(struct {
		Type    string `json:"Type"`
		Message string `json:"Message"`
	}{Type: "Notification", Message: s3event})
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestSQSBodyHasLogEvent(t *testing.T) {
	logEvent := `{"Records":[{"eventName":"s3:ObjectCreated:Put","s3":{"object":{"key":"c-abc/log/00000000000000000042"}}}]}`
	urlEncodedLog := `{"Records":[{"eventName":"s3:ObjectCreated:Put","s3":{"object":{"key":"c-abc%2Flog%2F00000000000000000042"}}}]}`
	nonLogEvent := `{"Records":[{"eventName":"s3:ObjectCreated:Put","s3":{"object":{"key":"c-abc/snap/00000000000000000042.db"}}}]}`
	metaEvent := `{"Records":[{"eventName":"s3:ObjectCreated:Put","s3":{"object":{"key":"c-abc/meta/head"}}}]}`

	cases := []struct {
		name string
		body string
		want bool
	}{
		{"raw s3 log event", logEvent, true},
		{"url-encoded log key", urlEncodedLog, true},
		{"sns-wrapped log event", snsWrap(t, logEvent), true},
		{"sns-wrapped non-log", snsWrap(t, nonLogEvent), false},
		{"raw non-log (snap)", nonLogEvent, false},
		{"raw non-log (meta)", metaEvent, false},
		{"garbage", "not json at all", false},
		{"empty", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sqsBodyHasLogEvent(tc.body); got != tc.want {
				t.Errorf("sqsBodyHasLogEvent(%q) = %v, want %v", tc.body, got, tc.want)
			}
		})
	}
}

func TestNewSQSNotifierFromEnv(t *testing.T) {
	t.Setenv("ETCD_S3LOG_SQS_URL", "")
	if n := newSQSNotifierFromEnv(); n != nil {
		t.Fatalf("expected nil notifier when SQS URL unset, got %+v", n)
	}
	t.Setenv("ETCD_S3LOG_SQS_URL", "https://sqs.us-east-1.amazonaws.com/123/q")
	t.Setenv("ETCD_S3LOG_REGION", "eu-west-2")
	n := newSQSNotifierFromEnv()
	if n == nil {
		t.Fatal("expected non-nil notifier when SQS URL set")
	}
	if n.QueueURL != "https://sqs.us-east-1.amazonaws.com/123/q" || n.Region != "eu-west-2" {
		t.Errorf("unexpected notifier config: %+v", n)
	}
}
