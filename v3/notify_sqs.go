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
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// SQSNotifier is a LogNotifier for AWS deployments that are not Lambda: an SQS
// queue subscribed (directly or via SNS) to the bucket's S3 Event Notifications.
// It long-polls the queue and wakes the node for each event that creates a log
// object, deleting handled messages. This is the pull analog of the Lambda
// PushNotifier — same interface, no polling of S3 itself.
//
// Construct from the environment with newSQSNotifierFromEnv (ETCD_S3LOG_SQS_URL)
// and install with SetNotifier before the node starts, or build one directly.
//
// e2e requires a live SQS queue wired to the bucket; the event parsing is unit
// tested (notify_sqs_test.go). SigV4 for the "sqs" service is hand-rolled to
// keep s3raft dependency-free, mirroring the S3 signer in client.go.
type SQSNotifier struct {
	QueueURL     string // https://sqs.<region>.amazonaws.com/<acct>/<queue>
	Region       string
	AccessKey    string
	SecretKey    string
	SessionToken string
	// WaitSeconds is the SQS long-poll wait (0..20); defaults to 20.
	WaitSeconds int

	hc *http.Client
}

// newSQSNotifierFromEnv builds an SQSNotifier from ETCD_S3LOG_SQS_URL and the
// same credential/region environment the S3 client uses, or returns nil if no
// queue URL is configured.
func newSQSNotifierFromEnv() *SQSNotifier {
	q := firstEnv("", "ETCD_S3LOG_SQS_URL")
	if q == "" {
		return nil
	}
	cr := awsCredsFromEnv()
	// An explicitly configured region wins. Otherwise fall back to the queue
	// host (sqs.<region>.amazonaws.com), which is authoritative for AWS, so a
	// queue in a non-default region is signed correctly with no extra config.
	region := cr.region
	if firstEnv("", "ETCD_S3LOG_REGION", "AWS_REGION", "AWS_DEFAULT_REGION") == "" {
		if r := regionFromSQSHost(q); r != "" {
			region = r
		}
	}
	return &SQSNotifier{
		QueueURL:     q,
		Region:       region,
		AccessKey:    cr.accessKey,
		SecretKey:    cr.secretKey,
		SessionToken: cr.sessionToken,
	}
}

// regionFromSQSHost extracts the region from an AWS SQS queue URL, whose host is
// sqs.<region>.amazonaws.com. Returns "" for non-AWS hosts (e.g. LocalStack), so
// the caller keeps its configured region.
func regionFromSQSHost(queueURL string) string {
	u, err := url.Parse(queueURL)
	if err != nil {
		return ""
	}
	parts := strings.Split(u.Host, ".")
	if len(parts) >= 3 && parts[0] == "sqs" {
		return parts[1]
	}
	return ""
}

// Watch long-polls the queue until ctx is cancelled, calling wake once per batch
// that contains at least one log-object-created event. It returns the first
// connection error so the node can fall back to polling; once running, transient
// receive errors are retried after a short pause.
func (s *SQSNotifier) Watch(ctx context.Context, wake func()) error {
	if s.hc == nil {
		s.hc = &http.Client{Timeout: 40 * time.Second} // > long-poll wait
	}
	if s.WaitSeconds <= 0 || s.WaitSeconds > 20 {
		s.WaitSeconds = 20
	}
	connectedOnce := false
	for {
		if ctx.Err() != nil {
			return nil
		}
		msgs, err := s.receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if !connectedOnce {
				return err // source unreachable: caller falls back to polling
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(notifyReconnectDelay):
			}
			continue
		}
		connectedOnce = true

		woke := false
		for _, m := range msgs {
			if !woke && sqsBodyHasLogEvent(m.Body) {
				wake()
				woke = true
			}
			_ = s.deleteMessage(ctx, m.ReceiptHandle) // best-effort
		}
	}
}

type sqsMessage struct {
	Body          string
	ReceiptHandle string
}

func (s *SQSNotifier) receive(ctx context.Context) ([]sqsMessage, error) {
	form := url.Values{}
	form.Set("Action", "ReceiveMessage")
	form.Set("Version", "2012-11-05")
	form.Set("MaxNumberOfMessages", "10")
	form.Set("WaitTimeSeconds", fmt.Sprintf("%d", s.WaitSeconds))

	body, err := s.call(ctx, form)
	if err != nil {
		return nil, err
	}
	var parsed struct {
		Messages []struct {
			Body          string `xml:"Body"`
			ReceiptHandle string `xml:"ReceiptHandle"`
		} `xml:"ReceiveMessageResult>Message"`
	}
	if err := xml.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("s3raft: sqs receive: bad xml: %w", err)
	}
	out := make([]sqsMessage, 0, len(parsed.Messages))
	for _, m := range parsed.Messages {
		out = append(out, sqsMessage{Body: m.Body, ReceiptHandle: m.ReceiptHandle})
	}
	return out, nil
}

func (s *SQSNotifier) deleteMessage(ctx context.Context, receipt string) error {
	form := url.Values{}
	form.Set("Action", "DeleteMessage")
	form.Set("Version", "2012-11-05")
	form.Set("ReceiptHandle", receipt)
	_, err := s.call(ctx, form)
	return err
}

// call issues one SigV4-signed POST (service "sqs") with the form as the body.
func (s *SQSNotifier) call(ctx context.Context, form url.Values) ([]byte, error) {
	u, err := url.Parse(s.QueueURL)
	if err != nil {
		return nil, err
	}
	payload := form.Encode()
	payloadHash := sha256Hex([]byte(payload))
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.QueueURL, strings.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Host", u.Host)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	req.Header.Set("X-Amz-Date", amzDate)
	if s.SessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", s.SessionToken)
	}

	// Sign host, content-type, and the x-amz-* headers (sorted).
	canonicalHeaders := "content-type:application/x-www-form-urlencoded\n" +
		"host:" + u.Host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"
	signedHeaders := "content-type;host;x-amz-content-sha256;x-amz-date"
	if s.SessionToken != "" {
		canonicalHeaders += "x-amz-security-token:" + s.SessionToken + "\n"
		signedHeaders += ";x-amz-security-token"
	}

	canonicalRequest := strings.Join([]string{
		http.MethodPost, u.EscapedPath(), "", canonicalHeaders, signedHeaders, payloadHash,
	}, "\n")
	req.Header.Set("Authorization", sigV4Authorization(
		s.AccessKey, s.SecretKey, s.Region, "sqs", amzDate, dateStamp, signedHeaders, canonicalRequest))

	resp, err := s.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("s3raft: sqs %s: status %d", form.Get("Action"), resp.StatusCode)
	}
	return b, nil
}

// sqsBodyHasLogEvent reports whether an SQS message body references a created
// log object. It handles both a raw S3 event notification and one wrapped in an
// SNS envelope (Body is JSON with a "Message" string holding the S3 event).
func sqsBodyHasLogEvent(body string) bool {
	if s3EventHasLogKey(body) {
		return true
	}
	// SNS envelope: {"Type":"Notification","Message":"<escaped S3 event json>"}
	var env struct {
		Message string `json:"Message"`
	}
	if json.Unmarshal([]byte(body), &env) == nil && env.Message != "" {
		return s3EventHasLogKey(env.Message)
	}
	return false
}

// s3EventHasLogKey parses an S3 event notification and reports whether any
// record creates an object whose key names a log object.
func s3EventHasLogKey(payload string) bool {
	var ev s3Event
	if json.Unmarshal([]byte(payload), &ev) != nil {
		return false
	}
	return s3EventCreatesLogObject(ev)
}
