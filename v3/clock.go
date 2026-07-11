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

import "time"

// clock abstracts the wall-clock reads and sleeps on the timing-sensitive
// control paths — the S3 retry budget and the sync-debounce cooldown — so they
// can be driven deterministically in tests (see clock_test.go). Periodic
// tickers and SigV4 request-signing time deliberately stay on the real clock:
// faking a select-loop ticker adds risk for little gain, and a frozen signing
// clock would break signature validity.
type clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	Sleep(d time.Duration)
}

// realClock is the production clock backed by the time package.
type realClock struct{}

func (realClock) Now() time.Time                  { return time.Now() }
func (realClock) Since(t time.Time) time.Duration { return time.Since(t) }
func (realClock) Sleep(d time.Duration)           { time.Sleep(d) }
