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
	"testing"
	"time"
)

// fakeClock is a deterministic clock for tests: Sleep records the requested
// duration and advances the clock instead of blocking, so retry/backoff logic
// runs instantly and its timing is assertable.
type fakeClock struct {
	now   time.Time
	slept []time.Duration
}

func (f *fakeClock) Now() time.Time                  { return f.now }
func (f *fakeClock) Since(t time.Time) time.Duration { return f.now.Sub(t) }
func (f *fakeClock) Sleep(d time.Duration) {
	f.slept = append(f.slept, d)
	f.now = f.now.Add(d)
}

// TestRetryCASBacksOffViaClock drives the CAS conflict-retry loop with the fake
// clock: it retries until fn stops returning errConflict, sleeping between
// attempts — and thanks to the clock seam the test neither blocks nor flakes.
func TestRetryCASBacksOffViaClock(t *testing.T) {
	fc := &fakeClock{}
	c := &client{clk: fc}

	calls := 0
	err := c.retryCAS(func() error {
		calls++
		if calls < 3 {
			return errConflict
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retryCAS: %v", err)
	}
	if calls != 3 {
		t.Fatalf("fn called %d times, want 3", calls)
	}
	// One sleep between each pair of attempts: 3 attempts -> 2 sleeps.
	if len(fc.slept) != 2 {
		t.Fatalf("slept %d times, want 2", len(fc.slept))
	}
	// Backoff ramps: the second wait is strictly longer than the first
	// (base doubles; jitter only adds).
	if fc.slept[1] <= fc.slept[0] {
		t.Errorf("backoff did not ramp: %v then %v", fc.slept[0], fc.slept[1])
	}
}

// TestRetryCASGivesUp confirms the loop gives up with an error after exhausting
// its attempts rather than looping forever, and sleeps once per attempt.
func TestRetryCASGivesUp(t *testing.T) {
	fc := &fakeClock{}
	c := &client{clk: fc}
	calls := 0
	err := c.retryCAS(func() error { calls++; return errConflict })
	if err == nil {
		t.Fatal("retryCAS on persistent conflict returned nil, want an error")
	}
	if calls != casMaxAttempts {
		t.Errorf("fn called %d times, want casMaxAttempts=%d", calls, casMaxAttempts)
	}
	if len(fc.slept) != casMaxAttempts {
		t.Errorf("slept %d times, want %d", len(fc.slept), casMaxAttempts)
	}
}
