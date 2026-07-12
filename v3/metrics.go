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

import "github.com/prometheus/client_golang/prometheus"

var (
	casConflicts = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "libraft",
		Name:      "cas_conflicts_total",
		Help:      "Total lost compare-and-swap races on shared log appends.",
	})

	s3Retries = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "libraft",
		Name:      "s3_retries_total",
		Help:      "Total S3 request retries due to transient faults (network/throttle/5xx).",
	})

	s3RequestSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "etcd",
		Subsystem: "libraft",
		Name:      "s3_request_seconds",
		Help:      "Latency of individual S3 requests (one round trip).",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 14), // 1ms .. ~8s
	}, []string{"method"})

	appendSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "etcd",
		Subsystem: "libraft",
		Name:      "append_seconds",
		Help:      "End-to-end latency of a batched log append, including CAS retries.",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 14),
	})

	appendBatchEntries = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "etcd",
		Subsystem: "libraft",
		Name:      "append_batch_entries",
		Help:      "Number of proposals coalesced into one log-object append.",
		Buckets:   []float64{1, 2, 4, 8, 16, 32, 64, 128, 256},
	})

	readsDenied = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "libraft",
		Name:      "reads_denied_total",
		Help:      "Linearizable reads failed closed (store unreachable or node superseded).",
	})

	fenceDemotions = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "libraft",
		Name:      "fence_demotions_total",
		Help:      "Times this node demoted itself after observing a higher fencing epoch.",
	})

	checkpointIndex = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "libraft",
		Name:      "checkpoint_index",
		Help:      "Raft index of the most recent bucket snapshot this node uploaded.",
	})
)

func init() {
	prometheus.MustRegister(
		casConflicts,
		s3Retries,
		s3RequestSeconds,
		appendSeconds,
		appendBatchEntries,
		readsDenied,
		fenceDemotions,
		checkpointIndex,
	)
}
