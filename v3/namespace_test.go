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

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.uber.org/zap"
)

func cfgWith(t *testing.T, token, cluster string) config.ServerConfig {
	t.Helper()
	m, err := types.NewURLsMap(cluster)
	if err != nil {
		t.Fatalf("NewURLsMap(%q): %v", cluster, err)
	}
	return config.ServerConfig{InitialClusterToken: token, InitialPeerURLsMap: m}
}

const (
	genesis = "m1=http://10.0.0.1:2380,m2=http://10.0.0.2:2380,m3=http://10.0.0.3:2380"
	grown   = genesis + ",m4=http://10.0.0.4:2380"
)

// TestCidFromConfigMatchesEtcd proves cidFromConfig reproduces the exact cluster
// ID etcd freezes at bootstrap — it must, since both go through the same
// membership.NewClusterFromURLsMap path over identical inputs.
func TestCidFromConfigMatchesEtcd(t *testing.T) {
	lg := zap.NewNop()
	cfg := cfgWith(t, "tok", genesis)

	want, err := membership.NewClusterFromURLsMap(lg, cfg.InitialClusterToken, cfg.InitialPeerURLsMap)
	if err != nil {
		t.Fatal(err)
	}
	if got := cidFromConfig(lg, cfg); got != want.ID() {
		t.Fatalf("cidFromConfig = %s, etcd cluster ID = %s", got, want.ID())
	}
}

// TestNsFromClusterIDFormat pins the namespace prefix format.
func TestNsFromClusterIDFormat(t *testing.T) {
	if got, want := nsFromClusterID(types.ID(0x1a2b)), "c-0000000000001a2b"; got != want {
		t.Fatalf("nsFromClusterID = %q, want %q", got, want)
	}
}

// TestNsIdenticalAcrossMembers: every member of a cluster passes the same
// --initial-cluster / token, so all derive the same namespace regardless of
// which member's config is inspected.
func TestNsIdenticalAcrossMembers(t *testing.T) {
	lg := zap.NewNop()
	a := nsFromConfig(lg, cfgWith(t, "tok", genesis))
	b := nsFromConfig(lg, cfgWith(t, "tok", genesis))
	if a != b {
		t.Fatalf("namespace differs across members: %q vs %q", a, b)
	}
}

// TestNsTokenDiscriminatesClusters: two clusters sharing member URLs but not the
// cluster token get distinct namespaces (the token feeds each member ID hash).
func TestNsTokenDiscriminatesClusters(t *testing.T) {
	lg := zap.NewNop()
	a := nsFromConfig(lg, cfgWith(t, "tok-a", genesis))
	b := nsFromConfig(lg, cfgWith(t, "tok-b", genesis))
	if a == b {
		t.Fatalf("distinct tokens collided on namespace %q", a)
	}
}

// TestGrownSetShiftsCid documents WHY rebindNamespace exists: a later joiner's
// --initial-cluster describes the grown set, whose genID differs from the frozen
// genesis cluster ID — so config derivation alone would point the joiner at the
// wrong log. newRaftNode corrects this from the authoritative cl.ID().
func TestGrownSetShiftsCid(t *testing.T) {
	lg := zap.NewNop()
	if nsFromConfig(lg, cfgWith(t, "tok", genesis)) == nsFromConfig(lg, cfgWith(t, "tok", grown)) {
		t.Fatal("expected genesis and grown-set configs to derive different cluster IDs")
	}
}

// TestResolveNSEnvOverride: an explicit ETCD_S3LOG_NS wins over config
// derivation and is returned verbatim (whitespace-trimmed).
func TestResolveNSEnvOverride(t *testing.T) {
	t.Setenv(EnvNS, "  c-deadbeef  ")
	cfg := cfgWith(t, "tok", genesis)
	cfg.DataDir = t.TempDir()
	if got := resolveNS(zap.NewNop(), cfg); got != "c-deadbeef" {
		t.Fatalf("resolveNS with %s set = %q, want c-deadbeef", EnvNS, got)
	}
}

// TestCidFromConfigInvalidFallsBackToZero: a malformed cluster (duplicate member
// ID) can't be hashed; derivation returns a deterministic zero rather than
// panicking, staying consistent across members.
func TestCidFromConfigInvalidFallsBackToZero(t *testing.T) {
	lg := zap.NewNop()
	// Two members with identical peer URLs collapse to the same member ID.
	dup := "m1=http://10.0.0.1:2380,m2=http://10.0.0.1:2380"
	if got := cidFromConfig(lg, cfgWith(t, "tok", dup)); got != types.ID(0) {
		t.Fatalf("cidFromConfig on duplicate members = %s, want 0", got)
	}
}
