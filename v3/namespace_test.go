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

// cfgWith builds a ServerConfig with a given cluster token, --initial-cluster,
// and data dir (its parent is the namespace's isolation discriminator).
func cfgWith(t *testing.T, token, cluster, dataDir string) config.ServerConfig {
	t.Helper()
	m, err := types.NewURLsMap(cluster)
	if err != nil {
		t.Fatalf("NewURLsMap(%q): %v", cluster, err)
	}
	return config.ServerConfig{InitialClusterToken: token, InitialPeerURLsMap: m, DataDir: dataDir}
}

const (
	genesis = "m1=http://10.0.0.1:2380,m2=http://10.0.0.2:2380,m3=http://10.0.0.3:2380"
	grown   = genesis + ",m4=http://10.0.0.4:2380"
	dirA    = "/var/lib/etcd/member"
	dirB    = "/tmp/TestFoo123/member"
)

// TestCidFromConfigMatchesEtcd proves cidFromConfig reproduces the exact cluster
// ID etcd freezes at bootstrap — it must, since both go through the same
// membership.NewClusterFromURLsMap path over identical inputs.
func TestCidFromConfigMatchesEtcd(t *testing.T) {
	lg := zap.NewNop()
	cfg := cfgWith(t, "tok", genesis, dirA)

	want, err := membership.NewClusterFromURLsMap(lg, cfg.InitialClusterToken, cfg.InitialPeerURLsMap)
	if err != nil {
		t.Fatal(err)
	}
	if got := cidFromConfig(lg, cfg); got != want.ID() {
		t.Fatalf("cidFromConfig = %s, etcd cluster ID = %s", got, want.ID())
	}
}

// TestNsKeyFormat pins the namespace prefix format: a legible cluster ID plus a
// hashed data-dir root.
func TestNsKeyFormat(t *testing.T) {
	got := nsKey("/var", types.ID(0x1a2b))
	want := "c-0000000000001a2b-" // cluster ID stays legible; root is the hashed tail
	if len(got) != len("c-")+16+1+16 || got[:len(want)] != want {
		t.Fatalf("nsKey = %q, want prefix %q and hashed root tail", got, want)
	}
}

// TestNsIdenticalAcrossMembers: every member of a cluster passes the same
// --initial-cluster / token and shares a deployment root, so all derive the same
// namespace regardless of which member's config is inspected.
func TestNsIdenticalAcrossMembers(t *testing.T) {
	lg := zap.NewNop()
	a := nsFromConfig(lg, cfgWith(t, "tok", genesis, dirA+"/0"))
	b := nsFromConfig(lg, cfgWith(t, "tok", genesis, dirA+"/1"))
	if a != b {
		t.Fatalf("namespace differs across members: %q vs %q", a, b)
	}
}

// TestNsTokenDiscriminatesClusters: two clusters sharing member URLs and root but
// not the cluster token get distinct namespaces (the token feeds the cluster ID).
func TestNsTokenDiscriminatesClusters(t *testing.T) {
	lg := zap.NewNop()
	a := nsFromConfig(lg, cfgWith(t, "tok-a", genesis, dirA))
	b := nsFromConfig(lg, cfgWith(t, "tok-b", genesis, dirA))
	if a == b {
		t.Fatalf("distinct tokens collided on namespace %q", a)
	}
}

// TestNsRootDiscriminatesClusters guards the regression that broke the e2e suite:
// clusters sharing a token, member URLs, AND cluster ID (all the single-node
// localhost:20001 / token "new" tests) must still be isolated by their data-dir
// root, or they share one S3 log.
func TestNsRootDiscriminatesClusters(t *testing.T) {
	lg := zap.NewNop()
	single := "m0=http://localhost:2380"
	a := nsFromConfig(lg, cfgWith(t, "new", single, dirA))
	b := nsFromConfig(lg, cfgWith(t, "new", single, dirB))
	if a == b {
		t.Fatalf("clusters with identical cluster ID but distinct roots collided on %q", a)
	}
	// Same cluster ID confirms the root is the ONLY discriminator here.
	if cidFromConfig(lg, cfgWith(t, "new", single, dirA)) != cidFromConfig(lg, cfgWith(t, "new", single, dirB)) {
		t.Fatal("expected identical cluster IDs; test no longer exercises the root discriminator")
	}
}

// TestGrownSetShiftsCid documents WHY rebindNamespace exists: a later joiner's
// --initial-cluster describes the grown set, whose genID differs from the frozen
// genesis cluster ID — so config derivation alone would point the joiner at the
// wrong log. newRaftNode corrects this from the authoritative cl.ID().
func TestGrownSetShiftsCid(t *testing.T) {
	lg := zap.NewNop()
	if nsFromConfig(lg, cfgWith(t, "tok", genesis, dirA)) == nsFromConfig(lg, cfgWith(t, "tok", grown, dirA)) {
		t.Fatal("expected genesis and grown-set configs to derive different cluster IDs")
	}
}

// TestResolveNSEnvOverride: an explicit ETCD_S3LOG_NS wins over config
// derivation and is returned verbatim (whitespace-trimmed).
func TestResolveNSEnvOverride(t *testing.T) {
	t.Setenv(EnvNS, "  c-deadbeef  ")
	if got := resolveNS(zap.NewNop(), cfgWith(t, "tok", genesis, dirA)); got != "c-deadbeef" {
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
	if got := cidFromConfig(lg, cfgWith(t, "tok", dup, dirA)); got != types.ID(0) {
		t.Fatalf("cidFromConfig on duplicate members = %s, want 0", got)
	}
}
