package operator

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
)

func phase9ClusterWithProtocol(protocol string) string {
	if protocol == "" {
		return phase8Cluster
	}
	return strings.Replace(phase8Cluster, "\"genesisIntent\":\"Create\"", "\"genesisIntent\":\"Create\",\"applicationProtocol\":\""+protocol+"\"", 1)
}

func TestManagedProfileCRDEnumRetiresExperimentalProtocols(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/operator/cluster-crd.yaml")
	if err != nil {
		t.Fatal(err)
	}
	const enum = "enum: [rhiza-managed-sql-kv/v1, rhiza-managed-request-append-only/v1, rhiza-managed-request-graph-append-only/v1]"
	if strings.Count(string(raw), enum) != 2 {
		t.Fatal("cluster and assigned-plan protocol enums must retain SQLKV and only final append-only profiles")
	}
	for _, retired := range []string{"rhiza-managed-request/v1", "rhiza-managed-request-graph/v1"} {
		if strings.Contains(string(raw), retired) {
			t.Fatalf("CRD still advertises retired experimental protocol %q", retired)
		}
	}
}

func phase9MixedRuntimeProfile() V1Alpha2RuntimeProfile {
	profile := phase9RuntimeProfile()
	profile.ApplicationProtocol = types.ManagedApplicationProtocolSQLKV
	profile.EvidenceFormat = V1Alpha2ManagedSQLKVEvidenceFormat
	return profile
}

func TestManagedBootstrapKnownProfilesCreateOnce(t *testing.T) {
	for _, profile := range []V1Alpha2RuntimeProfile{phase9RuntimeProfile(), phase9MixedRuntimeProfile()} {
		t.Run(profile.EvidenceFormat, func(t *testing.T) {
			s, kube := phase9Setup(t, phase9ClusterWithProtocol(profile.ApplicationProtocol), profile)
			first, err := ReconcileV1Alpha2ManagedInitialization(t.Context(), kube, phase2Ref, profile)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := first.BootstrapPlan()
			if err != nil || len(plan.Members) != 3 || plan.EvidenceFormat != profile.EvidenceFormat || first.Authority().Generation != 0 || first.Authority().Binding.DeploymentUID != "deployment" || s.posts != 1 || s.puts != 1 {
				t.Fatalf("normal genesis failed: plan=%+v err=%v posts=%d puts=%d", plan, err, s.posts, s.puts)
			}
			var cluster struct {
				Status struct {
					ApplicationInit V1Alpha2ApplicationInitAssignment
				}
			}
			if err := json.Unmarshal([]byte(s.cluster), &cluster); err != nil || cluster.Status.ApplicationInit.Plan.ApplicationProtocol != profile.ApplicationProtocol {
				t.Fatalf("explicit protocol not preserved: %v", err)
			}
			if _, err := ReconcileV1Alpha2ManagedInitialization(t.Context(), kube, phase2Ref, profile); err != nil || s.posts != 1 || s.puts != 1 {
				t.Fatalf("repeat changed genesis: %v posts=%d puts=%d", err, s.posts, s.puts)
			}
		})
	}
}

func TestManagedBootstrapProfileInstalledMismatchBeforeWrites(t *testing.T) {
	for _, test := range []struct {
		name string
		live string
		edit func(*V1Alpha2RuntimeProfile)
	}{
		{"legacy live mixed installed", "", func(*V1Alpha2RuntimeProfile) {}},
		{"mixed live legacy installed", types.ManagedApplicationProtocolSQLKV, func(p *V1Alpha2RuntimeProfile) { *p = phase9RuntimeProfile() }},
		{"retired request protocol", sqlpolicy.ManagedRequestApplicationProtocol, func(p *V1Alpha2RuntimeProfile) {
			p.ApplicationProtocol, p.EvidenceFormat = sqlpolicy.ManagedRequestApplicationProtocol, sqlpolicy.ManagedRequestApplicationProtocol
		}},
		{"retired graph protocol", sqlpolicy.ManagedGraphRequestApplicationProtocol, func(p *V1Alpha2RuntimeProfile) {
			p.ApplicationProtocol, p.EvidenceFormat = sqlpolicy.ManagedGraphRequestApplicationProtocol, sqlpolicy.ManagedGraphRequestApplicationProtocol
		}},
		{"unknown protocol", "unknown/v1", func(p *V1Alpha2RuntimeProfile) { p.ApplicationProtocol = "unknown/v1" }},
		{"wrong evidence", types.ManagedApplicationProtocolSQLKV, func(p *V1Alpha2RuntimeProfile) { p.EvidenceFormat = V1Alpha2ManagedSQLEvidenceFormat }},
		{"missing image", types.ManagedApplicationProtocolSQLKV, func(p *V1Alpha2RuntimeProfile) { p.InitImage = "" }},
		{"local storage", types.ManagedApplicationProtocolSQLKV, func(p *V1Alpha2RuntimeProfile) { p.Storage.ObjStoreDir = "local" }},
		{"invalid Secret", types.ManagedApplicationProtocolSQLKV, func(p *V1Alpha2RuntimeProfile) { p.ProfileSecretName = "invalid/name" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			profile := phase9MixedRuntimeProfile()
			test.edit(&profile)
			s, kube := phase9Setup(t, phase9ClusterWithProtocol(test.live), profile)
			if _, err := ReconcileV1Alpha2ManagedInitialization(t.Context(), kube, phase2Ref, profile); err == nil || s.posts != 0 || s.puts != 0 {
				t.Fatalf("invalid profile wrote: err=%v posts=%d puts=%d", err, s.posts, s.puts)
			}
		})
	}
	profile := phase9MixedRuntimeProfile()
	s, kube := phase9Setup(t, phase9ClusterWithProtocol(profile.ApplicationProtocol), profile)
	s.runtimeProfile.InitImage = "different:v1"
	if _, err := ReconcileV1Alpha2ManagedInitialization(t.Context(), kube, phase2Ref, profile); err == nil || s.posts != 0 || s.puts != 0 {
		t.Fatalf("different installed Secret wrote: %v posts=%d puts=%d", err, s.posts, s.puts)
	}
	b, err := json.Marshal(V1Alpha2RuntimeProfile{})
	if err != nil || strings.Contains(string(b), "ApplicationProtocol") {
		t.Fatalf("legacy profile serialization changed: %s %v", b, err)
	}
}
