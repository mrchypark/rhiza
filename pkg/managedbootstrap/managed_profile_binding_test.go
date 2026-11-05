package managedbootstrap

import (
	"context"
	"strings"

	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
)

func TestManagedProfileBindingPureRoundTrip(t *testing.T) {
	ref := operator.V1Alpha2ClusterRef{Name: "sql", Namespace: "ns", UID: "uid"}
	for _, protocol := range []string{sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol} {
		t.Run(protocol, func(t *testing.T) {
			format := protocol
			profile := operator.V1Alpha2RuntimeProfile{ApplicationProtocol: protocol, EvidenceFormat: format, InitImage: "example/init:v1", PeerPort: 9090, ProfileSecretName: "runtime", Storage: types.ExecutionConfig{AdminToken: "private", ObjStoreProvider: "s3", ObjStoreBucket: "test", ObjStorePrefix: "managed", ObjStoreEndpoint: "s3.example.test"}}
			assignment, plan, err := operator.BuildV1Alpha2RuntimePlan(ref, "logical", "deployment", profile, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := validateManagedProfileBinding(ref, profile, assignment, plan); err != nil {
				t.Fatal(err)
			}
			ns, err := node.ManagedNamespaceFor(profile.Storage.ObjStorePrefix, ref.UID, 0, assignment.AssignmentID, string(plan.ClusterID))
			if err != nil {
				t.Fatal(err)
			}
			cfg := profile.Storage
			cfg.ClusterID, cfg.NodeID, cfg.ObjStorePrefix = plan.ClusterID, "member", ns.ConfigPrefix
			cfg.Members = []types.NodeConfig{} // Match the builder's empty common member list.
			n := node.New(&cfg)
			got, err := n.ManagedBootstrapPlanDigestForProtocol(plan.PhysicalBinding, format, protocol)
			if err != nil || got != assignment.BootstrapPlanDigest {
				t.Fatalf("builder/node digest mismatch: %v", err)
			}
			_, member, err := n.PreparedStartupBindingDigestsForProtocol(protocol)
			want, _ := types.ManagedMemberConfigDigest(cfg, protocol)
			if err != nil || member != want {
				t.Fatalf("member digest mismatch: %v", err)
			}
			for _, fault := range []string{"installed", "marker", "digest", "namespace", "generation", "cluster", "format", "durability", "reconfiguration"} {
				t.Run(fault, func(t *testing.T) {
					p, a, b := profile, assignment, plan
					switch fault {
					case "installed":
						p.ApplicationProtocol = "unknown"
					case "marker":
						if a.ApplicationProtocol == "" {
							a.ApplicationProtocol = types.ManagedApplicationProtocolSQLKV
						} else {
							a.ApplicationProtocol = ""
						}
					case "digest":
						a.BootstrapPlanDigest = "wrong"
					case "namespace":
						p.Storage.ObjStorePrefix = "other"
					case "generation":
						a.Generation = 1
					case "cluster":
						b.ClusterID = "other"
					case "format":
						b.EvidenceFormat = "wrong"
					case "durability":
						b.Durability = types.ObjectStoreDurabilityAsync
					case "reconfiguration":
						b.Reconfiguration = !b.Reconfiguration
					}
					if err := validateManagedProfileBinding(ref, p, a, b); err == nil {
						t.Fatal("profile mismatch accepted")
					}
				})
			}
		})
	}
}

func TestManagedConfigRetiredProfilesRefusedBeforeEffects(t *testing.T) {
	for _, protocol := range []string{"", types.ManagedApplicationProtocolSQLKV} {
		profile := operator.V1Alpha2RuntimeProfile{ApplicationProtocol: protocol, EvidenceFormat: protocol}
		if _, err := ManagedConfig(context.Background(), nil, operator.V1Alpha2ClusterRef{}, "", "", "missing-identity", "missing-bootstrap", profile); err == nil || !strings.Contains(err.Error(), "profiles 7/8 are retired") {
			t.Fatalf("retired ManagedConfig did not reject before file/Kubernetes access: %v", err)
		}
		if _, err := ManagedRuntimeConfig(context.Background(), nil, operator.V1Alpha2ClusterRef{}, "", "missing-identity", profile, operator.V1Alpha2BootstrapPlan{}); err == nil || !strings.Contains(err.Error(), "profiles 7/8 are retired") {
			t.Fatalf("retired runtime config did not reject before file/Kubernetes access: %v", err)
		}
		if _, _, err := ManagedRepairConfig(context.Background(), nil, operator.V1Alpha2ClusterRef{}, "", "", "missing-identity", "missing-bootstrap", "missing-repair", profile); err == nil || !strings.Contains(err.Error(), "profiles 7/8 are retired") {
			t.Fatalf("retired repair config did not reject before file/Kubernetes access: %v", err)
		}
	}
}
