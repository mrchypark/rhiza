package managedbootstrap

import (
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
)

func TestManagedProfileBindingPureRoundTrip(t *testing.T) {
	ref := operator.V1Alpha2ClusterRef{Name: "sql", Namespace: "ns", UID: "uid"}
	for _, protocol := range []string{"", types.ManagedApplicationProtocolSQLKV} {
		t.Run(protocol, func(t *testing.T) {
			format := operator.V1Alpha2ManagedSQLEvidenceFormat
			if protocol != "" {
				format = operator.V1Alpha2ManagedSQLKVEvidenceFormat
			}
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
