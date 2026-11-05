package operator

import (
	"context"
	"strings"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"testing"
)

func TestManagedRequestProfileRequiresOwnEvidence(t *testing.T) {
	for _, tc := range []struct {
		protocol, evidence string
		valid              bool
	}{
		{"", V1Alpha2ManagedSQLEvidenceFormat, false},
		{V1Alpha2ManagedSQLKVEvidenceFormat, V1Alpha2ManagedSQLKVEvidenceFormat, false},
		{sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, true},
		{sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol, true},
		{sqlpolicy.ManagedRequestApplicationProtocol, sqlpolicy.ManagedRequestApplicationProtocol, false},
		{sqlpolicy.ManagedGraphRequestApplicationProtocol, sqlpolicy.ManagedGraphRequestApplicationProtocol, false},
		{sqlpolicy.ManagedRequestApplicationProtocol, V1Alpha2ManagedSQLKVEvidenceFormat, false},
		{V1Alpha2ManagedSQLKVEvidenceFormat, sqlpolicy.ManagedRequestApplicationProtocol, false},
		{"unknown", "unknown", false},
	} {
		if err := ValidateV1Alpha2ApplicationProfile(tc.protocol, tc.evidence); (err == nil) != tc.valid {
			t.Errorf("profile=%q evidence=%q error=%v", tc.protocol, tc.evidence, err)
		}
	}
}

func TestManagedRetiredProfilesRefusedBeforeKubernetes(t *testing.T) {
	for _, protocol := range []string{"", V1Alpha2ManagedSQLKVEvidenceFormat} {
		t.Run(protocol, func(t *testing.T) {
			profile := V1Alpha2RuntimeProfile{ApplicationProtocol: protocol, EvidenceFormat: protocol}
			// Nil Kubernetes is intentional: rejection must precede any API access.
			if _, err := ReconcileV1Alpha2ManagedInitialization(context.Background(), nil, V1Alpha2ClusterRef{}, profile); err == nil || !strings.Contains(err.Error(), "profiles 7/8 are retired") {
				t.Fatalf("retired initialization was not rejected first: %v", err)
			}
			if _, _, err := BuildV1Alpha2RuntimePlan(V1Alpha2ClusterRef{}, "", "", profile, nil); err == nil || !strings.Contains(err.Error(), "profiles 7/8 are retired") {
				t.Fatalf("retired runtime plan was not rejected first: %v", err)
			}
		})
	}
}
