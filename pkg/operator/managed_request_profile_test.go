package operator

import (
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"testing"
)

func TestManagedRequestProfileRequiresOwnEvidence(t *testing.T) {
	for _, tc := range []struct {
		protocol, evidence string
		valid              bool
	}{
		{"", V1Alpha2ManagedSQLEvidenceFormat, true},
		{V1Alpha2ManagedSQLKVEvidenceFormat, V1Alpha2ManagedSQLKVEvidenceFormat, true},
		{sqlpolicy.ManagedRequestApplicationProtocol, sqlpolicy.ManagedRequestApplicationProtocol, true},
		{sqlpolicy.ManagedRequestApplicationProtocol, V1Alpha2ManagedSQLKVEvidenceFormat, false},
		{V1Alpha2ManagedSQLKVEvidenceFormat, sqlpolicy.ManagedRequestApplicationProtocol, false},
		{"unknown", "unknown", false},
	} {
		if err := ValidateV1Alpha2ApplicationProfile(tc.protocol, tc.evidence); (err == nil) != tc.valid {
			t.Errorf("profile=%q evidence=%q error=%v", tc.protocol, tc.evidence, err)
		}
	}
}
