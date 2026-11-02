package recovery

import (
	"context"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
)

func TestManagedRequestHandoffPolicyIsExplicit(t *testing.T) {
	for _, tc := range []struct {
		policy  sqlpolicy.MaterializationPolicy
		version int
	}{{sqlpolicy.LegacyMaterializationPolicy, 1}, {sqlpolicy.ManagedSQLKVMaterializationPolicy, 2}, {sqlpolicy.ManagedRequestMaterializationPolicy, 3}} {
		got, err := preparedVersion(tc.policy, types.DefaultIdempotencyWindowSlots)
		if err != nil || got != tc.version {
			t.Fatalf("policy=%d version=%d want=%d error=%v", tc.policy, got, tc.version, err)
		}
	}
	if _, err := preparedVersion(sqlpolicy.MaterializationPolicy(999), types.DefaultIdempotencyWindowSlots); err == nil {
		t.Fatal("unknown handoff policy accepted")
	}
	if _, err := preparedVersion(sqlpolicy.LegacyMaterializationPolicy, 1024); err == nil {
		t.Fatal("legacy nondefault window accepted")
	}
}

func TestManagedRequestGenerationAnchorRefusedBeforeSource(t *testing.T) {
	_, err := VerifyGenerationAnchorForPolicy(context.Background(), nil, "", MembershipRecord{}, [32]byte{}, sqlpolicy.ManagedRequestMaterializationPolicy)
	if err == nil || !strings.Contains(err.Error(), "managed request generation anchors are not supported") {
		t.Fatalf("new request profile must refuse generation anchors before source access: %v", err)
	}
}
