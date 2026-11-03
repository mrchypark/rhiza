package types

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
)

func TestManagedBootstrapProfileLegacyBytesAndMixedBinding(t *testing.T) {
	old := `{"Version":1,"ClusterID":"cluster","Members":null,"PhysicalBinding":"source","EvidenceFormat":"sql-v1","Durability":"before-ack","Reconfiguration":false}`
	want := fmt.Sprintf("%x", sha256.Sum256([]byte(old)))
	legacy, err := ManagedBootstrapPlanDigest("cluster", nil, "source", "sql-v1", "", false)
	if err != nil || legacy != want {
		t.Fatalf("legacy digest bytes changed: %s want %s: %v", legacy, want, err)
	}
	mixed, err := ManagedBootstrapPlanDigestForProtocol("cluster", nil, "source", "sql-v1", "", false, ManagedApplicationProtocolSQLKV)
	if err != nil || mixed == legacy {
		t.Fatalf("mixed protocol/window not bound: %v", err)
	}
	config := ExecutionConfig{ClusterID: "cluster", NodeID: "node"}
	oldMember := `{"Cluster":"cluster","Node":"node","Members":null,"Learner":null,"Durability":"","Reconfiguration":false}`
	member, err := ManagedMemberConfigDigest(config, "")
	if err != nil || member != fmt.Sprintf("%x", sha256.Sum256([]byte(oldMember))) {
		t.Fatalf("legacy member bytes changed: %v", err)
	}
	mixedMember, err := ManagedMemberConfigDigest(config, ManagedApplicationProtocolSQLKV)
	if err != nil || member == mixedMember {
		t.Fatalf("mixed member profile unbound: %v", err)
	}
	for _, tc := range []struct {
		protocol string
		policy   sqlpolicy.MaterializationPolicy
	}{{"", sqlpolicy.LegacyMaterializationPolicy}, {ManagedApplicationProtocolSQLKV, sqlpolicy.ManagedSQLKVMaterializationPolicy}, {sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedRequestMaterializationPolicy}, {sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedGraphRequestMaterializationPolicy}} {
		policy, window, err := ManagedApplicationPolicy(tc.protocol)
		if err != nil || policy != tc.policy || window != 65536 {
			t.Fatalf("profile: %v %d %v", policy, window, err)
		}
	}
	requestDigest, err := ManagedMemberConfigDigest(config, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol)
	if err != nil || requestDigest == member || requestDigest == mixedMember {
		t.Fatalf("request profile member binding is not distinct: %v", err)
	}
	graphDigest, err := ManagedMemberConfigDigest(config, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol)
	if err != nil || graphDigest == requestDigest || graphDigest == member || graphDigest == mixedMember {
		t.Fatalf("Graph profile member binding is not distinct: %v", err)
	}
	for _, retired := range []string{sqlpolicy.ManagedRequestApplicationProtocol, sqlpolicy.ManagedGraphRequestApplicationProtocol} {
		if _, _, err := ManagedApplicationPolicy(retired); err == nil || !sqlpolicy.IsRetiredExperimentalApplicationProtocol(retired) {
			t.Fatalf("retired protocol %q remains selectable: %v", retired, err)
		}
		if _, err := ManagedMemberConfigDigest(config, retired); err == nil {
			t.Fatalf("retired protocol %q was accepted for member binding", retired)
		}
		if _, err := ManagedBootstrapPlanDigestForProtocol("cluster", nil, "source", retired, "", false, retired); err == nil {
			t.Fatalf("retired protocol %q was accepted for plan binding", retired)
		}
	}
	if _, _, err := ManagedApplicationPolicy("unknown"); err == nil {
		t.Fatal("unknown protocol accepted")
	}
	if _, err := ManagedMemberConfigDigest(config, "unknown"); err == nil {
		t.Fatal("unknown member protocol accepted")
	}
}
