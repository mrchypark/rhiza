package node

import (
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestValidateEngineRepairSourceSnapshotRequiresExactFreshSurvivorTuple(t *testing.T) {
	old := quepaxa.Member{ID: "node-0", URL: "http://old:8080", PeerURL: "quic://old:9090", LogURL: "http://old:8081", PublicKey: quepaxa.PublicKey{1}, WALIdentity: "old-wal"}
	survivor := quepaxa.Member{ID: "node-1", URL: "http://survivor:8080", PeerURL: "quic://survivor:9090", LogURL: "http://survivor:8081", PublicKey: quepaxa.PublicKey{2}, WALIdentity: "survivor-wal"}
	candidate := quepaxa.Member{ID: "candidate", URL: "http://candidate:8080", PeerURL: "quic://candidate:9090", LogURL: "http://candidate:8081", PublicKey: quepaxa.PublicKey{3}, WALIdentity: "candidate-wal"}
	source := quepaxa.Cluster{ConfigID: 7, Members: []quepaxa.Member{old, survivor}}
	caller := fencing.Identity{SourceClusterID: "source", NodeID: string(survivor.ID), PodUID: "survivor-pod", RunID: "survivor-run", WALIdentity: survivor.WALIdentity}
	digest := func(value string) string {
		sum := sha256.Sum256([]byte(value))
		return hex.EncodeToString(sum[:])
	}
	repair := fencing.EngineRepairRecord{
		Version: fencing.EngineRepairRecordVersion,
		Binding: fencing.EngineRemovalBinding{Version: fencing.EngineRemovalBindingVersion, Mode: fencing.EngineRemovalMode,
			OperationID: "repair-op", RequestHash: digest("remove-request"), SourceClusterID: caller.SourceClusterID,
			StatefulSetUID: "sts-uid", OldConfigID: uint64(source.ConfigID), ExpectedAbortSlot: 11,
			OldMemberID: string(old.ID), OldWALIdentity: old.WALIdentity, TargetMemberID: string(candidate.ID)},
		SourceConfig: source, SourceAdminCredential: fencing.EngineRepairAdminCredential{Name: "admin", UID: "admin-uid", AdminTokenDigest: digest("admin-token")},
		Incarnations:      []fencing.RepairIncarnation{{Identity: fencing.Identity{SourceClusterID: caller.SourceClusterID, NodeID: string(old.ID), PodUID: "old-pod", RunID: "old-run", WALIdentity: old.WALIdentity}, Role: fencing.StartupRoleVoterActive}},
		CandidateIdentity: fencing.Identity{SourceClusterID: caller.SourceClusterID, NodeID: string(candidate.ID), PodName: "sts-2", PodUID: "candidate-pod", RunID: "candidate-run", WALIdentity: candidate.WALIdentity, AssignmentID: digest("assignment")},
		CandidateMember:   candidate, CandidateCredential: fencing.EngineRepairCredential{Name: "candidate-auth", UID: "candidate-auth-uid", PeerTokenDigest: digest("peer-token")},
		CandidateMemberDigest: fencing.EngineRepairMemberDigest(candidate),
		RemoveRequestHash:     digest("remove-request"), Phase: fencing.RepairCandidatePrepared,
	}
	if err := validateEngineRepairSourceSnapshot(repair, source, repair.Binding.ExpectedAbortSlot, caller, fencing.StartupRoleVoterActive); err != nil {
		t.Fatalf("exact fresh source snapshot rejected: %v", err)
	}
	tests := []struct {
		name     string
		current  quepaxa.Cluster
		abort    uint64
		identity fencing.Identity
		role     fencing.StartupRole
	}{
		{name: "new config", current: quepaxa.Cluster{ConfigID: source.ConfigID + 1, Members: source.Members}, abort: repair.Binding.ExpectedAbortSlot, identity: caller, role: fencing.StartupRoleVoterActive},
		{name: "changed source membership", current: quepaxa.Cluster{ConfigID: source.ConfigID, Members: []quepaxa.Member{old}}, abort: repair.Binding.ExpectedAbortSlot, identity: caller, role: fencing.StartupRoleVoterActive},
		{name: "changed abort slot", current: source, abort: repair.Binding.ExpectedAbortSlot + 1, identity: caller, role: fencing.StartupRoleVoterActive},
		{name: "caller is removed member", current: source, abort: repair.Binding.ExpectedAbortSlot, identity: repair.Incarnations[0].Identity, role: fencing.StartupRoleVoterActive},
		{name: "caller WAL differs", current: source, abort: repair.Binding.ExpectedAbortSlot, identity: func() fencing.Identity { id := caller; id.WALIdentity = "other-wal"; return id }(), role: fencing.StartupRoleVoterActive},
		{name: "caller not yet active", current: source, abort: repair.Binding.ExpectedAbortSlot, identity: caller, role: fencing.StartupRoleLearnerPendingRemove},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateEngineRepairSourceSnapshot(repair, test.current, test.abort, test.identity, test.role); err == nil {
				t.Fatal("stale or unauthorized source snapshot was accepted")
			}
		})
	}
}

func TestEngineRepairAddRequestIsExactAcrossCommitRetry(t *testing.T) {
	removeTarget := quepaxa.Cluster{ConfigID: 8, Members: []quepaxa.Member{{ID: "survivor", URL: "http://survivor:8080", PeerURL: "quic://survivor:9090", LogURL: "http://survivor:8081", PublicKey: quepaxa.PublicKey{1}, WALIdentity: "survivor-wal"}}}
	candidate := quepaxa.Member{ID: "candidate", URL: "http://candidate:8080", PeerURL: "quic://candidate:9090", LogURL: "http://candidate:8081", PublicKey: quepaxa.PublicKey{2}, WALIdentity: "candidate-wal"}
	repair := fencing.EngineRepairRecord{
		Binding:          fencing.EngineRemovalBinding{OperationID: "repair-operation", SourceClusterID: "source", ExpectedAbortSlot: 11},
		RemovalOperation: &quepaxa.EngineRemoval{Target: removeTarget}, CandidateMember: candidate,
		StorageProofDigest: "settled", RemoveTerminalDigest: "removed",
	}
	request, err := engineRepairAddRequestFor(repair, removeTarget, 0)
	if err != nil {
		t.Fatalf("build exact ADD request before commit: %v", err)
	}
	if request.ExpectedAbortSlot != 0 || repair.Binding.ExpectedAbortSlot != 11 {
		t.Fatal("successor ADD revision did not reset independently of the pinned REMOVE source abort slot")
	}
	committed := quepaxa.Cluster{ConfigID: removeTarget.ConfigID + 1, Members: append(append([]quepaxa.Member(nil), removeTarget.Members...), candidate)}
	retry, err := engineRepairAddRequestFor(repair, committed, 0)
	if err != nil || !reflect.DeepEqual(request, retry) {
		t.Fatalf("committed retry did not reproduce exact pinned ADD request: got=%+v err=%v", retry, err)
	}
	for _, tc := range []struct {
		name    string
		current quepaxa.Cluster
		abort   uint64
	}{
		{name: "unrelated config", current: quepaxa.Cluster{ConfigID: 10, Members: committed.Members}, abort: 0},
		{name: "changed abort slot", current: committed, abort: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := engineRepairAddRequestFor(repair, tc.current, tc.abort); err == nil {
				t.Fatal("stale ADD retry was accepted")
			}
		})
	}
}

func TestValidatePendingEngineRepairSnapshotUsesCanonicalAssignmentBeforeRuntimeEnrollment(t *testing.T) {
	digest := func(value string) string {
		sum := sha256.Sum256([]byte(value))
		return hex.EncodeToString(sum[:])
	}
	old := quepaxa.Member{ID: "node-0", URL: "http://old:8080", PeerURL: "quic://old:9090", LogURL: "http://old:8081", PublicKey: quepaxa.PublicKey{1}, WALIdentity: "old-wal"}
	survivor := quepaxa.Member{ID: "node-1", URL: "http://survivor:8080", PeerURL: "quic://survivor:9090", LogURL: "http://survivor:8081", PublicKey: quepaxa.PublicKey{2}, WALIdentity: "survivor-wal"}
	candidate := quepaxa.Member{ID: "candidate", URL: "http://candidate:8080", PeerURL: "quic://candidate:9090", LogURL: "http://candidate:8081", PublicKey: quepaxa.PublicKey{3}, WALIdentity: "candidate-wal"}
	source := quepaxa.Cluster{ConfigID: 7, Members: []quepaxa.Member{old, survivor}}
	proposal := fencing.PendingEngineRepairProposal{
		Version: fencing.PendingEngineRepairRequestVersion, SourceClusterID: "source", StatefulSetUID: "sts-uid",
		BootstrapIdentity: fencing.Identity{SourceClusterID: "source", NodeID: string(old.ID), PodName: "rhiza-2", PodUID: "pod-uid", RunID: "run-id"},
		StableOrdinal:     2, OldMemberID: string(old.ID), IncumbentReferenceDigest: digest("incumbent-reference"),
		Outcome:              fencing.RecoveryBootstrapCandidateWAL,
		SourceOldWALIdentity: old.WALIdentity, LocalIdentityHash: digest("local-identity"),
		CandidateIdentity: fencing.Identity{SourceClusterID: "source", NodeID: string(candidate.ID), PodName: "rhiza-2", PodUID: "pod-uid", RunID: "run-id", WALIdentity: candidate.WALIdentity},
		CandidateMember:   candidate, CandidateCredential: fencing.EngineRepairCredential{Name: "candidate-auth", UID: "candidate-auth-uid", PeerTokenDigest: digest("peer-token")},
		SourceAdminCredential: fencing.EngineRepairAdminCredential{Name: "admin", UID: "admin-uid", AdminTokenDigest: digest("admin-token")},
	}
	pending, err := fencing.NewPendingEngineRepairRequest(proposal)
	if err != nil {
		t.Fatalf("construct canonical pending request: %v", err)
	}
	caller := fencing.Identity{SourceClusterID: "source", NodeID: string(survivor.ID), PodUID: "survivor-pod", RunID: "survivor-run", WALIdentity: survivor.WALIdentity}
	repair := fencing.EngineRepairRecord{
		Version: fencing.EngineRepairRecordVersion,
		Binding: fencing.EngineRemovalBinding{Version: fencing.EngineRemovalBindingVersion, Mode: fencing.EngineRemovalMode,
			OperationID: pending.OperationID, RequestHash: digest("remove-request"), SourceClusterID: pending.SourceClusterID,
			StatefulSetUID: pending.StatefulSetUID, OldConfigID: uint64(source.ConfigID), ExpectedAbortSlot: 11,
			OldMemberID: pending.OldMemberID, OldWALIdentity: old.WALIdentity, TargetMemberID: string(candidate.ID)},
		BootstrapRequestDigest: pending.RequestDigest, SourceConfig: source, SourceAdminCredential: pending.SourceAdminCredential,
		Incarnations:      []fencing.RepairIncarnation{{Identity: fencing.Identity{SourceClusterID: "source", NodeID: string(old.ID), PodUID: "old-pod", RunID: "old-run", WALIdentity: old.WALIdentity}, Role: fencing.StartupRoleVoterActive}},
		CandidateIdentity: pending.CandidateIdentity, CandidateMember: pending.CandidateMember, CandidateCredential: pending.CandidateCredential,
		CandidateMemberDigest: fencing.EngineRepairMemberDigest(candidate), RemoveRequestHash: digest("remove-request"), Phase: fencing.RepairCandidatePrepared,
	}
	if repair.CandidateRuntimeBinding != nil || repair.CandidateRuntimeHash != "" {
		t.Fatal("pre-enrollment reservation unexpectedly has candidate runtime evidence")
	}
	if err := validatePendingEngineRepairSnapshot(pending, repair, source, 11, caller, fencing.StartupRoleVoterActive); err != nil {
		t.Fatalf("canonical pending request before runtime enrollment rejected: %v", err)
	}

	tampered := repair
	tampered.CandidateIdentity.AssignmentID = digest("invented-assignment")
	if err := validatePendingEngineRepairSnapshot(pending, tampered, source, 11, caller, fencing.StartupRoleVoterActive); err == nil {
		t.Fatal("record with caller-invented assignment was accepted")
	}
	if err := validatePendingEngineRepairSnapshot(pending, repair, source, 12, caller, fencing.StartupRoleVoterActive); err == nil {
		t.Fatal("pending request with stale source abort slot was accepted")
	}
}

func TestEngineRepairCandidateCannotFreezeBeforeTrustedRuntimeEnrollment(t *testing.T) {
	identity := fencing.Identity{SourceClusterID: "source", NodeID: "candidate", PodName: "rhiza-2", PodUID: "pod-uid", RunID: "run-id", AssignmentID: "assignment", WALIdentity: "candidate-wal"}
	runtime := fencing.RuntimeBinding{AuthorityID: "authority", NamespaceUID: "namespace-uid", BindingUID: "binding-uid", StatefulSetUID: "sts-uid",
		SourceClusterID: identity.SourceClusterID, NodeID: identity.NodeID, PodName: identity.PodName, PodUID: identity.PodUID, RunID: identity.RunID,
		AssignmentID: identity.AssignmentID, NodeName: "worker", NodeUID: "worker-uid", MachineID: "machine", BootID: "boot",
		RuntimeID: "runtime", SandboxID: "sandbox", ContainerID: "container", ImageID: "image"}
	repair := fencing.EngineRepairRecord{CandidateIdentity: identity}
	if err := validateEngineRepairCandidateRuntime(repair); err == nil {
		t.Fatal("un-enrolled pending candidate was allowed to freeze")
	}
	repair.CandidateRuntimeBinding = &runtime
	repair.CandidateRuntimeHash = fencing.RuntimeBindingDigest(runtime)
	if err := validateEngineRepairCandidateRuntime(repair); err != nil {
		t.Fatalf("trusted runtime enrollment was rejected: %v", err)
	}

	continuedIdentity := identity
	continuedIdentity.RunID = "continued-run"
	continuedRuntime := runtime
	continuedRuntime.RunID = continuedIdentity.RunID
	continuedRuntime.ContainerID = "continued-container"
	continuedRuntime.RuntimeID = "continued-runtime"
	continuedDigest := sha256.Sum256([]byte("continued-record"))
	repair.CandidateContinuations = []fencing.EngineRepairCandidateContinuation{{
		PriorRunID: identity.RunID, ExpectedRecordDigest: hex.EncodeToString(continuedDigest[:]),
		Identity: continuedIdentity, RuntimeBinding: continuedRuntime,
		RuntimeHash: fencing.RuntimeBindingDigest(continuedRuntime),
	}}
	if err := validateEngineRepairCandidateRuntime(repair); err != nil {
		t.Fatalf("trusted current continuation was rejected: %v", err)
	}
	if repair.CandidateIdentity != identity || repair.CandidateRuntimeHash != fencing.RuntimeBindingDigest(runtime) {
		t.Fatal("continuation changed the immutable original reservation")
	}

	changed := continuedRuntime
	changed.AssignmentID = "other-assignment"
	repair.CandidateContinuations[0].RuntimeBinding = changed
	repair.CandidateContinuations[0].RuntimeHash = fencing.RuntimeBindingDigest(changed)
	if err := validateEngineRepairCandidateRuntime(repair); err == nil {
		t.Fatal("runtime enrollment for another assignment was allowed to freeze")
	}
}
