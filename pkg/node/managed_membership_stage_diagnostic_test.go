package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func TestManagedMembershipStageDiagnosticIsBoundedAndRedacted(t *testing.T) {
	request := network.MembershipChange{
		OperationID: "private-operation-id",
		ClusterID:   "private-cluster-id",
		Add: &quepaxa.Member{
			ID:          "private-node-id",
			WALIdentity: "private-wal-token",
			PeerURL:     "https://private-peer.invalid",
		},
	}
	permitRequest := ManagedMembershipRequest{Action: ManagedMembershipAdd}
	ctx := context.WithValue(context.Background(), managedMembershipPermitKey{}, managedMembershipPermit{
		request: permitRequest,
		change:  request,
	})
	original := errors.New("private object path and payload")
	wrapped := errors.Join(network.ErrRequestConflict, original)

	var output bytes.Buffer
	previousOutput := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previousOutput) })

	logManagedMembershipStageFailure(ctx, request, managedMembershipStageRegistration, wrapped)
	line := output.String()
	if !strings.Contains(line, "MANAGED_MEMBERSHIP_STAGE_FAILURE action=add stage=registration cause=request_conflict") {
		t.Fatalf("diagnostic omitted fixed stage/cause: %q", line)
	}
	for _, secret := range []string{"private-operation-id", "private-cluster-id", "private-node-id", "private-wal-token", "private-peer.invalid", "private object path", "payload"} {
		if strings.Contains(line, secret) {
			t.Fatalf("diagnostic leaked %q: %q", secret, line)
		}
	}
	if !errors.Is(wrapped, network.ErrRequestConflict) || !errors.Is(wrapped, original) {
		t.Fatalf("diagnostic changed the original wrapped error: %v", wrapped)
	}
}

func TestManagedMembershipRegistrationSubstepsAreFixedAndRedacted(t *testing.T) {
	request := network.MembershipChange{
		OperationID: "private-operation-id",
		ClusterID:   "private-cluster-id",
		Add: &quepaxa.Member{
			ID:          "private-node-id",
			WALIdentity: "private-wal-token",
			PeerURL:     "https://private-peer.invalid",
		},
	}
	ctx := context.WithValue(context.Background(), managedMembershipPermitKey{}, managedMembershipPermit{
		request: ManagedMembershipRequest{Action: ManagedMembershipAdd},
		change:  request,
	})
	for _, test := range []struct {
		stage managedMembershipStage
		label string
	}{
		{managedMembershipStageRegistrationRead, "registration_read"},
		{managedMembershipStageRegistrationRecordValidation, "registration_record_validation"},
		{managedMembershipStageRegistrationProfileCompute, "registration_profile_compute"},
		{managedMembershipStageRegistrationProfileMatch, "registration_profile_match"},
	} {
		t.Run(test.label, func(t *testing.T) {
			var output bytes.Buffer
			previousOutput := log.Writer()
			log.SetOutput(&output)
			t.Cleanup(func() { log.SetOutput(previousOutput) })
			original := errors.New("private registration detail")
			logManagedMembershipStageFailure(ctx, request, test.stage, original)
			line := output.String()
			if !strings.Contains(line, "MANAGED_MEMBERSHIP_STAGE_FAILURE action=add stage="+test.label+" cause=other") {
				t.Fatalf("diagnostic omitted fixed registration substep: %q", line)
			}
			for _, secret := range []string{"private-operation-id", "private-cluster-id", "private-node-id", "private-wal-token", "private-peer.invalid", "private registration detail"} {
				if strings.Contains(line, secret) {
					t.Fatalf("diagnostic leaked %q: %q", secret, line)
				}
			}
		})
	}
}

func TestPreparedLearnerAndRegisteredAddUseSameMemberProfile(t *testing.T) {
	ctx := context.Background()
	n, _, bucket, _ := newMembershipOperationNode(t)
	protocol := sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol
	n.config.ObjStorePrefix = "managed-member-profile-regression"
	for i := range n.config.Members {
		n.config.Members[i].PeerURL = "quic://" + string(n.config.Members[i].ID) + ":9090"
	}
	physicalBinding, memberDigest, err := n.preparedBindingDigestsForProtocol(protocol)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapDigest, err := n.ManagedBootstrapPlanDigestForProtocol(physicalBinding, protocol, protocol)
	if err != nil {
		t.Fatal(err)
	}
	n.managedStartup = &ManagedStartupConfig{Identity: ManagedStartupIdentity{
		ApplicationProtocol: protocol,
		EvidenceFormat:      protocol,
		PhysicalBinding:     physicalBinding,
		BootstrapPlanDigest: bootstrapDigest,
		Member: recovery.PreparedStartupIdentity{
			NodeID: string(n.config.NodeID), ClusterUID: "fixture-cluster", DeploymentUID: "fixture-deployment",
			Generation: 0, SourceBinding: physicalBinding, ConfigDigest: memberDigest,
		},
	}}

	// Establish the disposable namespace through the production enrollment path
	// so candidate registration checks see a real immutable bootstrap record.
	seedConfig := *n.config
	seedDir := t.TempDir()
	seedConfig.DataDir = seedDir
	seedWAL, err := qlog.Open(filepath.Join(seedDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	if err := seedWAL.Close(); err != nil {
		t.Fatal(err)
	}
	seedState, err := loadVoterIdentity(&seedConfig)
	if err != nil {
		t.Fatal(err)
	}
	seedWAL, err = qlog.Open(filepath.Join(seedDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = seedWAL.Close() })
	seedProfile, err := n.managedVoterProfileFor(&seedConfig)
	if err != nil || seedProfile == nil {
		t.Fatalf("bootstrap voter profile: profile=%+v err=%v", seedProfile, err)
	}
	if err := ensureVoterIdentityWithProfile(ctx, &seedConfig, bucket, seedWAL, seedState, true, false, seedProfile); err != nil {
		t.Fatalf("create disposable bootstrap registration: %v", err)
	}

	preparedConfig := *n.config
	preparedConfig.NodeID = "profile-learner"
	preparedConfig.Learner = &quepaxa.Member{ID: "profile-learner", PublicKey: peerMember("cluster", "profile-learner", "fresh-token").PublicKey, PeerURL: "https://profile-learner"}
	learnerDir := t.TempDir()
	preparedConfig.DataDir = learnerDir
	preparedProfile, err := n.managedVoterProfileFor(&preparedConfig)
	if err != nil || preparedProfile == nil {
		t.Fatalf("prepared learner profile: profile=%+v err=%v", preparedProfile, err)
	}
	local, err := loadVoterIdentity(&types.ExecutionConfig{DataDir: learnerDir})
	if err != nil {
		t.Fatal(err)
	}
	wal, err := qlog.Open(filepath.Join(learnerDir, "qlog"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	if err := ensureVoterIdentityWithProfile(ctx, &preparedConfig, bucket, wal, local, false, true, preparedProfile); err != nil {
		t.Fatalf("create actual prepared learner registration: %v", err)
	}
	registrationKey := path.Join(preparedConfig.ObjStorePrefix, string(preparedConfig.ClusterID), "voters", fmt.Sprintf("%x.json", sha256.Sum256([]byte(preparedConfig.NodeID))))
	registrationData, err := readVoterRegistration(ctx, bucket, registrationKey)
	if err != nil {
		t.Fatal(err)
	}
	var registration voterIdentity
	if err := json.Unmarshal(registrationData, &registration); err != nil {
		t.Fatal(err)
	}
	if registration.MemberConfigDigest != preparedProfile.MemberConfigDigest || registration.Nonce == "" {
		t.Fatalf("created registration does not retain its production profile: digest-match=%t nonce-present=%t", registration.MemberConfigDigest == preparedProfile.MemberConfigDigest, registration.Nonce != "")
	}

	// This is the same configuration construction used by changeMembership:
	// request.Add is the registered member and therefore has a non-empty WAL ID.
	addConfig := *n.config
	addConfig.NodeID = quepaxa.NodeID(registration.Node)
	addConfig.Learner = &quepaxa.Member{ID: addConfig.NodeID, PublicKey: preparedConfig.Learner.PublicKey, PeerURL: preparedConfig.Learner.PeerURL, WALIdentity: registration.Nonce}
	originalLearner := *addConfig.Learner
	addProfile, err := n.managedLearnerIntentProfileFor(&addConfig, managedLearnerProfileForRepairAdd)
	if err != nil || addProfile == nil {
		t.Fatalf("ADD candidate profile: profile=%+v err=%v", addProfile, err)
	}
	if preparedProfile.MemberConfigDigest != addProfile.MemberConfigDigest || registration.MemberConfigDigest != addProfile.MemberConfigDigest {
		t.Fatalf("prepared registration differs from validated ADD intent profile: prepared=%s registration=%s ADD=%s", preparedProfile.MemberConfigDigest, registration.MemberConfigDigest, addProfile.MemberConfigDigest)
	}
	formerLearnerProfile, err := n.managedLearnerIntentProfileFor(&addConfig, managedLearnerProfileForFormerVoterLookup)
	if err != nil || formerLearnerProfile == nil || formerLearnerProfile.MemberConfigDigest != registration.MemberConfigDigest {
		t.Fatalf("former-learner lookup profile differs from registration: profile=%+v err=%v registration=%s", formerLearnerProfile, err, registration.MemberConfigDigest)
	}
	if *addConfig.Learner != originalLearner {
		t.Fatal("intent profile projection mutated the candidate member")
	}
	if _, err := n.managedLearnerIntentProfileFor(&addConfig, managedLearnerProfilePurpose(255)); err == nil {
		t.Fatal("unknown intent profile purpose accepted")
	}
	if err := n.validateRegisteredVoterWAL(ctx, addConfig.NodeID, registration.Nonce); err != nil {
		t.Fatalf("exact registered WAL identity rejected: %v", err)
	}
	if err := n.validateRegisteredVoterWAL(ctx, addConfig.NodeID, strings.Repeat("f", 64)); err == nil {
		t.Fatal("wrong registered WAL identity was accepted")
	}

	for _, test := range []struct {
		name   string
		change func(*types.ExecutionConfig)
	}{
		{name: "candidate ID", change: func(config *types.ExecutionConfig) {
			config.NodeID = "different-learner"
			config.Learner.ID = config.NodeID
		}},
		{name: "candidate key", change: func(config *types.ExecutionConfig) { config.Learner.PublicKey[0] ^= 1 }},
		{name: "candidate URL", change: func(config *types.ExecutionConfig) { config.Learner.PeerURL += "/different" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := addConfig
			learner := *addConfig.Learner
			changed.Learner = &learner
			test.change(&changed)
			profile, err := n.managedLearnerIntentProfileFor(&changed, managedLearnerProfileForRepairAdd)
			if err != nil {
				t.Fatalf("compute changed identity profile: %v", err)
			}
			if profile.MemberConfigDigest == registration.MemberConfigDigest {
				t.Fatal("changed learner intent matched the registered profile")
			}
		})
	}

	fixedWAL := addConfig
	fixedLearner := *addConfig.Learner
	fixedLearner.WALIdentity = strings.Repeat("a", 64)
	fixedWAL.Learner = &fixedLearner
	fixedProfile, err := n.managedVoterProfileFor(&fixedWAL)
	if err != nil || fixedProfile == nil || fixedProfile.MemberConfigDigest == registration.MemberConfigDigest {
		t.Fatalf("ordinary fixed-WAL profile was normalized: profile=%+v err=%v", fixedProfile, err)
	}

	originalIdentity := n.managedStartup.Identity
	n.managedStartup.Identity.ApplicationProtocol = types.ManagedApplicationProtocolSQLKV
	if _, err := n.managedLearnerIntentProfileFor(&addConfig, managedLearnerProfileForRepairAdd); err == nil {
		t.Fatal("non-append-only profile accepted learner intent projection")
	}
	n.managedStartup.Identity = originalIdentity
	n.managedStartup.Identity.ApplicationProtocol = sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol
	otherProtocolBootstrap, err := n.ManagedBootstrapPlanDigestForProtocol(physicalBinding, originalIdentity.EvidenceFormat, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	n.managedStartup.Identity.BootstrapPlanDigest = otherProtocolBootstrap
	protocolProfile, err := n.managedLearnerIntentProfileFor(&addConfig, managedLearnerProfileForRepairAdd)
	if err != nil {
		if err.Error() != "managed voter original bootstrap profile differs" {
			t.Fatalf("changed protocol failed for an unexpected reason: %v", err)
		}
	} else if protocolProfile == nil || protocolProfile.MemberConfigDigest == registration.MemberConfigDigest {
		t.Fatalf("changed protocol matched the registered profile: profile=%+v err=%v", protocolProfile, err)
	}
	n.managedStartup.Identity = originalIdentity

	n.managedStartup.Identity.Member.Generation = 1
	if _, err := n.managedLearnerIntentProfileFor(&addConfig, managedLearnerProfileForRepairAdd); err == nil {
		t.Fatal("non-bootstrap generation accepted for learner intent profile")
	}
	n.managedStartup.Identity = originalIdentity
	n.managedStartup.Identity.BootstrapPlanDigest = strings.Repeat("0", 64)
	if _, err := n.managedLearnerIntentProfileFor(&addConfig, managedLearnerProfileForRepairAdd); err == nil {
		t.Fatal("changed bootstrap binding accepted for learner intent profile")
	}
}

func TestOrdinaryMembershipAddKeepsUnmanagedProfilePath(t *testing.T) {
	ctx := context.Background()
	n, _, _, _ := newMembershipOperationNode(t)
	learner := registerMembershipLearner(t, n.bucket, "ordinary-fresh")
	request := network.MembershipChange{OperationID: "ordinary-add", ClusterID: string(n.config.ClusterID), ExpectedConfigID: 1, Add: &learner}
	if err := n.changeMembership(ctx, request); err == nil {
		t.Fatal("ordinary addition unexpectedly completed without an admission callback")
	} else if strings.Contains(err.Error(), "verified managed learner intent required") {
		t.Fatalf("ordinary addition was routed through the append-only intent helper: %v", err)
	}
	if pending, ok := n.core.PendingReconfiguration(); !ok || pending.ConfigID != 2 {
		t.Fatalf("ordinary addition did not reach the existing proposal lifecycle: pending=%+v ok=%v", pending, ok)
	}
}

func TestManagedMembershipStageDiagnosticRequiresManagedAddPermit(t *testing.T) {
	request := network.MembershipChange{Add: &quepaxa.Member{ID: "node"}}
	managedAdd := context.WithValue(context.Background(), managedMembershipPermitKey{}, managedMembershipPermit{
		request: ManagedMembershipRequest{Action: ManagedMembershipAdd},
		change:  request,
	})
	ordinaryAdd := context.Background()
	wrongChange := context.WithValue(context.Background(), managedMembershipPermitKey{}, managedMembershipPermit{
		request: ManagedMembershipRequest{Action: ManagedMembershipAdd},
		change:  network.MembershipChange{Add: &quepaxa.Member{ID: "different-node"}},
	})
	managedRemove := context.WithValue(context.Background(), managedMembershipPermitKey{}, managedMembershipPermit{
		request: ManagedMembershipRequest{Action: ManagedMembershipRemove},
		change:  request,
	})

	for _, test := range []struct {
		name string
		ctx  context.Context
		want bool
	}{
		{name: "managed add", ctx: managedAdd, want: true},
		{name: "ordinary add", ctx: ordinaryAdd},
		{name: "mismatched permit", ctx: wrongChange},
		{name: "managed remove", ctx: managedRemove},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			previousOutput := log.Writer()
			log.SetOutput(&output)
			t.Cleanup(func() { log.SetOutput(previousOutput) })
			logManagedMembershipStageFailure(test.ctx, request, managedMembershipStageStorageCAS, errors.New("unavailable"))
			got := strings.Contains(output.String(), "MANAGED_MEMBERSHIP_STAGE_FAILURE")
			if got != test.want {
				t.Fatalf("diagnostic emitted=%v, want %v; output=%q", got, test.want, output.String())
			}
		})
	}
}
