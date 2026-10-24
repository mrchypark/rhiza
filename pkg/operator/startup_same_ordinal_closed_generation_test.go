package operator

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

// This component test exercises the real canonical same-ordinal resolver and
// authenticated registration handler after a direct canonical-ledger close.
// It does not claim the production assessment/dispatch/executor sequence; that
// sequence is exercised by TestStartupAfterAutomaticGenerationEscalationIsNotAdmitted.
func TestSameOrdinalRestartAfterGenerationClosureDeniedEarly(t *testing.T) {
	runAuthenticatedLearnerPromotionRetriesExactCanonicalCompletion(t, func(t *testing.T, setup authenticatedPendingLearnerSetup) {
		ctx, f := setup.ctx, setup.f
		secretPath := f.e.controller().corePath("secrets", setup.intent.LearnerSecretName)
		var secretBefore object
		if err := f.e.Kube.Get(ctx, secretPath, &secretBefore); err != nil {
			t.Fatal(err)
		}
		controlHash, childHash, secretHash := hashJSON(setup.state), hashJSON(setup.child), hashJSON(secretBefore)

		// Close through the ledger writer to isolate the same-ordinal early guard.
		// This preserves the exact pending assignment and models closure before
		// the ordinary same-ordinal process restart.
		const closeOperation = "mixed-generation-close"
		closeHash := strings.Repeat("f", 64)
		if _, err := f.e.change(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
			return ledger.Close(closeOperation, closeHash)
		}); err != nil {
			t.Fatalf("close canonical generation: %v", err)
		}
		rootBefore, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
		if err != nil {
			t.Fatal(err)
		}
		if rootBefore.Ledger.ClosedBy != closeOperation || rootBefore.Ledger.ClosedHash != closeHash {
			t.Fatalf("fixture did not persist generation closure: %+v", rootBefore.Ledger)
		}

		var captured bytes.Buffer
		previousWriter, previousFlags := log.Writer(), log.Flags()
		log.SetOutput(&captured)
		log.SetFlags(0)
		t.Cleanup(func() { log.SetOutput(previousWriter); log.SetFlags(previousFlags) })

		identity := setup.physicalID
		identity.NodeID = identity.PodName
		identity.RunID = strings.Repeat("d", 32)
		status := &statusRecordingTransport{next: setup.client.Client.Transport}
		client := setup.client
		client.Client = &http.Client{Transport: status}
		_, err = client.RegisterStartup(ctx, identity)
		if !errors.Is(err, fencing.ErrClosed) || status.code != http.StatusGone {
			t.Fatalf("restart did not reach closed-ledger rejection (HTTP 410): err=%v status=%d", err, status.code)
		}

		lines := strings.Split(strings.TrimSpace(captured.String()), "\n")
		if len(lines) != 1 {
			t.Fatalf("restart emitted %d denial records, want one: %q", len(lines), captured.String())
		}
		var event startupRegistrationDenialEvent
		if err := json.Unmarshal([]byte(lines[0]), &event); err != nil {
			t.Fatalf("decode actual denial event: %v; line=%q", err, lines[0])
		}
		if event.Event != startupRegistrationDenialEventName || event.Stage != startupRegistrationStageGrant ||
			event.GrantResolved || event.HTTPStatus != http.StatusGone || event.AttemptedRunID != identity.RunID ||
			event.PodUID != identity.PodUID || event.StatefulSetUID != f.runtime.StatefulSetUID ||
			event.SourceClusterID != identity.SourceClusterID || event.RequestedNodeID != identity.NodeID ||
			event.ResolvedNodeID != "" || event.AssignmentID != "" || event.RuntimeContainerID != "" {
			t.Fatalf("event does not prove early denial before resolving/enrolling the closed same-ordinal grant: %+v", event)
		}

		rootAfter, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
		if err != nil {
			t.Fatal(err)
		}
		if hashJSON(rootBefore.Ledger) != hashJSON(rootAfter.Ledger) {
			t.Fatal("denied restart changed the canonical closed ledger")
		}
		storedControl, _, err := setup.controller.loadAuto(ctx, setup.controller.autoKey(&setup.cluster))
		if err != nil {
			t.Fatal(err)
		}
		var childAfter Resource
		if err := f.e.Kube.Get(ctx, f.e.controller().resourcePath(setup.intent.RecoveryName), &childAfter); err != nil {
			t.Fatal(err)
		}
		var secretAfter object
		if err := f.e.Kube.Get(ctx, secretPath, &secretAfter); err != nil {
			t.Fatal(err)
		}
		if hashJSON(storedControl) != controlHash || hashJSON(childAfter) != childHash || hashJSON(secretAfter) != secretHash {
			t.Fatal("restart changed the frozen canonical intent, child, or immutable credential")
		}
	})
}

func TestAbortingSameOrdinalRestartAfterGenerationClosureIsDeniedEarly(t *testing.T) {
	runAuthenticatedLearnerPromotionRetriesExactCanonicalCompletion(t, func(t *testing.T, setup authenticatedPendingLearnerSetup) {
		ctx, f := setup.ctx, setup.f
		secretPath := f.e.controller().corePath("secrets", setup.intent.LearnerSecretName)
		var secretBefore object
		if err := f.e.Kube.Get(ctx, secretPath, &secretBefore); err != nil {
			t.Fatal(err)
		}
		childHash, secretHash := hashJSON(setup.child), hashJSON(secretBefore)

		const closeOperation = "mixed-generation-close-aborting"
		closeHash := strings.Repeat("e", 64)
		if _, err := f.e.change(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
			return ledger.Close(closeOperation, closeHash)
		}); err != nil {
			t.Fatalf("close canonical generation: %v", err)
		}
		rootBefore, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
		if err != nil {
			t.Fatal(err)
		}

		// Model the canonical retirement transition that can race the restart.
		// The resolver must keep this state pending rather than inventing an
		// early closed-ledger denial event.
		setup.intent.Aborting = true
		if err := setup.controller.saveAuto(ctx, &setup.cluster, setup.state, mustAutoVersion(t, setup.controller, &setup.cluster)); err != nil {
			t.Fatalf("persist canonical aborting intent: %v", err)
		}
		abortingControlHash := hashJSON(setup.state)

		var captured bytes.Buffer
		previousWriter, previousFlags := log.Writer(), log.Flags()
		log.SetOutput(&captured)
		log.SetFlags(0)
		t.Cleanup(func() { log.SetOutput(previousWriter); log.SetFlags(previousFlags) })

		identity := setup.physicalID
		identity.NodeID = identity.PodName
		identity.RunID = strings.Repeat("c", 32)
		status := &statusRecordingTransport{next: setup.client.Client.Transport}
		client := setup.client
		client.Client = &http.Client{Transport: status}
		_, err = client.RegisterStartup(ctx, identity)
		if !errors.Is(err, fencing.ErrClosed) || status.code != http.StatusGone {
			t.Fatalf("closed generation must deny an aborting restart (HTTP 410), got err=%v status=%d", err, status.code)
		}
		lines := strings.Split(strings.TrimSpace(captured.String()), "\n")
		if len(lines) != 1 {
			t.Fatalf("early closed aborting restart emitted %d denial records, want one: %q", len(lines), captured.String())
		}
		var event startupRegistrationDenialEvent
		if err := json.Unmarshal([]byte(lines[0]), &event); err != nil {
			t.Fatalf("decode early closed aborting denial: %v; %q", err, lines[0])
		}
		if event.Stage != startupRegistrationStageGrant || event.HTTPStatus != http.StatusGone || event.GrantResolved ||
			event.AttemptedRunID != identity.RunID || event.PodUID != identity.PodUID || event.ResolvedNodeID != "" ||
			event.AssignmentID != "" || event.RuntimeContainerID != "" {
			t.Fatalf("early closed aborting event resolved/enrolled the old assignment: %+v", event)
		}

		rootAfter, _, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
		if err != nil {
			t.Fatal(err)
		}
		storedControl, _, err := setup.controller.loadAuto(ctx, setup.controller.autoKey(&setup.cluster))
		if err != nil {
			t.Fatal(err)
		}
		var childAfter Resource
		if err := f.e.Kube.Get(ctx, f.e.controller().resourcePath(setup.intent.RecoveryName), &childAfter); err != nil {
			t.Fatal(err)
		}
		var secretAfter object
		if err := f.e.Kube.Get(ctx, secretPath, &secretAfter); err != nil {
			t.Fatal(err)
		}
		if hashJSON(rootBefore.Ledger) != hashJSON(rootAfter.Ledger) || hashJSON(storedControl) != abortingControlHash ||
			hashJSON(childAfter) != childHash || hashJSON(secretAfter) != secretHash {
			t.Fatal("pending aborting restart changed canonical ledger, aborting intent, child, or immutable credential")
		}
		if !storedControl.Intent.Aborting {
			t.Fatal("pending restart did not retain the canonical aborting intent")
		}
	})
}

func TestAbortingSameOrdinalRestartOnOpenGenerationRemainsPending(t *testing.T) {
	runAuthenticatedLearnerPromotionRetriesExactCanonicalCompletion(t, func(t *testing.T, setup authenticatedPendingLearnerSetup) {
		ctx := setup.ctx
		setup.intent.Aborting = true
		if err := setup.controller.saveAuto(ctx, &setup.cluster, setup.state, mustAutoVersion(t, setup.controller, &setup.cluster)); err != nil {
			t.Fatalf("persist canonical aborting intent: %v", err)
		}
		var captured bytes.Buffer
		previousWriter, previousFlags := log.Writer(), log.Flags()
		log.SetOutput(&captured)
		log.SetFlags(0)
		t.Cleanup(func() { log.SetOutput(previousWriter); log.SetFlags(previousFlags) })
		identity := setup.physicalID
		identity.NodeID = identity.PodName
		identity.RunID = strings.Repeat("b", 32)
		status := &statusRecordingTransport{next: setup.client.Client.Transport}
		client := setup.client
		client.Client = &http.Client{Transport: status}
		_, err := client.RegisterStartup(ctx, identity)
		if !errors.Is(err, fencing.ErrPending) || status.code != http.StatusAccepted {
			t.Fatalf("open generation aborting assignment must remain pending: status=%d err=%v", status.code, err)
		}
		if captured.Len() != 0 {
			t.Fatalf("open pending restart emitted a closed-generation denial: %q", captured.String())
		}
		root, _, err := setup.f.e.AuthorityState.ReadRoot(ctx, setup.f.runtime.StatefulSetUID, setup.f.runtime.SourceClusterID)
		if err != nil || root.Ledger.ClosedBy != "" {
			t.Fatalf("pending open-generation registration changed closure state: closedBy=%q err=%v", root.Ledger.ClosedBy, err)
		}
	})
}
