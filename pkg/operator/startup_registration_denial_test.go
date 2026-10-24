package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

func TestAuthenticatedClosedRegistrationEmitsOneCorrelatedEvent(t *testing.T) {
	f := newSupervisorRegistrationFixture(t, true)
	ctx := t.Context()
	if err := f.e.initializeRoot(ctx, "sts-uid", f.identity.SourceClusterID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.e.change(ctx, "sts-uid", f.identity.SourceClusterID, func(ledger *fencing.Ledger) error {
		return ledger.Close("closed-operation", strings.Repeat("a", 64))
	}); err != nil {
		t.Fatal(err)
	}

	var captured bytes.Buffer
	previousWriter, previousFlags := log.Writer(), log.Flags()
	log.SetOutput(&captured)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(previousWriter); log.SetFlags(previousFlags) })

	status := &statusRecordingTransport{next: f.server.Client().Transport}
	client := fencing.AuthorityClient{URL: f.server.URL + "/v1/supervisor", TokenFile: f.token, Client: &http.Client{Transport: status}}
	_, err := client.RegisterStartup(ctx, f.identity)
	if !errors.Is(err, fencing.ErrClosed) || status.code != http.StatusGone {
		t.Fatalf("real client did not preserve the authenticated HTTP 410 as ErrClosed: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(captured.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("closed register emitted %d log records, want one: %q", len(lines), captured.String())
	}
	var event startupRegistrationDenialEvent
	if err := json.Unmarshal([]byte(lines[0]), &event); err != nil {
		t.Fatalf("decode structured denial event: %v; line=%q", err, lines[0])
	}
	if event.Event != startupRegistrationDenialEventName || event.Action != "register" || event.Result != "closed" || event.HTTPStatus != http.StatusGone ||
		event.Stage != startupRegistrationStageGrant || event.Namespace != "test" || event.StatefulSetUID != "sts-uid" ||
		event.PodUID != f.identity.PodUID || event.SourceClusterID != f.identity.SourceClusterID || event.AttemptedRunID != f.identity.RunID ||
		event.RequestedNodeID != f.identity.NodeID || event.GrantResolved || event.ResolvedNodeID != "" || event.AssignmentID != "" ||
		event.RuntimeContainerID != "" {
		t.Fatalf("event fields do not identify the actual authenticated closed registration: %+v", event)
	}
	root, _, err := f.state.ReadRoot(ctx, "sts-uid", f.identity.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if root.Ledger.ClosedBy != "closed-operation" || len(root.Ledger.Runs) != 0 {
		t.Fatalf("closed registration admitted a canonical run or changed closure: %+v", root.Ledger)
	}
}

func TestNonRegisterClosedActionDoesNotEmitRegistrationDenial(t *testing.T) {
	f := newSupervisorRegistrationFixture(t, false)
	ctx := t.Context()
	if err := f.e.initializeRoot(ctx, "sts-uid", f.identity.SourceClusterID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.e.change(ctx, "sts-uid", f.identity.SourceClusterID, func(ledger *fencing.Ledger) error {
		return ledger.Close("closed-operation", strings.Repeat("b", 64))
	}); err != nil {
		t.Fatal(err)
	}
	var captured bytes.Buffer
	previousWriter, previousFlags := log.Writer(), log.Flags()
	log.SetOutput(&captured)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(previousWriter); log.SetFlags(previousFlags) })
	client := fencing.AuthorityClient{URL: f.server.URL + "/v1/supervisor", TokenFile: f.token, Client: f.server.Client()}
	// This fixture is intentionally not a canonical pending learner, so the
	// handler rejects the identity action before the closed-ledger branch. It
	// still proves that non-register actions cannot produce register events.
	err := client.RegisterLearnerIdentity(ctx, f.identity, fencing.LearnerIdentityRegistration{Key: "learner-key", Data: []byte("{}")})
	if err == nil {
		t.Fatal("invalid learner-identity unexpectedly succeeded")
	}
	if strings.Contains(captured.String(), startupRegistrationDenialEventName) {
		t.Fatalf("learner-identity 410 was misreported as register denial: %q", captured.String())
	}
}

func TestMalformedRegisterAuxiliaryPayloadsFailBeforeClosedAuthoritySideEffects(t *testing.T) {
	types := []struct {
		name    string
		message func(fencing.Identity) fencing.AuthorityMessage
	}{
		{"drain", func(id fencing.Identity) fencing.AuthorityMessage {
			return fencing.AuthorityMessage{Action: "register", Identity: id, Drain: &fencing.DrainReceipt{}}
		}},
		{"stop-request", func(id fencing.Identity) fencing.AuthorityMessage {
			return fencing.AuthorityMessage{Action: "register", Identity: id, Request: &fencing.StopRequest{}}
		}},
		{"mutation-lookup", func(id fencing.Identity) fencing.AuthorityMessage {
			return fencing.AuthorityMessage{Action: "register", Identity: id, MutationLookup: &fencing.MutationLookupRequest{}}
		}},
		{"archive-genesis", func(id fencing.Identity) fencing.AuthorityMessage {
			return fencing.AuthorityMessage{Action: "register", Identity: id, ArchiveGenesis: &fencing.ArchiveGenesisStatusRequest{}}
		}},
		{"learner-identity", func(id fencing.Identity) fencing.AuthorityMessage {
			return fencing.AuthorityMessage{Action: "register", Identity: id, LearnerIdentity: &fencing.LearnerIdentityRegistration{Key: "unexpected", Data: []byte("{}")}}
		}},
	}
	for _, tt := range types {
		t.Run(tt.name, func(t *testing.T) {
			f := newSupervisorRegistrationFixture(t, true)
			ctx := t.Context()
			if err := f.e.initializeRoot(ctx, "sts-uid", f.identity.SourceClusterID); err != nil {
				t.Fatal(err)
			}
			if _, err := f.e.change(ctx, "sts-uid", f.identity.SourceClusterID, func(ledger *fencing.Ledger) error {
				return ledger.Close("closed-operation", strings.Repeat("e", 64))
			}); err != nil {
				t.Fatal(err)
			}
			before, _, err := f.state.ReadRoot(ctx, "sts-uid", f.identity.SourceClusterID)
			if err != nil {
				t.Fatal(err)
			}
			var captured bytes.Buffer
			previousWriter, previousFlags := log.Writer(), log.Flags()
			log.SetOutput(&captured)
			log.SetFlags(0)
			t.Cleanup(func() { log.SetOutput(previousWriter); log.SetFlags(previousFlags) })
			body, err := json.Marshal(tt.message(f.identity))
			if err != nil {
				t.Fatal(err)
			}
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, f.server.URL+"/v1/supervisor", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Authorization", "Bearer test")
			request.Header.Set("Content-Type", "application/json")
			response, err := f.server.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != http.StatusBadRequest {
				t.Fatalf("malformed register %s returned %d, want 400", tt.name, response.StatusCode)
			}
			if captured.Len() != 0 {
				t.Fatalf("malformed register %s emitted a denial diagnostic: %q", tt.name, captured.String())
			}
			after, _, err := f.state.ReadRoot(ctx, "sts-uid", f.identity.SourceClusterID)
			if err != nil {
				t.Fatal(err)
			}
			if hashJSON(before) != hashJSON(after) {
				t.Fatalf("malformed register %s changed canonical authority", tt.name)
			}
		})
	}
}

type closedLearnerIdentityBucket struct{ objstore.Bucket }

type startupRootReadErrorBucket struct{ objstore.Bucket }

func (b startupRootReadErrorBucket) Get(context.Context, string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("injected canonical root read failure")
}

func TestClosedGenerationStartupGuardRequiresExactGlobalCanonicalClosure(t *testing.T) {
	t.Run("open root continues", func(t *testing.T) {
		f := newSupervisorRegistrationFixture(t, false)
		if err := f.e.initializeRoot(t.Context(), "sts-uid", f.identity.SourceClusterID); err != nil {
			t.Fatal(err)
		}
		if err := f.e.rejectClosedCanonicalStartup(t.Context(), "sts-uid", f.identity.SourceClusterID); err != nil {
			t.Fatalf("open canonical root was rejected: %v", err)
		}
	})
	t.Run("node-only closure is not global", func(t *testing.T) {
		f := newSupervisorRegistrationFixture(t, false)
		if err := f.e.initializeRoot(t.Context(), "sts-uid", f.identity.SourceClusterID); err != nil {
			t.Fatal(err)
		}
		if _, err := f.e.change(t.Context(), "sts-uid", f.identity.SourceClusterID, func(ledger *fencing.Ledger) error {
			return ledger.CloseNodes("old-voter", strings.Repeat("a", 64), []string{"old-node"})
		}); err != nil {
			t.Fatal(err)
		}
		if err := f.e.rejectClosedCanonicalStartup(t.Context(), "sts-uid", f.identity.SourceClusterID); err != nil {
			t.Fatalf("ClosedNodes-only history was treated as a global closure: %v", err)
		}
	})
	t.Run("exact global closure is closed", func(t *testing.T) {
		f := newSupervisorRegistrationFixture(t, false)
		if err := f.e.initializeRoot(t.Context(), "sts-uid", f.identity.SourceClusterID); err != nil {
			t.Fatal(err)
		}
		if _, err := f.e.change(t.Context(), "sts-uid", f.identity.SourceClusterID, func(ledger *fencing.Ledger) error {
			return ledger.Close("generation-fence", strings.Repeat("b", 64))
		}); err != nil {
			t.Fatal(err)
		}
		if err := f.e.rejectClosedCanonicalStartup(t.Context(), "sts-uid", f.identity.SourceClusterID); !errors.Is(err, fencing.ErrClosed) {
			t.Fatalf("exact global closure = %v, want ErrClosed", err)
		}
	})
	t.Run("partial or malformed closure fails without 410", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			by   string
			hash string
		}{{name: "missing hash", by: "generation-fence"}, {name: "malformed hash", by: "generation-fence", hash: "not-a-digest"}, {name: "hash without operation", hash: strings.Repeat("c", 64)}} {
			t.Run(tc.name, func(t *testing.T) {
				f := newSupervisorRegistrationFixture(t, false)
				if err := f.e.initializeRoot(t.Context(), "sts-uid", f.identity.SourceClusterID); err != nil {
					t.Fatal(err)
				}
				state, version, err := f.state.ReadRoot(t.Context(), "sts-uid", f.identity.SourceClusterID)
				if err != nil {
					t.Fatal(err)
				}
				state.Ledger.ClosedBy, state.Ledger.ClosedHash = tc.by, tc.hash
				if err := f.state.WriteRoot(t.Context(), "sts-uid", f.identity.SourceClusterID, state, version); err != nil {
					t.Fatal(err)
				}
				err = f.e.rejectClosedCanonicalStartup(t.Context(), "sts-uid", f.identity.SourceClusterID)
				if err == nil || errors.Is(err, fencing.ErrClosed) {
					t.Fatalf("malformed closure = %v, want non-ErrClosed rejection", err)
				}
			})
		}
	})
	t.Run("wrong source and missing root are not closed", func(t *testing.T) {
		f := newSupervisorRegistrationFixture(t, false)
		if err := f.e.initializeRoot(t.Context(), "sts-uid", f.identity.SourceClusterID); err != nil {
			t.Fatal(err)
		}
		if err := f.e.rejectClosedCanonicalStartup(t.Context(), "sts-uid", "different-source"); err == nil || errors.Is(err, fencing.ErrClosed) {
			t.Fatalf("wrong source result = %v, want a non-closed error", err)
		}
		bucket := objstore.NewInMemBucket()
		t.Cleanup(func() { _ = bucket.Close() })
		store, err := NewAuthorityStateStore(bucket, "missing-root")
		if err != nil {
			t.Fatal(err)
		}
		f.e.AuthorityState = store
		if err := f.e.rejectClosedCanonicalStartup(t.Context(), "sts-uid", f.identity.SourceClusterID); err == nil || errors.Is(err, fencing.ErrClosed) {
			t.Fatalf("missing root result = %v, want a non-closed error", err)
		}
	})
	t.Run("read failure is not closed", func(t *testing.T) {
		f := newSupervisorRegistrationFixture(t, false)
		if err := f.e.initializeRoot(t.Context(), "sts-uid", f.identity.SourceClusterID); err != nil {
			t.Fatal(err)
		}
		f.state.bucket = startupRootReadErrorBucket{Bucket: f.state.bucket}
		if err := f.e.rejectClosedCanonicalStartup(t.Context(), "sts-uid", f.identity.SourceClusterID); err == nil || errors.Is(err, fencing.ErrClosed) {
			t.Fatalf("root read failure = %v, want a non-closed error", err)
		}
	})
}

func (b closedLearnerIdentityBucket) Upload(_ context.Context, _ string, _ io.Reader, _ ...objstore.ObjectUploadOption) error {
	return fencing.ErrClosed
}

func TestCanonicalPendingLearnerIdentity410DoesNotEmitRegisterDenial(t *testing.T) {
	runAuthenticatedLearnerPromotionRetriesExactCanonicalCompletion(t, func(t *testing.T, fixture authenticatedPendingLearnerSetup) {
		id := fixture.physicalID
		id.WALIdentity = fixture.intent.LearnerWAL
		if _, err := fixture.client.Bind(fixture.ctx, id); err != nil {
			t.Fatalf("bind canonical pending learner WAL: %v", err)
		}
		f := fixture.f
		var pod object
		if err := f.e.Kube.Get(fixture.ctx, f.e.controller().corePath("pods", id.PodName), &pod); err != nil {
			t.Fatal(err)
		}
		ctr := asObject(list(nested(pod, "spec", "containers"))[0])
		env, err := f.e.controller().environment(fixture.ctx, ctr)
		if err != nil {
			t.Fatal(err)
		}
		members, err := recoveryMembers(env["RHIZA_CLUSTER_MEMBERS"])
		if err != nil {
			t.Fatal(err)
		}
		durability := env["RHIZA_OBJSTORE_DURABILITY"]
		if durability == "" {
			durability = "async"
		}
		memberRecord := recovery.NewMembershipRecord(id.SourceClusterID, members, durability)
		storeData, err := json.Marshal([]string{env["RHIZA_OBJSTORE_PROVIDER"], env["RHIZA_OBJSTORE_ENDPOINT"], env["RHIZA_OBJSTORE_BUCKET"], path.Clean(env["RHIZA_OBJSTORE_PREFIX"]), env["RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT"]})
		if err != nil {
			t.Fatal(err)
		}
		storeDigest := sha256.Sum256(storeData)
		var secret object
		if err := f.e.Kube.Get(fixture.ctx, f.e.controller().corePath("secrets", fixture.intent.LearnerSecretName), &secret); err != nil {
			t.Fatal(err)
		}
		memberBytes, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "member")))
		if err != nil {
			t.Fatal(err)
		}
		var member quepaxa.Member
		if err := json.Unmarshal(memberBytes, &member); err != nil {
			t.Fatal(err)
		}
		record := learnerVoterIdentityRecord{Version: 2, Cluster: id.SourceClusterID, Node: id.NodeID,
			Membership: memberRecord.Membership, Store: hex.EncodeToString(storeDigest[:]), Nonce: id.WALIdentity,
			Registered: true, LearnerPublicKey: fmt.Sprintf("%x", member.PublicKey[:])}
		data, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		key := path.Join(env["RHIZA_OBJSTORE_PREFIX"], id.SourceClusterID, "voters", fmt.Sprintf("%x.json", sha256.Sum256([]byte(id.NodeID))))
		f.e.ArchiveBucket = closedLearnerIdentityBucket{Bucket: f.e.ArchiveBucket}
		var captured bytes.Buffer
		previousWriter, previousFlags := log.Writer(), log.Flags()
		log.SetOutput(&captured)
		log.SetFlags(0)
		t.Cleanup(func() { log.SetOutput(previousWriter); log.SetFlags(previousFlags) })
		status := &statusRecordingTransport{next: fixture.client.Client.Transport}
		client := fencing.AuthorityClient{URL: fixture.client.URL, TokenFile: fixture.client.TokenFile, Client: &http.Client{Transport: status}}
		err = client.RegisterLearnerIdentity(fixture.ctx, id, fencing.LearnerIdentityRegistration{Key: key, Data: data})
		if !errors.Is(err, fencing.ErrClosed) || status.code != http.StatusGone {
			t.Fatalf("canonical pending learner identity did not retain its separate HTTP 410: err=%v status=%d", err, status.code)
		}
		if strings.Contains(captured.String(), startupRegistrationDenialEventName) {
			t.Fatalf("learner-identity 410 was misreported as register denial: %q", captured.String())
		}
	})
}

type registrationCASBarrierBucket struct {
	objstore.Bucket
	mu      sync.Mutex
	held    int
	reached chan struct{}
	release chan struct{}
}

func (b *registrationCASBarrierBucket) Upload(ctx context.Context, name string, reader io.Reader, options ...objstore.ObjectUploadOption) error {
	params := objstore.ApplyObjectUploadOptions(options...)
	if params.Condition != nil {
		b.mu.Lock()
		pause := b.held < 2
		if pause {
			b.held++
		}
		b.mu.Unlock()
		if pause {
			b.reached <- struct{}{}
			select {
			case <-b.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return b.Bucket.Upload(ctx, name, reader, options...)
}

func TestConcurrentClosedRegistrationCASRetriesEmitOneEventPerIdentity(t *testing.T) {
	f := newSupervisorRegistrationFixture(t, false)
	ctx := t.Context()
	if err := f.e.initializeRoot(ctx, "sts-uid", f.identity.SourceClusterID); err != nil {
		t.Fatal(err)
	}
	barrier := &registrationCASBarrierBucket{Bucket: f.state.bucket, reached: make(chan struct{}, 2), release: make(chan struct{})}
	f.state.bucket = barrier
	f.e.ArchiveBucket = barrier

	var captured bytes.Buffer
	previousWriter, previousFlags := log.Writer(), log.Flags()
	log.SetOutput(&captured)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(previousWriter); log.SetFlags(previousFlags) })
	type result struct {
		run string
		err error
	}
	results := make(chan result, 2)
	client := fencing.AuthorityClient{URL: f.server.URL + "/v1/supervisor", TokenFile: f.token, Client: f.server.Client()}
	for _, run := range []string{"run-left", "run-right"} {
		identity := f.identity
		identity.RunID = run
		go func(identity fencing.Identity) {
			_, err := client.RegisterStartup(ctx, identity)
			results <- result{run: identity.RunID, err: err}
		}(identity)
	}
	for range 2 {
		select {
		case <-barrier.reached:
		case <-time.After(10 * time.Second):
			close(barrier.release)
			t.Fatal("registration requests did not reach concurrent root CAS attempts")
		}
	}
	if _, err := f.e.change(ctx, "sts-uid", f.identity.SourceClusterID, func(ledger *fencing.Ledger) error {
		return ledger.Close("concurrent-close", strings.Repeat("f", 64))
	}); err != nil {
		close(barrier.release)
		t.Fatalf("close authority between registration CAS attempts and their retries: %v", err)
	}
	close(barrier.release)
	for range 2 {
		got := <-results
		if !errors.Is(got.err, fencing.ErrClosed) {
			t.Fatalf("registration %s did not observe closed state after CAS retry: %v", got.run, got.err)
		}
	}
	lines := strings.Split(strings.TrimSpace(captured.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("concurrent CAS retries emitted %d records, want one per request: %q", len(lines), captured.String())
	}
	seen := map[string]bool{}
	for _, line := range lines {
		var event startupRegistrationDenialEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("decode handler event: %v; %q", err, line)
		}
		if event.Stage != startupRegistrationStageLedgerRegister || !event.GrantResolved || event.HTTPStatus != http.StatusGone ||
			(event.AttemptedRunID != "run-left" && event.AttemptedRunID != "run-right") || seen[event.AttemptedRunID] {
			t.Fatalf("CAS retry event has wrong stage, identity, or multiplicity: %+v", event)
		}
		seen[event.AttemptedRunID] = true
	}
	if !seen["run-left"] || !seen["run-right"] {
		t.Fatalf("request identities were lost or mixed in concurrent events: %+v", seen)
	}
}

func TestStartupRegistrationDenialEncodingEscapesAndOmitsOversizedIdentity(t *testing.T) {
	marker := "run-\n{\"event\":\"startup_registration_denial\"}"
	line, err := encodeStartupRegistrationDenial(startupRegistrationDenialEvent{
		Stage: startupRegistrationStageLedgerRegister, AttemptedRunID: marker, RequestedNodeID: "physical\"node",
		GrantResolved: true, ResolvedNodeID: "logical-node", AssignmentID: strings.Repeat("a", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(line, "\n") || strings.Contains(line, `"event":"startup_registration_denial"}","event"`) || len(line)+1 > startupRegistrationDenialMaxLine {
		t.Fatalf("event JSON is multiline, forgeable, or unbounded: %q", line)
	}
	var decoded startupRegistrationDenialEvent
	if err := json.Unmarshal([]byte(line), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.AttemptedRunID != marker || decoded.RequestedNodeID != "physical\"node" || decoded.ResolvedNodeID != "logical-node" {
		t.Fatalf("JSON escaping or requested/resolved identity separation changed values: %+v", decoded)
	}
	oversized, err := encodeStartupRegistrationDenial(startupRegistrationDenialEvent{
		Stage: startupRegistrationStageLedgerRegister, AttemptedRunID: strings.Repeat("x", 129), GrantResolved: true,
		ResolvedNodeID: "resolved", AssignmentID: strings.Repeat("c", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	decoded = startupRegistrationDenialEvent{}
	if err := json.Unmarshal([]byte(oversized), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.AttemptedRunID != "" || decoded.OmittedReason != "identity_field_oversize" {
		t.Fatalf("oversized identity was truncated into a false correlation: %+v", decoded)
	}
	if got := safeFenceLogField("action\n{\"event\":\"startup_registration_denial\"}", 128); strings.Contains(got, "\n") {
		t.Fatalf("adjacent diagnostic field retained a literal newline: %q", got)
	}
}

func TestUnauthenticatedRequestCannotForgeRegistrationDenialLogLine(t *testing.T) {
	f := newSupervisorRegistrationFixture(t, false)
	var captured bytes.Buffer
	previousWriter, previousFlags := log.Writer(), log.Flags()
	log.SetOutput(&captured)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(previousWriter); log.SetFlags(previousFlags) })
	action := "register\n{\"event\":\"startup_registration_denial\",\"result\":\"closed\"}"
	message := fencing.AuthorityMessage{Action: action, Identity: f.identity}
	message.Identity.PodUID = "wrong-pod\n{\"event\":\"startup_registration_denial\"}"
	body, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, f.server.URL+"/v1/supervisor", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer test")
	request.Header.Set("Content-Type", "application/json")
	response, err := f.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("unbound identity returned status %d, want 403", response.StatusCode)
	}
	if strings.Count(captured.String(), "\n") != 1 || strings.Contains(captured.String(), "\n{\"event\":\"startup_registration_denial\"") {
		t.Fatalf("untrusted fields forged additional log records: %q", captured.String())
	}
}

type startupDenialFailingWriter struct{ err error }

func (w startupDenialFailingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestStartupRegistrationDenialWriterReportsOutputFailures(t *testing.T) {
	event := startupRegistrationDenialEvent{Stage: startupRegistrationStageLedgerRegister, AttemptedRunID: "run"}
	failingLogger := log.New(startupDenialFailingWriter{err: errors.New("write failed")}, "", 0)
	if err := emitStartupRegistrationDenialWithLogger(failingLogger, event); err == nil {
		t.Fatal("writer failure was hidden from the qualification-side observer")
	}
}

func TestStartupRegistrationDenialOutputFailureDoesNotChangeHTTP410(t *testing.T) {
	f := newSupervisorRegistrationFixture(t, true)
	ctx := t.Context()
	if err := f.e.initializeRoot(ctx, "sts-uid", f.identity.SourceClusterID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.e.change(ctx, "sts-uid", f.identity.SourceClusterID, func(ledger *fencing.Ledger) error {
		return ledger.Close("closed-operation", strings.Repeat("d", 64))
	}); err != nil {
		t.Fatal(err)
	}
	previousWriter := log.Writer()
	log.SetOutput(startupDenialFailingWriter{err: errors.New("writer failure")})
	t.Cleanup(func() { log.SetOutput(previousWriter) })
	status := &statusRecordingTransport{next: f.server.Client().Transport}
	client := fencing.AuthorityClient{URL: f.server.URL + "/v1/supervisor", TokenFile: f.token, Client: &http.Client{Transport: status}}
	_, err := client.RegisterStartup(ctx, f.identity)
	if !errors.Is(err, fencing.ErrClosed) || status.code != http.StatusGone {
		t.Fatalf("diagnostic output failure changed the existing 410 decision: %v", err)
	}
}

type statusRecordingTransport struct {
	next http.RoundTripper
	code int
}

func (r *statusRecordingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := r.next.RoundTrip(request)
	if response != nil {
		r.code = response.StatusCode
	}
	return response, err
}

var _ http.RoundTripper = (*statusRecordingTransport)(nil)

func TestStatusRecordingTransportSatisfiesHTTPClientContract(t *testing.T) {
	// Keep the fixture type's contract obvious; the handler tests above exercise
	// it against the actual supervisor server.
	transport := &statusRecordingTransport{next: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return httptest.NewRecorder().Result(), nil
	})}
	response, err := transport.RoundTrip(httptest.NewRequest(http.MethodGet, "http://example.test", nil))
	if err != nil || response == nil {
		t.Fatalf("RoundTrip() = (%v, %v)", response, err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestStartupRegistrationDenialEventsStayRequestLocalUnderConcurrency(t *testing.T) {
	var captured bytes.Buffer
	previousWriter, previousFlags := log.Writer(), log.Flags()
	log.SetOutput(&captured)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(previousWriter); log.SetFlags(previousFlags) })
	const count = 24
	var wg sync.WaitGroup
	for i := range count {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := emitStartupRegistrationDenial(startupRegistrationDenialEvent{
				Stage: startupRegistrationStageLedgerRegister, Namespace: "test", AttemptedRunID: fmt.Sprintf("run-%d", i),
				RequestedNodeID: fmt.Sprintf("node-%d", i), GrantResolved: true, ResolvedNodeID: fmt.Sprintf("logical-%d", i),
			}); err != nil {
				t.Errorf("emit event %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	lines := strings.Split(strings.TrimSpace(captured.String()), "\n")
	if len(lines) != count {
		t.Fatalf("concurrent output produced %d records, want %d: %q", len(lines), count, captured.String())
	}
	seen := make(map[string]bool, count)
	for _, line := range lines {
		var event startupRegistrationDenialEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("concurrent event is not one complete JSON record: %v; %q", err, line)
		}
		if event.AttemptedRunID == "" || seen[event.AttemptedRunID] {
			t.Fatalf("concurrent fields are missing, duplicated, or mixed: %+v", event)
		}
		seen[event.AttemptedRunID] = true
		wantNode := "node-" + strings.TrimPrefix(event.AttemptedRunID, "run-")
		wantResolved := "logical-" + strings.TrimPrefix(event.AttemptedRunID, "run-")
		if event.RequestedNodeID != wantNode || event.ResolvedNodeID != wantResolved {
			t.Fatalf("concurrent event combined fields from different requests: %+v", event)
		}
	}
}
