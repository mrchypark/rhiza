package operator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

func TestFrozenSourcePreflightUsesReadOnlyControlAuthority(t *testing.T) {
	f := newHeldProfileFixture(t)
	ctx := t.Context()
	dataBucket := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = dataBucket.Close() })
	f.e.ArchiveBucket = dataBucket
	newFrozenEmptyNoEffectServer(t, f)
	if _, err := f.e.change(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
		ledger.RequireRuntimeBinding = true
		ledger.RequireMutationJournal = true
		ledger.ArchiveGenesisState = fencing.ArchiveGenesisRegistrationPending
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	f.e.RequireMutationJournal = true
	if err := f.e.claimArchiveScopeForRegistration(ctx, f.runtime.StatefulSetUID, f.id, f.runtime); err != nil {
		t.Fatal(err)
	}
	payload, err := recovery.EmptyArchiveHeadBytes()
	if err != nil {
		t.Fatal(err)
	}
	intent := f.admitUpload(t, 1, "archive/head.bin", fencing.MutationCondition{Mode: fencing.MutationCreateOnly}, payload)
	if err := f.e.prepareArchiveGenesisMutation(ctx, f.runtime.StatefulSetUID, f.runtime, f.producer, 1, intent); err != nil {
		t.Fatal(err)
	}
	if _, err := f.e.changeMutationProducer(ctx, f.runtime, func(producer *fencing.MutationProducer) error {
		preparation := fencing.MutationPreparation{Profile: fencing.MutationHeldExecutionProfile, StoreScopeHash: intent.StoreScopeHash, GenerationSet: true, Generation: 0}
		if err := producer.Prepare(1, preparation); err != nil {
			return err
		}
		if err := producer.GrantDispatch(1, "unknown-original-dispatch"); err != nil {
			return err
		}
		return producer.SealDispatch(1)
	}); err != nil {
		t.Fatal(err)
	}

	dataPrefix := f.scope.Prefix
	dataAuthority, err := NewAuthorityStateStore(dataBucket, dataPrefix)
	if err != nil {
		t.Fatal(err)
	}
	copiedAuthorityObjects := 0
	controlPrefix := f.e.AuthorityState.prefix + "/"
	if err := f.e.AuthorityState.bucket.Iter(ctx, controlPrefix, func(name string) error {
		suffix, ok := strings.CutPrefix(name, controlPrefix)
		if !ok || suffix == "" {
			return fmt.Errorf("unexpected CONTROL authority key outside its prefix")
		}
		reader, err := f.e.AuthorityState.bucket.Get(ctx, name)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return err
		}
		if err := dataBucket.Upload(ctx, dataPrefix+"/"+suffix, bytes.NewReader(body)); err != nil {
			return err
		}
		copiedAuthorityObjects++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if copiedAuthorityObjects == 0 {
		t.Fatal("DATA-side authority fixture did not copy canonical CONTROL objects")
	}

	spec := Spec{StatefulSet: "rhiza", SourceClusterID: f.runtime.SourceClusterID, Durability: "async", RecoveryID: "source-fence", AllowDataLoss: true,
		Fence: Fence{RecoveryID: "source-fence", ClusterID: f.runtime.SourceClusterID, StatefulSetUID: f.runtime.StatefulSetUID}}
	resource := Resource{APIVersion: "rhiza.mrchypark.dev/v1alpha1", Kind: "RhizaRecovery",
		Metadata: object{"name": "rhiza-recovery", "namespace": "test", "uid": "recovery-uid"}, Spec: spec,
		Status: Status{RecoveryMode: RecoveryModeFrozenSource, RecoveryID: spec.RecoveryID, SpecHash: immutableSpecHash(spec),
			Source: spec.SourceClusterID, SourceDurability: "async", StatefulSetUID: f.runtime.StatefulSetUID,
			TargetBaseKind: "empty", FrozenEmptyGenesisPending: true}}
	f.api.mu.Lock()
	f.api.objects[f.e.controller().resourcePath(resourceName(&resource))] = jsonObjectForPreflight(t, resource)
	f.api.mu.Unlock()
	f.e.ControllerServiceAccount = "default"

	controlBefore, controlVersion, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	producerBefore, _, _, err := f.e.AuthorityState.ReadProducer(ctx, f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(f.e.Handler())
	t.Cleanup(server.Close)
	client := &FencingClient{URL: server.URL, TokenFile: f.e.Kube.TokenFile, Client: server.Client()}
	response, err := client.FrozenSourcePreflight(ctx, FrozenSourcePreflightRequest{RecoveryName: resourceName(&resource), RecoveryUID: "recovery-uid"})
	if err != nil || response.Identity.StatefulSetUID != f.runtime.StatefulSetUID || !response.EmptyGenesisEligible || response.ClosureReady {
		t.Fatalf("authenticated read-only preflight did not use canonical CONTROL state: response=%+v err=%v", response, err)
	}
	controlAfter, controlVersionAfter, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil || hashJSON(controlBefore) != hashJSON(controlAfter) || controlVersion.Value != controlVersionAfter.Value {
		t.Fatalf("preflight mutated canonical CONTROL state: before=%+v after=%+v err=%v", controlBefore, controlAfter, err)
	}
	producerAfter, _, _, err := f.e.AuthorityState.ReadProducer(ctx, f.runtime)
	if err != nil || hashJSON(producerBefore) != hashJSON(producerAfter) {
		t.Fatalf("preflight mutated canonical CONTROL producer state: before=%+v after=%+v err=%v", producerBefore, producerAfter, err)
	}
	wrong := *f.e
	wrong.AuthorityState = dataAuthority
	if response, err := wrong.frozenSourcePreflight(ctx, FrozenSourcePreflightRequest{RecoveryName: resourceName(&resource), RecoveryUID: "recovery-uid"}); err != nil || !response.EmptyGenesisEligible {
		t.Fatalf("copied DATA-side authority fixture is not independently valid: response=%+v err=%v", response, err)
	}
	controlRootKey, _, err := f.e.AuthorityState.rootKey(f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.e.AuthorityState.bucket.Delete(ctx, controlRootKey); err != nil {
		t.Fatal(err)
	}
	requestBody := []byte(fmt.Sprintf(`{"recoveryName":%q,"recoveryUID":%q}`, resourceName(&resource), "recovery-uid"))
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+frozenSourcePreflightPath, bytes.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer test")
	request.Header.Set("Content-Type", "application/json")
	result, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = result.Body.Close()
	if result.StatusCode != http.StatusConflict {
		t.Fatalf("RPC fell back to a valid DATA-side authority after CONTROL root removal: HTTP %d", result.StatusCode)
	}

	requestBody = []byte(fmt.Sprintf(`{"recoveryName":%q,"recoveryUID":%q,"root":{"closedBy":"source-fence"}}`, resourceName(&resource), "recovery-uid"))
	request, err = http.NewRequestWithContext(ctx, http.MethodPost, server.URL+frozenSourcePreflightPath, bytes.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer test")
	request.Header.Set("Content-Type", "application/json")
	result, err = server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = result.Body.Close()
	if result.StatusCode != http.StatusBadRequest {
		t.Fatalf("preflight accepted caller-supplied root bytes: HTTP %d", result.StatusCode)
	}

	requestBody = []byte(fmt.Sprintf(`{"recoveryName":%q,"recoveryUID":%q}`, resourceName(&resource), "wrong-recovery-uid"))
	request, err = http.NewRequestWithContext(ctx, http.MethodPost, server.URL+frozenSourcePreflightPath, bytes.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer test")
	request.Header.Set("Content-Type", "application/json")
	result, err = server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = result.Body.Close()
	if result.StatusCode != http.StatusConflict {
		t.Fatalf("preflight accepted a mismatched recovery UID: HTTP %d", result.StatusCode)
	}

	requestBody = []byte(fmt.Sprintf(`{"recoveryName":%q,"recoveryUID":%q}`, resourceName(&resource), "recovery-uid"))
	request, err = http.NewRequestWithContext(ctx, http.MethodPost, server.URL+frozenSourcePreflightPath, bytes.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	result, err = server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = result.Body.Close()
	if result.StatusCode != http.StatusForbidden {
		t.Fatalf("preflight accepted a request without controller authentication: HTTP %d", result.StatusCode)
	}
}

func jsonObjectForPreflight(t *testing.T, value any) object {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var decoded object
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}
