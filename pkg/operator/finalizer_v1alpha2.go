package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

var (
	ErrV1Alpha2FinalizerConflict       = errors.New("v1alpha2 finalizer conflict")
	ErrV1Alpha2FinalizerOutcomeUnknown = errors.New("v1alpha2 finalizer update outcome unknown")
)

type V1Alpha2RecoveryRef struct{ Namespace, Name, UID, ResourceVersion string }
type V1Alpha2FinalizerStore struct{ Kube *Kubernetes }

// Install protects exactly the observed recovery request. It never retries a PUT.
func (s *V1Alpha2FinalizerStore) Install(ctx context.Context, ref V1Alpha2RecoveryRef) error {
	if s == nil || s.Kube == nil || !validNamespace(ref.Namespace) || s.Kube.Namespace != ref.Namespace || len(ref.Name) == 0 || len(ref.Name) > 253 || !v1alpha2Name.MatchString(ref.Name) || !v1alpha2NonBlank.MatchString(ref.UID) || !v1alpha2NonBlank.MatchString(ref.ResourceVersion) {
		return fmt.Errorf("finalizer requires an exact same-namespace recovery identity")
	}
	p := "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/" + ref.Namespace + "/rhizarecoveries/" + ref.Name
	load := func() (map[string]json.RawMessage, error) {
		var obj map[string]json.RawMessage
		if err := s.Kube.Get(ctx, p, &obj); err != nil {
			return nil, err
		}
		return obj, nil
	}
	obj, err := load()
	if err != nil {
		return fmt.Errorf("load recovery: %w", err)
	}
	meta, protected, err := finalizerMetadata(obj, ref)
	if err != nil {
		return err
	}
	var rv string
	_ = json.Unmarshal(meta["resourceVersion"], &rv)
	if rv != ref.ResourceVersion {
		return ErrV1Alpha2FinalizerConflict
	}
	if protected {
		return nil
	}
	if raw, ok := meta["deletionTimestamp"]; ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("cannot protect a deleting recovery")
	}
	var finalizers []string
	if raw, ok := meta["finalizers"]; ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if err := json.Unmarshal(raw, &finalizers); err != nil || finalizers == nil {
			return fmt.Errorf("invalid recovery finalizers")
		}
	}
	finalizers = append(finalizers, V1Alpha2RecoveryRecordFinalizer)
	meta["finalizers"], _ = json.Marshal(finalizers)
	obj["metadata"], _ = json.Marshal(meta)
	var response map[string]json.RawMessage
	err = s.Kube.Put(ctx, p, obj, &response)
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict {
		return fmt.Errorf("%w: %v", ErrV1Alpha2FinalizerConflict, err)
	}
	if err == nil && finalizerMatches(response, obj, ref) {
		return nil
	}
	live, readErr := load()
	if readErr == nil && finalizerMatches(live, obj, ref) {
		return nil
	}
	return ErrV1Alpha2FinalizerOutcomeUnknown
}

func finalizerMetadata(obj map[string]json.RawMessage, ref V1Alpha2RecoveryRef) (map[string]json.RawMessage, bool, error) {
	var version, kind string
	_ = json.Unmarshal(obj["apiVersion"], &version)
	_ = json.Unmarshal(obj["kind"], &kind)
	meta, err := jsonObject(obj["metadata"])
	if err != nil {
		return nil, false, fmt.Errorf("invalid recovery metadata")
	}
	for key, expected := range map[string]string{"name": ref.Name, "namespace": ref.Namespace, "uid": ref.UID} {
		var value string
		if json.Unmarshal(meta[key], &value) != nil || value != expected {
			return nil, false, fmt.Errorf("recovery identity mismatch: %s", key)
		}
	}
	var rv string
	if version != V1Alpha2APIVersion || kind != "RhizaRecovery" || json.Unmarshal(meta["resourceVersion"], &rv) != nil || !v1alpha2NonBlank.MatchString(rv) {
		return nil, false, fmt.Errorf("invalid recovery identity or resourceVersion")
	}
	var finalizers []string
	if raw, ok := meta["finalizers"]; ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if json.Unmarshal(raw, &finalizers) != nil || finalizers == nil {
			return nil, false, fmt.Errorf("invalid recovery finalizers")
		}
	}
	for _, f := range finalizers {
		if f == V1Alpha2RecoveryRecordFinalizer {
			return meta, true, nil
		}
	}
	return meta, false, nil
}

func finalizerMatches(got, expected map[string]json.RawMessage, ref V1Alpha2RecoveryRef) bool {
	meta, protected, err := finalizerMetadata(got, ref)
	if err != nil || !protected {
		return false
	}
	var rv string
	_ = json.Unmarshal(meta["resourceVersion"], &rv)
	if rv == ref.ResourceVersion {
		return false
	}
	expectedMeta, err := jsonObject(expected["metadata"])
	if err != nil {
		return false
	}
	// The API server owns managedFields and may update it during the PUT.
	delete(meta, "managedFields")
	delete(expectedMeta, "managedFields")
	meta["resourceVersion"], _ = json.Marshal(ref.ResourceVersion)
	got["metadata"], _ = json.Marshal(meta)
	expectedCopy := make(map[string]json.RawMessage, len(expected))
	for key, value := range expected {
		expectedCopy[key] = value
	}
	expectedCopy["metadata"], _ = json.Marshal(expectedMeta)
	a, e1 := json.Marshal(got)
	b, e2 := json.Marshal(expectedCopy)
	return e1 == nil && e2 == nil && sameJSON(a, b)
}
