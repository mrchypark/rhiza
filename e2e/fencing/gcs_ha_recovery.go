package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/thanos-io/objstore"
)

type gcsRecoverySource struct {
	Namespace        string
	NamespaceUID     string
	BindingName      string
	LogicalID        string
	StatefulSet      string
	Container        string
	ConfigMap        string
	OperatorPod      string
	OperatorIdentity gcsControlPodIdentity
	FencerPod        string
	AgentDaemonUID   string
	AgentPods        []gcsControlPodIdentity
	InitialRestart   map[string]int
	ClusterID        string
	StatefulSetUID   string
	BindingUID       string
	Bucket           string
	Endpoint         string
	Prefix           string
	Durability       string
	Targets          []operator.FenceTarget
}

type gcsControlPodIdentity struct {
	Name           string `json:"name"`
	UID            string `json:"uid"`
	OwnerName      string `json:"ownerName,omitempty"`
	OwnerUID       string `json:"ownerUID"`
	ParentOwnerUID string `json:"parentOwnerUID,omitempty"`
}

type gcsRecoveryOutcome struct {
	Source    gcsRecoverySource
	Fence     operator.FenceRequest
	Proof     operator.FenceProof
	Recovery  operator.Resource
	OldReturn obj
	Forward   *forward
	Admin     string
}

func qualifyGCSHA(ctx context.Context) (resultErr error) {
	return qualifyGCSHAWithMode(ctx, false)
}

func qualifyGCSHAExplicit(ctx context.Context) error {
	return qualifyGCSHAWithMode(ctx, true)
}

func qualifyGCSHAWithMode(ctx context.Context, explicit bool) (resultErr error) {
	out := os.Getenv("FENCING_E2E_OUT")
	if out == "" {
		out = "gcs-ha-recovery-results"
	}
	if err := os.MkdirAll(out, 0700); err != nil {
		return err
	}
	events, err := os.OpenFile(filepath.Join(out, "gcs-ha-events.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer events.Close()
	event := func(name string, data any) {
		_ = json.NewEncoder(events).Encode(obj{"event": name, "time": time.Now().UTC(), "data": data})
	}
	if err := verifyGCSIndependentReferenceAPIServerRoundTrip(ctx); err != nil {
		return fmt.Errorf("fence CRD terminal reference API-server roundtrip: %w", err)
	}
	event("terminal_reference_schema_roundtrip", obj{"proofOrigin": "synthetic schema-only fixture", "outerVersion": 2, "terminalVersions": []int{2, 3}, "status": "preserved-and-invalid-combinations-rejected"})
	var faults []*gcsChaosFault
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), schemaNamespaceCleanupTimeout)
		defer cancel()
		for i := len(faults) - 1; i >= 0; i-- {
			if err := faults[i].Restore(cleanupCtx); err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("restore owned GCS Chaos fault: %w", err))
			}
			event("chaos_cleanup", gcsChaosEvidence(faults[i]))
		}
	}()

	firstSource, err := captureGCSRecoverySource(ctx, out)
	if err != nil {
		return err
	}
	if explicit {
		firstSource, err = prepareExplicitGCSRecoverySource(ctx, firstSource, out, false)
		if err != nil {
			return err
		}
	}
	adminToken, err := sourceAdminToken(ctx, firstSource.StatefulSet, firstSource.Container)
	if err != nil {
		return err
	}
	firstForward, err := startForward(ctx, firstSource.Targets[0].Pod, out)
	if err != nil {
		return err
	}
	if _, err := call(ctx, firstForward, "/sql/execute", obj{"request_id": "gcs-ha-create-table", "sql": "CREATE TABLE fencing_items (id INTEGER PRIMARY KEY, value TEXT)"}, adminToken); err != nil {
		firstForward.close()
		return fmt.Errorf("create GCS qualification table: %w", err)
	}
	ack1, err := writeGCSAcknowledgedRow(ctx, firstForward, adminToken, 1, "gcs-ack-1")
	firstForward.close()
	if err != nil {
		return err
	}
	rows1 := `[[1,"gcs-ack-1"]]`
	event("source_ack", obj{"source": firstSource.ClusterID, "ack": ack1})
	firstOutcome, err := gcsLoseQuorumAndRecover(ctx, firstSource, adminToken, rows1, "first", explicit, out, event, &faults)
	if err != nil {
		return fmt.Errorf("first GCS recovery: %w", err)
	}
	defer firstOutcome.Forward.close()
	event("first_recovery_complete", obj{"source": firstSource.ClusterID, "target": firstOutcome.Recovery.Status.Target, "proofID": firstOutcome.Proof.ProofID})

	secondSource, err := captureGCSRecoverySource(ctx, out)
	if err != nil {
		return fmt.Errorf("capture first recovered generation: %w", err)
	}
	if secondSource.ClusterID != firstOutcome.Recovery.Status.Target || secondSource.ClusterID == firstSource.ClusterID {
		return fmt.Errorf("second source is not the distinct first recovery target")
	}
	if explicit {
		if err := requireExplicitGCSRecoveryProfile(ctx, secondSource, false); err != nil {
			return err
		}
	}
	ack2, err := writeGCSAcknowledgedRow(ctx, firstOutcome.Forward, firstOutcome.Admin, 2, "gcs-ack-2")
	if err != nil {
		return fmt.Errorf("second-generation acknowledged row: %w", err)
	}
	rows2 := `[[1,"gcs-ack-1"],[2,"gcs-ack-2"]]`
	event("second_source_ack", obj{"source": secondSource.ClusterID, "ack": ack2})
	secondOutcome, err := gcsLoseQuorumAndRecover(ctx, secondSource, firstOutcome.Admin, rows2, "second", explicit, out, event, &faults)
	if err != nil {
		return fmt.Errorf("second GCS recovery: %w", err)
	}
	defer secondOutcome.Forward.close()
	if err := verifyRecoveryLineage(firstOutcome.Recovery, secondOutcome.Recovery, firstOutcome.Fence, secondOutcome.Fence); err != nil {
		return err
	}
	event("second_recovery_lineage_complete", obj{"source": secondOutcome.Recovery.Status.Source, "target": secondOutcome.Recovery.Status.Target, "proofID": secondOutcome.Proof.ProofID})
	oldReturn := obj{"status": "qualified", "generations": []obj{firstOutcome.OldReturn, secondOutcome.OldReturn}}
	if err := os.WriteFile(filepath.Join(out, "gcs-old-pod-return.json"), encoded(oldReturn), 0600); err != nil {
		return err
	}
	evidence := obj{
		"qualification":    "gcs-two-recovery-noPVC",
		"profile":          obj{"provider": "gcs", "bucket": firstSource.Bucket, "noPVC": true},
		"acknowledgements": []gcsAcknowledgement{ack1, ack2},
		"recoveries": []obj{
			gcsRecoveryEvidence(firstOutcome),
			gcsRecoveryEvidence(secondOutcome),
		},
		"proofSource":  "RhizaFence.status.proof read through the authenticated Kubernetes API; remote terminal payloads were not independently fetched by this driver",
		"oldPodReturn": oldReturn,
	}
	if err := os.WriteFile(filepath.Join(out, "gcs-ha-evidence.json"), encoded(evidence), 0600); err != nil {
		return err
	}
	event("old_pod_return_qualified", oldReturn)
	return nil
}

// verifyGCSIndependentReferenceAPIServerRoundTrip exercises the installed
// CRD's real pruning and conditional validation with synthetic schema-only
// data, without placing a request in the operator's watched namespace. The
// authority-generated proof is separately read back and checked in
// waitForGCSRecovery.
func verifyGCSIndependentReferenceAPIServerRoundTrip(ctx context.Context) (resultErr error) {
	if len(qualificationRunID) != 8 {
		return fmt.Errorf("run identity is unavailable")
	}
	ns := "rhiza-fence-schema-" + qualificationRunID
	var createMarkerBytes [16]byte
	if _, err := rand.Read(createMarkerBytes[:]); err != nil {
		return fmt.Errorf("generate isolated roundtrip namespace ownership marker: %w", err)
	}
	createMarker := hex.EncodeToString(createMarkerBytes[:])
	namespaceBody := obj{"apiVersion": "v1", "kind": "Namespace", "metadata": obj{"name": ns, "labels": obj{
		"chaos.rhiza.io/run": qualificationRunID, schemaNamespaceCreateTokenLabel: createMarker,
	}}}
	created, err := commandWithInput(ctx, encoded(namespaceBody), "kubectl", "--context", kubeContext, "create", "-f", "-", "-o", "json")
	if err != nil {
		readback := func(readCtx context.Context, name string) (obj, error) {
			data, readErr := command(readCtx, "kubectl", "--context", kubeContext, "get", "namespace", name, "-o", "json")
			if readErr != nil {
				return nil, readErr
			}
			var namespaceObject obj
			if decodeErr := json.Unmarshal(data, &namespaceObject); decodeErr != nil {
				return nil, fmt.Errorf("decode isolated roundtrip namespace readback: %w", decodeErr)
			}
			return namespaceObject, nil
		}
		cleanup := func(cleanupCtx context.Context, name, runID, uid string) error {
			return deleteOwnedSchemaNamespace(cleanupCtx, name, runID, uid)
		}
		return cleanupUncertainSchemaNamespaceCreate(ctx, ns, qualificationRunID, createMarker, err, readback, cleanup)
	}
	namespaceUID := ""
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if namespaceUID == "" {
			identity, err := command(cleanupCtx, "kubectl", "--context", kubeContext, "get", "namespace", ns, "-o", "json")
			var recovered obj
			if err != nil || json.Unmarshal(identity, &recovered) != nil || text(nested(recovered, "metadata", "name")) != ns ||
				nested(recovered, "metadata", "labels", "chaos.rhiza.io/run") != qualificationRunID ||
				nested(recovered, "metadata", "labels", schemaNamespaceCreateTokenLabel) != createMarker {
				resultErr = errors.Join(resultErr, fmt.Errorf("isolated roundtrip namespace %q may remain; exact UID and run ownership could not be verified", ns))
				return
			}
			namespaceUID = text(nested(recovered, "metadata", "uid"))
			if namespaceUID == "" {
				resultErr = errors.Join(resultErr, fmt.Errorf("isolated roundtrip namespace %q may remain; exact UID is unavailable", ns))
				return
			}
		}
		if err := deleteOwnedSchemaNamespace(cleanupCtx, ns, qualificationRunID, namespaceUID); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("cleanup isolated roundtrip namespace %q failed; it may remain: %w", ns, err))
		}
	}()
	var namespaceObject obj
	if err := json.Unmarshal(created, &namespaceObject); err != nil {
		return fmt.Errorf("decode isolated roundtrip namespace identity: %w", err)
	}
	createdUID := text(nested(namespaceObject, "metadata", "uid"))
	if createdUID == "" || text(nested(namespaceObject, "metadata", "name")) != ns ||
		nested(namespaceObject, "metadata", "labels", "chaos.rhiza.io/run") != qualificationRunID ||
		nested(namespaceObject, "metadata", "labels", schemaNamespaceCreateTokenLabel) != createMarker {
		return fmt.Errorf("isolated roundtrip namespace identity is incomplete")
	}
	namespaceUID = createdUID

	requestHash := strings.Repeat("a", 64)
	proofHash := strings.Repeat("b", 64)
	scopeDigest := strings.Repeat("c", 64)
	identity := func(nodeID, podUID, runID, walIdentity string) obj {
		return obj{"sourceClusterID": "schema-roundtrip-source", "nodeID": nodeID, "podName": nodeID + "-pod", "podUID": podUID,
			"runID": runID, "walIdentity": walIdentity, "assignmentID": strings.Repeat("d", 64)}
	}
	terminal := func(version int, requestIdentity obj) obj {
		ref := obj{"evidenceVersion": version, "request": obj{"operationID": "source-stop-" + text(requestIdentity["runID"]), "requestHash": requestHash, "identity": requestIdentity},
			"runtimeHash": proofHash, "producerUID": "", "proofName": "terminal-proof", "proofUID": "proof-uid", "proofDigest": proofHash,
			"runtimeReceiptDigest": proofHash, "mutationReceiptDigest": "", "storageScopeDigest": scopeDigest}
		if version == 2 {
			ref["producerUID"], ref["mutationReceiptDigest"] = "producer-uid", proofHash
		}
		return ref
	}
	v2 := terminal(2, identity("rhiza-voter-0", "voter-pod-uid", "voter-run", "voter-wal"))
	v3 := terminal(3, identity("rhiza-learner-0", "learner-pod-uid", "learner-run", ""))
	proof := obj{"operationID": "schema-roundtrip", "sourceClusterID": "schema-roundtrip-source", "bindingUID": "schema-roundtrip-binding",
		"requestHash": requestHash, "proofID": "schema-roundtrip-proof", "processesTerminated": true, "recreationBlocked": true, "storageQuiesced": true,
		"independentV2": obj{"version": 2, "requestHash": requestHash, "ledgerHash": proofHash,
			"storageScope":   obj{"provider": "gcs", "bucket": "roundtrip-bucket", "prefix": "roundtrip-prefix"},
			"currentPodUIDs": []string{"voter-pod-uid"}, "terminals": []obj{v2, v3}}}
	resource := obj{"apiVersion": "rhiza.mrchypark.dev/v1alpha1", "kind": "RhizaFence",
		"metadata": obj{"name": "terminal-schema-roundtrip", "namespace": ns},
		"spec": obj{"logicalID": "schema-roundtrip", "bindingUID": "schema-roundtrip-binding", "namespace": ns, "statefulSet": "unbound",
			"statefulSetUID": "unbound-sts", "sourceClusterID": "schema-roundtrip-source", "operationID": "schema-roundtrip", "scope": "Generation", "targets": []obj{}},
		"status": obj{"proof": proof}}
	created, err = commandWithInput(ctx, encoded(resource), "kubectl", "--context", kubeContext, "-n", ns, "create", "-f", "-", "-o", "json")
	if err != nil {
		return fmt.Errorf("create terminal schema roundtrip fixture: %w", err)
	}
	var createdFence obj
	if err := json.Unmarshal(created, &createdFence); err != nil {
		return fmt.Errorf("decode terminal schema fixture identity: %w", err)
	}
	fenceUID := text(nested(createdFence, "metadata", "uid"))
	if fenceUID == "" {
		return fmt.Errorf("terminal schema fixture UID is missing")
	}
	if _, err := command(ctx, "kubectl", "--context", kubeContext, "-n", ns, "patch", "rhizafence", "terminal-schema-roundtrip", "--subresource=status", "--type=merge", "-p", string(encoded(obj{"status": obj{"proof": proof}}))); err != nil {
		return fmt.Errorf("write mixed terminal references through status subresource: %w", err)
	}
	readback, err := command(ctx, "kubectl", "--context", kubeContext, "-n", ns, "get", "rhizafence", "terminal-schema-roundtrip", "-o", "json")
	if err != nil {
		return fmt.Errorf("read back mixed terminal references: %w", err)
	}
	var got obj
	if err := json.Unmarshal(readback, &got); err != nil {
		return fmt.Errorf("decode mixed terminal references: %w", err)
	}
	if text(nested(got, "metadata", "uid")) != fenceUID || string(encoded(nested(got, "status", "proof"))) != string(encoded(proof)) {
		return fmt.Errorf("API server pruned or changed terminal identity/reference fields")
	}
	mutateReference := func(candidate obj, index int, mutate func(obj)) error {
		refs, ok := nested(candidate, "independentV2", "terminals").([]any)
		if !ok || len(refs) <= index {
			return fmt.Errorf("terminal list is malformed")
		}
		ref, ok := refs[index].(map[string]any)
		if !ok {
			return fmt.Errorf("terminal reference is malformed")
		}
		mutate(ref)
		return nil
	}
	for _, tc := range []struct {
		name string
		edit func(obj) error
	}{
		{name: "V2 without producer evidence", edit: func(candidate obj) error {
			return mutateReference(candidate, 0, func(ref obj) { ref["producerUID"], ref["mutationReceiptDigest"] = "", "" })
		}},
		{name: "V3 with producer UID", edit: func(candidate obj) error {
			return mutateReference(candidate, 1, func(ref obj) { ref["producerUID"] = "producer-uid" })
		}},
		{name: "V3 with mutation digest", edit: func(candidate obj) error {
			return mutateReference(candidate, 1, func(ref obj) { ref["mutationReceiptDigest"] = proofHash })
		}},
		{name: "unsupported reference version", edit: func(candidate obj) error {
			return mutateReference(candidate, 1, func(ref obj) { ref["evidenceVersion"] = 4 })
		}},
	} {
		candidate := obj{}
		if err := json.Unmarshal(encoded(proof), &candidate); err != nil {
			return fmt.Errorf("copy proof for %s: %w", tc.name, err)
		}
		if err := tc.edit(candidate); err != nil {
			return fmt.Errorf("prepare invalid %s case: %w", tc.name, err)
		}
		statusCode, statusBody, err := patchGCSRoundTripFenceStatus(ctx, ns, candidate)
		if err != nil {
			return fmt.Errorf("patch %s returned without an API validation response: %w", tc.name, err)
		}
		diagnostic, invalid := gcsAPIServerValidationRejection(statusCode, statusBody)
		if !invalid {
			return fmt.Errorf("patch %s was not rejected by API validation (HTTP %d): %s", tc.name, statusCode, diagnostic)
		}
		readback, err = command(ctx, "kubectl", "--context", kubeContext, "-n", ns, "get", "rhizafence", "terminal-schema-roundtrip", "-o", "json")
		if err != nil {
			return fmt.Errorf("read proof after rejecting %s: %w", tc.name, err)
		}
		if err := json.Unmarshal(readback, &got); err != nil || text(nested(got, "metadata", "uid")) != fenceUID || string(encoded(nested(got, "status", "proof"))) != string(encoded(proof)) {
			return fmt.Errorf("rejected %s changed the valid stored proof", tc.name)
		}
	}
	return nil
}

const (
	schemaNamespaceCreateTokenLabel = "chaos.rhiza.io/schema-create-token"
	schemaNamespaceCleanupTimeout   = 30 * time.Second
)

func cleanupUncertainSchemaNamespaceCreate(
	ctx context.Context,
	name, runID, createMarker string,
	createErr error,
	readback func(context.Context, string) (obj, error),
	cleanup func(context.Context, string, string, string) error,
) error {
	if createErr == nil {
		return fmt.Errorf("uncertain namespace cleanup requires the original create error")
	}
	createFailure := fmt.Errorf("create isolated CRD roundtrip namespace: %w", createErr)
	lowerCreateErr := strings.ToLower(createErr.Error())
	if strings.Contains(lowerCreateErr, "alreadyexists") || strings.Contains(lowerCreateErr, "already exists") {
		return fmt.Errorf("namespace already existed and was not adopted or deleted: %w", createFailure)
	}
	if len(runID) != 8 || name != "rhiza-fence-schema-"+runID || createMarker == "" || readback == nil || cleanup == nil {
		return errors.Join(createFailure, fmt.Errorf("isolated roundtrip namespace may remain; ownership could not be verified"))
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), schemaNamespaceCleanupTimeout)
	defer cancel()
	current, err := readback(cleanupCtx, name)
	if err != nil {
		if strings.Contains(err.Error(), "(NotFound)") {
			return createFailure
		}
		return errors.Join(createFailure, fmt.Errorf("isolated roundtrip namespace may remain; ownership readback failed: %w", err))
	}
	uid := text(nested(current, "metadata", "uid"))
	if text(nested(current, "metadata", "name")) != name ||
		nested(current, "metadata", "labels", "chaos.rhiza.io/run") != runID ||
		nested(current, "metadata", "labels", schemaNamespaceCreateTokenLabel) != createMarker || uid == "" {
		return errors.Join(createFailure, fmt.Errorf("isolated roundtrip namespace may remain; exact name, run ownership, marker, and UID could not be verified"))
	}
	if err := cleanup(cleanupCtx, name, runID, uid); err != nil {
		return errors.Join(createFailure, fmt.Errorf("cleanup isolated roundtrip namespace failed; it may remain: %w", err))
	}
	return createFailure
}

func patchGCSRoundTripFenceStatus(ctx context.Context, namespace string, proof obj) (int, []byte, error) {
	body, err := json.Marshal(obj{"status": obj{"proof": proof}})
	if err != nil {
		return 0, nil, fmt.Errorf("encode roundtrip proof patch: %w", err)
	}
	endpoint := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/" + url.PathEscape(namespace) + "/rhizafences/terminal-schema-roundtrip/status"
	return kubernetesAPIRequest(ctx, http.MethodPatch, endpoint, "application/merge-patch+json", body)
}

func gcsAPIServerValidationRejection(statusCode int, body []byte) (string, bool) {
	var status struct {
		Code    int    `json:"code"`
		Reason  string `json:"reason"`
		Message string `json:"message"`
		Details struct {
			Causes []struct {
				Reason  string `json:"reason"`
				Message string `json:"message"`
				Field   string `json:"field"`
			} `json:"causes"`
		} `json:"details"`
	}
	if json.Unmarshal(body, &status) != nil {
		return "API response was not a Kubernetes Status object", false
	}
	diagnostic := status.Reason
	if status.Message != "" {
		diagnostic += ": " + status.Message
	}
	for _, cause := range status.Details.Causes {
		if cause.Field != "" || cause.Message != "" {
			diagnostic += fmt.Sprintf("; %s %s: %s", cause.Reason, cause.Field, cause.Message)
		}
	}
	diagnostic = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, diagnostic)
	if len(diagnostic) > 512 {
		diagnostic = diagnostic[:512]
	}
	return diagnostic, statusCode == http.StatusUnprocessableEntity && status.Code == http.StatusUnprocessableEntity && status.Reason == "Invalid"
}

func prepareExplicitGCSRecoverySource(ctx context.Context, source gcsRecoverySource, out string, async bool) (gcsRecoverySource, error) {
	if source.Namespace != namespace || source.BindingName == "" || source.StatefulSet == "" {
		return source, fmt.Errorf("explicit recovery source is outside the configured run namespace")
	}
	if err := ensureNoGCSRecoveryInProgress(ctx, source); err != nil {
		return source, err
	}
	wantDurability := "before-ack"
	if async {
		wantDurability = "async"
	}
	if source.Durability != wantDurability {
		return source, fmt.Errorf("explicit recovery mode does not match the captured source durability")
	}
	// Prove every object that would be changed was created inside this fresh,
	// run-labeled namespace before touching either control plane.
	ns, err := get(ctx, "namespace", namespace)
	if err != nil {
		return source, err
	}
	if nested(ns, "metadata", "labels", "chaos.rhiza.io/run") != qualificationRunID {
		return source, fmt.Errorf("explicit recovery requires the current run-owned namespace")
	}
	namespaceCreated, err := time.Parse(time.RFC3339Nano, text(nested(ns, "metadata", "creationTimestamp")))
	if err != nil {
		return source, fmt.Errorf("read run namespace creation identity: %w", err)
	}
	binding, err := get(ctx, "rhizacluster", source.BindingName)
	if err != nil {
		return source, err
	}
	if err := requireRunOwnedResource(binding, "RhizaCluster", namespace, namespaceCreated); err != nil {
		return source, err
	}
	sts, err := get(ctx, "statefulset", source.StatefulSet)
	if err != nil {
		return source, err
	}
	if err := requireRunOwnedResource(sts, "StatefulSet", namespace, namespaceCreated); err != nil {
		return source, err
	}
	operatorDeployment, err := get(ctx, "deployment", "rhiza-operator")
	if err != nil {
		return source, err
	}
	if err := requireRunOwnedResource(operatorDeployment, "operator Deployment", namespace, namespaceCreated); err != nil {
		return source, err
	}
	if nested(operatorDeployment, "spec", "selector", "matchLabels", "app.kubernetes.io/name") != "rhiza-operator" ||
		nested(operatorDeployment, "spec", "template", "metadata", "labels", "app.kubernetes.io/name") != "rhiza-operator" {
		return source, fmt.Errorf("operator Deployment does not match the run fixture identity")
	}
	container, err := boundContainer(operatorDeployment, "operator")
	if err != nil {
		return source, err
	}
	automatic, err := effectiveGCSOptionalSetting(ctx, container, "RHIZA_AUTOMATIC_RECOVERY")
	if err != nil {
		return source, err
	}
	if automatic != "false" {
		if _, err := kubectl(ctx, "set", "env", "deployment/rhiza-operator", "RHIZA_AUTOMATIC_RECOVERY=false"); err != nil {
			return source, fmt.Errorf("disable automatic recovery for explicit qualification: %w", err)
		}
		if _, err := kubectl(ctx, "rollout", "status", "deployment/rhiza-operator", "--timeout=120s"); err != nil {
			return source, fmt.Errorf("wait for explicit-mode operator rollout: %w", err)
		}
	}
	if !explicitGCSRecoveryProfileSafe(nested(binding, "spec"), "false") {
		patch := `{"spec":{"automatic":false,"policy":null}}`
		if _, err := kubectl(ctx, "patch", "rhizacluster", source.BindingName, "--type=merge", "-p", patch); err != nil {
			return source, fmt.Errorf("disable automatic incident policy for explicit qualification: %w", err)
		}
	}
	if err := wait(ctx, "persisted explicit-only recovery profile", func() (bool, error) {
		binding, err := get(ctx, "rhizacluster", source.BindingName)
		if err != nil {
			return false, err
		}
		deployment, err := get(ctx, "deployment", "rhiza-operator")
		if err != nil {
			return false, err
		}
		container, err := boundContainer(deployment, "operator")
		if err != nil {
			return false, err
		}
		setting, err := effectiveGCSSetting(ctx, container, "RHIZA_AUTOMATIC_RECOVERY")
		if err != nil {
			return false, err
		}
		return explicitGCSRecoveryProfileSafe(nested(binding, "spec"), setting), nil
	}); err != nil {
		return source, err
	}
	refreshed, err := captureGCSRecoverySource(ctx, out)
	if err != nil {
		return source, err
	}
	if err := requireExplicitGCSRecoveryProfile(ctx, refreshed, async); err != nil {
		return source, err
	}
	return refreshed, nil
}

func requireRunOwnedResource(resource any, description, expectedNamespace string, namespaceCreated time.Time) error {
	resourceNamespace := text(nested(resource, "metadata", "namespace"))
	uid := text(nested(resource, "metadata", "uid"))
	createdAt, err := time.Parse(time.RFC3339Nano, text(nested(resource, "metadata", "creationTimestamp")))
	if err != nil || resourceNamespace != expectedNamespace || uid == "" || createdAt.Before(namespaceCreated) {
		return fmt.Errorf("%s is not proven to belong to the fresh run namespace", description)
	}
	if runLabel := text(nested(resource, "metadata", "labels", "chaos.rhiza.io/run")); runLabel != "" && runLabel != qualificationRunID {
		return fmt.Errorf("%s carries a different run identity", description)
	}
	return nil
}

func explicitGCSRecoveryProfileSafe(spec any, operatorAutomatic string) bool {
	return nested(spec, "automatic") == false && nested(spec, "policy", "allowDataLoss") != true && operatorAutomatic == "false"
}

func requireExplicitGCSRecoveryProfile(ctx context.Context, source gcsRecoverySource, async bool) error {
	binding, err := get(ctx, "rhizacluster", source.BindingName)
	if err != nil {
		return err
	}
	deployment, err := get(ctx, "deployment", "rhiza-operator")
	if err != nil {
		return err
	}
	container, err := boundContainer(deployment, "operator")
	if err != nil {
		return err
	}
	automatic, err := effectiveGCSSetting(ctx, container, "RHIZA_AUTOMATIC_RECOVERY")
	if err != nil || !explicitGCSRecoveryProfileSafe(nested(binding, "spec"), automatic) {
		return fmt.Errorf("explicit recovery requires automatic recovery disabled and no automatic loss policy")
	}
	want := "before-ack"
	if async {
		want = "async"
	}
	if source.Durability != want {
		return fmt.Errorf("explicit recovery source durability is %q, want %q", source.Durability, want)
	}
	return nil
}

func ensureNoGCSRecoveryInProgress(ctx context.Context, source gcsRecoverySource) error {
	recoveries, err := get(ctx, "rhizarecoveries", "")
	if err != nil {
		return err
	}
	if items, _ := recoveries["items"].([]any); len(items) != 0 {
		return fmt.Errorf("explicit recovery qualification requires a fresh namespace with no prior recovery resources")
	}
	fences, err := get(ctx, "rhizafences", "")
	if err != nil {
		return err
	}
	items, _ := nested(fences, "items").([]any)
	for _, item := range items {
		var request operator.FenceRequest
		if err := json.Unmarshal(encoded(nested(item, "spec")), &request); err != nil {
			return err
		}
		if request.Scope == operator.ScopeGeneration && request.SourceClusterID == source.ClusterID {
			return fmt.Errorf("explicit recovery qualification found a prior generation fence for the source")
		}
	}
	return nil
}

func gcsRecoveryEvidence(outcome *gcsRecoveryOutcome) obj {
	return obj{"source": outcome.Source, "fence": outcome.Fence, "proof": outcome.Proof, "recovery": outcome.Recovery}
}

func sourceAdminToken(ctx context.Context, statefulSet, container string) (string, error) {
	sts, err := get(ctx, "statefulset", statefulSet)
	if err != nil {
		return "", err
	}
	containers, _ := nested(sts, "spec", "template", "spec", "containers").([]any)
	var appContainer any
	for _, candidate := range containers {
		if nested(candidate, "name") == container {
			appContainer = candidate
		}
	}
	if appContainer == nil {
		return "", fmt.Errorf("source admin credential lookup cannot find bound application container")
	}
	env, _ := nested(appContainer, "env").([]any)
	for _, value := range env {
		if nested(value, "name") != "RHIZA_ADMIN_TOKEN" {
			continue
		}
		name := text(nested(value, "valueFrom", "secretKeyRef", "name"))
		key := text(nested(value, "valueFrom", "secretKeyRef", "key"))
		if name != "" && key != "" {
			secret, err := get(ctx, "secret", name)
			if err != nil {
				return "", err
			}
			if token, err := base64.StdEncoding.DecodeString(text(nested(secret, "data", key))); err == nil && len(token) != 0 {
				return string(token), nil
			}
		}
	}
	refs, _ := nested(appContainer, "envFrom").([]any)
	for _, ref := range refs {
		name := text(nested(ref, "secretRef", "name"))
		if name == "" || nested(ref, "secretRef", "name") == "" {
			continue
		}
		secret, err := get(ctx, "secret", name)
		if err != nil {
			return "", err
		}
		for _, key := range []string{"RHIZA_ADMIN_TOKEN", "admin"} {
			if token, err := base64.StdEncoding.DecodeString(text(nested(secret, "data", key))); err == nil && len(token) != 0 {
				return string(token), nil
			}
		}
	}
	return "", fmt.Errorf("source workload has no readable admin Secret")
}

func explicitGenerationFenceRequest(source gcsRecoverySource, recoveryID string) operator.FenceRequest {
	return operator.FenceRequest{
		LogicalID: source.LogicalID, BindingUID: source.BindingUID, Namespace: source.Namespace,
		StatefulSet: source.StatefulSet, StatefulSetUID: source.StatefulSetUID,
		SourceClusterID: source.ClusterID, OperationID: recoveryID,
		Scope: operator.ScopeGeneration, Targets: append([]operator.FenceTarget(nil), source.Targets...),
	}
}

func validateExplicitGenerationFenceRequest(request operator.FenceRequest, source gcsRecoverySource, recoveryID string) error {
	if recoveryID == "" || request.Scope != operator.ScopeGeneration || request.OperationID != recoveryID ||
		!sameFenceRequest(request, explicitGenerationFenceRequest(source, recoveryID)) {
		return fmt.Errorf("explicit fence request differs from the captured source workload")
	}
	return nil
}

func explicitRecoveryIdentity(runID, phase string) (string, string, error) {
	if len(runID) != 8 || (phase != "first" && phase != "second" && phase != "async-empty" && phase != "manual-terminal") {
		return "", "", fmt.Errorf("explicit recovery requires a run-owned ID and known phase")
	}
	for _, r := range runID {
		if !(r >= 'a' && r <= 'f' || r >= '0' && r <= '9') {
			return "", "", fmt.Errorf("explicit recovery requires a lowercase hexadecimal run ID")
		}
	}
	name := "rhiza-manual-" + runID + "-" + phase
	recoveryID := "manual-" + runID + "-" + phase
	return name, recoveryID, nil
}

func explicitRecoveryManifest(source gcsRecoverySource, runID, phase, targetDurability string, allowDataLoss bool) (obj, string, string, error) {
	name, recoveryID, err := explicitRecoveryIdentity(runID, phase)
	if err != nil {
		return nil, "", "", err
	}
	async := targetDurability == "async"
	if (targetDurability != "async" && targetDurability != "before-ack") || allowDataLoss != async {
		return nil, "", "", fmt.Errorf("explicit recovery durability and loss policy are inconsistent")
	}
	request := obj{
		"apiVersion": "rhiza.mrchypark.dev/v1alpha1", "kind": "RhizaRecovery",
		"metadata": obj{"name": name, "namespace": source.Namespace, "labels": obj{"chaos.rhiza.io/run": runID}},
		"spec": obj{
			"statefulSet": source.StatefulSet, "container": source.Container,
			"sourceClusterID": source.ClusterID, "durability": targetDurability,
			"recoveryID": recoveryID, "allowDataLoss": allowDataLoss,
			"fence": obj{"recoveryID": recoveryID, "clusterID": source.ClusterID, "statefulSetUID": source.StatefulSetUID, "confirmed": false},
		},
	}
	return request, name, recoveryID, nil
}

func createExplicitGCSRecovery(ctx context.Context, source gcsRecoverySource, phase, targetDurability string, allowDataLoss bool) (explicitGCSRecoveryRef, error) {
	async := targetDurability == "async"
	if err := requireExplicitGCSRecoveryProfile(ctx, source, async); err != nil {
		return explicitGCSRecoveryRef{}, err
	}
	request, name, recoveryID, err := explicitRecoveryManifest(source, qualificationRunID, phase, targetDurability, allowDataLoss)
	if err != nil {
		return explicitGCSRecoveryRef{}, err
	}
	if _, err := commandWithInput(ctx, encoded(request), "kubectl", "--context", kubeContext, "-n", source.Namespace, "create", "-f", "-"); err != nil {
		return explicitGCSRecoveryRef{}, fmt.Errorf("create unconfirmed explicit recovery resource: %w", err)
	}
	uid, err := waitForExplicitGCSRecoveryAwaitingFence(ctx, source, name, recoveryID, "", allowDataLoss)
	if err != nil {
		return explicitGCSRecoveryRef{}, err
	}
	if err := verifyNoGCSReservation(ctx, source); err != nil {
		return explicitGCSRecoveryRef{}, err
	}
	return explicitGCSRecoveryRef{Name: name, UID: uid, Namespace: source.Namespace, RunLabel: qualificationRunID,
		OperationID: recoveryID, SourceClusterID: source.ClusterID, StatefulSet: source.StatefulSet,
		Container: source.Container, Durability: targetDurability, AllowDataLoss: allowDataLoss}, nil
}

func waitForExplicitGCSRecoveryAwaitingFence(ctx context.Context, source gcsRecoverySource, name, recoveryID, expectedUID string, allowDataLoss bool) (string, error) {
	var recoveryUID string
	err := wait(ctx, "explicit recovery awaits its exact external fence", func() (bool, error) {
		resource, err := get(ctx, "rhizarecovery", name)
		if err != nil {
			return false, err
		}
		uid := text(nested(resource, "metadata", "uid"))
		wantDurability := "before-ack"
		if allowDataLoss {
			wantDurability = "async"
		}
		if uid == "" || expectedUID != "" && uid != expectedUID || text(nested(resource, "metadata", "namespace")) != source.Namespace ||
			text(nested(resource, "metadata", "labels", "chaos.rhiza.io/run")) != qualificationRunID ||
			nested(resource, "spec", "sourceClusterID") != source.ClusterID || nested(resource, "spec", "statefulSet") != source.StatefulSet || nested(resource, "spec", "container") != source.Container ||
			nested(resource, "spec", "durability") != wantDurability || nested(resource, "spec", "recoveryID") != recoveryID || nested(resource, "spec", "allowDataLoss") != allowDataLoss ||
			nested(resource, "spec", "fence", "recoveryID") != recoveryID || nested(resource, "spec", "fence", "confirmed") != false ||
			nested(resource, "spec", "fence", "statefulSetUID") != source.StatefulSetUID || nested(resource, "spec", "fence", "clusterID") != source.ClusterID ||
			nested(resource, "status", "target") != nil || nested(resource, "status", "stage") != nil {
			return false, fmt.Errorf("explicit recovery identity changed or reserved a successor before proof")
		}
		phase := text(nested(resource, "status", "phase"))
		if phase != "" && phase != "AwaitingFence" {
			return false, fmt.Errorf("explicit recovery entered unexpected phase %q before its fence", phase)
		}
		if phase == "AwaitingFence" {
			recoveryUID = uid
			return true, nil
		}
		return false, nil
	})
	return recoveryUID, err
}

func verifyNoGCSReservation(ctx context.Context, source gcsRecoverySource) error {
	bucket, err := newGCSArchiveReader(source)
	if err != nil {
		return err
	}
	defer bucket.Close()
	return verifyNoGCSReservationAt(ctx, bucket, source.Prefix)
}

func verifyNoGCSReservationAt(ctx context.Context, bucket objstore.Bucket, prefix string) error {
	key := path.Join(prefix, "recovery/successor.json")
	reader, err := bucket.Get(ctx, key)
	if err != nil {
		if bucket.IsObjNotFoundErr(err) {
			return nil
		}
		return fmt.Errorf("verify no pre-fence GCS successor reservation: %w", err)
	}
	_ = reader.Close()
	return fmt.Errorf("explicit recovery reserved a successor before its exact fence proof")
}

func confirmExplicitGCSRecoveryFence(ctx context.Context, source gcsRecoverySource, pending pendingGCSFence, recovery explicitGCSRecoveryRef) error {
	item, err := get(ctx, "rhizafence", pending.Name)
	if err != nil {
		return err
	}
	request, err := exactPendingGCSFenceRequest(item, pending)
	if err != nil {
		return err
	}
	if err := verifyGCSGenerationProofAgainstCanonicalRoot(ctx, item, source); err != nil {
		return fmt.Errorf("explicit recovery proof differs from canonical generation authority: %w", err)
	}
	request, proof, err := verifyIndependentGenerationProof(item, source.ClusterID, source.StatefulSetUID, source.BindingUID, source.Targets, source.Bucket, source.Prefix, source.Namespace, source.StatefulSet, source.LogicalID)
	if err != nil || !sameFenceRequest(request, pending.Request) || proof.OperationID != pending.Request.OperationID {
		return fmt.Errorf("explicit recovery proof is not the exact captured generation fence")
	}
	recoveryResource, err := get(ctx, "rhizarecovery", recovery.Name)
	if err != nil {
		return err
	}
	if err := validateExplicitRecoveryResource(recoveryResource, source, recovery); err != nil ||
		nested(recoveryResource, "status", "phase") != "AwaitingFence" || nested(recoveryResource, "spec", "fence", "confirmed") != false ||
		recovery.OperationID != pending.Request.OperationID {
		return fmt.Errorf("explicit recovery changed before the exact fence proof was confirmed")
	}
	patchOps, err := explicitRecoveryConfirmationPatch(recoveryResource, recovery, proof.ProofID)
	if err != nil {
		return err
	}
	_, err = kubectl(ctx, "patch", "rhizarecovery", recovery.Name, "--type=json", "-p", string(encoded(patchOps)))
	if err != nil {
		return fmt.Errorf("confirm explicit recovery with the exact persisted proof: %w", err)
	}
	return nil
}

func gcsLoseQuorumAndRecover(ctx context.Context, source gcsRecoverySource, token, expectedRows, phase string, explicit bool, out string, event func(string, any), faults *[]*gcsChaosFault) (*gcsRecoveryOutcome, error) {
	chaosCtx, cancelChaos := context.WithTimeout(ctx, 165*time.Second)
	defer cancelChaos()
	if len(source.Targets) < 3 || len(source.Targets) > 64 || len(source.AgentPods) == 0 || source.OperatorPod == "" {
		return nil, fmt.Errorf("quorum-loss scenario lacks a supported voter cohort or exact control Pods")
	}
	if err := verifyGCSControlPodSnapshots(chaosCtx, source.AgentPods); err != nil {
		return nil, fmt.Errorf("runtime-agent cohort changed since source capture: %w", err)
	}
	agentFault, err := createGCSControlLoss(chaosCtx, namespace, qualificationRunID, phase, "runtime-agent", source.AgentPods)
	if agentFault != nil {
		*faults = append(*faults, agentFault)
	}
	if err != nil {
		return nil, fmt.Errorf("isolate source runtime-agent cohort: %w", err)
	}
	if !gcsFaultSourceMatches(agentFault, source.AgentPods) {
		return nil, fmt.Errorf("runtime-agent NetworkChaos source UIDs differ from captured DaemonSet Pod snapshot")
	}
	if err := agentFault.WaitInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("runtime-agent cohort isolation was not injected: %w", err)
	}
	event("runtime_agent_network_isolated", gcsChaosEvidence(agentFault))
	cohortFault, err := partitionGCSVoterCohort(chaosCtx, namespace, qualificationRunID, phase, source.Targets)
	if cohortFault != nil {
		*faults = append(*faults, cohortFault)
	}
	if err != nil {
		return nil, fmt.Errorf("partition exact source voter cohort: %w", err)
	}
	if err := cohortFault.WaitInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("source voter full-mesh partition was not injected: %w", err)
	}
	event("source_voter_full_mesh_partitioned", gcsChaosEvidence(cohortFault))
	observer, err := startForward(chaosCtx, source.Targets[0].Pod, out)
	if err != nil {
		return nil, err
	}
	err = wait(chaosCtx, "source voter reports no quorum", func() (bool, error) {
		data, err := call(chaosCtx, observer, "/recovery/status", nil, token)
		if err != nil {
			return false, err
		}
		var status network.VoterRecoveryStatus
		if err := json.Unmarshal(encoded(data), &status); err != nil {
			return false, err
		}
		if status.ClusterID != source.ClusterID {
			return false, fmt.Errorf("quorum observer changed source generation")
		}
		return !status.Quorum, nil
	})
	observer.close()
	if err != nil {
		return nil, err
	}
	event("source_quorum_lost", obj{"source": source.ClusterID, "isolatedPodUIDs": podUIDs(source.Targets)})
	var pending pendingGCSFence
	var explicitRecovery explicitGCSRecoveryRef
	if explicit {
		explicitRecovery, err = createExplicitGCSRecovery(chaosCtx, source, phase, "before-ack", false)
		if err != nil {
			return nil, err
		}
		if err := requestGenerationFence(chaosCtx, source, explicitGenerationFenceRequest(source, explicitRecovery.OperationID), out); err != nil {
			return nil, err
		}
		pending, err = waitForPendingGCSFenceOperation(chaosCtx, source, explicitRecovery.OperationID)
	} else {
		pending, err = waitForPendingGCSFence(chaosCtx, source)
	}
	if err != nil {
		return nil, err
	}
	if explicit {
		if _, err := waitForExplicitGCSRecoveryAwaitingFence(chaosCtx, source, explicitRecovery.Name, pending.Request.OperationID, explicitRecovery.UID, explicitRecovery.AllowDataLoss); err != nil {
			return nil, err
		}
		if err := verifyNoGCSReservation(chaosCtx, source); err != nil {
			return nil, err
		}
	}
	if err := agentFault.RecheckInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("runtime-agent partition expired before pending fence observation: %w", err)
	}
	if err := cohortFault.RecheckInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("voter partition expired before pending fence observation: %w", err)
	}
	event("generation_fence_pending", pending)
	operatorSnapshot := []gcsControlPodIdentity{source.OperatorIdentity}
	if err := verifyGCSControlPodSnapshots(chaosCtx, operatorSnapshot); err != nil {
		return nil, fmt.Errorf("operator Pod changed since source capture: %w", err)
	}
	operatorFault, err := createGCSControlLoss(chaosCtx, namespace, qualificationRunID, phase, "operator", operatorSnapshot)
	if operatorFault != nil {
		*faults = append(*faults, operatorFault)
	}
	if err != nil {
		return nil, fmt.Errorf("isolate operator Pod after fence request persisted: %w", err)
	}
	if !gcsFaultSourceMatches(operatorFault, operatorSnapshot) {
		return nil, fmt.Errorf("operator NetworkChaos source UID differs from captured Deployment Pod snapshot")
	}
	if err := operatorFault.WaitInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("operator network isolation was not injected: %w", err)
	}
	if err := confirmPendingGCSFence(chaosCtx, source, pending); err != nil {
		return nil, fmt.Errorf("operator isolation raced fence publication: %w", err)
	}
	if err := operatorFault.RecheckInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("operator partition expired before proof observation: %w", err)
	}
	if err := cohortFault.RecheckInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("voter partition expired before proof observation: %w", err)
	}
	event("operator_network_isolated", gcsChaosEvidence(operatorFault))
	if err := agentFault.Restore(chaosCtx); err != nil {
		return nil, fmt.Errorf("restore source runtime-agent network: %w", err)
	}
	if err := waitForGCSFenceProof(chaosCtx, source, pending); err != nil {
		return nil, err
	}
	if explicit {
		if err := confirmExplicitGCSRecoveryFence(chaosCtx, source, pending, explicitRecovery); err != nil {
			return nil, err
		}
	}
	if err := operatorFault.RecheckInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("operator partition expired before closed-generation return observation: %w", err)
	}
	if err := cohortFault.RecheckInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("voter partition expired before closed-generation return observation: %w", err)
	}
	event("generation_fence_proven", obj{"fenceName": pending.Name, "operationID": pending.Request.OperationID})
	oldReturn, err := waitForOldGenerationReturn(chaosCtx, source, out)
	if err != nil {
		return nil, err
	}
	if err := operatorFault.RecheckInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("operator partition expired during closed-generation return observation: %w", err)
	}
	if err := cohortFault.RecheckInjected(chaosCtx); err != nil {
		return nil, fmt.Errorf("voter partition expired during closed-generation return observation: %w", err)
	}
	event("old_pod_return_refused", obj{"source": source.ClusterID, "podUIDs": podUIDs(source.Targets), "oracle": "same Pod UID restarted, unready, previous container log reports fencing: incarnation terminally fenced"})
	if err := cohortFault.Restore(chaosCtx); err != nil {
		return nil, fmt.Errorf("restore voter partition after closed-generation return observation: %w", err)
	}
	if err := operatorFault.Restore(chaosCtx); err != nil {
		return nil, fmt.Errorf("restore operator control after return observation: %w", err)
	}
	var expectedRecovery *explicitGCSRecoveryRef
	if explicit {
		expectedRecovery = &explicitRecovery
	}
	outcome, err := waitForGCSRecovery(ctx, source, pending, "fork", expectedRows, out, expectedRecovery)
	if err != nil {
		return nil, err
	}
	outcome.OldReturn = oldReturn
	return outcome, nil
}

type pendingGCSFence struct {
	Name    string                `json:"name"`
	UID     string                `json:"uid"`
	Request operator.FenceRequest `json:"request"`
}

type explicitGCSRecoveryRef struct {
	Name            string
	UID             string
	Namespace       string
	RunLabel        string
	OperationID     string
	SourceClusterID string
	StatefulSet     string
	Container       string
	Durability      string
	AllowDataLoss   bool
}

func validateExplicitRecoveryResource(resource any, source gcsRecoverySource, recovery explicitGCSRecoveryRef) error {
	resourceObject, ok := resource.(map[string]any)
	if !ok {
		return fmt.Errorf("explicit recovery response is not a Kubernetes object")
	}
	resource = obj(resourceObject)
	if text(nested(resource, "metadata", "name")) != recovery.Name ||
		text(nested(resource, "metadata", "uid")) != recovery.UID ||
		text(nested(resource, "metadata", "namespace")) != recovery.Namespace ||
		text(nested(resource, "metadata", "labels", "chaos.rhiza.io/run")) != recovery.RunLabel ||
		recovery.Namespace != source.Namespace || recovery.SourceClusterID != source.ClusterID || recovery.StatefulSet != source.StatefulSet ||
		recovery.Container != source.Container ||
		nested(resource, "spec", "recoveryID") != recovery.OperationID ||
		nested(resource, "spec", "sourceClusterID") != recovery.SourceClusterID ||
		nested(resource, "spec", "statefulSet") != recovery.StatefulSet || nested(resource, "spec", "container") != recovery.Container ||
		nested(resource, "spec", "durability") != recovery.Durability || nested(resource, "spec", "allowDataLoss") != recovery.AllowDataLoss ||
		nested(resource, "spec", "fence", "recoveryID") != recovery.OperationID ||
		nested(resource, "spec", "fence", "clusterID") != recovery.SourceClusterID ||
		nested(resource, "spec", "fence", "statefulSetUID") != source.StatefulSetUID {
		return fmt.Errorf("explicit recovery resource identity or policy changed")
	}
	return nil
}

func explicitRecoveryConfirmationPatch(resource obj, recovery explicitGCSRecoveryRef, proofID string) ([]obj, error) {
	resourceVersion := text(nested(resource, "metadata", "resourceVersion"))
	if resourceVersion == "" || proofID == "" || recovery.UID == "" || recovery.OperationID == "" {
		return nil, fmt.Errorf("explicit recovery confirmation lacks versioned identity or proof")
	}
	return []obj{
		{"op": "test", "path": "/metadata/uid", "value": recovery.UID},
		{"op": "test", "path": "/metadata/resourceVersion", "value": resourceVersion},
		{"op": "test", "path": "/spec/recoveryID", "value": recovery.OperationID},
		{"op": "test", "path": "/spec/sourceClusterID", "value": recovery.SourceClusterID},
		{"op": "test", "path": "/spec/durability", "value": recovery.Durability},
		{"op": "test", "path": "/spec/allowDataLoss", "value": recovery.AllowDataLoss},
		{"op": "test", "path": "/spec/fence/confirmed", "value": false},
		{"op": "add", "path": "/spec/fence/evidence", "value": proofID},
		{"op": "replace", "path": "/spec/fence/confirmed", "value": true},
	}, nil
}

func podUIDs(targets []operator.FenceTarget) []string {
	uids := make([]string, 0, len(targets))
	for _, target := range targets {
		uids = append(uids, target.PodUID)
	}
	return uids
}

func waitForPendingGCSFence(ctx context.Context, source gcsRecoverySource) (pendingGCSFence, error) {
	return waitForPendingGCSFenceOperation(ctx, source, "")
}

func waitForPendingGCSFenceOperation(ctx context.Context, source gcsRecoverySource, operationID string) (pendingGCSFence, error) {
	var pending pendingGCSFence
	err := wait(ctx, "exact source generation fence request without proof", func() (bool, error) {
		list, err := get(ctx, "rhizafences", "")
		if err != nil {
			return false, err
		}
		items, _ := list["items"].([]any)
		var matches []pendingGCSFence
		for _, item := range items {
			var request operator.FenceRequest
			if err := json.Unmarshal(encoded(nested(item, "spec")), &request); err != nil {
				return false, err
			}
			if request.Scope != operator.ScopeGeneration || request.SourceClusterID != source.ClusterID || request.StatefulSet != source.StatefulSet || request.StatefulSetUID != source.StatefulSetUID || request.BindingUID != source.BindingUID || request.Namespace != source.Namespace || request.LogicalID != source.LogicalID || request.OperationID == "" || operationID != "" && request.OperationID != operationID || !sameTargets(request.Targets, source.Targets) {
				continue
			}
			if nested(item, "status", "proof") != nil {
				return false, fmt.Errorf("matching generation fence proof arrived before operator isolation")
			}
			name := text(nested(item, "metadata", "name"))
			uid := text(nested(item, "metadata", "uid"))
			if name == "" || uid == "" {
				return false, fmt.Errorf("pending generation fence lacks a stable resource identity")
			}
			matches = append(matches, pendingGCSFence{Name: name, UID: uid, Request: request})
		}
		if len(matches) > 1 {
			return false, fmt.Errorf("multiple exact generation fence requests exist")
		}
		if len(matches) == 1 {
			if matches[0].Name == "" {
				return false, fmt.Errorf("pending generation fence has no resource name")
			}
			pending = matches[0]
			return true, nil
		}
		return false, nil
	})
	return pending, err
}

func confirmPendingGCSFence(ctx context.Context, source gcsRecoverySource, pending pendingGCSFence) error {
	item, err := get(ctx, "rhizafence", pending.Name)
	if err != nil {
		return err
	}
	request, err := exactPendingGCSFenceRequest(item, pending)
	if err != nil {
		return err
	}
	if request.SourceClusterID != source.ClusterID || !sameTargets(request.Targets, source.Targets) || nested(item, "status", "proof") != nil {
		return fmt.Errorf("exact fence request is no longer pending without proof")
	}
	return nil
}

func waitForGCSFenceProof(ctx context.Context, source gcsRecoverySource, pending pendingGCSFence) error {
	return wait(ctx, "authenticated exact-scope GCS generation fence proof", func() (bool, error) {
		item, err := get(ctx, "rhizafence", pending.Name)
		if err != nil {
			return false, err
		}
		if _, err := exactPendingGCSFenceRequest(item, pending); err != nil {
			return false, fmt.Errorf("generation fence request mutated after creation")
		}
		if nested(item, "status", "proof") == nil {
			return false, nil
		}
		err = verifyGCSGenerationProofAgainstCanonicalRoot(ctx, item, source)
		return err == nil, err
	})
}

func waitForOldGenerationReturn(ctx context.Context, source gcsRecoverySource, out string) (obj, error) {
	type observation struct {
		Pod       string `json:"pod"`
		PodUID    string `json:"podUID"`
		Restarted bool   `json:"restarted"`
		Unready   bool   `json:"unready"`
		Closed    bool   `json:"closedGenerationFatal"`
	}
	observations := make([]observation, len(source.Targets))
	err := wait(ctx, "same-PodUID restart rejects closed source generation", func() (bool, error) {
		for i, target := range source.Targets {
			pod, err := get(ctx, "pod", target.Pod)
			if err != nil {
				return false, err
			}
			uid := text(nested(pod, "metadata", "uid"))
			if uid != target.PodUID {
				return false, fmt.Errorf("source pod %s was replaced instead of restarted in place", target.Pod)
			}
			status := namedContainerStatus(pod, source.Container)
			if status == nil {
				return false, fmt.Errorf("source pod %s lost the bound application container status", target.Pod)
			}
			restarts := int(numberOrZero(nested(status, "restartCount")))
			ready := false
			conditions, _ := nested(pod, "status", "conditions").([]any)
			for _, condition := range conditions {
				ready = ready || nested(condition, "type") == "Ready" && nested(condition, "status") == "True"
			}
			obs := observation{Pod: target.Pod, PodUID: uid, Restarted: restarts > source.InitialRestart[uid], Unready: !ready}
			if !obs.Restarted || !obs.Unready {
				observations[i] = obs
				return false, nil
			}
			logs, err := command(ctx, "kubectl", "--context", kubeContext, "-n", namespace, "logs", target.Pod, "-c", source.Container, "--previous")
			if err != nil {
				return false, err
			}
			obs.Closed = strings.Contains(string(logs), "fencing: incarnation terminally fenced")
			observations[i] = obs
			if !obs.Closed {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	if err := verifyGCSPinnedImages(ctx, source, "old-return-"+source.ClusterID, out); err != nil {
		return nil, fmt.Errorf("verify pinned images after old-Pod restart: %w", err)
	}
	evidence := obj{"status": "qualified", "source": source.ClusterID, "observations": observations}
	path := filepath.Join(out, "gcs-old-pod-return-"+source.ClusterID+".json")
	if err := os.WriteFile(path, encoded(evidence), 0600); err != nil {
		return nil, err
	}
	return evidence, nil
}

func namedContainerStatus(pod any, name string) any {
	statuses, _ := nested(pod, "status", "containerStatuses").([]any)
	for _, status := range statuses {
		if nested(status, "name") == name {
			return status
		}
	}
	return nil
}

func sameFenceRequest(actual, expected operator.FenceRequest) bool {
	return string(encoded(actual)) == string(encoded(expected))
}

func sameFenceRequestSpec(spec any, expected operator.FenceRequest) bool {
	var actual operator.FenceRequest
	if err := json.Unmarshal(encoded(spec), &actual); err != nil {
		return false
	}
	return sameFenceRequest(actual, expected)
}

func captureGCSRecoverySource(ctx context.Context, out string) (gcsRecoverySource, error) {
	var source gcsRecoverySource
	ns, err := get(ctx, "namespace", namespace)
	if err != nil {
		return source, fmt.Errorf("read exact run namespace identity: %w", err)
	}
	source.NamespaceUID = text(nested(ns, "metadata", "uid"))
	if source.NamespaceUID == "" || text(nested(ns, "metadata", "labels", "chaos.rhiza.io/run")) != qualificationRunID {
		return source, fmt.Errorf("run namespace UID or owner label is unavailable")
	}
	bindings, err := get(ctx, "rhizaclusters", "")
	if err != nil {
		return source, err
	}
	items, _ := bindings["items"].([]any)
	var binding any
	for _, candidate := range items {
		active := text(nested(candidate, "status", "activeClusterID"))
		if active == "" || nested(candidate, "status", "phase") != "Healthy" {
			continue
		}
		if binding != nil {
			return source, fmt.Errorf("more than one healthy active RhizaCluster exists in the run namespace")
		}
		binding = candidate
	}
	if binding == nil {
		return source, fmt.Errorf("no healthy active RhizaCluster exists in the run namespace")
	}
	source.Namespace = text(nested(binding, "metadata", "namespace"))
	if source.Namespace == "" || source.Namespace != namespace {
		return source, fmt.Errorf("active RhizaCluster is not in the configured run namespace")
	}
	source.BindingName = text(nested(binding, "metadata", "name"))
	source.LogicalID = text(nested(binding, "spec", "logicalID"))
	source.StatefulSet = text(nested(binding, "spec", "statefulSet"))
	source.Container = text(nested(binding, "spec", "container"))
	if source.BindingName == "" || source.LogicalID == "" || source.StatefulSet == "" || source.Container == "" {
		return source, fmt.Errorf("active RhizaCluster does not identify its logical workload")
	}
	sts, err := get(ctx, "statefulset", source.StatefulSet)
	if err != nil {
		return source, err
	}
	appContainer, err := boundContainer(sts, source.Container)
	if err != nil {
		return source, err
	}
	config, configMapName, err := sourceConfig(ctx, appContainer)
	if err != nil {
		return source, err
	}
	source.ConfigMap = configMapName
	configuredClusterID := config["RHIZA_CLUSTER_ID"]
	source.Durability, err = effectiveGCSSetting(ctx, appContainer, "RHIZA_OBJSTORE_DURABILITY")
	if err != nil {
		return source, err
	}
	source.Bucket = config["RHIZA_OBJSTORE_BUCKET"]
	source.Endpoint = config["RHIZA_OBJSTORE_ENDPOINT"]
	basePrefix := strings.Trim(path.Clean(config["RHIZA_OBJSTORE_PREFIX"]), "/")
	if configuredClusterID == "" || source.Bucket == "" || basePrefix == "." || source.Endpoint != "" || config["RHIZA_OBJSTORE_PROVIDER"] != "gcs" || config["STORAGE_EMULATOR_HOST"] != "" {
		return source, fmt.Errorf("source is not configured for an explicit GCS object-store profile")
	}
	source.ClusterID = text(nested(binding, "status", "activeClusterID"))
	if source.ClusterID == "" || configuredClusterID != source.ClusterID || text(nested(binding, "status", "phase")) != "Healthy" {
		return source, fmt.Errorf("active RhizaCluster source differs from the configured workload generation")
	}
	source.Prefix = path.Join(basePrefix, source.ClusterID)
	source.StatefulSetUID = text(nested(sts, "metadata", "uid"))
	source.BindingUID = text(nested(binding, "metadata", "uid"))
	if source.StatefulSetUID == "" || source.BindingUID == "" || !podUsesConfigMap(nested(sts, "spec", "template"), source.ConfigMap) || !gcsWorkloadHasNoPVC(sts) {
		return source, fmt.Errorf("source workload and RhizaCluster identity are not bound")
	}
	replicasValue, ok := nested(sts, "spec", "replicas").(float64)
	if !ok {
		return source, fmt.Errorf("source voter count is unavailable")
	}
	replicas := int(replicasValue)
	if replicasValue != float64(replicas) || replicas < 1 || replicas > 64 {
		return source, fmt.Errorf("source voter count is outside the independent fence target limit")
	}
	pods, err := get(ctx, "pods", "")
	if err != nil {
		return source, err
	}
	selector, _ := nested(sts, "spec", "selector", "matchLabels").(map[string]any)
	if len(selector) == 0 {
		return source, fmt.Errorf("StatefulSet has no bound pod selector")
	}
	allPods, _ := pods["items"].([]any)
	source.InitialRestart = make(map[string]int)
	for _, pod := range allPods {
		if !labelsMatch(nested(pod, "metadata", "labels"), selector) || !gcsPodOwnedBy(pod, source.StatefulSetUID) {
			continue
		}
		name := text(nested(pod, "metadata", "name"))
		uid := text(nested(pod, "metadata", "uid"))
		if uid == "" || nested(pod, "metadata", "deletionTimestamp") != nil || !podUsesConfigMap(pod, source.ConfigMap) {
			return source, fmt.Errorf("source voter %s has no live Pod UID", name)
		}
		f, err := startForward(ctx, name, out)
		if err != nil {
			return source, err
		}
		status, statusErr := readMembership(ctx, f)
		f.close()
		if statusErr != nil || status.ClusterID != source.ClusterID || status.NodeID == "" || !status.Voting || status.Pending || status.WALIdentity == "" {
			return source, fmt.Errorf("source voter %s membership is not authenticated: %v", name, statusErr)
		}
		podObject, ok := pod.(obj)
		if !ok {
			return source, fmt.Errorf("source voter %s Pod record has an unexpected type", name)
		}
		target := capturedTarget(operator.FenceTarget{NodeID: name, Pod: name, PodUID: uid, WALIdentity: status.WALIdentity}, podObject)
		if target.NodeName == "" || target.ContainerID == "" || target.ImageID == "" {
			return source, fmt.Errorf("source voter %s runtime identity is incomplete", name)
		}
		source.Targets = append(source.Targets, target)
		statuses, _ := nested(pod, "status", "containerStatuses").([]any)
		if len(statuses) == 0 {
			return source, fmt.Errorf("source voter %s has no container restart baseline", name)
		}
		appStatus := namedContainerStatus(pod, source.Container)
		if appStatus == nil {
			return source, fmt.Errorf("source voter %s has no restart baseline for bound container %s", name, source.Container)
		}
		source.InitialRestart[uid] = int(numberOrZero(nested(appStatus, "restartCount")))
	}
	if len(source.Targets) != replicas {
		return source, fmt.Errorf("live StatefulSet voter cohort does not match its replica count")
	}
	source.OperatorIdentity, err = captureGCSOperatorPodIdentity(ctx)
	if err != nil {
		return source, err
	}
	source.OperatorPod = source.OperatorIdentity.Name
	if err := verifyGCSOperatorHTTPFencer(ctx, source.OperatorPod); err != nil {
		return source, err
	}
	source.FencerPod, err = captureGCSFencerPod(ctx)
	if err != nil {
		return source, err
	}
	source.AgentDaemonUID, source.AgentPods, err = captureGCSRuntimeAgentPods(ctx, source.Targets)
	if err != nil {
		return source, err
	}
	if err := verifyGCSPinnedImages(ctx, source, "source", out); err != nil {
		return source, err
	}
	return source, nil
}

func numberOrZero(value any) float64 {
	n, _ := value.(float64)
	return n
}

func ownerRefs(resource any) []any {
	owners, _ := nested(resource, "metadata", "ownerReferences").([]any)
	return owners
}

func gcsFaultSourceMatches(fault *gcsChaosFault, expected []gcsControlPodIdentity) bool {
	if fault == nil || len(fault.sourceUIDs) != len(expected) {
		return false
	}
	for _, identity := range expected {
		if fault.sourceUIDs[identity.Name] != identity.UID || identity.Name == "" || identity.UID == "" || identity.OwnerUID == "" {
			return false
		}
	}
	return true
}

func gcsControlPodSnapshotMatches(expected gcsControlPodIdentity, currentName, currentUID, currentOwnerName, currentOwnerUID, currentParentOwnerUID string) bool {
	return expected.Name == currentName && expected.UID != "" && expected.UID == currentUID && expected.OwnerUID != "" &&
		(expected.OwnerName == "" || expected.OwnerName == currentOwnerName) && expected.OwnerUID == currentOwnerUID &&
		expected.ParentOwnerUID == currentParentOwnerUID
}

func validateGCSOperatorRestartIdentity(previous, current gcsControlPodIdentity) error {
	if previous.Name == "" || previous.UID == "" || previous.OwnerName == "" || previous.OwnerUID == "" || previous.ParentOwnerUID == "" ||
		current.Name == "" || current.UID == "" || current.OwnerName == "" || current.OwnerUID == "" || current.ParentOwnerUID == "" {
		return fmt.Errorf("operator restart identity is incomplete")
	}
	if current.UID == previous.UID || current.ParentOwnerUID != previous.ParentOwnerUID {
		return fmt.Errorf("operator restart did not produce a new Pod under the captured Deployment")
	}
	return nil
}

func verifyGCSControlPodSnapshots(ctx context.Context, expected []gcsControlPodIdentity) error {
	if len(expected) == 0 {
		return fmt.Errorf("captured control Pod identity set is empty")
	}
	for _, identity := range expected {
		pod, err := get(ctx, "pod", identity.Name)
		if err != nil {
			return err
		}
		if text(nested(pod, "metadata", "uid")) != identity.UID || nested(pod, "metadata", "deletionTimestamp") != nil {
			return fmt.Errorf("control Pod %s incarnation changed", identity.Name)
		}
		ownerName, ownerUID, ownerKind := "", "", ""
		for _, owner := range ownerRefs(pod) {
			if nested(owner, "controller") == true {
				if ownerKind != "" {
					return fmt.Errorf("control Pod %s has multiple controller owners", identity.Name)
				}
				ownerName, ownerUID, ownerKind = text(nested(owner, "name")), text(nested(owner, "uid")), text(nested(owner, "kind"))
			}
		}
		parentUID := ""
		if identity.ParentOwnerUID != "" {
			if ownerKind != "ReplicaSet" || identity.OwnerName == "" || ownerName != identity.OwnerName {
				return fmt.Errorf("operator Pod %s no longer has its captured ReplicaSet owner", identity.Name)
			}
			rs, err := get(ctx, "replicaset", identity.OwnerName)
			if err != nil {
				return err
			}
			if text(nested(rs, "metadata", "name")) != identity.OwnerName || text(nested(rs, "metadata", "uid")) != identity.OwnerUID {
				return fmt.Errorf("operator Pod %s ReplicaSet owner changed", identity.Name)
			}
			deploymentOwners := 0
			for _, owner := range ownerRefs(rs) {
				if nested(owner, "controller") == true {
					deploymentOwners++
					if nested(owner, "kind") == "Deployment" && nested(owner, "name") == "rhiza-operator" {
						parentUID = text(nested(owner, "uid"))
					}
				}
			}
			if deploymentOwners != 1 || parentUID != identity.ParentOwnerUID {
				return fmt.Errorf("operator ReplicaSet Deployment owner changed")
			}
			deployment, err := get(ctx, "deployment", "rhiza-operator")
			if err != nil {
				return err
			}
			if text(nested(deployment, "metadata", "name")) != "rhiza-operator" || text(nested(deployment, "metadata", "uid")) != identity.ParentOwnerUID {
				return fmt.Errorf("operator Deployment owner changed")
			}
		}
		if !gcsControlPodSnapshotMatches(identity, identity.Name, text(nested(pod, "metadata", "uid")), ownerName, ownerUID, parentUID) {
			return fmt.Errorf("control Pod %s owner identity changed", identity.Name)
		}
	}
	return nil
}

func captureGCSOperatorPodIdentity(ctx context.Context) (gcsControlPodIdentity, error) {
	var result gcsControlPodIdentity
	deployment, err := get(ctx, "deployment", "rhiza-operator")
	if err != nil {
		return result, err
	}
	deploymentUID := text(nested(deployment, "metadata", "uid"))
	selector, _ := nested(deployment, "spec", "selector", "matchLabels").(map[string]any)
	if deploymentUID == "" || len(selector) == 0 || nested(deployment, "spec", "replicas") != float64(1) {
		return result, fmt.Errorf("operator Deployment must have one run-owned replica and exact selector")
	}
	sets, err := get(ctx, "replicasets", "")
	if err != nil {
		return result, err
	}
	items, _ := sets["items"].([]any)
	ownedRS := map[string]string{}
	for _, rs := range items {
		for _, owner := range ownerRefs(rs) {
			if nested(owner, "kind") == "Deployment" && nested(owner, "name") == "rhiza-operator" && nested(owner, "uid") == deploymentUID && nested(owner, "controller") == true {
				rsName, rsUID := text(nested(rs, "metadata", "name")), text(nested(rs, "metadata", "uid"))
				if rsName == "" || rsUID == "" || ownedRS[rsUID] != "" {
					return result, fmt.Errorf("operator ReplicaSet identity is missing or ambiguous")
				}
				ownedRS[rsUID] = rsName
			}
		}
	}
	pods, err := get(ctx, "pods", "")
	if err != nil {
		return result, err
	}
	var found []gcsControlPodIdentity
	allPods, _ := pods["items"].([]any)
	for _, pod := range allPods {
		if !labelsMatch(nested(pod, "metadata", "labels"), selector) || nested(pod, "metadata", "deletionTimestamp") != nil {
			continue
		}
		for _, owner := range ownerRefs(pod) {
			ownerUID, ownerName := text(nested(owner, "uid")), text(nested(owner, "name"))
			if nested(owner, "kind") == "ReplicaSet" && ownedRS[ownerUID] != "" && ownedRS[ownerUID] == ownerName && nested(owner, "controller") == true {
				found = append(found, gcsControlPodIdentity{Name: text(nested(pod, "metadata", "name")), UID: text(nested(pod, "metadata", "uid")), OwnerName: ownerName, OwnerUID: ownerUID, ParentOwnerUID: deploymentUID})
			}
		}
	}
	if len(found) != 1 || found[0].Name == "" || found[0].UID == "" {
		return result, fmt.Errorf("operator Pod is not uniquely bound to the run-owned Deployment")
	}
	return found[0], nil
}

func restartGCSOperatorAtRemoveCheckpointAndCapture(ctx context.Context, source gcsRecoverySource) (gcsControlPodIdentity, error) {
	if err := restartGCSOperatorAtRemoveCheckpoint(ctx, source); err != nil {
		return gcsControlPodIdentity{}, err
	}
	current, err := captureGCSOperatorPodIdentity(ctx)
	if err != nil {
		return gcsControlPodIdentity{}, fmt.Errorf("capture replacement operator Pod identity: %w", err)
	}
	if err := validateGCSOperatorRestartIdentity(source.OperatorIdentity, current); err != nil {
		return gcsControlPodIdentity{}, err
	}
	if err := verifyGCSControlPodSnapshots(ctx, []gcsControlPodIdentity{current}); err != nil {
		return gcsControlPodIdentity{}, fmt.Errorf("replacement operator Pod identity changed after restart: %w", err)
	}
	pod, err := get(ctx, "pod", current.Name)
	if err != nil {
		return gcsControlPodIdentity{}, err
	}
	if !podReady(pod) {
		return gcsControlPodIdentity{}, fmt.Errorf("replacement operator Pod is not Ready")
	}
	if err := verifyGCSOperatorHTTPFencer(ctx, current.Name); err != nil {
		return gcsControlPodIdentity{}, err
	}
	return current, nil
}

func verifyGCSOperatorHTTPFencer(ctx context.Context, podName string) error {
	pod, err := get(ctx, "pod", podName)
	if err != nil {
		return err
	}
	containers, _ := nested(pod, "spec", "containers").([]any)
	var operatorContainer any
	for _, container := range containers {
		if nested(container, "name") == "operator" {
			if operatorContainer != nil {
				return fmt.Errorf("operator Pod has ambiguous operator containers")
			}
			operatorContainer = container
		}
	}
	if operatorContainer == nil {
		return fmt.Errorf("operator Pod has no deployed operator container")
	}
	values := make(map[string]string)
	if err := mergeGCSOperatorEnvFrom(ctx, operatorContainer, values); err != nil {
		return err
	}
	env, _ := nested(operatorContainer, "env").([]any)
	for _, item := range env {
		name := text(nested(item, "name"))
		if name != "RHIZA_FENCER_BACKEND" && name != "RHIZA_FENCER_URL" && name != "RHIZA_FENCER_TOKEN_FILE" && name != "RHIZA_FENCER_CA_FILE" {
			continue
		}
		if value := nested(item, "value"); value != nil {
			stringValue, ok := value.(string)
			if !ok {
				return fmt.Errorf("operator %s environment value is not a string", name)
			}
			values[name] = stringValue
			continue
		}
		if key := text(nested(item, "valueFrom", "configMapKeyRef", "key")); key != "" {
			configMapName := text(nested(item, "valueFrom", "configMapKeyRef", "name"))
			configMap, err := get(ctx, "configmap", configMapName)
			if err != nil {
				return err
			}
			values[name] = text(nested(configMap, "data", key))
			continue
		}
		if key := text(nested(item, "valueFrom", "secretKeyRef", "key")); key != "" {
			secretName := text(nested(item, "valueFrom", "secretKeyRef", "name"))
			secret, err := get(ctx, "secret", secretName)
			if err != nil {
				return err
			}
			decoded, err := base64.StdEncoding.DecodeString(text(nested(secret, "data", key)))
			if err != nil {
				return fmt.Errorf("decode deployed operator %s environment reference: %w", name, err)
			}
			values[name] = string(decoded)
			continue
		}
		return fmt.Errorf("operator %s environment uses an unresolvable value source", name)
	}
	backend := strings.TrimSpace(values["RHIZA_FENCER_BACKEND"])
	if backend == "kubernetes" {
		return fmt.Errorf("GCS control-isolation scenario requires operator HTTP fencing; deployed operator explicitly selects Kubernetes")
	}
	if backend != "" && backend != "http" {
		return fmt.Errorf("deployed operator has an unsupported fencing backend %q", backend)
	}
	if strings.TrimSpace(values["RHIZA_FENCER_URL"]) == "" || strings.TrimSpace(values["RHIZA_FENCER_TOKEN_FILE"]) == "" {
		return fmt.Errorf("deployed operator does not have an effective HTTP fencer URL and token file")
	}
	return nil
}

func mergeGCSOperatorEnvFrom(ctx context.Context, container any, values map[string]string) error {
	refs, _ := nested(container, "envFrom").([]any)
	for _, ref := range refs {
		prefix := text(nested(ref, "prefix"))
		var data map[string]any
		if name := text(nested(ref, "configMapRef", "name")); name != "" {
			configMap, err := get(ctx, "configmap", name)
			if err != nil {
				if nested(ref, "configMapRef", "optional") == true {
					continue
				}
				return err
			}
			data, _ = nested(configMap, "data").(map[string]any)
		} else if name := text(nested(ref, "secretRef", "name")); name != "" {
			secret, err := get(ctx, "secret", name)
			if err != nil {
				if nested(ref, "secretRef", "optional") == true {
					continue
				}
				return err
			}
			encodedData, _ := nested(secret, "data").(map[string]any)
			data = make(map[string]any, len(encodedData))
			for key, value := range encodedData {
				decoded, err := base64.StdEncoding.DecodeString(text(value))
				if err != nil {
					return fmt.Errorf("decode operator envFrom Secret value: %w", err)
				}
				data[key] = string(decoded)
			}
		} else {
			return fmt.Errorf("operator envFrom has no resolvable ConfigMap or Secret source")
		}
		for _, key := range []string{"RHIZA_FENCER_BACKEND", "RHIZA_FENCER_URL", "RHIZA_FENCER_TOKEN_FILE", "RHIZA_FENCER_CA_FILE"} {
			if value, exists := data[key]; exists {
				values[prefix+key] = text(value)
			}
		}
	}
	return nil
}

func captureGCSFencerPod(ctx context.Context) (string, error) {
	deployment, err := get(ctx, "deployment", "rhiza-fencer")
	if err != nil {
		return "", err
	}
	deploymentUID := text(nested(deployment, "metadata", "uid"))
	selector, _ := nested(deployment, "spec", "selector", "matchLabels").(map[string]any)
	if deploymentUID == "" || len(selector) == 0 || nested(deployment, "spec", "replicas") != float64(1) {
		return "", fmt.Errorf("fencer Deployment must have one run-owned replica and exact selector")
	}
	sets, err := get(ctx, "replicasets", "")
	if err != nil {
		return "", err
	}
	items, _ := sets["items"].([]any)
	ownedRS := map[string]bool{}
	for _, rs := range items {
		for _, owner := range ownerRefs(rs) {
			if nested(owner, "kind") == "Deployment" && nested(owner, "uid") == deploymentUID && nested(owner, "controller") == true {
				ownedRS[text(nested(rs, "metadata", "uid"))] = true
			}
		}
	}
	pods, err := get(ctx, "pods", "")
	if err != nil {
		return "", err
	}
	var names []string
	allPods, _ := pods["items"].([]any)
	for _, pod := range allPods {
		if !labelsMatch(nested(pod, "metadata", "labels"), selector) || nested(pod, "metadata", "deletionTimestamp") != nil {
			continue
		}
		for _, owner := range ownerRefs(pod) {
			if nested(owner, "kind") == "ReplicaSet" && ownedRS[text(nested(owner, "uid"))] && nested(owner, "controller") == true {
				names = append(names, text(nested(pod, "metadata", "name")))
			}
		}
	}
	if len(names) != 1 || names[0] == "" {
		return "", fmt.Errorf("fencer Pod is not uniquely bound to the run-owned Deployment")
	}
	return names[0], nil
}

func captureGCSRuntimeAgentPods(ctx context.Context, targets []operator.FenceTarget) (string, []gcsControlPodIdentity, error) {
	daemonSet, err := get(ctx, "daemonset", "rhiza-runtime-agent")
	if err != nil {
		return "", nil, err
	}
	daemonSetUID := text(nested(daemonSet, "metadata", "uid"))
	selector, _ := nested(daemonSet, "spec", "selector", "matchLabels").(map[string]any)
	if daemonSetUID == "" || len(selector) == 0 {
		return "", nil, fmt.Errorf("runtime-agent DaemonSet identity is incomplete")
	}
	pods, err := get(ctx, "pods", "")
	if err != nil {
		return "", nil, err
	}
	byNode := make(map[string]gcsControlPodIdentity)
	allPods, _ := pods["items"].([]any)
	for _, pod := range allPods {
		if !labelsMatch(nested(pod, "metadata", "labels"), selector) || nested(pod, "metadata", "deletionTimestamp") != nil {
			continue
		}
		owned := false
		for _, owner := range ownerRefs(pod) {
			owned = owned || nested(owner, "kind") == "DaemonSet" && nested(owner, "uid") == daemonSetUID && nested(owner, "controller") == true
		}
		if !owned {
			continue
		}
		node := text(nested(pod, "spec", "nodeName"))
		name := text(nested(pod, "metadata", "name"))
		_, alreadyCaptured := byNode[node]
		if node == "" || name == "" || alreadyCaptured {
			return "", nil, fmt.Errorf("runtime-agent Pod identity is ambiguous on source node %s", node)
		}
		uid := text(nested(pod, "metadata", "uid"))
		if uid == "" {
			return "", nil, fmt.Errorf("runtime-agent Pod %s has no UID", name)
		}
		byNode[node] = gcsControlPodIdentity{Name: name, UID: uid, OwnerUID: daemonSetUID}
	}
	seenNodes := map[string]bool{}
	var identities []gcsControlPodIdentity
	for _, target := range targets {
		if target.NodeName == "" || seenNodes[target.NodeName] {
			continue
		}
		seenNodes[target.NodeName] = true
		identity := byNode[target.NodeName]
		if identity.Name == "" {
			return "", nil, fmt.Errorf("no runtime-agent Pod on captured voter node %s", target.NodeName)
		}
		identities = append(identities, identity)
	}
	if len(identities) == 0 {
		return "", nil, fmt.Errorf("source voter cohort has no runtime-agent Pod")
	}
	return daemonSetUID, identities, nil
}

func imageIDMatchesPinned(imageID, pinned string) bool {
	expected, err := immutableDigest(pinned)
	if err != nil || imageID == "" {
		return false
	}
	actual, err := immutableDigest(digest(imageID))
	return err == nil && actual == expected
}

func gcsChaosEvidence(fault *gcsChaosFault) obj {
	if fault == nil {
		return obj{"status": "missing"}
	}
	return obj{
		"name":       fault.name,
		"uid":        fault.uid,
		"namespace":  fault.namespace,
		"runID":      fault.runID,
		"mode":       fault.mode,
		"selector":   fault.selector,
		"target":     fault.target,
		"sourceUIDs": fault.sourceUIDs,
		"targetUIDs": fault.targetUIDs,
	}
}

func verifyGCSPodImage(pod any, containerName, pinned string) (obj, error) {
	name := text(nested(pod, "metadata", "name"))
	uid := text(nested(pod, "metadata", "uid"))
	if name == "" || uid == "" || nested(pod, "status", "phase") != "Running" || nested(pod, "metadata", "deletionTimestamp") != nil {
		return nil, fmt.Errorf("image evidence Pod is not a live Running incarnation")
	}
	containers, _ := nested(pod, "spec", "containers").([]any)
	var spec any
	for _, candidate := range containers {
		if nested(candidate, "name") == containerName {
			if spec != nil {
				return nil, fmt.Errorf("image evidence container is ambiguous")
			}
			spec = candidate
		}
	}
	if spec == nil || nested(spec, "image") != pinned {
		return nil, fmt.Errorf("Pod %s does not declare its exact pinned %s image", name, containerName)
	}
	status := namedContainerStatus(pod, containerName)
	imageID := text(nested(status, "imageID"))
	if status == nil || !imageIDMatchesPinned(imageID, pinned) {
		return nil, fmt.Errorf("Pod %s observed imageID does not match the pinned %s digest", name, containerName)
	}
	return obj{"pod": name, "uid": uid, "container": containerName, "image": pinned, "imageID": imageID}, nil
}

func verifyGCSPinnedImages(ctx context.Context, source gcsRecoverySource, phase, out string) error {
	appImage, operatorImage := os.Getenv("RHIZA_APP_IMAGE"), os.Getenv("RHIZA_OPERATOR_IMAGE")
	if _, err := immutableDigest(appImage); err != nil {
		return fmt.Errorf("RHIZA_APP_IMAGE must be digest-pinned: %w", err)
	}
	if _, err := immutableDigest(operatorImage); err != nil {
		return fmt.Errorf("RHIZA_OPERATOR_IMAGE must be digest-pinned: %w", err)
	}
	pods, err := get(ctx, "pods", "")
	if err != nil {
		return err
	}
	items, _ := pods["items"].([]any)
	byName := make(map[string]any, len(items))
	for _, pod := range items {
		byName[text(nested(pod, "metadata", "name"))] = pod
	}
	evidence := obj{"phase": phase, "runID": qualificationRunID, "sourceClusterID": source.ClusterID, "statefulSetUID": source.StatefulSetUID}
	var voterEvidence []obj
	for _, target := range source.Targets {
		pod := byName[target.Pod]
		if text(nested(pod, "metadata", "uid")) != target.PodUID {
			return fmt.Errorf("pinned-image voter Pod %s changed UID", target.Pod)
		}
		entry, err := verifyGCSPodImage(pod, source.Container, appImage)
		if err != nil {
			return err
		}
		voterEvidence = append(voterEvidence, entry)
	}
	evidence["voters"] = voterEvidence
	for _, rolePod := range []struct{ role, pod, container string }{{"operator", source.OperatorPod, "operator"}, {"fencer", source.FencerPod, "authority"}} {
		pod := byName[rolePod.pod]
		entry, err := verifyGCSPodImage(pod, rolePod.container, operatorImage)
		if err != nil {
			return fmt.Errorf("%s image inventory: %w", rolePod.role, err)
		}
		evidence[rolePod.role] = entry
	}
	daemonSet, err := get(ctx, "daemonset", "rhiza-runtime-agent")
	if err != nil {
		return err
	}
	if text(nested(daemonSet, "metadata", "uid")) != source.AgentDaemonUID || source.AgentDaemonUID == "" {
		return fmt.Errorf("runtime-agent DaemonSet UID changed after source capture")
	}
	if _, err := verifyGCSPodImage(obj{"metadata": obj{"name": "runtime-agent-template", "uid": source.AgentDaemonUID}, "status": obj{"phase": "Running"}, "spec": nested(daemonSet, "spec", "template", "spec")}, "runtime-agent", operatorImage); err != nil {
		return fmt.Errorf("runtime-agent DaemonSet image template: %w", err)
	}
	selector, _ := nested(daemonSet, "spec", "selector", "matchLabels").(map[string]any)
	owned := 0
	ready := 0
	var agentEvidence []obj
	for _, pod := range items {
		isOwned := false
		for _, owner := range ownerRefs(pod) {
			isOwned = isOwned || nested(owner, "kind") == "DaemonSet" && nested(owner, "uid") == source.AgentDaemonUID && nested(owner, "controller") == true
		}
		if !isOwned {
			continue
		}
		owned++
		if !labelsMatch(nested(pod, "metadata", "labels"), selector) {
			return fmt.Errorf("runtime-agent owned Pod is outside its DaemonSet selector")
		}
		entry, err := verifyGCSPodImage(pod, "runtime-agent", operatorImage)
		if err != nil {
			return fmt.Errorf("runtime-agent owned Pod image: %w", err)
		}
		conditions, _ := nested(pod, "status", "conditions").([]any)
		for _, condition := range conditions {
			ready = ready + btoi(nested(condition, "type") == "Ready" && nested(condition, "status") == "True")
		}
		agentEvidence = append(agentEvidence, entry)
	}
	desired := int(numberOrZero(nested(daemonSet, "status", "desiredNumberScheduled")))
	if desired == 0 || owned != desired || ready != desired {
		return fmt.Errorf("runtime-agent DaemonSet owned-pod inventory is incomplete: desired=%d owned=%d ready=%d", desired, owned, ready)
	}
	evidence["runtimeAgent"] = obj{"daemonSetUID": source.AgentDaemonUID, "desiredPods": desired, "pods": agentEvidence}
	return os.WriteFile(filepath.Join(out, "gcs-image-evidence-"+phase+".json"), encoded(evidence), 0600)
}

func btoi(value bool) int {
	if value {
		return 1
	}
	return 0
}

type gcsAcknowledgement struct {
	RequestID string `json:"requestID"`
	Slot      uint64 `json:"slot"`
	Rows      string `json:"rows"`
}

func writeGCSAcknowledgedRow(ctx context.Context, f *forward, token string, id int, value string) (gcsAcknowledgement, error) {
	var ack gcsAcknowledgement
	if id < 1 || value == "" || strings.ContainsAny(value, "'\r\n") {
		return ack, fmt.Errorf("acknowledged row identity is invalid")
	}
	requestID := fmt.Sprintf("gcs-ha-ack-%d", id)
	if _, err := call(ctx, f, "/sql/execute", obj{"request_id": requestID, "sql": fmt.Sprintf("INSERT INTO fencing_items VALUES (%d, '%s')", id, value)}, token); err != nil {
		return ack, err
	}
	var receipt struct {
		State   string `json:"state"`
		Receipt *struct {
			Slot   uint64 `json:"slot"`
			Status string `json:"status"`
		} `json:"receipt"`
	}
	status, err := call(ctx, f, "/request/status", obj{"kind": "sql", "request_id": requestID}, token)
	if err != nil {
		return ack, err
	}
	if err := json.Unmarshal(encoded(status), &receipt); err != nil || receipt.State != "committed" || receipt.Receipt == nil || receipt.Receipt.Status != "committed" || receipt.Receipt.Slot == 0 {
		return ack, fmt.Errorf("ACK request status lacks the exact committed receipt")
	}
	rows, err := call(ctx, f, "/sql/query", obj{"sql": fmt.Sprintf("SELECT id,value FROM fencing_items WHERE id = %d", id), "consistency": "linearizable"}, token)
	if err != nil {
		return ack, err
	}
	if string(encoded(rows["rows"])) != fmt.Sprintf("[[%d,%q]]", id, value) {
		return ack, fmt.Errorf("GCS scenario ACK row %d was not read back", id)
	}
	ack = gcsAcknowledgement{RequestID: requestID, Slot: receipt.Receipt.Slot, Rows: string(encoded(rows["rows"]))}
	return ack, nil
}

func boundContainer(statefulSet any, name string) (any, error) {
	containers, _ := nested(statefulSet, "spec", "template", "spec", "containers").([]any)
	var found any
	for _, candidate := range containers {
		if nested(candidate, "name") == name {
			if found != nil {
				return nil, fmt.Errorf("bound workload container name is ambiguous")
			}
			found = candidate
		}
	}
	if found == nil {
		return nil, fmt.Errorf("bound application container is absent from StatefulSet")
	}
	return found, nil
}

func sourceConfig(ctx context.Context, container any) (map[string]string, string, error) {
	config := make(map[string]string)
	refs, _ := nested(container, "envFrom").([]any)
	var profileConfigMap string
	for _, ref := range refs {
		name := text(nested(ref, "configMapRef", "name"))
		if name == "" {
			continue
		}
		cm, err := get(ctx, "configmap", name)
		if err != nil {
			return nil, "", err
		}
		data, _ := nested(cm, "data").(map[string]any)
		prefix := text(nested(ref, "prefix"))
		for key, value := range data {
			if _, ok := value.(string); !ok {
				continue
			}
			name := prefix + key
			if isGCSProfileKey(name) {
				config[name] = value.(string)
			}
		}
		if data["RHIZA_OBJSTORE_PROVIDER"] != nil {
			if profileConfigMap != "" && profileConfigMap != name {
				return nil, "", fmt.Errorf("multiple ConfigMaps define the object-store provider")
			}
			profileConfigMap = name
		}
	}
	env, _ := nested(container, "env").([]any)
	for _, entry := range env {
		key := text(nested(entry, "name"))
		if !isGCSProfileKey(key) {
			continue
		}
		if value, ok := nested(entry, "value").(string); ok && value != "" {
			config[key] = value
			continue
		}
		refName := text(nested(entry, "valueFrom", "configMapKeyRef", "name"))
		refKey := text(nested(entry, "valueFrom", "configMapKeyRef", "key"))
		if refName == "" || refKey == "" {
			continue
		}
		cm, err := get(ctx, "configmap", refName)
		if err != nil {
			return nil, "", err
		}
		if value := text(nested(cm, "data", refKey)); value != "" {
			config[key] = value
		}
		if key == "RHIZA_OBJSTORE_PROVIDER" {
			if profileConfigMap != "" && profileConfigMap != refName {
				return nil, "", fmt.Errorf("multiple ConfigMaps define the object-store provider")
			}
			profileConfigMap = refName
		}
	}
	if config["RHIZA_CLUSTER_ID"] == "" {
		return nil, "", fmt.Errorf("source workload environment does not set RHIZA_CLUSTER_ID")
	}
	return config, profileConfigMap, nil
}

func isGCSProfileKey(key string) bool {
	switch key {
	case "RHIZA_CLUSTER_ID", "RHIZA_OBJSTORE_PROVIDER", "RHIZA_OBJSTORE_BUCKET", "RHIZA_OBJSTORE_ENDPOINT", "RHIZA_OBJSTORE_PREFIX", "STORAGE_EMULATOR_HOST":
		return true
	default:
		return false
	}
}

func podUsesConfigMap(workload any, configMap string) bool {
	if configMap == "" {
		return false
	}
	spec := nested(workload, "spec", "template", "spec")
	if spec == nil {
		spec = nested(workload, "spec")
	}
	containers, _ := nested(spec, "containers").([]any)
	if len(containers) == 0 {
		return false
	}
	for _, container := range containers {
		from, _ := nested(container, "envFrom").([]any)
		for _, item := range from {
			if nested(item, "configMapRef", "name") == configMap {
				return true
			}
		}
		env, _ := nested(container, "env").([]any)
		for _, item := range env {
			if nested(item, "valueFrom", "configMapKeyRef", "name") == configMap {
				return true
			}
		}
	}
	return false
}

func labelsMatch(labelsValue any, required map[string]any) bool {
	labels, _ := labelsValue.(map[string]any)
	for key, value := range required {
		if labels[key] != value {
			return false
		}
	}
	return len(required) != 0
}

func gcsPodOwnedBy(pod any, stsUID string) bool {
	owners, _ := nested(pod, "metadata", "ownerReferences").([]any)
	for _, owner := range owners {
		if nested(owner, "kind") == "StatefulSet" && nested(owner, "uid") == stsUID && nested(owner, "controller") == true {
			return true
		}
	}
	return false
}

func gcsWorkloadHasNoPVC(sts any) bool {
	claims, _ := nested(sts, "spec", "volumeClaimTemplates").([]any)
	if len(claims) != 0 {
		return false
	}
	volumes, _ := nested(sts, "spec", "template", "spec", "volumes").([]any)
	allowedEphemeral := map[string]bool{"emptyDir": true, "configMap": true, "secret": true, "projected": true, "downwardAPI": true}
	for _, volume := range volumes {
		var sources []string
		for _, source := range []string{"emptyDir", "configMap", "secret", "projected", "downwardAPI", "persistentVolumeClaim", "hostPath", "csi", "ephemeral", "nfs", "local", "awsElasticBlockStore", "azureDisk", "azureFile", "cephfs", "cinder", "fc", "flexVolume", "flocker", "gcePersistentDisk", "glusterfs", "iscsi", "photonPersistentDisk", "portworxVolume", "quobyte", "rbd", "scaleIO", "storageos", "vsphereVolume"} {
			if nested(volume, source) != nil {
				sources = append(sources, source)
			}
		}
		if len(sources) != 1 || !allowedEphemeral[sources[0]] {
			return false
		}
	}
	return true
}

func pendingFenceIdentityMatches(item any, pending pendingGCSFence) bool {
	return pending.Name != "" && pending.UID != "" && text(nested(item, "metadata", "name")) == pending.Name && text(nested(item, "metadata", "uid")) == pending.UID
}

func exactPendingGCSFenceRequest(item any, pending pendingGCSFence) (operator.FenceRequest, error) {
	var request operator.FenceRequest
	if !pendingFenceIdentityMatches(item, pending) {
		return request, fmt.Errorf("pending generation fence resource identity changed")
	}
	if err := json.Unmarshal(encoded(nested(item, "spec")), &request); err != nil {
		return request, err
	}
	if !sameFenceRequest(request, pending.Request) {
		return request, fmt.Errorf("pending generation fence request changed")
	}
	return request, nil
}

func waitForGCSRecovery(ctx context.Context, source gcsRecoverySource, pending pendingGCSFence, expectedBase, expectedRows, out string, explicitRecovery *explicitGCSRecoveryRef) (*gcsRecoveryOutcome, error) {
	if source.ClusterID == "" || source.StatefulSetUID == "" || source.BindingUID == "" || source.Bucket == "" || source.Prefix == "" || len(source.Targets) == 0 || expectedBase != "fork" && expectedBase != "empty" || expectedRows == "" {
		return nil, fmt.Errorf("GCS recovery observation contract is incomplete")
	}
	if pending.Name == "" || pending.UID == "" || pending.Request.OperationID == "" || pending.Request.SourceClusterID != source.ClusterID || !sameTargets(pending.Request.Targets, source.Targets) {
		return nil, fmt.Errorf("GCS recovery is not bound to the exact pending fence")
	}
	outcome := &gcsRecoveryOutcome{Source: source}
	if err := wait(ctx, "IndependentV2 generation fence", func() (bool, error) {
		list, err := get(ctx, "rhizafences", "")
		if err != nil {
			return false, err
		}
		items, _ := list["items"].([]any)
		for _, item := range items {
			if text(nested(item, "metadata", "name")) != pending.Name {
				continue
			}
			candidate, err := exactPendingGCSFenceRequest(item, pending)
			if err != nil {
				return false, err
			}
			if candidate.Scope != operator.ScopeGeneration || candidate.SourceClusterID != source.ClusterID || !sameTargets(candidate.Targets, source.Targets) {
				return false, fmt.Errorf("pending generation fence no longer matches the captured source cohort")
			}
			if nested(item, "status", "proof") == nil {
				continue
			}
			request, proof, err := verifyIndependentGenerationProof(item, source.ClusterID, source.StatefulSetUID, source.BindingUID, source.Targets, source.Bucket, source.Prefix, source.Namespace, source.StatefulSet, source.LogicalID)
			if err != nil {
				return false, err
			}
			readback, err := get(ctx, "rhizafence", pending.Name)
			if err != nil {
				return false, fmt.Errorf("read back authority generation proof through Kubernetes API: %w", err)
			}
			if err := verifyGCSFenceProofReadback(item, readback, pending, source); err != nil {
				return false, err
			}
			if err := verifyGCSGenerationProofAgainstCanonicalRoot(ctx, readback, source); err != nil {
				return false, err
			}
			outcome.Fence, outcome.Proof = request, proof
			return true, nil
		}
		return false, nil
	}); err != nil {
		return nil, err
	}
	return waitForGCSRecoveryAfterProof(ctx, source, pending, outcome.Fence, outcome.Proof, expectedBase, expectedRows, out, explicitRecovery)
}

// waitForGCSRecoveryAfterProof is the common completion/readback tail shared by
// ordinary and mixed generation recovery. Callers must verify the generation
// proof with their own exact target/terminal contract before entering here.
func waitForGCSRecoveryAfterProof(ctx context.Context, source gcsRecoverySource, pending pendingGCSFence,
	request operator.FenceRequest, proof operator.FenceProof, expectedBase, expectedRows, out string,
	explicitRecovery *explicitGCSRecoveryRef) (*gcsRecoveryOutcome, error) {
	if err := validateGCSRecoveryAfterProofInputs(source, pending, request, proof, expectedBase, expectedRows); err != nil {
		return nil, err
	}
	outcome := &gcsRecoveryOutcome{Source: source, Fence: request, Proof: proof}
	if err := wait(ctx, "committed recovered generation", func() (bool, error) {
		list, err := get(ctx, "rhizarecoveries", "")
		if err != nil {
			return false, err
		}
		items, _ := list["items"].([]any)
		matches := 0
		for _, item := range items {
			if explicitRecovery != nil {
				if nested(item, "metadata", "name") != explicitRecovery.Name && nested(item, "spec", "recoveryID") != explicitRecovery.OperationID {
					continue
				}
				if err := validateExplicitRecoveryResource(item, source, *explicitRecovery); err != nil {
					return false, err
				}
			} else if nested(item, "spec", "sourceClusterID") != source.ClusterID || nested(item, "spec", "recoveryID") != pending.Request.OperationID {
				continue
			}
			matches++
			if err := json.Unmarshal(encoded(item), &outcome.Recovery); err != nil {
				return false, err
			}
		}
		if matches > 1 {
			return false, fmt.Errorf("multiple recoveries match the captured fence operation")
		}
		if matches == 0 || outcome.Recovery.Status.Phase != "Complete" || outcome.Recovery.Status.Stage != "Complete" {
			return false, nil
		}
		if explicitRecovery != nil && (outcome.Recovery.Metadata["uid"] != explicitRecovery.UID || outcome.Recovery.Metadata["name"] != explicitRecovery.Name ||
			outcome.Recovery.Metadata["namespace"] != explicitRecovery.Namespace) {
			return false, fmt.Errorf("completed recovery does not match the captured resource identity")
		}
		if outcome.Recovery.Spec.SourceClusterID != source.ClusterID || outcome.Recovery.Spec.RecoveryID != request.OperationID ||
			outcome.Recovery.Status.Source != source.ClusterID || outcome.Recovery.Status.StatefulSetUID != source.StatefulSetUID ||
			outcome.Recovery.Status.TargetBaseKind != expectedBase ||
			(expectedBase == "fork" && (outcome.Recovery.Spec.Durability != "before-ack" || outcome.Recovery.Status.SourceDurability != "before-ack")) ||
			(expectedBase == "empty" && (outcome.Recovery.Status.RecoveredTip != 0 || outcome.Recovery.Status.ManifestHash != "")) {
			return false, fmt.Errorf("completed recovery differs from the captured source or expected archive base")
		}
		if err := recoveryFenceMatchesProof(outcome.Recovery, request, proof); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		return nil, err
	}
	if err := waitForGCSRecoveryTargetAdoption(ctx, source, outcome.Recovery); err != nil {
		return nil, fmt.Errorf("controller root did not adopt the completed recovered target: %w", err)
	}
	secretName := outcome.Recovery.Status.SecretName
	if secretName == "" {
		return nil, fmt.Errorf("completed recovery has no target admin Secret")
	}
	secret, err := get(ctx, "secret", secretName)
	if err != nil {
		return nil, err
	}
	adminBytes, err := base64.StdEncoding.DecodeString(text(nested(secret, "data", "admin")))
	if err != nil || len(adminBytes) == 0 {
		return nil, fmt.Errorf("recovered target admin credential is unavailable")
	}
	outcome.Admin = string(adminBytes)
	if len(outcome.Recovery.Status.TargetIdentities) == 0 || outcome.Recovery.Status.TargetIdentities[0].Pod == "" {
		return nil, fmt.Errorf("completed recovery has no concrete target voter identity")
	}
	target := outcome.Recovery.Status.TargetIdentities[0]
	if target.NodeID == "" || target.PodUID == "" || target.WALIdentity == "" {
		return nil, fmt.Errorf("completed recovery target voter identity is incomplete")
	}
	targetPod, err := get(ctx, "pod", target.Pod)
	if err != nil || text(nested(targetPod, "metadata", "uid")) != target.PodUID || !podReady(targetPod) {
		return nil, fmt.Errorf("recovered target voter Pod is not the exact Ready incarnation: %v", err)
	}
	outcome.Forward, err = startForward(ctx, target.Pod, out)
	if err != nil {
		return nil, err
	}
	status, err := readMembershipWithToken(ctx, outcome.Forward, outcome.Admin)
	if err != nil || !gcsRecoveryMembershipMatchesTargetIdentities(status, outcome.Recovery) {
		return nil, joinForwardCloseError(outcome.Forward,
			fmt.Errorf("recovered voter did not pass exact authenticated membership and quorum read: %v", err))
	}
	rows, err := call(ctx, outcome.Forward, "/sql/query", obj{"sql": "SELECT id,value FROM fencing_items ORDER BY id", "consistency": "linearizable"}, outcome.Admin)
	if err != nil || !gcsRecoveryRowsMatch(rows["rows"], expectedRows) {
		return nil, joinForwardCloseError(outcome.Forward,
			fmt.Errorf("recovered linearizable SQL readback differs: %v", err))
	}
	return outcome, nil
}

func validateGCSRecoveryAfterProofInputs(source gcsRecoverySource, pending pendingGCSFence, request operator.FenceRequest,
	proof operator.FenceProof, expectedBase, expectedRows string) error {
	if source.ClusterID == "" || source.StatefulSetUID == "" || source.BindingUID == "" || source.BindingName == "" ||
		source.Bucket == "" || source.Prefix == "" || expectedBase != "fork" && expectedBase != "empty" || expectedRows == "" {
		return fmt.Errorf("GCS recovery completion contract is incomplete")
	}
	if expectedBase == "fork" && source.Durability != "before-ack" {
		return fmt.Errorf("fork recovery requires before-ACK source durability")
	}
	if pending.Name == "" || pending.UID == "" ||
		pending.Request.OperationID == "" || !sameFenceRequest(pending.Request, request) ||
		request.Scope != operator.ScopeGeneration || request.LogicalID != source.LogicalID || request.Namespace != source.Namespace ||
		request.StatefulSet != source.StatefulSet || request.SourceClusterID != source.ClusterID ||
		request.StatefulSetUID != source.StatefulSetUID || request.BindingUID != source.BindingUID ||
		proof.OperationID != request.OperationID || proof.SourceClusterID != request.SourceClusterID ||
		proof.BindingUID != request.BindingUID || proof.RequestHash != digestJSON(request) || proof.ProofID == "" ||
		proof.IndependentV2 == nil || proof.IndependentV2.RequestHash != digestJSON(request) {
		return fmt.Errorf("recovery completion inputs are not bound to the validated generation proof")
	}
	return nil
}

func gcsRecoveryRowsMatch(actual any, expected string) bool {
	return expected != "" && string(encoded(actual)) == expected
}

func validateGCSRecoveryTargetAdoption(root any, recovery operator.Resource) error {
	target := recovery.Status.Target
	if target == "" || nested(root, "status", "phase") != "Healthy" || nested(root, "status", "activeClusterID") != target {
		return fmt.Errorf("controller root has not adopted the completed recovered target")
	}
	return nil
}

func waitForGCSRecoveryTargetAdoption(ctx context.Context, source gcsRecoverySource, recovery operator.Resource) error {
	if source.BindingName == "" || source.BindingUID == "" || source.Namespace == "" || source.ClusterID == "" || source.StatefulSet == "" || source.StatefulSetUID == "" ||
		text(recovery.Metadata["name"]) == "" || text(recovery.Metadata["namespace"]) != source.Namespace || text(recovery.Metadata["uid"]) == "" || recovery.Spec.RecoveryID == "" ||
		recovery.Spec.StatefulSet != source.StatefulSet || recovery.Spec.SourceClusterID != source.ClusterID || recovery.Status.Source != source.ClusterID ||
		recovery.Status.StatefulSetUID != source.StatefulSetUID || recovery.Status.Target == "" || recovery.Status.Target == source.ClusterID {
		return fmt.Errorf("completed recovery lineage is incomplete for root adoption")
	}
	targetObserved := false
	return wait(ctx, "captured RhizaCluster adoption of completed recovery", func() (bool, error) {
		root, err := get(ctx, "rhizacluster", source.BindingName)
		if err != nil {
			return false, err
		}
		var done bool
		targetObserved, done, err = observeGCSRecoveryRoot(source, recovery, targetObserved, root)
		if err != nil || !done {
			return done, err
		}
		if err := validateGCSRecoveryTargetAdoption(root, recovery); err != nil {
			return false, err
		}
		return true, nil
	})
}

func observeGCSRecoveryRoot(source gcsRecoverySource, recovery operator.Resource, targetObserved bool, root any) (bool, bool, error) {
	if text(nested(root, "metadata", "name")) != source.BindingName ||
		text(nested(root, "metadata", "namespace")) != source.Namespace ||
		text(nested(root, "metadata", "uid")) != source.BindingUID {
		return targetObserved, false, fmt.Errorf("captured RhizaCluster name, namespace, or UID changed during recovery adoption")
	}
	phaseName := text(nested(root, "status", "phase"))
	activeCluster := text(nested(root, "status", "activeClusterID"))
	activeRecovery := text(nested(root, "status", "activeRecovery"))
	operationID := recovery.Spec.RecoveryID
	if phaseName == "Recovering" && activeCluster == source.ClusterID && activeRecovery == operationID {
		if targetObserved {
			return targetObserved, false, fmt.Errorf("controller root regressed to the source after observing the recovery target")
		}
		return false, false, nil
	}
	if activeCluster == recovery.Status.Target && activeRecovery == "" {
		switch phaseName {
		case "Stabilizing":
			return true, false, nil
		case "Healthy":
			return true, true, nil
		}
	}
	return targetObserved, false, fmt.Errorf("controller root changed to an unexpected recovery target, phase, or operation")
}

func gcsRecoveryMembershipMatchesTargetIdentities(status network.MembershipStatus, recovery operator.Resource) bool {
	identities := recovery.Status.TargetIdentities
	if status.ClusterID != recovery.Status.Target || status.Pending || !status.Voting || len(identities) == 0 || len(status.Voters) != len(identities) {
		return false
	}
	want := make(map[string]bool, len(identities))
	seenPods := make(map[string]bool, len(identities))
	seenUIDs := make(map[string]bool, len(identities))
	var first operator.FenceTarget
	for i, identity := range identities {
		if identity.NodeID == "" || identity.Pod == "" || identity.PodUID == "" || identity.WALIdentity == "" ||
			want[identity.NodeID] || seenPods[identity.Pod] || seenUIDs[identity.PodUID] {
			return false
		}
		want[identity.NodeID] = true
		seenPods[identity.Pod], seenUIDs[identity.PodUID] = true, true
		if i == 0 {
			first = identity
		}
	}
	if string(status.NodeID) != first.NodeID || status.WALIdentity != first.WALIdentity {
		return false
	}
	for _, voter := range status.Voters {
		id := string(voter)
		if !want[id] {
			return false
		}
		delete(want, id)
	}
	return len(want) == 0
}

// verifyGCSFenceProofReadback binds the API list result to a second, exact-name
// API read. Comparing the complete stored proof preserves immutable terminal
// identities, references, and hashes; the existing client verifier validates
// the proof returned by that read rather than a synthetic fixture.
func verifyGCSFenceProofReadback(observed, readback any, pending pendingGCSFence, source gcsRecoverySource) error {
	if !pendingFenceIdentityMatches(observed, pending) || !pendingFenceIdentityMatches(readback, pending) {
		return fmt.Errorf("generation proof API readback changed the pending fence resource identity")
	}
	if _, err := exactPendingGCSFenceRequest(observed, pending); err != nil {
		return fmt.Errorf("generation proof list result changed the pending fence request: %w", err)
	}
	if _, err := exactPendingGCSFenceRequest(readback, pending); err != nil {
		return fmt.Errorf("generation proof API readback changed the pending fence request: %w", err)
	}
	if string(encoded(nested(observed, "spec"))) != string(encoded(nested(readback, "spec"))) ||
		string(encoded(nested(observed, "status", "proof"))) != string(encoded(nested(readback, "status", "proof"))) {
		return fmt.Errorf("generation proof API readback changed immutable request or proof fields")
	}
	_, _, err := verifyIndependentGenerationProof(readback, source.ClusterID, source.StatefulSetUID, source.BindingUID,
		source.Targets, source.Bucket, source.Prefix, source.Namespace, source.StatefulSet, source.LogicalID)
	if err != nil {
		return fmt.Errorf("generation proof API readback failed the existing client verifier: %w", err)
	}
	return nil
}

// recoveryFenceMatchesProof ties the persisted recovery fence to the exact
// captured generation-fence request and validated proof, not merely to the
// source cluster and workload UID.
func recoveryFenceMatchesProof(recovery operator.Resource, request operator.FenceRequest, proof operator.FenceProof) error {
	fence := recovery.Spec.Fence
	if request.OperationID == "" || request.SourceClusterID == "" || request.StatefulSetUID == "" || proof.ProofID == "" ||
		proof.OperationID != request.OperationID || proof.SourceClusterID != request.SourceClusterID || proof.BindingUID != request.BindingUID ||
		recovery.Spec.RecoveryID != request.OperationID || recovery.Spec.SourceClusterID != request.SourceClusterID ||
		!fence.Confirmed || fence.RecoveryID != request.OperationID || fence.ClusterID != request.SourceClusterID ||
		fence.StatefulSetUID != request.StatefulSetUID || fence.Evidence != proof.ProofID {
		return fmt.Errorf("recovery fence does not bind the exact captured generation proof")
	}
	return nil
}

func readMembershipWithToken(ctx context.Context, f *forward, token string) (network.MembershipStatus, error) {
	data, err := call(ctx, f, "/membership/status", nil, token)
	var status network.MembershipStatus
	if err == nil {
		err = json.Unmarshal(encoded(data), &status)
	}
	return status, err
}

func verifyRecoveryLineage(previous, next operator.Resource, previousFence, nextFence operator.FenceRequest) error {
	if previous.Status.Phase != "Complete" || previous.Status.Stage != "Complete" || next.Status.Phase != "Complete" || next.Status.Stage != "Complete" ||
		previous.Status.StatefulSetUID == "" || previous.Status.StatefulSetUID != next.Status.StatefulSetUID ||
		previous.Status.Target == "" || next.Spec.SourceClusterID != previous.Status.Target || next.Status.Source != previous.Status.Target ||
		previousFence.OperationID == "" || nextFence.OperationID == "" || previousFence.OperationID == nextFence.OperationID ||
		previous.Status.Target == next.Status.Target || nextFence.SourceClusterID != previous.Status.Target {
		return fmt.Errorf("second recovery does not continue the completed target lineage")
	}
	if len(previous.Status.TargetIdentities) == 0 || len(nextFence.Targets) == 0 {
		return fmt.Errorf("second recovery lacks observed target incarnation identities")
	}
	previousUIDs := map[string]bool{}
	for _, identity := range previous.Status.TargetIdentities {
		if identity.PodUID == "" || identity.WALIdentity == "" {
			return fmt.Errorf("completed target identity is incomplete")
		}
		previousUIDs[identity.PodUID] = true
	}
	for _, target := range nextFence.Targets {
		if target.PodUID == "" || target.WALIdentity == "" || previousUIDs[target.PodUID] {
			return fmt.Errorf("second recovery reused a prior target Pod identity or omitted WAL identity")
		}
	}
	return nil
}

func verifyAsyncEmptyRecovery(resource operator.Resource, request operator.FenceRequest, proof operator.FenceProof) error {
	if resource.Status.Phase != "Complete" || resource.Status.Stage != "Complete" || resource.Status.TargetBaseKind != "empty" ||
		resource.Status.SourceDurability != "async" || resource.Spec.Durability != "async" || !resource.Spec.AllowDataLoss ||
		resource.Status.RecoveredTip != 0 || resource.Status.ManifestHash != "" || proof.IndependentV2 == nil || proof.IndependentV2.StorageScope.Provider != "gcs" {
		return fmt.Errorf("async zero-tip recovery lacks explicit loss authorization or committed empty GCS lineage")
	}
	return recoveryFenceMatchesProof(resource, request, proof)
}

// verifyIndependentGenerationProof validates the issuer-published V2 metadata
// and its captured scope/target references; it does not fetch remote payloads.
func verifyIndependentGenerationProof(item any, sourceID, stsUID, bindingUID string, targets []operator.FenceTarget, bucket, prefix string, requestScope ...string) (operator.FenceRequest, operator.FenceProof, error) {
	return verifyIndependentGenerationProofWithV2Targets(item, sourceID, stsUID, bindingUID, targets, targets, bucket, prefix, requestScope...)
}

// verifyIndependentGenerationProofWithV2Targets keeps the complete generation
// request separate from the active-voter set that must carry V2 terminals.
func verifyIndependentGenerationProofWithV2Targets(item any, sourceID, stsUID, bindingUID string, requestTargets, v2Targets []operator.FenceTarget, bucket, prefix string, requestScope ...string) (operator.FenceRequest, operator.FenceProof, error) {
	var request operator.FenceRequest
	var proof operator.FenceProof
	if err := json.Unmarshal(encoded(nested(item, "spec")), &request); err != nil {
		return request, proof, err
	}
	if err := json.Unmarshal(encoded(nested(item, "status", "proof")), &proof); err != nil {
		return request, proof, err
	}
	statefulSet, requestNamespace, logicalID := "", "", ""
	if len(requestScope) == 3 {
		requestNamespace, statefulSet, logicalID = requestScope[0], requestScope[1], requestScope[2]
	}
	if request.SourceClusterID != sourceID || request.StatefulSet != statefulSet || request.StatefulSetUID != stsUID || request.BindingUID != bindingUID || request.Namespace != requestNamespace || request.LogicalID != logicalID || request.Scope != operator.ScopeGeneration || request.OperationID == "" || !sameTargets(request.Targets, requestTargets) {
		return request, proof, fmt.Errorf("independent generation fence request differs from captured source cohort")
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(encoded(request)))
	evidence := proof.IndependentV2
	if evidence == nil || evidence.Version != 2 || evidence.RequestHash != hash || evidence.LedgerHash == "" || evidence.StorageScope.Provider != "gcs" || evidence.StorageScope.Bucket != bucket || evidence.StorageScope.Prefix != prefix || proof.ProofID != "independent-v2-"+fmt.Sprintf("%x", sha256.Sum256(encoded(evidence))) ||
		proof.Independent != nil || proof.OperationID != request.OperationID || proof.SourceClusterID != sourceID || proof.BindingUID != bindingUID || proof.RequestHash != hash || !proof.ProcessesTerminated || !proof.RecreationBlocked || !proof.StorageQuiesced || len(evidence.Terminals) == 0 || len(evidence.Terminals) > 64 || len(evidence.CurrentPodUIDs) > 64 {
		return request, proof, fmt.Errorf("fence does not contain a complete GCS IndependentV2 proof")
	}
	if _, err := immutableDigest("sha256:" + evidence.LedgerHash); err != nil {
		return request, proof, fmt.Errorf("IndependentV2 ledger reference is invalid: %w", err)
	}
	scopeDigest := fmt.Sprintf("%x", sha256.Sum256(encoded(evidence.StorageScope)))
	currentUIDs := map[string]bool{}
	for _, uid := range evidence.CurrentPodUIDs {
		if uid == "" || currentUIDs[uid] {
			return request, proof, fmt.Errorf("IndependentV2 current Pod inventory is invalid")
		}
		currentUIDs[uid] = true
	}
	for _, ref := range evidence.Terminals {
		if err := validateGCSGenerationTerminalReference(ref, sourceID, scopeDigest); err != nil {
			return request, proof, fmt.Errorf("IndependentV2 terminal reference is incomplete")
		}
	}
	for _, target := range v2Targets {
		covered := false
		for _, ref := range evidence.Terminals {
			id := ref.Request.Identity
			if id.SourceClusterID == sourceID && id.NodeID == target.NodeID && id.PodUID == target.PodUID && id.WALIdentity == target.WALIdentity && ref.EvidenceVersion == 2 && ref.StorageScopeDigest == scopeDigest {
				covered = true
				break
			}
		}
		if !covered {
			return request, proof, fmt.Errorf("IndependentV2 terminals omit captured source incarnation %s", target.NodeID)
		}
	}
	return request, proof, nil
}

// verifyIndependentGenerationProofAgainstRoot is the canonical-root-backed
// verifier used by the live GCS qualification. The root is read-only evidence;
// it is never repaired or mutated by the qualification.
func verifyIndependentGenerationProofAgainstRoot(item any, sourceID, stsUID, bindingUID string, targets []operator.FenceTarget, bucket, prefix string, root fenceLedgerState, requestScope ...string) (operator.FenceRequest, operator.FenceProof, error) {
	return verifyIndependentGenerationProofAgainstRootWithV2Targets(item, sourceID, stsUID, bindingUID, targets, targets, bucket, prefix, root, requestScope...)
}

func verifyIndependentGenerationProofAgainstRootWithV2Targets(item any, sourceID, stsUID, bindingUID string, requestTargets, v2Targets []operator.FenceTarget, bucket, prefix string, root fenceLedgerState, requestScope ...string) (operator.FenceRequest, operator.FenceProof, error) {
	request, proof, err := verifyIndependentGenerationProofWithV2Targets(item, sourceID, stsUID, bindingUID, requestTargets, v2Targets, bucket, prefix, requestScope...)
	if err != nil {
		return request, proof, err
	}
	evidence := proof.IndependentV2
	requestHash := fmt.Sprintf("%x", sha256.Sum256(encoded(request)))
	if root.StatefulSetUID != request.StatefulSetUID || root.Ledger.SourceClusterID != request.SourceClusterID ||
		root.Ledger.ClosedBy != request.OperationID || root.Ledger.ClosedHash != requestHash ||
		!root.Ledger.RequireRuntimeBinding || !root.Ledger.RequireMutationJournal {
		return request, proof, fmt.Errorf("canonical authority root does not identify the closed generation fence")
	}
	scopeDigest := fmt.Sprintf("%x", sha256.Sum256(encoded(evidence.StorageScope)))
	if len(root.Ledger.Runs) == 0 || len(root.Ledger.Runs) > 64 {
		return request, proof, fmt.Errorf("canonical authority root has an unsupported terminal run history")
	}
	canonical := make(map[string]fencing.TerminalReference, len(root.Ledger.Runs))
	for runID, run := range root.Ledger.Runs {
		ref := run.IndependentTerminal
		if ref == nil || run.Runtime == nil || run.Runtime.Validate(run.Identity) != nil ||
			run.Runtime.StatefulSetUID != request.StatefulSetUID || run.Runtime.BindingUID != request.BindingUID ||
			run.Runtime.SourceClusterID != request.SourceClusterID {
			return request, proof, fmt.Errorf("canonical authority root has a run without complete terminal enrollment")
		}
		id := ref.Request.Identity
		role := fencing.RunRole(run)
		_, duplicate := canonical[runID]
		if runID == "" || id.RunID != runID || run.Identity != id || run.Stop == nil || *run.Stop != ref.Request ||
			id.SourceClusterID != request.SourceClusterID || duplicate {
			return request, proof, fmt.Errorf("canonical authority root has an inconsistent terminal reference")
		}
		validatedRef, err := root.Ledger.IndependentTerminal(runID, scopeDigest)
		if err != nil || validatedRef != *ref {
			return request, proof, fmt.Errorf("canonical authority root has an invalid independent terminal reference")
		}
		if ref.EvidenceVersion == 2 && role != fencing.StartupRoleVoterActive {
			return request, proof, fmt.Errorf("canonical V2 terminal is not an active voter")
		}
		if ref.EvidenceVersion == 3 && (role != fencing.StartupRoleLearnerPendingRemove || run.Promotion != nil) {
			return request, proof, fmt.Errorf("canonical V3 terminal is not a closed pending learner")
		}
		canonical[runID] = *ref
	}
	if len(canonical) != len(evidence.Terminals) {
		return request, proof, fmt.Errorf("IndependentV2 terminal set differs from canonical run history")
	}
	seen := make(map[string]bool, len(evidence.Terminals))
	for _, ref := range evidence.Terminals {
		runID := ref.Request.Identity.RunID
		canonicalRef, exists := canonical[runID]
		if runID == "" || seen[runID] || !exists || canonicalRef != ref {
			return request, proof, fmt.Errorf("IndependentV2 terminal reference differs from canonical run history")
		}
		seen[runID] = true
	}
	return request, proof, nil
}

func verifyGCSGenerationProofAgainstCanonicalRoot(ctx context.Context, item any, source gcsRecoverySource) error {
	deployment, err := get(ctx, "deployment", "rhiza-fencer")
	if err != nil {
		return fmt.Errorf("read fencer configuration for canonical authority observer: %w", err)
	}
	readScope, err := newGCSAckLossReadScope(ctx, source, deployment)
	if err != nil {
		return fmt.Errorf("configure read-only canonical authority observer: %w", err)
	}
	stateStore, _, client, _, closeStores, err := openGCSAckLossAuthorityStores(ctx, source, readScope)
	if err != nil {
		return fmt.Errorf("open read-only canonical authority observer: %w", err)
	}
	if client != nil {
		defer client.Close()
	}
	if closeStores != nil {
		defer closeStores()
	}
	root, _, err := stateStore.ReadRoot(ctx, source.StatefulSetUID, source.ClusterID)
	if err != nil {
		return fmt.Errorf("read canonical authority root for generation proof: %w", err)
	}
	_, _, err = verifyIndependentGenerationProofAgainstRoot(item, source.ClusterID, source.StatefulSetUID, source.BindingUID,
		source.Targets, source.Bucket, source.Prefix,
		fenceLedgerState{StatefulSetUID: root.StatefulSetUID, Ledger: root.Ledger}, source.Namespace, source.StatefulSet, source.LogicalID)
	return err
}

// validateGCSGenerationTerminalReference accepts only the two supported
// per-run reference contracts. V2 remains producer-backed; V3 is the
// closed-pending learner shape and must not claim producer mutation evidence.
func validateGCSGenerationTerminalReference(ref fencing.TerminalReference, sourceID, scopeDigest string) error {
	id := ref.Request.Identity
	if (ref.EvidenceVersion != 2 && ref.EvidenceVersion != 3) || ref.Request.OperationID == "" || len(ref.Request.OperationID) > 256 ||
		id.SourceClusterID != sourceID || len(id.SourceClusterID) > 256 || id.NodeID == "" || len(id.NodeID) > 256 || id.PodUID == "" || len(id.PodUID) > 256 ||
		id.RunID == "" || len(id.RunID) > 128 || len(id.PodName) > 256 ||
		id.AssignmentID != "" && len(id.AssignmentID) != 64 ||
		ref.ProofName == "" || len(ref.ProofName) > 63 || ref.ProofUID == "" || len(ref.ProofUID) > 128 || ref.StorageScopeDigest != scopeDigest {
		return fmt.Errorf("reference identity or storage scope is incomplete")
	}
	if id.AssignmentID != "" {
		if _, err := immutableDigest("sha256:" + id.AssignmentID); err != nil {
			return fmt.Errorf("assignment identity digest is invalid")
		}
	}
	if ref.EvidenceVersion == 2 {
		if id.WALIdentity == "" || len(id.WALIdentity) > 128 || ref.ProducerUID == "" || len(ref.ProducerUID) > 128 || len(ref.MutationReceiptDigest) != 64 {
			return fmt.Errorf("V2 producer mutation evidence is incomplete")
		}
	} else if ref.ProducerUID != "" || ref.MutationReceiptDigest != "" || len(id.WALIdentity) > 128 {
		return fmt.Errorf("V3 closed-pending reference claims producer evidence")
	}
	for _, digest := range []string{ref.Request.RequestHash, ref.RuntimeHash, ref.ProofDigest, ref.RuntimeReceiptDigest, ref.StorageScopeDigest} {
		if _, err := immutableDigest("sha256:" + digest); err != nil {
			return fmt.Errorf("reference digest is invalid")
		}
	}
	if ref.EvidenceVersion == 2 {
		if _, err := immutableDigest("sha256:" + ref.MutationReceiptDigest); err != nil {
			return fmt.Errorf("V2 mutation digest is invalid")
		}
	}
	return nil
}
