package operator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
)

type drAdmissionTestBackend struct {
	record  recoveryanchor.Record
	version string
}

func (b *drAdmissionTestBackend) Read(context.Context, string) (recoveryanchor.Record, string, error) {
	return b.record, b.version, nil
}

func (b *drAdmissionTestBackend) CAS(_ context.Context, _ string, version string, record recoveryanchor.Record) error {
	if version != b.version {
		return fencing.ErrUnavailable
	}
	b.record, b.version = record, b.version+"x"
	return nil
}

func prepareProfilePod() object {
	field := func(name, path string) object {
		return object{"name": name, "valueFrom": object{"fieldRef": object{"fieldPath": path}}}
	}
	value := func(name, value string) object { return object{"name": name, "value": value} }
	secret := object{"name": "RHIZA_CLUSTER_MEMBERS", "valueFrom": object{"secretKeyRef": object{"name": "cluster-credentials", "key": "members"}}}
	main := object{
		"name": "rhiza", "image": "rhiza@sha256:abc",
		"command": []any{"/usr/local/bin/rhiza-fencer"},
		"args":    []any{"supervise", "--", "/usr/local/bin/rhiza"},
		"env": []any{
			value("RHIZA_CLUSTER_ID", "cluster"), field("RHIZA_NODE_ID", "metadata.name"),
			secret, value("RHIZA_DATA_DIR", "/var/lib/rhiza"), value("RHIZA_ENABLE_RECONFIGURATION", "true"),
			value("RHIZA_OBJSTORE_PROVIDER", "s3"), value("FENCER_AUTHORITY_URL", "https://rhiza-fencer:9444/v1/supervisor"), value("FENCER_CA_FILE", "/fencer-ca/ca.crt"),
		},
		"volumeMounts": []any{
			object{"name": "data", "mountPath": "/var/lib/rhiza"},
			object{"name": "fence-token", "mountPath": "/var/run/secrets/rhiza-fencer", "readOnly": true},
			object{"name": "fence-ca", "mountPath": "/fencer-ca", "readOnly": true},
		},
		"securityContext": object{"allowPrivilegeEscalation": false, "capabilities": object{"drop": []any{"ALL"}}},
	}
	init := object{
		"name": prepareInitContainerName, "image": main["image"],
		"command": []any{"/usr/local/bin/rhiza"}, "args": []any{"--prepare-restore"},
		"env": []any{
			field("POD_UID", "metadata.uid"), field("RHIZA_PREPARED_POD_UID", "metadata.uid"),
			field("POD_NAME", "metadata.name"), field("RHIZA_PREPARE_OPERATION_ID", "metadata.annotations['"+fencingPrepareOperationAnnotation+"']"),
			value("RHIZA_CLUSTER_ID", "cluster"), field("RHIZA_NODE_ID", "metadata.name"), secret,
			value("RHIZA_DATA_DIR", "/var/lib/rhiza"), value("RHIZA_ENABLE_RECONFIGURATION", "true"), value("RHIZA_OBJSTORE_PROVIDER", "s3"),
			value("FENCER_AUTHORITY_URL", "https://rhiza-fencer:9444/v1/supervisor"), value("FENCER_CA_FILE", "/fencer-ca/ca.crt"),
		},
		"volumeMounts": list(main["volumeMounts"]),
	}
	return object{
		"metadata": object{"name": "rhiza-0", "uid": "pod-uid", "annotations": object{
			fencingProtocolAnnotation: "1", fencingRootAnnotation: "root-uid", fencingPrepareOperationAnnotation: "operation-1",
		}},
		"spec": object{
			"containers": []any{main}, "initContainers": []any{init},
			"volumes": []any{
				object{"name": "data", "emptyDir": object{}},
				object{"name": "fence-token", "projected": object{"sources": []any{object{"serviceAccountToken": object{"path": "token", "audience": "rhiza-fencer", "expirationSeconds": float64(600)}}}}},
				object{"name": "fence-ca", "configMap": object{"name": "rhiza-fencer-ca"}},
			},
		},
	}
}

func TestValidatePreparePodRequiresFixedSecretlessInitProfile(t *testing.T) {
	if err := validatePreparePod(prepareProfilePod(), "pod-uid"); err != nil {
		t.Fatalf("valid fixed init profile rejected: %v", err)
	}
	for _, scenario := range []string{"private-env", "private-prefixed-env", "unknown-env", "restartable-init", "wrong-image", "wrong-operation-ref", "wrong-token-audience", "missing-data-mount"} {
		t.Run(scenario, func(t *testing.T) {
			pod := prepareProfilePod()
			spec := asObject(pod["spec"])
			init := asObject(list(spec["initContainers"])[0])
			switch scenario {
			case "private-env":
				init["env"] = append(list(init["env"]), object{"name": "RHIZA_ADMIN_TOKEN", "value": "secret"})
			case "private-prefixed-env":
				init["env"] = append(list(init["env"]), object{"name": "RHIZA_OBJSTORE_PEER_TOKEN", "value": "secret"})
			case "unknown-env":
				init["env"] = append(list(init["env"]), object{"name": "UNRELATED_SECRET", "value": "secret"})
			case "restartable-init":
				init["restartPolicy"] = "Always"
			case "wrong-image":
				init["image"] = "other@sha256:abc"
			case "wrong-operation-ref":
				asObject(asObject(list(init["env"])[3])["valueFrom"])["fieldRef"] = object{"fieldPath": "metadata.name"}
			case "wrong-token-audience":
				asObject(asObject(list(nested(asObject(list(spec["volumes"])[1]), "projected", "sources"))[0])["serviceAccountToken"])["audience"] = "kubernetes.default"
			case "missing-data-mount":
				mounts := list(init["volumeMounts"])
				init["volumeMounts"] = mounts[1:]
			}
			if err := validatePreparePod(pod, "pod-uid"); err == nil {
				t.Fatal("invalid prepare init profile accepted")
			}
		})
	}
}

func TestDRSuccessorAssignmentUsesFreshTLSAnchorAdmissionAndConfiguredStorage(t *testing.T) {
	ctx := context.Background()
	ms := members()
	targetMembership := recovery.NewMembershipRecord("target", ms, "before-ack")
	bindingConfig := recoveryanchor.StorageBindingConfig{Policy: recoveryanchor.StorageBindingDerived, Provider: "s3", Endpoint: "https://objects.example", Bucket: "archive", Prefix: "root"}
	sourceBinding, err := bindingConfig.ExpectedBinding("source")
	if err != nil {
		t.Fatal(err)
	}
	targetBinding, err := bindingConfig.ExpectedBinding("target")
	if err != nil {
		t.Fatal(err)
	}
	backend := &drAdmissionTestBackend{record: recoveryanchor.Record{Version: recoveryanchor.Version1, EvidenceFormat: "test/v1", Binding: sourceBinding, Evidence: []byte("source-evidence"), Generation: 4}, version: "1"}
	coordinator := &recoveryanchor.Coordinator{Backend: backend, EvidenceFormat: "test/v1", Verifier: func(_ context.Context, _ recoveryanchor.Request, record recoveryanchor.Record) (recoveryanchor.Binding, []byte, error) {
		return targetBinding, append([]byte(nil), record.Evidence...), nil
	}}
	request := recoveryanchor.Request{AnchorID: "ha", OperationID: "recover-1", StatefulSetUID: "sts-uid", SourceClusterID: "source", TargetClusterID: "target", SourcePrefix: "root/source", TargetPrefix: "root/target", SourceMembership: "source-membership", FenceHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", TargetAnchorHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Fork: recovery.ForkResult{Tip: 3, ManifestHash: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", PrefixHash: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"}, TargetMembership: targetMembership}
	receipt, err := coordinator.Activate(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(recoveryanchor.NewHandler(coordinator, "anchor-token"))
	defer server.Close()
	tokenPath := path.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("anchor-token"), 0600); err != nil {
		t.Fatal(err)
	}
	anchorClient := &recoveryanchor.Client{URL: server.URL, TokenFile: tokenPath, Client: server.Client()}

	operationID := "dr-successor-operation"
	rootUID, stsUID, ledgerUID := "root-uid", "sts-uid", "ledger-uid"
	credentialName, credentialUID := "target-credentials", "secret-uid"
	membershipHash := hashJSON(ms)
	assignment := fencing.BootstrapAssignment{OperationID: operationID, RequestHash: "bootstrap-request-hash", Purpose: fencing.BootstrapDRSuccessor, RootUID: rootUID, StatefulSetUID: stsUID, SourceClusterID: "target", PodName: "n1", PodUID: "pod-uid-1", NodeID: "n1", CredentialName: credentialName, CredentialUID: credentialUID, MembershipHash: membershipHash, ProfileHash: "profile-hash", StoreIdentityHash: fencing.BootstrapStoreIdentityHash(storeIdentity()), LedgerUID: ledgerUID, Durability: "before-ack", DRApproval: fencing.BootstrapDRApproval{Request: request, Receipt: receipt}}
	assignment.AuthorizationHash = fencing.BootstrapDRAuthorizationHash(assignment)
	operation := fencing.BootstrapOperation{OperationID: operationID, RequestHash: assignment.RequestHash, AuthorizationHash: assignment.AuthorizationHash, Purpose: fencing.BootstrapDRSuccessor, RootUID: rootUID, StatefulSetUID: stsUID, SourceClusterID: "target", Assignments: map[string]fencing.BootstrapAssignment{assignment.PodUID: assignment}, Attempts: map[string]fencing.InitAttempt{}}
	ledger := fencing.Ledger{SourceClusterID: "target", Bootstrap: &fencing.BootstrapHistory{RootUID: rootUID, StatefulSetUID: stsUID, Operations: map[string]fencing.BootstrapOperation{operationID: operation}}}
	ledgerBytes, err := json.Marshal(fenceLedger{StatefulSetUID: stsUID, Ledger: ledger})
	if err != nil {
		t.Fatal(err)
	}
	ledgerName := path.Base((&FencingExecutor{Kube: &Kubernetes{Namespace: "default"}}).ledgerPath(stsUID, "target"))
	ledgerObject := object{"metadata": object{"name": ledgerName, "namespace": "default", "uid": ledgerUID, "resourceVersion": "1"}, "data": object{"ledger.json": string(ledgerBytes)}}
	pod := prepareProfilePod()
	podMeta := asObject(pod["metadata"])
	podMeta["name"], podMeta["uid"] = "n1", "pod-uid-1"
	podMeta["annotations"] = object{fencingProtocolAnnotation: "1", fencingPrepareOperationAnnotation: operationID, fencingRootAnnotation: rootUID}
	podMeta["ownerReferences"] = []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": stsUID, "controller": true}}
	podSpec := asObject(pod["spec"])
	podSpec["nodeName"], podSpec["serviceAccountName"] = "worker-1", "default"
	for _, container := range append(list(podSpec["containers"]), list(podSpec["initContainers"])...) {
		for _, raw := range list(asObject(container)["env"]) {
			entry := asObject(raw)
			if entry["name"] == "RHIZA_CLUSTER_ID" {
				entry["value"] = "target"
			}
			if ref := asObject(nested(entry, "valueFrom", "secretKeyRef")); ref != nil {
				ref["name"] = credentialName
			}
		}
	}
	pod["status"] = object{"initContainerStatuses": []any{object{"name": prepareInitContainerName, "containerID": "containerd://init-1", "imageID": "rhiza@sha256:init", "state": object{"running": object{}}}}}
	secretBytes, err := json.Marshal(ms)
	if err != nil {
		t.Fatal(err)
	}
	kubeAPI := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/authentication.k8s.io/v1/tokenreviews":
			_ = json.NewEncoder(w).Encode(object{"status": object{"authenticated": true, "audiences": []any{"rhiza-fencer"}, "user": object{
				"username": "system:serviceaccount:default:default",
				"extra":    object{"authentication.kubernetes.io/pod-uid": []any{"pod-uid-1"}, "authentication.kubernetes.io/pod-name": []any{"n1"}},
			}}})
		case "/api/v1/namespaces/default/configmaps/" + ledgerName:
			if r.Method == http.MethodPut {
				var updated object
				if json.NewDecoder(r.Body).Decode(&updated) != nil {
					http.Error(w, "bad request", http.StatusBadRequest)
					return
				}
				asObject(updated["metadata"])["resourceVersion"] = "2"
				ledgerObject = updated
			}
			_ = json.NewEncoder(w).Encode(ledgerObject)
		case "/api/v1/namespaces/default/secrets/" + credentialName:
			_ = json.NewEncoder(w).Encode(object{"metadata": object{"name": credentialName, "uid": credentialUID, "annotations": object{ownerAnnotation: request.OperationID}}, "immutable": true, "data": object{"members": base64.StdEncoding.EncodeToString(secretBytes)}})
		case "/api/v1/namespaces/default/pods/n1":
			_ = json.NewEncoder(w).Encode(pod)
		case "/api/v1/namespaces/default/nodes/worker-1":
			_ = json.NewEncoder(w).Encode(object{"metadata": object{"name": "worker-1", "uid": "node-uid-1"}})
		case "/apis/apps/v1/namespaces/default/statefulsets/rhiza":
			_ = json.NewEncoder(w).Encode(object{"metadata": object{"name": "rhiza", "uid": stsUID}, "spec": object{"template": object{"spec": object{"serviceAccountName": "default"}}}})
		case "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/default/rhizaclusters":
			_ = json.NewEncoder(w).Encode(object{"items": []any{object{"metadata": object{"uid": rootUID}, "spec": object{"statefulSet": "rhiza", "automatic": true}}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeAPI.Close()
	kubeToken := path.Join(t.TempDir(), "kube-token")
	if err := os.WriteFile(kubeToken, []byte("kube-token"), 0600); err != nil {
		t.Fatal(err)
	}
	e := &FencingExecutor{Kube: &Kubernetes{BaseURL: kubeAPI.URL, Namespace: "default", TokenFile: kubeToken, Client: kubeAPI.Client()}, Anchor: anchorClient, AnchorBindingConfig: &bindingConfig}
	authority := httptest.NewTLSServer(e.Handler())
	defer authority.Close()
	client := &fencing.AuthorityClient{URL: authority.URL + "/v1/supervisor", TokenFile: kubeToken, Client: authority.Client()}
	id := fencing.Identity{SourceClusterID: "target", PodName: "n1", PodUID: "pod-uid-1", NodeID: "n1"}
	if err := assignment.Validate(); err != nil {
		t.Fatalf("test DR assignment invalid: %v", err)
	}
	if err := anchorClient.VerifyAdmission(ctx, request, receipt, targetBinding, receipt.Generation); err != nil {
		t.Fatalf("TLS anchor admission failed independently: %v", err)
	}
	loaded, loadedUID, _, loadErr := e.readWithUID(ctx, stsUID, "target")
	if loadErr != nil || loadedUID != ledgerUID || loaded.Ledger.Bootstrap == nil {
		t.Fatalf("test retained ledger load mismatch: uid=%q err=%v state=%+v", loadedUID, loadErr, loaded)
	}
	got, _, gotLedgerUID, err := e.drSuccessorAssignment(ctx, stsUID, rootUID, id, operationID)
	if err != nil {
		t.Fatalf("valid retained DR assignment rejected: %v", err)
	}
	if got != assignment || gotLedgerUID != ledgerUID {
		t.Fatalf("assignment/ledger = %+v/%q, want %+v/%q", got, gotLedgerUID, assignment, ledgerUID)
	}
	e.Prefix = "root"
	initialMembers := make([]quepaxa.Member, len(ms))
	for i := range ms {
		initialMembers[i] = ms[i]
	}
	options, anchored, err := e.membershipRepairEvidenceOptions(ctx, peerClosureBinding{rootUID: rootUID, workloadUID: stsUID, environment: map[string]string{
		"RHIZA_CLUSTER_MEMBERS": string(secretBytes), "RHIZA_OBJSTORE_DURABILITY": "before-ack",
	}}, "target", initialMembers)
	if err != nil || !anchored || options.ExpectedAnchorHash == ([32]byte{}) || options.ExpectedMembership != request.TargetMembership || options.ExpectedForkResult != request.Fork || options.ExpectedSourcePrefix != request.SourcePrefix || options.ExpectedOperationID != request.OperationID {
		t.Fatalf("retained DR approval did not resolve anchored-source evidence options: options=%+v anchored=%t err=%v", options, anchored, err)
	}
	wrongInitial := append([]quepaxa.Member(nil), initialMembers...)
	wrongInitial[0].PublicKey[0] ^= 1
	if _, _, err := e.membershipRepairEvidenceOptions(ctx, peerClosureBinding{rootUID: rootUID, workloadUID: stsUID, environment: map[string]string{
		"RHIZA_CLUSTER_MEMBERS": string(secretBytes), "RHIZA_OBJSTORE_DURABILITY": "before-ack",
	}}, "target", wrongInitial); err == nil {
		t.Fatal("anchored source accepted configured membership that differs from retained DR target")
	}
	attempt, err := e.beginInit(ctx, "", "n1", id, fencing.BeginInitRequest{Assignment: assignment, AttemptID: "attempt-1"})
	if err != nil || attempt.Assignment != assignment || attempt.Scope.ContainerID != "containerd://init-1" {
		t.Fatalf("DR init begin did not retain actual scope: attempt=%+v err=%v", attempt, err)
	}
	outcome := fencing.InitAttemptOutcome{State: fencing.InitAttemptMaterialized, LiveCleanup: fencing.InitLiveCleanup{Mode: fencing.InitModeRestore, BucketDrainSucceeded: true, CleanupSucceeded: true, NamespaceQuiescent: true}}
	if err := e.recordInitOutcome(ctx, "", "n1", id, fencing.RecordInitOutcomeRequest{Assignment: assignment, AttemptID: "attempt-1", Outcome: outcome}); err != nil {
		t.Fatalf("DR init outcome was not retained after fresh approval check: %v", err)
	}
	updatedLedger, _, _, err := e.readWithUID(ctx, stsUID, "target")
	if err != nil || len(updatedLedger.Ledger.Bootstrap.Operations[operationID].Attempts["attempt-1"].Outcomes) != 1 {
		t.Fatalf("DR init outcome missing from retained ledger: err=%v ledger=%+v", err, updatedLedger.Ledger.Bootstrap)
	}
	candidate := id
	candidate.RunID = "run-1"
	if _, err := client.BootstrapRuntimeAdmission(ctx, operationID, id); err == nil {
		t.Fatal("DR pre-bind readiness succeeded before terminal init evidence")
	}
	if _, err := e.registerIncarnation(ctx, candidate, stsUID); err == nil {
		t.Fatal("DR runtime registration succeeded before authority-observed init termination")
	}
	initStatus := asObject(list(nested(pod, "status", "initContainerStatuses"))[0])
	initStatus["state"] = object{"terminated": object{"containerID": "containerd://init-1", "imageID": "rhiza@sha256:init", "exitCode": float64(0), "reason": "Completed"}}
	_, observedSTS, observedRoot, observedErr := e.bootstrapCaller(ctx, "n1", id, operationID)
	if observedErr != nil || observedSTS != stsUID || observedRoot != rootUID {
		t.Fatalf("DR terminal caller scope mismatch: sts=%q root=%q err=%v", observedSTS, observedRoot, observedErr)
	}
	observedLedger, _, _, observedErr := e.readWithUID(ctx, stsUID, "target")
	if observedErr != nil || observedLedger.Ledger.Bootstrap.Operations[operationID].Purpose != fencing.BootstrapDRSuccessor {
		t.Fatalf("DR terminal operation unreadable: err=%v ledger=%+v", observedErr, observedLedger.Ledger.Bootstrap)
	}
	if err := updatedLedger.Ledger.RecordInitTerminal(assignment, "attempt-1", fencing.InitTerminalEvidence{ContainerID: "containerd://init-1", ImageID: "rhiza@sha256:init", ExitCode: 0, Reason: "Completed"}); err != nil {
		t.Fatalf("test terminal evidence invalid for retained attempt: %v", err)
	}
	if err := e.recordObservedInitTerminal(ctx, id, stsUID); err != nil {
		loaded, _, _, _ := e.readWithUID(ctx, stsUID, "target")
		t.Fatalf("DR init terminal observation was not retained: %v pod=%s ledger=%+v", err, jsonBytes(pod), loaded.Ledger.Bootstrap)
	}
	admission, err := client.BootstrapRuntimeAdmission(ctx, operationID, id)
	if err != nil || admission.Assignment != assignment {
		t.Fatalf("DR pre-bind runtime readiness did not return exact assignment: admission=%+v err=%v", admission, err)
	}
	registration, err := e.registerIncarnation(ctx, candidate, stsUID)
	if err != nil {
		t.Fatalf("completed DR assignment could not register through the production handler: %v", err)
	}
	runtime := registration.Identity
	if runtime.PodName != "" || runtime.CredentialUID != "" || runtime.NodeID != assignment.NodeID {
		t.Fatalf("DR runtime identity lost static-member semantics: %+v", runtime)
	}
	runtime.WALIdentity = "actual-wal-identity"
	if err := e.bindRuntime(ctx, stsUID, runtime); err != nil {
		t.Fatalf("DR runtime bind failed after fresh admission recheck: %v", err)
	}
	wrongBindingConfig := bindingConfig
	wrongBindingConfig.Bucket = "other-archive"
	e.AnchorBindingConfig = &wrongBindingConfig
	if _, _, _, err := e.drSuccessorAssignment(ctx, stsUID, rootUID, id, operationID); err == nil {
		t.Fatal("receipt binding inconsistent with independently derived target storage was accepted")
	}
	e.AnchorBindingConfig = &bindingConfig
	wrongUID := id
	wrongUID.PodUID = "different-pod-uid"
	if _, _, _, err := e.drSuccessorAssignment(ctx, stsUID, rootUID, wrongUID, operationID); err == nil {
		t.Fatal("assignment for another Pod UID was accepted")
	}
	backend.record.Generation++ // Simulate a fresh authority generation that revokes the retained receipt.
	if _, _, err := e.membershipRepairEvidenceOptions(ctx, peerClosureBinding{rootUID: rootUID, workloadUID: stsUID, environment: map[string]string{
		"RHIZA_CLUSTER_MEMBERS": string(secretBytes), "RHIZA_OBJSTORE_DURABILITY": "before-ack",
	}}, "target", initialMembers); err == nil {
		t.Fatal("anchored source evidence options survived revoked anchor generation")
	}
	if _, err := client.BootstrapRuntimeAdmission(ctx, operationID, id); err == nil {
		t.Fatal("pre-bind runtime admission accepted a revoked anchor generation")
	}
}

func TestValidateBootstrapLedgerOperationBindsEveryAssignmentAndAttempt(t *testing.T) {
	pin := bootstrapLedgerPin{
		OperationID: "op", RequestHash: "request", AuthorizationHash: "authorization",
		RootUID: "root", StatefulSetUID: "sts", SourceClusterID: "source",
		CredentialName: "rhiza-credentials", CredentialUID: "secret-uid",
		MembershipHash: "membership", ProfileHash: "profile", LedgerUID: "ledger-uid",
	}
	ledger := fencing.Ledger{SourceClusterID: pin.SourceClusterID}
	operation := fencing.BootstrapOperation{
		OperationID: pin.OperationID, RequestHash: pin.RequestHash, AuthorizationHash: pin.AuthorizationHash,
		Purpose: fencing.BootstrapInitial, RootUID: pin.RootUID, StatefulSetUID: pin.StatefulSetUID,
		SourceClusterID: pin.SourceClusterID,
	}
	if err := ledger.InstallBootstrapOperation(operation); err != nil {
		t.Fatal(err)
	}
	assignment := fencing.BootstrapAssignment{
		OperationID: pin.OperationID, RequestHash: pin.RequestHash, AuthorizationHash: pin.AuthorizationHash,
		Purpose: fencing.BootstrapInitial, RootUID: pin.RootUID, StatefulSetUID: pin.StatefulSetUID,
		SourceClusterID: pin.SourceClusterID, PodName: "rhiza-0", PodUID: "pod-uid", NodeID: "rhiza-0",
		CredentialName: pin.CredentialName, CredentialUID: pin.CredentialUID, MembershipHash: pin.MembershipHash,
		ProfileHash: pin.ProfileHash, LedgerUID: pin.LedgerUID,
	}
	if err := ledger.AssignBootstrap(assignment); err != nil {
		t.Fatal(err)
	}
	if err := validateBootstrapLedgerOperation(ledger, pin); err != nil {
		t.Fatalf("valid pinned ledger rejected: %v", err)
	}

	badAssignment := ledger
	badOperation := badAssignment.Bootstrap.Operations[pin.OperationID]
	badOperation.Assignments = map[string]fencing.BootstrapAssignment{assignment.PodUID: assignment}
	bad := assignment
	bad.CredentialUID = "replacement-secret-uid"
	badOperation.Assignments[bad.PodUID] = bad
	badAssignment.Bootstrap = &fencing.BootstrapHistory{RootUID: pin.RootUID, StatefulSetUID: pin.StatefulSetUID, Operations: map[string]fencing.BootstrapOperation{pin.OperationID: badOperation}}
	if err := validateBootstrapLedgerOperation(badAssignment, pin); err == nil {
		t.Fatal("assignment with a different immutable credential was accepted")
	}

	badOperationLedger := ledger
	badOperationLedger.Bootstrap = &fencing.BootstrapHistory{RootUID: pin.RootUID, StatefulSetUID: pin.StatefulSetUID, Operations: map[string]fencing.BootstrapOperation{}}
	if err := validateBootstrapLedgerOperation(badOperationLedger, pin); err == nil {
		t.Fatal("missing pinned operation was accepted")
	}

	orphanAttemptLedger := ledger
	orphanOperation := ledger.Bootstrap.Operations[pin.OperationID]
	orphanOperation.Assignments = map[string]fencing.BootstrapAssignment{assignment.PodUID: assignment}
	orphanOperation.Attempts = map[string]fencing.InitAttempt{"different-key": {AttemptID: "nonce", Assignment: assignment}}
	orphanAttemptLedger.Bootstrap = &fencing.BootstrapHistory{RootUID: pin.RootUID, StatefulSetUID: pin.StatefulSetUID, Operations: map[string]fencing.BootstrapOperation{pin.OperationID: orphanOperation}}
	if err := validateBootstrapLedgerOperation(orphanAttemptLedger, pin); err == nil {
		t.Fatal("attempt stored under a different nonce key was accepted")
	}
}
