package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"gopkg.in/yaml.v3"
)

func TestManagedApplicationExplicitProfileLoaders(t *testing.T) {
	for _, protocol := range []string{types.ManagedApplicationProtocolSQLKV, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol} {
		t.Run(protocol, func(t *testing.T) {
			cluster := strings.Replace(phase8Cluster, `"genesisIntent":"Create"`, `"genesisIntent":"Create","applicationProtocol":"`+protocol+`"`, 1)
			server, store, name := phase8Setup(t, cluster, "")
			bootstrap := initTestBootstrap(t)
			bootstrap.EvidenceFormat = protocol
			members := make([]quepaxa.Member, len(bootstrap.Members))
			for i, member := range bootstrap.Members {
				members[i] = member.Member
			}
			digest, err := types.ManagedBootstrapPlanDigestForProtocol(string(bootstrap.ClusterID), members, bootstrap.PhysicalBinding, protocol, bootstrap.Durability, bootstrap.Reconfiguration, protocol)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := NewV1Alpha2ApplicationInitPlan(phase2Ref, "logical", "deployment", "assignment", bootstrap.PhysicalBinding, digest, protocol)
			if err != nil {
				t.Fatal(err)
			}
			plan.ApplicationProtocol = protocol
			ctx := context.Background()
			if _, err := InitializeV1Alpha2AuthorityWithBootstrap(ctx, store, phase2Ref, name, plan, bootstrap); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadV1Alpha2Admission(ctx, store, phase2Ref); err == nil || !strings.Contains(err.Error(), "missing application evidence") {
				t.Fatalf("unused initialization must not admit without evidence: %v", err)
			}
			// The existing phase8 HTTP fixture models one 47->48 CAS. Reset
			// only its RV between the two real writes, as mixedAuthoritySetup does.
			server.cluster = strings.Replace(server.cluster, `"resourceVersion":"48"`, `"resourceVersion":"47"`, 1)
			initial, err := LoadV1Alpha2ApplicationInit(ctx, store, phase2Ref)
			if err != nil {
				t.Fatal(err)
			}
			if _, result, err := CommitV1Alpha2ApplicationInit(ctx, store, initial, plan, authorityTestEvidence); err != nil || result != V1Alpha2CASApplied {
				t.Fatalf("E0 consumption must publish evidence: observation=%s error=%v", result, err)
			}
			if _, err := LoadV1Alpha2Admission(ctx, store, phase2Ref); err != nil {
				t.Fatal(err)
			}
			// Changing only the spec marker cannot relabel the bound plan.
			other := types.ManagedApplicationProtocolSQLKV
			if protocol == other {
				other = sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol
			}
			server.cluster = strings.Replace(server.cluster, `"applicationProtocol":"`+protocol+`"`, `"applicationProtocol":"`+other+`"`, 1)
			if _, err := LoadV1Alpha2ApplicationInit(ctx, store, phase2Ref); err == nil {
				t.Fatal("mismatched spec/assignment profile accepted")
			}
		})
	}
	for _, raw := range []string{`null`, `""`, `"unknown"`, `"rhiza-managed-graph/v1"`, `"rhiza-managed-request/v2"`} {
		obj := map[string]json.RawMessage{"spec": json.RawMessage(`{"applicationProtocol":` + raw + `}`)}
		if _, err := mixedApplicationProtocol(obj); err == nil {
			t.Fatalf("unsupported explicit profile %s accepted", raw)
		}
	}
}

func TestManagedMixedProfileCELReservationSelectors(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/operator/cluster-crd.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	get := func(value any, key string) any { object, _ := value.(map[string]any); return object[key] }
	versions, _ := get(get(doc, "spec"), "versions").([]any)
	var schema any
	for _, version := range versions {
		if get(version, "name") == "v1alpha2" {
			schema = get(get(version, "schema"), "openAPIV3Schema")
		}
	}
	rules, _ := get(schema, "x-kubernetes-validations").([]any)
	if len(rules) < 3 {
		t.Fatal("root validation rules missing")
	}
	guard := `!has(self.spec.applicationProtocol) || !has(self.status) || !has(self.status.authority) || !has(self.status.authority.pendingWrite) || !self.status.authority.pendingWrite || `
	var equalities []string
	for _, pair := range [][2]string{{"generation", "generation"}, {"deploymentUID", "binding.deploymentUID"}, {"storageID", "binding.storageID"}, {"__namespace__", "owner.__namespace__"}, {"name", "owner.name"}, {"uid", "owner.uid"}, {"logicalID", "owner.logicalID"}} {
		equalities = append(equalities, "self.status.authority.pendingRequest.reservation."+pair[0]+" == self.status.authority."+pair[1])
	}
	want := guard + "(has(self.status.authority.pendingRequest) && has(self.status.authority.pendingRequest.reservation) && " + strings.Join(equalities, " && ") + ")"
	if get(rules[1], "rule") != want || get(rules[1], "message") != "mixed pending must retain its exact authority reservation" {
		t.Fatal("bounded escaped reservation equality or its guards changed")
	}
	wantPending := `!has(self.spec.applicationProtocol) || !has(oldSelf.status) || !has(oldSelf.status.authority) || !has(oldSelf.status.authority.pendingWrite) || !oldSelf.status.authority.pendingWrite || !has(self.status) || !has(self.status.authority) || !has(self.status.authority.pendingWrite) || !self.status.authority.pendingWrite || (has(self.status.authority.pendingRequest) && has(oldSelf.status.authority.pendingRequest) && self.status.authority.pendingRequest == oldSelf.status.authority.pendingRequest)`
	if get(rules[2], "rule") != wantPending {
		t.Fatal("whole pending transition equality changed")
	}
	properties := get(get(get(get(schema, "properties"), "status"), "properties"), "authority")
	authority := get(properties, "properties")
	owner := get(authority, "owner")
	reservation := get(get(get(authority, "pendingRequest"), "properties"), "reservation")
	for _, object := range []any{owner, reservation} {
		fields := get(object, "properties")
		leaf := get(fields, "namespace")
		if get(leaf, "type") != "string" || get(leaf, "maxLength") != 63 || get(fields, "__namespace__") != nil {
			t.Fatal("JSON namespace property or 63-character bound changed")
		}
	}
	wantRequired := []any{"namespace", "name", "uid", "logicalID", "generation", "deploymentUID", "storageID"}
	if !reflect.DeepEqual(get(reservation, "required"), wantRequired) {
		t.Fatal("reservation required fields changed")
	}
}

func mixedAuthoritySetup(t *testing.T) (*phase8Server, *V1Alpha2StatusStore, V1Alpha2AdmissionSnapshot) {
	t.Helper()
	cluster := strings.Replace(phase8Cluster, `"genesisIntent":"Create"`, `"genesisIntent":"Create","applicationProtocol":"rhiza-managed-sql-kv/v1"`, 1)
	server, store, name := phase8Setup(t, cluster, "")
	plan, bootstrap := mixedTestBootstrapBinding(t, "deployment", "assignment", initTestBootstrap(t))
	ctx := context.Background()
	if _, err := InitializeV1Alpha2AuthorityWithBootstrap(ctx, store, phase2Ref, name, plan, bootstrap); err != nil {
		t.Fatal(err)
	}
	server.cluster = strings.Replace(server.cluster, `"resourceVersion":"48"`, `"resourceVersion":"47"`, 1)
	initial, err := LoadV1Alpha2ApplicationInit(ctx, store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, result, err := CommitV1Alpha2ApplicationInit(ctx, store, initial, plan, authorityTestEvidence); err != nil || result != V1Alpha2CASApplied {
		t.Fatalf("consume %s %v", result, err)
	}
	server.cluster = strings.Replace(server.cluster, `"resourceVersion":"48"`, `"resourceVersion":"47"`, 1)
	snapshot, err := LoadV1Alpha2Admission(ctx, store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	return server, store, snapshot
}

// The explicit protocol, evidence format and bootstrap digest are one binding.
// Build it with the production canonicalizer before handing the fixture to any
// status loader; changing only a legacy assignment's marker is invalid.
func mixedTestBootstrapBinding(t *testing.T, deploymentUID, assignmentID string, bootstrap V1Alpha2BootstrapPlan) (V1Alpha2ApplicationInitPlan, V1Alpha2BootstrapPlan) {
	t.Helper()
	bootstrap.EvidenceFormat = V1Alpha2ManagedSQLKVEvidenceFormat
	members := make([]quepaxa.Member, len(bootstrap.Members))
	for i, member := range bootstrap.Members {
		members[i] = member.Member
	}
	digest, err := types.ManagedBootstrapPlanDigestForProtocol(string(bootstrap.ClusterID), members, bootstrap.PhysicalBinding, bootstrap.EvidenceFormat, bootstrap.Durability, bootstrap.Reconfiguration, types.ManagedApplicationProtocolSQLKV)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := NewV1Alpha2ApplicationInitPlan(phase2Ref, "logical", deploymentUID, assignmentID, bootstrap.PhysicalBinding, digest, bootstrap.EvidenceFormat)
	if err != nil {
		t.Fatal(err)
	}
	plan.ApplicationProtocol = types.ManagedApplicationProtocolSQLKV
	return plan, bootstrap
}

func TestManagedMixedProfileRejectsLegacyBootstrapDigestBeforeWrite(t *testing.T) {
	cluster := strings.Replace(phase8Cluster, `"genesisIntent":"Create"`, `"genesisIntent":"Create","applicationProtocol":"rhiza-managed-sql-kv/v1"`, 1)
	server, store, name := phase8Setup(t, cluster, "")
	plan, bootstrap := mixedTestBootstrapBinding(t, "deployment", "assignment", initTestBootstrap(t))
	members := make([]quepaxa.Member, len(bootstrap.Members))
	for i, member := range bootstrap.Members {
		members[i] = member.Member
	}
	legacyDigest, err := types.ManagedBootstrapPlanDigest(string(bootstrap.ClusterID), members, bootstrap.PhysicalBinding, bootstrap.EvidenceFormat, bootstrap.Durability, bootstrap.Reconfiguration)
	if err != nil || legacyDigest == plan.BootstrapPlanDigest {
		t.Fatalf("mixed and legacy bootstrap domains did not differ: %v", err)
	}
	plan.BootstrapPlanDigest = legacyDigest
	if _, err := InitializeV1Alpha2AuthorityWithBootstrap(context.Background(), store, phase2Ref, name, plan, bootstrap); err == nil || !strings.Contains(err.Error(), "bootstrap digest mismatch") || server.puts != 0 {
		t.Fatalf("legacy digest reached mixed genesis write: err=%v puts=%d", err, server.puts)
	}
}

func TestManagedMixedProfileKVReservation(t *testing.T) {
	for _, operation := range []string{"put", "delete", "cas"} {
		t.Run(operation, func(t *testing.T) {
			server, store, old := mixedAuthoritySetup(t)
			ctx := context.Background()
			before := server.puts
			if _, _, err := ReserveV1Alpha2Write(ctx, store, old, old.Authority(), old.Evidence(), authorityTestRequest); err == nil || server.puts != before {
				t.Fatal("original-less mixed reservation reached API")
			}
			command := types.KVCommand{RequestID: string([]byte{0xff, 0, 'i'}), Operation: operation, Key: string([]byte{0xfe, 'k'}), Value: []byte{}, Expected: []byte{0, 0xff}, ExpectedExists: true, TTLMS: 13}
			reserved, result, err := ReserveV1Alpha2KVWrite(ctx, store, old, old.Authority(), old.Evidence(), command)
			if err != nil || result != V1Alpha2CASApplied {
				t.Fatalf("reserve %s %v", result, err)
			}
			got, present, err := reserved.PendingKV()
			if err != nil || !present || !reflect.DeepEqual(got, command) {
				t.Fatalf("intent changed %#v %v", got, err)
			}
			got.Expected[0] = 9
			again, _, _ := reserved.PendingKV()
			if again.Expected[0] != 0 {
				t.Fatal("pending accessor aliases state")
			}
			request, _ := reserved.Pending()
			wantID, _ := types.ManagedKVPendingWireID(command.RequestID)
			if request.ID != wantID || request.Kind != "kv" {
				t.Fatal("pending logical ID not transported losslessly")
			}
			// The fake API uses one successor RV; reset it between separate writes.
			server.cluster = strings.Replace(server.cluster, `"resourceVersion":"48"`, `"resourceVersion":"47"`, 1)
			loaded, err := LoadV1Alpha2Admission(ctx, store, phase2Ref)
			if err != nil {
				t.Fatal(err)
			}
			final, result, err := FinalizeV1Alpha2Write(ctx, store, loaded, loaded.Authority(), request, V1Alpha2WriteCommitted, json.RawMessage(`{"tip":8}`))
			if err != nil || result != V1Alpha2CASApplied {
				t.Fatalf("finalize %s %v", result, err)
			}
			if _, present := final.Pending(); present {
				t.Fatal("finalize retained pending")
			}
		})
	}
}

func TestManagedMixedProfileSQLAndLegacy(t *testing.T) {
	_, store, old := mixedAuthoritySetup(t)
	if _, result, err := ReserveV1Alpha2SQLWrite(context.Background(), store, old, old.Authority(), old.Evidence(), network.ExecuteRequest{RequestID: "sql", SQL: "CREATE TABLE t(v)"}); err != nil || result != V1Alpha2CASApplied {
		t.Fatalf("mixed SQL %s %v", result, err)
	}
	_, store, old = authorityTestSetup(t)
	if _, _, err := ReserveV1Alpha2KVWrite(context.Background(), store, old, old.Authority(), old.Evidence(), types.KVCommand{RequestID: "i", Operation: "put", Key: "k"}); err == nil {
		t.Fatal("legacy accepted KV")
	}
	if _, result, err := ReserveV1Alpha2Write(context.Background(), store, old, old.Authority(), old.Evidence(), authorityTestRequest); err != nil || result != V1Alpha2CASApplied {
		t.Fatalf("legacy changed %s %v", result, err)
	}
}

func TestManagedMixedProfilePendingRawFingerprint(t *testing.T) {
	var previous []string
	for _, key := range []string{string([]byte{0xff}), string([]byte{0xfe}), "\ufffd"} {
		_, store, old := mixedAuthoritySetup(t)
		command := types.KVCommand{RequestID: string([]byte{'i', 0xff}), Operation: "delete", Key: key}
		reserved, obs, err := ReserveV1Alpha2KVWrite(context.Background(), store, old, old.Authority(), old.Evidence(), command)
		if err != nil || obs != V1Alpha2CASApplied {
			t.Fatalf("reserve %s %v", obs, err)
		}
		request, _ := reserved.Pending()
		want, _ := types.ManagedKVFingerprint(command)
		legacy, _ := types.KVFingerprint(command)
		if request.Fingerprint != fmt.Sprintf("%x", want) || request.Fingerprint == fmt.Sprintf("%x", legacy) {
			t.Fatal("pending still uses legacy JSON fingerprint")
		}
		for _, seen := range previous {
			if request.Fingerprint == seen {
				t.Fatal("raw key identities collided")
			}
		}
		previous = append(previous, request.Fingerprint)
		loaded, err := LoadV1Alpha2Admission(context.Background(), store, phase2Ref)
		if err != nil {
			t.Fatal(err)
		}
		got, found, err := loaded.PendingKV()
		if err != nil || !found || !reflect.DeepEqual(got, command) {
			t.Fatalf("fresh load lost raw original %#v %v", got, err)
		}
	}
}

func TestManagedMixedProfilePendingRawKeySwapRejected(t *testing.T) {
	server, store, old := mixedAuthoritySetup(t)
	command := types.KVCommand{RequestID: "same", Operation: "put", Key: string([]byte{0xff})}
	reserved, _, err := ReserveV1Alpha2KVWrite(context.Background(), store, old, old.Authority(), old.Evidence(), command)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := reserved.Pending()
	loaded, err := LoadV1Alpha2Admission(context.Background(), store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{string([]byte{0xfe}), "\ufffd"} {
		changed := command
		changed.Key = key
		raw, _, err := types.EncodeManagedKVOriginal(changed)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodePendingKV(raw, request); err == nil {
			t.Fatal("same ID legacy-colliding raw key swap accepted")
		}
		before := server.puts
		if _, _, err := ReserveV1Alpha2KVWrite(context.Background(), store, loaded, loaded.Authority(), loaded.Evidence(), changed); err == nil || server.puts != before {
			t.Fatal("pending conflict reached status API")
		}
	}
}

func TestManagedMixedProfilePendingIntentAndExactOriginal(t *testing.T) {
	server, store, old := mixedAuthoritySetup(t)
	command := types.KVCommand{RequestID: "same", Operation: "delete", Key: "key"}
	reserved, obs, err := ReserveV1Alpha2KVWrite(context.Background(), store, old, old.Authority(), old.Evidence(), command)
	if err != nil || obs != V1Alpha2CASApplied {
		t.Fatal(obs, err)
	}
	request, _ := reserved.Pending()
	for _, change := range []func(*types.KVCommand){
		func(c *types.KVCommand) { c.Value = []byte{1} },
		func(c *types.KVCommand) { c.Expected = []byte{2} },
		func(c *types.KVCommand) { c.ExpectedExists = true },
		func(c *types.KVCommand) { c.Operation = "cas" },
		func(c *types.KVCommand) { c.TTLMS = 13 },
		func(c *types.KVCommand) { c.RequestID = "other" },
	} {
		changed := command
		change(&changed)
		raw, _, err := types.EncodeManagedKVOriginal(changed)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodePendingKV(raw, request); err == nil {
			t.Fatal("different intent/identity reused pending identity")
		}
	}
	// Fingerprint equivalence is NOT permission to replace the exact retained
	// original. Normal status mutation must reject even null/empty-only changes.
	equivalent := command
	equivalent.Value = []byte{}
	equivalent.Expected = []byte{}
	fp, _ := types.ManagedKVFingerprint(equivalent)
	if fmt.Sprintf("%x", fp) != request.Fingerprint {
		t.Fatal("nil/empty logical equivalence changed")
	}
	raw, _, err := types.EncodeManagedKVOriginal(equivalent)
	if err != nil {
		t.Fatal(err)
	}
	before := server.puts
	_, _, err = updateV1Alpha2AdmissionStatus(context.Background(), store, reserved, func(_ map[string]json.RawMessage, auth map[string]json.RawMessage) {
		var pending v1alpha2PendingRequest
		_ = json.Unmarshal(auth["pendingRequest"], &pending)
		pending.OriginalKV = raw
		auth["pendingRequest"], _ = json.Marshal(pending)
	})
	if err == nil || server.puts != before {
		t.Fatal("equivalent fingerprint authorized original swap")
	}
	got, found, err := reserved.PendingKV()
	if err != nil || !found || got.Value != nil || got.Expected != nil {
		t.Fatal("retained exact null original changed")
	}
}

func TestManagedMixedProfilePendingRejectsLegacyDomain(t *testing.T) {
	server, store, old := mixedAuthoritySetup(t)
	command := types.KVCommand{RequestID: "domain", Operation: "put", Key: "key"}
	if _, _, err := ReserveV1Alpha2KVWrite(context.Background(), store, old, old.Authority(), old.Evidence(), command); err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(server.cluster), &object); err != nil {
		t.Fatal(err)
	}
	status, _ := jsonObject(object["status"])
	auth, _ := jsonObject(status["authority"])
	var pending v1alpha2PendingRequest
	_ = json.Unmarshal(auth["pendingRequest"], &pending)
	legacy, _ := types.KVFingerprint(command)
	pending.Fingerprint = fmt.Sprintf("%x", legacy)
	auth["pendingRequest"], _ = json.Marshal(pending)
	status["authority"], _ = json.Marshal(auth)
	object["status"], _ = json.Marshal(status)
	b, _ := json.Marshal(object)
	server.cluster = string(b)
	if _, err := LoadV1Alpha2Admission(context.Background(), store, phase2Ref); err == nil {
		t.Fatal("legacy domain silently accepted as mixed")
	}
}

func TestManagedMixedProfileAmbiguousReservation(t *testing.T) {
	server, store, old := mixedAuthoritySetup(t)
	server.timeout = true
	_, result, err := ReserveV1Alpha2KVWrite(context.Background(), store, old, old.Authority(), old.Evidence(), types.KVCommand{RequestID: "i", Operation: "put", Key: "k"})
	if !errors.Is(err, ErrV1Alpha2StatusOutcomeUnknown) || result != V1Alpha2CASMatchingReadback {
		t.Fatalf("matching readback authorized submission: %s %v", result, err)
	}
}

func TestManagedMixedProfileRejectsInflationAndMutation(t *testing.T) {
	server, store, old := mixedAuthoritySetup(t)
	command := types.KVCommand{RequestID: "i", Operation: "cas", Key: "k", Value: []byte{1}}
	reserved, _, err := ReserveV1Alpha2KVWrite(context.Background(), store, old, old.Authority(), old.Evidence(), command)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(map[string]json.RawMessage, map[string]json.RawMessage){
		func(_ map[string]json.RawMessage, auth map[string]json.RawMessage) {
			var r v1alpha2PendingRequest
			_ = json.Unmarshal(auth["pendingRequest"], &r)
			r.ID = "foreign"
			auth["pendingRequest"], _ = json.Marshal(r)
		},
		func(_ map[string]json.RawMessage, auth map[string]json.RawMessage) {
			var r v1alpha2PendingRequest
			_ = json.Unmarshal(auth["pendingRequest"], &r)
			r.OriginalSQL = json.RawMessage(`{}`)
			auth["pendingRequest"], _ = json.Marshal(r)
		},
		func(_ map[string]json.RawMessage, auth map[string]json.RawMessage) {
			auth["evidence"], _ = json.Marshal(strings.Repeat("e", 64<<10))
		},
		func(status map[string]json.RawMessage, _ map[string]json.RawMessage) {
			status["opaque"], _ = json.Marshal(strings.Repeat("x", 64<<10))
		},
		func(status map[string]json.RawMessage, _ map[string]json.RawMessage) {
			var a V1Alpha2ApplicationInitAssignment
			_ = json.Unmarshal(status["applicationInit"], &a)
			a.Plan.ApplicationProtocol = ""
			status["applicationInit"], _ = json.Marshal(a)
		},
	} {
		before := server.puts
		if _, _, err := updateV1Alpha2AdmissionStatus(context.Background(), store, reserved, change); err == nil || server.puts != before {
			t.Fatal("invalid mixed object reached status API")
		}
	}
	// A response with inflated metadata is not an Applied proof even if status matches.
	server, store, old = mixedAuthoritySetup(t)
	base := store.Kube.Client.Transport
	store.Kube.Client.Transport = phase2Transport(func(r *http.Request) (*http.Response, error) {
		response, err := base.RoundTrip(r)
		if err == nil && r.Method == http.MethodPut {
			_ = response.Body.Close()
			var obj map[string]json.RawMessage
			_ = json.Unmarshal([]byte(server.cluster), &obj)
			meta, _ := jsonObject(obj["metadata"])
			meta["annotations"], _ = json.Marshal(map[string]string{"large": strings.Repeat("x", 128<<10)})
			obj["metadata"], _ = json.Marshal(meta)
			raw, _ := json.Marshal(obj)
			return phase2Response(http.StatusOK, string(raw)), nil
		}
		return response, err
	})
	_, result, err := ReserveV1Alpha2KVWrite(context.Background(), store, old, old.Authority(), old.Evidence(), command)
	if err == nil || result == V1Alpha2CASApplied {
		t.Fatal("inflated response established ownership")
	}
}

func TestManagedMixedProfileComponentBoundary(t *testing.T) {
	_, _, old := mixedAuthoritySetup(t)
	for _, count := range []int{(64 << 10) - 2, 64 << 10} {
		var obj map[string]json.RawMessage
		raw, _ := json.Marshal(old.status.object)
		_ = json.Unmarshal(raw, &obj)
		status, _ := jsonObject(obj["status"])
		auth, _ := jsonObject(status["authority"])
		auth["evidence"], _ = json.Marshal(strings.Repeat("e", count))
		status["authority"], _ = json.Marshal(auth)
		obj["status"], _ = json.Marshal(status)
		err := validateManagedMixedProfile(obj)
		if (err == nil) != (count == (64<<10)-2) {
			t.Fatalf("evidence boundary %d: %v", count, err)
		}
	}
	var obj map[string]json.RawMessage
	raw, _ := json.Marshal(old.status.object)
	_ = json.Unmarshal(raw, &obj)
	status, _ := jsonObject(obj["status"])
	status["repair"] = json.RawMessage(append(append([]byte(`"`), bytes.Repeat([]byte("r"), 128<<10)...), '"'))
	obj["status"], _ = json.Marshal(status)
	if validateManagedMixedProfile(obj) == nil {
		t.Fatal("oversized repair accepted")
	}
}

func TestManagedMixedProfileFutureHeadroom(t *testing.T) {
	_, _, old := mixedAuthoritySetup(t)
	var object map[string]json.RawMessage
	raw, _ := json.Marshal(old.status.object)
	_ = json.Unmarshal(raw, &object)
	status, _ := jsonObject(object["status"])
	auth, _ := jsonObject(status["authority"])
	evidence := auth["evidence"]
	delete(auth, "evidence")
	status["authority"], _ = json.Marshal(auth)
	status["recoveryQueue"] = json.RawMessage(`{"padding":""}`)
	object["status"], _ = json.Marshal(status)
	base, _ := json.Marshal(object)
	metadata, _ := json.Marshal(object["metadata"])
	padding := managedMixedProjectedBaseLimit - managedMixedMetadataCeiling + len(metadata) - len(base)
	if padding <= 0 || managedMixedProjectedBaseLimit+managedMixedFutureReserve != 1004<<10 {
		t.Fatal("invalid profile arithmetic")
	}
	auth["evidence"] = evidence
	status["authority"], _ = json.Marshal(auth)
	for _, extra := range []int{0, 1} {
		status["recoveryQueue"], _ = json.Marshal(map[string]string{"padding": strings.Repeat("x", padding+extra)})
		object["status"], _ = json.Marshal(status)
		actual, _ := json.Marshal(object)
		if len(actual) >= managedMixedObjectLimit {
			t.Fatal("test must remain below current full object limit")
		}
		err := validateManagedMixedProfile(object)
		if (err == nil) != (extra == 0) {
			t.Fatalf("projected base boundary extra=%d actual=%d: %v", extra, len(actual), err)
		}
	}
}

func TestManagedMixedProfileQueueWriterRejectsInflation(t *testing.T) {
	server, store, old := mixedAuthoritySetup(t)
	queue, _ := json.Marshal(map[string]string{"padding": strings.Repeat("q", 384<<10)})
	before := server.puts
	if _, err := store.compareAndSwapQueue(context.Background(), old.status, queue, false); err == nil || server.puts != before {
		t.Fatal("queue status writer bypassed mixed projection")
	}
}

func mixedTestMetadataBytes(t *testing.T, object string, size int) string {
	t.Helper()
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(object), &obj); err != nil {
		t.Fatal(err)
	}
	meta, _ := jsonObject(obj["metadata"])
	meta["annotations"] = json.RawMessage(`{"padding":""}`)
	base, _ := json.Marshal(meta)
	if size < len(base) {
		t.Fatal("metadata target too small")
	}
	meta["annotations"], _ = json.Marshal(map[string]string{"padding": strings.Repeat("m", size-len(base))})
	obj["metadata"], _ = json.Marshal(meta)
	raw, _ := json.Marshal(obj)
	return string(raw)
}

func TestManagedMixedProfileMetadataGrowth(t *testing.T) {
	server, store, _ := mixedAuthoritySetup(t)
	server.cluster = mixedTestMetadataBytes(t, server.cluster, 48<<10)
	old, err := LoadV1Alpha2Admission(context.Background(), store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	base := store.Kube.Client.Transport
	store.Kube.Client.Transport = phase2Transport(func(r *http.Request) (*http.Response, error) {
		response, err := base.RoundTrip(r)
		if err == nil && r.Method == http.MethodPut && response.StatusCode == http.StatusOK {
			_ = response.Body.Close()
			server.cluster = mixedTestMetadataBytes(t, server.cluster, 70<<10)
			return phase2Response(http.StatusOK, server.cluster), nil
		}
		return response, err
	})
	reserved, result, err := ReserveV1Alpha2KVWrite(context.Background(), store, old, old.Authority(), old.Evidence(), types.KVCommand{RequestID: "growth", Operation: "put", Key: "k"})
	if err != nil || result != V1Alpha2CASApplied {
		t.Fatalf("permitted metadata growth rejected: %s %v", result, err)
	}
	for i := 0; i < 2; i++ {
		if _, err := LoadV1Alpha2Admission(context.Background(), store, phase2Ref); err != nil {
			t.Fatalf("repeated Load consumed fresh reserve: %v", err)
		}
	}
	server.cluster = strings.Replace(server.cluster, `"resourceVersion":"48"`, `"resourceVersion":"47"`, 1)
	loaded, err := LoadV1Alpha2Admission(context.Background(), store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := reserved.Pending()
	if _, result, err := FinalizeV1Alpha2Write(context.Background(), store, loaded, loaded.Authority(), request, V1Alpha2WriteCommitted, loaded.Evidence()); err != nil || result != V1Alpha2CASApplied {
		t.Fatalf("finalize after growth: %s %v", result, err)
	}
}

func TestManagedMixedProfilePendingContextImmutable(t *testing.T) {
	for _, field := range []string{"generation", "binding", "owner"} {
		t.Run(field, func(t *testing.T) {
			server, store, old := mixedAuthoritySetup(t)
			reserved, _, err := ReserveV1Alpha2KVWrite(context.Background(), store, old, old.Authority(), old.Evidence(), types.KVCommand{RequestID: "context", Operation: "put", Key: "k"})
			if err != nil {
				t.Fatal(err)
			}
			before := server.puts
			_, _, err = updateV1Alpha2AdmissionStatus(context.Background(), store, reserved, func(_ map[string]json.RawMessage, auth map[string]json.RawMessage) {
				switch field {
				case "generation":
					auth["generation"] = json.RawMessage("1")
				case "binding":
					auth["binding"] = json.RawMessage(`{"deploymentUID":"other","storageID":"other"}`)
				case "owner":
					auth["owner"] = json.RawMessage(`{"namespace":"ns","name":"cluster","uid":"other","logicalID":"logical"}`)
				}
			})
			if err == nil || server.puts != before {
				t.Fatalf("pending %s changed: puts=%d before=%d error=%v", field, server.puts, before, err)
			}
			// A later raw observation has no old snapshot. Its retained context
			// must independently reject authority replacement on Load.
			var object map[string]json.RawMessage
			_ = json.Unmarshal([]byte(server.cluster), &object)
			status, _ := jsonObject(object["status"])
			auth, _ := jsonObject(status["authority"])
			auth["generation"] = json.RawMessage("1")
			status["authority"], _ = json.Marshal(auth)
			object["status"], _ = json.Marshal(status)
			raw, _ := json.Marshal(object)
			server.cluster = string(raw)
			if _, err := LoadV1Alpha2Admission(context.Background(), store, phase2Ref); err == nil {
				t.Fatal("reload reattached original to another generation")
			}
		})
	}
}

func TestManagedMixedProfileMetadataCeiling(t *testing.T) {
	server, store, _ := mixedAuthoritySetup(t)
	for _, size := range []int{managedMixedMetadataCeiling, managedMixedMetadataCeiling + 1} {
		server.cluster = mixedTestMetadataBytes(t, server.cluster, size)
		_, err := LoadV1Alpha2Admission(context.Background(), store, phase2Ref)
		if (err == nil) != (size == managedMixedMetadataCeiling) {
			t.Fatalf("metadata ceiling %d: %v", size, err)
		}
	}
}

func TestManagedMixedProfileRepairPreservesReservation(t *testing.T) {
	f, _ := repairTestSetup(t)
	spec, _ := jsonObject(f.cluster["spec"])
	spec["applicationProtocol"], _ = json.Marshal(types.ManagedApplicationProtocolSQLKV)
	f.cluster["spec"], _ = json.Marshal(spec)
	status, _ := jsonObject(f.cluster["status"])
	var assignment V1Alpha2ApplicationInitAssignment
	_ = json.Unmarshal(status["applicationInit"], &assignment)
	var sourceBootstrap V1Alpha2BootstrapPlan
	if err := json.Unmarshal(status["bootstrapPlan"], &sourceBootstrap); err != nil {
		t.Fatal(err)
	}
	var mixedBootstrap V1Alpha2BootstrapPlan
	assignment.Plan, mixedBootstrap = mixedTestBootstrapBinding(t, assignment.Plan.DeploymentUID, assignment.Plan.AssignmentID, sourceBootstrap)
	status["bootstrapPlan"], _ = json.Marshal(mixedBootstrap)
	f.plan.BootstrapPlanDigest = assignment.Plan.BootstrapPlanDigest
	status["applicationInit"], _ = json.Marshal(assignment)
	f.cluster["status"], _ = json.Marshal(status)
	raw, _ := json.Marshal(f.cluster)
	grown := mixedTestMetadataBytes(t, string(raw), 70<<10)
	_ = json.Unmarshal([]byte(grown), &f.cluster)
	ctx := context.Background()
	old, err := LoadV1Alpha2Admission(ctx, f.store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	pending, obs, err := ReserveV1Alpha2KVWrite(ctx, f.store, old, old.Authority(), old.Evidence(), types.KVCommand{RequestID: "repair-context", Operation: "cas", Key: "k"})
	if err != nil || obs != V1Alpha2CASApplied {
		t.Fatalf("reserve after metadata growth %s %v", obs, err)
	}
	closed, obs, err := BeginV1Alpha2Repair(ctx, f.store, pending, pending.Authority(), f.plan)
	if err != nil || obs != V1Alpha2CASApplied {
		t.Fatalf("legitimate close %s %v", obs, err)
	}
	bootstrap, err := closed.status.BootstrapPlan()
	if err != nil {
		t.Fatal(err)
	}
	_, prefix, err := objectstore.ManagedNamespacePrefixes("", f.plan.ClusterUID, f.plan.Generation, assignment.Plan.AssignmentID, string(bootstrap.ClusterID))
	if err != nil {
		t.Fatal(err)
	}
	head := bytes.Repeat([]byte("h"), V1Alpha2RepairHeadLimit)
	handoff := V1Alpha2RepairHandoff{PlanDigest: f.plan.Digest(), SourcePrefix: prefix, PhysicalBinding: assignment.Plan.PhysicalBinding, Head: head, HeadHash: fmt.Sprintf("%x", sha256.Sum256(head)), RemoveTerminalSlot: 17, RemoveTerminalHash: strings.Repeat("a", 64), Survivors: quepaxa.Cluster{ConfigID: 2}}
	for _, m := range bootstrap.Members {
		if m.Member.ID != f.plan.OldNodeID {
			handoff.Survivors.Members = append(handoff.Survivors.Members, m.Member)
		}
	}
	captured, obs, err := CaptureV1Alpha2RepairHandoff(ctx, f.store, closed, closed.Authority(), handoff)
	if err != nil || obs != V1Alpha2CASApplied {
		t.Fatalf("legitimate handoff %s %v", obs, err)
	}
	completion := repairCompletion(f.plan, "Terminal")
	finished, obs, err := FinishV1Alpha2Repair(ctx, f.store, captured, captured.Authority(), completion)
	if err != nil || obs != V1Alpha2CASApplied {
		t.Fatalf("legitimate completion %s %v", obs, err)
	}
	request, _ := pending.Pending()
	if _, obs, err := FinalizeV1Alpha2Write(ctx, f.store, finished, finished.Authority(), request, V1Alpha2WriteCommitted, finished.Evidence()); err != nil || obs != V1Alpha2CASApplied {
		t.Fatalf("finalize after legitimate repair %s %v", obs, err)
	}
}

func TestManagedMixedProfileCompletionHeadroom(t *testing.T) {
	plan := V1Alpha2RepairPlan{OperationID: "repair", FinalVoterIDs: []types.NodeID{types.NodeID(strings.Repeat("a", 72)), types.NodeID(strings.Repeat("b", 72))}}
	record := V1Alpha2RepairRecord{Plan: plan, State: "Closed", PreparedHandoff: &V1Alpha2RepairHandoff{Survivors: quepaxa.Cluster{Members: []quepaxa.Member{{URL: ""}}}}}
	raw, _ := json.Marshal(record)
	// The current record fits, but adding its required completion does not.
	record.PreparedHandoff.Survivors.Members[0].URL = strings.Repeat("x", V1Alpha2RepairRecordLimit-len(raw)-1)
	raw, _ = json.Marshal(record)
	if len(raw) >= V1Alpha2RepairRecordLimit {
		t.Fatal("fixture current record must fit")
	}
	if validateMixedRepairBudget(raw) == nil {
		t.Fatal("capture consumed future completion headroom")
	}
	// Restore a bounded actual handoff field; the complete successor now fits.
	record.PreparedHandoff.Survivors.Members[0].URL = "quic://127.0.0.1:9090"
	raw, _ = json.Marshal(record)
	if err := validateMixedRepairBudget(raw); err != nil {
		t.Fatal(err)
	}
}
