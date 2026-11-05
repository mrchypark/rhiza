package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"gopkg.in/yaml.v3"
)

func TestManagedApplicationExplicitProfileLoaders(t *testing.T) {
	for _, protocol := range []string{sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol} {
		t.Run(protocol, func(t *testing.T) {
			cluster := phase9ClusterWithProtocol(protocol)
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
	return historicalConsumedFixture(t, types.ManagedApplicationProtocolSQLKV)
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
	cluster := phase9ClusterWithProtocol(types.ManagedApplicationProtocolSQLKV)
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
	if _, err := InitializeV1Alpha2AuthorityWithBootstrap(context.Background(), store, phase2Ref, name, plan, bootstrap); err == nil || !strings.Contains(err.Error(), "profiles 7/8 are retired") || server.puts != 0 {
		t.Fatalf("legacy digest reached mixed genesis write: err=%v puts=%d", err, server.puts)
	}
}

func TestManagedMixedProfilePendingRawFingerprint(t *testing.T) {
	var previous []string
	for _, key := range []string{string([]byte{0xff}), string([]byte{0xfe}), "\ufffd"} {
		server, store, old := mixedAuthoritySetup(t)
		command := types.KVCommand{RequestID: string([]byte{'i', 0xff}), Operation: "delete", Key: key}
		reserved := historicalPhase8Pending(t, server, store, old, command)
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
	reserved := historicalPhase8Pending(t, server, store, old, command)
	request, _ := reserved.Pending()
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
	}
}

func TestManagedMixedProfilePendingIntentAndExactOriginal(t *testing.T) {
	server, store, old := mixedAuthoritySetup(t)
	command := types.KVCommand{RequestID: "same", Operation: "delete", Key: "key"}
	reserved := historicalPhase8Pending(t, server, store, old, command)
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
	historicalPhase8Pending(t, server, store, old, command)
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

func TestManagedMixedProfileRejectsInflationAndMutation(t *testing.T) {
	server, store, old := mixedAuthoritySetup(t)
	command := types.KVCommand{RequestID: "i", Operation: "cas", Key: "k", Value: []byte{1}}
	reserved := historicalPhase8Pending(t, server, store, old, command)
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
	server, store, old := mixedAuthoritySetup(t)
	stored := historicalPhase8Pending(t, server, store, old, types.KVCommand{RequestID: "growth", Operation: "put", Key: "k"})
	want, _ := stored.Pending()
	puts := server.puts
	for _, size := range []int{48 << 10, 70 << 10} {
		server.cluster = mixedTestMetadataBytes(t, server.cluster, size)
		for range 2 {
			loaded, err := LoadV1Alpha2Admission(context.Background(), store, phase2Ref)
			got, pending := loaded.Pending()
			if err != nil || !pending || got != want || !sameJSON(loaded.Evidence(), stored.Evidence()) {
				t.Fatalf("metadata growth changed UNKNOWN: size=%d pending=%t error=%v", size, pending, err)
			}
		}
	}
	if server.puts != puts {
		t.Fatal("observation wrote status")
	}
}

func TestManagedMixedProfilePendingContextImmutable(t *testing.T) {
	for _, field := range []string{"generation", "binding", "owner"} {
		t.Run(field, func(t *testing.T) {
			server, store, old := mixedAuthoritySetup(t)
			reserved := historicalPhase8Pending(t, server, store, old, types.KVCommand{RequestID: "context", Operation: "put", Key: "k"})
			before := server.puts
			_, _, err := updateV1Alpha2AdmissionStatus(context.Background(), store, reserved, func(_ map[string]json.RawMessage, auth map[string]json.RawMessage) {
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

func TestManagedMixedProfileRepairRefusesHistoricalReservation(t *testing.T) {
	for _, protocol := range []string{"", types.ManagedApplicationProtocolSQLKV} {
		t.Run(protocol, func(t *testing.T) {
			server, store, old := historicalConsumedFixture(t, protocol)
			pending := historicalPhase8Pending(t, server, store, old, network.ExecuteRequest{RequestID: "historical-unknown", SQL: "SELECT 1"})
			before, puts := server.cluster, server.puts
			want, _ := pending.Pending()
			for _, action := range []struct {
				name string
				run  func() error
			}{
				{"begin", func() error {
					_, _, err := BeginV1Alpha2Repair(context.Background(), store, pending, pending.Authority(), V1Alpha2RepairPlan{})
					return err
				}},
				{"finish", func() error {
					_, _, err := FinishV1Alpha2Repair(context.Background(), store, pending, pending.Authority(), V1Alpha2RepairCompletion{})
					return err
				}},
				{"capture", func() error {
					_, _, err := CaptureV1Alpha2RepairHandoff(context.Background(), store, pending, pending.Authority(), V1Alpha2RepairHandoff{})
					return err
				}},
				{"record-add", func() error {
					_, _, err := RecordV1Alpha2RepairAddition(context.Background(), store, pending, pending.Authority(), V1Alpha2RepairCompletion{})
					return err
				}},
			} {
				if err := action.run(); err == nil || !strings.Contains(err.Error(), "profiles 7/8 are retired") {
					t.Fatalf("%s did not reject historical execution profile first: %v", action.name, err)
				}
				if server.puts != puts || server.cluster != before {
					t.Fatalf("%s mutated historical UNKNOWN", action.name)
				}
			}
			loaded, err := LoadV1Alpha2Admission(context.Background(), store, phase2Ref)
			got, remains := loaded.Pending()
			if err != nil || !remains || got != want || !sameJSON(loaded.Evidence(), pending.Evidence()) {
				t.Fatalf("historical UNKNOWN was resolved or replaced: %v", err)
			}
		})
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
