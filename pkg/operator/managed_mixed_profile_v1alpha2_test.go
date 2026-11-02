package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func mixedAuthoritySetup(t *testing.T) (*phase8Server, *V1Alpha2StatusStore, V1Alpha2AdmissionSnapshot) {
	t.Helper()
	cluster := strings.Replace(phase8Cluster, `"genesisIntent":"Create"`, `"genesisIntent":"Create","applicationProtocol":"rhiza-managed-sql-kv/v1"`, 1)
	server, store, name := phase8Setup(t, cluster, "")
	plan := initTestPlan(t)
	plan.ApplicationProtocol = types.ManagedApplicationProtocolSQLKV
	ctx := context.Background()
	if _, err := InitializeV1Alpha2AuthorityWithBootstrap(ctx, store, phase2Ref, name, plan, initTestBootstrap(t)); err != nil {
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
	assignment.Plan.ApplicationProtocol = types.ManagedApplicationProtocolSQLKV
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
