//go:build rhiza_local_testhooks

package managedsql

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func TestReplaceVoterCoordinatesActualPreparedLearner(t *testing.T) {
	runReplaceVoterCoordinatesActualPreparedLearner(t, nil)
}

func TestReplaceVoterFreshInvocationCompletesActualAddThenRemove(t *testing.T) {
	runReplaceVoterCoordinatesActualPreparedLearner(t, nil, true)
}

func runReplaceVoterCoordinatesActualPreparedLearner(t *testing.T, beforeRetainedRestart func(context.Context, *Host, Config, *Host) func(*Host), freshInvocation ...bool) {
	freshCall := len(freshInvocation) == 1 && freshInvocation[0]
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	const protocol = sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol
	// Freeze actual background publishers before any runtime starts. Foreground
	// BeforeAck archive sync and all real quorum/membership operations remain live.
	backgroundPaused := pauseCertifiedRecoveryBackground(t)
	configs, seed := sqlClusterForProtocol(t, protocol)
	// Hold the real archive HEAD request after application execution so this
	// profile's engine-side original becomes UNKNOWN without inventing a normal
	// Kubernetes PendingWrite (request protocols do not use that reservation).
	archiveEntered := make(chan string, 8)
	archiveRelease := make(chan struct{})
	var archiveArmed atomic.Bool
	var archiveReleased atomic.Bool
	seed.source.putReady = archiveEntered
	seed.source.putRelease = archiveRelease
	seed.source.putSuffix = "archive/head.bin"
	seed.source.putEnabled = &archiveArmed
	releaseArchive := func() {
		archiveArmed.Store(false)
		if archiveReleased.CompareAndSwap(false, true) {
			close(archiveRelease)
		}
	}

	fresh := &sqlStatusFixture{obj: make(map[string]json.RawMessage)}
	for key, value := range seed.obj {
		if key != "status" {
			fresh.obj[key] = append(json.RawMessage(nil), value...)
		}
	}
	fresh.obj["status"] = json.RawMessage(`{}`)
	api := httptest.NewTLSServer(http.HandlerFunc(fresh.serve))
	t.Cleanup(api.Close)
	kube := *configs[0].Store.Kube
	kube.BaseURL, kube.Client = api.URL, api.Client()
	store := &operator.V1Alpha2StatusStore{Kube: &kube}
	for i := range configs {
		configs[i].Runtime.EnableReconfiguration = true
		configs[i].Store = store
	}
	bootstrapNode := node.New(&configs[0].Runtime)
	source, _, err := bootstrapNode.PreparedStartupBindingDigestsForProtocol(protocol)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := bootstrapNode.ManagedBootstrapPlanDigestForProtocol(source, protocol, protocol)
	if err != nil {
		t.Fatal(err)
	}
	initPlan, err := operator.NewV1Alpha2ApplicationInitPlan(configs[0].Ref, "logical", "deployment", "assignment", source, digest, protocol)
	if err != nil {
		t.Fatal(err)
	}
	initPlan.ApplicationProtocol = protocol
	bootstrap := operator.V1Alpha2BootstrapPlan{ClusterID: configs[0].Runtime.ClusterID, PhysicalBinding: source, EvidenceFormat: protocol, Durability: types.ObjectStoreDurabilityBeforeAck, Reconfiguration: true}
	for i, member := range configs[0].Runtime.Members {
		bootstrap.Members = append(bootstrap.Members, operator.V1Alpha2BootstrapMember{PodUID: configs[i].PodUID, Member: member})
	}
	deployment, _ := operator.V1Alpha2GenesisDeploymentName(configs[0].Ref.UID)
	if _, err := operator.InitializeV1Alpha2AuthorityWithBootstrap(ctx, store, configs[0].Ref, deployment, initPlan, bootstrap); err != nil {
		t.Fatal(err)
	}
	hosts := make([]*Host, 3)
	for _, i := range []int{1, 2} {
		configs[i].Plan = initPlan
		hosts[i], err = seed.open(ctx, configs[i])
		if hosts[i] != nil {
			h := hosts[i]
			t.Cleanup(func() {
				if err := h.Close(); err != nil {
					t.Error(err)
				}
			})
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	configs[0].Plan = initPlan
	writer, plan := hostRepairWriter(t, configs, hosts)
	plan.Sequence = replacementSequenceForProtocol(protocol)
	peerAddr := seed.reservePeer(t, plan.Replacement.NodeID)
	plan.Replacement.PeerURL = "quic://" + peerAddr
	originalTransport := writer.Kube.Client.Transport
	var rejectAddRecord atomic.Bool
	rejectAddRecord.Store(!freshCall)
	var rejectFinish atomic.Bool
	rejectFinish.Store(!freshCall)
	freshAddProofEntered := make(chan struct{}, 1)
	freshRemoveReady := make(chan struct{})
	type coordinatorStartupKey struct{}
	var listenerGateArmed atomic.Bool
	listenerVerified := make(chan struct{})
	var releaseListenerOnce sync.Once
	releaseListener := func() { releaseListenerOnce.Do(func() { close(listenerVerified) }) }
	t.Cleanup(releaseListener)
	writer.Kube.Client.Transport = repairGateTransport(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodGet && request.Context().Value(coordinatorStartupKey{}) == true && listenerGateArmed.Load() {
			select {
			case <-listenerVerified:
			case <-request.Context().Done():
				return nil, request.Context().Err()
			}
		}
		if request.Method == http.MethodPut && request.Body != nil {
			body, err := io.ReadAll(request.Body)
			if err != nil {
				return nil, err
			}
			request.Body = io.NopCloser(bytes.NewReader(body))
			var update struct {
				Status struct {
					Repair operator.V1Alpha2RepairRecord
				}
			}
			if json.Unmarshal(body, &update) == nil {
				if freshCall && update.Status.Repair.State == "Closed" && update.Status.Repair.AddCompletion != nil {
					select {
					case freshAddProofEntered <- struct{}{}:
					default:
					}
					select {
					case <-freshRemoveReady:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				if update.Status.Repair.State == "Closed" && update.Status.Repair.AddCompletion != nil && rejectAddRecord.CompareAndSwap(true, false) {
					return nil, errors.New("injected ADD proof CAS failure after committed terminal")
				}
				if update.Status.Repair.State == "Completed" && rejectFinish.CompareAndSwap(true, false) {
					return nil, errors.New("injected Finish failure after committed ADD and REMOVE")
				}
			}
		}
		response, err := originalTransport.RoundTrip(request)
		if err != nil || !strings.Contains(request.URL.Path, "/pods/") {
			return response, err
		}
		defer response.Body.Close()
		var pod map[string]any
		if err := json.NewDecoder(response.Body).Decode(&pod); err != nil {
			return nil, err
		}
		pod["status"].(map[string]any)["podIP"] = "127.0.0.1"
		return repairGateJSON(pod), nil
	})
	configs[0].Store = writer
	hosts[0], err = seed.open(ctx, configs[0])
	if hosts[0] != nil {
		h := hosts[0]
		t.Cleanup(func() {
			if err := h.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range hosts {
		readySQL(t, h)
	}

	// Preserve an actual request-profile original across the control-only repair.
	if _, err := hosts[0].Execute(ctx, ExecuteRequest{RequestID: "repair-schema", SQL: "CREATE TABLE app(value TEXT)"}); err != nil {
		t.Fatal(err)
	}
	requestCtx, cancelRequest := context.WithCancel(ctx)
	var requestWG sync.WaitGroup
	t.Cleanup(func() {
		releaseArchive()
		cancelRequest()
		requestWG.Wait()
	})
	type execution struct {
		response ExecuteResponse
		err      error
	}
	executed := make(chan execution, 1)
	archiveArmed.Store(true)
	requestWG.Add(1)
	go func() {
		defer requestWG.Done()
		response, err := hosts[0].Execute(requestCtx, ExecuteRequest{RequestID: "repair-pending", SQL: "INSERT INTO app VALUES ('retained')"})
		executed <- execution{response: response, err: err}
	}()
	select {
	case <-archiveEntered:
	case result := <-executed:
		t.Fatalf("request completed before real archive gate: %+v %v", result.response, result.err)
	case <-ctx.Done():
		t.Fatal("real archive gate was not reached", ctx.Err())
	}
	var original materializer.RequestOriginal
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		status, statusErr := hosts[0].node.RequestStatusManaged(ctx, network.RequestStatusRequest{Kind: "sql", RequestID: "repair-pending"})
		if statusErr == nil && status.Receipt != nil && status.State == string(types.MutationCommitted) {
			var found bool
			original, found, statusErr = hosts[0].node.RetainedRequest(ctx, "sql", "repair-pending")
			if statusErr == nil && found && original.SQL != nil && original.Receipt == *status.Receipt {
				break
			}
		}
		select {
		case result := <-executed:
			t.Fatalf("request completed before actual committed original: %+v %v", result.response, result.err)
		case <-ctx.Done():
			t.Fatalf("actual committed original not observed: %v", ctx.Err())
		case <-tick.C:
		}
	}
	if original.SQL.RequestID != "repair-pending" || original.SQL.SQL != "INSERT INTO app VALUES ('retained')" {
		t.Fatalf("unexpected retained SQL original: %+v", original)
	}
	cancelRequest()
	var unknownResult execution
	select {
	case unknownResult = <-executed:
	case <-ctx.Done():
		t.Fatal("canceled SQL caller did not drain", ctx.Err())
	}
	var unknown *network.CommitUnknownError
	if !errors.As(unknownResult.err, &unknown) || !errors.Is(unknownResult.err, context.Canceled) || !errors.Is(unknownResult.err, network.ErrCommitUnknown) || unknown.RequestID != "repair-pending" || errors.Is(unknownResult.err, ErrSQLNotSubmitted) {
		t.Fatalf("real post-commit SQL cancellation did not preserve UNKNOWN: response=%+v err=%v", unknownResult.response, unknownResult.err)
	}
	releaseArchive()
	backgroundPaused(ctx, 3)
	beforeRestart, err := hosts[0].node.ManagedState()
	if err != nil {
		t.Fatal(err)
	}
	if err := hosts[0].Close(); err != nil {
		t.Fatal(err)
	}
	cold, err := seed.open(ctx, configs[0])
	if cold != nil {
		t.Cleanup(func() {
			if err := cold.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatalf("cold survivor with original pending SQL: %v", err)
	}
	hosts[0] = cold
	// Reopen need not start another worker of each kind. The installed hook
	// blocks any newly started publisher before its first publication.
	awaitPublicInternalReady(t, ctx, cold)
	afterRestart, err := cold.node.ManagedState()
	if err != nil || afterRestart.WALIdentity != beforeRestart.WALIdentity {
		t.Fatalf("cold restart changed voter WAL identity: %v", err)
	}
	afterRestartOriginal, found, err := cold.node.RetainedRequest(ctx, "sql", "repair-pending")
	if err != nil || !found || afterRestartOriginal.Slot != original.Slot || afterRestartOriginal.Fingerprint != original.Fingerprint || afterRestartOriginal.Receipt != original.Receipt || !bytes.Equal(afterRestartOriginal.ExecutionItem, original.ExecutionItem) {
		t.Fatalf("cold restart changed or lost the exact uncertain SQL original: found=%t err=%v", found, err)
	}

	coordination := make(chan error, 1)
	coordinatorCtx := context.WithValue(ctx, coordinatorStartupKey{}, true)
	go func() { coordination <- hosts[0].ReplaceVoter(coordinatorCtx, plan) }()
	var handoff operator.V1Alpha2RepairHandoff
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for handoff.Head == nil {
		live, loadErr := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
		if loadErr == nil {
			if repair, ok := live.Repair(); ok && repair.Plan.OperationID == plan.OperationID && repair.PreparedHandoff != nil {
				handoff = *repair.PreparedHandoff
				break
			}
		}
		select {
		case err := <-coordination:
			t.Fatalf("coordinator ended before publishing prepared handoff: %v", err)
		case <-deadline.C:
			t.Fatal("coordinator did not publish prepared handoff")
		case <-time.After(20 * time.Millisecond):
		}
	}
	listenerGateArmed.Store(true)
	originAdmission, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	originRepair, ok := originAdmission.Repair()
	if !ok || originRepair.State != "Closed" || originRepair.PreparedHandoff == nil || !reflect.DeepEqual(*originRepair.PreparedHandoff, handoff) {
		t.Fatalf("captured handoff is not the exact live closed repair: %+v", originRepair)
	}
	originBinding, err := operator.ValidateV1Alpha2RepairPodBinding(ctx, writer, configs[0].Ref, originRepair.Plan)
	if err != nil {
		t.Fatalf("capture live repair Pod owner chain: %v", err)
	}
	repairOrigin, err := json.Marshal(managedRepairOriginFile{Version: 1, Repair: originRepair, Binding: originBinding})
	if err != nil {
		t.Fatal(err)
	}

	learnerCfg := configs[0]
	learnerCfg.Store = writer
	learnerCfg.PodUID = plan.Replacement.PodUID
	learnerCfg.Runtime = node.CloneManagedRuntimeConfig(learnerCfg.Runtime)
	learnerCfg.Runtime.NodeID = plan.Replacement.NodeID
	learnerCfg.Runtime.PeerAddr, learnerCfg.Runtime.PeerToken = peerAddr, "replacement-test-token"
	learnerCfg.Runtime.Learner = &quepaxa.Member{ID: plan.Replacement.NodeID, PublicKey: plan.Replacement.PublicKey, PeerURL: plan.Replacement.PeerURL}
	learnerCfg.Runtime.DataDir = filepath.Join(t.TempDir(), "verified")
	learnerCfg.RepairOrigin = repairOrigin
	learnerNode := node.New(&learnerCfg.Runtime)
	physical, configDigest, err := learnerNode.PreparedStartupBindingDigestsForProtocol(protocol)
	if err != nil {
		t.Fatal(err)
	}
	identity := recovery.PreparedStartupIdentity{ClusterUID: learnerCfg.Ref.UID, Generation: learnerCfg.Plan.Generation, DeploymentUID: learnerCfg.Plan.DeploymentUID, PodUID: learnerCfg.PodUID, NodeID: string(learnerCfg.Runtime.NodeID), SourceBinding: physical, ConfigDigest: configDigest}
	bucket, err := objstore.NewBucketWithoutProvisioning(ctx, objstore.Config{Provider: objstore.ProviderS3, Endpoint: learnerCfg.Runtime.ObjStoreEndpoint, Bucket: learnerCfg.Runtime.ObjStoreBucket, Region: learnerCfg.Runtime.ObjStoreRegion, Insecure: true, AccessKey: learnerCfg.Runtime.ObjStoreAccessKey, SecretKey: learnerCfg.Runtime.ObjStoreSecretKey})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := bucket.Close(); err != nil {
			t.Error(err)
		}
	})
	trustedBootstrap := quepaxa.Cluster{ConfigID: 1, Members: learnerCfg.Runtime.Members}
	policy, window, err := types.ManagedApplicationPolicy(protocol)
	if err != nil {
		t.Fatal(err)
	}
	view, err := recovery.OpenReadOnlyViewAtHeadForApplicationProtocol(ctx, bucket, handoff.SourcePrefix, 1, trustedBootstrap, handoff.Head, policy, window, protocol, true)
	if err != nil {
		t.Fatal(err)
	}
	handoffDir := filepath.Join(t.TempDir(), "handoff")
	if err := recovery.PublishPreparedStartupForPolicy(ctx, view, handoffDir, identity, policy, window); err != nil {
		t.Fatal(err)
	}
	learnerCfg.Prepared, err = recovery.LoadPreparedStartupForApplicationProtocol(ctx, bucket, handoff.SourcePrefix, 1, trustedBootstrap, handoffDir, learnerCfg.Runtime.DataDir, identity, policy, window, protocol, true)
	if err != nil {
		t.Fatal(err)
	}
	badHandoffOrigin := originRepair
	badHandoff := *originRepair.PreparedHandoff
	badHandoff.PreparedHash = strings.Repeat("0", 64)
	badHandoffOrigin.PreparedHandoff = &badHandoff
	badHandoffBytes, err := json.Marshal(managedRepairOriginFile{Version: 1, Repair: badHandoffOrigin, Binding: originBinding})
	if err != nil {
		t.Fatal(err)
	}
	badHandoffCfg := learnerCfg
	badHandoffCfg.RepairOrigin = badHandoffBytes
	if badLearner, err := seed.open(ctx, badHandoffCfg); err == nil {
		_ = badLearner.Close()
		t.Fatal("prepared adoption accepted an origin whose prepared prefix differed from the live source")
	} else if !strings.Contains(err.Error(), "ADD-first prepared source differs from immutable repair origin") {
		t.Fatalf("prepared handoff mismatch failed at an unexpected boundary: %v", err)
	}
	readyBlocked, readyObserved, releaseReady, readyHooked := holdReplacementBeforeReady(t, ctx, learnerCfg.Runtime.NodeID)
	if freshCall {
		releaseReady()
	}
	learner, err := seed.open(ctx, learnerCfg)
	if learner != nil {
		t.Cleanup(func() {
			if err := learner.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	// Register after the learner Close cleanup so a failed assertion cannot
	// leave its catch-up worker blocked while Close waits to join it.
	t.Cleanup(releaseReady)
	if err != nil {
		t.Fatalf("open actual prepared learner: %v", err)
	}
	// Registration is published before the QUIC listener is opened. Keep only
	// the coordinator's pre-ADD authority GET behind this startup handshake;
	// main-thread origin/adoption GETs remain live and require no ADD to finish.
	registration, err := hosts[0].node.ManagedRepairSnapshot(ctx, plan.Replacement.NodeID, plan.Replacement.PublicKey)
	if err != nil || registration.LearnerWALIdentity == "" || uint64(registration.Current.ConfigID) != plan.ExpectedConfigID {
		t.Fatal("learner listener handshake lost exact pre-ADD registration", err)
	}
	learnerMember := *learnerCfg.Runtime.Learner
	learnerMember.WALIdentity = registration.LearnerWALIdentity
	prefixBytes, err := hex.DecodeString(handoff.PreparedHash)
	if err != nil || len(prefixBytes) != len(quepaxa.ValueHash{}) {
		t.Fatal("invalid actual prepared prefix encoding", err)
	}
	var preparedPrefix quepaxa.ValueHash
	copy(preparedPrefix[:], prefixBytes)
	listenerPeer, err := network.NewTransportForManagedProfile(configs[0].Runtime.ClusterID, configs[0].Runtime.NodeID, &registration.Current, configs[0].Runtime.PeerToken, network.ManagedPeerProfile{ApplicationProtocol: protocol, BootstrapPlanDigest: configs[0].Plan.BootstrapPlanDigest, Generation: configs[0].Plan.Generation})
	if err != nil {
		t.Fatal("create bound learner listener observer", err)
	}
	defer listenerPeer.Close()
	if err := listenerPeer.VerifyLearner(ctx, learnerMember, quepaxa.Slot(handoff.PreparedSlot), preparedPrefix); err != nil {
		t.Fatalf("learner listener handshake peer=%s endpoint=%s listener_open=true prefix_rpc_ok=false: %v", learnerMember.ID, learnerMember.PeerURL, err)
	}
	releaseListener()
	waitConfig2ReadIndex := func(committed quepaxa.Cluster) {
		t.Helper()
		readinessPeer, err := network.NewTransportForManagedProfile(configs[0].Runtime.ClusterID, configs[0].Runtime.NodeID, &committed, configs[0].Runtime.PeerToken, network.ManagedPeerProfile{ApplicationProtocol: protocol, BootstrapPlanDigest: configs[0].Plan.BootstrapPlanDigest, Generation: int64(configs[0].Plan.Generation)})
		if err != nil {
			t.Fatal("create bound config-2 readiness observer", err)
		}
		defer readinessPeer.Close()
		for _, id := range []quepaxa.NodeID{plan.Replacement.NodeID, configs[2].Runtime.NodeID} {
			var readinessErr error
			for {
				var tip quepaxa.Slot
				tip, readinessErr = readinessPeer.ReadTip(ctx, id)
				if readinessErr == nil {
					if uint64(tip) < handoff.PreparedSlot {
						t.Fatalf("config-2 voter ReadIndex regressed below the verified H: peer=%s tip=%d H=%d", id, tip, handoff.PreparedSlot)
					}
					break
				}
				if readyHooked && id == plan.Replacement.NodeID {
					t.Fatalf("replacement raw-ready was observed but actual config-2 ReadIndex failed: %v", readinessErr)
				}
				select {
				case <-ctx.Done():
					t.Fatalf("config-2 voter never reached actual ReadIndex readiness: peer=%s context=%v peer_error=%v", id, ctx.Err(), readinessErr)
				case <-tick.C:
				}
			}
		}
	}
	if freshCall {
		select {
		case <-freshAddProofEntered:
		case <-ctx.Done():
			t.Fatal("fresh ADD proof CAS was not reached", ctx.Err())
		}
		select {
		case <-readyObserved:
		case <-ctx.Done():
			t.Fatal("fresh D did not publish actual raw-ready observation", ctx.Err())
		}
		committed, err := hosts[0].node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
		if err != nil || uint64(committed.Current.ConfigID) != plan.ExpectedConfigID+1 {
			t.Fatal("fresh ADD did not establish exact C+1 before readiness proof", err)
		}
		waitConfig2ReadIndex(committed.Current)
		close(freshRemoveReady)
		select {
		case err := <-coordination:
			if err != nil {
				t.Fatalf("fresh single ReplaceVoter invocation did not complete actual ADD then REMOVE: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("fresh replacement coordinator did not finish", ctx.Err())
		}
	} else {
		select {
		case err := <-coordination:
			if err == nil || rejectAddRecord.Load() {
				t.Fatalf("expected injected ADD proof CAS failure after the real terminal: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("replacement coordinator timed out: %v", ctx.Err())
		}
		closedAfterADD, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
		if err != nil {
			t.Fatal(err)
		}
		closedRecord, ok := closedAfterADD.Repair()
		committed, snapshotErr := hosts[0].node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
		if !ok || closedRecord.State != "Closed" || closedRecord.AddCompletion != nil || snapshotErr != nil || uint64(committed.Current.ConfigID) != plan.ExpectedConfigID+1 {
			t.Fatalf("injected ADD proof CAS failure did not preserve the real unjournaled ADD terminal: state=%s config=%d err=%v", closedRecord.State, committed.Current.ConfigID, snapshotErr)
		}
		if readyHooked {
			select {
			case <-readyBlocked:
			case <-ctx.Done():
				t.Fatal("fresh D did not reach actual pre-ready hook", ctx.Err())
			}
			if learner.node.Ready() {
				t.Fatal("held fresh D became ready")
			}
			if err := hosts[0].ReplaceVoter(ctx, plan); !errors.Is(err, quepaxa.ErrQuorumUnavailable) {
				t.Fatalf("excluded-old fresh quorum must fail while D is held not ready: %v", err)
			}
			stillClosed, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
			if err != nil {
				t.Fatal(err)
			}
			stillRecord, exists := stillClosed.Repair()
			stillConfig, configErr := hosts[0].node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
			if !exists || stillRecord.State != "Closed" || !reflect.DeepEqual(stillRecord.Plan, plan) || !reflect.DeepEqual(stillRecord.PreparedHandoff, closedRecord.PreparedHandoff) || stillRecord.AddCompletion == nil || stillRecord.AddCompletion.PlanDigest != plan.Digest() || stillRecord.AddCompletion.ConfigID != plan.ExpectedConfigID+1 || configErr != nil || uint64(stillConfig.Current.ConfigID) != plan.ExpectedConfigID+1 || !rejectFinish.Load() {
				t.Fatalf("not-ready resume changed Closed/ADD proof/config2 or attempted REMOVE: record=%+v config=%d err=%v", stillRecord, stillConfig.Current.ConfigID, configErr)
			}
		}
		releaseReady()
		if readyHooked {
			select {
			case <-readyObserved:
			case <-ctx.Done():
				t.Fatal("fresh D did not publish actual raw-ready observation after release", ctx.Err())
			}
		}
		// Public Node.Ready also requires application admission, which remains
		// closed by this unfinished repair. The peer server instead uses the raw
		// catch-up readiness callback. Probe that actual authenticated ReadIndex
		// endpoint at the committed ADD configuration; ReplaceVoter below still
		// performs its own excluded-old quorum check before REMOVE.
		waitConfig2ReadIndex(committed.Current)
		if err := hosts[0].ReplaceVoter(ctx, plan); err == nil || rejectFinish.Load() {
			t.Fatalf("expected injected final completion failure after resume from actual ADD terminal: %v", err)
		}
		closedAfterADD, err = operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
		if err != nil {
			t.Fatal(err)
		}
		closedRecord, ok = closedAfterADD.Repair()
		committed, snapshotErr = hosts[0].node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
		if !ok || closedRecord.State != "Closed" || closedRecord.AddCompletion == nil || closedRecord.AddCompletion.ConfigID != plan.ExpectedConfigID+1 || snapshotErr != nil || uint64(committed.Current.ConfigID) != plan.ExpectedConfigID+2 {
			t.Fatalf("injection did not preserve Closed after actual ADD: state=%s config=%d err=%v", closedRecord.State, committed.Current.ConfigID, snapshotErr)
		}
		if err := hosts[0].ReplaceVoter(ctx, plan); err != nil {
			t.Fatalf("resume exact committed ADD without repeating REMOVE: %v", err)
		}
	}
	after, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	record, ok := after.Repair()
	if !ok || record.State != "Completed" || !reflect.DeepEqual(record.Plan, plan) || record.AddCompletion == nil || record.AddCompletion.ConfigID != plan.ExpectedConfigID+1 || record.Completion == nil || record.Completion.ConfigID != plan.ExpectedConfigID+2 || record.Completion.Slot <= record.AddCompletion.Slot {
		t.Fatalf("actual repair not completed: %+v", record)
	}
	afterOriginal, found, err := hosts[0].node.RetainedRequest(ctx, "sql", "repair-pending")
	if err != nil || !found || afterOriginal.Slot != original.Slot || afterOriginal.Fingerprint != original.Fingerprint || afterOriginal.Receipt != original.Receipt || !bytes.Equal(afterOriginal.ExecutionItem, original.ExecutionItem) {
		t.Fatalf("replacement changed uncertain SQL original: found=%t err=%v", found, err)
	}
	snapshot, err := hosts[0].node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil || uint64(snapshot.Current.ConfigID) != plan.ExpectedConfigID+2 {
		t.Fatalf("actual final voter configuration: %+v err=%v", snapshot.Current, err)
	}
	dSnapshot, err := learner.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil || dSnapshot.LocalWALIdentity == "" {
		t.Fatalf("read adopted D WAL identity: %+v err=%v", dSnapshot, err)
	}
	var verifyRetainedRestart func(*Host)
	recoverOriginal := func() {
		recovered, err := hosts[0].RecoverRequest(ctx, network.RequestStatusRequest{Kind: "sql", RequestID: "repair-pending"})
		if err != nil || recovered.RequestID == nil || string(recovered.RequestID) != "repair-pending" || recovered.SQL == nil || recovered.SQL.Slot != original.Slot {
			t.Fatalf("exact retained SQL original did not recover after repair: %+v err=%v", recovered, err)
		}
	}
	if beforeRetainedRestart != nil {
		// Finish the existing original before injecting the focused retained
		// recorder tail; no unrelated foreground barrier may drain that tail.
		recoverOriginal()
		verifyRetainedRestart = beforeRetainedRestart(ctx, learner, learnerCfg, hosts[0])
	}
	if err := learner.Close(); err != nil {
		t.Fatal(err)
	}
	completedWithoutOrigin := learnerCfg
	completedWithoutOrigin.RepairOrigin = nil
	if _, err := seed.open(ctx, completedWithoutOrigin); err == nil || !strings.Contains(err.Error(), "prepared repair learner requires the exact current closed repair") {
		t.Fatalf("completed D record accepted a fresh prepared adoption without its closed origin: %v", err)
	}
	if beforeRetainedRestart == nil {
		recoverOriginal()
	}
	historicalCfg := learnerCfg
	historicalCfg.Prepared = nil
	noAdoptionCfg := historicalCfg
	noAdoptionCfg.Runtime = node.CloneManagedRuntimeConfig(learnerCfg.Runtime)
	noAdoptionCfg.Runtime.DataDir = filepath.Join(t.TempDir(), "missing-adoption")
	noAdoption, noAdoptionErr := seed.open(ctx, noAdoptionCfg)
	if noAdoption != nil {
		if closeErr := noAdoption.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	}
	if noAdoptionErr == nil || !strings.Contains(noAdoptionErr.Error(), "prepared adoption is missing or invalid") {
		t.Fatalf("ADD-first restart without persisted H adoption was not rejected by Node preflight: %v", noAdoptionErr)
	}
	historical, err := seed.open(ctx, historicalCfg)
	if historical != nil {
		t.Cleanup(func() {
			if err := historical.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatalf("same-Pod D restart from immutable origin after completion: %v", err)
	}
	firstDRestart, err := historical.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil || firstDRestart.LocalWALIdentity != dSnapshot.LocalWALIdentity {
		t.Fatalf("same-Pod D restart changed its durable WAL nonce: before=%q after=%q err=%v", dSnapshot.LocalWALIdentity, firstDRestart.LocalWALIdentity, err)
	}
	awaitPublicInternalReady(t, ctx, historical)
	if verifyRetainedRestart != nil {
		verifyRetainedRestart(historical)
	}
	if _, err := historical.Execute(ctx, ExecuteRequest{RequestID: "after-d-restart", SQL: "INSERT INTO app VALUES ('after-d-restart')"}); err != nil {
		t.Fatalf("same-Pod D restart could not admit SQL while quorum was available: %v", err)
	}
	if err := historical.Close(); err != nil {
		t.Fatal(err)
	}

	// Roll the live repair record forward to E while retaining D's exact origin
	// and adopted WAL. D's next restart must recover locally under E's closed
	// reservation gate without making SQL writable.
	rolloverRuntime := node.CloneManagedRuntimeConfig(configs[0].Runtime)
	eNodeID, err := types.ManagedNodeID("replacement-e-uid")
	if err != nil {
		t.Fatal(err)
	}
	eToken := "replacement-e-token"
	eKey := quepaxa.PublicKey(network.PeerPublicKey(rolloverRuntime.ClusterID, eNodeID, eToken))
	eAddr := seed.reservePeer(t, eNodeID)
	ePeerURL := "quic://" + eAddr
	eIdentityMessage, _ := json.Marshal(map[string]any{"version": 1, "publicKey": eKey})
	baseTransport := writer.Kube.Client.Transport
	deploymentName, err := operator.V1Alpha2GenesisDeploymentName(configs[0].Ref.UID)
	if err != nil {
		t.Fatal(err)
	}
	ePodPath := "/api/v1/namespaces/" + configs[0].Ref.Namespace + "/pods/replacement-e"
	eRSPath := "/apis/apps/v1/namespaces/" + configs[0].Ref.Namespace + "/replicasets/replacement-e-rs"
	writer.Kube.Client.Transport = repairGateTransport(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Path {
		case ePodPath:
			return repairGateJSON(map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": "replacement-e", "namespace": configs[0].Ref.Namespace, "uid": "replacement-e-uid", "resourceVersion": "11", "ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "replacement-e-rs", "uid": "replacement-e-rs-uid", "controller": true}}}, "status": map[string]any{"podIP": "127.0.0.1", "initContainerStatuses": []any{map[string]any{"name": operator.V1Alpha2PrepareIdentityContainer, "state": map[string]any{"terminated": map[string]any{"exitCode": 0, "message": string(eIdentityMessage)}}}}, "containerStatuses": []any{map[string]any{"name": "app", "restartCount": 0, "state": map[string]any{"waiting": map[string]any{"reason": "ContainerCreating"}}}}}}), nil
		case eRSPath:
			return repairGateJSON(map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "metadata": map[string]any{"name": "replacement-e-rs", "namespace": configs[0].Ref.Namespace, "uid": "replacement-e-rs-uid", "resourceVersion": "12", "ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": deploymentName, "uid": configs[0].Plan.DeploymentUID, "controller": true}}}}), nil
		default:
			response, err := baseTransport.RoundTrip(request)
			if err != nil || request.URL.Path != "/api/v1/namespaces/"+configs[0].Ref.Namespace+"/pods/replacement" {
				return response, err
			}
			defer response.Body.Close()
			var pod map[string]any
			if err := json.NewDecoder(response.Body).Decode(&pod); err != nil {
				return nil, err
			}
			pod["status"].(map[string]any)["containerStatuses"] = []any{map[string]any{"name": "app", "restartCount": 1, "state": map[string]any{"running": map[string]any{"startedAt": "2026-10-30T00:00:00Z"}}}}
			return repairGateJSON(pod), nil
		}
	})
	current, err := hosts[0].node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil || current.Current.ConfigID != snapshot.Current.ConfigID {
		t.Fatalf("read current D membership before E close: %+v err=%v", current, err)
	}
	ePlan := plan
	ePlan.OperationID = "origin-rollover-e"
	ePlan.ExpectedConfigID = uint64(current.Current.ConfigID)
	ePlan.ExpectedAbortSlot = uint64(current.AbortSlot)
	ePlan.OldNodeID = plan.Replacement.NodeID
	ePlan.OldWALIdentity = dSnapshot.LocalWALIdentity
	ePlan.Replacement = operator.V1Alpha2RepairMember{PodName: "replacement-e", PodUID: "replacement-e-uid", NodeID: eNodeID, PublicKey: eKey, PeerURL: ePeerURL}
	ePlan.FinalVoterIDs = make([]types.NodeID, 0, len(current.Current.Members))
	for _, member := range current.Current.Members {
		if member.ID != ePlan.OldNodeID {
			ePlan.FinalVoterIDs = append(ePlan.FinalVoterIDs, member.ID)
		}
	}
	ePlan.FinalVoterIDs = append(ePlan.FinalVoterIDs, eNodeID)
	slices.Sort(ePlan.FinalVoterIDs)
	eAdmission, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	eClosed, observation, err := operator.BeginV1Alpha2Repair(ctx, writer, eAdmission, eAdmission.Authority(), ePlan)
	if err != nil || observation != operator.V1Alpha2CASApplied || !eClosed.SQLReservationsClosed() {
		t.Fatalf("roll current repair record from D to E: observation=%s err=%v", observation, err)
	}
	eClosedVersion := eClosed.ResourceVersion()
	historicalE, err := seed.open(ctx, historicalCfg)
	if historicalE != nil {
		t.Cleanup(func() {
			if err := historicalE.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatalf("same-Pod D restart with live E record: %v", err)
	}
	secondDRestart, err := historicalE.node.ManagedRepairSnapshot(ctx, "", quepaxa.PublicKey{})
	if err != nil || secondDRestart.LocalWALIdentity != dSnapshot.LocalWALIdentity {
		t.Fatalf("same-Pod D restart under E changed its durable WAL nonce: before=%q after=%q err=%v", dSnapshot.LocalWALIdentity, secondDRestart.LocalWALIdentity, err)
	}
	if err := historicalE.Ready(ctx); err == nil || !strings.Contains(err.Error(), "initial admission has pending control or closed repair") {
		t.Fatalf("same-Pod D restart under closed E repair was not held NotReady: %v", err)
	}
	if historicalE.node.Ready() {
		t.Fatal("same-Pod D restart became locally ready under the closed E repair")
	}
	if _, err := historicalE.Execute(ctx, ExecuteRequest{RequestID: "must-not-reserve-under-e", SQL: "INSERT INTO app VALUES ('blocked')"}); err == nil {
		t.Fatal("same-Pod D restart submitted a fresh SQL request under the closed E repair")
	} else {
		var notSubmitted *SQLNotSubmittedError
		if !errors.As(err, &notSubmitted) || notSubmitted.Stage != SQLStageAdmission || notSubmitted.RecoveryRequired || !strings.Contains(err.Error(), "closed repair") {
			t.Fatalf("closed E repair did not refuse fresh SQL at initial admission: %v", err)
		}
	}
	eAfter, err := operator.LoadV1Alpha2Admission(ctx, writer, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if eAfter.ResourceVersion() != eClosedVersion {
		t.Fatalf("rejected SQL changed status resourceVersion: before=%s after=%s", eClosedVersion, eAfter.ResourceVersion())
	}
	if _, pending := eAfter.Pending(); pending {
		t.Fatal("rejected SQL created a pending reservation")
	}
	if currentRepair, ok := eAfter.Repair(); !ok || currentRepair.State != "Closed" || !reflect.DeepEqual(currentRepair.Plan, ePlan) {
		t.Fatalf("D restart changed live E repair: %+v", currentRepair)
	}
}
