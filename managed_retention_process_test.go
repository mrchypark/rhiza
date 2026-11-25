//go:build rhiza_local_testhooks

package rhiza_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/peertest"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/quic-go/quic-go"
)

type retentionProcessConfig struct {
	Runtime          types.ExecutionConfig
	Identity         node.ManagedStartupIdentity
	Ref              operator.V1Alpha2ClusterRef
	Plan             operator.V1Alpha2ApplicationInitPlan
	Root, API, Token string
	Certificate      []byte
}
type retentionProcessCommand struct {
	Kind string
	SQL  network.ExecuteRequest
}
type retentionProcessReply struct {
	Error          string
	Ready          bool
	CaughtUp       bool
	ReadinessError string
	Cycles         uint64
	Receipt        types.MutationReceipt
	Snapshot       node.ManagedKVSnapshot
	Status         network.RequestStatusResponse
	Barriers       node.SQLRetentionBarrierStatus
	Rows           [][]any
}

// Test-only subprocess. Each invocation owns exactly one real OpenManaged node.
func TestManagedRetentionProcessHelper(t *testing.T) {
	path := os.Getenv("RHIZA_TEST_RETENTION_PROCESS")
	if path == "" {
		return
	}
	localtesthooks.SetRetentionPaused(true)
	protocol := os.NewFile(uintptr(3), "retention-process-protocol")
	if protocol == nil {
		t.Fatal("missing retention process protocol pipe")
	}
	defer protocol.Close()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var c retentionProcessConfig
	if err = json.Unmarshal(data, &c); err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(c.Certificate) {
		t.Fatal("invalid fixture CA")
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}}
	store := &operator.V1Alpha2StatusStore{Kube: &operator.Kubernetes{BaseURL: c.API, Namespace: c.Ref.Namespace, TokenFile: c.Token, Client: client}}
	n := node.New(&c.Runtime)
	checker := &mixedProviderNodeChecker{n: n, store: store, ref: c.Ref, plan: c.Plan, identity: c.Identity}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, err := net.ListenPacket("udp", c.Runtime.PeerAddr)
	if err != nil {
		t.Fatal(err)
	}
	transport := &quic.Transport{Conn: conn}
	defer transport.Close()
	defer conn.Close()
	ctx = peertest.WithTransport(ctx, string(c.Runtime.ClusterID), string(c.Runtime.NodeID), c.Runtime.PeerAddr, transport)
	if c.Identity.Fresh {
		if _, err := n.PrepareManagedSQLSource(ctx, c.Identity, true); err != nil {
			t.Fatal(err)
		}
	}
	enc := json.NewEncoder(protocol)
	if err = n.OpenManaged(ctx, node.ManagedStartupConfig{StorageRoot: c.Root, PeerRecovery: true, Identity: c.Identity, Checker: checker, BootstrapMember: true}); err != nil {
		_ = enc.Encode(retentionProcessReply{Error: err.Error()})
		return
	}
	defer n.Shutdown()
	if err = enc.Encode(retentionProcessReply{}); err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bufio.NewReader(os.Stdin))
	for {
		var request retentionProcessCommand
		if err := dec.Decode(&request); err != nil {
			if err == io.EOF {
				return
			}
			t.Fatal(err)
		}
		var reply retentionProcessReply
		op, stop := context.WithTimeout(ctx, 20*time.Second)
		switch request.Kind {
		case "resume-retention":
			localtesthooks.SetRetentionPaused(false)
		case "pause-retention":
			localtesthooks.SetRetentionPaused(true)
		case "readiness":
			reply.CaughtUp = n.StartupCaughtUp()
			reply.Ready = n.Ready()
			if readinessErr := n.CheckManagedLocalRead(op); readinessErr != nil {
				reply.ReadinessError = readinessErr.Error()
			}
		case "inspect":
			reply.Snapshot, err = n.InspectManagedKV(op, true)
		case "inspect-local":
			reply.Snapshot, err = n.InspectManagedKVRead(op)
		case "admit":
			err = n.AdmitManaged(op)
		case "sql":
			var out network.ExecuteResponse
			out, err = n.ExecuteManagedSQL(op, request.SQL)
			reply.Receipt = out.MutationReceipt
		case "status":
			reply.Status, err = n.RequestStatusManaged(op, network.RequestStatusRequest{Kind: "sql", RequestID: request.SQL.RequestID})
		case "barriers":
			reply.Barriers = n.SQLRetentionBarriers()
			reply.Cycles = localtesthooks.RetentionCycles()
		case "query":
			var out network.QueryResponse
			out, err = n.QueryManagedSQL(op, network.QueryRequest{SQL: "SELECT count FROM retained", Consistency: "local"})
			reply.Rows = out.Rows
		case "local-query":
			var m *materializer.Materializer
			m, err = materializer.OpenManagedSQLKVWithAppendOnlyOriginals(filepath.Join(c.Runtime.DataDir, "sqlite.db"), 1, 1024)
			if err == nil {
				var out types.SQLStatementResult
				out, err = m.QueryResult(op, "SELECT count FROM retained", nil)
				reply.Rows = out.Rows
				err = errors.Join(err, m.Close())
			}
		case "local-receipt":
			var m *materializer.Materializer
			m, err = materializer.OpenManagedSQLKVWithAppendOnlyOriginals(filepath.Join(c.Runtime.DataDir, "sqlite.db"), 1, 1024)
			if err == nil {
				var found bool
				reply.Receipt, found, err = m.MutationReceipt(op, types.MutationSQL, request.SQL.RequestID)
				if err == nil && !found {
					err = fmt.Errorf("local receipt missing: %s", request.SQL.RequestID)
				}
				err = errors.Join(err, m.Close())
			}
		default:
			err = fmt.Errorf("unknown control command")
		}
		stop()
		if err != nil {
			reply.Error = err.Error()
		}
		err = nil
		if err = enc.Encode(reply); err != nil {
			return
		}
	}
}

type retentionProcess struct {
	cmd     *exec.Cmd
	input   io.WriteCloser
	output  io.ReadCloser
	decoder *json.Decoder
	logs    *bytes.Buffer
	exited  bool
}

func launchRetentionProcess(t *testing.T, c retentionProcessConfig) *retentionProcess {
	t.Helper()
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "startup.json")
	if err := os.WriteFile(file, data, 0600); err != nil {
		t.Fatal(err)
	}
	protocolRead, protocolWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	p := &retentionProcess{cmd: exec.Command(os.Args[0], "-test.run=^TestManagedRetentionProcessHelper$", "-test.timeout=8m"), output: protocolRead, logs: &bytes.Buffer{}}
	p.cmd.Env = append(os.Environ(), "RHIZA_TEST_RETENTION_1024=1", "RHIZA_TEST_RETENTION_PROCESS="+file)
	p.cmd.Stdout, p.cmd.Stderr = p.logs, p.logs
	p.cmd.ExtraFiles = []*os.File{protocolWrite}
	p.input, err = p.cmd.StdinPipe()
	if err != nil {
		_ = protocolRead.Close()
		_ = protocolWrite.Close()
		t.Fatal(err)
	}
	p.decoder = json.NewDecoder(protocolRead)
	if err := p.cmd.Start(); err != nil {
		_ = protocolRead.Close()
		_ = protocolWrite.Close()
		t.Fatal(err)
	}
	_ = protocolWrite.Close()
	t.Cleanup(func() { p.stop() })
	return p
}

func awaitRetentionProcessStartup(t *testing.T, p *retentionProcess) retentionProcessReply {
	t.Helper()
	var ready retentionProcessReply
	if err := p.decoder.Decode(&ready); err != nil {
		p.stop()
		t.Fatalf("child startup: %v; logs: %s", err, p.logs.String())
	}
	return ready
}

func startRetentionProcess(t *testing.T, c retentionProcessConfig) *retentionProcess {
	t.Helper()
	p := launchRetentionProcess(t, c)
	ready := awaitRetentionProcessStartup(t, p)
	if ready.Error != "" {
		p.stop()
		t.Fatalf("child startup: %s; logs: %s", ready.Error, p.logs.String())
	}
	return p
}

func readLocalRetentionState(t *testing.T, c retentionProcessConfig) (uint64, [][]any) {
	t.Helper()
	m, err := materializer.OpenManagedSQLKVWithAppendOnlyOriginals(filepath.Join(c.Runtime.DataDir, "sqlite.db"), 1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	result, queryErr := m.QueryResult(context.Background(), "SELECT count FROM retained", nil)
	tip := m.Tip()
	if err := errors.Join(queryErr, m.Close()); err != nil {
		t.Fatal(err)
	}
	return tip, result.Rows
}
func (p *retentionProcess) stop() {
	if p.exited {
		return
	}
	p.exited = true
	_ = p.input.Close()
	_ = p.output.Close()
	_ = p.cmd.Process.Kill()
	_ = p.cmd.Wait()
}
func (p *retentionProcess) kill(t *testing.T) {
	t.Helper()
	if p.exited {
		t.Fatal("process already reaped")
	}
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatalf("kill pid %d: %v", p.cmd.Process.Pid, err)
	}
	waitErr := p.cmd.Wait()
	p.exited = true
	_ = p.input.Close()
	_ = p.output.Close()
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) {
		t.Fatalf("wait pid %d after kill: %v", p.cmd.Process.Pid, waitErr)
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("wait pid %d status: %v", p.cmd.Process.Pid, exitErr)
	}
	t.Logf("killed pid=%d wait_error=%v signal=%v", p.cmd.Process.Pid, waitErr, status.Signal())
}
func (p *retentionProcess) call(t *testing.T, req retentionProcessCommand) retentionProcessReply {
	t.Helper()
	if err := json.NewEncoder(p.input).Encode(req); err != nil {
		t.Fatalf("child write: %v", err)
	}
	var reply retentionProcessReply
	if err := p.decoder.Decode(&reply); err != nil {
		t.Fatalf("child reply: %v", err)
	}
	return reply
}

// Uses 1024-slot/1ms test hooks but the real 256 MiB pool, three independent
// OpenManaged processes, authenticated loopback QUIC and isolated shared S3.
func TestManagedRetentionThreeProcessesRestart(t *testing.T) {
	provider := os.Getenv("RHIZA_EMBEDDED_PROVIDER")
	if provider != "s3" && provider != "gcs" && provider != "azure" {
		t.Skip("requires an isolated embedded object-store provider")
	}
	t.Setenv("RHIZA_TEST_RETENTION_1024", "1")
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	defer cancel()
	base := embeddedStoreConfig(t)
	base.Local, base.EnableReconfiguration = false, true
	base.AdminToken = "retention-test-admin"
	base.ObjStoreDurability = types.ObjectStoreDurabilityBeforeAck
	base.CheckpointInterval, base.CheckpointTailBytes = 24*time.Hour, 1<<40
	ref := operator.V1Alpha2ClusterRef{Name: "fixture", Namespace: "ns", UID: "retention-process-fixture"}
	cluster, err := types.ManagedClusterID(ref.UID, 0)
	if err != nil {
		t.Fatal(err)
	}
	base.ClusterID = string(cluster)
	const root = "retention-process-root"
	profile := operator.V1Alpha2RuntimeProfile{ApplicationProtocol: sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, EvidenceFormat: sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, InitImage: "fixture:v1", PeerPort: 9090, ProfileSecretName: "runtime", Storage: managedProviderRuntime(base)}
	profile.Storage.ObjStorePrefix = root
	entries := make([]operator.V1Alpha2BootstrapMember, 3)
	tokens := []string{"retention-one", "retention-two", "retention-three"}
	for i := range entries {
		pod := fmt.Sprintf("retention-pod-%d", i)
		id, err := types.ManagedNodeID(pod)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		endpoint := conn.LocalAddr().String()
		_ = conn.Close()
		entries[i] = operator.V1Alpha2BootstrapMember{PodUID: pod, Member: quepaxa.Member{ID: id, PeerURL: "quic://" + endpoint}}
		copy(entries[i].Member.PublicKey[:], network.PeerPublicKey(cluster, id, tokens[i]))
	}
	plan, bootstrap, err := operator.BuildV1Alpha2RuntimePlan(ref, "managed-provider-logical", "managed-provider-deployment", profile, entries)
	if err != nil {
		t.Fatal(err)
	}
	store, deployment := managedProviderStatusStore(t, ref, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol)
	if _, err = operator.InitializeV1Alpha2AuthorityWithBootstrap(ctx, store, ref, deployment, plan, bootstrap); err != nil {
		t.Fatal(err)
	}
	ns, err := node.ManagedNamespaceFor(root, ref.UID, 0, plan.AssignmentID, string(cluster))
	if err != nil {
		t.Fatal(err)
	}
	base.ObjStorePrefix = ns.ConfigPrefix
	base.Members = make([]quepaxa.Member, 3)
	for i := range entries {
		base.Members[i] = entries[i].Member
	}
	// Capture the exact loopback httptest certificate, not an unverified client.
	u := strings.TrimPrefix(store.Kube.BaseURL, "https://")
	tlsConn, err := tls.Dial("tcp", u, &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: tlsConn.ConnectionState().PeerCertificates[0].Raw})
	_ = tlsConn.Close()
	configs := make([]retentionProcessConfig, 3)
	children := make([]*retentionProcess, 3)
	for i := range entries {
		runtime := managedProviderRuntime(base)
		runtime.NodeID, runtime.PeerToken = entries[i].Member.ID, tokens[i]
		runtime.PeerAddr, runtime.BindAddr = strings.TrimPrefix(entries[i].Member.PeerURL, "quic://"), ""
		runtime.DataDir = filepath.Join(t.TempDir(), "data")
		probe := node.New(&runtime)
		physical, member, err := probe.PreparedStartupBindingDigestsForProtocol(plan.ApplicationProtocol)
		if err != nil {
			t.Fatal(err)
		}
		identity := node.ManagedStartupIdentity{ApplicationProtocol: plan.ApplicationProtocol, EvidenceFormat: plan.EvidenceFormat, AssignmentID: plan.AssignmentID, IntentHash: plan.IntentHash, StorageID: plan.StorageID, PhysicalBinding: physical, BootstrapPlanDigest: plan.BootstrapPlanDigest, Fresh: true, Member: recovery.PreparedStartupIdentity{ClusterUID: ref.UID, DeploymentUID: plan.DeploymentUID, PodUID: entries[i].PodUID, NodeID: string(runtime.NodeID), SourceBinding: physical, ConfigDigest: member}}
		configs[i] = retentionProcessConfig{Runtime: runtime, Identity: identity, Ref: ref, Plan: plan, Root: root, API: store.Kube.BaseURL, Token: store.Kube.TokenFile, Certificate: cert}
		children[i] = startRetentionProcess(t, configs[i])
	}
	initial, err := operator.LoadV1Alpha2ApplicationInit(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	snap := children[0].call(t, retentionProcessCommand{Kind: "inspect"})
	if snap.Error != "" {
		t.Fatal(snap.Error)
	}
	if _, obs, err := operator.CommitV1Alpha2ApplicationInit(ctx, store, initial, plan, mixedProviderEvidence(snap.Snapshot)); err != nil || obs != operator.V1Alpha2CASApplied {
		t.Fatal(obs, err)
	}
	for _, child := range children {
		for {
			result := child.call(t, retentionProcessCommand{Kind: "admit"})
			if result.Error == "" {
				break
			}
			if ctx.Err() != nil {
				t.Fatal(result.Error)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	execSQL := func(req network.ExecuteRequest) types.MutationReceipt {
		t.Helper()
		reply := children[0].call(t, retentionProcessCommand{Kind: "sql", SQL: req})
		if reply.Error != "" {
			probe := children[0].call(t, retentionProcessCommand{Kind: "readiness"})
			t.Fatalf("%s: %s; subsequent readiness=%+v", req.RequestID, reply.Error, probe)
		}
		return reply.Receipt
	}
	for {
		probe := children[0].call(t, retentionProcessCommand{Kind: "readiness"})
		if probe.CaughtUp && probe.Ready && probe.ReadinessError == "" {
			t.Logf("setup readiness observed: caught_up=%t ready=%t", probe.CaughtUp, probe.Ready)
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("setup readiness: %+v", probe)
		}
		time.Sleep(100 * time.Millisecond)
	}
	stableInspection := func(child *retentionProcess) retentionProcessReply {
		t.Helper()
		var lastError string
		for {
			reply := child.call(t, retentionProcessCommand{Kind: "inspect-local"})
			if reply.Error == "" {
				return reply
			}
			if lastError != reply.Error {
				t.Logf("stable inspection pending: %s", reply.Error)
				lastError = reply.Error
			}
			if ctx.Err() != nil {
				t.Fatalf("stable inspection: %s", lastError)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	for _, req := range []network.ExecuteRequest{{RequestID: "setup", SQL: "CREATE TABLE retained (count INTEGER NOT NULL, payload BLOB NOT NULL)"}, {RequestID: "insert", SQL: "INSERT INTO retained VALUES (0, zeroblob(?))", Args: []any{materializer.MaxMutationResultBytes - 56}}} {
		if got := execSQL(req); got.Status != types.MutationCommitted {
			t.Fatal(got)
		}
	}
	request := func(id string) network.ExecuteRequest {
		return network.ExecuteRequest{RequestID: id, SQL: "UPDATE retained SET count=count+1, payload=payload RETURNING payload", WantRows: true}
	}
	for i := 0; i < 256; i++ {
		if got := execSQL(request(fmt.Sprintf("full-%d", i))); got.Status != types.MutationCommitted {
			t.Fatal(i, got)
		}
	}
	failed := execSQL(request("failed-capacity"))
	if failed.Status != types.MutationRejected || failed.ErrorCode != types.MutationErrorCodeResultRetentionFull {
		t.Fatal(failed)
	}
	beforeLoss := stableInspection(children[0])
	if err := children[1].cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	defer children[1].cmd.Process.Signal(syscall.SIGCONT)
	if err := children[2].cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	defer children[2].cmd.Process.Signal(syscall.SIGCONT)
	activeBefore := children[0].call(t, retentionProcessCommand{Kind: "barriers"}).Barriers
	children[0].call(t, retentionProcessCommand{Kind: "resume-retention"})
	activeDeadline := time.NewTimer(12 * time.Second)
	defer activeDeadline.Stop()
	activeTicker := time.NewTicker(25 * time.Millisecond)
	defer activeTicker.Stop()
	for {
		status := children[0].call(t, retentionProcessCommand{Kind: "barriers"}).Barriers
		if status.Completed != activeBefore.Completed {
			t.Fatalf("active voter completed barrier without quorum: before=%+v after=%+v", activeBefore, status)
		}
		if status.QuorumCheckPending && status.Errors > activeBefore.Errors {
			t.Logf("active voter observed quorum failure: %+v", status)
			break
		}
		select {
		case <-activeDeadline.C:
			t.Fatalf("active voter quorum failure not observed: %+v", status)
		case <-activeTicker.C:
		}
	}
	children[0].call(t, retentionProcessCommand{Kind: "pause-retention"})
	withoutQuorum := stableInspection(children[0])
	if withoutQuorum.Snapshot.Runtime.AppliedTip != beforeLoss.Snapshot.Runtime.AppliedTip || withoutQuorum.Snapshot.Runtime.CertifiedTip != beforeLoss.Snapshot.Runtime.CertifiedTip {
		t.Fatalf("active voter advanced without quorum: before=%+v after=%+v", beforeLoss.Snapshot.Runtime, withoutQuorum.Snapshot.Runtime)
	}
	if err := children[1].cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	for {
		if result := children[0].call(t, retentionProcessCommand{Kind: "admit"}); result.Error == "" {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("quorum restoration failed")
		}
		time.Sleep(100 * time.Millisecond)
	}
	beforeKill := stableInspection(children[0])
	if beforeKill.Snapshot.Runtime.RecorderTip != beforeKill.Snapshot.Runtime.CertifiedTip {
		t.Fatalf("kill point has unresolved recorder: %+v", beforeKill.Snapshot.Runtime)
	}
	if err := children[1].cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	if err := children[0].cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	children[0].kill(t)
	localTip, directRows := readLocalRetentionState(t, configs[0])
	if localTip != beforeKill.Snapshot.Runtime.AppliedTip || len(directRows) != 1 || directRows[0][0] != int64(256) {
		t.Fatalf("killed voter local tip=%d rows=%v; before=%+v", localTip, directRows, beforeKill.Snapshot.Runtime)
	}
	configs[0].Identity.Fresh = false
	children[0] = launchRetentionProcess(t, configs[0])
	type startupResult struct {
		reply retentionProcessReply
		err   error
	}
	started := make(chan startupResult, 1)
	go func() {
		var reply retentionProcessReply
		err := children[0].decoder.Decode(&reply)
		started <- startupResult{reply, err}
	}()
	select {
	case ready := <-started:
		if ready.err != nil || ready.reply.Error != "" {
			t.Fatalf("quorumless restart failed: %+v err=%v", ready.reply, ready.err)
		}
	case <-time.After(25 * time.Second):
		t.Fatal("quorumless restart did not reach open state")
	}
	restarted := stableInspection(children[0])
	if restarted.Snapshot.Runtime.AppliedTip < localTip {
		t.Fatalf("restarted applied tip=%d before quorumless local tip=%d", restarted.Snapshot.Runtime.AppliedTip, localTip)
	}
	localRows := children[0].call(t, retentionProcessCommand{Kind: "local-query"})
	if localRows.Error != "" || len(localRows.Rows) != 1 || localRows.Rows[0][0] != float64(256) {
		t.Fatalf("restarted local rows=%v err=%s", localRows.Rows, localRows.Error)
	}
	workerProbe := children[0].call(t, retentionProcessCommand{Kind: "barriers"})
	workerBefore := workerProbe.Barriers
	t.Logf("restarted voter: local_tip=%d node_applied_tip=%d rows=%v worker_before=%+v", localTip, restarted.Snapshot.Runtime.AppliedTip, localRows.Rows, workerBefore)
	if reply := children[0].call(t, retentionProcessCommand{Kind: "resume-retention"}); reply.Error != "" {
		t.Fatal(reply.Error)
	}
	noQuorumDeadline := time.NewTimer(8 * time.Second)
	defer noQuorumDeadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		status := children[0].call(t, retentionProcessCommand{Kind: "barriers"}).Barriers
		if status.Completed != workerBefore.Completed {
			t.Fatalf("barrier completed without quorum: before=%+v after=%+v", workerBefore, status)
		}
		if observed := children[0].call(t, retentionProcessCommand{Kind: "barriers"}); observed.Cycles > workerProbe.Cycles {
			ready := children[0].call(t, retentionProcessCommand{Kind: "readiness"})
			if ready.Ready || observed.Barriers.Attempts != workerBefore.Attempts {
				t.Fatalf("restarted voter proposed before admission: ready=%+v worker=%+v", ready, observed)
			}
			t.Logf("restarted voter cycle blocked before admission: ready=%+v worker=%+v", ready, observed)
			break
		}
		select {
		case <-noQuorumDeadline.C:
			t.Fatalf("restarted voter cycle not observed: %+v", status)
		case <-ticker.C:
		}
	}
	quorumless := stableInspection(children[0])
	if quorumless.Snapshot.Runtime.AppliedTip != restarted.Snapshot.Runtime.AppliedTip || quorumless.Snapshot.Runtime.CertifiedTip != restarted.Snapshot.Runtime.CertifiedTip {
		t.Fatalf("history advanced without quorum: before=%+v after=%+v", restarted.Snapshot.Runtime, quorumless.Snapshot.Runtime)
	}
	localReceipt := children[0].call(t, retentionProcessCommand{Kind: "local-receipt", SQL: request("failed-capacity")})
	if localReceipt.Error != "" || localReceipt.Receipt != failed {
		t.Fatalf("quorumless receipt changed: got=%+v want=%+v error=%s", localReceipt.Receipt, failed, localReceipt.Error)
	}
	if err := children[1].cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	for {
		result := children[0].call(t, retentionProcessCommand{Kind: "admit"})
		if result.Error == "" {
			break
		}
		if ctx.Err() != nil {
			t.Fatal(result.Error)
		}
		time.Sleep(100 * time.Millisecond)
	}
	for {
		time.Sleep(100 * time.Millisecond)
		status := children[0].call(t, retentionProcessCommand{Kind: "barriers"})
		if status.Barriers.LastTargetSlot == 0 && status.Barriers.Completed > workerBefore.Completed && status.Barriers.Attempts > workerBefore.Attempts {
			t.Logf("restarted worker recovered: before=%+v after=%+v", workerBefore, status.Barriers)
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("restarted barrier recovery: %+v", status)
		}
	}
	if err := children[2].cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	if got := execSQL(request("failed-capacity")); got != failed {
		t.Fatalf("old failed request changed: %+v -> %+v", failed, got)
	}
	recovered := execSQL(request("after-recovery"))
	if recovered.Status != types.MutationCommitted {
		t.Fatal(recovered)
	}
	for id, want := range map[string]types.MutationReceipt{"failed-capacity": failed, "after-recovery": recovered} {
		status := children[0].call(t, retentionProcessCommand{Kind: "status", SQL: request(id)})
		if status.Error != "" || status.Status.Receipt == nil || *status.Status.Receipt != want {
			t.Fatalf("restart receipt %s: got=%+v want=%+v err=%s", id, status.Status.Receipt, want, status.Error)
		}
	}
	if replay := execSQL(request("after-recovery")); replay != recovered {
		t.Fatalf("ACK replay changed: got=%+v want=%+v", replay, recovered)
	}
	result := children[0].call(t, retentionProcessCommand{Kind: "query"})
	if result.Error != "" || len(result.Rows) != 1 || result.Rows[0][0] != float64(257) {
		t.Fatalf("ACK rows=%v err=%s", result.Rows, result.Error)
	}
	t.Logf("provider=%s completed; failed_receipt=%+v recovered_receipt=%+v", provider, failed, recovered)
}
