//go:build rhiza_local_testhooks

package managedsql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/quic-go/quic-go"
)

type graphNodeCrashInput struct {
	Runtime                    RuntimeConfig
	Plan                       operator.V1Alpha2ApplicationInitPlan
	Ref                        operator.V1Alpha2ClusterRef
	PodUID, KubeURL, TokenFile string
	Certificate                []byte
}

func TestManagedGraphNodeInstallCrashChild(t *testing.T) {
	path := os.Getenv("RHIZA_TEST_GRAPH_NODE_CRASH_INPUT")
	if path == "" {
		t.Skip("owned subprocess helper")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var input graphNodeCrashInput
	if err := json.Unmarshal(data, &input); err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(input.Certificate)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
	defer client.CloseIdleConnections()
	store := &operator.V1Alpha2StatusStore{Kube: &operator.Kubernetes{BaseURL: input.KubeURL, Namespace: input.Ref.Namespace, TokenFile: input.TokenFile, Client: client}}
	restore := localtesthooks.Set(func(event string) {
		if event == "materializer:restore:managed:after-staged" {
			os.Exit(73)
		}
	})
	defer restore()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	host, err := Open(ctx, Config{Runtime: input.Runtime, Store: store, Ref: input.Ref, Plan: input.Plan, PodUID: input.PodUID})
	if host != nil {
		_ = host.Close()
	}
	t.Fatalf("child did not reach retained-input crash boundary: %v", err)
}

func TestManagedGraphPublicNodeRecoversRetainedInstallAndLaterSuffix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	configs, fixture := sqlClusterForProtocol(t, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol)
	hosts := make([]*Host, 3)
	for i := range configs {
		configs[i].Runtime.CheckpointInterval = 200 * time.Millisecond
		host, err := fixture.open(ctx, configs[i])
		if host != nil {
			t.Cleanup(func() { _ = host.Close() })
		}
		if err != nil {
			t.Fatal(err)
		}
		hosts[i] = host
	}
	for _, host := range hosts {
		waitGraphHostPublicReady(t, ctx, host)
	}
	a, err := hosts[0].GraphExecute(ctx, types.GraphCommand{RequestID: "\xff-retained-A", Cypher: "CREATE (:RetainedInstall {name:'A'})"})
	if err != nil {
		t.Fatal("actual A ACK", err)
	}
	cfg := configs[0]
	bucket, err := objectstore.NewBucketForNamespace(ctx, objectstore.Config{Provider: objectstore.ProviderS3, Endpoint: cfg.Runtime.ObjStoreEndpoint, Bucket: cfg.Runtime.ObjStoreBucket, Region: cfg.Runtime.ObjStoreRegion, Insecure: true, AccessKey: cfg.Runtime.ObjStoreAccessKey, SecretKey: cfg.Runtime.ObjStoreSecretKey}, fixture.source.finalPrefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	cp, err := checkpoint.NewAppendOnlyManagerForPolicy(bucket, fixture.source.finalPrefix, t.TempDir(), sqlpolicy.ManagedGraphRequestMaterializationPolicy, 1)
	if err != nil {
		t.Fatal(err)
	}
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		locator, found, err := cp.LoadManagedGraphLocator(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if found && locator.Index >= a.Slot {
			break
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatal("product paired root unavailable", ctx.Err())
		}
	}
	response, err := cfg.Store.Kube.Client.Get(cfg.Store.Kube.BaseURL)
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := io.Copy(io.Discard, response.Body)
	if err := errors.Join(readErr, response.Body.Close()); err != nil {
		t.Fatal(err)
	}
	if response.TLS == nil || len(response.TLS.PeerCertificates) == 0 {
		t.Fatal("owned Kubernetes TLS certificate missing")
	}
	input := graphNodeCrashInput{Runtime: cfg.Runtime, Plan: cfg.Plan, Ref: cfg.Ref, PodUID: cfg.PodUID, KubeURL: cfg.Store.Kube.BaseURL, TokenFile: cfg.Store.Kube.TokenFile, Certificate: response.TLS.PeerCertificates[0].Raw}
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	inputPath := filepath.Join(t.TempDir(), "child.json")
	if err := os.WriteFile(inputPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := hosts[0].Close(); err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	peer := fixture.peers[cfg.Runtime.NodeID]
	fixture.mu.Unlock()
	if err := peer.close(); err != nil {
		t.Fatal(err)
	}
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestManagedGraphNodeInstallCrashChild$")
	child.Env = append(os.Environ(), "RHIZA_TEST_GRAPH_NODE_CRASH_INPUT="+inputPath)
	output, err := child.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 73 {
		t.Fatalf("actual child failed before expected exit73: error=%v output=%s", err, output)
	}
	t.Logf("actual Node installer child PID=%d reaped exit=73", child.Process.Pid)
	dbPath := filepath.Join(cfg.Runtime.DataDir, "sqlite.db")
	observed, err := materializer.InspectManagedGraphInstallTarget(ctx, dbPath, types.DefaultIdempotencyWindowSlots, cfg.Plan.ApplicationProtocol)
	if err != nil || observed.InstallAttempt == nil || len(observed.InstallAttempt.Files) != 2 {
		t.Fatalf("actual retained inputs unavailable: %v", err)
	}
	originalCut := observed.InstallAttempt.Cut
	for _, file := range observed.InstallAttempt.Files {
		data, err := os.ReadFile(file.Path)
		if err != nil || uint64(len(data)) != file.ExpectedLength || file.ExpectedSHA256 != fmtHash(data) {
			t.Fatal("retained actual source bytes differ", err)
		}
	}
	// A corrupt retained source is unknown, not permission to reconstruct the
	// journal at a newer cut. The actual public opener must preserve the attempt.
	journalPath := dbPath + ".local-restore.json"
	journalBefore, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	inputFile := observed.InstallAttempt.Files[1]
	inputBefore, err := os.ReadFile(inputFile.Path)
	if err != nil || len(inputBefore) == 0 {
		t.Fatal("retained input missing", err)
	}
	corrupt := append([]byte(nil), inputBefore...)
	corrupt[len(corrupt)-1] ^= 1
	if err := os.WriteFile(inputFile.Path, corrupt, 0600); err != nil {
		t.Fatal(err)
	}
	rejected, rejectErr := Open(ctx, cfg)
	if rejected != nil {
		_ = rejected.Close()
	}
	if rejectErr == nil {
		t.Fatal("public startup accepted unknown retained input bytes")
	}
	journalAfter, err := os.ReadFile(journalPath)
	if err != nil || !bytes.Equal(journalBefore, journalAfter) {
		t.Fatal("unknown source changed the original install journal", err)
	}
	stillCorrupt, err := os.ReadFile(inputFile.Path)
	if err != nil || !bytes.Equal(stillCorrupt, corrupt) {
		t.Fatal("unknown source was replaced or deleted", err)
	}
	// Restore only this test's exact original bytes before the positive case.
	if err := os.WriteFile(inputFile.Path, inputBefore, 0600); err != nil {
		t.Fatal(err)
	}
	b, err := hosts[1].GraphExecute(ctx, types.GraphCommand{RequestID: "\xfe-retained-B", Cypher: "CREATE (:RetainedInstall {name:'B'})"})
	if err != nil || b.Slot <= originalCut.CertifiedTip {
		t.Fatalf("actual later ACK missing: receipt=%+v error=%v", b, err)
	}
	conn, err := net.ListenPacket("udp", cfg.Runtime.PeerAddr)
	if err != nil {
		t.Fatal(err)
	}
	rebound := &sqlFixturePeer{conn: conn, transport: &quic.Transport{Conn: conn}}
	t.Cleanup(func() {
		if err := rebound.close(); err != nil {
			t.Error(err)
		}
	})
	fixture.mu.Lock()
	fixture.peers[cfg.Runtime.NodeID] = rebound
	fixture.mu.Unlock()
	reopened, err := fixture.open(ctx, cfg)
	if reopened != nil {
		t.Cleanup(func() { _ = reopened.Close() })
	}
	if err != nil {
		t.Fatal("actual public known-attempt Node reopen", err)
	}
	waitGraphHostPublicReady(t, ctx, reopened)
	for _, want := range []struct {
		id      string
		receipt types.MutationReceipt
	}{{"\xff-retained-A", a.MutationReceipt}, {"\xfe-retained-B", b.MutationReceipt}} {
		status, err := reopened.RequestStatus(ctx, network.RequestStatusRequest{Kind: "graph", RequestID: want.id})
		if err != nil || status.Receipt == nil || *status.Receipt != want.receipt {
			t.Fatalf("original retained/later receipt lost: %+v %v", status, err)
		}
	}
	rows, err := reopened.GraphQuery(ctx, network.GraphQueryRequest{Cypher: "MATCH (n:RetainedInstall) RETURN n.name", Consistency: "local"})
	if err != nil || len(rows.Rows) != 2 || rows.AppliedSlot < b.Slot {
		t.Fatal("known attempt/later suffix effects lost or duplicated", rows, err)
	}
	if err := materializer.ValidateNoLocalRestore(dbPath); err != nil {
		t.Fatal("known install did not finish cleanly", err)
	}
	if fixture.source.deleteRequests.Load() != 0 {
		t.Fatal("owned SDK wire observed provider Delete")
	}
	t.Logf("actual known cut T=%d followed by ACK=%d restored through=%d", originalCut.CertifiedTip, b.Slot, rows.AppliedSlot)
}

func fmtHash(data []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(data))
}
