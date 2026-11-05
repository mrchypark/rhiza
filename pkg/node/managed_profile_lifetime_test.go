package node

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/peertest"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/quic-go/quic-go"
)

// This checker accepts only the fixture's exact startup identity and never
// authorizes application admission/writes. Its second existing startup callback
// occurs after config publication, before writable voter registration/peers.
type managedProfileLifetimeChecker struct {
	n            *Node
	want         ManagedStartupIdentity
	policy       sqlpolicy.MaterializationPolicy
	startupCalls atomic.Int32
	stopOnce     sync.Once
	stopRead     chan struct{}
	readDone     chan error
}

func (c *managedProfileLifetimeChecker) ValidateManagedStartup(ctx context.Context, identity ManagedStartupIdentity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if identity != c.want {
		return errors.New("unexpected fixture startup identity")
	}
	if c.startupCalls.Add(1) != 2 {
		return nil
	}
	ready := make(chan struct{})
	go func() {
		var nonce [types.ReadBarrierNonceSize]byte
		value := types.EncodeReadBarrier(nonce)
		close(ready)
		for {
			select {
			case <-c.stopRead:
				c.readDone <- nil
				return
			default:
			}
			if got := c.n.materializationPolicy(); got != c.policy {
				c.readDone <- fmt.Errorf("policy changed: %v", got)
				return
			}
			if got := c.n.receiptWindow(); got != types.DefaultIdempotencyWindowSlots {
				c.readDone <- fmt.Errorf("receipt window changed: %v", got)
				return
			}
			if err := c.n.validateExecutionPolicy(value); err != nil {
				c.readDone <- err
				return
			}
			runtime.Gosched()
		}
	}()
	<-ready
	// Let the owned reader enter before the actual public opener continues.
	// No sleeps, replacement pointer writes or production hooks are used.
	runtime.Gosched()
	return nil
}

func (c *managedProfileLifetimeChecker) ValidateManagedAdmission(context.Context, ManagedStartupIdentity, ManagedRuntimeState) error {
	return errors.New("application admission intentionally closed")
}

func (c *managedProfileLifetimeChecker) stop() error {
	c.stopOnce.Do(func() { close(c.stopRead) })
	if c.startupCalls.Load() < 2 {
		return errors.New("actual post-publication startup callback not reached")
	}
	return <-c.readDone
}

func TestManagedProfileStableAcrossPublicOpen(t *testing.T) {
	for _, protocol := range []string{sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol} {
		t.Run(protocol, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			store := &managedTestObjectStore{objects: make(map[string][]byte)}
			server := httptest.NewServer(http.HandlerFunc(store.serve))
			defer server.Close()
			conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			transport := &quic.Transport{Conn: conn}
			defer transport.Close()
			other, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			namespace, err := ManagedNamespaceFor("profile-lifetime", "uid", 0, "assignment", "cluster")
			if err != nil {
				t.Fatal(err)
			}
			config := &types.ExecutionConfig{NodeID: "member", ClusterID: "cluster", DataDir: filepath.Join(t.TempDir(), "node"), PeerAddr: conn.LocalAddr().String(), PeerToken: "peer", AdminToken: "admin", ObjStoreProvider: "s3", ObjStoreBucket: "test", ObjStoreEndpoint: strings.TrimPrefix(server.URL, "http://"), ObjStoreInsecure: true, ObjStoreAccessKey: "fixture", ObjStoreSecretKey: "fixture", ObjStoreRegion: "us-east-1", ObjStorePrefix: namespace.ConfigPrefix, ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck, EnableReconfiguration: true, Members: []types.NodeConfig{peerMember("cluster", "member", "peer"), peerMember("cluster", "other", "other")}}
			config.Members[0].PeerURL = "quic://" + conn.LocalAddr().String()
			config.Members[1].PeerURL = "quic://" + other.LocalAddr().String()
			if isManagedAppendOnlyProtocol(protocol) {
				for i := range config.Members {
					config.Members[i].WALIdentity = fmt.Sprintf("%064x", i+1)
				}
			}
			n := New(config)
			defer func() {
				if err := n.Shutdown(); err != nil {
					t.Error(err)
				}
			}()
			physical, member, err := n.PreparedStartupBindingDigestsForProtocol(protocol)
			if err != nil {
				t.Fatal(err)
			}
			format := "sql-v1"
			if protocol != "" {
				format = protocol
			}
			common, err := n.ManagedBootstrapPlanDigestForProtocol(physical, format, protocol)
			if err != nil {
				t.Fatal(err)
			}
			identity := ManagedStartupIdentity{ApplicationProtocol: protocol, EvidenceFormat: format, Member: recovery.PreparedStartupIdentity{ClusterUID: "uid", DeploymentUID: "deployment", PodUID: "pod", NodeID: "member", SourceBinding: physical, ConfigDigest: member}, AssignmentID: "assignment", IntentHash: "intent", StorageID: "storage", PhysicalBinding: physical, BootstrapPlanDigest: common, Fresh: true}
			if isManagedAppendOnlyProtocol(protocol) {
				localNonce, err := hex.DecodeString(config.Members[0].WALIdentity)
				if err != nil {
					t.Fatal(err)
				}
				wal, err := qlog.Open(filepath.Join(config.DataDir, "qlog"))
				if err != nil {
					t.Fatal(err)
				}
				if err := wal.BindIdentity(localNonce); err != nil {
					_ = wal.Close()
					t.Fatal(err)
				}
				if err := wal.Close(); err != nil {
					t.Fatal(err)
				}
				memberDigest, err := types.ManagedMemberConfigDigest(*config, protocol)
				if err != nil {
					t.Fatal(err)
				}
				profile := &managedVoterProfileBinding{ApplicationProtocol: protocol, BootstrapPlanDigest: common, MemberConfigDigest: memberDigest, Generation: 0}
				localIdentity := voterIdentityForProfile(config, profile)
				localIdentity.Nonce = config.Members[0].WALIdentity
				if err := writeVoterIdentity(config.DataDir, localIdentity); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := n.PrepareManagedSQLSource(ctx, identity, true); err != nil {
				t.Fatal(err)
			}
			policy, _, err := types.ManagedApplicationPolicy(protocol)
			if err != nil {
				t.Fatal(err)
			}
			checker := &managedProfileLifetimeChecker{n: n, want: identity, policy: policy, stopRead: make(chan struct{}), readDone: make(chan error, 1)}
			ctx = peertest.WithTransport(ctx, "cluster", "member", conn.LocalAddr().String(), transport)
			openErr := n.OpenManaged(ctx, ManagedStartupConfig{StorageRoot: "profile-lifetime", Identity: identity, Checker: checker, BootstrapMember: true})
			readErr := checker.stop()
			if openErr != nil {
				t.Fatal("actual public OpenManaged", openErr)
			}
			if readErr != nil {
				t.Fatal(readErr)
			}
			if !n.managedPeerVerified.Load() || n.managedStartup.Identity != identity || n.materializationPolicy() != policy {
				t.Fatal("public startup did not retain exact verified profile")
			}
		})
	}
}
