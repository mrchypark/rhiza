package managedsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestActualManagedPeerLearnRequiresRuntimeBinding(t *testing.T) {
	for _, protocol := range []string{sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol} {
		t.Run(protocol, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			configs, fixture := sqlClusterForProtocol(t, protocol)
			var checkpointRootPUT <-chan string
			if protocol == sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol {
				// Hold every SQL candidate before it can certify a checkpoint seal.
				// BeforeAck archives and the Graph first-root publisher stay live.
				entered, release := make(chan string), make(chan struct{})
				rootPrefix := "/bucket/" + fixture.source.finalPrefix + "/checkpoint/roots/"
				fixture.source.putReady, fixture.source.putRelease = entered, release
				fixture.source.putPrefix, fixture.source.putSuffix = rootPrefix, ".json"
				fixture.source.putGate = func(_ context.Context, path string) bool {
					return strings.HasPrefix(path, rootPrefix)
				}
				checkpointRootPUT = entered
				// Hosts close and cancel the blocked requests before this cleanup.
				t.Cleanup(func() { close(release) })
			}
			var crGets atomic.Int64
			client := *configs[0].Store.Kube.Client
			base := client.Transport
			if base == nil {
				base = http.DefaultTransport
			}
			client.Transport = repairGateTransport(func(r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/rhizaclusters/") {
					crGets.Add(1)
				}
				return base.RoundTrip(r)
			})
			configs[0].Store.Kube.Client = &client
			hosts := make([]*Host, 2)
			for i := range hosts {
				h, err := fixture.open(ctx, configs[i])
				if h != nil {
					t.Cleanup(func() {
						if err := h.Close(); err != nil {
							t.Error(err)
						}
					})
				}
				if err != nil {
					t.Fatal(err)
				}
				hosts[i] = h
			}
			for _, h := range hosts {
				readySQL(t, h)
			}
			if _, err := hosts[0].Execute(ctx, ExecuteRequest{RequestID: "peer-learn-create", SQL: "CREATE TABLE peer_learn_test(value)"}); err != nil {
				t.Fatal(err)
			}
			cfg := configs[0].Runtime
			// Route only to live B. This does not change B's actual configuration
			// or the quorum certificate; it removes unrelated unopened-C dial errors.
			cluster := quepaxa.Cluster{ConfigID: 1, Members: append([]quepaxa.Member(nil), cfg.Members[:2]...)}
			profile := network.ManagedPeerProfile{ApplicationProtocol: protocol, BootstrapPlanDigest: configs[0].Plan.BootstrapPlanDigest, Generation: configs[0].Plan.Generation}
			transport, err := network.NewTransportForManagedProfile(cfg.ClusterID, cfg.NodeID, &cluster, cfg.PeerToken, profile)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := transport.Close(); err != nil {
					t.Error(err)
				}
			})
			page, err := transport.FetchDecisions(ctx, cfg.Members[1].ID, 1, 1)
			if err != nil || len(page.Decisions) != 1 {
				t.Fatalf("actual certified decision: %v", err)
			}
			value := page.Decisions[0]
			var decision quepaxa.Decision
			if err := json.Unmarshal(value.Certificate, &decision); err != nil {
				t.Fatal(err)
			}
			decision.Proposal.Value = value.Value
			if err := transport.SendDecision(ctx, decision); err != nil {
				t.Fatalf("live B rejected valid duplicate: %v", err)
			}
			readWAL := func() []qlog.Entry {
				t.Helper()
				wal, err := qlog.OpenReadOnly(filepath.Join(configs[1].Runtime.DataDir, "qlog"))
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := wal.Close(); err != nil {
						t.Error(err)
					}
				}()
				entries, err := wal.Read()
				if err != nil {
					t.Fatal(err)
				}
				return entries
			}
			readApplied := func() uint64 {
				t.Helper()
				dsn := (&url.URL{Scheme: "file", Path: filepath.ToSlash(filepath.Join(configs[1].Runtime.DataDir, "sqlite.db")), RawQuery: "mode=ro"}).String()
				db, err := sql.Open("sqlite3", dsn)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				var applied uint64
				if err := db.QueryRowContext(ctx, "SELECT value FROM _rhiza_meta WHERE key='applied_slot'").Scan(&applied); err != nil {
					t.Fatal(err)
				}
				return applied
			}
			if checkpointRootPUT != nil {
				select {
				case <-checkpointRootPUT:
				case <-ctx.Done():
					t.Fatal("SQL checkpoint root PUT never reached the gate", ctx.Err())
				}
			}
			if applied := readApplied(); applied < uint64(decision.Slot) {
				t.Fatalf("first Learned did not apply slot %d on B: applied=%d", decision.Slot, applied)
			}
			fixture.mu.Lock()
			saved := append(json.RawMessage(nil), fixture.obj["status"]...)
			fixture.mu.Unlock()
			defer func() { fixture.mu.Lock(); fixture.obj["status"] = saved; fixture.mu.Unlock() }()
			for _, missing := range []bool{false, true} {
				var status map[string]json.RawMessage
				if err := json.Unmarshal(saved, &status); err != nil {
					t.Fatal(err)
				}
				if missing {
					delete(status, "authority")
				} else {
					var authority map[string]json.RawMessage
					if err := json.Unmarshal(status["authority"], &authority); err != nil {
						t.Fatal(err)
					}
					authority["generation"] = json.RawMessage("999")
					status["authority"], _ = json.Marshal(authority)
				}
				fixture.mu.Lock()
				fixture.obj["status"], err = json.Marshal(status)
				fixture.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
				before, applied, gets := readWAL(), readApplied(), crGets.Load()
				if err := transport.SendDecision(ctx, decision); err != nil {
					t.Fatalf("CR drift rejected certified idempotent Learned: %v", err)
				}
				after, appliedAfter, getsAfter := readWAL(), readApplied(), crGets.Load()
				if !reflect.DeepEqual(before, after) {
					t.Errorf("duplicate Learned changed WAL (authority missing=%t): entries %d -> %d", missing, len(before), len(after))
				}
				if applied != appliedAfter {
					t.Errorf("duplicate Learned changed applied slot (authority missing=%t): %d -> %d", missing, applied, appliedAfter)
				}
				if gets != getsAfter {
					t.Errorf("duplicate Learned changed CR GET count (authority missing=%t): %d -> %d", missing, gets, getsAfter)
				}
			}
			fixture.mu.Lock()
			fixture.obj["status"] = saved
			fixture.mu.Unlock()
			if !localtesthooks.Enabled {
				return
			}

			// Hold one real borrower so shutdown ends the runtime binding/lifetime
			// while the authenticated peer listener is still reachable.
			type borrowerKey struct{}
			entered, release := make(chan struct{}), make(chan struct{})
			var enterOnce, releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			restore := localtesthooks.SetManagedRuntimeStateContext(func(observed context.Context) {
				if observed.Value(borrowerKey{}) != true {
					return
				}
				enterOnce.Do(func() { close(entered) })
				<-release
			})
			borrowDone := make(chan error, 1)
			go func() { borrowDone <- hosts[1].node.CheckManagedLocalRead(context.WithValue(ctx, borrowerKey{}, true)) }()
			closeDone := make(chan error, 1)
			var closeStarted bool
			t.Cleanup(func() {
				unblock()
				restore()
				select {
				case <-borrowDone:
				case <-time.After(5 * time.Second):
					t.Error("held runtime borrower did not join")
				}
				if closeStarted {
					select {
					case err := <-closeDone:
						if err != nil {
							t.Error(err)
						}
					case <-time.After(5 * time.Second):
						t.Error("owned close did not join")
					}
				}
			})
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("runtime borrower did not reach hook", ctx.Err())
			}
			before, applied := readWAL(), readApplied()
			closeStarted = true
			go func() { closeDone <- hosts[1].Close() }()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				_, err := hosts[1].node.ManagedState()
				if err != nil && err.Error() == "internal application binding unavailable" {
					break
				}
				select {
				case <-ticker.C:
				case <-ctx.Done():
					t.Fatal("runtime binding did not close", ctx.Err())
				}
			}
			err = transport.SendDecision(ctx, decision)
			var rpc interface {
				RPCDiagnostic() network.PeerRPCDiagnostic
			}
			// decodePeerResponse returns a rejection together with the wire
			// response; callConnection preserves its response-decode phase.
			if err == nil || !errors.As(err, &rpc) || rpc.RPCDiagnostic().Phase != "response-decode" || !strings.Contains(err.Error(), "peer rejected request: "+network.ErrNotReady.Error()) || ctx.Err() != nil {
				t.Fatalf("live peer did not refuse at the runtime readiness boundary: %v", err)
			}
			if !reflect.DeepEqual(before, readWAL()) || applied != readApplied() {
				t.Fatal("refused Learned changed WAL/application state")
			}
			unblock()
		})
	}
}
