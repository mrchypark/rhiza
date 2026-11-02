package node

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	sqlite3 "github.com/ncruces/go-sqlite3"
)

func TestOperationSyncDiagnosticsTypedLocalCauses(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "typed.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	_, sqlErr := db.Exec(`CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES(1); INSERT INTO t VALUES(1)`)
	var sqliteErr *sqlite3.Error
	if !errors.As(sqlErr, &sqliteErr) {
		t.Fatalf("not an actual SQLite error: %v", sqlErr)
	}
	_, fileErr := os.Open(filepath.Join(t.TempDir(), "secret-filename"))
	if fileErr == nil {
		t.Fatal("missing file unexpectedly opened")
	}
	for _, item := range []struct {
		cause error
		class string
	}{{sqlErr, "sqlite"}, {fileErr, "filesystem"}} {
		wrapped := &catchUpDiagnosticError{stage: "apply", from: 4, cause: fmt.Errorf("SQL secret credential: %w", item.cause)}
		var line string
		logOperationSyncFailure("node", 0, []catchUpAttempt{{source: "peer", err: wrapped}}, func(args ...any) { line = fmt.Sprint(args...) })
		if !strings.Contains(line, "error_class="+item.class) {
			t.Fatalf("typed local cause collapsed: %s", line)
		}
		code, extended, known, op := catchUpCauseCode(wrapped)
		if !known || code == 0 {
			t.Fatal("typed numeric error missing")
		}
		if item.class == "sqlite" && (code != int(sqliteErr.Code()) || extended != int(sqliteErr.ExtendedCode())) {
			t.Fatal("SQLite result code lost")
		}
		if item.class == "filesystem" && op != "open" {
			t.Fatal("file operation lost")
		}
		if strings.Contains(line, "secret") || strings.Contains(line, "credential") || strings.Contains(line, "CREATE TABLE") {
			t.Fatal("raw cause or path leaked")
		}
		if !errors.Is(wrapped, item.cause) {
			t.Fatal("typed local cause unwrap changed")
		}
	}
}

type catchUpRPCFixture struct{ cause error }

func (e catchUpRPCFixture) Error() string { return "SQL secret credential" }
func (e catchUpRPCFixture) Unwrap() error { return e.cause }
func (e catchUpRPCFixture) RPCDiagnostic() network.PeerRPCDiagnostic {
	return network.PeerRPCDiagnostic{Phase: "dial", Elapsed: time.Second, GateWait: 700 * time.Millisecond, MutexWait: 200 * time.Millisecond, DialElapsed: 100 * time.Millisecond, DialRemaining: 50 * time.Millisecond, DialDeadlineKnown: true, GateWaitKnown: true, MutexWaitKnown: true, DialMeasured: true}
}

func TestOperationSyncDiagnosticsTransportTiming(t *testing.T) {
	err := &catchUpDiagnosticError{stage: "fetch", from: 11, cause: catchUpRPCFixture{context.DeadlineExceeded}}
	logOperationSyncFailure("local", 3, []catchUpAttempt{{source: "peer", err: err}}, func(args ...any) {
		line := fmt.Sprint(args...)
		for _, field := range []string{"stage=fetch from=11 remote_tip_known=false", "rpc_phase=dial rpc_elapsed_us=1000000", "gate_wait_known=true gate_wait_us=700000 mutex_wait_known=true mutex_wait_us=200000 dial_measured=true dial_us=100000", "dial_deadline_known=true dial_remaining_us=50000"} {
			if !strings.Contains(line, field) {
				t.Fatalf("transport timing field lost: %s", field)
			}
		}
		if strings.Contains(line, "secret") || strings.Contains(line, "credential") {
			t.Fatal("diagnostic printed arbitrary RPC cause")
		}
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("layered diagnostics lost original cause")
	}
}

func TestOperationSyncDiagnosticsAllFailuresAndOrder(t *testing.T) {
	alive := errors.New("SQL secret-value credential")
	for _, order := range [][]quepaxa.NodeID{{"dead", "alive"}, {"alive", "dead"}} {
		var called []quepaxa.NodeID
		tip := uint64(8)
		attempts, err := runOperationSyncRound(order, func(source quepaxa.NodeID) error {
			called = append(called, source)
			if source == "dead" {
				return &catchUpDiagnosticError{stage: "fetch", from: quepaxa.Slot(tip + 1), cause: context.DeadlineExceeded}
			}
			from := tip + 1
			tip += 2 // One source may make partial progress before a later page fails.
			return &catchUpDiagnosticError{stage: "apply", from: quepaxa.Slot(from), remoteTip: 20, remoteTipKnown: true, cause: alive}
		}, func() uint64 { return tip })
		if !reflect.DeepEqual(called, order) || len(attempts) != 2 || !errors.Is(err, attempts[1].err) {
			t.Fatal("source loop semantics changed")
		}
		if order[0] == "alive" && attempts[1].startTip != 10 {
			t.Fatal("second source reused stale from-tip")
		}
		var lines []string
		logOperationSyncFailure("local", 7, attempts, func(args ...any) { lines = append(lines, fmt.Sprint(args...)) })
		if len(lines) != 2 {
			t.Fatal("all-failed sources were lost")
		}
		for i, line := range lines {
			if !strings.Contains(line, fmt.Sprintf("source_hash=%x", sha256.Sum256([]byte(order[i])))) || !strings.Contains(line, fmt.Sprintf("attempt=%d count=2", i+1)) {
				t.Fatal("actual order/identity lost")
			}
			if !regexp.MustCompile(`(?i)error|failed|timeout`).MatchString(line) {
				t.Fatal("diagnostic evaded unchanged Performance failure gate")
			}
			if strings.Contains(line, "secret-value") || strings.Contains(line, "credential") || strings.Contains(line, "SQL") {
				t.Fatal("raw payload in diagnostic")
			}
			if order[i] == "alive" && (!strings.Contains(line, "stage=apply") || !strings.Contains(line, "remote_tip_known=true remote_tip=20")) {
				t.Fatal("local apply mislabeled RPC")
			}
			if order[i] == "dead" && !strings.Contains(line, "remote_tip_known=false remote_tip=0") {
				t.Fatal("unknown response tip invented")
			}
		}
	}
}

func TestOperationSyncDiagnosticsSuccessStopsWithoutEvent(t *testing.T) {
	for _, sources := range [][]quepaxa.NodeID{{"dead", "alive", "unused"}, {"alive", "unused"}} {
		calls := 0
		attempts, err := runOperationSyncRound(sources, func(source quepaxa.NodeID) error {
			calls++
			if source == "unused" {
				t.Fatal("continued after success")
			}
			if source == "dead" {
				return context.DeadlineExceeded
			}
			return nil
		}, func() uint64 { return 0 })
		if err != nil || len(attempts) != 0 || calls != len(sources)-1 {
			t.Fatal("success changed")
		}
		logOperationSyncFailure("local", 0, attempts, func(...any) { t.Fatal("success emitted deferral event") })
	}
}

func TestOperationSyncDiagnosticsBoundsAndUnwrap(t *testing.T) {
	sources := make([]quepaxa.NodeID, 70)
	for i := range sources {
		sources[i] = quepaxa.NodeID(fmt.Sprintf("%d\n%s", i, strings.Repeat("secret", 10000)))
	}
	attempts, _ := runOperationSyncRound(sources, func(quepaxa.NodeID) error {
		return &catchUpDiagnosticError{stage: "local-replay", from: 5, cause: context.Canceled}
	}, func() uint64 { return 4 })
	count := 0
	logOperationSyncFailure(sources[0], 1, attempts, func(args ...any) {
		line := fmt.Sprint(args...)
		count++
		if len(line) > 1024 || strings.ContainsAny(line, "\r\n") || strings.Contains(line, "secret") {
			t.Fatal("unbounded/raw identity diagnostic")
		}
		if !strings.Contains(line, "stage=local-replay") || !strings.Contains(line, "error_class=canceled") {
			t.Fatal("local stage/cancel cause lost")
		}
		if !strings.Contains(line, "rpc_known=false") || !strings.Contains(line, "connection_known=false") || !strings.Contains(line, "request_known=false") || !strings.Contains(line, "dial_measured=false") {
			t.Fatal("missing transport observation reported as measured zero")
		}
	})
	if count != 70 {
		t.Fatal("ordinary membership truncated at managed limit")
	}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, quepaxa.ErrCompacted} {
		err := &catchUpDiagnosticError{stage: "restore-archive", from: 1, cause: cause}
		if !errors.Is(err, cause) {
			t.Fatal("original error identity lost")
		}
	}
}
