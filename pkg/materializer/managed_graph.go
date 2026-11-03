package materializer

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
)

// GraphHistory is a certified-apply lineage component, not a digest of live
// Graph contents or permission to adopt arbitrary existing native files.
type GraphHistory struct {
	LastGraphSlot uint64
	Hash          [32]byte
}

func OpenManagedSQLKVGraphWithOriginals(path string, readers int, window ...uint64) (*Materializer, error) {
	return openWithProtocol(path, readers, sqlpolicy.ManagedGraphRequestApplicationProtocol, window...)
}

// InspectManagedGraphSQLFile validates only the SQLite component. It does not
// establish native Graph provenance; startup must reconstruct the actual pair.
func InspectManagedGraphSQLFile(ctx context.Context, path string, window uint64) (ManagedApplicationState, uint64, error) {
	if window < 1024 || window > 1_048_576 {
		return ManagedApplicationState{}, 0, fmt.Errorf("invalid Graph receipt window")
	}
	if err := ValidateCleanRestoreState(path); err != nil {
		return ManagedApplicationState{}, 0, err
	}
	state, tip, err := inspectApplicationFile(ctx, path, window, sqlpolicy.ManagedGraphRequestApplicationProtocol)
	return ManagedApplicationState(state), tip, err
}

func (m *Materializer) ProjectionReadable(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.writer == nil {
		return sql.ErrConnDone
	}
	if graphProtocol(m.managedProtocol) {
		return m.graphReadableLocked()
	}
	return m.sqlReceiptReadLocked()
}

func graphProtocol(protocol string) bool {
	return protocol == sqlpolicy.ManagedGraphRequestApplicationProtocol
}
func originalsProtocol(protocol string) bool {
	return protocol == sqlpolicy.ManagedRequestApplicationProtocol || graphProtocol(protocol)
}
func originalsPolicy(policy sqlpolicy.MaterializationPolicy) bool {
	return policy == sqlpolicy.ManagedRequestMaterializationPolicy || policy == sqlpolicy.ManagedGraphRequestMaterializationPolicy
}

func foldGraphHistory(previous GraphHistory, slot uint64, decision [32]byte) (GraphHistory, error) {
	if slot == 0 || slot <= previous.LastGraphSlot || decision == ([32]byte{}) {
		return GraphHistory{}, fmt.Errorf("invalid graph history progression")
	}
	h := sha256.New()
	h.Write([]byte("rhiza-graph-history/v1\x00"))
	h.Write(previous.Hash[:])
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], slot)
	h.Write(encoded[:])
	h.Write(decision[:])
	next := GraphHistory{LastGraphSlot: slot}
	copy(next.Hash[:], h.Sum(nil))
	return next, nil
}

func sqliteGraphHistory(ctx context.Context, reader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) (GraphHistory, error) {
	var text, hash string
	if err := reader.QueryRowContext(ctx, `SELECT (SELECT value FROM _rhiza_meta WHERE key='last_graph_slot'),(SELECT value FROM _rhiza_meta WHERE key='graph_history_hash')`).Scan(&text, &hash); err != nil {
		return GraphHistory{}, err
	}
	slot, err := strconv.ParseUint(text, 10, 64)
	decoded, hashErr := hex.DecodeString(hash)
	if err != nil || hashErr != nil || len(decoded) != 32 {
		return GraphHistory{}, fmt.Errorf("invalid SQLite graph history")
	}
	out := GraphHistory{LastGraphSlot: slot}
	copy(out.Hash[:], decoded)
	if (slot == 0) != (out.Hash == ([32]byte{})) {
		return GraphHistory{}, fmt.Errorf("invalid SQLite graph history origin")
	}
	return out, nil
}

func foldSQLiteGraphHistory(ctx context.Context, tx *sql.Tx, slot uint64, decision [32]byte) error {
	previous, err := sqliteGraphHistory(ctx, tx)
	if err != nil {
		return err
	}
	next, err := foldGraphHistory(previous, slot, decision)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE _rhiza_meta SET value=CASE key WHEN 'last_graph_slot' THEN ? ELSE ? END WHERE key IN ('last_graph_slot','graph_history_hash')`, strconv.FormatUint(next.LastGraphSlot, 10), hex.EncodeToString(next.Hash[:]))
	return err
}

// ManagedGraphHistory requires a clean paired projection. Startup must first
// independently establish the checkpoint/WAL provenance of the actual pair.
func (m *Materializer) ManagedGraphHistory(ctx context.Context) (GraphHistory, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !graphProtocol(m.managedProtocol) {
		return GraphHistory{}, sqlpolicy.ErrIncompatible
	}
	if err := m.graphReadableLocked(); err != nil {
		return GraphHistory{}, err
	}
	history, err := sqliteGraphHistory(ctx, m.writer)
	if err != nil {
		return GraphHistory{}, err
	}
	m.graph.mu.RLock()
	defer m.graph.mu.RUnlock()
	if history != m.graph.history {
		return GraphHistory{}, fmt.Errorf("paired graph history mismatch")
	}
	return history, nil
}

// RuntimeHealth allows exact certified replay to complete a pending projection.
// Public observations use the stricter graphReadableLocked guard instead.
func (m *Materializer) RuntimeHealth(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.writer == nil {
		return sql.ErrConnDone
	}
	if err := m.fatalHealthLocked(); err != nil {
		return err
	}
	if err := m.validateGraphHistoryLocked(ctx, false); err != nil {
		return err
	}
	return m.writer.PingContext(ctx)
}

func (m *Materializer) fatalHealthLocked() error {
	if m.commitUnknown {
		return ErrGraphCommitUnknown
	}
	if m.graph == nil {
		return nil
	}
	m.graph.mu.RLock()
	defer m.graph.mu.RUnlock()
	if m.graph.commitUnknown {
		return ErrGraphCommitUnknown
	}
	return nil
}
