package materializer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"

	latticedb "github.com/mrchypark/latticedb-go"
	"github.com/mrchypark/rhiza/internal/types"
)

var graphOriginalRecordMagic = []byte{'R', 'G', 'O', 1}
var graphHistoryKey = []byte("rhiza/graph_history/v1")
var graphOriginalBytesKey = []byte("rhiza/original_bytes/v1")

func encodeGraphHistory(history GraphHistory) []byte {
	out := make([]byte, 40)
	binary.BigEndian.PutUint64(out, history.LastGraphSlot)
	copy(out[8:], history.Hash[:])
	return out
}
func decodeGraphHistory(data []byte) (GraphHistory, error) {
	if len(data) != 40 {
		return GraphHistory{}, fmt.Errorf("invalid graph history encoding")
	}
	out := GraphHistory{LastGraphSlot: binary.BigEndian.Uint64(data)}
	copy(out.Hash[:], data[8:])
	if out.LastGraphSlot > math.MaxInt64 || (out.LastGraphSlot == 0) != (out.Hash == ([32]byte{})) {
		return GraphHistory{}, fmt.Errorf("invalid graph history origin")
	}
	return out, nil
}

func (g *graphState) advanceMetadata(tx *latticedb.Tx, slot uint64, hash [32]byte, confirmedThrough uint64) error {
	if g.originals {
		data, _, err := tx.GetAppMetadata(graphHistoryKey)
		if err != nil {
			return err
		}
		history, err := decodeGraphHistory(data)
		if err != nil {
			return err
		}
		next, err := foldGraphHistory(history, slot, hash)
		if err != nil {
			return err
		}
		if err := tx.PutAppMetadata(graphHistoryKey, encodeGraphHistory(next)); err != nil {
			return err
		}
	}
	return advanceGraphMetadata(tx, slot, hash, confirmedThrough)
}

func (g *graphState) originalRequest(command types.GraphCommand, fp [32]byte, receipt types.MutationReceipt, decision [32]byte, ordinal []uint32) (graphRequest, error) {
	out := graphRequest{Fingerprint: fp, Receipt: receipt}
	if !g.originals {
		return out, nil
	}
	if len(ordinal) != 1 || ordinal[0] != 0 {
		return graphRequest{}, fmt.Errorf("invalid singleton graph ordinal")
	}
	item, err := types.EncodeManagedGraphExecution(command)
	if err != nil {
		return graphRequest{}, err
	}
	original, err := types.EncodeManagedGraphOriginal(command)
	if err != nil {
		return graphRequest{}, err
	}
	out.Original = &RequestOriginal{Kind: types.MutationGraph, RequestID: command.RequestID, Slot: receipt.Slot, BatchIndex: ordinal[0], Fingerprint: fp, CanonicalOriginal: original, ExecutionItem: item, DecisionHash: decision, Receipt: receipt}
	return out, nil
}

func encodeGraphOriginalRequest(id string, request graphRequest) ([]byte, error) {
	original := request.Original
	if original == nil || original.RequestID != id || original.Slot != request.Receipt.Slot || original.Fingerprint != request.Fingerprint || original.BatchIndex != 0 || original.DecisionHash == ([32]byte{}) || len(original.ExecutionItem) == 0 || len(original.ExecutionItem) > 256<<10 {
		return nil, fmt.Errorf("invalid graph original record")
	}
	out := append([]byte(nil), graphOriginalRecordMagic...)
	out = append(out, encodeGraphRequest(graphRequest{Fingerprint: request.Fingerprint, Receipt: request.Receipt})...)
	out = binary.BigEndian.AppendUint32(out, original.BatchIndex)
	out = append(out, original.DecisionHash[:]...)
	out = binary.BigEndian.AppendUint16(out, uint16(len(id)))
	out = append(out, id...)
	out = binary.BigEndian.AppendUint32(out, uint32(len(original.ExecutionItem)))
	out = append(out, original.ExecutionItem...)
	if _, _, err := decodeGraphOriginalRequest(out); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeGraphOriginalRequest(data []byte) (graphRequest, bool, error) {
	const fixed = 4 + 42 + 4 + 32 + 2
	if len(data) < fixed+4 || !bytes.Equal(data[:4], graphOriginalRecordMagic) {
		return graphRequest{}, false, fmt.Errorf("invalid graph original envelope")
	}
	request, _, err := decodeGraphRequest(data[4:46])
	if err != nil {
		return graphRequest{}, false, err
	}
	if !bytes.Equal(data[4:46], encodeGraphRequest(request)) {
		return graphRequest{}, false, fmt.Errorf("noncanonical graph receipt")
	}
	index := binary.BigEndian.Uint32(data[46:50])
	var decision [32]byte
	copy(decision[:], data[50:82])
	idLen := int(binary.BigEndian.Uint16(data[82:84]))
	data = data[84:]
	if index != 0 || decision == ([32]byte{}) || idLen == 0 || idLen > types.MaxRequestIDBytes || idLen > len(data)-4 {
		return graphRequest{}, false, fmt.Errorf("invalid graph original identity")
	}
	id := string(data[:idLen])
	data = data[idLen:]
	n := int(binary.BigEndian.Uint32(data))
	item := data[4:]
	if sha256.Sum256(item) != decision {
		return graphRequest{}, false, fmt.Errorf("native original decision hash differs from singleton execution")
	}
	if n != len(item) || n == 0 || n > 256<<10 || request.Receipt.Slot == 0 || request.Receipt.Slot > math.MaxInt64 || (request.Receipt.Status == types.MutationRejected && request.Receipt.Applied) || (request.Receipt.Status == types.MutationCommitted && !request.Receipt.Applied) {
		return graphRequest{}, false, fmt.Errorf("invalid graph original execution")
	}
	command, ok, err := types.DecodeManagedGraphExecution(item)
	if err != nil || !ok {
		return graphRequest{}, false, fmt.Errorf("invalid graph original command")
	}
	fp, err := types.ManagedGraphIntentFingerprint(command)
	if err != nil {
		return graphRequest{}, false, err
	}
	canonical, err := types.EncodeManagedGraphExecution(command)
	if err != nil || command.RequestID != id || fp != request.Fingerprint || !bytes.Equal(canonical, item) {
		return graphRequest{}, false, fmt.Errorf("graph original differs from receipt")
	}
	original, err := types.EncodeManagedGraphOriginal(command)
	if err != nil {
		return graphRequest{}, false, err
	}
	request.Original = &RequestOriginal{Kind: types.MutationGraph, RequestID: id, Slot: request.Receipt.Slot, BatchIndex: index, Fingerprint: fp, DecisionHash: decision, CanonicalOriginal: original, ExecutionItem: bytes.Clone(item), Graph: &command, Receipt: request.Receipt}
	return request, true, nil
}

func adjustGraphOriginalBytes(tx *latticedb.Tx, delta int64) error {
	data, _, err := tx.GetAppMetadata(graphOriginalBytesKey)
	if err != nil {
		return err
	}
	n, err := decodeGraphTip(data)
	if err != nil {
		return err
	}
	if delta < 0 && n < uint64(-delta) || delta > 0 && n > math.MaxUint64-uint64(delta) {
		return fmt.Errorf("invalid graph original accounting")
	}
	if delta < 0 {
		n -= uint64(-delta)
	} else {
		n += uint64(delta)
	}
	return tx.PutAppMetadata(graphOriginalBytesKey, encodeGraphTip(n))
}

func (g *graphState) knownNonGraphValue(value []byte) (bool, error) {
	if !g.originals {
		return knownNonGraphValue(value)
	}
	if _, ok, err := types.DecodeManagedGraphExecution(value); ok || err != nil {
		return false, fmt.Errorf("unexpected Graph value in non-Graph path")
	}
	if err := types.ValidateManagedGraphExecutionPolicy(value); err != nil {
		return false, err
	}
	return true, nil
}

func (m *Materializer) retainedGraphOriginalLocked(ctx context.Context, id string) (RequestOriginal, bool, error) {
	if err := ctx.Err(); err != nil {
		return RequestOriginal{}, false, err
	}
	if id == "" || len(id) > types.MaxRequestIDBytes {
		return RequestOriginal{}, false, fmt.Errorf("invalid graph original lookup")
	}
	g := m.graph
	g.mu.RLock()
	defer g.mu.RUnlock()
	request, found, err := g.request(id)
	if err != nil || !found {
		return RequestOriginal{}, false, err
	}
	if request.Original == nil || request.Receipt.Slot > m.tip || request.Receipt.Slot > math.MaxUint64-m.idempotencyWindow+1 {
		return RequestOriginal{}, false, fmt.Errorf("invalid retained graph original")
	}
	if m.tip > request.Receipt.RetryThroughSlot {
		return RequestOriginal{}, false, nil
	}
	out := *request.Original
	out.Receipt = request.Receipt
	return out, true, nil
}

func (m *Materializer) GraphRequestStatusFingerprint(ctx context.Context, id string, fp [32]byte) (types.MutationReceipt, bool, bool, error) {
	if id == "" || len(id) > types.MaxRequestIDBytes {
		return types.MutationReceipt{}, false, false, fmt.Errorf("invalid graph request identity")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := m.graphReadableLocked(); err != nil {
		return types.MutationReceipt{}, false, false, err
	}
	if err := ctx.Err(); err != nil {
		return types.MutationReceipt{}, false, false, err
	}
	g := m.graph
	g.mu.RLock()
	defer g.mu.RUnlock()
	request, found, err := g.request(id)
	if err != nil {
		return types.MutationReceipt{}, false, false, err
	}
	if !found {
		return types.MutationReceipt{}, false, true, nil
	}
	if request.Receipt.Slot == 0 || request.Receipt.Slot > m.tip || request.Receipt.Slot > math.MaxUint64-m.idempotencyWindow+1 {
		return types.MutationReceipt{}, false, false, fmt.Errorf("invalid graph receipt position")
	}
	if m.tip > request.Receipt.RetryThroughSlot {
		return types.MutationReceipt{}, false, true, nil
	}
	return request.Receipt, true, request.Fingerprint == fp, nil
}

// Caller owns m.mu. Pending native completion can legitimately lead SQLite;
// its retained full-decision journal must account for every history fold.
func (m *Materializer) validateGraphHistoryLocked(ctx context.Context, clean bool) error {
	if !graphProtocol(m.managedProtocol) {
		return nil
	}
	history, err := sqliteGraphHistory(ctx, m.writer)
	if err != nil {
		return err
	}
	g := m.graph
	g.mu.RLock()
	defer g.mu.RUnlock()
	if history.LastGraphSlot > m.tip {
		return fmt.Errorf("SQLite graph history exceeds applied tip")
	}
	if clean || g.history.LastGraphSlot <= m.tip {
		if history != g.history {
			return fmt.Errorf("paired graph history mismatch")
		}
		return nil
	}
	data, err := g.getMetadata(graphJournalKey)
	if err != nil {
		return err
	}
	journal, err := decodeGraphJournal(data)
	if err != nil {
		return err
	}
	for _, entry := range journal {
		if entry.Slot > m.tip {
			history, err = foldGraphHistory(history, entry.Slot, entry.Hash)
			if err != nil {
				return err
			}
		}
	}
	if history != g.history {
		return fmt.Errorf("native graph history lacks completed decision journal")
	}
	return nil
}

// The existing bounded slot index is authoritative for retained native
// projection records; no Graph-content inventory/export is performed.
func (g *graphState) validateOriginalAccounting() error {
	if g.durableTip > math.MaxInt64 || g.idempotencyWindow == 0 {
		return fmt.Errorf("invalid graph original accounting position")
	}
	data, err := g.getMetadata(graphOriginalBytesKey)
	if err != nil {
		return err
	}
	charged, err := decodeGraphTip(data)
	if err != nil {
		return err
	}
	start := uint64(1)
	if g.durableTip >= g.idempotencyWindow {
		start = g.durableTip - g.idempotencyWindow + 1
	}
	var actual uint64
	for slot := start; slot <= g.durableTip; slot++ {
		ids, err := g.getMetadata(graphSlotKey(slot))
		if err != nil {
			return err
		}
		count := 0
		for len(ids) > 0 {
			length, n := binary.Uvarint(ids)
			if n <= 0 || length == 0 || length > types.MaxRequestIDBytes || length > uint64(len(ids)-n) {
				return fmt.Errorf("invalid graph original slot index")
			}
			ids = ids[n:]
			id := string(ids[:int(length)])
			ids = ids[int(length):]
			count++
			if count > 1 {
				return fmt.Errorf("multiple originals for singleton Graph slot")
			}
			encoded, err := g.getMetadata(graphRequestKey(id))
			if err != nil {
				return err
			}
			request, found, err := decodeGraphOriginalRequest(encoded)
			if err != nil || !found || request.Original == nil || request.Original.RequestID != id || request.Receipt.Slot != slot {
				return fmt.Errorf("graph original slot index differs from record")
			}
			if uint64(len(encoded)) > math.MaxUint64-actual {
				return fmt.Errorf("graph original accounting overflow")
			}
			actual += uint64(len(encoded))
		}
	}
	if actual != charged {
		return fmt.Errorf("graph original byte accounting differs from retained index")
	}
	return nil
}
