//go:build rhiza_local_testhooks

package materializer

import (
	"fmt"

	"github.com/mrchypark/rhiza/internal/types"
)

type GraphApplySnapshot struct {
	SQLiteTip       uint64
	GraphTip        uint64
	GraphDurableTip uint64
	History         GraphHistory
	Receipt         types.MutationReceipt
	ReceiptFound    bool
}

// TestGraphApplySnapshot observes state from the GraphApplyContext hook while
// ApplyBatch owns m.mu and has already committed the native Graph decision.
func (m *Materializer) TestGraphApplySnapshot(requestID string) (GraphApplySnapshot, error) {
	return m.graphApplySnapshot(requestID)
}

func (m *Materializer) TestGraphSnapshot(requestID string) (GraphApplySnapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.graphApplySnapshot(requestID)
}

func (m *Materializer) graphApplySnapshot(requestID string) (GraphApplySnapshot, error) {
	g := m.graph
	if g == nil {
		return GraphApplySnapshot{}, fmt.Errorf("Graph materializer is not open")
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	request, found, err := g.request(requestID)
	if err != nil {
		return GraphApplySnapshot{}, err
	}
	return GraphApplySnapshot{
		SQLiteTip:       m.tip,
		GraphTip:        g.tip,
		GraphDurableTip: g.durableTip,
		History:         g.history,
		Receipt:         request.Receipt,
		ReceiptFound:    found,
	}, nil
}
