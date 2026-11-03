//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"fmt"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/node"
)

// QualificationApplicationKind classifies only inside the synchronous CI
// observer. Recognized malformed application envelopes still count as entries;
// control values do not. No borrowed input is retained or logged.
func QualificationApplicationKind(value []byte) string {
	if _, ok, _ := types.DecodeManagedGraphExecution(value); ok {
		return "graph"
	}
	if _, ok, _ := types.DecodeGraphBatch(value); ok {
		return "graph"
	}
	if _, ok, _ := types.DecodeManagedKVCommand(value); ok {
		return "kv"
	}
	if _, ok, _ := types.DecodeManagedKVBatch(value); ok {
		return "kv"
	}
	if _, ok, _ := types.DecodeKVCommand(value); ok {
		return "kv"
	}
	if _, ok, _ := types.DecodeKVBatch(value); ok {
		return "kv"
	}
	if _, ok, _ := types.DecodeSQLBatch(value); ok {
		return "sql"
	}
	return ""
}

type ManagedGraphQualificationObservation struct {
	Original       materializer.RequestOriginal
	OriginalFound  bool
	Stats          objectstore.Stats
	StatsAvailable bool
	Archive        node.ManagedGraphQualificationArchive
}

// ObserveManagedGraphQualification is compiled only into CI binaries. It
// reveals no Node pointer/raw server and changes no ordinary SDK wire surface.
func (h *Host) ObserveManagedGraphQualification(ctx context.Context, rawID string, locator *checkpoint.ManagedGraphLocator) (ManagedGraphQualificationObservation, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out ManagedGraphQualificationObservation
	if h.closed || h.node == nil || !sqlpolicy.IsManagedGraphRequestApplicationProtocol(h.cfg.Plan.ApplicationProtocol) {
		return out, fmt.Errorf("open policy10 Host required")
	}
	if err := h.node.EnsureInitialManagedAdmission(ctx); err != nil {
		return out, err
	}
	if rawID != "" {
		original, found, err := h.node.RetainedRequest(ctx, "graph", rawID)
		if err != nil {
			return out, err
		}
		out.Original, out.OriginalFound = original, found
	}
	archive, err := h.node.ObserveManagedGraphQualificationArchive(ctx, locator)
	if err != nil {
		return out, err
	}
	out.Archive = archive
	out.Stats, out.StatsAvailable = h.node.ObjectStoreStats()
	return out, nil
}
