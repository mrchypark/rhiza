package network

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// Fieldwise accounting for the actual chargeState.add grammar:
// S(string)=32+13L; raw wire R=4+L, so 3S/16-R >= 2.
// Each scalar has node32 credit6; int64/f64 wire9 additionally uses its
// parent's list-entry16 credit3, map-entry32 credit6, or event64 credit12.
// Other scalar wires fit those same credits. A string/Number node includes
// another S and its wire tag+length+L fits without parent credit.
// List/map node+container96 credit18 pays tag+count5. Map entry credit6 plus
// S(key)'s spare2 pays a scalar9 with its node credit6. Nested containers
// carry their own credit. Each event's credit12 pays its scalar payload
// deficit3 and stream/kind each leave spare2. Root node32+Events container64
// credit18 pays magic4+eventCount4+presenceFlags2; requestID/Cypher each leave
// spare2 and Args map leaves spare13. Offset's two raw strings leave4 toward
// uint64's8; trim's string leaves2 toward uint64's8. Even both optional structs
// together consume only10 of the root's remaining25. Hence B<=3C/16.
// Adding singleton envelope9 still fits C/4 because Graph root's minimum
// charge exceeds144. Tests below call the real mutationCharge, not a copied
// estimator; this is representation/admission accounting, not an engine ACK.
func TestManagedGraphBudgetUsesActualOrdinaryCharge(t *testing.T) {
	values := []any{nil, false, true, int64(math.MinInt64), float32(.1), float64(1), json.Number("1e0"), string([]byte{0xff}), []any(nil), []any{}, map[string]any(nil), map[string]any{}, []any{int64(1), map[string]any{"a": float64(2)}}}
	for _, v := range values {
		command := types.GraphCommand{RequestID: "r", Cypher: "RETURN $x", Args: map[string]any{"x": v}, Events: []types.GraphStreamEvent{{Stream: "events", Kind: "kind", Payload: v}}}
		charge, err := mutationCharge(command)
		if err != nil {
			t.Fatal(err)
		}
		body, err := types.EncodeManagedGraphOriginal(command)
		if err != nil {
			t.Fatal(err)
		}
		if len(body)*16 > charge*3 {
			t.Fatalf("body bound failed type=%T body=%d charge=%d", v, len(body), charge)
		}
		frame, err := PrepareManagedGraphExecution(command)
		if err != nil || len(frame)*4 > charge {
			t.Fatalf("frame bound type=%T len=%d charge=%d err=%v", v, len(frame), charge, err)
		}
		clone, _ := cloneGraphCommand(command)
		clone.RequestID = ""
		item, _ := types.EncodeGraphBatchItem(clone)
		fp, err := types.ManagedGraphIntentFingerprint(command)
		if err != nil || fp != types.FingerprintBatchItem("rhiza/managed-graph-intent/v1", item) {
			t.Fatal("ordinary clone intent differs", err)
		}
	}
	for _, command := range []types.GraphCommand{{RequestID: "r", StreamOffset: &types.GraphStreamOffsetMutation{Stream: "s", Consumer: "c", Sequence: math.MaxUint64}}, {RequestID: "r", StreamTrim: &types.GraphStreamTrimMutation{Stream: "s", ThroughSequence: math.MaxUint64}}} {
		charge, _ := mutationCharge(command)
		body, err := types.EncodeManagedGraphOriginal(command)
		if err != nil || len(body)*16 > charge*3 {
			t.Fatalf("operation bound len=%d charge=%d err=%v", len(body), charge, err)
		}
		if _, err := PrepareManagedGraphExecution(command); err != nil {
			t.Fatal(err)
		}
	}
}

func TestManagedGraphSingletonPreservesLargeOrdinaryInput(t *testing.T) {
	items := make([]any, 21800)
	for i := range items {
		items[i] = float64(1)
	}
	command := types.GraphCommand{RequestID: "large", Cypher: "RETURN $x", Args: map[string]any{"x": items}}
	charge, err := mutationCharge(command)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := PrepareManagedGraphExecution(command)
	if err != nil {
		t.Fatal(err)
	}
	if len(frame) <= quepaxa.MaxReplicatedValueBytes || len(frame)*4 > charge {
		t.Fatalf("bad large fixture len=%d charge=%d", len(frame), charge)
	}
	var proposals atomic.Int32
	b := newManagedGraphBatcher(func(_ context.Context, value []byte) (quepaxa.Slot, error) {
		proposals.Add(1)
		if !bytes.Equal(value, frame) {
			t.Error("proposed frame changed")
		}
		return 1, nil
	}, func(context.Context, quepaxa.Slot) error { return nil })
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := b.submit(ctx, command); err != nil {
		t.Fatal(err)
	}
	if proposals.Load() != 1 {
		t.Fatal("not exactly one proposal")
	}
	// Ordinary eligibility is retained even if a small typed representation
	// could encode this input: the old JSON item exceeds its existing cap.
	bad := types.GraphCommand{RequestID: "bad", Cypher: "RETURN $x", Args: map[string]any{"x": strings.Repeat("<", 23000)}}
	if _, err := b.submit(ctx, bad); err == nil {
		t.Fatal("ineligible ordinary input admitted")
	}
	if proposals.Load() != 1 {
		t.Fatal("invalid input proposed")
	}
}
