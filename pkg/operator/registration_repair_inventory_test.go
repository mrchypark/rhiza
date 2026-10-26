package operator

import (
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestRepairInventoryChecksSupersededProducerState(t *testing.T) {
	for _, mode := range []string{"reserved", "open", "closed-without-root", "closed-exact"} {
		t.Run(mode, func(t *testing.T) {
			e, _, id, runtime := newMutationStoreFixture(t)
			if _, err := e.reserveMutationProducer(t.Context(), runtime); err != nil {
				t.Fatal(err)
			}
			root, _, err := e.AuthorityState.ReadRoot(t.Context(), runtime.StatefulSetUID, id.SourceClusterID)
			if err != nil {
				t.Fatal(err)
			}
			run := root.Ledger.Runs[id.RunID]
			run.RegistrationSupersededBy = "successor"
			producer, _, version, err := e.AuthorityState.ReadProducer(t.Context(), runtime)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "open":
				producer.Phase, producer.WALIdentity = fencing.MutationOpen, "unexpected-wal"
			case "closed-without-root", "closed-exact":
				stop := fencing.StopRequest{OperationID: "close", RequestHash: strings.Repeat("a", 64), Identity: id}
				if err := producer.Close(stop.OperationID, stop.RequestHash); err != nil {
					t.Fatal(err)
				}
				if mode == "closed-exact" {
					run.Stop = &stop
				}
			}
			if err := e.AuthorityState.WriteProducer(t.Context(), runtime, producer, version); err != nil {
				t.Fatal(err)
			}
			root.Ledger.Runs[id.RunID] = run
			inventory, err := fencing.SortedRepairIncarnations(root.Ledger)
			if err != nil || len(inventory) != 1 || !inventory[0].SupersededReservation() {
				t.Fatalf("reservation snapshot: %v %v", inventory, err)
			}
			store, err := NewEngineRepairStore(e.AuthorityState, runtime.StatefulSetUID)
			if err != nil {
				t.Fatal(err)
			}
			err = store.verifySupersededReservations(t.Context(), root.Ledger, inventory)
			wantPass := mode == "reserved" || mode == "closed-exact"
			if (err == nil) != wantPass {
				t.Fatalf("producer verification: %v, want pass=%t", err, wantPass)
			}
		})
	}
}
