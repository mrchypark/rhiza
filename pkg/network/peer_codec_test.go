package network

import (
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network/peerfb"
)

func TestEncodePeerRequestDoesNotMutateSharedRequest(t *testing.T) {
	request := &peerfb.RequestT{
		Magic:     17,
		Operation: peerfb.OperationSync,
		ClusterId: "cluster",
		SenderId:  "node",
		ConfigId:  3,
		From:      1,
		Limit:     1,
	}
	const callers = 8
	encoded := make([][]byte, callers)
	var workers sync.WaitGroup
	workers.Add(callers)
	for i := range callers {
		go func() {
			defer workers.Done()
			encoded[i] = encodePeerRequest(request)
		}()
	}
	workers.Wait()

	if request.Magic != 17 {
		t.Fatalf("encoder mutated shared request magic: got %d, want 17", request.Magic)
	}
	for i, frame := range encoded {
		decoded, err := decodePeerRequest(frame)
		if err != nil {
			t.Fatalf("decode encoded request %d: %v", i, err)
		}
		if decoded.Magic != peerWireMagic || decoded.ClusterId != request.ClusterId || decoded.SenderId != request.SenderId {
			t.Fatalf("encoded request %d has invalid wire fields: %+v", i, decoded)
		}
	}
}
