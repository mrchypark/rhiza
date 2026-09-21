package network

import (
	"context"
	"runtime"
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// allocatedBytes reports the cumulative bytes allocated while fn ran and
// whether fn panicked, so a rejection that still materialized the declared
// vector length is visible instead of being masked by the panic.
func allocatedBytes(fn func()) (uint64, bool) {
	var before, after runtime.MemStats
	panicked := false
	runtime.ReadMemStats(&before)
	func() {
		defer func() {
			if recover() != nil {
				panicked = true
			}
			runtime.ReadMemStats(&after)
		}()
		fn()
	}()
	return after.TotalAlloc - before.TotalAlloc, panicked
}

// vectorLengthOffset locates the u32 length slot of a vector field through the
// frame's own structure, so a packing layout change cannot silently retarget a
// test patch.
func vectorLengthOffset(t *testing.T, table flatbuffers.Table, field int) int {
	t.Helper()
	off := peerTableVOffsetT(table, peerFieldVOffsetT(field))
	if off == 0 {
		t.Fatalf("field %d absent from frame", field)
	}
	return int(peerIndirect(table.Bytes, table.Pos+flatbuffers.UOffsetT(off)))
}

func patchVectorLength(t *testing.T, frame []byte, offset int, length uint32) {
	t.Helper()
	if offset < 0 || len(frame)-offset < flatbuffers.SizeUOffsetT {
		t.Fatalf("vector length offset %d outside frame of %d bytes", offset, len(frame))
	}
	flatbuffers.WriteUint32(frame[offset:], length)
}

func requestRoot(t *testing.T, frame []byte) flatbuffers.Table {
	t.Helper()
	return peerTableAt(frame, peerIndirect(frame, 0))
}

func responseRoot(t *testing.T, frame []byte) flatbuffers.Table {
	t.Helper()
	return peerTableAt(frame, peerIndirect(frame, 0))
}

// requestWithSharedSummaries builds a valid request whose decision vector holds
// refs entries that all point at one Summary, which is legal FlatBuffers
// encoding and the shape that multiplies UnPack materialization.
func requestWithSharedSummaries(refs int, recorderID string) []byte {
	builder := flatbuffers.NewBuilder(1024)
	recorder := builder.CreateString(recorderID)
	peerfb.SummaryStart(builder)
	peerfb.SummaryAddRecorderId(builder, recorder)
	summary := peerfb.SummaryEnd(builder)
	peerfb.DecisionStartSummariesVector(builder, refs)
	for i := 0; i < refs; i++ {
		builder.PrependUOffsetT(summary)
	}
	summaries := builder.EndVector(refs)
	peerfb.DecisionStart(builder)
	peerfb.DecisionAddSummaries(builder, summaries)
	decision := peerfb.DecisionEnd(builder)
	peerfb.RequestStart(builder)
	peerfb.RequestAddMagic(builder, peerWireMagic)
	peerfb.RequestAddDecision(builder, decision)
	request := peerfb.RequestEnd(builder)
	peerfb.FinishRequestBuffer(builder, request)
	return append([]byte(nil), builder.FinishedBytes()...)
}

// requestWithSharedProposal covers the same amplification through a nested
// table: refs summaries that all point at one Proposal carrying a ProposerId.
func requestWithSharedProposal(refs int, proposerID string) []byte {
	builder := flatbuffers.NewBuilder(1024)
	proposer := builder.CreateString(proposerID)
	peerfb.ProposalStart(builder)
	peerfb.ProposalAddProposerId(builder, proposer)
	proposal := peerfb.ProposalEnd(builder)
	peerfb.SummaryStart(builder)
	peerfb.SummaryAddFirstCurrent(builder, proposal)
	summary := peerfb.SummaryEnd(builder)
	peerfb.DecisionStartSummariesVector(builder, refs)
	for i := 0; i < refs; i++ {
		builder.PrependUOffsetT(summary)
	}
	summaries := builder.EndVector(refs)
	peerfb.DecisionStart(builder)
	peerfb.DecisionAddSummaries(builder, summaries)
	decision := peerfb.DecisionEnd(builder)
	peerfb.RequestStart(builder)
	peerfb.RequestAddMagic(builder, peerWireMagic)
	peerfb.RequestAddDecision(builder, decision)
	request := peerfb.RequestEnd(builder)
	peerfb.FinishRequestBuffer(builder, request)
	return append([]byte(nil), builder.FinishedBytes()...)
}

// responseWithSharedDecided builds a response whose decisions vector holds refs
// entries that all point at one DecidedValue carrying value and certificate.
func responseWithSharedDecided(refs int, value, certificate []byte) []byte {
	builder := flatbuffers.NewBuilder(1024)
	valueOffset := builder.CreateByteString(value)
	certificateOffset := builder.CreateByteString(certificate)
	peerfb.DecidedValueStart(builder)
	peerfb.DecidedValueAddValue(builder, valueOffset)
	peerfb.DecidedValueAddCertificate(builder, certificateOffset)
	decided := peerfb.DecidedValueEnd(builder)
	peerfb.ResponseStartDecisionsVector(builder, refs)
	for i := 0; i < refs; i++ {
		builder.PrependUOffsetT(decided)
	}
	decisions := builder.EndVector(refs)
	peerfb.ResponseStart(builder)
	peerfb.ResponseAddMagic(builder, peerWireMagic)
	peerfb.ResponseAddDecisions(builder, decisions)
	response := peerfb.ResponseEnd(builder)
	peerfb.FinishResponseBuffer(builder, response)
	return append([]byte(nil), builder.FinishedBytes()...)
}

func TestPeerBoundsRejectsForgedRequestSummariesLength(t *testing.T) {
	frame := requestWithSharedSummaries(2, "n1")
	decision, ok := peerTableField(requestRoot(t, frame), peerFieldVOffsetT(8))
	if !ok {
		t.Fatal("decision absent from built frame")
	}
	offset := vectorLengthOffset(t, decision, 3)
	forged := append([]byte(nil), frame...)
	patchVectorLength(t, forged, offset, 1<<16)

	var decoded *peerfb.RequestT
	var err error
	allocated, _ := allocatedBytes(func() { decoded, err = decodePeerRequest(forged) })
	if err == nil || decoded != nil {
		t.Fatalf("forged summaries length accepted: decoded=%v err=%v", decoded, err)
	}
	if allocated > 64<<10 {
		t.Fatalf("rejecting a forged summaries length allocated %d bytes", allocated)
	}

	patchVectorLength(t, forged, offset, ^uint32(0))
	if err := peerRequestBounds(forged); err == nil {
		t.Fatal("0xffffffff summaries length accepted")
	}
}

func TestPeerBoundsRejectsForgedResponseDecisionsLength(t *testing.T) {
	frame := responseWithSharedDecided(2, []byte("value"), []byte("certificate"))
	offset := vectorLengthOffset(t, responseRoot(t, frame), 8)
	forged := append([]byte(nil), frame...)
	patchVectorLength(t, forged, offset, 1<<16)

	var decoded *peerfb.ResponseT
	var err error
	allocated, _ := allocatedBytes(func() { decoded, err = decodePeerResponse(forged) })
	if err == nil || decoded != nil {
		t.Fatalf("forged decisions length accepted: decoded=%v err=%v", decoded, err)
	}
	if allocated > 64<<10 {
		t.Fatalf("rejecting a forged decisions length allocated %d bytes", allocated)
	}

	patchVectorLength(t, forged, offset, ^uint32(0))
	if err := peerResponseBounds(forged); err == nil {
		t.Fatal("0xffffffff decisions length accepted")
	}
}

func TestPeerBoundsChargesSharedReferencesPerReference(t *testing.T) {
	const recorderID = "recorder"
	perCopy := len(recorderID)
	frame := requestWithSharedSummaries(4, recorderID)

	bounds := newPeerBounds()
	bounds.maxPayload = 3 * perCopy
	if err := peerRequestBoundsWith(frame, bounds); err == nil {
		t.Fatal("four references to one summary were charged once")
	}
	bounds = newPeerBounds()
	bounds.maxPayload = 4 * perCopy
	if err := peerRequestBoundsWith(frame, bounds); err != nil {
		t.Fatalf("four references within budget rejected: %v", err)
	}

	const proposerID = "proposer"
	proposalFrame := requestWithSharedProposal(4, proposerID)
	bounds = newPeerBounds()
	bounds.maxPayload = 3 * len(proposerID)
	if err := peerRequestBoundsWith(proposalFrame, bounds); err == nil {
		t.Fatal("four references to one proposer id were charged once")
	}

	value := []byte("decided-value")
	certificate := []byte("decided-certificate")
	responseFrame := responseWithSharedDecided(4, value, certificate)
	bounds = newPeerBounds()
	bounds.maxPayload = 3 * (len(value) + len(certificate))
	if err := peerResponseBoundsWith(responseFrame, bounds); err == nil {
		t.Fatal("four references to one decided value were charged once")
	}
	bounds = newPeerBounds()
	bounds.maxPayload = 4 * (len(value) + len(certificate))
	if err := peerResponseBoundsWith(responseFrame, bounds); err != nil {
		t.Fatalf("four decided value references within budget rejected: %v", err)
	}
}

func TestPeerBoundsChargesObjectSlots(t *testing.T) {
	// Half the budget in references: the root, decision, and summary tables alone
	// (2+refs) fit, so only the vector slots (2+2*refs) push the frame over the
	// budget. A fixture larger than the budget is rejected by the tables alone and
	// would not fail if the slot charge were dropped.
	const refs = peerObjectBudget / 2
	frame := requestWithSharedSummaries(refs, "n1")
	allocated, _ := allocatedBytes(func() {
		if err := peerRequestBounds(frame); err == nil {
			t.Error("an object-dense frame was accepted")
		}
	})
	if allocated > 64<<10 {
		t.Fatalf("rejecting an object-dense frame allocated %d bytes", allocated)
	}

	// One reference fewer lands exactly on the budget and must be accepted.
	frame = requestWithSharedSummaries(refs-1, "n1")
	if err := peerRequestBounds(frame); err != nil {
		t.Fatalf("frame at exactly the object budget rejected: %v", err)
	}
}

func TestPeerBoundsAcceptsDecisionCatchUpPage(t *testing.T) {
	member := testMember("cluster", "n1", "secret")
	core := mustCore(t, member.ID, []quepaxa.Member{member}, nil, nil)
	server := NewServer(core, nil, "cluster", true, nil)
	defer server.Close()
	for slot := quepaxa.Slot(1); slot <= 64; slot++ {
		value := make([]byte, 8<<10)
		value[0] = byte(slot)
		if _, _, err := core.Propose(context.Background(), value); err != nil {
			t.Fatal(err)
		}
	}
	response, err := serverPeerHandleDecisions(server, member, 1)
	if err != nil {
		t.Fatal(err)
	}
	encoded := encodePeerResponse(response)
	if len(encoded) > maxPeerFrame {
		t.Fatalf("catch-up page is %d bytes", len(encoded))
	}
	decoded, err := decodePeerResponse(encoded)
	if err != nil {
		t.Fatalf("legitimate catch-up page rejected: %v", err)
	}
	if len(decoded.Decisions) != len(response.Decisions) {
		t.Fatalf("decoded %d decisions, encoded %d", len(decoded.Decisions), len(response.Decisions))
	}
}

func FuzzPeerRequestBounds(f *testing.F) {
	f.Add(requestWithSharedSummaries(2, "n1"))
	f.Add(requestWithSharedProposal(2, "n1"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxPeerFrame {
			return
		}
		_ = peerRequestBounds(data)
	})
}

func FuzzPeerResponseBounds(f *testing.F) {
	f.Add(responseWithSharedDecided(2, []byte("value"), []byte("certificate")))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxPeerFrame {
			return
		}
		_ = peerResponseBounds(data)
	})
}

func FuzzPeerRequestDecode(f *testing.F) {
	f.Add(encodePeerRequest(&peerfb.RequestT{
		Magic: peerWireMagic, Operation: peerfb.OperationSync, ClusterId: "cluster", SenderId: "n1", From: 1, Limit: 128,
		Decision: &peerfb.DecisionT{Slot: 1, Summaries: []*peerfb.SummaryT{{RecorderId: "n1"}}},
	}))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxPeerFrame {
			return
		}
		_, _ = decodePeerRequest(data)
	})
}

func FuzzPeerResponseDecode(f *testing.F) {
	f.Add(encodePeerResponse(&peerfb.ResponseT{
		Magic: peerWireMagic, ClusterId: "cluster", ProposerId: "n1",
		Decisions: []*peerfb.DecidedValueT{{Slot: 1, Value: []byte("value"), Certificate: []byte("certificate")}},
	}))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxPeerFrame {
			return
		}
		_, _ = decodePeerResponse(data)
	})
}
