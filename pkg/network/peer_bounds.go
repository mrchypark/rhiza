package network

// Maintenance contract: this preflight mirrors the allocation-relevant shape of
// peer.fbs (proposal, summary, record_request, decision, decided_value, request,
// response). Any schema change or change to the generated UnPackTo traversal
// requires revisiting the walk below and the budget accounting that mirrors it.
// It is deliberately not a general-purpose FlatBuffers verifier: it bounds what
// UnPack materializes, and it relies on the decoders' recover boundary for
// malformed metadata that the generated accessors reject by panicking.

import (
	"errors"
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"
)

const (
	// Expanded variable-sized payload bytes (copied strings and byte vectors)
	// that one peer frame may materialize. The measured worst-case legitimate
	// decision page is roughly 1.4 MiB, so a 1 MiB policy is the initial
	// trade-off in favor of a tighter bound; raise it if large values plus
	// certificates are rejected in practice.
	peerMaterializeBudget = 1 << 20
	// Tables plus vector slots that one peer frame may materialize.
	peerObjectBudget = 16384
)

// A frame longer than a whole peer frame cannot be produced by any peer, so a
// walk over it can never yield a legitimate result.
var errPeerFrameOverflow = errors.New("peer frame exceeds peer frame limit")

// peerVOffsetT is a raw vtable slot displacement inside a table.
type peerVOffsetT uint16

// peerFieldVOffsetT is the vtable slot position for schema field id.
func peerFieldVOffsetT(id int) peerVOffsetT { return peerVOffsetT(4 + 2*id) }

// peerTableVOffsetT returns the table-local displacement of the field, or 0
// when the field is absent.
func peerTableVOffsetT(table flatbuffers.Table, field peerVOffsetT) peerVOffsetT {
	if table.Pos > flatbuffers.UOffsetT(len(table.Bytes)) || len(table.Bytes)-int(table.Pos) < flatbuffers.SizeSOffsetT {
		panic("peer bounds: table outside frame")
	}
	vtable := int64(table.Pos) - int64(flatbuffers.GetSOffsetT(table.Bytes[table.Pos:]))
	if vtable < 0 || vtable > int64(len(table.Bytes))-flatbuffers.SizeVOffsetT {
		panic("peer bounds: vtable outside frame")
	}
	length := int64(flatbuffers.GetVOffsetT(table.Bytes[vtable:]))
	// A short vtable simply does not carry this field; the slot may legitimately
	// lie past the vtable's own end and must not be read before this check.
	if int64(field) >= length {
		return 0
	}
	slot := vtable + int64(field)
	if slot > int64(len(table.Bytes))-flatbuffers.SizeVOffsetT {
		panic("peer bounds: vtable slot outside frame")
	}
	return peerVOffsetT(flatbuffers.GetVOffsetT(table.Bytes[slot:]))
}

// peerIndirect resolves the relative offset stored at frame offset.
func peerIndirect(frame []byte, off flatbuffers.UOffsetT) flatbuffers.UOffsetT {
	if off > flatbuffers.UOffsetT(len(frame)) || len(frame)-int(off) < flatbuffers.SizeUOffsetT {
		panic("peer bounds: offset outside frame")
	}
	target := uint64(off) + uint64(flatbuffers.GetUOffsetT(frame[off:]))
	if target > uint64(len(frame)) {
		panic("peer bounds: offset target outside frame")
	}
	return flatbuffers.UOffsetT(target)
}

// peerTableAt returns a table view at frame offset.
func peerTableAt(frame []byte, off flatbuffers.UOffsetT) flatbuffers.Table {
	if off > flatbuffers.UOffsetT(len(frame)) || len(frame)-int(off) < flatbuffers.SizeSOffsetT {
		panic("peer bounds: table outside frame")
	}
	return flatbuffers.Table{Bytes: frame, Pos: off}
}

// peerTableField returns the sub-table at the field, or false when absent.
func peerTableField(table flatbuffers.Table, field peerVOffsetT) (flatbuffers.Table, bool) {
	off := peerTableVOffsetT(table, field)
	if off == 0 {
		return flatbuffers.Table{}, false
	}
	if int(off) > len(table.Bytes)-flatbuffers.SizeUOffsetT {
		panic("peer bounds: field outside frame")
	}
	return peerTableAt(table.Bytes, peerIndirect(table.Bytes, table.Pos+flatbuffers.UOffsetT(off))), true
}

// peerVector resolves an offset vector to its element start and element count.
func peerVector(table flatbuffers.Table, field peerVOffsetT) (flatbuffers.UOffsetT, int, bool) {
	off := peerTableVOffsetT(table, field)
	if off == 0 {
		return 0, 0, false
	}
	if int(off) > len(table.Bytes)-flatbuffers.SizeUOffsetT {
		panic("peer bounds: vector field outside frame")
	}
	header := peerIndirect(table.Bytes, table.Pos+flatbuffers.UOffsetT(off))
	if header > flatbuffers.UOffsetT(len(table.Bytes)) || len(table.Bytes)-int(header) < flatbuffers.SizeUOffsetT {
		panic("peer bounds: vector header outside frame")
	}
	count := int64(flatbuffers.GetUOffsetT(table.Bytes[header:]))
	start := header + flatbuffers.SizeUOffsetT
	if count < 0 || count > int64(len(table.Bytes)-int(start))/flatbuffers.SizeUOffsetT {
		panic("peer bounds: vector length outside frame")
	}
	return start, int(count), true
}

// peerByteVector returns the extent of a byte vector, or false when absent.
func peerByteVector(table flatbuffers.Table, field peerVOffsetT) (int, bool) {
	off := peerTableVOffsetT(table, field)
	if off == 0 {
		return 0, false
	}
	if int(off) > len(table.Bytes)-flatbuffers.SizeUOffsetT {
		panic("peer bounds: byte vector field outside frame")
	}
	header := peerIndirect(table.Bytes, table.Pos+flatbuffers.UOffsetT(off))
	if header > flatbuffers.UOffsetT(len(table.Bytes)) || len(table.Bytes)-int(header) < flatbuffers.SizeUOffsetT {
		panic("peer bounds: byte vector header outside frame")
	}
	length := int64(flatbuffers.GetUOffsetT(table.Bytes[header:]))
	start := int64(header) + flatbuffers.SizeUOffsetT
	if length < 0 || length > int64(len(table.Bytes))-start {
		panic("peer bounds: byte vector length outside frame")
	}
	return int(length), true
}

// peerVectorElement returns the table at index of an offset vector. Out of
// range indices are rejected here rather than left to the generated accessor,
// whose error path also allocates.
func peerVectorElement(table flatbuffers.Table, start flatbuffers.UOffsetT, count, index int) flatbuffers.Table {
	if index < 0 || index >= count {
		panic("peer bounds: vector index outside frame")
	}
	return peerTableAt(table.Bytes, peerIndirect(table.Bytes, start+flatbuffers.UOffsetT(index)*flatbuffers.SizeUOffsetT))
}

// peerBounds tracks the two independent materialization budgets. Every
// reference is charged, including repeated references to one shared target,
// because UnPack materializes one object per reference.
type peerBounds struct {
	objects    int
	payload    int
	maxObjects int
	maxPayload int
}

func newPeerBounds() *peerBounds {
	return &peerBounds{maxObjects: peerObjectBudget, maxPayload: peerMaterializeBudget}
}

func (b *peerBounds) chargeObject() {
	b.objects++
	if b.objects > b.maxObjects {
		panic("peer bounds: object budget exceeded")
	}
}

func (b *peerBounds) chargePayload(size int) {
	if size < 0 || size > b.maxPayload-b.payload {
		panic("peer bounds: payload budget exceeded")
	}
	b.payload += size
}

func (b *peerBounds) walkProposal(table flatbuffers.Table) {
	b.chargeObject()
	b.chargeString(table, peerFieldVOffsetT(1))
	b.chargeByteVector(table, peerFieldVOffsetT(0))
	b.chargeByteVector(table, peerFieldVOffsetT(2))
	b.chargeByteVector(table, peerFieldVOffsetT(3))
}

func (b *peerBounds) walkSummary(table flatbuffers.Table) {
	b.chargeObject()
	b.chargeString(table, peerFieldVOffsetT(0))
	b.chargeByteVector(table, peerFieldVOffsetT(4))
	if nested, ok := peerTableField(table, peerFieldVOffsetT(2)); ok {
		b.walkProposal(nested)
	}
	if nested, ok := peerTableField(table, peerFieldVOffsetT(3)); ok {
		b.walkProposal(nested)
	}
}

func (b *peerBounds) walkRecordRequest(table flatbuffers.Table) {
	b.chargeObject()
	if nested, ok := peerTableField(table, peerFieldVOffsetT(2)); ok {
		b.walkProposal(nested)
	}
}

func (b *peerBounds) walkDecision(table flatbuffers.Table) {
	b.chargeObject()
	if nested, ok := peerTableField(table, peerFieldVOffsetT(2)); ok {
		b.walkProposal(nested)
	}
	if start, count, ok := peerVector(table, peerFieldVOffsetT(3)); ok {
		b.chargeObjectSlots(count)
		for index := 0; index < count; index++ {
			b.walkSummary(peerVectorElement(table, start, count, index))
		}
	}
}

func (b *peerBounds) walkDecidedValue(table flatbuffers.Table) {
	b.chargeObject()
	b.chargeByteVector(table, peerFieldVOffsetT(1))
	b.chargeByteVector(table, peerFieldVOffsetT(2))
	b.chargeByteVector(table, peerFieldVOffsetT(3))
}

func (b *peerBounds) chargeObjectSlots(count int) {
	if count < 0 || count > b.maxObjects-b.objects {
		panic("peer bounds: object budget exceeded")
	}
	b.objects += count
}

func (b *peerBounds) chargeString(table flatbuffers.Table, field peerVOffsetT) {
	if length, ok := peerByteVector(table, field); ok {
		b.chargePayload(length)
	}
}

func (b *peerBounds) chargeByteVector(table flatbuffers.Table, field peerVOffsetT) {
	if length, ok := peerByteVector(table, field); ok {
		b.chargePayload(length)
	}
}

// peerRequestBounds walks everything Request.UnPackTo materializes. It does not
// select a payload by Operation: an otherwise unused record or decision is
// still unpacked whenever it is present.
func peerRequestBounds(frame []byte) (err error) {
	return peerRequestBoundsWith(frame, newPeerBounds())
}

// peerRequestBoundsWith is the walk behind peerRequestBounds with an explicit
// budget, so tests can prove multiplicity accounting without large frames.
func peerRequestBoundsWith(frame []byte, bounds *peerBounds) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.New("peer bounds: malformed request frame")
		}
	}()
	if len(frame) > maxPeerFrame {
		return errPeerFrameOverflow
	}
	root := peerTableAt(frame, peerIndirect(frame, 0))
	bounds.chargeObject()
	bounds.chargeString(root, peerFieldVOffsetT(2))
	bounds.chargeString(root, peerFieldVOffsetT(3))
	bounds.chargeString(root, peerFieldVOffsetT(5))
	if nested, ok := peerTableField(root, peerFieldVOffsetT(6)); ok {
		bounds.walkRecordRequest(nested)
	}
	bounds.chargeByteVector(root, peerFieldVOffsetT(7))
	if nested, ok := peerTableField(root, peerFieldVOffsetT(8)); ok {
		bounds.walkDecision(nested)
	}
	bounds.chargeByteVector(root, peerFieldVOffsetT(11))
	return nil
}

// peerResponseBounds walks everything Response.UnPackTo materializes. Responses
// carry no file identifier, so this must not require one.
func peerResponseBounds(frame []byte) (err error) {
	return peerResponseBoundsWith(frame, newPeerBounds())
}

func peerResponseBoundsWith(frame []byte, bounds *peerBounds) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("peer bounds: malformed response frame: %v", recovered)
		}
	}()
	if len(frame) > maxPeerFrame {
		return errPeerFrameOverflow
	}
	root := peerTableAt(frame, peerIndirect(frame, 0))
	bounds.chargeObject()
	bounds.chargeString(root, peerFieldVOffsetT(1))
	if nested, ok := peerTableField(root, peerFieldVOffsetT(2)); ok {
		bounds.walkSummary(nested)
	}
	if nested, ok := peerTableField(root, peerFieldVOffsetT(3)); ok {
		bounds.walkDecidedValue(nested)
	}
	bounds.chargeString(root, peerFieldVOffsetT(4))
	bounds.chargeString(root, peerFieldVOffsetT(5))
	if start, count, ok := peerVector(root, peerFieldVOffsetT(8)); ok {
		bounds.chargeObjectSlots(count)
		for index := 0; index < count; index++ {
			bounds.walkDecidedValue(peerVectorElement(root, start, count, index))
		}
	}
	bounds.chargeByteVector(root, peerFieldVOffsetT(10))
	return nil
}
