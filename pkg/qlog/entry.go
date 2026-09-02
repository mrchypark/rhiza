package qlog

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

var entryCRCTable = crc32.MakeTable(crc32.Castagnoli)

const (
	entryLengthMarker uint32 = 3 << 30
	entryLengthMask   uint32 = ^entryLengthMarker
)

func entryPayloadLength(encoded uint32) (uint32, bool) {
	return encoded & entryLengthMask, encoded&entryLengthMarker == entryLengthMarker
}

// EntryType is the type of QLog entry.
type EntryType uint8

const (
	EntryProposal           EntryType = iota // 제안된 값
	EntryReceipt                             // receipt 기록
	EntryDecide                              // quorum 도달 (결정)
	EntryCheckpoint                          // certified compacted prefix floor
	EntryCheckpointVerified                  // locally verified checkpoint root
)

// Entry is a single QLog entry.
type Entry struct {
	Slot    uint64
	Hash    [32]byte
	Type    EntryType
	Payload []byte
}

// Encode serializes an entry to bytes with CRC32 checksum.
func (e Entry) Encode() []byte {
	return e.appendEncoded(make([]byte, 0, e.encodedLen()))
}

func (e Entry) encodedLen() int {
	return 49 + len(e.Payload)
}

func (e Entry) appendEncoded(buf []byte) []byte {
	start := len(buf)
	payloadLen := len(e.Payload)
	// Layout: Slot(8) + Hash(32) + Type(1) + PayloadLen(4) + CRC(4) + Payload
	buf = binary.LittleEndian.AppendUint64(buf, e.Slot)
	buf = append(buf, e.Hash[:]...)
	buf = append(buf, byte(e.Type))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(payloadLen)|entryLengthMarker)
	buf = binary.LittleEndian.AppendUint32(buf, 0)
	buf = append(buf, e.Payload...)
	crc := crc32.Update(crc32.Checksum(buf[start:start+45], entryCRCTable), entryCRCTable, buf[start+49:])
	binary.LittleEndian.PutUint32(buf[start+45:start+49], crc)

	return buf
}

// DecodeEntry deserializes an entry from bytes.
func DecodeEntry(data []byte) (Entry, int, error) {
	if len(data) < 49 { // 8+32+1+4+4 minimum
		return Entry{}, 0, io.ErrUnexpectedEOF
	}

	entry := Entry{
		Slot: binary.LittleEndian.Uint64(data[0:8]),
		Type: EntryType(data[40]),
	}
	copy(entry.Hash[:], data[8:40])

	payloadLen, current := entryPayloadLength(binary.LittleEndian.Uint32(data[41:45]))
	if !current {
		return Entry{}, 0, fmt.Errorf("unknown WAL entry format")
	}
	storedCRC := binary.LittleEndian.Uint32(data[45:49])

	totalLen := 49 + int(payloadLen)
	if len(data) < totalLen {
		return Entry{}, 0, io.ErrUnexpectedEOF
	}

	actualCRC := crc32.Update(crc32.Checksum(data[:45], entryCRCTable), entryCRCTable, data[49:totalLen])
	if storedCRC != actualCRC {
		return Entry{}, 0, fmt.Errorf("CRC mismatch: stored=%08x actual=%08x", storedCRC, actualCRC)
	}

	entry.Payload = make([]byte, payloadLen)
	copy(entry.Payload, data[49:totalLen])

	return entry, totalLen, nil
}
