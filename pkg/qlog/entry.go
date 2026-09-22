package qlog

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

var entryCRCTable = crc32.MakeTable(crc32.Castagnoli)

const (
	entryHeaderSize             = 53
	entryRecordCRCOffset        = 45
	entryHeaderCRCOffset        = 49
	entryFormatMask      uint32 = 3 << 30
	entryLengthMarker    uint32 = 2 << 30
	entryLengthMask      uint32 = (1 << 30) - 1
)

func entryPayloadLength(encoded uint32) (uint32, bool) {
	return encoded & entryLengthMask, encoded&entryFormatMask == entryLengthMarker
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
	payloadLen := len(e.Payload)
	// Layout: Slot(8) + Hash(32) + Type(1) + PayloadLen(4) + RecordCRC(4) + HeaderCRC(4) + Payload
	buf := make([]byte, 0, entryHeaderSize+payloadLen)

	// Slot
	buf = binary.LittleEndian.AppendUint64(buf, e.Slot)

	// Hash
	buf = append(buf, e.Hash[:]...)

	// Type
	buf = append(buf, byte(e.Type))

	// Payload length
	buf = binary.LittleEndian.AppendUint32(buf, uint32(payloadLen)|entryLengthMarker)

	// The marker rejects bytes written by any other WAL layout.
	buf = binary.LittleEndian.AppendUint32(buf, 0)
	buf = binary.LittleEndian.AppendUint32(buf, 0)

	// Payload
	buf = append(buf, e.Payload...)
	crc := crc32.Update(crc32.Checksum(buf[:entryRecordCRCOffset], entryCRCTable), entryCRCTable, buf[entryHeaderSize:])
	binary.LittleEndian.PutUint32(buf[entryRecordCRCOffset:entryHeaderCRCOffset], crc)
	binary.LittleEndian.PutUint32(buf[entryHeaderCRCOffset:entryHeaderSize], crc32.Checksum(buf[:entryHeaderCRCOffset], entryCRCTable))

	return buf
}

// DecodeEntry deserializes an entry from bytes.
func DecodeEntry(data []byte) (Entry, int, error) {
	payloadLen, storedCRC, err := decodeEntryHeader(data)
	if err != nil {
		return Entry{}, 0, err
	}

	entry := Entry{
		Slot: binary.LittleEndian.Uint64(data[0:8]),
		Type: EntryType(data[40]),
	}
	copy(entry.Hash[:], data[8:40])

	totalLen := entryHeaderSize + int(payloadLen)
	if len(data) < totalLen {
		return Entry{}, 0, io.ErrUnexpectedEOF
	}

	actualCRC := crc32.Update(crc32.Checksum(data[:entryRecordCRCOffset], entryCRCTable), entryCRCTable, data[entryHeaderSize:totalLen])
	if storedCRC != actualCRC {
		return Entry{}, 0, fmt.Errorf("CRC mismatch: stored=%08x actual=%08x", storedCRC, actualCRC)
	}

	entry.Payload = make([]byte, payloadLen)
	copy(entry.Payload, data[entryHeaderSize:totalLen])

	return entry, totalLen, nil
}

// decodeEntryHeader never exposes a length until its independent checksum passes.
func decodeEntryHeader(data []byte) (uint32, uint32, error) {
	if len(data) >= entryRecordCRCOffset {
		if _, current := entryPayloadLength(binary.LittleEndian.Uint32(data[41:45])); !current {
			return 0, 0, fmt.Errorf("unsupported WAL entry format; preserve old data and migrate logically")
		}
	}
	if len(data) < entryHeaderSize {
		return 0, 0, io.ErrUnexpectedEOF
	}
	stored := binary.LittleEndian.Uint32(data[entryHeaderCRCOffset:entryHeaderSize])
	if actual := crc32.Checksum(data[:entryHeaderCRCOffset], entryCRCTable); stored != actual {
		return 0, 0, fmt.Errorf("WAL header CRC mismatch: stored=%08x actual=%08x", stored, actual)
	}
	length, _ := entryPayloadLength(binary.LittleEndian.Uint32(data[41:45]))
	return length, binary.LittleEndian.Uint32(data[entryRecordCRCOffset:entryHeaderCRCOffset]), nil
}
