package qlog

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// EntryType is the type of QLog entry.
type EntryType uint8

const (
	EntryProposal EntryType = iota // 제안된 값
	EntryReceipt                   // receipt 기록
	EntryDecide                    // quorum 도달 (결정)
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
	// Layout: Slot(8) + Hash(32) + Type(1) + PayloadLen(4) + CRC(4) + Payload
	buf := make([]byte, 0, 8+32+1+4+4+payloadLen)

	// Slot
	buf = binary.LittleEndian.AppendUint64(buf, e.Slot)

	// Hash
	buf = append(buf, e.Hash[:]...)

	// Type
	buf = append(buf, byte(e.Type))

	// Payload length
	buf = binary.LittleEndian.AppendUint32(buf, uint32(payloadLen))

	// CRC32 (over everything so far)
	crc := crc32.ChecksumIEEE(buf)
	buf = binary.LittleEndian.AppendUint32(buf, crc)

	// Payload
	buf = append(buf, e.Payload...)

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

	payloadLen := binary.LittleEndian.Uint32(data[41:45])
	storedCRC := binary.LittleEndian.Uint32(data[45:49])

	// Verify CRC (over bytes 0-45, before CRC field)
	actualCRC := crc32.ChecksumIEEE(data[0:45])
	if storedCRC != actualCRC {
		return Entry{}, 0, fmt.Errorf("CRC mismatch: stored=%08x actual=%08x", storedCRC, actualCRC)
	}

	totalLen := 49 + int(payloadLen)
	if len(data) < totalLen {
		return Entry{}, 0, io.ErrUnexpectedEOF
	}

	entry.Payload = make([]byte, payloadLen)
	copy(entry.Payload, data[49:totalLen])

	return entry, totalLen, nil
}
