package encoding

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/mrchypark/rhiza/internal/types"
)

// Entry is a QLog entry for encoding/decoding.
type Entry struct {
	Slot    types.Slot
	Hash    types.ValueHash
	Payload []byte
}

// EncodeEntries serializes entries to a compact binary format.
func EncodeEntries(entries []Entry) ([]byte, error) {
	var buf bytes.Buffer

	// Header: entry count
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(entries))); err != nil {
		return nil, fmt.Errorf("write count: %w", err)
	}

	for _, e := range entries {
		// Slot (8 bytes)
		if err := binary.Write(&buf, binary.LittleEndian, uint64(e.Slot)); err != nil {
			return nil, fmt.Errorf("write slot: %w", err)
		}

		// Hash (32 bytes)
		if _, err := buf.Write(e.Hash[:]); err != nil {
			return nil, fmt.Errorf("write hash: %w", err)
		}

		// Payload length + payload
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(e.Payload))); err != nil {
			return nil, fmt.Errorf("write payload length: %w", err)
		}
		if _, err := buf.Write(e.Payload); err != nil {
			return nil, fmt.Errorf("write payload: %w", err)
		}
	}

	return buf.Bytes(), nil
}

// DecodeEntries deserializes entries from a binary format.
func DecodeEntries(r io.Reader) ([]Entry, error) {
	var count uint32
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return nil, fmt.Errorf("read count: %w", err)
	}

	entries := make([]Entry, count)
	for i := uint32(0); i < count; i++ {
		var slot uint64
		if err := binary.Read(r, binary.LittleEndian, &slot); err != nil {
			return nil, fmt.Errorf("read slot: %w", err)
		}
		entries[i].Slot = types.Slot(slot)

		if _, err := io.ReadFull(r, entries[i].Hash[:]); err != nil {
			return nil, fmt.Errorf("read hash: %w", err)
		}

		var payloadLen uint32
		if err := binary.Read(r, binary.LittleEndian, &payloadLen); err != nil {
			return nil, fmt.Errorf("read payload length: %w", err)
		}

		entries[i].Payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(r, entries[i].Payload); err != nil {
			return nil, fmt.Errorf("read payload: %w", err)
		}
	}

	return entries, nil
}

// EncodeJSON encodes any value as JSON.
func EncodeJSON(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// DecodeJSON decodes JSON into the given value.
func DecodeJSON(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
