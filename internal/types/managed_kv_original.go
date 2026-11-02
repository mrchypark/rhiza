package types

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

const MaxManagedKVOriginalBytes = 300 << 10

// ManagedKVOriginal is the typed mixed-protocol intent, not a timestamped
// execution command. Byte fields use JSON's base64 representation exactly once.
// Value and Expected deliberately retain null versus non-nil empty bytes.
// Use the codec helpers to validate this representation before trusting it.
type ManagedKVOriginal struct {
	RequestID      []byte `json:"request_id_b64"`
	Operation      string `json:"operation"`
	Key            []byte `json:"key_b64"`
	Value          []byte `json:"value"`
	Expected       []byte `json:"expected"`
	ExpectedExists bool   `json:"expected_exists"`
	TTLMS          int64  `json:"ttl_ms"`
}

// EncodeManagedKVOriginal returns canonical JSON and a detached logical intent.
// Observed/expiry timestamps are execution state and are always cleared.
func EncodeManagedKVOriginal(command KVCommand) (json.RawMessage, KVCommand, error) {
	if err := validateManagedKVOriginal(command); err != nil {
		return nil, KVCommand{}, err
	}
	owned := KVCommand{RequestID: strings.Clone(command.RequestID), Operation: strings.Clone(command.Operation), Key: strings.Clone(command.Key), Value: bytes.Clone(command.Value), Expected: bytes.Clone(command.Expected), ExpectedExists: command.ExpectedExists, TTLMS: command.TTLMS}
	wire := managedKVOriginalWire(owned)
	encoded, err := json.Marshal(wire)
	if err != nil || len(encoded) > MaxManagedKVOriginalBytes {
		return nil, KVCommand{}, errors.New("invalid managed KV original encoding")
	}
	return encoded, owned, nil
}

func managedKVOriginalWire(command KVCommand) ManagedKVOriginal {
	return ManagedKVOriginal{RequestID: []byte(command.RequestID), Operation: command.Operation, Key: []byte(command.Key), Value: command.Value, Expected: command.Expected, ExpectedExists: command.ExpectedExists, TTLMS: command.TTLMS}
}

// DecodeManagedKVOriginal accepts JSON object ordering/escaping differences, but
// rejects duplicate, missing, unknown and noncanonical base64 field values.
func DecodeManagedKVOriginal(raw json.RawMessage) (KVCommand, error) {
	invalid := errors.New("invalid managed KV original")
	if len(raw) == 0 || len(raw) > MaxManagedKVOriginalBytes {
		return KVCommand{}, invalid
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return KVCommand{}, invalid
	}
	var fields [7]json.RawMessage
	var seen uint8
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok {
			return KVCommand{}, invalid
		}
		var index uint8
		switch key {
		case "request_id_b64":
			index = 0
		case "operation":
			index = 1
		case "key_b64":
			index = 2
		case "value":
			index = 3
		case "expected":
			index = 4
		case "expected_exists":
			index = 5
		case "ttl_ms":
			index = 6
		default:
			return KVCommand{}, invalid
		}
		bit := uint8(1) << index
		if seen&bit != 0 || decoder.Decode(&fields[index]) != nil {
			return KVCommand{}, invalid
		}
		seen |= bit
	}
	last, err := decoder.Token()
	if err != nil || last != json.Delim('}') || seen != 127 {
		return KVCommand{}, invalid
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return KVCommand{}, invalid
	}
	var wire ManagedKVOriginal
	for _, field := range []struct {
		index    int
		target   *[]byte
		nullable bool
	}{
		{0, &wire.RequestID, false}, {2, &wire.Key, false}, {3, &wire.Value, true}, {4, &wire.Expected, true},
	} {
		value, err := decodeManagedKVOriginalBytes(fields[field.index], field.nullable)
		if err != nil {
			return KVCommand{}, invalid
		}
		*field.target = value
	}
	for _, index := range []int{1, 5, 6} {
		if bytes.Equal(bytes.TrimSpace(fields[index]), []byte("null")) {
			return KVCommand{}, invalid
		}
	}
	if json.Unmarshal(fields[1], &wire.Operation) != nil || json.Unmarshal(fields[5], &wire.ExpectedExists) != nil || json.Unmarshal(fields[6], &wire.TTLMS) != nil {
		return KVCommand{}, invalid
	}
	command := KVCommand{RequestID: string(wire.RequestID), Operation: wire.Operation, Key: string(wire.Key), Value: bytes.Clone(wire.Value), Expected: bytes.Clone(wire.Expected), ExpectedExists: wire.ExpectedExists, TTLMS: wire.TTLMS}
	if err := validateManagedKVOriginal(command); err != nil {
		return KVCommand{}, err
	}
	canonical, err := json.Marshal(wire)
	if err != nil || len(canonical) > MaxManagedKVOriginalBytes {
		return KVCommand{}, invalid
	}
	return command, nil
}

func decodeManagedKVOriginalBytes(raw json.RawMessage, nullable bool) ([]byte, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if nullable {
			return nil, nil
		}
		return nil, errors.New("managed KV identity bytes cannot be null")
	}
	var text string
	if json.Unmarshal(raw, &text) != nil {
		return nil, errors.New("invalid managed KV original bytes")
	}
	value, err := base64.StdEncoding.Strict().DecodeString(text)
	if err != nil || base64.StdEncoding.EncodeToString(value) != text {
		return nil, errors.New("noncanonical managed KV original base64")
	}
	return value, nil
}

// ManagedKVPendingWireID encodes transport identity only. Application execution
// and receipt lookup must continue to use the original logical RequestID.
func ManagedKVPendingWireID(requestID string) (string, error) {
	if len(requestID) == 0 || len(requestID) > MaxRequestIDBytes {
		return "", errors.New("invalid managed KV request ID")
	}
	return base64.StdEncoding.EncodeToString([]byte(requestID)), nil
}

func validateManagedKVOriginal(command KVCommand) error {
	const chargeLimit = 1 << 20
	invalid := errors.New("invalid managed KV original input")
	if command.Operation != "put" && command.Operation != "delete" && command.Operation != "cas" {
		return invalid
	}
	if len(command.RequestID) == 0 || len(command.RequestID) > MaxRequestIDBytes || len(command.Key) == 0 || len(command.Key) > 1024 || command.TTLMS < 0 {
		return invalid
	}
	// Mirror pkg/network's current pure KV mutationCharge, before copying. These
	// individual bounds also make all subsequent integer arithmetic bounded.
	if len(command.Value) > chargeLimit/5 || len(command.Expected) > chargeLimit/5 {
		return invalid
	}
	stringsBytes := len(command.RequestID) + len(command.Operation) + len(command.Key)
	blobBytes := len(command.Value) + len(command.Expected)
	if 192+13*stringsBytes+5*blobBytes > chargeLimit {
		return invalid
	}
	return nil
}
