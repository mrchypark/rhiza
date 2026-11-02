package types

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const MaxManagedKVExecutionBytes = MaxManagedKVOriginalBytes + 128
const MaxManagedKVCommandsPerDecidedValue = 64

var managedKVCommandMagic = []byte("QKVC\x01")
var managedKVBatchMagic = []byte("QKVB\x01")

// EncodeManagedKVBatchItem encodes the explicit mixed-profile execution record.
// It does not select a profile or change legacy encoders.
func EncodeManagedKVBatchItem(command KVCommand) ([]byte, error) {
	original, _, err := EncodeManagedKVOriginal(command)
	if err != nil {
		return nil, err
	}
	times, err := json.Marshal(struct {
		Observed int64 `json:"observed_at_unix_ms"`
		Expires  int64 `json:"expires_at_unix_ms"`
	}{command.ObservedAtUnixMS, command.ExpiresAtUnixMS})
	if err != nil {
		return nil, err
	}
	out := append(bytes.Clone(original[:len(original)-1]), ',')
	out = append(out, times[1:]...)
	return out, nil
}

func EncodeManagedKVCommand(command KVCommand) ([]byte, error) {
	item, err := EncodeManagedKVBatchItem(command)
	if err != nil {
		return nil, err
	}
	return append(bytes.Clone(managedKVCommandMagic), item...), nil
}

// decodeManagedKVExecution delegates the seven intent fields to the original
// strict codec; only the two required execution timestamps are decoded here.
func decodeManagedKVExecution(raw []byte) (KVCommand, error) {
	invalid := errors.New("invalid managed KV execution")
	if len(raw) == 0 || len(raw) > MaxManagedKVExecutionBytes {
		return KVCommand{}, invalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	first, err := d.Token()
	if err != nil || first != json.Delim('{') {
		return KVCommand{}, invalid
	}
	original := []byte{'{'}
	var observed, expires int64
	var seen uint8
	for d.More() {
		token, err := d.Token()
		key, ok := token.(string)
		if err != nil || !ok {
			return KVCommand{}, invalid
		}
		var value json.RawMessage
		if d.Decode(&value) != nil {
			return KVCommand{}, invalid
		}
		var bit uint8
		var target *int64
		switch key {
		case "observed_at_unix_ms":
			bit, target = 1, &observed
		case "expires_at_unix_ms":
			bit, target = 2, &expires
		default:
			if len(original) > 1 {
				original = append(original, ',')
			}
			name, _ := json.Marshal(key)
			original = append(original, name...)
			original = append(original, ':')
			original = append(original, value...)
			continue
		}
		if seen&bit != 0 || bytes.Equal(bytes.TrimSpace(value), []byte("null")) || json.Unmarshal(value, target) != nil {
			return KVCommand{}, invalid
		}
		seen |= bit
	}
	last, err := d.Token()
	if err != nil || last != json.Delim('}') || seen != 3 {
		return KVCommand{}, invalid
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return KVCommand{}, invalid
	}
	command, err := DecodeManagedKVOriginal(append(original, '}'))
	if err != nil {
		return KVCommand{}, err
	}
	command.ObservedAtUnixMS, command.ExpiresAtUnixMS = observed, expires
	return command, nil
}

func DecodeManagedKVCommand(value []byte) (KVCommand, bool, error) {
	if !bytes.HasPrefix(value, managedKVCommandMagic) {
		return KVCommand{}, false, nil
	}
	command, err := decodeManagedKVExecution(value[len(managedKVCommandMagic):])
	return command, true, err
}

func AssembleManagedKVBatch(items [][]byte) ([]byte, error) {
	if len(items) == 0 || len(items) > MaxManagedKVCommandsPerDecidedValue {
		return nil, errors.New("invalid managed KV batch count")
	}
	for _, item := range items {
		if _, err := decodeManagedKVExecution(item); err != nil {
			return nil, err
		}
	}
	return assembleBatch(managedKVBatchMagic, items), nil
}

func DecodeManagedKVBatch(value []byte) ([]KVCommand, bool, error) {
	if !bytes.HasPrefix(value, managedKVBatchMagic) {
		return nil, false, nil
	}
	invalid := errors.New("invalid managed KV batch")
	if len(value) > len(managedKVBatchMagic)+2+MaxManagedKVCommandsPerDecidedValue*(MaxManagedKVExecutionBytes+1) {
		return nil, true, invalid
	}
	d := json.NewDecoder(bytes.NewReader(value[len(managedKVBatchMagic):]))
	first, err := d.Token()
	if err != nil || first != json.Delim('[') {
		return nil, true, invalid
	}
	var commands []KVCommand
	for d.More() {
		if len(commands) == MaxManagedKVCommandsPerDecidedValue {
			return nil, true, invalid
		}
		var raw json.RawMessage
		if d.Decode(&raw) != nil {
			return nil, true, invalid
		}
		command, err := decodeManagedKVExecution(raw)
		if err != nil {
			return nil, true, err
		}
		commands = append(commands, command)
	}
	last, err := d.Token()
	var extra any
	if err != nil || last != json.Delim(']') || len(commands) == 0 || d.Decode(&extra) != io.EOF {
		return nil, true, invalid
	}
	return commands, true, nil
}

// ManagedKVFingerprint hashes lossless intent in a separate domain. Only this
// projection equates nil and empty blobs; execution preserves both exactly.
func ManagedKVFingerprint(command KVCommand) ([32]byte, error) {
	if err := validateManagedKVOriginal(command); err != nil {
		return [32]byte{}, err
	}
	wire := managedKVOriginalWire(command)
	wire.RequestID = nil
	if len(wire.Value) == 0 {
		wire.Value = nil
	}
	if len(wire.Expected) == 0 {
		wire.Expected = nil
	}
	item, err := json.Marshal(wire)
	if err != nil {
		return [32]byte{}, err
	}
	return FingerprintBatchItem("rhiza/managed-kv/v1", item), nil
}

// ValidateManagedKVExecutionPolicy is the explicit mixed-profile allowlist.
// SQL semantics remain unchanged; legacy KV, Graph and Notify are not admitted.
// The execution limit is the consensus limit, not the codec's parsing ceiling.
func ValidateManagedKVExecutionPolicy(value []byte) error {
	if len(value) == 0 || len(value) > quepaxa.MaxReplicatedValueBytes {
		return sqlpolicy.ErrIncompatible
	}
	if _, ok, err := DecodeSQLBatch(value); ok || err != nil {
		return err
	}
	if _, ok, err := DecodeManagedKVCommand(value); ok || err != nil {
		return err
	}
	if _, ok, err := DecodeManagedKVBatch(value); ok || err != nil {
		return err
	}
	if ok, err := DecodeReadBarrier(value); ok || err != nil {
		return err
	}
	if _, ok, err := DecodeCheckpointSeal(value); ok || err != nil {
		return err
	}
	if _, ok, err := DecodeLeaderSchedule(value); ok || err != nil {
		return err
	}
	if ok, err := DecodeReconfiguration(value); ok || err != nil {
		return err
	}
	return sqlpolicy.ErrIncompatible
}
