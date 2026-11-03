package types

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const MaxManagedGraphExecutionBytes = 256 << 10

var managedGraphExecutionMagic = []byte("QGRX\x01")

// EncodeManagedGraphExecution stores one original, not duplicate original,
// intent and execution bodies. Profile selection belongs to trusted startup.
func EncodeManagedGraphExecution(command GraphCommand) ([]byte, error) {
	original, err := EncodeManagedGraphOriginal(command)
	if err != nil {
		return nil, err
	}
	if _, err := managedGraphExecutionProjectionValidated(command); err != nil {
		return nil, err
	}
	out := make([]byte, len(managedGraphExecutionMagic)+4+len(original))
	copy(out, managedGraphExecutionMagic)
	binary.BigEndian.PutUint32(out[5:9], uint32(len(original)))
	copy(out[9:], original)
	return out, nil
}

func DecodeManagedGraphExecution(value []byte) (GraphCommand, bool, error) {
	if !bytes.HasPrefix(value, []byte("QGRX")) {
		return GraphCommand{}, false, nil
	}
	if len(value) < 9 || len(value) > MaxManagedGraphExecutionBytes || !bytes.HasPrefix(value, managedGraphExecutionMagic) || uint64(binary.BigEndian.Uint32(value[5:9])) != uint64(len(value)-9) {
		return GraphCommand{}, true, fmt.Errorf("invalid managed Graph execution frame")
	}
	original, err := DecodeManagedGraphOriginal(value[9:])
	if err != nil {
		return GraphCommand{}, true, err
	}
	if _, err = managedGraphExecutionProjectionValidated(original); err != nil {
		return GraphCommand{}, true, err
	}
	return original, true, nil
}

// cloneGraphCommandForOrdinaryEncoding reproduces the existing ordinary clone's
// container normalization before JSON. It is not graphArg's numeric conversion.
// Raw byte strings survive here; JSON's historical replacement happens later.
func cloneGraphCommandForOrdinaryEncoding(command GraphCommand) (GraphCommand, error) {
	args, err := cloneGraphOriginalValue(command.Args, 0)
	if err != nil {
		return GraphCommand{}, err
	}
	command.Args = args.(map[string]any)
	if command.Events != nil {
		events := make([]GraphStreamEvent, len(command.Events))
		copy(events, command.Events)
		for i := range events {
			events[i].Payload, err = cloneGraphOriginalValue(events[i].Payload, 0)
			if err != nil {
				return GraphCommand{}, err
			}
		}
		command.Events = events
	}
	if command.StreamOffset != nil {
		v := *command.StreamOffset
		command.StreamOffset = &v
	}
	if command.StreamTrim != nil {
		v := *command.StreamTrim
		command.StreamTrim = &v
	}
	return command, nil
}
func cloneGraphOriginalValue(value any, depth int) (any, error) {
	if depth > 32 {
		return nil, fmt.Errorf("graph clone exceeds depth")
	}
	switch v := value.(type) {
	case []any:
		out := make([]any, len(v))
		for i := range v {
			item, err := cloneGraphOriginalValue(v[i], depth+1)
			if err != nil {
				return nil, err
			}
			out[i] = item
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			clone, err := cloneGraphOriginalValue(item, depth+1)
			if err != nil {
				return nil, err
			}
			out[key] = clone
		}
		return out, nil
	case nil, bool, string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, json.Number:
		return v, nil
	default:
		return nil, fmt.Errorf("unsupported graph clone type %T", value)
	}
}

// ManagedGraphIntentFingerprint is I(raw): the exact ordinary-clone JSON with
// only the ID cleared, under the existing Graph intent domain. It is not E(raw).
func ManagedGraphIntentFingerprint(command GraphCommand) ([32]byte, error) {
	if _, err := EncodeManagedGraphOriginal(command); err != nil {
		return [32]byte{}, err
	}
	clone, err := cloneGraphCommandForOrdinaryEncoding(command)
	if err != nil {
		return [32]byte{}, err
	}
	clone.RequestID = ""
	item, err := EncodeGraphBatchItem(clone)
	if err != nil {
		return [32]byte{}, err
	}
	return FingerprintBatchItem("rhiza/managed-graph-intent/v1", item), nil
}

// ManagedGraphExecutionProjection is E(raw): existing JSON/UseNumber semantics.
// Request identity remains the lossless original; it is not a Cypher argument.
// The old singleton QGRB value must still fit its actual 128KiB boundary.
func ManagedGraphExecutionProjection(command GraphCommand) (GraphCommand, error) {
	if _, err := EncodeManagedGraphOriginal(command); err != nil {
		return GraphCommand{}, err
	}
	return managedGraphExecutionProjectionValidated(command)
}

func managedGraphExecutionProjectionValidated(command GraphCommand) (GraphCommand, error) {
	clone, err := cloneGraphCommandForOrdinaryEncoding(command)
	if err != nil {
		return GraphCommand{}, err
	}
	encoded, err := EncodeGraphCommand(clone)
	if err != nil {
		return GraphCommand{}, err
	}
	if len(encoded) > quepaxa.MaxReplicatedValueBytes {
		return GraphCommand{}, fmt.Errorf("ordinary Graph execution exceeds existing value limit")
	}
	commands, ok, err := DecodeGraphBatch(encoded)
	if err != nil || !ok || len(commands) != 1 {
		return GraphCommand{}, fmt.Errorf("invalid ordinary Graph execution projection: %w", err)
	}
	commands[0].RequestID = command.RequestID
	return commands[0], nil
}

// ValidateManagedGraphExecutionPolicy is installed only by trusted policy10
// construction. Old policies must continue using their existing validators.
func ValidateManagedGraphExecutionPolicy(value []byte) error {
	if _, ok, err := DecodeManagedGraphExecution(value); ok || err != nil {
		return err
	}
	if len(value) > quepaxa.MaxReplicatedValueBytes {
		return sqlpolicy.ErrIncompatible
	}
	return ValidateManagedKVExecutionPolicy(value)
}
