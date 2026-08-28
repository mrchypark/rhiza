package materializer

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/mrchypark/rhiza/internal/types"
)

const MaxGraphStreamRecords = 1_000

// ValidateGraphCommand keeps replicated Cypher deterministic and bounded.
func ValidateGraphCommand(command types.GraphCommand) error {
	if command.RequestID == "" || len(command.RequestID) > types.MaxRequestIDBytes {
		return fmt.Errorf("request_id is required and must not exceed %d bytes", types.MaxRequestIDBytes)
	}
	cypher := strings.TrimSpace(command.Cypher)
	operations := 0
	if cypher != "" {
		operations++
	}
	if command.StreamOffset != nil {
		operations++
	}
	if command.StreamTrim != nil {
		operations++
	}
	if operations != 1 {
		return fmt.Errorf("exactly one graph mutation is required")
	}
	if cypher != "" && len(cypher) > MaxSQLBytes {
		return fmt.Errorf("cypher is required and must not exceed %d bytes", MaxSQLBytes)
	}
	if cypher != "" && strings.Count(strings.TrimSuffix(cypher, ";"), ";") != 0 {
		return fmt.Errorf("exactly one cypher statement is allowed")
	}
	if len(command.Args) > MaxSQLArgs {
		return fmt.Errorf("cypher has more than %d arguments", MaxSQLArgs)
	}
	for key, value := range command.Args {
		if key == "" || len(key) > 256 {
			return fmt.Errorf("invalid cypher parameter name")
		}
		if _, err := graphArg(value); err != nil {
			return fmt.Errorf("parameter %q: %w", key, err)
		}
	}
	for i, event := range command.Events {
		if cypher == "" {
			return fmt.Errorf("stream events require a cypher mutation")
		}
		if err := validateGraphStreamName(event.Stream, false); err != nil {
			return fmt.Errorf("event %d: %w", i, err)
		}
		if !utf8.ValidString(event.Kind) || event.Kind == "" || len(event.Kind) > 255 {
			return fmt.Errorf("event %d: kind is required and must be valid UTF-8 not exceeding 255 bytes", i)
		}
		if _, err := graphArg(event.Payload); err != nil {
			return fmt.Errorf("event %d payload: %w", i, err)
		}
	}
	if command.StreamOffset != nil {
		if err := validateGraphStreamName(command.StreamOffset.Stream, true); err != nil {
			return err
		}
		if !utf8.ValidString(command.StreamOffset.Consumer) || command.StreamOffset.Consumer == "" || len(command.StreamOffset.Consumer) > 255 {
			return fmt.Errorf("consumer is required and must be valid UTF-8 not exceeding 255 bytes")
		}
	}
	if command.StreamTrim != nil {
		if err := validateGraphStreamName(command.StreamTrim.Stream, true); err != nil {
			return err
		}
	}
	lower := strings.ToLower(cypher)
	for _, denied := range []string{"random(", "uuid(", "current_timestamp", "current_date", "current_time", "load from", "copy from", "install ", "load extension", "import database", "export database", "attach "} {
		if strings.Contains(lower, denied) {
			return fmt.Errorf("non-deterministic or external cypher operation %q is not allowed", denied)
		}
	}
	return nil
}

func validateGraphStreamName(name string, allowReserved bool) error {
	if !utf8.ValidString(name) || name == "" || len(name) > 255 {
		return fmt.Errorf("stream is required and must be valid UTF-8 not exceeding 255 bytes")
	}
	if !allowReserved && strings.HasPrefix(name, "__lattice_") {
		return fmt.Errorf("stream names beginning with __lattice_ are reserved")
	}
	return nil
}

func graphArg(value any) (any, error) {
	switch value := value.(type) {
	case nil, bool, string, float64:
		return value, nil
	case float32:
		return float64(value), nil
	case int:
		return int64(value), nil
	case int8:
		return int64(value), nil
	case int16:
		return int64(value), nil
	case int32:
		return int64(value), nil
	case int64:
		return value, nil
	case uint:
		if uint64(value) > math.MaxInt64 {
			return nil, fmt.Errorf("integer exceeds int64")
		}
		return int64(value), nil
	case uint8:
		return int64(value), nil
	case uint16:
		return int64(value), nil
	case uint32:
		return int64(value), nil
	case uint64:
		if value > math.MaxInt64 {
			return nil, fmt.Errorf("integer exceeds int64")
		}
		return int64(value), nil
	case json.Number:
		if integer, err := value.Int64(); err == nil {
			return integer, nil
		}
		return value.Float64()
	case []any:
		result := make([]any, len(value))
		for i := range value {
			converted, err := graphArg(value[i])
			if err != nil {
				return nil, err
			}
			result[i] = converted
		}
		return result, nil
	case map[string]any:
		result := make(map[string]any, len(value))
		for key, item := range value {
			converted, err := graphArg(item)
			if err != nil {
				return nil, err
			}
			result[key] = converted
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported graph argument type %T", value)
	}
}
