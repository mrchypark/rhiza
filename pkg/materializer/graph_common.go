package materializer

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mrchypark/rhiza/internal/types"
)

// ValidateGraphCommand keeps replicated Cypher deterministic and bounded.
func ValidateGraphCommand(command types.GraphCommand) error {
	if command.RequestID == "" || len(command.RequestID) > types.MaxRequestIDBytes {
		return fmt.Errorf("request_id is required and must not exceed %d bytes", types.MaxRequestIDBytes)
	}
	cypher := strings.TrimSpace(command.Cypher)
	if cypher == "" || len(cypher) > MaxSQLBytes {
		return fmt.Errorf("cypher is required and must not exceed %d bytes", MaxSQLBytes)
	}
	if strings.Count(strings.TrimSuffix(cypher, ";"), ";") != 0 {
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
	lower := strings.ToLower(cypher)
	for _, denied := range []string{"random(", "uuid(", "current_timestamp", "current_date", "current_time", "load from", "copy from", "install ", "load extension", "import database", "export database", "attach "} {
		if strings.Contains(lower, denied) {
			return fmt.Errorf("non-deterministic or external cypher operation %q is not allowed", denied)
		}
	}
	return nil
}

func graphArg(value any) (any, error) {
	switch value := value.(type) {
	case nil, bool, string, float64:
		return value, nil
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
