//go:build !graph

package materializer

import (
	"context"
	"fmt"

	"github.com/mrchypark/rhiza/internal/types"
)

type graphState struct{}

func BuildProfile() types.Profile { return types.ProfileSQL }
func GraphEnabled() bool          { return false }

func openGraph(string, uint64) (*graphState, error) { return nil, nil }
func (*graphState) close()                          {}

func (m *Materializer) applyGraph(_ uint64, _ []byte, _ types.GraphCommand, graph bool) error {
	if graph {
		return fmt.Errorf("graph command is not supported by the sql-kv build")
	}
	return nil
}

func (*Materializer) GraphQuery(context.Context, string, map[string]any) (types.GraphCommandResult, error) {
	return types.GraphCommandResult{}, fmt.Errorf("graph API is not supported by the sql-kv build")
}

func (*Materializer) GraphRequestResult(context.Context, string) (types.GraphCommandResult, error) {
	return types.GraphCommandResult{}, fmt.Errorf("graph API is not supported by the sql-kv build")
}

func (*Materializer) GraphRequestMatches(context.Context, types.GraphCommand) (bool, error) {
	return false, fmt.Errorf("graph API is not supported by the sql-kv build")
}

func (*Materializer) encodeSnapshot(sqlite []byte) ([]byte, error) { return sqlite, nil }

func prepareSnapshot(data []byte, _ string) (snapshotParts, error) {
	return snapshotParts{sqlite: data}, nil
}

func (*Materializer) validateRestoredSnapshot() error { return nil }

func (*Materializer) graphHealth() error { return nil }
