package materializer

import (
	"github.com/mrchypark/rhiza/internal/types"
	"testing"
)

func TestReplicatedSQLRejectsTempAdmission(t *testing.T) {
	for _, query := range []string{
		"CREATE TEMP TABLE scratch (id INTEGER)",
		"CREATE TEMPORARY VIEW scratch AS SELECT 1",
		"CREATE TEMP TRIGGER scratch AFTER INSERT ON items BEGIN SELECT 1; END",
		"CREATE TABLE temp.scratch (id INTEGER)",
		"INSERT INTO items SELECT * FROM temp.scratch",
	} {
		t.Run(query, func(t *testing.T) {
			if err := ValidateSQLCommand(types.SQLCommand{RequestID: "temp", SQL: query}); err == nil {
				t.Fatalf("admitted temporary state: %s", query)
			}
		})
	}
}
