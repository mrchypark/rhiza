package materializer

import (
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
)

func TestReplicatedSQLRejectsTempAdmission(t *testing.T) {
	for _, query := range []string{
		"CREATE TEMP TABLE scratch (id INTEGER)",
		"CREATE\vTEMP TABLE scratch (id INTEGER)",
		"CREATE \vTEMP TABLE scratch (id INTEGER)",
		"CREATE \ufeff TEMP TABLE scratch (id INTEGER)",
		"CREATE TABLE \ufefftemp.scratch(id INTEGER)",
		"CREATE TEMPORARY VIEW scratch AS SELECT 1",
		"CREATE TEMP TRIGGER scratch AFTER INSERT ON items BEGIN SELECT 1; END",
		"CREATE TABLE temp.scratch (id INTEGER)",
		"INSERT INTO items SELECT * FROM temp.scratch",
		"CREATE /* comment */ TEMPORARY TABLE scratch(id INTEGER)",
		"CREATE TABLE 'temp'.scratch(id INTEGER)",
		"INSERT INTO items SELECT * FROM \"TeMp\" /* comment */ .scratch",
		"INSERT INTO items SELECT * FROM [temp].scratch",
		"INSERT INTO items SELECT * FROM `temp`.scratch",
		"SELECT #x(temp.foo) FROM temp.scratch",
	} {
		t.Run(query, func(t *testing.T) {
			if err := ValidateSQLCommand(types.SQLCommand{RequestID: "temp", SQL: query}); err == nil {
				t.Fatalf("admitted temporary state: %s", query)
			}
		})
	}
}

func TestReplicatedSQLAllowsPersistentTempNames(t *testing.T) {
	for _, query := range []string{
		"CREATE TABLE temp(id INTEGER)",
		"CREATE TABLE main.temp(id INTEGER)",
		"INSERT INTO temp VALUES(1)",
		"SELECT 'temp.scratch', 'CREATE TEMP TABLE x(a)'",
		"CREATE TABLE temperature(id INTEGER)",
		"CREATE TABLE items(temporary INTEGER)",
		"ALTER TABLE items RENAME TO renamed",
		"SELECT :p(temp.x)",
		"SELECT @p(temp.x)",
		"SELECT $p(temp.x)",
		"SELECT #p(temp.x)",
	} {
		if err := ValidateSQLCommand(types.SQLCommand{RequestID: "persistent", SQL: query}); err != nil {
			t.Errorf("%s: %v", query, err)
		}
	}
}

func TestSingleSchemaMaintenanceSQL(t *testing.T) {
	for _, tt := range []struct {
		query string
		want  bool
	}{
		{"INSERT INTO items VALUES ('DROP TABLE items')", false},
		{"-- comment\nUPDATE items SET id = 1", false},
		{"/* comment */ aLtEr TABLE items RENAME TO renamed", true},
		{"-- comment\n\ufeffDROP TABLE items", true},
		{"DROP TABLE \"semi;colon\"; /* trailing */", true},
		{"DROP TABLE items; SELECT 1", false},
		{"ALTER TABLE items ADD COLUMN id; DROP TABLE items", false},
		{"DROP TABLE \"unterminated", false},
		{"\"DROP\" TABLE items", false},
		{"DROP INDEX items_idx", false},
		{"", false},
	} {
		if got := singleSchemaMaintenanceSQL(tt.query); got != tt.want {
			t.Errorf("singleSchemaMaintenanceSQL(%q) = %t, want %t", tt.query, got, tt.want)
		}
	}
}
