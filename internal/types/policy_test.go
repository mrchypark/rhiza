package types

import (
	"errors"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"testing"
)

func TestExecutionPolicyRejectsLegacyAndUnknownValues(t *testing.T) {
	for _, value := range []string{"", "Q", "QBAT", "QBAT\x00[]", "QBAT\x01[]", "QBAT\x02[]", "QBAT\x03[]", "QBAT\x05[]", `QBAT`, "SELECT 1", "QBAT\x00[{\"policy\":1,\"sql\":\"SELECT 1\"}]"} {
		if err := ValidateExecutionPolicy([]byte(value)); !errors.Is(err, sqlpolicy.ErrIncompatible) {
			t.Errorf("%q: %v", value, err)
		}
	}
	if err := ValidateExecutionPolicy([]byte("QBAT\x04{bad")); err == nil {
		t.Fatal("accepted malformed envelope")
	}
	value, err := EncodeSQLBatch([]SQLCommand{{SQL: "SELECT 1"}})
	if err != nil {
		t.Fatal(err)
	}
	if err = ValidateExecutionPolicy(value); err != nil {
		t.Fatal(err)
	}
	if string(value[:5]) != "QBAT\x04" {
		t.Fatalf("policy header %q", value[:5])
	}
}
