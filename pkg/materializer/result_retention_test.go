package materializer

import "testing"

func TestSQLReceiptRetentionUsageBounds(t *testing.T) {
	var usage sqlReceiptRetentionUsage
	if !usage.admits(maxRetainedSQLResultBytes, 1) {
		t.Fatal("one maximum-size result should fit")
	}
	usage.add(maxRetainedSQLResultBytes)
	if usage.admits(1, 1) {
		t.Fatal("result byte budget exceeded")
	}
	if got, want := usage.chargedBytes(), uint64(maxRetainedSQLResultBytes+maxSQLReceiptEnvelopeBytes); got != want {
		t.Fatalf("charged bytes=%d want=%d", got, want)
	}

	usage = sqlReceiptRetentionUsage{}
	for range maxSQLCommandsPerDecidedValue {
		if !usage.admits(0, 1) {
			t.Fatal("receipt envelope budget filled before one slot")
		}
		usage.add(0)
	}
	if usage.admits(0, 1) {
		t.Fatal("receipt envelope budget exceeded")
	}
	if got, want := usage.chargedBytes(), uint64(maxSQLCommandsPerDecidedValue)*maxSQLReceiptEnvelopeBytes; got != want {
		t.Fatalf("charged bytes=%d want=%d", got, want)
	}
}
