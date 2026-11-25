package qlog

import (
	"os"
	"testing"
)

// The WAL's logical capacity does not reserve physical space for the rewrite.
func TestCompactionRewriteBytesAreOutsideWALCapacity(t *testing.T) {
	w, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.SetMaxBytes(1 << 20); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Entry{Slot: 1, Type: EntryReceipt, Payload: []byte("old")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	plan, err := w.BeginCompaction(Entry{Slot: 1, Type: EntryCheckpoint, Payload: []byte("base")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Abort()
	before := w.Capacity()
	if err := plan.Build(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(plan.targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() <= 0 {
		t.Fatalf("rewrite has size %d", info.Size())
	}
	if after := w.Capacity(); after.Used != before.Used {
		t.Fatalf("logical WAL usage changed: before=%d after=%d", before.Used, after.Used)
	}
}
