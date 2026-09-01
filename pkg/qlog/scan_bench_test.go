package qlog

import "testing"

func BenchmarkWALScanScratch(b *testing.B) {
	wal, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer wal.Close()
	payload := make([]byte, 4096)
	for i := range 256 {
		if err := wal.Append(Entry{Slot: uint64(i + 1), Type: EntryProposal, Payload: payload}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := wal.Scan(func(Entry) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWALAppendSync(b *testing.B) {
	wal, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer wal.Close()
	payload := make([]byte, 256)
	var slot uint64
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		slot++
		if err := wal.Append(Entry{Slot: slot, Type: EntryReceipt, Payload: payload}); err != nil {
			b.Fatal(err)
		}
		if err := wal.Sync(); err != nil {
			b.Fatal(err)
		}
	}
}
