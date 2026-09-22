package qlog

import (
	"errors"
	"os"
	"testing"
)

func TestWALAppendFailurePoisonsWriter(t *testing.T) {
	w, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	path := w.current.file.Name()
	if err = w.current.file.Close(); err != nil {
		t.Fatal(err)
	}
	failure := w.Append(Entry{Slot: 1, Type: EntryProposal, Payload: []byte("uncertain")})
	if failure == nil {
		t.Fatal("expected write failure")
	}
	w.current.file, err = os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Append(Entry{Slot: 2, Type: EntryProposal, Payload: []byte("x")}); !errors.Is(err, failure) {
		t.Fatalf("append after failure=%v, want original failure=%v", err, failure)
	}
	if err = w.Sync(); !errors.Is(err, failure) {
		t.Fatalf("sync after failure=%v", err)
	}
}
