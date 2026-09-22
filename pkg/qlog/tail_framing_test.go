package qlog

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"
)

func TestWALCorruptLengthPreservesEvidence(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	first := Entry{Slot: 1, Type: EntryProposal, Payload: []byte("first")}
	for _, e := range []Entry{first, {Slot: 2, Type: EntryProposal, Payload: []byte("middle")}, {Slot: 3, Type: EntryProposal, Payload: []byte("last")}} {
		if err = w.Append(e); err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Sync(); err != nil {
		t.Fatal(err)
	}
	path := w.current.file.Name()
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint32(data[len(first.Encode())+41:], entryLengthMarker|1000000)
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err == nil {
		reopened.Close()
		t.Error("corrupt length accepted as torn tail")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, data) {
		t.Fatal("corruption evidence was truncated")
	}
}
