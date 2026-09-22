package qlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
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

func legacyEntry(e Entry) []byte {
	b := make([]byte, 49+len(e.Payload))
	binary.LittleEndian.PutUint64(b, e.Slot)
	copy(b[8:40], e.Hash[:])
	b[40] = byte(e.Type)
	binary.LittleEndian.PutUint32(b[41:45], uint32(len(e.Payload))|3<<30)
	copy(b[49:], e.Payload)
	binary.LittleEndian.PutUint32(b[45:49], crc32.Update(crc32.Checksum(b[:45], entryCRCTable), entryCRCTable, b[49:]))
	return b
}

func TestWALProtectedHeaderAndRestore(t *testing.T) {
	valid := Entry{Slot: 1, Type: EntryProposal, Payload: []byte("payload")}.Encode()
	cases := map[string][]byte{"legacy-empty": legacyEntry(Entry{}), "partial": valid[:entryHeaderSize-1]}
	for _, offset := range []int{0, 8, 40, 41, 44, 45, 49, 52, 53} {
		data := bytes.Clone(valid)
		data[offset] ^= 1
		cases[fmt.Sprintf("byte-%d", offset)] = data
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, decodeErr := DecodeEntry(data)
			if decodeErr == nil {
				t.Fatal("invalid record accepted")
			}
			if len(data) >= entryHeaderSize && errors.Is(decodeErr, io.ErrUnexpectedEOF) {
				t.Fatal("corruption classified as incomplete")
			}
			w, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			for _, index := range []uint32{1, 2} {
				if err = w.RestoreSegment(index, data); err == nil {
					t.Fatalf("restored invalid segment %d", index)
				}
			}
			info, err := w.current.file.Stat()
			if err != nil || info.Size() != 0 || len(w.segments) != 1 {
				t.Fatalf("restore mutated state: %v %v", info, err)
			}
		})
	}
	empty := Entry{Slot: 1}.Encode()
	if _, used, err := DecodeEntry(empty); err != nil || used != entryHeaderSize {
		t.Fatalf("empty roundtrip=%d,%v", used, err)
	}
}

func TestWALLegacyManifestRefusedWithoutMutation(t *testing.T) {
	old := legacyEntry(Entry{Slot: 1, Payload: []byte("legacy")})
	for _, data := range [][]byte{nil, old, old[:12], old[:len(old)-1], legacyEntry(Entry{})} {
		t.Run(fmt.Sprint(len(data)), func(t *testing.T) {
			dir := t.TempDir()
			w, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			manifest, err := encodeManifest(1, []manifestRef{{index: 1, active: true}}, []byte("identity"))
			if err != nil {
				t.Fatal(err)
			}
			copy(manifest[:8], "RHZAWAL!")
			binary.BigEndian.PutUint32(manifest[len(manifest)-4:], crc32.Checksum(manifest[:len(manifest)-4], entryCRCTable))
			mp := filepath.Join(dir, "manifest_00000000000000000001.bin")
			sp := filepath.Join(dir, "seg_001.log")
			if err = os.WriteFile(mp, manifest, 0600); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(sp, data, 0600); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if opened, err := Open(dir); err == nil {
					opened.Close()
					t.Fatal("legacy WAL accepted")
				}
			}
			after, err := os.ReadFile(sp)
			if err != nil || !bytes.Equal(after, data) {
				t.Fatal("legacy segment changed", err)
			}
			after, err = os.ReadFile(mp)
			if err != nil || !bytes.Equal(after, manifest) {
				t.Fatal("legacy manifest changed", err)
			}
		})
	}
}

func TestWALCorruptionNeverRepairs(t *testing.T) {
	for _, sealed := range []bool{false, true} {
		for _, kind := range []string{"large", "zero", "marker", "payload", "legacy"} {
			t.Run(fmt.Sprintf("%s/sealed=%v", kind, sealed), func(t *testing.T) {
				dir := t.TempDir()
				w, err := Open(dir)
				if err != nil {
					t.Fatal(err)
				}
				e := Entry{Slot: 1, Type: EntryProposal, Payload: []byte("payload")}
				if err = w.Append(e); err != nil {
					t.Fatal(err)
				}
				if err = w.Sync(); err != nil {
					t.Fatal(err)
				}
				path := w.current.file.Name()
				if sealed {
					if err = w.createSegment(2); err != nil {
						t.Fatal(err)
					}
				}
				if err = w.Close(); err != nil {
					t.Fatal(err)
				}
				data := e.Encode()
				switch kind {
				case "large":
					binary.LittleEndian.PutUint32(data[41:45], entryLengthMarker|1000000)
				case "zero":
					binary.LittleEndian.PutUint32(data[41:45], entryLengthMarker)
				case "marker":
					data[44] ^= 0x40
				case "payload":
					data[entryHeaderSize] ^= 1
				case "legacy":
					data = legacyEntry(Entry{})
				}
				if err = os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 2; i++ {
					if opened, err := Open(dir); err == nil {
						opened.Close()
						t.Fatal("corruption accepted")
					}
				}
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(after, data) {
					t.Fatal("corruption evidence changed", err)
				}
			})
		}
	}
}

func TestTailRepairRequiresDurableTruncation(t *testing.T) {
	failure := errors.New("repair failure")
	for _, failTruncate := range []bool{true, false} {
		s := Segment{offset: 99}
		synced := false
		err := s.repairTail(10, func(int64) error {
			if failTruncate {
				return failure
			}
			return nil
		}, func() error { synced = true; return failure })
		if !errors.Is(err, failure) || s.offset != 99 || synced == failTruncate {
			t.Fatalf("err=%v offset=%d synced=%v", err, s.offset, synced)
		}
	}
}
