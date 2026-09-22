package qlog

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
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

func TestWALUncertainAppendPreservesTail(t *testing.T) {
	injected := errors.New("injected write failure")
	for _, dirty := range []bool{false, true} {
		for _, tc := range []struct {
			name  string
			cut   int
			cause error
		}{
			{"zero-error", 0, injected}, {"header-error", 12, injected}, {"payload-error", 128, injected},
			{"short-nil", 128, nil}, {"zero-nil", 0, nil}, {"full-error", -1, injected},
		} {
			t.Run(fmt.Sprintf("%s/dirty=%v", tc.name, dirty), func(t *testing.T) {
				dir := t.TempDir()
				w, err := Open(dir)
				if err != nil {
					t.Fatal(err)
				}
				defer w.Close()
				a := Entry{Slot: 1, Type: EntryProposal, Payload: []byte("prefix")}
				b := Entry{Slot: 2, Type: EntryProposal, Payload: bytes.Repeat([]byte("b"), 1024)}
				c := Entry{Slot: 3, Type: EntryProposal, Payload: []byte("c")}
				if err = w.Append(a); err != nil {
					t.Fatal(err)
				}
				if !dirty {
					if err = w.Sync(); err != nil {
						t.Fatal(err)
					}
				}
				offset, total := w.current.offset, w.Bytes()
				cut := tc.cut
				if cut < 0 {
					cut = len(b.Encode())
				}
				w.writeAt = func(f *os.File, data []byte, off int64) (int, error) {
					n, e := f.WriteAt(data[:cut], off)
					if e != nil {
						return n, e
					}
					return n, tc.cause
				}
				failure := w.Append(b)
				cause := tc.cause
				if cause == nil {
					cause = io.ErrShortWrite
				}
				if !errors.Is(failure, cause) {
					t.Fatalf("failure=%v want=%v", failure, cause)
				}
				w.writeAt = func(*os.File, []byte, int64) (int, error) {
					t.Fatal("poisoned WAL reached physical write")
					return 0, nil
				}
				for i := 0; i < 2; i++ {
					if err = w.Append(c); err != failure {
						t.Fatalf("retry=%v want stored=%v", err, failure)
					}
					if err = w.Sync(); err != failure {
						t.Fatalf("sync=%v", err)
					}
				}
				if w.current.offset != offset || w.Bytes() != total || w.dirty != dirty {
					t.Fatalf("bookkeeping changed: offset=%d bytes=%d dirty=%v", w.current.offset, w.Bytes(), w.dirty)
				}
				path := w.current.file.Name()
				before, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				wantBytes := append(a.Encode(), b.Encode()[:cut]...)
				if !bytes.Equal(before, wantBytes) {
					t.Fatal("uncertain physical tail changed")
				}
				if err = w.BindIdentity([]byte("identity")); err != failure {
					t.Fatalf("bind=%v", err)
				}
				if err = w.RestoreSegment(2, c.Encode()); err != failure {
					t.Fatalf("restore=%v", err)
				}
				if _, err = w.BeginCompaction(Entry{Slot: 1, Type: EntryCheckpoint}, nil); err != failure {
					t.Fatalf("compact=%v", err)
				}
				if err = w.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := Open(dir)
				if err != nil {
					t.Fatal(err)
				}
				want := []Entry{a}
				if cut == len(b.Encode()) {
					want = append(want, b)
				}
				got, err := reopened.Read()
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("recovery=%+v err=%v want=%+v", got, err, want)
				}
				if err = reopened.Append(c); err != nil {
					t.Fatal(err)
				}
				if err = reopened.Sync(); err != nil {
					t.Fatal(err)
				}
				if err = reopened.Close(); err != nil {
					t.Fatal(err)
				}
				again, err := Open(dir)
				if err != nil {
					t.Fatal(err)
				}
				defer again.Close()
				want = append(want, c)
				got, err = again.Read()
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("second recovery=%+v err=%v", got, err)
				}
				var size int64
				for _, e := range want {
					size += int64(len(e.Encode()))
				}
				if again.Bytes() != size {
					t.Fatalf("bytes=%d want=%d", again.Bytes(), size)
				}
			})
		}
	}
}

func TestWALAppendFailureBlocksCompactionCommit(t *testing.T) {
	w, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err = w.Append(Entry{Slot: 1, Type: EntryProposal, Payload: []byte("prefix")}); err != nil {
		t.Fatal(err)
	}
	comp, err := w.BeginCompaction(Entry{Slot: 1, Type: EntryCheckpoint}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer comp.Abort()
	if err = comp.Build(); err != nil {
		t.Fatal(err)
	}
	generation := w.generation
	w.writeAt = func(f *os.File, b []byte, o int64) (int, error) {
		n, err := f.WriteAt(b[:12], o)
		if err != nil {
			return n, err
		}
		return n, io.ErrShortWrite
	}
	failure := w.Append(Entry{Slot: 2, Type: EntryProposal, Payload: []byte("tail")})
	if !errors.Is(failure, io.ErrShortWrite) {
		t.Fatal(failure)
	}
	if err = comp.Commit(); err != failure {
		t.Fatalf("commit=%v", err)
	}
	if w.generation != generation {
		t.Fatal("fatal compaction published manifest")
	}
	comp.Abort()
	first, last := w.segments[0].file, w.current.file
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err == nil {
		t.Fatal("cleanup error discarded")
	}
	if _, err = last.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("tail file not closed: %v", err)
	}
}

func TestWALCapacityRejectionDoesNotPoison(t *testing.T) {
	w, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	small := Entry{Slot: 1, Type: EntryProposal, Payload: []byte("x")}
	if err = w.SetMaxBytes(int64(len(small.Encode()))); err != nil {
		t.Fatal(err)
	}
	if err = w.Append(Entry{Slot: 1, Type: EntryProposal, Payload: make([]byte, 1024)}); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	if err = w.Append(small); err != nil {
		t.Fatal(err)
	}
	if err = w.Sync(); err != nil {
		t.Fatal(err)
	}
}
