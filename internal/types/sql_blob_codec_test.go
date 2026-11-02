package types

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"strings"
	"testing"
)

func TestSQLBlobWireAndFingerprintCompatibility(t *testing.T) {
	for _, test := range []struct {
		name string
		blob []byte
		wire string
	}{
		{"nil", nil, ""},
		{"empty", []byte{}, ""},
		{"binary", []byte{0, 255}, "AP8="},
	} {
		t.Run(test.name, func(t *testing.T) {
			command := SQLCommand{RequestID: "blob", SQL: "SELECT ?", Args: []any{test.blob}, Statements: []SQLStatement{{SQL: "SELECT ?", Args: []any{test.blob}}}}
			before, err := json.Marshal(command)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := EncodeSQLBatchItem(command)
			if err != nil {
				t.Fatal(err)
			}
			want := "{\"request_id\":\"blob\",\"sql\":\"SELECT ?\",\"args\":[{\"$rhiza_blob\":\"" + test.wire + "\"}],\"statements\":[{\"sql\":\"SELECT ?\",\"args\":[{\"$rhiza_blob\":\"" + test.wire + "\"}]}]}"
			if string(encoded) != want {
				t.Fatalf("wire=%s want=%s", encoded, want)
			}
			after, err := json.Marshal(command)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("input changed: %s -> %s err=%v", before, after, err)
			}
			fingerprint, err := SQLFingerprint(command)
			fingerprintWire := strings.Replace(want, "\"request_id\":\"blob\"", "\"request_id\":\"\"", 1)
			wantFingerprint := sha256.Sum256(append([]byte("rhiza/sql\x00"), []byte(fingerprintWire)...))
			if err != nil || fingerprint != wantFingerprint {
				t.Fatalf("fingerprint=%x want=%x err=%v", fingerprint, wantFingerprint, err)
			}
			decoded, ok, err := DecodeSQLBatch(AssembleSQLBatch([][]byte{encoded}))
			if err != nil || !ok || len(decoded) != 1 {
				t.Fatalf("decode=%v recognized=%t err=%v", decoded, ok, err)
			}
			for _, arg := range []any{decoded[0].Args[0], decoded[0].Statements[0].Args[0]} {
				blob, typed := arg.([]byte)
				if !typed || !bytes.Equal(blob, test.blob) {
					t.Fatalf("decoded blob=%#v want=%#v", arg, test.blob)
				}
			}
			if len(test.blob) > 0 {
				test.blob[0] ^= 1
				if string(encoded) != want || decoded[0].Args[0].([]byte)[0] != 0 {
					t.Fatal("encoded or decoded output aliases caller blob")
				}
			}
		})
	}
}

func TestSQLBlobCodecPreservesErrorAndObjectSemantics(t *testing.T) {
	for _, payload := range []string{
		"[{\"request_id\":\"bad\",\"args\":[{\"$rhiza_blob\":\"!\"}]}]",
		"[{\"request_id\":\"bad\",\"args\":[{\"$rhiza_blob\":42}]}]",
		"[{\"request_id\":\"bad\",\"statements\":[{\"sql\":\"SELECT ?\",\"args\":[{\"$rhiza_blob\":\"!\"}]}]}]",
		"[{\"request_id\":\"bad\",\"args\":[{\"$rhiza_blob\":\"AA==\"}]}] true",
	} {
		if _, recognized, err := DecodeSQLBatch(append(append([]byte(nil), sqlBatchMagic...), payload...)); !recognized || err == nil {
			t.Fatalf("invalid blob accepted: %s recognized=%t err=%v", payload, recognized, err)
		}
	}
	object := map[string]any{"$rhiza_blob": "not-base64", "other": "ordinary object"}
	command := SQLCommand{RequestID: "object", Args: []any{object}}
	encoded, err := EncodeSQLBatch([]SQLCommand{command})
	if err != nil {
		t.Fatal(err)
	}
	decoded, _, err := DecodeSQLBatch(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := decoded[0].Args[0].(map[string]any); !ok || got["other"] != object["other"] {
		t.Fatalf("ordinary map changed: %#v", decoded[0].Args[0])
	}
	unsupported := make(chan int)
	blob := []byte{1, 2}
	bad := SQLCommand{RequestID: "unsupported", Args: []any{blob, unsupported}}
	if _, err := EncodeSQLBatchItem(bad); err == nil {
		t.Fatal("unsupported JSON value accepted")
	}
	if got, ok := bad.Args[0].([]byte); !ok || !bytes.Equal(got, blob) || bad.Args[1] != unsupported {
		t.Fatal("failed encoding changed caller input")
	}
}
