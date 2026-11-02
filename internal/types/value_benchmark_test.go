package types

import (
	"bytes"
	"testing"
)

func BenchmarkSQLBatchCodec(b *testing.B) {
	for _, payload := range []struct {
		name  string
		value any
	}{
		{name: "json", value: "representative embedded SQL value"},
		{name: "blob_1k", value: bytes.Repeat([]byte{0x5a}, 1024)},
	} {
		b.Run(payload.name, func(b *testing.B) {
			command := SQLCommand{
				RequestID: "benchmark-request",
				SQL:       "INSERT INTO records (id, value) VALUES (?, ?)",
				Args:      []any{int64(1), payload.value},
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				encoded, err := EncodeSQLBatchItem(command)
				if err != nil {
					b.Fatal(err)
				}
				decoded, recognized, err := DecodeSQLBatch(AssembleSQLBatch([][]byte{encoded}))
				if err != nil || !recognized || len(decoded) != 1 {
					b.Fatalf("SQL batch codec: recognized=%t commands=%d err=%v", recognized, len(decoded), err)
				}
			}
		})
		b.Run(payload.name+"_encode", func(b *testing.B) {
			command := SQLCommand{RequestID: "benchmark-request", SQL: "INSERT INTO records (id, value) VALUES (?, ?)", Args: []any{int64(1), payload.value}}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := EncodeSQLBatchItem(command); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(payload.name+"_decode", func(b *testing.B) {
			command := SQLCommand{RequestID: "benchmark-request", SQL: "INSERT INTO records (id, value) VALUES (?, ?)", Args: []any{int64(1), payload.value}}
			encoded, err := EncodeSQLBatch([]SQLCommand{command})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				decoded, recognized, err := DecodeSQLBatch(encoded)
				if err != nil || !recognized || len(decoded) != 1 {
					b.Fatalf("SQL batch decode: recognized=%t commands=%d err=%v", recognized, len(decoded), err)
				}
			}
		})
	}
}
