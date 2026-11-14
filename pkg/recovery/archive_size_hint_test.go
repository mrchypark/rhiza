package recovery

import (
	"bytes"
	"context"
	"testing"

	"github.com/thanos-io/objstore"
)

func TestReadObjectWithSizeHint(t *testing.T) {
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	data := bytes.Repeat([]byte("data"), 1024)
	if err := bucket.Upload(ctx, "object", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	m := NewManager(bucket, "", 1)
	defer m.Close()
	for _, hint := range []int64{-1, 0, 1, int64(len(data)), int64(len(data) + 100), 2 << 20} {
		got, err := m.readObjectWithSizeHint(ctx, "object", int64(len(data)), hint)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("hint %d: got %d bytes, err %v", hint, len(got), err)
		}
		if _, err := m.readObjectWithSizeHint(ctx, "object", int64(len(data)-1), hint); err == nil {
			t.Fatalf("hint %d: accepted oversized object", hint)
		}
	}
}
