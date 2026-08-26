package recovery

import (
	"bytes"
	"context"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

func TestSharedArchiveRoundTripUsesBoundedExtents(t *testing.T) {
	ctx := context.Background()
	config := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: config, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxExtentItems+3; i++ {
		if _, _, err := core.Propose(ctx, bytes.Repeat([]byte{byte(i + 1)}, 1024)); err != nil {
			t.Fatal(err)
		}
	}
	bucket := objstore.NewInMemBucket()
	writer := NewManager(bucket, "cluster", 1)
	if err := writer.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	reader := NewManager(bucket, "cluster", 1)
	if err := reader.Load(ctx); err != nil {
		t.Fatal(err)
	}
	values, tip, err := reader.DecisionsFrom(1, 256)
	if err != nil || tip != core.Tip() || len(values) != int(core.Tip()) {
		t.Fatalf("archive tip=%d values=%d err=%v", tip, len(values), err)
	}
}
