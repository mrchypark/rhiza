package operator

import (
	"bytes"
	"context"
	"testing"

	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

func TestAutomaticArchiveAllowsEmptyAsyncOnlyWithExplicitLossAndHEAD(t *testing.T) {
	ctx := context.Background()
	canonicalHead, err := recovery.EmptyArchiveHeadBytes()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name          string
		durability    string
		allowDataLoss bool
		head          []byte
		want          bool
	}{
		{name: "async explicitly allows data loss with initialized empty HEAD", durability: "async", allowDataLoss: true, head: canonicalHead, want: true},
		{name: "async requires explicit data loss permission", durability: "async", head: canonicalHead},
		{name: "before-ack empty archive is not a recovery source", durability: "before-ack", allowDataLoss: true, head: canonicalHead},
		{name: "missing empty HEAD is not a fresh source", durability: "async", allowDataLoss: true},
		{name: "invalid empty HEAD is not a recovery source", durability: "async", allowDataLoss: true, head: []byte("not a head")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket := autoBucket(t)
			if tt.head != nil {
				if err := bucket.Upload(ctx, "root/source/archive/head.bin", bytes.NewReader(tt.head), objstore.WithIfNotExists()); err != nil {
					t.Fatal(err)
				}
			}
			got := automaticArchiveAllowsGenerationRecovery(ctx, bucket, "root/source", tt.durability, tt.allowDataLoss)
			if got != tt.want {
				t.Fatalf("automaticArchiveAllowsGenerationRecovery() = %v, want %v", got, tt.want)
			}
		})
	}
}
