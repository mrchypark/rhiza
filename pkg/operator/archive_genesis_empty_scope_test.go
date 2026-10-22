package operator

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/thanos-io/objstore"
)

type archiveGenesisIterFailureBucket struct {
	objstore.Bucket
	err error
}

func (b archiveGenesisIterFailureBucket) Iter(context.Context, string, func(string) error, ...objstore.IterOption) error {
	return b.err
}

func TestAdmittedArchiveGenesisRejectsPriorOrUnlistableScope(t *testing.T) {
	for _, tc := range []struct {
		name        string
		object      string
		failListing bool
	}{
		{name: "headless archive extent", object: "root/source/archive/blocks/legacy.bin"},
		{name: "headless checkpoint metadata", object: "root/source/checkpoint/CURRENT"},
		{name: "listing error", failListing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var writes atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodDelete {
					writes.Add(1)
				}
				http.NotFound(w, r)
			}))
			t.Cleanup(server.Close)

			f := newArchiveGenesisFixture(t)
			f.setStorage(t, server.URL, server.Client())
			bucket := f.e.ArchiveBucket
			if tc.object != "" {
				if err := bucket.Upload(t.Context(), tc.object, strings.NewReader("prior state")); err != nil {
					t.Fatal(err)
				}
			}
			if tc.failListing {
				f.e.ArchiveBucket = archiveGenesisIterFailureBucket{Bucket: bucket, err: errors.New("injected listing failure")}
			}

			err := f.e.executeSourceMutationRequest(t.Context(), "Bearer test", f.id, sourceMutationRequest{
				ProducerUID: f.producer, Sequence: 1, Intent: f.intent,
			}, true)
			if err == nil {
				t.Fatal("admitted empty-HEAD mutation was accepted for a nonempty or unlistable archive scope")
			}
			if writes.Load() != 0 {
				t.Fatalf("provider writes=%d, want none", writes.Load())
			}

			scope, _, err := f.e.archiveGenesisScope(t.Context(), f.runtime)
			if err != nil {
				t.Fatal(err)
			}
			claim, _, err := f.e.AuthorityState.ReadArchiveScopeClaim(t.Context(), scope)
			if err != nil || claim.GenesisRef != nil {
				t.Fatalf("scope genesis ref committed despite rejection: ref=%+v err=%v", claim.GenesisRef, err)
			}
			root, err := f.e.read(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
			if err != nil || root.Ledger.ArchiveGenesisRef != nil {
				t.Fatalf("root genesis ref committed despite rejection: ref=%+v err=%v", root.Ledger.ArchiveGenesisRef, err)
			}
			producer, _, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
			if err != nil {
				t.Fatal(err)
			}
			entry := producer.Entries[1]
			if entry.Intent != f.intent || entry.Preparation != nil || len(entry.Dispatches) != 0 || entry.Resolution != nil {
				t.Fatalf("rejected genesis admission reached preparation or dispatch: entry=%+v", entry)
			}
		})
	}
}
