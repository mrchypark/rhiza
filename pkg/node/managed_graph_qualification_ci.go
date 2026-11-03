//go:build rhiza_local_testhooks

package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

// ManagedGraphQualificationArchive exists only in tagged CI binaries. It is
// observed candidate data, never a startup/activation authority.
type ManagedGraphQualificationArchive struct {
	Locator              checkpoint.ManagedGraphLocator
	Base, Tip            uint64
	GraphSuffixDecisions int
}

// This substitutes only an earlier actually observed CURRENT locator, exactly
// as capturedHeadBucket fixes HEAD in recovery. All root/block/history objects
// are read from the actual SDK bucket and independently authenticated.
type graphQualificationLocatorBucket struct {
	objstore.Bucket
	key     string
	locator []byte
}

func (b graphQualificationLocatorBucket) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if key == b.key {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return io.NopCloser(bytes.NewReader(b.locator)), nil
	}
	return b.Bucket.Get(ctx, key)
}

// ObserveManagedGraphQualificationArchive performs only actual SDK reads. A
// nil selection captures CURRENT; a supplied earlier observation fixes root C
// while the provider HEAD may advance. No publisher/GC pins or writes occur.
func (n *Node) ObserveManagedGraphQualificationArchive(ctx context.Context, selection *checkpoint.ManagedGraphLocator) (ManagedGraphQualificationArchive, error) {
	var out ManagedGraphQualificationArchive
	if n.materializationPolicy() != sqlpolicy.ManagedGraphRequestMaterializationPolicy || n.managedStartup == nil || n.bucket == nil || n.checkpoints == nil {
		return out, fmt.Errorf("actual policy10 runtime required")
	}
	_, ctx, release, err := n.borrowInternalApplication(ctx)
	if err != nil {
		return out, err
	}
	defer release()
	if err := n.CheckManagedLocalRead(ctx); err != nil {
		return out, err
	}
	var locator checkpoint.ManagedGraphLocator
	if selection == nil {
		observed, exists, err := n.checkpoints.LoadManagedGraphLocator(ctx)
		if err != nil {
			return out, err
		}
		if !exists {
			return out, network.ErrNotReady
		}
		locator = observed
	} else {
		locator = *selection
	}
	data, err := json.Marshal(locator)
	if err != nil {
		return out, err
	}
	prefix := path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID))
	bucket := graphQualificationLocatorBucket{Bucket: n.bucket, key: path.Join(prefix, "checkpoint/CURRENT"), locator: data}
	view, err := recovery.OpenReadOnlyViewForPolicyWithReconfiguration(ctx, bucket, prefix, 1, *n.loadClusterConfig(), sqlpolicy.ManagedGraphRequestMaterializationPolicy, n.receiptWindow(), n.config.EnableReconfiguration)
	if err != nil {
		return out, err
	}
	seal, _, exists := view.RecoveryBase()
	if !exists || uint64(seal.Index) != locator.Index || seal.RootHash != locator.RootHash {
		return out, fmt.Errorf("actual SDK root selection differs from fixed locator")
	}
	out = ManagedGraphQualificationArchive{Locator: locator, Base: uint64(seal.Index), Tip: uint64(view.Tip())}
	for _, d := range view.Decisions() {
		_, graph, err := types.DecodeManagedGraphExecution(d.Value)
		if err != nil {
			return out, err
		}
		if graph {
			out.GraphSuffixDecisions++
		}
	}
	return out, nil
}
