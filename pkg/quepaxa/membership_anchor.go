package quepaxa

import (
	"context"
	"fmt"
	"path"
)

// MembershipAnchorProofLoader returns the bounded proof record committed by
// an anchor digest from immutable object storage.
type MembershipAnchorProofLoader func(context.Context, [32]byte) (MembershipRecord, error)

func MembershipAnchorProofKey(namespace string, digest [32]byte) string {
	return path.Join(namespace, "membership-anchors", fmt.Sprintf("%x.json", digest))
}

// VerifyMembershipAnchorProof walks the immutable proof chain back to the
// trusted genesis record and validates every certified transition.
func VerifyMembershipAnchorProof(ctx context.Context, initial Cluster, anchor MembershipAnchor, load MembershipAnchorProofLoader) error {
	if load == nil || anchor.Digest == ([32]byte{}) {
		return fmt.Errorf("membership anchor proof is unavailable")
	}
	full := MembershipRecord{Genesis: cloneCluster(initial), Anchor: nil}
	type cut struct {
		anchor MembershipAnchor
	}
	var cuts []cut
	seen := make(map[[32]byte]struct{})
	next := anchor
	for {
		if _, duplicate := seen[next.Digest]; duplicate {
			return fmt.Errorf("membership anchor proof cycle")
		}
		seen[next.Digest] = struct{}{}
		record, err := load(ctx, next.Digest)
		if err != nil {
			return fmt.Errorf("load membership anchor proof: %w", err)
		}
		if !sameCluster(record.Genesis, initial) || membershipRecordDigest(record) != next.Digest {
			return fmt.Errorf("membership anchor proof does not match its digest")
		}
		full.Transitions = append(record.Transitions, full.Transitions...)
		if record.Abort != nil {
			full.Abort = record.Abort
		}
		cuts = append(cuts, cut{anchor: next})
		if record.Anchor == nil {
			break
		}
		if record.Anchor.Through >= next.Through {
			return fmt.Errorf("membership anchor proof does not move forward")
		}
		next = *record.Anchor
	}
	verifier, err := validateMembershipHistory(initial, full)
	if err != nil {
		return fmt.Errorf("membership anchor transition chain: %w", err)
	}
	for _, item := range cuts {
		cluster := verifier.configHistory[0].cluster
		for _, epoch := range verifier.configHistory {
			if epoch.start <= item.anchor.Through+1 {
				cluster = epoch.cluster
			}
		}
		if !sameCluster(cluster, item.anchor.Cluster) {
			return fmt.Errorf("membership anchor cluster does not match its cut")
		}
	}
	return ctx.Err()
}
