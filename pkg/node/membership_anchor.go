package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
)

const membershipAnchorProofLimit = 256 << 10

func (n *Node) membershipAnchorKey(digest [32]byte) string {
	return quepaxa.MembershipAnchorProofKey(path.Join(n.config.ObjStorePrefix, string(n.config.ClusterID)), digest)
}

func (n *Node) storeMembershipAnchor(ctx context.Context, digest [32]byte, proof quepaxa.MembershipRecord) error {
	if digest == ([32]byte{}) {
		return fmt.Errorf("membership anchor proof has no digest")
	}
	data, err := json.Marshal(proof)
	if err != nil || len(data) > membershipAnchorProofLimit {
		return fmt.Errorf("membership anchor proof exceeds its bounded size")
	}
	key := n.membershipAnchorKey(digest)
	if err := n.bucket.Upload(ctx, key, bytes.NewReader(data), objstore.WithIfNotExists()); err != nil && !n.bucket.IsConditionNotMetErr(err) {
		return err
	}
	got, err := n.loadMembershipAnchor(ctx, digest)
	if err != nil {
		return err
	}
	gotData, err := json.Marshal(got)
	if err != nil || !bytes.Equal(gotData, data) {
		return fmt.Errorf("immutable membership anchor proof differs")
	}
	return nil
}

func (n *Node) verifyMembershipAnchor(ctx context.Context, initial quepaxa.Cluster, anchor quepaxa.MembershipAnchor) error {
	return quepaxa.VerifyMembershipAnchorProof(ctx, initial, anchor, n.loadMembershipAnchor)
}

func (n *Node) loadMembershipAnchor(ctx context.Context, digest [32]byte) (quepaxa.MembershipRecord, error) {
	r, err := n.bucket.Get(objmetrics.WithExpectedNotFound(ctx), n.membershipAnchorKey(digest))
	if n.bucket.IsObjNotFoundErr(err) {
		return quepaxa.MembershipRecord{}, fmt.Errorf("membership anchor proof is absent")
	}
	if err != nil {
		return quepaxa.MembershipRecord{}, err
	}
	defer r.Close()
	data, err := io.ReadAll(io.LimitReader(r, membershipAnchorProofLimit+1))
	if err != nil || len(data) == 0 || len(data) > membershipAnchorProofLimit {
		return quepaxa.MembershipRecord{}, fmt.Errorf("invalid membership anchor proof")
	}
	var record quepaxa.MembershipRecord
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&record) != nil || decoder.Decode(new(any)) != io.EOF {
		return quepaxa.MembershipRecord{}, fmt.Errorf("invalid membership anchor proof encoding")
	}
	return record, nil
}
