package checkpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func (m *Manager) VerifyMembershipAnchor(ctx context.Context, initial quepaxa.Cluster, anchor quepaxa.MembershipAnchor) error {
	return quepaxa.VerifyMembershipAnchorProof(ctx, initial, anchor, func(ctx context.Context, digest [32]byte) (quepaxa.MembershipRecord, error) {
		r, err := m.bucket.Get(objmetrics.WithExpectedNotFound(ctx), quepaxa.MembershipAnchorProofKey(m.prefix, digest))
		if m.bucket.IsObjNotFoundErr(err) {
			return quepaxa.MembershipRecord{}, fmt.Errorf("membership anchor proof is absent")
		}
		if err != nil {
			return quepaxa.MembershipRecord{}, err
		}
		defer r.Close()
		data, err := io.ReadAll(io.LimitReader(r, (256<<10)+1))
		if err != nil || len(data) == 0 || len(data) > 256<<10 {
			return quepaxa.MembershipRecord{}, fmt.Errorf("invalid membership anchor proof")
		}
		var record quepaxa.MembershipRecord
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&record) != nil || decoder.Decode(new(any)) != io.EOF {
			return quepaxa.MembershipRecord{}, fmt.Errorf("invalid membership anchor proof encoding")
		}
		return record, nil
	})
}
