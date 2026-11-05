package checkpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"slices"

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/thanos-io/objstore"
)

// ManagedGraphLocator is an untrusted discovery hint. A reader must bind its
// exact seal decision to a captured certified archive before using the root.
// Index is the actual paired snapshot cut, not the Kubernetes generation.
type ManagedGraphLocator struct {
	Policy           int      `json:"policy"`
	Index            uint64   `json:"index"`
	RootHash         [32]byte `json:"root_hash"`
	SealDecisionSlot uint64   `json:"seal_decision_slot"`
	SealDecisionHash [32]byte `json:"seal_decision_hash"`
}

// ManagedAppendOnlyLocator is an untrusted discovery hint for the explicit
// request/Graph append-only protocols. Its certificate and immutable root must
// be authenticated by the caller before promotion and again by every reader.
// The JSON field names intentionally match the existing Graph locator format.
type ManagedAppendOnlyLocator struct {
	Policy           int      `json:"policy"`
	Index            uint64   `json:"index"`
	RootHash         [32]byte `json:"root_hash"`
	SealDecisionSlot uint64   `json:"seal_decision_slot"`
	SealDecisionHash [32]byte `json:"seal_decision_hash"`
}

func (l ManagedAppendOnlyLocator) validate(policy sqlpolicy.MaterializationPolicy) error {
	if policy != sqlpolicy.ManagedRequestMaterializationPolicy && policy != sqlpolicy.ManagedGraphRequestMaterializationPolicy ||
		l.Policy != int(policy) || l.Index == 0 || l.Index >= l.SealDecisionSlot ||
		l.RootHash == ([32]byte{}) || l.SealDecisionHash == ([32]byte{}) {
		return fmt.Errorf("invalid managed append-only checkpoint locator")
	}
	return nil
}

func (l ManagedGraphLocator) validate() error {
	if l.Policy != int(sqlpolicy.ManagedGraphRequestMaterializationPolicy) || l.Index == 0 || l.Index >= l.SealDecisionSlot || l.RootHash == ([32]byte{}) || l.SealDecisionHash == ([32]byte{}) {
		return fmt.Errorf("invalid managed Graph checkpoint locator")
	}
	return nil
}

// CreateManagedGraphCandidate uploads only immutable objects. The owner must
// supply a clean, provenance-verified pair captured at cut; this is not a seal
// or CURRENT publication. Legacy CreateFiles still requires its original claim.
func (m *Manager) CreateManagedGraphCandidate(ctx context.Context, sources []Source, cut uint64) (*Checkpoint, error) {
	if !m.appendOnly || m.expectedPolicy != sqlpolicy.ManagedGraphRequestMaterializationPolicy || cut == 0 {
		return nil, fmt.Errorf("managed Graph candidate requires its append-only profile and positive cut")
	}
	if err := m.validateSourceProfile(sources); err != nil {
		return nil, err
	}
	for _, source := range sources {
		if source.Role == RoleSQLite {
			if err := sqlpolicy.CheckFileForPolicy(ctx, source.Path, m.expectedPolicy); err != nil {
				return nil, err
			}
		}
	}
	return m.uploadCandidate(ctx, sources, cut, cut, nil, nil)
}

// CreateManagedAppendOnlyCandidate uploads an immutable candidate for the
// explicit request (policy 9) or Graph (policy 10) profile. It creates no
// publisher claim and confers no voting or recovery authority.
func (m *Manager) CreateManagedAppendOnlyCandidate(ctx context.Context, sources []Source, cut uint64) (*Checkpoint, error) {
	if !m.appendOnly || cut == 0 ||
		m.expectedPolicy != sqlpolicy.ManagedRequestMaterializationPolicy && m.expectedPolicy != sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		return nil, fmt.Errorf("managed append-only candidate requires a supported profile and positive cut")
	}
	if err := m.validateSourceProfile(sources); err != nil {
		return nil, err
	}
	return m.uploadCandidate(ctx, sources, cut, cut, nil, nil)
}

// Every failed conditional upload, including a lost ACK, requires exact bounded
// readback. An unavailable/mismatching readback preserves the original failure.
func (m *Manager) uploadManagedGraphImmutable(ctx context.Context, key string, data io.Reader, size int64, hash [32]byte) error {
	err := m.bucket.Upload(ctx, key, data, objstore.WithIfNotExists())
	if err == nil {
		return nil
	}
	r, readErr := m.bucket.Get(ctx, key)
	if readErr != nil {
		return errors.Join(err, readErr)
	}
	h := sha256.New()
	n, readErr := io.Copy(h, io.LimitReader(r, size+1))
	readErr = errors.Join(readErr, r.Close())
	if readErr != nil || n != size || !bytes.Equal(h.Sum(nil), hash[:]) {
		return errors.Join(err, readErr, fmt.Errorf("immutable managed Graph object readback mismatch"))
	}
	return nil
}

func (m *Manager) readManagedGraphLocator(ctx context.Context) (ManagedGraphLocator, bool, error) {
	var l ManagedGraphLocator
	r, err := m.bucket.Get(objmetrics.WithExpectedNotFound(ctx), m.key("checkpoint/CURRENT"))
	if err != nil {
		if m.bucket.IsObjNotFoundErr(err) {
			return l, false, nil
		}
		return l, false, err
	}
	data, readErr := io.ReadAll(io.LimitReader(r, maxRootSize+1))
	readErr = errors.Join(readErr, r.Close())
	if readErr != nil || len(data) > maxRootSize {
		return l, false, errors.Join(readErr, fmt.Errorf("invalid managed Graph locator size"))
	}
	if err := decodePersistedJSON(data, &l); err != nil {
		return l, false, err
	}
	return l, true, l.validate()
}

func (m *Manager) LoadManagedGraphLocator(ctx context.Context) (ManagedGraphLocator, bool, error) {
	if m.expectedPolicy != sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		return ManagedGraphLocator{}, false, sqlpolicy.ErrIncompatible
	}
	return m.readManagedGraphLocator(ctx)
}

// PromoteManagedGraphLocator advances only the discovery hint. The caller must
// first authenticate the exact seal and read it from the durable archive.
// Readers must repeat that authentication; this method grants no authority.
func (m *Manager) PromoteManagedGraphLocator(ctx context.Context, l ManagedGraphLocator) error {
	if !m.appendOnly || m.expectedPolicy != sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		return sqlpolicy.ErrIncompatible
	}
	if err := l.validate(); err != nil {
		return err
	}
	if _, err := m.OpenRoot(ctx, l.Index, l.RootHash); err != nil {
		return err
	}
	data, err := json.Marshal(l)
	if err != nil {
		return err
	}
	key := m.key("checkpoint/CURRENT")
	for range 4 {
		attr, attrErr := m.bucket.Attributes(objmetrics.WithExpectedNotFound(ctx), key)
		options := []objstore.ObjectUploadOption{objstore.WithIfNotExists()}
		if attrErr != nil && !m.bucket.IsObjNotFoundErr(attrErr) {
			return attrErr
		}
		if attrErr == nil {
			if attr.Version == nil {
				return fmt.Errorf("managed Graph CURRENT lacks conditional version")
			}
			prior, exists, err := m.readManagedGraphLocator(ctx)
			if err != nil || !exists {
				return errors.Join(err, fmt.Errorf("managed Graph CURRENT changed during read"))
			}
			after, err := m.bucket.Attributes(ctx, key)
			if err != nil {
				return err
			}
			if after.Version == nil || *after.Version != *attr.Version {
				continue
			}
			if prior.Index > l.Index {
				return ErrStaleCheckpoint
			}
			if prior.Index == l.Index {
				if prior != l {
					return fmt.Errorf("conflicting managed Graph locator at same cut")
				}
				return nil
			}
			options = []objstore.ObjectUploadOption{objstore.WithIfMatch(attr.Version)}
		}
		err = m.bucket.Upload(ctx, key, bytes.NewReader(data), options...)
		if err == nil {
			return nil
		}
		if stored, exists, readErr := m.readManagedGraphLocator(ctx); readErr == nil && exists && stored == l {
			return nil
		}
		if !m.bucket.IsConditionNotMetErr(err) {
			return err
		}
	}
	return fmt.Errorf("managed Graph CURRENT CAS did not converge")
}

func managedCurrentReadFailureDiagnostic(stage string, err error) string {
	if err == nil {
		return ""
	}
	switch stage {
	case "attributes_before", "get", "body", "close", "attributes_after", "version_changed", "decode", "policy":
		return "MANAGED_CHECKPOINT_CURRENT_READ_FAILURE stage=" + stage
	default:
		return ""
	}
}

func logManagedCurrentReadFailure(stage string, err error) error {
	if line := managedCurrentReadFailureDiagnostic(stage, err); line != "" {
		log.Print(line)
	}
	return err
}

// LoadManagedAppendOnlyLocator reads the new profile's CURRENT hint. The
// expected policy is explicit so policy 9/10 objects cannot cross profiles.
func (m *Manager) LoadManagedAppendOnlyLocator(ctx context.Context, policy sqlpolicy.MaterializationPolicy) (ManagedAppendOnlyLocator, bool, error) {
	if !m.appendOnly || m.expectedPolicy != policy ||
		policy != sqlpolicy.ManagedRequestMaterializationPolicy && policy != sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		return ManagedAppendOnlyLocator{}, false, logManagedCurrentReadFailure("policy", sqlpolicy.ErrIncompatible)
	}
	key := m.key("checkpoint/CURRENT")
	attr, err := m.bucket.Attributes(objmetrics.WithExpectedNotFound(ctx), key)
	if err != nil {
		if m.bucket.IsObjNotFoundErr(err) {
			return ManagedAppendOnlyLocator{}, false, nil
		}
		return ManagedAppendOnlyLocator{}, false, logManagedCurrentReadFailure("attributes_before", err)
	}
	if attr.Version == nil {
		return ManagedAppendOnlyLocator{}, false, logManagedCurrentReadFailure("attributes_before", fmt.Errorf("managed append-only CURRENT lacks conditional version"))
	}
	r, err := m.bucket.Get(ctx, key)
	if err != nil {
		return ManagedAppendOnlyLocator{}, false, logManagedCurrentReadFailure("get", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(r, maxRootSize+1))
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || len(data) > maxRootSize {
		stage := "body"
		if readErr == nil && len(data) <= maxRootSize {
			stage = "close"
		}
		return ManagedAppendOnlyLocator{}, false, logManagedCurrentReadFailure(stage, errors.Join(readErr, closeErr, fmt.Errorf("invalid managed append-only CURRENT")))
	}
	after, err := m.bucket.Attributes(ctx, key)
	if err != nil || after.Version == nil || *after.Version != *attr.Version {
		stage := "version_changed"
		if err != nil || after.Version == nil {
			stage = "attributes_after"
		}
		return ManagedAppendOnlyLocator{}, false, logManagedCurrentReadFailure(stage, errors.Join(err, fmt.Errorf("managed append-only CURRENT changed during read")))
	}
	var locator ManagedAppendOnlyLocator
	if err := decodePersistedJSON(data, &locator); err != nil {
		return ManagedAppendOnlyLocator{}, false, logManagedCurrentReadFailure("decode", err)
	}
	if err := locator.validate(policy); err != nil {
		return ManagedAppendOnlyLocator{}, false, logManagedCurrentReadFailure("policy", err)
	}
	return locator, true, nil
}

// PromoteManagedAppendOnlyLocator monotonically advances CURRENT by the
// certified (cut, decision slot) rank. The caller authenticates and archives
// the exact seal before calling this method; CURRENT itself grants no authority.
func (m *Manager) PromoteManagedAppendOnlyLocator(ctx context.Context, l ManagedAppendOnlyLocator) error {
	policy := sqlpolicy.MaterializationPolicy(l.Policy)
	if !m.appendOnly || m.expectedPolicy != policy {
		return sqlpolicy.ErrIncompatible
	}
	if err := l.validate(policy); err != nil {
		return err
	}
	if _, err := m.OpenRoot(ctx, l.Index, l.RootHash); err != nil {
		return err
	}
	data, err := json.Marshal(l)
	if err != nil {
		return err
	}
	key := m.key("checkpoint/CURRENT")
	if !slices.Contains(m.bucket.SupportedObjectUploadOptions(), objstore.IfNotExists) || !slices.Contains(m.bucket.SupportedObjectUploadOptions(), objstore.IfMatch) {
		return fmt.Errorf("managed append-only CURRENT requires conditional object writes")
	}
	for range 4 {
		attr, attrErr := m.bucket.Attributes(objmetrics.WithExpectedNotFound(ctx), key)
		options := []objstore.ObjectUploadOption{objstore.WithIfNotExists()}
		if attrErr != nil && !m.bucket.IsObjNotFoundErr(attrErr) {
			return attrErr
		}
		if attrErr == nil {
			if attr.Version == nil {
				return fmt.Errorf("managed append-only CURRENT lacks conditional version")
			}
			prior, exists, readErr := m.LoadManagedAppendOnlyLocator(ctx, policy)
			if readErr != nil || !exists {
				return errors.Join(readErr, fmt.Errorf("managed append-only CURRENT changed during read"))
			}
			after, readErr := m.bucket.Attributes(ctx, key)
			if readErr != nil {
				return readErr
			}
			if after.Version == nil || *after.Version != *attr.Version {
				continue
			}
			priorRank := [2]uint64{prior.Index, prior.SealDecisionSlot}
			newRank := [2]uint64{l.Index, l.SealDecisionSlot}
			if priorRank[0] > newRank[0] || priorRank[0] == newRank[0] && priorRank[1] > newRank[1] {
				return ErrStaleCheckpoint
			}
			if priorRank == newRank {
				if prior != l {
					return fmt.Errorf("conflicting managed append-only locator at equal rank")
				}
				return nil
			}
			options = []objstore.ObjectUploadOption{objstore.WithIfMatch(attr.Version)}
		}
		err = m.bucket.Upload(ctx, key, bytes.NewReader(data), options...)
		if err == nil {
			return nil
		}
		if stored, exists, readErr := m.LoadManagedAppendOnlyLocator(ctx, policy); readErr == nil && exists && stored == l {
			return nil
		}
		if !m.bucket.IsConditionNotMetErr(err) {
			return err
		}
	}
	return fmt.Errorf("managed append-only CURRENT CAS did not converge")
}
