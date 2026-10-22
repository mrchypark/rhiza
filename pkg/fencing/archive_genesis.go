package fencing

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"strings"
)

const ArchiveGenesisVersion = 1

const (
	ArchiveGenesisRegistrationPending = "registration-pending"
	ArchiveGenesisAwaitingRef         = "awaiting-ref"
	ArchiveGenesisCommitted           = "committed"
)

type ArchiveGenesisMutation struct {
	ProducerUID string         `json:"producerUID"`
	Sequence    uint64         `json:"sequence"`
	Intent      MutationIntent `json:"intent"`
}

// ArchiveGenesisFork is an immutable descriptor of a recovery-verified
// nonzero target base. The recovery package remains responsible for verifying
// the referenced anchor and fork records before this descriptor is admitted.
type ArchiveGenesisFork struct {
	AnchorKey      string `json:"anchorKey"`
	AnchorDigest   string `json:"anchorDigest"`
	IntentDigest   string `json:"intentDigest"`
	ResultDigest   string `json:"resultDigest"`
	LineageDigest  string `json:"lineageDigest"`
	TargetPrefix   string `json:"targetPrefix"`
	MembershipHash string `json:"membershipHash"`
	ConfigID       uint   `json:"configID"`
	BaseTip        uint64 `json:"baseTip"`
	BasePrefixHash string `json:"basePrefixHash"`
	AnchorHash     string `json:"anchorHash"`
}

// ArchiveGenesisRef is the single immutable authority pointer to the initial
// archive state for one stable ObjectStorage scope. Exactly one proof form is
// populated. Hashes are references only; trusted readers must verify the
// referenced journal or recovery evidence before use.
type ArchiveGenesisRef struct {
	Version   int                     `json:"version"`
	ConfigID  uint                    `json:"configID"`
	ScopeHash string                  `json:"scopeHash"`
	Mutation  *ArchiveGenesisMutation `json:"mutation,omitempty"`
	Fork      *ArchiveGenesisFork     `json:"fork,omitempty"`
}

func (r ArchiveGenesisRef) Validate() error {
	if r.Version != ArchiveGenesisVersion || r.ConfigID == 0 || !mutationDigestValid(r.ScopeHash) || (r.Mutation == nil) == (r.Fork == nil) {
		return fmt.Errorf("fencing: invalid archive genesis reference")
	}
	if r.Mutation != nil {
		m := r.Mutation
		if m.ProducerUID == "" || m.Sequence == 0 || m.Intent.ContractVersion != MutationIntentVersion ||
			!mutationDigestValid(m.Intent.StoreScopeHash) || m.Intent.OperationID != MutationRequestID(m.ProducerUID, m.Sequence) ||
			m.Intent.Kind != "upload" || m.Intent.Condition != (MutationCondition{Mode: MutationCreateOnly}) || m.Intent.Key != "archive/head.bin" ||
			!mutationDigestValid(m.Intent.PayloadDigest) || m.Intent.PayloadRef == "" || m.Intent.PayloadSize < 0 {
			return fmt.Errorf("fencing: invalid archive mutation genesis tuple")
		}
		return nil
	}
	f := r.Fork
	if f.AnchorKey == "" || !mutationDigestValid(f.AnchorDigest) || !mutationDigestValid(f.IntentDigest) ||
		!mutationDigestValid(f.ResultDigest) || !mutationDigestValid(f.LineageDigest) || f.TargetPrefix == "" ||
		!mutationDigestValid(f.MembershipHash) || f.ConfigID == 0 || f.ConfigID != r.ConfigID || f.BaseTip == 0 ||
		!mutationDigestValid(f.BasePrefixHash) || !mutationDigestValid(f.AnchorHash) {
		return fmt.Errorf("fencing: invalid anchored fork genesis descriptor")
	}
	return nil
}

// ValidateForScope checks the stable archive identity independently from the
// mutation execution scope carried by an admitted upload intent.
func (r ArchiveGenesisRef) ValidateForScope(scope ArchiveScope) error {
	hash, err := scope.Hash()
	if err != nil || r.ScopeHash != hash {
		return fmt.Errorf("fencing: archive genesis stable scope differs")
	}
	if err := r.Validate(); err != nil {
		return err
	}
	if r.Fork != nil && r.Fork.TargetPrefix != scope.Prefix {
		return fmt.Errorf("fencing: archive fork target prefix differs")
	}
	return nil
}

// ArchiveScope is the provider-neutral identity used for the one-time claim.
// Runtime, Pod and StatefulSet identities are deliberately not part of it.
type ArchiveScope struct {
	Provider string `json:"provider"`
	Bucket   string `json:"bucket"`
	// Prefix is the complete object prefix, including the source-cluster path
	// component when the caller derives prefixes from a base prefix.
	Prefix    string `json:"prefix"`
	ClusterID string `json:"clusterID"`
}

func (s ArchiveScope) Validate() error {
	if s.Provider == "" || strings.TrimSpace(s.Provider) != s.Provider || s.Bucket == "" ||
		strings.TrimSpace(s.Bucket) != s.Bucket || s.ClusterID == "" || strings.TrimSpace(s.ClusterID) != s.ClusterID {
		return fmt.Errorf("fencing: incomplete archive scope")
	}
	if s.Prefix != strings.Trim(s.Prefix, "/") || strings.Contains(s.Prefix, "\\") || path.Clean(s.Prefix) != s.Prefix && s.Prefix != "" {
		return fmt.Errorf("fencing: noncanonical archive prefix")
	}
	return nil
}

func (s ArchiveScope) Hash() (string, error) {
	if err := s.Validate(); err != nil {
		return "", err
	}
	encoded, err := json.Marshal([]string{"rhiza-archive-scope-v1", s.Provider, s.Bucket, s.Prefix, s.ClusterID})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// ArchiveScopeClaim is create-only canonical authority state. ConfigID is the
// immutable archive protocol identity selected by the protected profile (not
// QuePaxa's evolving membership ConfigID). GenesisRef is written once by CAS.
type ArchiveScopeClaim struct {
	Version              int                `json:"version"`
	Scope                ArchiveScope       `json:"scope"`
	ScopeHash            string             `json:"scopeHash"`
	ConfigID             uint               `json:"configID,omitempty"`
	OwnerStatefulSetUID  string             `json:"ownerStatefulSetUID"`
	OwnerSourceClusterID string             `json:"ownerSourceClusterID"`
	RegistrationIdentity string             `json:"registrationIdentity"`
	GenesisRef           *ArchiveGenesisRef `json:"genesisRef,omitempty"`
}

type ArchiveGenesisStatusRequest struct {
	ArchiveKey string `json:"archiveKey"`
	ConfigID   uint   `json:"configID"`
}

// ArchiveGenesisStatus is returned only by the authenticated POST status
// operation. ConfigID is the registered immutable archive identity, including
// when no genesis reference exists yet. CanInitializeEmpty means the canonical
// claim is owned by this root and has no genesis reference; the caller must
// still validate that its local archive has no current head before writing.
type ArchiveGenesisStatus struct {
	ScopeHash          string                `json:"scopeHash"`
	MutationScopeHash  string                `json:"mutationScopeHash"`
	ConfigID           uint                  `json:"configID,omitempty"`
	CanInitializeEmpty bool                  `json:"canInitializeEmpty,omitempty"`
	Ref                *ArchiveGenesisRef    `json:"ref,omitempty"`
	Mutation           *MutationLookupResult `json:"mutation,omitempty"`
}

func (s ArchiveGenesisStatus) Validate(mutationScopeHash string, expectedConfigID uint) error {
	if expectedConfigID == 0 || s.ConfigID != expectedConfigID || !mutationDigestValid(s.ScopeHash) || !mutationDigestValid(mutationScopeHash) || s.MutationScopeHash != mutationScopeHash || (s.CanInitializeEmpty && s.Ref != nil) {
		return fmt.Errorf("fencing: invalid archive genesis status")
	}
	if s.Ref == nil {
		if !s.CanInitializeEmpty {
			return fmt.Errorf("fencing: archive genesis is not authorized")
		}
		return nil
	}
	if s.Ref.ScopeHash != s.ScopeHash || s.Ref.ConfigID != s.ConfigID || s.ConfigID != expectedConfigID || s.Ref.Validate() != nil || s.CanInitializeEmpty {
		return fmt.Errorf("fencing: archive genesis status reference differs")
	}
	if s.Ref.Mutation != nil {
		if s.Ref.Mutation.Intent.StoreScopeHash != s.MutationScopeHash || s.Mutation == nil || !s.Mutation.Found || s.Mutation.Entry == nil || s.Mutation.Entry.Intent != s.Ref.Mutation.Intent {
			return fmt.Errorf("fencing: archive genesis mutation evidence is incomplete")
		}
	} else if s.Mutation != nil {
		return fmt.Errorf("fencing: fork genesis status has mutation evidence")
	}
	return nil
}

func (c ArchiveScopeClaim) Validate() error {
	hash, err := c.Scope.Hash()
	if err != nil || c.Version != ArchiveGenesisVersion || c.ConfigID == 0 || c.ScopeHash != hash || c.OwnerStatefulSetUID == "" ||
		c.OwnerSourceClusterID != c.Scope.ClusterID || !mutationDigestValid(c.RegistrationIdentity) {
		return fmt.Errorf("fencing: invalid archive scope claim")
	}
	if c.GenesisRef != nil && (c.GenesisRef.ConfigID == 0 || c.GenesisRef.ConfigID != c.ConfigID || c.GenesisRef.ValidateForScope(c.Scope) != nil) {
		return fmt.Errorf("fencing: archive scope claim genesis reference differs")
	}
	return nil
}
