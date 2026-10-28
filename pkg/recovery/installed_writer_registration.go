package recovery

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const installedWriterRegistrationMaxSize = 64 << 10

// InstalledWriterRegistration binds a writer incarnation to immutable public
// installation facts. It does not grant admission or selection authority.
type InstalledWriterRegistration struct {
	ClusterID           string            `json:"cluster_id"`
	InstalledPolicyHash [32]byte          `json:"installed_policy_hash"`
	NodeID              quepaxa.NodeID    `json:"node_id"`
	PublicKey           quepaxa.PublicKey `json:"public_key"`
	Nonce               string            `json:"nonce"`
	BootstrapMembership string            `json:"bootstrap_membership"`
	StorageBindingID    string            `json:"storage_binding_id"`
	Registered          bool              `json:"registered"`
}

// FinalizedCanonicalBytes encodes only a complete remote registration.
func (r InstalledWriterRegistration) FinalizedCanonicalBytes() ([]byte, error) {
	if r.ClusterID == "" || r.InstalledPolicyHash == ([32]byte{}) || r.NodeID == "" || r.PublicKey == (quepaxa.PublicKey{}) || r.StorageBindingID == "" || !r.Registered {
		return nil, fmt.Errorf("installed writer registration is incomplete")
	}
	if value, err := decodeInstalledPolicyHash(r.Nonce); err != nil || value == ([32]byte{}) {
		return nil, fmt.Errorf("installed writer registration nonce is invalid")
	}
	if value, err := decodeInstalledPolicyHash(r.BootstrapMembership); err != nil || value == ([32]byte{}) {
		return nil, fmt.Errorf("installed writer registration bootstrap membership is invalid")
	}
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(data) >= installedWriterRegistrationMaxSize {
		return nil, fmt.Errorf("installed writer registration exceeds size limit")
	}
	return data, nil
}

// ParseFinalizedInstalledWriterRegistration accepts only exact canonical bytes.
func ParseFinalizedInstalledWriterRegistration(data []byte) (InstalledWriterRegistration, error) {
	var r InstalledWriterRegistration
	if len(data) == 0 || len(data) >= installedWriterRegistrationMaxSize {
		return r, fmt.Errorf("installed writer registration size is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&r); err != nil {
		return InstalledWriterRegistration{}, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return InstalledWriterRegistration{}, fmt.Errorf("installed writer registration has trailing data")
	}
	canonical, err := r.FinalizedCanonicalBytes()
	if err != nil {
		return InstalledWriterRegistration{}, err
	}
	if !bytes.Equal(data, canonical) {
		return InstalledWriterRegistration{}, fmt.Errorf("installed writer registration is not canonical JSON")
	}
	return r, nil
}

// InstalledWriterIdentityHash hashes the exact validated finalized remote bytes.
func InstalledWriterIdentityHash(data []byte) ([32]byte, error) {
	if _, err := ParseFinalizedInstalledWriterRegistration(data); err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(data), nil
}
