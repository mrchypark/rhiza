package operator

import (
	"fmt"

	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
)

// expectedAnchorBinding computes the local trust expectation without receipt
// data. opaqueStorageID is retained only for existing explicitly constructed
// API/test callers; production command configuration supplies config.
func expectedAnchorBinding(config *recoveryanchor.StorageBindingConfig, opaqueStorageID, clusterID string) (recoveryanchor.Binding, error) {
	if config != nil {
		if opaqueStorageID != "" {
			return recoveryanchor.Binding{}, fmt.Errorf("recovery anchor: derived/explicit binding configuration is ambiguous")
		}
		return config.ExpectedBinding(clusterID)
	}
	if clusterID == "" || opaqueStorageID == "" {
		return recoveryanchor.Binding{}, fmt.Errorf("recovery anchor: trusted binding configuration is unavailable")
	}
	return recoveryanchor.Binding{ClusterID: clusterID, StorageID: opaqueStorageID}, nil
}
