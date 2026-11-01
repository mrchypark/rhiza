// Package managedbootstrap validates the installed managed SQL startup inputs.
// It starts no application listener and does not relax live authority checks.
package managedbootstrap

import (
	"context"
	"errors"

	"github.com/mrchypark/rhiza/pkg/managedsql"
	"github.com/mrchypark/rhiza/pkg/operator"
)

type ConfigRequest struct {
	Kube                                    *operator.Kubernetes
	Ref                                     operator.V1Alpha2ClusterRef
	PodName, PodUID                         string
	IdentityPath, BootstrapPath, RepairPath string
	Profile                                 operator.V1Alpha2RuntimeProfile
}

// LoadConfig returns verified configuration and an owned SDK cleanup function.
// Cleanup must run after Host.Close; failed/partial local evidence is preserved.
func LoadConfig(ctx context.Context, r ConfigRequest) (managedsql.Config, func() error, error) {
	cfg, err := ManagedConfig(ctx, r.Kube, r.Ref, r.PodName, r.PodUID, r.IdentityPath, r.BootstrapPath, r.Profile)
	if errors.Is(err, ErrLearnerAdmissionPending) {
		return ManagedRepairConfig(ctx, r.Kube, r.Ref, r.PodName, r.PodUID, r.IdentityPath, r.BootstrapPath, r.RepairPath, r.Profile)
	}
	return cfg, func() error { return nil }, err
}
