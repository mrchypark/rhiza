package main

import (
	"context"
	"github.com/mrchypark/rhiza/pkg/managedbootstrap"
	"github.com/mrchypark/rhiza/pkg/managedsql"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

const repairHandoffPath = managedbootstrap.RepairHandoffPath
const repairFileLimit = managedbootstrap.RepairFileLimit

type repairGateFile = managedbootstrap.RepairGateFile

func checkRepairGate(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, generation int64, podName, podUID, identity string, allowCompleted bool) (operator.V1Alpha2BootstrapPlan, repairGateFile, error) {
	return managedbootstrap.CheckRepairGate(ctx, kube, ref, generation, podName, podUID, identity, allowCompleted)
}

func checkSavedRepairGate(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, generation int64, podName, podUID, identity string, saved repairGateFile) (operator.V1Alpha2BootstrapPlan, error) {
	return managedbootstrap.CheckSavedRepairGate(ctx, kube, ref, generation, podName, podUID, identity, saved)
}

func writeGateFile(identity, output string, value any) error {
	return managedbootstrap.WriteGateFile(identity, output, value)
}

func writeImmutableRepairFile(identity, output string, value repairGateFile) error {
	return managedbootstrap.WriteImmutableRepairFile(identity, output, value)
}

func writeImmutableGateFile(identity, output string, value any) error {
	return managedbootstrap.WriteImmutableGateFile(identity, output, value)
}

func readGateFile(path string, dst any, limit int64, private bool) error {
	return managedbootstrap.ReadGateFile(path, dst, limit, private)
}

func managedRepairConfig(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, podName, podUID, identity, bootstrapPath, repairPath string, profile operator.V1Alpha2RuntimeProfile) (managedsql.Config, func() error, error) {
	return managedbootstrap.ManagedRepairConfig(ctx, kube, ref, podName, podUID, identity, bootstrapPath, repairPath, profile)
}

func verifyRepairControl(ctx context.Context, bucket objstore.Bucket, bootstrap quepaxa.Cluster, view *recovery.ReadOnlyView, capture operator.V1Alpha2RepairHandoff) (resultErr error) {
	return managedbootstrap.VerifyRepairControl(ctx, bucket, bootstrap, view, capture)
}
