package main

import (
	"context"
	"github.com/mrchypark/rhiza/pkg/managedbootstrap"
	"github.com/mrchypark/rhiza/pkg/operator"
)

var errBootstrapPending = managedbootstrap.ErrBootstrapPending
var errLearnerAdmissionPending = managedbootstrap.ErrLearnerAdmissionPending

type gateOwner = managedbootstrap.GateOwner
type gateMeta = managedbootstrap.GateMeta

func gateController(m gateMeta, api, kind string) (gateOwner, error) {
	return managedbootstrap.GateController(m, api, kind)
}

func checkGate(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, generation int64, podName, podUID, identityPath string) (operator.V1Alpha2BootstrapPlan, error) {
	return managedbootstrap.CheckGate(ctx, kube, ref, generation, podName, podUID, identityPath)
}

func checkGateMember(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, generation int64, podName, podUID, identityPath string, replacement *operator.V1Alpha2BootstrapMember) (operator.V1Alpha2BootstrapPlan, error) {
	return managedbootstrap.CheckGateMember(ctx, kube, ref, generation, podName, podUID, identityPath, replacement)
}

func awaitBootstrap(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, generation int64, podName, podUID, identityPath, output string) error {
	return managedbootstrap.AwaitBootstrap(ctx, kube, ref, generation, podName, podUID, identityPath, output)
}

func awaitBootstrapWithRepair(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, generation int64, podName, podUID, identityPath, output, repairOutput string) error {
	return managedbootstrap.AwaitBootstrapWithRepair(ctx, kube, ref, generation, podName, podUID, identityPath, output, repairOutput)
}

func validateHandoffPath(identity, output string) error {
	return managedbootstrap.ValidateHandoffPath(identity, output)
}

func probeBootstrap(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef) error {
	return managedbootstrap.ProbeBootstrap(ctx, kube, ref)
}
