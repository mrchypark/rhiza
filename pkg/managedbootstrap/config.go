package managedbootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"reflect"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/managedsql"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
)

func ManagedConfig(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, podName, podUID, identityPath, handoffPath string, profile operator.V1Alpha2RuntimeProfile) (managedsql.Config, error) {
	var result managedsql.Config
	plan, err := CheckGate(ctx, kube, ref, 0, podName, podUID, identityPath)
	if err != nil {
		return result, err
	}
	f, err := os.Open(handoffPath)
	if err != nil {
		return result, errors.New("invalid public handoff")
	}
	data, readErr := io.ReadAll(io.LimitReader(f, 65537))
	err = errors.Join(readErr, f.Close())
	if err != nil || len(data) > 65536 {
		return result, errors.New("invalid public handoff")
	}
	var handoff operator.V1Alpha2BootstrapPlan
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(&handoff) != nil || d.Decode(new(any)) != io.EOF || !reflect.DeepEqual(handoff, plan) {
		return result, errors.New("public handoff differs from live plan")
	}
	return ManagedRuntimeConfig(ctx, kube, ref, podUID, identityPath, profile, plan)
}

func ManagedRuntimeConfig(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, podUID, identityPath string, profile operator.V1Alpha2RuntimeProfile, plan operator.V1Alpha2BootstrapPlan) (managedsql.Config, error) {
	var result managedsql.Config
	store := &operator.V1Alpha2StatusStore{Kube: kube}
	init, err := operator.LoadV1Alpha2ApplicationInit(ctx, store, ref)
	if err != nil {
		return result, err
	}
	if init.Assignment().Plan.Generation != init.Authority().Generation || init.Assignment().Plan.DeploymentUID != init.Authority().Binding.DeploymentUID || init.Assignment().Plan.StorageID != init.Authority().Binding.StorageID {
		return result, errors.New("live initialization authority differs from assignment")
	}
	if err := validateManagedProfileBinding(ref, profile, init.Assignment().Plan, plan); err != nil {
		return result, err
	}
	// Do not reach legacy restore/open paths with an explicitly mixed assignment.
	if profile.ApplicationProtocol != "" {
		return result, errors.New("mixed managed runtime activation is not supported")
	}
	cluster, err := types.ManagedClusterID(ref.UID, 0)
	if err != nil {
		return result, err
	}
	id, err := types.ManagedNodeID(podUID)
	if err != nil {
		return result, err
	}
	secret, err := ReadIdentityFile(identityPath, string(cluster), string(id))
	if err != nil {
		return result, err
	}
	root := profile.Storage.ObjStorePrefix
	namespace, err := node.ManagedNamespaceFor(root, ref.UID, 0, init.Assignment().Plan.AssignmentID, string(cluster))
	if err != nil {
		return result, err
	}
	cfg := node.CloneManagedRuntimeConfig(profile.Storage)
	cfg.ClusterID = cluster
	cfg.NodeID = id
	cfg.PeerToken = secret
	cfg.DataDir = "/var/lib/rhiza/data"
	cfg.ObjStorePrefix = namespace.ConfigPrefix
	cfg.BindAddr = ""
	cfg.PeerAddr = net.JoinHostPort("0.0.0.0", fmt.Sprint(profile.PeerPort))
	cfg.Members = nil
	for _, m := range plan.Members {
		cfg.Members = append(cfg.Members, m.Member)
		if m.PodUID == podUID {
			u, e := url.Parse(m.Member.PeerURL)
			if e != nil {
				return result, e
			}
			_, port, e := net.SplitHostPort(u.Host)
			if e != nil || port != fmt.Sprint(profile.PeerPort) {
				return result, errors.New("peer port mismatch")
			}
			cfg.PeerAddr = u.Host
		}
	}
	if cfg.AdminToken == "" || cfg.AdminToken == secret {
		return result, errors.New("private API authentication token required")
	}
	return managedsql.Config{StorageRoot: root, Runtime: cfg, Store: store, Ref: ref, Plan: init.Assignment().Plan, PodUID: podUID}, nil
}

func validateManagedProfileBinding(ref operator.V1Alpha2ClusterRef, profile operator.V1Alpha2RuntimeProfile, assignment operator.V1Alpha2ApplicationInitPlan, plan operator.V1Alpha2BootstrapPlan) error {
	if err := operator.ValidateV1Alpha2ApplicationProfile(profile.ApplicationProtocol, profile.EvidenceFormat); err != nil {
		return err
	}
	if assignment.ApplicationProtocol != profile.ApplicationProtocol || assignment.ClusterUID != ref.UID || assignment.Generation != 0 || assignment.EvidenceFormat != profile.EvidenceFormat || plan.EvidenceFormat != profile.EvidenceFormat {
		return errors.New("live managed application profile differs from installed capability")
	}
	cluster, err := types.ManagedClusterID(ref.UID, assignment.Generation)
	if err != nil || plan.ClusterID != cluster {
		return errors.New("managed bootstrap cluster mismatch")
	}
	namespace, err := node.ManagedNamespaceFor(profile.Storage.ObjStorePrefix, ref.UID, assignment.Generation, assignment.AssignmentID, string(cluster))
	if err != nil {
		return err
	}
	config := node.CloneManagedRuntimeConfig(profile.Storage)
	config.ClusterID, config.ObjStorePrefix = cluster, namespace.ConfigPrefix
	binding, err := types.ManagedStorageBinding(config)
	if err != nil || binding != assignment.PhysicalBinding || binding != plan.PhysicalBinding {
		return errors.New("managed physical profile mismatch")
	}
	durability := config.ObjStoreDurability
	if durability == "" {
		durability = types.ObjectStoreDurabilityBeforeAck
	}
	if plan.Durability != durability || plan.Reconfiguration != config.EnableReconfiguration {
		return errors.New("managed bootstrap configuration mismatch")
	}
	members := make([]types.NodeConfig, 0, len(plan.Members))
	for _, member := range plan.Members {
		members = append(members, member.Member)
	}
	digest, err := types.ManagedBootstrapPlanDigestForProtocol(string(cluster), members, binding, profile.EvidenceFormat, durability, config.EnableReconfiguration, profile.ApplicationProtocol)
	if err != nil || digest != assignment.BootstrapPlanDigest {
		return errors.New("managed bootstrap profile digest mismatch")
	}
	return nil
}
