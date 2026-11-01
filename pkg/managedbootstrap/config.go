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
	if init.Assignment().Plan.EvidenceFormat != profile.EvidenceFormat || profile.EvidenceFormat != managedsql.EvidenceFormat {
		return result, errors.New("unsupported evidence profile")
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
