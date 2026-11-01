package managedbootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/operator"
)

var ErrBootstrapPending = errors.New("bootstrap genesis pending")
var ErrLearnerAdmissionPending = errors.New("learner admission pending")

type GateOwner struct {
	APIVersion, Kind, Name, UID string
	Controller                  *bool
}
type GateMeta struct {
	Name, Namespace, UID, ResourceVersion string
	DeletionTimestamp                     *string
	OwnerReferences                       []GateOwner
}

func GateController(m GateMeta, api, kind string) (GateOwner, error) {
	var result GateOwner
	count := 0
	for _, o := range m.OwnerReferences {
		if o.Controller != nil && *o.Controller {
			result = o
			count++
		}
	}
	if count != 1 || result.APIVersion != api || result.Kind != kind || result.Name == "" || result.UID == "" {
		return result, errors.New("invalid controller owner")
	}
	return result, nil
}

func CheckGate(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, generation int64, podName, podUID, identityPath string) (operator.V1Alpha2BootstrapPlan, error) {
	return CheckGateMember(ctx, kube, ref, generation, podName, podUID, identityPath, nil)
}

func CheckGateMember(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, generation int64, podName, podUID, identityPath string, replacement *operator.V1Alpha2BootstrapMember) (operator.V1Alpha2BootstrapPlan, error) {
	var empty operator.V1Alpha2BootstrapPlan
	cluster, err := types.ManagedClusterID(ref.UID, generation)
	if err != nil {
		return empty, err
	}
	node, err := types.ManagedNodeID(podUID)
	if err != nil {
		return empty, err
	}
	token, err := ReadIdentityFile(identityPath, string(cluster), string(node))
	if err != nil {
		return empty, err
	}
	if err := ProbeBootstrap(ctx, kube, ref); err != nil {
		return empty, err
	}
	store := &operator.V1Alpha2StatusStore{Kube: kube}
	snap, err := store.Load(ctx, ref)
	if err != nil {
		return empty, err
	}
	plan, err := snap.BootstrapPlan()
	if err != nil {
		return empty, err
	}
	init, err := operator.LoadV1Alpha2ApplicationInit(ctx, store, ref)
	if err != nil {
		return empty, err
	}
	if init.Authority() != snap.Authority() || init.Assignment().Plan.Generation != generation || init.Authority().Generation != generation || plan.ClusterID != cluster {
		return empty, errors.New("live bootstrap authority changed")
	}
	var entry *operator.V1Alpha2BootstrapMember
	for i := range plan.Members {
		if plan.Members[i].PodUID == podUID {
			entry = &plan.Members[i]
			break
		}
	}
	if entry == nil && replacement != nil && replacement.PodUID == podUID {
		entry = replacement
	}
	if entry == nil {
		return empty, ErrLearnerAdmissionPending
	}
	if entry.Member.ID != node || entry.Member.PublicKey != rhiza.PeerPublicKey(string(cluster), string(node), token) {
		return empty, errors.New("local identity differs from bootstrap plan")
	}
	base := "/api/v1/namespaces/" + url.PathEscape(ref.Namespace) + "/pods/" + url.PathEscape(podName)
	var pod struct {
		APIVersion, Kind string
		Metadata         GateMeta
		Status           struct{ PodIP string }
	}
	if err := kube.Get(ctx, base, &pod); err != nil {
		return empty, err
	}
	if pod.APIVersion != "v1" || pod.Kind != "Pod" || pod.Metadata.Name != podName || pod.Metadata.Namespace != ref.Namespace || pod.Metadata.UID != podUID || pod.Metadata.ResourceVersion == "" || pod.Metadata.DeletionTimestamp != nil {
		return empty, errors.New("live pod identity mismatch")
	}
	owner, err := GateController(pod.Metadata, "apps/v1", "ReplicaSet")
	if err != nil {
		return empty, err
	}
	apps := "/apis/apps/v1/namespaces/" + url.PathEscape(ref.Namespace)
	var rs struct {
		APIVersion, Kind string
		Metadata         GateMeta
	}
	if err := kube.Get(ctx, apps+"/replicasets/"+url.PathEscape(owner.Name), &rs); err != nil {
		return empty, err
	}
	if rs.APIVersion != "apps/v1" || rs.Kind != "ReplicaSet" || rs.Metadata.Name != owner.Name || rs.Metadata.Namespace != ref.Namespace || rs.Metadata.UID != owner.UID || rs.Metadata.ResourceVersion == "" || rs.Metadata.DeletionTimestamp != nil {
		return empty, errors.New("replica set identity mismatch")
	}
	depOwner, err := GateController(rs.Metadata, "apps/v1", "Deployment")
	if err != nil {
		return empty, err
	}
	var dep struct {
		APIVersion, Kind string
		Metadata         GateMeta
	}
	if err := kube.Get(ctx, apps+"/deployments/"+url.PathEscape(depOwner.Name), &dep); err != nil {
		return empty, err
	}
	if dep.APIVersion != "apps/v1" || dep.Kind != "Deployment" || dep.Metadata.Name != depOwner.Name || dep.Metadata.Namespace != ref.Namespace || dep.Metadata.UID != depOwner.UID || dep.Metadata.UID != init.Authority().Binding.DeploymentUID || dep.Metadata.ResourceVersion == "" || dep.Metadata.DeletionTimestamp != nil {
		return empty, errors.New("deployment identity mismatch")
	}
	crOwner, err := GateController(dep.Metadata, operator.V1Alpha2APIVersion, "RhizaCluster")
	if err != nil || crOwner.Name != ref.Name || crOwner.UID != ref.UID {
		return empty, errors.New("cluster controller mismatch")
	}
	u, err := url.Parse(entry.Member.PeerURL)
	if err != nil {
		return empty, err
	}
	host, port, err := net.SplitHostPort(u.Host)
	ip := net.ParseIP(pod.Status.PodIP)
	n, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || n < 1 || n > 65535 || ip == nil || !ip.Equal(net.ParseIP(host)) {
		return empty, errors.New("live pod endpoint mismatch")
	}
	return plan, nil
}

func AwaitBootstrap(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, generation int64, podName, podUID, identityPath, output string) error {
	return AwaitBootstrapWithRepair(ctx, kube, ref, generation, podName, podUID, identityPath, output, filepath.Join(filepath.Dir(output), "repair.json"))
}

func AwaitBootstrapWithRepair(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef, generation int64, podName, podUID, identityPath, output, repairOutput string) error {
	if _, err := os.Lstat(repairOutput); err == nil {
		var saved RepairGateFile
		if err := ReadGateFile(repairOutput, &saved, RepairFileLimit, true); err != nil {
			return err
		}
		plan, err := CheckSavedRepairGate(ctx, kube, ref, generation, podName, podUID, identityPath, saved)
		if err != nil {
			return err
		}
		var original operator.V1Alpha2BootstrapPlan
		if err := ReadGateFile(output, &original, 65536, false); err != nil || !reflect.DeepEqual(original, plan) {
			return errors.New("original bootstrap handoff missing or changed")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for {
		plan, err := CheckGate(ctx, kube, ref, generation, podName, podUID, identityPath)
		if errors.Is(err, ErrLearnerAdmissionPending) {
			var repair RepairGateFile
			plan, repair, err = CheckRepairGate(ctx, kube, ref, generation, podName, podUID, identityPath, false)
			if err == nil {
				if filepath.Clean(output) == filepath.Clean(repairOutput) {
					return errors.New("distinct bootstrap and repair handoffs required")
				}
				if err := WriteImmutableGateFile(identityPath, output, plan); err != nil {
					return err
				}
				if err := ValidateHandoffPath(output, repairOutput); err != nil {
					return err
				}
				return WriteImmutableRepairFile(identityPath, repairOutput, repair)
			}
		}
		if err == nil {
			return WriteGateFile(identityPath, output, plan)
		}
		if !errors.Is(err, ErrBootstrapPending) && !errors.Is(err, ErrLearnerAdmissionPending) {
			return err
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func ValidateHandoffPath(identity, output string) error {
	identityInfo, err := os.Stat(identity)
	if err != nil {
		return err
	}
	info, err := os.Stat(output)
	if err == nil && os.SameFile(identityInfo, info) {
		return errors.New("handoff aliases private identity")
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	identityDir, err := filepath.EvalSymlinks(filepath.Dir(identity))
	if err != nil {
		return err
	}
	outputDir, err := filepath.EvalSymlinks(filepath.Dir(output))
	if err != nil {
		return err
	}
	if filepath.Join(identityDir, filepath.Base(identity)) == filepath.Join(outputDir, filepath.Base(output)) {
		return errors.New("handoff aliases private identity")
	}
	return nil
}

// ProbeBootstrap only treats a completely absent status as a new pending CR.
func ProbeBootstrap(ctx context.Context, kube *operator.Kubernetes, ref operator.V1Alpha2ClusterRef) error {
	if kube == nil || kube.Namespace != ref.Namespace || ref.Namespace == "" || ref.Name == "" || ref.UID == "" {
		return errors.New("invalid cluster reference")
	}
	path := "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/" + url.PathEscape(ref.Namespace) + "/rhizaclusters/" + url.PathEscape(ref.Name)
	var raw json.RawMessage
	if err := kube.Get(ctx, path, &raw); err != nil {
		return err
	}
	c, err := operator.DecodeV1Alpha2Cluster(raw)
	if err != nil {
		return err
	}
	var name, namespace, uid, rv string
	_ = json.Unmarshal(c.Metadata["name"], &name)
	_ = json.Unmarshal(c.Metadata["namespace"], &namespace)
	_ = json.Unmarshal(c.Metadata["uid"], &uid)
	_ = json.Unmarshal(c.Metadata["resourceVersion"], &rv)
	if name != ref.Name || namespace != ref.Namespace || uid != ref.UID || rv == "" || c.Spec.GenesisIntent == nil || *c.Spec.GenesisIntent != "Create" {
		return errors.New("invalid bootstrap cluster identity")
	}
	if deletion := c.Metadata["deletionTimestamp"]; len(deletion) != 0 && !bytes.Equal(bytes.TrimSpace(deletion), []byte("null")) {
		return errors.New("cluster deleting")
	}
	var status map[string]json.RawMessage
	if len(c.Status) != 0 {
		if json.Unmarshal(c.Status, &status) != nil || status == nil {
			return errors.New("invalid bootstrap status")
		}
	}
	if len(status) == 0 {
		return ErrBootstrapPending
	}
	store := &operator.V1Alpha2StatusStore{Kube: kube}
	_, err = store.Load(ctx, ref)
	if err != nil {
		return fmt.Errorf("invalid committed bootstrap status: %w", err)
	}
	return nil
}
