package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
)

const V1Alpha2ManagedHostCleanupFinalizer = "rhiza.mrchypark.dev/managed-host-cleanup"

type managedHostClusterRecord struct {
	object     map[string]json.RawMessage
	metadata   map[string]json.RawMessage
	finalizers []string
	deleting   bool
}

// No Pod/API observation proves process termination or completion of in-flight
// work. The operator installs the finalizer but never removes it automatically.
func reconcileV1Alpha2ManagedHostClusterLifecycle(ctx context.Context, kube *Kubernetes, ref V1Alpha2ClusterRef) (bool, error) {
	record, err := loadManagedHostClusterRecord(ctx, kube, ref)
	if err != nil {
		return false, err
	}
	if record.deleting {
		if !managedHostFinalizerPresent(record.finalizers) {
			return true, fmt.Errorf("deleting cluster lacks %s; no automatic cleanup was attempted", V1Alpha2ManagedHostCleanupFinalizer)
		}
		return true, nil
	}
	if managedHostFinalizerPresent(record.finalizers) {
		return false, nil
	}
	finalizers := append(append([]string(nil), record.finalizers...), V1Alpha2ManagedHostCleanupFinalizer)
	if err := installManagedHostCleanupFinalizer(ctx, kube, ref, record, finalizers); err != nil {
		return false, err
	}
	return false, nil
}

func loadManagedHostClusterRecord(ctx context.Context, kube *Kubernetes, ref V1Alpha2ClusterRef) (managedHostClusterRecord, error) {
	var zero managedHostClusterRecord
	if kube == nil || kube.Namespace != ref.Namespace || !validNamespace(ref.Namespace) || len(ref.Name) == 0 || len(ref.Name) > 253 || !v1alpha2Name.MatchString(ref.Name) || !v1alpha2NonBlank.MatchString(ref.UID) {
		return zero, fmt.Errorf("managed host finalizer requires exact same-namespace cluster identity")
	}
	path := managedHostClusterPath(ref)
	var obj map[string]json.RawMessage
	if err := kube.Get(ctx, path, &obj); err != nil {
		return zero, fmt.Errorf("load managed host cluster: %w", err)
	}
	var version, kind string
	_ = json.Unmarshal(obj["apiVersion"], &version)
	_ = json.Unmarshal(obj["kind"], &kind)
	meta, err := jsonObject(obj["metadata"])
	if err != nil {
		return zero, fmt.Errorf("invalid managed host cluster metadata")
	}
	var name, namespace, uid, rv string
	_ = json.Unmarshal(meta["name"], &name)
	_ = json.Unmarshal(meta["namespace"], &namespace)
	_ = json.Unmarshal(meta["uid"], &uid)
	_ = json.Unmarshal(meta["resourceVersion"], &rv)
	if version != V1Alpha2APIVersion || kind != "RhizaCluster" || name != ref.Name || namespace != ref.Namespace || uid != ref.UID || !v1alpha2NonBlank.MatchString(rv) {
		return zero, fmt.Errorf("managed host cluster identity changed")
	}
	finalizers := []string{}
	if raw, ok := meta["finalizers"]; ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if json.Unmarshal(raw, &finalizers) != nil || finalizers == nil {
			return zero, fmt.Errorf("invalid managed host cluster finalizers")
		}
	}
	deleting := false
	if raw, ok := meta["deletionTimestamp"]; ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		var timestamp string
		if json.Unmarshal(raw, &timestamp) != nil || timestamp == "" {
			return zero, fmt.Errorf("invalid managed host cluster deletion timestamp")
		}
		deleting = true
	}
	return managedHostClusterRecord{object: obj, metadata: meta, finalizers: finalizers, deleting: deleting}, nil
}

func managedHostClusterPath(ref V1Alpha2ClusterRef) string {
	return "/apis/" + V1Alpha2APIVersion + "/namespaces/" + ref.Namespace + "/rhizaclusters/" + ref.Name
}

func managedHostFinalizerPresent(finalizers []string) bool {
	for _, finalizer := range finalizers {
		if finalizer == V1Alpha2ManagedHostCleanupFinalizer {
			return true
		}
	}
	return false
}

func installManagedHostCleanupFinalizer(ctx context.Context, kube *Kubernetes, ref V1Alpha2ClusterRef, record managedHostClusterRecord, finalizers []string) error {
	meta := record.metadata
	meta["finalizers"], _ = json.Marshal(finalizers)
	record.object["metadata"], _ = json.Marshal(meta)
	path := managedHostClusterPath(ref)
	err := kube.Put(ctx, path, record.object, nil)
	live, readErr := loadManagedHostClusterRecord(ctx, kube, ref)
	if readErr == nil && managedHostFinalizerPresent(live.finalizers) {
		for _, finalizer := range record.finalizers {
			if !managedHostContainsFinalizer(live.finalizers, finalizer) {
				return fmt.Errorf("managed host cluster lost another finalizer")
			}
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("install managed host cleanup finalizer: %w", err)
	}
	return fmt.Errorf("managed host cleanup finalizer was not persisted")
}

func managedHostContainsFinalizer(finalizers []string, wanted string) bool {
	for _, finalizer := range finalizers {
		if finalizer == wanted {
			return true
		}
	}
	return false
}
