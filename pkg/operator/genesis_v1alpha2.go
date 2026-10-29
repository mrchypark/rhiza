package operator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"unicode/utf8"
)

type v1alpha2Genesis struct {
	OwnerUID      string `json:"ownerUID"`
	LogicalID     string `json:"logicalID"`
	DeploymentUID string `json:"deploymentUID"`
	StorageID     string `json:"storageID"`
	IntentHash    string `json:"intentHash"`
}

func genesisHash(ref V1Alpha2ClusterRef, logicalID, deploymentUID, storageID string) string {
	b, _ := json.Marshal([]string{"rhiza-v1alpha2-genesis", ref.Namespace, ref.Name, ref.UID, logicalID, deploymentUID, storageID, "Create"})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func genesisStorageID(uid string) string {
	h := sha256.Sum256([]byte("rhiza-v1alpha2-storage\x00" + uid + "\x000"))
	return "rhiza-" + hex.EncodeToString(h[:])
}

// V1Alpha2GenesisDeploymentName identifies the generation-zero Deployment plan.
func V1Alpha2GenesisDeploymentName(clusterUID string) (string, error) {
	if !v1alpha2NonBlank.MatchString(clusterUID) || utf8.RuneCountInString(clusterUID) > 256 {
		return "", fmt.Errorf("invalid cluster UID")
	}
	h := sha256.Sum256([]byte(clusterUID))
	return "rhiza-" + hex.EncodeToString(h[:10]) + "-g0", nil
}

func validateGenesisReceipt(raw json.RawMessage, ref V1Alpha2ClusterRef, logicalID string) error {
	var r v1alpha2Genesis
	if decodeV1Alpha2(raw, &r) != nil || r.OwnerUID != ref.UID || !v1alpha2NonBlank.MatchString(r.OwnerUID) || utf8.RuneCountInString(r.OwnerUID) > 256 || r.LogicalID != logicalID || !v1alpha2NonBlank.MatchString(r.LogicalID) || utf8.RuneCountInString(r.LogicalID) > 256 || !v1alpha2NonBlank.MatchString(r.DeploymentUID) || utf8.RuneCountInString(r.DeploymentUID) > 256 || r.StorageID != genesisStorageID(ref.UID) || r.IntentHash != genesisHash(ref, logicalID, r.DeploymentUID, r.StorageID) {
		return fmt.Errorf("invalid genesis receipt")
	}
	return nil
}

// InitializeV1Alpha2Authority initializes only a creation-intent cluster. The
// storage ID is a logical namespace, not evidence of a provisioned bucket.
func InitializeV1Alpha2Authority(ctx context.Context, s *V1Alpha2StatusStore, ref V1Alpha2ClusterRef, deploymentName string) (V1Alpha2StatusSnapshot, error) {
	return initializeV1Alpha2Authority(ctx, s, ref, deploymentName, nil, "", "")
}

func initializeV1Alpha2Authority(ctx context.Context, s *V1Alpha2StatusStore, ref V1Alpha2ClusterRef, deploymentName string, expected *V1Alpha2ClusterSpec, expectedUID, expectedRV string) (V1Alpha2StatusSnapshot, error) {
	p, err := s.path(ref)
	if err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	expectedName, err := V1Alpha2GenesisDeploymentName(ref.UID)
	if err != nil || deploymentName != expectedName {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("deployment name does not match genesis plan")
	}
	var obj map[string]json.RawMessage
	if err = s.Kube.Get(ctx, p, &obj); err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	clusterJSON, err := json.Marshal(obj)
	if err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	cluster, err := DecodeV1Alpha2Cluster(clusterJSON)
	if err != nil {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("invalid cluster: %w", err)
	}
	if expected != nil && !reflect.DeepEqual(cluster.Spec, *expected) {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("cluster intent changed during candidate creation")
	}
	meta, err := jsonObject(obj["metadata"])
	if err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	var version, kind, uid, name, ns, rv string
	_ = json.Unmarshal(obj["apiVersion"], &version)
	_ = json.Unmarshal(obj["kind"], &kind)
	_ = json.Unmarshal(meta["uid"], &uid)
	_ = json.Unmarshal(meta["name"], &name)
	_ = json.Unmarshal(meta["namespace"], &ns)
	_ = json.Unmarshal(meta["resourceVersion"], &rv)
	if version != V1Alpha2APIVersion || kind != "RhizaCluster" || uid != ref.UID || name != ref.Name || ns != ref.Namespace || !v1alpha2NonBlank.MatchString(rv) || (len(meta["deletionTimestamp"]) > 0 && string(meta["deletionTimestamp"]) != "null") {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("invalid cluster identity")
	}
	spec, err := jsonObject(obj["spec"])
	if err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	var intent, logicalID string
	if json.Unmarshal(spec["genesisIntent"], &intent) != nil || intent != "Create" || json.Unmarshal(spec["logicalID"], &logicalID) != nil || !v1alpha2NonBlank.MatchString(logicalID) || utf8.RuneCountInString(logicalID) > 256 {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("cluster lacks valid creation intent")
	}
	status := map[string]json.RawMessage{}
	if len(obj["status"]) > 0 && string(obj["status"]) != "null" {
		status, err = jsonObject(obj["status"])
		if err != nil {
			return V1Alpha2StatusSnapshot{}, err
		}
	}
	if _, ok := status["genesis"]; ok {
		return s.snapshot(ref, obj)
	}
	if _, ok := status["authority"]; ok {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("authority without genesis receipt")
	}
	for _, key := range []string{"recoveryQueue", "pendingWrite", "transition", "receipt", "decision"} {
		if _, ok := status[key]; ok {
			return V1Alpha2StatusSnapshot{}, fmt.Errorf("existing %s prevents genesis", key)
		}
	}
	var deployment struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Metadata   struct {
			Name              string            `json:"name"`
			Namespace         string            `json:"namespace"`
			UID               string            `json:"uid"`
			ResourceVersion   string            `json:"resourceVersion"`
			DeletionTimestamp *string           `json:"deletionTimestamp"`
			Labels            map[string]string `json:"labels"`
			OwnerReferences   []struct {
				APIVersion string `json:"apiVersion"`
				Kind       string `json:"kind"`
				Name       string `json:"name"`
				UID        string `json:"uid"`
				Controller bool   `json:"controller"`
			} `json:"ownerReferences"`
		} `json:"metadata"`
	}
	if err = s.Kube.Get(ctx, "/apis/apps/v1/namespaces/"+ref.Namespace+"/deployments/"+deploymentName, &deployment); err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	d := deployment.Metadata
	if expectedUID != "" && (d.UID != expectedUID || d.ResourceVersion != expectedRV) {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("candidate changed")
	}
	owners := 0
	for _, owner := range d.OwnerReferences {
		if owner.Controller {
			owners++
			if owner.APIVersion != V1Alpha2APIVersion || owner.Kind != "RhizaCluster" || owner.Name != ref.Name || owner.UID != ref.UID {
				return V1Alpha2StatusSnapshot{}, fmt.Errorf("unexpected controller owner")
			}
		}
	}
	if deployment.APIVersion != "apps/v1" || deployment.Kind != "Deployment" || d.Name != deploymentName || d.Namespace != ref.Namespace || !v1alpha2NonBlank.MatchString(d.UID) || utf8.RuneCountInString(d.UID) > 256 || !v1alpha2NonBlank.MatchString(d.ResourceVersion) || d.DeletionTimestamp != nil || owners != 1 || d.Labels["rhiza.mrchypark.dev/cluster-uid"] != ref.UID || d.Labels["rhiza.mrchypark.dev/generation"] != "0" {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("deployment identity or generation mismatch")
	}
	storageID := genesisStorageID(ref.UID)
	receipt := v1alpha2Genesis{ref.UID, logicalID, d.UID, storageID, genesisHash(ref, logicalID, d.UID, storageID)}
	authority := map[string]any{"owner": map[string]any{"namespace": ref.Namespace, "name": ref.Name, "uid": ref.UID, "logicalID": logicalID}, "generation": 0, "binding": map[string]string{"deploymentUID": d.UID, "storageID": storageID}, "pendingWrite": false}
	status["genesis"], _ = json.Marshal(receipt)
	status["authority"], _ = json.Marshal(authority)
	statusRaw, _ := json.Marshal(status)
	obj["status"] = statusRaw
	var response map[string]json.RawMessage
	err = s.Kube.Put(ctx, p+"/status", obj, &response)
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("%w: %v", ErrV1Alpha2StatusConflict, err)
	}
	if err == nil {
		if snap, e := s.snapshot(ref, response); e == nil && snap.resourceVersion != rv && sameJSON(snap.object["status"], statusRaw) {
			return snap, nil
		}
	}
	var live map[string]json.RawMessage
	if s.Kube.Get(ctx, p, &live) == nil {
		if snap, e := s.snapshot(ref, live); e == nil && snap.resourceVersion != rv && sameJSON(snap.object["status"], statusRaw) {
			return snap, nil
		}
	}
	return V1Alpha2StatusSnapshot{}, fmt.Errorf("%w: genesis update or readback could not establish the result", ErrV1Alpha2StatusOutcomeUnknown)
}
