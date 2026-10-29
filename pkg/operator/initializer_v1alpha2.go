package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"
)

// ReconcileV1Alpha2Initialization creates only an inert generation-zero candidate.
func ReconcileV1Alpha2Initialization(ctx context.Context, kube *Kubernetes, ref V1Alpha2ClusterRef) (V1Alpha2StatusSnapshot, error) {
	store := &V1Alpha2StatusStore{Kube: kube}
	p, err := store.path(ref)
	if err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	var obj map[string]json.RawMessage
	if err = kube.Get(ctx, p, &obj); err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	b, _ := json.Marshal(obj)
	c, err := DecodeV1Alpha2Cluster(b)
	if err != nil {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("invalid cluster: %w", err)
	}
	var name, ns, uid, rv string
	_ = json.Unmarshal(c.Metadata["name"], &name)
	_ = json.Unmarshal(c.Metadata["namespace"], &ns)
	_ = json.Unmarshal(c.Metadata["uid"], &uid)
	_ = json.Unmarshal(c.Metadata["resourceVersion"], &rv)
	if name != ref.Name || ns != ref.Namespace || uid != ref.UID || !v1alpha2NonBlank.MatchString(rv) || (len(c.Metadata["deletionTimestamp"]) > 0 && string(c.Metadata["deletionTimestamp"]) != "null") {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("invalid cluster identity")
	}
	if c.Spec.GenesisIntent == nil || *c.Spec.GenesisIntent != "Create" {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("cluster lacks creation intent")
	}
	status := map[string]json.RawMessage{}
	if len(c.Status) > 0 && string(c.Status) != "null" {
		status, err = jsonObject(c.Status)
		if err != nil {
			return V1Alpha2StatusSnapshot{}, err
		}
	}
	if _, ok := status["genesis"]; ok {
		return store.snapshot(ref, obj)
	}
	for _, key := range []string{"authority", "recoveryQueue", "pendingWrite", "transition", "receipt", "decision"} {
		if _, ok := status[key]; ok {
			return V1Alpha2StatusSnapshot{}, fmt.Errorf("existing %s prevents genesis", key)
		}
	}
	depName, err := V1Alpha2GenesisDeploymentName(ref.UID)
	if err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	labels := map[string]string{"rhiza.mrchypark.dev/cluster-uid": ref.UID, "rhiza.mrchypark.dev/generation": "0"}
	desired := map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": depName, "namespace": ref.Namespace, "labels": labels, "ownerReferences": []any{map[string]any{"apiVersion": V1Alpha2APIVersion, "kind": "RhizaCluster", "name": ref.Name, "uid": ref.UID, "controller": true}}}, "spec": map[string]any{"replicas": 0, "selector": map[string]any{"matchLabels": labels}, "template": map[string]any{"metadata": map[string]any{"labels": labels}, "spec": map[string]any{"containers": c.Spec.Workload.PodTemplate.Spec.Containers}}}}
	base := "/apis/apps/v1/namespaces/" + ref.Namespace + "/deployments"
	var created map[string]json.RawMessage
	postErr := kube.Post(ctx, base, desired, &created)
	var apiErr *APIError
	if errors.As(postErr, &apiErr) && apiErr.StatusCode != http.StatusConflict {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("create candidate: %w", postErr)
	}
	// Even a successful response is read back: the live UID and admitted template matter.
	var live map[string]json.RawMessage
	if err := kube.Get(ctx, base+"/"+depName, &live); err != nil {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("candidate creation outcome unknown: %w", err)
	}
	if err := validateInertCandidate(live, desired, ref, depName); err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	meta, _ := jsonObject(live["metadata"])
	var deploymentUID string
	var deploymentRV string
	_ = json.Unmarshal(meta["uid"], &deploymentUID)
	_ = json.Unmarshal(meta["resourceVersion"], &deploymentRV)
	if postErr == nil {
		createdMeta, err := jsonObject(created["metadata"])
		var createdUID string
		if err != nil || json.Unmarshal(createdMeta["uid"], &createdUID) != nil || createdUID == "" || createdUID != deploymentUID {
			return V1Alpha2StatusSnapshot{}, fmt.Errorf("candidate creation UID mismatch")
		}
	}
	return initializeV1Alpha2Authority(ctx, store, ref, depName, &c.Spec, deploymentUID, deploymentRV)
}

func validateInertCandidate(live map[string]json.RawMessage, desired map[string]any, ref V1Alpha2ClusterRef, name string) error {
	var d struct {
		APIVersion, Kind string
		Metadata         struct {
			Name, Namespace, UID, ResourceVersion string
			DeletionTimestamp                     *string
			Labels                                map[string]string
			OwnerReferences                       []struct {
				APIVersion, Kind, Name, UID string
				Controller                  bool
			}
		}
		Spec struct {
			Replicas *int
			Selector json.RawMessage
			Template struct {
				Metadata map[string]json.RawMessage
				Spec     map[string]json.RawMessage
			}
		}
	}
	b, _ := json.Marshal(live)
	if json.Unmarshal(b, &d) != nil {
		return fmt.Errorf("invalid candidate")
	}
	labels := desired["metadata"].(map[string]any)["labels"].(map[string]string)
	if d.APIVersion != "apps/v1" || d.Kind != "Deployment" || d.Metadata.Name != name || d.Metadata.Namespace != ref.Namespace || !v1alpha2NonBlank.MatchString(d.Metadata.UID) || !v1alpha2NonBlank.MatchString(d.Metadata.ResourceVersion) || d.Metadata.DeletionTimestamp != nil || !reflect.DeepEqual(d.Metadata.Labels, labels) || len(d.Metadata.OwnerReferences) != 1 || d.Metadata.OwnerReferences[0].APIVersion != V1Alpha2APIVersion || d.Metadata.OwnerReferences[0].Kind != "RhizaCluster" || d.Metadata.OwnerReferences[0].Name != ref.Name || d.Metadata.OwnerReferences[0].UID != ref.UID || !d.Metadata.OwnerReferences[0].Controller || d.Spec.Replicas == nil || *d.Spec.Replicas != 0 {
		return fmt.Errorf("candidate identity or replicas mismatch")
	}
	selector, _ := json.Marshal(map[string]any{"matchLabels": labels})
	if !sameJSON(d.Spec.Selector, selector) {
		return fmt.Errorf("candidate selector mismatch")
	}
	meta, _ := json.Marshal(map[string]any{"labels": labels})
	if raw, ok := d.Spec.Template.Metadata["creationTimestamp"]; ok && sameJSON(raw, json.RawMessage("null")) {
		delete(d.Spec.Template.Metadata, "creationTimestamp")
	}
	rawMeta, _ := json.Marshal(d.Spec.Template.Metadata)
	if !sameJSON(rawMeta, meta) {
		return fmt.Errorf("candidate template metadata mismatch")
	}
	// Kubernetes adds these deterministic defaults to a Pod template.
	defaults := map[string]string{"restartPolicy": "\"Always\"", "dnsPolicy": "\"ClusterFirst\"", "schedulerName": "\"default-scheduler\"", "terminationGracePeriodSeconds": "30", "securityContext": "{}"}
	for key, value := range defaults {
		if raw, ok := d.Spec.Template.Spec[key]; ok && sameJSON(raw, json.RawMessage(value)) {
			delete(d.Spec.Template.Spec, key)
		}
	}
	var containers []map[string]json.RawMessage
	if json.Unmarshal(d.Spec.Template.Spec["containers"], &containers) != nil {
		return fmt.Errorf("invalid candidate containers")
	}
	for _, c := range containers {
		for key, value := range map[string]string{"terminationMessagePath": "\"/dev/termination-log\"", "terminationMessagePolicy": "\"File\"", "resources": "{}"} {
			if raw, ok := c[key]; ok && sameJSON(raw, json.RawMessage(value)) {
				delete(c, key)
			}
		}
		var image string
		if json.Unmarshal(c["image"], &image) != nil {
			return fmt.Errorf("invalid candidate image")
		}
		policy, _ := json.Marshal(defaultImagePullPolicy(image))
		if raw, ok := c["imagePullPolicy"]; ok && sameJSON(raw, policy) {
			delete(c, "imagePullPolicy")
		}
	}
	d.Spec.Template.Spec["containers"], _ = json.Marshal(containers)
	want, _ := json.Marshal(desired["spec"].(map[string]any)["template"].(map[string]any)["spec"])
	got, _ := json.Marshal(d.Spec.Template.Spec)
	if !sameJSON(got, want) {
		return fmt.Errorf("candidate template mismatch")
	}
	return nil
}

func defaultImagePullPolicy(image string) string {
	name, _, digest := strings.Cut(image, "@")
	colon := strings.LastIndexByte(name, ':')
	if colon > strings.LastIndexByte(name, '/') {
		if name[colon+1:] == "latest" {
			return "Always"
		}
		return "IfNotPresent"
	}
	if digest {
		return "IfNotPresent"
	}
	return "Always"
}

// RunV1Alpha2Initialization polls complete namespace-scoped lists until canceled.
func RunV1Alpha2Initialization(ctx context.Context, kube *Kubernetes, interval time.Duration) error {
	if kube == nil || !validNamespace(kube.Namespace) || interval <= 0 {
		return fmt.Errorf("invalid initializer configuration")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := initializeV1Alpha2List(ctx, kube); err != nil && ctx.Err() == nil {
			log.Printf("initializer scan failed: %v", err)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func initializeV1Alpha2List(ctx context.Context, kube *Kubernetes) error {
	base := "/apis/rhiza.mrchypark.dev/v1alpha2/namespaces/" + kube.Namespace + "/rhizaclusters"
	var refs []V1Alpha2ClusterRef
	seen := map[string]bool{}
	seenUID := map[string]bool{}
	tokens := map[string]bool{}
	token, version := "", ""
	for {
		path := base
		if token != "" {
			path += "?continue=" + url.QueryEscape(token)
		}
		var page struct {
			APIVersion, Kind string
			Metadata         struct {
				ResourceVersion string
				Continue        string
			}
			Items []json.RawMessage
		}
		if err := kube.Get(ctx, path, &page); err != nil {
			return err
		}
		if page.APIVersion != V1Alpha2APIVersion || page.Kind != "RhizaClusterList" || !v1alpha2NonBlank.MatchString(page.Metadata.ResourceVersion) || (version != "" && version != page.Metadata.ResourceVersion) || page.Items == nil {
			return fmt.Errorf("invalid cluster list")
		}
		version = page.Metadata.ResourceVersion
		for _, raw := range page.Items {
			c, err := DecodeV1Alpha2Cluster(raw)
			if err != nil {
				return fmt.Errorf("invalid list item")
			}
			var ref V1Alpha2ClusterRef
			var rv string
			_ = json.Unmarshal(c.Metadata["name"], &ref.Name)
			_ = json.Unmarshal(c.Metadata["namespace"], &ref.Namespace)
			_ = json.Unmarshal(c.Metadata["uid"], &ref.UID)
			_ = json.Unmarshal(c.Metadata["resourceVersion"], &rv)
			if _, err := (&V1Alpha2StatusStore{Kube: kube}).path(ref); err != nil || !v1alpha2NonBlank.MatchString(rv) || seen[ref.Name] || seenUID[ref.UID] {
				return fmt.Errorf("invalid list identity")
			}
			seen[ref.Name] = true
			seenUID[ref.UID] = true
			if c.Spec.GenesisIntent != nil && *c.Spec.GenesisIntent == "Create" {
				refs = append(refs, ref)
			}
		}
		token = page.Metadata.Continue
		if token == "" {
			break
		}
		if tokens[token] {
			return fmt.Errorf("repeated list continuation")
		}
		tokens[token] = true
	}
	for _, ref := range refs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, err := ReconcileV1Alpha2Initialization(ctx, kube, ref); err != nil {
			log.Printf("initializer cluster %s failed: %v", ref.Name, err)
		}
	}
	return nil
}
