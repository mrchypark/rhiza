package operator

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

const (
	V1Alpha2PrepareIdentityContainer    = "rhiza-prepare-identity"
	V1Alpha2AwaitBootstrapContainer     = "rhiza-await-bootstrap-plan"
	V1Alpha2BootstrapVolume             = "rhiza-data"
	V1Alpha2BootstrapDataDir            = "/var/lib/rhiza"
	V1Alpha2BootstrapIdentityPath       = "/var/lib/rhiza/identity.json"
	V1Alpha2ManagedRuntimeProfilePath   = "/etc/rhiza/managed/runtime.json"
	V1Alpha2ManagedRuntimeProfileKey    = "runtime.json"
	V1Alpha2ManagedRuntimeProfileVolume = "rhiza-runtime-profile"
	V1Alpha2BootstrapHandoffPath        = "/var/lib/rhiza/bootstrap.json"
)

// V1Alpha2ManagedPodSecurity is the initial managed profile. Application
// images must support UID and GID 65532 and the shared data mount.
func V1Alpha2ManagedPodSecurity() map[string]any {
	return map[string]any{"runAsUser": 65532, "runAsGroup": 65532, "fsGroup": 65532, "runAsNonRoot": true}
}

func V1Alpha2ManagedPodSpec(image, crUID string, containers []V1Alpha2Container, clusterName ...string) (map[string]any, error) {
	init, err := V1Alpha2BootstrapInitContainers(image, crUID, clusterName...)
	if err != nil {
		return nil, err
	}
	if len(containers) == 0 {
		return nil, fmt.Errorf("missing managed application container")
	}
	main := make([]map[string]any, 0, len(containers))
	for _, c := range containers {
		main = append(main, map[string]any{"name": c.Name, "image": c.Image, "volumeMounts": []any{map[string]any{"name": V1Alpha2BootstrapVolume, "mountPath": V1Alpha2BootstrapDataDir}}})
	}
	return map[string]any{"securityContext": V1Alpha2ManagedPodSecurity(), "initContainers": init, "containers": main, "volumes": []any{map[string]any{"name": V1Alpha2BootstrapVolume, "emptyDir": map[string]any{}}}}, nil
}

// V1Alpha2ManagedSQLPodSpec is the first managed reference profile, not an
// arbitrary image adapter. A missing reference-host executable fails closed.
func V1Alpha2ManagedSQLPodSpec(ref V1Alpha2ClusterRef, profile V1Alpha2RuntimeProfile, containers []V1Alpha2Container) (map[string]any, error) {
	if len(containers) != 1 || !validV1Alpha2ProfileSecretName(profile.ProfileSecretName) {
		return nil, fmt.Errorf("managed SQL requires one container and a valid profile Secret name")
	}
	pod, err := V1Alpha2ManagedPodSpec(profile.InitImage, ref.UID, containers, ref.Name)
	if err != nil {
		return nil, err
	}
	init := pod["initContainers"].([]map[string]any)
	main := pod["containers"].([]map[string]any)[0]
	main["command"] = []string{"/usr/local/bin/rhiza-ha-init"}
	main["args"] = []string{"serve-managed-sql", "--runtime-profile", V1Alpha2ManagedRuntimeProfilePath, "--identity-file", V1Alpha2BootstrapIdentityPath, "--handoff-file", V1Alpha2BootstrapHandoffPath, "--listen", "0.0.0.0:8080"}
	main["readinessProbe"] = map[string]any{"exec": map[string]any{"command": []string{"/usr/local/bin/rhiza-ha-init", "check-ready", "--runtime-profile", V1Alpha2ManagedRuntimeProfilePath}}, "periodSeconds": 5, "timeoutSeconds": 2, "successThreshold": 1, "failureThreshold": 3}
	main["env"] = append(append([]any{}, init[0]["env"].([]any)...), map[string]any{"name": "RHIZA_CLUSTER_UID", "value": ref.UID})
	main["securityContext"] = map[string]any{"allowPrivilegeEscalation": false, "capabilities": map[string]any{"drop": []string{"ALL"}}}
	main["volumeMounts"] = append(main["volumeMounts"].([]any), map[string]any{"name": V1Alpha2ManagedRuntimeProfileVolume, "mountPath": "/etc/rhiza/managed", "readOnly": true})
	pod["volumes"] = append(pod["volumes"].([]any), map[string]any{"name": V1Alpha2ManagedRuntimeProfileVolume, "secret": map[string]any{"secretName": profile.ProfileSecretName, "defaultMode": 0440, "items": []any{map[string]any{"key": V1Alpha2ManagedRuntimeProfileKey, "path": V1Alpha2ManagedRuntimeProfileKey}}}})
	return pod, nil
}

// V1Alpha2BootstrapInitContainers is the fixed operator-owned init contract.
func V1Alpha2BootstrapInitContainers(image, crUID string, clusterName ...string) ([]map[string]any, error) {
	if !validInitString(image) || !validInitString(crUID) {
		return nil, fmt.Errorf("missing bootstrap image or cluster UID")
	}
	env := []any{map[string]any{"name": "POD_UID", "valueFrom": map[string]any{"fieldRef": map[string]any{"apiVersion": "v1", "fieldPath": "metadata.uid"}}}, map[string]any{"name": "POD_NAME", "valueFrom": map[string]any{"fieldRef": map[string]any{"apiVersion": "v1", "fieldPath": "metadata.name"}}}, map[string]any{"name": "POD_NAMESPACE", "valueFrom": map[string]any{"fieldRef": map[string]any{"apiVersion": "v1", "fieldPath": "metadata.namespace"}}}}
	if len(clusterName) == 1 && clusterName[0] != "" {
		env = append(env, map[string]any{"name": "RHIZA_CLUSTER_NAME", "value": clusterName[0]})
	}
	mount := []any{map[string]any{"name": V1Alpha2BootstrapVolume, "mountPath": V1Alpha2BootstrapDataDir}}
	init := func(name string, args []string) map[string]any {
		return map[string]any{"name": name, "image": image, "command": []string{"/usr/local/bin/rhiza-ha-init"}, "args": args, "env": env, "volumeMounts": mount, "terminationMessagePath": "/dev/termination-log", "terminationMessagePolicy": "File", "securityContext": map[string]any{"allowPrivilegeEscalation": false, "capabilities": map[string]any{"drop": []string{"ALL"}}}}
	}
	return []map[string]any{
		init(V1Alpha2PrepareIdentityContainer, []string{"prepare-identity", "--cr-uid", crUID, "--generation", "0", "--pod-uid", "$(POD_UID)", "--identity-file", V1Alpha2BootstrapIdentityPath, "--termination-message", "/dev/termination-log"}),
		init(V1Alpha2AwaitBootstrapContainer, []string{"await-bootstrap-plan", "--cr-uid", crUID, "--generation", "0", "--pod-uid", "$(POD_UID)", "--identity-file", V1Alpha2BootstrapIdentityPath}),
	}, nil
}

type v1alpha2BootstrapOwner struct {
	APIVersion, Kind, Name, UID string
	Controller                  *bool
}
type v1alpha2BootstrapMetadata struct {
	Name, Namespace, UID, ResourceVersion string
	DeletionTimestamp                     *string
	OwnerReferences                       []v1alpha2BootstrapOwner
}

func bootstrapOwner(meta v1alpha2BootstrapMetadata, api, kind, name, uid string) bool {
	count := 0
	for _, owner := range meta.OwnerReferences {
		if owner.Controller != nil && *owner.Controller {
			count++
			if owner.APIVersion != api || owner.Kind != kind || owner.Name != name || owner.UID != uid {
				return false
			}
		}
	}
	return count == 1
}

func bootstrapInitMatches(raw, expected json.RawMessage) bool {
	var actual []map[string]json.RawMessage
	if json.Unmarshal(raw, &actual) != nil || len(actual) != 2 {
		return false
	}
	for _, c := range actual {
		for key, value := range map[string]string{"terminationMessagePath": `"/dev/termination-log"`, "terminationMessagePolicy": `"File"`, "resources": `{}`} {
			if raw, ok := c[key]; ok && sameJSON(raw, json.RawMessage(value)) && key == "resources" {
				delete(c, key)
			}
		}
		if raw, ok := c["imagePullPolicy"]; ok {
			var policy, image string
			if json.Unmarshal(raw, &policy) != nil || json.Unmarshal(c["image"], &image) != nil || policy != defaultImagePullPolicy(image) {
				return false
			}
			delete(c, "imagePullPolicy")
		}
		var mounts []map[string]json.RawMessage
		if json.Unmarshal(c["volumeMounts"], &mounts) != nil {
			return false
		}
		filtered := mounts[:0]
		for _, m := range mounts {
			var name, path string
			_ = json.Unmarshal(m["name"], &name)
			_ = json.Unmarshal(m["mountPath"], &path)
			if strings.HasPrefix(name, "kube-api-access-") && path == "/var/run/secrets/kubernetes.io/serviceaccount" && sameJSON(m["readOnly"], json.RawMessage("true")) {
				continue
			}
			filtered = append(filtered, m)
		}
		c["volumeMounts"], _ = json.Marshal(filtered)
	}
	canonical, _ := json.Marshal(actual)
	return sameJSON(canonical, expected)
}

func bootstrapServiceAccountProjection(raw json.RawMessage) bool {
	var p struct {
		DefaultMode *int                         `json:"defaultMode"`
		Sources     []map[string]json.RawMessage `json:"sources"`
	}
	if decodeV1Alpha2(raw, &p) != nil || p.DefaultMode != nil && *p.DefaultMode != 420 || len(p.Sources) != 3 {
		return false
	}
	var token struct {
		ExpirationSeconds int64  `json:"expirationSeconds"`
		Path              string `json:"path"`
	}
	var config struct {
		Name  string                       `json:"name"`
		Items []struct{ Key, Path string } `json:"items"`
	}
	var downward struct {
		Items []struct {
			Path     string                                 `json:"path"`
			FieldRef struct{ APIVersion, FieldPath string } `json:"fieldRef"`
		} `json:"items"`
	}
	if len(p.Sources[0]) != 1 || decodeV1Alpha2(p.Sources[0]["serviceAccountToken"], &token) != nil || token.Path != "token" || token.ExpirationSeconds <= 0 {
		return false
	}
	if len(p.Sources[1]) != 1 || decodeV1Alpha2(p.Sources[1]["configMap"], &config) != nil || config.Name != "kube-root-ca.crt" || len(config.Items) != 1 || config.Items[0].Key != "ca.crt" || config.Items[0].Path != "ca.crt" {
		return false
	}
	if len(p.Sources[2]) != 1 || decodeV1Alpha2(p.Sources[2]["downwardAPI"], &downward) != nil || len(downward.Items) != 1 || downward.Items[0].Path != "namespace" || downward.Items[0].FieldRef.FieldPath != "metadata.namespace" || downward.Items[0].FieldRef.APIVersion != "v1" {
		return false
	}
	return true
}

// CollectV1Alpha2BootstrapMembers reads a complete live Pod list and verifies
// each selected Pod's controller chain; it does not authorize application I/O.
func CollectV1Alpha2BootstrapMembers(ctx context.Context, kube *Kubernetes, ref V1Alpha2ClusterRef, deploymentName, deploymentUID, image string, count, peerPort int, profiles ...V1Alpha2RuntimeProfile) ([]V1Alpha2BootstrapMember, error) {
	if len(profiles) > 1 {
		return nil, fmt.Errorf("multiple managed profiles")
	}
	if kube == nil || count < 3 || count > 64 || peerPort < 1 || peerPort > 65535 || !validInitString(deploymentUID) {
		return nil, fmt.Errorf("invalid bootstrap collection configuration")
	}
	clusterPath, err := (&V1Alpha2StatusStore{Kube: kube}).path(ref)
	if err != nil {
		return nil, err
	}
	var liveCluster json.RawMessage
	if err := kube.Get(ctx, clusterPath, &liveCluster); err != nil {
		return nil, err
	}
	cluster, err := DecodeV1Alpha2Cluster(liveCluster)
	if err != nil {
		return nil, err
	}
	var crMeta v1alpha2BootstrapMetadata
	metaBytes, _ := json.Marshal(cluster.Metadata)
	if json.Unmarshal(metaBytes, &crMeta) != nil || crMeta.Name != ref.Name || crMeta.Namespace != ref.Namespace || crMeta.UID != ref.UID || crMeta.ResourceVersion == "" || crMeta.DeletionTimestamp != nil || cluster.Spec.GenesisIntent == nil || *cluster.Spec.GenesisIntent != "Create" || int(cluster.Spec.VoterCount) != count {
		return nil, fmt.Errorf("invalid bootstrap cluster")
	}
	init, err := V1Alpha2BootstrapInitContainers(image, ref.UID, ref.Name)
	if err != nil {
		return nil, err
	}
	expected, _ := json.Marshal(init)
	var expectedMain map[string]any
	var expectedProfileVolume json.RawMessage
	if len(profiles) == 1 {
		pod, err := V1Alpha2ManagedSQLPodSpec(ref, profiles[0], cluster.Spec.Workload.PodTemplate.Spec.Containers)
		if err != nil {
			return nil, err
		}
		expectedMain = pod["containers"].([]map[string]any)[0]
		expectedProfileVolume, _ = json.Marshal(pod["volumes"].([]any)[1])
	}
	base := "/apis/apps/v1/namespaces/" + ref.Namespace
	var dep struct {
		APIVersion, Kind string
		Metadata         v1alpha2BootstrapMetadata
	}
	if err := kube.Get(ctx, base+"/deployments/"+deploymentName, &dep); err != nil {
		return nil, err
	}
	if dep.APIVersion != "apps/v1" || dep.Kind != "Deployment" || dep.Metadata.Name != deploymentName || dep.Metadata.Namespace != ref.Namespace || dep.Metadata.UID != deploymentUID || dep.Metadata.ResourceVersion == "" || dep.Metadata.DeletionTimestamp != nil || !bootstrapOwner(dep.Metadata, V1Alpha2APIVersion, "RhizaCluster", ref.Name, ref.UID) {
		return nil, fmt.Errorf("invalid bootstrap deployment")
	}
	type pod struct {
		APIVersion, Kind string
		Metadata         v1alpha2BootstrapMetadata
		Spec             struct {
			HostNetwork, HostPID, HostIPC bool
			InitContainers                json.RawMessage
			SecurityContext               json.RawMessage
			Containers                    []struct {
				Name, Image        string
				Command, Args, Env json.RawMessage
				ReadinessProbe     json.RawMessage
				LivenessProbe      json.RawMessage
				VolumeMounts       []struct {
					Name, MountPath string
					ReadOnly        bool
				}
				SecurityContext json.RawMessage
			}
			Volumes []json.RawMessage
		}
		Status struct {
			PodIP                 string
			InitContainerStatuses []struct {
				Name  string
				State struct {
					Terminated *struct {
						ExitCode int
						Message  string
					}
					Waiting *json.RawMessage
					Running *json.RawMessage
				}
				LastState json.RawMessage
			}
			ContainerStatuses []struct {
				State struct {
					Waiting    json.RawMessage
					Running    json.RawMessage
					Terminated json.RawMessage
				}
				LastState    json.RawMessage
				RestartCount int
				Started      *bool
			}
		}
	}
	var pods []pod
	version, token := "", ""
	seenTokens := map[string]bool{}
	for {
		path := "/api/v1/namespaces/" + ref.Namespace + "/pods?labelSelector=" + url.QueryEscape("rhiza.mrchypark.dev/cluster-uid="+ref.UID)
		if token != "" {
			path += "&continue=" + url.QueryEscape(token)
		}
		var page struct {
			APIVersion, Kind string
			Metadata         struct{ ResourceVersion, Continue string }
			Items            []pod
		}
		if err := kube.Get(ctx, path, &page); err != nil {
			return nil, err
		}
		if page.APIVersion != "v1" || page.Kind != "PodList" || page.Metadata.ResourceVersion == "" || version != "" && page.Metadata.ResourceVersion != version || page.Items == nil {
			return nil, fmt.Errorf("invalid bootstrap pod list")
		}
		version = page.Metadata.ResourceVersion
		pods = append(pods, page.Items...)
		token = page.Metadata.Continue
		if token == "" {
			break
		}
		if seenTokens[token] {
			return nil, fmt.Errorf("repeated bootstrap continuation")
		}
		seenTokens[token] = true
	}
	if len(pods) != count {
		return nil, fmt.Errorf("bootstrap identities not ready")
	}
	seenUID, seenKey, seenEndpoint := map[string]bool{}, map[quepaxa.PublicKey]bool{}, map[string]bool{}
	members := make([]V1Alpha2BootstrapMember, 0, count)
	for _, p := range pods {
		if p.APIVersion != "v1" || p.Kind != "Pod" || p.Metadata.Namespace != ref.Namespace || p.Metadata.UID == "" || p.Metadata.Name == "" || p.Metadata.ResourceVersion == "" || p.Metadata.DeletionTimestamp != nil || seenUID[p.Metadata.UID] {
			return nil, fmt.Errorf("invalid bootstrap pod")
		}
		var rsOwner *v1alpha2BootstrapOwner
		for i := range p.Metadata.OwnerReferences {
			o := &p.Metadata.OwnerReferences[i]
			if o.Controller != nil && *o.Controller {
				if rsOwner != nil {
					return nil, fmt.Errorf("ambiguous pod owner")
				}
				rsOwner = o
			}
		}
		if rsOwner == nil || rsOwner.Kind != "ReplicaSet" || rsOwner.APIVersion != "apps/v1" || rsOwner.Name == "" || rsOwner.UID == "" {
			return nil, fmt.Errorf("invalid pod owner")
		}
		var rs struct {
			APIVersion, Kind string
			Metadata         v1alpha2BootstrapMetadata
		}
		if err := kube.Get(ctx, base+"/replicasets/"+rsOwner.Name, &rs); err != nil {
			return nil, err
		}
		if rs.APIVersion != "apps/v1" || rs.Kind != "ReplicaSet" || rs.Metadata.Name != rsOwner.Name || rs.Metadata.Namespace != ref.Namespace || rs.Metadata.UID != rsOwner.UID || rs.Metadata.ResourceVersion == "" || rs.Metadata.DeletionTimestamp != nil || !bootstrapOwner(rs.Metadata, "apps/v1", "Deployment", deploymentName, deploymentUID) {
			return nil, fmt.Errorf("invalid replica set owner")
		}
		if !bootstrapInitMatches(p.Spec.InitContainers, expected) {
			return nil, fmt.Errorf("unexpected bootstrap init template")
		}
		security, _ := json.Marshal(V1Alpha2ManagedPodSecurity())
		if p.Spec.HostNetwork || p.Spec.HostPID || p.Spec.HostIPC || !sameJSON(p.Spec.SecurityContext, security) || len(p.Spec.Containers) != len(cluster.Spec.Workload.PodTemplate.Spec.Containers) {
			return nil, fmt.Errorf("invalid managed pod security")
		}
		dataVolumes, profileVolumes := 0, 0
		projectionName := ""
		for _, raw := range p.Spec.Volumes {
			if expectedMain != nil && sameJSON(raw, expectedProfileVolume) {
				profileVolumes++
				continue
			}
			var v struct {
				Name      string          `json:"name"`
				EmptyDir  json.RawMessage `json:"emptyDir"`
				Projected json.RawMessage `json:"projected"`
			}
			if decodeV1Alpha2(raw, &v) != nil {
				return nil, fmt.Errorf("invalid bootstrap volume")
			}
			if v.Name == V1Alpha2BootstrapVolume && sameJSON(v.EmptyDir, json.RawMessage(`{}`)) && len(v.Projected) == 0 {
				dataVolumes++
				continue
			}
			if !strings.HasPrefix(v.Name, "kube-api-access-") || !bootstrapServiceAccountProjection(v.Projected) || len(v.EmptyDir) != 0 {
				return nil, fmt.Errorf("invalid bootstrap volume")
			}
			if projectionName != "" {
				return nil, fmt.Errorf("duplicate service account volume")
			}
			projectionName = v.Name
		}
		if dataVolumes != 1 || expectedMain != nil && profileVolumes != 1 {
			return nil, fmt.Errorf("invalid bootstrap volume")
		}
		for i, c := range p.Spec.Containers {
			want := cluster.Spec.Workload.PodTemplate.Spec.Containers[i]
			if c.Name != want.Name || c.Image != want.Image || expectedMain == nil && len(c.SecurityContext) > 0 && !sameJSON(c.SecurityContext, json.RawMessage(`{}`)) {
				return nil, fmt.Errorf("invalid managed main container")
			}
			if expectedMain != nil {
				if len(c.LivenessProbe) > 0 && !sameJSON(c.LivenessProbe, json.RawMessage(`null`)) {
					return nil, fmt.Errorf("managed bootstrap must not have a liveness probe")
				}
				for key, raw := range map[string]json.RawMessage{"command": c.Command, "args": c.Args, "env": c.Env, "securityContext": c.SecurityContext, "readinessProbe": c.ReadinessProbe} {
					want, _ := json.Marshal(expectedMain[key])
					if !sameJSON(raw, want) {
						return nil, fmt.Errorf("invalid managed main contract")
					}
				}
			}
			data, projected, profileMounts := 0, 0, 0
			for _, m := range c.VolumeMounts {
				if expectedMain != nil && m.Name == V1Alpha2ManagedRuntimeProfileVolume && m.MountPath == "/etc/rhiza/managed" && m.ReadOnly {
					profileMounts++
					continue
				}
				if m.Name == V1Alpha2BootstrapVolume && m.MountPath == V1Alpha2BootstrapDataDir && !m.ReadOnly {
					data++
					continue
				}
				if projectionName != "" && m.Name == projectionName && m.MountPath == "/var/run/secrets/kubernetes.io/serviceaccount" && m.ReadOnly {
					projected++
					continue
				}
				return nil, fmt.Errorf("invalid managed main mount")
			}
			if data != 1 || projected > 1 || expectedMain != nil && profileMounts != 1 {
				return nil, fmt.Errorf("invalid managed main mounts")
			}
		}
		if len(p.Status.InitContainerStatuses) != 2 || p.Status.InitContainerStatuses[0].Name != V1Alpha2PrepareIdentityContainer || p.Status.InitContainerStatuses[0].State.Terminated == nil || p.Status.InitContainerStatuses[0].State.Terminated.ExitCode != 0 || p.Status.InitContainerStatuses[1].Name != V1Alpha2AwaitBootstrapContainer || p.Status.InitContainerStatuses[1].State.Terminated != nil || (p.Status.InitContainerStatuses[1].State.Waiting == nil) == (p.Status.InitContainerStatuses[1].State.Running == nil) {
			return nil, fmt.Errorf("bootstrap init not gated")
		}
		for _, s := range p.Status.ContainerStatuses {
			if len(s.State.Running) > 0 || len(s.State.Terminated) > 0 || s.RestartCount != 0 || s.Started != nil && *s.Started || len(s.LastState) > 0 && !sameJSON(s.LastState, json.RawMessage(`{}`)) {
				return nil, fmt.Errorf("bootstrap main already started")
			}
		}
		key, err := DecodeV1Alpha2BootstrapReport(p.Status.InitContainerStatuses[0].State.Terminated.Message)
		if err != nil {
			return nil, err
		}
		ip := net.ParseIP(p.Status.PodIP)
		if ip == nil || ip.IsUnspecified() {
			return nil, fmt.Errorf("invalid bootstrap pod IP")
		}
		endpoint := "quic://" + net.JoinHostPort(ip.String(), strconv.Itoa(peerPort))
		if seenKey[key] || seenEndpoint[endpoint] {
			return nil, fmt.Errorf("duplicate bootstrap identity")
		}
		id, err := types.ManagedNodeID(p.Metadata.UID)
		if err != nil {
			return nil, err
		}
		seenUID[p.Metadata.UID], seenKey[key], seenEndpoint[endpoint] = true, true, true
		members = append(members, V1Alpha2BootstrapMember{PodUID: p.Metadata.UID, Member: quepaxa.Member{ID: id, PeerURL: endpoint, PublicKey: key}})
	}
	sort.Slice(members, func(i, j int) bool {
		return strings.Compare(string(members[i].Member.ID), string(members[j].Member.ID)) < 0
	})
	return members, nil
}

// V1Alpha2BootstrapMember binds a public bootstrap identity to its actual Pod.
type V1Alpha2BootstrapMember struct {
	PodUID string         `json:"podUID"`
	Member quepaxa.Member `json:"member"`
}

// V1Alpha2BootstrapPlan is immutable genesis history, not current membership.
type V1Alpha2BootstrapPlan struct {
	ClusterID       types.ClusterID             `json:"clusterID"`
	Members         []V1Alpha2BootstrapMember   `json:"members"`
	PhysicalBinding string                      `json:"physicalBinding"`
	EvidenceFormat  string                      `json:"evidenceFormat"`
	Durability      types.ObjectStoreDurability `json:"durability"`
	Reconfiguration bool                        `json:"reconfiguration"`
}

// DecodeV1Alpha2BootstrapReport accepts only the bounded version-one public
// termination message. It is discovery data, not authorization.
func DecodeV1Alpha2BootstrapReport(message string) (quepaxa.PublicKey, error) {
	var result quepaxa.PublicKey
	if len(message) == 0 || len(message) > 512 {
		return result, fmt.Errorf("invalid bootstrap report size")
	}
	decoder := json.NewDecoder(bytes.NewBufferString(message))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return result, fmt.Errorf("invalid bootstrap report")
	}
	var version int
	var publicKey string
	seen := map[string]bool{}
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok || seen[key] {
			return result, fmt.Errorf("invalid bootstrap report field")
		}
		seen[key] = true
		switch key {
		case "version":
			err = decoder.Decode(&version)
		case "publicKey":
			err = decoder.Decode(&publicKey)
		default:
			return result, fmt.Errorf("unknown bootstrap report field")
		}
		if err != nil {
			return result, fmt.Errorf("invalid bootstrap report field")
		}
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') || !seen["version"] || !seen["publicKey"] || version != 1 {
		return result, fmt.Errorf("invalid bootstrap report")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return result, fmt.Errorf("trailing bootstrap report data")
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(publicKey)
	if err != nil || len(decoded) != len(result) || base64.StdEncoding.EncodeToString(decoded) != publicKey {
		return result, fmt.Errorf("invalid bootstrap public key")
	}
	copy(result[:], decoded)
	if result == (quepaxa.PublicKey{}) {
		return result, fmt.Errorf("invalid bootstrap public key")
	}
	return result, nil
}
