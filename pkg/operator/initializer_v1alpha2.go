package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// V1Alpha2RuntimeProfile is the installed SQL reference-host contract. Storage
// may contain credentials: keep its JSON in a private file/Secret, never status
// or logs. The selected main image must contain /usr/local/bin/rhiza-ha-init.
type V1Alpha2RuntimeProfile struct {
	InitImage         string
	PeerPort          int
	EvidenceFormat    string
	Storage           types.ExecutionConfig
	ProfileSecretName string
}

// V1Alpha2ManagedSQLEvidenceFormat is the sole supported reference-host format.
// The CLI parity test binds it to managedsql without creating an import cycle.
const V1Alpha2ManagedSQLEvidenceFormat = "rhiza-managed-sql/v1"

func validV1Alpha2ProfileSecretName(name string) bool {
	if len(name) == 0 || len(name) > 253 {
		return false
	}
	for _, part := range strings.Split(name, ".") {
		if len(part) == 0 || len(part) > 63 || !v1alpha2Name.MatchString(part) {
			return false
		}
	}
	return true
}

// DecodeV1Alpha2RuntimeProfile reads bounded typed Secret JSON without echoing
// potentially secret input in errors. Both operator and reference host use it.
func DecodeV1Alpha2RuntimeProfile(r io.Reader) (V1Alpha2RuntimeProfile, error) {
	var profile V1Alpha2RuntimeProfile
	b, err := io.ReadAll(io.LimitReader(r, 65537))
	if err != nil || len(b) > 65536 || !bytes.HasPrefix(bytes.TrimSpace(b), []byte("{")) {
		return profile, fmt.Errorf("invalid managed runtime profile JSON")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if !uniqueManagedProfileJSON(json.NewDecoder(bytes.NewReader(b))) || d.Decode(&profile) != nil || d.Decode(new(any)) != io.EOF {
		return V1Alpha2RuntimeProfile{}, fmt.Errorf("invalid managed runtime profile JSON")
	}
	if _, _, err := BuildV1Alpha2RuntimePlan(V1Alpha2ClusterRef{UID: "profile-validation"}, "profile-validation", "profile-validation", profile, nil); err != nil {
		return V1Alpha2RuntimeProfile{}, fmt.Errorf("invalid managed runtime profile")
	}
	return profile, nil
}

func uniqueManagedProfileJSON(d *json.Decoder) bool {
	t, err := d.Token()
	if err != nil {
		return false
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return true
	}
	seen := map[string]bool{}
	for d.More() {
		if delim == '{' {
			key, err := d.Token()
			if err != nil {
				return false
			}
			s, ok := key.(string)
			if !ok {
				return false
			}
			s = strings.ToLower(s)
			if seen[s] {
				return false
			}
			seen[s] = true
		}
		if !uniqueManagedProfileJSON(d) {
			return false
		}
	}
	end, err := d.Token()
	return err == nil && (delim == '{' && end == json.Delim('}') || delim == '[' && end == json.Delim(']'))
}

// ErrV1Alpha2BootstrapPending leaves the candidate and its identity gates intact.
var ErrV1Alpha2BootstrapPending = errors.New("v1alpha2 bootstrap identities pending")

// ReconcileV1Alpha2ManagedInitialization freezes actual gated Pod identities in
// genesis. A successful return describes the committed plan, never our draft.
func ReconcileV1Alpha2ManagedInitialization(ctx context.Context, kube *Kubernetes, ref V1Alpha2ClusterRef, profile V1Alpha2RuntimeProfile) (V1Alpha2StatusSnapshot, error) {
	store := &V1Alpha2StatusStore{Kube: kube}
	path, err := store.path(ref)
	if err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	var obj map[string]json.RawMessage
	if err = kube.Get(ctx, path, &obj); err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	raw, _ := json.Marshal(obj)
	c, err := DecodeV1Alpha2Cluster(raw)
	if err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	var meta v1alpha2BootstrapMetadata
	b, _ := json.Marshal(c.Metadata)
	if json.Unmarshal(b, &meta) != nil || meta.Name != ref.Name || meta.Namespace != ref.Namespace || meta.UID != ref.UID || meta.ResourceVersion == "" || meta.DeletionTimestamp != nil {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("invalid cluster identity")
	}
	status := map[string]json.RawMessage{}
	if len(c.Status) > 0 && string(c.Status) != "null" {
		status, err = jsonObject(c.Status)
		if err != nil {
			return V1Alpha2StatusSnapshot{}, err
		}
	}
	committed := func() (V1Alpha2StatusSnapshot, error) {
		s, e := store.Load(ctx, ref)
		if e != nil {
			return s, e
		}
		_, e = s.BootstrapPlan()
		return s, e
	}
	if _, ok := status["genesis"]; ok {
		return committed()
	}
	if c.Spec.GenesisIntent == nil || *c.Spec.GenesisIntent != "Create" {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("cluster lacks creation intent")
	}
	for _, key := range []string{"authority", "applicationInit", "bootstrapPlan", "recoveryQueue", "pendingWrite", "transition", "receipt", "decision"} {
		if _, ok := status[key]; ok {
			return V1Alpha2StatusSnapshot{}, fmt.Errorf("existing %s prevents genesis", key)
		}
	}
	// Validate installed storage inputs before creating any Kubernetes resource.
	if _, _, err = BuildV1Alpha2RuntimePlan(ref, c.Spec.LogicalID, "validation", profile, nil); err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	podSpec, err := V1Alpha2ManagedSQLPodSpec(ref, profile, c.Spec.Workload.PodTemplate.Spec.Containers)
	if err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	var secret struct {
		Metadata v1alpha2BootstrapMetadata
		Data     map[string][]byte
	}
	if err := kube.Get(ctx, "/api/v1/namespaces/"+ref.Namespace+"/secrets/"+profile.ProfileSecretName, &secret); err != nil {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("managed profile Secret unavailable")
	}
	secretProfile, err := DecodeV1Alpha2RuntimeProfile(bytes.NewReader(secret.Data[V1Alpha2ManagedRuntimeProfileKey]))
	if err != nil || secret.Metadata.Name != profile.ProfileSecretName || secret.Metadata.Namespace != ref.Namespace || secret.Metadata.UID == "" || secret.Metadata.DeletionTimestamp != nil || !reflect.DeepEqual(secretProfile, profile) {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("managed profile Secret differs from installed profile")
	}
	var account struct {
		APIVersion, Kind string
		Metadata         v1alpha2BootstrapMetadata
	}
	if err := kube.Get(ctx, "/api/v1/namespaces/"+ref.Namespace+"/serviceaccounts/"+V1Alpha2ManagedHostServiceAccount, &account); err != nil {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("managed host ServiceAccount unavailable")
	}
	if account.APIVersion != "v1" || account.Kind != "ServiceAccount" || account.Metadata.Name != V1Alpha2ManagedHostServiceAccount || account.Metadata.Namespace != ref.Namespace || account.Metadata.UID == "" || account.Metadata.DeletionTimestamp != nil {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("invalid managed host ServiceAccount")
	}
	name, err := V1Alpha2GenesisDeploymentName(ref.UID)
	if err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	labels := map[string]string{"rhiza.mrchypark.dev/cluster-uid": ref.UID, "rhiza.mrchypark.dev/generation": "0"}
	desired := map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": name, "namespace": ref.Namespace, "labels": labels, "ownerReferences": []any{map[string]any{"apiVersion": V1Alpha2APIVersion, "kind": "RhizaCluster", "name": ref.Name, "uid": ref.UID, "controller": true}}}, "spec": map[string]any{"replicas": c.Spec.VoterCount, "selector": map[string]any{"matchLabels": labels}, "template": map[string]any{"metadata": map[string]any{"labels": labels}, "spec": podSpec}}}
	base := "/apis/apps/v1/namespaces/" + ref.Namespace + "/deployments"
	var created map[string]json.RawMessage
	postErr := kube.Post(ctx, base, desired, &created)
	var apiErr *APIError
	if postErr != nil && (!errors.As(postErr, &apiErr) || apiErr.StatusCode != http.StatusConflict) {
		return V1Alpha2StatusSnapshot{}, postErr
	}
	readCandidate := func() (v1alpha2BootstrapMetadata, error) {
		var live struct {
			APIVersion, Kind string
			Metadata         v1alpha2BootstrapMetadata
			Spec             json.RawMessage
		}
		if e := kube.Get(ctx, base+"/"+name, &live); e != nil {
			return live.Metadata, e
		}
		want, _ := json.Marshal(desired["spec"])
		if live.APIVersion != "apps/v1" || live.Kind != "Deployment" || live.Metadata.Name != name || live.Metadata.Namespace != ref.Namespace || live.Metadata.UID == "" || live.Metadata.ResourceVersion == "" || live.Metadata.DeletionTimestamp != nil || !bootstrapOwner(live.Metadata, V1Alpha2APIVersion, "RhizaCluster", ref.Name, ref.UID) || !managedCandidateSpecMatches(live.Spec, want) {
			return live.Metadata, fmt.Errorf("managed candidate template or identity mismatch")
		}
		return live.Metadata, nil
	}
	d, err := readCandidate()
	if err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	if postErr == nil {
		var cm v1alpha2BootstrapMetadata
		if json.Unmarshal(created["metadata"], &cm) != nil || cm.UID != d.UID {
			return V1Alpha2StatusSnapshot{}, fmt.Errorf("candidate creation UID mismatch")
		}
	}
	collect := func() ([]V1Alpha2BootstrapMember, error) {
		return CollectV1Alpha2BootstrapMembers(ctx, kube, ref, name, d.UID, profile.InitImage, int(c.Spec.VoterCount), profile.PeerPort, profile)
	}
	members, err := collect()
	if err != nil {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("%w: %v", ErrV1Alpha2BootstrapPending, err)
	}
	assignment, bootstrap, err := BuildV1Alpha2RuntimePlan(ref, c.Spec.LogicalID, d.UID, profile, members)
	if err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	again, err := collect()
	if err != nil || !reflect.DeepEqual(members, again) {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("%w: candidate members changed", ErrV1Alpha2BootstrapPending)
	}
	last, err := readCandidate()
	if err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	if last.UID != d.UID || last.ResourceVersion != d.ResourceVersion {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("%w: deployment changed", ErrV1Alpha2BootstrapPending)
	}
	_, err = InitializeV1Alpha2AuthorityWithExpectedBootstrap(ctx, store, ref, name, c.Spec, d.UID, d.ResourceVersion, assignment, bootstrap)
	if err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	return committed()
}

// Admit only deterministic API defaults; unexpected injected fields fail closed.
func managedCandidateSpecMatches(raw, expected json.RawMessage) bool {
	var spec map[string]json.RawMessage
	if json.Unmarshal(raw, &spec) != nil {
		return false
	}
	strip := func(obj map[string]json.RawMessage, defaults map[string]string) {
		for k, v := range defaults {
			if sameJSON(obj[k], json.RawMessage(v)) {
				delete(obj, k)
			}
		}
	}
	strip(spec, map[string]string{"progressDeadlineSeconds": "600", "revisionHistoryLimit": "10", "strategy": `{"type":"RollingUpdate","rollingUpdate":{"maxSurge":"25%","maxUnavailable":"25%"}}`})
	var template map[string]json.RawMessage
	if json.Unmarshal(spec["template"], &template) != nil {
		return false
	}
	var meta map[string]json.RawMessage
	if json.Unmarshal(template["metadata"], &meta) != nil {
		return false
	}
	strip(meta, map[string]string{"creationTimestamp": "null"})
	template["metadata"], _ = json.Marshal(meta)
	var pod map[string]json.RawMessage
	if json.Unmarshal(template["spec"], &pod) != nil {
		return false
	}
	strip(pod, map[string]string{"restartPolicy": `"Always"`, "dnsPolicy": `"ClusterFirst"`, "schedulerName": `"default-scheduler"`, "terminationGracePeriodSeconds": "30"})
	strip(pod, map[string]string{"serviceAccount": `"rhiza-managed-host"`})
	for _, key := range []string{"containers", "initContainers"} {
		var containers []map[string]json.RawMessage
		if json.Unmarshal(pod[key], &containers) != nil {
			return false
		}
		for _, c := range containers {
			defaults := map[string]string{"resources": "{}"}
			if key == "containers" {
				defaults["terminationMessagePath"] = `"/dev/termination-log"`
				defaults["terminationMessagePolicy"] = `"File"`
			}
			var image string
			if json.Unmarshal(c["image"], &image) != nil {
				return false
			}
			policy := `"IfNotPresent"`
			last := image[strings.LastIndex(image, "/")+1:]
			if !strings.Contains(image, "@") && (!strings.Contains(last, ":") || strings.HasSuffix(last, ":latest")) {
				policy = `"Always"`
			}
			defaults["imagePullPolicy"] = policy
			strip(c, defaults)
		}
		pod[key], _ = json.Marshal(containers)
	}
	template["spec"], _ = json.Marshal(pod)
	spec["template"], _ = json.Marshal(template)
	b, _ := json.Marshal(spec)
	return sameJSON(b, expected)
}

// BuildV1Alpha2RuntimePlan derives the scoped storage binding and common
// digest from the same concrete inputs that a managed runtime must consume.
func BuildV1Alpha2RuntimePlan(ref V1Alpha2ClusterRef, logicalID, deploymentUID string, profile V1Alpha2RuntimeProfile, members []V1Alpha2BootstrapMember) (V1Alpha2ApplicationInitPlan, V1Alpha2BootstrapPlan, error) {
	var assignment V1Alpha2ApplicationInitPlan
	var plan V1Alpha2BootstrapPlan
	if profile.Storage.AdminToken == "" || profile.EvidenceFormat != V1Alpha2ManagedSQLEvidenceFormat {
		return assignment, plan, fmt.Errorf("managed SQL requires an admin token and the reference evidence format")
	}
	if profile.InitImage == "" || profile.PeerPort < 1 || profile.PeerPort > 65535 || profile.EvidenceFormat == "" || profile.Storage.ObjStoreBucket == "" {
		return assignment, plan, fmt.Errorf("incomplete managed runtime profile")
	}
	if !validV1Alpha2ProfileSecretName(profile.ProfileSecretName) {
		return assignment, plan, fmt.Errorf("invalid managed profile Secret name")
	}
	config := profile.Storage
	provider := objectstore.Provider(config.ObjStoreProvider)
	if config.ObjStoreDir != "" || (provider != objectstore.ProviderS3 && provider != objectstore.ProviderGCS && provider != objectstore.ProviderAzure) {
		return assignment, plan, fmt.Errorf("managed HA requires explicit shared S3, GCS, or Azure storage")
	}
	if config.ObjStoreDurability != "" && config.ObjStoreDurability != types.ObjectStoreDurabilityBeforeAck {
		return assignment, plan, fmt.Errorf("managed HA requires before-ack durability")
	}
	// This is the runtime's side-effect-free provider validation, not a bucket
	// constructor: pre-genesis checks must not contact or provision storage.
	if err := objectstore.ValidateConfig(objectstore.Config{
		Provider: provider, Endpoint: config.ObjStoreEndpoint, Bucket: config.ObjStoreBucket,
		Insecure: config.ObjStoreInsecure, AzureStorageAccount: config.ObjStoreAzureStorageAccount,
	}); err != nil {
		return assignment, plan, fmt.Errorf("invalid managed storage profile: %w", err)
	}
	// The actual S3 adapter also requires an explicit endpoint at construction.
	if provider == objectstore.ProviderS3 && strings.TrimSpace(config.ObjStoreEndpoint) == "" {
		return assignment, plan, fmt.Errorf("managed S3 endpoint is required")
	}
	cluster, err := types.ManagedClusterID(ref.UID, 0)
	if err != nil {
		return assignment, plan, err
	}
	hash := sha256.Sum256([]byte("rhiza-assignment-v1\x00" + ref.UID + "\x00" + deploymentUID))
	id := fmt.Sprintf("init-%x", hash)
	configPrefix, _, err := objectstore.ManagedNamespacePrefixes(profile.Storage.ObjStorePrefix, ref.UID, 0, id, string(cluster))
	if err != nil {
		return assignment, plan, err
	}
	config.ClusterID = cluster
	config.ObjStorePrefix = configPrefix
	binding, err := types.ManagedStorageBinding(config)
	if err != nil {
		return assignment, plan, err
	}
	durability := config.ObjStoreDurability
	if durability == "" {
		durability = types.ObjectStoreDurabilityBeforeAck
	}
	common := make([]quepaxa.Member, 0, len(members))
	for _, member := range members {
		common = append(common, member.Member)
	}
	digest, err := types.ManagedBootstrapPlanDigest(string(cluster), common, binding, profile.EvidenceFormat, durability, config.EnableReconfiguration)
	if err != nil {
		return assignment, plan, err
	}
	assignment, err = NewV1Alpha2ApplicationInitPlan(ref, logicalID, deploymentUID, id, binding, digest, profile.EvidenceFormat)
	if err != nil {
		return assignment, plan, err
	}
	plan = V1Alpha2BootstrapPlan{ClusterID: cluster, Members: members, PhysicalBinding: binding, EvidenceFormat: profile.EvidenceFormat, Durability: durability, Reconfiguration: config.EnableReconfiguration}
	return assignment, plan, nil
}

// V1Alpha2InitializationPlanProvider derives a common plan from trusted runtime
// storage and bootstrap configuration, bound to the live candidate identity.
type V1Alpha2InitializationPlanProvider func(context.Context, V1Alpha2ClusterRef, string, string) (V1Alpha2ApplicationInitPlan, error)

// ReconcileV1Alpha2Initialization creates only an inert generation-zero candidate.
func ReconcileV1Alpha2Initialization(ctx context.Context, kube *Kubernetes, ref V1Alpha2ClusterRef, providers ...V1Alpha2InitializationPlanProvider) (V1Alpha2StatusSnapshot, error) {
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
	if len(providers) != 1 || providers[0] == nil {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("initialization requires a runtime common plan provider")
	}
	for _, key := range []string{"authority", "applicationInit", "recoveryQueue", "pendingWrite", "transition", "receipt", "decision"} {
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
	plan, err := providers[0](ctx, ref, c.Spec.LogicalID, deploymentUID)
	if err != nil {
		return V1Alpha2StatusSnapshot{}, fmt.Errorf("build initialization plan: %w", err)
	}
	return initializeV1Alpha2AuthorityWithPlan(ctx, store, ref, depName, &c.Spec, deploymentUID, deploymentRV, &plan)
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
func RunV1Alpha2Initialization(ctx context.Context, kube *Kubernetes, interval time.Duration, providers ...V1Alpha2InitializationPlanProvider) error {
	return runV1Alpha2Initialization(ctx, kube, interval, func() error { return initializeV1Alpha2List(ctx, kube, providers...) })
}

// RunV1Alpha2ManagedInitialization explicitly selects the SQL reference profile.
func RunV1Alpha2ManagedInitialization(ctx context.Context, kube *Kubernetes, interval time.Duration, profile V1Alpha2RuntimeProfile) error {
	if _, _, err := BuildV1Alpha2RuntimePlan(V1Alpha2ClusterRef{UID: "profile-validation"}, "profile-validation", "profile-validation", profile, nil); err != nil {
		return fmt.Errorf("invalid managed runtime profile")
	}
	return runV1Alpha2Initialization(ctx, kube, interval, func() error {
		return initializeV1Alpha2ListWith(ctx, kube, func(ref V1Alpha2ClusterRef) error {
			_, err := ReconcileV1Alpha2ManagedInitialization(ctx, kube, ref, profile)
			return err
		})
	})
}

func runV1Alpha2Initialization(ctx context.Context, kube *Kubernetes, interval time.Duration, scan func() error) error {
	if kube == nil || !validNamespace(kube.Namespace) || interval <= 0 {
		return fmt.Errorf("invalid initializer configuration")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := scan(); err != nil && ctx.Err() == nil {
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

func initializeV1Alpha2List(ctx context.Context, kube *Kubernetes, providers ...V1Alpha2InitializationPlanProvider) error {
	return initializeV1Alpha2ListWith(ctx, kube, func(ref V1Alpha2ClusterRef) error {
		_, err := ReconcileV1Alpha2Initialization(ctx, kube, ref, providers...)
		return err
	})
}

func initializeV1Alpha2ListWith(ctx context.Context, kube *Kubernetes, reconcile func(V1Alpha2ClusterRef) error) error {
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
		if err := reconcile(ref); err != nil {
			log.Printf("initializer cluster %s failed: %v", ref.Name, err)
		}
	}
	return nil
}
