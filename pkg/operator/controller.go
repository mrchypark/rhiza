package operator

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

const ownerAnnotation = "rhiza.mrchypark.dev/recovery-owner"

var identifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`)

type Controller struct {
	Kube          *Kubernetes
	Bucket        objstore.Bucket
	Prefix        string
	HTTP          *http.Client
	StoreIdentity map[string]string
}
type recoveryList struct {
	Items []Resource `json:"items"`
}
type podList struct {
	Items []object `json:"items"`
}
type sourceState struct {
	prefix          string
	members         []quepaxa.Member
	durability      string
	reconfiguration bool
	env             map[string]string
	stateful        object
}

func (c *Controller) Run(ctx context.Context, interval time.Duration) error {
	if c.Kube == nil || c.Bucket == nil || interval <= 0 {
		return fmt.Errorf("Kubernetes client, bucket, and positive poll interval are required")
	}
	for {
		if err := c.reconcileAll(ctx); err != nil && ctx.Err() == nil {
			log.Printf("recovery reconciliation: %v", err)
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
func (c *Controller) reconcileAll(ctx context.Context) error {
	var resources recoveryList
	if err := c.Kube.Get(ctx, c.resourcePath(""), &resources); err != nil {
		return err
	}
	var failures []error
	for i := range resources.Items {
		if err := c.reconcile(ctx, &resources.Items[i]); err != nil {
			failures = append(failures, fmt.Errorf("resource %s: %w", resourceName(&resources.Items[i]), err))
		}
	}
	return errors.Join(failures...)
}
func resourceName(r *Resource) string { return str(r.Metadata["name"]) }
func (c *Controller) operationID(r *Resource) string {
	return str(r.Metadata["uid"]) + ":" + r.Spec.RecoveryID
}
func (c *Controller) reconcile(ctx context.Context, r *Resource) error {
	if r.Spec.Membership != nil {
		return c.reconcileMembership(ctx, r)
	}
	reject := func(message string) error { return c.record(ctx, r, "Blocked", message) }
	for _, value := range []string{resourceName(r), r.Spec.StatefulSet, r.Spec.SourceClusterID} {
		if !identifier.MatchString(value) {
			return reject("invalid recovery resource, StatefulSet, or cluster identifier")
		}
	}
	if r.Spec.Durability != "async" && r.Spec.Durability != "before-ack" {
		return reject("target durability must be async or before-ack")
	}
	if r.Spec.MaxArchiveAgeSeconds < 0 {
		return reject("maxArchiveAgeSeconds must be nonnegative")
	}
	if r.Spec.RecoveryID != "" && (!identifier.MatchString(r.Spec.RecoveryID) || str(r.Metadata["uid"]) == "") {
		return reject("recovery ID and resource UID are required")
	}
	if r.Status.RecoveryID != "" && (r.Status.RecoveryID != r.Spec.RecoveryID || r.Status.SpecHash != immutableSpecHash(r.Spec)) {
		return reject("reserved recovery request is immutable; create a new resource")
	}
	if r.Status.Stage == "Complete" {
		return c.releaseCompleted(ctx, r)
	}
	var sts object
	if err := c.Kube.Get(ctx, c.stsPath(r.Spec.StatefulSet), &sts); err != nil {
		return err
	}
	uid := str(nested(sts, "metadata", "uid"))
	if uid == "" || r.Status.RecoveryID != "" && uid != r.Status.StatefulSetUID {
		return reject("StatefulSet identity changed during recovery")
	}
	r.Status.ObservedGeneration = number(r.Metadata["generation"])
	if r.Status.Stage == "Starting" || r.Status.Stage == "Restarted" {
		return c.startAndObserve(ctx, r, sts)
	}
	ctr, err := container(sts, r.Spec.Container)
	if err != nil {
		return reject(err.Error())
	}
	env, err := c.environment(ctx, ctr)
	if err != nil {
		return reject(err.Error())
	}
	if err = noPVC(sts, ctr, env); err != nil {
		return reject(err.Error())
	}
	if err = c.storeConfigMatches(env); err != nil {
		return reject(err.Error())
	}
	if env["RHIZA_CLUSTER_ID"] != r.Spec.SourceClusterID {
		return reject("source cluster ID differs from StatefulSet environment")
	}
	members, err := recoveryMembers(env["RHIZA_CLUSTER_MEMBERS"])
	if err != nil {
		return reject(err.Error())
	}
	if len(members) != 3 {
		return reject("recovery requires exactly three voters")
	}
	if r.Status.Stage == "" && number(nested(sts, "spec", "replicas")) != 3 {
		return reject("source StatefulSet must have three desired replicas")
	}
	mode := env["RHIZA_OBJSTORE_DURABILITY"]
	if mode == "" {
		mode = "async"
	}
	reconfiguration, err := reconfigurationEnabled(env)
	if err != nil {
		return reject(err.Error())
	}
	prefix := path.Join(c.Prefix, r.Spec.SourceClusterID)
	registered, err := c.sourceMembership(ctx, prefix)
	if err != nil {
		return reject(err.Error())
	}
	if registered != recovery.NewMembershipRecord(r.Spec.SourceClusterID, members, mode) {
		return reject("immutable source membership or durability differs from StatefulSet")
	}
	fingerprint := sourceFingerprint(r.Spec.SourceClusterID, mode, members, prefix)
	if r.Status.RecoveryID != "" && fingerprint != r.Status.SourceMembership {
		return reject("source configuration changed during recovery")
	}
	r.Status.Source, r.Status.SourceDurability, r.Status.SourceMembership = r.Spec.SourceClusterID, registered.Durability, fingerprint
	r.Status.StatefulSetUID = uid
	source := sourceState{prefix, members, mode, reconfiguration, env, sts}
	if r.Spec.RecoveryID == "" {
		peers, err := c.probe(ctx, r, sts)
		if err != nil {
			return err
		}
		r.Status.Peers = peers
		return c.record(ctx, r, "Observed", "observation only; missing peers never authorize recovery")
	}
	if mode == "async" && !r.Spec.AllowDataLoss {
		return reject("async source requires explicit allowDataLoss, regardless of target mode")
	}
	if r.Status.Stage == "" {
		r.Status.ArchiveCapture = c.captureArchive(ctx, r, sts, env["RHIZA_ADMIN_TOKEN"])
	}
	if err := validateFence(r.Spec.Fence, r.Spec.RecoveryID, r.Spec.SourceClusterID, uid); err != nil {
		return c.record(ctx, r, "AwaitingFence", err.Error())
	}
	if r.Status.Stage == "" {
		archive := recovery.NewManager(c.Bucket, prefix, 1)
		defer archive.Close()
		if err := archive.Load(ctx); err != nil || archive.Tip() == 0 {
			return reject("source archive has no available certified history")
		}
		if r.Spec.MaxArchiveAgeSeconds > 0 {
			attrs, err := c.Bucket.Attributes(ctx, path.Join(prefix, "archive/head.bin"))
			if err != nil {
				return reject("archive evidence age is unavailable")
			}
			if time.Since(attrs.LastModified).Seconds() > float64(r.Spec.MaxArchiveAgeSeconds) {
				return reject("archive evidence exceeds maxArchiveAgeSeconds; this is not a loss-window bound")
			}
		}
		r.Status.Target = "rhiza-r-" + shortID(c.operationID(r))
		r.Status.RecoveryID, r.Status.SpecHash = r.Spec.RecoveryID, immutableSpecHash(r.Spec)
		reservation := c.reservation(r)
		if err := c.immutable(ctx, path.Join(prefix, "recovery/successor.json"), jsonBytes(reservation)); err != nil {
			return reject("source generation already reserved or reservation unavailable")
		}
		r.Status.Stage = "Reserved"
		return c.record(ctx, r, "Recovering", "unique successor reserved; original generation must remain fenced")
	}
	if err := c.verifyReservation(ctx, r); err != nil {
		return reject("reserved source successor changed or disappeared")
	}
	return c.advance(ctx, r, source)
}

func (c *Controller) advance(ctx context.Context, r *Resource, s sourceState) error {
	if err := c.claimStatefulSet(ctx, s.stateful, c.operationID(r)); err != nil {
		return c.record(ctx, r, "Blocked", err.Error())
	}
	switch r.Status.Stage {
	case "Reserved":
		// The external fence is already required. Preserve and validate the
		// complete target before deleting any surviving emptyDir state.
		if err := recovery.Seal(ctx, c.Bucket, s.prefix, c.operationID(r)); err != nil {
			return c.record(ctx, r, "Blocked", "source archive seal failed")
		}
		r.Status.Stage = "Sealed"
		return c.record(ctx, r, "Recovering", "source publication permanently sealed before evidence copy")
	case "Stopping":
		if number(nested(s.stateful, "spec", "replicas")) != 0 {
			asObject(s.stateful["spec"])["replicas"] = float64(0)
			if err := c.Kube.Put(ctx, c.stsPath(r.Spec.StatefulSet), s.stateful, &s.stateful); err != nil {
				return err
			}
			return c.record(ctx, r, "Recovering", "waiting for source StatefulSet Pods to terminate after external fence")
		}
		pods, err := c.ownedPods(ctx, r.Status.StatefulSetUID)
		if err != nil {
			return err
		}
		if len(pods) > 0 {
			return c.record(ctx, r, "Recovering", "waiting for source StatefulSet Pods to terminate after external fence")
		}
		r.Status.Stage = "Starting"
		return c.record(ctx, r, "Recovering", "source StatefulSet Pods terminated; externally fenced target may start")
	case "Sealed":
		secret := "rhiza-recovery-" + shortID(c.operationID(r))
		var targetMembers []quepaxa.Member
		if s.reconfiguration {
			var err error
			targetMembers, err = c.targetCredentials(ctx, r, secret, s.members)
			if err != nil {
				return c.record(ctx, r, "Blocked", err.Error())
			}
		}
		result, err := recovery.Fork(ctx, c.Bucket, c.forkOptions(r, s.prefix, s.members, s.reconfiguration, targetMembers))
		if err != nil {
			return c.record(ctx, r, "Blocked", "certified archive fork failed: "+err.Error())
		}
		if !s.reconfiguration {
			targetMembers, err = c.targetCredentials(ctx, r, secret, s.members)
			if err != nil {
				return c.record(ctx, r, "Blocked", err.Error())
			}
		}
		members := targetMembers
		record := recovery.NewMembershipRecord(r.Status.Target, members, r.Spec.Durability)
		if err := c.immutable(ctx, path.Join(c.Prefix, r.Status.Target, "voters/membership.json"), jsonBytes(record)); err != nil {
			return c.record(ctx, r, "Blocked", "target membership is unavailable or inconsistent")
		}
		lineage := object{"version": 1, "operation_id": c.operationID(r), "source": r.Status.Source, "source_durability": r.Status.SourceDurability, "source_membership": r.Status.SourceMembership, "target_membership": record, "fork": result}
		if err := c.immutable(ctx, path.Join(c.Prefix, r.Status.Target, "recovery/generation.json"), jsonBytes(lineage)); err != nil {
			return c.record(ctx, r, "Blocked", "target lineage is unavailable or inconsistent")
		}
		r.Status.SecretName, r.Status.ManifestHash, r.Status.RecoveredTip = secret, result.ManifestHash, result.Tip
		r.Status.Stage = "Stopping"
		return c.record(ctx, r, "Recovering", "verified target and credentials preserved before deleting source pods")
	default:
		return c.record(ctx, r, "Blocked", "unknown recovery stage")
	}
}

func (c *Controller) startAndObserve(ctx context.Context, r *Resource, sts object) error {
	if err := c.verifyReservation(ctx, r); err != nil {
		return c.record(ctx, r, "Blocked", "reserved source successor changed or disappeared")
	}
	if err := validateFence(r.Spec.Fence, r.Spec.RecoveryID, r.Spec.SourceClusterID, r.Status.StatefulSetUID); err != nil {
		return c.record(ctx, r, "AwaitingFence", err.Error())
	}
	if r.Status.SourceDurability == "async" && !r.Spec.AllowDataLoss {
		return c.record(ctx, r, "Blocked", "async recovery still requires allowDataLoss")
	}
	if r.Status.Target != "rhiza-r-"+shortID(c.operationID(r)) || r.Status.SecretName != "rhiza-recovery-"+shortID(c.operationID(r)) || r.Status.RecoveredTip == 0 {
		return c.record(ctx, r, "Blocked", "invalid target recovery journal")
	}
	if str(nested(sts, "metadata", "annotations", ownerAnnotation)) != c.operationID(r) {
		return c.record(ctx, r, "Blocked", "StatefulSet recovery ownership changed")
	}
	ctr, err := container(sts, r.Spec.Container)
	if err != nil {
		return err
	}
	env, err := c.environment(ctx, ctr)
	if err != nil {
		return err
	}
	if err := c.storeConfigMatches(env); err != nil {
		return c.record(ctx, r, "Blocked", err.Error())
	}
	if err := noPVC(sts, ctr, env); err != nil {
		return c.record(ctx, r, "Blocked", err.Error())
	}
	members, err := c.targetCredentials(ctx, r, r.Status.SecretName, nil)
	if err != nil {
		return c.record(ctx, r, "Blocked", err.Error())
	}
	record, err := c.sourceMembership(ctx, path.Join(c.Prefix, r.Status.Target))
	if err != nil || record != recovery.NewMembershipRecord(r.Status.Target, members, r.Spec.Durability) {
		return c.record(ctx, r, "Blocked", "target membership changed")
	}
	if r.Status.Stage == "Starting" {
		if env["RHIZA_CLUSTER_ID"] == r.Spec.SourceClusterID {
			sourceMembers, err := recoveryMembers(env["RHIZA_CLUSTER_MEMBERS"])
			sourcePrefix := path.Join(c.Prefix, r.Spec.SourceClusterID)
			if err != nil || sourceFingerprint(r.Spec.SourceClusterID, r.Status.SourceDurability, sourceMembers, sourcePrefix) != r.Status.SourceMembership {
				return c.record(ctx, r, "Blocked", "source configuration changed before activation")
			}
			// Validate the durable copy again after a paused/restarted controller,
			// before any target voter can interpret an absent archive as empty.
			reconfiguration, err := reconfigurationEnabled(env)
			if err != nil {
				return c.record(ctx, r, "Blocked", err.Error())
			}
			result, err := recovery.Fork(ctx, c.Bucket, c.forkOptions(r, sourcePrefix, sourceMembers, reconfiguration, members))
			if err != nil || result.Tip != r.Status.RecoveredTip || result.ManifestHash != r.Status.ManifestHash {
				return c.record(ctx, r, "Blocked", "prepared target evidence changed before activation")
			}
			if number(nested(sts, "spec", "replicas")) != 0 {
				return c.record(ctx, r, "Blocked", "source replicas reappeared before activation")
			}
			pods, err := c.ownedPods(ctx, r.Status.StatefulSetUID)
			if err != nil {
				return err
			}
			if len(pods) != 0 {
				return c.record(ctx, r, "Blocked", "source pods reappeared before activation")
			}
			values := list(ctr["env"])
			values = replaceEnv(values, "RHIZA_CLUSTER_ID", object{"value": r.Status.Target})
			values = replaceEnv(values, "RHIZA_OBJSTORE_DURABILITY", object{"value": r.Spec.Durability})
			values = replaceEnv(values, "RHIZA_CLUSTER_MEMBERS", object{"valueFrom": object{"secretKeyRef": object{"name": r.Status.SecretName, "key": "members"}}})
			values = replaceEnv(values, "RHIZA_ADMIN_TOKEN", object{"valueFrom": object{"secretKeyRef": object{"name": r.Status.SecretName, "key": "admin"}}})
			ctr["env"] = values
			asObject(sts["spec"])["replicas"] = float64(3)
			if err := c.Kube.Put(ctx, c.stsPath(r.Spec.StatefulSet), sts, &sts); err != nil {
				return err
			}
		} else if env["RHIZA_CLUSTER_ID"] != r.Status.Target {
			return c.record(ctx, r, "Blocked", "unexpected StatefulSet generation during activation")
		}
		r.Status.Stage = "Restarted"
		return c.record(ctx, r, "WaitingReady", "target started; waiting for a matching recovered quorum")
	}
	actual, err := recoveryMembers(env["RHIZA_CLUSTER_MEMBERS"])
	if err != nil || env["RHIZA_CLUSTER_ID"] != r.Status.Target || env["RHIZA_OBJSTORE_DURABILITY"] != r.Spec.Durability || sourceFingerprint(r.Status.Target, r.Spec.Durability, actual, c.Prefix) != sourceFingerprint(r.Status.Target, r.Spec.Durability, members, c.Prefix) {
		return c.record(ctx, r, "Blocked", "active target configuration changed")
	}
	peers, err := c.probe(ctx, r, sts)
	if err != nil {
		return err
	}
	r.Status.Peers = peers
	allowed := map[string]bool{}
	for _, member := range members {
		allowed[string(member.ID)] = true
	}
	ready := map[string]bool{}
	for _, peer := range peers {
		if allowed[peer.NodeID] && peer.ClusterID == r.Status.Target && peer.Durability == r.Spec.Durability && peer.Ready && peer.Quorum && peer.CertifiedTip >= r.Status.RecoveredTip && peer.AppliedTip >= r.Status.RecoveredTip {
			ready[peer.NodeID] = true
		}
	}
	if len(ready) < 2 {
		return c.record(ctx, r, "WaitingReady", "waiting for a matching recovered quorum")
	}
	// Commit completion before releasing ownership. A stale reconciler cannot
	// mutate a future generation after its annotation changes.
	r.Status.Stage = "Complete"
	return c.record(ctx, r, "Complete", "new generation has a verified recovered quorum")
}

func (c *Controller) reservation(r *Resource) object {
	return object{"version": 1, "operation_id": c.operationID(r), "target": r.Status.Target, "source": r.Spec.SourceClusterID, "source_membership": r.Status.SourceMembership, "statefulset_uid": r.Status.StatefulSetUID, "spec_hash": r.Status.SpecHash}
}
func reconfigurationEnabled(env map[string]string) (bool, error) {
	raw := env["RHIZA_ENABLE_RECONFIGURATION"]
	if raw == "" {
		return false, nil
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("RHIZA_ENABLE_RECONFIGURATION must be a boolean")
	}
	return enabled, nil
}
func (c *Controller) forkOptions(r *Resource, sourcePrefix string, sourceMembers []quepaxa.Member, reconfiguration bool, targetMembers []quepaxa.Member) recovery.ForkOptions {
	options := recovery.ForkOptions{SourcePrefix: sourcePrefix, TargetPrefix: path.Join(c.Prefix, r.Status.Target), Members: sourceMembers, OperationID: c.operationID(r)}
	if reconfiguration {
		options.SourceBootstrap = quepaxa.Cluster{ConfigID: 1, Members: sourceMembers}
		options.TargetMembers = targetMembers
		options.TargetMembership = recovery.NewMembershipRecord(r.Status.Target, targetMembers, r.Spec.Durability)
	}
	return options
}
func (c *Controller) verifyReservation(ctx context.Context, r *Resource) error {
	reader, err := c.Bucket.Get(ctx, path.Join(c.Prefix, r.Spec.SourceClusterID, "recovery/successor.json"))
	if err != nil {
		return err
	}
	want := jsonBytes(c.reservation(r))
	got, err := io.ReadAll(io.LimitReader(reader, int64(len(want))+1))
	closeErr := reader.Close()
	if err != nil || closeErr != nil || !bytes.Equal(want, got) {
		return fmt.Errorf("source successor reservation changed")
	}
	return nil
}

func (c *Controller) releaseCompleted(ctx context.Context, r *Resource) error {
	var sts object
	if err := c.Kube.Get(ctx, c.stsPath(r.Spec.StatefulSet), &sts); err != nil {
		return err
	}
	if str(nested(sts, "metadata", "uid")) != r.Status.StatefulSetUID || str(nested(sts, "metadata", "annotations", ownerAnnotation)) != c.operationID(r) {
		return nil
	}
	delete(asObject(nested(sts, "metadata", "annotations")), ownerAnnotation)
	return c.Kube.Put(ctx, c.stsPath(r.Spec.StatefulSet), sts, nil)
}

func (c *Controller) ownedPods(ctx context.Context, uid string) ([]object, error) {
	var pods podList
	if err := c.Kube.Get(ctx, c.corePath("pods", ""), &pods); err != nil {
		return nil, err
	}
	var owned []object
	for _, pod := range pods.Items {
		for _, ref := range list(nested(pod, "metadata", "ownerReferences")) {
			owner := asObject(ref)
			if str(owner["uid"]) == uid && str(owner["kind"]) == "StatefulSet" {
				owned = append(owned, pod)
				break
			}
		}
	}
	return owned, nil
}
func podEndpoint(pod object, containerName, endpoint string) (string, error) {
	ip := str(nested(pod, "status", "podIP"))
	if net.ParseIP(ip) == nil {
		return "", fmt.Errorf("pod IP unavailable")
	}
	ctr, err := container(object{"spec": object{"template": object{"spec": pod["spec"]}}}, containerName)
	if err != nil {
		return "", err
	}
	port := "8080"
	// Embedded hosts can keep the application HTTP port separate.
	for _, name := range []string{"recovery", "http"} {
		for _, p := range list(ctr["ports"]) {
			v := asObject(p)
			if str(v["name"]) != name {
				continue
			}
			n := number(v["containerPort"])
			if n < 1 || n > 65535 || str(v["protocol"]) != "" && str(v["protocol"]) != "TCP" {
				return "", fmt.Errorf("invalid recovery HTTP port")
			}
			return "http://" + net.JoinHostPort(ip, fmt.Sprint(n)) + endpoint, nil
		}
	}
	return "http://" + net.JoinHostPort(ip, port) + endpoint, nil
}
func (c *Controller) client() *http.Client {
	result := http.Client{Timeout: 30 * time.Second}
	if c.HTTP != nil {
		result = *c.HTTP
	}
	result.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &result
}
func (c *Controller) probe(ctx context.Context, r *Resource, sts object) ([]network.VoterRecoveryStatus, error) {
	pods, err := c.ownedPods(ctx, str(nested(sts, "metadata", "uid")))
	if err != nil {
		return nil, err
	}
	var peers []network.VoterRecoveryStatus
	for _, pod := range pods {
		if nested(pod, "metadata", "deletionTimestamp") != nil {
			continue
		}
		endpoint, err := podEndpoint(pod, r.Spec.Container, "/recovery/status")
		if err != nil {
			continue
		}
		call, cancel := context.WithTimeout(ctx, 3*time.Second)
		req, _ := http.NewRequestWithContext(call, http.MethodGet, endpoint, nil)
		response, err := c.client().Do(req)
		if err == nil {
			var peer network.VoterRecoveryStatus
			if response.StatusCode == 200 && json.NewDecoder(io.LimitReader(response.Body, 16<<10)).Decode(&peer) == nil && peer.NodeID == str(nested(pod, "metadata", "name")) {
				peers = append(peers, peer)
			}
			response.Body.Close()
		}
		cancel()
	}
	return peers, nil
}
func (c *Controller) captureArchive(ctx context.Context, r *Resource, sts object, token string) string {
	if token == "" {
		return "unavailable: source admin token is not configured"
	}
	pods, err := c.ownedPods(ctx, str(nested(sts, "metadata", "uid")))
	if err != nil {
		return "unavailable: pod inventory failed"
	}
	attempted, success := 0, 0
	for _, pod := range pods {
		endpoint, err := podEndpoint(pod, r.Spec.Container, "/recovery/archive")
		if err != nil {
			continue
		}
		attempted++
		call, cancel := context.WithTimeout(ctx, 10*time.Second)
		req, _ := http.NewRequestWithContext(call, http.MethodPost, endpoint, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		response, err := c.client().Do(req)
		if err == nil {
			if response.StatusCode == 200 {
				success++
			}
			response.Body.Close()
		}
		cancel()
	}
	return fmt.Sprintf("certified suffix captured from %d/%d reachable pod endpoints; missing history is not bounded", success, attempted)
}
func (c *Controller) claimStatefulSet(ctx context.Context, sts object, op string) error {
	meta := asObject(sts["metadata"])
	annotations := asObject(meta["annotations"])
	if annotations == nil {
		annotations = object{}
		meta["annotations"] = annotations
	}
	existing := str(annotations[ownerAnnotation])
	if existing != "" && existing != op {
		return fmt.Errorf("StatefulSet belongs to another recovery operation")
	}
	if existing == op {
		return nil
	}
	annotations[ownerAnnotation] = op
	return c.Kube.Put(ctx, c.stsPath(str(meta["name"])), sts, &sts)
}
func (c *Controller) targetCredentials(ctx context.Context, r *Resource, name string, source []quepaxa.Member) ([]quepaxa.Member, error) {
	var secret object
	err := c.Kube.Get(ctx, c.corePath("secrets", name), &secret)
	var api *APIError
	if errors.As(err, &api) && api.StatusCode == 404 && source != nil {
		members, admin, err := freshMembers(source)
		if err != nil {
			return nil, err
		}
		secret = object{"apiVersion": "v1", "kind": "Secret", "type": "Opaque", "immutable": true, "metadata": object{"name": name, "annotations": object{ownerAnnotation: c.operationID(r)}}, "data": object{"members": base64.StdEncoding.EncodeToString(jsonBytes(members)), "admin": base64.StdEncoding.EncodeToString([]byte(admin))}}
		// Even an uncertain create is resolved by reading the same deterministic key.
		createErr := c.Kube.Post(ctx, c.corePath("secrets", ""), secret, nil)
		if err = c.Kube.Get(ctx, c.corePath("secrets", name), &secret); err != nil {
			return nil, errors.Join(createErr, err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("target credential secret unavailable")
	}
	if str(nested(secret, "metadata", "annotations", ownerAnnotation)) != c.operationID(r) || secret["immutable"] != true {
		return nil, fmt.Errorf("target secret does not belong to this immutable recovery operation")
	}
	data, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "members")))
	if err != nil {
		return nil, fmt.Errorf("invalid target credentials")
	}
	members, err := recoveryMembers(string(data))
	if err != nil || len(members) != 3 {
		return nil, fmt.Errorf("invalid target membership")
	}
	admin, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "admin")))
	if err != nil || len(admin) < 32 {
		return nil, fmt.Errorf("invalid target admin credential")
	}
	if source != nil {
		old := map[quepaxa.NodeID]quepaxa.Member{}
		for _, m := range source {
			old[m.ID] = m
		}
		for _, m := range members {
			prior, ok := old[m.ID]
			if !ok || m.Token == prior.Token || m.URL != prior.URL || m.PeerURL != prior.PeerURL || m.LogURL != prior.LogURL {
				return nil, fmt.Errorf("target credentials do not preserve source voter endpoints")
			}
		}
	}
	return members, nil
}
func freshMembers(source []quepaxa.Member) ([]quepaxa.Member, string, error) {
	members := append([]quepaxa.Member(nil), source...)
	for i := range members {
		token, err := randomToken()
		if err != nil {
			return nil, "", err
		}
		members[i].Token = token
	}
	admin, err := randomToken()
	return members, admin, err
}
func randomToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
func (c *Controller) immutable(ctx context.Context, key string, data []byte) error {
	writeErr := c.Bucket.Upload(ctx, key, bytes.NewReader(data), objstore.WithIfNotExists())
	reader, err := c.Bucket.Get(ctx, key)
	if err != nil {
		return errors.Join(writeErr, err)
	}
	got, err := io.ReadAll(io.LimitReader(reader, int64(len(data))+1))
	closeErr := reader.Close()
	if err != nil || closeErr != nil || !bytes.Equal(data, got) {
		return fmt.Errorf("immutable recovery object differs or is unavailable")
	}
	return nil
}
func (c *Controller) sourceMembership(ctx context.Context, prefix string) (recovery.MembershipRecord, error) {
	var record recovery.MembershipRecord
	reader, err := c.Bucket.Get(ctx, path.Join(prefix, "voters/membership.json"))
	if err != nil {
		return record, fmt.Errorf("source membership history unavailable")
	}
	data, err := io.ReadAll(io.LimitReader(reader, 64<<10))
	closeErr := reader.Close()
	if err != nil || closeErr != nil || len(data) == 64<<10 || json.Unmarshal(data, &record) != nil || record.Version != 1 || (record.Durability != "async" && record.Durability != "before-ack") || !bytes.Equal(data, jsonBytes(record)) {
		return record, fmt.Errorf("source membership or durability history is invalid")
	}
	return record, nil
}
func (c *Controller) record(ctx context.Context, r *Resource, phase, message string) error {
	r.Status.Phase, r.Status.Message = phase, message
	return c.Kube.Put(ctx, c.resourcePath(resourceName(r))+"/status", r, r)
}
func validateFence(f Fence, op, cluster, uid string) error {
	if op == "" || uid == "" || !f.Confirmed || strings.TrimSpace(f.Evidence) == "" || f.RecoveryID != op || f.ClusterID != cluster || f.StatefulSetUID != uid {
		return fmt.Errorf("confirmed fence must bind this operation, source cluster, and StatefulSet UID")
	}
	return nil
}
func replaceEnv(env []any, name string, value object) []any {
	entry := object{"name": name}
	for k, v := range value {
		entry[k] = v
	}
	var result []any
	for _, item := range env {
		if str(asObject(item)["name"]) != name {
			result = append(result, item)
		}
	}
	return append(result, entry)
}
func shortID(value string) string { return hashJSON(value)[:24] }
func hashJSON(value any) string {
	sum := sha256.Sum256(jsonBytes(value))
	return hex.EncodeToString(sum[:])
}
func sourceFingerprint(cluster, mode string, members []quepaxa.Member, prefix string) string {
	m := append([]quepaxa.Member(nil), members...)
	sort.Slice(m, func(i, j int) bool { return m[i].ID < m[j].ID })
	return hashJSON(struct {
		Cluster, Mode, Prefix string
		Members               []quepaxa.Member
	}{cluster, mode, prefix, m})
}
func immutableSpecHash(s Spec) string { s.Fence = Fence{}; s.AllowDataLoss = false; return hashJSON(s) }
func recoveryMembers(raw string) ([]quepaxa.Member, error) {
	var members []quepaxa.Member
	if json.Unmarshal([]byte(raw), &members) != nil || len(members) == 0 {
		return nil, fmt.Errorf("invalid source membership")
	}
	seen := map[quepaxa.NodeID]bool{}
	for _, m := range members {
		if !identifier.MatchString(string(m.ID)) || m.Token == "" || seen[m.ID] {
			return nil, fmt.Errorf("incomplete or duplicate voter identity")
		}
		seen[m.ID] = true
		for _, endpoint := range []string{m.URL, m.PeerURL, m.LogURL} {
			if endpoint == "" {
				continue
			}
			u, err := url.Parse(endpoint)
			if err != nil || u.Host == "" || u.User != nil {
				return nil, fmt.Errorf("invalid voter endpoint")
			}
		}
	}
	return members, nil
}
