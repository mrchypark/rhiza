package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/operator"
)

const (
	gcsChaosRunLabel = "chaos.rhiza.io/run"
	gcsChaosDuration = "180s"
	gcsChaosMaxPods  = 64
)

var gcsChaosNamePart = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// gcsChaosFault pins the exact created NetworkChaos object and both sides of
// its selector. A same-name replacement is never treated as this fault.
type gcsChaosFault struct {
	contextName  string
	namespace    string
	runID        string
	name         string
	uid          string
	mode         string
	role         string
	phase        string
	intentDigest string
	expiresAt    time.Time
	selector     obj
	target       obj
	sourcePods   map[string]gcsControlPodIdentity
	// sourceUIDs is retained for the driver evidence envelope; sourcePods is
	// the authority used for owner-chain verification.
	sourceUIDs   map[string]string
	targetUIDs   map[string]string
	capturedPods map[string]gcsControlPodIdentity
	captureLabel string
}

// createGCSControlLoss installs a run-scoped, exact-Pod NetworkChaos rule.
// role is either "runtime-agent" or "operator"; pods must be the exact live
// source Pod and controller identities captured by the caller. The only
// target is the exact live rhiza-fencer Pod.
func createGCSControlLoss(ctx context.Context, namespace, runID, phase, role string, pods []gcsControlPodIdentity) (*gcsChaosFault, error) {
	if role != "runtime-agent" && role != "operator" {
		return nil, fmt.Errorf("unsupported GCS control-loss role")
	}
	if err := validateGCSChaosScope(namespace, runID, phase); err != nil {
		return nil, err
	}
	contextName := strings.TrimSpace(os.Getenv("RHIZA_KUBE_CONTEXT"))
	if contextName == "" {
		return nil, fmt.Errorf("RHIZA_KUBE_CONTEXT is required for GCS NetworkChaos")
	}
	if len(pods) == 0 || len(pods) > gcsChaosMaxPods {
		return nil, fmt.Errorf("GCS control-loss requires 1..%d captured source Pod identities", gcsChaosMaxPods)
	}
	byName := make(map[string]gcsControlPodIdentity, len(pods))
	for _, pod := range pods {
		if !validGCSChaosPodName(pod.Name) || pod.UID == "" || pod.OwnerUID == "" || byName[pod.Name].Name != "" ||
			(role == "runtime-agent" && pod.ParentOwnerUID != "") || (role == "operator" && pod.ParentOwnerUID == "") {
			return nil, fmt.Errorf("GCS control-loss requires unique captured Pod, UID and controller identities")
		}
		byName[pod.Name] = pod
	}
	podNames := make([]string, 0, len(byName))
	for name := range byName {
		podNames = append(podNames, name)
	}
	sortedNames, err := sortedGCSChaosPodNames(podNames, 1)
	if err != nil {
		return nil, err
	}
	if role == "operator" && len(sortedNames) != 1 {
		return nil, fmt.Errorf("operator control-loss must name exactly one operator Pod")
	}

	sourcePods := make(map[string]gcsControlPodIdentity, len(sortedNames))
	for _, name := range sortedNames {
		pod, err := getGCSChaosPod(ctx, contextName, namespace, name)
		if err != nil {
			return nil, fmt.Errorf("read exact %s source Pod %s: %w", role, name, err)
		}
		if !gcsChaosPodBelongs(pod, runID, role, byName[name]) {
			return nil, fmt.Errorf("source Pod %s is not the expected run-owned %s identity", name, role)
		}
		if err := verifyGCSChaosControlOwner(ctx, contextName, namespace, role, pod, byName[name]); err != nil {
			return nil, err
		}
		sourcePods[name] = byName[name]
	}

	fencer, err := getGCSChaosPods(ctx, contextName, namespace, map[string]string{"app": "rhiza-fencer", gcsChaosRunLabel: runID})
	if err != nil {
		return nil, fmt.Errorf("read run-owned fencer Pod: %w", err)
	}
	if len(fencer) != 1 {
		return nil, fmt.Errorf("GCS control-loss requires exactly one run-owned fencer Pod")
	}
	fencerName := text(nested(fencer[0], "metadata", "name"))
	if !strings.HasPrefix(fencerName, "rhiza-fencer-") || text(nested(fencer[0], "metadata", "uid")) == "" || nested(fencer[0], "metadata", "deletionTimestamp") != nil || !podReady(fencer[0]) {
		return nil, fmt.Errorf("run-owned fencer Pod identity is incomplete")
	}

	var sourceLabels map[string]string
	switch role {
	case "runtime-agent":
		// The shared authority DaemonSet has no per-run Pod label. The
		// isolated run namespace, explicit Pod names and app label bind it.
		sourceLabels = map[string]string{"app.kubernetes.io/name": "rhiza-runtime-agent"}
	case "operator":
		sourceLabels = map[string]string{"app": "rhiza-operator", gcsChaosRunLabel: runID}
	}
	selector := gcsChaosPodSelector(namespace, sourceLabels, sortedNames)
	target := gcsChaosPodSelector(namespace, map[string]string{"app": "rhiza-fencer", gcsChaosRunLabel: runID}, []string{fencerName})
	mode := "one"
	if role == "runtime-agent" {
		mode = "all"
	}
	return createGCSChaosFault(ctx, contextName, namespace, runID, phase, role, mode, selector, target, sourcePods, map[string]string{fencerName: text(nested(fencer[0], "metadata", "uid"))})
}

// partitionGCSVoterCohort cuts every source voter from every other source
// voter. Both selectors contain the same bounded, explicit Pod set; it never
// leaves a two-of-three majority accidentally connected.
func partitionGCSVoterCohort(ctx context.Context, namespace, runID, phase string, voters []operator.FenceTarget) (*gcsChaosFault, error) {
	if err := validateGCSChaosScope(namespace, runID, phase); err != nil {
		return nil, err
	}
	if len(voters) < 2 || len(voters) > gcsChaosMaxPods {
		return nil, fmt.Errorf("GCS voter cohort must contain 2..%d explicit voters", gcsChaosMaxPods)
	}
	contextName := strings.TrimSpace(os.Getenv("RHIZA_KUBE_CONTEXT"))
	if contextName == "" {
		return nil, fmt.Errorf("RHIZA_KUBE_CONTEXT is required for GCS NetworkChaos")
	}
	byName := make(map[string]operator.FenceTarget, len(voters))
	for _, voter := range voters {
		if voter.Pod == "" || voter.PodUID == "" || !strings.HasPrefix(voter.Pod, "rhiza-voter-") || byName[voter.Pod].Pod != "" {
			return nil, fmt.Errorf("GCS voter cohort contains an incomplete or duplicate identity")
		}
		byName[voter.Pod] = voter
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	validatedNames, err := sortedGCSChaosPodNames(names, 2)
	if err != nil {
		return nil, err
	}
	names = validatedNames
	uidByName := make(map[string]string, len(names))
	for _, name := range names {
		pod, err := getGCSChaosPod(ctx, contextName, namespace, name)
		if err != nil {
			return nil, fmt.Errorf("read voter cohort Pod %s: %w", name, err)
		}
		if text(nested(pod, "metadata", "uid")) != byName[name].PodUID || nested(pod, "metadata", "deletionTimestamp") != nil || !podReady(pod) ||
			nested(pod, "metadata", "labels", gcsChaosRunLabel) != runID || nested(pod, "metadata", "labels", "app") != "rhiza-voter" ||
			nested(pod, "metadata", "labels", "app.kubernetes.io/part-of") != "rhiza-chaos" {
			return nil, fmt.Errorf("voter cohort Pod %s changed incarnation or is outside this run", name)
		}
		uidByName[name] = byName[name].PodUID
	}
	labels := map[string]string{"app": "rhiza-voter", "app.kubernetes.io/part-of": "rhiza-chaos", gcsChaosRunLabel: runID}
	selector := gcsChaosPodSelector(namespace, labels, names)
	target := gcsChaosPodSelector(namespace, labels, names)
	sourcePods := make(map[string]gcsControlPodIdentity, len(uidByName))
	for name, uid := range uidByName {
		sourcePods[name] = gcsControlPodIdentity{Name: name, UID: uid}
	}
	return createGCSChaosFault(ctx, contextName, namespace, runID, phase, "voter-cohort", "all", selector, target, sourcePods, uidByName)
}

func (f *gcsChaosFault) WaitInjected(ctx context.Context) error {
	if f == nil || f.uid == "" || f.expiresAt.IsZero() {
		return fmt.Errorf("GCS NetworkChaos identity is not pinned")
	}
	return wait(ctx, "GCS NetworkChaos injection", func() (bool, error) {
		return f.checkInjected(ctx)
	})
}

// RecheckInjected guards each long-running scenario phase against Chaos Mesh's
// fixed 180-second fault lifetime. Callers must stop advancing if it expires.
func (f *gcsChaosFault) RecheckInjected(ctx context.Context) error {
	if f == nil || f.expiresAt.IsZero() || !time.Now().Before(f.expiresAt.Add(-5*time.Second)) {
		return fmt.Errorf("GCS NetworkChaos lifetime expired or is not pinned")
	}
	injected, err := f.checkInjected(ctx)
	if err != nil {
		return err
	}
	if !injected {
		return fmt.Errorf("GCS NetworkChaos is no longer confirmed injected")
	}
	return nil
}

func (f *gcsChaosFault) checkInjected(ctx context.Context) (bool, error) {
	if !time.Now().Before(f.expiresAt.Add(-5 * time.Second)) {
		return false, fmt.Errorf("GCS NetworkChaos lifetime expired")
	}
	current, err := getGCSChaosObject(ctx, f.contextName, f.namespace, f.name)
	if err != nil {
		return false, err
	}
	if !f.matchesObject(current) {
		return false, fmt.Errorf("GCS NetworkChaos changed identity or selector while waiting")
	}
	conditions, _ := nested(current, "status", "conditions").([]any)
	injected, recovered := 0, 0
	for _, raw := range conditions {
		if nested(raw, "status") != "True" {
			continue
		}
		switch nested(raw, "type") {
		case "AllInjected":
			injected++
		case "AllRecovered":
			recovered++
		}
	}
	if recovered != 0 || injected > 1 {
		return false, fmt.Errorf("GCS NetworkChaos has recovered or duplicate injection status")
	}
	if injected != 1 {
		return false, nil
	}
	if err := f.checkSelectedPods(ctx); err != nil {
		return false, err
	}
	if err := validateGCSChaosRecords(current, f.namespace, f.sourcePods, f.targetUIDs); err != nil {
		return false, err
	}
	if !time.Now().Before(f.expiresAt.Add(-5 * time.Second)) {
		return false, fmt.Errorf("GCS NetworkChaos lifetime expired during injection verification")
	}
	return true, nil
}

func (f *gcsChaosFault) Restore(ctx context.Context) error {
	if f == nil || f.uid == "" || f.contextName == "" || f.namespace == "" || f.name == "" {
		return fmt.Errorf("refusing to restore an unpinned GCS NetworkChaos")
	}
	deleteErr := deleteGCSNetworkChaosUID(ctx, f.contextName, f.namespace, f.name, f.uid)
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := wait(waitCtx, "GCS NetworkChaos UID cleanup", func() (bool, error) {
		current, err := getGCSChaosObject(waitCtx, f.contextName, f.namespace, f.name)
		if err != nil {
			return false, err
		}
		currentUID := text(nested(current, "metadata", "uid"))
		if currentUID == "" || currentUID == f.uid {
			return currentUID == "", nil
		}
		return false, fmt.Errorf("GCS NetworkChaos name was reused by another UID; refusing to claim cleanup")
	}); err != nil {
		return fmt.Errorf("delete error %v; confirm exact GCS NetworkChaos UID cleanup: %w", deleteErr, err)
	}
	if err := unmarkGCSChaosPods(ctx, f.contextName, f.namespace, f.capturedPods, f.captureLabel); err != nil {
		return fmt.Errorf("GCS NetworkChaos UID %s is gone but captured Pod labels remain: %w", f.uid, err)
	}
	return deleteErr
}

func (f *gcsChaosFault) checkSelectedPods(ctx context.Context) error {
	for name, identity := range f.sourcePods {
		pod, err := getGCSChaosPod(ctx, f.contextName, f.namespace, name)
		if err != nil || text(nested(pod, "metadata", "uid")) != identity.UID ||
			text(nested(pod, "metadata", "labels", "rhiza.dev/gcs-capture")) != f.captureLabel {
			return fmt.Errorf("GCS NetworkChaos source Pod %s changed during injection: %v", name, err)
		}
		if f.role != "voter-cohort" && !gcsChaosPodBelongs(pod, f.runID, f.role, identity) {
			return fmt.Errorf("GCS NetworkChaos source Pod %s identity changed during injection", name)
		}
		if f.role != "voter-cohort" {
			if err := verifyGCSChaosControlOwner(ctx, f.contextName, f.namespace, f.role, pod, identity); err != nil {
				return err
			}
		}
	}
	for name, uid := range f.targetUIDs {
		pod, err := getGCSChaosPod(ctx, f.contextName, f.namespace, name)
		if err != nil || text(nested(pod, "metadata", "uid")) != uid ||
			text(nested(pod, "metadata", "labels", "rhiza.dev/gcs-capture")) != f.captureLabel {
			return fmt.Errorf("GCS NetworkChaos target Pod %s changed during injection: %v", name, err)
		}
	}
	return nil
}

func createGCSChaosFault(ctx context.Context, contextName, namespace, runID, phase, role, mode string, selector, targetSelector obj, sourcePods map[string]gcsControlPodIdentity, targetUIDs map[string]string) (*gcsChaosFault, error) {
	name := "gcs-" + phase + "-" + role
	name = strings.ReplaceAll(name, "_", "-")
	if len(name) > 63 || !validGCSChaosPodName(name) {
		return nil, fmt.Errorf("GCS NetworkChaos name is invalid")
	}
	labels := obj{gcsChaosRunLabel: runID, "rhiza.dev/fault-phase": phase, "rhiza.dev/fault-role": role}
	targetSpec := obj{"mode": "all", "selector": targetSelector}
	intent := obj{"namespace": namespace, "runID": runID, "phase": phase, "role": role, "mode": mode,
		"sourcePods": sourcePods, "targetUIDs": targetUIDs}
	intentBytes, err := json.Marshal(intent)
	if err != nil {
		return nil, fmt.Errorf("encode GCS NetworkChaos intent: %w", err)
	}
	intentHash := sha256.Sum256(intentBytes)
	intentDigest := hex.EncodeToString(intentHash[:])
	captureLabel := intentDigest[:32]
	capturedPods := make(map[string]gcsControlPodIdentity, len(sourcePods)+len(targetUIDs))
	for name, identity := range sourcePods {
		capturedPods[name] = identity
	}
	for name, uid := range targetUIDs {
		if previous, ok := capturedPods[name]; ok && previous.UID != uid {
			return nil, fmt.Errorf("GCS NetworkChaos source and target Pod identities conflict")
		}
		if _, ok := capturedPods[name]; !ok {
			capturedPods[name] = gcsControlPodIdentity{Name: name, UID: uid}
		}
	}
	markedPods := make(map[string]gcsControlPodIdentity, len(capturedPods))
	capturedNames := make([]string, 0, len(capturedPods))
	for name := range capturedPods {
		capturedNames = append(capturedNames, name)
	}
	sort.Strings(capturedNames)
	captured := make([]gcsControlPodIdentity, 0, len(capturedNames))
	for _, name := range capturedNames {
		captured = append(captured, capturedPods[name])
	}
	if err := captureGCSChaosPods(captured, captureLabel,
		func(pod gcsControlPodIdentity) (bool, error) {
			return markGCSChaosPod(ctx, contextName, namespace, pod, captureLabel)
		},
		func(pods map[string]gcsControlPodIdentity) error {
			return unmarkGCSChaosPods(ctx, contextName, namespace, pods, captureLabel)
		}, markedPods); err != nil {
		return nil, err
	}
	addCaptureSelector(selector, captureLabel)
	addCaptureSelector(targetSelector, captureLabel)
	annotations := obj{"rhiza.dev/intent-sha256": intentDigest}
	body := obj{"apiVersion": "chaos-mesh.org/v1alpha1", "kind": "NetworkChaos",
		"metadata": obj{"name": name, "namespace": namespace, "labels": labels, "annotations": annotations},
		"spec": obj{"action": "partition", "direction": "both", "mode": mode, "duration": gcsChaosDuration,
			"selector": selector, "target": targetSpec}}
	data, err := commandWithInput(ctx, encoded(body), "kubectl", "--context", contextName, "-n", namespace, "create", "-f", "-", "-o", "json")
	var created obj
	if err == nil {
		err = json.Unmarshal(data, &created)
	}
	if err != nil || !chaosObjectMatchesExpected(created, body, intentDigest, namespace, name, runID) {
		// The create response may be lost after the API persisted the object. Only
		// reconcile by the deterministic name and exact full expected spec/intent.
		current, getErr := getGCSChaosObject(ctx, contextName, namespace, name)
		if getErr != nil {
			return nil, fmt.Errorf("create GCS NetworkChaos outcome is unknown; inspect exact name %s/%s: create=%v get=%w", namespace, name, err, getErr)
		}
		if !chaosObjectMatchesExpected(current, body, intentDigest, namespace, name, runID) {
			if text(nested(current, "metadata", "uid")) != "" {
				return nil, fmt.Errorf("GCS NetworkChaos create outcome is ambiguous; mismatched object remains at %s/%s UID %s", namespace, name, text(nested(current, "metadata", "uid")))
			}
			return nil, fmt.Errorf("create GCS NetworkChaos outcome is unknown after an absent read; capture labels retained for cleanup at %s/%s: %v", namespace, name, err)
		}
		created = current
	}
	uid := text(nested(created, "metadata", "uid"))
	if uid == "" {
		return nil, fmt.Errorf("created GCS NetworkChaos did not return its exact run-owned identity")
	}
	sourceUIDs := make(map[string]string, len(sourcePods))
	for podName, identity := range sourcePods {
		sourceUIDs[podName] = identity.UID
	}
	createdAt, parseErr := time.Parse(time.RFC3339, text(nested(created, "metadata", "creationTimestamp")))
	if parseErr != nil {
		fault := &gcsChaosFault{contextName: contextName, namespace: namespace, runID: runID, name: name, uid: uid,
			mode: mode, role: role, phase: phase, intentDigest: intentDigest, selector: selector, target: targetSelector,
			sourcePods: sourcePods, targetUIDs: targetUIDs, capturedPods: capturedPods, captureLabel: captureLabel}
		cleanupErr := fault.Restore(ctx)
		return nil, fmt.Errorf("NetworkChaos creation timestamp is missing or invalid; exact UID cleanup: %v", cleanupErr)
	}
	return &gcsChaosFault{contextName: contextName, namespace: namespace, runID: runID, name: name, uid: uid, mode: mode,
		role: role, phase: phase, intentDigest: intentDigest, expiresAt: createdAt.Add(180 * time.Second), selector: selector,
		target: targetSelector, sourcePods: sourcePods, sourceUIDs: sourceUIDs, targetUIDs: targetUIDs, capturedPods: capturedPods, captureLabel: captureLabel}, nil
}

func (f *gcsChaosFault) matchesObject(current obj) bool {
	return text(nested(current, "metadata", "uid")) == f.uid &&
		text(nested(current, "metadata", "name")) == f.name &&
		text(nested(current, "metadata", "namespace")) == f.namespace &&
		nested(current, "metadata", "labels", gcsChaosRunLabel) == f.runID &&
		nested(current, "metadata", "labels", "rhiza.dev/fault-phase") == f.phase &&
		nested(current, "metadata", "labels", "rhiza.dev/fault-role") == f.role &&
		nested(current, "metadata", "annotations", "rhiza.dev/intent-sha256") == f.intentDigest &&
		nested(current, "spec", "action") == "partition" && nested(current, "spec", "direction") == "both" &&
		nested(current, "spec", "mode") == f.mode && nested(current, "spec", "duration") == gcsChaosDuration &&
		nested(current, "spec", "target", "mode") == "all" &&
		jsonSemanticEqual(nested(current, "spec", "selector"), f.selector) &&
		jsonSemanticEqual(nested(current, "spec", "target", "selector"), f.target)
}

func addCaptureSelector(selector obj, value string) {
	labels, _ := selector["labelSelectors"].(obj)
	if labels == nil {
		labels = obj{}
		selector["labelSelectors"] = labels
	}
	labels["rhiza.dev/gcs-capture"] = value
}

func captureGCSChaosPods(pods []gcsControlPodIdentity, value string,
	mark func(gcsControlPodIdentity) (bool, error), unmark func(map[string]gcsControlPodIdentity) error,
	marked map[string]gcsControlPodIdentity) error {
	for _, pod := range pods {
		attempted, err := mark(pod)
		if attempted {
			marked[pod.Name] = pod
		}
		if err == nil {
			continue
		}
		cleanupErr := unmark(marked)
		if cleanupErr != nil {
			return fmt.Errorf("capture exact Pod UID %s for NetworkChaos: %w; cleanup failed: %v", pod.Name, err, cleanupErr)
		}
		return fmt.Errorf("capture exact Pod UID %s for NetworkChaos: %w", pod.Name, err)
	}
	return nil
}

func markGCSChaosPod(ctx context.Context, contextName, namespace string, identity gcsControlPodIdentity, value string) (bool, error) {
	if identity.Name == "" || identity.UID == "" || value == "" {
		return false, fmt.Errorf("captured Pod identity is incomplete")
	}
	pod, err := getGCSChaosPod(ctx, contextName, namespace, identity.Name)
	if err != nil {
		return false, err
	}
	if text(nested(pod, "metadata", "uid")) != identity.UID {
		return false, fmt.Errorf("captured Pod UID changed before pinning")
	}
	labels, _ := nested(pod, "metadata", "labels").(map[string]any)
	if labels == nil {
		return false, fmt.Errorf("captured Pod has no labels to safely pin")
	}
	if existing := text(labels["rhiza.dev/gcs-capture"]); existing != "" {
		return false, fmt.Errorf("captured Pod already carries another chaos identity")
	}
	patch := []any{
		obj{"op": "test", "path": "/metadata/uid", "value": identity.UID},
		obj{"op": "test", "path": "/metadata/labels", "value": labels},
		obj{"op": "add", "path": "/metadata/labels/rhiza.dev~1gcs-capture", "value": value},
	}
	encodedPatch, _ := json.Marshal(patch)
	_, err = command(ctx, "kubectl", "--context", contextName, "-n", namespace, "patch", "pod", identity.Name, "--type=json", "-p", string(encodedPatch))
	if err != nil {
		current, readErr := getGCSChaosPod(ctx, contextName, namespace, identity.Name)
		if readErr != nil {
			return true, fmt.Errorf("Pod label patch outcome unknown: patch=%v read=%w", err, readErr)
		}
		if text(nested(current, "metadata", "uid")) == identity.UID &&
			text(nested(current, "metadata", "labels", "rhiza.dev/gcs-capture")) == value {
			return true, nil
		}
		return true, fmt.Errorf("Pod label patch outcome unknown for exact UID %s; inspect capture label: patch=%v", identity.UID, err)
	}
	return true, err
}

func unmarkGCSChaosPods(ctx context.Context, contextName, namespace string, pods map[string]gcsControlPodIdentity, value string) error {
	var failures []string
	for name, identity := range pods {
		pod, err := getGCSChaosPod(ctx, contextName, namespace, name)
		if err != nil {
			// A deleted captured incarnation has no label left to clean up.
			if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "not found") {
				continue
			}
			failures = append(failures, name+": inspect: "+err.Error())
			continue
		}
		if text(nested(pod, "metadata", "uid")) != identity.UID {
			continue
		}
		label := text(nested(pod, "metadata", "labels", "rhiza.dev/gcs-capture"))
		if label == "" {
			// The exact captured Pod is already clean, including a failed patch
			// that never took effect.
			continue
		}
		if label != value {
			failures = append(failures, name+": capture label changed; left untouched")
			continue
		}
		patch := []any{
			obj{"op": "test", "path": "/metadata/uid", "value": identity.UID},
			obj{"op": "test", "path": "/metadata/labels/rhiza.dev~1gcs-capture", "value": value},
			obj{"op": "remove", "path": "/metadata/labels/rhiza.dev~1gcs-capture"},
		}
		encodedPatch, _ := json.Marshal(patch)
		if _, err := command(ctx, "kubectl", "--context", contextName, "-n", namespace, "patch", "pod", name, "--type=json", "-p", string(encodedPatch)); err != nil {
			failures = append(failures, name+": remove capture label: "+err.Error())
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("capture-label cleanup incomplete: %s", strings.Join(failures, "; "))
	}
	return nil
}

func chaosObjectMatchesExpected(current, expected obj, intentDigest, namespace, name, runID string) bool {
	if nested(current, "apiVersion") != nested(expected, "apiVersion") || nested(current, "kind") != nested(expected, "kind") ||
		text(nested(current, "metadata", "uid")) == "" || text(nested(current, "metadata", "name")) != name ||
		text(nested(current, "metadata", "namespace")) != namespace || nested(current, "metadata", "labels", gcsChaosRunLabel) != runID ||
		nested(current, "metadata", "labels", "rhiza.dev/fault-phase") != nested(expected, "metadata", "labels", "rhiza.dev/fault-phase") ||
		nested(current, "metadata", "labels", "rhiza.dev/fault-role") != nested(expected, "metadata", "labels", "rhiza.dev/fault-role") ||
		nested(current, "metadata", "annotations", "rhiza.dev/intent-sha256") != intentDigest ||
		!jsonSemanticEqual(nested(current, "spec"), nested(expected, "spec")) {
		return false
	}
	return true
}

func gcsChaosPodSelector(namespace string, labels map[string]string, pods []string) obj {
	labelCopy := make(obj, len(labels))
	for key, value := range labels {
		labelCopy[key] = value
	}
	return obj{"namespaces": []string{namespace}, "labelSelectors": labelCopy, "pods": obj{namespace: append([]string(nil), pods...)}}
}

func validateGCSChaosRecords(object obj, namespace string, sourcePods map[string]gcsControlPodIdentity, targetUIDs map[string]string) error {
	statusRecords := nested(object, "status", "experiment", "containerRecords")
	records, ok := statusRecords.([]any)
	if !ok {
		return fmt.Errorf("GCS NetworkChaos injection lacks container records")
	}
	seenSource, seenTarget := map[string]bool{}, map[string]bool{}
	for _, record := range records {
		id := text(nested(record, "id"))
		phase := text(nested(record, "phase"))
		if phase != "Injected" || nested(record, "injectedCount") != float64(1) || nested(record, "recoveredCount") != float64(0) {
			return fmt.Errorf("GCS NetworkChaos record is not exclusively injected")
		}
		events, _ := nested(record, "events").([]any)
		applied := 0
		for _, event := range events {
			if nested(event, "operation") == "Apply" && nested(event, "type") == "Succeeded" {
				applied++
			}
		}
		if applied != 1 {
			return fmt.Errorf("GCS NetworkChaos record lacks one successful apply event")
		}
		name, ok := strings.CutPrefix(id, namespace+"/")
		if !ok || name == "" {
			return fmt.Errorf("GCS NetworkChaos record is outside the exact namespace")
		}
		key := text(nested(record, "selectorKey"))
		if key == "." {
			if _, expected := sourcePods[name]; !expected || seenSource[name] {
				return fmt.Errorf("GCS NetworkChaos source record is unexpected or duplicated")
			}
			seenSource[name] = true
		} else if key == ".Target" {
			if _, expected := targetUIDs[name]; !expected || seenTarget[name] {
				return fmt.Errorf("GCS NetworkChaos target record is unexpected or duplicated")
			}
			seenTarget[name] = true
		} else {
			return fmt.Errorf("GCS NetworkChaos has an unexpected selector record")
		}
	}
	if len(seenSource) != len(sourcePods) || len(seenTarget) != len(targetUIDs) {
		return fmt.Errorf("GCS NetworkChaos did not inject every exact selected Pod")
	}
	return nil
}

func getGCSChaosPod(ctx context.Context, contextName, namespace, name string) (obj, error) {
	data, err := command(ctx, "kubectl", "--context", contextName, "-n", namespace, "get", "pod", name, "-o", "json")
	if err != nil {
		return nil, err
	}
	var pod obj
	if err := json.Unmarshal(data, &pod); err != nil || text(nested(pod, "metadata", "uid")) == "" {
		return nil, fmt.Errorf("Pod identity response is invalid")
	}
	return pod, nil
}

func getGCSChaosPods(ctx context.Context, contextName, namespace string, labels map[string]string) ([]any, error) {
	selector := make([]string, 0, len(labels))
	for key, value := range labels {
		selector = append(selector, key+"="+value)
	}
	sort.Strings(selector)
	data, err := command(ctx, "kubectl", "--context", contextName, "-n", namespace, "get", "pods", "-l", strings.Join(selector, ","), "-o", "json")
	if err != nil {
		return nil, err
	}
	var list obj
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("Pod list response is invalid")
	}
	items, ok := list["items"].([]any)
	if !ok {
		return nil, fmt.Errorf("Pod list has no items")
	}
	return items, nil
}

func getGCSChaosObject(ctx context.Context, contextName, namespace, name string) (obj, error) {
	data, err := command(ctx, "kubectl", "--context", contextName, "-n", namespace, "get", "networkchaos", name, "--ignore-not-found", "-o", "json")
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return obj{}, nil
	}
	var result obj
	if err := json.Unmarshal(data, &result); err != nil || text(nested(result, "metadata", "uid")) == "" {
		return nil, fmt.Errorf("NetworkChaos response is invalid")
	}
	return result, nil
}

func validateGCSChaosScope(namespace, runID, phase string) error {
	if !validGCSChaosPodName(namespace) || len(runID) != 8 || len(phase) == 0 || len(phase) > 20 {
		return fmt.Errorf("GCS NetworkChaos scope is invalid")
	}
	for _, r := range runID {
		if !(r >= 'a' && r <= 'f' || r >= '0' && r <= '9') {
			return fmt.Errorf("GCS NetworkChaos run ID must be eight lowercase hexadecimal characters")
		}
	}
	for _, r := range phase {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
			return fmt.Errorf("GCS NetworkChaos phase must be a lowercase DNS token")
		}
	}
	return nil
}

func validGCSChaosPodName(name string) bool {
	return len(name) > 0 && len(name) <= 63 && gcsChaosNamePart.MatchString(name)
}

func sortedGCSChaosPodNames(names []string, minimum int) ([]string, error) {
	if minimum < 1 || len(names) < minimum || len(names) > gcsChaosMaxPods {
		return nil, fmt.Errorf("GCS fault requires %d..%d explicit Pods", minimum, gcsChaosMaxPods)
	}
	sorted := append([]string(nil), names...)
	sort.Strings(sorted)
	for i, name := range sorted {
		if !validGCSChaosPodName(name) || (i > 0 && sorted[i-1] == name) {
			return nil, fmt.Errorf("GCS fault Pod names must be unique explicit DNS names")
		}
	}
	return sorted, nil
}

func gcsChaosPodBelongs(pod obj, runID, role string, identity gcsControlPodIdentity) bool {
	if text(nested(pod, "metadata", "name")) != identity.Name || text(nested(pod, "metadata", "uid")) != identity.UID || nested(pod, "metadata", "deletionTimestamp") != nil || !podReady(pod) ||
		(role != "runtime-agent" && nested(pod, "metadata", "labels", gcsChaosRunLabel) != runID) {
		return false
	}
	expectedOwnerKind := map[string]string{"runtime-agent": "DaemonSet", "operator": "ReplicaSet"}[role]
	ownerMatched := false
	for _, owner := range ownerRefs(pod) {
		if nested(owner, "controller") == true && nested(owner, "kind") == expectedOwnerKind && text(nested(owner, "uid")) == identity.OwnerUID {
			ownerMatched = true
			break
		}
	}
	if !ownerMatched {
		return false
	}
	switch role {
	case "runtime-agent":
		return strings.HasPrefix(identity.Name, "rhiza-runtime-agent-") && nested(pod, "metadata", "labels", "app.kubernetes.io/name") == "rhiza-runtime-agent"
	case "operator":
		return strings.HasPrefix(identity.Name, "rhiza-operator-") && nested(pod, "metadata", "labels", "app") == "rhiza-operator"
	default:
		return false
	}
}

func verifyGCSChaosControlOwner(ctx context.Context, contextName, namespace, role string, pod obj, identity gcsControlPodIdentity) error {
	var ownerKind string
	var ownerUID string
	for _, owner := range ownerRefs(pod) {
		if nested(owner, "controller") == true {
			ownerKind, ownerUID = text(nested(owner, "kind")), text(nested(owner, "uid"))
			break
		}
	}
	if role == "runtime-agent" {
		if ownerKind != "DaemonSet" || ownerUID != identity.OwnerUID {
			return fmt.Errorf("runtime-agent Pod %s DaemonSet UID changed", identity.Name)
		}
		data, err := command(ctx, "kubectl", "--context", contextName, "-n", namespace, "get", "daemonset", "rhiza-runtime-agent", "-o", "json")
		if err != nil {
			return err
		}
		var daemonSet obj
		if json.Unmarshal(data, &daemonSet) != nil || text(nested(daemonSet, "metadata", "uid")) != identity.OwnerUID {
			return fmt.Errorf("runtime-agent DaemonSet identity changed")
		}
		return nil
	}
	if role != "operator" || ownerKind != "ReplicaSet" || ownerUID != identity.OwnerUID || identity.ParentOwnerUID == "" {
		return fmt.Errorf("operator Pod %s ReplicaSet identity changed", identity.Name)
	}
	ownerName := ""
	for _, owner := range ownerRefs(pod) {
		if nested(owner, "controller") == true && nested(owner, "kind") == "ReplicaSet" {
			ownerName = text(nested(owner, "name"))
			break
		}
	}
	if !validGCSChaosPodName(ownerName) {
		return fmt.Errorf("operator Pod %s ReplicaSet owner name is invalid", identity.Name)
	}
	data, err := command(ctx, "kubectl", "--context", contextName, "-n", namespace, "get", "replicaset", ownerName, "-o", "json")
	if err != nil {
		return err
	}
	var replicaSet obj
	if json.Unmarshal(data, &replicaSet) != nil || text(nested(replicaSet, "metadata", "uid")) != identity.OwnerUID {
		return fmt.Errorf("operator ReplicaSet identity changed")
	}
	for _, owner := range ownerRefs(replicaSet) {
		if nested(owner, "controller") == true && nested(owner, "kind") == "Deployment" && text(nested(owner, "uid")) == identity.ParentOwnerUID {
			data, err := command(ctx, "kubectl", "--context", contextName, "-n", namespace, "get", "deployment", "rhiza-operator", "-o", "json")
			if err != nil {
				return err
			}
			var deployment obj
			if json.Unmarshal(data, &deployment) == nil && text(nested(deployment, "metadata", "uid")) == identity.ParentOwnerUID {
				return nil
			}
		}
	}
	return fmt.Errorf("operator Pod %s Deployment owner identity changed", identity.Name)
}

func deleteGCSNetworkChaosUID(ctx context.Context, contextName, namespace, name, uid string) error {
	if uid == "" || !validGCSChaosPodName(namespace) || !validGCSChaosPodName(name) {
		return fmt.Errorf("refusing NetworkChaos cleanup without exact name and UID")
	}
	dir, err := os.MkdirTemp("/tmp", "rhiza-chaos-proxy-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "proxy.sock")
	proxy := exec.CommandContext(ctx, "kubectl", "--context", contextName, "proxy", "--unix-socket", socket, "--api-prefix=/")
	proxy.Stdout, proxy.Stderr = io.Discard, io.Discard
	if err := proxy.Start(); err != nil {
		return fmt.Errorf("start private Kubernetes proxy: %w", err)
	}
	done := make(chan struct{})
	var proxyErr error
	go func() { proxyErr = proxy.Wait(); close(done) }()
	defer func() {
		if proxy.Process != nil {
			_ = proxy.Process.Kill()
		}
		<-done
	}()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	base := "http://localhost"
	readyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		request, _ := http.NewRequestWithContext(readyCtx, http.MethodGet, base+"/version", nil)
		response, requestErr := client.Do(request)
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				break
			}
		}
		select {
		case <-readyCtx.Done():
			return fmt.Errorf("private Kubernetes proxy is unavailable: %w", readyCtx.Err())
		case <-done:
			return fmt.Errorf("private Kubernetes proxy exited: %v", proxyErr)
		case <-time.After(50 * time.Millisecond):
		}
	}
	options, _ := json.Marshal(obj{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": obj{"uid": uid}})
	path := fmt.Sprintf("%s/apis/chaos-mesh.org/v1alpha1/namespaces/%s/networkchaos/%s", base, namespace, name)
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, path, bytes.NewReader(options))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("delete exact NetworkChaos UID: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusAccepted && response.StatusCode != http.StatusNotFound && response.StatusCode != http.StatusConflict {
		return fmt.Errorf("delete exact NetworkChaos UID returned HTTP %d", response.StatusCode)
	}
	return nil
}

func jsonSemanticEqual(a, b any) bool {
	left, leftErr := json.Marshal(a)
	right, rightErr := json.Marshal(b)
	return leftErr == nil && rightErr == nil && bytes.Equal(left, right)
}
