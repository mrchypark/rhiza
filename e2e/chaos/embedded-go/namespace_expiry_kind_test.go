package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	expiryKindContext = "kind-rhiza-anchor-chaos"
	expiryKindImage   = "rhiza-chaos:local"
)

func TestNamespaceExpiryDeletesOwnedNamespaceInKind(t *testing.T) {
	if os.Getenv("RHIZA_NAMESPACE_EXPIRY_KIND_TEST") != "1" {
		t.Skip("requires explicit disposable anchor Kind opt-in")
	}
	if os.Getenv("GITHUB_ACTIONS") != "true" {
		t.Fatal("refusing namespace expiry Kind test outside GitHub Actions")
	}
	if os.Getenv("RHIZA_NAMESPACE_EXPIRY_KIND_CONTEXT") != expiryKindContext ||
		os.Getenv("RHIZA_NAMESPACE_EXPIRY_KIND_IMAGE") != expiryKindImage {
		t.Fatal("refusing unexpected Kind context or qualification image")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	clusterFound := false
	for _, cluster := range strings.Fields(expiryCommand(t, ctx, nil, "kind", "get", "clusters")) {
		clusterFound = clusterFound || cluster == "rhiza-anchor-chaos"
	}
	if !clusterFound {
		t.Fatal("expected disposable anchor Kind cluster is absent")
	}
	node := strings.TrimSpace(expiryKubectl(t, ctx, nil, "get", "nodes", "-o", "jsonpath={.items[0].metadata.name}"))
	if node == "" {
		t.Fatal("owned Kind cluster has no schedulable node")
	}

	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	runID := hex.EncodeToString(nonce[:])
	namespace := "rhiza-expiry-" + runID
	uid := strings.TrimSpace(expiryKubectl(t, ctx, strings.NewReader(fmt.Sprintf(
		`{"apiVersion":"v1","kind":"Namespace","metadata":{"name":%q,"labels":{"chaos.rhiza.io/run":%q}}}`,
		namespace, runID)), "create", "-f", "-", "-o", "jsonpath={.metadata.uid}"))
	if uid == "" {
		t.Fatalf("created namespace %s without a UID", namespace)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if t.Failed() {
			for _, args := range [][]string{
				{"-n", namespace, "get", "job", "rhiza-namespace-expiry", "-o", "yaml"},
				{"-n", namespace, "get", "pods", "-l", "chaos.rhiza.io/owner=namespace-expiry", "-o", "wide"},
				{"-n", namespace, "get", "events", "--sort-by=.lastTimestamp"},
				{"-n", namespace, "logs", "-l", "chaos.rhiza.io/owner=namespace-expiry", "--all-containers=true", "--prefix"},
			} {
				if out, err := expiryKubectlOutput(cleanupCtx, nil, args...); len(out) > 0 || err != nil {
					t.Logf("expiry diagnostics (%s; %v):\n%s", strings.Join(args, " "), err, out)
				}
			}
		}
		if err := cleanupExpiryNamespace(cleanupCtx, namespace, runID, uid); err != nil {
			t.Errorf("clean up exact test namespace UID: %v", err)
		}
	})

	deadline := time.Now().Add(45 * time.Second).Unix()
	manifest := readContractFile(t, "../gcs-namespace-expiry.yaml.in")
	for _, pair := range [][2]string{
		{"__NAMESPACE__", namespace}, {"__NAMESPACE_UID__", uid}, {"__RUN_ID__", runID},
		{"__EXPIRY_DEADLINE__", fmt.Sprint(deadline)}, {"__NODE_A__", node}, {"__NODE_B__", node}, {"__NODE_C__", node},
		{"__KUBECTL_IMAGE__", expiryKindImage},
	} {
		manifest = strings.ReplaceAll(manifest, pair[0], pair[1])
	}
	if strings.Contains(manifest, "__") {
		t.Fatal("expiry manifest contains an unrendered placeholder")
	}
	expiryKubectl(t, ctx, strings.NewReader(manifest), "apply", "-f", "-")
	selector := "chaos.rhiza.io/owner=namespace-expiry"
	expiryKubectl(t, ctx, nil, "-n", namespace, "wait", "--for=condition=Ready", "pod", "-l", selector, "--timeout=90s")
	pod := strings.TrimSpace(expiryKubectl(t, ctx, nil, "-n", namespace, "get", "pods", "-l", selector,
		"-o", "jsonpath={.items[0].spec.nodeName}|{.items[0].spec.containers[0].image}"))
	if pod != node+"|"+expiryKindImage || time.Now().Unix() >= deadline {
		t.Fatalf("expiry Pod did not use the qualification image on the expected node before deadline: %q", pod)
	}
	owner := "{.metadata.ownerReferences[0].kind}/{.metadata.ownerReferences[0].name}/{.metadata.ownerReferences[0].uid}"
	for _, kind := range []string{"clusterrole", "clusterrolebinding"} {
		got := strings.TrimSpace(expiryKubectl(t, ctx, nil, "get", kind, namespace+"-namespace-expiry", "-o", "jsonpath="+owner))
		if got != "Namespace/"+namespace+"/"+uid {
			t.Fatalf("%s owner reference=%q, want the exact test Namespace UID", kind, got)
		}
	}
	serviceAccount := "system:serviceaccount:" + namespace + ":rhiza-namespace-expiry"
	if got := strings.TrimSpace(expiryKubectl(t, ctx, nil, "auth", "can-i", "delete", "namespaces/"+namespace,
		"--all-namespaces", "--as", serviceAccount)); got != "yes" {
		t.Fatalf("expiry ServiceAccount cannot delete its namespace: %q", got)
	}
	other := exec.CommandContext(ctx, "kubectl", "--context", expiryKindContext, "auth", "can-i", "delete",
		"namespaces/rhiza-expiry-outside-"+runID, "--all-namespaces", "--as", serviceAccount)
	if got, err := other.CombinedOutput(); err == nil || strings.TrimSpace(string(got)) != "no" {
		t.Fatalf("expiry ServiceAccount can delete outside its exact namespace scope: %q", got)
	}
	waitExpiryDeadline(t, ctx, namespace, runID, uid, time.Unix(deadline, 0))
	for _, resource := range []string{"namespace/" + namespace, "clusterrole/" + namespace + "-namespace-expiry",
		"clusterrolebinding/" + namespace + "-namespace-expiry"} {
		expiryWaitAbsent(t, ctx, resource)
	}
}

func expiryCommand(t *testing.T, ctx context.Context, stdin io.Reader, binary string, args ...string) string {
	t.Helper()
	out, err := expiryCommandOutput(ctx, stdin, binary, args...)
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", binary, strings.Join(args, " "), err, out)
	}
	return out
}

func expiryKubectl(t *testing.T, ctx context.Context, stdin io.Reader, args ...string) string {
	t.Helper()
	out, err := expiryKubectlOutput(ctx, stdin, args...)
	if err != nil {
		t.Fatalf("kubectl %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func expiryKubectlOutput(ctx context.Context, stdin io.Reader, args ...string) (string, error) {
	return expiryCommandOutput(ctx, stdin, "kubectl", append([]string{"--context", expiryKindContext}, args...)...)
}

func expiryCommandOutput(ctx context.Context, stdin io.Reader, binary string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Stdin = stdin
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func cleanupExpiryNamespace(ctx context.Context, namespace, runID, uid string) error {
	identity, err := expiryKubectlOutput(ctx, nil, "get", "namespace", namespace, "--ignore-not-found",
		"-o", "jsonpath={.metadata.uid}|{.metadata.resourceVersion}|{.metadata.labels.chaos\\.rhiza\\.io/run}")
	if err != nil {
		return fmt.Errorf("read namespace identity: %w", err)
	}
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return nil
	}
	parts := strings.Split(identity, "|")
	if len(parts) != 3 || parts[0] != uid || parts[1] == "" || parts[2] != runID {
		return fmt.Errorf("namespace identity changed; refusing cleanup")
	}
	body := fmt.Sprintf(`{"apiVersion":"v1","kind":"DeleteOptions","preconditions":{"uid":%q,"resourceVersion":%q}}`, uid, parts[1])
	_, err = expiryKubectlOutput(ctx, strings.NewReader(body), "delete", "--raw", "/api/v1/namespaces/"+namespace, "-f", "-")
	if err != nil {
		current, checkErr := expiryKubectlOutput(ctx, nil, "get", "namespace", namespace, "--ignore-not-found", "-o", "name")
		if checkErr == nil && strings.TrimSpace(current) == "" {
			return nil
		}
		return fmt.Errorf("request UID/resourceVersion-preconditioned delete: %w", err)
	}
	return nil
}

func waitExpiryDeadline(t *testing.T, ctx context.Context, namespace, runID, uid string, deadline time.Time) {
	t.Helper()
	for {
		started := time.Now()
		identity, err := expiryKubectlOutput(ctx, nil, "get", "namespace", namespace, "--ignore-not-found",
			"-o", "jsonpath={.metadata.uid}|{.metadata.labels.chaos\\.rhiza\\.io/run}|{.metadata.deletionTimestamp}")
		finished := time.Now()
		if err != nil {
			t.Fatalf("check namespace identity through expiry deadline: %v\n%s", err, identity)
		}
		identity = strings.TrimSpace(identity)
		if identity == "" {
			if started.Before(deadline) && finished.Before(deadline) {
				t.Fatal("expiry deleted the namespace before its fixed deadline")
			}
			return
		}
		parts := strings.Split(identity, "|")
		if len(parts) != 3 || parts[0] != uid || parts[1] != runID {
			t.Fatal("namespace UID or run label changed during the expiry wait")
		}
		if parts[2] != "" && finished.Before(deadline) {
			t.Fatal("namespace deletion started before its fixed expiry deadline")
		}
		if !finished.Before(deadline) {
			return
		}
		pause := time.Until(deadline)
		if pause > time.Second {
			pause = time.Second
		}
		time.Sleep(pause)
	}
}

func expiryWaitAbsent(t *testing.T, ctx context.Context, resource string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		out := strings.TrimSpace(expiryKubectl(t, ctx, nil, "get", resource, "--ignore-not-found", "-o", "name"))
		if out == "" {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("expiry did not remove owned %s", resource)
}
