// Command fencing-e2e drives a disposable Kind qualification of the real
// cooperative executor. Runtime inspection below is a fault-injector oracle;
// it is never supplied to the executor as a fencing proof.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/operator"
)

const namespace = "rhiza-fencing-e2e"
const kubeContext = "kind-rhiza-generic-fencing"
const kindNode = "rhiza-generic-fencing-control-plane"
const admin = "fencing-test-admin"

type obj = map[string]any

func main() {
	if len(os.Args) != 2 {
		fatal(fmt.Errorf("usage: fencing-e2e manifests|wait-source|run"))
	}
	if os.Args[1] == "manifests" {
		fatal(json.NewEncoder(os.Stdout).Encode(manifests()))
		return
	}
	if os.Args[1] == "wait-source" {
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer cancel()
		fatal(waitSource(ctx))
		return
	}
	if os.Args[1] != "run" {
		fatal(fmt.Errorf("unknown command"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	fatal(qualify(ctx))
}

func waitSource(ctx context.Context) error {
	return wait(ctx, "three source-generation voters Ready", func() (bool, error) {
		config, err := get(ctx, "configmap", "rhiza-config")
		if err != nil {
			return false, err
		}
		if nested(config, "data", "RHIZA_CLUSTER_ID") != "chaos-source" {
			return false, fmt.Errorf("source generation config differs")
		}
		sts, err := get(ctx, "statefulset", "rhiza")
		if err != nil {
			return false, err
		}
		uid := text(nested(sts, "metadata", "uid"))
		if uid == "" {
			return false, fmt.Errorf("source StatefulSet UID missing")
		}
		if nested(sts, "spec", "replicas") != float64(3) || nested(sts, "status", "readyReplicas") != float64(3) {
			return false, nil
		}
		pods, err := get(ctx, "pods", "")
		if err != nil {
			return false, err
		}
		items, _ := pods["items"].([]any)
		ready := map[string]bool{}
		for _, pod := range items {
			if nested(pod, "metadata", "deletionTimestamp") != nil || nested(pod, "metadata", "labels", "app.kubernetes.io/name") != "rhiza" {
				continue
			}
			owners, _ := nested(pod, "metadata", "ownerReferences").([]any)
			owned := false
			for _, owner := range owners {
				if nested(owner, "kind") == "StatefulSet" && nested(owner, "uid") == uid && nested(owner, "controller") == true {
					owned = true
				}
			}
			if !owned {
				continue
			}
			containers, _ := nested(pod, "spec", "containers").([]any)
			if len(containers) != 1 {
				return false, fmt.Errorf("source voter is not a dedicated container")
			}
			from, _ := nested(containers[0], "envFrom").([]any)
			sourceConfig := false
			for _, entry := range from {
				if nested(entry, "configMapRef", "name") == "rhiza-config" {
					sourceConfig = true
				}
			}
			if !sourceConfig {
				return false, fmt.Errorf("source voter generation config missing")
			}
			env, _ := nested(containers[0], "env").([]any)
			for _, entry := range env {
				if nested(entry, "name") == "RHIZA_CLUSTER_ID" && nested(entry, "value") != "chaos-source" {
					return false, fmt.Errorf("source voter generation overridden")
				}
			}
			name := text(nested(pod, "metadata", "name"))
			if name != "rhiza-0" && name != "rhiza-1" && name != "rhiza-2" {
				return false, fmt.Errorf("unexpected source voter Pod")
			}
			conditions, _ := nested(pod, "status", "conditions").([]any)
			for _, condition := range conditions {
				if nested(condition, "type") == "Ready" && nested(condition, "status") == "True" {
					ready[name] = true
				}
			}
		}
		return len(ready) == 3, nil
	})
}

func fatal(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func encoded(value any) []byte {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return data
}
func metadata(name string) obj { return obj{"name": name, "namespace": namespace} }
func resource(api, kind, name string, spec obj) obj {
	return obj{"apiVersion": api, "kind": kind, "metadata": metadata(name), "spec": spec}
}
func field(name, path string) obj {
	return obj{"name": name, "valueFrom": obj{"fieldRef": obj{"fieldPath": path}}}
}
func value(name, val string) obj { return obj{"name": name, "value": val} }
func selector(name string) obj   { return obj{"app.kubernetes.io/name": name} }

func manifests() obj {
	var members, voters []obj
	tokens := map[string]string{}
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("rhiza-%d", i)
		token := "fencing-peer-" + id
		tokens[id] = token
		members = append(members, obj{"node_id": id, "url": "http://" + id + ".rhiza-headless:8080", "peer_url": "quic://" + id + ".rhiza-headless:9090", "public_key": rhiza.PeerPublicKey("chaos-source", id, token)})
		voters = append(voters, obj{"nodeID": id, "pod": id})
	}
	config := obj{"RHIZA_CLUSTER_ID": "chaos-source", "RHIZA_BIND_ADDR": "0.0.0.0:8080", "RHIZA_PEER_ADDR": "0.0.0.0:9090", "RHIZA_DATA_DIR": "/data", "RHIZA_ENABLE_RECONFIGURATION": "true", "RHIZA_CHECKPOINT_INTERVAL": "0", "RHIZA_OBJSTORE_SYNC_INTERVAL": "1s", "RHIZA_OBJSTORE_GC_INTERVAL": "0", "RHIZA_OBJSTORE_PROVIDER": "s3", "RHIZA_OBJSTORE_PREFIX": "rhiza", "RHIZA_OBJSTORE_DURABILITY": "before-ack", "RHIZA_CLUSTER_MEMBERS": string(encoded(members))}
	configMap := obj{"apiVersion": "v1", "kind": "ConfigMap", "metadata": metadata("rhiza-config"), "data": config}
	credentials := obj{"apiVersion": "v1", "kind": "Secret", "metadata": metadata("rhiza-peer-credentials"), "stringData": obj{"RHIZA_ADMIN_TOKEN": admin, "RHIZA_PEER_TOKENS": string(encoded(tokens))}}
	storeCredentials := obj{"apiVersion": "v1", "kind": "Secret", "metadata": metadata("rhiza-object-store"), "stringData": obj{"RHIZA_OBJSTORE_ENDPOINT": "rhiza-minio:9000", "RHIZA_OBJSTORE_BUCKET": "rhiza", "RHIZA_OBJSTORE_INSECURE": "true", "RHIZA_OBJSTORE_ACCESS_KEY": "fencing-local", "RHIZA_OBJSTORE_SECRET_KEY": "fencing-local-secret", "RHIZA_OBJSTORE_REGION": "us-east-1"}}
	store := resource("apps/v1", "Deployment", "rhiza-minio", obj{"replicas": 1, "selector": obj{"matchLabels": selector("rhiza-minio")}, "template": obj{"metadata": obj{"labels": selector("rhiza-minio")}, "spec": obj{"containers": []obj{{"name": "minio", "image": "ghcr.io/versity/versitygw@sha256:30292fc2eeacc67a36993b01f7a7a5e3361a19cced0e80c1d71cfa2a4b0a2499", "args": []string{"--port", ":9000", "--health", "/_/health", "posix", "/data"}, "env": []obj{value("ROOT_ACCESS_KEY", "fencing-local"), value("ROOT_SECRET_KEY", "fencing-local-secret")}, "ports": []obj{{"name": "s3", "containerPort": 9000}}, "readinessProbe": obj{"httpGet": obj{"path": "/_/health", "port": "s3"}}, "volumeMounts": []obj{{"name": "data", "mountPath": "/data"}}}}, "volumes": []obj{{"name": "data", "emptyDir": obj{}}}}}})
	storeService := resource("v1", "Service", "rhiza-minio", obj{"selector": selector("rhiza-minio"), "ports": []obj{{"port": 9000, "targetPort": "s3"}}})
	bucketJob := resource("batch/v1", "Job", "rhiza-create-bucket", obj{"backoffLimit": 0, "activeDeadlineSeconds": 120, "template": obj{"spec": obj{"restartPolicy": "Never", "containers": []obj{{"name": "create", "image": "amazon/aws-cli@sha256:83f8ffe939569070c5b66d22231862ab78718766d9d8e4c44ca84dd0be5569a5", "args": []string{"--endpoint-url=http://rhiza-minio:9000", "s3api", "create-bucket", "--bucket", "rhiza"}, "env": []obj{value("AWS_ACCESS_KEY_ID", "fencing-local"), value("AWS_SECRET_ACCESS_KEY", "fencing-local-secret"), value("AWS_DEFAULT_REGION", "us-east-1"), value("AWS_EC2_METADATA_DISABLED", "true")}}}}}})
	headless := resource("v1", "Service", "rhiza-headless", obj{"clusterIP": "None", "publishNotReadyAddresses": true, "selector": selector("rhiza"), "ports": []obj{{"name": "http", "port": 8080, "targetPort": "http"}, {"name": "peer-quic", "port": 9090, "targetPort": "peer-quic", "protocol": "UDP"}}})
	envFrom := []obj{{"configMapRef": obj{"name": "rhiza-config"}}, {"secretRef": obj{"name": "rhiza-peer-credentials"}}, {"secretRef": obj{"name": "rhiza-object-store"}}}
	sts := resource("apps/v1", "StatefulSet", "rhiza", obj{"replicas": 0, "serviceName": "rhiza-headless", "podManagementPolicy": "Parallel", "updateStrategy": obj{"type": "OnDelete"}, "selector": obj{"matchLabels": selector("rhiza")}, "template": obj{"metadata": obj{"labels": selector("rhiza")}, "spec": obj{"securityContext": obj{"runAsNonRoot": true, "fsGroup": 65532}, "containers": []obj{{"name": "rhiza", "image": "rhiza-fencing-host:local", "envFrom": envFrom, "env": []obj{field("RHIZA_NODE_ID", "metadata.name")}, "ports": []obj{{"name": "http", "containerPort": 8080}, {"name": "peer-quic", "containerPort": 9090, "protocol": "UDP"}}, "volumeMounts": []obj{{"name": "data", "mountPath": "/data"}}, "readinessProbe": obj{"httpGet": obj{"path": "/ready", "port": "http"}, "periodSeconds": 2}, "resources": obj{"requests": obj{"cpu": "250m", "memory": "256Mi"}, "limits": obj{"memory": "2Gi"}}}}, "volumes": []obj{{"name": "data", "emptyDir": obj{}}}}}})
	opEnv := []obj{field("POD_NAMESPACE", "metadata.namespace"), value("RHIZA_AUTOMATIC_RECOVERY", "true"), value("RHIZA_FENCER_BACKEND", "kubernetes")}
	op := resource("apps/v1", "Deployment", "rhiza-operator", obj{"replicas": 0, "selector": obj{"matchLabels": selector("rhiza-operator")}, "template": obj{"metadata": obj{"labels": selector("rhiza-operator")}, "spec": obj{"serviceAccountName": "rhiza-operator", "securityContext": obj{"runAsNonRoot": true, "fsGroup": 65532}, "containers": []obj{{"name": "operator", "image": "rhiza-fencing-operator:local", "args": []string{"--namespace=$(POD_NAMESPACE)", "--poll-interval=1s"}, "env": opEnv, "envFrom": []obj{envFrom[0], envFrom[2]}, "volumeMounts": []obj{{"name": "tmp", "mountPath": "/tmp"}}}}, "volumes": []obj{{"name": "tmp", "emptyDir": obj{}}}}}})
	binding := resource("rhiza.mrchypark.dev/v1alpha1", "RhizaCluster", "rhiza", obj{"logicalID": "fencing-e2e", "statefulSet": "rhiza", "container": "rhiza", "adoptClusterID": "chaos-source", "automatic": true, "voterPods": voters, "policy": obj{"failureGraceSeconds": 15, "cooldownSeconds": 3, "maxRecoveriesPerDay": 3, "allowDataLoss": false}})
	bucketJob["spec"].(obj)["suspend"] = true
	return obj{"apiVersion": "v1", "kind": "List", "items": []obj{configMap, credentials, storeCredentials, store, storeService, bucketJob, headless, sts, op, binding}}
}

func command(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	data, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%s command failed: %w: %s", name, err, data)
	}
	return data, nil
}
func kubectl(ctx context.Context, args ...string) ([]byte, error) {
	return command(ctx, "kubectl", append([]string{"--context", kubeContext, "-n", namespace}, args...)...)
}
func get(ctx context.Context, kind, name string) (obj, error) {
	args := []string{"get", kind}
	if name != "" {
		args = append(args, name)
	}
	data, err := kubectl(ctx, append(args, "-o", "json")...)
	if err != nil {
		return nil, err
	}
	var out obj
	err = json.Unmarshal(data, &out)
	return out, err
}
func nested(value any, keys ...string) any {
	for _, key := range keys {
		m, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		value = m[key]
	}
	return value
}
func text(value any) string { s, _ := value.(string); return s }
func wait(ctx context.Context, description string, check func() (bool, error)) error {
	for {
		ok, err := check()
		if err != nil {
			return fmt.Errorf("%s: %w", description, err)
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", description, ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

type forward struct {
	cmd  *exec.Cmd
	port int
	done chan error
	log  *os.File
}

func (f *forward) close() { _ = f.cmd.Process.Kill(); <-f.done; _ = f.log.Close() }
func startForward(ctx context.Context, pod, out string) (*forward, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	log, err := os.CreateTemp(out, pod+"-portforward-*.log")
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, "kubectl", "--context", kubeContext, "-n", namespace, "port-forward", "pod/"+pod, fmt.Sprintf("%d:8080", port), "--address=127.0.0.1")
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		_ = log.Close()
		return nil, err
	}
	f := &forward{cmd: cmd, port: port, done: make(chan error, 1), log: log}
	go func() { f.done <- cmd.Wait() }()
	ready, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = wait(ready, "port-forward "+pod, func() (bool, error) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		if err != nil {
			return false, nil
		}
		_ = conn.Close()
		return true, nil
	})
	if err != nil {
		f.close()
		return nil, err
	}
	return f, nil
}
func call(ctx context.Context, f *forward, path string, body any, token string) (obj, error) {
	method := http.MethodGet
	var reader io.Reader
	if body != nil {
		method = http.MethodPost
		reader = bytes.NewReader(encoded(body))
	}
	req, err := http.NewRequestWithContext(ctx, method, fmt.Sprintf("http://127.0.0.1:%d%s", f.port, path), reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s returned HTTP %d", path, resp.StatusCode)
	}
	var value obj
	err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&value)
	if err == nil && text(value["error_code"]) != "" {
		return nil, fmt.Errorf("%s application error %s", path, text(value["error_code"]))
	}
	return value, err
}

func qualify(ctx context.Context) error {
	out := os.Getenv("FENCING_E2E_OUT")
	if out == "" {
		out = "generic-fencing-results"
	}
	if err := os.MkdirAll(out, 0700); err != nil {
		return err
	}
	events, err := os.Create(filepath.Join(out, "events.jsonl"))
	if err != nil {
		return err
	}
	defer events.Close()
	event := func(kind string, data obj) {
		data["event"] = kind
		data["time"] = time.Now().UTC()
		_ = json.NewEncoder(events).Encode(data)
	}
	defer func() {
		collectCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, kind := range []string{"pods", "rhizafences", "rhizaclusters", "rhizarecoveries", "configmaps", "events"} {
			if data, err := kubectl(collectCtx, "get", kind, "-o", "json"); err == nil {
				_ = os.WriteFile(filepath.Join(out, kind+".json"), data, 0600)
			}
		}
	}()
	if err := wait(ctx, "healthy source", func() (bool, error) {
		c, err := get(ctx, "rhizacluster", "rhiza")
		if err != nil {
			return false, err
		}
		return nested(c, "status", "phase") == "Healthy", nil
	}); err != nil {
		return err
	}
	sts, err := get(ctx, "statefulset", "rhiza")
	if err != nil {
		return err
	}
	binding, err := get(ctx, "rhizacluster", "rhiza")
	if err != nil {
		return err
	}
	stsUID, bindingUID := text(nested(sts, "metadata", "uid")), text(nested(binding, "metadata", "uid"))
	if stsUID == "" || bindingUID == "" {
		return fmt.Errorf("missing source workload/binding UID")
	}
	if err := verifyImages(ctx, out, "source"); err != nil {
		return err
	}
	var oldUIDs []string
	f, err := startForward(ctx, "rhiza-0", out)
	if err != nil {
		return err
	}
	defer f.close()
	if _, err = call(ctx, f, "/sql/execute", obj{"request_id": "fencing-create", "sql": "CREATE TABLE fencing_items (id INTEGER PRIMARY KEY, value TEXT)"}, admin); err != nil {
		return err
	}
	if _, err = call(ctx, f, "/sql/execute", obj{"request_id": "fencing-ack", "sql": "INSERT INTO fencing_items VALUES (1, 'acknowledged')"}, admin); err != nil {
		return err
	}
	event("source_acknowledged", obj{"rows": 1})
	incarnations := map[string]string{}
	for i := 0; i < 3; i++ {
		pod := fmt.Sprintf("rhiza-%d", i)
		p, err := get(ctx, "pod", pod)
		if err != nil {
			return err
		}
		incarnations[pod] = incarnationSnapshot(p)
	}
	if _, err = kubectl(ctx, "scale", "deployment", "rhiza-fencer", "--replicas=0"); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
	}
	if _, err = kubectl(ctx, "scale", "deployment", "rhiza-fencer", "--replicas=1"); err != nil {
		return err
	}
	if _, err = kubectl(ctx, "rollout", "status", "deployment/rhiza-fencer", "--timeout=120s"); err != nil {
		return err
	}
	if _, err = call(ctx, f, "/sql/query", obj{"sql": "SELECT id FROM fencing_items", "consistency": "linearizable"}, admin); err != nil {
		return fmt.Errorf("authority restart stopped healthy engine: %w", err)
	}
	for pod, before := range incarnations {
		p, err := get(ctx, "pod", pod)
		if err != nil {
			return err
		}
		after := incarnationSnapshot(p)
		if before != after {
			return fmt.Errorf("source incarnation changed during authority restart: %s", pod)
		}
	}
	event("authority_restart_preserved_incarnations", obj{"voters": 3})
	if err := verifyImages(ctx, out, "authority-restarted"); err != nil {
		return err
	}
	for i := 0; i < 3; i++ {
		pod := fmt.Sprintf("rhiza-%d", i)
		p, err := get(ctx, "pod", pod)
		if err != nil {
			return err
		}
		oldUIDs = append(oldUIDs, text(nested(p, "metadata", "uid")))
		statuses, _ := nested(p, "status", "containerStatuses").([]any)
		if len(statuses) != 1 {
			return fmt.Errorf("expected dedicated single container")
		}
		cid := strings.TrimPrefix(text(nested(statuses[0], "containerID")), "containerd://")
		data, err := command(ctx, "docker", "exec", kindNode, "crictl", "inspect", cid)
		if err != nil {
			return err
		}
		var inspect obj
		if err = json.Unmarshal(data, &inspect); err != nil {
			return err
		}
		pid := fmt.Sprint(nested(inspect, "info", "pid"))
		if pid == "<nil>" || pid == "0" {
			return fmt.Errorf("missing runtime PID")
		}
		if _, err = command(ctx, "docker", "exec", kindNode, "nsenter", "-t", pid, "-n", "iptables", "-I", "INPUT", "-p", "udp", "--dport", "9090", "-j", "DROP"); err != nil {
			return err
		}
		event("peer_partition", obj{"pod": pod, "uid": oldUIDs[len(oldUIDs)-1], "container": cid})
	}
	if err := wait(ctx, "generic generation fence proof", func() (bool, error) {
		list, err := get(ctx, "rhizafences", "")
		if err != nil {
			return false, err
		}
		items, _ := list["items"].([]any)
		for _, item := range items {
			if nested(item, "spec", "scope") != "Generation" || nested(item, "status", "proof") == nil {
				continue
			}
			return true, verifyGenerationProof(ctx, item, stsUID, bindingUID, oldUIDs, out)
		}
		return false, nil
	}); err != nil {
		return err
	}
	event("generic_generation_proof", obj{"oldUIDs": oldUIDs})
	if err := wait(ctx, "new generation healthy", func() (bool, error) {
		c, err := get(ctx, "rhizacluster", "rhiza")
		if err != nil {
			return false, err
		}
		return nested(c, "status", "phase") == "Healthy" && nested(c, "status", "activeClusterID") != "chaos-source" && text(nested(c, "status", "activeClusterID")) != "", nil
	}); err != nil {
		return err
	}
	if err := verifyImages(ctx, out, "recovered"); err != nil {
		return err
	}
	newForward, err := startForward(ctx, "rhiza-0", out)
	if err != nil {
		return err
	}
	defer newForward.close()
	secret, err := get(ctx, "statefulset", "rhiza")
	if err != nil {
		return err
	}
	containers, _ := nested(secret, "spec", "template", "spec", "containers").([]any)
	if len(containers) != 1 {
		return fmt.Errorf("unexpected target container count")
	}
	env, _ := nested(containers[0], "env").([]any)
	secretName := ""
	for _, v := range env {
		if nested(v, "name") == "RHIZA_ADMIN_TOKEN" {
			secretName = text(nested(v, "valueFrom", "secretKeyRef", "name"))
		}
	}
	if secretName == "" {
		return fmt.Errorf("target admin Secret missing")
	}
	secret, err = get(ctx, "secret", secretName)
	if err != nil {
		return err
	}
	raw, err := base64.StdEncoding.DecodeString(text(nested(secret, "data", "admin")))
	if err != nil {
		return err
	}
	targetAdmin := string(raw)
	rows, err := call(ctx, newForward, "/sql/query", obj{"sql": "SELECT id,value FROM fencing_items ORDER BY id", "consistency": "linearizable"}, targetAdmin)
	if err != nil {
		return err
	}
	values, _ := rows["rows"].([]any)
	if len(values) != 1 {
		return fmt.Errorf("ACK row count differs")
	}
	row, _ := values[0].([]any)
	if len(row) != 2 || row[0] != float64(1) || row[1] != "acknowledged" {
		return fmt.Errorf("ACK row differs")
	}
	if _, err = call(ctx, newForward, "/sql/execute", obj{"request_id": "fencing-new-generation-write", "sql": "INSERT INTO fencing_items VALUES (2, 'recovered')"}, targetAdmin); err != nil {
		return err
	}
	readback, err := call(ctx, newForward, "/sql/query", obj{"sql": "SELECT id,value FROM fencing_items ORDER BY id", "consistency": "linearizable"}, targetAdmin)
	if err != nil {
		return err
	}
	if string(encoded(readback["rows"])) != `[[1,"acknowledged"],[2,"recovered"]]` {
		return fmt.Errorf("new-generation linearizable readback differs")
	}
	event("new_generation_linearizable_readback", obj{"rows": readback["rows"], "appliedSlot": readback["applied_slot"], "consensusTip": readback["consensus_tip"]})
	data, err := command(ctx, "docker", "exec", kindNode, "crictl", "ps", "-a", "-o", "json")
	if err != nil {
		return err
	}
	var runtime obj
	if err = json.Unmarshal(data, &runtime); err != nil {
		return err
	}
	runtimes, _ := runtime["containers"].([]any)
	for _, container := range runtimes {
		if nested(container, "state") != "CONTAINER_RUNNING" {
			continue
		}
		uid := text(nested(container, "labels", "io.kubernetes.pod.uid"))
		for _, old := range oldUIDs {
			if uid == old {
				return fmt.Errorf("old runtime remains live after proof")
			}
		}
	}
	event("PASS", obj{"sourceACKRows": 1, "newGenerationWrite": true, "oldRuntimesGone": true, "executor": "generic-cooperative"})
	return nil
}

func digest(value string) string {
	value = strings.TrimPrefix(strings.TrimPrefix(value, "docker-pullable://"), "containerd://")
	if _, d, ok := strings.Cut(value, "@"); ok {
		return d
	}
	return value
}

func incarnationSnapshot(pod obj) string {
	statuses, _ := nested(pod, "status", "containerStatuses").([]any)
	var processes []obj
	for _, status := range statuses {
		processes = append(processes, obj{"containerID": nested(status, "containerID"), "restartCount": nested(status, "restartCount")})
	}
	return string(encoded(obj{"uid": nested(pod, "metadata", "uid"), "processes": processes}))
}

func verifyImages(ctx context.Context, out, phase string) error {
	pods, err := get(ctx, "pods", "")
	if err != nil {
		return err
	}
	items, _ := pods["items"].([]any)
	counts := map[string]int{}
	var evidence []obj
	for _, pod := range items {
		if nested(pod, "metadata", "deletionTimestamp") != nil || nested(pod, "status", "phase") != "Running" {
			continue
		}
		role := text(nested(pod, "metadata", "labels", "app.kubernetes.io/name"))
		image := ""
		switch role {
		case "rhiza":
			image = "rhiza-fencing-host:local"
		case "rhiza-operator", "rhiza-fencer":
			image = "rhiza-fencing-operator:local"
		default:
			continue
		}
		statuses, _ := nested(pod, "status", "containerStatuses").([]any)
		if len(statuses) != 1 {
			return fmt.Errorf("%s has no single running image", role)
		}
		podImage := text(nested(statuses[0], "imageID"))
		if podImage == "" {
			return fmt.Errorf("%s imageID missing", role)
		}
		local, err := command(ctx, "docker", "image", "inspect", image)
		if err != nil {
			return err
		}
		var built []obj
		if err = json.Unmarshal(local, &built); err != nil || len(built) != 1 {
			return fmt.Errorf("invalid built image inspection")
		}
		lookup := strings.TrimPrefix(strings.TrimPrefix(podImage, "docker-pullable://"), "containerd://")
		criData, err := command(ctx, "docker", "exec", kindNode, "crictl", "inspecti", "-o", "json", lookup)
		if err != nil {
			return err
		}
		var cri obj
		if err = json.Unmarshal(criData, &cri); err != nil {
			return err
		}
		configID := text(nested(cri, "status", "id"))
		builtID := text(built[0]["Id"])
		repos, _ := nested(cri, "status", "repoDigests").([]any)
		evidence = append(evidence, obj{"pod": nested(pod, "metadata", "name"), "uid": nested(pod, "metadata", "uid"), "role": role, "podImageID": podImage, "builtConfigID": builtID, "criConfigID": configID, "criRepoDigests": repos})
		if err := os.WriteFile(filepath.Join(out, phase+"-image-evidence.json"), encoded(evidence), 0600); err != nil {
			return err
		}
		if configID == "" || digest(configID) != digest(builtID) {
			return fmt.Errorf("%s running image configuration differs from built image", role)
		}
		matched := digest(podImage) == digest(configID)
		for _, repo := range repos {
			matched = matched || digest(podImage) == digest(text(repo))
		}
		if !matched {
			return fmt.Errorf("%s Pod image digest does not resolve to inspected configuration", role)
		}
		counts[role]++
	}
	if counts["rhiza"] != 3 || counts["rhiza-operator"] != 1 || counts["rhiza-fencer"] != 1 {
		return fmt.Errorf("qualification image inventory incomplete: %v", counts)
	}
	return os.WriteFile(filepath.Join(out, phase+"-image-evidence.json"), encoded(evidence), 0600)
}

func verifyGenerationProof(ctx context.Context, item any, stsUID, bindingUID string, oldUIDs []string, out string) error {
	var request operator.FenceRequest
	var proof operator.FenceProof
	if err := json.Unmarshal(encoded(nested(item, "spec")), &request); err != nil {
		return err
	}
	if err := json.Unmarshal(encoded(nested(item, "status", "proof")), &proof); err != nil {
		return err
	}
	if request.SourceClusterID != "chaos-source" || request.StatefulSet != "rhiza" || request.StatefulSetUID != stsUID || request.BindingUID != bindingUID || request.Namespace != namespace || request.LogicalID != "fencing-e2e" || request.OperationID == "" || request.Scope != operator.ScopeGeneration {
		return fmt.Errorf("generation fence request identity differs")
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(encoded(request)))
	if proof.OperationID != request.OperationID || proof.SourceClusterID != request.SourceClusterID || proof.BindingUID != request.BindingUID || proof.RequestHash != hash || proof.ProofID == "" || !proof.ProcessesTerminated || !proof.RecreationBlocked || !proof.StorageQuiesced {
		return fmt.Errorf("generation proof binding differs")
	}
	maps, err := get(ctx, "configmaps", "")
	if err != nil {
		return err
	}
	items, _ := maps["items"].([]any)
	for _, item := range items {
		data := text(nested(item, "data", "ledger.json"))
		if data == "" {
			continue
		}
		var state struct {
			StatefulSetUID string         `json:"statefulSetUID"`
			Ledger         fencing.Ledger `json:"ledger"`
		}
		if err := json.Unmarshal([]byte(data), &state); err != nil {
			return err
		}
		if state.StatefulSetUID != stsUID || state.Ledger.SourceClusterID != request.SourceClusterID {
			continue
		}
		if !state.Ledger.Complete(request.OperationID, hash, oldUIDs) {
			return fmt.Errorf("generation ledger has no clean drain/exit for all source incarnations")
		}
		for _, run := range state.Ledger.Runs {
			for _, uid := range oldUIDs {
				if run.Identity.PodUID == uid && (run.Stop == nil || run.Stop.OperationID != request.OperationID || run.Stop.RequestHash != hash) {
					return fmt.Errorf("source incarnation drained for a different operation")
				}
			}
		}
		if proof.ProofID != "cooperative-"+fmt.Sprintf("%x", sha256.Sum256(encoded(state))) {
			return fmt.Errorf("proof ID does not bind observed durable ledger")
		}
		return os.WriteFile(filepath.Join(out, "validated-generation-proof.json"), encoded(obj{"request": request, "proof": proof, "ledger": state, "oldUIDs": oldUIDs}), 0600)
	}
	return fmt.Errorf("generation proof has no matching durable ledger")
}
