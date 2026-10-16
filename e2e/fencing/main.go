// Command fencing-e2e drives a disposable Kind qualification of the real
// cooperative executor. Runtime inspection below is a fault-injector oracle;
// it is never supplied to the executor as a fencing proof.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
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
	if os.Args[1] == "safe-artifacts" {
		fatal(safeArtifacts(os.Getenv("FENCING_E2E_OUT")))
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
	workload := nested(sts, "spec", "template", "spec").(obj)
	workload["hostPID"], workload["shareProcessNamespace"] = false, false
	application := workload["containers"].([]obj)[0]
	application["securityContext"] = obj{"allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true, "capabilities": obj{"drop": []string{"ALL"}}}
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

func qualify(ctx context.Context) (resultErr error) {
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
	var observationFaults []*observationFault
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, fault := range observationFaults {
			cleanupErr := fault.remove(cleanupCtx)
			resultErr = errors.Join(resultErr, cleanupErr)
			event("operator_observation_fault_cleanup", obj{"fault": fault, "error": fmt.Sprint(cleanupErr)})
		}
	}()
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
	if err := verifyScratch(sts); err != nil {
		return err
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
	var initialUIDs []string
	for i := 0; i < 3; i++ {
		pod := fmt.Sprintf("rhiza-%d", i)
		p, err := get(ctx, "pod", pod)
		if err != nil {
			return err
		}
		incarnations[pod] = incarnationSnapshot(p)
		initialUIDs = append(initialUIDs, text(nested(p, "metadata", "uid")))
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
	onlineCtx, onlineCancel := context.WithTimeout(ctx, 6*time.Minute)
	current, err := qualifyOnline(onlineCtx, f, stsUID, bindingUID, out, event, &observationFaults)
	onlineCancel()
	if err != nil {
		return fmt.Errorf("online voter qualification: %w", err)
	}
	for _, target := range current {
		oldUIDs = append(oldUIDs, target.PodUID)
		partition, err := partitionPeer(ctx, target)
		if err != nil {
			return err
		}
		event("generation_peer_partition", partition)
		fault, err := blockOperatorObservation(ctx, target)
		if fault != nil {
			observationFaults = append(observationFaults, fault)
		}
		if err != nil {
			return err
		}
		event("generation_operator_observation_fault", obj{"fault": fault})
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
			return true, verifyGenerationProof(ctx, item, stsUID, bindingUID, oldUIDs, out, current...)
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
	if len(values) != 3 {
		return fmt.Errorf("ACK row count differs")
	}
	if string(encoded(values)) != `[[1,"acknowledged"],[2,"quorum-survived"],[3,"learner-promoted"]]` {
		return fmt.Errorf("ACK row differs")
	}
	if _, err = call(ctx, newForward, "/sql/execute", obj{"request_id": "fencing-new-generation-write", "sql": "INSERT INTO fencing_items VALUES (4, 'recovered')"}, targetAdmin); err != nil {
		return err
	}
	readback, err := call(ctx, newForward, "/sql/query", obj{"sql": "SELECT id,value FROM fencing_items ORDER BY id", "consistency": "linearizable"}, targetAdmin)
	if err != nil {
		return err
	}
	if string(encoded(readback["rows"])) != `[[1,"acknowledged"],[2,"quorum-survived"],[3,"learner-promoted"],[4,"recovered"]]` {
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
		for _, old := range append(append([]string(nil), oldUIDs...), initialUIDs...) {
			if uid == old {
				return fmt.Errorf("old runtime remains live after proof")
			}
		}
	}
	event("PASS", obj{"sourceACKRows": 3, "onlineVoterReplacement": true, "newGenerationWrite": true, "oldRuntimesGone": true, "executor": "generic-cooperative"})
	return nil
}

func verifyScratch(sts obj) error {
	spec := nested(sts, "spec", "template", "spec")
	containers, _ := nested(spec, "containers").([]any)
	if len(containers) != 1 || nested(containers[0], "securityContext", "readOnlyRootFilesystem") != true || nested(containers[0], "securityContext", "allowPrivilegeEscalation") != false || !disabledPodOption(spec, "hostPID") || !disabledPodOption(spec, "shareProcessNamespace") {
		return fmt.Errorf("applied supervised workload security differs")
	}
	mounts, _ := nested(containers[0], "volumeMounts").([]any)
	volumes, _ := nested(spec, "volumes").([]any)
	for _, mount := range mounts {
		if nested(mount, "mountPath") != "/tmp" || nested(mount, "readOnly") == true {
			continue
		}
		for _, volume := range volumes {
			if nested(volume, "name") == nested(mount, "name") && nested(volume, "emptyDir") != nil {
				return nil
			}
		}
	}
	return fmt.Errorf("applied checkpoint restore lacks writable /tmp emptyDir")
}

func disabledPodOption(spec any, key string) bool {
	fields, ok := spec.(map[string]any)
	if !ok {
		return false
	}
	value, present := fields[key]
	// Kubernetes omits default-false Pod fields when serializing API objects.
	return !present || value == false
}

func readMembership(ctx context.Context, f *forward) (network.MembershipStatus, error) {
	data, err := call(ctx, f, "/membership/status", nil, admin)
	var status network.MembershipStatus
	if err == nil {
		err = json.Unmarshal(encoded(data), &status)
	}
	return status, err
}

func partitionPeer(ctx context.Context, target operator.FenceTarget) (obj, error) {
	pod, err := get(ctx, "pod", target.Pod)
	if err != nil {
		return nil, err
	}
	if text(nested(pod, "metadata", "uid")) != target.PodUID || target.PodUID == "" || target.NodeID == "" || target.WALIdentity == "" {
		return nil, fmt.Errorf("partition target incarnation changed")
	}
	statuses, _ := nested(pod, "status", "containerStatuses").([]any)
	if len(statuses) != 1 {
		return nil, fmt.Errorf("partition requires dedicated single container")
	}
	cid := strings.TrimPrefix(text(nested(statuses[0], "containerID")), "containerd://")
	data, err := command(ctx, "docker", "exec", kindNode, "crictl", "inspect", "-o", "json", cid)
	if err != nil {
		return nil, err
	}
	var runtime obj
	if err := json.Unmarshal(data, &runtime); err != nil {
		return nil, err
	}
	pid := fmt.Sprint(nested(runtime, "info", "pid"))
	if pid == "<nil>" || pid == "0" || nested(runtime, "status", "labels", "io.kubernetes.pod.uid") != target.PodUID || nested(runtime, "status", "id") != cid || nested(runtime, "status", "state") != "CONTAINER_RUNNING" {
		return nil, fmt.Errorf("partition runtime does not bind live target")
	}
	if _, err := command(ctx, "docker", "exec", kindNode, "nsenter", "-t", pid, "-n", "iptables", "-I", "INPUT", "-p", "udp", "--dport", "9090", "-j", "DROP"); err != nil {
		return nil, err
	}
	return obj{"target": target, "containerID": cid, "imageID": nested(statuses[0], "imageID"), "pid": pid}, nil
}

type observationFault struct {
	OperatorPod string               `json:"operatorPod"`
	OperatorUID string               `json:"operatorUID"`
	OperatorIP  string               `json:"operatorIP"`
	ContainerID string               `json:"containerID"`
	PID         string               `json:"pid"`
	Target      operator.FenceTarget `json:"target"`
	TargetIP    string               `json:"targetIP"`
	Rule        []string             `json:"rule"`
	Removed     bool                 `json:"removed"`
}

func observationRule(source, destination, uid string) ([]string, error) {
	src, dst := net.ParseIP(source), net.ParseIP(destination)
	if src == nil || dst == nil || src.To4() == nil || dst.To4() == nil || !src.IsGlobalUnicast() || !dst.IsGlobalUnicast() || src.Equal(dst) || uid == "" || strings.ContainsAny(uid, " \t\n") {
		return nil, fmt.Errorf("observation fault requires distinct exact IPv4 Pod addresses and target UID")
	}
	return []string{"OUTPUT", "-s", src.String(), "-d", dst.String(), "-p", "tcp", "--dport", "8080", "-m", "comment", "--comment", "rhiza-fencing-e2e-" + uid, "-j", "DROP"}, nil
}

func blockOperatorObservation(ctx context.Context, target operator.FenceTarget) (*observationFault, error) {
	targetPod, err := get(ctx, "pod", target.Pod)
	if err != nil {
		return nil, err
	}
	if nested(targetPod, "metadata", "uid") != target.PodUID {
		return nil, fmt.Errorf("observation target Pod incarnation changed")
	}
	list, err := get(ctx, "pods", "")
	if err != nil {
		return nil, err
	}
	items, _ := list["items"].([]any)
	var operatorPod any
	for _, pod := range items {
		if nested(pod, "metadata", "labels", "app.kubernetes.io/name") == "rhiza-operator" && nested(pod, "metadata", "deletionTimestamp") == nil && nested(pod, "status", "phase") == "Running" {
			if operatorPod != nil {
				return nil, fmt.Errorf("observation fault requires one exact operator Pod")
			}
			operatorPod = pod
		}
	}
	statuses, _ := nested(operatorPod, "status", "containerStatuses").([]any)
	if len(statuses) != 1 {
		return nil, fmt.Errorf("observation fault requires dedicated operator container")
	}
	fault := &observationFault{OperatorPod: text(nested(operatorPod, "metadata", "name")), OperatorUID: text(nested(operatorPod, "metadata", "uid")), OperatorIP: text(nested(operatorPod, "status", "podIP")), ContainerID: strings.TrimPrefix(text(nested(statuses[0], "containerID")), "containerd://"), Target: target, TargetIP: text(nested(targetPod, "status", "podIP"))}
	fault.Rule, err = observationRule(fault.OperatorIP, fault.TargetIP, target.PodUID)
	if err != nil {
		return nil, err
	}
	fault.PID, err = fault.runtimePID(ctx)
	if err != nil {
		return nil, err
	}
	_, err = command(ctx, "docker", fault.args("-I")...)
	if err != nil {
		return fault, err // A lost command result may still have installed the rule.
	}
	if _, err := command(ctx, "docker", fault.args("-C")...); err != nil {
		return fault, fmt.Errorf("owned observation rule installation unconfirmed: %w", err)
	}
	return fault, nil
}

func (f *observationFault) args(operation string) []string {
	return append([]string{"exec", kindNode, "nsenter", "-t", f.PID, "-n", "iptables", operation}, f.Rule...)
}

func (f *observationFault) runtimePID(ctx context.Context) (string, error) {
	data, err := command(ctx, "docker", "exec", kindNode, "crictl", "inspect", "-o", "json", f.ContainerID)
	if err != nil {
		return "", err
	}
	var runtime obj
	if err := json.Unmarshal(data, &runtime); err != nil {
		return "", err
	}
	pid := fmt.Sprint(nested(runtime, "info", "pid"))
	if pid == "<nil>" || pid == "0" || nested(runtime, "status", "id") != f.ContainerID || nested(runtime, "status", "labels", "io.kubernetes.pod.uid") != f.OperatorUID || nested(runtime, "status", "state") != "CONTAINER_RUNNING" {
		return "", fmt.Errorf("operator observation-fault runtime identity changed")
	}
	return pid, nil
}

func (f *observationFault) remove(ctx context.Context) error {
	if f.Removed {
		return nil
	}
	pid, err := f.runtimePID(ctx)
	if err != nil || pid != f.PID {
		return fmt.Errorf("cannot safely remove owned observation rule: %v", err)
	}
	if _, err := command(ctx, "docker", f.args("-D")...); err != nil {
		return err
	}
	data, err := exec.CommandContext(ctx, "docker", f.args("-C")...).CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		return fmt.Errorf("owned observation rule absence unconfirmed: %v: %s", err, data)
	}
	f.Removed = true
	return nil
}

func qualifyOnline(ctx context.Context, survivor *forward, stsUID, bindingUID, out string, event func(string, obj), faults *[]*observationFault) ([]operator.FenceTarget, error) {
	failedPod, err := get(ctx, "pod", "rhiza-2")
	if err != nil {
		return nil, err
	}
	failedForward, err := startForward(ctx, "rhiza-2", out)
	if err != nil {
		return nil, err
	}
	failedStatus, err := readMembership(ctx, failedForward)
	failedForward.close()
	if err != nil || failedStatus.NodeID != "rhiza-2" || failedStatus.ClusterID != "chaos-source" || !failedStatus.Voting || failedStatus.Pending || failedStatus.WALIdentity == "" {
		return nil, fmt.Errorf("failed voter identity unavailable: %v", err)
	}
	failed := operator.FenceTarget{NodeID: string(failedStatus.NodeID), Pod: "rhiza-2", PodUID: text(nested(failedPod, "metadata", "uid")), WALIdentity: failedStatus.WALIdentity}
	partition, err := partitionPeer(ctx, failed)
	if err != nil {
		return nil, err
	}
	event("online_peer_partition", partition)
	fault, err := blockOperatorObservation(ctx, failed)
	if fault != nil {
		*faults = append(*faults, fault)
	}
	if err != nil {
		return nil, err
	}
	event("online_operator_observation_fault", obj{"fault": fault})
	if _, err := call(ctx, survivor, "/sql/execute", obj{"request_id": "fencing-surviving-quorum", "sql": "INSERT INTO fencing_items VALUES (2, 'quorum-survived')"}, admin); err != nil {
		return nil, err
	}
	if err := checkRows(ctx, survivor, admin, `[[1,"acknowledged"],[2,"quorum-survived"]]`); err != nil {
		return nil, err
	}
	event("surviving_quorum_ack_readback", obj{"rows": 2})
	var voterRequest operator.FenceRequest
	var voterProof operator.FenceProof
	if err := wait(ctx, "exact online voter drain/exit proof", func() (bool, error) {
		list, err := get(ctx, "rhizafences", "")
		if err != nil {
			return false, err
		}
		items, _ := list["items"].([]any)
		for _, item := range items {
			if nested(item, "spec", "scope") != "Voter" || nested(item, "status", "proof") == nil {
				continue
			}
			if err := verifyScopedProof(ctx, item, stsUID, bindingUID, []string{failed.PodUID}, operator.ScopeVoter, []operator.FenceTarget{failed}, out); err != nil {
				return false, err
			}
			if err := json.Unmarshal(encoded(nested(item, "spec")), &voterRequest); err != nil {
				return false, err
			}
			return true, json.Unmarshal(encoded(nested(item, "status", "proof")), &voterProof)
		}
		return false, nil
	}); err != nil {
		return nil, err
	}
	event("generic_voter_proof", obj{"request": voterRequest, "proof": voterProof})
	var child operator.Resource
	if err := wait(ctx, "online membership promotion and healthy parent", func() (bool, error) {
		resources, err := get(ctx, "rhizarecoveries", "")
		if err != nil {
			return false, err
		}
		items, _ := resources["items"].([]any)
		var resource any
		for _, item := range items {
			if nested(item, "metadata", "name") == voterRequest.OperationID {
				resource = item
			}
		}
		if resource == nil {
			return false, nil
		}
		if err := json.Unmarshal(encoded(resource), &child); err != nil {
			return false, err
		}
		if child.Status.Phase != "Complete" || child.Status.Membership == nil || child.Status.Membership.Phase != "Complete" {
			return false, nil
		}
		root, err := get(ctx, "rhizacluster", "rhiza")
		if err != nil {
			return false, err
		}
		return nested(root, "status", "phase") == "Healthy" && nested(root, "status", "activeClusterID") == "chaos-source", nil
	}); err != nil {
		return nil, err
	}
	m := child.Spec.Membership
	if m == nil || m.Remove != "rhiza-2" || m.RemovedPod != failed.Pod || m.Fence.WorkloadUID != failed.PodUID || m.Fence.WALIdentity != failed.WALIdentity || m.Fence.Evidence != voterProof.ProofID || !m.Fence.Confirmed || m.ReplacementPod == "" || child.Status.Membership.Add == nil || child.Status.Membership.RemoveRequest == nil || text(nested(child.Metadata, "uid")) == "" || text(nested(child.Metadata, "annotations", "rhiza.mrchypark.dev/automatic-owner")) != bindingUID || text(nested(child.Metadata, "annotations", "rhiza.mrchypark.dev/automatic-logical")) != "fencing-e2e" {
		return nil, fmt.Errorf("completed membership differs from exact voter fence")
	}
	remove := child.Status.Membership.RemoveRequest
	operationPrefix := text(child.Metadata["uid"]) + ":" + m.OperationID + ":"
	if m.OperationID != voterRequest.OperationID || remove.OperationID != operationPrefix+"remove" || remove.ClusterID != "chaos-source" || string(remove.Remove) != failed.NodeID || remove.Fence == nil || remove.Fence.Evidence != voterProof.ProofID || remove.Fence.WALIdentity != failed.WALIdentity || remove.Fence.WorkloadUID != failed.PodUID || child.Status.Membership.Add.OperationID != operationPrefix+"add" {
		return nil, fmt.Errorf("membership remove/add journals differ from fenced operation")
	}
	refs := append(append([]operator.VoterPod(nil), m.VoterPods...), operator.VoterPod{NodeID: child.Status.Membership.Add.MemberID, Pod: m.ReplacementPod})
	if err := validateReplacementRefs(refs, failed.NodeID, m.ReplacementPod); err != nil {
		return nil, err
	}
	var targets []operator.FenceTarget
	var observations []obj
	var configID uint
	for _, ref := range refs {
		pod, err := get(ctx, "pod", ref.Pod)
		if err != nil {
			return nil, err
		}
		f, err := startForward(ctx, ref.Pod, out)
		if err != nil {
			return nil, err
		}
		status, statusErr := readMembership(ctx, f)
		f.close()
		if statusErr != nil || !membershipMatches(status, refs, string(ref.NodeID)) || configID != 0 && status.ConfigID != configID {
			return nil, fmt.Errorf("promoted voter %s membership differs: %v", ref.NodeID, statusErr)
		}
		configID = status.ConfigID
		targets = append(targets, operator.FenceTarget{NodeID: string(ref.NodeID), Pod: ref.Pod, PodUID: text(nested(pod, "metadata", "uid")), WALIdentity: status.WALIdentity})
		observations = append(observations, obj{"podUID": nested(pod, "metadata", "uid"), "pod": ref.Pod, "membership": status})
		if ref.Pod == m.ReplacementPod {
			if err := verifyLearnerBinding(ctx, pod, child, status, stsUID, bindingUID, voterRequest.OperationID, out); err != nil {
				return nil, err
			}
		}
	}
	if err := verifyRegisteredTargets(ctx, targets, stsUID, out); err != nil {
		return nil, err
	}
	if err := verifyImages(ctx, out, "learner-promoted", targets...); err != nil {
		return nil, err
	}
	learnerForward, err := startForward(ctx, m.ReplacementPod, out)
	if err != nil {
		return nil, err
	}
	defer learnerForward.close()
	if _, err := call(ctx, learnerForward, "/sql/execute", obj{"request_id": "fencing-learner-promoted", "sql": "INSERT INTO fencing_items VALUES (3, 'learner-promoted')"}, admin); err != nil {
		return nil, err
	}
	if err := checkRows(ctx, learnerForward, admin, `[[1,"acknowledged"],[2,"quorum-survived"],[3,"learner-promoted"]]`); err != nil {
		return nil, err
	}
	event("online_replacement_linearizable_readback", obj{"targets": targets, "rows": 3, "configID": configID})
	if err := fault.remove(ctx); err != nil {
		return nil, err
	}
	event("online_operator_observation_fault_removed", obj{"fault": fault})
	return targets, os.WriteFile(filepath.Join(out, "online-completed-membership.json"), encoded(obj{"recovery": child, "targets": targets, "observations": observations}), 0600)
}

func checkRows(ctx context.Context, f *forward, token, expected string) error {
	response, err := call(ctx, f, "/sql/query", obj{"sql": "SELECT id,value FROM fencing_items ORDER BY id", "consistency": "linearizable"}, token)
	if err != nil {
		return err
	}
	if string(encoded(response["rows"])) != expected {
		return fmt.Errorf("linearizable acknowledged values differ")
	}
	return nil
}

func validateReplacementRefs(refs []operator.VoterPod, removed, learner string) error {
	seen, pods := map[string]bool{}, map[string]bool{}
	for _, ref := range refs {
		id := string(ref.NodeID)
		if id == "" || ref.Pod == "" || id == removed || seen[id] || pods[ref.Pod] {
			return fmt.Errorf("replacement membership mapping is invalid")
		}
		seen[id], pods[ref.Pod] = true, true
	}
	if len(refs) != 3 || !seen[learner] || !pods[learner] || !seen["rhiza-0"] || !seen["rhiza-1"] {
		return fmt.Errorf("replacement must retain both survivors and promote the authenticated learner")
	}
	return nil
}

func sameTargets(actual, expected []operator.FenceTarget) bool {
	if len(actual) != len(expected) {
		return false
	}
	seen := map[string]operator.FenceTarget{}
	for _, target := range actual {
		if _, duplicate := seen[target.NodeID]; duplicate || target.NodeID == "" || target.Pod == "" || target.PodUID == "" || target.WALIdentity == "" {
			return false
		}
		seen[target.NodeID] = target
	}
	for _, target := range expected {
		if seen[target.NodeID] != target {
			return false
		}
		delete(seen, target.NodeID)
	}
	return true
}

func membershipMatches(status network.MembershipStatus, refs []operator.VoterPod, nodeID string) bool {
	if string(status.NodeID) != nodeID || status.ClusterID != "chaos-source" || status.WALIdentity == "" || !status.Voting || status.Pending || status.ConfigID == 0 || len(status.Voters) != len(refs) {
		return false
	}
	seen := map[quepaxa.NodeID]bool{}
	for _, id := range status.Voters {
		if seen[id] {
			return false
		}
		seen[id] = true
	}
	for _, ref := range refs {
		if !seen[ref.NodeID] {
			return false
		}
	}
	return true
}

func verifyLearnerBinding(ctx context.Context, pod obj, child operator.Resource, status network.MembershipStatus, stsUID, bindingUID, operation, out string) error {
	m, add := child.Spec.Membership, child.Status.Membership.Add
	name := text(nested(pod, "metadata", "name"))
	if name != m.ReplacementPod || string(add.MemberID) != name || status.WALIdentity != add.WALIdentity || text(nested(pod, "metadata", "uid")) == "" || nested(pod, "metadata", "labels", "rhiza.mrchypark.dev/automatic-learner") != name || nested(pod, "metadata", "annotations", "rhiza.mrchypark.dev/fencing-root-uid") != bindingUID || nested(pod, "metadata", "annotations", "rhiza.mrchypark.dev/recovery-owner") != operation {
		return fmt.Errorf("promoted learner Pod/WAL/root identity differs")
	}
	owners, _ := nested(pod, "metadata", "ownerReferences").([]any)
	owned := false
	for _, owner := range owners {
		owned = owned || nested(owner, "kind") == "StatefulSet" && nested(owner, "uid") == stsUID
	}
	if !owned {
		return fmt.Errorf("promoted learner lacks exact StatefulSet owner")
	}
	secret, err := get(ctx, "secret", m.ReplacementSecret)
	if err != nil {
		return err
	}
	if secret["immutable"] != true || nested(secret, "metadata", "uid") != add.SecretUID || nested(secret, "metadata", "resourceVersion") != add.SecretResourceVer || nested(pod, "metadata", "annotations", "rhiza.mrchypark.dev/fencing-credential-uid") != add.SecretUID || nested(secret, "metadata", "annotations", "rhiza.mrchypark.dev/recovery-owner") != operation {
		return fmt.Errorf("promoted learner immutable credential binding differs")
	}
	raw, err := base64.StdEncoding.DecodeString(text(nested(secret, "data", "member")))
	if err != nil {
		return err
	}
	var member quepaxa.Member
	if err := json.Unmarshal(raw, &member); err != nil {
		return err
	}
	member.WALIdentity = status.WALIdentity
	request := network.MembershipChange{OperationID: add.OperationID, ClusterID: "chaos-source", ExpectedConfigID: add.ExpectedConfigID, ExpectedAbortSlot: add.ExpectedAbortSlot, Add: &member}
	if member.ID != status.NodeID || member.PublicKey == (quepaxa.PublicKey{}) || fmt.Sprintf("%x", sha256.Sum256(encoded(request))) != add.RequestHash {
		return fmt.Errorf("promoted learner member differs from committed addition")
	}
	return os.WriteFile(filepath.Join(out, "learner-authenticated-binding.json"), encoded(obj{"podUID": nested(pod, "metadata", "uid"), "nodeID": status.NodeID, "walIdentity": status.WALIdentity, "rootUID": bindingUID, "statefulSetUID": stsUID, "credentialUID": add.SecretUID, "member": member, "addition": add}), 0600)
}

type fenceLedgerState struct {
	StatefulSetUID string         `json:"statefulSetUID"`
	Ledger         fencing.Ledger `json:"ledger"`
}

func verifyRegisteredTargets(ctx context.Context, targets []operator.FenceTarget, stsUID, out string) error {
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
		var state fenceLedgerState
		if err := json.Unmarshal([]byte(data), &state); err != nil {
			return err
		}
		if state.StatefulSetUID != stsUID || state.Ledger.SourceClusterID != "chaos-source" {
			continue
		}
		for _, target := range targets {
			found := false
			for _, run := range state.Ledger.Runs {
				if run.Identity.NodeID == target.NodeID && run.Identity.PodUID == target.PodUID && run.Identity.WALIdentity == target.WALIdentity && run.Stop == nil && !run.Exited {
					found = true
				}
			}
			if !found {
				return fmt.Errorf("promoted current voter lacks active WAL-bound startup registration: %s", target.NodeID)
			}
		}
		return os.WriteFile(filepath.Join(out, "online-active-ledger.json"), encoded(state), 0600)
	}
	return fmt.Errorf("active source ledger unavailable")
}

func digest(value string) string {
	value = strings.TrimPrefix(strings.TrimPrefix(value, "docker-pullable://"), "containerd://")
	if _, d, ok := strings.Cut(value, "@"); ok {
		return d
	}
	return value
}

func immutableDigest(value string) (string, error) {
	d := digest(value)
	if !strings.HasPrefix(d, "sha256:") || len(d) != len("sha256:")+64 {
		return "", fmt.Errorf("image reference has no immutable SHA256 digest: %q", value)
	}
	if _, err := hex.DecodeString(strings.TrimPrefix(d, "sha256:")); err != nil {
		return "", err
	}
	return d, nil
}

// Kind can discard the archive's import repository name while retaining its
// immutable content. Follow only hash-verified descriptors, never a mutable tag.
func imageConfig(ctx context.Context, reference string) (obj, []obj, error) {
	d, err := immutableDigest(reference)
	if err != nil {
		return nil, nil, err
	}
	var chain []obj
	for range 4 {
		data, lookupErr := command(ctx, "docker", "exec", kindNode, "crictl", "inspecti", "-o", "json", d)
		if lookupErr == nil {
			var cri obj
			if err := json.Unmarshal(data, &cri); err != nil {
				return nil, chain, err
			}
			if _, err := immutableDigest(text(nested(cri, "status", "id"))); err != nil {
				return nil, chain, err
			}
			chain = append(chain, obj{"digest": d, "cri": publicCRIImage(cri)})
			return cri, chain, nil
		}
		content, err := command(ctx, "docker", "exec", kindNode, "ctr", "-n", "k8s.io", "content", "get", d)
		if err != nil {
			return nil, chain, fmt.Errorf("resolve immutable image %s: CRI: %v; content: %w", d, lookupErr, err)
		}
		next, err := imageDescriptor(d, content)
		chain = append(chain, obj{"digest": d, "criLookupFailed": true, "contentSHA256": fmt.Sprintf("%x", sha256.Sum256(content)), "descriptor": publicDescriptor(content)})
		if err != nil {
			return nil, chain, err
		}
		d = next
	}
	return nil, chain, fmt.Errorf("image descriptor chain exceeds qualification bound")
}

func imageDescriptor(d string, content []byte) (string, error) {
	if "sha256:"+fmt.Sprintf("%x", sha256.Sum256(content)) != d {
		return "", fmt.Errorf("image content digest differs from %s", d)
	}
	var descriptor struct {
		Config struct {
			Digest string `json:"digest"`
		} `json:"config"`
		Manifests []struct {
			Digest    string `json:"digest"`
			Size      int64  `json:"size"`
			MediaType string `json:"mediaType"`
			Platform  obj    `json:"platform"`
		} `json:"manifests"`
	}
	if err := json.Unmarshal(content, &descriptor); err != nil {
		return "", err
	}
	if descriptor.Config.Digest != "" && len(descriptor.Manifests) == 0 {
		return immutableDigest(descriptor.Config.Digest)
	}
	if descriptor.Config.Digest == "" && len(descriptor.Manifests) > 0 {
		first := descriptor.Manifests[0]
		resolved, err := immutableDigest(first.Digest)
		if err != nil {
			return "", err
		}
		knownMediaType := first.MediaType == "application/vnd.oci.image.manifest.v1+json" || first.MediaType == "application/vnd.oci.image.index.v1+json" || first.MediaType == "application/vnd.docker.distribution.manifest.v2+json" || first.MediaType == "application/vnd.docker.distribution.manifest.list.v2+json"
		for _, alias := range descriptor.Manifests[1:] {
			// Docker archives may index two tag aliases of the same manifest.
			// Ignore only annotations, not conflicting content or platform metadata.
			if first.Size <= 0 || !knownMediaType || alias.Digest != resolved || alias.Size != first.Size || alias.MediaType != first.MediaType || !bytes.Equal(encoded(alias.Platform), encoded(first.Platform)) {
				return "", fmt.Errorf("ambiguous immutable image index %s", d)
			}
		}
		return resolved, nil
	}
	return "", fmt.Errorf("ambiguous or unsupported immutable image descriptor %s", d)
}

func incarnationSnapshot(pod obj) string {
	statuses, _ := nested(pod, "status", "containerStatuses").([]any)
	var processes []obj
	for _, status := range statuses {
		processes = append(processes, obj{"containerID": nested(status, "containerID"), "restartCount": nested(status, "restartCount")})
	}
	return string(encoded(obj{"uid": nested(pod, "metadata", "uid"), "processes": processes}))
}

func verifyImages(ctx context.Context, out, phase string, current ...operator.FenceTarget) error {
	pods, err := get(ctx, "pods", "")
	if err != nil {
		return err
	}
	items, _ := pods["items"].([]any)
	counts := map[string]int{}
	wanted := map[string]string{}
	for _, target := range current {
		if target.Pod == "" || target.PodUID == "" || wanted[target.Pod] != "" {
			return fmt.Errorf("invalid current image inventory")
		}
		wanted[target.Pod] = target.PodUID
	}
	var evidence []obj
	for _, pod := range items {
		if nested(pod, "metadata", "deletionTimestamp") != nil || nested(pod, "status", "phase") != "Running" {
			continue
		}
		role := text(nested(pod, "metadata", "labels", "app.kubernetes.io/name"))
		name := text(nested(pod, "metadata", "name"))
		if uid := wanted[name]; uid != "" {
			if nested(pod, "metadata", "uid") != uid {
				return fmt.Errorf("qualified voter image incarnation changed")
			}
			role = "rhiza"
		} else if len(wanted) > 0 && role == "rhiza" {
			continue // Retired source slots are not current voting engines.
		}
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
		containerID := strings.TrimPrefix(text(nested(statuses[0], "containerID")), "containerd://")
		runtimeData, err := command(ctx, "docker", "exec", kindNode, "crictl", "inspect", "-o", "json", containerID)
		if err != nil {
			return err
		}
		var runtime obj
		if err := json.Unmarshal(runtimeData, &runtime); err != nil {
			return err
		}
		if text(nested(runtime, "status", "id")) != containerID || nested(runtime, "status", "labels", "io.kubernetes.pod.uid") != nested(pod, "metadata", "uid") || digest(text(nested(runtime, "status", "imageRef"))) != digest(podImage) {
			return fmt.Errorf("%s runtime image does not bind the observed Pod incarnation", role)
		}
		cri, chain, resolveErr := imageConfig(ctx, podImage)
		configID := text(nested(cri, "status", "id"))
		builtID := text(built[0]["Id"])
		repos, _ := nested(cri, "status", "repoDigests").([]any)
		evidence = append(evidence, obj{"pod": nested(pod, "metadata", "name"), "uid": nested(pod, "metadata", "uid"), "role": role, "podImageID": podImage, "container": publicRuntime(runtime), "resolution": chain, "builtConfigID": builtID, "criConfigID": configID, "criRepoDigests": repos})
		if err := os.WriteFile(filepath.Join(out, phase+"-image-evidence.json"), encoded(evidence), 0600); err != nil {
			return err
		}
		if resolveErr != nil {
			return resolveErr
		}
		if configID == "" || digest(configID) != digest(builtID) {
			return fmt.Errorf("%s running image configuration differs from built image", role)
		}
		counts[role]++
	}
	if counts["rhiza"] != 3 || counts["rhiza-operator"] != 1 || counts["rhiza-fencer"] != 1 {
		return fmt.Errorf("qualification image inventory incomplete: %v", counts)
	}
	return os.WriteFile(filepath.Join(out, phase+"-image-evidence.json"), encoded(evidence), 0600)
}

func verifyGenerationProof(ctx context.Context, item any, stsUID, bindingUID string, oldUIDs []string, out string, targets ...operator.FenceTarget) error {
	return verifyScopedProof(ctx, item, stsUID, bindingUID, oldUIDs, operator.ScopeGeneration, targets, out)
}

func verifyScopedProof(ctx context.Context, item any, stsUID, bindingUID string, oldUIDs []string, scope operator.Scope, targets []operator.FenceTarget, out string) error {
	var request operator.FenceRequest
	var proof operator.FenceProof
	if err := json.Unmarshal(encoded(nested(item, "spec")), &request); err != nil {
		return err
	}
	if err := json.Unmarshal(encoded(nested(item, "status", "proof")), &proof); err != nil {
		return err
	}
	if request.SourceClusterID != "chaos-source" || request.StatefulSet != "rhiza" || request.StatefulSetUID != stsUID || request.BindingUID != bindingUID || request.Namespace != namespace || request.LogicalID != "fencing-e2e" || request.OperationID == "" || request.Scope != scope {
		return fmt.Errorf("generation fence request identity differs")
	}
	var nodes []string
	if scope == operator.ScopeVoter {
		if len(targets) != 1 || string(encoded(request.Targets)) != string(encoded(targets)) {
			return fmt.Errorf("voter fence target differs from observed exact incarnation")
		}
		nodes = []string{targets[0].NodeID}
	} else if len(targets) > 0 && !sameTargets(request.Targets, targets) {
		return fmt.Errorf("generation fence does not bind all current voters including the learner")
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
		var state fenceLedgerState
		if err := json.Unmarshal([]byte(data), &state); err != nil {
			return err
		}
		if state.StatefulSetUID != stsUID || state.Ledger.SourceClusterID != request.SourceClusterID {
			continue
		}
		if !state.Ledger.CompleteNodes(request.OperationID, hash, nodes, oldUIDs) {
			return fmt.Errorf("generation ledger has no clean drain/exit for all source incarnations")
		}
		for _, run := range state.Ledger.Runs {
			for _, uid := range oldUIDs {
				if run.Identity.PodUID == uid && (run.Stop == nil || run.Stop.OperationID != request.OperationID || run.Stop.RequestHash != hash) {
					return fmt.Errorf("source incarnation drained for a different operation")
				}
			}
		}
		if err := verifyLedgerProof(state, request, proof); err != nil {
			return err
		}
		filename := "validated-voter-proof.json"
		if scope == operator.ScopeGeneration {
			filename = "validated-generation-proof.json"
			if err := verifyPriorVoterStops(out, state.Ledger); err != nil {
				return err
			}
		}
		provenance := "exact-current-ledger-hash"
		if scope == operator.ScopeVoter {
			provenance = "historical-issuer-proof-id; current-ledger-exact-terminal-targets; issuance-snapshot-not-reconstructed"
		}
		return os.WriteFile(filepath.Join(out, filename), encoded(obj{"request": request, "proof": proof, "ledger": state, "oldUIDs": oldUIDs, "ledgerVerification": provenance}), 0600)
	}
	return fmt.Errorf("generation proof has no matching durable ledger")
}

func verifyLedgerProof(state fenceLedgerState, request operator.FenceRequest, proof operator.FenceProof) error {
	hash := fmt.Sprintf("%x", sha256.Sum256(encoded(request)))
	if state.StatefulSetUID != request.StatefulSetUID || state.Ledger.SourceClusterID != request.SourceClusterID || proof.OperationID != request.OperationID || proof.SourceClusterID != request.SourceClusterID || proof.BindingUID != request.BindingUID || proof.RequestHash != hash || !proof.ProcessesTerminated || !proof.RecreationBlocked || !proof.StorageQuiesced {
		return fmt.Errorf("ledger proof identity/request binding differs")
	}
	if request.Scope == operator.ScopeGeneration {
		if proof.ProofID != "cooperative-"+fmt.Sprintf("%x", sha256.Sum256(encoded(state))) {
			return fmt.Errorf("generation proof does not bind observed closed ledger")
		}
		return nil
	}
	// This is an issuer-produced historical proof, not a hash of today's ledger:
	// learner registration advances unrelated runs after the one-time issuance.
	if request.Scope != operator.ScopeVoter || !strings.HasPrefix(proof.ProofID, "cooperative-") {
		return fmt.Errorf("unsupported historical voter proof")
	}
	if _, err := immutableDigest("sha256:" + strings.TrimPrefix(proof.ProofID, "cooperative-")); err != nil {
		return err
	}
	if len(request.Targets) != 1 {
		return fmt.Errorf("online voter proof requires one exact target")
	}
	target := request.Targets[0]
	if target.NodeID == "" || target.PodUID == "" || target.WALIdentity == "" || !state.Ledger.CompleteNodes(request.OperationID, hash, []string{target.NodeID}, []string{target.PodUID}) {
		return fmt.Errorf("historical voter proof lacks clean completed target authority")
	}
	found := false
	for _, run := range state.Ledger.Runs {
		if run.Identity.NodeID != target.NodeID {
			continue
		}
		if run.Stop == nil || run.Stop.OperationID != request.OperationID || run.Stop.RequestHash != hash {
			return fmt.Errorf("terminal voter run belongs to a different operation")
		}
		if run.Identity.PodUID == target.PodUID && run.Identity.WALIdentity == target.WALIdentity && run.Drain != nil && run.Drain.Request == *run.Stop && run.Drain.Clean() && run.Exited {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("historical voter proof target UID/WAL receipt differs")
	}
	return nil
}

func verifyPriorVoterStops(out string, current fencing.Ledger) error {
	data, err := os.ReadFile(filepath.Join(out, "validated-voter-proof.json"))
	if err != nil {
		return fmt.Errorf("generation has no preserved online voter proof: %w", err)
	}
	var prior struct {
		Request operator.FenceRequest `json:"request"`
		Ledger  fenceLedgerState      `json:"ledger"`
	}
	if err := json.Unmarshal(data, &prior); err != nil {
		return err
	}
	matched := false
	for id, old := range prior.Ledger.Ledger.Runs {
		if old.Stop == nil || old.Stop.OperationID != prior.Request.OperationID {
			continue
		}
		if !bytes.Equal(encoded(old), encoded(current.Runs[id])) {
			return fmt.Errorf("prior terminal voter evidence changed during generation fencing")
		}
		matched = true
	}
	if !matched {
		return fmt.Errorf("preserved online proof has no immutable stopped voter")
	}
	return nil
}
