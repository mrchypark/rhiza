package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/quic-go/quic-go"
)

func TestObjectStoreCountersAllowlist(t *testing.T) {
	handler := objectStoreCounters(func() (rhiza.ObjectStoreStats, bool) {
		return rhiza.ObjectStoreStats{HTTPRequests: 17, BytesUploaded: 29, BytesPublished: 23,
			S3HTTPRequests: 99, ObservedRequestRepeats: 88}, true
	})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/qualification/object-store", nil))
	var counters map[string]uint64
	if err := json.Unmarshal(response.Body.Bytes(), &counters); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || len(counters) != 16 || counters["http_requests"] != 17 || counters["bytes_uploaded"] != 29 || counters["bytes_published"] != 23 {
		t.Fatalf("unexpected allowlisted counters: status=%d counters=%v", response.Code, counters)
	}
	for _, excluded := range []string{"s3_http_requests", "observed_request_repeats", "config", "token", "service_account"} {
		if _, ok := counters[excluded]; ok {
			t.Fatalf("unexpected disclosure: %s", excluded)
		}
	}
}

func TestQualificationQUICHandshake(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal("test identity unavailable")
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Unix(0, 0), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal("test certificate unavailable")
	}
	// Match PeerServer's early listener: the probe closes immediately after its
	// client handshake, before a normal listener may enqueue the connection.
	listener, err := quic.ListenAddrEarly("127.0.0.1:0", &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{sqlpolicy.PeerALPN}, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: private}}}, nil)
	if err != nil {
		t.Fatal("test QUIC listener unavailable")
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	type observation struct {
		stage      string
		err        error
		contextErr error
	}
	streamResult := make(chan observation, 1)
	go func() {
		conn, err := listener.Accept(ctx)
		if err != nil {
			streamResult <- observation{"accept", err, ctx.Err()}
			return
		}
		_, err = conn.AcceptStream(ctx)
		streamResult <- observation{"stream", err, ctx.Err()}
	}()
	var pinned rhiza.PublicKey
	copy(pinned[:], public)
	if probeQUICHandshake(ctx, listener.Addr().String(), "rhiza-voter-1", pinned) != nil {
		t.Fatal("pinned handshake failed")
	}
	observed := <-streamResult
	if observed.stage != "stream" || observed.err == nil {
		t.Fatalf("probe observation: stage=%s errType=%T contextErr=%v deadline=%t", observed.stage, observed.err, observed.contextErr, errors.Is(observed.err, context.DeadlineExceeded))
	}
	var closed *quic.ApplicationError
	var transport *quic.TransportError
	applicationClose := errors.As(observed.err, &closed) && closed.Remote && closed.ErrorCode == 0 && closed.ErrorMessage == "qualification handshake only"
	// QUIC maps application close to transport APPLICATION_ERROR in Initial
	// or Handshake packets (quic-go packet_packer.go). Neither permits a stream;
	// arbitrary transport errors and context timeouts must still fail here.
	handshakeClose := errors.As(observed.err, &transport) && transport.Remote && transport.ErrorCode == quic.ApplicationErrorErrorCode && transport.FrameType == 0 && transport.ErrorMessage == ""
	if !applicationClose && !handshakeClose {
		t.Fatalf("probe did not close without a stream: errType=%T contextErr=%v", observed.err, observed.contextErr)
	}
	t.Logf("no-stream close observed: application=%t handshakeTransport=%t contextErr=%v", applicationClose, handshakeClose, observed.contextErr)
	pinned[0] ^= 1
	if err := probeQUICHandshake(ctx, listener.Addr().String(), "rhiza-voter-1", pinned); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("certificate mismatch was not rejected distinctly")
	}
	// An owned black-hole UDP socket proves deadline classification locally;
	// it does NOT constitute Kubernetes NetworkPolicy evidence.
	socket, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("test UDP socket unavailable")
	}
	defer socket.Close()
	blocked, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stop()
	if err := probeQUICHandshake(blocked, socket.LocalAddr().String(), "rhiza-voter-1", pinned); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("blocked probe did not return context deadline")
	}
}

func TestQualificationFixture(t *testing.T) {
	run := "a1b2c3d4"
	namespace := "rhiza-v0191-20261008-" + run
	owner := "rhiza-postrelease-" + run + "-" + strings.Repeat("a", 40)
	// Synthetic deterministic entropy only; no generated live credentials/logs.
	entropy := make([]byte, 160)
	for i := range entropy {
		entropy[i] = byte(i)
	}
	fixture, err := qualificationFixture(namespace, run, owner, bytes.NewReader(entropy))
	if err != nil {
		t.Fatal("valid fixture failed")
	}
	data := fixture["stringData"].(map[string]string)
	if len(data) != 5 || fixture["immutable"] != true || fixture["kind"] != "Secret" || fixture["type"] != "Opaque" {
		t.Fatal("fixture schema mismatch")
	}
	var peers map[string]string
	var members []rhiza.Member
	var learner rhiza.Member
	if json.Unmarshal([]byte(data["RHIZA_PEER_TOKENS"]), &peers) != nil || json.Unmarshal([]byte(data["RHIZA_CLUSTER_MEMBERS"]), &members) != nil || json.Unmarshal([]byte(data["learner_member"]), &learner) != nil {
		t.Fatal("fixture configuration invalid")
	}
	seen := map[string]bool{data["RHIZA_ADMIN_TOKEN"]: true, data["learner_token"]: true}
	if len(members) != 3 || len(peers) != 3 || len(seen) != 2 {
		t.Fatal("fixture cardinality mismatch")
	}
	for _, member := range members {
		id := string(member.ID)
		token := peers[id]
		if len(token) != 64 || seen[token] || member.PublicKey != rhiza.PeerPublicKey("gcs-"+run, id, token) || member.URL != "http://"+id+".rhiza-peers:8080" || member.PeerURL != "quic://"+id+".rhiza-peers:9090" {
			t.Fatal("voter fixture/KDF mismatch")
		}
		seen[token] = true
	}
	if learner.ID != "rhiza-learner" || learner.URL != "http://rhiza-learner:8080" || learner.PeerURL != "quic://rhiza-learner:9090" || learner.PublicKey != rhiza.PeerPublicKey("gcs-"+run, string(learner.ID), data["learner_token"]) {
		t.Fatal("learner fixture/KDF mismatch")
	}
	metadata := fixture["metadata"].(map[string]any)
	if metadata["namespace"] != namespace || metadata["annotations"].(map[string]string)["rhiza.dev/auth-owner"] != owner || metadata["labels"].(map[string]string)["chaos.rhiza.io/run"] != run {
		t.Fatal("fixture ownership mismatch")
	}
	t.Setenv("RHIZA_CLUSTER_ID", "gcs-"+run)
	t.Setenv("RHIZA_NODE_ID", "rhiza-voter-0")
	t.Setenv("RHIZA_PEER_TOKEN", "")
	t.Setenv("RHIZA_LEARNER", "")
	t.Setenv("RHIZA_CLUSTER_MEMBERS", data["RHIZA_CLUSTER_MEMBERS"])
	t.Setenv("RHIZA_PEER_TOKENS", data["RHIZA_PEER_TOKENS"])
	config, err := rhiza.ConfigFromEnv()
	if err != nil || config.PeerToken != peers["rhiza-voter-0"] || len(config.Members) != 3 {
		t.Fatal("voter fixture rejected by released config API")
	}
	t.Setenv("RHIZA_NODE_ID", "rhiza-learner")
	t.Setenv("RHIZA_PEER_TOKENS", "")
	t.Setenv("RHIZA_PEER_TOKEN", data["learner_token"])
	t.Setenv("RHIZA_LEARNER", data["learner_member"])
	config, err = rhiza.ConfigFromEnv()
	if err != nil || config.PeerToken != data["learner_token"] || config.Learner == nil || config.Learner.PublicKey != learner.PublicKey {
		t.Fatal("learner fixture rejected by released config API")
	}
	for _, args := range [][3]string{{"wrong", run, owner}, {"rhiza-v0190-20261008-" + run, run, owner}, {namespace, "BAD", owner}, {namespace, run, "wrong"}} {
		if _, err := qualificationFixture(args[0], args[1], args[2], bytes.NewReader(entropy)); err == nil {
			t.Fatal("invalid identity accepted")
		}
	}
	if _, err := qualificationFixture(namespace, run, owner, bytes.NewReader(nil)); err == nil {
		t.Fatal("missing entropy accepted")
	}
	if _, err := qualificationFixture(namespace, run, owner, bytes.NewReader(make([]byte, 160))); err == nil {
		t.Fatal("repeated entropy accepted")
	}
}

// Exercise the actual shell entry with synthetic files and a non-networking
// kubectl. No token is minted, no application opens, and no cloud call is made.
func TestQualificationLocalRuntime(t *testing.T) {
	if _, err := exec.LookPath("timeout"); err != nil {
		t.Fatal("native GNU timeout required for the local runtime tests")
	}
	scriptPath := os.Getenv("RHIZA_RUNTIME_TEST_SCRIPT")
	if scriptPath == "" {
		scriptPath = "../run-gcs-postrelease.sh"
	}
	script, err := filepath.Abs(scriptPath)
	if err != nil {
		t.Fatal(err)
	}
	head, err := exec.Command("git", "-C", filepath.Join(filepath.Dir(script), "../.."), "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal("runtime source identity unavailable")
	}
	for _, scenario := range []string{"embedded-token", "exec", "alternate-context", "token-permissions", "wrong-identity", "fake-ci", "expiry", "command-failure", "deadline", "watchdog"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			bin := filepath.Join(dir, "bin")
			if err := os.Mkdir(bin, 0700); err != nil {
				t.Fatal(err)
			}
			write := func(name, content string, mode os.FileMode) {
				t.Helper()
				if err := os.WriteFile(name, []byte(content), mode); err != nil {
					t.Fatal(err)
				}
			}
			const run = "a1b2c3d4"
			const harness = "f4d4cfee6d0928d813e84fe6e7d8c2b01c69f32c"
			actualHarness := strings.TrimSpace(string(head))
			const contextName = "gke_patch2-the-new-era_asia-northeast3_ied-cluster"
			namespace := "rhiza-v0191-20261008-" + run
			token := filepath.Join(dir, "token")
			write(token, "synthetic-offline-token", 0600)
			user := map[string]any{"tokenFile": token}
			if scenario == "embedded-token" {
				user["token"] = "synthetic-rejected-token"
			}
			if scenario == "exec" {
				user["exec"] = map[string]string{"command": "never-execute"}
			}
			config := map[string]any{"apiVersion": "v1", "kind": "Config", "current-context": contextName,
				"clusters": []any{map[string]any{"name": "ied", "cluster": map[string]string{"server": "https://127.0.0.1", "certificate-authority-data": "synthetic-pinned-ca"}}},
				"contexts": []any{map[string]any{"name": contextName, "context": map[string]string{"cluster": "ied", "namespace": namespace, "user": "runtime"}}},
				"users":    []any{map[string]any{"name": "runtime", "user": user}}}
			if scenario == "alternate-context" {
				config["contexts"] = append(config["contexts"].([]any), map[string]string{"name": "admin"})
			}
			encoded, err := json.Marshal(config)
			if err != nil {
				t.Fatal(err)
			}
			kubeconfig := filepath.Join(dir, "config")
			write(kubeconfig, string(encoded), 0600)
			if scenario == "token-permissions" {
				if err := os.Chmod(token, 0644); err != nil {
					t.Fatal(err)
				}
			}
			now := time.Now().Unix()
			write(filepath.Join(bin, "date"), "#!/bin/sh\nif [ \"$*\" = '+%s' ]; then\n if [ -f \"$FAKE_CLOCK\" ]; then cat \"$FAKE_CLOCK\"; else printf '%s\\n' \"$FAKE_NOW\"; fi\nelse exec /bin/date \"$@\"; fi\n", 0700)
			write(filepath.Join(bin, "kubectl"), `#!/bin/sh
case "$1" in --kubeconfig=*) config=${1#*=}; shift ;; *) exit 92 ;; esac
[ "$1" = "--context=gke_patch2-the-new-era_asia-northeast3_ied-cluster" ] || exit 93
shift
[ "$1" = "--namespace=rhiza-v0191-20261008-a1b2c3d4" ] || exit 94
shift
case "$1" in --request-timeout=*) shift ;; esac
printf '%s\n' "$1" >> "$FAKE_CALLS"
case "$1 $2" in
 "config view") cat "$config" ;;
 "auth whoami")
  if [ "$FAKE_SCENARIO" = watchdog ]; then touch "$FAKE_SIGNAL_READY"; /bin/sleep 1; fi
  if [ "$FAKE_SCENARIO" = wrong-identity ]; then printf '%s\n' '{"status":{"userInfo":{"username":"admin","groups":[]}}}'
  else printf '%s\n' '{"status":{"userInfo":{"username":"system:serviceaccount:rhiza-v0191-20261008-a1b2c3d4:rhiza-runtime","groups":["system:authenticated","system:serviceaccounts","system:serviceaccounts:rhiza-v0191-20261008-a1b2c3d4"]}}}'; fi ;;
 "auth can-i")
  case "$*" in
   "auth can-i get secrets"|"auth can-i create serviceaccounts --subresource=token"|"auth can-i create roles"|"auth can-i create validatingadmissionpolicies.admissionregistration.k8s.io --all-namespaces") printf 'no\n'; exit 1 ;;
   *) exit 96 ;;
  esac ;;
 "get namespace")
  if [ "$FAKE_SCENARIO" = command-failure ]; then exit 53; fi
  printf '%s\n' '{"metadata":{"uid":"synthetic-uid","labels":{"chaos.rhiza.io/run":"a1b2c3d4"},"annotations":{"rhiza.dev/auth-owner":"rhiza-postrelease-a1b2c3d4-f4d4cfee6d0928d813e84fe6e7d8c2b01c69f32c","chaos-mesh.org/inject":"enabled"}}}' ;;
 "get pods,statefulsets,services,configmaps,networkpolicies,podchaos,networkchaos")
  if [ "$FAKE_SCENARIO" = deadline ]; then printf '%s\n' "$((FAKE_NOW + 1199))" > "$FAKE_CLOCK"; fi
  printf '%s\n' '{"items":[]}' ;;
 "create -f")
  /bin/sleep 30 & child=$!
  printf '%s\n' "$child" > "$FAKE_CHILD"
  trap 'kill "$child" 2>/dev/null || true; wait "$child" 2>/dev/null || true; exit 143' TERM
  wait "$child" ;;
 "get pods") printf '%s\n' '{"items":[]}' ;;
 "logs "*) kill -USR1 "$(cat "$FAKE_PARENT")"; exit 0 ;;
 "scale "*|"wait "*|"delete "*) exit 0 ;;
 *) exit 95 ;;
esac
`, 0700)
			expiry := time.Unix(now+3300, 0).UTC().Format(time.RFC3339)
			if scenario == "expiry" {
				expiry = time.Unix(now+14400, 0).UTC().Format(time.RFC3339)
			}
			out := filepath.Join(dir, "evidence")
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "/bin/sh", script, "run-local")
			// Do not inherit any live credential/CI/approval variables.
			cmd.Env = []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "RHIZA_RUN_ID=" + run, "RHIZA_NAMESPACE=" + namespace,
				"RHIZA_AUTH_CREATION_SHA=" + harness, "RHIZA_HARNESS_SHA=" + actualHarness, "RHIZA_APPLICATION_SHA=" + harness,
				"RHIZA_EXECUTION_GO=" + actualHarness + ":" + run, "RHIZA_APPROVED_CAP_KRW=10000", "RHIZA_BOOTSTRAP_UID=synthetic-uid",
				"RHIZA_HOST_IMAGE=ghcr.io/mrchypark/rhiza-sql@sha256:" + strings.Repeat("a", 64),
				"RHIZA_METADATA_IMAGE=gcr.io/google.com/cloudsdktool/google-cloud-cli@sha256:" + strings.Repeat("b", 64),
				"RHIZA_NODE_A=gke-ied-cluster-fixture-a", "RHIZA_NODE_B=gke-ied-cluster-fixture-b", "RHIZA_NODE_C=gke-ied-cluster-fixture-c",
				"RHIZA_OUTPUT=" + out, "RHIZA_LOCAL_RUNTIME_KUBECONFIG=" + kubeconfig,
				"RHIZA_LOCAL_API_SERVER=https://127.0.0.1", "RHIZA_LOCAL_CA_DATA=synthetic-pinned-ca",
				"RHIZA_AUTH_STARTED_EPOCH=" + strconv.FormatInt(now, 10), "RHIZA_AUTH_EXPIRES=" + expiry, "RHIZA_LOCAL_TOKEN_EXPIRES=" + expiry,
				"FAKE_NOW=" + strconv.FormatInt(now, 10), "FAKE_CLOCK=" + filepath.Join(dir, "clock"),
				"FAKE_SCENARIO=" + scenario, "FAKE_CALLS=" + filepath.Join(dir, "calls"), "FAKE_CHILD=" + filepath.Join(dir, "child"),
				"FAKE_PARENT=" + filepath.Join(dir, "parent"), "FAKE_SIGNAL_READY=" + filepath.Join(dir, "signal-ready")}
			if scenario == "fake-ci" {
				cmd.Env = append(cmd.Env, "GITHUB_ACTIONS=true")
			}
			var captured bytes.Buffer
			cmd.Stdout, cmd.Stderr = &captured, &captured
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			write(filepath.Join(dir, "parent"), strconv.Itoa(cmd.Process.Pid), 0600)
			if scenario == "watchdog" {
				deadline := time.Now().Add(5 * time.Second)
				for {
					if _, err := os.Stat(filepath.Join(dir, "signal-ready")); err == nil {
						if err := syscall.Kill(cmd.Process.Pid, syscall.SIGUSR1); err != nil {
							t.Fatal("owned runtime signal failed")
						}
						break
					}
					if time.Now().After(deadline) {
						_ = cmd.Process.Kill()
						_ = cmd.Wait()
						t.Fatal("runtime did not arm its owned watchdog")
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			err = cmd.Wait()
			output := captured.Bytes()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || ctx.Err() != nil {
				t.Fatalf("runtime did not fail within its bounded test: err=%v context=%v", err, ctx.Err())
			}
			if bytes.Contains(output, []byte("synthetic-offline-token")) || bytes.Contains(output, []byte("synthetic-rejected-token")) {
				t.Fatal("rejected credential content disclosed")
			}
			want := 1
			if scenario == "command-failure" {
				want = 53
			}
			if scenario == "deadline" || scenario == "watchdog" {
				want = 124
			}
			if exited.ExitCode() != want {
				t.Fatalf("runtime exit=%d want=%d diagnostic=%s", exited.ExitCode(), want, output)
			}
			if scenario != "deadline" && scenario != "command-failure" && scenario != "watchdog" {
				reason := "local kubeconfig must be one pinned tokenFile-only identity"
				switch scenario {
				case "token-permissions":
					reason = "private credential ownership/permissions required"
				case "wrong-identity":
					reason = "local runtime server identity mismatch"
				case "fake-ci":
					reason = "local entry must not impersonate CI"
				case "expiry":
					reason = "fixed 55-minute scope/preparation/runtime/cleanup reserve invalid"
				}
				if !bytes.Contains(output, []byte(reason)) {
					t.Fatalf("rejected at the wrong boundary: %s", output)
				}
				calls, _ := os.ReadFile(filepath.Join(dir, "calls"))
				if bytes.Contains(calls, []byte("create")) {
					t.Fatal("rejected local entry attempted a resource write")
				}
				return
			}
			for _, name := range []string{"workload.exit", "root.exit"} {
				value, err := os.ReadFile(filepath.Join(out, name))
				if err != nil || strings.TrimSpace(string(value)) != strconv.Itoa(want) {
					t.Fatalf("original exit not preserved in %s", name)
				}
			}
			watchdog, err := os.ReadFile(filepath.Join(out, "watchdog.pid"))
			if err != nil {
				t.Fatal("watchdog ownership evidence absent")
			}
			pid, err := strconv.Atoi(strings.TrimSpace(string(watchdog)))
			if err != nil || syscall.Kill(pid, 0) == nil {
				t.Fatal("owned watchdog not reaped before sealing")
			}
			verify := exec.Command("shasum", "-a", "256", "-c", "SHA256SUMS")
			verify.Dir = out
			if err := verify.Run(); err != nil {
				t.Fatal("runtime seal does not verify")
			}
			if scenario == "deadline" {
				child, err := os.ReadFile(filepath.Join(dir, "child"))
				if err != nil {
					t.Fatal("actual timed child was not started")
				}
				pid, err := strconv.Atoi(strings.TrimSpace(string(child)))
				if err != nil || syscall.Kill(pid, 0) == nil {
					t.Fatal("timed command's owned child escaped timeout")
				}
			}
		})
	}
}

func TestObjectStoreCountersUnavailable(t *testing.T) {
	response := httptest.NewRecorder()
	objectStoreCounters(func() (rhiza.ObjectStoreStats, bool) { return rhiza.ObjectStoreStats{}, false }).ServeHTTP(response,
		httptest.NewRequest(http.MethodGet, "/qualification/object-store", nil))
	if response.Code != http.StatusServiceUnavailable || response.Body.String() != "object store unavailable\n" {
		t.Fatalf("unexpected unavailable response: %d %q", response.Code, response.Body.String())
	}
}
