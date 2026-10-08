package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
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
	const syntheticFixtureRevision = "1111111111111111111111111111111111111111"
	for _, scenario := range []string{"embedded-token", "exec", "alternate-context", "token-permissions", "wrong-identity", "fake-ci", "head-mismatch", "expiry", "command-failure", "deadline", "watchdog", "parser-timeout", "parser-argument-error"} {
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
			mockFixtureRevision := syntheticFixtureRevision
			if scenario == "head-mismatch" {
				mockFixtureRevision = strings.Repeat("2", 40)
			}
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
			write(filepath.Join(bin, "git"), `#!/bin/sh
[ "$#" = 4 ] && [ "$1" = -C ] && [ "$2" = "$FAKE_GIT_ROOT" ] &&
 [ "$3" = rev-parse ] && [ "$4" = HEAD ] || exit 91
printf '%s\n' "$FAKE_GIT_REVISION"
`, 0700)
			write(filepath.Join(bin, "kubectl"), `#!/bin/sh
printf 'mock-entry\n' >> "$FAKE_CONFIG_EVENTS"
if [ "$FAKE_SCENARIO" = parser-argument-error ]; then set -- --unexpected "$@"; fi
case "$1" in --kubeconfig=*) config=${1#*=}; shift ;; *) exit 92 ;; esac
[ "$1" = "--context=gke_patch2-the-new-era_asia-northeast3_ied-cluster" ] || exit 93
shift
[ "$1" = "--namespace=rhiza-v0191-20261008-a1b2c3d4" ] || exit 94
shift
case "$1" in --request-timeout=*) shift ;; esac
printf 'args-ok\n' >> "$FAKE_CONFIG_EVENTS"
printf '%s\n' "$1" >> "$FAKE_CALLS"
case "$1 $2" in
 "config view")
  printf 'config-view-start\n' >> "$FAKE_CONFIG_EVENTS"
  if [ "$FAKE_SCENARIO" = parser-timeout ]; then
   /bin/sleep 30 & child=$!
   printf '%s\n' "$child" > "$FAKE_CHILD"
   trap 'kill "$child" 2>/dev/null || true; wait "$child" 2>/dev/null || true; exit 143' TERM
   wait "$child"
  fi
  cat "$config"
  code=$?
  printf 'config-view-end exit=%s\n' "$code" >> "$FAKE_CONFIG_EVENTS"
  exit "$code" ;;
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
				"RHIZA_AUTH_CREATION_SHA=" + harness, "RHIZA_HARNESS_SHA=" + syntheticFixtureRevision, "RHIZA_APPLICATION_SHA=" + harness,
				"RHIZA_EXECUTION_GO=" + syntheticFixtureRevision + ":" + run, "RHIZA_APPROVED_CAP_KRW=10000", "RHIZA_BOOTSTRAP_UID=synthetic-uid",
				"RHIZA_HOST_IMAGE=ghcr.io/mrchypark/rhiza-sql@sha256:" + strings.Repeat("a", 64),
				"RHIZA_METADATA_IMAGE=gcr.io/google.com/cloudsdktool/google-cloud-cli@sha256:" + strings.Repeat("b", 64),
				"RHIZA_NODE_A=gke-ied-cluster-fixture-a", "RHIZA_NODE_B=gke-ied-cluster-fixture-b", "RHIZA_NODE_C=gke-ied-cluster-fixture-c",
				"RHIZA_OUTPUT=" + out, "RHIZA_LOCAL_RUNTIME_KUBECONFIG=" + kubeconfig,
				"RHIZA_LOCAL_API_SERVER=https://127.0.0.1", "RHIZA_LOCAL_CA_DATA=synthetic-pinned-ca",
				"RHIZA_AUTH_STARTED_EPOCH=" + strconv.FormatInt(now, 10), "RHIZA_AUTH_EXPIRES=" + expiry, "RHIZA_LOCAL_TOKEN_EXPIRES=" + expiry,
				"FAKE_NOW=" + strconv.FormatInt(now, 10), "FAKE_CLOCK=" + filepath.Join(dir, "clock"),
				"FAKE_SCENARIO=" + scenario, "FAKE_CALLS=" + filepath.Join(dir, "calls"), "FAKE_CHILD=" + filepath.Join(dir, "child"),
				"FAKE_CONFIG_EVENTS=" + filepath.Join(dir, "config-events"),
				"FAKE_GIT_ROOT=" + filepath.Dir(script) + "/../..", "FAKE_GIT_REVISION=" + mockFixtureRevision,
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
			events, eventErr := os.ReadFile(filepath.Join(dir, "config-events"))
			calls, callsErr := os.ReadFile(filepath.Join(dir, "calls"))
			t.Logf("mock events: %q readError=%v; calls: %q readError=%v", events, eventErr, calls, callsErr)
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
				case "head-mismatch":
					reason = "local harness HEAD mismatch"
				case "parser-argument-error":
					reason = "local kubeconfig parsing failed (exit 92)"
				case "parser-timeout":
					reason = "local kubeconfig parsing failed (exit 124)"
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
				if bytes.Contains(calls, []byte("create")) {
					t.Fatal("rejected local entry attempted a resource write")
				}
				if scenario == "fake-ci" || scenario == "head-mismatch" {
					if !errors.Is(eventErr, os.ErrNotExist) || !errors.Is(callsErr, os.ErrNotExist) {
						t.Fatal("pre-parser rejection unexpectedly invoked the local mock")
					}
				} else if scenario == "parser-argument-error" {
					if eventErr != nil || string(events) != "mock-entry\n" || !errors.Is(callsErr, os.ErrNotExist) {
						t.Fatal("argument rejection did not preserve mock-entry without dispatch")
					}
				} else if eventErr != nil || callsErr != nil || !bytes.HasPrefix(events, []byte("mock-entry\nargs-ok\nconfig-view-start\n")) {
					t.Fatal("local mock entry/dispatch evidence absent or unreadable")
				}
				if scenario == "parser-timeout" {
					if string(events) != "mock-entry\nargs-ok\nconfig-view-start\n" {
						t.Fatal("timed parser did not preserve its start-only events")
					}
					child, err := os.ReadFile(filepath.Join(dir, "child"))
					if err != nil {
						t.Fatal("timed parser's owned child was not recorded")
					}
					pid, err := strconv.Atoi(strings.TrimSpace(string(child)))
					if err != nil || syscall.Kill(pid, 0) == nil {
						t.Fatal("timed parser's owned child was not reaped")
					}
					t.Log("native parser exit124 preserved; owned child reaped")
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
			verify := exec.Command("sha256sum", "-c", "SHA256SUMS")
			verify.Dir = out
			if verifyOutput, err := verify.CombinedOutput(); err != nil {
				if len(verifyOutput) > 1024 {
					verifyOutput = verifyOutput[:1024]
				}
				t.Fatalf("runtime seal does not verify: %v; diagnostic=%s", err, verifyOutput)
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

func TestQualificationHostShutdown(t *testing.T) {
	identity := shutdownIdentity{"namespace", "a1b2c3d4", "pod-uid", "node", "process"}
	for _, scenario := range []string{"complete", "close-error", "close-canceled", "close-timeout", "serve-error", "drain-timeout"} {
		t.Run(scenario, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				<-release
				_, _ = w.Write([]byte("done"))
			}))
			defer server.Close()
			requestDone := make(chan struct{})
			go func() {
				defer close(requestDone)
				response, err := http.Get(server.URL)
				if err == nil {
					_ = response.Body.Close()
				}
			}()
			<-entered
			closeStarted, closeReleased := make(chan struct{}), make(chan struct{})
			defer close(closeReleased)
			closeDB := func() error {
				close(closeStarted)
				if scenario == "close-timeout" {
					<-closeReleased
				}
				if scenario == "close-error" {
					return errors.New("synthetic close failure")
				}
				if scenario == "close-canceled" {
					return context.Canceled
				}
				return nil
			}
			var cause error
			if scenario == "serve-error" {
				cause = errors.New("synthetic serve failure")
			}
			var receipt bytes.Buffer
			done := make(chan error, 1)
			drainLimit, closeLimit := 5*time.Second, 5*time.Second
			if scenario == "drain-timeout" {
				drainLimit = 10 * time.Millisecond
			}
			if scenario == "close-timeout" {
				closeLimit = 10 * time.Millisecond
			}
			go func() {
				done <- shutdownHost([]*http.Server{server.Config}, closeDB, identity, &receipt, cause, drainLimit, closeLimit)
			}()
			// An entered request must finish before DB.Close. Release it only
			// after the drain-timeout case has actually returned.
			var err error
			if scenario == "drain-timeout" {
				err = <-done
				select {
				case <-closeStarted:
					t.Fatal("Close raced an undrained HTTP handler")
				default:
				}
				close(release)
			} else {
				select {
				case <-closeStarted:
					t.Fatal("Close preceded HTTP handler completion")
				default:
				}
				close(release)
				err = <-done
			}
			<-requestDone
			var marker map[string]any
			if json.Unmarshal(receipt.Bytes(), &marker) != nil {
				t.Fatal("shutdown receipt missing")
			}
			complete := scenario == "complete"
			if (err == nil) != complete || (marker["event"] == "qualification-host-shutdown-complete") != complete {
				t.Fatalf("incorrect outcome: scenario=%s err=%v marker=%v", scenario, err, marker)
			}
			if marker["pod_uid"] != identity.PodUID || marker["process"] != identity.Process {
				t.Fatal("shutdown identity lost")
			}
		})
	}
}

func TestQualificationHostSIGTERM(t *testing.T) {
	const child = "RHIZA_OFFLINE_SIGTERM_CHILD"
	if os.Getenv(child) == "yes" {
		db, err := rhiza.Open(context.Background(), rhiza.Config{Local: true, ClusterID: "shutdown", NodeID: "child", DataDir: os.Getenv("RHIZA_OFFLINE_DIR")})
		if err != nil {
			t.Fatal(err)
		}
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
		defer stop()
		identity := shutdownIdentity{"namespace", "a1b2c3d4", "pod-uid", "child", "child-process"}
		servers := []*http.Server{{Addr: "127.0.0.1:0", Handler: http.NewServeMux()}, {Addr: "127.0.0.1:0", Handler: http.NewServeMux()}}
		err = serveHost(ctx, servers, identity, os.Stdout)
		if err := shutdownHost(servers, db.Close, identity, os.Stdout, err, time.Second, 5*time.Second); err != nil {
			t.Fatal(err)
		}
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^TestQualificationHostSIGTERM$")
	command.Env = append(os.Environ(), child+"=yes", "RHIZA_OFFLINE_DIR="+t.TempDir())
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	var captured bytes.Buffer
	scanner := bufio.NewScanner(stdout)
	signaled := false
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		captured.Write(line)
		captured.WriteByte('\n')
		var marker map[string]any
		if json.Unmarshal(line, &marker) == nil && marker["event"] == "qualification-host-start" && !signaled {
			signaled = true
			if err := command.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := command.Wait(); err != nil || ctx.Err() != nil || scanner.Err() != nil {
		t.Fatalf("actual SIGTERM child failed: %v %v stderr=%s stdout=%s", err, ctx.Err(), stderr.String(), captured.String())
	}
	if !signaled || !bytes.Contains(captured.Bytes(), []byte(`"event":"qualification-host-shutdown-complete"`)) || !bytes.Contains(captured.Bytes(), []byte(`"db_closed":true`)) {
		t.Fatalf("actual owned DB Close completion absent: %s", captured.String())
	}
}

func TestQualificationShutdownProof(t *testing.T) {
	for _, scenario := range []string{"complete", "missing-completion", "close-failure", "wrong-uid", "wrong-process", "forced-kill", "different-container", "different-image", "restarted", "no-terminal-event", "log-failure", "recreation", "replacement-event", "writers-remain", "watch-timeout", "watch-error", "empty-before", "missing-voter", "replacement-uid", "learner-omitted", "unresolved-learner-uid", "incomplete-startup", "planned-restart-current", "learner-current", "async-terminal", "async-parse-failure"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			const run = "a1b2c3d4"
			ns := "rhiza-v0191-20261008-" + run
			image := "ghcr.io/mrchypark/rhiza-sql@sha256:" + strings.Repeat("a", 64)
			writeJSON := func(name string, value any) {
				t.Helper()
				encoded, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, name), append(encoded, '\n'), 0600); err != nil {
					t.Fatal(err)
				}
			}
			pod := map[string]any{"name": "rhiza-voter-0", "namespace": ns, "run": run, "uid": "owned-uid", "container_id": "containerd://owned", "image": image, "image_id": image, "restart_count": 0}
			status := map[string]any{"name": "rhiza", "containerID": "containerd://owned", "imageID": image, "restartCount": 0,
				"state": map[string]any{"terminated": map[string]any{"exitCode": 0, "signal": 0}}}
			if scenario == "planned-restart-current" {
				// Original PodUID is unchanged; do not demand graceful Close of
				// the historical killed container. Only this current one closes0.
				pod["container_id"], pod["restart_count"] = "containerd://current", 1
				status["containerID"], status["restartCount"] = "containerd://current", 1
			}
			if scenario == "forced-kill" {
				status["state"] = map[string]any{"terminated": map[string]any{"exitCode": 137, "signal": 9}}
			}
			if scenario == "different-container" {
				status["containerID"] = "containerd://replacement"
			}
			if scenario == "different-image" {
				status["imageID"] = "other-image"
			}
			if scenario == "restarted" {
				status["restartCount"] = 1
			}
			event := map[string]any{"type": "DELETED", "object": map[string]any{"metadata": map[string]any{"uid": "owned-uid", "name": "rhiza-voter-0"}, "status": map[string]any{"containerStatuses": []any{status}}}}
			writeJSON("shutdown-watch.json", event)
			if scenario == "replacement-event" || scenario == "watch-error" {
				second := map[string]any{"type": "ADDED", "object": map[string]any{"metadata": map[string]any{"uid": "replacement", "name": "rhiza-voter-0"}}}
				if scenario == "watch-error" {
					second = map[string]any{"type": "ERROR", "object": map[string]any{"code": 410}}
				}
				encoded, _ := json.Marshal(second)
				file, err := os.OpenFile(filepath.Join(dir, "shutdown-watch.json"), os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				_, err = file.Write(append(encoded, '\n'))
				_ = file.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			watchExit := "143\n"
			if scenario == "watch-timeout" {
				watchExit = "124\n"
			}
			if err := os.WriteFile(filepath.Join(dir, "shutdown-watch.exit"), []byte(watchExit), 0600); err != nil {
				t.Fatal(err)
			}
			after := []any{}
			if scenario == "writers-remain" {
				after = append(after, pod)
			}
			writeJSON("shutdown-writers-after.json", map[string]any{"items": after})
			if scenario == "no-terminal-event" {
				writeJSON("shutdown-watch.json", map[string]any{"type": "MODIFIED", "object": map[string]any{"metadata": map[string]any{"uid": "owned-uid"}}})
			}
			replicas := 0
			if scenario == "recreation" {
				replicas = 1
			}
			writeJSON("shutdown-controller.json", map[string]any{"spec": map[string]any{"replicas": replicas}, "status": map[string]any{"replicas": replicas}})
			identity := shutdownIdentity{ns, run, "owned-uid", "rhiza-voter-0", strings.Repeat("b", 32)}
			var logs bytes.Buffer
			if err := hostMarker(&logs, "qualification-host-start", identity, false, false); err != nil {
				t.Fatal(err)
			}
			if scenario == "wrong-uid" {
				identity.PodUID = "replacement"
			}
			if scenario == "wrong-process" {
				identity.Process = strings.Repeat("c", 32)
			}
			completion := "qualification-host-shutdown-complete"
			if scenario == "close-failure" {
				completion = "qualification-host-shutdown-failed"
			}
			if scenario != "missing-completion" {
				if err := hostMarker(&logs, completion, identity, true, scenario != "close-failure"); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(dir, "rhiza-voter-0-shutdown.log"), logs.Bytes(), 0600); err != nil {
				t.Fatal(err)
			}
			logExit := "0\n"
			if scenario == "log-failure" {
				logExit = "1\n"
			}
			if err := os.WriteFile(filepath.Join(dir, "rhiza-voter-0-shutdown-log.exit"), []byte(logExit), 0600); err != nil {
				t.Fatal(err)
			}
			before := []any{pod}
			expected := []any{map[string]any{"name": "rhiza-voter-0", "namespace": ns, "run": run, "uid": "owned-uid"}}
			peers := 2
			if scenario == "learner-current" {
				peers = 3
			}
			for i := 1; i <= peers; i++ {
				name, uid := fmt.Sprintf("rhiza-voter-%d", i), fmt.Sprintf("owned-uid-%d", i)
				if i == 3 {
					name, uid = "rhiza-learner", "created-learner-uid"
				}
				before = append(before, map[string]any{"name": name, "namespace": ns, "run": run, "uid": uid, "container_id": "containerd://owned", "image": image, "image_id": image, "restart_count": 0})
				expected = append(expected, map[string]any{"name": name, "namespace": ns, "run": run, "uid": uid})
				encoded, err := json.Marshal(map[string]any{"type": "DELETED", "object": map[string]any{"metadata": map[string]any{"name": name, "uid": uid}, "status": map[string]any{"containerStatuses": []any{map[string]any{"name": "rhiza", "containerID": "containerd://owned", "imageID": image, "restartCount": 0, "state": map[string]any{"terminated": map[string]int{"exitCode": 0, "signal": 0}}}}}}})
				if err != nil {
					t.Fatal(err)
				}
				file, err := os.OpenFile(filepath.Join(dir, "shutdown-watch.json"), os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				_, err = file.Write(append(encoded, '\n'))
				_ = file.Close()
				if err != nil {
					t.Fatal(err)
				}
				var peerLogs bytes.Buffer
				peerIdentity := shutdownIdentity{ns, run, uid, name, strings.Repeat("b", 32)}
				if err := hostMarker(&peerLogs, "qualification-host-start", peerIdentity, false, false); err != nil {
					t.Fatal(err)
				}
				if err := hostMarker(&peerLogs, "qualification-host-shutdown-complete", peerIdentity, true, true); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, name+"-shutdown.log"), peerLogs.Bytes(), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, name+"-shutdown-log.exit"), []byte("0\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			switch scenario {
			case "empty-before":
				before = []any{}
			case "missing-voter":
				before = before[:2]
			case "replacement-uid":
				pod["uid"] = "replacement"
			case "learner-omitted", "unresolved-learner-uid":
				learnerUID := "created-learner-uid"
				if scenario == "unresolved-learner-uid" {
					learnerUID = ""
				}
				expected = append(expected, map[string]any{"name": "rhiza-learner", "namespace": ns, "run": run, "uid": learnerUID})
			case "incomplete-startup":
				expected, before = expected[:2], before[:2]
			}
			writeJSON("shutdown-before.json", before)
			writeJSON("shutdown-expected.json", expected)
			script, err := filepath.Abs("../run-gcs-postrelease.sh")
			if err != nil {
				t.Fatal(err)
			}
			command := exec.Command("sh", script, "check-shutdown")
			command.Env = []string{"PATH=" + os.Getenv("PATH"), "RHIZA_RUN_ID=" + run, "RHIZA_NAMESPACE=" + ns, "RHIZA_AUTH_CREATION_SHA=" + strings.Repeat("d", 40),
				"RHIZA_HOST_IMAGE=" + image, "RHIZA_METADATA_IMAGE=gcr.io/google.com/cloudsdktool/google-cloud-cli@sha256:" + strings.Repeat("e", 64),
				"RHIZA_NODE_A=gke-ied-cluster-fixture-a", "RHIZA_NODE_B=gke-ied-cluster-fixture-b", "RHIZA_NODE_C=gke-ied-cluster-fixture-c",
				"RHIZA_OUTPUT=" + filepath.Join(dir, "render"), "RHIZA_SHUTDOWN_FIXTURE=" + dir}
			var producer *exec.Cmd
			if scenario == "async-terminal" || scenario == "async-parse-failure" {
				// Logs already have EOF; the independent native stream completes
				// later. Exercise the actual shell waiter, not a completed file.
				ready, err := os.ReadFile(filepath.Join(dir, "shutdown-watch.json"))
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "watch-ready.json"), ready, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(filepath.Join(dir, "shutdown-watch.exit")); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "shutdown-watch.json"), []byte("{\"type\":"), 0600); err != nil {
					t.Fatal(err)
				}
				producer = exec.Command("sh", "-c", `sleep 1; if [ "$2" = async-terminal ]; then cp "$1/watch-ready.json" "$1/shutdown-watch.json"; fi; printf '0\n' > "$1/shutdown-watch.exit"`, "fixture", dir, scenario)
				if err := producer.Start(); err != nil {
					t.Fatal(err)
				}
				command.Env = append(command.Env, "RHIZA_SHUTDOWN_WAIT=yes")
			}
			output, err := command.CombinedOutput()
			if producer != nil {
				if err := producer.Wait(); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(filepath.Join(dir, "shutdown-watch-first-parse.stderr")); err != nil {
					t.Fatal("initial incomplete parser evidence not retained")
				}
			}
			if (err == nil) != (scenario == "complete" || scenario == "planned-restart-current" || scenario == "learner-current" || scenario == "async-terminal") {
				for _, name := range []string{"shutdown-watch.json", "shutdown-watch.exit", "shutdown-watch-proof.json", "shutdown-watch-parse.stderr", "shutdown-watch-first-parse.stderr", "shutdown-watch-first-parse.exit", "shutdown-before.json", "shutdown-expected.json"} {
					data, readErr := os.ReadFile(filepath.Join(dir, name))
					t.Logf("synthetic %s: read=%v bytes=%s", name, readErr, data)
				}
				t.Fatalf("proof scenario=%s result=%v output=%s", scenario, err, output)
			}
		})
	}
}
