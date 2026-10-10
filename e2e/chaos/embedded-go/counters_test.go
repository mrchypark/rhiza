package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
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

func TestBootstrapCustomRoleDeleteReceiptIsStructuredAndValidated(t *testing.T) {
	source, err := os.ReadFile("../bootstrap-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, required := range []string{
		`record "$tag-delete" g iam roles delete "$role" --format=json`,
		`.name==$created[0].name and .name==$current[0].name`,
		`.description==$owner and .deleted==true`,
		`Custom role delete receipt rejected`,
		`record data-delete g iam service-accounts delete "$data_uid"`,
		`wait_deleted_sa data`,
		`GSA deletion identity drift`,
		`ERROR: (gcloud.iam.service-accounts.describe) NOT_FOUND: Unknown service account`,
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("missing structured custom-role rollback check %q", required)
		}
	}
}

func TestBootstrapGSADeleteWaitIsBoundedAndFailClosed(t *testing.T) {
	source, err := os.ReadFile("../bootstrap-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(source, []byte(`timeout --kill-after=5s 415s /bin/sh "$script_dir/bootstrap-gcs-postrelease.sh"`)) {
		t.Fatal("GSA deletion wait lacks the 415-second worker plus five-second monotonic kill bound")
	}
	const runID, workflowSHA, expiry, admin = "a1b2c3d4", "0123456789abcdef0123456789abcdef01234567", "2099-01-01T00:00:00Z", "fixture-admin@example.invalid"
	owner := "rhiza-postrelease-" + runID + "-" + workflowSHA
	workerEnv := func(dir, scenario string) []string {
		state, err := filepath.EvalSymlinks(dir)
		if err != nil {
			t.Fatal(err)
		}
		return append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "FAKE_DIR="+dir, "FAKE_SCENARIO="+scenario,
			"RHIZA_BOOTSTRAP_GO=I_APPROVE_NEW_SCOPE", "RHIZA_COST_CAP_KRW=10000", "RHIZA_EXPECTED_ADMIN_ACCOUNT="+admin,
			"RHIZA_AUTH_STATE="+state, "RHIZA_RUN_ID="+runID, "RHIZA_WORKFLOW_SHA="+workflowSHA, "RHIZA_AUTH_EXPIRES="+expiry)
	}
	seedState := func(t *testing.T, dir string) {
		t.Helper()
		if err := os.Chmod(dir, 0700); err != nil {
			t.Fatal(err)
		}
		files := map[string]string{
			"attempts":         "0\n",
			"identity.json":    `{"run":"` + runID + `","workflow_sha":"` + workflowSHA + `","expiry":"` + expiry + `","cost_cap_krw":"10000","owner":"` + owner + `"}`,
			"data-create.json": `{"email":"rhiza-v0191-gcs-` + runID + `@patch2-the-new-era.iam.gserviceaccount.com","uniqueId":"fixture-uid","description":"` + owner + `"}`,
			"data.created":     owner + "\n",
		}
		for name, content := range files {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(dir, "kubectl"), []byte("#!/bin/sh\nexit 99\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	for _, scenario := range []struct {
		name           string
		want, attempts int
	}{
		{"eventual-not-found", 0, 2},
		{"identity-drift", 1, 1},
		{"permission-denied", 1, 1},
		{"rate-limit", 1, 1},
		{"server-error", 1, 1},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			dir := t.TempDir()
			gcloud := `#!/bin/sh
set -eu
case "$*" in
 *'config get-value account') printf '%s\n' 'fixture-admin@example.invalid'; exit 0 ;;
esac
 attempt=$(cat "$FAKE_DIR/attempts"); attempt=$((attempt + 1)); printf '%s\n' "$attempt" > "$FAKE_DIR/attempts"
 printf '%s\n' "$*" >> "$FAKE_DIR/calls"
 case "$FAKE_SCENARIO" in
  eventual-not-found)
   if [ "$attempt" = 1 ]; then printf '%s\n' '{"email":"rhiza-v0191-gcs-a1b2c3d4@patch2-the-new-era.iam.gserviceaccount.com","uniqueId":"fixture-uid","description":"rhiza-postrelease-a1b2c3d4-0123456789abcdef0123456789abcdef01234567"}'; exit 0; fi
   printf '%s\n' 'ERROR: (gcloud.iam.service-accounts.describe) NOT_FOUND: Unknown service account. This command is authenticated as [fixture@example.invalid] which is the active account specified by the [core/account] property' >&2; exit 1 ;;
  identity-drift) printf '%s\n' '{"email":"other@example.invalid","uniqueId":"fixture-uid","description":"rhiza-postrelease-a1b2c3d4-0123456789abcdef0123456789abcdef01234567"}'; exit 0 ;;
  permission-denied) printf '%s\n' 'ERROR: PERMISSION_DENIED' >&2; exit 1 ;;
  rate-limit) printf '%s\n' 'ERROR: HTTPError 429' >&2; exit 1 ;;
  server-error) printf '%s\n' 'ERROR: HTTPError 503' >&2; exit 1 ;;
 esac
`
			seedState(t, dir)
			if err := os.WriteFile(filepath.Join(dir, "gcloud"), []byte(gcloud), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "sleep"), []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("/bin/sh", "../bootstrap-gcs-postrelease.sh", "gsa-delete-wait-worker", "data")
			command.Env = workerEnv(dir, scenario.name)
			output, runErr := command.CombinedOutput()
			code := 0
			if runErr != nil {
				var exited *exec.ExitError
				if !errors.As(runErr, &exited) {
					t.Fatal(runErr)
				}
				code = exited.ExitCode()
			}
			attempts, err := os.ReadFile(filepath.Join(dir, "attempts"))
			if err != nil || code != scenario.want || strings.TrimSpace(string(attempts)) != strconv.Itoa(scenario.attempts) {
				t.Fatalf("exit=%d want=%d attempts=%s want=%d output=%s error=%v", code, scenario.want, attempts, scenario.attempts, output, err)
			}
			calls, err := os.ReadFile(filepath.Join(dir, "calls"))
			if err != nil || bytes.Contains(calls, []byte(" delete ")) || !bytes.Contains(calls, []byte("iam service-accounts describe rhiza-v0191-gcs-a1b2c3d4@patch2-the-new-era.iam.gserviceaccount.com --format=json")) {
				t.Fatalf("worker issued a non-describe operation: %q error=%v", calls, err)
			}
		})
	}
	t.Run("persistent-exists-frozen-backward-clock", func(t *testing.T) {
		dir := t.TempDir()
		gcloud := "#!/bin/sh\ncase \"$*\" in *'config get-value account') printf '%s\\n' 'fixture-admin@example.invalid'; exit 0;; esac\nprintf '%s\\n' \"$*\" >> \"$FAKE_DIR/calls\"\nprintf '%s\\n' '{\"email\":\"rhiza-v0191-gcs-a1b2c3d4@patch2-the-new-era.iam.gserviceaccount.com\",\"uniqueId\":\"fixture-uid\",\"description\":\"" + owner + "\"}'\n"
		date := "#!/bin/sh\nvalue=$(cat \"$FAKE_DIR/clock\")\nprintf '%s\\n' \"$value\"\nprintf '%s\\n' \"$((value - 100))\" > \"$FAKE_DIR/clock\"\n"
		for name, content := range map[string]string{"gcloud": gcloud, "date": date} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0700); err != nil {
				t.Fatal(err)
			}
		}
		seedState(t, dir)
		if err := os.WriteFile(filepath.Join(dir, "clock"), []byte("1000\n"), 0600); err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		command := exec.Command("timeout", "--kill-after=1s", "1s", "/bin/sh", "../bootstrap-gcs-postrelease.sh", "gsa-delete-wait-worker", "data")
		command.Env = workerEnv(dir, "persistent")
		runErr := command.Run()
		var exited *exec.ExitError
		if !errors.As(runErr, &exited) || exited.ExitCode() != 124 || time.Since(started) > 3*time.Second {
			t.Fatalf("outer watchdog failed: error=%v elapsed=%s", runErr, time.Since(started))
		}
		calls, err := os.ReadFile(filepath.Join(dir, "calls"))
		if err != nil || bytes.Contains(calls, []byte(" delete ")) || !bytes.Contains(calls, []byte("iam service-accounts describe")) {
			t.Fatalf("bounded worker issued a non-describe operation: %q error=%v", calls, err)
		}
	})
	for _, unsafe := range []struct {
		name string
		args []string
	}{
		{"path-tag", []string{"gsa-delete-wait-worker", "../outside"}},
		{"arbitrary-account", []string{"gsa-delete-wait-worker", "data", "attacker@example.invalid"}},
	} {
		t.Run("reject-"+unsafe.name, func(t *testing.T) {
			dir := t.TempDir()
			outside := filepath.Join(filepath.Dir(dir), "outside-write")
			gcloud := "#!/bin/sh\nprintf '%s\\n' called >> \"$FAKE_DIR/calls\"\nexit 99\n"
			if err := os.WriteFile(filepath.Join(dir, "gcloud"), []byte(gcloud), 0700); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("/bin/sh", append([]string{"../bootstrap-gcs-postrelease.sh"}, unsafe.args...)...)
			command.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "FAKE_DIR="+dir, "RHIZA_AUTH_STATE="+outside,
				"RHIZA_GSA_DELETE_ACCOUNT=attacker@example.invalid")
			if err := command.Run(); err == nil {
				t.Fatal("unsafe direct worker invocation passed")
			}
			if _, err := os.Stat(filepath.Join(dir, "calls")); !os.IsNotExist(err) {
				t.Fatalf("unsafe invocation reached cloud command: %v", err)
			}
			if _, err := os.Stat(outside); !os.IsNotExist(err) {
				t.Fatalf("unsafe invocation wrote outside validated state: %v", err)
			}
		})
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

func TestPostreleaseCustomRoleReadiness(t *testing.T) {
	source, err := os.ReadFile("../bootstrap-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nnow_epoch() {")
	end := strings.Index(string(source), "\nlocal_token_files() {")
	if start < 0 || end <= start {
		t.Fatal("custom role readiness helpers missing")
	}
	if strings.Count(string(source), "custom_role_deadline=$(($(now_epoch) + 420))") != 1 {
		t.Fatal("single exact 420-second custom role deadline missing")
	}
	helpers := string(source[start:end])
	for _, scenario := range []struct {
		name           string
		deadline, want int
		attempts       int
	}{
		{"transient", 1420, 0, 2},
		{"deleted-omitted", 1420, 0, 1},
		{"forbidden", 1420, 1, 1},
		{"deleted", 1420, 1, 1},
		{"deleted-null", 1420, 1, 1},
		{"deleted-string", 1420, 1, 1},
		{"wrong-name", 1420, 1, 1},
		{"missing-etag", 1420, 1, 1},
		{"wrong-description", 1420, 1, 1},
		{"wrong-stage", 1420, 1, 1},
		{"wrong-permissions", 1420, 1, 1},
		{"deadline", 1004, 1, 1},
		{"inflight-timeout", 1420, 1, 0},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, value := range map[string]string{"clock": "1000\n", "attempts": "0\n"} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0600); err != nil {
					t.Fatal(err)
				}
			}
			fixture := `
set -eu
project=fixture-project; owner=fixture-owner; RHIZA_AUTH_STATE=$FAKE_DIR
now_epoch() { cat "$FAKE_DIR/clock"; }
sleep() { now=$(now_epoch); printf '%s\n' "$((now + $1))" > "$FAKE_DIR/clock"; printf '%s\n' "$1" >> "$FAKE_DIR/sleeps"; }
die() { printf '%s\n' "$*" >&2; return 1; }
timeout() {
 [ "$1" = --signal=KILL ] || return 98
 seconds=${2%s}; shift 2; printf '%s\n' "$seconds" >> "$FAKE_DIR/timeouts"
 [ "$FAKE_SCENARIO" != inflight-timeout ] || return 124
 "$@"
}
gcloud() {
 [ "$1" = --project=fixture-project ] && [ "$2" = --quiet ] && [ "$3 $4 $5 $6 $7" = 'iam roles describe fixture-role --format=json' ] || return 97
 attempt=$(cat "$FAKE_DIR/attempts"); attempt=$((attempt + 1)); printf '%s\n' "$attempt" > "$FAKE_DIR/attempts"
 case "$FAKE_SCENARIO:$attempt" in transient:1|deadline:1) printf 'NOT_FOUND\n' >&2; return 1;; forbidden:1) printf 'PERMISSION_DENIED\n' >&2; return 1;; esac
 name=projects/fixture-project/roles/fixture-role; etag=etag; description=fixture-owner; stage=GA; deleted=false; permissions='["p.one","p.two"]'
 case "$FAKE_SCENARIO" in deleted) deleted=true;; wrong-name) name=projects/fixture-project/roles/other;; missing-etag) etag=;; wrong-description) description=other;; wrong-stage) stage=BETA;; wrong-permissions) permissions='["p.one"]';; esac
 jq -n --arg scenario "$FAKE_SCENARIO" --arg name "$name" --arg etag "$etag" --arg description "$description" --arg stage "$stage" --argjson deleted "$deleted" --argjson permissions "$permissions" '
   {name:$name,etag:$etag,description:$description,stage:$stage,deleted:$deleted,includedPermissions:$permissions} |
   if $scenario=="deleted-omitted" then del(.deleted) elif $scenario=="deleted-null" then .deleted=null elif $scenario=="deleted-string" then .deleted="false" else . end'
}
`
			command := exec.Command("/bin/sh", "-c", helpers+fixture+"\nwait_custom_role fixture fixture-role p.two,p.one "+strconv.Itoa(scenario.deadline))
			command.Env = append(os.Environ(), "FAKE_DIR="+dir, "FAKE_SCENARIO="+scenario.name)
			output, runErr := command.CombinedOutput()
			code := 0
			if runErr != nil {
				var exited *exec.ExitError
				if !errors.As(runErr, &exited) {
					t.Fatal(runErr)
				}
				code = exited.ExitCode()
			}
			attempts, readErr := os.ReadFile(filepath.Join(dir, "attempts"))
			if readErr != nil || code != scenario.want || strings.TrimSpace(string(attempts)) != strconv.Itoa(scenario.attempts) {
				t.Fatalf("exit=%d want=%d attempts=%s want=%d output=%s error=%v", code, scenario.want, attempts, scenario.attempts, output, readErr)
			}
			timeouts, timeoutErr := os.ReadFile(filepath.Join(dir, "timeouts"))
			if timeoutErr != nil {
				t.Fatal(timeoutErr)
			}
			firstTimeout := strings.Split(strings.TrimSpace(string(timeouts)), "\n")[0]
			wantTimeout := "420"
			if scenario.name == "deadline" {
				wantTimeout = "4"
			}
			if firstTimeout != wantTimeout {
				t.Fatalf("bounded timeout=%s want=%s", firstTimeout, wantTimeout)
			}
			for attempt := 1; attempt <= scenario.attempts; attempt++ {
				for _, suffix := range []string{"json", "stderr", "exit"} {
					if _, err := os.Stat(filepath.Join(dir, fmt.Sprintf("fixture-readiness-%d.%s", attempt, suffix))); err != nil {
						t.Fatalf("attempt evidence missing: %v", err)
					}
				}
			}
		})
	}
}

func TestPostreleaseSHA256ToolFallback(t *testing.T) {
	for _, script := range []string{"../bootstrap-gcs-postrelease.sh", "../run-gcs-postrelease.sh"} {
		source, err := os.ReadFile(script)
		if err != nil {
			t.Fatal(err)
		}
		start := strings.Index(string(source), "\nsha256_file() {\n")
		end := strings.Index(string(source)[start+1:], "\n}\n")
		if start < 0 || end < 0 {
			t.Fatalf("%s: SHA-256 helper missing", script)
		}
		helper := string(source)[start+1 : start+1+end+3]
		for _, scenario := range []string{"sha256sum-only", "shasum-only", "neither", "malformed"} {
			t.Run(filepath.Base(script)+"/"+scenario, func(t *testing.T) {
				dir := t.TempDir()
				tool := "sha256sum"
				body := "printf '%064d  %s\\n' 0 \"$1\"\n"
				if scenario == "shasum-only" {
					tool = "shasum"
					body = "[ \"$1 $2\" = '-a 256' ] || exit 9\nprintf '%064d  %s\\n' 0 \"$3\"\n"
				} else if scenario == "malformed" {
					body = "printf 'not-a-digest  %s\\n' \"$1\"\n"
				}
				if scenario != "neither" {
					if err := os.WriteFile(filepath.Join(dir, tool), []byte("#!/bin/sh\n"+body), 0700); err != nil {
						t.Fatal(err)
					}
				}
				command := exec.Command("/bin/sh", "-c", helper+"\nsha256_file fixture")
				command.Env = []string{"PATH=" + dir}
				output, err := command.CombinedOutput()
				wantSuccess := scenario == "sha256sum-only" || scenario == "shasum-only"
				if (err == nil) != wantSuccess {
					t.Fatalf("success=%v output=%q", err == nil, output)
				}
				if wantSuccess && string(output) != strings.Repeat("0", 64)+"\n" {
					t.Fatalf("unexpected digest %q", output)
				}
			})
		}
	}
}

func TestQualificationCurlHelper(t *testing.T) {
	if os.Getenv("RHIZA_CURL_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	if os.Getenv("FAKE_SCENARIO") != "bind-conflict" {
		if _, err := os.Stat(filepath.Join(os.Getenv("FAKE_DIR"), "listening")); err != nil {
			t.Fatal(err)
		}
		return
	}
	marker := filepath.Join(os.Getenv("FAKE_DIR"), "bind-exited")
	for attempt := 0; attempt < 100; attempt++ {
		if _, err := os.Stat(marker); err == nil {
			time.Sleep(time.Second)
			for _, arg := range os.Args {
				if !strings.HasPrefix(arg, "http://") {
					continue
				}
				client := &http.Client{Timeout: 2 * time.Second}
				response, err := client.Get(arg)
				if err != nil {
					t.Fatal(err)
				}
				response.Body.Close()
				if response.StatusCode != http.StatusOK {
					t.Fatalf("health status=%d", response.StatusCode)
				}
				if err := os.WriteFile(filepath.Join(os.Getenv("FAKE_DIR"), "health-ok"), []byte("ok\n"), 0600); err != nil {
					t.Fatal(err)
				}
				return
			}
			t.Fatal("health URL missing")
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("bind failure did not complete")
}

func TestQualificationArchiveHeadAndPortForwardLifecycle(t *testing.T) {
	sourceBytes, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(sourceBytes)
	for _, forbidden := range []string{"archive/HEAD", "HEAD.json", "jq -e --slurpfile current"} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("binary archive head regressed to JSON/uppercase lookup: %s", forbidden)
		}
	}
	metadata := `gcloud storage objects describe "${storage}${cluster}/archive/head.bin" '--format=json(bucket,name,generation,size)'`
	metadataAt, learnerAt := strings.Index(source, metadata), strings.Index(source, "run cold-create")
	readbackAt, metadataAfterAt := strings.Index(source, "readback 18083 local"), strings.Index(source, "run head-metadata-after")
	if metadataAt < 0 || learnerAt <= metadataAt || readbackAt <= learnerAt || metadataAfterAt <= readbackAt {
		t.Fatal("head.bin metadata proof or required fresh-learner semantic proof missing")
	}
	validationStart := strings.Index(source, "\nvalidate_archive_head_metadata() {\n")
	validationEnd := strings.Index(source, "\nprivate_file() {\n")
	if validationStart < 0 || validationEnd <= validationStart {
		t.Fatal("archive head metadata validators missing")
	}
	validators := source[validationStart:validationEnd]
	metadataDir := t.TempDir()
	valid := `{"bucket":"rhiza-v070-chaos-ied-20260811","generation":"1","name":"prefix/cluster/archive/head.bin","size":"132"}`
	metadataCases := []struct {
		name, value string
		valid       bool
	}{
		{"valid", valid, true},
		{"wrong-bucket", strings.Replace(valid, "rhiza-v070-chaos-ied-20260811", "other", 1), false},
		{"wrong-name", strings.Replace(valid, "prefix/cluster/archive/head.bin", "prefix/cluster/archive/HEAD", 1), false},
		{"numeric-generation", strings.Replace(valid, `"generation":"1"`, `"generation":1`, 1), false},
		{"leading-zero-generation", strings.Replace(valid, `"generation":"1"`, `"generation":"01"`, 1), false},
		{"numeric-size", strings.Replace(valid, `"size":"132"`, `"size":132`, 1), false},
		{"leading-zero-size", strings.Replace(valid, `"size":"132"`, `"size":"0132"`, 1), false},
		{"small", strings.Replace(valid, `"size":"132"`, `"size":"131"`, 1), false},
		{"large", strings.Replace(valid, `"size":"132"`, `"size":"8388633"`, 1), false},
		{"extra-field", strings.TrimSuffix(valid, "}") + `,"etag":"secret"}`, false},
	}
	for _, tc := range metadataCases {
		t.Run("metadata-"+tc.name, func(t *testing.T) {
			path := filepath.Join(metadataDir, tc.name+".json")
			if err := os.WriteFile(path, []byte(tc.value), 0600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("/bin/sh", "-c", "prefix=prefix/; cluster=cluster; "+validators+"\nvalidate_archive_head_metadata \"$1\"", "fixture", path)
			err := command.Run()
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%t error=%v", tc.valid, err)
			}
		})
	}
	before, after := filepath.Join(metadataDir, "before.json"), filepath.Join(metadataDir, "after.json")
	if err := os.WriteFile(before, []byte(valid), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, value string
		valid       bool
	}{{"same", valid, true}, {"generation-change", strings.Replace(valid, `"generation":"1"`, `"generation":"2"`, 1), false}, {"size-change", strings.Replace(valid, `"size":"132"`, `"size":"133"`, 1), false}} {
		if err := os.WriteFile(after, []byte(tc.value), 0600); err != nil {
			t.Fatal(err)
		}
		command := exec.Command("/bin/sh", "-c", validators+"\nsame_archive_head_metadata \"$1\" \"$2\"", "fixture", before, after)
		if err := command.Run(); (err == nil) != tc.valid {
			t.Fatalf("metadata comparison %s valid=%t error=%v", tc.name, tc.valid, err)
		}
	}

	stopStart := strings.Index(source, "\nstop_forward() {\n")
	stopEnd := strings.Index(source, "\npf_pids=''\n")
	start := strings.Index(source, "\nforward() {\n")
	end := strings.Index(source[start+1:], "\nforward rhiza-voter-0")
	if stopStart < 0 || stopEnd <= stopStart || start < 0 || end < 0 {
		t.Fatal("port-forward helper missing")
	}
	functions := source[stopStart:stopEnd] + source[start:start+1+end]
	for _, scenario := range []struct {
		name string
		want int
	}{{"complete", 0}, {"uid-change", 1}, {"container-change", 1}, {"image-change", 1}, {"wrong-stable-image", 1}, {"extra-sidecar", 1}, {"forward-death", 1}, {"bind-conflict", 1}, {"stubborn-child", 0}} {
		t.Run("forward-"+scenario.name, func(t *testing.T) {
			dir := t.TempDir()
			selectedPort := 0
			var occupied net.Listener
			for _, candidate := range []int{18080, 18081, 18082, 18083} {
				listener, listenErr := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", candidate))
				if listenErr != nil {
					continue
				}
				selectedPort = candidate
				if scenario.name == "bind-conflict" {
					occupied = listener
				} else {
					listener.Close()
				}
				break
			}
			if selectedPort == 0 {
				t.Fatal("no supported production port available")
			}
			bin := filepath.Join(dir, "bin")
			if err := os.Mkdir(bin, 0700); err != nil {
				t.Fatal(err)
			}
			write := func(name, content string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(bin, name), []byte(content), 0700); err != nil {
					t.Fatal(err)
				}
			}
			testBinary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			write("curl", "#!/bin/sh\nexec \"$FAKE_TEST_BINARY\" -test.run '^TestQualificationCurlHelper$' -- \"$@\"\n")
			write("kubectl", `#!/bin/sh
case "$*" in *" port-forward pod/rhiza-voter-0 $FAKE_PORT:8080 "*) ;; *) exit 97 ;; esac
[ "$FAKE_SCENARIO" != forward-death ] || exit 53
if [ -e "$FAKE_DIR/listening" ]; then
  owner=$(cat "$FAKE_DIR/listening")
  if [ "$owner" = forced ] || kill -0 "$owner" 2>/dev/null; then touch "$FAKE_DIR/address-in-use"; touch "$FAKE_DIR/bind-exited"; exit 98; fi
  rm -f "$FAKE_DIR/listening"
fi
printf '%s\n' "$$" > "$FAKE_DIR/listening"; printf 'start\n' >> "$FAKE_DIR/events"
trap '[ "$FAKE_SCENARIO" = stubborn-child ] || { rm -f "$FAKE_DIR/listening"; printf "reaped\n" >> "$FAKE_DIR/events"; exit 0; }' TERM INT
while :; do sleep 1; done
`)
			fixture := `
set -eu
mode=run; out=$FAKE_DIR; pf_pids=''; context=synthetic; ns=synthetic; started=$(date +%s); pf_generation=0
RHIZA_HOST_IMAGE=ghcr.io/mrchypark/rhiza-sql@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
port=$FAKE_PORT
die() { printf '%s\n' "$*" >&2; exit 1; }
k() {
  [ "$1" = get ] || return 97
  count=0; [ ! -f "$FAKE_DIR/gets" ] || count=$(cat "$FAKE_DIR/gets"); count=$((count + 1)); printf '%s\n' "$count" > "$FAKE_DIR/gets"
  uid=uid-a; container=container-a; image=$RHIZA_HOST_IMAGE; spec_extra=''; status_extra=''
  [ "$FAKE_SCENARIO" != wrong-stable-image ] || image=ghcr.io/mrchypark/rhiza-sql@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  if [ "$FAKE_SCENARIO" = extra-sidecar ]; then spec_extra=',{"name":"sidecar","image":"sidecar"}'; status_extra=',{"name":"sidecar","containerID":"sidecar","imageID":"sidecar"}'; fi
  [ "$count" = 1 ] || case "$FAKE_SCENARIO" in uid-change) uid=uid-b;; container-change) container=container-b;; image-change) image=image-b;; esac
  printf '{"metadata":{"name":"rhiza-voter-0","uid":"%s"},"spec":{"containers":[{"name":"rhiza","image":"%s"}%s]},"status":{"conditions":[{"type":"Ready","status":"True"}],"containerStatuses":[{"name":"rhiza","containerID":"%s","imageID":"host@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}%s]}}\n' "$uid" "$image" "$spec_extra" "$container" "$status_extra"
}
` + functions + `
trap 'for pid in $pf_pids; do stop_forward "$pid" || true; done' EXIT
[ "$FAKE_SCENARIO" != bind-conflict ] || printf 'forced\n' > "$FAKE_DIR/listening"
forward rhiza-voter-0 "$port"
case "$port" in 18080) first=$pf_18080;; 18081) first=$pf_18081;; 18082) first=$pf_18082;; 18083) first=$pf_18083;; esac
[ "$FAKE_SCENARIO" != complete ] && [ "$FAKE_SCENARIO" != stubborn-child ] && exit 0
forward rhiza-voter-0 "$port"
case "$port" in 18080) second=$pf_18080;; 18081) second=$pf_18081;; 18082) second=$pf_18082;; 18083) second=$pf_18083;; esac
[ "$first" != "$second" ]
! kill -0 "$first" 2>/dev/null
[ "$(printf '%s\n' $pf_pids | wc -l | tr -d ' ')" = 1 ]
stop_forward "$second"
[ "$(grep -c '^start$' "$FAKE_DIR/events")" = 2 ]
[ ! -e "$FAKE_DIR/address-in-use" ]
`
			var occupiedServer *http.Server
			if scenario.name == "bind-conflict" {
				occupiedServer = &http.Server{Handler: http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
					if request.URL.Path != "/healthz" {
						http.NotFound(response, request)
						return
					}
					response.WriteHeader(http.StatusOK)
				})}
				go func() { _ = occupiedServer.Serve(occupied) }()
				defer occupiedServer.Close()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "/bin/sh", "-c", fixture)
			command.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "FAKE_DIR="+dir, "FAKE_SCENARIO="+scenario.name,
				"FAKE_TEST_BINARY="+testBinary, "RHIZA_CURL_HELPER=1", "FAKE_PORT="+strconv.Itoa(selectedPort))
			output, err := command.CombinedOutput()
			code := 0
			if err != nil {
				var exited *exec.ExitError
				if !errors.As(err, &exited) || ctx.Err() != nil {
					t.Fatalf("fixture did not terminate: %v output=%s", err, output)
				}
				code = exited.ExitCode()
			}
			if (code == 0) != (scenario.want == 0) {
				t.Fatalf("exit=%d want=%d output=%s", code, scenario.want, output)
			}
			if scenario.name == "bind-conflict" {
				health, healthErr := os.ReadFile(filepath.Join(dir, "health-ok"))
				if healthErr != nil || string(health) != "ok\n" {
					t.Fatalf("occupied listener health was not proven before PID failure: %q %v", health, healthErr)
				}
			}
		})
	}
}

// Exercise the actual shell entry with synthetic files and a non-networking
// kubectl. No token is minted, no application opens, and no cloud call is made.
func TestQualificationLocalRuntime(t *testing.T) {
	nativeTimeout, err := exec.LookPath("timeout")
	if err != nil {
		t.Fatal("native GNU timeout required for the local runtime tests")
	}
	nativeTimeout, err = filepath.Abs(nativeTimeout)
	if err != nil {
		t.Fatal(err)
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
	for _, scenario := range []string{"embedded-token", "exec", "alternate-context", "token-permissions", "wrong-identity", "fake-ci", "head-mismatch", "expiry", "command-failure", "deadline", "watchdog", "parser-timeout", "parser-argument-error", "runtime-kubeconfig-path", "cluster-receipt-changed", "workload-identity-receipt-changed", "workload-identity-wrong-binding", "workload-identity-extra-member", "workload-identity-extra-binding", "workload-identity-preexisting-binding", "workload-identity-recreated-gsa", "workload-identity-mode-false", "workload-identity-mode-absent", "workload-identity-mode-other", "metadata-worker-entrypoint", "metadata-credential-failure", "metadata-wrong-service-account", "metadata-gce-mode", "outside-prefix-auth-failure", "outside-prefix-accessible", "inside-prefix-auth-failure", "voter-create-failure", "voter-create-unknown"} {
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
			const harness = "315a5ee6bc6635b4ff4f85b9534afa4abe537c79"
			mockFixtureRevision := syntheticFixtureRevision
			if scenario == "head-mismatch" {
				mockFixtureRevision = strings.Repeat("2", 40)
			}
			const contextName = "gke_patch2-the-new-era_asia-northeast3_ied-cluster"
			namespace := "rhiza-v0191-20261008-" + run
			metadata, err := json.Marshal(map[string]any{"kind": "Pod", "metadata": map[string]any{"name": "rhiza-metadata", "namespace": namespace,
				"uid": "metadata-uid", "resourceVersion": "7", "labels": map[string]string{"chaos.rhiza.io/run": run}},
				"spec": map[string]any{"serviceAccountName": "rhiza-gcs", "nodeName": "gke-ied-cluster-fixture-a",
					"containers": []any{map[string]string{"name": "metadata", "image": "gcr.io/google.com/cloudsdktool/google-cloud-cli@sha256:" + strings.Repeat("b", 64)}}}})
			if err != nil {
				t.Fatal(err)
			}
			write(filepath.Join(dir, "metadata.json"), string(metadata), 0600)
			clusterMetadata := `{"name":"ied-cluster","location":"asia-northeast3","endpoint":"example.invalid","masterAuth":{"clusterCaCertificate":"synthetic-ca"},"nodePools":[{"name":"fixture","config":{"workloadMetadataConfig":{"mode":"GKE_METADATA"}}}]}`
			if scenario == "metadata-gce-mode" {
				clusterMetadata = `{"name":"ied-cluster","location":"asia-northeast3","endpoint":"example.invalid","masterAuth":{"clusterCaCertificate":"synthetic-ca"},"nodePools":[{"name":"fixture","config":{"workloadMetadataConfig":{"mode":"GCE_METADATA"}}}]}`
			}
			clusterMetadataPath := filepath.Join(dir, "mint-cluster.json")
			write(clusterMetadataPath, clusterMetadata, 0600)
			clusterMetadataSum := sha256.Sum256([]byte(clusterMetadata))
			write(filepath.Join(dir, "mint-cluster.sha256"), fmt.Sprintf("%x\n", clusterMetadataSum), 0600)
			if scenario == "cluster-receipt-changed" {
				write(clusterMetadataPath, clusterMetadata+"\n", 0600)
			}
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
			kubeconfig := filepath.Join(dir, "runtime.kubeconfig")
			write(kubeconfig, string(encoded), 0600)
			runtimeKubeconfig := kubeconfig
			if scenario == "runtime-kubeconfig-path" {
				runtimeKubeconfig = filepath.Join(dir, "other.kubeconfig")
				write(runtimeKubeconfig, string(encoded), 0600)
			}
			physicalDir, err := filepath.EvalSymlinks(dir)
			if err != nil {
				t.Fatal(err)
			}
			physicalKubeconfig := filepath.Join(physicalDir, "runtime.kubeconfig")
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
printf '%s\n' "$*" >> "$FAKE_CALLS"
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
  printf '%s\n' '{"metadata":{"uid":"synthetic-uid","labels":{"chaos.rhiza.io/run":"a1b2c3d4"},"annotations":{"rhiza.dev/auth-owner":"rhiza-postrelease-a1b2c3d4-315a5ee6bc6635b4ff4f85b9534afa4abe537c79","chaos-mesh.org/inject":"enabled"}}}' ;;
 "get pods,statefulsets,services,configmaps,networkpolicies,podchaos,networkchaos")
  if [ "$FAKE_SCENARIO" = deadline ]; then printf '%s\n' "$((FAKE_NOW + 1199))" > "$FAKE_CLOCK"; fi
  printf '%s\n' '{"items":[]}' ;;
 "create -f")
  case "$FAKE_SCENARIO" in
   metadata-credential-failure|metadata-wrong-service-account|metadata-gce-mode|outside-prefix-auth-failure|outside-prefix-accessible|inside-prefix-auth-failure|voter-create-failure|voter-create-unknown)
    case "$3" in
     */metadata.yaml) [ "$4 $5" = '-o json' ] || exit 97; cat "$FAKE_METADATA"; exit 0 ;;
     */voters.yaml)
      if [ "$FAKE_SCENARIO" = voter-create-unknown ]; then touch "$FAKE_VOTER_ATTEMPT"; printf '{"kind":"StatefulSet"}'; exit 55; fi
      exit 53 ;;
     *) exit 97 ;;
    esac ;;
  esac
  /bin/sleep 30 & child=$!
  printf '%s\n' "$child" > "$FAKE_CHILD"
  trap 'kill "$child" 2>/dev/null || true; wait "$child" 2>/dev/null || true; exit 143' TERM
  wait "$child" ;;
 "exec rhiza-metadata")
  case "$4" in
   curl)
    case "$*" in
     *'/instance/service-accounts/default/email')
      if [ "$FAKE_SCENARIO" = metadata-wrong-service-account ]; then printf 'node@project.iam.gserviceaccount.com\n200'
      else printf 'rhiza-v0191-gcs-a1b2c3d4@patch2-the-new-era.iam.gserviceaccount.com\n200'; fi ;;
     *'/instance/service-accounts/default/token')
      if [ "$FAKE_SCENARIO" = metadata-credential-failure ]; then printf 'synthetic-metadata-token\n401'
      else printf '{"access_token":"synthetic-metadata-token","token_type":"Bearer","expires_in":100}\n200'; fi ;;
     *) exit 98 ;;
    esac ;;
   gcloud)
    case "$5 $6" in
     'storage cat')
      [ "$FAKE_SCENARIO" != outside-prefix-accessible ] || exit 0
      if [ "$FAKE_SCENARIO" = outside-prefix-auth-failure ]; then printf 'MetadataServerException authentication unavailable\n' >&2
      else printf '403\n' >&2; fi
      exit 1 ;;
     'storage ls')
      if [ "$FAKE_SCENARIO" = inside-prefix-auth-failure ]; then printf '401 UNAUTHENTICATED\n' >&2
      else printf 'matched no objects\n' >&2; fi
      exit 1 ;;
     *) exit 99 ;;
    esac ;;
   *) exit 99 ;;
  esac ;;
 "get pods,statefulsets,deployments,replicasets,daemonsets,jobs,cronjobs,podchaos,networkchaos")
  if [ -f "$FAKE_METADATA_DELETED" ]; then printf '{"items":[]}'
  else jq '{items:[.]}' "$FAKE_METADATA"; fi ;;
 "get pods") printf '%s\n' '{"items":[]}' ;;
 "delete --raw")
  [ "$3" = /api/v1/namespaces/rhiza-v0191-20261008-a1b2c3d4/pods/rhiza-metadata ] && [ "$4" = -f ] || exit 99
  jq -e '.preconditions=={uid:"metadata-uid",resourceVersion:"7"}' "$5" >/dev/null || exit 99
  touch "$FAKE_METADATA_DELETED" ;;
 "logs "*) kill -USR1 "$(cat "$FAKE_PARENT")"; exit 0 ;;
 "scale "*|"wait "*|"delete "*) exit 0 ;;
 *) exit 95 ;;
esac
`, 0700)
			// Functional cases check parser arguments and auth boundaries, not
			// process-launch performance. Timing cases keep the native timer.
			if scenario != "parser-timeout" && scenario != "deadline" && scenario != "watchdog" {
				write(filepath.Join(bin, "timeout"), `#!/bin/sh
previous=
parser=no
for argument do
 [ "$previous $argument" != "config view" ] || parser=yes
 previous=$argument
done
if [ "$parser" = yes ]; then
 [ "$#" = 11 ] && [ "$1" = --kill-after=5s ] && [ "$2" = 5s ] &&
  [ "$3" = kubectl ] && [ "$4" = "--kubeconfig=$FAKE_KUBECONFIG" ] &&
  [ "$5" = --context=gke_patch2-the-new-era_asia-northeast3_ied-cluster ] &&
  [ "$6" = --namespace=rhiza-v0191-20261008-a1b2c3d4 ] &&
  [ "$7" = config ] && [ "$8" = view ] && [ "$9" = --raw ] &&
  [ "${10}" = -o ] && [ "${11}" = json ] || exit 98
 shift 3
 exec "$FAKE_KUBECTL" "$@"
fi
exec "$FAKE_NATIVE_TIMEOUT" "$@"
`, 0700)
			}
			expiry := time.Unix(now+3300, 0).UTC().Format(time.RFC3339)
			if scenario == "expiry" {
				expiry = time.Unix(now+14400, 0).UTC().Format(time.RFC3339)
			}
			dataEmail := "rhiza-v0191-gcs-" + run + "@patch2-the-new-era.iam.gserviceaccount.com"
			owner := "rhiza-postrelease-" + run + "-" + harness
			member := "serviceAccount:patch2-the-new-era.svc.id.goog[" + namespace + "/rhiza-gcs]"
			condition := map[string]string{"title": "rhiza-" + run + "-expiry", "expression": "request.time < timestamp('" + expiry + "')"}
			dataCreate, _ := json.Marshal(map[string]string{"email": dataEmail, "name": "projects/patch2-the-new-era/serviceAccounts/" + dataEmail, "description": owner, "uniqueId": "synthetic-gsa-unique-id"})
			dataPolicy, _ := json.Marshal(map[string]any{"bindings": []any{map[string]any{"role": "roles/iam.workloadIdentityUser", "members": []string{member}, "condition": condition}}})
			write(filepath.Join(dir, "data-create.json"), string(dataCreate), 0600)
			write(filepath.Join(dir, "data-wi-before.json"), `{"bindings":[]}`, 0600)
			write(filepath.Join(dir, "data-wi-grant.json"), string(dataPolicy), 0600)
			workloadIdentity := fmt.Sprintf(`{"run":"%s","namespace":"%s","ksa":{"name":"rhiza-gcs","uid":"synthetic-data-ksa-uid","gcp_service_account":"%s","return_principal_id_as_email":true},"gsa":{"email":"%s","unique_id":"synthetic-gsa-unique-id","owner":"%s","member":"%s","role":"roles/iam.workloadIdentityUser","condition":{"title":"rhiza-%s-expiry","expression":"request.time < timestamp('%s')"}}}`,
				run, namespace, dataEmail, dataEmail, owner, member, run, expiry)
			workloadIdentityPath := filepath.Join(dir, "workload-identity.json")
			write(workloadIdentityPath, workloadIdentity, 0600)
			workloadIdentitySum := sha256.Sum256([]byte(workloadIdentity))
			write(filepath.Join(dir, "workload-identity.sha256"), fmt.Sprintf("%x\n", workloadIdentitySum), 0600)
			if scenario == "workload-identity-receipt-changed" {
				write(workloadIdentityPath, workloadIdentity+"\n", 0600)
			}
			if scenario == "workload-identity-wrong-binding" {
				workloadIdentity = strings.Replace(workloadIdentity, namespace+"/rhiza-gcs]", namespace+"/other]", 1)
				write(workloadIdentityPath, workloadIdentity, 0600)
				workloadIdentitySum = sha256.Sum256([]byte(workloadIdentity))
				write(filepath.Join(dir, "workload-identity.sha256"), fmt.Sprintf("%x\n", workloadIdentitySum), 0600)
			}
			if scenario == "workload-identity-extra-member" {
				write(filepath.Join(dir, "data-wi-grant.json"), strings.Replace(string(dataPolicy), `"members":["`+member+`"]`, `"members":["`+member+`","user:extra@example.invalid"]`, 1), 0600)
			}
			if scenario == "workload-identity-extra-binding" {
				write(filepath.Join(dir, "data-wi-grant.json"), strings.Replace(string(dataPolicy), `]}`, `,{"role":"roles/viewer","members":["user:extra@example.invalid"]}]}`, 1), 0600)
			}
			if scenario == "workload-identity-preexisting-binding" {
				write(filepath.Join(dir, "data-wi-before.json"), string(dataPolicy), 0600)
			}
			if scenario == "workload-identity-recreated-gsa" {
				workloadIdentity = strings.Replace(workloadIdentity, "synthetic-gsa-unique-id", "replacement-gsa-unique-id", 1)
				write(workloadIdentityPath, workloadIdentity, 0600)
				workloadIdentitySum = sha256.Sum256([]byte(workloadIdentity))
				write(filepath.Join(dir, "workload-identity.sha256"), fmt.Sprintf("%x\n", workloadIdentitySum), 0600)
			}
			if strings.HasPrefix(scenario, "workload-identity-mode-") {
				switch scenario {
				case "workload-identity-mode-false":
					workloadIdentity = strings.Replace(workloadIdentity, `"return_principal_id_as_email":true`, `"return_principal_id_as_email":false`, 1)
				case "workload-identity-mode-absent":
					workloadIdentity = strings.Replace(workloadIdentity, `,"return_principal_id_as_email":true`, ``, 1)
				case "workload-identity-mode-other":
					workloadIdentity = strings.Replace(workloadIdentity, `"return_principal_id_as_email":true`, `"return_principal_id_as_email":"true"`, 1)
				}
				write(workloadIdentityPath, workloadIdentity, 0600)
				workloadIdentitySum = sha256.Sum256([]byte(workloadIdentity))
				write(filepath.Join(dir, "workload-identity.sha256"), fmt.Sprintf("%x\n", workloadIdentitySum), 0600)
			}
			for _, name := range []string{"data-create.json", "data-wi-before.json", "data-wi-grant.json", "workload-identity.json"} {
				content, err := os.ReadFile(filepath.Join(dir, name))
				if err != nil || !json.Valid(content) {
					t.Fatalf("invalid synthetic trust receipt %s: %v", name, err)
				}
			}
			out := filepath.Join(dir, "evidence")
			if scenario == "metadata-worker-entrypoint" {
				if err := os.Mkdir(out, 0700); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			entrypoint := "run-local"
			if scenario == "metadata-worker-entrypoint" {
				entrypoint = "metadata-readiness-worker"
			}
			cmd := exec.CommandContext(ctx, "/bin/sh", script, entrypoint)
			// Do not inherit any live credential/CI/approval variables.
			cmd.Env = []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "RHIZA_RUN_ID=" + run, "RHIZA_NAMESPACE=" + namespace,
				"RHIZA_AUTH_CREATION_SHA=" + harness, "RHIZA_HARNESS_SHA=" + syntheticFixtureRevision, "RHIZA_APPLICATION_SHA=" + harness,
				"RHIZA_EXECUTION_GO=" + syntheticFixtureRevision + ":" + run, "RHIZA_APPROVED_CAP_KRW=10000", "RHIZA_BOOTSTRAP_UID=synthetic-uid",
				"RHIZA_HOST_IMAGE=ghcr.io/mrchypark/rhiza-sql@sha256:" + strings.Repeat("a", 64),
				"RHIZA_METADATA_IMAGE=gcr.io/google.com/cloudsdktool/google-cloud-cli@sha256:" + strings.Repeat("b", 64),
				"RHIZA_NODE_A=gke-ied-cluster-fixture-a", "RHIZA_NODE_B=gke-ied-cluster-fixture-b", "RHIZA_NODE_C=gke-ied-cluster-fixture-c",
				"RHIZA_OUTPUT=" + out, "RHIZA_AUTH_STATE=" + dir, "RHIZA_LOCAL_RUNTIME_KUBECONFIG=" + runtimeKubeconfig,
				"RHIZA_LOCAL_API_SERVER=https://127.0.0.1", "RHIZA_LOCAL_CA_DATA=synthetic-pinned-ca",
				"RHIZA_AUTH_STARTED_EPOCH=" + strconv.FormatInt(now, 10), "RHIZA_AUTH_EXPIRES=" + expiry, "RHIZA_LOCAL_TOKEN_EXPIRES=" + expiry,
				"FAKE_NOW=" + strconv.FormatInt(now, 10), "FAKE_CLOCK=" + filepath.Join(dir, "clock"),
				"FAKE_SCENARIO=" + scenario, "FAKE_CALLS=" + filepath.Join(dir, "calls"), "FAKE_CHILD=" + filepath.Join(dir, "child"),
				"FAKE_METADATA=" + filepath.Join(dir, "metadata.json"), "FAKE_METADATA_DELETED=" + filepath.Join(dir, "metadata-deleted"),
				"FAKE_VOTER_ATTEMPT=" + filepath.Join(dir, "voter-attempt"),
				"FAKE_CONFIG_EVENTS=" + filepath.Join(dir, "config-events"),
				"FAKE_NATIVE_TIMEOUT=" + nativeTimeout, "FAKE_KUBECTL=" + filepath.Join(bin, "kubectl"), "FAKE_KUBECONFIG=" + physicalKubeconfig,
				"FAKE_GIT_ROOT=" + filepath.Dir(script) + "/../..", "FAKE_GIT_REVISION=" + mockFixtureRevision,
				"FAKE_PARENT=" + filepath.Join(dir, "parent"), "FAKE_SIGNAL_READY=" + filepath.Join(dir, "signal-ready")}
			if scenario == "metadata-worker-entrypoint" {
				cmd.Env = append(cmd.Env, "RHIZA_METADATA_PARENT_MODE=run-local", "RHIZA_CLUSTER_METADATA="+clusterMetadataPath)
			}
			if scenario == "fake-ci" {
				cmd.Env = append(cmd.Env, "GITHUB_ACTIONS=true")
			}
			if scenario == "embedded-token" {
				rejected := exec.Command(filepath.Join(bin, "timeout"), "--kill-after=5s", "6s", "kubectl",
					"--kubeconfig="+kubeconfig, "--context="+contextName, "--namespace="+namespace, "config", "view", "--raw", "-o", "json")
				rejected.Env = cmd.Env
				var exited *exec.ExitError
				if err := rejected.Run(); !errors.As(err, &exited) || exited.ExitCode() != 98 {
					t.Fatalf("malformed parser budget accepted: %v", err)
				}
				if _, err := os.Stat(filepath.Join(dir, "config-events")); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("malformed parser tuple reached kubectl")
				}
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
			if scenario == "metadata-worker-entrypoint" {
				if err != nil || ctx.Err() != nil {
					t.Fatalf("actual metadata worker rejected sealed inputs: err=%v context=%v output=%s", err, ctx.Err(), output)
				}
				if !bytes.Contains(calls, []byte("/instance/service-accounts/default/email")) || !bytes.Contains(calls, []byte("/instance/service-accounts/default/token")) ||
					!bytes.Contains(output, []byte("category=linked-gsa-email")) || !bytes.Contains(output, []byte("category=token-valid")) ||
					bytes.Contains(output, []byte("cluster metadata receipt required")) {
					t.Fatalf("actual worker did not reach sealed token classification: calls=%s output=%s", calls, output)
				}
				return
			}
			var exited *exec.ExitError
			if !errors.As(err, &exited) || ctx.Err() != nil {
				t.Fatalf("runtime did not fail within its bounded test: err=%v context=%v", err, ctx.Err())
			}
			if bytes.Contains(output, []byte("synthetic-offline-token")) || bytes.Contains(output, []byte("synthetic-rejected-token")) || bytes.Contains(output, []byte("synthetic-metadata-token")) {
				t.Fatal("rejected credential content disclosed")
			}
			if bytes.Contains(output, []byte("node@project.iam.gserviceaccount.com")) {
				t.Fatal("rejected metadata identity body disclosed")
			}
			want := 1
			if scenario == "command-failure" || scenario == "voter-create-failure" {
				want = 53
			}
			if scenario == "voter-create-unknown" {
				want = 55
			}
			if scenario == "deadline" || scenario == "watchdog" {
				want = 124
			}
			if exited.ExitCode() != want {
				t.Fatalf("runtime exit=%d want=%d diagnostic=%s", exited.ExitCode(), want, output)
			}
			if scenario == "metadata-credential-failure" || scenario == "metadata-wrong-service-account" || scenario == "outside-prefix-auth-failure" || scenario == "outside-prefix-accessible" || scenario == "inside-prefix-auth-failure" || scenario == "voter-create-failure" || scenario == "voter-create-unknown" {
				ready := bytes.Index(calls, []byte("wait pod/rhiza-metadata --for=condition=Ready"))
				credential := bytes.Index(calls, []byte("exec rhiza-metadata -- curl"))
				voters := bytes.Index(calls, []byte(out+"/voters.yaml"))
				outside := bytes.Index(calls, []byte("exec rhiza-metadata -- gcloud storage cat"))
				inside := bytes.Index(calls, []byte("exec rhiza-metadata -- gcloud storage ls"))
				if ready < 0 || (scenario != "metadata-gce-mode" && credential <= ready) {
					t.Fatalf("actual metadata Ready/readiness order missing: %s", calls)
				}
				status, err := os.ReadFile(filepath.Join(out, "cleanup-status.txt"))
				if err != nil {
					t.Fatal(err)
				}
				if scenario == "metadata-credential-failure" || scenario == "metadata-wrong-service-account" || scenario == "metadata-gce-mode" || scenario == "outside-prefix-auth-failure" || scenario == "outside-prefix-accessible" || scenario == "inside-prefix-auth-failure" {
					if voters >= 0 || !bytes.Contains(status, []byte("Metadata-only Pod cleanup verified")) {
						t.Fatalf("credential failure bypassed EXIT cleanup or created voters: %s %s", calls, status)
					}
					if _, err := os.Stat(filepath.Join(out, "shutdown-capture.exit")); !errors.Is(err, os.ErrNotExist) {
						t.Fatal("metadata-only invoked voter Close capture")
					}
					switch scenario {
					case "metadata-credential-failure":
						if bytes.Contains(calls, []byte("gcloud")) || bytes.Contains(calls, []byte("storage")) {
							t.Fatal("failed credential readiness reached a storage probe")
						}
					case "metadata-wrong-service-account", "metadata-gce-mode":
						if bytes.Contains(calls, []byte("/token")) || bytes.Contains(calls, []byte("gcloud")) || bytes.Contains(calls, []byte("storage")) {
							t.Fatal("metadata identity/mode rejection reached token or storage")
						}
					case "outside-prefix-auth-failure", "outside-prefix-accessible":
						if outside <= credential || inside >= 0 || !bytes.Contains(output, []byte("outside-folder access did not fail closed")) {
							t.Fatalf("outside authentication failure was mistaken for expected denial: %s %s", calls, output)
						}
						if scenario == "outside-prefix-accessible" {
							paths, err := filepath.Glob(filepath.Join(out, "*-outside-scope.exit"))
							if err != nil || len(paths) != 1 {
								t.Fatalf("actual outside probe exit missing: %v %v", paths, err)
							}
							code, err := os.ReadFile(paths[0])
							if err != nil || strings.TrimSpace(string(code)) != "0" {
								t.Fatalf("outside probe did not actually succeed: %s %v", code, err)
							}
						}
					case "inside-prefix-auth-failure":
						if outside <= credential || inside <= outside || !bytes.Contains(output, []byte("prefix/list permission gate failed")) {
							t.Fatalf("inside authentication failure bypassed its distinct gate: %s %s", calls, output)
						}
					}
				} else {
					if outside <= credential || inside <= outside || voters <= inside || !bytes.Contains(status, []byte("NOT CLEAN")) {
						t.Fatalf("voter attempt bypassed strict cleanup: %s %s", calls, status)
					}
					capture, err := os.ReadFile(filepath.Join(out, "shutdown-capture.exit"))
					if err != nil || strings.TrimSpace(string(capture)) != "1" {
						t.Fatalf("voter attempt did not require actual Close capture: %s %v", capture, err)
					}
					if scenario == "voter-create-unknown" {
						if _, err := os.Stat(filepath.Join(dir, "voter-attempt")); err != nil {
							t.Fatal("ambiguous create did not record its possible side effect")
						}
					}
				}
				if err := filepath.WalkDir(out, func(path string, entry os.DirEntry, walkErr error) error {
					if walkErr != nil {
						return walkErr
					}
					if entry.IsDir() {
						return nil
					}
					data, err := os.ReadFile(path)
					if err != nil {
						return err
					}
					if bytes.Contains(data, []byte("synthetic-metadata-token")) || bytes.Contains(data, []byte("node@project.iam.gserviceaccount.com")) {
						return fmt.Errorf("metadata response persisted in %s", entry.Name())
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			if scenario != "deadline" && scenario != "command-failure" && scenario != "watchdog" && scenario != "metadata-credential-failure" && scenario != "metadata-wrong-service-account" && scenario != "outside-prefix-auth-failure" && scenario != "outside-prefix-accessible" && scenario != "inside-prefix-auth-failure" && scenario != "voter-create-failure" && scenario != "voter-create-unknown" {
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
				case "runtime-kubeconfig-path":
					reason = "runtime kubeconfig must be the mint-local receipt"
				case "cluster-receipt-changed":
					reason = "trusted cluster metadata preparation failed"
				case "workload-identity-receipt-changed", "workload-identity-wrong-binding", "workload-identity-extra-member", "workload-identity-extra-binding", "workload-identity-preexisting-binding", "workload-identity-recreated-gsa", "workload-identity-mode-false", "workload-identity-mode-absent", "workload-identity-mode-other":
					reason = "trusted workload identity binding receipt rejected"
				case "metadata-gce-mode":
					reason = "trusted cluster metadata preparation failed"
				}
				if !bytes.Contains(output, []byte(reason)) {
					t.Fatalf("rejected at the wrong boundary: %s", output)
				}
				if bytes.Contains(calls, []byte("create")) {
					t.Fatal("rejected local entry attempted a resource write")
				}
				if scenario == "fake-ci" || scenario == "head-mismatch" || scenario == "runtime-kubeconfig-path" || strings.HasPrefix(scenario, "workload-identity-") {
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

func TestQualificationSQLReceipt(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nexecute() {\n")
	end := strings.Index(string(source), "\nreadback() {\n")
	if start < 0 || end <= start {
		t.Fatal("actual SQL execution function missing")
	}
	// Exercise the host's real response encoding, including Applied's omission.
	response := httptest.NewRecorder()
	respond(response, rhiza.ExecuteResponse{MutationReceipt: rhiza.MutationReceipt{Slot: 1, Status: rhiza.MutationCommitted, RetryThroughSlot: 65536}}, nil)
	if response.Code != http.StatusOK || bytes.Contains(response.Body.Bytes(), []byte(`"applied"`)) {
		t.Fatal("SQL receipt response contract changed")
	}
	for _, scenario := range []struct {
		name, receipt string
		want          int
	}{
		{"encoded-sql", response.Body.String(), 0},
		{"observed-sql", `{"slot":1,"status":"committed","retry_through_slot":65536}`, 0},
		{"sql-false-applied", `{"slot":2,"status":"committed","applied":false,"error_code":""}`, 0},
		{"rejected", `{"slot":1,"status":"rejected"}`, 1},
		{"execution-error", `{"slot":1,"status":"committed","error_code":"execution_failed"}`, 1},
		{"unknown", `{"slot":1,"status":"commit_unknown"}`, 1},
		{"zero", `{"slot":0,"status":"committed"}`, 1},
		{"negative", `{"slot":-1,"status":"committed"}`, 1},
		{"fraction", `{"slot":1.5,"status":"committed"}`, 1},
		{"string", `{"slot":"1","status":"committed"}`, 1},
		{"missing-slot", `{"status":"committed"}`, 1},
		{"malformed", `{`, 1},
		{"curl-error", response.Body.String(), 22},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "receipt.json"), []byte(scenario.receipt), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "/bin/sh", "-c", `set -eu
out=$1; seq=0
die() { printf '%s\n' "$*" >&2; exit 1; }
run() { [ "$1" = ack ] && [ "$2" = curl ] || exit 91; seq=1; [ "$3" = -fsS ] || exit 92; [ "$FAKE_CURL_EXIT" = 0 ] || exit "$FAKE_CURL_EXIT"; cp "$out/receipt.json" "$out/1-ack.stdout"; }
metrics() { touch "$out/metrics"; }
`+string(source[start:end])+`
execute synthetic-schema 'CREATE TABLE qualification (id INTEGER PRIMARY KEY,value TEXT NOT NULL)'`, "fixture", dir)
			curlExit := "0"
			if scenario.name == "curl-error" {
				curlExit = "22"
			}
			command.Env = append(os.Environ(), "FAKE_CURL_EXIT="+curlExit)
			output, err := command.CombinedOutput()
			code := 0
			if err != nil {
				var exited *exec.ExitError
				if !errors.As(err, &exited) || ctx.Err() != nil {
					t.Fatalf("receipt fixture did not exit normally: %v", err)
				}
				code = exited.ExitCode()
			}
			if code != scenario.want {
				t.Fatalf("exit=%d want=%d output=%s", code, scenario.want, output)
			}
			ids, idsErr := os.ReadFile(filepath.Join(dir, "ack-ids.txt"))
			_, metricsErr := os.Stat(filepath.Join(dir, "metrics"))
			if scenario.want == 0 {
				if idsErr != nil || string(ids) != "synthetic-schema\n" || metricsErr != nil {
					t.Fatal("valid SQL ACK was not retained/sampled")
				}
			} else if !errors.Is(idsErr, os.ErrNotExist) || !errors.Is(metricsErr, os.ErrNotExist) {
				t.Fatal("failed SQL receipt was counted as a successful ACK")
			}
		})
	}
}

func TestQualificationContainerKillRecovery(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\ncontainer_kill() {\n")
	end := strings.Index(string(source), "\nfault NetworkChaos partition network-one\n")
	if start < 0 || end <= start {
		t.Fatal("actual container-kill branch missing")
	}
	for _, scenario := range []string{"success", "before-replaced", "two-injections", "wrong-target", "empty-records", "apply-failed", "delete-failed", "not-ready", "pod-replaced", "wal-replaced", "no-restart", "two-restarts", "same-container", "other-restarted", "image-changed", "barrier-failed"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			writeJSON := func(name string, value any) {
				t.Helper()
				data, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			pods := func(after bool) []any {
				var result []any
				for i := range 3 {
					uid, volume, containerID, image, restarts, ready := fmt.Sprintf("uid-%d", i), "data", fmt.Sprintf("containerd://old-%d", i), "sha256:fixed", 0, true
					if after && i == 0 {
						containerID, restarts = "containerd://new-0", 1
						switch scenario {
						case "not-ready":
							ready = false
						case "pod-replaced":
							uid = "replacement"
						case "wal-replaced":
							volume = "new-wal"
						case "no-restart":
							restarts = 0
						case "two-restarts":
							restarts = 2
						case "same-container":
							containerID = "containerd://old-0"
						case "image-changed":
							image = "sha256:other"
						}
					}
					if after && i == 1 && scenario == "other-restarted" {
						restarts = 1
					}
					result = append(result, map[string]any{"name": fmt.Sprintf("rhiza-voter-%d", i), "uid": uid, "node": fmt.Sprintf("node-%d", i), "volumes": []any{map[string]any{"name": volume, "emptyDir": map[string]any{}}}, "ready": ready, "deleting": nil,
						"containers": []any{map[string]any{"name": "rhiza", "containerID": containerID, "imageID": image, "restartCount": restarts}}})
				}
				return result
			}
			writeJSON("voters-before.json", pods(false))
			before := pods(false)
			if scenario == "before-replaced" {
				before[0].(map[string]any)["uid"] = "replacement"
			}
			writeJSON("before.json", before)
			writeJSON("after.json", pods(true))
			count, target, eventType := 1, "synthetic/rhiza-voter-0/rhiza", "Succeeded"
			if scenario == "two-injections" {
				count = 2
			}
			if scenario == "wrong-target" {
				target = "synthetic/rhiza-voter-1/rhiza"
			}
			if scenario == "apply-failed" {
				eventType = "Failed"
			}
			records := []any{map[string]any{"id": target, "phase": "Injected", "injectedCount": count, "recoveredCount": 0, "events": []any{map[string]any{"operation": "Apply", "type": eventType}}}}
			if scenario == "empty-records" {
				records = nil
			}
			// Same shape as the saved 620769db one-shot response, including the
			// deliberately false AllRecovered condition; never claim recovery from it.
			writeJSON("injection.json", map[string]any{"kind": "PodChaos", "metadata": map[string]any{"name": "process-one", "namespace": "synthetic", "labels": map[string]any{"chaos.rhiza.io/run": "a1b2c3d4"}},
				"spec":   map[string]any{"action": "container-kill", "mode": "one", "duration": "10s", "containerNames": []string{"rhiza"}, "selector": map[string]any{"namespaces": []string{"synthetic"}, "labelSelectors": map[string]any{"chaos.rhiza.io/run": "a1b2c3d4", "statefulset.kubernetes.io/pod-name": "rhiza-voter-0"}}},
				"status": map[string]any{"conditions": []any{map[string]any{"type": "AllInjected", "status": "True"}, map[string]any{"type": "AllRecovered", "status": "False"}}, "experiment": map[string]any{"containerRecords": records}}})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "/bin/sh", "-c", `set -eu
out=$1; seq=0; ns=synthetic; RHIZA_RUN_ID=a1b2c3d4; active_fault=''
die() { printf '%s\n' "$*" >&2; exit 1; }
run() { name=$1; shift; seq=$((seq+1)); printf '%s\n' "$name" >> "$out/order"; "$@" > "$out/$seq-$name.stdout"; }
pod_snapshot() { if [ "$seq" = 1 ]; then cat "$out/before.json"; else [ -f "$out/deleted" ] || exit 91; cat "$out/after.json"; fi; }
fault() { [ "$*" = 'PodChaos container-kill process-one' ] || exit 92; active_fault=PodChaos/process-one; seq=$((seq+1)); cp "$out/injection.json" "$out/$seq-injection.stdout"; printf 'injected\n' >> "$out/order"; }
execute() { printf 'ack\n' >> "$out/order"; }
k() { [ "$*" = 'delete PodChaos/process-one --wait=true --timeout=60s' ] || exit 93; [ "$FAKE_SCENARIO" != delete-failed ] || exit 53; touch "$out/deleted"; }
wait_voters() { [ "$1" = 120 ] && [ -f "$out/deleted" ] || exit 94; }
forward() { [ -f "$out/deleted" ] || exit 95; }
metrics() { printf 'metrics\n' >> "$out/order"; }
readback() { [ "$2" = linearizable ] && [ "$3" = '[[1,"before"],[2,"network"],[3,"process"]]' ] || exit 96; printf 'barrier-%s\n' "$1" >> "$out/order"; [ "$FAKE_SCENARIO" != barrier-failed ] || exit 54; }
`+string(source[start:end])+`
container_kill
[ -z "$active_fault" ] && [ "$voter0_restarts" = 1 ]`, "fixture", dir)
			command.Env = append(os.Environ(), "FAKE_SCENARIO="+scenario)
			output, err := command.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("fixture timeout: %v", ctx.Err())
			}
			if scenario == "success" {
				if err != nil {
					t.Fatalf("actual branch: %v: %s", err, output)
				}
				order, err := os.ReadFile(filepath.Join(dir, "order"))
				if err != nil || string(order) != "process-before\ninjected\nack\nfault-delete\nvoters-ready\nafter\nmetrics\nbarrier-18080\nbarrier-18081\nbarrier-18082\n" {
					t.Fatalf("wrong one-shot recovery order: %s (%v)", order, err)
				}
			} else {
				if err == nil {
					t.Fatal("unsafe recovery accepted")
				}
				order, readErr := os.ReadFile(filepath.Join(dir, "order"))
				if readErr != nil {
					t.Fatal(readErr)
				}
				if scenario == "before-replaced" && string(order) != "process-before\n" {
					t.Fatalf("fault injected into a replacement Pod: %s", order)
				}
				switch scenario {
				case "two-injections", "wrong-target", "empty-records", "apply-failed":
					if string(order) != "process-before\ninjected\n" {
						t.Fatalf("unproven injection reached mutation: %s", order)
					}
				case "delete-failed":
					if bytes.Contains(order, []byte("voters-ready")) {
						t.Fatalf("finalizer failure reached recovery: %s", order)
					}
				}
				if scenario != "barrier-failed" && bytes.Contains(order, []byte("barrier-")) {
					t.Fatalf("unsafe incarnation reached SQL recovery: %s", order)
				}
			}
		})
	}
}

func TestQualificationShutdownSnapshotWatch(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nshutdown_watch() {\n")
	end := strings.Index(string(source), "\nfinish_shutdown_capture() {\n")
	if start < 0 || end <= start {
		t.Fatal("actual capture/watch functions missing")
	}
	for _, scenario := range []string{"success", "learner", "generic-list", "empty-rv", "continued", "wrong-uid", "wrong-namespace", "wrong-run", "missing-writer", "duplicate-writer", "missing-learner", "list-error"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			writeJSON := func(name string, value any) {
				t.Helper()
				data, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			var before, items []any
			for i := range 3 {
				name, uid := fmt.Sprintf("rhiza-voter-%d", i), fmt.Sprintf("uid-%d", i)
				before = append(before, map[string]any{"name": name, "uid": uid})
				if i == 0 && scenario == "wrong-uid" {
					uid = "replacement"
				}
				if i == 0 && scenario == "missing-writer" {
					continue
				}
				ns, run := "synthetic", "a1b2c3d4"
				if scenario == "wrong-namespace" {
					ns = "foreign"
				}
				if scenario == "wrong-run" {
					run = "other"
				}
				items = append(items, map[string]any{"metadata": map[string]any{"name": name, "uid": uid, "namespace": ns, "labels": map[string]any{"app": "rhiza-voter", "chaos.rhiza.io/run": run}}, "spec": map[string]any{"containers": []any{map[string]any{"name": "rhiza", "image": "pinned"}}}, "status": map[string]any{"containerStatuses": []any{map[string]any{"name": "rhiza", "containerID": "containerd://" + name, "imageID": "pinned", "restartCount": 0}}}})
			}
			learner := map[string]any{"metadata": map[string]any{"name": "rhiza-learner", "uid": "learner-uid", "namespace": "synthetic", "labels": map[string]any{"chaos.rhiza.io/run": "a1b2c3d4"}}, "spec": map[string]any{"containers": []any{map[string]any{"name": "rhiza", "image": "pinned"}}}, "status": map[string]any{"containerStatuses": []any{map[string]any{"name": "rhiza", "containerID": "containerd://learner", "imageID": "pinned", "restartCount": 0}}}}
			if scenario == "learner" {
				items = append(items, learner)
			}
			if scenario == "duplicate-writer" {
				items = append(items, items[0])
			}
			kind, rv, continuation := "PodList", "opaque/&?=rv", ""
			if scenario == "generic-list" {
				kind = "List"
			}
			if scenario == "empty-rv" {
				rv = ""
			}
			if scenario == "continued" {
				continuation = "next-page"
			}
			writeJSON("voters-before.json", before)
			writeJSON("learner-created.json", learner)
			writeJSON("list.json", map[string]any{"apiVersion": "v1", "kind": kind, "metadata": map[string]any{"resourceVersion": rv, "continue": continuation}, "items": items})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "/bin/sh", "-c", `set -eu
out=$1; ns=synthetic; RHIZA_RUN_ID=a1b2c3d4; mode=run-local; local_deadline=9999999999
learner_attempted=false; case "$FAKE_SCENARIO" in learner|missing-learner) learner_attempted=true;; esac
shutdown_log_pids=''; shutdown_watch_pid=''; shutdown_capture=false
k() {
 [ "$#" = 2 ] && [ "$1" = get ] && [ "$2" = '--raw=/api/v1/namespaces/synthetic/pods?labelSelector=chaos.rhiza.io%2Frun%3Da1b2c3d4' ] || return 91
 printf 'snapshot\n' > "$out/transport-order"
 [ "$FAKE_SCENARIO" != list-error ] || return 53
 cat "$out/list.json"
}
shutdown_stream() {
 if [ "$1" = get ]; then
  case "$2" in '--raw=/api/v1/namespaces/synthetic/pods?watch=true&resourceVersion=opaque%2F%26%3F%3Drv&labelSelector=chaos.rhiza.io%2Frun%3Da1b2c3d4&timeoutSeconds='*) ;; *) return 92;; esac
  [ "$(cat "$out/transport-order")" = snapshot ] || return 93
  printf '%s\n' "$2" > "$out/watch-command"
  printf '{"type":"ERROR","object":{"code":410}}\n'
 else
  [ "$1" = logs ] && [ "$3" = --container=rhiza ] && [ "$4" = --follow ] || return 94
  printf 'fixture log\n'
 fi
}
`+string(source[start:end])+`
capture_shutdown
[ "$shutdown_capture" = true ]
wait "$shutdown_watch_pid"
for log_owner in $shutdown_log_pids; do wait "${log_owner#*:}"; done`, "fixture", dir)
			command.Env = append(os.Environ(), "FAKE_SCENARIO="+scenario)
			output, err := command.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("snapshot fixture timeout: %v", ctx.Err())
			}
			_, watchErr := os.Stat(filepath.Join(dir, "watch-command"))
			if scenario == "success" || scenario == "learner" {
				if err != nil || watchErr != nil {
					t.Fatalf("snapshot/watch: %v (%v): %s", err, watchErr, output)
				}
				stream, err := os.ReadFile(filepath.Join(dir, "shutdown-watch.json"))
				if err != nil || !bytes.Contains(stream, []byte(`"code":410`)) {
					t.Fatal("native watch error stream was not retained")
				}
				// Capture is not a proof of Close: the existing proof parser must
				// still reject this ERROR event; this fixture only checks transport.
			} else if err == nil || !errors.Is(watchErr, os.ErrNotExist) {
				t.Fatalf("invalid snapshot reached watch: %v (%v): %s", err, watchErr, output)
			}
		})
	}
}

func TestQualificationShutdownWatchCommand(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nshutdown_stream() {\n")
	end := strings.Index(string(source), "\ncapture_shutdown() {\n")
	if start < 0 || end <= start || !strings.Contains(string(source), `shutdown_watch "$resource_version" "$shutdown_watch_deadline"`) {
		t.Fatal("actual scoped shutdown watch/caller missing")
	}
	for _, scenario := range []struct {
		name, mode, version string
		deadline, want      int
	}{
		{"local", "run-local", "1791476651663055017", 340, 0},
		{"ci", "run", "1791476651663055017", 340, 0},
		{"encoded-version", "run-local", "rv/&?=next", 340, 0},
		{"empty-version", "run-local", "", 340, 1},
		{"deadline", "run-local", "1791476651663055017", 100, 124},
		{"client-error", "run-local", "1791476651663055017", 340, 53},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, content := range map[string]string{
				"date":    "#!/bin/sh\nprintf '100\\n'\n",
				"timeout": "#!/bin/sh\n[ \"$1\" = --kill-after=5s ] && [ \"$2\" = \"$FAKE_LIMIT\" ] || exit 91\nshift 2\nexec \"$@\"\n",
				"kubectl": `#!/bin/sh
if [ "$FAKE_MODE" = run-local ]; then [ "$1" = --kubeconfig=/synthetic/private/config ] || exit 92; shift; fi
[ "$1" = --context=synthetic ] && [ "$2" = --namespace=rhiza-v0191-20261008-a1b2c3d4 ] && [ "$3" = --request-timeout=0 ] || exit 93
shift 3
[ "$#" = 2 ] && [ "$1" = get ] && [ "$2" = "$FAKE_RAW" ] || exit 94
touch "$FAKE_DIR/called"
[ "$FAKE_SCENARIO" != client-error ] || exit 53
printf '{"type":"ERROR","object":{"code":410}}\n'
`,
			} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0700); err != nil {
					t.Fatal(err)
				}
			}
			version := scenario.version
			if scenario.name == "encoded-version" {
				version = "rv%2F%26%3F%3Dnext"
			}
			limit := "240s"
			if scenario.mode == "run-local" {
				limit = "7s"
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "/bin/sh", "-c", `set -eu
mode=$FAKE_MODE; context=synthetic; ns=rhiza-v0191-20261008-a1b2c3d4
RHIZA_RUN_ID=a1b2c3d4; RHIZA_LOCAL_RUNTIME_KUBECONFIG=/synthetic/private/config
remaining() { printf '7\n'; }
`+string(source[start:end])+`
shutdown_watch "$1" "$2"`, "fixture", scenario.version, strconv.Itoa(scenario.deadline))
			command.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "FAKE_DIR="+dir, "FAKE_MODE="+scenario.mode, "FAKE_SCENARIO="+scenario.name, "FAKE_LIMIT="+limit,
				"FAKE_RAW=--raw=/api/v1/namespaces/rhiza-v0191-20261008-a1b2c3d4/pods?watch=true&resourceVersion="+version+"&labelSelector=chaos.rhiza.io%2Frun%3Da1b2c3d4&timeoutSeconds=240")
			output, err := command.CombinedOutput()
			code := 0
			if err != nil {
				var exited *exec.ExitError
				if !errors.As(err, &exited) || ctx.Err() != nil {
					t.Fatalf("watch fixture did not exit normally: %v", err)
				}
				code = exited.ExitCode()
			}
			if code != scenario.want {
				t.Fatalf("exit=%d want=%d output=%s", code, scenario.want, output)
			}
			_, called := os.Stat(filepath.Join(dir, "called"))
			if scenario.name == "empty-version" || scenario.name == "deadline" {
				if !errors.Is(called, os.ErrNotExist) {
					t.Fatal("invalid watch input reached transport")
				}
			} else if called != nil || (scenario.want == 0 && !bytes.Contains(output, []byte(`"code":410`))) {
				t.Fatal("native watch response/error was not preserved")
			}
		})
	}
}

func TestQualificationVoterReadiness(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nk() {\n")
	end := strings.Index(string(source), "\nprivate_file() {\n")
	if start < 0 || end <= start {
		t.Fatal("actual scoped kubectl/readiness functions missing")
	}
	functions := string(source[start:end])
	for _, caller := range []string{"run voters-ready wait_voters 180", "run voters-ready wait_voters 120"} {
		if !strings.Contains(string(source), caller) {
			t.Fatalf("shared readiness caller missing: %s", caller)
		}
	}
	if strings.Contains(string(source), "rollout status") {
		t.Fatal("OnDelete voters must not use RollingUpdate rollout status")
	}
	template, err := os.ReadFile("../gcs-postrelease.yaml.in")
	if err != nil || !bytes.Contains(template, []byte("updateStrategy: {type: OnDelete}")) {
		t.Fatal("deliberate OnDelete strategy changed")
	}
	for _, scenario := range []struct {
		name   string
		mode   string
		budget int
		want   int
	}{
		{"initial", "run", 180, 0},
		{"postfault", "run-local", 120, 0},
		{"absent", "run", 180, 1},
		{"create-timeout", "run-local", 120, 124},
		{"ready-timeout", "run", 180, 124},
		{"deadline", "run-local", 120, 124},
		{"missing", "run", 180, 1},
		{"not-ready", "run-local", 120, 1},
		{"terminating", "run", 180, 1},
		{"replacement", "run-local", 120, 1},
		{"get-error", "run", 180, 53},
		{"empty", "run-local", 120, 1},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			dir := t.TempDir()
			write := func(name, content string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0700); err != nil {
					t.Fatal(err)
				}
			}
			write("clock", "100\n")
			write("date", "#!/bin/sh\ncat \"$FAKE_DIR/clock\"\n")
			write("timeout", `#!/bin/sh
[ "$1" = --kill-after=5s ] || exit 91
seconds=${2%s}
[ "$seconds" -gt 0 ] && [ "$seconds" -le "$FAKE_BUDGET" ] || exit 92
printf '%s\n' "$seconds" >> "$FAKE_DIR/timeouts"
shift 2
exec "$@"
`)
			write("kubectl", `#!/bin/sh
case "$1" in --kubeconfig=*) [ "$1" = --kubeconfig=/synthetic/config ] || exit 93; shift ;; esac
[ "$1" = --context=synthetic ] && [ "$2" = --namespace=synthetic ] || exit 94
shift 2
case "$1" in --request-timeout=30s) shift ;; esac
printf '%s\n' "$*" >> "$FAKE_DIR/calls"
now=$(cat "$FAKE_DIR/clock")
step=10; [ "$FAKE_SCENARIO" != deadline ] || step=40
printf '%s\n' "$((now + step))" > "$FAKE_DIR/clock"
case "$1" in
 wait)
  case "$2" in pod/rhiza-voter-[012]) ;; *) exit 95 ;; esac
  case "$4" in --timeout=*) ;; *) exit 96 ;; esac
  case "$3" in
   --for=create)
    [ "$FAKE_SCENARIO" != absent ] || exit 1
    [ "$FAKE_SCENARIO" != create-timeout ] || exit 124
    touch "$FAKE_DIR/${2#pod/}" ;;
   --for=condition=Ready)
    [ -f "$FAKE_DIR/${2#pod/}" ] || exit 97
    [ "$FAKE_SCENARIO" != ready-timeout ] || exit 124 ;;
   *) exit 98 ;;
  esac ;;
 get)
  [ "$*" = 'get pods rhiza-voter-0 rhiza-voter-1 rhiza-voter-2 -o json' ] || exit 99
  if [ -e "$FAKE_DIR/got-before" ]; then
   [ "$FAKE_SCENARIO" != get-error ] || exit 53
   [ "$FAKE_SCENARIO" != empty ] || exit 0
   cat "$FAKE_DIR/after.json"
  else touch "$FAKE_DIR/got-before"; cat "$FAKE_DIR/before.json"; fi ;;
 *) exit 90 ;;
esac
`)
			pods := make([]map[string]any, 3)
			for i := range pods {
				pods[i] = map[string]any{"metadata": map[string]any{"name": fmt.Sprintf("rhiza-voter-%d", i), "uid": fmt.Sprintf("uid-%d", i)},
					"status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}}
			}
			before, err := json.Marshal(map[string]any{"items": pods})
			if err != nil {
				t.Fatal(err)
			}
			write("before.json", string(before))
			switch scenario.name {
			case "missing":
				pods = pods[:2]
			case "not-ready":
				pods[0]["status"] = map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "False"}}}
			case "terminating":
				pods[0]["metadata"].(map[string]any)["deletionTimestamp"] = "2026-10-09T00:00:00Z"
			case "replacement":
				pods[0]["metadata"].(map[string]any)["uid"] = "replacement-uid"
			}
			after, err := json.Marshal(map[string]any{"items": pods})
			if err != nil {
				t.Fatal(err)
			}
			write("after.json", string(after))
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "/bin/sh", "-c", "mode=$FAKE_MODE; context=synthetic; ns=synthetic; RHIZA_LOCAL_RUNTIME_KUBECONFIG=/synthetic/config; remaining() { printf '300\\n'; }; "+functions+"\nwait_voters \"$FAKE_BUDGET\"; code=$?; [ -z \"${voter_wait_deadline:-}\" ] || exit 89; exit \"$code\"")
			command.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "FAKE_DIR="+dir, "FAKE_SCENARIO="+scenario.name,
				"FAKE_MODE="+scenario.mode, "FAKE_BUDGET="+strconv.Itoa(scenario.budget))
			output, err := command.CombinedOutput()
			code := 0
			if err != nil {
				var exited *exec.ExitError
				if !errors.As(err, &exited) || ctx.Err() != nil {
					t.Fatalf("fixture did not terminate normally: %v", err)
				}
				code = exited.ExitCode()
			}
			if code != scenario.want {
				t.Fatalf("exit=%d want=%d output=%s", code, scenario.want, output)
			}
			if scenario.want == 0 {
				calls, err := os.ReadFile(filepath.Join(dir, "calls"))
				if err != nil || strings.Count(string(calls), "--for=create") != 3 || strings.Count(string(calls), "--for=condition=Ready") != 3 {
					t.Fatalf("named creation/readiness calls missing: %s error=%v", calls, err)
				}
				timeouts, err := os.ReadFile(filepath.Join(dir, "timeouts"))
				lastTimeout := scenario.budget - 70
				if scenario.mode == "run-local" && lastTimeout > 30 {
					lastTimeout = 30
				}
				if err != nil || !strings.HasPrefix(string(timeouts), strconv.Itoa(scenario.budget)+"\n") || !strings.HasSuffix(string(timeouts), strconv.Itoa(lastTimeout)+"\n") || !strings.Contains(string(calls), "--timeout="+strconv.Itoa(scenario.budget-60)+"s") {
					t.Fatalf("shared deadline reset: %q error=%v", timeouts, err)
				}
			}
		})
	}
}

func TestQualificationMetadataCredentialReadiness(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nmetadata_credentials_ready_inner() (\n")
	end := strings.Index(string(source), "\nwait_voters() (\n")
	if start < 0 || end <= start {
		t.Fatal("actual metadata readiness function missing")
	}
	invalidResponses := map[string]string{
		"missing-token":   `{"token_type":"Bearer","expires_in":100}`,
		"empty-token":     `{"access_token":"","token_type":"Bearer","expires_in":100}`,
		"token-type":      `{"access_token":123,"token_type":"Bearer","expires_in":100}`,
		"bearer-type":     `{"access_token":"synthetic-token-never-public","token_type":123,"expires_in":100}`,
		"expiry-type":     `{"access_token":"synthetic-token-never-public","token_type":"Bearer","expires_in":"100"}`,
		"expiry-zero":     `{"access_token":"synthetic-token-never-public","token_type":"Bearer","expires_in":0}`,
		"expiry-negative": `{"access_token":"synthetic-token-never-public","token_type":"Bearer","expires_in":-1}`,
	}
	for _, scenario := range []struct {
		name             string
		want, attempts   int
		metadataCategory string
	}{
		{"ready", 0, 1, "token-valid"}, {"post-token-drift", 1, 1, "token-valid"}, {"post-token-native-body", 1, 1, "token-valid"}, {"principal-uri-identity", 1, 0, ""}, {"legacy-alias-identity", 1, 0, ""},
		{"wrong-run-gsa-email", 1, 0, ""}, {"wrong-project-gsa-email", 1, 0, ""}, {"wrong-gsa-email", 1, 0, ""},
		{"node-default-identity", 1, 0, ""}, {"wrong-identity", 1, 0, ""}, {"identity-http", 1, 0, ""}, {"identity-native", 1, 0, ""},
		{"propagation-then-ready", 0, 2, "propagation-pending"}, {"whitespace-then-ready", 0, 2, "propagation-pending"},
		{"arbitrary-403-then-ready", 0, 2, "propagation-pending"},
		{"plain-propagation-then-ready", 0, 2, "propagation-pending"}, {"plain-whitespace-then-ready", 0, 2, "propagation-pending"},
		{"backward-wall-then-ready", 0, 2, "propagation-pending"}, {"propagation-exhausted", 124, 23, "propagation-pending"},
		{"connection", 1, 1, "native-error"}, {"server", 1, 1, "unknown"}, {"not-found", 1, 1, "unknown"}, {"rate-limit", 1, 1, "unknown"},
		{"unknown-native", 1, 1, "native-error"}, {"unknown-http", 1, 1, "unknown"}, {"context-deadline", 124, 1, "deadline"},
		{"invalid", 1, 1, "invalid-response"}, {"multiple", 1, 1, "invalid-response"}, {"deadline", 124, 1, "token-valid"},
		{"budget-reserve", 124, 0, ""}, {"missing-token", 1, 1, "invalid-response"}, {"empty-token", 1, 1, "invalid-response"},
		{"token-type", 1, 1, "invalid-response"}, {"bearer-type", 1, 1, "invalid-response"},
		{"expiry-type", 1, 1, "invalid-response"}, {"expiry-zero", 1, 1, "invalid-response"}, {"expiry-negative", 1, 1, "invalid-response"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, content := range map[string]string{"clock": "1000\n", "attempts": "0\n", "identity-attempts": "0\n"} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(dir, "cluster.json"), []byte(`{"nodePools":[{"name":"fixture","config":{"workloadMetadataConfig":{"mode":"GKE_METADATA"}}}]}`), 0600); err != nil {
				t.Fatal(err)
			}
			invalid, malformed := invalidResponses[scenario.name]
			if malformed {
				if err := os.WriteFile(filepath.Join(dir, "response"), []byte(invalid), 0600); err != nil {
					t.Fatal(err)
				}
			}
			localDeadline, authExpiry, expectedDeadline := 1420, 3000, 1420
			if scenario.name == "budget-reserve" {
				authExpiry, expectedDeadline = 2200, 1000
			}
			fixture := `
mode=run-local; local_deadline=$FAKE_LOCAL_DEADLINE; auth_expiry=$FAKE_AUTH_EXPIRY; RHIZA_RUN_ID=a1b2c3d4; ns=rhiza-v0191-20261008-a1b2c3d4; data_gsa_email=rhiza-v0191-gcs-a1b2c3d4@patch2-the-new-era.iam.gserviceaccount.com; RHIZA_NODE_A=gke-ied-cluster-fixture-a; RHIZA_NODE_B=gke-ied-cluster-fixture-b; RHIZA_NODE_C=gke-ied-cluster-fixture-c; RHIZA_CLUSTER_METADATA="$FAKE_DIR/cluster.json"
date() { cat "$FAKE_DIR/clock"; }
sleep() { now=$(date); case "$FAKE_SCENARIO" in backward-wall-then-ready) now=$((now - 100));; esac; printf '%s\n' "$((now + $1))" > "$FAKE_DIR/clock"; }
k() {
 [ "$metadata_wait_deadline" = "$FAKE_EXPECTED_DEADLINE" ] || return 96
 case "$*" in
  *'/instance/service-accounts/default/email')
   identity_attempt=$(cat "$FAKE_DIR/identity-attempts"); identity_attempt=$((identity_attempt + 1)); printf '%s\n' "$identity_attempt" > "$FAKE_DIR/identity-attempts"
   case "$FAKE_SCENARIO" in
    principal-uri-identity) printf 'principal://iam.googleapis.com/projects/602454948273/locations/global/workloadIdentityPools/patch2-the-new-era.svc.id.goog/subject/ns/rhiza-v0191-20261008-a1b2c3d4/sa/rhiza-gcs\n200' ;;
    legacy-alias-identity) printf 'rhiza-v0191-gcs-a1b2c3d4.svc.id.goog\n200' ;;
    wrong-run-gsa-email) printf 'rhiza-v0191-gcs-deadbeef@patch2-the-new-era.iam.gserviceaccount.com\n200' ;;
    wrong-project-gsa-email) printf 'rhiza-v0191-gcs-a1b2c3d4@other-project.iam.gserviceaccount.com\n200' ;;
    wrong-gsa-email) printf 'other@patch2-the-new-era.iam.gserviceaccount.com\n200' ;;
    node-default-identity) printf '602454948273-compute@developer.gserviceaccount.com\n200' ;;
    wrong-identity) printf 'unrelated.svc.id.goog\n200' ;;
    identity-http) printf 'rhiza-v0191-gcs-a1b2c3d4.svc.id.goog\n404' ;;
    identity-native) return 7 ;;
    post-token-drift) if [ "$identity_attempt" = 1 ]; then printf 'rhiza-v0191-gcs-a1b2c3d4@patch2-the-new-era.iam.gserviceaccount.com\n200'; else printf 'other@patch2-the-new-era.iam.gserviceaccount.com\n200'; fi ;;
    post-token-native-body) if [ "$identity_attempt" = 1 ]; then printf 'rhiza-v0191-gcs-a1b2c3d4@patch2-the-new-era.iam.gserviceaccount.com\n200'; else printf 'synthetic-identity-body-never-public'; return 7; fi ;;
    *) printf 'rhiza-v0191-gcs-a1b2c3d4@patch2-the-new-era.iam.gserviceaccount.com\n200' ;;
   esac
   return 0 ;;
  *'/instance/service-accounts/default/token') ;;
  *) return 95 ;;
 esac
 attempt=$(cat "$FAKE_DIR/attempts"); attempt=$((attempt + 1)); printf '%s\n' "$attempt" > "$FAKE_DIR/attempts"
 printf 'synthetic-token-never-public\n' >&2
 case "$FAKE_SCENARIO" in
  propagation-then-ready|backward-wall-then-ready) [ "$attempt" != 1 ] || { printf '%s\n403' '{"error":{"code":403,"message":"loading GenerateAccessToken(\"rhiza-v0191-gcs-a1b2c3d4@patch2-the-new-era.iam.gserviceaccount.com\"): Permission iam.serviceAccounts.getAccessToken denied on resource","status":"PERMISSION_DENIED"}}'; return 0; } ;;
  arbitrary-403-then-ready) [ "$attempt" != 1 ] || { printf '%s\n403' 'synthetic-arbitrary-forbidden-body-never-public'; return 0; } ;;
  whitespace-then-ready) [ "$attempt" != 1 ] || { printf '%s\n403\r' '{ "error" : { "status" : "PERMISSION_DENIED", "message" : "Permission  iam.serviceAccounts.getAccessToken denied.\nGenerateAccessToken ( rhiza-v0191-gcs-a1b2c3d4@patch2-the-new-era.iam.gserviceaccount.com )", "code" : 403 } }'; return 0; } ;;
  plain-propagation-then-ready) [ "$attempt" != 1 ] || { printf '%s\n403' 'HTTP/403: generic::permission_denied: loading: GenerateAccessToken("rhiza-v0191-gcs-a1b2c3d4@patch2-the-new-era.iam.gserviceaccount.com", ""): googleapi: Error 403: Permission '\''iam.serviceAccounts.getAccessToken'\'' denied on resource (or it may not exist).'; return 0; } ;;
  plain-whitespace-then-ready) [ "$attempt" != 1 ] || { printf 'HTTP/403:\tgeneric::permission_denied: loading: GenerateAccessToken("rhiza-v0191-gcs-a1b2c3d4@patch2-the-new-era.iam.gserviceaccount.com", ""):\r\ngoogleapi: Error 403: Permission '\''iam.serviceAccounts.getAccessToken'\'' denied on resource (or it may not exist).\n403'; return 0; } ;;
  propagation-exhausted) printf '%s\n403' '{"error":{"status":"PERMISSION_DENIED","message":"GenerateAccessToken for rhiza-v0191-gcs-a1b2c3d4@patch2-the-new-era.iam.gserviceaccount.com failed: iam.serviceAccounts.getAccessToken permission denied","code":403}}'; return 0 ;;
  nonretryable-403) printf 'Permission denied for an unrelated reason\n403'; return 0 ;;
  wrong-gsa-403) printf '%s\n403' '{"error":{"code":403,"message":"GenerateAccessToken for rhiza-v0191-gcs-deadbeef@patch2-the-new-era.iam.gserviceaccount.com: iam.serviceAccounts.getAccessToken denied","status":"PERMISSION_DENIED"}}'; return 0 ;;
  wrong-permission-403) printf '%s\n403' '{"error":{"code":403,"message":"GenerateAccessToken for rhiza-v0191-gcs-a1b2c3d4@patch2-the-new-era.iam.gserviceaccount.com: iam.serviceAccounts.actAs denied","status":"PERMISSION_DENIED"}}'; return 0 ;;
  wrong-operation-403) printf '%s\n403' '{"error":{"code":403,"message":"SignBlob for rhiza-v0191-gcs-a1b2c3d4@patch2-the-new-era.iam.gserviceaccount.com: iam.serviceAccounts.getAccessToken denied","status":"PERMISSION_DENIED"}}'; return 0 ;;
  noncanonical-403) printf '%s\n403' '{"error":{"code":403,"message":"GenerateAccessToken for rhiza-v0191-gcs-a1b2c3d4@patch2-the-new-era.iam.gserviceaccount.com: iam.serviceAccounts.getAccessToken denied","status":"FORBIDDEN"}}'; return 0 ;;
  plain-wrong-gsa-403) printf '%s\n403' 'HTTP/403: generic::permission_denied: loading: GenerateAccessToken("wrong@patch2-the-new-era.iam.gserviceaccount.com", ""): googleapi: Error 403: Permission '\''iam.serviceAccounts.getAccessToken'\'' denied on resource (or it may not exist).'; return 0 ;;
  plain-wrong-permission-403) printf '%s\n403' 'HTTP/403: generic::permission_denied: loading: GenerateAccessToken("rhiza-v0191-gcs-a1b2c3d4@patch2-the-new-era.iam.gserviceaccount.com", ""): googleapi: Error 403: Permission '\''iam.serviceAccounts.actAs'\'' denied on resource (or it may not exist).'; return 0 ;;
  plain-arbitrary-403) printf '%s\n403' 'permission denied while GenerateAccessToken rhiza-v0191-gcs-a1b2c3d4@patch2-the-new-era.iam.gserviceaccount.com iam.serviceAccounts.getAccessToken'; return 0 ;;
  connection) [ "$attempt" != 1 ] || return 7 ;;
  server) if [ "$attempt" = 1 ]; then printf 'synthetic-token-never-public\n503'; return 0; fi ;;
  not-found) printf 'synthetic-token-never-public\n404'; return 0 ;;
  rate-limit) printf 'synthetic-token-never-public\n429'; return 0 ;;
  unknown-native) return 53 ;;
  unknown-http) printf 'synthetic-token-never-public\n401'; return 0 ;;
  context-deadline) return 124 ;;
  invalid) printf '{"access_token":"synthetic-token-never-public","token_type":"Other","expires_in":100}\n200'; return 0 ;;
  missing-token|empty-token|token-type|bearer-type|expiry-type|expiry-zero|expiry-negative) cat "$FAKE_RESPONSE"; printf '\n200'; return 0 ;;
  multiple) printf '{"access_token":"synthetic-token-never-public","token_type":"Bearer","expires_in":100}\n';;
  deadline) printf '%s\n' "$FAKE_EXPECTED_DEADLINE" > "$FAKE_DIR/clock" ;;
 esac
 printf '{"access_token":"synthetic-token-never-public","token_type":"Bearer","expires_in":100}\n200'
}

`
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "/bin/sh", "-c", fixture+string(source[start:end])+"\nmetadata_credentials_ready_inner")
			command.Env = append(os.Environ(), "FAKE_DIR="+dir, "FAKE_SCENARIO="+scenario.name, "FAKE_RESPONSE="+filepath.Join(dir, "response"),
				"FAKE_LOCAL_DEADLINE="+strconv.Itoa(localDeadline), "FAKE_AUTH_EXPIRY="+strconv.Itoa(authExpiry), "FAKE_EXPECTED_DEADLINE="+strconv.Itoa(expectedDeadline))
			output, runErr := command.CombinedOutput()
			code := 0
			if runErr != nil {
				var exited *exec.ExitError
				if !errors.As(runErr, &exited) || ctx.Err() != nil {
					t.Fatalf("fixture did not terminate: %v", runErr)
				}
				code = exited.ExitCode()
			}
			attempts, err := os.ReadFile(filepath.Join(dir, "attempts"))
			if err != nil || code != scenario.want || strings.TrimSpace(string(attempts)) != strconv.Itoa(scenario.attempts) {
				t.Fatalf("exit=%d want=%d attempts=%s want=%d output=%s error=%v", code, scenario.want, attempts, scenario.attempts, output, err)
			}
			if bytes.Contains(output, []byte("synthetic-token-never-public")) || bytes.Contains(output, []byte("synthetic-identity-body-never-public")) || bytes.Contains(output, []byte("synthetic-arbitrary-forbidden-body-never-public")) || bytes.Contains(output, []byte("access_token")) {
				t.Fatal("credential response leaked into public output")
			}
			if scenario.metadataCategory != "" && !bytes.Contains(output, []byte("category="+scenario.metadataCategory)) {
				t.Fatalf("metadata category=%s missing from %s", scenario.metadataCategory, output)
			}
			if scenario.metadataCategory == "propagation-pending" && !bytes.Contains(output, []byte("http=403")) {
				t.Fatalf("status-only propagation evidence missing from %s", output)
			}
			identityCategory := "linked-gsa-email"
			switch scenario.name {
			case "principal-uri-identity":
				identityCategory = "principal-uri"
			case "legacy-alias-identity":
				identityCategory = "legacy-ksa-alias"
			case "wrong-run-gsa-email", "wrong-project-gsa-email", "wrong-gsa-email":
				identityCategory = "other-gsa-email"
			case "node-default-identity":
				identityCategory = "node-default"
			case "wrong-identity":
				identityCategory = "unknown"
			case "identity-http":
				identityCategory = "http-error"
			case "identity-native":
				identityCategory = "native-error"
			case "budget-reserve":
				identityCategory = "budget-exhausted"
			}
			if !bytes.Contains(output, []byte("category="+identityCategory)) {
				t.Fatalf("metadata identity category=%s missing from %s", identityCategory, output)
			}
			for _, identity := range []string{
				"principal://iam.googleapis.com/projects/602454948273/locations/global/workloadIdentityPools/patch2-the-new-era.svc.id.goog/subject/ns/rhiza-v0191-20261008-a1b2c3d4/sa/rhiza-gcs",
				"rhiza-v0191-gcs-a1b2c3d4.svc.id.goog",
				"rhiza-v0191-gcs-a1b2c3d4@patch2-the-new-era.iam.gserviceaccount.com",
				"rhiza-v0191-gcs-deadbeef@patch2-the-new-era.iam.gserviceaccount.com",
				"rhiza-v0191-gcs-a1b2c3d4@other-project.iam.gserviceaccount.com",
				"other@patch2-the-new-era.iam.gserviceaccount.com",
				"602454948273-compute@developer.gserviceaccount.com",
				"unrelated.svc.id.goog",
				"iam.serviceAccounts.getAccessToken",
				"iam.serviceAccounts.actAs",
				"Permission denied for an unrelated reason",
				"denied on some resource",
				"GenerateAccessToken",
				"PERMISSION_DENIED",
			} {
				if bytes.Contains(output, []byte(identity)) {
					t.Fatal("raw metadata identity leaked into public output")
				}
			}
			for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
				fields := strings.Fields(line)
				if fields[0] != "stage=metadata-credential-readiness" || !strings.HasPrefix(fields[len(fields)-1], "category=") {
					t.Fatalf("unexpected public evidence: %q", line)
				}
			}
			entries, err := os.ReadDir(dir)
			wantEntries := 4
			if malformed {
				wantEntries++
			}
			if err != nil || len(entries) != wantEntries {
				t.Fatalf("response persisted: entries=%v error=%v", entries, err)
			}
		})
	}
}

func TestQualificationMetadataReadinessOuterWatchdog(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nmetadata_credentials_ready() {\n")
	end := strings.Index(string(source), "\nvalidate_cluster_metadata() {\n")
	if start < 0 || end <= start {
		t.Fatal("actual metadata readiness watchdog missing")
	}
	if !bytes.Contains(source[start:end], []byte("auth_expiry - 1200 - metadata_budget_now - metadata_kill_grace")) {
		t.Fatal("watchdog does not reserve kill grace before the full auth cleanup reserve")
	}
	if output, err := exec.Command("timeout", "--version").CombinedOutput(); err != nil || !bytes.Contains(output, []byte("GNU coreutils")) {
		t.Fatalf("GNU timeout required for monotonic watchdog regression: %v %s", err, output)
	}
	dir := t.TempDir()
	worker := filepath.Join(dir, "run-gcs-postrelease.sh")
	if err := os.WriteFile(worker, []byte("#!/bin/sh\ntrap '' TERM\nwhile :; do sleep 1; done\n"), 0700); err != nil {
		t.Fatal(err)
	}
	fixture := `
mode=run-local
script_dir=$FAKE_SCRIPT_DIR
local_deadline=1006
auth_expiry=2206
# A frozen/backward wall clock cannot extend GNU timeout's monotonic budget.
date() { printf '%s\n' 1000; }
`
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "/bin/sh", "-c", fixture+string(source[start:end])+"\nmetadata_credentials_ready")
	command.Env = append(os.Environ(), "FAKE_SCRIPT_DIR="+dir)
	started := time.Now()
	output, runErr := command.CombinedOutput()
	elapsed := time.Since(started)
	var exited *exec.ExitError
	if !errors.As(runErr, &exited) || exited.ExitCode() != 137 || ctx.Err() != nil {
		t.Fatalf("uncooperative worker escaped watchdog: error=%v output=%s", runErr, output)
	}
	if elapsed < 5*time.Second || elapsed > 8*time.Second {
		t.Fatalf("watchdog elapsed outside one-second budget plus five-second kill grace: %s", elapsed)
	}
}

func TestQualificationCIClusterMetadataReceipt(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\nvalidate_cluster_metadata() {\n")
	end := strings.Index(string(source), "\nwait_voters() (\n")
	privateStart := strings.Index(string(source), "\nprivate_file() {\n")
	privateEnd := strings.Index(string(source), "\nsed -e \"s|__NAMESPACE__|")
	helperStart := strings.Index(string(source), "\nsha256_file() {\n")
	helperEnd := strings.Index(string(source)[helperStart+1:], "\n}\n") + helperStart + 4
	if start < 0 || end <= start || privateStart < 0 || privateEnd <= privateStart || helperStart < 0 || helperEnd <= helperStart {
		t.Fatal("cluster metadata preparation functions missing")
	}
	for _, scenario := range []string{"caller-ignored", "describe-failure"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			bin := filepath.Join(dir, "bin")
			if err := os.Mkdir(bin, 0700); err != nil {
				t.Fatal(err)
			}
			gcloud := `#!/bin/sh
[ "$*" = '--project=patch2-the-new-era --quiet container clusters describe ied-cluster --region=asia-northeast3 --format=json' ] || exit 97
[ "$FAKE_SCENARIO" != describe-failure ] || exit 53
cat "$FAKE_CLUSTER"
`
			if err := os.WriteFile(filepath.Join(bin, "gcloud"), []byte(gcloud), 0700); err != nil {
				t.Fatal(err)
			}
			cluster := `{"name":"ied-cluster","location":"asia-northeast3","endpoint":"example.invalid","masterAuth":{"clusterCaCertificate":"synthetic-ca"},"nodePools":[{"name":"fixture","config":{"workloadMetadataConfig":{"mode":"GKE_METADATA"}}}]}`
			clusterPath := filepath.Join(dir, "fresh.json")
			if err := os.WriteFile(clusterPath, []byte(cluster), 0600); err != nil {
				t.Fatal(err)
			}
			callerPath := filepath.Join(dir, "caller.json")
			if err := os.WriteFile(callerPath, []byte(`{"nodePools":[]}`), 0600); err != nil {
				t.Fatal(err)
			}
			fixture := `
mode=run; out="$FAKE_DIR"; RHIZA_CLUSTER_METADATA="$FAKE_CALLER"
RHIZA_NODE_A=gke-ied-cluster-fixture-a; RHIZA_NODE_B=gke-ied-cluster-fixture-b; RHIZA_NODE_C=gke-ied-cluster-fixture-c
prepare_cluster_metadata || exit $?
[ "$RHIZA_CLUSTER_METADATA" = "$FAKE_DIR/cluster-metadata-validation.json" ] || exit 96
touch "$FAKE_DIR/metadata-pod-created"
`
			command := exec.Command("/bin/sh", "-c", string(source[helperStart:helperEnd])+string(source[privateStart:privateEnd])+string(source[start:end])+fixture)
			command.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "FAKE_DIR="+dir, "FAKE_SCENARIO="+scenario,
				"FAKE_CLUSTER="+clusterPath, "FAKE_CALLER="+callerPath, "TMPDIR="+dir)
			output, runErr := command.CombinedOutput()
			if scenario == "caller-ignored" {
				if runErr != nil {
					t.Fatalf("fresh CI metadata rejected: %v %s", runErr, output)
				}
				data, err := os.ReadFile(filepath.Join(dir, "cluster-metadata-validation.json"))
				if err != nil || bytes.Contains(data, []byte("endpoint")) || bytes.Contains(data, []byte("masterAuth")) || !bytes.Contains(data, []byte("GKE_METADATA")) {
					t.Fatalf("sanitized validation receipt invalid: %v %q", err, data)
				}
				if matches, _ := filepath.Glob(filepath.Join(dir, "rhiza-cluster-metadata.*")); len(matches) != 0 {
					t.Fatalf("raw CI metadata survived success: %v", matches)
				}
				return
			}
			var exited *exec.ExitError
			if !errors.As(runErr, &exited) || exited.ExitCode() != 53 {
				t.Fatalf("describe failure exit=%v output=%s", runErr, output)
			}
			if _, err := os.Stat(filepath.Join(dir, "metadata-pod-created")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("describe failure reached metadata pod creation")
			}
			if matches, _ := filepath.Glob(filepath.Join(dir, "rhiza-cluster-metadata.*")); len(matches) != 0 {
				t.Fatalf("raw CI metadata/stderr survived failure: %v", matches)
			}
		})
	}
}

func TestQualificationMetadataOnlyCleanup(t *testing.T) {
	source, err := os.ReadFile("../run-gcs-postrelease.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(source), "\ncleanup_metadata_only() (\n")
	end := strings.Index(string(source), "\ntrap cleanup EXIT\n")
	captureStart := strings.Index(string(source), "\ncapture_shutdown() {\n")
	captureEnd := strings.Index(string(source), "\nfinish_shutdown_capture() {\n")
	stopStart := strings.Index(string(source), "\nstop_forward() {\n")
	stopEnd := strings.Index(string(source), "\npf_pids=''\n")
	helperStart := strings.Index(string(source), "\nsha256_file() {\n")
	helperEnd := strings.Index(string(source)[helperStart+1:], "\n}\n") + helperStart + 4
	if start < 0 || end <= start || captureStart < 0 || captureEnd <= captureStart || stopStart < 0 || stopEnd <= stopStart || helperStart < 0 || helperEnd <= helperStart {
		t.Fatal("actual cleanup functions missing")
	}
	if !strings.Contains(string(source), "voters_attempted=false\n") || !strings.Contains(string(source), "voters_attempted=true\nrun create-voters") ||
		!strings.Contains(string(source), "run create-metadata k create -f \"$out/metadata.yaml\" -o json\ncp \"$out/$seq-create-metadata.stdout\" \"$out/metadata-created.json\"") {
		t.Fatal("create attempt/ACK ordering changed")
	}
	auth, err := os.ReadFile("../gcs-postrelease-auth.yaml.in")
	if err != nil {
		t.Fatal(err)
	}
	for _, rule := range []string{"resources: [deployments, replicasets, daemonsets]\n    verbs: [list]", "resources: [cronjobs]\n    verbs: [list]"} {
		if !bytes.Contains(auth, []byte(rule)) {
			t.Fatalf("required list-only controller rule missing: %s", rule)
		}
	}
	nativeChecksum, err := exec.LookPath("sha256sum")
	if err != nil {
		t.Fatal(err)
	}
	nativeFind, err := exec.LookPath("find")
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"metadata", "no-owned", "unknown-create", "partial-voters", "namespace-owner", "namespace-uid", "pod-uid", "run", "ksa", "image", "node", "rv", "owner-reference", "controller", "replicaset", "daemonset", "statefulset", "cronjob", "writer", "job", "fault", "list-error", "delete-error", "wait-timeout", "replacement", "delayed-child", "manifest-failure", "manifest-failure-existing", "manifest-malformed", "manifest-find-failure"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			out := filepath.Join(dir, "out")
			if err := os.Mkdir(out, 0700); err != nil {
				t.Fatal(err)
			}
			bin := filepath.Join(dir, "bin")
			if err := os.Mkdir(bin, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(bin, "sha256sum"), []byte(`#!/bin/sh
if [ "$FAKE_SCENARIO" = delayed-child ]; then
 child=$(cat "$FAKE_DIR/child") || exit 98
 kill -0 "$child" 2>/dev/null && exit 98
 printf 'begin\ncomplete\n' | cmp - "$FAKE_OUT/child-final.txt" || exit 98
 printf 'reaped-before-checksum\n' > "$FAKE_DIR/reap-at-seal"
fi
case "$FAKE_SCENARIO" in manifest-failure|manifest-failure-existing) exit 86 ;; esac
[ "$FAKE_SCENARIO" != manifest-malformed ] || { printf 'not-a-digest  %s\n' "$1"; exit 0; }
exec "$FAKE_NATIVE_CHECKSUM" "$@"
`), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(bin, "find"), []byte(`#!/bin/sh
if [ "$FAKE_SCENARIO" = manifest-find-failure ]; then
 printf './cleanup-owner.txt\n'
 exit 87
fi
exec "$FAKE_NATIVE_FIND" "$@"
`), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(bin, "delayed-child"), []byte(`#!/bin/sh
finish() {
 trap - TERM
 kill "$sleep_pid" 2>/dev/null || true
 wait "$sleep_pid" 2>/dev/null || true
 printf 'begin\n' > "$FAKE_OUT/child-final.txt"
 printf 'complete\n' >> "$FAKE_OUT/child-final.txt"
 exit 0
}
trap finish TERM
/bin/sleep 30 & sleep_pid=$!
printf 'armed\n' > "$FAKE_DIR/child-ready"
wait "$sleep_pid"
finish
`), 0700); err != nil {
				t.Fatal(err)
			}
			writeJSON := func(path string, value any) {
				t.Helper()
				data, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			pod := map[string]any{"kind": "Pod", "metadata": map[string]any{"name": "rhiza-metadata", "namespace": "synthetic", "uid": "metadata-uid", "resourceVersion": "7", "labels": map[string]any{"chaos.rhiza.io/run": "a1b2c3d4"}},
				"spec": map[string]any{"serviceAccountName": "rhiza-gcs", "nodeName": "node-a", "containers": []any{map[string]any{"name": "metadata", "image": "metadata-digest"}}}}
			if scenario != "unknown-create" {
				writeJSON(filepath.Join(out, "metadata-created.json"), pod)
			}
			meta := pod["metadata"].(map[string]any)
			spec := pod["spec"].(map[string]any)
			namespace := map[string]any{"metadata": map[string]any{"uid": "namespace-uid", "labels": map[string]any{"chaos.rhiza.io/run": "a1b2c3d4"}, "annotations": map[string]any{"rhiza.dev/auth-owner": "rhiza-postrelease-a1b2c3d4-owner"}}}
			switch scenario {
			case "namespace-owner":
				namespace["metadata"].(map[string]any)["annotations"] = map[string]any{}
			case "namespace-uid":
				namespace["metadata"].(map[string]any)["uid"] = "other"
			case "pod-uid":
				meta["uid"] = "other"
			case "run":
				meta["labels"] = map[string]any{"chaos.rhiza.io/run": "other"}
			case "ksa":
				spec["serviceAccountName"] = "other"
			case "image":
				spec["containers"].([]any)[0].(map[string]any)["image"] = "other"
			case "node":
				spec["nodeName"] = "other"
			case "rv":
				delete(meta, "resourceVersion")
			case "owner-reference":
				meta["ownerReferences"] = []any{map[string]any{"uid": "unexpected-controller"}}
			}
			items := []any{pod}
			switch scenario {
			case "writer", "job", "fault", "controller", "replicaset", "daemonset", "statefulset", "cronjob":
				kind := map[string]string{"writer": "Pod", "job": "Job", "fault": "NetworkChaos", "controller": "Deployment", "replicaset": "ReplicaSet", "daemonset": "DaemonSet", "statefulset": "StatefulSet", "cronjob": "CronJob"}[scenario]
				items = append(items, map[string]any{"kind": kind, "metadata": map[string]any{"name": "unexpected"}})
			}
			writeJSON(filepath.Join(dir, "namespace.json"), namespace)
			writeJSON(filepath.Join(dir, "actors.json"), map[string]any{"items": items})
			fixture := `
set +e
out=$FAKE_OUT; ns=synthetic; RHIZA_RUN_ID=a1b2c3d4; RHIZA_AUTH_CREATION_SHA=owner
RHIZA_BOOTSTRAP_UID=namespace-uid; RHIZA_METADATA_IMAGE=metadata-digest; RHIZA_NODE_A=node-a
mode=run; pf_pids=''; active_fault=''; learner_attempted=false; shutdown_capture=false
shutdown_watch_pid=''; shutdown_log_pids=''; owned_created=true; voters_attempted=false
[ "$FAKE_SCENARIO" != no-owned ] || owned_created=false
[ "$FAKE_SCENARIO" != partial-voters ] || voters_attempted=true
if [ "$FAKE_SCENARIO" = delayed-child ]; then
 voters_attempted=true
 mkfifo "$FAKE_DIR/child-ready" || exit 98
 /bin/sh "$FAKE_DIR/bin/delayed-child" & pf_pids=$!
 printf '%s\n' "$pf_pids" > "$FAKE_DIR/child"
 IFS= read -r child_ready < "$FAKE_DIR/child-ready"
 [ "$child_ready" = armed ] || exit 98
 kill -0 "$pf_pids" || exit 98
fi
stop_watchdog() { :; }
k() {
 printf '%s\n' "$1" >> "$FAKE_DIR/calls"
 case "$*" in
  'get namespace synthetic -o json') cat "$FAKE_DIR/namespace.json" ;;
  'get pods,statefulsets,deployments,replicasets,daemonsets,jobs,cronjobs,podchaos,networkchaos -o json')
   [ "$FAKE_SCENARIO" != list-error ] || return 53
   if [ -f "$FAKE_DIR/deleted" ]; then
    if [ "$FAKE_SCENARIO" = replacement ]; then cat "$FAKE_DIR/actors.json"; else printf '{"items":[]}'; fi
   else cat "$FAKE_DIR/actors.json"; fi ;;
  'delete --raw /api/v1/namespaces/synthetic/pods/rhiza-metadata -f '*)
   [ "$voter_wait_deadline" -gt "$(date +%s)" ] || return 124
   jq -e '.kind=="DeleteOptions" and .preconditions.uid=="metadata-uid" and .preconditions.resourceVersion=="7"' "$5" >/dev/null || return 95
   [ "$FAKE_SCENARIO" != delete-error ] || return 53
   touch "$FAKE_DIR/deleted" ;;
  'wait pod/rhiza-metadata --for=delete --timeout=90s') [ "$FAKE_SCENARIO" != wait-timeout ] || return 124 ;;
  'get pods --selector='*) printf '{"items":[]}' ;;
  logs\ *|scale\ *|wait\ *|delete\ *) [ "$voters_attempted" = true ] || return 96 ;;
  *) return 97 ;;
 esac
}
`
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			initialCode := 53
			if scenario == "manifest-failure" || scenario == "manifest-malformed" || scenario == "manifest-find-failure" {
				initialCode = 0
			}
			command := exec.CommandContext(ctx, "/bin/sh", "-c", fixture+string(source[helperStart:helperEnd])+string(source[stopStart:stopEnd])+string(source[captureStart:captureEnd])+string(source[start:end])+fmt.Sprintf("\n(exit %d); cleanup", initialCode))
			command.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "FAKE_DIR="+dir, "FAKE_OUT="+out, "FAKE_SCENARIO="+scenario, "FAKE_NATIVE_CHECKSUM="+nativeChecksum, "FAKE_NATIVE_FIND="+nativeFind)
			output, runErr := command.CombinedOutput()
			var exited *exec.ExitError
			wantCode := initialCode
			if scenario == "manifest-failure" {
				wantCode = 86
			} else if scenario == "manifest-malformed" {
				wantCode = 1
			} else if scenario == "manifest-find-failure" {
				wantCode = 87
			}
			if !errors.As(runErr, &exited) || exited.ExitCode() != wantCode || ctx.Err() != nil {
				t.Fatalf("original exit lost: %v output=%s", runErr, output)
			}
			status, err := os.ReadFile(filepath.Join(out, "cleanup-status.txt"))
			if err != nil {
				t.Fatal(err)
			}
			clean := scenario == "metadata" || scenario == "no-owned" || strings.HasPrefix(scenario, "manifest-")
			if bytes.Contains(status, []byte("NOT CLEAN")) == clean || bytes.Contains(status, []byte("HTTP/DB close and pinned")) {
				t.Fatalf("wrong cleanup claim: %s", status)
			}
			if strings.HasPrefix(scenario, "manifest-") {
				rootExit, err := os.ReadFile(filepath.Join(out, "root.exit"))
				if err != nil || strings.TrimSpace(string(rootExit)) != strconv.Itoa(wantCode) {
					t.Fatalf("manifest failure root exit=%q error=%v", rootExit, err)
				}
				if _, err := os.Stat(filepath.Join(out, "SHA256SUMS")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("failed manifest retained: %v", err)
				}
				return
			}
			_, deleteErr := os.Stat(filepath.Join(out, "metadata-delete-options.json"))
			shouldDelete := scenario == "metadata" || scenario == "delete-error" || scenario == "wait-timeout" || scenario == "replacement"
			if (deleteErr == nil) != shouldDelete {
				t.Fatalf("unsafe/missing delete: %v output=%s", deleteErr, output)
			}
			if scenario == "partial-voters" || scenario == "delayed-child" {
				capture, err := os.ReadFile(filepath.Join(out, "shutdown-capture.exit"))
				if err != nil || strings.TrimSpace(string(capture)) != "1" {
					t.Fatalf("partial voter bypassed Close capture: %s %v", capture, err)
				}
			}
			if scenario == "delayed-child" {
				marker, err := os.ReadFile(filepath.Join(dir, "reap-at-seal"))
				if err != nil || string(marker) != "reaped-before-checksum\n" {
					t.Fatalf("child not reaped when checksum ran: %s %v", marker, err)
				}
				child, err := os.ReadFile(filepath.Join(dir, "child"))
				if err != nil {
					t.Fatal(err)
				}
				pid, err := strconv.Atoi(strings.TrimSpace(string(child)))
				if err != nil || syscall.Kill(pid, 0) == nil {
					t.Fatal("delayed cleanup child escaped reaping")
				}
				artifact, err := os.ReadFile(filepath.Join(out, "child-final.txt"))
				if err != nil || string(artifact) != "begin\ncomplete\n" {
					t.Fatalf("child final artifact incomplete: %q %v", artifact, err)
				}
				seal, err := os.ReadFile(filepath.Join(out, "SHA256SUMS"))
				if err != nil || !bytes.Contains(seal, []byte("  ./child-final.txt\n")) {
					t.Fatalf("child final artifact omitted from seal: %s %v", seal, err)
				}
			}
			for _, name := range []string{"workload.exit", "root.exit"} {
				code, err := os.ReadFile(filepath.Join(out, name))
				if err != nil || strings.TrimSpace(string(code)) != "53" {
					t.Fatalf("%s changed original exit: %s %v", name, code, err)
				}
			}
			verify := exec.Command("sha256sum", "-c", "SHA256SUMS")
			verify.Dir = out
			if output, err := verify.CombinedOutput(); err != nil {
				t.Fatalf("cleanup seal: %v %s", err, output)
			}
		})
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
