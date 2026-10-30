package operator

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type repairDiagnosticTransport struct {
	base http.RoundTripper
	logf func(string, ...any)
}

type repairDiagnosticBody struct {
	io.Reader
	io.Closer
}

type repairDiagnosticError struct{ err error }

func (r *repairDiagnosticError) Read([]byte) (int, error) {
	if r.err == nil {
		return 0, io.EOF
	}
	err := r.err
	r.err = nil
	return 0, err
}

type repairDiagnosticFault struct {
	err    error
	closed bool
	read   bool
}

func (r *repairDiagnosticFault) Read(p []byte) (int, error) {
	if r.read {
		return 0, io.EOF
	}
	r.read = true
	return copy(p, `{"valid":true}`), r.err
}
func (r *repairDiagnosticFault) Close() error { r.closed = true; return nil }

type repairDiagnosticRoundTrip func(*http.Request) (*http.Response, error)

type repairDiagnosticRule struct{ expression, message, id string }

func repairDiagnosticRules() []repairDiagnosticRule {
	f, err := os.Open(filepath.Join("..", "..", "deploy", "operator", "cluster-crd.yaml"))
	if err != nil {
		return nil
	}
	defer f.Close()
	var rules []repairDiagnosticRule
	s := bufio.NewScanner(f)
	pending := false
	for line := 1; s.Scan(); line++ {
		text := strings.TrimSpace(s.Text())
		if strings.HasPrefix(text, "message: ") && pending {
			rules[len(rules)-1].message = strings.Trim(strings.TrimSpace(strings.TrimPrefix(text, "message: ")), "\"'")
			pending = false
			continue
		}
		pending = false
		if !strings.HasPrefix(text, "- rule: '") || !strings.HasSuffix(text, "'") {
			continue
		}
		expression := strings.TrimSuffix(strings.TrimPrefix(text, "- rule: '"), "'")
		sum := sha256.Sum256([]byte(expression))
		rules = append(rules, repairDiagnosticRule{expression: expression, id: fmt.Sprintf("line-%d-%x", line, sum[:6])})
		pending = true
	}
	if s.Err() != nil {
		return nil
	}
	return rules
}

func repairDiagnosticCEL(message string, rules []repairDiagnosticRule) (classes, keys, ids []string) {
	lower := strings.ToLower(message)
	for _, class := range []struct{ needle, name string }{{"no such key", "missing-key"}, {"no such overload", "overload"}, {"type conversion", "type-conversion"}, {"cost budget", "cost-budget"}, {"cost limit", "cost-limit"}, {"cost could not be calculated", "cost-unavailable"}, {"call cost exceeds limit", "call-cost-limit"}, {"rule compile error", "compile-error"}, {"index out of bounds", "index-out-of-bounds"}, {"integer overflow", "integer-overflow"}, {"division by zero", "division-by-zero"}, {"evaluating rule", "rule-evaluation"}} {
		if strings.Contains(lower, class.needle) {
			classes = append(classes, class.name)
		}
	}
	for _, key := range []string{"repair", "completion", "authority", "pendingWrite", "pendingRequest", "applicationInit", "bootstrapPlan", "plan", "state", "operationID", "generation", "binding", "owner", "configID", "slot"} {
		for _, form := range []string{key, "'" + key + "'", "\"" + key + "\""} {
			needle := "no such key: " + form
			if at := strings.Index(message, needle); at >= 0 {
				end := at + len(needle)
				if end == len(message) || strings.ContainsRune(" \t\r\n,.;:)]}", rune(message[end])) {
					keys = append(keys, key)
					break
				}
			}
		}
	}
	for _, rule := range rules {
		if strings.Contains(message, rule.expression) || rule.message != "" && strings.Contains(message, "evaluating rule: "+rule.message) || rule.message != "" && strings.Contains(message, "for rule: "+rule.message) {
			ids = append(ids, rule.id)
		}
	}
	return
}

// Only fixed words survive. Quoted scalars, numbers, and all other words are
// replaced, so a Kubernetes response cannot inject SQL, credentials, or data.
func repairDiagnosticSkeleton(message string) string {
	allowed := map[string]bool{}
	for _, word := range strings.Fields("validation failed due to running out of cost budget no further rules will be run runtime could not calculated for rule compile error evaluating such key overload operation cancelled actual limit exceeded call cost exceeds index bounds integer overflow division by zero invalid value status field type self oldSelf repair completion authority pendingWrite pendingRequest applicationInit bootstrapPlan plan state operationID generation binding owner configID slot") {
		allowed[word] = true
	}
	var words []string
	quoted := byte(0)
	for i := 0; i < len(message) && len(words) < 96; {
		c := message[i]
		if quoted != 0 {
			if c == '\\' && i+1 < len(message) {
				i += 2
				continue
			}
			if c == quoted {
				quoted = 0
			}
			i++
			continue
		}
		if c == '\'' || c == '"' {
			quoted = c
			words = append(words, "_")
			i++
			continue
		}
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_') {
			i++
			continue
		}
		j := i + 1
		for j < len(message) {
			c = message[j]
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_') {
				break
			}
			j++
		}
		word := message[i:j]
		if !allowed[word] {
			word = "_"
		}
		words = append(words, word)
		i = j
	}
	return strings.Join(words, " ")
}

func (f repairDiagnosticRoundTrip) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestRepairDiagnosticPreservesBodyAndRedacts(t *testing.T) {
	secret := "fixture-secret-do-not-print"
	body := `{"reason":"Invalid","details":{"causes":[{"message":"repair state and completion must agree ` + secret + `"}]}}` + strings.Repeat("x", 20000)
	var log string
	d := repairDiagnosticTransport{base: repairDiagnosticRoundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 422, Body: io.NopCloser(strings.NewReader(body))}, nil
	}), logf: func(format string, args ...any) { log = fmt.Sprintf(format, args...) }}
	req, err := http.NewRequest(http.MethodPut, "https://example.invalid/apis/rhiza/namespaces/n/rhizaclusters/genesis-contract/status", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := d.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if string(got) != body || strings.Contains(log, secret) || !strings.Contains(log, "HTTP 422") {
		t.Fatalf("diagnostic lost body or exposed data: %q", log)
	}
}

func TestRepairDiagnosticPreservesBodyReadError(t *testing.T) {
	want := errors.New("fixture read failure")
	body := &repairDiagnosticFault{err: want}
	d := repairDiagnosticTransport{base: repairDiagnosticRoundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: body}, nil
	}), logf: func(string, ...any) {}}
	req, err := http.NewRequest(http.MethodPut, "https://example.invalid/apis/rhiza/namespaces/n/rhizaclusters/genesis-contract/status", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := d.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(resp.Body)
	if string(got) != `{"valid":true}` || !errors.Is(err, want) {
		t.Fatalf("body or read error changed: %q %v", got, err)
	}
	if err := resp.Body.Close(); err != nil || !body.closed {
		t.Fatalf("underlying body not closed: %v", err)
	}
}

func TestRepairDiagnosticAllowlistedCause(t *testing.T) {
	body := `{"reason":"Invalid","details":{"causes":[{"reason":"FieldValueInvalid","field":"status.repair.plan.replacement.publicKey","message":"private-value"},{"reason":"secret-type","field":"status.repair.secret-field","message":"private-value"}]}}`
	var log string
	d := repairDiagnosticTransport{base: repairDiagnosticRoundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 422, Body: io.NopCloser(strings.NewReader(body))}, nil
	}), logf: func(format string, args ...any) { log = fmt.Sprintf(format, args...) }}
	req, err := http.NewRequest(http.MethodPut, "https://example.invalid/apis/rhiza/namespaces/n/rhizaclusters/genesis-contract/status", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := d.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if string(got) != body || !strings.Contains(log, "status.repair.plan.replacement.publicKey:FieldValueInvalid") || !strings.Contains(log, "other:other") || strings.Contains(log, "private-value") || strings.Contains(log, "secret-field") || strings.Contains(log, "secret-type") {
		t.Fatalf("unsafe or missing cause diagnostic: %q", log)
	}
}

func TestRepairDiagnosticCELClassification(t *testing.T) {
	rules := repairDiagnosticRules()
	if len(rules) == 0 {
		t.Fatal("CRD CEL rules unavailable")
	}
	var selected repairDiagnosticRule
	for _, rule := range rules {
		if strings.Contains(rule.expression, "oldSelf.repair") {
			selected = rule
			break
		}
	}
	if selected.id == "" {
		t.Fatal("repair CEL rule unavailable")
	}
	message := "no such key: repair evaluating rule " + selected.expression + " private-SQL-token-xyz"
	classes, keys, ids := repairDiagnosticCEL(message, rules)
	if !slices.Contains(classes, "missing-key") || !slices.Contains(keys, "repair") || !slices.Contains(ids, selected.id) {
		t.Fatalf("missing expected CEL classification: %q %q %q", classes, keys, ids)
	}
	if strings.Contains(fmt.Sprint(classes, keys, ids), "private-SQL-token-xyz") || strings.Contains(fmt.Sprint(classes, keys, ids), selected.expression) {
		t.Fatal("diagnostic leaked raw cause")
	}
	classes, _, _ = repairDiagnosticCEL("validation failed due to running out of cost budget", rules)
	if !slices.Contains(classes, "cost-budget") {
		t.Fatal("cost failure not classified")
	}
	_, keys, _ = repairDiagnosticCEL("no such key: repair-private-token", rules)
	if len(keys) != 0 {
		t.Fatal("non-allowlisted key matched by prefix")
	}
	classes, _, ids = repairDiagnosticCEL("no such key: repair evaluating rule: "+selected.message, rules)
	if !slices.Contains(classes, "missing-key") || !slices.Contains(ids, selected.id) {
		t.Fatal("rule message was not mapped to its static ID")
	}
}

func TestRepairDiagnosticTopLevelAndTruncation(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       []string
	}{
		{"top", `{"reason":"Invalid","message":"validation failed due to running out of cost budget secretvalue 'private SQL 123'","details":{"causes":[]}}`, []string{"cost-budget", "body-truncated=false", "json-decode-complete=true"}},
		{"truncated", `{"message":"private-value` + strings.Repeat("x", 20000), []string{"body-truncated=true", "json-decode-complete=false"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var log string
			d := repairDiagnosticTransport{base: repairDiagnosticRoundTrip(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 422, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			}), logf: func(format string, args ...any) { log = fmt.Sprintf(format, args...) }}
			req, err := http.NewRequest(http.MethodPut, "https://example.invalid/apis/rhiza/namespaces/n/rhizaclusters/genesis-contract/status", nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := d.RoundTrip(req)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if err := resp.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.body || strings.Contains(log, "secretvalue") || strings.Contains(log, "private") || strings.Contains(log, "123") {
				t.Fatalf("unsafe diagnostic or changed body: %q", log)
			}
			for _, value := range tc.want {
				if !strings.Contains(log, value) {
					t.Fatalf("missing %q in %q", value, log)
				}
			}
		})
	}
}

func TestRepairPlanPublicKeyJSONSchemaShape(t *testing.T) {
	var key quepaxa.PublicKey
	key[0] = 1
	b, err := json.Marshal(V1Alpha2RepairMember{PublicKey: key})
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		PublicKey string `json:"publicKey"`
	}
	if err := json.Unmarshal(b, &decoded); err != nil || len(decoded.PublicKey) != 44 {
		t.Fatalf("public key is not a 44-character JSON string: %v", err)
	}
}

func TestRepairDiagnosticMissingStatusFieldIsNotDifferent(t *testing.T) {
	body := `{"status":{"authority":{"generation":0}}}`
	var log string
	d := repairDiagnosticTransport{base: repairDiagnosticRoundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body))}, nil
	}), logf: func(format string, args ...any) { log = fmt.Sprintf(format, args...) }}
	req, err := http.NewRequest(http.MethodPut, "https://example.invalid/apis/rhiza/namespaces/n/rhizaclusters/genesis-contract/status", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := d.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(log, "differing-status-fields=[]") {
		t.Fatalf("absent repair incorrectly marked different: %s", log)
	}
}

func (d repairDiagnosticTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := d.base.RoundTrip(req)
	if err != nil || resp == nil || req.Method != http.MethodPut || !strings.HasSuffix(req.URL.Path, "/rhizaclusters/genesis-contract/status") {
		return resp, err
	}
	// Preserve the exact stream for the production client, including any bytes
	// beyond the diagnostic bound. Never print request data or raw API bodies.
	prefix, readErr := io.ReadAll(io.LimitReader(resp.Body, 16385))
	truncated := len(prefix) > 16384
	readers := []io.Reader{bytes.NewReader(prefix)}
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		readers = append(readers, &repairDiagnosticError{err: readErr})
	}
	readers = append(readers, resp.Body)
	resp.Body = repairDiagnosticBody{Reader: io.MultiReader(readers...), Closer: resp.Body}
	var status struct {
		Reason  string `json:"reason"`
		Message string `json:"message"`
		Details struct {
			Causes []struct {
				Message string `json:"message"`
				Field   string `json:"field"`
				Type    string `json:"reason"`
			} `json:"causes"`
		} `json:"details"`
	}
	decodeErr := json.Unmarshal(prefix, &status)
	reason := "other"
	for _, allowed := range []string{"Invalid", "Forbidden", "Conflict", "BadRequest", "InternalError"} {
		if status.Reason == allowed {
			reason = allowed
		}
	}
	matches := []string{}
	causes := []string{}
	var celClasses, celKeys, celRules []string
	var skeletons []string
	rules := repairDiagnosticRules()
	classify := func(message string) {
		if message == "" {
			return
		}
		classes, keys, ids := repairDiagnosticCEL(message, rules)
		celClasses = append(celClasses, classes...)
		celKeys = append(celKeys, keys...)
		celRules = append(celRules, ids...)
		skeletons = append(skeletons, repairDiagnosticSkeleton(message))
	}
	classify(status.Message)
	for _, cause := range status.Details.Causes {
		classify(cause.Message)
		field := "other"
		for _, allowed := range []string{"status", "status.repair", "status.repair.state", "status.repair.plan", "status.repair.plan.operationID", "status.repair.plan.clusterUID", "status.repair.plan.generation", "status.repair.plan.deploymentUID", "status.repair.plan.storageID", "status.repair.plan.expectedConfigID", "status.repair.plan.expectedAbortSlot", "status.repair.plan.oldNodeID", "status.repair.plan.oldWALIdentity", "status.repair.plan.bootstrapPlanDigest", "status.repair.plan.finalVoterIDs", "status.repair.plan.replacement", "status.repair.plan.replacement.podName", "status.repair.plan.replacement.podUID", "status.repair.plan.replacement.nodeID", "status.repair.plan.replacement.publicKey", "status.repair.plan.replacement.peerURL", "status.repair.completion"} {
			if cause.Field == allowed {
				field = allowed
			}
		}
		causeType := "other"
		for _, allowed := range []string{"FieldValueInvalid", "FieldValueRequired", "FieldValueNotSupported", "FieldValueDuplicate", "FieldValueTooLong", "FieldValueTooMany", "UnexpectedServerResponse"} {
			if cause.Type == allowed {
				causeType = allowed
			}
		}
		causes = append(causes, field+":"+causeType)
		for _, message := range []string{"repair record cannot be removed", "repair requires consumed managed bootstrap history", "closed repair must match authority and cannot overlap a transition", "repair intent is immutable per operation", "terminal repair is immutable per operation", "a new repair must start closed", "replacement repair requires a terminal predecessor and advanced position", "repair CAS cannot overwrite application authority", "closed repair forbids new application reservations", "repair state and completion must agree", "repair completion must bind the exact operation and position", "REMOVE alone is not final repair completion", "final target must include replacement and exclude old voter"} {
			if strings.Contains(cause.Message, message) {
				matches = append(matches, message)
			}
		}
	}
	var different []string
	if resp.StatusCode >= 200 && resp.StatusCode < 300 && req.GetBody != nil {
		requestBody, e := req.GetBody()
		if e == nil {
			requestData, e := io.ReadAll(io.LimitReader(requestBody, 1<<20))
			_ = requestBody.Close()
			var requested, returned struct {
				Status map[string]json.RawMessage `json:"status"`
			}
			if e == nil && json.Unmarshal(requestData, &requested) == nil && json.Unmarshal(prefix, &returned) == nil {
				for _, key := range []string{"authority", "repair", "genesis", "applicationInit", "bootstrapPlan"} {
					left, leftOK := requested.Status[key]
					right, rightOK := returned.Status[key]
					if leftOK != rightOK || (leftOK && !sameJSON(left, right)) {
						different = append(different, key)
					}
				}
			}
		}
	}
	d.logf("repair status PUT: HTTP %d reason=%s causes=%q known-validation=%q cel-class=%q cel-known-key=%q cel-rule=%q message-skeleton=%q differing-status-fields=%q body-prefix-read-error=%t body-truncated=%t json-decode-complete=%t", resp.StatusCode, reason, causes, matches, celClasses, celKeys, celRules, skeletons, different, readErr != nil, truncated, decodeErr == nil)
	return resp, nil
}

// This exercises API-server schema transitions, not consensus proof. The
// genesis contract creates and consumes the assignment before this test.
func TestV1Alpha2RepairAPIServerContract(t *testing.T) {
	server := os.Getenv("RHIZA_CONTRACT_SERVER")
	if server == "" {
		t.Skip("API server contract environment not configured")
	}
	required := func(k string) string {
		t.Helper()
		v := os.Getenv(k)
		if v == "" {
			t.Fatalf("%s is required", k)
		}
		return v
	}
	ns := required("RHIZA_CONTRACT_NAMESPACE")
	ref := V1Alpha2ClusterRef{Name: "genesis-contract", Namespace: ns, UID: required("RHIZA_CONTRACT_GENESIS_UID")}
	ca, err := os.ReadFile(required("RHIZA_CONTRACT_CA"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid contract CA")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	t.Cleanup(transport.CloseIdleConnections)
	store := &V1Alpha2StatusStore{Kube: &Kubernetes{BaseURL: server, Namespace: ns, TokenFile: required("RHIZA_CONTRACT_TOKEN"), Client: &http.Client{Transport: repairDiagnosticTransport{base: transport, logf: t.Logf}}}}
	ctx := context.Background()
	a, err := LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	init, err := LoadV1Alpha2ApplicationInit(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	if init.Assignment().State != "Consumed" {
		t.Fatal("genesis fixture not consumed")
	}
	request := V1Alpha2WriteRequest{Kind: "fixture", ID: "repair-pending", Fingerprint: "repair-pending-fingerprint"}
	pending, obs, err := ReserveV1Alpha2Write(ctx, store, a, a.Authority(), a.Evidence(), request)
	if err != nil || obs != V1Alpha2CASApplied {
		t.Fatalf("reserve: %s %v", obs, err)
	}
	var key quepaxa.PublicKey
	key[0] = 1
	kubectl := func(input string, args ...string) string {
		t.Helper()
		args = append([]string{"--kubeconfig", required("RHIZA_CONTRACT_KUBECONFIG"), "--context", required("RHIZA_CONTRACT_CONTEXT"), "--namespace", ns, "--request-timeout=30s"}, args...)
		cmd := exec.Command("kubectl", args...)
		cmd.Stdin = strings.NewReader(input)
		out, e := cmd.CombinedOutput()
		if e != nil {
			t.Fatalf("kubectl: %v: %s", e, out)
		}
		return string(out)
	}
	depName, err := V1Alpha2GenesisDeploymentName(ref.UID)
	if err != nil {
		t.Fatal(err)
	}
	deploymentPatch, err := json.Marshal(map[string]any{"spec": map[string]any{"replicas": 1, "template": map[string]any{"spec": map[string]any{"nodeName": "repair-contract-no-such-node", "initContainers": []any{map[string]any{"name": V1Alpha2PrepareIdentityContainer, "image": "example.invalid/init"}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	kubectl("", "patch", "deployment", depName, "--type=merge", "-p", string(deploymentPatch))
	var podName string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		podName = kubectl("", "get", "pods", "-l", "app=genesis-contract", "-o", "jsonpath={.items[*].metadata.name}")
		if podName != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if podName == "" {
		t.Fatal("Deployment did not create its fixture Pod")
	}
	podUID := kubectl("", "get", "pod", podName, "-o", "jsonpath={.metadata.uid}")
	nodeID, err := types.ManagedNodeID(podUID)
	if err != nil {
		t.Fatal(err)
	}
	report, err := json.Marshal(map[string]any{"version": 1, "publicKey": key})
	if err != nil {
		t.Fatal(err)
	}
	status, err := json.Marshal(map[string]any{"status": map[string]any{"podIP": "10.0.0.1", "initContainerStatuses": []any{map[string]any{"name": V1Alpha2PrepareIdentityContainer, "state": map[string]any{"terminated": map[string]any{"exitCode": 0, "message": string(report)}}, "ready": true, "restartCount": 0, "image": "example.invalid/init", "imageID": "fixture"}}}})
	if err != nil {
		t.Fatal(err)
	}
	kubectl("", "patch", "pod", podName, "--subresource=status", "--type=merge", "-p", string(status))
	voters := []types.NodeID{nodeID, "survivor"}
	plan := V1Alpha2RepairPlan{OperationID: "repair-cel", ClusterUID: ref.UID, Generation: a.Authority().Generation, DeploymentUID: a.Authority().Binding.DeploymentUID, StorageID: a.Authority().Binding.StorageID, ExpectedConfigID: 1, OldNodeID: "old", OldWALIdentity: "registered-nonce", BootstrapPlanDigest: init.Assignment().Plan.BootstrapPlanDigest, FinalVoterIDs: voters, Replacement: V1Alpha2RepairMember{PodName: podName, PodUID: podUID, NodeID: nodeID, PublicKey: key, PeerURL: "quic://10.0.0.1:9090"}}
	patch := func(body any, want string) {
		t.Helper()
		b, e := json.Marshal(map[string]any{"status": body})
		if e != nil {
			t.Fatal(e)
		}
		args := []string{"--kubeconfig", required("RHIZA_CONTRACT_KUBECONFIG"), "--context", required("RHIZA_CONTRACT_CONTEXT"), "--namespace", ns, "--request-timeout=30s", "patch", "rhizacluster", ref.Name, "--subresource=status", "--type=merge", "-p", string(b)}
		out, e := exec.Command("kubectl", args...).CombinedOutput()
		if want == "" && e != nil {
			t.Fatalf("status patch: %v: %s", e, out)
		}
		if want != "" && (e == nil || !strings.Contains(string(out), want)) {
			t.Fatalf("expected %q: %v: %s", want, e, out)
		}
	}
	closed, obs, err := BeginV1Alpha2Repair(ctx, store, pending, pending.Authority(), plan)
	if err != nil || obs != V1Alpha2CASApplied || !closed.SQLReservationsClosed() {
		t.Fatalf("close: %s %v", obs, err)
	}
	patch(map[string]any{"repair": map[string]any{"plan": map[string]any{"oldWALIdentity": "changed"}}}, "repair intent is immutable per operation")
	patch(map[string]any{"authority": map[string]any{"binding": map[string]any{"storageID": "changed"}}}, "closed repair must match authority")
	patch(map[string]any{"authority": map[string]any{"transition": map[string]any{"id": "other"}}}, "closed repair must match authority")
	patch(map[string]any{"repair": map[string]any{"state": "Completed"}}, "repair state and completion must agree")
	patch(map[string]any{"repair": map[string]any{"plan": map[string]any{"operationID": "replacement"}}}, "replacement repair requires a terminal predecessor")
	observed, err := LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := observed.Pending()
	if !ok || got != request || !sameJSON(observed.Evidence(), pending.Evidence()) {
		t.Fatal("closing repair lost pending request or evidence")
	}
	final, obs, err := FinalizeV1Alpha2Write(ctx, store, observed, observed.Authority(), request, V1Alpha2WriteCommitted, json.RawMessage(`{"host":"after-repair"}`))
	if err != nil || obs != V1Alpha2CASApplied {
		t.Fatalf("pending finalization: %s %v", obs, err)
	}
	if _, ok := final.Pending(); ok || !sameJSON(final.Evidence(), json.RawMessage(`{"host":"after-repair"}`)) {
		t.Fatal("finalization lost evidence")
	}
	patch(map[string]any{"authority": map[string]any{"pendingWrite": true, "pendingRequest": map[string]any{"kind": "fixture", "id": "new", "fingerprint": "new"}}}, "closed repair forbids new application reservations")
	completion := V1Alpha2RepairCompletion{OperationID: plan.OperationID, PlanDigest: plan.Digest(), ExpectedConfigID: plan.ExpectedConfigID, ExpectedAbortSlot: plan.ExpectedAbortSlot, Kind: "Terminal", ConfigID: 2, Slot: 1, Hash: strings.Repeat("b", 64), FinalVoterIDs: voters}
	patch(map[string]any{"repair": map[string]any{"state": "Completed", "completion": completion}}, "REMOVE alone is not final repair completion")
	completion.ConfigID = 3
	finished, obs, err := FinishV1Alpha2Repair(ctx, store, final, final.Authority(), completion)
	if err != nil || obs != V1Alpha2CASApplied || finished.SQLReservationsClosed() {
		t.Fatalf("finish: %s %v", obs, err)
	}
	loaded, err := LoadV1Alpha2Admission(ctx, store, ref)
	if err != nil {
		t.Fatalf("load terminal: %v", err)
	}
	record, ok := loaded.Repair()
	if !ok || record.State != "Completed" || record.Completion == nil || record.Completion.PlanDigest != plan.Digest() {
		t.Fatal("reader did not accept exact completion")
	}
	patch(map[string]any{"repair": map[string]any{"completion": map[string]any{"hash": strings.Repeat("c", 64)}}}, "terminal repair is immutable per operation")
}
