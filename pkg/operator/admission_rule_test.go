package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

// This test-only recorder keeps APIError.Error sanitized. It retains only the
// expected rule's match result, never raw status/resource data for logging.
type admissionRuleRecorder struct {
	base          http.RoundTripper
	path, rule    string
	seen, matched bool
	code          int
}

func admissionStatusMatchesRule(body []byte, rule string) bool {
	var status struct {
		Kind, APIVersion, Status, Reason string
		Code                            int
		Details struct {
			Causes []struct{ Reason, Message string }
		}
	}
	if json.Unmarshal(body, &status) != nil || status.Kind != "Status" || status.APIVersion != "v1" || status.Status != "Failure" || status.Reason != "Invalid" || status.Code != http.StatusUnprocessableEntity {
		return false
	}
	for _, cause := range status.Details.Causes {
		if cause.Reason == "FieldValueInvalid" && (cause.Message == rule || strings.HasSuffix(cause.Message, ": "+rule)) {
			return true
		}
	}
	return false
}

func (r *admissionRuleRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := r.base.RoundTrip(req)
	if err != nil || resp == nil || req.Method != http.MethodPut || req.URL.RequestURI() != r.path {
		return resp, err
	}
	r.seen, r.code = true, resp.StatusCode
	prefix, readErr := io.ReadAll(io.LimitReader(resp.Body, 16385))
	readers := []io.Reader{bytes.NewReader(prefix)}
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		readers = append(readers, &repairDiagnosticError{err: readErr})
	}
	readers = append(readers, resp.Body)
	resp.Body = repairDiagnosticBody{Reader: io.MultiReader(readers...), Closer: resp.Body}
	r.matched = readErr == nil && len(prefix) <= 16384 && resp.StatusCode == http.StatusUnprocessableEntity && admissionStatusMatchesRule(prefix, r.rule)
	return resp, nil
}

func assertV1Alpha2StatusRejectedByRule(t *testing.T, ctx context.Context, kube *Kubernetes, endpoint string, proposed any, rule string) {
	t.Helper()
	copyKube, copyClient := *kube, *kube.Client
	base := copyClient.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	recorder := &admissionRuleRecorder{base: base, path: endpoint, rule: rule}
	copyClient.Transport = recorder
	copyKube.Client = &copyClient
	var response map[string]json.RawMessage
	err := copyKube.Put(ctx, endpoint, proposed, &response)
	var api *APIError
	if !errors.As(err, &api) || api.StatusCode != http.StatusUnprocessableEntity || !recorder.seen || !recorder.matched {
		t.Fatalf("expected admission rule %q was not confirmed: response_seen=%t HTTP=%d matching_rule=%t", rule, recorder.seen, recorder.code, recorder.matched)
	}
}

func TestAdmissionRuleRecorderRequiresStructuredCause(t *testing.T) {
	const rule = "historical unresolved CR pending is read-only; updates are unsupported"
	valid := `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Invalid","code":422,"details":{"causes":[{"reason":"FieldValueInvalid","message":"Invalid value: PRIVATE_RESOURCE: ` + rule + `"}]}}`
	for _, test := range []struct {
		name, body string
		want       bool
	}{
		{"expected cause", valid, true},
		{"different schema failure", strings.Replace(valid, rule, "another schema failure", 1), false},
		{"wrong cause type", strings.Replace(valid, "FieldValueInvalid", "FieldValueRequired", 1), false},
		{"wrong status reason", strings.Replace(valid, `"reason":"Invalid"`, `"reason":"Forbidden"`, 1), false},
		{"wrong code", strings.Replace(valid, `"code":422`, `"code":403`, 1), false},
		{"no structured cause", `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Invalid","code":422,"message":"` + rule + `"}`, false},
		{"truncated status", valid[:len(valid)-1], false},
		{"oversized status", valid + strings.Repeat(" ", 16385), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := &admissionRuleRecorder{path: "/contract/status", rule: rule, base: repairDiagnosticRoundTrip(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 422, Body: io.NopCloser(strings.NewReader(test.body))}, nil
			})}
			req, err := http.NewRequest(http.MethodPut, "https://example.invalid/contract/status", nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := recorder.RoundTrip(req)
			if err != nil {
				t.Fatal(err)
			}
			restored, readErr := io.ReadAll(resp.Body)
			closeErr := resp.Body.Close()
			if readErr != nil || closeErr != nil || string(restored) != test.body || recorder.matched != test.want {
				t.Fatalf("response preservation or rule predicate failed: read_error=%t close_error=%t body_preserved=%t matched=%t want=%t", readErr != nil, closeErr != nil, string(restored) == test.body, recorder.matched, test.want)
			}
		})
	}
}
