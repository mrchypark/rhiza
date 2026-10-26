package runtimeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/fencing/cri"
)

type Client struct {
	BaseURL   string
	TokenFile string
	HTTP      *http.Client
}

// EnrollmentDiagnosticError carries only an allowlisted rejection category.
// It never includes the server's response body or authentication details.
type EnrollmentDiagnosticError struct {
	Stage string
	Code  string
	cause error
}

func (e *EnrollmentDiagnosticError) Error() string {
	return "fencing: runtime enrollment rejected at " + e.Stage + "/" + e.Code
}

func (e *EnrollmentDiagnosticError) Unwrap() error { return e.cause }

// NewEnrollmentDiagnosticError preserves only context cancellation sentinels,
// never arbitrary transport or response errors.
func NewEnrollmentDiagnosticError(stage, code string, cause error) *EnrollmentDiagnosticError {
	if !ValidEnrollmentDiagnostic(stage, code) {
		return nil
	}
	var safeCause error
	if errors.Is(cause, context.Canceled) {
		safeCause = context.Canceled
	} else if errors.Is(cause, context.DeadlineExceeded) {
		safeCause = context.DeadlineExceeded
	}
	return &EnrollmentDiagnosticError{Stage: stage, Code: code, cause: safeCause}
}

func ValidEnrollmentDiagnostic(stage, code string) bool {
	switch stage + "/" + code {
	case "authorization/token_missing", "authorization/token_review_failed", "authorization/principal_mismatch", "authorization/audience_mismatch",
		"client/config_invalid", "client/token_unavailable", "client/token_empty", "client/request_invalid",
		"client/node_lookup_failed", "client/node_address_unavailable", "client/trust_invalid",
		"transport/unavailable", "transport/context_canceled", "transport/deadline_exceeded",
		"client/context_canceled", "client/deadline_exceeded",
		"response/http_rejected", "response/body_unavailable", "response/body_oversized",
		"response/context_canceled", "response/deadline_exceeded",
		"response/decode_invalid", "response/trailing_data", "binding/invalid", "binding/mismatch":
		return true
	default:
		return false
	}
}

func enrollmentFailure(route, stage, code string, fallback error) error {
	if route == "/v1/enroll" && ValidEnrollmentDiagnostic(stage, code) {
		return NewEnrollmentDiagnosticError(stage, code, fallback)
	}
	return fallback
}

func (c *Client) Enroll(ctx context.Context, binding fencing.RuntimeBinding) (fencing.RuntimeBinding, error) {
	var result fencing.RuntimeBinding
	err := c.post(ctx, "/v1/enroll", enrollRequest{Binding: binding}, &result)
	if err == nil {
		id := fencing.Identity{SourceClusterID: binding.SourceClusterID, NodeID: binding.NodeID, PodUID: binding.PodUID, RunID: binding.RunID, AssignmentID: binding.AssignmentID}
		if err = result.Validate(id); err != nil {
			err = enrollmentFailure("/v1/enroll", "binding", "invalid", fmt.Errorf("fencing: runtime agent enrollment binding is invalid"))
		} else if !sameEnrollmentBase(result, binding) {
			err = enrollmentFailure("/v1/enroll", "binding", "mismatch", fmt.Errorf("fencing: runtime agent enrollment binding differs"))
		}
	}
	return result, err
}

func (c *Client) Stop(ctx context.Context, binding fencing.RuntimeBinding, request fencing.StopRequest) (json.RawMessage, error) {
	var result json.RawMessage
	if err := c.post(ctx, "/v1/stop", stopRequest{Binding: binding, Request: request}, &result); err != nil {
		return nil, err
	}
	if len(result) == 0 || !json.Valid(result) {
		return nil, fmt.Errorf("fencing: malformed runtime receipt")
	}
	return result, nil
}

func (c *Client) Verify(ctx context.Context, binding fencing.RuntimeBinding, request fencing.StopRequest, receipt []byte) error {
	return c.post(ctx, "/v1/verify", verifyRequest{Binding: binding, Request: request, Receipt: receipt}, nil)
}

func (c *Client) post(ctx context.Context, route string, value any, result any) error {
	if c == nil || c.HTTP == nil || c.TokenFile == "" {
		return enrollmentFailure(route, "client", "config_invalid", fmt.Errorf("fencing: bounded HTTPS runtime-agent client required"))
	}
	base, err := url.Parse(c.BaseURL)
	if err != nil || base.Scheme != "https" || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" || (base.Path != "" && base.Path != "/") {
		return enrollmentFailure(route, "client", "config_invalid", fmt.Errorf("fencing: bounded HTTPS runtime-agent client required"))
	}
	tokenBytes, err := os.ReadFile(c.TokenFile)
	if err != nil {
		return enrollmentFailure(route, "client", "token_unavailable", err)
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" {
		return enrollmentFailure(route, "client", "token_empty", fmt.Errorf("fencing: empty runtime-agent token"))
	}
	body, err := json.Marshal(value)
	if err != nil {
		return enrollmentFailure(route, "client", "request_invalid", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(base.String(), "/")+route, bytes.NewReader(body))
	if err != nil {
		return enrollmentFailure(route, "client", "request_invalid", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	client := *c.HTTP
	if client.Timeout <= 0 || client.Timeout > 30*time.Second {
		client.Timeout = 30 * time.Second
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(request)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return enrollmentFailure(route, "transport", "context_canceled", err)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return enrollmentFailure(route, "transport", "deadline_exceeded", err)
		}
		return enrollmentFailure(route, "transport", "unavailable", fmt.Errorf("fencing: runtime agent unavailable: %w", err))
	}
	defer response.Body.Close()
	if route == "/v1/verify" && response.StatusCode == http.StatusNoContent {
		return nil
	}
	if route == "/v1/stop" && response.StatusCode == http.StatusAccepted {
		stage, code := response.Header.Get("X-Rhiza-Stop-Stage"), response.Header.Get("X-Rhiza-Stop-Code")
		if !cri.ValidStopDiagnostic(stage, code) {
			stage, code = "backend_stop", "backend_unclassified"
		}
		return cri.NewStopDiagnosticError(stage, code, fencing.ErrPending)
	}
	if route == "/v1/enroll" && response.StatusCode == http.StatusForbidden {
		stage, code := response.Header.Get("X-Rhiza-Enrollment-Stage"), response.Header.Get("X-Rhiza-Enrollment-Code")
		if ValidEnrollmentDiagnostic(stage, code) {
			return &EnrollmentDiagnosticError{Stage: stage, Code: code}
		}
	}
	if response.StatusCode != http.StatusOK {
		return enrollmentFailure(route, "response", "http_rejected", fmt.Errorf("fencing: runtime agent HTTP %d", response.StatusCode))
	}
	if result == nil {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxMessage+1))
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return enrollmentFailure(route, "response", "context_canceled", err)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return enrollmentFailure(route, "response", "deadline_exceeded", err)
		}
		return enrollmentFailure(route, "response", "body_unavailable", fmt.Errorf("fencing: runtime agent response unavailable"))
	}
	if len(data) > maxMessage {
		return enrollmentFailure(route, "response", "body_oversized", fmt.Errorf("fencing: runtime agent response exceeds capacity"))
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(result); err != nil {
		return enrollmentFailure(route, "response", "decode_invalid", err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return enrollmentFailure(route, "response", "trailing_data", fmt.Errorf("fencing: trailing runtime agent response"))
	}
	return nil
}
