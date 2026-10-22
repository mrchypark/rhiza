package runtimeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

type Client struct {
	BaseURL   string
	TokenFile string
	HTTP      *http.Client
}

func (c *Client) Enroll(ctx context.Context, binding fencing.RuntimeBinding) (fencing.RuntimeBinding, error) {
	var result fencing.RuntimeBinding
	err := c.post(ctx, "/v1/enroll", enrollRequest{Binding: binding}, &result)
	if err == nil {
		id := fencing.Identity{SourceClusterID: binding.SourceClusterID, NodeID: binding.NodeID, PodUID: binding.PodUID, RunID: binding.RunID}
		if err = result.Validate(id); err == nil && !sameEnrollmentBase(result, binding) {
			err = fmt.Errorf("fencing: runtime agent enrollment binding differs")
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
		return fmt.Errorf("fencing: bounded HTTPS runtime-agent client required")
	}
	base, err := url.Parse(c.BaseURL)
	if err != nil || base.Scheme != "https" || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" || (base.Path != "" && base.Path != "/") {
		return fmt.Errorf("fencing: bounded HTTPS runtime-agent client required")
	}
	tokenBytes, err := os.ReadFile(c.TokenFile)
	if err != nil {
		return err
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" {
		return fmt.Errorf("fencing: empty runtime-agent token")
	}
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(base.String(), "/")+route, bytes.NewReader(body))
	if err != nil {
		return err
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
		return fmt.Errorf("fencing: runtime agent unavailable: %w", err)
	}
	defer response.Body.Close()
	if route == "/v1/verify" && response.StatusCode == http.StatusNoContent {
		return nil
	}
	if route == "/v1/stop" && response.StatusCode == http.StatusAccepted {
		return fencing.ErrPending
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("fencing: runtime agent HTTP %d", response.StatusCode)
	}
	if result == nil {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxMessage+1))
	if err != nil || len(data) > maxMessage {
		return fmt.Errorf("fencing: runtime agent response exceeds capacity")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(result); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("fencing: trailing runtime agent response")
	}
	return nil
}
