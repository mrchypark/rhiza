// Package client provides a bounded HTTP adapter client for Rhiza services.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

// ErrOutcomeUnknown means a mutation may have reached the service. Do not
// automatically resubmit it: follow the request-status retention contract.
var ErrOutcomeUnknown = errors.New("mutation outcome unknown")

// ErrReadUnavailable means every configured endpoint failed or returned a
// retryable HTTP status. Response retains the final attempt's
// HTTP status when that attempt received a response.
var ErrReadUnavailable = errors.New("all read endpoints unavailable")

// ErrReadRejected means the service returned a non-retryable HTTP response.
var ErrReadRejected = errors.New("read rejected")

const maxResponseBytes = 17 << 20

// Response retains the server status and body, including commit_unknown errors.
type Response struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

// Client contacts only caller-configured service endpoints. The caller must
// ensure they belong to the same cluster and provide suitable authentication.
type Client struct {
	endpoints []string
	http      http.Client
	timeout   time.Duration
	preferred atomic.Uint64
}

// New configures the service endpoints and a per-attempt deadline for reads.
// A nonpositive timeout is rejected to keep failover bounded.
func New(endpoints []string, transport *http.Client, readTimeout time.Duration) (*Client, error) {
	if len(endpoints) == 0 || len(endpoints) > 32 || readTimeout <= 0 {
		return nil, errors.New("expected 1 to 32 endpoints and a positive read timeout")
	}
	c := &Client{timeout: readTimeout}
	if transport != nil {
		c.http = *transport
	}
	// Redirects could forward credentials or replay a mutation to another host.
	c.http.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	for _, endpoint := range endpoints {
		u, err := url.Parse(endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return nil, fmt.Errorf("invalid service endpoint %q", endpoint)
		}
		c.endpoints = append(c.endpoints, strings.TrimSuffix(u.String(), "/"))
	}
	return c, nil
}

// Read retries only explicitly read-only endpoints on transient failures.
// A local-consistency read on a different replica can return different data.
func (c *Client) Read(ctx context.Context, path string, body []byte) (Response, error) {
	switch path {
	case "/sql/query", "/kv/get", "/graph/query":
	default:
		return Response{}, errors.New("unsupported read endpoint")
	}
	var last error
	var response Response
	state := c.preferred.Load()
	start := int(uint32(state)) % len(c.endpoints)
	for i := range c.endpoints {
		index := (start + i) % len(c.endpoints)
		if err := ctx.Err(); err != nil {
			return Response{}, err
		}
		attempt, cancel := context.WithTimeout(ctx, c.timeout)
		response, last = c.call(attempt, c.endpoints[index], path, body)
		cancel()
		if last == nil {
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				c.preferred.CompareAndSwap(state, nextPreference(state, uint32(index)))
				return response, nil
			}
			if response.StatusCode != http.StatusRequestTimeout && response.StatusCode < 500 {
				return response, fmt.Errorf("%w: HTTP %d", ErrReadRejected, response.StatusCode)
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return response, err
	}
	if last != nil {
		return response, fmt.Errorf("%w: %w", ErrReadUnavailable, last)
	}
	return response, ErrReadUnavailable
}

// Mutate makes exactly one application-level request. If delivery or response
// is uncertain, it returns ErrOutcomeUnknown; never retry based on this alone.
func (c *Client) Mutate(ctx context.Context, path string, body []byte) (Response, error) {
	switch path {
	case "/sql/execute", "/sql/execute-returning", "/sql/execute-returning-one", "/sql/transaction", "/kv/put", "/kv/delete", "/kv/cas", "/graph/execute":
	default:
		return Response{}, errors.New("unsupported mutation endpoint")
	}
	state := c.preferred.Load()
	index := uint32(state) % uint32(len(c.endpoints))
	attempt, cancel := context.WithTimeout(ctx, c.timeout)
	response, err := c.call(attempt, c.endpoints[index], path, body)
	cancel()
	if err != nil {
		if ctx.Err() == nil {
			c.preferred.CompareAndSwap(state, nextPreference(state, (index+1)%uint32(len(c.endpoints))))
		}
		return Response{}, fmt.Errorf("%w: %w", ErrOutcomeUnknown, err)
	}
	var envelope struct {
		Code             string `json:"code"`
		Status           string `json:"status"`
		Slot             uint64 `json:"slot"`
		RetryThroughSlot uint64 `json:"retry_through_slot"`
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 || json.Unmarshal(response.Body, &envelope) != nil || envelope.Code != "" || envelope.Slot == 0 || envelope.RetryThroughSlot < envelope.Slot || (envelope.Status != "committed" && envelope.Status != "rejected") {
		if response.StatusCode == http.StatusRequestTimeout || response.StatusCode >= 500 {
			c.preferred.CompareAndSwap(state, nextPreference(state, (index+1)%uint32(len(c.endpoints))))
		}
		return response, ErrOutcomeUnknown
	}
	c.preferred.CompareAndSwap(state, nextPreference(state, index))
	return response, nil
}

func nextPreference(state uint64, index uint32) uint64 {
	return (state & 0xffffffff00000000) + (1 << 32) | uint64(index)
}

func (c *Client) call(ctx context.Context, endpoint, path string, body []byte) (Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+path, bytes.NewReader(body))
	if err != nil {
		return Response{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return Response{}, err
	}
	if len(data) > maxResponseBytes {
		return Response{}, errors.New("response exceeds limit")
	}
	return Response{StatusCode: resp.StatusCode, Header: resp.Header.Clone(), Body: data}, nil
}
