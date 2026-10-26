package fencing

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type authorityClientRoundTrip func(*http.Request) (*http.Response, error)

func (f authorityClientRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type truncatedAuthorityResponse struct {
	read bool
}

func (r *truncatedAuthorityResponse) Read(p []byte) (int, error) {
	if r.read {
		return 0, io.ErrUnexpectedEOF
	}
	r.read = true
	return copy(p, []byte(`{"operationID":"stop-op"`)), io.ErrUnexpectedEOF
}

func (*truncatedAuthorityResponse) Close() error { return nil }

func TestAuthorityClientInterruptedStopResponseRemainsTransient(t *testing.T) {
	id := testIdentity()
	id.WALIdentity = "wal"
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	newAuthorityClient := func() *AuthorityClient {
		return &AuthorityClient{URL: "https://authority.invalid/v1/supervisor", TokenFile: tokenPath, Client: &http.Client{Transport: authorityClientRoundTrip(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: &truncatedAuthorityResponse{}}, nil
		})}}
	}

	client := newAuthorityClient()
	if _, err := client.Stop(context.Background(), id); !errors.Is(err, io.ErrUnexpectedEOF) || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("interrupted authority Stop response error=%v; want ErrUnavailable retaining unexpected EOF", err)
	}

	// Exercise the actual local supervisor handler and Session HTTP decoding.
	// A body-read failure is translated to HTTP 503 and remains retryable.
	const ticket = "local-supervisor-ticket"
	supervisor := &Supervisor{Authority: newAuthorityClient(), Identity: id, requested: id, bound: id,
		grant: StartupGrant{Identity: id, Role: StartupRoleVoterActive}, ticket: ticket}
	localServer := httptest.NewServer(supervisor.handler())
	defer localServer.Close()
	session := &Session{ticket: ticket, identity: id, client: &http.Client{Transport: authorityClientRoundTrip(func(r *http.Request) (*http.Response, error) {
		request := r.Clone(r.Context())
		request.URL.Scheme = "http"
		request.URL.Host = strings.TrimPrefix(localServer.URL, "http://")
		return http.DefaultTransport.RoundTrip(request)
	})}}
	if _, err := session.WaitStop(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("local Session.WaitStop error=%v; want retryable unavailable status", err)
	}
}

func TestAuthorityClientStopDecoderEOFIsTransientButOtherResponseErrorsArePermanent(t *testing.T) {
	id := testIdentity()
	id.WALIdentity = "wal"
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	newAuthorityClient := func(body []byte) *AuthorityClient {
		return &AuthorityClient{URL: "https://authority.invalid/v1/supervisor", TokenFile: tokenPath, Client: &http.Client{Transport: authorityClientRoundTrip(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body)))}, nil
		})}}
	}
	localWaitStop := func(authority *AuthorityClient) error {
		t.Helper()
		const ticket = "local-supervisor-ticket"
		supervisor := &Supervisor{Authority: authority, Identity: id, requested: id, bound: id,
			grant: StartupGrant{Identity: id, Role: StartupRoleVoterActive}, ticket: ticket}
		localServer := httptest.NewServer(supervisor.handler())
		defer localServer.Close()
		session := &Session{ticket: ticket, identity: id, client: &http.Client{Transport: authorityClientRoundTrip(func(r *http.Request) (*http.Response, error) {
			request := r.Clone(r.Context())
			request.URL.Scheme = "http"
			request.URL.Host = strings.TrimPrefix(localServer.URL, "http://")
			return http.DefaultTransport.RoundTrip(request)
		})}}
		_, err := session.WaitStop(context.Background())
		return err
	}

	for _, tc := range []struct {
		name  string
		body  []byte
		cause error
	}{
		{name: "clean EOF before JSON", body: nil, cause: io.EOF},
		{name: "clean EOF during JSON value", body: []byte(`{"operationID":"stop-op"`), cause: io.ErrUnexpectedEOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, clientErr := newAuthorityClient(tc.body).Stop(context.Background(), id)
			if !errors.Is(clientErr, ErrUnavailable) || !errors.Is(clientErr, tc.cause) {
				t.Errorf("AuthorityClient.Stop error=%v; want ErrUnavailable retaining %v", clientErr, tc.cause)
			}
			sessionErr := localWaitStop(newAuthorityClient(tc.body))
			if !errors.Is(sessionErr, ErrUnavailable) {
				t.Errorf("local Session.WaitStop error=%v; want HTTP 503 / ErrUnavailable", sessionErr)
			}
		})
	}

	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{name: "completed malformed JSON", body: `{"operationID": }`, want: "HTTP 409"},
		{name: "unknown field", body: `{"operationID":"op","unknown":true}`, want: "HTTP 409"},
		{name: "trailing JSON", body: `{"operationID":"op"}{}`, want: "HTTP 409"},
		{name: "well-formed semantic mismatch", body: `{}`, want: "identity mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := localWaitStop(newAuthorityClient([]byte(tc.body)))
			if err == nil || errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("local Session.WaitStop error=%v; want permanent %q", err, tc.want)
			}
		})
	}
}
