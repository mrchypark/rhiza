package objstore

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

func TestGoogleAuthorizedUserCredentialsUseConfiguredTokenURI(t *testing.T) {
	const (
		clientID     = "loopback-test-client"
		clientSecret = "loopback-test-secret"
		refreshToken = "loopback-test-refresh"
		accessToken  = "loopback-test-access"
	)
	var tokenRequests atomic.Int32
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenRequests.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/token" {
			http.Error(w, "unexpected token endpoint", http.StatusBadRequest)
			return
		}
		if err := r.ParseForm(); err != nil || r.Form.Get("grant_type") != "refresh_token" ||
			r.Form.Get("client_id") != clientID || r.Form.Get("client_secret") != clientSecret ||
			r.Form.Get("refresh_token") != refreshToken {
			http.Error(w, "unexpected refresh grant", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"`+accessToken+`","token_type":"Bearer","expires_in":3600}`)
	}))
	defer tokenServer.Close()

	serverURL, err := url.Parse(tokenServer.URL)
	if err != nil {
		t.Fatalf("parse loopback token URL: %v", err)
	}
	localTransport := &http.Transport{Proxy: nil}
	defer localTransport.CloseIdleConnections()
	client := &http.Client{Transport: loopbackOnlyRoundTripper{
		allowedHost: serverURL.Host,
		base:        localTransport,
	}}
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, client)
	credentialsJSON, err := json.Marshal(struct {
		Type         string `json:"type"`
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
		RefreshToken string `json:"refresh_token"`
		TokenURI     string `json:"token_uri"`
	}{
		Type: "authorized_user", ClientID: clientID, ClientSecret: clientSecret,
		RefreshToken: refreshToken, TokenURI: tokenServer.URL + "/token",
	})
	if err != nil {
		t.Fatalf("marshal authorized-user credentials: %v", err)
	}

	// This is the same parser and scope used by Thanos's GCS provider.
	credentials, err := google.CredentialsFromJSON(ctx, credentialsJSON, storage.ScopeFullControl)
	if err != nil {
		t.Fatalf("parse authorized-user credentials: %v", err)
	}
	token, err := credentials.TokenSource.Token()
	if err != nil {
		t.Fatalf("fetch token through configured token_uri: %v", err)
	}
	if token.AccessToken != accessToken {
		t.Fatalf("access token = %q, want loopback stub token", token.AccessToken)
	}
	if got := tokenRequests.Load(); got != 1 {
		t.Fatalf("loopback token requests = %d, want exactly one", got)
	}
}

type loopbackOnlyRoundTripper struct {
	allowedHost string
	base        http.RoundTripper
}

func (rt loopbackOnlyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != "http" || req.URL.Host != rt.allowedHost || req.URL.Path != "/token" {
		return nil, &url.Error{Op: "roundtrip", URL: req.URL.String(), Err: errUnexpectedOAuthDestination{}}
	}
	return rt.base.RoundTrip(req)
}

type errUnexpectedOAuthDestination struct{}

func (errUnexpectedOAuthDestination) Error() string {
	return "OAuth request was not sent to the loopback token stub"
}
