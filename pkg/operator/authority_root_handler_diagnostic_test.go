package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/thanos-io/objstore"
)

type rootDiagnosticRoundTripper func(*http.Request) (*http.Response, error)

func (f rootDiagnosticRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type rootInitNthFailingBucket struct {
	objstore.Bucket
	rootReads *int
	failAt    int
	err       error
}

func (b rootInitNthFailingBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if strings.Contains(name, "rhiza-authority/v1/roots/") {
		*b.rootReads++
		if *b.rootReads == b.failAt {
			return objstore.ObjectAttributes{}, b.err
		}
	}
	return b.Bucket.Attributes(ctx, name)
}

func TestTransientRootInitRegistrationLogsDiagnosisAndReturns503(t *testing.T) {
	tests := []struct {
		name           string
		configure      func(*testing.T, supervisorRegistrationFixture)
		wantMessage    string
		rootReadFailAt int
		wantRootReads  int
	}{
		{
			name:           "root read deadline",
			rootReadFailAt: 4,
			wantRootReads:  4,
			wantMessage:    "fencing: root-init phase=load cause=timeout:",
		},
		{
			name:           "bootstrap pending lookup deadline",
			rootReadFailAt: 3,
			wantRootReads:  3,
		},
		{
			name:          "legacy read service unavailable",
			wantRootReads: 4,
			configure: func(t *testing.T, fixture supervisorRegistrationFixture) {
				client := *fixture.e.Kube.Client
				base := client.Transport
				if base == nil {
					t.Fatal("supervisor fixture Kubernetes transport is not configured")
				}
				ledgerPath := fixture.e.ledgerPath("sts-uid", fixture.identity.SourceClusterID)
				client.Transport = rootDiagnosticRoundTripper(func(request *http.Request) (*http.Response, error) {
					if request.Method == http.MethodGet && request.URL.Path == ledgerPath {
						return &http.Response{
							StatusCode: http.StatusServiceUnavailable,
							Header:     make(http.Header),
							Body:       io.NopCloser(strings.NewReader("")),
							Request:    request,
						}, nil
					}
					return base.RoundTrip(request)
				})
				fixture.e.Kube.Client = &client
			},
			wantMessage: "fencing: root-init phase=legacy_check cause=legacy_check_unavailable:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newSupervisorRegistrationFixture(t, false)
			baseBucket := fixture.state.bucket
			rootReads := 0
			if tt.rootReadFailAt != 0 {
				fixture.state.bucket = rootInitNthFailingBucket{Bucket: baseBucket, rootReads: &rootReads, failAt: tt.rootReadFailAt, err: context.DeadlineExceeded}
			} else {
				fixture.state.bucket = rootInitNthFailingBucket{Bucket: baseBucket, rootReads: &rootReads}
			}
			if tt.configure != nil {
				tt.configure(t, fixture)
			}

			var captured bytes.Buffer
			previous := log.Writer()
			log.SetOutput(&captured)
			t.Cleanup(func() { log.SetOutput(previous) })

			body, err := json.Marshal(fencing.AuthorityMessage{Action: "register", Identity: fixture.identity})
			if err != nil {
				t.Fatal(err)
			}
			request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, fixture.server.URL+"/v1/supervisor", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			token, err := os.ReadFile(fixture.token)
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
			request.Header.Set("Content-Type", "application/json")
			response, err := fixture.server.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			responseBody, readErr := io.ReadAll(response.Body)
			closeErr := response.Body.Close()
			if readErr != nil || closeErr != nil {
				t.Fatalf("read response body: read=%v close=%v", readErr, closeErr)
			}
			if response.StatusCode != http.StatusServiceUnavailable || string(responseBody) != "authority unavailable\n" {
				t.Fatalf("transient root-init response changed: status=%d body=%q", response.StatusCode, responseBody)
			}
			logText := captured.String()
			t.Logf("captured root-init transition diagnostic: %s", strings.TrimSpace(logText))
			if tt.wantMessage != "" {
				if !strings.Contains(logText, `fencing supervisor transition rejected: action="register" podUID="pod-0" error="fencing: register root-init rejected:`) || !strings.Contains(logText, tt.wantMessage) {
					t.Fatalf("root-init failure log omitted fixed diagnosis: wanted %q in %q", tt.wantMessage, logText)
				}
			} else if strings.Contains(logText, "root-init phase=") {
				t.Fatalf("pre-root-init failure was mislabeled as root-init: %q", logText)
			}
			if rootReads != tt.wantRootReads {
				t.Fatalf("request reached %d canonical-root reads, want %d", rootReads, tt.wantRootReads)
			}

			rootKey, identity, err := fixture.state.rootKey("sts-uid", fixture.identity.SourceClusterID)
			if err != nil {
				t.Fatal(err)
			}
			fixture.state.bucket = baseBucket
			if _, _, err := fixture.state.read(t.Context(), rootKey, identity); !errors.Is(err, errAuthorityStateNotFound) {
				t.Errorf("registration created canonical root after transient failure: %v", err)
			}
			if _, _, err := fixture.state.read(t.Context(), fixture.state.genesisKey(identity), identity); !errors.Is(err, errAuthorityStateNotFound) {
				t.Errorf("registration created genesis after transient failure: %v", err)
			}
		})
	}
}
