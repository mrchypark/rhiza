package recoveryanchor

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const maxBody = 64 << 10

// actionRequest is the JSON envelope for handler requests.
type actionRequest struct {
	Action  string   `json:"action"`
	Request Request  `json:"request"`
	Receipt *Receipt `json:"receipt,omitempty"`
}

// NewHandler returns an HTTP handler exposing anchor activation and
// verification. Empty token denies ALL (401). Nil coordinator returns 503.
func NewHandler(coord *Coordinator, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if coord == nil {
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !authConstantTime(r, token) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		body, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if len(body) > maxBody {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
			return
		}

		var ar actionRequest
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&ar); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		// Second decode must be EOF (no trailing data).
		if err := dec.Decode(&struct{}{}); err != io.EOF {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		switch ar.Action {
		case "activate":
			receipt, err := coord.Activate(r.Context(), ar.Request)
			if err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(receipt)

		case "verify":
			if ar.Receipt == nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			if err := coord.Verify(r.Context(), ar.Request, *ar.Receipt); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(*ar.Receipt)

		default:
			http.Error(w, "bad request", http.StatusBadRequest)
		}
	})
}

func authConstantTime(r *http.Request, token string) bool {
	if token == "" {
		return false
	}
	hdr := r.Header.Get("Authorization")
	if !strings.HasPrefix(hdr, "Bearer ") {
		return false
	}
	got := strings.TrimSpace(hdr[7:])
	return subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1
}

// Client calls a remote recovery anchor service over HTTPS.
type Client struct {
	URL       string
	TokenFile string
	Client    *http.Client
}

// Activate calls the remote service to activate an anchor.
func (cl *Client) Activate(ctx context.Context, req Request) (Receipt, error) {
	return cl.call(ctx, "activate", req, Receipt{})
}

// Verify calls the remote service to verify an anchor.
func (cl *Client) Verify(ctx context.Context, req Request, receipt Receipt) error {
	_, err := cl.call(ctx, "verify", req, receipt)
	return err
}

func (cl *Client) call(ctx context.Context, action string, req Request, receipt Receipt) (Receipt, error) {
	u, err := url.Parse(cl.URL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return Receipt{}, fmt.Errorf("invalid or non-HTTPS URL")
	}

	tokenBytes, err := os.ReadFile(cl.TokenFile)
	if err != nil {
		return Receipt{}, fmt.Errorf("read token: %w", err)
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" {
		return Receipt{}, fmt.Errorf("empty token")
	}

	ar := actionRequest{Action: action, Request: req, Receipt: &receipt}
	payload, err := json.Marshal(ar)
	if err != nil {
		return Receipt{}, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, cl.URL, bytes.NewReader(payload))
	if err != nil {
		return Receipt{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+token)

	hc := cl.cloneClient()
	resp, err := hc.Do(httpReq)
	if err != nil {
		return Receipt{}, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return Receipt{}, fmt.Errorf("read response: %w", err)
	}
	if len(respBody) > maxBody {
		return Receipt{}, fmt.Errorf("response too large")
	}

	if resp.StatusCode != http.StatusOK {
		return Receipt{}, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	// Strict decode: DisallowUnknownFields + second Decode EOF.
	var receiptOut Receipt
	dec := json.NewDecoder(bytes.NewReader(respBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&receiptOut); err != nil {
		return Receipt{}, fmt.Errorf("decode receipt: %w", err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return Receipt{}, fmt.Errorf("trailing data in response")
	}

	// Validate response.
	if receiptOut.AnchorID != req.AnchorID || receiptOut.OperationID != req.OperationID {
		return Receipt{}, fmt.Errorf("anchor or operation ID mismatch")
	}
	if receiptOut.Target.ClusterID == "" || receiptOut.Target.StorageID == "" {
		return Receipt{}, fmt.Errorf("empty target binding")
	}
	if receiptOut.Generation == 0 {
		return Receipt{}, fmt.Errorf("zero generation")
	}

	// Verify request hash matches.
	expectedHash, err := RequestHash(req)
	if err != nil {
		return Receipt{}, err
	}
	if receiptOut.RequestHash != expectedHash {
		return Receipt{}, fmt.Errorf("request hash mismatch")
	}
	if receiptOut.Target.ClusterID != req.TargetClusterID {
		return Receipt{}, fmt.Errorf("target cluster mismatch")
	}

	// For verify action, receipt must match submitted exactly.
	if action == "verify" && receiptOut != receipt {
		return Receipt{}, fmt.Errorf("receipt mismatch")
	}

	return receiptOut, nil
}

func (cl *Client) cloneClient() *http.Client {
	if cl.Client == nil {
		return &http.Client{
			Timeout:       10 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	hc := *cl.Client
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if hc.Timeout <= 0 || hc.Timeout > 10*time.Second {
		hc.Timeout = 10 * time.Second
	}
	return &hc
}
