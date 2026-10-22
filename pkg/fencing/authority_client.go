package fencing

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
)

// AuthorityMessage is sent only over authenticated TLS. The server authenticates
// the bound service account token independently of the claimed identity.
type AuthorityMessage struct {
	Action         string                 `json:"action"`
	Identity       Identity               `json:"identity"`
	Drain          *DrainReceipt          `json:"drain,omitempty"`
	Request        *StopRequest           `json:"request,omitempty"`
	MutationLookup *MutationLookupRequest `json:"mutationLookup,omitempty"`
}

// MutationLookupRequest asks the authority for one original producer tuple.
// It is read-only: the server must never interpret absence as admission.
type MutationLookupRequest struct {
	ProducerUID string         `json:"producerUID"`
	Sequence    uint64         `json:"sequence"`
	Intent      MutationIntent `json:"intent"`
}

// MutationLookupResult preserves the exact durable entry, including an
// unresolved result. Found=false means only that this tuple was never admitted.
type MutationLookupResult struct {
	Found            bool                      `json:"found"`
	Archived         bool                      `json:"archived,omitempty"`
	CompactedThrough uint64                    `json:"compactedThrough,omitempty"`
	ArchiveHead      *MutationArchiveReference `json:"archiveHead,omitempty"`
	Entry            *MutationEntry            `json:"entry,omitempty"`
}

type AuthorityClient struct {
	URL       string
	TokenFile string
	Client    *http.Client
}

func (a *AuthorityClient) call(ctx context.Context, message AuthorityMessage, result any) error {
	u, err := url.Parse(a.URL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("fencing: HTTPS authority required")
	}
	token, err := os.ReadFile(a.TokenFile)
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(token)) == 0 {
		return fmt.Errorf("fencing: empty authority token")
	}
	body, err := json.Marshal(message)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	req.Header.Set("Content-Type", "application/json")
	if a.Client == nil {
		return fmt.Errorf("fencing: bounded TLS client required")
	}
	// Never forward a projected token to a redirect destination.
	client := *a.Client
	if client.Timeout <= 0 || client.Timeout > 30*time.Second {
		client.Timeout = 30 * time.Second
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusAccepted {
		return ErrPending
	}
	if resp.StatusCode == http.StatusGone {
		return ErrClosed
	}
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
			return ErrUnavailable
		}
		return fmt.Errorf("fencing: authority HTTP %d", resp.StatusCode)
	}
	if result == nil {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if len(data) > 65536 {
		return fmt.Errorf("fencing: authority response too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(result); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("fencing: trailing authority response")
	}
	return nil
}

func (a *AuthorityClient) Register(ctx context.Context, id Identity) error {
	return a.call(ctx, AuthorityMessage{Action: "register", Identity: id}, nil)
}
func (a *AuthorityClient) Bind(ctx context.Context, id Identity) (BindReceipt, error) {
	var receipt BindReceipt
	if err := a.call(ctx, AuthorityMessage{Action: "bind", Identity: id}, &receipt); err != nil {
		return BindReceipt{}, err
	}
	return receipt, receipt.Validate(id)
}
func (a *AuthorityClient) Stop(ctx context.Context, id Identity) (StopRequest, error) {
	var request StopRequest
	err := a.call(ctx, AuthorityMessage{Action: "stop", Identity: id}, &request)
	return request, err
}
func (a *AuthorityClient) Drain(ctx context.Context, r DrainReceipt) error {
	return a.call(ctx, AuthorityMessage{Action: "drain", Identity: r.Request.Identity, Drain: &r}, nil)
}
func (a *AuthorityClient) Exit(ctx context.Context, r StopRequest) error {
	return a.call(ctx, AuthorityMessage{Action: "exit", Identity: r.Identity, Request: &r}, nil)
}

func (a *AuthorityClient) NeverBoundExit(ctx context.Context, r StopRequest) error {
	return a.call(ctx, AuthorityMessage{Action: "never-bound-exit", Identity: r.Identity, Request: &r}, nil)
}

func (a *AuthorityClient) LookupMutation(ctx context.Context, id Identity, producerUID string, sequence uint64, intent MutationIntent) (MutationLookupResult, error) {
	if id.WALIdentity == "" || producerUID == "" || sequence == 0 || intent.OperationID != MutationRequestID(producerUID, sequence) {
		return MutationLookupResult{}, fmt.Errorf("fencing: mutation lookup identity differs")
	}
	if err := validateMutationIntent(intent); err != nil {
		return MutationLookupResult{}, err
	}
	request := MutationLookupRequest{ProducerUID: producerUID, Sequence: sequence, Intent: intent}
	var result MutationLookupResult
	if err := a.call(ctx, AuthorityMessage{Action: "mutation-lookup", Identity: id, MutationLookup: &request}, &result); err != nil {
		return MutationLookupResult{}, err
	}
	if (result.Found && result.Entry == nil) || (!result.Found && result.Entry != nil) ||
		(result.Archived && (!result.Found || sequence > result.CompactedThrough)) ||
		((result.CompactedThrough == 0) != (result.ArchiveHead == nil)) || (result.ArchiveHead != nil && !result.ArchiveHead.Valid()) ||
		(!result.Archived && result.Found && sequence <= result.CompactedThrough) {
		return MutationLookupResult{}, fmt.Errorf("fencing: malformed mutation lookup result")
	}
	if result.Entry != nil && result.Entry.Intent != intent {
		return MutationLookupResult{}, fmt.Errorf("fencing: mutation lookup tuple differs")
	}
	if result.Entry != nil {
		if err := validateMutationLookupEntry(*result.Entry); err != nil {
			return MutationLookupResult{}, err
		}
	}
	return result, nil
}

func validateMutationLookupEntry(entry MutationEntry) error {
	if err := validateMutationIntent(entry.Intent); err != nil {
		return err
	}
	if len(entry.Dispatches) > MaxMutationDispatches || (len(entry.Dispatches) != 0 && entry.Preparation == nil) {
		return fmt.Errorf("fencing: malformed mutation lookup dispatch state")
	}
	for id, dispatch := range entry.Dispatches {
		if id == "" || len(id) > 128 || (dispatch.ReceiptDigest != "" && !mutationDigestValid(dispatch.ReceiptDigest)) {
			return fmt.Errorf("fencing: malformed mutation lookup dispatch receipt")
		}
	}
	if entry.Preparation != nil {
		if err := validateMutationPreparation(entry.Intent, *entry.Preparation); err != nil {
			return err
		}
	}
	if entry.Resolution != nil {
		r := entry.Resolution
		if !mutationDigestValid(r.ReceiptDigest) || (r.AllCopiesTerminal && !allDispatchesTerminal(entry)) {
			return fmt.Errorf("fencing: malformed mutation lookup resolution")
		}
		if entry.Preparation == nil {
			if r.Outcome != MutationRejected || r.PreparationHash != "" || r.PayloadDigest != "" || !r.AllCopiesTerminal {
				return fmt.Errorf("fencing: malformed cancelled mutation lookup resolution")
			}
		} else {
			if !mutationDigestValid(r.PreparationHash) {
				return fmt.Errorf("fencing: malformed mutation lookup preparation binding")
			}
			switch r.Outcome {
			case MutationCommitted:
				if r.PayloadDigest != entry.Intent.PayloadDigest {
					return fmt.Errorf("fencing: malformed committed mutation lookup")
				}
			case MutationRejected:
				if !r.AllCopiesTerminal || r.PayloadDigest != "" {
					return fmt.Errorf("fencing: malformed rejected mutation lookup")
				}
			case MutationExcluded:
				if entry.Preparation.Generation == 0 || r.PayloadDigest != "" {
					return fmt.Errorf("fencing: malformed excluded mutation lookup")
				}
			default:
				return fmt.Errorf("fencing: unknown mutation lookup outcome")
			}
		}
	}
	if entry.CopyCompletion != nil && (entry.Preparation == nil || entry.Resolution == nil || !mutationDigestValid(entry.CopyCompletion.PreparationHash) || !mutationDigestValid(entry.CopyCompletion.ReceiptDigest) || !allDispatchesTerminal(entry)) {
		return fmt.Errorf("fencing: malformed mutation lookup copy completion")
	}
	return nil
}
