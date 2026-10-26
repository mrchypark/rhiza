package fencing

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// AuthorityMessage is sent only over authenticated TLS. The server authenticates
// the bound service account token independently of the claimed identity.
type AuthorityMessage struct {
	Action                  string                       `json:"action"`
	Identity                Identity                     `json:"identity"`
	InitialRegistration     bool                         `json:"initialRegistration,omitempty"`
	RegistrationWALIdentity string                       `json:"registrationWALIdentity,omitempty"`
	EngineRepair            *EngineRepairRequest         `json:"engineRepair,omitempty"`
	Drain                   *DrainReceipt                `json:"drain,omitempty"`
	Request                 *StopRequest                 `json:"request,omitempty"`
	MutationLookup          *MutationLookupRequest       `json:"mutationLookup,omitempty"`
	ArchiveGenesis          *ArchiveGenesisStatusRequest `json:"archiveGenesis,omitempty"`
	LearnerIdentity         *LearnerIdentityRegistration `json:"learnerIdentity,omitempty"`
	PreflightProposal       *PendingEngineRepairProposal `json:"preflightProposal,omitempty"`
}

// LearnerIdentityRegistration is the one create-only voter identity object a
// pending learner may register. The authority checks the exact key and bytes
// against its authenticated pending run before performing the conditional write.
type LearnerIdentityRegistration struct {
	Key  string `json:"key"`
	Data []byte `json:"data"`
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
	// Bound identity and producer are populated only by a validated Bind.
	// Source mutation endpoints are unavailable before that receipt.
	boundIdentity Identity
	producer      *ProducerBinding
	role          StartupRole
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
	if resp.StatusCode == http.StatusPreconditionFailed {
		return ErrMutationConditionFailed
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

func (a *AuthorityClient) RegisterStartup(ctx context.Context, id Identity) (StartupGrant, error) {
	return a.registerStartup(ctx, id, false)
}

func (a *AuthorityClient) RegisterInitialStartup(ctx context.Context, id Identity) (StartupGrant, error) {
	return a.registerStartup(ctx, id, true)
}

func (a *AuthorityClient) RegisterStartupContinuation(ctx context.Context, id Identity, wal string) (StartupGrant, error) {
	if !mutationDigestValid(wal) {
		return StartupGrant{}, fmt.Errorf("fencing: invalid registration continuation WAL")
	}
	var grant StartupGrant
	if err := a.call(ctx, AuthorityMessage{Action: "register", Identity: id, RegistrationWALIdentity: wal}, &grant); err != nil {
		return StartupGrant{}, err
	}
	if err := grant.Validate(id); err != nil {
		return StartupGrant{}, err
	}
	if grant.Role != StartupRoleVoterActive || grant.InitialRegistration != nil {
		return StartupGrant{}, fmt.Errorf("fencing: invalid registration continuation grant")
	}
	a.role = grant.Role
	return grant, nil
}

func (a *AuthorityClient) registerStartup(ctx context.Context, id Identity, initial bool) (StartupGrant, error) {
	var grant StartupGrant
	err := a.call(ctx, AuthorityMessage{Action: "register", Identity: id, InitialRegistration: initial}, &grant)
	if err != nil {
		return StartupGrant{}, err
	}
	if err := grant.Validate(id); err != nil {
		return StartupGrant{}, err
	}
	if initial && grant.InitialRegistration == nil {
		return StartupGrant{}, fmt.Errorf("fencing: initial startup grant lacks first-registration evidence")
	}
	a.role = grant.Role
	return grant, nil
}

func (a *AuthorityClient) CreatePendingEngineRepair(ctx context.Context, id Identity, proposal PendingEngineRepairProposal) error {
	if proposal.BootstrapIdentity != id || proposal.ValidatePreflight() != nil {
		return fmt.Errorf("fencing: invalid physical engine repair preflight")
	}
	return a.call(ctx, AuthorityMessage{Action: "engine-repair-preflight", Identity: id, PreflightProposal: &proposal}, nil)
}

func (a *AuthorityClient) CurrentEngineRepair(ctx context.Context, id Identity) (EngineRepairRecord, bool, error) {
	if a.role != StartupRoleVoterActive || id != a.boundIdentity || id.WALIdentity == "" {
		return EngineRepairRecord{}, false, fmt.Errorf("fencing: engine repair requires an active bound voter")
	}
	var response EngineRepairResponse
	request := EngineRepairRequest{Identity: id}
	if err := a.call(ctx, AuthorityMessage{Action: "engine-repair-current", Identity: id, EngineRepair: &request}, &response); err != nil {
		return EngineRepairRecord{}, false, err
	}
	if !response.Found {
		if response.Record != nil || response.Proof != nil {
			return EngineRepairRecord{}, false, fmt.Errorf("fencing: malformed empty engine repair response")
		}
		return EngineRepairRecord{}, false, nil
	}
	if response.Record == nil || response.Proof != nil || response.Record.Validate() != nil || response.Record.Binding.SourceClusterID != id.SourceClusterID {
		return EngineRepairRecord{}, false, fmt.Errorf("fencing: invalid engine repair response")
	}
	return *response.Record, true, nil
}

func (a *AuthorityClient) CompareAndSwapEngineRepair(ctx context.Context, id Identity, expectedDigest string, next EngineRepairRecord) (EngineRepairRecord, error) {
	if a.role != StartupRoleVoterActive || id != a.boundIdentity || id.WALIdentity == "" || expectedDigest == "" ||
		next.Validate() != nil || next.Binding.SourceClusterID != id.SourceClusterID {
		return EngineRepairRecord{}, fmt.Errorf("fencing: engine repair CAS is unavailable")
	}
	request := EngineRepairRequest{Identity: id, OperationID: next.Binding.OperationID, ExpectedDigest: expectedDigest, Record: &next}
	var response EngineRepairResponse
	if err := a.call(ctx, AuthorityMessage{Action: "engine-repair-cas", Identity: id, EngineRepair: &request}, &response); err != nil {
		return EngineRepairRecord{}, err
	}
	if !response.Found || response.Record == nil || response.Proof != nil || response.Record.Validate() != nil ||
		response.Record.IdentityDigest() != next.IdentityDigest() || response.Record.Binding.OperationID != next.Binding.OperationID {
		return EngineRepairRecord{}, fmt.Errorf("fencing: invalid engine repair CAS response")
	}
	return *response.Record, nil
}

func (a *AuthorityClient) CloseEngineRepairStorageEffects(ctx context.Context, id Identity, operationID string) (StorageEffectsExcluded, error) {
	if a.role != StartupRoleVoterActive || id != a.boundIdentity || id.WALIdentity == "" || operationID == "" {
		return StorageEffectsExcluded{}, fmt.Errorf("fencing: engine repair storage closure is unavailable")
	}
	request := EngineRepairRequest{Identity: id, OperationID: operationID}
	var response EngineRepairResponse
	if err := a.call(ctx, AuthorityMessage{Action: "engine-repair-storage-effects", Identity: id, EngineRepair: &request}, &response); err != nil {
		return StorageEffectsExcluded{}, err
	}
	if response.Found || response.Record != nil || response.Proof == nil || response.Proof.Binding.SourceClusterID != id.SourceClusterID || response.Proof.Binding.OperationID != operationID {
		return StorageEffectsExcluded{}, fmt.Errorf("fencing: invalid engine repair storage-closure response")
	}
	return *response.Proof, nil
}

func (a *AuthorityClient) VerifyEngineRemovalStorageEffects(ctx context.Context, id Identity, operation quepaxa.EngineRemoval) error {
	if a.role != StartupRoleVoterActive || id != a.boundIdentity || id.WALIdentity == "" || operation.OperationID == "" ||
		operation.StorageEffectsExcludedHash == (quepaxa.ValueHash{}) {
		return fmt.Errorf("fencing: engine removal storage-effects verification is unavailable")
	}
	request := EngineRepairRequest{Identity: id, OperationID: operation.OperationID, RemovalOperation: &operation}
	var response EngineRepairResponse
	if err := a.call(ctx, AuthorityMessage{Action: "engine-repair-storage-effects-verify", Identity: id, EngineRepair: &request}, &response); err != nil {
		return err
	}
	if response.Found || response.Record != nil || response.Proof != nil || len(response.PendingRequests) != 0 {
		return fmt.Errorf("fencing: malformed engine removal storage-effects verification response")
	}
	return nil
}

func (a *AuthorityClient) ConfirmEngineRepair(ctx context.Context, id Identity, operationID string) (EngineRepairRecord, error) {
	if a.role != StartupRoleVoterActive || id != a.boundIdentity || id.WALIdentity == "" || operationID == "" {
		return EngineRepairRecord{}, fmt.Errorf("fencing: engine repair confirmation is unavailable")
	}
	request := EngineRepairRequest{Identity: id, OperationID: operationID}
	var response EngineRepairResponse
	if err := a.call(ctx, AuthorityMessage{Action: "engine-repair-confirm", Identity: id, EngineRepair: &request}, &response); err != nil {
		return EngineRepairRecord{}, err
	}
	if !response.Found || response.Record == nil || response.Proof != nil || response.Record.Validate() != nil ||
		response.Record.Binding.SourceClusterID != id.SourceClusterID || response.Record.Binding.OperationID != operationID || response.Record.Phase != RepairActive {
		return EngineRepairRecord{}, fmt.Errorf("fencing: invalid engine repair confirmation response")
	}
	return *response.Record, nil
}

func (a *AuthorityClient) ListPendingEngineRepairs(ctx context.Context, id Identity) ([]PendingEngineRepairRequest, error) {
	if a.role != StartupRoleVoterActive || id != a.boundIdentity || id.WALIdentity == "" {
		return nil, fmt.Errorf("fencing: pending engine repair lookup requires an active bound voter")
	}
	request := EngineRepairRequest{Identity: id}
	var response EngineRepairResponse
	if err := a.call(ctx, AuthorityMessage{Action: "engine-repair-pending", Identity: id, EngineRepair: &request}, &response); err != nil {
		return nil, err
	}
	if response.Found || response.Record != nil || response.Proof != nil {
		return nil, fmt.Errorf("fencing: malformed pending engine repair response")
	}
	for _, pending := range response.PendingRequests {
		if pending.Validate() != nil || pending.SourceClusterID != id.SourceClusterID {
			return nil, fmt.Errorf("fencing: invalid pending engine repair response")
		}
	}
	return append([]PendingEngineRepairRequest(nil), response.PendingRequests...), nil
}

func (a *AuthorityClient) AdoptPendingEngineRepair(ctx context.Context, id Identity, pendingID, pendingDigest string, record EngineRepairRecord) (EngineRepairRecord, error) {
	if a.role != StartupRoleVoterActive || id != a.boundIdentity || id.WALIdentity == "" || pendingID == "" || !mutationDigestValid(pendingDigest) ||
		record.ValidateAdoptionProposal() != nil || record.Binding.SourceClusterID != id.SourceClusterID || record.BootstrapRequestDigest != pendingDigest {
		return EngineRepairRecord{}, fmt.Errorf("fencing: pending engine repair adoption is unavailable")
	}
	request := EngineRepairRequest{Identity: id, PendingID: pendingID, PendingDigest: pendingDigest, Record: &record}
	var response EngineRepairResponse
	if err := a.call(ctx, AuthorityMessage{Action: "engine-repair-adopt", Identity: id, EngineRepair: &request}, &response); err != nil {
		return EngineRepairRecord{}, err
	}
	if !response.Found || response.Record == nil || response.Proof != nil || len(response.PendingRequests) != 0 ||
		response.Record.Validate() != nil || response.Record.Binding.SourceClusterID != id.SourceClusterID || response.Record.BootstrapRequestDigest != pendingDigest {
		return EngineRepairRecord{}, fmt.Errorf("fencing: invalid pending engine repair adoption response")
	}
	return *response.Record, nil
}

func (a *AuthorityClient) RegisterLearnerIdentity(ctx context.Context, id Identity, registration LearnerIdentityRegistration) error {
	return a.call(ctx, AuthorityMessage{Action: "learner-identity", Identity: id, LearnerIdentity: &registration}, nil)
}

// PromoteLearner asks the authority to activate this already-bound WAL only
// after it has independently verified the committed membership journal.
func (a *AuthorityClient) PromoteLearner(ctx context.Context, id Identity) (BindReceipt, error) {
	var receipt BindReceipt
	err := a.call(ctx, AuthorityMessage{Action: "promote-learner", Identity: id}, &receipt)
	return receipt, err
}
func (a *AuthorityClient) Bind(ctx context.Context, id Identity) (BindReceipt, error) {
	a.boundIdentity = Identity{}
	a.producer = nil
	if a.role == "" {
		a.role = StartupRoleVoterActive
	}
	var receipt BindReceipt
	if err := a.call(ctx, AuthorityMessage{Action: "bind", Identity: id}, &receipt); err != nil {
		return BindReceipt{}, err
	}
	if err := receipt.Validate(id); err != nil {
		return BindReceipt{}, err
	}
	receiptRole := receipt.Role
	if receiptRole == "" {
		receiptRole = StartupRoleVoterActive
	}
	if receiptRole != a.role {
		return BindReceipt{}, fmt.Errorf("fencing: bind role differs from startup grant")
	}
	a.boundIdentity = id
	a.producer = nil
	if receipt.Producer != nil {
		producer := *receipt.Producer
		a.producer = &producer
	}
	return receipt, nil
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

func (a *AuthorityClient) sourceMutationBinding() (Identity, ProducerBinding, error) {
	if a == nil || (a.role != "" && a.role != StartupRoleVoterActive) || a.producer == nil || a.producer.StageIncarnation == "" || a.producer.ProducerUID == "" || a.producer.Runtime.StatefulSetUID == "" || a.boundIdentity.WALIdentity == "" {
		return Identity{}, ProducerBinding{}, fmt.Errorf("fencing: source mutation producer is not bound")
	}
	return a.boundIdentity, *a.producer, nil
}

func (a *AuthorityClient) sourceEndpoint(suffix string) (string, error) {
	u, err := url.Parse(a.URL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || !strings.HasSuffix(strings.TrimSuffix(u.Path, "/"), "/v1/supervisor") {
		return "", fmt.Errorf("fencing: HTTPS authority required")
	}
	pathSuffix, query, _ := strings.Cut(suffix, "?")
	u.Path = strings.TrimSuffix(strings.TrimSuffix(u.Path, "/"), "/v1/supervisor") + pathSuffix
	u.RawPath = ""
	u.RawQuery = query
	return u.String(), nil
}

func (a *AuthorityClient) sourcePost(ctx context.Context, endpoint string, body []byte, contentType string, headers http.Header, result any) error {
	token, err := os.ReadFile(a.TokenFile)
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(token)) == 0 || a.Client == nil {
		return fmt.Errorf("fencing: authenticated bounded authority client required")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	req.Header.Set("Content-Type", contentType)
	for key, values := range headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
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
	if resp.StatusCode == http.StatusPreconditionFailed {
		return ErrMutationConditionFailed
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
			return ErrUnavailable
		}
		return fmt.Errorf("fencing: source authority HTTP %d", resp.StatusCode)
	}
	if result == nil {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if err != nil || len(data) > 65536 {
		return fmt.Errorf("%w: source authority response unreadable", ErrUnavailable)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(result); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("fencing: trailing source authority response")
	}
	return nil
}

func (a *AuthorityClient) ReserveUpload(ctx context.Context, ticket string, draft MutationIntent) error {
	id, producer, err := a.sourceMutationBinding()
	if err != nil {
		return err
	}
	if !mutationDigestValid(ticket) || draft != uploadStageDraft(draft) || draft.StoreScopeHash != producer.StoreScopeHash {
		return fmt.Errorf("fencing: invalid source upload reservation")
	}
	endpoint, err := a.sourceEndpoint("/v1/source-stages/reserve")
	if err != nil {
		return err
	}
	body, err := json.Marshal(struct {
		Identity Identity       `json:"identity"`
		Ticket   string         `json:"ticket"`
		Draft    MutationIntent `json:"draft"`
	}{id, ticket, draft})
	if err != nil {
		return err
	}
	return a.sourcePost(ctx, endpoint, body, "application/json", nil, nil)
}

func (a *AuthorityClient) SealUpload(ctx context.Context, ticket string, payload []byte) (sourceSealedPayload, error) {
	id, _, err := a.sourceMutationBinding()
	if err != nil {
		return sourceSealedPayload{}, err
	}
	if !mutationDigestValid(ticket) || int64(len(payload)) > maxStagePayloadBytes {
		return sourceSealedPayload{}, fmt.Errorf("fencing: invalid source upload payload")
	}
	digest := sha256.Sum256(payload)
	endpoint, err := a.sourceEndpoint("/v1/source-stages/" + ticket + "?digest=" + hex.EncodeToString(digest[:]) + "&size=" + fmt.Sprint(len(payload)))
	if err != nil {
		return sourceSealedPayload{}, err
	}
	identity, err := json.Marshal(id)
	if err != nil {
		return sourceSealedPayload{}, err
	}
	headers := make(http.Header)
	headers.Set(sourceStageIdentityHeader, base64.RawURLEncoding.EncodeToString(identity))
	var sealed sourceSealedPayload
	if err := a.sourcePost(ctx, endpoint, payload, "application/octet-stream", headers, &sealed); err != nil {
		return sourceSealedPayload{}, err
	}
	if sealed.Ref != "payload-"+ticket || sealed.Size != int64(len(payload)) || sealed.Digest != hex.EncodeToString(digest[:]) {
		return sourceSealedPayload{}, fmt.Errorf("fencing: source authority sealed different bytes")
	}
	return sealed, nil
}

func (a *AuthorityClient) Admit(ctx context.Context, sequence uint64, intent MutationIntent) error {
	id, producer, err := a.sourceMutationBinding()
	if err != nil {
		return err
	}
	if sequence == 0 || intent.OperationID != MutationRequestID(producer.ProducerUID, sequence) || intent.StoreScopeHash != producer.StoreScopeHash {
		return fmt.Errorf("fencing: source mutation admission tuple differs")
	}
	return a.postMutation(ctx, "/v1/source-mutations/admit", id, producer.ProducerUID, sequence, intent)
}

func (a *AuthorityClient) Lookup(ctx context.Context, sequence uint64, intent MutationIntent) (MutationLookupResult, error) {
	id, producer, err := a.sourceMutationBinding()
	if err != nil {
		return MutationLookupResult{}, err
	}
	return a.LookupMutation(ctx, id, producer.ProducerUID, sequence, intent)
}

func (a *AuthorityClient) Execute(ctx context.Context, sequence uint64, intent MutationIntent) error {
	id, producer, err := a.sourceMutationBinding()
	if err != nil {
		return err
	}
	if sequence == 0 || intent.OperationID != MutationRequestID(producer.ProducerUID, sequence) || intent.StoreScopeHash != producer.StoreScopeHash {
		return fmt.Errorf("fencing: source mutation execution tuple differs")
	}
	return a.postMutation(ctx, "/v1/source-mutations/execute", id, producer.ProducerUID, sequence, intent)
}

// ArchiveGenesisStatus asks the canonical authority to reconcile the retained
// genesis tuple and report the current stable-scope reference. This is a POST
// because reconciliation may complete durable CAS/dispatch work.
func (a *AuthorityClient) ArchiveGenesisStatus(ctx context.Context, archiveKey string, configID uint) (ArchiveGenesisStatus, error) {
	id, producer, err := a.sourceMutationBinding()
	if err != nil {
		return ArchiveGenesisStatus{}, err
	}
	if archiveKey != "archive/head.bin" || configID == 0 {
		return ArchiveGenesisStatus{}, fmt.Errorf("fencing: invalid archive genesis key")
	}
	endpoint, err := a.sourceEndpoint("/v1/source-mutations/genesis-status")
	if err != nil {
		return ArchiveGenesisStatus{}, err
	}
	body, err := json.Marshal(struct {
		Identity       Identity                    `json:"identity"`
		ArchiveGenesis ArchiveGenesisStatusRequest `json:"archiveGenesis"`
	}{id, ArchiveGenesisStatusRequest{ArchiveKey: archiveKey, ConfigID: configID}})
	if err != nil {
		return ArchiveGenesisStatus{}, err
	}
	var result ArchiveGenesisStatus
	if err := a.sourcePost(ctx, endpoint, body, "application/json", nil, &result); err != nil {
		return ArchiveGenesisStatus{}, err
	}
	if err := result.Validate(producer.StoreScopeHash, configID); err != nil {
		return ArchiveGenesisStatus{}, err
	}
	return result, nil
}

func (a *AuthorityClient) postMutation(ctx context.Context, path string, id Identity, producerUID string, sequence uint64, intent MutationIntent) error {
	endpoint, err := a.sourceEndpoint(path)
	if err != nil {
		return err
	}
	body, err := json.Marshal(struct {
		Identity    Identity       `json:"identity"`
		ProducerUID string         `json:"producerUID"`
		Sequence    uint64         `json:"sequence"`
		Intent      MutationIntent `json:"intent"`
	}{id, producerUID, sequence, intent})
	if err != nil {
		return err
	}
	return a.sourcePost(ctx, endpoint, body, "application/json", nil, nil)
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
		preparedCancellation := entry.Preparation != nil && entry.DispatchClosed && len(entry.Dispatches) == 0 &&
			r.Outcome == MutationRejected && !r.ConditionFailed && r.PayloadDigest == ""
		if !mutationDigestValid(r.ReceiptDigest) || (r.AllCopiesTerminal && !allDispatchesTerminal(entry)) ||
			(r.AllCopiesTerminal && entry.Preparation != nil && len(entry.Dispatches) == 0 && !preparedCancellation && r.Reason != MutationResolutionReasonGenerationZeroPinned) {
			return fmt.Errorf("fencing: malformed mutation lookup resolution")
		}
		if err := validateMutationResolutionReason(entry, *r); err != nil {
			return fmt.Errorf("fencing: malformed mutation lookup terminal reason: %w", err)
		}
		if r.ConditionFailed && (r.Outcome != MutationRejected || entry.Intent.Condition.Mode == MutationUnconditional) {
			return fmt.Errorf("fencing: malformed conditional mutation rejection")
		}
		if entry.Preparation == nil {
			if r.Outcome != MutationRejected || r.PreparationHash != "" || r.PayloadDigest != "" || r.ConditionFailed || !r.AllCopiesTerminal {
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
	if entry.CopyCompletion != nil && (entry.Preparation == nil || entry.Resolution == nil || len(entry.Dispatches) == 0 || !mutationDigestValid(entry.CopyCompletion.PreparationHash) || !mutationDigestValid(entry.CopyCompletion.ReceiptDigest) || !allDispatchesTerminal(entry)) {
		return fmt.Errorf("fencing: malformed mutation lookup copy completion")
	}
	return nil
}
