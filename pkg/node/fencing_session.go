package node

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func validateAuthorityProfile(config *types.ExecutionConfig, socket, ticket string) error {
	if config == nil {
		return fmt.Errorf("fencing: configuration is unavailable")
	}
	configured := config.AuthorityProfile != "" || config.AuthorityURL != "" || config.AuthorityCAFile != "" || config.AuthorityTokenFile != ""
	if !configured {
		return nil
	}
	if config.AuthorityProfile != fencing.LogicalWriterProfileV1 {
		return fmt.Errorf("fencing: unsupported or incomplete authority profile %q", config.AuthorityProfile)
	}
	if config.AuthorityURL == "" || config.AuthorityTokenFile == "" {
		return fmt.Errorf("fencing: logical authority requires RHIZA_AUTHORITY_URL and RHIZA_AUTHORITY_TOKEN_FILE")
	}
	if config.AuthorityCAFile == "" {
		return fmt.Errorf("fencing: logical authority requires RHIZA_AUTHORITY_CA_FILE for its authority trust root")
	}
	if socket != "" || ticket != "" {
		return fmt.Errorf("fencing: logical authority profile cannot be combined with supervisor socket or ticket")
	}
	u, err := url.Parse(config.AuthorityURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "/v1/supervisor" {
		return fmt.Errorf("fencing: logical authority URL must be the authenticated HTTPS /v1/supervisor endpoint")
	}
	if config.Local || len(config.Members) < 2 || !config.EnableReconfiguration {
		return fmt.Errorf("fencing: logical authority requires multi-voter reconfiguration mode")
	}
	if objectstore.Provider(config.ObjStoreProvider) != objectstore.ProviderGCS {
		return fmt.Errorf("fencing: logical source-mutation authority currently requires GCS")
	}
	if config.ObjStoreDurability != types.ObjectStoreDurabilityBeforeAck {
		return fmt.Errorf("fencing: logical authority requires before-ack durability")
	}
	return nil
}

func newAuthorityHTTPClient(caFile string) (*http.Client, error) {
	if caFile == "" {
		return nil, fmt.Errorf("fencing: authority CA file is required")
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("fencing: load system CA roots: %w", err)
	}
	if roots == nil {
		roots = x509.NewCertPool()
	}
	file, err := os.Open(caFile)
	if err != nil {
		return nil, fmt.Errorf("fencing: open authority CA file: %w", err)
	}
	const maxAuthorityCABytes = 1 << 20
	data, readErr := io.ReadAll(io.LimitReader(file, maxAuthorityCABytes+1))
	closeErr := file.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, fmt.Errorf("fencing: read authority CA file: %w", err)
	}
	if len(data) > maxAuthorityCABytes {
		return nil, fmt.Errorf("fencing: authority CA file exceeds size limit")
	}
	if !roots.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("fencing: authority CA file contains no certificates")
	}
	return &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}}, nil
}

func (n *Node) connectFenceSession(ctx context.Context) error {
	if n != nil && n.config != nil && n.config.AuthorityProfile == fencing.LogicalWriterProfileV1 {
		if n.fenceSession != nil || n.authorityClient != nil {
			return fmt.Errorf("fencing: startup authority is already connected")
		}
		podName := os.Getenv("RHIZA_POD_NAME")
		if podName == "" {
			podName = string(n.config.NodeID)
		}
		podUID := os.Getenv("POD_UID")
		if podUID == "" {
			return fmt.Errorf("fencing: logical authority requires the authenticated POD_UID environment")
		}
		local, err := loadVoterIdentity(n.config)
		if err != nil {
			return fmt.Errorf("fencing: inspect local identity before logical registration: %w", err)
		}
		runState, continuation, err := loadOrCreateLogicalRunState(n.config, podUID, local)
		if err != nil {
			return err
		}
		identity := fencing.Identity{SourceClusterID: string(n.config.ClusterID), NodeID: string(n.config.NodeID),
			PodName: podName, PodUID: podUID, RunID: runState.RunID}
		if runState.AssignedNodeID != "" {
			identity.NodeID = runState.AssignedNodeID
			identity.AssignmentID = runState.AssignmentID
		}
		if continuation {
			identity.WALIdentity = local.identity.Nonce
		}
		client, err := newAuthorityHTTPClient(n.config.AuthorityCAFile)
		if err != nil {
			return err
		}
		authority := &fencing.AuthorityClient{URL: n.config.AuthorityURL, TokenFile: n.config.AuthorityTokenFile,
			Client: client, AuthorityProfile: n.config.AuthorityProfile}
		var grant fencing.StartupGrant
		if continuation {
			grant, err = authority.RegisterStartupContinuation(ctx, identity, identity.WALIdentity)
		} else {
			grant, err = authority.RegisterStartup(ctx, identity)
		}
		if err != nil {
			client.CloseIdleConnections()
			return fmt.Errorf("fencing: authenticated logical startup registration: %w", err)
		}
		if grant.AuthorityProfile != fencing.LogicalWriterProfileV1 || grant.Identity.SourceClusterID != identity.SourceClusterID ||
			grant.Identity.PodUID != identity.PodUID || grant.Identity.PodName != identity.PodName || grant.Identity.RunID != identity.RunID || grant.LogicalBinding == nil ||
			grant.LogicalBinding.Validate() != nil || grant.LogicalBinding.Role != grant.Role ||
			(identity.AssignmentID != "" && (grant.Identity.NodeID != identity.NodeID || grant.Identity.AssignmentID != identity.AssignmentID)) {
			client.CloseIdleConnections()
			return fmt.Errorf("fencing: logical startup grant differs from the requested workload identity")
		}
		if continuation {
			// The authority validates the exact persisted Run/Pod/WAL tuple from
			// RegistrationWALIdentity, then returns that Run's canonical grant.
			// It deliberately does not synthesize a Recovery binding: doing so
			// would change the binding pinned to the mutation producer/journal.
			wantIdentity := identity
			wantIdentity.WALIdentity = ""
			if grant.Identity != wantIdentity || grant.ExpectedWALIdentity != "" ||
				grant.LogicalBinding.Recovery != (fencing.LogicalRecoveryBindingV1{}) {
				client.CloseIdleConnections()
				return fmt.Errorf("fencing: logical continuation changed the canonical Run binding")
			}
		}
		runState, err = persistLogicalRunAssignment(n.config, runState, grant.Identity)
		if err != nil {
			client.CloseIdleConnections()
			return err
		}
		n.authorityClient = authority
		n.authorityGrant = grant
		n.authorityIdentity = grant.Identity
		return nil
	}
	socket, ticket := os.Getenv("RHIZA_FENCE_SOCKET"), os.Getenv("RHIZA_FENCE_TICKET")
	if socket == "" && ticket == "" {
		return nil
	}
	if socket == "" || ticket == "" {
		return fmt.Errorf("supervised fencing requires both socket and ticket")
	}
	if n.config.Local || len(n.config.Members) < 2 {
		return fmt.Errorf("supervised fencing requires registered shared-storage voters")
	}
	session, err := fencing.Connect(ctx, socket, ticket, string(n.config.ClusterID), string(n.config.NodeID))
	if err != nil {
		return fmt.Errorf("connect fencing startup gate: %w", err)
	}
	n.fenceSession = session
	return nil
}

func (n *Node) hasFencingAuthority() bool {
	return n != nil && (n.fenceSession != nil || n.authorityClient != nil)
}

func (n *Node) applyFenceStartupEnvironment() error {
	if n == nil || n.config == nil || n.fenceSession == nil {
		return fmt.Errorf("fencing: authenticated startup configuration is unavailable")
	}
	identity := n.fenceSession.Identity()
	if identity.SourceClusterID != string(n.config.ClusterID) {
		return fmt.Errorf("fencing: startup grant source cluster differs from child configuration")
	}
	environment := n.fenceSession.StartupEnvironment()
	if logicalID := environment["RHIZA_NODE_ID"]; logicalID != "" && logicalID != identity.NodeID {
		return fmt.Errorf("fencing: startup grant environment differs from its assigned identity")
	}
	if membersJSON := environment["RHIZA_CLUSTER_MEMBERS"]; membersJSON != "" {
		var members []types.NodeConfig
		if err := decodeFenceStartupJSON(membersJSON, &members); err != nil || len(members) < 2 {
			return fmt.Errorf("fencing: startup grant contains invalid voter membership")
		}
		n.config.Members = members
	}
	if token := environment["RHIZA_PEER_TOKEN"]; token != "" {
		n.config.PeerToken = token
	}
	if n.fenceSession.StartupRole() == fencing.StartupRoleLearnerPendingRemove {
		learnerJSON := environment["RHIZA_LEARNER"]
		var learner quepaxa.Member
		if learnerJSON == "" || decodeFenceStartupJSON(learnerJSON, &learner) != nil || learner.ID != quepaxa.NodeID(identity.NodeID) || learner.PublicKey == (quepaxa.PublicKey{}) {
			return fmt.Errorf("fencing: pending learner grant contains invalid candidate membership")
		}
		n.config.Learner = &learner
	} else {
		n.config.Learner = nil
	}
	if identity.NodeID == "" {
		return fmt.Errorf("fencing: startup grant has no logical identity")
	}
	n.config.NodeID = types.NodeID(identity.NodeID)
	if err := validateVoterMembership(n.config); err != nil {
		return fmt.Errorf("fencing: authority-pinned startup membership is invalid: %w", err)
	}
	return nil
}

func (n *Node) applyAuthorityStartupEnvironment() error {
	if n == nil || n.config == nil || n.authorityClient == nil || n.authorityGrant.AuthorityProfile != fencing.LogicalWriterProfileV1 {
		return fmt.Errorf("fencing: authenticated logical startup configuration is unavailable")
	}
	grant, physical := n.authorityGrant, n.authorityIdentity.PodName
	if physical == "" {
		physical = n.authorityIdentity.NodeID
	}
	if grant.Identity.SourceClusterID != string(n.config.ClusterID) || grant.Identity.PodUID != n.authorityIdentity.PodUID ||
		grant.Identity.RunID != n.authorityIdentity.RunID || physical == "" {
		return fmt.Errorf("fencing: logical startup grant differs from authenticated workload")
	}
	if nodeID := grant.Environment["RHIZA_NODE_ID"]; nodeID != "" && nodeID != grant.Identity.NodeID {
		return fmt.Errorf("fencing: logical startup environment differs from its assigned identity")
	}
	if membersJSON := grant.Environment["RHIZA_CLUSTER_MEMBERS"]; membersJSON != "" {
		var members []types.NodeConfig
		if err := decodeFenceStartupJSON(membersJSON, &members); err != nil {
			return fmt.Errorf("fencing: logical startup grant contains invalid voter membership: %w", err)
		}
		if len(members) < 2 {
			return fmt.Errorf("fencing: logical startup grant contains invalid voter membership")
		}
		n.config.Members = members
	}
	if token := grant.Environment["RHIZA_PEER_TOKEN"]; token != "" {
		n.config.PeerToken = token
	}
	n.config.NodeID = types.NodeID(grant.Identity.NodeID)
	if grant.Role == fencing.StartupRoleLearnerPendingRemove {
		var learner quepaxa.Member
		if err := decodeFenceStartupJSON(grant.Environment["RHIZA_LEARNER"], &learner); err != nil {
			return fmt.Errorf("fencing: logical pending learner grant contains invalid member identity: %w", err)
		}
		if learner.ID != quepaxa.NodeID(grant.Identity.NodeID) || learner.PublicKey == (quepaxa.PublicKey{}) {
			return fmt.Errorf("fencing: logical pending learner grant contains an invalid member identity")
		}
		n.config.Learner = &learner
	} else if grant.Role == fencing.StartupRoleVoterActive {
		n.config.Learner = nil
	} else {
		return fmt.Errorf("fencing: unsupported logical startup role %q", grant.Role)
	}
	if err := validateVoterMembership(n.config); err != nil {
		return fmt.Errorf("fencing: authority-pinned logical membership is invalid: %w", err)
	}
	return nil
}

func decodeFenceStartupJSON(raw string, target any) error {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("fencing: startup configuration has trailing JSON")
	}
	return nil
}

func logicalAssignedWALForStartup(grant fencing.StartupGrant, local localVoterState) (string, error) {
	if grant.ExpectedWALIdentity == "" {
		return "", nil
	}
	if local.identity != nil {
		if local.identity.Nonce != grant.ExpectedWALIdentity {
			return "", fmt.Errorf("%w: local WAL identity differs from authority-pinned logical recovery", ErrVoterStateLost)
		}
		return "", nil
	}
	if grant.Role != fencing.StartupRoleLearnerPendingRemove {
		return "", fmt.Errorf("%w: active logical grant cannot restore an assigned WAL into a fresh local directory", ErrVoterStateLost)
	}
	if local.hadWAL {
		return "", fmt.Errorf("%w: assigned logical learner has WAL data but no local identity", ErrVoterStateLost)
	}
	return grant.ExpectedWALIdentity, nil
}

func (n *Node) bindFenceWAL(ctx context.Context, walIdentity string) error {
	if n.authorityClient != nil {
		return n.bindAuthorityWAL(ctx, walIdentity)
	}
	if n.fenceSession == nil {
		return fmt.Errorf("supervised WAL binding requires an authenticated fencing session")
	}
	if err := n.fenceSession.BindWAL(ctx, walIdentity); err != nil {
		return err
	}
	if n.fenceSession.Identity().WALIdentity != walIdentity {
		return fmt.Errorf("fencing: authenticated WAL binding differs")
	}
	if n.protectedBucket == nil {
		return fmt.Errorf("supervised source mutation bucket is unavailable")
	}
	if n.fenceSession.StartupRole() == fencing.StartupRoleLearnerPendingRemove {
		// Pending learners may bind their WAL and read canonical archive state,
		// but receive no producer and no source write path. Their sole remote
		// registration write is separately authorized by RegisterLearnerIdentity.
		n.fenceWriterActive.Store(false)
		return nil
	}
	if _, independent := n.fenceSession.MutationBinding(); !independent {
		if err := n.protectedBucket.activateCooperativeDirect(); err != nil {
			return err
		}
		n.fenceWriterActive.Store(true)
		return nil
	}
	provider := objectstore.Provider(n.config.ObjStoreProvider)
	if provider == "" {
		provider = objectstore.ProviderS3
	}
	if provider != objectstore.ProviderGCS {
		return fmt.Errorf("supervised source mutations currently require GCS")
	}
	if err := rejectSupervisedWriterCredentials(n.config); err != nil {
		return err
	}
	dir, err := fencing.SourceRetryDirectory(n.config.DataDir, n.fenceSession.Identity())
	if err != nil {
		return fmt.Errorf("prepare source retry directory: %w", err)
	}
	client, err := fencing.OpenSourceMutationClient(dir, n.fenceSession)
	if err != nil {
		return fmt.Errorf("open source mutation client: %w", err)
	}
	n.protectedBucket.source = client
	binding, ok := n.fenceSession.MutationBinding()
	if !ok || binding.StoreScopeHash == "" {
		return fmt.Errorf("fencing: validated mutation binding disappeared")
	}
	scope := fencing.ArchiveScope{
		Provider: string(provider), Bucket: n.config.ObjStoreBucket,
		Prefix: n.protectedBucket.prefix, ClusterID: string(n.config.ClusterID),
	}
	membership := recovery.NewMembershipRecord(string(n.config.ClusterID), n.config.Members, string(n.config.ObjStoreDurability))
	if err := ensureArchiveGenesis(ctx, client, n.protectedBucket, scope, binding.StoreScopeHash, membership); err != nil {
		return err
	}
	n.fenceWriterActive.Store(true)
	return nil
}

func (n *Node) bindAuthorityWAL(ctx context.Context, walIdentity string) error {
	walBytes, decodeErr := hex.DecodeString(walIdentity)
	if n == nil || n.authorityClient == nil || n.protectedBucket == nil || decodeErr != nil || len(walBytes) != 32 {
		return fmt.Errorf("fencing: logical WAL binding prerequisites are unavailable")
	}
	identity := n.authorityIdentity
	identity.WALIdentity = walIdentity
	receipt, err := n.authorityClient.Bind(ctx, identity)
	if err != nil {
		return fmt.Errorf("fencing: bind logical WAL to authority: %w", err)
	}
	if err := receipt.Validate(identity); err != nil {
		return fmt.Errorf("fencing: invalid logical WAL bind receipt: %w", err)
	}
	if receipt.AuthorityProfile != fencing.LogicalWriterProfileV1 || receipt.Role != n.authorityGrant.Role {
		return fmt.Errorf("fencing: authority WAL receipt differs from logical startup role")
	}
	if receipt.Role == fencing.StartupRoleLearnerPendingRemove {
		if receipt.Producer != nil {
			return fmt.Errorf("fencing: pending logical learner received a mutation producer")
		}
		n.authorityIdentity = identity
		n.fenceWriterActive.Store(false)
		return nil
	}
	if receipt.Role != fencing.StartupRoleVoterActive || receipt.Producer == nil {
		return fmt.Errorf("fencing: authority returned no active logical mutation producer")
	}
	return n.activateAuthorityMutationClient(ctx, identity, receipt)
}

func (n *Node) activateAuthorityMutationClient(ctx context.Context, identity fencing.Identity, receipt fencing.BindReceipt) error {
	provider := objectstore.Provider(n.config.ObjStoreProvider)
	if provider == "" {
		provider = objectstore.ProviderS3
	}
	if provider != objectstore.ProviderGCS {
		return fmt.Errorf("fencing: logical source mutations currently require GCS")
	}
	if err := rejectSupervisedWriterCredentials(n.config); err != nil {
		return err
	}
	client, err := n.openAuthorityMutationClient(identity, receipt)
	if err != nil {
		return err
	}
	n.protectedBucket.source = client
	scope := fencing.ArchiveScope{Provider: string(provider), Bucket: n.config.ObjStoreBucket,
		Prefix: n.protectedBucket.prefix, ClusterID: string(n.config.ClusterID)}
	membership := recovery.NewMembershipRecord(string(n.config.ClusterID), n.config.Members, string(n.config.ObjStoreDurability))
	if err := ensureArchiveGenesis(ctx, client, n.protectedBucket, scope, receipt.Producer.StoreScopeHash, membership); err != nil {
		return err
	}
	n.authorityIdentity = identity
	n.authorityGrant.Role = fencing.StartupRoleVoterActive
	if receipt.LogicalBinding != nil {
		binding := *receipt.LogicalBinding
		n.authorityGrant.LogicalBinding = &binding
	}
	n.fenceWriterActive.Store(true)
	return nil
}

func (n *Node) openAuthorityMutationClient(identity fencing.Identity, receipt fencing.BindReceipt) (*fencing.SourceMutationClient, error) {
	if n == nil || n.config == nil || n.authorityClient == nil || identity.WALIdentity == "" {
		return nil, fmt.Errorf("fencing: logical source mutation identity is unavailable")
	}
	dir, err := fencing.SourceRetryDirectory(n.config.DataDir, identity)
	if err != nil {
		return nil, fmt.Errorf("fencing: prepare logical source retry directory: %w", err)
	}
	var client *fencing.SourceMutationClient
	if _, statErr := os.Lstat(dir); statErr == nil {
		client, err = fencing.OpenAuthoritySourceMutationClient(dir, n.authorityClient, receipt)
	} else if errors.Is(statErr, os.ErrNotExist) {
		client, err = fencing.CreateAuthoritySourceMutationClient(dir, n.authorityClient, receipt)
	} else {
		err = statErr
	}
	if err != nil {
		return nil, fmt.Errorf("fencing: open logical source mutation journal: %w", err)
	}
	return client, nil
}

func (n *Node) startFenceWatcher(ctx context.Context) {
	session := n.fenceSession
	logicalPending := n.authorityClient != nil && n.authorityGrant.Role == fencing.StartupRoleLearnerPendingRemove
	if session == nil && !logicalPending {
		return
	}
	if logicalPending || session != nil && session.StartupRole() == fencing.StartupRoleLearnerPendingRemove {
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				if n.core == nil || !n.core.IsVoter() {
					continue
				}
				// Once the locally applied configuration includes this identity,
				// keep client mutations gated until the authority confirms the
				// canonical REMOVE/ADD journal and post-commit durability ACK.
				n.ready.Store(false)
				promoteCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				err := error(nil)
				if logicalPending {
					if n.authorityPromotionReceipt == nil {
						var receipt fencing.BindReceipt
						receipt, err = n.authorityClient.PromoteLearner(promoteCtx, n.authorityIdentity)
						if err == nil {
							copy := receipt
							n.authorityPromotionReceipt = &copy
						}
					}
					if err == nil {
						err = n.activateAuthorityMutationClient(promoteCtx, n.authorityIdentity, *n.authorityPromotionReceipt)
					}
				} else if session.StartupRole() == fencing.StartupRoleLearnerPendingRemove {
					err = session.PromoteLearner(promoteCtx)
				}
				if err == nil && !n.fenceWriterActive.Load() {
					err = n.bindFenceWAL(promoteCtx, n.core.WALIdentity())
				}
				cancel()
				if err != nil {
					if !errors.Is(err, fencing.ErrPending) && !errors.Is(err, fencing.ErrUnavailable) && ctx.Err() == nil {
						log.Printf("fencing learner promotion remains unavailable")
					}
					continue
				}
				// Promotion changes the local voting boundary; require a fresh
				// quorum round against the current membership before serving.
				n.wakeCatchUp()
				return
			}
		}()
	}
	if session == nil {
		return
	}
	// This watcher must not join n.wg: it initiates the shutdown that joins it.
	go func() {
		for {
			request, err := session.WaitStop(ctx)
			if retryFenceSession(err) {
				timer := time.NewTimer(250 * time.Millisecond)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
				continue
			}
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("fencing supervisor unavailable; stopping engine without a drain receipt")
					_ = n.Shutdown()
				}
				return
			}
			expected := network.TerminalDrainRequest{ClusterID: request.Identity.SourceClusterID, NodeID: request.Identity.NodeID, WALIdentity: request.Identity.WALIdentity}
			result, drainErr := n.TerminalDrain(context.Background(), expected)
			if drainErr != nil || !result.StorageKnown {
				log.Printf("fencing terminal drain remains unconfirmed")
				return
			}
			for {
				ackCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				err := session.AcknowledgeDrain(ackCtx, fencing.DrainReceipt{Request: request, WritersJoined: true})
				cancel()
				if err == nil {
					return
				}
				if !retryFenceSession(err) {
					log.Printf("fencing drain acknowledgement rejected")
					return
				}
				// Replay only the same clean receipt; a lost acknowledgement never
				// authorizes a fresh process or a different recovery operation.
				time.Sleep(250 * time.Millisecond)
			}
		}
	}()
}

func retryFenceSession(err error) bool {
	if errors.Is(err, fencing.ErrPending) || errors.Is(err, fencing.ErrUnavailable) {
		return true
	}
	var transport net.Error
	return errors.As(err, &transport)
}
