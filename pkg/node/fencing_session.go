package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
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

func (n *Node) connectFenceSession(ctx context.Context) error {
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

func (n *Node) bindFenceWAL(ctx context.Context, walIdentity string) error {
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

func (n *Node) startFenceWatcher(ctx context.Context) {
	session := n.fenceSession
	if session == nil {
		return
	}
	if session.StartupRole() == fencing.StartupRoleLearnerPendingRemove {
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
				if session.StartupRole() == fencing.StartupRoleLearnerPendingRemove {
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
