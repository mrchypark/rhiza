package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
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

func (n *Node) bindFenceWAL(ctx context.Context, walIdentity string) error {
	if err := n.fenceSession.BindWAL(ctx, walIdentity); err != nil {
		return err
	}
	if n.protectedBucket == nil {
		return fmt.Errorf("supervised source mutation bucket is unavailable")
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
	return nil
}

func (n *Node) startFenceWatcher(ctx context.Context) {
	session := n.fenceSession
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
