package node

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestManagedMembershipStageDiagnosticIsBoundedAndRedacted(t *testing.T) {
	request := network.MembershipChange{
		OperationID: "private-operation-id",
		ClusterID:   "private-cluster-id",
		Add: &quepaxa.Member{
			ID:          "private-node-id",
			WALIdentity: "private-wal-token",
			PeerURL:     "https://private-peer.invalid",
		},
	}
	permitRequest := ManagedMembershipRequest{Action: ManagedMembershipAdd}
	ctx := context.WithValue(context.Background(), managedMembershipPermitKey{}, managedMembershipPermit{
		request: permitRequest,
		change:  request,
	})
	original := errors.New("private object path and payload")
	wrapped := errors.Join(network.ErrRequestConflict, original)

	var output bytes.Buffer
	previousOutput := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previousOutput) })

	logManagedMembershipStageFailure(ctx, request, managedMembershipStageRegistration, wrapped)
	line := output.String()
	if !strings.Contains(line, "MANAGED_MEMBERSHIP_STAGE_FAILURE action=add stage=registration cause=request_conflict") {
		t.Fatalf("diagnostic omitted fixed stage/cause: %q", line)
	}
	for _, secret := range []string{"private-operation-id", "private-cluster-id", "private-node-id", "private-wal-token", "private-peer.invalid", "private object path", "payload"} {
		if strings.Contains(line, secret) {
			t.Fatalf("diagnostic leaked %q: %q", secret, line)
		}
	}
	if !errors.Is(wrapped, network.ErrRequestConflict) || !errors.Is(wrapped, original) {
		t.Fatalf("diagnostic changed the original wrapped error: %v", wrapped)
	}
}

func TestManagedMembershipStageDiagnosticRequiresManagedAddPermit(t *testing.T) {
	request := network.MembershipChange{Add: &quepaxa.Member{ID: "node"}}
	managedAdd := context.WithValue(context.Background(), managedMembershipPermitKey{}, managedMembershipPermit{
		request: ManagedMembershipRequest{Action: ManagedMembershipAdd},
		change:  request,
	})
	ordinaryAdd := context.Background()
	wrongChange := context.WithValue(context.Background(), managedMembershipPermitKey{}, managedMembershipPermit{
		request: ManagedMembershipRequest{Action: ManagedMembershipAdd},
		change:  network.MembershipChange{Add: &quepaxa.Member{ID: "different-node"}},
	})
	managedRemove := context.WithValue(context.Background(), managedMembershipPermitKey{}, managedMembershipPermit{
		request: ManagedMembershipRequest{Action: ManagedMembershipRemove},
		change:  request,
	})

	for _, test := range []struct {
		name string
		ctx  context.Context
		want bool
	}{
		{name: "managed add", ctx: managedAdd, want: true},
		{name: "ordinary add", ctx: ordinaryAdd},
		{name: "mismatched permit", ctx: wrongChange},
		{name: "managed remove", ctx: managedRemove},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			previousOutput := log.Writer()
			log.SetOutput(&output)
			t.Cleanup(func() { log.SetOutput(previousOutput) })
			logManagedMembershipStageFailure(test.ctx, request, managedMembershipStageStorageCAS, errors.New("unavailable"))
			got := strings.Contains(output.String(), "MANAGED_MEMBERSHIP_STAGE_FAILURE")
			if got != test.want {
				t.Fatalf("diagnostic emitted=%v, want %v; output=%q", got, test.want, output.String())
			}
		})
	}
}
