package rhiza_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	thanosobjstore "github.com/thanos-io/objstore"
)

// TestS3MembershipGenerationForkE2E verifies an externally fenced, anchored
// membership generation transition against real S3 and QUIC voters.
func TestS3MembershipGenerationForkE2E(t *testing.T) {
	endpoint, bucketName := requireS3E2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	generation := fmt.Sprintf("membership-generation-%d", time.Now().UnixNano())
	sourceID, targetID := generation+"-source", generation+"-target"
	source, sourceMembers := e2eVoters(t, ctx, sourceID, 3, rhiza.ObjectStoreDurabilityBeforeAck)
	if _, err := source[0].Execute(ctx, rhiza.ExecuteRequest{RequestID: "generation-schema", SQL: "CREATE TABLE generation_items (id INTEGER PRIMARY KEY, value TEXT)"}); err != nil {
		t.Fatal(err)
	}
	request := rhiza.ExecuteRequest{RequestID: "generation-source-value", SQL: "INSERT INTO generation_items VALUES (1, 'preserved')"}
	if _, err := source[0].Execute(ctx, request); err != nil {
		t.Fatal(err)
	}

	lost, err := source[2].MembershipStatus()
	if err != nil || lost.WALIdentity == "" {
		t.Fatalf("removed voter identity: %+v err=%v", lost, err)
	}
	if err := source[2].Close(); err != nil {
		t.Fatal(err)
	}
	remove := rhiza.MembershipChange{
		OperationID: generation + "-remove", ClusterID: sourceID, ExpectedConfigID: 1, Remove: sourceMembers[2].ID,
		Fence: &rhiza.MembershipFence{NodeID: sourceMembers[2].ID, WALIdentity: lost.WALIdentity, WorkloadUID: "closed-test-process", Confirmed: true, Evidence: "test closed the original voter process"},
	}
	if err := source[0].ChangeMembership(ctx, remove); err != nil {
		t.Fatalf("remove fenced source voter: %v", err)
	}
	if status, err := source[0].MembershipStatus(); err != nil || status.ConfigID != 2 {
		t.Fatalf("source membership did not advance: %+v err=%v", status, err)
	}
	// Closing the remaining source processes is the test's external writer fence.
	closeAll(t, source[0], source[1])

	bucket, err := objstore.NewBucket(objstore.Config{
		Provider: objstore.ProviderS3, Endpoint: endpoint, Bucket: bucketName, Region: "us-east-1", Insecure: true,
		AccessKey: os.Getenv("RHIZA_E2E_S3_ACCESS_KEY"), SecretKey: os.Getenv("RHIZA_E2E_S3_SECRET_KEY"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closer, ok := bucket.Bucket.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}()

	targetMembers := make([]rhiza.Member, 3)
	for i := range targetMembers {
		targetMembers[i] = rhiza.Member{ID: quepaxa.NodeID(fmt.Sprintf("target-%d", i+1)), PeerURL: "quic://" + freeUDPAddr(t), Token: fmt.Sprintf("target-token-%d", i+1)}
	}
	sourcePrefix, targetPrefix := sourceID, targetID
	operationID := generation + "-fork"
	if err := recovery.Seal(ctx, bucket, sourcePrefix, operationID); err != nil {
		t.Fatalf("seal externally fenced source: %v", err)
	}
	targetMembership := recovery.NewMembershipRecord(targetID, targetMembers, string(rhiza.ObjectStoreDurabilityBeforeAck))
	result, err := recovery.Fork(ctx, bucket, recovery.ForkOptions{
		SourcePrefix: sourcePrefix, TargetPrefix: targetPrefix, SourceBootstrap: quepaxa.Cluster{ConfigID: 1, Members: sourceMembers},
		TargetMembers: targetMembers, TargetMembership: targetMembership, OperationID: operationID,
	})
	if err != nil || result.Tip == 0 {
		t.Fatalf("fork anchored generation: result=%+v err=%v", result, err)
	}
	anchor, _, err := recovery.ReadGenerationAnchor(ctx, bucket, targetPrefix)
	if err != nil || anchor.TargetMembership != targetMembership || anchor.SourcePrefix != sourcePrefix {
		t.Fatalf("generation anchor: %+v err=%v", anchor, err)
	}
	membership, err := json.Marshal(targetMembership)
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(ctx, path.Join(targetPrefix, "voters", "membership.json"), bytes.NewReader(membership), thanosobjstore.WithIfNotExists()); err != nil {
		t.Fatalf("register immutable target membership: %v", err)
	}

	wrong := e2eS3Config(t, targetID, string(targetMembers[0].ID), strings.TrimPrefix(targetMembers[0].PeerURL, "quic://"), targetMembers)
	wrong.ObjStoreDurability = rhiza.ObjectStoreDurabilityBeforeAck
	wrong.Members = append([]rhiza.Member(nil), targetMembers...)
	wrong.Members[0].Token = "wrong-target-token"
	blocker, err := net.ListenPacket("udp", wrong.PeerAddr)
	if err != nil {
		t.Fatal(err)
	}
	if reopened, err := rhiza.Open(ctx, wrong); reopened != nil || !errors.Is(err, rhiza.ErrVoterStateLost) {
		if reopened != nil {
			_ = reopened.Close()
		}
		_ = blocker.Close()
		t.Fatalf("wrong target credentials started or reached peer bind: err=%v", err)
	}
	if err := blocker.Close(); err != nil {
		t.Fatal(err)
	}

	target := make([]*rhiza.DB, len(targetMembers))
	configs := make([]rhiza.Config, len(targetMembers))
	for i, member := range targetMembers {
		configs[i] = e2eS3Config(t, targetID, string(member.ID), strings.TrimPrefix(member.PeerURL, "quic://"), targetMembers)
		configs[i].ObjStoreDurability = rhiza.ObjectStoreDurabilityBeforeAck
		target[i], err = rhiza.Open(ctx, configs[i])
		if err != nil {
			closeAll(t, target[:i]...)
			t.Fatalf("open target voter %s: %v", member.ID, err)
		}
		db := target[i]
		t.Cleanup(func() { _ = db.Close() })
	}
	for _, voter := range target {
		membershipWaitReady(t, ctx, voter)
		rows, err := voter.Query(ctx, rhiza.QueryRequest{SQL: "SELECT value FROM generation_items WHERE id = 1", Consistency: rhiza.ConsistencyLocal})
		if err != nil || len(rows.Rows) != 1 || rows.Rows[0][0] != "preserved" {
			t.Fatalf("target did not recover source data: rows=%v err=%v", rows.Rows, err)
		}
	}
	if _, err := target[0].Execute(ctx, request); err != nil {
		t.Fatalf("recovered request deduplication: %v", err)
	}
	if _, err := target[0].Execute(ctx, rhiza.ExecuteRequest{RequestID: "generation-target-value", SQL: "INSERT INTO generation_items VALUES (2, 'target')"}); err != nil {
		t.Fatalf("target credential write: %v", err)
	}
	if err := target[0].Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := rhiza.Open(ctx, configs[0])
	if err != nil {
		t.Fatalf("restart target preserves anchored lineage: %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	target[0] = restarted
	membershipWaitReady(t, ctx, restarted)
	rows, err := restarted.Query(ctx, rhiza.QueryRequest{SQL: "SELECT value FROM generation_items ORDER BY id", Consistency: rhiza.ConsistencyLocal})
	if err != nil || len(rows.Rows) != 2 || rows.Rows[0][0] != "preserved" || rows.Rows[1][0] != "target" {
		t.Fatalf("target restart lost generation state: rows=%v err=%v", rows.Rows, err)
	}
}
