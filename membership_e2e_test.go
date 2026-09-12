package rhiza_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func requireS3E2E(t *testing.T) (endpoint, bucket string) {
	t.Helper()
	endpoint, bucket = os.Getenv("RHIZA_E2E_S3_ENDPOINT"), os.Getenv("RHIZA_E2E_S3_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Skip("RHIZA_E2E_S3_ENDPOINT and RHIZA_E2E_S3_BUCKET are required")
	}
	return
}

func e2eS3Config(t *testing.T, clusterID, nodeID, peerAddr string, members []rhiza.Member) rhiza.Config {
	t.Helper()
	return rhiza.Config{
		ClusterID: clusterID, NodeID: nodeID, DataDir: t.TempDir(), PeerAddr: peerAddr,
		Members: members, AdminToken: "admin", EnableReconfiguration: true, ObjStoreProvider: "s3", ObjStoreEndpoint: os.Getenv("RHIZA_E2E_S3_ENDPOINT"),
		ObjStoreBucket: os.Getenv("RHIZA_E2E_S3_BUCKET"), ObjStoreRegion: "us-east-1", ObjStoreInsecure: true,
		ObjStoreAccessKey: os.Getenv("RHIZA_E2E_S3_ACCESS_KEY"), ObjStoreSecretKey: os.Getenv("RHIZA_E2E_S3_SECRET_KEY"),
		ObjStoreDurability: rhiza.ObjectStoreDurabilityAsync, ObjStoreSyncInterval: time.Hour,
	}
}

func e2eVoters(t *testing.T, ctx context.Context, clusterID string, count int, durability ...rhiza.ObjectStoreDurability) ([]*rhiza.DB, []rhiza.Member) {
	t.Helper()
	names := []string{"voter-1", "voter-2", "voter-3"}
	tokens := []string{"voter-token-1", "voter-token-2", "voter-token-3"}
	members := make([]rhiza.Member, count)
	for i := range members {
		members[i] = rhiza.Member{
			ID: quepaxa.NodeID(names[i]), PeerURL: "quic://" + freeUDPAddr(t),
			Token: tokens[i],
		}
	}
	dbs := make([]*rhiza.DB, count)
	for i, m := range members {
		config := e2eS3Config(t, clusterID, string(m.ID), strings.TrimPrefix(m.PeerURL, "quic://"), members)
		if len(durability) != 0 {
			config.ObjStoreDurability = durability[0]
		}
		db, err := rhiza.Open(ctx, config)
		if err != nil {
			t.Fatalf("open voter %s: %v", m.ID, err)
		}
		dbs[i] = db
		t.Cleanup(func() { _ = db.Close() })
	}
	for _, db := range dbs {
		membershipWaitReady(t, ctx, db)
	}
	return dbs, members
}

func closeAll(t *testing.T, dbs ...*rhiza.DB) {
	t.Helper()
	for _, db := range dbs {
		if err := db.Close(); err != nil {
			t.Logf("close: %v", err)
		}
	}
}

func learnerS3Config(t *testing.T, clusterID, peerAddr string, members []rhiza.Member, token string) rhiza.Config {
	t.Helper()
	return rhiza.Config{
		ClusterID: clusterID, NodeID: "learner", DataDir: t.TempDir(), PeerAddr: peerAddr,
		AdminToken: "admin", Members: members, EnableReconfiguration: true,
		Learner:          &rhiza.Member{ID: "learner", Token: token},
		ObjStoreProvider: "s3", ObjStoreEndpoint: os.Getenv("RHIZA_E2E_S3_ENDPOINT"),
		ObjStoreBucket: os.Getenv("RHIZA_E2E_S3_BUCKET"), ObjStoreRegion: "us-east-1", ObjStoreInsecure: true,
		ObjStoreAccessKey: os.Getenv("RHIZA_E2E_S3_ACCESS_KEY"), ObjStoreSecretKey: os.Getenv("RHIZA_E2E_S3_SECRET_KEY"),
		ObjStoreDurability: rhiza.ObjectStoreDurabilityAsync,
	}
}

// TestS3MembershipLearnerStartsNonvoterAndRejectsPropose proves that a learner opened
// through the real Node path (rhiza.Open + Learner config) starts as a
// nonvoter and cannot execute mutations.
func TestS3MembershipLearnerStartsNonvoterAndRejectsPropose(t *testing.T) {
	_, _ = requireS3E2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	clusterID := fmt.Sprintf("learner-nonvoter-%d", time.Now().UnixNano())
	voters, members := e2eVoters(t, ctx, clusterID, 3)
	defer closeAll(t, voters...)

	learner, err := rhiza.Open(ctx, learnerS3Config(t, clusterID, freeUDPAddr(t), members, "learner-token"))
	if err != nil {
		t.Fatal(err)
	}
	defer learner.Close()

	membershipWaitReady(t, ctx, learner)
	status, statusErr := learner.MembershipStatus()
	if statusErr != nil || status.Voting || status.WALIdentity == "" {
		t.Fatalf("learner status: %+v err=%v", status, statusErr)
	}
	// Learner is nonvoter — Execute must be rejected.
	_, err = learner.Execute(ctx, rhiza.ExecuteRequest{RequestID: "learner-write", SQL: "CREATE TABLE t (id INTEGER)"})
	if err == nil {
		t.Fatal("learner accepted a mutation")
	}
}

// TestS3MembershipLearnerRestartPreservesIdentity proves that closing and reopening a
// learner with the same config succeeds — the WAL nonce was persisted and
// the voter identity system accepts the restart.
func TestS3MembershipLearnerRestartPreservesIdentity(t *testing.T) {
	_, _ = requireS3E2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	clusterID := fmt.Sprintf("learner-restart-%d", time.Now().UnixNano())
	voters, members := e2eVoters(t, ctx, clusterID, 3)
	defer closeAll(t, voters...)

	config := learnerS3Config(t, clusterID, freeUDPAddr(t), members, "learner-token")
	learner, err := rhiza.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	before, err := learner.MembershipStatus()
	if err != nil || before.WALIdentity == "" {
		t.Fatalf("initial WAL identity: %+v %v", before, err)
	}
	if err := learner.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen with the same config — the WAL nonce is bound, so this must succeed.
	learner2, err := rhiza.Open(ctx, config)
	if err != nil {
		t.Fatalf("learner restart with same config: %v", err)
	}
	after, err := learner2.MembershipStatus()
	if err != nil || after.WALIdentity != before.WALIdentity {
		t.Fatalf("restart changed WAL identity: %+v %v", after, err)
	}
	if err := learner2.Close(); err != nil {
		t.Fatal(err)
	}
	wrong := config
	copyMember := *config.Learner
	copyMember.WALIdentity = "wrong-incarnation"
	wrong.Learner = &copyMember
	if reopened, err := rhiza.Open(ctx, wrong); err == nil {
		_ = reopened.Close()
		t.Fatal("accepted wrong configured WAL identity")
	}
}

// TestS3MembershipLearnerRestartRejectsWrongToken proves that reopening a learner with a
// different token is rejected — the voter identity system detects the token/
// identity mismatch and returns ErrVoterStateLost.
func TestS3MembershipLearnerRestartRejectsWrongToken(t *testing.T) {
	_, _ = requireS3E2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	clusterID := fmt.Sprintf("learner-wrong-token-%d", time.Now().UnixNano())
	voters, members := e2eVoters(t, ctx, clusterID, 3)
	defer closeAll(t, voters...)

	config := learnerS3Config(t, clusterID, freeUDPAddr(t), members, "learner-token")
	learner, err := rhiza.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := learner.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen with a different token — must be rejected.
	wrongConfig := config
	wrongConfig.Learner = &rhiza.Member{ID: "learner", Token: "wrong-token"}
	_, err = rhiza.Open(ctx, wrongConfig)
	if !errors.Is(err, rhiza.ErrVoterStateLost) {
		t.Fatalf("wrong token restart: err=%v, want ErrVoterStateLost", err)
	}
}

// TestS3MembershipLearnerWriteViaVoterVisibleAfterSync proves that data written through
// a voter is visible to the learner after it catches up, confirming the
// learner's QUIC peer path works end-to-end.
func TestS3MembershipLearnerWriteViaVoterVisibleAfterSync(t *testing.T) {
	_, _ = requireS3E2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	clusterID := fmt.Sprintf("learner-sync-%d", time.Now().UnixNano())
	voters, members := e2eVoters(t, ctx, clusterID, 3)
	defer closeAll(t, voters...)

	// Write through a voter.
	if _, err := voters[0].Execute(ctx, rhiza.ExecuteRequest{
		RequestID: "schema", SQL: "CREATE TABLE sync_test (id INTEGER PRIMARY KEY, val TEXT)",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := voters[0].Execute(ctx, rhiza.ExecuteRequest{
		RequestID: "insert", SQL: "INSERT INTO sync_test VALUES (1, 'e2e')",
	}); err != nil {
		t.Fatal(err)
	}

	learner, err := rhiza.Open(ctx, learnerS3Config(t, clusterID, freeUDPAddr(t), members, "learner-token"))
	if err != nil {
		t.Fatal(err)
	}
	defer learner.Close()

	// Poll until the learner catches up via QUIC peer sync.
	deadline := time.After(90 * time.Second)
	for {
		rows, err := learner.Query(ctx, rhiza.QueryRequest{SQL: "SELECT val FROM sync_test WHERE id = 1", Consistency: rhiza.ConsistencyLocal})
		if err == nil && len(rows.Rows) == 1 && rows.Rows[0][0] == "e2e" {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("learner did not sync: rows=%v err=%v", rows.Rows, err)
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func membershipWaitReady(t *testing.T, ctx context.Context, db *rhiza.DB) {
	t.Helper()
	for !db.Ready() {
		select {
		case <-ctx.Done():
			t.Fatalf("membership node not ready: %v", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestS3MembershipReplacement(t *testing.T) {
	_, _ = requireS3E2E(t)
	for _, durability := range []rhiza.ObjectStoreDurability{rhiza.ObjectStoreDurabilityAsync, rhiza.ObjectStoreDurabilityBeforeAck} {
		t.Run(string(durability), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			clusterID := fmt.Sprintf("membership-replace-%d", time.Now().UnixNano())
			voters, members := e2eVoters(t, ctx, clusterID, 3, durability)
			if _, err := voters[0].Execute(ctx, rhiza.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE replacement (id INTEGER PRIMARY KEY)"}); err != nil {
				t.Fatal(err)
			}
			lost, err := voters[2].MembershipStatus()
			if err != nil || lost.WALIdentity == "" {
				t.Fatalf("lost incarnation: %+v %v", lost, err)
			}
			if err := voters[2].Close(); err != nil {
				t.Fatal(err)
			}
			remove := rhiza.MembershipChange{OperationID: "remove", ClusterID: clusterID, ExpectedConfigID: 1, Remove: members[2].ID, Fence: &rhiza.MembershipFence{NodeID: members[2].ID, WALIdentity: lost.WALIdentity, WorkloadUID: "test-stopped-voter", Confirmed: true, Evidence: "test closed the original process"}}
			if err := voters[0].ChangeMembership(ctx, remove); err != nil {
				t.Fatalf("remove unavailable voter: %v", err)
			}
			if err := voters[0].ChangeMembership(ctx, remove); err != nil {
				t.Fatalf("retry removal: %v", err)
			}
			config := learnerS3Config(t, clusterID, freeUDPAddr(t), members, "replacement-token")
			config.ObjStoreDurability = durability
			learner, err := rhiza.Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = learner.Close() })
			status, err := learner.MembershipStatus()
			if err != nil || status.Voting || status.WALIdentity == "" {
				t.Fatalf("learner before promotion: %+v %v", status, err)
			}
			candidate := *config.Learner
			candidate.PeerURL, candidate.WALIdentity = "quic://"+config.PeerAddr, status.WALIdentity
			add := rhiza.MembershipChange{OperationID: "add", ClusterID: clusterID, ExpectedConfigID: 2, Add: &candidate}
			if err := voters[0].ChangeMembership(ctx, add); err != nil {
				t.Fatalf("promote learner: %v", err)
			}
			for {
				promoted, err := learner.MembershipStatus()
				if err == nil && promoted.Voting && promoted.ConfigID == 3 {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatalf("learner not promoted: %+v %v", promoted, err)
				case <-time.After(20 * time.Millisecond):
				}
			}
			if _, err := learner.Execute(ctx, rhiza.ExecuteRequest{RequestID: "after-replacement", SQL: "INSERT INTO replacement VALUES (1)"}); err != nil {
				t.Fatalf("new voter write: %v", err)
			}
			if err := learner.Close(); err != nil {
				t.Fatal(err)
			}
			restarted, err := rhiza.Open(ctx, config)
			if err != nil {
				t.Fatalf("promoted learner restart: %v", err)
			}
			t.Cleanup(func() { _ = restarted.Close() })
			after, err := restarted.MembershipStatus()
			if err != nil || !after.Voting || after.WALIdentity != status.WALIdentity || after.ConfigID != 3 {
				t.Fatalf("restart status: %+v %v", after, err)
			}
		})
	}
}
