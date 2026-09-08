package rhiza_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza"
	internalobjstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	thanosobjstore "github.com/thanos-io/objstore"
)

// TestOperatorRecoveryForkE2E exercises the storage half of the operator
// contract. The source has already been fenced by the time Fork runs; closing
// every source voter is the local equivalent of that fence.
func TestOperatorRecoveryForkE2E(t *testing.T) {
	endpoint, bucketName := os.Getenv("RHIZA_E2E_S3_ENDPOINT"), os.Getenv("RHIZA_E2E_S3_BUCKET")
	if endpoint == "" || bucketName == "" {
		t.Skip("RHIZA_E2E_S3_ENDPOINT and RHIZA_E2E_S3_BUCKET are required")
	}
	for _, durability := range []rhiza.ObjectStoreDurability{
		rhiza.ObjectStoreDurabilityAsync,
		rhiza.ObjectStoreDurabilityBeforeAck,
	} {
		t.Run(string(durability), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			generation := fmt.Sprintf("operator-fork-%d", time.Now().UnixNano())
			sourceCluster, targetCluster := generation+"-source", generation+"-target"
			storePrefix := generation + "/store"
			sourceMembers := recoveryTestMembers(t, "source-token")
			targetMembers := recoveryTestMembers(t, "target-token")
			base := rhiza.Config{
				ObjStoreProvider: "s3", ObjStoreEndpoint: endpoint, ObjStoreBucket: bucketName,
				ObjStoreRegion: "us-east-1", ObjStoreInsecure: true,
				ObjStoreAccessKey: os.Getenv("RHIZA_E2E_S3_ACCESS_KEY"),
				ObjStoreSecretKey: os.Getenv("RHIZA_E2E_S3_SECRET_KEY"),
				ObjStorePrefix:    storePrefix, ObjStoreDurability: durability,
				ObjStoreSyncInterval: 10 * time.Millisecond,
			}

			source, sourceConfigs := openRecoveryTestVoters(t, ctx, base, sourceCluster, sourceMembers)
			if _, _, err := executeRecoveryTest(ctx, source, rhiza.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE recovered_items (id INTEGER PRIMARY KEY, value TEXT)"}); err != nil {
				t.Fatal(err)
			}
			if _, _, err := executeRecoveryTest(ctx, source, rhiza.ExecuteRequest{RequestID: "value", SQL: "INSERT INTO recovered_items VALUES (1, 'preserved')"}); err != nil {
				t.Fatal(err)
			}
			closeRecoveryTestVoters(t, source)

			bucket, err := internalobjstore.NewBucket(internalobjstore.Config{
				Provider: internalobjstore.ProviderS3, Endpoint: endpoint, Bucket: bucketName, Region: "us-east-1", Insecure: true,
				AccessKey: base.ObjStoreAccessKey, SecretKey: base.ObjStoreSecretKey,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if closer, ok := bucket.Bucket.(interface{ Close() error }); ok {
					_ = closer.Close()
				}
			}()
			sourcePrefix := path.Join(storePrefix, sourceCluster)
			targetPrefix := path.Join(storePrefix, targetCluster)
			operationID := generation + "-operation"
			if err := recovery.Seal(ctx, bucket, sourcePrefix, operationID); err != nil {
				t.Fatalf("seal fenced source archive: %v", err)
			}
			if reopened, err := rhiza.Open(ctx, sourceConfigs[0]); reopened != nil || !errors.Is(err, recovery.ErrArchiveSealed) {
				if reopened != nil {
					_ = reopened.Close()
				}
				t.Fatalf("sealed source restart db=%v err=%v, want ErrArchiveSealed", reopened, err)
			}
			result, err := recovery.Fork(ctx, bucket, recovery.ForkOptions{
				SourcePrefix: sourcePrefix, TargetPrefix: targetPrefix, Members: sourceMembers,
				OperationID: operationID,
			})
			if err != nil {
				t.Fatalf("fork fenced source generation: %v", err)
			}
			if result.Tip == 0 {
				t.Fatal("fork returned an empty certified history")
			}

			membership, err := json.Marshal(recovery.NewMembershipRecord(targetCluster, targetMembers, string(durability)))
			if err != nil {
				t.Fatal(err)
			}
			if err := bucket.Upload(ctx, path.Join(targetPrefix, "voters", "membership.json"), bytes.NewReader(membership), thanosobjstore.WithIfNotExists()); err != nil {
				t.Fatalf("record target generation membership: %v", err)
			}

			target, _ := openRecoveryTestVoters(t, ctx, base, targetCluster, targetMembers)
			defer closeRecoveryTestVoters(t, target)
			for _, voter := range target {
				if err := waitRecoveryValue(ctx, voter, 1, "preserved"); err != nil {
					t.Fatal(err)
				}
			}
			writer, _, err := executeRecoveryTest(ctx, target, rhiza.ExecuteRequest{RequestID: "new-generation-write", SQL: "INSERT INTO recovered_items VALUES (2, 'new-generation')"})
			if err != nil {
				t.Fatalf("write through new generation quorum: %v", err)
			}
			if err := waitRecoveryValue(ctx, writer, 2, "new-generation"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func recoveryTestMembers(t testing.TB, tokenPrefix string) []rhiza.Member {
	t.Helper()
	members := make([]rhiza.Member, 3)
	for i := range members {
		members[i] = rhiza.Member{
			ID: quepaxa.NodeID(fmt.Sprintf("n%d", i+1)), PeerURL: "quic://" + freeUDPAddr(t),
			Token: fmt.Sprintf("%s-%d", tokenPrefix, i+1),
		}
	}
	return members
}

func openRecoveryTestVoters(t testing.TB, ctx context.Context, base rhiza.Config, cluster string, members []rhiza.Member) ([]*rhiza.DB, []rhiza.Config) {
	t.Helper()
	voters := make([]*rhiza.DB, 0, len(members))
	configs := make([]rhiza.Config, 0, len(members))
	for _, member := range members {
		config := base
		config.ClusterID, config.NodeID, config.DataDir, config.Members = cluster, string(member.ID), t.TempDir(), members
		config.PeerAddr = strings.TrimPrefix(member.PeerURL, "quic://")
		voter, err := rhiza.Open(ctx, config)
		if err != nil {
			closeRecoveryTestVoters(t, voters)
			t.Fatalf("open voter %s: %v", member.ID, err)
		}
		voters = append(voters, voter)
		configs = append(configs, config)
	}
	return voters, configs
}

func closeRecoveryTestVoters(t testing.TB, voters []*rhiza.DB) {
	t.Helper()
	errs := make([]error, len(voters))
	var group sync.WaitGroup
	for i, voter := range voters {
		group.Add(1)
		go func(i int, voter *rhiza.DB) {
			defer group.Done()
			errs[i] = voter.Close()
		}(i, voter)
	}
	group.Wait()
	for i, err := range errs {
		if err != nil {
			t.Logf("fenced voter %d shutdown: %v", i, err)
		}
	}
}

func queryRecoveryTest(ctx context.Context, voter *rhiza.DB, request rhiza.QueryRequest) (rhiza.QueryResponse, error) {
	var last error
	for {
		response, err := voter.Query(ctx, request)
		if err == nil {
			return response, nil
		}
		last = err
		select {
		case <-ctx.Done():
			return rhiza.QueryResponse{}, fmt.Errorf("query: %w (last error: %v)", ctx.Err(), last)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func waitRecoveryValue(ctx context.Context, voter *rhiza.DB, id int, want string) error {
	for {
		rows, err := queryRecoveryTest(ctx, voter, rhiza.QueryRequest{SQL: fmt.Sprintf("SELECT value FROM recovered_items WHERE id = %d", id)})
		if err == nil && len(rows.Rows) == 1 && rows.Rows[0][0] == want {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("voter did not apply recovered row %d=%q: rows=%#v err=%v", id, want, rows.Rows, err)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func executeRecoveryTest(ctx context.Context, voters []*rhiza.DB, request rhiza.ExecuteRequest) (*rhiza.DB, rhiza.ExecuteResponse, error) {
	var last error
	for {
		for _, voter := range voters {
			response, err := voter.Execute(ctx, request)
			if err == nil {
				return voter, response, nil
			}
			last = err
		}
		select {
		case <-ctx.Done():
			return nil, rhiza.ExecuteResponse{}, fmt.Errorf("execute %q: %w (last error: %v)", request.RequestID, ctx.Err(), last)
		case <-time.After(25 * time.Millisecond):
		}
	}
}
