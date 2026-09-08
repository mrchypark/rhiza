package node

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/thanos-io/objstore"
)

var (
	ErrVoterStateLost          = errors.New("voter state continuity cannot be established")
	ErrVoterEnrollmentRequired = errors.New("existing voter requires offline enrollment")
)

// The registration is immutable. It detects lost/replaced WAL directories, not
// cloned disks or rolled-back storage; those still require external fencing.
type voterIdentity struct {
	Version    int    `json:"version"`
	Cluster    string `json:"cluster"`
	Node       string `json:"node"`
	Membership string `json:"membership"`
	Store      string `json:"store"`
	Nonce      string `json:"nonce"`
	Registered bool   `json:"registered"`
}

type localVoterState struct {
	identity *voterIdentity
	hadWAL   bool
}

// Read before qlog.Open, which can create a new empty WAL manifest.
func loadVoterIdentity(config *types.ExecutionConfig) (localVoterState, error) {
	dir := filepath.Join(config.DataDir, "qlog")
	manifests, err := filepath.Glob(filepath.Join(dir, "manifest_*.bin"))
	if err != nil {
		return localVoterState{}, err
	}
	state := localVoterState{hadWAL: len(manifests) != 0}
	data, err := os.ReadFile(filepath.Join(dir, "voter-identity.json"))
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	var identity voterIdentity
	if err := json.Unmarshal(data, &identity); err != nil {
		return state, fmt.Errorf("%w: invalid local identity", ErrVoterStateLost)
	}
	nonce, err := hex.DecodeString(identity.Nonce)
	if err != nil || len(nonce) != 32 || identity.Version != 1 || !state.hadWAL {
		return state, fmt.Errorf("%w: local identity or WAL manifest is missing/invalid; preserve the original disk", ErrVoterStateLost)
	}
	state.identity = &identity
	return state, nil
}

func voterIdentityFor(config *types.ExecutionConfig) voterIdentity {
	members := make([]string, 0, len(config.Members))
	for _, member := range config.Members {
		// Bind voting credentials, not addresses: moving an intact disk need not
		// change its authority. Never store credentials in registration metadata.
		encoded, _ := json.Marshal([]string{string(member.ID), fmt.Sprintf("%x", sha256.Sum256([]byte(member.Token)))})
		members = append(members, string(encoded))
	}
	slices.Sort(members)
	encoded, _ := json.Marshal(members)
	store, _ := json.Marshal([]string{config.ObjStoreProvider, config.ObjStoreEndpoint, config.ObjStoreBucket, path.Clean(config.ObjStorePrefix), config.ObjStoreAzureStorageAccount})
	return voterIdentity{Version: 1, Cluster: string(config.ClusterID), Node: string(config.NodeID), Membership: fmt.Sprintf("%x", sha256.Sum256(encoded)), Store: fmt.Sprintf("%x", sha256.Sum256(store))}
}

// ensureVoterIdentity runs before any archive replay or peer listener. An
// archive contains decided values, not the lost voter's outstanding promises.
func ensureVoterIdentity(ctx context.Context, config *types.ExecutionConfig, bucket objstore.Bucket, wal *qlog.WAL, local localVoterState, enroll bool) error {
	want := voterIdentityFor(config)
	bound, hasBinding := wal.Identity()
	if local.identity != nil {
		nonce, _ := hex.DecodeString(local.identity.Nonce)
		if hasBinding && !bytes.Equal(bound, nonce) || !hasBinding && local.identity.Registered {
			return fmt.Errorf("%w: WAL identity differs from local registration", ErrVoterStateLost)
		}
	} else if hasBinding {
		return fmt.Errorf("%w: bound WAL has no local identity; preserve the original registration", ErrVoterStateLost)
	}
	if local.identity != nil {
		got := *local.identity
		got.Nonce, got.Registered = "", false
		if got != want {
			return fmt.Errorf("%w: voter configuration changed", ErrVoterStateLost)
		}
	}
	if bucket == nil {
		if local.identity != nil || enroll {
			return fmt.Errorf("%w: registered voter requires its shared object store", ErrVoterStateLost)
		}
		return nil
	}
	prefix := path.Join(config.ObjStorePrefix, string(config.ClusterID), "voters")
	clusterKey := path.Join(prefix, "membership.json")
	clusterData, err := readVoterRegistration(ctx, bucket, clusterKey)
	if err != nil {
		return err
	}
	// Single-voter archive recovery remains supported, but must never bypass
	// a namespace that was registered for a multi-voter cluster.
	if len(config.Members) <= 1 && clusterData == nil && local.identity == nil && !enroll {
		return nil
	}
	if len(config.Members) <= 1 {
		return fmt.Errorf("%w: membership cannot be changed during startup", ErrVoterStateLost)
	}
	if !slices.Contains(bucket.SupportedObjectUploadOptions(), objstore.IfNotExists) {
		return fmt.Errorf("voter registration requires conditional object writes")
	}
	identityKey := path.Join(prefix, fmt.Sprintf("%x.json", sha256.Sum256([]byte(config.NodeID))))
	registered, err := readVoterRegistration(ctx, bucket, identityKey)
	if err != nil {
		return err
	}
	if local.identity != nil && !hasBinding && registered != nil {
		return fmt.Errorf("%w: registered WAL binding is absent", ErrVoterStateLost)
	}
	if local.identity == nil && registered != nil {
		return fmt.Errorf("%w: voter %s is registered but its local identity is absent", ErrVoterStateLost, config.NodeID)
	}
	if local.identity != nil && local.identity.Registered && (registered == nil || clusterData == nil) {
		return fmt.Errorf("%w: immutable remote registration is missing", ErrVoterStateLost)
	}
	if local.identity == nil && local.hadWAL && !enroll {
		return fmt.Errorf("%w: stop all voters and enroll each original WAL before upgrading", ErrVoterEnrollmentRequired)
	}
	if enroll && !local.hadWAL {
		return fmt.Errorf("%w: enrollment requires the original WAL", ErrVoterStateLost)
	}
	if clusterData == nil && !enroll {
		// A namespace containing older database state cannot be auto-adopted.
		if err := bucket.Iter(ctx, path.Join(config.ObjStorePrefix, string(config.ClusterID))+"/", func(key string) error {
			if !strings.HasPrefix(key, prefix+"/") {
				return ErrVoterEnrollmentRequired
			}
			return nil
		}); err != nil {
			return err
		}
	}
	clusterWant, _ := json.Marshal(struct {
		Version    int    `json:"version"`
		Cluster    string `json:"cluster"`
		Membership string `json:"membership"`
	}{1, want.Cluster, want.Membership})
	if local.identity == nil {
		var nonce [32]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return err
		}
		want.Nonce = hex.EncodeToString(nonce[:])
		local.identity = &want
		if err := writeVoterIdentity(config.DataDir, want); err != nil {
			return err
		}
	}
	if err := matchVoterRegistration(ctx, bucket, clusterKey, clusterWant, clusterData); err != nil {
		return err
	}
	// A pending marker may precede the WAL binding after an interrupted initial
	// registration. Only an absent remote registration permits completing it.
	if !hasBinding {
		nonce, _ := hex.DecodeString(local.identity.Nonce)
		if err := wal.BindIdentity(nonce); err != nil {
			return err
		}
	}
	remote := *local.identity
	remote.Registered = true
	data, _ := json.Marshal(remote)
	if err := matchVoterRegistration(ctx, bucket, identityKey, data, registered); err != nil {
		return err
	}
	if !local.identity.Registered {
		return writeVoterIdentity(config.DataDir, remote)
	}
	return nil
}

func readVoterRegistration(ctx context.Context, bucket objstore.Bucket, key string) ([]byte, error) {
	r, err := bucket.Get(ctx, key)
	if bucket.IsObjNotFoundErr(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read voter registration: %w", err)
	}
	defer r.Close()
	data, err := io.ReadAll(io.LimitReader(r, 64<<10))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || len(data) == 64<<10 {
		return nil, fmt.Errorf("%w: invalid remote registration", ErrVoterStateLost)
	}
	return data, nil
}

func matchVoterRegistration(ctx context.Context, bucket objstore.Bucket, key string, want, got []byte) error {
	if got == nil {
		if err := bucket.Upload(ctx, key, bytes.NewReader(want), objstore.WithIfNotExists()); err != nil && !bucket.IsConditionNotMetErr(err) {
			return fmt.Errorf("register voter: %w", err)
		}
		var err error
		got, err = readVoterRegistration(ctx, bucket, key)
		if err != nil {
			return err
		}
	}
	if !bytes.Equal(want, got) {
		return fmt.Errorf("%w: immutable voter registration differs", ErrVoterStateLost)
	}
	return nil
}

func writeVoterIdentity(dataDir string, identity voterIdentity) error {
	dir := filepath.Join(dataDir, "qlog")
	data, _ := json.Marshal(identity)
	f, err := os.CreateTemp(dir, ".voter-identity-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), filepath.Join(dir, "voter-identity.json")); err != nil {
		return err
	}
	// Sync both the identity rename and a newly created qlog directory entry.
	for _, directory := range []string{dir, dataDir} {
		d, err := os.Open(directory)
		if err != nil {
			return err
		}
		err = d.Sync()
		closeErr := d.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}
