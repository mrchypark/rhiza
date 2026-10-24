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

	objmetrics "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

var (
	ErrVoterStateLost          = errors.New("voter state continuity cannot be established")
	ErrVoterEnrollmentRequired = errors.New("existing voter requires offline enrollment")
	errStopVoterPrefixScan     = errors.New("stop voter prefix scan after legacy object")
)

// voterIdentityVersion 2 records the learner's public key instead of a hash of
// its retired private peer token. Version 1 registrations are rejected.
const voterIdentityVersion = 2

// The registration is immutable. It detects lost/replaced WAL directories, not
// cloned disks or rolled-back storage; those still require external fencing.
type voterIdentity struct {
	Version          int    `json:"version"`
	Cluster          string `json:"cluster"`
	Node             string `json:"node"`
	Membership       string `json:"membership"`
	Store            string `json:"store"`
	Nonce            string `json:"nonce"`
	Registered       bool   `json:"registered"`
	LearnerPublicKey string `json:"learner_public_key,omitempty"`
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
	if err != nil || len(nonce) != 32 || identity.Version != voterIdentityVersion || !state.hadWAL {
		return state, fmt.Errorf("%w: local identity or WAL manifest is missing/invalid; preserve the original disk", ErrVoterStateLost)
	}
	state.identity = &identity
	return state, nil
}

func voterIdentityFor(config *types.ExecutionConfig) voterIdentity {
	membership := recovery.NewMembershipRecord(string(config.ClusterID), config.Members, string(config.ObjStoreDurability))
	store, _ := json.Marshal([]string{config.ObjStoreProvider, config.ObjStoreEndpoint, config.ObjStoreBucket, path.Clean(config.ObjStorePrefix), config.ObjStoreAzureStorageAccount})
	identity := voterIdentity{Version: voterIdentityVersion, Cluster: string(config.ClusterID), Node: string(config.NodeID), Membership: membership.Membership, Store: fmt.Sprintf("%x", sha256.Sum256(store))}
	if config.Learner != nil {
		identity.LearnerPublicKey = fmt.Sprintf("%x", config.Learner.PublicKey[:])
	}
	return identity
}

// ensureVoterIdentity runs before any archive replay or peer listener. An
// archive contains decided values, not the lost voter's outstanding promises.
func ensureVoterIdentity(ctx context.Context, config *types.ExecutionConfig, bucket objstore.Bucket, wal *qlog.WAL, local localVoterState, enroll bool, beforeWrite ...func(context.Context, string) error) error {
	return ensureVoterIdentityWithHooks(ctx, config, bucket, wal, local, enroll, beforeWrite, nil)
}

func ensureVoterIdentityWithHooks(ctx context.Context, config *types.ExecutionConfig, bucket objstore.Bucket, wal *qlog.WAL, local localVoterState, enroll bool, beforeWrite []func(context.Context, string) error, pendingIdentityWrite []func(context.Context, string, []byte) error, authorityAssignedRestart ...bool) error {
	want := voterIdentityFor(config)
	assignedRestart := len(authorityAssignedRestart) != 0 && authorityAssignedRestart[0]
	if assignedRestart {
		if local.identity == nil || !local.identity.Registered || config.Learner != nil || local.identity.Node != string(config.NodeID) || local.identity.Cluster != string(config.ClusterID) {
			return fmt.Errorf("%w: authority-assigned restart lacks its registered logical identity", ErrVoterStateLost)
		}
		// Only the fencer can set this branch after verifying the completed
		// same-ordinal assignment. The immutable voter record intentionally
		// retains the cohort under which its learner identity was first bound.
		want.Membership = local.identity.Membership
		want.LearnerPublicKey = local.identity.LearnerPublicKey
	}
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
	clusterWant, _ := json.Marshal(recovery.NewMembershipRecord(want.Cluster, config.Members, string(config.ObjStoreDurability)))
	if clusterData == nil {
		// A voter namespace with identity records but no canonical membership is
		// ambiguous, including during offline enrollment. For the legacy
		// multi-voter non-enrollment path, also preserve the older-database guard.
		scanRoot := prefix + "/"
		if len(config.Members) > 1 && !enroll {
			scanRoot = path.Join(config.ObjStorePrefix, string(config.ClusterID)) + "/"
		}
		priorVoterRecord := false
		legacyObjectFound := false
		if err := bucket.Iter(ctx, scanRoot, func(key string) error {
			if !strings.HasPrefix(key, prefix+"/") {
				if len(config.Members) > 1 && !enroll {
					legacyObjectFound = true
					return errStopVoterPrefixScan
				}
				return nil
			}
			if key != clusterKey {
				priorVoterRecord = true
			}
			return nil
		}); err != nil && !errors.Is(err, errStopVoterPrefixScan) {
			return err
		}
		// Always reread after the scan: a concurrent first registrant may have
		// published membership without its identity yet, or just after Iter saw
		// an empty prefix. Single-voter recovery must not bypass that publication.
		clusterData, err = readVoterRegistration(ctx, bucket, clusterKey)
		if err != nil {
			return err
		}
		if clusterData == nil && priorVoterRecord {
			return fmt.Errorf("%w: voter records exist without canonical membership", ErrVoterStateLost)
		}
		if clusterData == nil && legacyObjectFound {
			return ErrVoterEnrollmentRequired
		}
		if clusterData != nil && !assignedRestart {
			if err := matchVoterRegistration(ctx, bucket, clusterKey, clusterWant, clusterData); err != nil {
				return err
			}
		}
	}
	// Single-voter archive recovery remains supported only when the scan and
	// reread establish that this is still an unregistered namespace.
	if len(config.Members) <= 1 && clusterData == nil && local.identity == nil && !enroll {
		return nil
	}
	if assignedRestart && clusterData == nil {
		return fmt.Errorf("%w: assigned restart lacks its immutable voter genesis", ErrVoterStateLost)
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
	if assignedRestart && clusterData != nil {
		var genesis recovery.MembershipRecord
		decoder := json.NewDecoder(bytes.NewReader(clusterData))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&genesis) != nil || decoder.Decode(new(any)) != io.EOF || genesis.Version != recovery.MembershipRecordVersion ||
			genesis.Cluster != want.Cluster || genesis.Membership != want.Membership || genesis.Durability != string(config.ObjStoreDurability) {
			return fmt.Errorf("%w: assigned restart does not match the immutable voter genesis", ErrVoterStateLost)
		}
		canonical, _ := json.Marshal(genesis)
		if !bytes.Equal(canonical, clusterData) {
			return fmt.Errorf("%w: assigned restart voter genesis is not canonical", ErrVoterStateLost)
		}
		clusterWant = append([]byte(nil), clusterData...)
	}
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
	if len(beforeWrite) != 0 {
		nonce, _ := hex.DecodeString(local.identity.Nonce)
		if err := wal.BindIdentity(nonce); err != nil {
			return err
		}
		if err := beforeWrite[0](ctx, local.identity.Nonce); err != nil {
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
	if registered == nil && len(pendingIdentityWrite) != 0 {
		if err := pendingIdentityWrite[0](ctx, identityKey, data); err != nil {
			return err
		}
		registered, err = readVoterRegistration(ctx, bucket, identityKey)
		if err != nil {
			return err
		}
		if registered == nil || !bytes.Equal(data, registered) {
			return fmt.Errorf("%w: authority learner identity readback differs", ErrVoterStateLost)
		}
	} else if err := matchVoterRegistration(ctx, bucket, identityKey, data, registered); err != nil {
		return err
	}
	if !local.identity.Registered {
		return writeVoterIdentity(config.DataDir, remote)
	}
	return nil
}

func readVoterRegistration(ctx context.Context, bucket objstore.Bucket, key string) ([]byte, error) {
	r, err := bucket.Get(objmetrics.WithExpectedNotFound(ctx), key)
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
