package operator

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/mrchypark/rhiza/pkg/recoveryanchor"
	"github.com/thanos-io/objstore"
)

const ownerAnnotation = "rhiza.mrchypark.dev/recovery-owner"

var identifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`)

// AnchorClient is the recovery anchor interface. The shared recoveryanchor
// package or an HTTP Client implements it. The Controller never issues
// receipts directly; it delegates to this interface.
type AnchorClient interface {
	Activate(context.Context, recoveryanchor.Request) (recoveryanchor.Receipt, error)
	Verify(context.Context, recoveryanchor.Request, recoveryanchor.Receipt) error
}

type Controller struct {
	Fencer        Fencer
	Automatic     bool
	Kube          *Kubernetes
	Bucket        objstore.Bucket
	Prefix        string
	HTTP          *http.Client
	StoreIdentity map[string]string
	Anchor        AnchorClient
}
type recoveryList struct {
	Items []Resource `json:"items"`
}
type podList struct {
	Items []object `json:"items"`
}
type sourceState struct {
	prefix          string
	members         []quepaxa.Member
	durability      string
	reconfiguration bool
	env             map[string]string
	stateful        object
}

func (c *Controller) Run(ctx context.Context, interval time.Duration) error {
	if c.Kube == nil || c.Bucket == nil || interval <= 0 {
		return fmt.Errorf("Kubernetes client, bucket, and positive poll interval are required")
	}
	for {
		if err := c.reconcileAll(ctx); err != nil && ctx.Err() == nil {
			log.Printf("recovery reconciliation: %v", err)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
func (c *Controller) reconcileAll(ctx context.Context) error {
	var resources recoveryList
	if err := c.Kube.Get(ctx, c.resourcePath(""), &resources); err != nil {
		return err
	}
	var failures []error
	for i := range resources.Items {
		if err := c.reconcile(ctx, &resources.Items[i]); err != nil {
			failures = append(failures, fmt.Errorf("resource %s: %w", resourceName(&resources.Items[i]), err))
		}
	}
	if c.Automatic {
		if err := c.reconcileClusters(ctx); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
func resourceName(r *Resource) string { return str(r.Metadata["name"]) }
func (c *Controller) operationID(r *Resource) string {
	return str(r.Metadata["uid"]) + ":" + r.Spec.RecoveryID
}
func (c *Controller) reconcile(ctx context.Context, r *Resource) error {
	authorized, err := c.authorizeAutomaticChild(ctx, r)
	if err != nil || !authorized {
		return err
	}
	bootstrapConfirmation, err := c.confirmedBootstrapFenceForChild(ctx, r)
	if err != nil {
		return err
	}
	if r.Spec.Membership != nil {
		return c.reconcileMembership(ctx, r)
	}
	reject := func(message string) error { return c.record(ctx, r, "Blocked", message) }
	for _, value := range []string{resourceName(r), r.Spec.StatefulSet, r.Spec.SourceClusterID} {
		if !identifier.MatchString(value) {
			return reject("invalid recovery resource, StatefulSet, or cluster identifier")
		}
	}
	if r.Spec.Durability != "async" && r.Spec.Durability != "before-ack" {
		return reject("target durability must be async or before-ack")
	}
	if r.Spec.MaxArchiveAgeSeconds < 0 {
		return reject("maxArchiveAgeSeconds must be nonnegative")
	}
	if r.Spec.RecoveryID != "" && (!identifier.MatchString(r.Spec.RecoveryID) || str(r.Metadata["uid"]) == "") {
		return reject("recovery ID and resource UID are required")
	}
	if r.Status.RecoveryID != "" && (r.Status.RecoveryID != r.Spec.RecoveryID || r.Status.SpecHash != immutableSpecHash(r.Spec)) {
		return reject("reserved recovery request is immutable; create a new resource")
	}
	if r.Status.Stage == "Complete" {
		return c.releaseCompleted(ctx, r)
	}
	var sts object
	if err := c.Kube.Get(ctx, c.stsPath(r.Spec.StatefulSet), &sts); err != nil {
		return err
	}
	uid := str(nested(sts, "metadata", "uid"))
	if uid == "" || r.Status.RecoveryID != "" && uid != r.Status.StatefulSetUID {
		return reject("StatefulSet identity changed during recovery")
	}
	r.Status.ObservedGeneration = number(r.Metadata["generation"])
	if r.Status.Stage == "Starting" || r.Status.Stage == "Restarted" {
		return c.startAndObserve(ctx, r, sts)
	}
	ctr, err := container(sts, r.Spec.Container)
	if err != nil {
		return reject(err.Error())
	}
	env, err := c.environment(ctx, ctr)
	if err != nil {
		return reject(err.Error())
	}
	if err = noPVC(sts, ctr, env); err != nil {
		return reject(err.Error())
	}
	if err = c.storeConfigMatches(env); err != nil {
		return reject(err.Error())
	}
	if env["RHIZA_CLUSTER_ID"] != r.Spec.SourceClusterID {
		return reject("source cluster ID differs from StatefulSet environment")
	}
	members, err := recoveryMembers(env["RHIZA_CLUSTER_MEMBERS"])
	if err != nil {
		return reject(err.Error())
	}
	if len(members) < 3 {
		return reject("recovery requires at least three voters")
	}
	// Before the source is stopped, the declared membership and the replica count
	// are one contract; a mismatch would recover a different generation.
	if replicas := int(number(nested(sts, "spec", "replicas"))); r.Status.Stage == "" && replicas != len(members) {
		return reject(fmt.Sprintf("source StatefulSet has %d desired replicas but RHIZA_CLUSTER_MEMBERS lists %d voters", replicas, len(members)))
	}
	mode := env["RHIZA_OBJSTORE_DURABILITY"]
	if mode == "" {
		mode = "async"
	}
	reconfiguration, err := reconfigurationEnabled(env)
	if err != nil {
		return reject(err.Error())
	}
	if err := anchorEnvGate(r.Spec.AnchorID, env, reconfiguration, c.Anchor); err != nil {
		return reject(err.Error())
	}
	source := sourceState{prefix: path.Join(c.Prefix, r.Spec.SourceClusterID), members: members, durability: mode, reconfiguration: reconfiguration, env: env, stateful: sts}
	prefix := source.prefix
	registered, membershipMissing, err := c.sourceMembershipOrMissing(ctx, prefix)
	if err != nil {
		return reject(err.Error())
	}
	manualMissingMembership := membershipMissing && bootstrapConfirmation == nil &&
		manualMissingSourceMembershipAllowed(r) && c.useFrozenSourceFork(source) && mode == "async" && r.Spec.AllowDataLoss
	if membershipMissing {
		if manualMissingMembership {
			// This is only a provisional target input. The canonical fencer must
			// independently bind the complete declared cohort to the closed root
			// before any source-stop or target authorization side effect.
			registered = recovery.NewMembershipRecord(r.Spec.SourceClusterID, members, mode)
		} else if bootstrapConfirmation == nil || !c.useFrozenSourceFork(source) || mode != "async" || !r.Spec.AllowDataLoss {
			return reject("immutable source membership is missing outside the confirmed frozen bootstrap-genesis path")
		} else {
			members, err = c.confirmedBootstrapSourceMembers(ctx, r, sts, ctr, env, members, *bootstrapConfirmation)
			if err != nil {
				return reject("confirmed bootstrap source cohort differs from canonical fence evidence: " + err.Error())
			}
			// This value is a derived input for the target only. It is never written
			// back to the source archive or treated as an existing source receipt.
			registered = recovery.NewMembershipRecord(r.Spec.SourceClusterID, members, mode)
		}
	} else if registered != recovery.NewMembershipRecord(r.Spec.SourceClusterID, members, mode) {
		return reject("immutable source membership or durability differs from StatefulSet")
	}
	source.members = members
	fingerprint := sourceFingerprint(r.Spec.SourceClusterID, mode, members, prefix)
	if r.Status.RecoveryID != "" && fingerprint != r.Status.SourceMembership {
		return reject("source configuration changed during recovery")
	}
	r.Status.Source, r.Status.SourceDurability, r.Status.SourceMembership = r.Spec.SourceClusterID, registered.Durability, fingerprint
	r.Status.StatefulSetUID = uid
	if r.Spec.RecoveryID == "" {
		peers, err := c.probe(ctx, r, sts)
		if err != nil {
			return err
		}
		r.Status.Peers = peers
		return c.record(ctx, r, "Observed", "observation only; missing peers never authorize recovery")
	}
	if r.Spec.AnchorID != "" {
		if _, ok := c.Fencer.(ArchiveForkGenesisRegistrar); !ok {
			return reject("anchored recovery requires a configured fencer with canonical archive fork registration")
		}
	}
	if mode == "async" && !r.Spec.AllowDataLoss {
		return reject("async source requires explicit allowDataLoss, regardless of target mode")
	}
	if r.Status.RecoveryMode != "" && r.Status.RecoveryMode != RecoveryModeFrozenSource {
		return reject("recovery journal contains an unsupported immutable mode")
	}
	if r.Status.RecoveryMode == "" && r.Status.Stage == "" && r.Status.RecoveryID == "" && r.Status.Target == "" && c.useFrozenSourceFork(source) {
		r.Status.RecoveryMode = RecoveryModeFrozenSource
		r.Status.RecoveryID, r.Status.SpecHash = r.Spec.RecoveryID, immutableSpecHash(r.Spec)
		r.Status.StatefulSetUID, r.Status.Source = uid, r.Spec.SourceClusterID
		return c.record(ctx, r, "Recovering", "frozen-source mode selected before authenticated fencer preflight")
	}
	frozen := r.Status.RecoveryMode == RecoveryModeFrozenSource
	if err := validateFence(r.Spec.Fence, r.Spec.RecoveryID, r.Spec.SourceClusterID, uid); err != nil {
		return c.record(ctx, r, "AwaitingFence", err.Error())
	}
	if r.Status.Stage == "" {
		persistedTargetBaseKind := r.Status.TargetBaseKind
		persistedHeadlessGenesis := r.Status.FrozenEmptyGenesisPending
		archive := recovery.NewManager(c.Bucket, prefix, recovery.ArchiveConfigID)
		defer archive.Close()
		headlessGenesisCandidate := false
		if err := archive.Load(ctx); err != nil {
			if !frozen || mode != "async" || !r.Spec.AllowDataLoss || r.Spec.MaxArchiveAgeSeconds != 0 {
				return reject("source archive is unavailable")
			}
			headKey := path.Join(prefix, "archive/head.bin")
			if exists, existsErr := c.Bucket.Exists(ctx, headKey); existsErr != nil || exists {
				return reject("source archive HEAD is present or its absence cannot be verified")
			}
			if _, ok := c.Fencer.(ArchiveEmptyRecoveryRegistrar); !ok {
				return reject("frozen empty recovery registrar is unavailable")
			}
			if err := verifyFrozenEmptyArchiveNamespace(ctx, c.Bucket, prefix); err != nil {
				return reject("headless source contains archive history or cannot be enumerated")
			}
			r.Status.FrozenEmptyGenesisPending = true
			headlessGenesisCandidate = true
			r.Status.TargetBaseKind = "empty"
		} else if archive.Tip() == 0 {
			if _, exists := archive.HeadVersion(); !exists {
				return reject("source archive has no initialized empty HEAD")
			}
			r.Status.TargetBaseKind = "empty"
		} else {
			r.Status.TargetBaseKind = "fork"
		}
		if r.Spec.MaxArchiveAgeSeconds > 0 {
			attrs, err := c.Bucket.Attributes(ctx, path.Join(prefix, "archive/head.bin"))
			if err != nil {
				return reject("archive evidence age is unavailable")
			}
			if time.Since(attrs.LastModified).Seconds() > float64(r.Spec.MaxArchiveAgeSeconds) {
				return reject("archive evidence exceeds maxArchiveAgeSeconds; this is not a loss-window bound")
			}
		}
		if frozen {
			if persistedTargetBaseKind != "" && persistedTargetBaseKind != r.Status.TargetBaseKind {
				return reject("frozen-source target base changed during recovery")
			}
			if persistedTargetBaseKind != "" && persistedHeadlessGenesis != headlessGenesisCandidate {
				return reject("frozen-source empty-genesis mode changed during recovery")
			}
			if persistedTargetBaseKind == "" {
				r.Status.FrozenEmptyGenesisPending = headlessGenesisCandidate
				r.Status.RecoveryID, r.Status.SpecHash = r.Spec.RecoveryID, immutableSpecHash(r.Spec)
				r.Status.StatefulSetUID, r.Status.Source = uid, r.Spec.SourceClusterID
				return c.record(ctx, r, "Recovering", "frozen-source target base persisted before authenticated preflight")
			}
			preflight, err := c.frozenSourcePreflight(ctx, r)
			if err != nil {
				if errors.Is(err, ErrFencePending) {
					return c.record(ctx, r, "Recovering", "frozen-source closure is pending")
				}
				return reject("frozen-source canonical preflight failed: " + err.Error())
			}
			if headlessGenesisCandidate && !preflight.EmptyGenesisEligible {
				return reject("canonical source root has no exact eligible empty genesis")
			}
			if !preflight.ClosureReady {
				if preflight.ManualRecoveryChallenge != nil {
					challengeDigest, digestErr := ManualRecoveryChallengeDigest(*preflight.ManualRecoveryChallenge)
					if digestErr != nil || challengeDigest != preflight.ManualRecoveryChallengeDigest {
						return reject("manual terminal recovery challenge is inconsistent")
					}
					if r.Status.ManualRecoveryChallengeDigest != "" && r.Status.ManualRecoveryChallengeDigest != challengeDigest {
						return reject("manual terminal recovery challenge changed during recovery")
					}
					if r.Status.ManualRecoveryChallenge != nil && hashJSON(r.Status.ManualRecoveryChallenge) != hashJSON(preflight.ManualRecoveryChallenge) {
						return reject("manual terminal recovery challenge object changed during recovery")
					}
					if r.Status.ManualRecoveryChallengeDigest == "" || r.Status.ManualRecoveryChallenge == nil {
						r.Status.ManualRecoveryChallenge = preflight.ManualRecoveryChallenge
						r.Status.ManualRecoveryChallengeDigest = challengeDigest
						return c.record(ctx, r, "Recovering", "manual terminal recovery challenge persisted; administrator attestation is required")
					}
				}
				return c.record(ctx, r, "Recovering", "frozen-source closure proof is pending")
			}
			if preflight.EvidenceKind == ManualTerminalEvidenceKind {
				if !validEvidenceDigest(preflight.ManualRecoveryChallengeDigest) || !validEvidenceDigest(preflight.ManualTerminalAttestationDigest) || preflight.ProofDigest != "" {
					return reject("manual terminal recovery evidence is incomplete or mixed with independent proof")
				}
				changed := false
				if r.Status.ManualRecoveryChallengeDigest == "" {
					r.Status.ManualRecoveryChallengeDigest = preflight.ManualRecoveryChallengeDigest
					changed = true
				} else if r.Status.ManualRecoveryChallengeDigest != preflight.ManualRecoveryChallengeDigest {
					return reject("manual terminal recovery challenge changed during recovery")
				}
				if r.Status.ManualAttestationDigest == "" {
					r.Status.ManualAttestationDigest = preflight.ManualTerminalAttestationDigest
					changed = true
				} else if r.Status.ManualAttestationDigest != preflight.ManualTerminalAttestationDigest {
					return reject("manual terminal recovery attestation changed during recovery")
				}
				if changed {
					return c.record(ctx, r, "Recovering", "manual terminal attestation persisted before target authorization")
				}
			} else if r.Status.ManualRecoveryChallengeDigest != "" || r.Status.ManualAttestationDigest != "" {
				return reject("manual terminal evidence disappeared or changed kind during recovery")
			}
			if membershipMissing {
				if bootstrapConfirmation != nil {
					// Existing automatic bootstrap recovery is authorized by its
					// independently re-derived canonical CONTROL confirmation, not
					// by the separate manual missing-membership opt-in.
					if r.Status.ManualMembershipBindingDigest != "" || preflight.ManualMembershipBindingDigest != "" {
						return reject("automatic bootstrap recovery carries a manual membership binding")
					}
				} else {
					if !manualMissingSourceMembershipAllowed(r) || mode != "async" || !r.Spec.AllowDataLoss || preflight.ManualMembershipBindingDigest == "" {
						return reject("manual missing-membership authorization is not bound to a closed canonical source cohort")
					}
					if r.Status.ManualMembershipBindingDigest == "" {
						r.Status.ManualMembershipBindingDigest = preflight.ManualMembershipBindingDigest
						return c.record(ctx, r, "Recovering", "manual source-cohort binding persisted before target authorization")
					}
					if r.Status.ManualMembershipBindingDigest != preflight.ManualMembershipBindingDigest {
						return reject("manual source-cohort binding changed during recovery")
					}
				}
			} else if r.Status.ManualMembershipBindingDigest != "" {
				return reject("manual source-cohort binding exists without a missing source membership")
			}
			configuredIdentity, err := c.frozenSourceIdentity(r, source)
			if err != nil || configuredIdentity != preflight.Identity {
				return reject("configured archive scope differs from canonical frozen-source identity")
			}
			target, err := recovery.FrozenTargetID(preflight.Identity)
			if err != nil {
				return reject("frozen source identity is invalid: " + err.Error())
			}
			bucketName := StoreIdentity(source.env)["RHIZA_OBJSTORE_BUCKET"]
			bucketEnvironment, err := c.captureFrozenBucketEnvironment(ctx, bucketName, prefix)
			if err != nil {
				return reject("source bucket lifecycle is not safely observable: " + err.Error())
			}
			r.Status.Target = target
			r.Status.RecoveryMode = RecoveryModeFrozenSource
			r.Status.FrozenBucketEnvironment = &bucketEnvironment
			r.Status.RecoveryID, r.Status.SpecHash = r.Spec.RecoveryID, immutableSpecHash(r.Spec)
			if r.Status.TargetBaseKind == "empty" {
				r.Status.Stage = "FrozenEmpty"
			} else {
				r.Status.Stage = "FrozenForked"
			}
			return c.record(ctx, r, "Recovering", "stable source-derived target claimed before recovery side effects")
		}
		r.Status.Target = "rhiza-r-" + shortID(c.operationID(r))
		r.Status.RecoveryID, r.Status.SpecHash = r.Spec.RecoveryID, immutableSpecHash(r.Spec)
		reservation := c.reservation(r)
		reserved, _, err := c.verifyOrFindSuccessorReservation(ctx, prefix, reservation)
		if err != nil {
			return reject("legacy successor reservation is inconsistent or unavailable: " + err.Error())
		}
		// DATA keeps the legacy retry reservation; the trusted archive registrar
		// arbitrates canonical successors in CONTROL.
		if !reserved {
			if err := c.immutable(ctx, path.Join(prefix, "recovery/successor.json"), jsonBytes(reservation)); err != nil {
				return reject("source generation already reserved or reservation unavailable")
			}
		}
		r.Status.Stage = "Reserved"
		return c.record(ctx, r, "Recovering", "unique successor reserved; original generation must remain fenced")
	}
	if r.Status.RecoveryMode == RecoveryModeFrozenSource {
		if (r.Status.TargetBaseKind != "fork" || r.Status.Stage != "FrozenForked") && (r.Status.TargetBaseKind != "empty" || r.Status.Stage != "FrozenEmpty") {
			return reject("frozen-source recovery mode is inconsistent with its immutable stage")
		}
		if r.Status.FrozenBucketEnvironment == nil {
			return reject("frozen-source recovery has no persisted bucket lifecycle snapshot")
		}
		if err := c.frozenBucketEnvironmentVerifier(StoreIdentity(source.env)["RHIZA_OBJSTORE_BUCKET"], source.prefix)(ctx, *r.Status.FrozenBucketEnvironment); err != nil {
			return reject("source bucket lifecycle changed or is unavailable: " + err.Error())
		}
		target, err := c.frozenTargetFor(r, source)
		if err != nil || target != r.Status.Target {
			return reject("frozen-source target differs from the stable source identity")
		}
	} else {
		if r.Status.Stage == "FrozenForked" || r.Status.Stage == "FrozenEmpty" {
			return reject("frozen-source stage has no immutable recovery mode")
		}
		if err := c.verifyReservation(ctx, r); err != nil {
			return reject("reserved source successor changed or disappeared")
		}
	}
	return c.advance(ctx, r, source)
}

func (c *Controller) useFrozenSourceFork(source sourceState) bool {
	return source.reconfiguration && StoreIdentity(source.env)["RHIZA_OBJSTORE_PROVIDER"] == "gcs" &&
		c.Fencer != nil && func() bool {
		if _, ok := c.Fencer.(ArchiveForkGenesisRegistrar); !ok {
			return false
		}
		_, cooperative := c.Fencer.(*KubernetesFencer)
		return !cooperative
	}()
}

func (c *Controller) advance(ctx context.Context, r *Resource, s sourceState) error {
	if err := c.claimStatefulSet(ctx, s.stateful, c.operationID(r)); err != nil {
		return c.record(ctx, r, "Blocked", err.Error())
	}
	switch r.Status.Stage {
	case "Reserved":
		// The external fence is already required. Preserve and validate the
		// complete target before deleting any surviving emptyDir state.
		if err := recovery.Seal(ctx, c.Bucket, s.prefix, c.operationID(r)); err != nil {
			return c.record(ctx, r, "Blocked", "source archive seal failed")
		}
		r.Status.Stage = "Sealed"
		return c.record(ctx, r, "Recovering", "source publication permanently sealed before evidence copy")
	case "Stopping":
		if number(nested(s.stateful, "spec", "replicas")) != 0 {
			asObject(s.stateful["spec"])["replicas"] = float64(0)
			if err := c.Kube.Put(ctx, c.stsPath(r.Spec.StatefulSet), s.stateful, &s.stateful); err != nil {
				return err
			}
			return c.record(ctx, r, "Recovering", "waiting for source StatefulSet Pods to terminate after external fence")
		}
		pods, err := c.ownedPods(ctx, r.Status.StatefulSetUID)
		if err != nil {
			return err
		}
		if len(pods) > 0 {
			if err := c.deleteFencedLearners(ctx, r, s.stateful, pods); err != nil {
				return c.record(ctx, r, "Blocked", "fenced standalone learner cleanup: "+err.Error())
			}
			return c.record(ctx, r, "Recovering", "waiting for source StatefulSet Pods to terminate after external fence")
		}
		r.Status.Stage = "Starting"
		return c.record(ctx, r, "Recovering", "source StatefulSet Pods terminated; externally fenced target may start")
	case "FrozenForked", "FrozenEmpty", "Sealed":
		secret := c.recoverySecretName(r)
		var targetMembers []quepaxa.Member
		if s.reconfiguration {
			var err error
			targetMembers, err = c.targetCredentials(ctx, r, secret, s.members)
			if err != nil {
				return c.record(ctx, r, "Blocked", err.Error())
			}
		}
		targetBaseKind := r.Status.TargetBaseKind
		if targetBaseKind == "" {
			// Pre-field reservations were only created for nonzero forks.
			targetBaseKind = "fork"
		}
		if targetBaseKind == "empty" {
			return c.prepareEmptyRecovery(ctx, r, s, secret, targetMembers)
		}
		if targetBaseKind != "fork" {
			return c.record(ctx, r, "Blocked", "recovery target base kind is missing or invalid")
		}
		var result recovery.ForkResult
		var err error
		if r.Status.RecoveryMode == RecoveryModeFrozenSource {
			if r.Status.FrozenBucketEnvironment == nil {
				return c.record(ctx, r, "Blocked", "frozen-source bucket lifecycle snapshot is missing")
			}
			identity, evidenceKind, proofDigest, manualAttestationDigest, rootDigest, err := c.frozenSourceClosure(ctx, r, s)
			if err != nil {
				return c.record(ctx, r, "Blocked", "frozen source closure changed or is unavailable: "+err.Error())
			}
			options := c.forkOptions(r, s.prefix, s.members, true, targetMembers)
			frozen, err := recovery.ForkFrozenSource(ctx, c.Bucket, recovery.FrozenForkOptions{
				ForkOptions: options, SourceIdentity: identity, EvidenceKind: evidenceKind, FenceProofDigest: proofDigest,
				ManualAttestationDigest: manualAttestationDigest, ClosedRootDigest: rootDigest,
				BucketEnvironment: *r.Status.FrozenBucketEnvironment,
				VerifyEnvironment: c.frozenBucketEnvironmentVerifier(StoreIdentity(s.env)["RHIZA_OBJSTORE_BUCKET"], s.prefix),
			})
			if err != nil {
				return c.record(ctx, r, "Blocked", "verified frozen-source fork failed: "+err.Error())
			}
			result = frozen.ForkResult
		} else {
			result, err = recovery.Fork(ctx, c.Bucket, c.forkOptions(r, s.prefix, s.members, s.reconfiguration, targetMembers))
			if err != nil {
				return c.record(ctx, r, "Blocked", "certified archive fork failed: "+err.Error())
			}
		}
		if !s.reconfiguration {
			targetMembers, err = c.targetCredentials(ctx, r, secret, s.members)
			if err != nil {
				return c.record(ctx, r, "Blocked", err.Error())
			}
		}
		members := targetMembers
		record := recovery.NewMembershipRecord(r.Status.Target, members, r.Spec.Durability)
		if err := c.immutable(ctx, path.Join(c.Prefix, r.Status.Target, "voters/membership.json"), jsonBytes(record)); err != nil {
			return c.record(ctx, r, "Blocked", "target membership is unavailable or inconsistent")
		}
		lineage := object{"version": 1, "operation_id": c.operationID(r), "source": r.Status.Source, "source_durability": r.Status.SourceDurability, "source_membership": r.Status.SourceMembership, "target_membership": record, "fork": result}
		if err := c.immutable(ctx, path.Join(c.Prefix, r.Status.Target, "recovery/generation.json"), jsonBytes(lineage)); err != nil {
			return c.record(ctx, r, "Blocked", "target lineage is unavailable or inconsistent")
		}

		// Only the explicitly configured Kubernetes fencer is the legacy
		// cooperative backend. Unknown or wrapped backends must prove the
		// canonical genesis capability; absence of that capability is not
		// evidence that the source uses cooperative storage.
		requiresArchiveGenesis := true
		if _, cooperative := c.Fencer.(*KubernetesFencer); cooperative {
			requiresArchiveGenesis = false
		}
		registrar, canRegisterArchiveGenesis := c.Fencer.(ArchiveForkGenesisRegistrar)
		if requiresArchiveGenesis && !canRegisterArchiveGenesis {
			return c.record(ctx, r, "Blocked", "configured fencing backend cannot register canonical archive fork genesis")
		}
		if requiresArchiveGenesis && !s.reconfiguration {
			return c.record(ctx, r, "Blocked", "canonical archive fork genesis requires verified reconfiguration lineage")
		}
		if requiresArchiveGenesis || r.Spec.AnchorID != "" {
			targetPrefix := path.Join(c.Prefix, r.Status.Target)
			anchor, anchorH, anchorErr := recovery.ReadGenerationAnchor(ctx, c.Bucket, targetPrefix)
			if anchorErr != nil {
				return c.record(ctx, r, "Blocked", "target generation anchor unavailable: "+anchorErr.Error())
			}
			gaRecord, gaRecErr := c.sourceMembership(ctx, targetPrefix)
			if gaRecErr != nil {
				return c.record(ctx, r, "Blocked", "target membership unavailable: "+gaRecErr.Error())
			}
			if _, vErr := recovery.VerifyGenerationAnchor(ctx, c.Bucket, targetPrefix, gaRecord, anchorH); vErr != nil {
				return c.record(ctx, r, "Blocked", "target generation anchor verification failed: "+vErr.Error())
			}
			if anchor.OperationID != c.operationID(r) || anchor.SourcePrefix != s.prefix || anchor.SourcePrefixHash != result.PrefixHash || anchor.SourceTip != result.Tip || anchor.SourceManifest != result.ManifestHash {
				return c.record(ctx, r, "Blocked", "target generation anchor does not match fork result")
			}
			if requiresArchiveGenesis {
				request := ArchiveForkGenesisRequest{
					StatefulSetUID: r.Status.StatefulSetUID, TargetClusterID: r.Status.Target,
					OperationID: c.operationID(r), SourceFenceOperationID: r.Spec.RecoveryID, RecoveryMode: r.Status.RecoveryMode,
					BucketEnvironment: r.Status.FrozenBucketEnvironment, ManualMembershipBindingDigest: r.Status.ManualMembershipBindingDigest,
					ManualRecoveryChallengeDigest: r.Status.ManualRecoveryChallengeDigest,
					ManualTerminalAttestationDigest: r.Status.ManualAttestationDigest,
				}
				if r.Status.ManualAttestationDigest != "" {
					request.EvidenceKind = ManualTerminalEvidenceKind
				}
				if r.Status.RecoveryMode == RecoveryModeFrozenSource {
					request.RecoveryName = resourceName(r)
				}
				if err := registrar.RegisterArchiveForkGenesis(ctx, request); err != nil {
					return c.record(ctx, r, "Blocked", "canonical archive fork genesis registration failed: "+err.Error())
				}
			}
			if r.Spec.AnchorID != "" {
				if c.Anchor == nil {
					return c.record(ctx, r, "Blocked", "anchor backend is not configured for anchorID "+r.Spec.AnchorID)
				}
				req := c.buildAnchorRequest(r, s, record, result, hex.EncodeToString(anchorH[:]))
				receipt, actErr := c.Anchor.Activate(ctx, req)
				if actErr != nil {
					return c.record(ctx, r, "Blocked", "anchor activation failed: "+actErr.Error())
				}
				r.Status.AnchorRequest = &req
				r.Status.AnchorReceipt = &receipt
			}
		}
		r.Status.SecretName, r.Status.ManifestHash, r.Status.RecoveredTip = secret, result.ManifestHash, result.Tip
		r.Status.Stage = "Stopping"
		return c.record(ctx, r, "Recovering", "verified target and credentials preserved before deleting source pods")
	default:
		return c.record(ctx, r, "Blocked", "unknown recovery stage")
	}
}

func (c *Controller) prepareEmptyRecovery(ctx context.Context, r *Resource, s sourceState, secret string, targetMembers []quepaxa.Member) error {
	if r.Spec.AnchorID != "" {
		return c.record(ctx, r, "Blocked", "empty archive recovery cannot claim fork-anchor evidence")
	}
	if targetMembers == nil {
		var err error
		targetMembers, err = c.targetCredentials(ctx, r, secret, s.members)
		if err != nil {
			return c.record(ctx, r, "Blocked", err.Error())
		}
	}
	record := recovery.NewMembershipRecord(r.Status.Target, targetMembers, r.Spec.Durability)
	if err := c.immutable(ctx, path.Join(c.Prefix, r.Status.Target, "voters/membership.json"), jsonBytes(record)); err != nil {
		return c.record(ctx, r, "Blocked", "target membership is unavailable or inconsistent")
	}
	if err := recovery.RequireEmptyArchiveScope(ctx, c.Bucket, path.Join(c.Prefix, r.Status.Target)); err != nil {
		return c.record(ctx, r, "Blocked", "target archive is not a pristine empty scope")
	}
	response, err := c.registerEmptyRecovery(ctx, r, record)
	if err != nil {
		if errors.Is(err, ErrFencePending) {
			return c.record(ctx, r, "Recovering", "canonical empty recovery authorization is pending")
		}
		return c.record(ctx, r, "Blocked", "canonical empty recovery authorization failed")
	}
	if response.State != "authorized" && response.State != "committed" {
		return c.record(ctx, r, "Blocked", "canonical empty recovery returned an invalid state")
	}
	r.Status.SecretName = secret
	r.Status.ManifestHash = ""
	r.Status.RecoveredTip = 0
	r.Status.Stage = "Stopping"
	return c.record(ctx, r, "Recovering", "authorized empty target and credentials preserved before source shutdown")
}

func (c *Controller) registerEmptyRecovery(ctx context.Context, r *Resource, membership recovery.MembershipRecord) (ArchiveEmptyRecoveryResponse, error) {
	registrar, ok := c.Fencer.(ArchiveEmptyRecoveryRegistrar)
	if !ok || registrar == nil {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("canonical empty recovery registrar is required")
	}
	request := ArchiveEmptyRecoveryRequest{
		RecoveryName: resourceName(r), StatefulSetUID: r.Status.StatefulSetUID,
		TargetClusterID: r.Status.Target, OperationID: c.operationID(r),
		SourceFenceOperationID: r.Spec.RecoveryID,
		TargetMembershipHash:   authorityStateDigest(jsonBytes(membership)),
		TargetDurability:       r.Spec.Durability, AllowDataLoss: r.Spec.AllowDataLoss,
		PolicyDigest: hashJSON(r.Spec), RecoveryMode: r.Status.RecoveryMode, BucketEnvironment: r.Status.FrozenBucketEnvironment,
		ManualMembershipBindingDigest: r.Status.ManualMembershipBindingDigest,
	}
	if request.RecoveryName == "" || request.StatefulSetUID == "" || request.TargetClusterID == "" || request.OperationID == "" ||
		request.SourceFenceOperationID == "" || request.TargetMembershipHash == "" || request.PolicyDigest == "" {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("incomplete canonical empty recovery request")
	}
	response, err := registrar.RegisterArchiveEmptyRecovery(ctx, request)
	if err != nil {
		return ArchiveEmptyRecoveryResponse{}, err
	}
	if response.State != "authorized" && response.State != "committed" {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("invalid canonical empty recovery state")
	}
	authorization := response.Authorization
	expectedSourcePrefix := ""
	if request.RecoveryMode == RecoveryModeFrozenSource {
		expectedSourcePrefix = path.Join(strings.Trim(c.Prefix, "/"), r.Status.Source)
	}
	if err := authorization.Validate(); err != nil || authorization.StatefulSetUID != request.StatefulSetUID ||
		authorization.TargetClusterID != request.TargetClusterID || authorization.OperationID != request.OperationID ||
		authorization.SourceFenceOperationID != request.SourceFenceOperationID || authorization.SourceTip != 0 ||
		authorization.TargetMembershipHash != request.TargetMembershipHash || authorization.TargetDurability != request.TargetDurability ||
		authorization.AllowDataLoss != request.AllowDataLoss || authorization.PolicyDigest != emptyRecoveryPolicyDigest(request.PolicyDigest, authorization.SourceDurability, request.TargetDurability, request.AllowDataLoss) ||
		authorization.RecoveryMode != request.RecoveryMode || authorization.SourcePrefix != expectedSourcePrefix ||
		authorization.BucketMetageneration != frozenBucketMetageneration(request.BucketEnvironment) || authorization.LifecyclePolicyDigest != frozenBucketLifecycleDigest(request.BucketEnvironment) {
		return ArchiveEmptyRecoveryResponse{}, fmt.Errorf("canonical empty recovery authorization differs from the requested operation")
	}
	return response, nil
}

func (c *Controller) startAndObserve(ctx context.Context, r *Resource, sts object) error {
	if r.Status.RecoveryMode != RecoveryModeFrozenSource {
		if err := c.verifyReservation(ctx, r); err != nil {
			return c.record(ctx, r, "Blocked", "reserved source successor changed or disappeared")
		}
	}
	if err := validateFence(r.Spec.Fence, r.Spec.RecoveryID, r.Spec.SourceClusterID, r.Status.StatefulSetUID); err != nil {
		return c.record(ctx, r, "AwaitingFence", err.Error())
	}
	if r.Status.SourceDurability == "async" && !r.Spec.AllowDataLoss {
		return c.record(ctx, r, "Blocked", "async recovery still requires allowDataLoss")
	}
	targetBaseKind := r.Status.TargetBaseKind
	if targetBaseKind == "" && r.Status.Stage != "" {
		// Prior journal entries were only created for nonzero certified forks.
		targetBaseKind = "fork"
	}
	if r.Status.Target != c.recoveryTargetID(r) || r.Status.SecretName != c.recoverySecretName(r) ||
		(targetBaseKind == "fork" && r.Status.RecoveredTip == 0) ||
		(targetBaseKind == "empty" && (r.Status.RecoveredTip != 0 || r.Status.ManifestHash != "")) ||
		(targetBaseKind != "fork" && targetBaseKind != "empty") {
		return c.record(ctx, r, "Blocked", "invalid target recovery journal")
	}
	if str(nested(sts, "metadata", "annotations", ownerAnnotation)) != c.operationID(r) {
		return c.record(ctx, r, "Blocked", "StatefulSet recovery ownership changed")
	}
	ctr, err := container(sts, r.Spec.Container)
	if err != nil {
		return err
	}
	env, err := c.environment(ctx, ctr)
	if err != nil {
		return err
	}
	if r.Status.RecoveryMode == RecoveryModeFrozenSource {
		prefix := path.Join(strings.Trim(StoreIdentity(env)["RHIZA_OBJSTORE_PREFIX"], "/"), r.Spec.SourceClusterID)
		target, err := c.frozenTargetFor(r, sourceState{prefix: prefix, env: env})
		if err != nil || target != r.Status.Target {
			return c.record(ctx, r, "Blocked", "stable frozen-source target identity changed")
		}
	}
	if err := c.storeConfigMatches(env); err != nil {
		return c.record(ctx, r, "Blocked", err.Error())
	}
	// Anchor opt-in gate: requires env and reconfiguration.
	reconfiguration, reconfigErr := reconfigurationEnabled(env)
	if reconfigErr != nil {
		return c.record(ctx, r, "Blocked", reconfigErr.Error())
	}
	if err := anchorEnvGate(r.Spec.AnchorID, env, reconfiguration, c.Anchor); err != nil {
		return c.record(ctx, r, "Blocked", err.Error())
	}
	if err := noPVC(sts, ctr, env); err != nil {
		return c.record(ctx, r, "Blocked", err.Error())
	}
	members, err := c.targetCredentials(ctx, r, r.Status.SecretName, nil)
	if err != nil {
		return c.record(ctx, r, "Blocked", err.Error())
	}
	record, err := c.sourceMembership(ctx, path.Join(c.Prefix, r.Status.Target))
	if err != nil || record != recovery.NewMembershipRecord(r.Status.Target, members, r.Spec.Durability) {
		return c.record(ctx, r, "Blocked", "target membership changed")
	}
	if targetBaseKind == "empty" {
		if _, err := c.registerEmptyRecovery(ctx, r, record); err != nil {
			if errors.Is(err, ErrFencePending) {
				return c.record(ctx, r, "Recovering", "canonical empty recovery authorization is pending")
			}
			return c.record(ctx, r, "Blocked", "canonical empty recovery authorization is unavailable")
		}
	}
	// Common anchor verification: covers Starting (both branches) and Restarted.
	sourcePrefix := path.Join(c.Prefix, r.Spec.SourceClusterID)
	if r.Spec.AnchorID != "" {
		if err := c.requireAnchorReceipt(r); err != nil {
			return c.record(ctx, r, "Blocked", err.Error())
		}
		targetPrefix := path.Join(c.Prefix, r.Status.Target)
		gaRecord, gaRecErr := c.sourceMembership(ctx, targetPrefix)
		if gaRecErr != nil {
			return c.record(ctx, r, "Blocked", "target membership unavailable: "+gaRecErr.Error())
		}
		anchor, anchorH, anchorErr := recovery.ReadGenerationAnchor(ctx, c.Bucket, targetPrefix)
		if anchorErr != nil {
			return c.record(ctx, r, "Blocked", "target generation anchor unavailable: "+anchorErr.Error())
		}
		if _, vErr := recovery.VerifyGenerationAnchor(ctx, c.Bucket, targetPrefix, gaRecord, anchorH); vErr != nil {
			return c.record(ctx, r, "Blocked", "target generation anchor verification failed: "+vErr.Error())
		}
		if anchor.SourcePrefix != sourcePrefix {
			return c.record(ctx, r, "Blocked", "anchor source prefix does not match spec")
		}
		if anchor.OperationID != c.operationID(r) {
			return c.record(ctx, r, "Blocked", "anchor operation ID does not match")
		}
		if anchor.SourceTip != r.Status.RecoveredTip || anchor.SourceManifest != r.Status.ManifestHash {
			return c.record(ctx, r, "Blocked", "anchor source tip/manifest does not match journal")
		}
		result := recovery.ForkResult{Tip: anchor.SourceTip, ManifestHash: anchor.SourceManifest, PrefixHash: anchor.SourcePrefixHash}
		s := sourceState{prefix: sourcePrefix}
		if vErr := c.verifyAnchorRequest(ctx, r, s, record, result, hex.EncodeToString(anchorH[:])); vErr != nil {
			return c.record(ctx, r, "Blocked", vErr.Error())
		}
	}
	if r.Status.Stage == "Starting" {
		if env["RHIZA_CLUSTER_ID"] == r.Spec.SourceClusterID {
			sourceMembers, err := recoveryMembers(env["RHIZA_CLUSTER_MEMBERS"])
			if err != nil || sourceFingerprint(r.Spec.SourceClusterID, r.Status.SourceDurability, sourceMembers, sourcePrefix) != r.Status.SourceMembership {
				return c.record(ctx, r, "Blocked", "source configuration changed before activation")
			}

			reconfiguration, err := reconfigurationEnabled(env)
			if err != nil {
				return c.record(ctx, r, "Blocked", err.Error())
			}
			var result recovery.ForkResult
			if targetBaseKind == "fork" {
				result, err = recovery.Fork(ctx, c.Bucket, c.forkOptions(r, sourcePrefix, sourceMembers, reconfiguration, members))
				if err != nil || result.Tip != r.Status.RecoveredTip || result.ManifestHash != r.Status.ManifestHash {
					return c.record(ctx, r, "Blocked", "prepared target evidence changed before activation")
				}
			} else if targetBaseKind == "empty" {
				if err := recovery.RequireEmptyArchiveScope(ctx, c.Bucket, path.Join(c.Prefix, r.Status.Target)); err != nil {
					return c.record(ctx, r, "Blocked", "empty target archive is not pristine before activation")
				}
			} else {
				return c.record(ctx, r, "Blocked", "invalid target recovery base kind")
			}
			if number(nested(sts, "spec", "replicas")) != 0 {
				return c.record(ctx, r, "Blocked", "source replicas reappeared before activation")
			}
			pods, err := c.ownedPods(ctx, r.Status.StatefulSetUID)
			if err != nil {
				return err
			}
			if len(pods) != 0 {
				return c.record(ctx, r, "Blocked", "source pods reappeared before activation")
			}
			// Live re-verify anchor receipt just before StatefulSet PUT.
			if r.Spec.AnchorID != "" && targetBaseKind == "fork" {
				targetPrefix := path.Join(c.Prefix, r.Status.Target)
				gaRecord, gaRecErr := c.sourceMembership(ctx, targetPrefix)
				if gaRecErr != nil {
					return c.record(ctx, r, "Blocked", "target membership unavailable: "+gaRecErr.Error())
				}
				_, anchorH, anchorErr := recovery.ReadGenerationAnchor(ctx, c.Bucket, targetPrefix)
				if anchorErr != nil {
					return c.record(ctx, r, "Blocked", "target generation anchor unavailable: "+anchorErr.Error())
				}
				if _, vErr := recovery.VerifyGenerationAnchor(ctx, c.Bucket, targetPrefix, gaRecord, anchorH); vErr != nil {
					return c.record(ctx, r, "Blocked", "target generation anchor verification failed: "+vErr.Error())
				}
				s := sourceState{prefix: sourcePrefix}
				if vErr := c.verifyAnchorRequest(ctx, r, s, record, result, hex.EncodeToString(anchorH[:])); vErr != nil {
					return c.record(ctx, r, "Blocked", vErr.Error())
				}
			}
			values := list(ctr["env"])
			values = replaceEnv(values, "RHIZA_CLUSTER_ID", object{"value": r.Status.Target})
			values = replaceEnv(values, "RHIZA_OBJSTORE_DURABILITY", object{"value": r.Spec.Durability})
			ctr["env"] = values
			applyTargetCredentials(ctr, r.Status.SecretName)
			asObject(sts["spec"])["replicas"] = float64(len(members))
			if err := c.Kube.Put(ctx, c.stsPath(r.Spec.StatefulSet), sts, &sts); err != nil {
				return err
			}
		} else if env["RHIZA_CLUSTER_ID"] != r.Status.Target {
			return c.record(ctx, r, "Blocked", "unexpected StatefulSet generation during activation")
		}
		r.Status.Stage = "Restarted"
		return c.record(ctx, r, "WaitingReady", "target started; waiting for a matching recovered quorum")
	}
	actual, err := recoveryMembers(env["RHIZA_CLUSTER_MEMBERS"])
	if err != nil || env["RHIZA_CLUSTER_ID"] != r.Status.Target || env["RHIZA_OBJSTORE_DURABILITY"] != r.Spec.Durability || sourceFingerprint(r.Status.Target, r.Spec.Durability, actual, c.Prefix) != sourceFingerprint(r.Status.Target, r.Spec.Durability, members, c.Prefix) {
		return c.record(ctx, r, "Blocked", "active target configuration changed")
	}
	// A target that lost desired replicas never completes: the frozen membership
	// and the installed workload must stay one-for-one.
	if replicas := int(number(nested(sts, "spec", "replicas"))); replicas != len(members) {
		return c.record(ctx, r, "Blocked", fmt.Sprintf("target StatefulSet has %d desired replicas but %d target voters are frozen", replicas, len(members)))
	}
	peers, err := c.probe(ctx, r, sts)
	if err != nil {
		return err
	}
	r.Status.Peers = peers
	allowed := map[string]bool{}
	for _, member := range members {
		allowed[string(member.ID)] = true
	}
	ready := map[string]bool{}
	for _, peer := range peers {
		if allowed[peer.NodeID] && peer.ClusterID == r.Status.Target && peer.Durability == r.Spec.Durability && peer.Ready && peer.Quorum && peer.CertifiedTip >= r.Status.RecoveredTip && peer.AppliedTip >= r.Status.RecoveredTip {
			ready[peer.NodeID] = true
		}
	}
	if len(ready) < len(members)/2+1 {
		return c.record(ctx, r, "WaitingReady", "waiting for a matching recovered quorum")
	}
	if targetBaseKind == "empty" {
		response, err := c.registerEmptyRecovery(ctx, r, record)
		if err != nil {
			if errors.Is(err, ErrFencePending) {
				return c.record(ctx, r, "WaitingReady", "canonical empty archive genesis commit is pending")
			}
			return c.record(ctx, r, "Blocked", "canonical empty archive genesis commit is unavailable")
		}
		if response.State != "committed" {
			return c.record(ctx, r, "WaitingReady", "target quorum is ready; canonical empty archive genesis is not committed")
		}
		if err := c.targetApplicationReadBarrier(ctx, r, sts, ready, env["RHIZA_ADMIN_TOKEN"]); err != nil {
			return c.record(ctx, r, "WaitingReady", "target application read-durability barrier is unavailable")
		}
		response, err = c.registerEmptyRecovery(ctx, r, record)
		if err != nil {
			if errors.Is(err, ErrFencePending) {
				return c.record(ctx, r, "WaitingReady", "canonical empty archive genesis commit is pending")
			}
			return c.record(ctx, r, "Blocked", "canonical empty archive genesis commit is unavailable")
		}
		if response.State != "committed" {
			return c.record(ctx, r, "WaitingReady", "target application barrier passed; canonical empty archive genesis is not committed")
		}
	}
	if str(nested(r.Metadata, "annotations", automaticOwner)) != "" {
		identities, err := c.captureAutomaticTargetIdentities(ctx, r, sts, members, env["RHIZA_ADMIN_TOKEN"])
		if err != nil {
			return c.record(ctx, r, "WaitingReady", "automatic target identities are not verified: "+err.Error())
		}
		r.Status.TargetIdentities = identities
	}
	// Commit completion before releasing ownership. A stale reconciler cannot
	// mutate a future generation after its annotation changes.
	r.Status.Stage = "Complete"
	return c.record(ctx, r, "Complete", "new generation has a verified recovered quorum")
}

// targetApplicationReadBarrier requires a live target voter to pass the
// authenticated ReadIndex probe and the application route's linearizable,
// read-durable query before an empty recovery can complete.
func (c *Controller) targetApplicationReadBarrier(ctx context.Context, r *Resource, sts object, ready map[string]bool, token string) error {
	if token == "" {
		return fmt.Errorf("target admin token is unavailable")
	}
	pods, err := c.ownedPods(ctx, str(nested(sts, "metadata", "uid")))
	if err != nil {
		return err
	}
	for _, pod := range pods {
		name, uid := str(nested(pod, "metadata", "name")), str(nested(pod, "metadata", "uid"))
		if !ready[name] || uid == "" || nested(pod, "metadata", "deletionTimestamp") != nil || !ownedBy(pod, r.Status.StatefulSetUID) {
			continue
		}
		probeEndpoint, err := podEndpoint(pod, r.Spec.Container, "/recovery/probe")
		if err != nil || !c.automaticQuorumProbe(ctx, probeEndpoint, token, r.Status.Target, name) {
			continue
		}
		queryEndpoint, err := podEndpoint(pod, r.Spec.Container, "/sql/query")
		if err != nil {
			continue
		}
		body, err := json.Marshal(network.QueryRequest{SQL: "SELECT 1", Consistency: "linearizable"})
		if err != nil {
			return err
		}
		call, cancel := context.WithTimeout(ctx, 5*time.Second)
		request, err := http.NewRequestWithContext(call, http.MethodPost, queryEndpoint, bytes.NewReader(body))
		if err != nil {
			cancel()
			return err
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := c.client().Do(request)
		if err != nil {
			cancel()
			continue
		}
		var result network.QueryResponse
		decodeErr := error(nil)
		if response.StatusCode != http.StatusOK {
			decodeErr = fmt.Errorf("linearizable application query returned HTTP %d", response.StatusCode)
		} else {
			decodeErr = json.NewDecoder(io.LimitReader(response.Body, 16<<10)).Decode(&result)
		}
		response.Body.Close()
		cancel()
		var value float64
		valueOK := false
		if len(result.Rows) == 1 && len(result.Rows[0]) == 1 {
			value, valueOK = result.Rows[0][0].(float64)
		}
		if decodeErr != nil || len(result.Columns) != 1 || result.Columns[0] != "1" || !valueOK || value != 1 || result.ConsensusTip < result.AppliedSlot {
			continue
		}
		// Bind both service calls to the same still-owned Pod incarnation.
		var current object
		if err := c.Kube.Get(ctx, c.corePath("pods", name), &current); err != nil ||
			str(nested(current, "metadata", "uid")) != uid ||
			nested(current, "status", "podIP") != nested(pod, "status", "podIP") ||
			nested(current, "metadata", "deletionTimestamp") != nil || !ownedBy(current, r.Status.StatefulSetUID) {
			continue
		}
		if !c.automaticQuorumProbe(ctx, probeEndpoint, token, r.Status.Target, name) {
			continue
		}
		// The final authenticated probe is the last service interaction. Re-read
		// the Pod afterward so a same-name replacement between the earlier
		// identity check and this probe cannot satisfy the barrier as the old
		// incarnation.
		var final object
		if err := c.Kube.Get(ctx, c.corePath("pods", name), &final); err != nil ||
			str(nested(final, "metadata", "uid")) != uid ||
			nested(final, "status", "podIP") != nested(pod, "status", "podIP") ||
			nested(final, "metadata", "deletionTimestamp") != nil || !ownedBy(final, r.Status.StatefulSetUID) {
			continue
		}
		return nil
	}
	return fmt.Errorf("no ready target voter completed an authenticated ReadIndex and linearizable application read-durability barrier")
}

func (c *Controller) reservation(r *Resource) object {
	reservation := object{"version": 1, "operation_id": c.operationID(r), "target": r.Status.Target, "source": r.Spec.SourceClusterID, "source_membership": r.Status.SourceMembership, "statefulset_uid": r.Status.StatefulSetUID, "spec_hash": r.Status.SpecHash}
	if r.Status.TargetBaseKind != "" {
		reservation["target_base_kind"] = r.Status.TargetBaseKind
	}
	return reservation
}

func (c *Controller) recoveryTargetID(r *Resource) string {
	if r.Status.RecoveryMode == RecoveryModeFrozenSource {
		return r.Status.Target
	}
	return "rhiza-r-" + shortID(c.operationID(r))
}

func (c *Controller) recoverySecretName(r *Resource) string {
	if r.Status.RecoveryMode == RecoveryModeFrozenSource {
		return "rhiza-recovery-" + shortID(r.Status.Target)
	}
	return "rhiza-recovery-" + shortID(c.operationID(r))
}
func reconfigurationEnabled(env map[string]string) (bool, error) {
	raw := env["RHIZA_ENABLE_RECONFIGURATION"]
	if raw == "" {
		return false, nil
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("RHIZA_ENABLE_RECONFIGURATION must be a boolean")
	}
	return enabled, nil
}
func (c *Controller) forkOptions(r *Resource, sourcePrefix string, sourceMembers []quepaxa.Member, reconfiguration bool, targetMembers []quepaxa.Member) recovery.ForkOptions {
	options := recovery.ForkOptions{SourcePrefix: sourcePrefix, TargetPrefix: path.Join(c.Prefix, r.Status.Target), Members: sourceMembers, OperationID: c.operationID(r)}
	if reconfiguration {
		options.SourceBootstrap = quepaxa.Cluster{ConfigID: 1, Members: sourceMembers}
		options.TargetMembers = targetMembers
		options.TargetMembership = recovery.NewMembershipRecord(r.Status.Target, targetMembers, r.Spec.Durability)
	}
	return options
}
func (c *Controller) verifyReservation(ctx context.Context, r *Resource) error {
	reader, err := c.Bucket.Get(ctx, path.Join(c.Prefix, r.Spec.SourceClusterID, "recovery/successor.json"))
	if err != nil {
		return err
	}
	want := jsonBytes(c.reservation(r))
	got, err := io.ReadAll(io.LimitReader(reader, int64(len(want))+1))
	closeErr := reader.Close()
	if err != nil || closeErr != nil || !bytes.Equal(want, got) {
		return fmt.Errorf("source successor reservation changed")
	}
	return nil
}

func (c *Controller) releaseCompleted(ctx context.Context, r *Resource) error {
	var sts object
	if err := c.Kube.Get(ctx, c.stsPath(r.Spec.StatefulSet), &sts); err != nil {
		return err
	}
	if str(nested(sts, "metadata", "uid")) != r.Status.StatefulSetUID || str(nested(sts, "metadata", "annotations", ownerAnnotation)) != c.operationID(r) {
		return nil
	}
	delete(asObject(nested(sts, "metadata", "annotations")), ownerAnnotation)
	return c.Kube.Put(ctx, c.stsPath(r.Spec.StatefulSet), sts, nil)
}

func (c *Controller) ownedPods(ctx context.Context, uid string) ([]object, error) {
	var pods podList
	if err := c.Kube.Get(ctx, c.corePath("pods", ""), &pods); err != nil {
		return nil, err
	}
	var owned []object
	for _, pod := range pods.Items {
		for _, ref := range list(nested(pod, "metadata", "ownerReferences")) {
			owner := asObject(ref)
			if str(owner["uid"]) == uid && str(owner["kind"]) == "StatefulSet" {
				owned = append(owned, pod)
				break
			}
		}
	}
	return owned, nil
}

func (c *Controller) deleteFencedLearners(ctx context.Context, r *Resource, sts object, pods []object) error {
	root := str(nested(r.Metadata, "annotations", automaticOwner))
	logical := str(nested(r.Metadata, "annotations", automaticLogical))
	if root == "" || !identifier.MatchString(logical) {
		return nil
	}
	var candidates []object
	for _, pod := range pods {
		name := str(nested(pod, "metadata", "name"))
		refs := list(nested(pod, "metadata", "ownerReferences"))
		if len(refs) != 1 || nested(pod, "metadata", "deletionTimestamp") != nil || nested(pod, "metadata", "annotations", fencingRootAnnotation) != root || nested(pod, "metadata", "labels", "rhiza.mrchypark.dev/automatic-learner") != name {
			continue
		}
		owner := asObject(refs[0])
		if !identifier.MatchString(name) || owner["controller"] != false || owner["apiVersion"] != "apps/v1" || owner["kind"] != "StatefulSet" || owner["name"] != r.Spec.StatefulSet || owner["uid"] != r.Status.StatefulSetUID || nested(sts, "metadata", "uid") != r.Status.StatefulSetUID {
			continue
		}
		candidates = append(candidates, pod)
	}
	if len(candidates) == 0 {
		return nil
	}
	state, _, err := c.loadAuto(ctx, path.Join(c.Prefix, "automatic", c.Kube.Namespace, logical, "control.json"))
	if err != nil {
		return err
	}
	intent := state.Intent
	if state.BindingUID != root || state.StatefulSetUID != r.Status.StatefulSetUID || state.ActiveClusterID != r.Spec.SourceClusterID || intent == nil || intent.RecoveryUID != str(r.Metadata["uid"]) || intent.RecoveryName != resourceName(r) || intent.SpecHash != hashJSON(r.Spec) || intent.Request.Scope != ScopeGeneration || intent.Request.BindingUID != root || intent.Request.Namespace != c.Kube.Namespace || intent.Request.StatefulSet != r.Spec.StatefulSet || intent.Request.StatefulSetUID != r.Status.StatefulSetUID || intent.Request.SourceClusterID != r.Spec.SourceClusterID || intent.Request.OperationID != r.Spec.RecoveryID || intent.Proof == nil || intent.Proof.ProofID != r.Spec.Fence.Evidence {
		return fmt.Errorf("generation cleanup differs from authorized fencing intent")
	}
	if err := validateFence(r.Spec.Fence, r.Spec.RecoveryID, r.Spec.SourceClusterID, r.Status.StatefulSetUID); err != nil {
		return err
	}
	if err := validateFenceProof(intent.Request, intent.Proof); err != nil {
		return err
	}
	if err := validateLearnerHistory(state.Learners); err != nil {
		return err
	}
	for _, pod := range candidates {
		name, uid := str(nested(pod, "metadata", "name")), str(nested(pod, "metadata", "uid"))
		bound := false
		var fencedTarget FenceTarget
		for _, target := range intent.Request.Targets {
			if target.Pod == name && target.NodeID == name && target.PodUID == uid && uid != "" {
				if fencedTarget.Pod != "" {
					return fmt.Errorf("duplicate learner fence target")
				}
				fencedTarget = target
				bound = target.WALIdentity != ""
				if !bound {
					if err := c.verifyNeverBoundCleanup(ctx, intent.Request, intent.Proof, target); err != nil {
						return err
					}
					bound = true
				}
			}
		}
		if !bound {
			return fmt.Errorf("standalone learner is outside the fenced incarnation inventory")
		}
		seen := false
		for _, entry := range state.Learners {
			if entry.SourceClusterID != r.Spec.SourceClusterID || entry.Target.Pod != name {
				continue
			}
			if seen || entry.Target != fencedTarget || entry.OperationID != nested(pod, "metadata", "annotations", ownerAnnotation) || entry.CredentialUID != nested(pod, "metadata", "annotations", fencingCredentialAnnotation) {
				return fmt.Errorf("learner cleanup creation history differs")
			}
			seen = true
		}
		if fencedTarget.WALIdentity == "" && !seen {
			return fmt.Errorf("never-bound learner lacks durable creation history")
		}
		if err := validateSupervisedPod(pod); err != nil {
			return err
		}
		if err := (&FencingExecutor{Kube: c.Kube}).validateLearnerBinding(ctx, pod, r.Spec.StatefulSet); err != nil {
			return err
		}
		// Deletion is cleanup after fencing, never evidence of termination.
		if err := c.Kube.DeletePod(ctx, c.Kube.Namespace, name, uid); err != nil && !apiNotFound(err) {
			return err
		}
	}
	return nil
}

func podEndpoint(pod object, containerName, endpoint string) (string, error) {
	ip := str(nested(pod, "status", "podIP"))
	if net.ParseIP(ip) == nil {
		return "", fmt.Errorf("pod IP unavailable")
	}
	ctr, err := container(object{"spec": object{"template": object{"spec": pod["spec"]}}}, containerName)
	if err != nil {
		return "", err
	}
	port := "8080"
	// Embedded hosts can keep the application HTTP port separate.
	for _, name := range []string{"recovery", "http"} {
		for _, p := range list(ctr["ports"]) {
			v := asObject(p)
			if str(v["name"]) != name {
				continue
			}
			n := number(v["containerPort"])
			if n < 1 || n > 65535 || str(v["protocol"]) != "" && str(v["protocol"]) != "TCP" {
				return "", fmt.Errorf("invalid recovery HTTP port")
			}
			return "http://" + net.JoinHostPort(ip, fmt.Sprint(n)) + endpoint, nil
		}
	}
	return "http://" + net.JoinHostPort(ip, port) + endpoint, nil
}
func (c *Controller) client() *http.Client {
	result := http.Client{Timeout: 30 * time.Second}
	if c.HTTP != nil {
		result = *c.HTTP
	}
	result.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &result
}
func (c *Controller) probe(ctx context.Context, r *Resource, sts object) ([]network.VoterRecoveryStatus, error) {
	pods, err := c.ownedPods(ctx, str(nested(sts, "metadata", "uid")))
	if err != nil {
		return nil, err
	}
	var peers []network.VoterRecoveryStatus
	for _, pod := range pods {
		if nested(pod, "metadata", "deletionTimestamp") != nil {
			continue
		}
		endpoint, err := podEndpoint(pod, r.Spec.Container, "/recovery/status")
		if err != nil {
			continue
		}
		call, cancel := context.WithTimeout(ctx, 3*time.Second)
		req, _ := http.NewRequestWithContext(call, http.MethodGet, endpoint, nil)
		response, err := c.client().Do(req)
		if err == nil {
			var peer network.VoterRecoveryStatus
			if response.StatusCode == 200 && json.NewDecoder(io.LimitReader(response.Body, 16<<10)).Decode(&peer) == nil && peer.NodeID == str(nested(pod, "metadata", "name")) {
				peers = append(peers, peer)
			}
			response.Body.Close()
		}
		cancel()
	}
	return peers, nil
}
func (c *Controller) captureArchive(ctx context.Context, r *Resource, sts object, token string) string {
	if token == "" {
		return "unavailable: source admin token is not configured"
	}
	pods, err := c.ownedPods(ctx, str(nested(sts, "metadata", "uid")))
	if err != nil {
		return "unavailable: pod inventory failed"
	}
	attempted, success := 0, 0
	for _, pod := range pods {
		endpoint, err := podEndpoint(pod, r.Spec.Container, "/recovery/archive")
		if err != nil {
			continue
		}
		attempted++
		call, cancel := context.WithTimeout(ctx, 10*time.Second)
		req, _ := http.NewRequestWithContext(call, http.MethodPost, endpoint, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		response, err := c.client().Do(req)
		if err == nil {
			if response.StatusCode == 200 {
				success++
			}
			response.Body.Close()
		}
		cancel()
	}
	return fmt.Sprintf("certified suffix captured from %d/%d reachable pod endpoints; missing history is not bounded", success, attempted)
}
func (c *Controller) claimStatefulSet(ctx context.Context, sts object, op string) error {
	meta := asObject(sts["metadata"])
	annotations := asObject(meta["annotations"])
	if annotations == nil {
		annotations = object{}
		meta["annotations"] = annotations
	}
	existing := str(annotations[ownerAnnotation])
	if existing != "" && existing != op {
		return fmt.Errorf("StatefulSet belongs to another recovery operation")
	}
	if existing == op {
		return nil
	}
	annotations[ownerAnnotation] = op
	return c.Kube.Put(ctx, c.stsPath(str(meta["name"])), sts, &sts)
}
func (c *Controller) targetCredentials(ctx context.Context, r *Resource, name string, source []quepaxa.Member) ([]quepaxa.Member, error) {
	var secret object
	err := c.Kube.Get(ctx, c.corePath("secrets", name), &secret)
	var api *APIError
	if errors.As(err, &api) && api.StatusCode == 404 && source != nil {
		members, admin, peerTokens, err := freshMembers(source, r.Status.Target)
		if err != nil {
			return nil, err
		}
		secret = object{"apiVersion": "v1", "kind": "Secret", "type": "Opaque", "immutable": true, "metadata": object{"name": name, "annotations": object{ownerAnnotation: c.operationID(r)}}, "data": object{
			"members":     base64.StdEncoding.EncodeToString(jsonBytes(members)),
			"admin":       base64.StdEncoding.EncodeToString([]byte(admin)),
			"peer_tokens": base64.StdEncoding.EncodeToString(jsonBytes(peerTokens)),
		}}
		// Even an uncertain create is resolved by reading the same deterministic key.
		createErr := c.Kube.Post(ctx, c.corePath("secrets", ""), secret, nil)
		if err = c.Kube.Get(ctx, c.corePath("secrets", name), &secret); err != nil {
			return nil, errors.Join(createErr, err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("target credential secret unavailable")
	}
	if str(nested(secret, "metadata", "annotations", ownerAnnotation)) != c.operationID(r) || secret["immutable"] != true {
		return nil, fmt.Errorf("target secret does not belong to this immutable recovery operation")
	}
	data, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "members")))
	if err != nil {
		return nil, fmt.Errorf("invalid target credentials")
	}
	members, err := recoveryMembers(string(data))
	if err != nil || len(members) < 3 {
		return nil, fmt.Errorf("invalid target membership")
	}
	admin, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "admin")))
	if err != nil || len(admin) < 32 {
		return nil, fmt.Errorf("invalid target admin credential")
	}
	// Peer identities are process-local secrets: the target generation's
	// private tokens live only in this Secret and are mounted per Pod.
	var peerTokens map[string]string
	rawTokens, err := base64.StdEncoding.DecodeString(str(nested(secret, "data", "peer_tokens")))
	if err != nil || json.Unmarshal(rawTokens, &peerTokens) != nil {
		return nil, fmt.Errorf("invalid target peer credentials")
	}
	for _, m := range members {
		if peerTokens[string(m.ID)] == "" {
			return nil, fmt.Errorf("target peer credentials are incomplete")
		}
	}
	if source != nil {
		// A reused immutable Secret must name exactly the voters being replaced;
		// a subset would fork a smaller generation than the one it succeeds.
		if len(members) != len(source) {
			return nil, fmt.Errorf("target credentials preserve %d of %d source voters", len(members), len(source))
		}
		old := map[quepaxa.NodeID]quepaxa.Member{}
		for _, m := range source {
			old[m.ID] = m
		}
		for _, m := range members {
			prior, ok := old[m.ID]
			if !ok || m.PublicKey == prior.PublicKey || m.URL != prior.URL || m.PeerURL != prior.PeerURL || m.LogURL != prior.LogURL {
				return nil, fmt.Errorf("target credentials do not preserve source voter endpoints")
			}
		}
	}
	return members, nil
}
func freshMembers(source []quepaxa.Member, clusterID string) ([]quepaxa.Member, string, map[string]string, error) {
	members := append([]quepaxa.Member(nil), source...)
	peerTokens := make(map[string]string, len(members))
	for i := range members {
		token, err := randomToken()
		if err != nil {
			return nil, "", nil, err
		}
		members[i].PublicKey = quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID(clusterID), members[i].ID, token))
		peerTokens[string(members[i].ID)] = token
	}
	admin, err := randomToken()
	return members, admin, peerTokens, err
}
func randomToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
func (c *Controller) immutable(ctx context.Context, key string, data []byte) error {
	writeErr := c.Bucket.Upload(ctx, key, bytes.NewReader(data), objstore.WithIfNotExists())
	reader, err := c.Bucket.Get(ctx, key)
	if err != nil {
		return errors.Join(writeErr, err)
	}
	got, err := io.ReadAll(io.LimitReader(reader, int64(len(data))+1))
	closeErr := reader.Close()
	if err != nil || closeErr != nil || !bytes.Equal(data, got) {
		return fmt.Errorf("immutable recovery object differs or is unavailable")
	}
	return nil
}
func (c *Controller) sourceMembership(ctx context.Context, prefix string) (recovery.MembershipRecord, error) {
	record, missing, err := c.sourceMembershipOrMissing(ctx, prefix)
	if err != nil {
		return recovery.MembershipRecord{}, err
	}
	if missing {
		return recovery.MembershipRecord{}, fmt.Errorf("source membership history unavailable")
	}
	return record, nil
}

// sourceMembershipOrMissing preserves the distinction between definitive
// absence and an unreadable or malformed source object. Only the former may be
// considered by the authenticated bootstrap-genesis exception.
func (c *Controller) sourceMembershipOrMissing(ctx context.Context, prefix string) (recovery.MembershipRecord, bool, error) {
	var record recovery.MembershipRecord
	reader, err := c.Bucket.Get(ctx, path.Join(prefix, "voters/membership.json"))
	if c.Bucket.IsObjNotFoundErr(err) {
		return record, true, nil
	}
	if err != nil {
		return record, false, fmt.Errorf("source membership history unavailable")
	}
	data, err := io.ReadAll(io.LimitReader(reader, 64<<10))
	closeErr := reader.Close()
	if err != nil || closeErr != nil || len(data) == 64<<10 || json.Unmarshal(data, &record) != nil || record.Version != 2 || (record.Durability != "async" && record.Durability != "before-ack") || !bytes.Equal(data, jsonBytes(record)) {
		return record, false, fmt.Errorf("source membership or durability history is invalid")
	}
	return record, false, nil
}

func manualMissingSourceMembershipAllowed(r *Resource) bool {
	if r == nil || !r.Spec.AllowMissingSourceMembership || r.Spec.RecoveryID == "" ||
		str(r.Metadata["uid"]) == "" || r.Spec.Membership != nil {
		return false
	}
	// Automatic recovery owns a different policy and binding. This opt-in is
	// deliberately unavailable to annotated automatic children.
	return str(nested(r.Metadata, "annotations", automaticOwner)) == "" &&
		str(nested(r.Metadata, "annotations", automaticLogical)) == ""
}

func (c *Controller) confirmedBootstrapSourceMembers(ctx context.Context, r *Resource, sts, ctr object, env map[string]string, members []quepaxa.Member, confirmation BootstrapGenesisFenceConfirmation) ([]quepaxa.Member, error) {
	request := confirmation.Request
	binding := request.BootstrapGenesis
	if binding == nil || request.Scope != ScopeGeneration || request.OperationID != r.Spec.RecoveryID || request.SourceClusterID != r.Spec.SourceClusterID ||
		request.StatefulSet != r.Spec.StatefulSet || request.StatefulSetUID != str(nested(sts, "metadata", "uid")) ||
		request.LogicalID == "" || request.LogicalID != str(nested(r.Metadata, "annotations", automaticLogical)) ||
		request.BindingUID != str(nested(r.Metadata, "annotations", automaticOwner)) || request.Namespace != c.Kube.Namespace || binding.ClusterName == "" || binding.ClusterUID == "" {
		return nil, fmt.Errorf("confirmed fence request does not identify this source generation")
	}
	var cluster ClusterResource
	if err := c.Kube.Get(ctx, c.clusterPath(binding.ClusterName), &cluster); err != nil {
		return nil, fmt.Errorf("bootstrap cluster configuration is unavailable")
	}
	normalizeClusterSpec(&cluster)
	logical := str(nested(r.Metadata, "annotations", automaticLogical))
	auto, _, err := c.loadAuto(ctx, path.Join(c.Prefix, "automatic", c.Kube.Namespace, logical, "control.json"))
	if err != nil {
		return nil, fmt.Errorf("protected automatic bootstrap intent is unavailable")
	}
	if str(cluster.Metadata["name"]) != binding.ClusterName || str(cluster.Metadata["namespace"]) != c.Kube.Namespace ||
		str(cluster.Metadata["uid"]) != binding.ClusterUID || cluster.Spec.StatefulSet != r.Spec.StatefulSet ||
		cluster.Spec.AdoptClusterID != r.Spec.SourceClusterID || cluster.Spec.LogicalID != request.LogicalID || !cluster.Spec.Automatic || !cluster.Spec.Bootstrap ||
		cluster.Spec.Policy == nil || !cluster.Spec.Policy.AllowDataLoss || cluster.Spec.VoterCount != len(members) ||
		logical != request.LogicalID || auto.BindingUID != str(cluster.Metadata["uid"]) || auto.BindingVersion != autoBindingVersion ||
		auto.BindingHash != autoBinding(&cluster) || auto.StatefulSetUID != request.StatefulSetUID || auto.ActiveClusterID != request.SourceClusterID ||
		auto.BootstrapGenesis == nil || *auto.BootstrapGenesis != *binding || auto.Intent == nil || auto.Intent.Proof == nil ||
		hashJSON(auto.Intent.Request) != hashJSON(request) || hashJSON(*auto.Intent.Proof) != hashJSON(confirmation.Proof) ||
		env["RHIZA_CLUSTER_ID"] != r.Spec.SourceClusterID || env["RHIZA_OBJSTORE_DURABILITY"] != "async" || env["RHIZA_ENABLE_RECONFIGURATION"] != "true" {
		return nil, fmt.Errorf("bootstrap source configuration no longer matches the confirmed policy")
	}
	if err := validateClusterRuntime(&cluster, ctr, env); err != nil {
		return nil, err
	}
	if err := noPVC(sts, ctr, env); err != nil {
		return nil, err
	}
	if err := c.storeConfigMatches(env); err != nil {
		return nil, err
	}
	if !bootstrapTemplateInstalled(ctr, &cluster) {
		return nil, fmt.Errorf("source membership is not from the immutable bootstrap configuration")
	}
	allowed, err := allowedGenerationVoterIDs(&cluster, sts, nil)
	if err != nil {
		return nil, err
	}
	if err := voterCountContract(&cluster, sts, members, true, true, allowed); err != nil {
		return nil, err
	}
	if err := c.validateBootstrappedMembers(ctx, &cluster, sts, true, members, allowed); err != nil {
		return nil, err
	}
	if len(request.Targets) != len(members) {
		return nil, fmt.Errorf("confirmed fence target count differs from bootstrap membership")
	}
	targets := make(map[string]FenceTarget, len(request.Targets))
	for _, target := range request.Targets {
		if target.NodeID == "" || target.Pod == "" || target.NodeID != target.Pod || targets[target.NodeID].NodeID != "" {
			return nil, fmt.Errorf("confirmed fence target inventory is incomplete or ambiguous")
		}
		targets[target.NodeID] = target
	}
	for _, member := range members {
		if _, ok := targets[string(member.ID)]; !ok {
			return nil, fmt.Errorf("bootstrap membership differs from the confirmed fence cohort")
		}
	}
	// Cross-check the bootstrap Secret-backed source endpoints against the
	// configured endpoints and CONTROL-confirmed voter IDs before using them for
	// the target-only source fingerprint.
	derived, err := clusterMemberSources(&cluster, sts, c.Kube.Namespace)
	if err != nil || len(derived) != len(request.Targets) {
		return nil, errors.Join(err, fmt.Errorf("bootstrap source cohort could not be derived"))
	}
	for _, member := range derived {
		if _, ok := targets[string(member.ID)]; !ok {
			return nil, fmt.Errorf("derived bootstrap membership differs from the confirmed fence cohort")
		}
	}
	if len(members) != len(derived) {
		return nil, fmt.Errorf("bootstrap membership differs from the configured source cohort")
	}
	configured := make(map[quepaxa.NodeID]quepaxa.Member, len(members))
	for _, member := range members {
		configured[member.ID] = member
	}
	for _, member := range derived {
		observed, ok := configured[member.ID]
		if !ok || observed.URL != member.URL || observed.PeerURL != member.PeerURL {
			return nil, fmt.Errorf("bootstrap source endpoints differ from the configured cohort")
		}
	}
	// Preserve the source public keys so the target-only fingerprint matches the
	// fencer's independently checked cohort; targetCredentials replaces them
	// with fresh target keys before any target membership is written.
	return members, nil
}
func (c *Controller) record(ctx context.Context, r *Resource, phase, message string) error {
	r.Status.Phase, r.Status.Message = phase, message
	return c.Kube.Put(ctx, c.resourcePath(resourceName(r))+"/status", r, r)
}
func validateFence(f Fence, op, cluster, uid string) error {
	if op == "" || uid == "" || !f.Confirmed || strings.TrimSpace(f.Evidence) == "" || f.RecoveryID != op || f.ClusterID != cluster || f.StatefulSetUID != uid {
		return fmt.Errorf("confirmed fence must bind this operation, source cluster, and StatefulSet UID")
	}
	return nil
}
func replaceEnv(env []any, name string, value object) []any {
	entry := object{"name": name}
	for k, v := range value {
		entry[k] = v
	}
	return append(removeEnv(env, name), entry)
}

func applyTargetCredentials(ctr object, secretName string) {
	values := list(ctr["env"])
	values = replaceEnv(values, "RHIZA_CLUSTER_MEMBERS", object{"valueFrom": object{"secretKeyRef": object{"name": secretName, "key": "members"}}})
	values = replaceEnv(values, "RHIZA_ADMIN_TOKEN", object{"valueFrom": object{"secretKeyRef": object{"name": secretName, "key": "admin"}}})
	// Each target Pod receives the whole token map and selects its own entry by
	// node ID; a StatefulSet cannot mount a per-Pod Secret.
	values = replaceEnv(values, "RHIZA_PEER_TOKENS", object{"valueFrom": object{"secretKeyRef": object{"name": secretName, "key": "peer_tokens"}}})
	// An inherited scalar conflicts with the map and cannot be cleared by deleting
	// an envFrom entry, so an explicit empty override must supersede it.
	values = replaceEnv(values, "RHIZA_PEER_TOKEN", object{"value": ""})
	ctr["env"] = values
}
func removeEnv(env []any, name string) []any {
	var result []any
	for _, item := range env {
		if str(asObject(item)["name"]) != name {
			result = append(result, item)
		}
	}
	return result
}
func shortID(value string) string { return hashJSON(value)[:24] }
func hashJSON(value any) string {
	sum := sha256.Sum256(jsonBytes(value))
	return hex.EncodeToString(sum[:])
}
func sourceFingerprint(cluster, mode string, members []quepaxa.Member, prefix string) string {
	m := append([]quepaxa.Member(nil), members...)
	sort.Slice(m, func(i, j int) bool { return m[i].ID < m[j].ID })
	return hashJSON(struct {
		Cluster, Mode, Prefix string
		Members               []quepaxa.Member
	}{cluster, mode, prefix, m})
}
func immutableSpecHash(s Spec) string { s.Fence = Fence{}; s.AllowDataLoss = false; return hashJSON(s) }
func recoveryMembers(raw string) ([]quepaxa.Member, error) {
	var members []quepaxa.Member
	if json.Unmarshal([]byte(raw), &members) != nil || len(members) == 0 {
		return nil, fmt.Errorf("invalid source membership")
	}
	seen := map[quepaxa.NodeID]bool{}
	for _, m := range members {
		if !identifier.MatchString(string(m.ID)) || m.PublicKey == (quepaxa.PublicKey{}) || seen[m.ID] {
			return nil, fmt.Errorf("incomplete or duplicate voter identity")
		}
		seen[m.ID] = true
		for _, endpoint := range []string{m.URL, m.PeerURL, m.LogURL} {
			if endpoint == "" {
				continue
			}
			u, err := url.Parse(endpoint)
			if err != nil || u.Host == "" || u.User != nil {
				return nil, fmt.Errorf("invalid voter endpoint")
			}
		}
	}
	return members, nil
}

// buildAnchorRequest constructs the canonical recoveryanchor.Request from
// current controller state. Shared by Sealed-stage activation and
// Starting/Restarted-stage revalidation.
func (c *Controller) buildAnchorRequest(r *Resource, s sourceState, record recovery.MembershipRecord, result recovery.ForkResult, anchorHash string) recoveryanchor.Request {
	return recoveryanchor.Request{
		AnchorID:         r.Spec.AnchorID,
		OperationID:      c.operationID(r),
		StatefulSetUID:   r.Status.StatefulSetUID,
		SourceClusterID:  r.Spec.SourceClusterID,
		TargetClusterID:  r.Status.Target,
		SourcePrefix:     s.prefix,
		TargetPrefix:     path.Join(c.Prefix, r.Status.Target),
		SourceMembership: r.Status.SourceMembership,
		FenceHash:        hashJSON(r.Spec.Fence),
		TargetAnchorHash: anchorHash,
		TargetMembership: record,
		Fork:             result,
	}
}

// requireAnchorReceipt verifies that AnchorRequest and AnchorReceipt are
// both present when Spec.AnchorID is set, and that the Anchor client is
// available. Returns nil when no anchor verification is required.
func (c *Controller) requireAnchorReceipt(r *Resource) error {
	if r.Spec.AnchorID == "" {
		return nil
	}
	if r.Status.AnchorRequest == nil || r.Status.AnchorReceipt == nil {
		return fmt.Errorf("anchor receipt missing for anchored recovery")
	}
	if c.Anchor == nil {
		return fmt.Errorf("anchor backend unavailable for receipt verification")
	}
	return nil
}

// verifyAnchorRequest reconstructs the request from current state and
// verifies it matches the persisted request before live-verifying the
// receipt. Called from Starting and Restarted stages.
func (c *Controller) verifyAnchorRequest(ctx context.Context, r *Resource, s sourceState, record recovery.MembershipRecord, result recovery.ForkResult, anchorHash string) error {
	if err := c.requireAnchorReceipt(r); err != nil {
		return err
	}
	wantReq := c.buildAnchorRequest(r, s, record, result, anchorHash)
	if wantReq != *r.Status.AnchorRequest {
		return fmt.Errorf("anchor request does not match current operation state")
	}
	return c.Anchor.Verify(ctx, *r.Status.AnchorRequest, *r.Status.AnchorReceipt)
}
