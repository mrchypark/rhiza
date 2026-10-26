//go:build localhooks

package logicalauthority_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
)

const (
	maxFailureSnapshotRuns      = 32
	maxFailureSnapshotProducers = 16
	maxFailureSnapshotNodes     = 16
	maxFailureSnapshotKeyReads  = 32
)

// collectLogicalAuthorityFailureSnapshot is a bounded, read-only failure
// snapshot for the Floci lifecycle test. It deliberately samples independent
// sources rather than presenting them as an atomic snapshot or a quorum proof.
func collectLogicalAuthorityFailureSnapshot(nodes map[string]*node.Node, authorityState *operator.AuthorityStateStore,
	executor *operator.FencingExecutor, statefulSetUID, sourceClusterID string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type producerEntrySnapshot struct {
		Sequence                       uint64 `json:"sequence"`
		IntentDigest                   string `json:"intent_digest"`
		KeyHash                        string `json:"key_hash"`
		KeyClass                       string `json:"key_class"`
		Prepared                       bool   `json:"prepared"`
		DispatchClosed                 bool   `json:"dispatch_closed"`
		DispatchCount                  int    `json:"dispatch_count"`
		DispatchReceiptCount           int    `json:"dispatch_receipt_count"`
		KeyInspection                  bool   `json:"key_inspection"`
		KeyStateReadError              string `json:"key_state_read_error,omitempty"`
		KeyRecordExists                bool   `json:"key_record_exists"`
		KeyOwnerHash                   string `json:"key_owner_hash,omitempty"`
		KeyOwnerSequence               uint64 `json:"key_owner_sequence,omitempty"`
		KeyOwnerEntryFound             bool   `json:"key_owner_entry_found"`
		KeyOwnerEntryTerminal          bool   `json:"key_owner_entry_terminal"`
		KeyOwnerEntryRetired           bool   `json:"key_owner_entry_retired"`
		CurrentTupleMatchesKeyOwner    bool   `json:"current_tuple_matches_key_owner"`
		CurrentEntryTerminal           bool   `json:"current_entry_terminal"`
		CurrentEntryRetired            bool   `json:"current_entry_retired"`
		PendingGenerationZero          bool   `json:"pending_generation_zero"`
		GenerationZeroBarrier          bool   `json:"generation_zero_barrier"`
		Resolution                     string `json:"resolution,omitempty"`
		ResolutionReceiptDigestPresent bool   `json:"resolution_receipt_digest_present"`
		GenerationZeroGuardPresent     bool   `json:"generation_zero_guard_present"`
		GuardedResolutionPresent       bool   `json:"guarded_resolution_present"`
	}
	type producerSnapshot struct {
		ProducerHash     string                  `json:"producer_hash,omitempty"`
		Profile          string                  `json:"profile,omitempty"`
		Phase            fencing.MutationPhase   `json:"phase,omitempty"`
		LastAdmitted     uint64                  `json:"last_admitted"`
		CompactedThrough uint64                  `json:"compacted_through"`
		EvidenceFault    bool                    `json:"evidence_fault"`
		Entries          []producerEntrySnapshot `json:"entries,omitempty"`
		ReadError        string                  `json:"read_error,omitempty"`
	}
	type runSnapshot struct {
		NodeHash     string              `json:"node_hash"`
		PodHash      string              `json:"pod_hash"`
		RunHash      string              `json:"run_hash"`
		Role         fencing.StartupRole `json:"role,omitempty"`
		Profile      string              `json:"profile,omitempty"`
		WALBound     bool                `json:"wal_bound"`
		ProducerHash string              `json:"producer_hash,omitempty"`
		ScopeHash    string              `json:"scope_hash,omitempty"`
		HasStop      bool                `json:"has_stop"`
		HasClosure   bool                `json:"has_closure"`
		HasPromotion bool                `json:"has_promotion"`
	}
	type nodeSnapshot struct {
		NodeHash    string   `json:"node_hash"`
		Present     bool     `json:"present"`
		Ready       bool     `json:"ready"`
		StatusRead  bool     `json:"status_read"`
		ConfigID    uint     `json:"config_id,omitempty"`
		Voting      bool     `json:"voting,omitempty"`
		Pending     bool     `json:"pending,omitempty"`
		WALHash     string   `json:"wal_hash,omitempty"`
		VoterCount  int      `json:"voter_count,omitempty"`
		VoterHashes []string `json:"voter_hashes,omitempty"`
		StatusError string   `json:"status_error,omitempty"`
	}
	type snapshot struct {
		SnapshotKind            string             `json:"snapshot_kind"`
		Atomic                  bool               `json:"atomic"`
		RootRead                bool               `json:"root_read"`
		RootVersion             bool               `json:"root_version_present"`
		RootReadError           string             `json:"root_read_error,omitempty"`
		RunsTruncated           bool               `json:"runs_truncated"`
		ProducersTruncated      bool               `json:"producers_truncated"`
		NodesTruncated          bool               `json:"nodes_truncated"`
		KeyInspectionsTruncated bool               `json:"key_inspections_truncated"`
		Runs                    []runSnapshot      `json:"runs,omitempty"`
		Producers               []producerSnapshot `json:"producers,omitempty"`
		Nodes                   []nodeSnapshot     `json:"nodes,omitempty"`
	}

	result := snapshot{SnapshotKind: "logical-authority-failure-observation-v1", Atomic: false}
	remainingKeyReads := maxFailureSnapshotKeyReads
	if authorityState != nil {
		root, version, err := authorityState.ReadRoot(ctx, statefulSetUID, sourceClusterID)
		if err == nil {
			result.RootRead = true
			result.RootVersion = version != nil
			producerSeen := make(map[string]bool)
			runIDs := make([]string, 0, len(root.Ledger.Runs))
			for runID := range root.Ledger.Runs {
				runIDs = append(runIDs, runID)
			}
			sort.Strings(runIDs)
			if len(runIDs) > maxFailureSnapshotRuns {
				runIDs = runIDs[:maxFailureSnapshotRuns]
				result.RunsTruncated = true
			}
			for _, runID := range runIDs {
				run := root.Ledger.Runs[runID]
				runRow := runSnapshot{
					NodeHash: diagnosticHash(run.Identity.NodeID), PodHash: diagnosticHash(run.Identity.PodUID),
					RunHash: diagnosticHash(run.Identity.RunID), Role: run.Role, HasStop: run.Stop != nil,
					HasClosure: run.LogicalClosure != nil, HasPromotion: run.Promotion != nil,
					WALBound:     run.Identity.WALIdentity != "",
					ProducerHash: diagnosticHash(run.MutationProducerUID), ScopeHash: diagnosticHash(run.MutationScopeHash),
				}
				if run.LogicalBinding != nil {
					runRow.Profile = run.LogicalBinding.Profile
					runRow.WALBound = run.LogicalBinding.WALIdentity != ""
				}
				result.Runs = append(result.Runs, runRow)
				if run.LogicalBinding == nil || run.MutationProducerUID == "" || producerSeen[run.MutationProducerUID] {
					continue
				}
				if len(result.Producers) >= maxFailureSnapshotProducers {
					result.ProducersTruncated = true
					continue
				}
				producerSeen[run.MutationProducerUID] = true
				producer, actualUID, _, readErr := authorityState.ReadLogicalProducer(ctx, statefulSetUID, *run.LogicalBinding)
				producerRow := producerSnapshot{ProducerHash: diagnosticHash(actualUID)}
				if readErr != nil {
					producerRow.ReadError = "logical_producer_read_failed"
					result.Producers = append(result.Producers, producerRow)
					continue
				}
				producerRow.Profile, producerRow.Phase = producer.Profile, producer.Phase
				producerRow.LastAdmitted, producerRow.CompactedThrough = producer.LastAdmitted, producer.CompactedThrough
				producerRow.EvidenceFault = producer.EvidenceFault != ""
				sequences := make([]uint64, 0, len(producer.Entries))
				for sequence := range producer.Entries {
					sequences = append(sequences, sequence)
				}
				sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
				for _, sequence := range sequences {
					entry := producer.Entries[sequence]
					intentData, _ := json.Marshal(entry.Intent)
					row := producerEntrySnapshot{Sequence: sequence, IntentDigest: diagnosticDigest(string(intentData)),
						KeyHash: diagnosticDigest(entry.Intent.Key), KeyClass: safeKeyClass(entry.Intent.Key),
						Prepared: entry.Preparation != nil, DispatchClosed: entry.DispatchClosed,
						DispatchCount: len(entry.Dispatches), GenerationZeroGuardPresent: entry.GenerationZeroGuard != nil,
						GuardedResolutionPresent: entry.GuardedLogicalResolution != nil}
					if entry.Resolution != nil {
						row.Resolution = string(entry.Resolution.Outcome)
						row.ResolutionReceiptDigestPresent = entry.Resolution.ReceiptDigest != ""
					}
					if entry.Resolution == nil || !entry.DispatchClosed {
						if remainingKeyReads == 0 {
							result.KeyInspectionsTruncated = true
						} else {
							remainingKeyReads--
							row.KeyInspection = true
							if executor == nil {
								row.KeyStateReadError = "logical_key_state_reader_unavailable"
							} else {
								keyState, keyErr := executor.InspectLogicalMutationKeyLocalhooks(ctx, statefulSetUID, run.Identity, sequence, entry.Intent)
								if keyErr != nil {
									row.KeyStateReadError = "logical_key_state_read_failed"
								} else {
									row.KeyRecordExists = keyState.Exists
									row.KeyOwnerHash = diagnosticHash(keyState.OwnerProducerUID)
									row.KeyOwnerSequence = keyState.OwnerSequence
									row.KeyOwnerEntryFound = keyState.OwnerEntryFound
									row.KeyOwnerEntryTerminal = keyState.OwnerEntryTerminal
									row.KeyOwnerEntryRetired = keyState.OwnerEntryRetired
									row.CurrentTupleMatchesKeyOwner = keyState.CurrentTupleMatch
									row.CurrentEntryTerminal = keyState.CurrentEntryTerminal
									row.CurrentEntryRetired = keyState.CurrentEntryRetired
									row.PendingGenerationZero = keyState.PendingGenerationZero
									row.GenerationZeroBarrier = keyState.GenerationZeroBarrier
								}
							}
						}
					}
					for _, dispatch := range entry.Dispatches {
						if dispatch.ReceiptDigest != "" {
							row.DispatchReceiptCount++
						}
					}
					producerRow.Entries = append(producerRow.Entries, row)
				}
				result.Producers = append(result.Producers, producerRow)
			}
			sort.Slice(result.Runs, func(i, j int) bool {
				if result.Runs[i].NodeHash != result.Runs[j].NodeHash {
					return result.Runs[i].NodeHash < result.Runs[j].NodeHash
				}
				return result.Runs[i].RunHash < result.Runs[j].RunHash
			})
			sort.Slice(result.Producers, func(i, j int) bool { return result.Producers[i].ProducerHash < result.Producers[j].ProducerHash })
		} else {
			result.RootReadError = "canonical_root_read_failed"
		}
	}

	names := make([]string, 0, len(nodes))
	for name := range nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) > maxFailureSnapshotNodes {
		names = names[:maxFailureSnapshotNodes]
		result.NodesTruncated = true
	}
	for _, name := range names {
		instance := nodes[name]
		row := nodeSnapshot{NodeHash: diagnosticHash(name), Present: instance != nil}
		if instance != nil {
			row.Ready = instance.Ready()
			if api, err := instance.API(); err == nil {
				if status, err := api.MembershipStatus(); err == nil {
					row.StatusRead, row.ConfigID, row.Voting, row.Pending = true, status.ConfigID, status.Voting, status.Pending
					row.WALHash = diagnosticHash(status.WALIdentity)
					for _, voter := range status.Voters {
						row.VoterHashes = append(row.VoterHashes, diagnosticHash(string(voter)))
					}
					sort.Strings(row.VoterHashes)
					row.VoterCount = len(row.VoterHashes)
				} else {
					row.StatusError = "membership_status_unavailable"
				}
			} else {
				row.StatusError = "node_api_unavailable"
			}
		}
		result.Nodes = append(result.Nodes, row)
	}

	data, err := json.Marshal(result)
	if err != nil {
		return `{"snapshot_kind":"logical-authority-failure-observation-v1","error":"marshal_failed"}`
	}
	return string(data)
}

func diagnosticHash(value string) string {
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:6])
}

func diagnosticDigest(value string) string {
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func safeKeyClass(value string) string {
	clean := strings.Trim(path.Clean("/"+value), "/")
	if clean == "archive/head.bin" {
		return "archive-head"
	}
	parts := strings.Split(clean, "/")
	for _, part := range parts {
		if part == "blocks" || part == "extents" {
			return "block"
		}
	}
	for _, part := range parts {
		if part == "voter" || part == "voters" || strings.HasPrefix(part, "voter-") {
			return "voter"
		}
	}
	for _, part := range parts {
		if part == "checkpoint" || part == "checkpoints" {
			return "checkpoint"
		}
	}
	return "other"
}

func TestSafeKeyClassDoesNotExposeObjectNames(t *testing.T) {
	for _, test := range []struct {
		key  string
		want string
	}{
		{key: "archive/head.bin", want: "archive-head"},
		{key: "archive/blocks/secret-object", want: "block"},
		{key: "checkpoint/blocks/secret-object", want: "block"},
		{key: "voters/node-secret/identity", want: "voter"},
		{key: "checkpoint/root-secret", want: "checkpoint"},
		{key: "user/private-secret", want: "other"},
	} {
		t.Run(test.want, func(t *testing.T) {
			if got := safeKeyClass(test.key); got != test.want {
				t.Fatalf("safeKeyClass(%q)=%q, want %q", test.key, got, test.want)
			}
		})
	}
}
