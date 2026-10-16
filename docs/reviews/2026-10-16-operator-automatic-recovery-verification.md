# Automatic recovery review verification (2026-10-16)

Review baseline: `73a75e1ee0c3a07bd1e15384fdf30c420e24fc28`.

Latest result: the reviewed final candidate passed full Go normal tests,
operator normal/race tests, vet/build/server E2E, and the unchanged actual
Linux Kind automatic-recovery fixture (`PASS`, seven scenarios). Generation
recovery preserved all 528 acknowledged rows, new writes and deduplication.
Final source manifest: `045c8b3d9ac00e7facb19429fe999a9c9160156a8c696e7b457d07e32e87b044`.
Owned test resources were cleaned up. Earlier failed candidates below are
retained history, not the current result. No cloud/release qualification is claimed.

The original review is preserved in [2026-10-16-operator-automatic-recovery-pro.md](2026-10-16-operator-automatic-recovery-pro.md). The table maps each original finding to its executable reproduction. Baseline failures are retained separately from corrected results.

| Finding | Reproduction | Required corrected behavior |
| --- | --- | --- |
| 1: learner retirement races with delayed addition | `TestLearnerRetirementReservesHandshakeWithoutSnapshotJournal`, `TestMembershipRetirementAcknowledgesNeverJournaledAddition` | Reserve retirement before rotating identity; executor acknowledges the exact revision and closes delayed dispatch. |
| 2: committed promotion is stranded after learner loss | `TestMembershipRetirementRecognizesCommittedAdditionAfterLearnerLoss`, `TestAutomaticMembershipCompletionInstallsFailedLearner` | Recognize authenticated committed membership without requiring the failed learner to respond; preserve installed Pod/WAL identity for the next incident. |
| 3: completed generation is stranded after a new outage | `TestAutomaticCompletionSurvivesTargetFailure` | Persist verified installed generation/inventory with the completion outcome before evaluating subsequent current health. |
| 4: local matching snapshots are mistaken for live quorum | `TestAutomaticEscalationRequiresLiveQuorum` | Require an authenticated fresh active quorum challenge; snapshots alone do not clear the no-quorum timer. |

Negative cases cover bare completion status, changed child UID/spec/reservation/lineage, duplicate quorum identities, stale parent CAS, missing or conflicting membership evidence, replayed challenge, invalid signature, wrong identity/cluster/exclusion, and missing certified archive evidence. Final integrated results must name the tested source revision or diff and retain actual command exit codes.

## Verification layers

This Go checkout does not contain the historical Rust paths `scripts/e2e-fast.sh` or `scripts/check-ci.sh`. Its executable verification contract is `.github/workflows/ci.yml`.

1. Inner loop: focused normal and race regressions in `./pkg/operator`, followed by `go test -race ./pkg/operator -count=1`.
2. Full local Go checks: `CGO_ENABLED=0 go test ./...`, the existing `rhiza_local_testhooks` normal/race selectors from the CI workflow, `CGO_ENABLED=0 go vet ./...`, `CGO_ENABLED=0 go build ./cmd/rhiza ./cmd/rhiza-operator`, the real-server `TestSQLServer` E2E, and `CGO_ENABLED=1 go test -race ./...`. Keep vulnerability-scan results distinct from functional tests. The Rust SDK, container build, S3 membership lifecycle, and Kubernetes jobs are separate CI layers.
3. Operator integration: use the existing `automatic-operator-chaos` fixture in a disposable local Kind cluster with candidate-built immutable image identities. The harness supports `--cluster`, `--db-image`, `--operator-image`, and `--output`; use unique owned names and preserve its final `PASS`, acknowledged-data results, fencing/retirement/recovery events, Kubernetes objects, logs, image digests, and source hashes. The normal `operator-chaos` fixture is also available. Both fixtures require disposable-cluster administrator/runtime permissions and must not target shared workloads.
4. Fresh real GCS/Chaos release qualification is a separate release-candidate action; unit or local Kind success does not establish it.

Tests run only after implementation owners finish integration. Serialize heavy suites and Kubernetes builds. Stop fixture processes and remove only the cluster, images, temporary files, and test resources created by that execution, after preserving failure evidence. Verify cleanup and report any remaining owned resource.

## Earlier candidate and retained failures

The earlier integrated candidate is based on the baseline above and is identified by
SHA-256 `1f81d4e112e32ea2a6cdc50bb6689bb1d05f5040913c3e0c5202d9d10f925fbb`
of the sorted per-file SHA-256 manifest of all 325 Go/module inputs. This is a
dirty-worktree verification identity, not a published commit or release.
Both independent cross-reviews cleared that earlier implementation; execution
receipts remain separate from static review conclusions.

Finding 3's initial production baseline failed as expected. Independent review then reproduced missing target voter identities in the first correction and an all-voter identity-capture wait in the next candidate. The executor now retains authenticated target Node ID, Pod UID, and WAL identity where available before its completion write; the parent adopts that snapshot with the installed generation and full frozen voter inventory. A subsequent known single-target failure permits online replacement. An unknown failed incarnation instead requires a new generation-wide fence against the installed generation, with the original durability, loss approval, grace, cooldown, archive, and complete fencing proof requirements. Authentication failures or contradictory identity responses block completion. Corrected targeted normal/race results are retained separately from the earlier results. Other finding owners retain their own baseline and corrected command results. No cloud qualification or release is claimed by this document.

Legacy compatibility: a completed child from an older operator may lack target identities. All existing installation proofs must still validate before adoption. Missing observations remain unknown; authenticated live observations can fill them, and an unknown failed voter requires generation-wide fencing rather than online removal. Missing or conflicting child authority, installation, or fencing proofs still block recovery. The regression checks both this legacy path and the async loss-approval gate.

## Earlier candidate's integrated execution receipts

On Go `go1.27.2 darwin/arm64`, all of the following finished with actual exit 0
and unchanged Go/module source manifests before and after each command:

- Integrated automatic-recovery/retirement normal and race selectors; whole
  `pkg/operator` race suite.
- `CGO_ENABLED=0 go test -p 2 ./...`, `go vet -p 2 ./...`, and builds of
  `cmd/rhiza` and `cmd/rhiza-operator`.
- Existing CI learner hard-kill/expired-pin tests in normal and race modes;
  node/checkpoint/network foreground, overload, terminal, read-barrier retry,
  and archive reuse selectors with `rhiza_local_testhooks`, normal and race.
- `CGO_ENABLED=1 go test -p 2 -race ./...` (including embedded Go and recovery).
- Existing operator fixture/read-barrier regression scripts and all 12 Kind
  fencer unit tests.
- Candidate-built server on private loopback ports: actual `TestSQLServer`
  E2E, not a skipped unit-test invocation. Its server and temporary data were
  removed and absence verified afterward.
- `govulncheck@v1.7.0`: exit 0; zero reachable vulnerabilities, zero imported
  package vulnerabilities, one required-module vulnerability not called by
  this code. This warning is retained separately from functional results.

The absent historical `e2e-fast.sh` invocation has exit 127, not a pass. An
interrupted earlier candidate race invocation and intermediate red tests are
retained separately; final source results above do not overwrite them.

The earlier candidate's local Kind integration did not pass. Its initial environment attempt failed
before workload creation because Dory exported the API wildcard bind address
`0.0.0.0`, which was absent from its TLS certificate. Its cluster was cleaned
up. The corrected attempt uses only its private kubeconfig's published loopback
endpoint, retains TLS verification, and passed authenticated node discovery
before starting the fixture. A second environment issue was the fixture's
Linux-host webhook callback assumption: Dory's gateway could not reach a
webhook on macOS. The unchanged fixture was then run in a disposable Linux
host-network container; no shared configuration or source was changed.

That actual runtime run passed intact-WAL restart, fence RBAC and immutability,
executor-outage restart, and real termination/recreation/storage fencing. It
then failed the existing failed-learner abort/retry assertion:
`automatic_recovery.py:163`, `expectedAbortSlot > 0`. After a pending learner
was lost, the parent escalated to generation recovery before completing its
retirement handshake. The original online addition retained abort slot zero.
At that point fresh quorum probing and retirement scheduling required correction
and another verification pass; the earlier results did not establish completion.
The fixture assertion was not weakened or retried unchanged.

All attempts preserve actual failures and source comparisons. Owned runner
and Kind containers, candidate tags and temporary build/config were removed;
cluster delete returned exit 0 and no owned cluster containers remained.
Pre-existing shared containers were untouched.

## Ordering correction and final verification

The runtime failure led to a bounded retirement-before-escalation correction,
not a relaxed fixture. A bound failed learner can authorize its exact abort
before an active ReadIndex probe is available; physical fencing proof is still
required before learner identity rotation. A non-mutating wait still assesses
escalation, with one finite retirement allowance per incident rather than a
renewed timer per learner. Authentication, Pod recreation, and conflicting WAL
identity remain hard stops. Actual quorum loss still requires the existing
grace/loss-policy/archive/full-generation-fence path.

The final corrected candidate has 326 Go/module inputs and manifest SHA-256
`045c8b3d9ac00e7facb19429fe999a9c9160156a8c696e7b457d07e32e87b044`.
The new ordering regressions passed normal/race in the implementation worker;
an independent quorum reviewer cleared the final ordering/WAL-guard delta.
The independent verification executor's affected normal/race selectors passed
in 10.554s/10.210s, whole operator race passed in 18.587s, and full Go normal,
vet, builds, vulnerability scan and actual server E2E all returned exit 0.
Every command's before/after source manifest matched. The final delta is
restricted to operator files: unaffected tagged and full-race results above
remain prior-candidate evidence, not newly executed all-package race results.
No new final-source complete all-package race run or blanket CI pass is claimed.

The SAME unchanged Go embedded automatic-recovery fixture then finished with
actual exit 0 and `PASS`, seven unattended scenarios, in a fresh disposable
local Kind cluster. The previously failing learner path now recorded
`Aborted`, physical learner-fence proof, one fresh learner identity and
`abort_slot=336`, then verified every acknowledged row through a linearizable
read. Subsequent live-process UDP partition required a fresh generation-wide
fence. The restored generation recovered all 528 acknowledged rows with
`missing=[]`; a new write and deduplication preservation both passed.

Runtime: Go1.27.2, linux/arm64 candidate images on Docker29.6.1, Kind0.31.0,
Kubernetes1.35.0. Local Kind differs from the hosted CI's pinned Kind0.32.0.
Candidate image IDs:

- Go host: `sha256:67a75b62590134c14695c3bb9f043c10f05f7a237e789bbf55a76c1d10c3ebf6`.
- Operator: `sha256:a23f69e07adb537a8eb86acde4cf0c7ef16bd37a7c035a3ba28090a86b4034f0`.

Runner/cluster removal completed, owned candidate image tags and temporary
data/configuration were removed, and no owned cluster containers remained.
Only pre-existing shared containers and reusable caches were left untouched.
No cloud actions, publishing, merge or release occurred. This is local
functional/fencing-boundary evidence, not real GCS/Chaos release qualification,
performance qualification or universal production-fencer validation.
