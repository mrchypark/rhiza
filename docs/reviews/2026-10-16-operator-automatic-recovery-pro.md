# Operator automatic recovery: Pro review original

- Repository: https://github.com/mrchypark/rhiza
- Baseline: `73a75e1ee0c3a07bd1e15384fdf30c420e24fc28`
- Reproduction and correction evidence: [verification](2026-10-16-operator-automatic-recovery-verification.md).
- Source: https://chatgpt.com/g/g-p-69ecdc42175c819186cf485b225c0e46-codex-request/c/6acf6f0a-96d4-83e8-a6d4-5f411534b8e0
- Reviewer: ChatGPT, user-requested Pro consultation; exact model identifier not exposed in the captured response.
- Source UI timestamp: Wednesday 9:01 PM, exact response timestamp not exposed; captured 2026-10-16 Asia/Seoul.
- Preservation: complete response text transcribed from the visible browser accessibility representation; Markdown formatting reconstructed. The review's claims are external static-analysis claims, not independently reproduced results. Verification and fix mapping belong in the companion verification document. No GitHub issues have been created for this review.

## Original response

## Review: `73a75e1ee0c3a07bd1e15384fdf30c420e24fc28`

Four P1 correctness/liveness defects follow from the pinned code. I would not qualify this commit for unattended production recovery until those interleavings are covered and fixed. I did not establish a P0 certified-data-loss interleaving; the broader safety consequences of delayed effects depend on the production fencing implementation. This was static source inspection. I did not execute repository tests, the Chaos harness, or GCS qualification.

## Confirmed defects

### [P1] Learner rollover can leave the child permanently bound to the previous learner’s add journal

**Location:** `pkg/operator/automatic_execution.go:49–62,107–129`; `pkg/operator/automatic_learner.go:106–124`; `pkg/operator/membership.go:167–176`.

**Issue:** The parent reads the child, performs fencing, and then decides whether an addition needs aborting using that earlier child snapshot. When the snapshot has no `Membership.Add`, it advances to a fresh learner without aborting. The child can independently persist an add journal during the fencing call. The subsequent child specification revision preserves that now-incompatible status journal.

**Why It Matters:** A concrete concurrent-controller interleaving is:

1. Parent A reads child status with `Add == nil`. Its learner-unavailability grace has expired.
2. Child controller B has passed authorization and obtains a valid response from the learner during a transient recovery in connectivity.
3. A starts fencing that learner. Before fencing completes, B persists `Membership.Add` for that learner.
4. A receives a valid fence proof. Its old snapshot still says `Add == nil`, so it increments `LearnerAttempt` and changes `LearnerName`.
5. Subsequent reconciles provision the new learner and revise the child spec. The child still contains the old learner’s add journal.
6. `reconcileMembershipAdd` rejects the new member against that journal. The new learner is healthy, so the parent does not initiate another learner abort; the surviving quorum remains available, so escalation does not resolve the incident.

The parent’s object-store CAS can succeed because B changed a different journal in Kubernetes. No fencing-provider violation is needed for this failure.

**Recommendation:** Introduce a candidate-retirement handshake before changing `LearnerName`. Reserve the retirement/abort revision, apply it to the exact child UID with a resource-version precondition, and reconcile the old addition’s terminal outcome. A fresh read after fencing alone is insufficient: an already-running child can still write or dispatch after that read. Use the child revision change to reject delayed status writes from the previous revision, and retain the membership operation’s configuration/abort-slot guards for already-dispatched mutations. Advance the learner identity only after the old round is durably classified as not dispatched, aborted, or committed.

**Verification:** Add a deterministic test that pauses the fake fencer after the parent’s initial child GET. Let a second controller persist `Membership.Add`, then complete fencing. Assert that the parent does not authorize a different learner while retaining the first learner’s add journal. Restart both controllers at each retirement boundary.

### [P1] A learner that commits promotion and then fails before completion is recorded cannot be recovered by the abort path

**Location:** `pkg/operator/membership.go:148–165,187–196`; `pkg/operator/automatic_learner.go:30–39,112–117`; `pkg/operator/automatic_execution.go:57–65`.

**Issue:** Promotion completion requires a live, matching response from the learner. When that response becomes unavailable, the parent fences the learner and selects abort whenever an add journal exists. However, `abortMembership` rejects an addition whose configuration has already committed, because the current configuration is no longer `Add.ExpectedConfigID`. There is no transition that resolves “the addition committed, but the promoted voter is now unavailable.”

**Why It Matters:** This requires only a crash window, not concurrent parent controllers:

- The original voter A has been removed. B and C commit the addition of learner L, advancing configuration `k` to `k+1`.
- Before the child records `Complete`, L loses its process/WAL.
- B and C remain a functioning majority of `{B,C,L}`.
- The parent fences L and requests an addition abort.
- The child rejects that abort because `k+1 != k`. The parent waits for `Aborted`, which cannot be produced by this path.
- Escalation continues to see B and C as a surviving quorum.

The incident remains stuck despite sufficient live quorum to remove the now-failed L and perform another replacement. The escalation and abort branches do not provide an exit for this state.

**Recommendation:** Resolve the old addition’s authoritative outcome before deciding to abort it. A committed addition is not an abortable pending addition. When an authenticated surviving quorum confirms that the exact journaled member committed, record that outcome even if the member is currently unavailable. Then handle its loss as a failed voter, using a new immutable removal operation and the appropriate fence. Do not “repair” the old abort request by changing its expected configuration or clearing the add journal without proving the outcome.

**Verification:** Commit the add, suppress the child’s completion write, and terminate the promoted learner. Restart the operator with the other two voters healthy. Assert that it neither retries the obsolete abort indefinitely nor reuses the failed identity, and eventually completes a correctly fenced replacement.

### [P1] Final completion can trap the controller on an already-finished intent after a new target failure

**Location:** `pkg/operator/automatic_execution.go:57–65,135–173`; `pkg/operator/automatic.go:248–258`.

**Issue:** Once the child reports `Complete`, learner handling and online escalation are skipped. The parent updates the active generation or voter inventory only in memory, then requires another successful quorum observation before persisting the update and clearing the intent. If that observation cannot succeed, the durable state remains on the old intent indefinitely.

**Why It Matters:** Consider generation recovery:

1. The successor generation becomes writable and the child records `Complete`.
2. Before the parent’s final control-record CAS, two target Pods lose their `emptyDir` state.
3. The target needs another generation recovery.
4. The parent repeatedly processes the completed old child and fails final quorum verification. It cannot admit a new incident because `state.Intent` remains non-nil.

The same class of failure exists after online replacement. A restart does not repair it: it reloads the same nonterminal parent state. In the generation case, recovery must now operate from the target generation, which may already contain additional acknowledged data—not from the previously sealed source.

**Recommendation:** Separate installed/committed outcome from currently healthy. Persist a verified installation or membership-commit outcome as its own transition, retaining its generation, membership, child identity, and recovery evidence. Continue health verification afterward. An installed-but-unhealthy state must have an explicit recovery path targeting the installed generation or committed membership. Do not simply remove the final probe or treat an arbitrary Kubernetes `Complete` string as sufficient authority. Preserve evidence verification; stop requiring current health merely to remember which irreversible transition already happened.

**Verification:** Pause immediately after the child’s completion status write, before the parent CAS. Destroy two target WALs and restart the operator. Assert that it records the installed target and can subsequently recover that target, without reopening or reforking the old source. Also test a temporary probe outage followed by restoration.

### [P1] Escalation treats matching local membership snapshots as evidence of a functioning quorum

**Location:** `pkg/operator/automatic_escalation.go:29–70`; `pkg/operator/membership.go:322–344`; compare `pkg/operator/automatic_probe.go:17–42`.

**Issue:** `maybeEscalateAutomatic` calls `/membership/status` and feeds the responses to `membershipQuorum`. That helper counts matching voter/configuration snapshots. A successful result clears or suppresses `NoQuorumSince`. The server’s membership status is a local observational snapshot. It does not establish current consensus connectivity. By contrast, the recovery probe binds a nonce to ReadIndex-backed status and can explicitly exclude the failed voter.

**Why It Matters:** Start an online replacement while B and C can communicate. Then partition their peer transport while preserving both operator-to-node HTTP paths. Both continue reporting matching membership and `Voting=true`. Escalation regards that as quorum indefinitely, while membership changes cannot make progress. There is also a converse inventory problem: the escalation loop queries `intent.Voters`, excluding the original failed voter, rather than a newly promoted learner. During an unrecorded promotion-completion window, a real quorum involving that learner can be omitted from the observation.

**Recommendation:** Use authenticated active quorum evidence to reset the no-quorum timer. Keep membership-status agreement as an identity/configuration consistency check, not the liveness predicate. Probe against the appropriate current membership and exclude the failed identity where applicable. Probe failure still means “no verified reachable quorum,” not “processes are dead”; preserve grace, archive preflight, and generation-wide fencing before takeover.

**Verification:** Keep two membership-status endpoints responsive and consistent while every active quorum probe fails. Assert that the grace timer advances and generation-wide fencing is eventually reserved. Conversely, a valid active probe must prevent escalation, including when its quorum involves a newly committed replacement.

## Authorization across later external operations

The entry check is not sufficient to revoke in-flight execution.

`reconcile` calls `authorizeAutomaticChild` once, then enters the membership or generation executor. Membership dispatch later sends a normal management request carrying the cluster/configuration/operation fields, not a live automatic-intent revision checked against `control.json`. The concrete interleaving is:

Child passes authorization under revision R → pauses before its HTTP mutation → parent CAS supersedes R → child resumes and dispatches.

The parent CAS does not itself change the membership configuration or abort slot. Therefore those data-plane preconditions can still match when the delayed request arrives. Similarly, parent-side fencing and learner provisioning happen before their subsequent control-record CAS; losing that CAS does not undo the external effect. This establishes stale dispatch, not automatically split brain or data loss. Exact generation/incarnation scoping, immutable operation identity, membership guards, and a complete generation fence can make some delayed effects harmless. Finding 1 demonstrates a harmful journal interleaving even with a correct fencer. The minimum design requirement is to choose and enforce one of two contracts:

- Strong revocation: the destination rejects superseded execution epochs, or execution is demonstrably quiesced before supersession completes.
- Safe completion of old work: delayed operations remain strictly scoped and idempotent, and the state machine resolves their possible outcomes before moving on.

A second authorization GET immediately before dispatch narrows the race but does not close it. Leader election also cannot substitute for either contract.

## Crash-window assessment

**Child creation before UID/spec-hash reservation:** No independent defect established for an ordinary restart. The deterministic child can be found again, but cannot dispatch until its exact UID and specification are recorded remotely. If its spec differs, authorization does not silently accept it. Preserve this separation.

**Spec-hash reservation before child PUT:** The intended restart behavior is fail-closed: the old child spec stops matching the executable hash, and `PreviousSpecHash` permits the controlled update to the desired spec. It must remain an update precondition, not a second executable authorization. This does not cancel an already-running reconcile.

**Learner creation before UID/WAL recording:** Sequential restart can rediscover the learner and bind its identity before membership dispatch. That argument does not cover delayed provisioning across supersession; the generation recreation barrier must reject obsolete creation requests, including learners absent from the recorded target list.

**Fencing/abort and final completion:** Not eventually recoverable in all relevant interleavings, as Findings 1–3 show.

**Deletion of an already-reserved child:** The code deliberately blocks a missing or different child UID rather than recreating execution authority. That is fail-closed, but it is not automatic reconstruction of a lost Kubernetes journal. The deployment documentation also warns against deleting the recovery resource.

## Policy and lifecycle: proven behavior, decisions still required

### Suspension freezes admission, not an existing incident

`state.Intent != nil` is handled before `spec.automatic` and `deletionTimestamp`. The intent also retains its policy snapshot. Thus `spec.automatic=false` stops new incidents but continues an already-reserved one—even one whose fence has not yet completed. This is consistent with the documented “new incidents only” suspension model; it should not be presented as an emergency cancellation control. The controller-wide `Automatic=false` switch behaves differently: it stops parent reconciliation, while annotated children still undergo remote authorization and can execute. These switches need separate documented semantics and regression tests.

### Parent deletion is not equivalent to suspension

The controller installs no parent finalizer in this path and discovers parents by listing Kubernetes `RhizaCluster` resources. With an ordinary resource lacking an externally managed finalizer, deletion removes the object that drives parent progression. Existing child authorization does not require the parent still to exist, while recreating the parent produces a different UID and fails the durable binding check.

**Decision required:** Is deletion intended to drain the operation, permanently abandon it for manual repair, or transfer ownership to a durable recovery worker? The implementation does not provide those as equivalent behaviors. The smallest drain design is a finalizer established before the first destructive intent, with explicit finish-or-handoff behavior. Retain the immutable binding tombstone; do not make deletion/recreation a way to reset authority. Test deletion after fence completion but before child creation, and after child completion but before parent finalization.

### Completion-based limits do not bound failed attempts

`MaxRecoveriesPerDay` and cooldown count completed recoveries. Completions are appended only after final successful verification. They therefore do not limit repeated failed provider calls or incomplete operations. The learner path separately caps candidate attempts at three. That is not evidence of unbounded new-generation churn: one intent remains outstanding, and learner attempts have a separate bound. Define the operational limit precisely. A new immutable fencing operation or new learner incarnation should consume an attempt budget; polling or retrying the same idempotent operation should not necessarily consume another attempt. Failed calls still need bounded backoff and visibility. Do not describe completion counters as an attempt circuit breaker.

## Fencing trust boundary and production qualification

The intended boundary is explicit: the operator verifies proof-to-request binding, while the backend is trusted to establish actual termination, recreation prevention, and storage quiescence. The implementation checks operation/source/binding/request hash and all three completion assertions. Those checks authenticate the relationship between records; they cannot verify physical isolation by themselves. A persisted proof has no expiry or live revalidation in `advanceAutomatic`. That is not inherently a defect if the proof records permanent, durable revocation. Adding a timeout would not make a temporary fence safe. The provider contract must instead establish that the old authority remains unusable after executor restart, credential changes, control-plane recovery, and successor activation. The unresolved production qualification questions are specific:

**Delayed execution:** After generation G is fenced and G+1 is active, can a queued G-era termination or provisioning request affect G+1? Termination must use immutable runtime identity, not resolve a Pod name again. Delayed G-era creation must be rejected.

**Scope completeness:** Does the barrier cover all source-generation authority, including unrecorded learner incarnations, retired voters, supervisors, publishers, and GC actors? `Targets` is an observation list, not a complete authorization inventory.

**Storage quiescence:** What happens to a PUT or DELETE already accepted by storage before credentials are revoked? Source HEAD sealing does not by itself protect every checkpoint or extent from an already-dispatched deletion. Qualification must either prove those operations drained or prove the certified recovery material is protected from them.

**Barrier persistence:** Can deleting a fence CR, restarting the executor, or restoring its state remove the barrier while the operator still holds a valid-looking old proof? These requirements are substantially reflected in the documented execution contract, including permanent recreation blocking and operations already in flight. They still require provider-specific enforcement and adversarial tests. Pod absence, successful DELETE, and failed probes are not fencing evidence. The disposable Chaos executor is not a production shared-cluster implementation, as the repository itself states. The supplied prior GCS qualification does not establish unattended writable-generation recovery under this contract.

## Simplification without weakening authorization or durability

Do not start with a new orchestration framework or a different storage layout. The immediate problem is missing durable outcomes and cross-journal ordering, not too few abstractions. Keep the existing remote CAS control record, but make the lifecycle explicit enough that the following facts cannot be conflated: intent reserved; fence proven; candidate identity bound; child UID/spec revision authorized; old candidate outcome resolved; target installed; target currently healthy. The present lifecycle is inferred from combinations of nullable fields, booleans, child status, and live observations. The two necessary additions are narrow:

**A candidate-retirement outcome.** Persist whether the old addition was never dispatched, aborted, or committed before changing candidate identity. This addresses Findings 1 and 2.

**An installed-but-unverified outcome.** Persist irreversible generation/membership installation independently of current health, with a recovery path against that installed state. This addresses Finding 3. Then centralize the transition guards and effect-result recording rather than adding more ad hoc branches. Derivable names can be reconstructed; authority-bearing UID, WAL identity, request hash, configuration/abort slot, and policy snapshot must remain durably bound. Do not collapse reservation with execution, child creation with authorization, abort request with abort confirmation, or installation with health verification. Do not authorize both old and new spec hashes during revision. Preserve the unique successor reservation, irreversible source seal, certified-prefix verification, credential separation, and fencing-before-successor requirements.

The required reduction is in ambiguous state combinations—not in authorization boundaries.
