# Pro review original — production dead-Pod autohealing

Source: https://chatgpt.com/g/g-p-69ecdc42175c819186cf485b225c0e46-codex-request/c/6acf6f0a-96d4-83e8-a6d4-5f411534b8e0
Reviewer: ChatGPT Pro (model visibly verified in the existing codex-request Chat conversation).
Retrieved: 2026-10-21 KST. Exact response completion time not exposed by the UI.
Baseline SHA: 05bb9aa99a93c24c2e606ef69abac61799c6cf6a.
Local harness-only follow-up candidate: 16a195a26e2e405100e130833766eb8cd230cc2f.
Preservation: complete response Markdown obtained through the response Copy button;
this public display removes trailing whitespace only. The exact raw copy is retained
in the task evidence. Integrity below refers to that raw copy, not this display.
Integrity: raw response normalized to one final newline is 30,188 UTF-8 bytes;
POSIX cksum 1988313602 independently matches the browser clipboard bytes;
SHA-256 95001fa52618f00ec5df780b3adbfe4fde1c7f31946352c2699894a20bef11dc.
The following is the reviewer's original text, not a locally validated conclusion.
Issue links: pending local reproduction and issue creation; none created yet.

<!-- BEGIN ORIGINAL RESPONSE -->
## Recommendation

**A Kubernetes node/CRI provider can be the portable default for dead-Pod healing on explicitly supported, reachable Linux nodes. A node/CRI provider plus an object-store quiet-window check is not sufficient.** The missing storage guarantee is independent of the termination provider; adding GCP VM power control would not fix it.

The smallest defensible implementation should preserve the existing recovery state machine and add three capabilities:

1. **A shipped, narrowly scoped runtime fencing service**, with runtime identity captured before writer admission.
2. **Durable storage-mutation accounting outside the database process’s failure domain**, with exact settlement of admitted physical requests—not inference from an unchanged bucket.
3. **Composable terminal evidence for historical runs**, so successful independent voter fencing remains usable during later generation recovery.

This is more than implementing `FenceAttestor.Attest`. The baseline only wires an optional HTTPS client, and `independentFence` returns `Pending` without it. The existing dead-Pod harness explicitly requires no proof and no child; it therefore cannot qualify healing completion. `cmd/rhiza-fencer/main.go:92–125`, `pkg/operator/fencing_executor.go:547–581`, `e2e/chaos/votercount_gcs_chaos.py:616–684`.

All Rhiza paths below refer to `05bb9aa99a93c24c2e606ef69abac61799c6cf6a`. Findings concern that baseline and the proposed provider design, not unrelated earlier reviews.

## Production blockers in the current contract

### [P0] A stable quiet window cannot authorize `StorageQuiesced`

**Location:** `pkg/operator/fencing_attestor.go:17–21,299–328`; `pkg/operator/fencing.go:148–183`.

**Issue:** The independent storage validator accepts matching before/after object observations, a matching inventory digest, and an elapsed window of at least 30 seconds. That establishes an observation about completed mutations during the window. It does not establish that no previously accepted request can complete afterward.

**Reachable interleaving:**

```text
T0: A physical object-store mutation is dispatched; its result becomes unknown.
T1: The node agent terminates the exact database runtime.
T2: The attestor reads storage state S.
T3: More than 30 seconds later, it again reads S and issues the proof.
T4: The already-dispatched request completes and changes storage.
```

Strongly consistent reads do not eliminate this interleaving: at T2 and T3, the mutation has not completed. GCS documents strong object consistency, but not a guarantee that client termination or a client-side request timeout cancels server-side work. Access revocation is also eventually consistent. [Google Cloud Documentation](https://docs.cloud.google.com/storage/docs/consistency?utm_source=chatgpt.com)

The current in-process mutation tracker explicitly retains unknown physical attempts across later successful retries and says its drain proof applies only to the live bucket incarnation. An independent quiet-window attestor must not bypass that uncertainty using a weaker predicate. `internal/objstore/mutation_drain.go:73–131`; `internal/objstore/metrics.go:446–513`.

**There is a separate liveness defect:** the storage scope is the entire source generation. During single-voter replacement, healthy survivors should continue acknowledging writes. Those writes can keep changing HEAD and inventory, preventing a generation-wide quiet window indefinitely. A target-voter fence needs evidence about the **closed producers’ outstanding mutations**, not global storage inactivity. `pkg/operator/fencing_executor.go:511–539`.

**Smallest safe fix:** Demote quiet-window observations to diagnostics. Require a durable mutation-barrier receipt that identifies the closed producer set, admitted operations, and the resolution of every outstanding physical attempt.

This is a blocker for the proposed production completion path. It is **not** a claim that the unconfigured baseline already emits unsafe independent proofs.

**Deterministic regression:** Hold a fake provider’s accepted mutation past both snapshots, terminate the runtime, and attempt proof issuance. It must remain unauthorized until that attempt has a verified terminal result or a storage-enforced proof that it cannot subsequently have an effect. Separately, continuously advance survivor writes and verify that a single-voter fence can still complete.

### [P1] The runtime contract does not bind each historical run to its execution incarnation

**Location:** `pkg/operator/fencing.go:24–50,134–146,202–207`; `pkg/operator/fencing_attestor.go:161–192,241–295`; `pkg/fencing/protocol.go:14–21`.

**Issue:** `capturedFenceIdentity` records one observed container/image pair and node name. It does not populate `NodeUID`. `AttestationRun` contains no container, sandbox, runtime epoch, or boot identity. The validator groups all runs by Pod UID and requires their evidence to match one captured container ID.

**Why it matters:** A Pod UID can survive multiple container attempts. Evidence for the most recently observed container does not, by itself, account for every earlier run. A Pod can also disappear before an operator polling cycle captures its runtime fields.

A future provider must not reinterpret “all these run IDs were supplied in the response” as proof that it independently identified and terminated all their execution incarnations.

**Smallest safe fix:** Before writer admission, durably bind each run to:

```text
cluster/authority identity
namespace UID, root binding UID, StatefulSet UID
source generation, NodeID, Pod UID, RunID, WAL identity
Node UID, trusted machine identity, boot identity
runtime identity, sandbox ID, container ID, image identity
```

Runtime discovery must come from the authenticated node agent and runtime, cross-checked against Kubernetes ownership—not from an application-supplied container ID or a post-fault lookup by name.

**Deterministic regression:** Create two run records for one Pod UID but different containers. Termination of the second must not cover the first. Also test node-name reuse, a Pod disappearing before its first status poll, and runtime metadata garbage collection.

### [P1] Independently fenced retired voters cannot currently be composed into a later generation proof

**Location:** `pkg/operator/automatic_execution.go:159–183`; `pkg/operator/automatic.go:974–994`; `pkg/operator/fencing_attestor.go:172–186`; `pkg/fencing/ledger.go:16–22,217–261`.

**Issue:** Online completion removes the failed voter from `state.Voters` and deletes its current identity. Generation requests are subsequently built from current voters and learner history. Meanwhile, the fencing ledger retains the failed voter’s historical run, without a field referencing its independent terminal evidence. `buildIndependentScope` requires every run in the generation ledger to match a current request target.

**Reachable interleaving:**

```text
Original membership: A, B, C.
A dies; independent fencing completes; L replaces A.
Current membership: B, C, L.
Later, a generation fence is required.
Its current targets contain B, C, L, but the ledger still contains A.
```

A’s run has no fabricated cooperative drain—which is correct. Consequently, cooperative completion cannot cover it. Independent scope construction then rejects A because A is absent from current targets, before the provider can supply reusable evidence.

**Smallest safe fix:** Persist immutable terminal-evidence references for **all retired runs**, including original voters—not just learners. Generation completion must account for each historical run through one of:

- Its genuine cooperative drain/exit.
- Its previously validated independent terminal receipt.
- New independent evidence for that exact run.

Do not set `Drain.Clean()` or pretend the engine generated an exit receipt. Do not discard A’s record.

**Deterministic regression:** Independently fence A, complete its online replacement, restart both controllers, lose a majority of `{B,C,L}`, and complete generation recovery while reusing A’s original immutable terminal evidence.

## Supported default: make the assumptions explicit

The initial default should be a **Linux runtime-fencing profile**, not “all Kubernetes installations.”

Its supported envelope should require trusted Kubernetes administrators and nodes, a qualified CRI runtime, private process namespaces, no application host privileges or escape-capable mounts, every writer enrolled before starting, and management/storage services that survive the database-Pod failures being qualified.

Deploy the privileged agent only to explicitly enrolled dedicated application nodes. One voter per node is appropriate for failure-domain testing; system DaemonSets may exist, but unrelated application workloads must not be termination targets.

CRI provides useful primitives: `StopPodSandbox` requires running sandbox processes to be stopped, and `StopContainer` is idempotent and requires forced termination after its grace period. However, Kubernetes describes CRI as a kubelet/runtime interface, not a general-purpose stable management API. Qualify specific runtime/kernel combinations instead of claiming interchangeable behavior across every CRI implementation. [GitHub](https://github.com/kubernetes/cri-api/blob/master/pkg/apis/runtime/v1/api.proto?utm_source=chatgpt.com)

A node agent with a runtime socket is effectively a host-privileged component. Namespaced RBAC does not constrain what that socket can do. Dedicated-node placement reduces collateral risk; it is not a substitute for authenticating commands and enforcing exact targets.

## Required implementation invariants

### 1. Enrollment and identity capture precede writer authority

The startup order should be:

```text
Supervisor starts without database writers.
Node agent authenticates and captures the exact runtime incarnation.
Authority durably registers that run and runtime binding.
WAL identity is bound through the existing admission protocol.
Storage producer authority is opened for that exact bound run.
Only then may the database start writers.
```

Use the existing durable ledger rather than a new parallel authorization system. Closure racing with registration or WAL binding must either include the admitted run or reject its admission.

There must be no interval in which an independently unidentifiable runtime can write and only later become observable to the operator.

Already-running legacy generations without complete enrollment cannot be retroactively qualified from Pod annotations. They need an explicit migration through an independently verified recovery or a fresh qualified generation.

### 2. Closure must be permanent and enforced at the effect boundary

Keep generation-wide and voter-slot closure distinct. A voter fence closes only that voter’s authority; survivor admission stays open.

The existing ledger rejects registration into closed generations and closed slots, and retains historical runs. Preserve those properties. `pkg/fencing/ledger.go:24–76,79–151`.

An admission check followed by an untracked external operation is not sufficient. Every operation admitted before closure must appear in the closure’s outstanding-work set. Operations arriving afterward must be rejected.

On node return, kubelet recreation, supervisor restart, and replay of an old request, the old generation/slot must remain closed. A cache miss, missing ledger, or lost local agent journal must never recreate open authority.

### 3. Process death must cover the actual execution scope

The runtime agent should accept a narrow command such as “terminate these enrolled runtime incarnations for this closed operation,” not arbitrary shell commands, PIDs, paths, node names, or caller-selected runtime sockets.

For each target it must:

- Verify immutable enrollment and node/runtime identity.
- Stop the exact container/sandbox.
- Establish that its execution scope and descendants have no live processes.
- Persist the terminal result before reporting success.

A Linux cgroup-v2 profile can use the captured cgroup identity and recursive populated state as additional confirmation. The kernel documents that `cgroup.events` reports whether the cgroup and descendants contain live processes. This is meaningful only if the application cannot escape that hierarchy and the agent is inspecting the correct host/runtime. [cdn.kernel.org](https://cdn.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html?utm_source=chatgpt.com)

A CRI timeout is uncertainty. `NotFound` without trustworthy prior capture and retirement evidence is uncertainty. Node deletion, Node `NotReady`, and Pod API absence are not termination. Kubernetes explicitly warns that a force-deleted Pod’s processes may continue running. [Kubernetes](https://kubernetes.io/docs/reference/kubectl/generated/kubectl_delete/?utm_source=chatgpt.com)

For a trusted reboot of the same physical/VM instance, a new boot identity may provide stronger evidence, but a newly created Node object with the same name does not prove the old machine stopped.

### 4. Storage evidence must survive database death

**Recommended smallest complete approach:** add a narrow mutation authority behind Rhiza’s existing object-store abstraction. Run it outside the application container and outside the database nodes that optional power fencing may terminate.

It need not be a generic storage proxy. It should implement only the storage operations Rhiza requires, with server-derived bucket/prefix authorization and bounded payloads. No arbitrary upstream endpoint, arbitrary bucket, or arbitrary metadata API should be accepted.

The authority should durably record:

```text
producer run and generation
operation/attempt identity
object key, operation kind, payload digest
fixed preconditions and provider session identity
admission sequence
terminal result or unresolved outcome
```

Admission and producer closure must be serialized against the same durable state. After closure, the authority completes or resolves the already admitted operations while rejecting new ones.

For a voter fence, settle only the closed voter’s producers. For generation fencing, settle all source-generation producers. Survivor traffic must not block a voter proof.

Application processes must not retain a bypass write path. The mutation authority owns mutating storage credentials; applications can use appropriately scoped read access or routed reads. Include archive publication, checkpoints, pins/leases, GC, deletes, multipart/resumable upload finalization, and startup provisioning—not just foreground `Upload`.

The current bucket wrapper already provides a useful interception point, but its tracker is memory-only, and `Delete` delegates without an exposed generation precondition. `internal/objstore/metrics.go:430–442`; `mutation_drain.go:73–131`.

**A broker is not magically sufficient:** it must preserve admitted work across its own restart, and a replacement worker must not race an old worker into a second uncontrolled mutation. Leadership leases alone do not solve that external-effect race.

### 5. Resolve ambiguity with operation semantics, never elapsed silence

A live mutation authority can finish reading a provider response after its database client dies. This is the common completion path that should make dead-Pod healing work under load.

For lost responses or authority restart, implement resolvers only where the storage operation has sufficient semantics. Examples include exact-version conditional deletion and fixed-precondition publication with verifiable operation identity/content.

GCS documents generation-match preconditions for precisely these stale-request and retry hazards. An old conditional delete must not be able to delete a recreated object. Retrying with a newly read precondition would be a new operation, not resolution of the old one. [Google Cloud Documentation](https://docs.cloud.google.com/storage/docs/request-preconditions?utm_source=chatgpt.com)

Provider contracts differ. Do not treat an ETag as a universally non-reusable version, or treat a current content match as proof that every earlier attempt is unable to act later. Multipart and resumable sessions must be accounted for explicitly; incomplete GCS resumable sessions can remain active without a completed object appearing in the bucket. [Google Cloud Documentation](https://docs.cloud.google.com/storage/docs/resumable-uploads?utm_source=chatgpt.com)

Keep original unknown-attempt records. A later resolution is additional evidence explaining why the attempt is terminal or cannot have another effect; it is not permission to erase uncertainty because another retry succeeded.

If an operation cannot be resolved, the result remains blocked. That is a real unsupported failure condition—not a successful healing test. **The advertised supported path must nevertheless complete ordinary dead-Pod and majority-loss cases with admitted writes in progress**, rather than qualifying only idle crashes.

### 6. Preserve before-ack durability and certified recovery

A mutation-authority response must not acknowledge success merely because work was queued. Any upload on the before-ack path must return success only after the existing storage durability condition and the required durable outcome record are satisfied.

Retain the existing certified archive, source seal, successor reservation, credential rotation, and recovery verification. Do not recover an older convenient prefix to avoid unresolved work. Preserve the existing async `allowDataLoss` policy.

The operator’s no-child-before-proof ordering is already explicit in `advanceAutomatic`; do not bypass it by moving speculative recovery into the attestor. `pkg/operator/automatic_execution.go:18–45`.

### 7. Restart, replay, and timeout behavior must be monotonic

Use a stable execution key based on cluster identity, operation ID, request hash, and target incarnation set. Separate it from the **observation snapshot key** used for an attestation response.

Currently `attestationKey` includes the whole request snapshot. That is reasonable for avoiding stale response reuse, but a changing ledger/inventory snapshot must not cause a provider to launch a fresh destructive job against newly resolved names. `pkg/operator/fencing_attestor.go:77–86`.

Persist terminal receipts immutably before referencing them. A lost response must be resolved by reading the same operation, not by generating another identity.

Every external call needs a bounded timeout and retry policy. A timeout changes operational status, not truth. It must never set `terminated`, clear an unknown mutation, expire a recreation barrier, or authorize a child.

Use explicit reasons such as `RuntimeUnreachable`, `HistoricalRuntimeUnknown`, `StorageAttemptUnresolved`, and `IdentityMismatch`. A long-running `Pending` may be safe, but is not a healing qualification result.

## Minimal response-contract changes

Introduce an independent-evidence v2. Keep cooperative evidence unchanged.

| Change | Minimum required content |
|---|---|
| **Per-run runtime binding** | Trusted node/machine/boot identity and sandbox/container identity attached before admission; no grouping all runs under one latest container |
| **Closure identity** | Immutable closed-scope digest/revision, producer/run set, operation and request hash |
| **Storage barrier evidence** | Exact store scope; closed producer set; admitted and settled sequence coverage; unresolved-attempt count; immutable resolution-journal digest |
| **Historical coverage** | References to prior validated cooperative or independent terminal receipts for retired runs |
| **Provider capability** | Finite supported mechanism/profile identifiers, rather than accepting arbitrary nonempty mode strings as meaningful evidence |

The authority should derive `ProcessesTerminated`, `RecreationBlocked`, and `StorageQuiesced` from validated evidence. Quiet-window snapshots can remain attached as diagnostics but cannot satisfy the storage predicate.

Avoid a self-referential hash design: persist the original closed-scope snapshot, then immutable receipts, then receipt references. Adding a receipt to the ledger must not invalidate the very closure hash the receipt proves.

If a storage mechanism only proves **future stale effects are impossible**, rather than physical completion of every request, encode that distinction explicitly and define its equivalence to the required safety property. Do not silently relabel it “clean drain.”

The v1 quiet-only independent path should not remain an alternate production authorization route after v2 is introduced. Unsupported evidence versions and incomplete capabilities must fail before provisioning a workload that advertises independent dead-Pod healing.

## Minimum repository surface

These are proposed additions, not existing commands.

| Component | Small implementation boundary |
|---|---|
| **`rhiza-fencer node-agent`** | Linux CRI client, runtime enrollment, exact stop/verify, durable terminal receipts; no cloud power API and no object-store credentials |
| **Built-in runtime attestor in `rhiza-fencer serve`** | Implements the existing attestor seam using enrolled agents, closure verification, historical receipt composition, and storage-barrier verification |
| **`rhiza-fencer mutation-service`** | Narrow storage-mutation API, durable admission/attempt journal, provider settlement, bounded staging and backpressure |
| **`pkg/fencing` changes** | Per-run runtime binding and immutable independent terminal-evidence references |
| **`internal/objstore` adapter** | Routes all relevant mutations through the authority without silent direct-write fallback |
| **Operator/CRD changes** | Evidence v2, capability/readiness reporting, bounded evidence transport, immutable receipt references |
| **Manifests** | Dedicated-node agent DaemonSet; unprivileged mutation service; narrow RBAC; TLS/audience trust; workload enrollment and scheduling profile |

Do not put per-attempt payloads or an unbounded history into the existing single ConfigMap ledger. Keep the control ledger bounded, with immutable journal references and admission backpressure. Bound outstanding operations and evidence bytes **before** admitting more work.

Preserve the HTTPS seam for optional infrastructure power providers. Personal project IDs, node-pool names, bucket names, credentials, and test assets remain external configuration.

A missing runtime agent or unsupported storage profile should produce a capability failure during deployment/preflight—not silently fall back to cooperative-only while advertising dead-Pod autohealing.

## Acceptance tests that qualify completion

### Deterministic tests

These tests should precede live destructive testing.

| Test | Required outcome |
|---|---|
| **Closure races startup/bind** | Run is included or admission fails; no unaccounted writer |
| **Same Pod, multiple runtime attempts** | Every historical run is covered; latest-container termination alone fails |
| **Late storage mutation beyond quiet window** | No proof from unchanged snapshots; subsequent settlement or enforced rejection is required |
| **Survivors continuously write** | Single-voter fencing and replacement complete without generation-wide silence |
| **Conditional retry/delete replay** | Delayed old attempt cannot overwrite/delete a newer version; no changed-precondition “retry” |
| **Headers received, body truncated** | Attempt remains unresolved; no terminal success inferred from headers |
| **Crash at every receipt boundary** | Same operation resumes; no lost terminal result, reopened barrier, or duplicate destructive effect |
| **Independent voter → generation composition** | Retired voter evidence remains usable after controller/attestor restart |
| **Wrong node/UID/boot/container** | Zero termination commands against the mismatched target |
| **Stale worker returns** | Old agent/store worker cannot affect successor runtime or storage authority |

Also simulate a runtime acknowledging a stop while a descendant remains alive. Proof must wait for the qualified terminal condition rather than trust a superficial status string.

### Kind qualification

Use the actual shipped node agent, authority, mutation service, supervisor, and operator. The test driver must not write successful proof status, clear ledgers, or restart the object store to manufacture quiescence.

Required positive cases are:

**D1 — bound container/supervisor hard kill:** keep foreground writes and background storage activity running. Require an independent proof, automatic learner promotion, matching parent/control inventory, preserved acknowledged data, and resumed writes.

**D2 — UID-specific Pod deletion and WAL loss:** preserve pre-fault runtime enrollment. Require automatic fencing and replacement despite the original Pod disappearing. Recreated old-slot Pods must not regain old writer authority.

**D3 — fault during an intent:** inject at deterministic barriers before proof, during learner admission/promotion, and after target installation. Assertions must follow the exact affected operation and incarnation; an old valid proof is not invalidated merely because a later learner fails.

**D4 — two of three voters lost:** require full generation recovery and new-generation writes—not merely a generation fence request.

**Composed failure:** independently replace a dead original voter, then lose a majority of the resulting membership. This is mandatory to catch the historical-evidence defect above.

**Return test:** partition or suspend the old runtime before API removal, then allow the agent to fence it. Restore connectivity and verify it cannot resume. Separately launch an attempted old-generation restart with preserved old configuration/WAL and require rejection before writer admission. These test different forms of “return.”

An unreachable node agent without a stronger provider is a negative test. Its safe block must not increment the dead-Pod completion pass count.

### Real GCS + Chaos qualification

Run the same shipped components on explicitly authorized dedicated test nodes, with management components outside the terminated failure domains. Keep checkpoints and GC enabled in at least one completion case.

Record acknowledged request IDs and values outside the database failure domain. After recovery, read back every acknowledged before-ack mutation through the linearizable API, verify duplicates according to the existing request-ID contract, and perform new writes. Do not count unacknowledged or unknown client requests as guaranteed lost or guaranteed committed.

Include a GCS mutation-boundary case with controlled response loss, not just an arbitrary network partition. Its evidence must demonstrate the mutation authority’s durable resolution path.

Use fresh per-run namespace/generation/prefix identities, immutable image digests, finite resource and retry limits, and allowlisted evidence. The KRW10,000 ceiling does not authorize shared-node power tests or unrelated workload disruption. Prefer small sequential three-voter runs; test 64-entry bounds deterministically rather than launching 64 cloud voters.

A required completion case that times out is a failed qualification. Preserve safe-blocking evidence separately, but do not rename the result a success.

## Unavoidable boundaries

These are limits of available evidence, not reasons to abandon the implementation.

**Unreachable node:** Without a trusted independent mechanism, a partitioned live machine and a dead machine can look identical. CRI cannot certify a host it cannot reach. Optional cloud/BMC power fencing addresses that boundary, provided it targets the captured immutable machine identity and prevents its return. It still needs the storage barrier.

**Unresolved external operation:** No finite sequence of unchanged reads proves that an arbitrarily delayed accepted mutation cannot execute. Completion requires a terminal result, enforceable version/epoch exclusion, or a stronger storage mechanism—not a longer quiet interval.

**Unenrolled history:** A new service cannot reconstruct complete old runtime and mutation authority from current Kubernetes absence. Existing generations need an explicit supported transition.

**Compromised host:** A node-root agent is not an independent trust boundary against a compromised kernel/runtime. The initial profile must trust those components and enforce dedicated placement. Use protected scheduling labels where available; taints alone are not a security boundary. [Kubernetes](https://v1-35.docs.kubernetes.io/docs/concepts/configuration/assign-pod-node/?utm_source=chatgpt.com)

Within the supported profile—database processes fail, enrolled runtime agents and management/storage services remain available—**dead-Pod and majority-loss recovery must reach completion automatically**. Outside it, blocking is correct, but must be reported as unsupported or unresolved, not qualified healing.

## Concrete next implementation action

**Start with three failing regressions: delayed storage completion past the quiet window, same-Pod multi-runtime coverage, and independently replaced voter followed by generation recovery. Then implement evidence v2 and the survivor-held mutation journal/barrier, followed by the narrowly scoped CRI node agent and production wiring.**

That is the smallest complete direction that preserves the stated invariants without making a cloud power adapter mandatory. Shipping a CRI service that sets `StorageQuiesced=true` after 30 seconds of unchanged objects would replace the present availability gap with an unsound fencing claim.

I inspected the pinned source paths above. I did not execute tests or inspect live infrastructure; the local source-archive download failed DNS resolution. The proposed service and protocol changes therefore remain implementation recommendations, not validated code.
<!-- END ORIGINAL RESPONSE -->
