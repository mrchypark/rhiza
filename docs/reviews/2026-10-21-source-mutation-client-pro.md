## Decision

**Add an explicit internal mutation client with durable caller-owned retry state. Assign the original sequence in that state after payload staging, before the first `Admit`. Use the existing survivor-held authority as the durable record of admitted work.**

The synchronous return contract can remain. The source mutating call sites need an additional **durable owner-step handle**, and deletes need an **exact selected object version**. The unchanged `Bucket` arguments cannot distinguish a retry from a new operation.

This does **not** require another authority-side sequence-reservation protocol. A source-local fsynced record is sufficient before submission, provided the authority durably retains the identical tuple, intent, owner correlation, and staged-payload reference before granting execution. After source/WAL loss, recovery operates on those externally retained records.

I inspected `183039aeac884d01e98533552cf24a482296755a` and the three preserved review documents. I did not execute tests or qualification. This response does not establish runtime, mutation-path, or production qualification.

## Integration blockers

These findings are blockers to enabling the proposed independent profile.

### [P0] The existing caller boundary cannot preserve logical mutation identity

**Location:**

- File: [`pkg/fencing/mutation_journal.go`](https://github.com/mrchypark/rhiza/blob/183039aeac884d01e98533552cf24a482296755a/pkg/fencing/mutation_journal.go#L31-L37)
- Lines: 31–37
- File: [`pkg/recovery/archive.go`](https://github.com/mrchypark/rhiza/blob/183039aeac884d01e98533552cf24a482296755a/pkg/recovery/archive.go#L938-L1020)
- Lines: 938–1020

**Issue:**

- `MutationIntent` does not bind the original upload condition.
- `syncNowLocked` reloads and rebuilds after any `publishHead` error when CAS is supported. An adapter that allocates a sequence on each call would turn an ambiguous HEAD publication into another operation, potentially with another precondition.

**Why It Matters:**

- The original mutation remains unresolved while a later attempt can succeed. Returning that later success cannot settle or acknowledge the original tuple.
- Identical key and bytes do not make different conditional writes interchangeable.

**Recommendation:**

- Retain an explicit operation handle above the retry loop.
- Bind complete caller semantics into immutable intent.
- Resolve the original handle before replanning. A new CAS attempt requires an explicit new caller decision after the preceding outcome permits it.

**Verification:**

- Drop the response to a committed HEAD publication. Re-enter the outer archive loop and assert that it queries the original tuple without allocating another sequence or refreshing its condition.

### [P0] Bootstrap binding does not deliver the identity needed by the mutation client

**Location:**

- File: [`pkg/fencing/session.go`](https://github.com/mrchypark/rhiza/blob/183039aeac884d01e98533552cf24a482296755a/pkg/fencing/session.go#L53-L66)
- Lines: 53–66
- File: [`pkg/fencing/supervisor.go`](https://github.com/mrchypark/rhiza/blob/183039aeac884d01e98533552cf24a482296755a/pkg/fencing/supervisor.go#L335-L348)
- Lines: 335–348
- File: [`pkg/node/node.go`](https://github.com/mrchypark/rhiza/blob/183039aeac884d01e98533552cf24a482296755a/pkg/node/node.go#L383-L415)
- Lines: 383–415

**Issue:**

- The bind handshake returns no usable producer binding to the application.
- Node construction still accepts provider mutation credentials and constructs a mutable bucket.

**Why It Matters:**

- The client cannot durably name its authoritative producer UID.
- Adding a wrapper would leave a direct-credential bypass.

**Recommendation:**

- Return and validate the root-pinned producer binding through the complete bind handshake.
- Enable the source mutation client only after that validation.
- Enforce a deployment profile in which source processes have no provider mutation authority.

**Verification:**

- Lose the bind response after durable activation: retry must recover the same producer UID.
- Attempt direct provider mutation with the actual source workload identity: it must fail.

### [P0] A delete handle must preserve the selected generation

**Location:**

- File: [`pkg/checkpoint/checkpoint.go`](https://github.com/mrchypark/rhiza/blob/183039aeac884d01e98533552cf24a482296755a/pkg/checkpoint/checkpoint.go#L1434-L1453)
- Lines: 1434–1453; also 1503–1525

**Issue:**

- GC evaluates candidate objects and then calls an unversioned `Delete`.
- Adding an exact-generation condition only when a delayed executor finally runs does not preserve the original selection.

**Why It Matters:**

- GC selects generation G1; the key is replaced with G2; the executor reads G2 and conditionally deletes it. The physical request is conditional, but it deletes an object that was never selected for collection.

**Recommendation:**

- Capture the exact provider generation with the GC selection and retain it in the operation descriptor.
- If a listing lacks that version, obtain and validate attributes while making the selection.
- Never refresh the selected generation while resolving the same delete.

**Verification:**

- Replace the selected object between selection and execution. The original delete must leave the replacement intact.

## 1. Concrete source-client contract

### Use an explicit internal writer interface

Introduce a scoped `MutationClient` used by bootstrap, checkpoint, archive, and maintenance code. Its conceptual operations are:

```go
Upload(ctx, durableStep, key, reader, uploadOptions...) error
Delete(ctx, durableStep, selectedObject) error
Lookup(ctx, originalRef) (MutationResult, error)
```

`durableStep` identifies one caller-owned storage-mutation attempt. `selectedObject` includes the exact store scope, key, and selected version.

The public embedded Go API and upstream Thanos interface need not change. Replace Rhiza’s internal mutating call sites with this client. The source-side bucket’s remaining mutators must fail closed if called outside the contract.

A context extension can carry the handle through compatibility plumbing, but **context is not its persistence mechanism**. A freshly generated UUID inside `Upload`, or a token attached once to the entire Node context, is insufficient. Checkpoint blocks, HEAD publication, lease renewal, and deletion are different mutations.

### Retain this descriptor

Each bound step must durably contain:

| Field | Required meaning |
|---|---|
| Producer UID and sequence | Original authoritative request tuple |
| Structural operation ID | `MutationRequestID(producerUID, sequence)` |
| Owner/workflow/step/attempt | Correlation to the caller state that must resume this operation |
| Contract version and store scope | Exact supported semantics and server-authorized destination |
| Kind and key | Upload or delete against the exact object |
| Original condition | Create-only, exact-match version, or an explicitly supported unconditional mode |
| Upload payload | Immutable stage identity, digest, and length |
| Delete target | Exact version selected by the caller |
| Result state | Pending, original result, and durable evidence reference |

The owner correlation is separate from `OperationID`. The current compaction contract structurally binds that ID to producer UID and sequence; it does not provide arbitrary historical owner-token deduplication. [`mutation_compaction.go:44–55, 75–85`](https://github.com/mrchypark/rhiza/blob/183039aeac884d01e98533552cf24a482296755a/pkg/fencing/mutation_compaction.go#L44-L85)

The same tuple with changed conditions, key, scope, payload identity, digest, length, or delete version must fail before execution.

For an initially unconditional upload, the authority may select a fixed generation once during preparation under the required per-key serialization. That preparation cannot subsequently be refreshed. A conflict requires a new caller decision; it is not permission to reinterpret the old request.

## 2. Exact assignment, submission, and retry sequence

### A. Create or resume the owning step

Before attempting storage work, the caller opens its existing workflow attempt and its specific child step.

Examples:

- A particular checkpoint publication’s third block upload.
- A particular archive publication’s HEAD CAS attempt.
- A particular lease renewal with an already-fixed expiry value.
- A particular GC selection’s deletion.

A repeated call for that step retrieves its saved handle. Two intentional operations with identical bytes receive different step identities.

Use a small fsynced retry-state file bound to the local WAL identity and producer UID, or equivalent records in the existing durable publication state. Existing state is insufficient unchanged; this is a required addition.

### B. Seal upload bytes outside the source Pod

Before consuming a mutation sequence, stream the payload into the authority-owned staging boundary described below.

Persist the staging ticket in the owner step before streaming. A lost staging response is resolved using that same ticket. It must not create another payload identity merely because the response was lost.

This ordering avoids assigning a mutation sequence to an incomplete arbitrary `io.Reader`.

### C. Allocate and fsync the original tuple locally

Under a per-producer admission allocator:

1. Choose the next sequence.
2. Construct the full immutable intent, including the sealed stage reference.
3. Atomically persist the owner-step mapping and original tuple.
4. Complete the required fsync before the first `Admit` submission.

**This is the literal “retained before first submission” boundary.**

Serialize allocation through uncertain admission responses. Once admission of sequence N is known, N+1 may proceed without waiting for N’s provider result. Previously admitted work can execute concurrently within the existing bounds.

Do not reconstruct a previously used producer’s allocator from `LastAdmitted` when its local retry state is missing. Missing local state is a recovery condition, not permission to resume that producer.

### D. Admit the exact descriptor into the existing authority

The authority must durably bind:

- Original tuple and full intent.
- Bounded owner/step correlation.
- Immutable stage attachment.
- Existing journal entry.

Only then may preparation or dispatch authorization follow.

Make the producer CAS the authority for stage attachment. Avoid an independently updated refcount whose success or failure can diverge from admission.

The current `Admit` already requires contiguous sequences and an immutable staged upload reference/digest. Preserve those requirements. [`mutation_journal.go:223–270`](https://github.com/mrchypark/rhiza/blob/183039aeac884d01e98533552cf24a482296755a/pkg/fencing/mutation_journal.go#L223-L270)

### E. Resolve lost responses through the same tuple

After any uncertain submission or result response:

- Read or resubmit the original tuple and complete intent.
- Do not allocate another sequence.
- Do not reread storage and replace the original condition.
- For an archived sequence, perform verified archived-result lookup.
- Never execute an archived request again.

Persist the authoritative result in the owner’s retry state before advancing that owner to another step.

A typed pending error containing the reference is useful, but insufficient by itself: outer code must retain the reference after returning that error.

### F. Consume canceled allocated sequences explicitly

A sequence allocated locally cannot be skipped or reused.

For an operation absent from the authority, add a narrow cancellation operation whose producer CAS performs:

```text
Admit(original sequence, original intent)
CancelUnprepared(original sequence, durable cancellation receipt)
```

Those transitions already exist. Composing them in one CAS consumes the sequence without granting execution. If the entry already exists, cancellation succeeds only while it remains unprepared. Prepared or dispatched work retains its original settlement obligation. [`mutation_journal.go:415–432`](https://github.com/mrchypark/rhiza/blob/183039aeac884d01e98533552cf24a482296755a/pkg/fencing/mutation_journal.go#L415-L432)

If permanent producer closure won first and the request was never admitted, return that definitive state. There will be no subsequent sequence allocation for that closed producer.

## 3. Why source-local retention is sufficient—and its limit

There are two relevant durability boundaries:

**Before admission:** no provider effect is authorized. Losing the local prospective tuple is not loss of an executable obligation.

**After admission:** the authority already retains the original tuple, complete intent, owner correlation, and payload reference outside the source failure domain.

However, “not currently admitted” does not mean “can never arrive.” An `Admit` request may still be in transit when the source disappears.

Producer closure must serialize against admission and stage attachment:

- If admission wins, the exact entry belongs to the closed producer’s obligations.
- If closure wins, the late admission is permanently rejected.

Consequently, a sealed but apparently unattached stage cannot be reclaimed from a journal snapshot alone.

After Pod/WAL loss, the survivor must enumerate the old producer’s accepted entries and staging inventory. A replacement runtime receives its own producer identity and may perform authorized historical lookup. It must not reopen the old producer or reconstruct its unknown operations under new sequences.

The externally retained owner metadata must be sufficient to explain accepted work. Full resumption of an unfinished checkpoint computation is optional: recovery may settle its accepted child mutations and abandon the incomplete higher-level workflow. It must preserve those child outcomes.

## 4. Synchronous return semantics

Use the following mapping at the mutation-client boundary:

| Authority state | Source result |
|---|---|
| Stage sealed or request admitted | Continue waiting; no successful mutation return |
| Attributable, durably recorded `Committed` | `nil` for that exact operation |
| Verified terminal rejection | Typed failure preserving the original reference |
| `FutureEffectExcluded` | Distinct non-success result; do not assert commitment |
| Pending, missing evidence, or transport uncertainty | Typed unknown/pending result preserving the reference |
| Compacted sequence | Verified lookup of the original result |

Cancellation of the caller’s context stops its wait. It does not cancel an accepted provider operation or remove its journal obligation.

A committed result can coexist with unresolved physical copies. Returning the original commitment must not release a zero-generation deletion guard or certify fencing completion. The current journal explicitly separates commitment, copy completion, and dispatch permissions. [`mutation_journal.go:50–86`](https://github.com/mrchypark/rhiza/blob/183039aeac884d01e98533552cf24a482296755a/pkg/fencing/mutation_journal.go#L50-L86)

Also change the ambiguity handling in `uploadExtent`: its existing `EPIPE` path can return success after content readback. Under this contract, matching bytes cannot turn an unknown mutation into `Committed`. Known immutable-content reuse can remain a separately validated workflow decision; it must not settle the original unknown tuple. [`archive.go:1044–1073`](https://github.com/mrchypark/rhiza/blob/183039aeac884d01e98533552cf24a482296755a/pkg/recovery/archive.go#L1044-L1073)

## 5. Callers and state that must change

| Caller family | Required retained state and behavior |
|---|---|
| **Bootstrap — `node/voter_identity.go`** | Separate steps for membership registration and voter registration. Retain exact bytes, create-only condition, original tuple, and result. Existing matching remote bytes validate registration state; they do not prove an uncertain original mutation committed. |
| **Checkpoint orchestration — `checkpoint/auto.go`** | Persist a publication attempt before claim acquisition and file uploads. Retain child handles even if `CreateFiles` or claim binding fails before `a.candidate` is assigned. The next timer invocation must resume or reconcile that attempt. |
| **Publisher claims — `checkpoint/checkpoint.go`** | Acquisition, binding, each renewal, and release need separate steps. Freeze timestamps and original versions. Recreating a lease body after timeout is another operation. |
| **Checkpoint blocks, roots, CURRENT** | Retain per-block handles, root-publication handle, and CURRENT CAS-attempt handle. Include separate handles for the generation-key fallback. A content hash identifies bytes, not the mutation. |
| **Checkpoint recovery pins and GC** | Retain pin creation/renewal/close steps. GC retains its candidate selection, exact object versions, and individual delete handles. |
| **Archive publication — `recovery/archive.go`** | Retain publication attempt, source range/predecessor, exact extent stages, HEAD body/condition, and child handles. Stop `syncNowLocked` from rebuilding over an unknown HEAD operation. |
| **Archive locks, pins, GC markers** | Every acquire/renew/release, marker upload, target delete, and marker delete gets its own step. An owner string such as `"archive-sync"` is not a request identity. |
| **Recovery/seal/fork writers** | Source sealing, fork records, copied objects, and generation-anchor publication must use the same authority boundary when they affect the protected namespace, under the appropriate trusted recovery identity. |

The checkpoint distinction is material: `a.candidate` is assigned only after file creation and claim binding, while earlier cleanup and error paths can already have performed writes. It is not a durable retry record for those earlier mutations. [`auto.go:226–289`](https://github.com/mrchypark/rhiza/blob/183039aeac884d01e98533552cf24a482296755a/pkg/checkpoint/auto.go#L226-L289)

Checkpoint block uploads also run in parallel. Each requires its own handle; a single workflow-wide context token would alias different mutations. [`checkpoint.go:746–820`](https://github.com/mrchypark/rhiza/blob/183039aeac884d01e98533552cf24a482296755a/pkg/checkpoint/checkpoint.go#L746-L820)

Inject the scoped mutation client into the managers. Cleanup paths that create fresh background contexts must still select and use their explicit durable step.

## 6. Bootstrap `BindWAL` handshake

Change the complete path:

**authority bind handler → `AuthorityClient.Bind` → supervisor bind response → `Session.BindWAL` → source mutation-client installation.**

Return a typed binding containing:

- Exact source/node/Pod/run/WAL identity.
- Root-pinned mutation producer UID.
- Contract version and supported execution profile.
- Store-scope binding required by the client.

The authority returns an enabled binding only after the matching root WAL bind and producer activation are durable. Retries complete or retrieve that same binding. A replaced producer UID, missing bound producer, closure, or inconsistent runtime/WAL binding must fail closed.

The source then persists its local retry-state header and enables the mutation client.

Keep the existing bootstrap ordering:

1. Persist local voter identity.
2. Bind local WAL identity.
3. Complete and validate authority binding.
4. Initialize the bound retry-state/client.
5. Perform membership and voter-registration mutations through it.

Those remote uploads already follow `beforeWrite`; there is no need to move them before binding or invent a bootstrap bypass. [`voter_identity.go:164–203`](https://github.com/mrchypark/rhiza/blob/183039aeac884d01e98533552cf24a482296755a/pkg/node/voter_identity.go#L164-L203)

Constructing the bucket earlier is acceptable only if its mutation path remains disabled and it possesses no source-accessible provider write authority.

## 7. Minimal durable staging boundary

**Use an authority-owned persistent spool outside the DB Pod and WAL for the first implementation.**

Its durability must cover authority restart and the deployment’s advertised failure domain. It needs enforced exclusive ownership during recovery; a missing volume or missing previously recorded payload must not initialize an empty replacement.

This avoids making staging uploads themselves another unresolved object-storage upload protocol.

For every staging ticket:

1. Reserve an operation slot and byte budget durably before receiving bytes.
2. Bind the ticket to the producer and owner step.
3. Write a bounded temporary payload.
4. Verify length and digest.
5. Fsync bytes, finalize an immutable identity, and fsync the directory.
6. Persist the sealed descriptor.
7. Attach that descriptor through the producer admission CAS.

No provider preparation may reference a receiving or replaceable payload. A crash between physical finalization and descriptor persistence produces a discoverable orphan, not executable work.

Keep staging metadata bounded and integrated with the existing producer authority. Payload bytes do not belong in its ConfigMap.

The profile must set finite per-payload, per-producer, and authority-wide byte limits, plus limits for receiving tickets, admitted work, and evidence storage. The existing checkpoint block size is 64 MiB, so a smaller payload ceiling would exclude a current write path unless that path changes. [`checkpoint.go:30–33`](https://github.com/mrchypark/rhiza/blob/183039aeac884d01e98533552cf24a482296755a/pkg/checkpoint/checkpoint.go#L30-L33)

Admission pressure must not prevent result lookup, cancellation, producer closure, or cleanup of already-accounted work. The existing 64-entry live bound does not bound lifetime archives.

### Staged-payload deletion rule

For the first implementation, choose the conservative rule:

> An attached payload becomes deletable only after its operation has a durable result, dispatch admission is sealed, every granted executable permission is terminal, and staging writers/finalizers are closed.

For an unattached stage, require cancellation serialized against attachment, or permanent producer closure that excludes late admission. Close and settle its staging writers before physical deletion.

Persist deletion eligibility, delete the exact stage identity, verify completion, and then release quota. A crash after deletion but before quota release may temporarily overcharge; it must never undercharge.

Retain the tuple, full intent, result, receipts, and deletion tombstone after removing bytes.

**Coordinate this with compaction.** The existing terminal predicate permits positive-generation entries to become barrier-terminal without all-copy completion. If staging cleanup uses the stricter rule above, do not compact its payload-owning live entry before cleanup obligations have been durably preserved or completed. The smallest first implementation is to delay that compaction. Otherwise the immutable archive can outlive the only mutable record through which cleanup would complete. [`mutation_journal.go:549–554`](https://github.com/mrchypark/rhiza/blob/183039aeac884d01e98533552cf24a482296755a/pkg/fencing/mutation_journal.go#L549-L554)

That stricter cleanup policy has a liveness cost. Permanent positive-generation exclusion may support a later relaxation; it must be specified separately.

### Destination-object deletion rule

Staging retention does not prevent zero-generation resurrection.

Before granting any zero-generation dispatch right, establish a durable guard on the exact store/key. Every path that can make that name absent must serialize with that guard.

While any such right remains unresolved:

- GC cannot physically delete the destination.
- Another producer cannot bypass that restriction.
- Closure, restart, and archival changes cannot discard the guard.
- Lifecycle deletion or external cleanup cannot be allowed to bypass the deployment profile.

Release the guard only after dispatch admission is sealed and all granted rights have terminal evidence, unless a separately reviewed permanent guard mechanism is implemented.

The current zero-generation rejection and terminal predicate are necessary, but they do not implement this cross-producer deletion rule. [`mutation_journal.go:402–405`](https://github.com/mrchypark/rhiza/blob/183039aeac884d01e98533552cf24a482296755a/pkg/fencing/mutation_journal.go#L402-L405)

## 8. Execution profile and unresolved obligations

The smallest first executor profile is fixed-generation GCS non-resumable execution, with raw mutation credentials held only by the authority.

It must still:

- Preserve the immutable preparation.
- Register dispatch rights before executable permission.
- Prevent hidden SDK or transport resends from escaping accounting.
- Never reuse a settled dispatch ID.
- Persist attributable provider evidence before returning commitment.
- Reject unsupported resumable creation/finalization paths.

Non-resumable execution does not solve a crash after provider acceptance but before receipt persistence. An attributable object may recover commitment evidence; it does not automatically settle every delayed zero-conditioned copy.

The no-bypass deployment must remove source mutation credentials, ambient write privileges, and authority impersonation capability. Read access can remain separately scoped. The current supervised constructor’s provisioning restriction does not establish this boundary. [`provider.go:76–133`](https://github.com/mrchypark/rhiza/blob/183039aeac884d01e98533552cf24a482296755a/internal/objstore/provider.go#L76-L133)

The authenticated runtime agent, durable runtime receipts, provider ambiguity resolution, restart behavior, real credential enforcement, and required live qualification remain outstanding. Safe pending behavior must remain visible where proof is unavailable.

## 9. Concrete failing regressions to add

These are required failure-injection tests, not tests I executed.

| Regression | Required assertion |
|---|---|
| **Bind response lost after activation** | Same pinned producer UID recovered; no bootstrap mutation before receipt validation. |
| **Stage fails before sequence allocation** | No sequence consumed, reused, or skipped; bounded orphan cleanup remains possible. |
| **Admission response lost** | Retry uses the exact original tuple/intent; allocator does not move past uncertainty. |
| **Allocated step canceled** | Same intent is atomically admitted-and-canceled, or its existing prepared obligation remains pending. |
| **Source/WAL loss with delayed admission** | Admission either belongs to the closed obligation set or is permanently rejected; its stage is not deleted during the race. |
| **Lost result followed by compaction** | Original-UID/sequence/full-intent lookup returns the original result without execution. |
| **Identical bytes, distinct intentional operations** | Distinct tuples; retrying either preserves its own tuple. |
| **Same tuple, changed condition/body/scope/version** | Rejected before provider I/O. |
| **Archive HEAD ambiguity** | Outer reload/rebuild does not allocate a replacement operation or refresh its condition. |
| **Checkpoint failure before `a.candidate` assignment** | Next invocation retains/reconciles original child handles. |
| **GC selects G1, object replaced by G2** | Original delete cannot remove G2. |
| **Zero-create A commits; granted B pauses before bytes** | Another producer’s delete remains blocked; restarting authority does not lose the guard. |
| **Stage cancellation versus late finalization/attachment** | Deleted or canceled stage cannot become executable or be recreated under its old identity. |
| **Cleanup crash before quota release** | Restart verifies exact deletion and releases quota once. |
| **Positive-generation result with pending stage cleanup** | Compaction does not lose cleanup/dispatch obligations. |
| **Limits filled with unknown work** | Bounded backpressure; no TTL settlement, stage ACK, or evidence deletion. |
| **Direct source-provider mutation** | Denied by actual deployed credentials, including upload finalization capabilities. |

## 10. Next repository edits, in order

1. **Define the client contract and immutable descriptor.** Extend `MutationIntent` or its bound descriptor with caller conditions, selected delete version, size, scope, and bounded owner correlation. Preserve structural request IDs.

2. **Complete the bind receipt.** Update `protocol.go`, `authority_client.go`, `supervisor.go`, `session.go`, and the operator bind handler. Install the source client only after the receipt and local retry-state header are durable.

3. **Add the WAL-bound source retry state and internal `MutationClient`.** Implement serialized sequence allocation, original-result lookup, durable owner advancement, and atomic admit-and-cancel. Missing retry state must not reopen an old producer.

4. **Implement bounded persistent staging in the existing authority.** Add ticket inventory, sealing, CAS-controlled attachment/cancellation, cleanup tombstones, and compaction coordination.

5. **Wire actual callers.** Start with both bootstrap registrations and archive HEAD retry behavior. Then wire checkpoint claims/blocks/roots/CURRENT, all pins and renewals/releases, and every GC marker/delete path. Carry exact deletion selections.

6. **Complete the effect boundary.** Add the authority-only provider executor, durable per-key zero-generation guards, and enforced credential separation. Then run the regressions through these wired callers before beginning the remaining live qualification.

**The next implementation should establish one complete vertical path: bound producer → sealed stage → fsynced original tuple → durable admission → attributable provider result → original synchronous return. Bootstrap registration is the smallest first path; archive HEAD publication is the next required path because it exercises the existing outer-retry hazard.**
