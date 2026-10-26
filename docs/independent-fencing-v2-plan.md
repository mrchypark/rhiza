# Independent dead-Pod healing implementation plan

Tracking: [issue #246](https://github.com/mrchypark/rhiza/issues/246).
Review: [complete Pro response](reviews/2026-10-21-dead-pod-autohealing-pro.md).
Status: partial implementation. Original red receipts are preserved; the three
authority-level regressions now pass after v2 rejection and actual retained-history
composition were connected to reconciliation using test providers. This document
is not Kubernetes/GCS/Chaos qualification evidence.

## Implemented foundation (not yet wired to writer admission)

`pkg/fencing/runtime_binding.go` retains each runtime incarnation independently.
`pkg/fencing/mutation_journal.go` implements bounded producer admission, closure,
immutable preparation, separate commitment/rejection/exclusion, and additive
all-copy completion receipts. Every receipt hash binds runtime, WAL, sequence
and the fixed prepared descriptor. Create-only commitment alone cannot complete
the barrier while copies may remain. No native metadata PATCH profile is accepted.

`pkg/operator/mutation_journal_store.go` persists pure producer transitions using
ConfigMap resourceVersion CAS and the enrolled root cohort, rather than a list
of currently existing producer objects. A missing bound producer cannot be
recreated as an empty journal. Delayed admission conflicts with durable closure;
response-loss retry resolves the same sequence/operation, not a replacement.
These are private backend methods, not unprotected application endpoints.

Focused normal/race tests cover JSON restart, closure/admission interleavings,
unscheduled obligations, late session/preparation rejection, receipt attribution,
create-zero ambiguity, corrupt/missing state, response loss and bounded conflicts.
The fake API tests do not constitute Kubernetes/GCS qualification or validate
provider receipt authenticity. A fixture-only initial failure used an empty WAL
at activation; it was corrected without relaxing the production WAL requirement.

The live journal retains at most 64 entries and applies backpressure. The pure
compaction contract now snapshots exact terminal prefixes with immutable prior-head
references and producer UID/sequence request identities. Commit rejects overlapping
stale plans, preserves unrelated tail/closure state, and rejects all executable
operations at or below the watermark. A 1,024-cycle JSON reload regression retains
4,096 operations' archive coverage while keeping the live value bounded.
The backend now persists immutable ConfigMap archives, verifies content-addressed
data plus server-assigned UID, and checks the pinned head on ordinary updates.
Full history verification advances in bounded batches with a producer-CAS cursor;
executor restart resumes it, while a new head or fencing operation resets it.
Confirmed archive loss/replacement/corruption is latched; transient API failure is
not automatically permanent corruption. Head-only admission does not detect
arbitrary ancestor loss: each new fence verifies the full retained chain, and
archives must not be deleted outside this protocol.

Compaction response loss is resolved only by finding the exact archive UID/digest
in the committed chain, not merely a larger watermark. Archived-result lookup
requires the original producer UID/sequence and full immutable intent; it preserves
Committed/Rejected/Excluded outcomes and grants no executable admission.
Prepared entries require the trusted provider receipt/session verifier at archive
creation, history verification and final live-entry authorization. Missing verifier
configuration refuses qualification; digest syntax alone is not evidence. The real
provider verifier, payload staging and source-routing integration remain missing,
so this is not yet a sustained-write deployment profile. Retention bounds the live
value, not total archive storage; no automatic evidence deletion was introduced.

Dispatch rights are registered before workers may send requests. All-copy completion
requires sealed permission admission and terminal evidence for every granted right,
including workers paused before sending. This follows the additional counterexample
in [Pro's complete compaction review](reviews/2026-10-21-compaction-pro-followup.md).
The producer object's Kubernetes
UID is now pinned in root enrollment before WAL admission when the new
RequireMutationJournal policy is selected; the backend refuses a replaced object,
including across executor restart. The policy is not yet selected in production.
Wire the
trusted provider worker, durable staged payloads, globally serialized per-key
dispatch and verified all-copy evidence. No storage bypass, terminal-reference,
CRI-runtime termination, or positive dead-Pod recovery qualification claim is made here.

`pkg/fencing/terminal_reference.go` separately preserves immutable per-run terminal
references for the verified evidence-v2 path. References remain bound to the original
voter closure, runtime, producer UID, object UID/digest and storage scope across a
later generation closure. They cannot manufacture a cooperative drain/exit record.
`pkg/operator/terminal_evidence_store.go` now persists and verifies the immutable
artifacts. `pkg/operator/fencing_independent_v2.go` connects retained history and
trusted runtime collection to the actual reconciliation path. It derives every
run from the frozen ledger, including historical runs absent from current targets.
The CRD carries bounded v2 references. The authority and recovery controller reject
v1 authorization; a normal cooperative drain under the new journal profile also
requires settlement of authority-held work. Legacy attestor command settings fail
startup instead of implying a working v2 provider.

The original historical-voter regression now exercises a real fake-API voter proof,
executor restart, continued survivor admission and later generation proof containing
both original references. The collector for the retired runtime deliberately becomes
unreachable; its original immutable receipt is reused and reverified, not recollected
or silently dropped. Same-Pod multi-runtime and stale-inventory regressions are also
covered. All these tests inject test runtime verifiers; the shipped command still
does not configure real CRI/mutation providers. Enforced source routing, runtime
service deployment, bounded history beyond one 64-reference proof, independent
review and live positive qualification remain unfinished.

## Acceptance contract

Keep cooperative evidence intact. Independent completion must include trusted
per-run runtime enrollment, permanent producer closure, durable settlement or
enforceable exclusion of all admitted physical attempts, and immutable terminal
references for historical runs. A quiet storage window is diagnostic only.

Normal hard-killed/removed voters and majority loss must recover automatically
with writes, checkpoints and GC active. Old runtime return must be rejected.
Unreachable infrastructure and unresolved effects are explicit failures or
unsupported capabilities; their Pending state is not a completion pass.

## Dependency order and ownership

1. Reproduce the three findings without cloud credentials. Preserve original
   failures. The parent owns current integration while the requested MiniMax
   route is unavailable; no live implementation/review worker is claimed.
2. Define evidence v2 and the mutation-barrier API. Reject quiet-only independent
   authorization. Do not present the version change alone as healed workloads.
3. Enroll immutable runtime identities before admitting any writers; preserve
   every runtime attempt for the same Pod UID.
4. Record immutable terminal references for retired original voters and learners.
   A later generation fence must compose genuine prior evidence without fabricating
   drain/exit records or deleting history.
5. Implement the survivor-held mutation authority and provider settlement. Route
   every source mutation through it, including metadata, GC/delete and upload
   finalization. No silent fallback to direct mutating credentials.
6. Implement and wire the narrow Linux CRI node agent. Keep runtime privileges
   separate from storage credentials and from the operator's recovery authority.
7. Qualify shipped components in Kubernetes, then real GCS/Chaos; preserve
   acknowledgements and all cleanup/evidence gates from the original review.

## Proposed mutation-authority design for further review

Use the existing Kubernetes API as the durable serialized control plane, with
bounded producer journals rather than an unbounded single ConfigMap. Closure
and admission must CAS the same producer state: a successful close includes all
previously admitted work, and a stale admission cannot commit afterwards.

Persist each immutable operation's producer/run, key, kind, payload digest and
fixed provider precondition before dispatch. Durably stage payloads outside the
database-Pod failure domain. Staging location and journal storage must survive
an authority-Pod restart and have finite admission limits. Never acknowledge a
before-ack request merely because it was staged or journaled.

For the first GCS profile, serialize operations per source key across all
producers and record the generation precondition once. An authority retry must
preserve the original operation identity, exact payload and original generation
condition. Verified commitment excludes later copies with the same **nonzero**
generation condition. This does not hold for generation zero after physical
deletion: a delayed create-only copy can recreate the object. Keep a live guard
or logical tombstone while any zero-conditioned attempt/session is unresolved,
or prove every copy terminal before physical deletion, including lifecycle rules.
Conditional deletion targets an exact observed generation; it must never become
an unconditional delete or change to a newer generation on retry.

Unknown outcomes remain recorded. Resolution must prove terminal completion or
future-effect exclusion through the provider's actual semantics. An unrelated
later successful request is not resolution. Multipart/resumable sessions need
explicit operation/session tracking or must fail the profile's capability gate.

An authority restart can replay only an existing immutable operation. It cannot
invent a replacement operation with new preconditions. Prepare the per-key
descriptor durably, then CAS-bind that exact descriptor to the still-nonterminal
producer entry before dispatch. Missing execution records remain unresolved;
they are not proof that a delayed scheduler will never dispatch. Preserve terminal
sequence watermarks after compaction so stale preparations cannot reopen work.

Generation closure freezes the immutable enrolled producer cohort, not a list
of producer objects observed at one instant. Enrollment must bind deterministic
producer identities; every identity, even one whose producer creation is pending,
is included in the closure. Missing producer state cannot initialize as open.

Record COMMITTED, terminal rejection and FUTURE_EFFECT_EXCLUDED separately.
Exclusion alone never authorizes a successful before-ack client response. Retain
original unknown-attempt records and their resolution evidence.

The focused Pro follow-up approves the CloseProducer/Prepare/Resolve contract,
with the additional guards above; it rejects the earlier blanket exclusion claim.
Native metadata PATCH requires generation plus metageneration. Resumable uploads
bind preconditions at initiation and persist the exact session descriptor before
payload dispatch; a late initiation response cannot bypass producer preparation.
Unsupported session modes fail closed. Other storage providers require their own
verified settlement/exclusion capabilities; an opaque ETag alone is not assumed
to be a non-reusable version.

## Test and deployment boundaries

The red reproductions are an implementation checkpoint, not passing tests to
ship unchanged. Add deterministic startup/closure, replay, two-runtime, retired
history, sustained survivor writes and late-effect tests before live faults.

Use fresh per-run identities and immutable artifacts. Runtime-agent manifests
must select explicitly enrolled dedicated test nodes. Personal project, bucket,
node and credential values remain external. Existing applications are not
termination targets. Any new live host/CRI privilege must have concrete reviewed
scope before deployment; this proposal does not grant it.
