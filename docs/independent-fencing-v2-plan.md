# Independent dead-Pod healing implementation plan

Tracking: [issue #246](https://github.com/mrchypark/rhiza/issues/246).
Review: [complete Pro response](reviews/2026-10-21-dead-pod-autohealing-pro.md).
Status: implementation proposal; three regressions reproduce the existing gaps.
This document is not implementation or qualification evidence.

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
   provider is not returning its pending review.
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
