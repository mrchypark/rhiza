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

For the first GCS profile, serialize operations per source key and record its
generation precondition once. An authority retry must preserve the original
operation identity, exact payload and original generation condition. If one
identical conditional attempt commits, its new generation must exclude any
later effect from other attempts with the old condition. Conditional deletion
targets an exact observed generation; it must never become an unconditional
delete or change to a newer generation on retry.

Unknown outcomes remain recorded. Resolution must prove terminal completion or
future-effect exclusion through the provider's actual semantics. An unrelated
later successful request is not resolution. Multipart/resumable sessions need
explicit operation/session tracking or must fail the profile's capability gate.

An authority restart can replay only an existing immutable operation. It cannot
invent a replacement operation with new preconditions. Old and new workers may
overlap only if every physical effect is rendered safe by those fixed provider
preconditions; a leadership lease alone is insufficient.

These choices, especially the provider-resolution proof and staging durability,
need focused Pro review before the service is implemented. Other storage
providers require their own verified settlement/exclusion capabilities; an
opaque ETag alone is not assumed to be a non-reusable version.

## Test and deployment boundaries

The red reproductions are an implementation checkpoint, not passing tests to
ship unchanged. Add deterministic startup/closure, replay, two-runtime, retired
history, sustained survivor writes and late-effect tests before live faults.

Use fresh per-run identities and immutable artifacts. Runtime-agent manifests
must select explicitly enrolled dedicated test nodes. Personal project, bucket,
node and credential values remain external. Existing applications are not
termination targets. Any new live host/CRI privilege must have concrete reviewed
scope before deployment; this proposal does not grant it.
