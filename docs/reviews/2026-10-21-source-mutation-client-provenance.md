# Source mutation client review provenance

Tracking: [issue #246](https://github.com/mrchypark/rhiza/issues/246).
Full response: [preserved review](2026-10-21-source-mutation-client-pro.md).

- Reviewer: ChatGPT Pro, as attributed by the user who relayed this transcript.
  Codex did not independently read the response UI; browser control was unavailable.
- Received: 2026-10-21, Asia/Seoul. The review's generation timestamp is unknown;
  receipt time is not represented as generation time.
- Source: complete user-supplied text attachment, not an agent-written summary.
  The private conversation reference and original attachment path are recorded
  in the task-state provenance record, not public infrastructure configuration.
- Review baseline and locally inspected implementation:
  `183039aeac884d01e98533552cf24a482296755a`.
- Original attachment: 30,123 UTF-8 bytes; SHA256
  `5743dde3d6901341639f8b7d828140cbc207dfdb5f5f82b944df037e6b8c1764`.
- Preserved Markdown: 30,124bytes; SHA256
  `45fac1b90b0abd34286d1ebdd4cc465083968512821a37ffc649c9ff8b9c65ba`.
  The only normalization is one added final LF; byte comparison against the
  original attachment plus that LF passed. No review paragraphs were omitted.

## Claims and current-code checks are separate

| Original finding | Existing issue | Local inspection at baseline | Verification still required |
|---|---|---|---|
| [P0 caller identity](2026-10-21-source-mutation-client-pro.md#p0-the-existing-caller-boundary-cannot-preserve-logical-mutation-identity) | [#246](https://github.com/mrchypark/rhiza/issues/246) | MutationIntent lacks original condition/owner descriptor; archive sync reloads after ambiguous publishHead errors; uploadExtent can accept EPIPE content readback | Lost committed HEAD response retains original tuple and condition; matching bytes do not settle that unknown tuple |
| [P0 bind receipt](2026-10-21-source-mutation-client-pro.md#p0-bootstrap-binding-does-not-deliver-the-identity-needed-by-the-mutation-client) | [#246](https://github.com/mrchypark/rhiza/issues/246) | AuthorityClient.Bind returns only error; supervisor bind returns nil; Session.BindWAL discards body | Lost activation response recovers same pinned producer UID; source client enables only after durable validation; actual source write identity is denied |
| [P0 selected deletion](2026-10-21-source-mutation-client-pro.md#p0-a-delete-handle-must-preserve-the-selected-generation) | [#246](https://github.com/mrchypark/rhiza/issues/246) | Checkpoint GC selects by metadata and calls unversioned Bucket.Delete | Replacing selected G1 with G2 cannot cause deletion of G2 |

These are source-inspection confirmations, not executed reproductions or fixed
defects. The review explicitly states that its author did not execute tests.
Existing CI39545035226 passed at the baseline; it does not qualify the new
client/staging design or these newly requested failure-injection cases.
No duplicate issue was created; the findings extend the existing issue's
unfinished mutation/effect-boundary requirements.

## Implementation decision and complete scope

Follow the review's ordered edits, retaining public embedded API compatibility:

1. Bind original condition, selected delete version, scope, size and bounded owner
   correlation into immutable intent; preserve structural request IDs.
2. Complete typed producer binding through authority, supervisor and Session.
3. Add WAL-bound fsynced caller retry state and explicit internal MutationClient;
   never infer a lost allocator from LastAdmitted or allocate through uncertainty.
4. Add the existing authority's bounded persistent spool, serialized attachment,
   cancellation, exact cleanup tombstones and compaction coordination.
5. Wire bootstrap first, then archive HEAD and every checkpoint/lease/pin/GC/
   recovery mutation path, each with its own durable owner-step attempt.
6. Implement authority-only fixed-generation GCS execution, durable cross-producer
   zero-generation deletion guards, dispatch accounting and credential separation.

The first positive vertical path is bootstrap registration through bound producer,
sealed stage, fsynced original tuple, durable admission, attributable provider
result and the original synchronous return. Completing just descriptor helpers
or a wrapper is not completion of that path or issue246. All17 failure-injection
regressions in review section9 remain required until individually evidenced.

Runtime-agent authentication/receipts, physical-provider ambiguity resolution,
retained-history bounds and actual GCS/Chaos qualification are still outstanding.
This consultation neither authorizes cloud access nor replaces independent code
review. Old failures and their exact receipts must remain preserved.
