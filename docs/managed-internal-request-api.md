# Internal managed request API (candidate, not released)

The explicit `rhiza-managed-request/v1` materialization profile9 moves normal
SQL/KV execution, reads, readiness and status to the verified engine runtime.
It does not upgrade profile7/8 archives, remove startup/restore/adopt/repair
authority checks or support a new generation transition. Gen0 restrictions and
BeforeAck durability remain. This document describes an unqualified candidate.

`Host.RecoverRequest(ctx, network.RequestStatusRequest{Kind: "sql" /* or kv */,
RequestID: originalRawID})` reads the engine's retained canonical original and
resolves its original result and configured durability. It does not submit a
replacement write, refresh TTL or accept caller payload as the original.
Missing/expired original and lookup uncertainty remain UNKNOWN, not no-effect.

Profile9 `RecoverPendingDetailed` / Rust `recover_pending` are no-argument
compatibility facades over this handle's known error target. Exactly one known
selector and owned attempted-intent fingerprint may resolve; unknown or ambiguous target returns UNKNOWN, never a
success `none` or an arbitrarily latest retained request. Independent successful
Y does not replace lost-response X. RAM selectors disappear on process restart
and are not execution authority or durable discovery. Once more than two
different uncertain targets overflow bounded hint storage, no-arg recovery
remains ambiguous for this handle; use explicit selectors. Runtime poison still
blocks requests; a request-specific lost ACK does not globally poison the Host.

The no-arg facade must compare the attempted fingerprint with the engine's
canonical original. A slot-zero UNKNOWN for intent A must not resolve as a
different intent B that won under the same kind/ID. Two different UNKNOWN
intents on one selector remain ambiguous. Fingerprint-equivalent nil/empty
representations remain eligible; a duplicate submit slot is not required to
equal the canonical first slot. Explicit `recover_request(kind, ID)` observes
the canonical owner, not an old attempted payload, and does not clear an
incompatible no-arg hint. CommitUnknown errors from SQL/KV preserve the existing
mutation-context kind/raw ID/known slot/retry/cause chain; validation, admission
and intent-conflict errors are not reclassified as CommitUnknown.

When an UNKNOWN includes a nonzero submission slot, the handle keeps the
earliest such slot as an upper bound on its canonical first result. A later
same-ID/same-intent result after retention must not resolve or clear that older
hint. The canonical slot may be **earlier** than a duplicate submit slot; exact
equality is not required. A later slot-zero error never erases a known bound.

The existing C ABI operation `recover_pending_detailed` accepts either `{}` or
the exact pair `{"kind":"kv","request_id_b64":"/w=="}`. Half-pairs, null,
unsupported kind, noncanonical/empty/oversized base64 ID and extra fields fail.
Rust adds `ManagedDb::recover_request(kind: MutationKind, request_id: &[u8])`
returning `Result<ManagedRecoveryResult, ManagedMutationError>` through that same
operation. Existing Error fields, SQL signatures, C symbols and response union
are unchanged. Legacy profile7/8 no-argument behavior is unchanged.

Normal duplicate calls can create more than one physical consensus record
across Hosts while retained logical effect/result remains singular. Recovery
itself must add zero application proposals. Pre-cert original loss cannot be
reconstructed from absence: it remains unresolved UNKNOWN. Receipt expiration
does not authorize resurrection from older archive history.
