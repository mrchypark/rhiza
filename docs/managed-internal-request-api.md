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
selector may resolve; unknown or ambiguous target returns UNKNOWN, never a
success `none` or an arbitrarily latest retained request. Independent successful
Y does not replace lost-response X. RAM selectors disappear on process restart
and are not execution authority or durable discovery. Once more than two
different uncertain targets overflow bounded hint storage, no-arg recovery
remains ambiguous for this handle; use explicit selectors. Runtime poison still
blocks requests; a request-specific lost ACK does not globally poison the Host.

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
