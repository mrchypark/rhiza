# Managed SQL errors

Managed operation failures use the existing JSON `error.code` envelope:

- `sql_not_submitted`: this invocation did not enter SQL execution. It does
  not prove that another invocation had no effect or authorize an automatic retry.
- `sql_recovery_required`: the FFI retains this older error mapping when the
  underlying error explicitly marks a non-submitted SQL call as requiring
  recovery. This code does not identify the original request or imply that the
  Host can select it automatically.
- `commit_unknown`: submitted SQL or its finalization is unconfirmed. Preserve
  the exact original request. This is never a rollback/no-effect acknowledgement.

`commit_unknown` takes precedence over cancellation and other nested errors.
For managed FFI recovery, use `recover_pending` with request `{}` only when the
Host has exactly one unambiguous recovery hint. If the hint is absent or
ambiguous and the exact original SQL request ID is known, use
`recover_pending_detailed` with request
`{"kind":"sql","request_id_b64":"<standard base64 of the original ID bytes>"}`.
If the original ID is unknown, do not guess or submit a replacement mutation.
Recovery only learns certified history and resolves retained results; it does
not resubmit SQL. A successful mutation transport response still requires
checking the mutation receipt's committed status.
