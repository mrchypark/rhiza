# Managed SQL errors

Managed operation failures use the existing JSON `error.code` envelope:

- `sql_not_submitted`: this invocation did not enter SQL execution. It does
  not prove that another invocation had no effect or authorize an automatic retry.
- `sql_recovery_required`: this invocation did not submit SQL, but an existing
  pending request or ambiguous reservation must be resolved first. Preserve the
  original request; use `recover_pending`, not a replacement request ID.
- `commit_unknown`: submitted SQL or its finalization is unconfirmed. Preserve
  the exact original request. This is never a rollback/no-effect acknowledgement.

`commit_unknown` takes precedence over cancellation and other nested errors.
Recovery only learns certified history and resolves retained results; it does
not resubmit SQL. A successful transport response still requires checking the
mutation receipt's committed status.
