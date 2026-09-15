# Repository work

Preserve unrelated changes. Never reset, clean, move, delete, or restore another
worker's files to make a check pass. Report the conflicting path and evidence.

## Parallel implementation

- Set the assigned checkout explicitly as each command's working directory; do
  not rely on the inherited session directory. Return tool output explicitly and
  verify the path before concluding that a required file is missing.
- Give each worker an isolated checkout, an explicit allowed write set, and
  observable acceptance checks. Workers do not commit, push, delegate further,
  or write shared memory unless explicitly authorized.
- Correct MiMo worker mistakes through instructions to that worker. Explain the
  failure, evidence, allowed paths, and required result. Do not silently rewrite
  the worker's implementation. Stop harmful activity before issuing repair work.
- The integrating agent reviews the returned changes and owns final verification.
- A dependency owned by another worker is not permission to invent a local
  replacement or stub package. Use the agreed API and report validation as
  pending until the real dependency is available.
- Write Go source with `apply_patch` or a quoted heredoc. Do not embed Go raw
  strings in JavaScript template literals or double-quoted shell commands.
  Check the file on disk before claiming that an edit succeeded.

## Recovery verification

Never equate a Kubernetes Pod deletion response with termination of an isolated
process. Recovery requires evidence for the complete requested identity scope,
recreation prevention, and outstanding storage work. Missing evidence must block
recovery. Run performance measurements and actual chaos qualification in CI;
local functional tests are appropriate. Do not use Google Cloud Build.

## External recovery anchor contract

- A missing external anchor is an error, never a bootstrap signal. Recovery
  must preserve the application's evidence and pending-write state.
- `RequestHash` hashes the entire request, including `FenceHash`. The fence
  digest describes independent fencing evidence; it is not the request hash.
- Recovery reserves `Transition`, not `PendingWrite`. The latter belongs to
  the application's ordinary write protocol.
- Receipt verification checks the exact request and live binding/generation.
  A stored receipt alone cannot authorize activation or cached future writes.
