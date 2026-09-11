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

## Recovery verification

Never equate a Kubernetes Pod deletion response with termination of an isolated
process. Recovery requires evidence for the complete requested identity scope,
recreation prevention, and outstanding storage work. Missing evidence must block
recovery. Run performance measurements and actual chaos qualification in CI;
local functional tests are appropriate. Do not use Google Cloud Build.
