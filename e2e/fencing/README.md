# Generic cooperative fencing runtime qualification

GitHub Actions builds the server, operator and supervisor from the same source
and includes them in the disposable `Dockerfile.fencing-test` image, tagged
`rhiza-fencing-host:local` and `rhiza-fencing-operator:local`. The repository's
production Dockerfiles also include `rhiza-fencer`; this fixture image is never
published and does not substitute for their image-build checks. Then run:

```sh
CGO_ENABLED=0 go build -o "$RUNNER_TEMP/rhiza-fencing-e2e" ./e2e/fencing
FENCING_E2E_BIN="$RUNNER_TEMP/rhiza-fencing-e2e" sh e2e/fencing/run.sh
```

This creates only the disposable `rhiza-generic-fencing` Kind cluster and
`rhiza-fencing-e2e` namespace. A pre-existing cluster with that name is rejected.
The script uses the canonical authority manifest and supervised StatefulSet
patch. Writers start only after the authority, projected token, TLS trust and
dedicated Versity bucket are ready. No external cloud or existing bucket is used.

The Go driver verifies each running voter/authority/operator image against the
built image configuration through the CRI manifest/configuration digest mapping,
preserves healthy incarnations across an authority outage and acknowledges a SQL
write. It then partitions one reachable voter's QUIC input and narrowly blocks
operator-Pod-IP → selected-voter-Pod-IP TCP/8080 in the operator's network
namespace. This makes the failed member unobservable to the operator without
blocking driver HTTP checks or supervisor-to-authority traffic. The driver verifies a surviving
quorum write/read, and requires an exact generic Voter fence with clean drain and
process-exit evidence. Automatic learner replacement must commit, with matching
Pod UID, WAL identity, immutable credential, startup registration and membership
on all three current voters. A new write and linearizable readback run through
the promoted learner. This stage has a six-minute deadline within the unchanged
15-minute driver deadline and 30-minute Actions job limit; neither stage is skipped
on timeout or failure.

Next, the driver partitions all current voters (including that learner), adds
equivalent exact operator-observation faults for each, requires the generic
Generation proof, and verifies automatic generation recovery with
linearizable readback of all three earlier ACKs plus a new-generation write.
Voter and generation proof snapshots and immutable image evidence are retained
separately. Proof identity/hash and clean drain/exit receipts are checked against
the durable ledger; the earlier voter-stop records must remain unchanged when
the generation closes. A Voter proof is issued once: subsequent learner
registration changes the current ledger, so its historical issuer proof ID is
preserved, not presented as a rehash of the current snapshot. Current target
UID/WAL, operation/request hash, clean drain and process exit remain mandatory.
The final Generation proof still requires the exact closed-ledger hash.
The applied supervised workload is checked for writable
`/tmp` backed by a dedicated emptyDir while retaining its read-only root and
private PID/security boundary. The executor
gets no Docker, CRI, node or host privileges. The separate test driver uses CRI
only to target the injected packet loss and independently check old runtime
absence. No storage restart or manually supplied successful proof is used.

Each HTTP fault records operator/container/target identities and its exact rule.
The online rule is removed after replacement; deferred cleanup removes/checks
absence of every remaining owned HTTP rule and fails if cleanup is unconfirmed.
The shell always collects diagnostics and deletes its owned Kind cluster.
`generic-fencing-results/` preserves the actual exit code, events, resource
snapshots and process logs, including failed runs. This is local Kubernetes/S3
scenario intended to qualify both cooperative scopes, not GCS, hard-crash recovery or runtime
injection of an ambiguous storage mutation. Unknown mutation,
stale receipt and crash history rejection are covered by focused Go regressions.
The added online stage is not qualified until this exact candidate passes runtime
CI; an earlier generation-only success does not qualify the extended scenario.
