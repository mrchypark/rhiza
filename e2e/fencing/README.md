# Generic cooperative fencing runtime qualification

Only `cooperative-fencing-results/public` is uploaded. The driver's
`safe-artifacts` command projects explicit image/identity/proof/readback fields,
resource identity summaries, and a cleanup boolean. Raw CRI/containerd/Kind logs,
Pod specs, Secret data, environment, and last-applied annotations remain on the
disposable runner and are never uploaded. A failed projection blocks upload;
there is no raw fallback. Descriptor evidence retains verified content SHA256
and allowlisted descriptor links, not raw image configuration or annotations.
Package tests inject credential canaries to guard this boundary. This packet
format change does not weaken the runtime fencing or readback checks.

GitHub Actions builds the server, operator and supervisor from the same source
and includes them in the disposable `Dockerfile.fencing-test` image, tagged
`rhiza-fencing-host:local` and `rhiza-fencing-operator:local`. The repository's
production Dockerfiles also include `rhiza-fencer`; this fixture image is never
published and does not substitute for their image-build checks. Then run:

```sh
CGO_ENABLED=0 go build -o "$RUNNER_TEMP/rhiza-fencing-e2e" ./e2e/fencing
FENCING_E2E_BIN="$RUNNER_TEMP/rhiza-fencing-e2e" sh e2e/fencing/run.sh
```

Before starting the source workload, the script records the pinned Kind node
image, actual CRI/containerd version and runtime type, kernel release, and cgroup
filesystem. It then drives the shipped `rhiza-runtime-agent` binary through the
authority's projected audience-bound token against a disposable one-shot Pod.
The Pod has `restartPolicy: Never` and a non-controller owner reference to a
non-matching zero-replica StatefulSet; the driver verifies those identities before
requesting the exact stop. It requires the agent's captured cgroup inode and
terminal receipt, deletes only the agent Pod on the target node, and asks the
replacement agent to recover and verify the same durable receipt. If containerd
removes the captured cgroup before it can be verified, the component result is
recorded as a failure; missing cgroup metadata is not treated as proof. This is
runtime-agent component evidence only, not full fencing or D1-D4 qualification.
The public packet contains the allowlisted profile and outcome fields plus a
receipt hash, never the raw receipt, token, Pod dump, or runtime logs.

This creates only the disposable `rhiza-generic-fencing` Kind cluster and
`rhiza-fencing-e2e` namespace. A pre-existing cluster with that name is rejected.
The script uses the canonical authority manifest and supervised StatefulSet
patch. Writers start only after the authority, projected token, TLS trust and
dedicated Versity bucket are ready. No external cloud or existing bucket is used.
Public archive coordinates stay in the fixture ConfigMap. Fake Versity Gateway writer keys
are a separate local-cooperative overlay applied only after the shipped
supervised patch; they are not part of that production patch. This disposable
S3-compatible fixture does not establish cloud identity separation or generic
provider qualification.

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

## Direct GCS HA qualification driver

The `cooperative-fencing` Actions job builds two standalone binaries from the
same commit: Linux/amd64 and macOS/arm64, both with `CGO_ENABLED=0`. It uploads
them, `source-sha.txt`, and `SHA256SUMS` as
`gcs-ha-qualification-driver-<sha>-<run>-<attempt>` only after its cooperative
tests and disposable-Kubernetes qualification pass. Building this artifact is
not a GCS or Chaos run.

Use the driver artifact and immutable application/operator image digests from
the same successful workflow run. The image digests are in that run's
`qualification-publication-<sha>-<run>-<attempt>` artifact when the existing
`workflow_dispatch` input `publish_chaos_image` is enabled. Read the
`RepoDigests` in its published-image records; do not use local tags or images
from another source SHA. The qualification host image is not needed: this
native command runs on a host with the approved `kubectl` context and `kubectl`
installed. The driver is not added to the app or operator
image.

After extracting the artifact, verify its checksums and compare its source SHA
with the commit SHA of that exact Actions run. On Linux:

```sh
sha256sum -c SHA256SUMS
test "$(cat source-sha.txt)" = "$EXPECTED_SOURCE_SHA"
chmod 700 ./fencing-e2e-linux-amd64 ./fencing-e2e-darwin-arm64
```

On macOS use `shasum -a 256 -c SHA256SUMS` for the first command. Set
`FENCING_E2E_OUT` to a new private local directory (mode 0700), then invoke
the existing command directly with the binary for your host:

```sh
./fencing-e2e-darwin-arm64 run-gcs-ha-ack-loss
# or on Linux/amd64:
./fencing-e2e-linux-amd64 run-gcs-ha-ack-loss
```

The separate single-voter dead-Pod scenario uses the same
configured GCS and read-only authority observer inputs:

```sh
./fencing-e2e-darwin-arm64 run-gcs-ha-dead-pod
# or on Linux/amd64:
./fencing-e2e-linux-amd64 run-gcs-ha-dead-pod
```

It requires a three-voter, no-PVC workload using `before-ack` durability and
safe automatic recovery. It kills only one captured voter with the admitted
10-second PodChaos profile; then it requires an exact single-target
IndependentV2 `Voter` proof, completed same-cluster membership replacement,
authenticated three-voter status, committed/read-back ACKs, and a new-UID
natural ordinal return refused before readiness. It also rereads the exact
canonical terminal evidence object and requires its receipt to carry
`mode: passive-kernel-v1`, the exact closed stop request/runtime/producer, and
both target and sandbox terminal observations. A normal CRI fallback receipt
does not qualify this scenario. The replacement may be a
separately named learner; this does not claim the original ordinal was promoted.
The event/evidence files are private driver output. The code path is not
qualification evidence until it completes in an actual GCS and Chaos Mesh run,
and it does not claim the broader D1-D4 matrix.

The passive receipt path is enabled only when the runtime-agent profile has an
independently approved `sandbox-executable-sha256` and exact
`sandbox-args-json` (default argv is `["/pause"]`). The SHA-256 must come from
the approved pause image artifact, not from observing the running node during
the qualification. The pinned executable must also be a static ELF executable
(ET_EXEC or static PIE), with no interpreter or external dynamic dependency;
the runtime agent checks the live executable through the same pinned file used
for the digest. The Kind setup script accepts the digest as
`RHIZA_QUALIFIED_SANDBOX_EXECUTABLE_SHA256`. The separate
`RHIZA_RUNTIME_AGENT_PASSIVE_KERNEL=yes` flag is default-off and applies only the
SYS_PTRACE overlay in the disposable test namespace; without it, the base agent
drops all capabilities. A digest alone never changes privileges. The passive
dead-Pod collector verifies the installed template, Pod annotations, exact
capability set, host-PID setting, and rollout before creating a fault. The
ordinary CRI path remains unchanged when the flag is unset, and that fallback
cannot satisfy this scenario.

This requires the GCS run inputs in [`../chaos/README.md`](../chaos/README.md),
including `RHIZA_KUBE_CONTEXT`, `RHIZA_NAMESPACE`, `RHIZA_RUN_ID`,
`RHIZA_APP_IMAGE`, `RHIZA_OPERATOR_IMAGE`, `RHIZA_GCP_PROJECT_ID`, and
`RHIZA_DATA_GSA_ID`. By default, it requires a distinct observer identity
provided through `RHIZA_GCS_AUTHORITY_READONLY_CREDENTIALS_FILE` and
`RHIZA_GCS_AUTHORITY_READER_EMAIL`; writer-credential reuse is rejected. For an
explicitly approved observer run using the existing data-writer credential,
set `RHIZA_GCS_ALLOW_WRITER_OBSERVER=yes` and point those two observer settings
at the writer credential file and its exact service-account email. This opt-in
defaults off and does not change IAM. The observer client still requests the
GCS read-only OAuth scope, and its bucket wrapper rejects uploads/deletes; this
does not prove that the identity lacks write permissions in IAM or establish
an independent observer principal. Before fault injection, ensure the
credential can read the selected canonical authority and source-object scopes.
Keep all credential values out of shell history, logs, source control, and
artifacts.

Keep the raw `FENCING_E2E_OUT` tree private. For a public packet, record the
actual exit code in `actual-exit.txt`, the verified source SHA in
`source-head.txt`, and the two digest-pinned image refs in `image-ids.txt`,
then run `safe-artifacts` and upload only `public/`. Never upload raw private
events, authority receipts, object names, bucket/prefix values, Pod identities,
or credential files. Follow the fresh namespace/prefix, cost-cap, and cleanup
requirements in the GCS runbook. This is a launch path, not a claim that a real
GCS/Chaos qualification has run.

### Manual-terminal and missing-membership selectors

These voter-count scenarios are default-off and run only after the normal
`install` phase. Keep all standard GCS run inputs above configured, use a
source-matched executable `FENCING_E2E_BIN`, and invoke exactly one selector
for a `run`. The launcher rejects combinations with initial-HEAD ACK-loss,
async-nonempty, or each other. Use a fresh namespace and object prefix for each
run; in particular, the missing-membership `negative` and `positive` cases are
separate executions because each test removes source membership in its own
scope.

```sh
RHIZA_GCS_MANUAL_TERMINAL=yes FENCING_E2E_BIN="$FENCING_E2E_BIN" \
  sh e2e/chaos/run-votercount-gcs-chaos.sh run

RHIZA_GCS_MISSING_MEMBERSHIP=yes RHIZA_GCS_MISSING_MEMBERSHIP_CASE=negative \
  FENCING_E2E_BIN="$FENCING_E2E_BIN" sh e2e/chaos/run-votercount-gcs-chaos.sh run

RHIZA_GCS_MISSING_MEMBERSHIP=yes RHIZA_GCS_MISSING_MEMBERSHIP_CASE=positive \
  FENCING_E2E_BIN="$FENCING_E2E_BIN" sh e2e/chaos/run-votercount-gcs-chaos.sh run
```

The negative missing-membership case verifies that absent source membership
without the explicit opt-in remains blocked and leaves the target unclaimed.
The positive case exercises an explicit async `RhizaRecovery` with
`allowMissingSourceMembership` and the existing `allowDataLoss` acknowledgment;
it is not an automatic-policy fallback.

For manual-terminal, install the separate attestation CRD and namespace RBAC as
described in the operator [manual-attestation setup](../../deploy/operator/README.md#fence-requirement-and-manual-terminal-evidence),
binding approver `create` only to the independently authenticated incident-admin
group. The collector creates the Recovery and waits for the typed challenge,
writing `gcs-manual-terminal-challenge.json` in the private
`FENCING_E2E_OUT` directory. A separately authenticated incident administrator
must independently verify the challenge-bound termination, recreation block,
old peer/client and archive-writer revocation, and storage quiescence, then
create the deterministic-name `RhizaManualRecoveryAttestation` with the exact
challenge digest and binding fields. Use `kubectl -n "$RHIZA_NAMESPACE" create
-f "$APPROVAL_MANIFEST"`; do not have the launcher, operator, or fencer create
the approval. The `attester` string and evidence references are descriptive,
not authorization. The run waits only until challenge expiry (bounded by the
collector deadline), and fails closed on missing approval or changed bindings.
For an async source, `allowDataLoss: true` remains required; a `before-ack`
source does not imply data loss. Keep the challenge and approval manifest
private, and publish only the validated `safe-artifacts` projection.

### Initial canonical HEAD fault pre-arm

The voter-count GCS launcher has a separate, default-off pre-arm option for
the first canonical `archive/head.bin` held-v2 create. It must use the
source-matched fencing driver and a fresh namespace/prefix. Set
`RHIZA_GCS_INITIAL_HEAD_ACK_LOSS=yes` and `FENCING_E2E_BIN` only for the
explicit bootstrap qualification run. The launcher installs the immutable,
credential-free fencer config after IAM preflight and before applying the
RhizaCluster. At dispatch, the fencer still requires the exact committed
CONTROL genesis reference, source runtime, held-v2 tuple, and canonical empty
HEAD digest. The injected event means only that a successful GCS response was
discarded; it does not establish a no-effect request or terminal provider
outcome.

The pre-arm command by itself is not qualification evidence. Keep the private
restore state and ACK-loss receipt local until the full bootstrap recovery
collector verifies the canonical guard, exact held GCS generation/bytes,
protected closure proof, target genesis, and target read barrier. Restore the
fencer only after those checks with
`fencing-e2e cleanup-gcs-ha-initial-head-ack-loss`; if verification does not
complete, preserve the private state for diagnosis and do not report success.
