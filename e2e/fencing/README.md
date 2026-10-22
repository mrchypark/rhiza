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
Public archive coordinates stay in the fixture ConfigMap. Fake MinIO writer keys
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

This requires the GCS run inputs in [`../chaos/README.md`](../chaos/README.md),
including `RHIZA_KUBE_CONTEXT`, `RHIZA_NAMESPACE`, `RHIZA_RUN_ID`,
`RHIZA_APP_IMAGE`, `RHIZA_OPERATOR_IMAGE`, `RHIZA_GCP_PROJECT_ID`, and
`RHIZA_DATA_GSA_ID`. It also requires a distinct read-only observer identity
provided through `RHIZA_GCS_AUTHORITY_READONLY_CREDENTIALS_FILE` and
`RHIZA_GCS_AUTHORITY_READER_EMAIL`; the driver rejects writer-credential
fallback. Before fault injection, separately verify that this identity has
read access to the selected authority and source-object scopes and no
write/delete grants. The driver does not verify least-privilege IAM. Keep all
credential values out of shell history, logs, source control, and artifacts.

Keep the raw `FENCING_E2E_OUT` tree private. For a public packet, record the
actual exit code in `actual-exit.txt`, the verified source SHA in
`source-head.txt`, and the two digest-pinned image refs in `image-ids.txt`,
then run `safe-artifacts` and upload only `public/`. Never upload raw private
events, authority receipts, object names, bucket/prefix values, Pod identities,
or credential files. Follow the fresh namespace/prefix, cost-cap, and cleanup
requirements in the GCS runbook. This is a launch path, not a claim that a real
GCS/Chaos qualification has run.
