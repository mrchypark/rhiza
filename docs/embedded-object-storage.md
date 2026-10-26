# Embedded local object-storage backup

Use the exported Go API with `Local: true` and the existing `ObjStore*` fields.
Local mode has no peer listener or replication. With object storage configured,
the embedded process continuously publishes backup history; a missing `DataDir`
restores before `Open` returns ready. The implementation is in the Go engine;
the Rust FFI SDK is unchanged. This example assumes a local VersityGW listener
configured on port 7070.

```go
cfg := rhiza.Config{
    Local:              true,
    NodeID:             "application",
    DataDir:            "./application-data",
    ObjStoreProvider:   rhiza.ObjectStoreProviderS3,
    ObjStoreEndpoint:   "127.0.0.1:7070",
    ObjStoreBucket:     "application-backup",
    ObjStoreRegion:     "us-east-1",
    ObjStoreInsecure:   true, // local development endpoint only
    ObjStoreAccessKey:  "local-development",
    ObjStoreSecretKey:  "local-development",
    ObjStoreDurability: rhiza.ObjectStoreDurabilityBeforeAck,
}
db, err := rhiza.Open(ctx, cfg)
```

Use explicit `ObjectStoreDurabilityBeforeAck` when successful mutation responses
must establish remote durability. An explicit asynchronous policy has a loss
window. An empty policy defaults to before-ack for Local plus object storage.
The same bounded storage-outage test exercises both the explicit policy and
the empty default. Nonlocal policy defaults are outside this test contract.

## Reconciliation contract

An unbound local database may adopt an empty remote store. Matching histories
may reopen; a verified local extension may publish its missing suffix. A
missing whole local root may restore. A verified remote extension may restore
over older local history after validating its existing recovery evidence.
Divergent histories must be refused without modifying either side.
Unequal nonempty histories require a verifiable overlap in retained history;
slot counts alone do not prove ancestry. If a local tip is older than the
archive's retained proof window, startup cannot prove the overlap and must
fail without changing either side. It must not blindly restore over that
nonempty local root.
Missing bound remote history is an error, not permission to bootstrap a new
archive. Partial or corrupt local roots must preserve surviving evidence on
refusal. Repair the missing evidence and retry; do not delete evidence merely
to bypass a startup error.

Only one process may own an archive identity. Stop the old process before an
offline restore, reconciliation, or copy of its local files. A Kubernetes Pod
delete response alone does not establish process termination.

## Automatic Local GC integration contract

Checkpoint rebasing bounds the active replay history. Automatic remote GC must
separately preserve the current root and tail, the newest N committed snapshots,
and every root held by a live restore pin. An uploaded but uncommitted snapshot
must not displace a committed snapshot from that retention window. Shared blocks
remain live while any protected root references them. Release of a pin makes an
otherwise unreferenced root eligible for later collection; it is not permission
to delete blocks shared with a protected root.

`CheckpointInterval` defaults to 15 minutes and controls periodic snapshot
creation. An inactive database with unchanged state skips creating a new
periodic checkpoint. Bounded-WAL and `CheckpointTailBytes` triggers can create
additional snapshots.
`ObjStoreGCInterval` defaults to 1 hour and controls collection cadence, while
`ObjStoreGCGracePeriod` supplies a minimum safety delay after reachability has
been established. Age alone never proves an object is safe to delete.
Zero `ObjStoreGCGracePeriod` selects 24 hours in Local mode, including an
explicit environment value of zero. Environment configuration also defaults
`RHIZA_OBJSTORE_GC_GRACE_PERIOD` to 24 hours. Tests request positive grace
explicitly, using 1ns when prompt collection is required.
`CheckpointRetainCount` is an `int` controlling N; zero selects the default of
3. Snapshot period, count, and collection grace
are separate settings, not a promise to retain a fixed number of days.

Automatic Node GC and `CheckpointRetainCount` configuration are integrated.
The three-provider suite passed with all six public GC tests on each emulator;
the validation snapshot below records the counts, skips, and evidence. The tests call
`LocalArchive.GarbageCollect` and `PinRestore` APIs, renew and release the pin,
and verify a pinned old root's files remain downloadable beyond N. A 260 MiB
immutable SQLite payload spans more than three native object blocks. The test
requires actual generation-qualified block-key overlap between old and current
roots and a uniquely old block not named by any other protected root. After pin
release, unique blocks must disappear and shared blocks must remain. After collection,
whole-local-root loss must still recover current SQL state and idempotency.
The public suite does not exercise selected-old-root application restore or
force every writer/collector/pin interleaving; deterministic race checks belong
to the engine suite. A separate prepared-root test
uploads a real portable root without a committed seal and requires both it and
the committed current root to remain downloadable with `keep=1`. Such prepared
roots are protected conservatively and do not consume the latest-N committed
snapshot budget; GC does not promise to delete every unreferenced prepared
artifact. Uncertain HEAD
publication and crash interleavings also need engine-level fault injection.
Missing or malformed HEAD must fail closed and preserve surviving evidence.

The GC CAS gate does not expire. An ambiguous deletion must leave it held;
elapsed time does not prove the outstanding deleter stopped. Until that evidence
is resolved through safe operator recovery, later collectors, restore pins, and
publications must be refused. Read-only archive inspection remains allowed.
Recovery requires proof that the old owner has terminated and all outstanding
storage operations are resolved. Do not blindly delete the control key or force
an unlock based on a TTL.
`GCUncertainDeleteGateHeld` injects a lost acknowledgement after an actual
abandoned-extent deletion and checks these refusals through a newly opened healthy
archive handle. It must not be made to pass by adding automatic gate takeover.
Restore admission and renewal require at least three quarters of the requested
lease to remain on every root and the captured view after final validation and
admission gate release. For the Node's two-minute lease, that is 90 seconds.
The Node renews every 20 seconds with a 20-second budget, including waiting for
the renewal lock; failure cancels restore before pins are released. The remaining
margin allows a 30-second in-flight RPC batch drain and 20 seconds of scheduling
slack. A merely unexpired but nearly exhausted lease is refused.
HEAD v2's catalog records committed retention roots; its protocol marker must
prevent a downgrade to a writer or collector that ignores that protocol.

Offline read-only init performs no remote writes and cannot acquire a remote
write-based pin. Its safety requires externally established single-writer
quiescence, including no concurrent GC, for the entire download/verification
window. Do not run manual age-based bucket lifecycle deletion. Retaining bytes
does not establish authenticated overlap for every stale nonempty local root.

## Executable integration tests

All DB operations use exported `rhiza.Config`, `Open`, `Execute`,
`ExecuteReturning`, `Query`, `Ready`, and `Close`. Fixture SDKs provision remote
buckets; the existing Go object-store adapter inventories and copies opaque
objects. GC tests inspect the existing Go portable-root descriptors and native
block keys; they do not assume SQLite pages correspond to object blocks or
borrow Rust behavior. Remote List/Get inventories retry the whole image on a
disappearing object for up to 20 seconds, but full namespace captures and
historical copies require all owners stopped and all SDK work drained. No keys,
including `.swap` keys, are filtered. Authority comparisons run with the writer stopped; GC deletion
assertions use object-name inventories and explicitly require removed keys.
Recovery-image capture asserts a valid, actually free maintenance gate and
refuses to copy invalid or held authority. It never clears a token or rewrites
authority to manufacture a recoverable image. The uncertain-deleter test deliberately opts into held-state inspection
and compares its nonempty gate token without changing or unlocking it.
Refusal comparisons exclude only the process coordination file
`qlog/lock.qlog` and a newly created `qlog/` directory. PID writes and lock
release legitimately change that file. Every other file and directory,
including restore journals and materialized data, remains part of the evidence.

| Test suffix | Evidence |
| --- | --- |
| `RepeatedTotalLoss` | Close and drain the DB, capture its actual free-gate namespace, copy only while stopped, delete the entire local root, recover original SQL and exact idempotent RETURNING result/slot, write, restart, and repeat total loss. This permits a shutdown flush and does not independently prove pre-Close publication. |
| `CheckpointAndTailTotalLoss` | Force reclamation with a bounded WAL, observe an existing Go published local checkpoint with nonempty snapshot files, append a tail whose slot is beyond that checkpoint, close/drain before capturing remote bytes, delete the entire DataDir and recover snapshot state plus tail. Require a restored checkpoint, original/tail idempotent results, new writes, restart, and a second whole-root loss. This permits a shutdown flush. |
| `Equal` | Reopen matching histories, retain SQL and original request results, and leave remote object bytes unchanged. |
| `FirstAdoption` | Create SQL locally without a store, attach an empty archive, then delete the root and recover it. |
| `LocalAheadCheckpointAndTail` | Force and observe a shared checkpoint, save that remote image, append a local suffix without another checkpoint, roll remote objects back while stopped, reconcile the extension with retained overlap, then prove whole-root recovery includes snapshot and suffix. |
| `RemoteAheadRestoresProvenExtension` | Restore a stale local image against a proven remote extension, require restored original/new rows and original idempotent results, preserve remote object bytes during restore, then write again and restart. |
| `RemoteAheadMalformedRestoreJournalNonmutating` | Add a malformed existing Go materializer restore journal to the stale local root; require refusal with all authority files and remote objects unchanged, remove only the injected journal and retry. Existing journals are validated before staging replacement files. |
| `LocalOlderThanCheckpointProofNonmutating` | Save an early local root, advance remote history through two observed checkpoint rebases, attach the stale nonempty root, and require refusal without changing either side. Repair the local evidence and retry. The test does not decode a private archive proof format. |
| `DivergedNonmutating` | Construct equal-length branches from stopped copies and opaque remote images; mismatched branches must refuse without changing evidence. Restore matching evidence and retry. |
| `PartialRootFailureRetry` | Retain only one nonempty local file, or corrupt all local files; require refusal without content changes, restore the original files and retry. |
| `MissingBoundRemoteFailureRetry` | Remove a previously bound remote history, require nonmutating refusal, restore the history and retry. |
| `BeforeAckUnavailablePreservesRetry` | Exercise explicit BeforeAck and the empty-policy default with the same fixture: replace the filesystem archive directory with a blocking file, execute with a one-second deadline, reject successful acknowledgement, repair the path, restart and retry the same request twice without duplicate SQL rows. This does not depend on chmod or UID. |
| `ProviderContract` | Real adapter upload/get/list, conditional create conflict, attributes, missing-object classification, delete and exists. Runs independently of the embedded recovery tests. |
| `GCRetentionAndTotalLoss` | Generate at least N+2 checkpoints with a long safety grace, enable tiny-grace automatic collection, require the newest N committed portable roots and removal of older committed roots plus a falling total object count, write while maintenance is enabled, then recover the current checkpoint and tail after complete local loss. Metadata retained for catalog validation is not required to disappear. Verify original idempotent results, new writes, and restart. |
| `GCUnknownHEADNonmutating` | Missing and malformed HEAD must refuse startup without changing local evidence or remote objects, including after several collector intervals; repair only the injected damage and reopen. This does not simulate an uncertain live HEAD publication. |
| `GCPinRelease` | Pin an old committed snapshot, advance beyond N, renew, collect with positive 1ns grace, require shared blocks and download both old snapshot files, release and require object reclamation, then recover current state after whole-local-root loss and restart. |
| `GCUncertainDeleteGateHeld` | Lose the acknowledgement of a real abandoned-extent deletion; a newly opened healthy archive must refuse collection, pinning, and publication without changing remote evidence. |
| `GCPreparedRootPreserved` | Upload a real portable root without committing it to HEAD, collect with `keep=1` and positive 1ns grace, then download both prepared and current committed roots and verify current SQL state. |
| `GCDeletionScope` | Unknown nested extent paths must fail closed with every sentinel unchanged. Remove only the test-owned nested extent, then require successful deletion of an owned abandoned extent while preserving sibling namespaces, lookalike prefixes, nested block paths, and unrelated objects. Prove whole-local-root recovery. Valid-looking basenames alone never authorize deletion. |

Run a focused local test with short macOS temporary paths:

```sh
TMPDIR=/tmp go test . -run '^TestEmbeddedObjectStorage' -count=1 -timeout=8m
```

The Kubernetes worker entry point skips outside its dedicated runner. The
filesystem outage fixture skips for remote providers; it does not claim remote
transport-failure coverage. Injected provider-retry cases remain a verification
gap.
`TestEmbeddedCheckpointFixtureBaseline` independently validates the bounded-WAL
workload and checkpoint observer against the existing local-only Go engine.
This observer reads the established Go local descriptor and nonempty snapshot
files; it does not parse the new remote archive tuple. Replay cost and memory
bounds still require implementation-level evidence beyond these functional
restore tests.

## Three-provider runner

```sh
scripts/test-embedded-object-providers.sh
# Independently verify SDK/emulator compatibility:
RHIZA_EMBEDDED_TEST_PATTERN='^TestEmbeddedObjectStorageProviderContract$' \
    scripts/test-embedded-object-providers.sh
# Run only the integrated public GC tests:
RHIZA_EMBEDDED_TEST_PATTERN='^TestEmbeddedObjectStorageGC' \
    scripts/test-embedded-object-providers.sh
```

The runner lists selected tests before provisioning emulators and rejects a
custom `RHIZA_EMBEDDED_TEST_PATTERN` that matches no test names.

The executable runner uses `floci/floci:2.3.0`,
`floci/floci-gcp:0.11.0`, and `floci/floci-az:0.15.0`. Each emulator and the
cross-compiled Go test binary run on an owned Docker network with no published
host ports. It provisions disposable buckets or containers through actual Go
clients, with explicit test credentials and no mounted host credentials or
ambient host cloud environment. It records pulled image digests and checks every
provider independently. Failed-run logs and digests remain in the reported
`/tmp/rhiza-embedded-providers.*` directory. Owned emulator containers and
networks are removed on success, failure, or interruption; successful-run temporary files
are removed. No cloud endpoint or ambient cloud credentials are used.

- S3 uses the pinned Thanos adapter and minio-go/v7 with explicit static test
  credentials, HTTP endpoint and region. VersityGW remains the ordinary local
  development interface; automated integration uses Floci.
- GCS uses cloud.google.com/go/storage and `STORAGE_EMULATOR_HOST`. The Rhiza
  GCS adapter rejects `ObjStoreEndpoint` and insecure overrides, so the fixture
  does not set those fields. Provisioning explicitly disables authentication.
  The Thanos HTTP wrapper initializes authentication before the storage SDK
  processes emulator settings; disposable test service-account credentials use
  the emulator's own `/token` endpoint. No live credential or Google token
  endpoint is used.
- Azure uses azblob and a connection string with an explicit local BlobEndpoint,
  account and test key. It does not use managed identity.

Existing dependencies suffice: storage v1.56.0, azblob v1.6.1, minio-go/v7
v7.0.95, and Thanos objstore v0.0.0-20260817070452-e42d91c337ee. No new helper
runtime or SDK module is added. An unsupported emulator operation is a failing
contract or reported gap, never evidence of production-cloud parity.

## Local Kubernetes runner

```sh
scripts/test-embedded-object-k8s.sh
```

This executable fixture cross-compiles the Go test binary for the local node,
builds a uniquely named image, imports it into dory-k8s containerd, creates its
own namespace, and deploys pinned Floci S3 outside the application Pod. Every Kubernetes command is
`docker exec -i dory-k8s kubectl`; the ambient host context is never used.

The embedded workload has one replica, strategy `Recreate`, and an `emptyDir`
for local data. The main container owns `rhiza.Open`, validates restored history
and the original RETURNING slot, writes a generation-specific idempotent SQL
mutation, and keeps the database running until SIGTERM. Readiness requires the
main mutation to succeed. The runner repeats twice: scale the workload to zero,
wait for Pod removal, require no running owned worker container in the local
CRI, then recreate the workload. The new Pod has a fresh emptyDir; the test
asserts DataDir is absent before Open. The remote emulator Pod must retain its
identity throughout. There is no PVC or wipe Job.

The initContainer's test helper downloads the checkpoint into a separate staging
emptyDir using existing read/download functions, not a public typed staging API.
It does not create DataDir, local
store identity or voter identity, call `rhiza.Open`, or perform remote mutations.
The main process reads `/staging/verified.json` and compares LocalBinding, then
calls `rhiza.Open` without a staging path. It independently downloads and
restores remote state under the local lock, writes new data, and becomes Ready.
It does not adopt the initContainer's downloaded files. There is no typed
staging handoff in this baseline; these tests establish init download verification
and an independent main restore. The separate HA protocol is outside these Local
tests. Read-only init safety depends on external single-writer quiescence and
exclusion of GC throughout the offline download.

The local NoPVC Kubernetes run passed with checkpoint 7 and two staged files.
It established zero remote mutations during init and no local store identity
or data mutation by init. The main process restored and wrote successfully
after each of two application Pod replacements, with the remote emulator
retained outside the application Pod.

The runner records Pod identities, runtime-stop evidence, application logs,
image identities and final state on failure. Cleanup removes its namespace and
unique test image; failures retain diagnostics and report cleanup errors.
This is a connected reusable-cluster functional test, not a fencing proof for
an isolated node, fresh-cluster release qualification, or real-cloud test.
The all-provider runner is separate; the Kubernetes smoke uses S3 only.

## Native repository verification

This Go checkout has no `scripts/e2e-fast.sh` or `scripts/check-ci.sh`.
`.github/workflows/ci.yml` uses `go test ./...`, hook-enabled targeted tests,
`go vet ./...`, `go build ./cmd/rhiza ./cmd/rhiza-operator`, and a separate
`go test -race ./...` job. Run the focused feature tests first, then affected
package checks and the native suite before integration is declared complete.

## Validation snapshot: 2026-10-26

### Integrated automatic GC qualification

The completed provider run recorded 18 passing top-level tests, no failures,
and two expected skips on each of S3, GCS, and Azure: 54 passes and six skips
across the three emulators. Evidence is preserved in
`/tmp/rhiza-gc-final-providers.log`. The runner verified removal of its owned
containers, networks, and test image for run `rhiza-embedded-1792996215-77406`.
These results precede the final minimum-lease-margin hardening. After that
change, all six GC tests passed again on each of S3, GCS, and Azure, with no
failures or skips: 18 additional passes. Evidence is preserved in
`/tmp/rhiza-gc-final-margin-providers.log`; cleanup was verified for run
`rhiza-embedded-1792999465-6870`. Focused race regressions also passed for
near-expiry roots and views, post-release admission checks, and bounded renewal.

The strengthened checkpoint/tail and remote-ahead tests then passed on all three
providers, two tests per provider without skips. They verify remote durability
before Close and accept only canonical, valid closed restore-pin additions.
Evidence is in `/tmp/rhiza-gc-final-evidence-providers.log`; cleanup was verified
for run `rhiza-embedded-1792999836-14993`. Final logs are also retained under
`/Users/cypark/.codex/artifacts/rhiza-local-gc-20261026.BCDo4V/`.

All six public GC tests passed on every provider: retention and whole-local-root
recovery, missing/malformed HEAD refusal, pinned-root renewal/release with shared
and uniquely old blocks, uncertain-delete gate retention, prepared-root
preservation, and canonical deletion scope. Namespace snapshots are captured
only after database Close drains writer and collector work; the fixture requires
an actually free gate and never sanitizes a held token or omits `.swap` keys.

Each provider skipped the dedicated Kubernetes worker entry point and the
filesystem-only outage injector. These skips do not establish Kubernetes or
remote transport-failure coverage. Production-cloud parity, injected provider
retry behavior, selected-old-root application restore, exhaustive concurrent
interleavings, and performance/chaos qualification remain outside this provider
run. The 24-hour Local zero-grace default is separate from tests' explicit
positive 1ns collection grace.

### Earlier baseline qualification

Before the GC additions, the Go runtime passed 14 selected top-level provider
tests on all three pinned Floci images: 12 passes and two expected skips per
provider. These earlier results alone did not qualify GC. The skips were
the dedicated Kubernetes entry point and the filesystem-only outage injector.
This included SDK contracts, repeated
whole-root loss, observed checkpoint plus tail restoration, first adoption,
equal history, local ahead, proven remote ahead, divergence refusal, partial
root repair/retry, missing bound remote, stale-local proof refusal, and preservation
of malformed materializer restore journals before replacement.

The complete native public feature suite passed with the race detector.
Both explicit BeforeAck and empty-default outage variants also passed three
consecutive race-enabled runs. The fixture stops database workers before
repairing the blocked archive, then verifies restart reconciliation and
idempotent retries.

Full Node, consensus, recovery, and checkpoint package suites passed. Node and
consensus were verified serially to avoid competing fsync-heavy test workloads.
Crash regressions cover pending recorder recovery, remote publication before
resumed WAL compaction, and every owned pre-install staging phase. `go vet`,
CLI/operator builds, formatting, and ShellCheck passed.

The final local NoPVC Kubernetes run passed read-only init staging and two Pod
losses. Each init verified two files at checkpoint 7 with zero remote mutations;
main restored, wrote, and reached Ready. Owned namespaces, containers, networks,
and test images were removed and cleanup verified. The standalone outage
regression can be rerun with:

```sh
TMPDIR=/tmp CGO_ENABLED=1 go test -race . -run '^TestEmbeddedObjectStorageBeforeAckUnavailablePreservesRetry$' -count=3 -timeout=2m
```

The provider runner's empty-pattern guard was also executed: it exited nonzero
before creating emulators when the pattern selected no tests. ShellCheck passes
for the provider runner. Owned containers, networks, and test images were
removed and absence was verified. Image manifest digests from the passing run:

- S3 2.3.0: `sha256:e0b292df4dc9559b7b1bf4f51bdda4bb8e269276236438cd03cae661260ea8a9`
- GCS 0.11.0: `sha256:eae9683d806ceb643bcb36aee97cdb8cad13c3e2959cd053bf40e481994fde12`
- Azure 0.15.0: `sha256:530635d603223e120adbf9221a97560cffc6991d65fa0c72d0f79d1097974c89`
