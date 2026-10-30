# `pkg/operator` current implementation reverse specification

Scope: commit `008f8e576ad76425182e10a69b987f10cfb48b2a`, production operator
reconciliation and automatic recovery. This describes implemented behavior,
not a proposed protocol. Canonical sources below are under
`/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza` (not this
temporary checkout).

## Boundary and runtime loop

`Controller.Run(ctx, interval)` requires non-nil `Kube` and `Bucket` and an
interval greater than zero; otherwise it returns an error before looping. Each
iteration lists `RhizaRecovery` resources, reconciles each independently,
then, only when `Controller.Automatic` is true, reconciles `RhizaCluster`
resources. Per-resource errors are joined, logged by `Run` while its context
is live, and do not prevent other resources from being attempted. Cancellation
at the timer wait returns `ctx.Err()`; an in-flight dependency receives the
same context. [controller.go](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/controller.go)

The Kubernetes helper is an HTTP API client, not an emulator. Its fake-client
tests exercise request semantics only; a fake success is not evidence of a
cluster-side admission, controller, or deletion outcome. The operator uses its
configured namespace for core, apps, and Rhiza CR paths.

## Resources and persistent boundary

`RhizaRecovery` has `apiVersion`, `kind`, arbitrary `metadata`, `spec`, and
optional `status`. `spec.statefulSet` and `spec.sourceClusterID` are required;
`container` defaults to `rhiza`; target `durability` is exactly `async` or
`before-ack`; `maxArchiveAgeSeconds` is nonnegative; and an optional recovery
ID must match `[A-Za-z0-9][A-Za-z0-9_.-]{0,127}` together with a metadata UID.
The same expression applies to resource name, StatefulSet name, and source
cluster ID. [types.go](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/types.go)

Before reservation, the controller records observed generation and validates
the StatefulSet container, flattened environment, no-PVC/`emptyDir` data
contract, shared non-filesystem object-store identity, source cluster ID,
three-or-more exported voters, source membership record, optional anchor gate,
and initial replica/voter-count equality. `RHIZA_OBJSTORE_DURABILITY` defaults
to `async`; `RHIZA_DATA_DIR` defaults to `/data`. Any violation is persisted
as `Blocked`, except missing/transport Kubernetes reads which return errors.

The immutable reservation binds `status.recoveryID`, `status.specHash`, and
`status.statefulSetUID`. Once `status.recoveryID` exists, a changed recovery
ID or immutable spec hash blocks with `reserved recovery request is immutable;
create a new resource`; a StatefulSet UID change also blocks. The status also
persists source durability and fingerprint, target, archive manifest/tip,
credential Secret name, stage, peer observations, captured fence identities,
and anchor request/receipt. Completion is released through its separate path,
not re-run as a new recovery.

## Manual recovery stage machine

The manual stage sequence is `"" -> Reserved -> Sealed -> Stopping -> Starting
-> Restarted -> Complete`; any other stage is blocked. `"" -> Reserved` follows
the immutable successor reservation, and `Reserved -> Sealed` follows the source
archive seal. `Sealed -> Stopping` preserves the fork result, target credentials,
membership and lineage, and optional anchor request/receipt before recording
`Stopping` (controller.go:294 onward). While `Stopping`, the controller scales
down the source and waits for source Pod termination (including fenced learner
cleanup); only then does it record `Starting` (controller.go:266 onward). While
`Starting`, it rechecks the fenced, terminated source and activates the successor
by applying target credentials and replicas; only then does it record `Restarted`
(controller.go:478 onward). `Restarted -> Complete` requires the matching
recovered quorum before its completion record. This means distinct target voter
IDs form a majority and each counted peer matches target cluster/durability,
reports Ready and Quorum, and has certified and applied tips at least the
recovered tip. Automatic children additionally capture verified target identities
before `Complete`. Async source durability additionally
requires `spec.allowDataLoss`.

The ordinary manual fence gate is a trusted assertion, not automatic-proof
validation: it requires confirmation, nonblank evidence, and equality of the
recovery operation, source cluster, and StatefulSet UID. It does not validate a
FenceRequest target set or cryptographic proof. Automatic-child authorization
separately validates its persisted request/proof and captured identities.

At `Sealed`, it forks the certified source archive, creates/reads the target
credentials, immutably writes target `voters/membership.json` and
`recovery/generation.json`, and records a manifest hash and recovered tip.
With anchor opt-in it reads and verifies the generation anchor, requires it to
match the operation/source/fork result, calls the configured anchor client,
and persists the exact request and receipt before source deletion. Any failure
is `Blocked` and does not silently substitute a receipt or bootstrap an anchor.

`Starting` verifies reservation and fence again, patches cluster ID, durability,
credentials, and replicas, then PUTs the StatefulSet to start the successor
before recording `Restarted`. If the target cluster ID is already installed,
the `Starting` retry skips that PUT and records `Restarted`; an unexpected
cluster ID blocks. While `Restarted`, it verifies installed configuration and
desired replica count against the frozen target membership, then probes peers
for the recovered quorum before recording `Complete`. Status writes are observable
side effects and use the CR `/status` endpoint. The status message is not an
error type: callers must use phase, stage, persisted binding, and actual
Kubernetes/object-store state.

## Automatic recovery and ownership

`RhizaCluster` opts an existing workload into automatic recovery. Its spec
contains logical ID, StatefulSet, container, adopted cluster ID, voter count,
optional HTTP/peer ports and cluster domain, bootstrap flag, anchor ID,
`automatic`, and policy. Default ports are 8080/9090 and default domain is
`cluster.local` when forming the binding hash. The voter count must be at least
3, no more than 64, equal desired StatefulSet replicas, and, after adoption,
equal exported membership count.

Automatic state is version-1 JSON at
`<prefix>/automatic/<namespace>/<logicalID>/control.json`, read with two object
attribute checks and a maximum 1 MiB payload. It must have binding UID, active
cluster ID, and non-nil identities. It is written with `IfNotExists` for first
creation or `IfMatch` using the exact version read. Four inconsistent reads
fail rather than proceeding. Thus a lost or unversioned control object is an
error, never permission to invent an incident.

The control record freezes `bindingUID`, binding hash/version, StatefulSet UID,
active cluster ID, voters, identities, suspicion time, completion history,
sequence, active intent, completed outcome, and learner history. A workload
ownership object at `<prefix>/automatic-workloads/<namespace>/<stsUID>/binding.json`
must immutably agree on logical ID and binding UID (and current/allowed legacy
hash); another owner blocks reconciliation. Legacy binding migration itself
writes immutable migration proof before updating control state.

Automatic children carry owner/logical annotations and are authorized before
ordinary recovery reconciliation. A child which does not exactly match the
persisted automatic intent/binding is ignored or rejected; automatic authority
does not make arbitrary `RhizaRecovery` CRs trusted.

## Intent, proof, completion, and resume

When policy admits a failure, an `autoIntent` persists the complete
`FenceRequest`, policy snapshot, durability, frozen voters, failed node,
recovery name/UID, expected spec hash, and optional proof. It also persists
learner UID/WAL/name, missing-since time, attempt count, abort/retirement
fields, prior hash, and no-quorum time. This journal is resumed before making
fresh decisions. The recovery child is created/adopted only when its exact
identity, spec hash, request/proof, and binding agree.

After installation, `autoCompletion` records recovery name/UID, spec hash,
fence proof ID and request hash, resultant cluster ID, voter list, and time.
It is expressly independent of later health. Completion verification checks
the installed generation and captured target identities before advancing active
cluster identity and retaining the journal. A missing or changed child does
not authorize a replacement; the lifecycle follows the persisted intent,
learner handling, escalation, or retirement branch.

Before manual advancement, `recovery-owner` on the StatefulSet is claimed for
the operation; a different value rejects the recovery, while the same value is
reused. Completion releases that annotation only when both the StatefulSet UID
and operation still match. Automatic completion separately verifies either a
membership replacement journal or a completed generation: the latter checks the
child identity/spec/annotations, reservation, target credentials and membership,
lineage/seal/anchor, installed workload, and captured identities. Legacy
incomplete target-identity capture is accepted as incomplete, not synthesized;
missing identities remain missing and prevent later single-voter repair.

## Learners, lineage, and retirement

Learner credentials are represented by immutable Secret UID/resource-version
binding and request hash; credential bytes/tokens are not written into status
or automatic control. Learner incarnations retain source cluster ID,
operation ID, credential UID, target identity, and optional retirement request
and proof. History is bounded at 64 entries. Compaction only removes an entry
after confirming terminated Pod cleanup and credential retirement; it fails
closed where proof, inventory, or cleanup is unavailable.

The worst-case fencing set includes retained learner incarnations, so a
subsequent incident cannot omit a formerly provisioned process merely because
it disappeared from a current listing. Never-bound learner cleanup requires
proof; a missing Pod UID alone is not a fence. Retirement first requires
membership commit evidence and uses its own fence request/proof before
credential deletion. These lineage rules are why old learner state survives a
successful recovery until the source generation has been replaced.

## Policy, probing, escalation

`DecideAutoRecovery` receives the policy and an observation containing quorum,
failed voter, current time, suspicion time, recent completions, and related
facts. It returns a named decision rather than performing I/O. The controller
uses the persisted policy snapshot once an intent exists, so later CR edits do
not alter an admitted operation. Automatic quorum probes contact workload
endpoints using configured credentials and treat failures conservatively.
The request is authenticated `GET /recovery/probe?nonce=…&exclude=…`, with a
fresh challenge in `nonce` and the excluded voter in `exclude`. The handler
rejects other methods. The controller checks the echoed exclusion, challenge
MAC, cluster/node identity, and quorum result. Sources:
`pkg/operator/automatic_probe.go`, `automaticQuorumProbe`;
`pkg/network/recovery_probe.go`, `handleRecoveryProbe`.
The normative [recovery probe wire contract](request-http-go-contracts-2026-10-30.md#recovery-probe-wire-contract)
specifies the nonce, nested status fields, HMAC byte serialization, errors and
client/handler timeouts. The automatic probe checks `Quorum`, not `Ready`,
durability or tip thresholds; completion probes use their own stronger checks.

Escalation is assessed only against an existing intent/control version and
persists the outcome through CAS. It distinguishes no action, progression, and
terminal/error disposition. It must not replace an established fence request
or proof merely because observations changed. No-quorum timing, learner
availability, retirement state, and target capacity affect eligibility.

## Fencing boundary

`FenceRequest`/`FenceProof` are documented exactly in the companion appendix.
The controller validates request/proof binding before accepting a fencer's
answer. A pending proof is not success. The capacity is 64 targets. Evidence
is closed over captured targets and request hash, and independent attestation
requires its own runtime/storage evidence; cooperative and independent ledgers
are not interchangeable. Kubernetes fencing persists a CR and resumes an
immutable operation rather than treating a recreated CR as success.

## Failure, cancellation, and ordering rules

All external calls take the reconciliation context. A cancelled context stops
the loop with its context error; lower-level calls may return the same error
and status recording is consequently not guaranteed. Read/parse/CAS failures
fail closed; only explicit object-not-found branches create immutable records.
Writes precede destructive progression: reservations and fork lineage precede
source deletion; anchor activation precedes transition beyond sealing; learner
retirement proof precedes secret removal. The manual activation branch mutates
the successor StatefulSet's cluster identity, durability, credentials, and
replicas, then PUTs it to start the successor. No live-cloud fallback occurs.

The appendix gives the complete operator-owned persisted schemas, including
automatic control/intent/completion and external fencing evidence. Dependency
exclusions apply only to dependency-owned wire schemas, not these structs.

Review: [corrected and independently rechecked within the recorded scope](operator-go-egm-review-2026-10-30.md).
