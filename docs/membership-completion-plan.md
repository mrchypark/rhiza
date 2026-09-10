# Membership completion: externally fenced Operator workflow

Status: implementation candidate, not enabled in production. User selected
explicit external fencing rather than infrastructure-specific automatic fencing.
Baseline: v0.13.0 (`d13c722`).

## Acceptance boundary

- With a surviving quorum, remove a permanently lost voter and promote a new
  learner only after externally attested fencing and durable WAL/prefix admission.
- A temporary outage or restart with intact WAL preserves the existing identity.
- With no surviving quorum, use the existing explicitly fenced whole-generation
  recovery flow; online membership must not invent a missing quorum.
- Retry after controller/Pod restart resumes the same operation and target
  incarnation. Conflicting operations and reused retired IDs are rejected.
- Go and Rust embedded hosts expose the same authenticated management operations
  as the Operator's server hosts.
- Existing fixed-membership deployments keep their registration and recovery
  behavior. A feature flag alone is not completion.

## Implemented boundary

1. Certified bootstrap and transition history, including retired IDs and abort
   revisions, is bound to checkpoint seals and local consensus bases. Compaction,
   restart, and cold observer restore validate that history before installing a
   floor.
2. Archive identity remains fixed while consensus ConfigID changes. Archive
   replay, before-ack publication, pins, and GC keep validating one namespace.
3. Node opens registered learners, binds dynamic QUIC transports, persists learner
   admission, and enforces immutable voter registration. Go and Rust expose
   authenticated change, status, and abort management calls.
4. The Operator journals exact remove/add requests, hashes the immutable learner
   credential Secret rather than copying its token into status, and resumes after
   controller restart. Online replacement never mutates a StatefulSet; whole-generation
   recovery retains its explicit StatefulSet replacement flow.
5. Whole-generation recovery seals an externally fenced source and materializes an
   anchored target generation with new membership credentials.

## Current evidence

Core checkpoint compaction, restart, and cold restore retain and validate
configuration history and retired IDs. Node, Go, and Rust management support
authenticated membership change, status, and abort calls. QUIC learner admission
persists the requested certified prefix, and immutable voter registration binds the
bootstrap membership separately from the learner token.

The Operator verifies an explicit fence against the removed voter's registered WAL
identity, journals requests before dispatch, and resumes an aborted addition only
after a newer abort revision. Real MinIO/QUIC replacement and restart tests pass
for both `async` and `before-ack`. A real before-ack generation E2E also passes:
the source is externally closed and sealed, anchored materialization preserves SQL
data and request-ID deduplication, target credentials can write, and a restarted
target revalidates its lineage. The full Go suite, vet, CLI/Operator builds, and
Rust format/lint/tests pass. Local race checks pass after correcting two test synchronization issues.
CI remains outstanding. Recovery regressions also cover a base-zero source with
membership controls and A→B→C recovery after B publishes a normal checkpoint;
the final generation retains A's request deduplication record.

## Operator replacement steps

All voters must run the supporting binary with `RHIZA_ENABLE_RECONFIGURATION=true`
and a configured `RHIZA_ADMIN_TOKEN`. Keep `RHIZA_CLUSTER_MEMBERS` equal to the
original immutable bootstrap on both surviving voters and new learners. A learner
uses its fresh `RHIZA_NODE_ID` and `RHIZA_LEARNER` JSON member (the same `node_id`,
`peer_url`, and `token` as its Secret). Go hosts use `EnableReconfiguration` and
`Learner`; Rust hosts can set the same Go configuration fields with `set_option`.

1. Provision the replacement learner out of band with a fresh node ID and an
   immutable Secret containing only `data.member`: a strict JSON `quepaxa.Member`
   object with `node_id`, `peer_url`, and `token` fields. Do not put the
   token in the resource status or this document.
2. Submit `spec.membership` with `operationID`, `remove`, `fence`, every reachable
   surviving voter in `voterPods` (`nodeID`, `pod`), `replacementPod`, and
   `replacementSecret`. `removedPod` is optional and defaults to `remove`.
   `fence` must contain the same removed `nodeID`, registered `walIdentity`,
   actual `workloadUID`, `confirmed: true`, and non-empty `evidence`.
3. The Operator reads authenticated membership status from `voterPods`, requires a
   surviving quorum, journals and dispatches remove, then journals and dispatches
   add only after the learner proves its persisted WAL identity and certified
   prefix. It does not create, delete, or fence Pods.
4. To abandon a pending addition, set `abortAddition: true`. After the surviving
   quorum records the abort, set it false and provide a fresh replacement Pod and
   immutable Secret; the Operator requires the newer abort revision before it
   resumes the add flow.

## Archive identity decision

Keep the archive's configured identity and its HEAD/extent equality checks.
Node passes the fixed archive identity 1; consensus ConfigID changes are carried
inside decision certificates. Changing membership does not require dropping the
archive identity checks or rotating the archive namespace. A dedicated regression
exercises publication, reload, and replay across both configurations.
Object publication generations and GC pins are a third independent concept.
This observation alone does not prove removed-writer fencing or checkpoint safety.

## Remaining recovery constraints

- An addition freeze binds a particular learner WAL. A replacement disk cannot
  silently inherit that identity.
- Inline configuration proofs are limited to 128 KiB. A history too large for a
  checkpoint fails explicitly rather than omitting transition evidence.
- Passive fixed read replicas remain outside membership reconfiguration.
- The Operator supplies no infrastructure fencing or learner/Pod provisioning;
  external systems must do both before it can act.
- Performance evidence belongs in CI. No local performance claim is made.
