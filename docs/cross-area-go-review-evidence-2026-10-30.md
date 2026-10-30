# Go 영역 간 독립 검토 원문

기준 HEAD: `008f8e576ad76425182e10a69b987f10cfb48b2a`. 아래는 초기 검토, 보정 중간 검토, 최종 독립 스냅샷 검토의 반환 원문이다. 당시 지적과 판정을 소급해 바꾸지 않는다. 최종 처리 상태는 [통합 검토 기록](cross-area-go-egm-review-2026-10-30.md)을 따른다.

## 최종 Nit 후속 확인

검토자: `01a18c1e-6e25-73f1-ab11-1e770b9907ac`.

Confirmed: **R7-C-01 is resolved.** E03 now links to [wal.go:662](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L662), where `func (w *WAL) Append(entry Entry) error` begins.

C7 has **11 data rows**, plus header and separator = **13 Markdown table lines**. My earlier “thirteen rows” wording was inaccurate.

Bounded final outcome: batch7 C has **0 remaining findings** (Blocker 0, Major 0, Nit 0). This was a read-only link/symbol and row-count confirmation; no fresh whole audit was performed.

## 2차 이해도 검토

검토자: `01a18bff-b4bf-7950-9db0-60ed5dacaf06`, `fork_context:false`. 이 검토는 Major 지적이 있고 읽기 범위가 미완료라고 보고했으므로 clean round로 계산하지 않는다.

## What this proposes

The hub connects 11 existing specifications through 17 producer/consumer edges. It defines which layer owns validation, application, durability, retry protection, resource cleanup, and recovery authority without adding runtime behavior or expanding engine internals.

## How the existing system works, per this document

Requests pass through shared admission into independently owned proposal work. Consensus decision, materialized receipt, durability completion, checkpoint publication, and recovery installation are separate transitions. Recovery combines archive integrity, Core certification, exact snapshot identity, and caller-owned protection; external-anchor activation adds a separate live authority check.

**Bounded result: 2 Major findings. Review incomplete; no clean verdict.** I did not author these edits.

### R2-comprehension-01 — Major — Ordinary shutdown is described as universally draining storage

**Document clauses:** Hub E16 at [line 32](cross-area-go-contracts-2026-10-30.md?plain=1#L32) and C4 at [line 69](cross-area-go-contracts-2026-10-30.md?plain=1#L69).

C4 states that Node shutdown joins writers and then drains storage. E16 similarly describes checking mutation outcomes after joining writers. These statements omit the distinction between ordinary shutdown and terminal drain.

**Owner/source rationale:** The Node owner appendix specifies **local** mutation drain in its close order, then separately describes terminal-drain execution at [lines 189–210](node-lifecycle-go-contracts-2026-10-30.md?plain=1#L189). Ordinary `shutdownEngine(true)` calls `DrainMutations` only inside `if n.localOwner != nil`, at [node.go:2110](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L2110). Without that owner, it proceeds to bucket close without that drain check.

`TerminalDrain` instead captures the bucket, calls `shutdownEngine(false)`, drains with a background context, closes the bucket, and derives `StorageKnown` from the joined errors at [fencing.go:56](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/fencing.go#L56).

A fresh implementer could incorrectly add a universal ordinary-shutdown drain or treat ordinary shutdown success as evidence that mutation outcomes were checked.

**Small fix:** Qualify E16/C4: ordinary shutdown performs the mutation drain when a Local filesystem owner exists; `TerminalDrain` performs the general captured-bucket drain and produces the identity-bound `StorageKnown` result. Preserve the existing owner-poison/qlog-release distinction.

### R2-comprehension-02 — Major — Anchor retry contract omits the committed fast path

**Document clauses:** Hub C5 at [line 75](cross-area-go-contracts-2026-10-30.md?plain=1#L75); operator owner appendix at [lines 201–209](operator-go-contracts-2026-10-30.md?plain=1#L201).

Both describe activation as rejecting pending application writes without explaining the already-committed retry case.

**Source rationale:** `Activate` first checks `committed(existing, req, rhash)` and returns the stored receipt at [coordinator.go:127](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recoveryanchor/coordinator.go#L127). The `PendingWrite` rejection occurs afterward at [line 137](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recoveryanchor/coordinator.go#L137). The committed predicate checks request/receipt identity, completed transition, live binding, and generation, but does **not** check `PendingWrite`, at [line 224](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recoveryanchor/coordinator.go#L224).

Consequently, an exact committed retry can return its receipt while the record has `PendingWrite=true`. `Verify` likewise uses that committed predicate; ordinary application authority is separately checked by `CheckBinding`, which does reject pending writes.

The omission changes observable retry/error behavior and blurs receipt verification versus permission for a new application operation.

**Small fix:** State the order explicitly: an exact already-committed request returns its stored receipt first; otherwise activation rejects pending application writes before reserving/resuming a transition. State that receipt verification does not substitute for `CheckBinding`.

### Coverage of the 17 edges

“Examined” below means the listed boundary was compared with owner clauses and selected source, not that every linked dependency was exhaustively audited.

| Edge | Boundary examined | Result |
|---|---|---|
| E01 | FFI dispatch → facade → SQL validation | Examined mixed fields and returning restriction |
| E02 | Admission → batch/proposal ownership | Examined transfer, cancellation, detached deadlines |
| E03 | QuePaxa → QLog | Examined codec, Append/Sync distinction and semantic responsibility |
| E04 | Consensus ↔ peer transport | Examined Learned apply-before-ACK and propagation quorum |
| E05 | Decisions → materializer | Examined `WaitTip`, ordered application and rejected receipts |
| E06 | Materializer → checkpoint | Examined equal-tip pair capture and cleanup ownership |
| E07 | Checkpoint/archive → object store | Examined conditional errors, dedup metrics and content responsibility |
| E08 | Candidate → prepare/seal → CURRENT | Examined validator identity checks and archive-before-promotion |
| E09 | Local checkpoint → archive | Examined generation fallback and immutable metadata mismatch |
| E10 | Archive → Node recovery | Examined startup protection and generation-anchor installation |
| E11 | Node → exact-base materializer restore | Examined authority matching and finalize-before-suffix order |
| E12 | Archive floor → Core → QLog | Examined Commit error before Core base installation |
| E13 | Node ↔ peer catch-up | Examined learner single-source path, durability and readiness |
| E14 | Operator → recovery probe | Examined authenticated GET, challenge and exclusion checks |
| E15 | Operator → external anchor | **R2-comprehension-02** |
| E16 | Node → storage shutdown/ownership | **R2-comprehension-01** |
| E17 | Receipt → status/retry adapters | Examined retained outcome versus durability barrier |

### Actual read scope and limitations

Read the requested standard and comprehension persona first; read the index and the **entire 104-line hub**, then re-resolved the finding clauses. Read boundary sections from all 11 original area specifications and their relevant contracts/formats appendices, including the S-01/S-02 explanations.

Source inspection covered the FFI dispatch and facade; network admission/proposal/batcher/status/application paths; Learned and transport fan-out; materializer SQL validation, snapshots and Local restore; checkpoint promotion/upload/hash generation; Local snapshot publication; startup recovery/generation installation; Core/QLog compaction; learner catch-up; probe client/handler; anchor coordinator/controller; and shutdown/filesystem ownership.

**Missing reads remain:** I did not exhaustively traverse every source link in those owner documents. In particular, full QuePaxa protocol/membership evidence, complete archive certification/load/replay implementations, full checkpoint maintenance-wrapper paths, and all C7 constant/error validation sites were not independently read end to end. Some multi-file tool output was truncated; only subsequently retrieved sections count as read. Under the requested completeness rule, this report is therefore **incomplete**, despite touching all 17 edges.

No writes, builds, tests, memory access, chat-history access, or delegation occurred. Historical review conclusions were not used to establish acceptance. The supplied identical-source baseline comparison was accepted without treating the baseline difference as a defect.

```json
{
  "mistake_candidates": [
    {
      "finding": "R2-comprehension-01",
      "pattern": "Generalizing a conditional cleanup branch into a universal lifecycle guarantee",
      "control": "State the branch predicate and distinguish ordinary shutdown from terminal drain."
    },
    {
      "finding": "R2-comprehension-02",
      "pattern": "Describing rejection rules without documenting an earlier idempotent-success branch",
      "control": "Document fast-path precedence before validation and rejection rules."
    }
  ]
}
```

## 2차 코드 대조 검토

검토자: `01a18bff-b2c2-7b32-9b1c-6ba49123414c`, `fork_context:false`. 초기 문서의 보정 뒤 확인한 보고이며, 미완료 읽기와 발견 사항을 이유로 clean round로 계산하지 않는다. 두 지적은 2차 이해도 검토와 중복된다. 각 검토자가 부여한 심각도는 원문대로 보존한다.

**Round 2 outcome: 2 Blockers and 1 Major established. Review completeness is incomplete; no clean determination.** Severity follows the supplied persona: “Blocker” identifies documentation contradicting current code, not a demonstrated runtime vulnerability.

I did not author these edits. No writes, builds, tests, memory access, chat-history access, or delegation occurred. Source HEAD is `008f8e576ad76425182e10a69b987f10cfb48b2a`. The supplied baseline equivalence is accepted; the older QuePaxa baseline is not a finding.

**Comprehension paraphrase**

The hub connects admission and cloned-command ownership to consensus, ordered materialization, retained receipts, checkpoint publication, archive recovery, and eventual compaction. A consensus decision, successful application, response durability barrier, certified checkpoint pointer, and completed recovery are separate transitions. Cancellation can end a caller’s wait while owned work continues.

Recovery authority comes from the appropriate certified history or externally authorized generation transition. Pins and filesystem ownership preserve dependencies; they do not independently certify them. Immutable archive lineage and the live application anchor serve different purposes. Readiness, Pod deletion, and bucket-lifetime drain likewise do not establish complete fencing.

The owner clauses generally preserve these distinctions in the inspected portions. The findings below identify two overbroad claims and one boundary whose detailed wire contract remains unowned.

**R2-codebase_details-01 — Blocker — Pending-write rejection omits the committed-retry exception**

Affected clauses:

- [Hub C5, line 75](cross-area-go-contracts-2026-10-30.md?plain=1#L75): `Activate` rejects an application `PendingWrite`.
- [Operator contracts, lines 206–209](operator-go-contracts-2026-10-30.md?plain=1#L206): activation rejects pending application writes.

The source checks `committed(existing, req, rhash)` and returns the stored receipt **before** testing `PendingWrite`. See [coordinator.go:127](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recoveryanchor/coordinator.go#L127). The [committed predicate](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recoveryanchor/coordinator.go#L224) requires matching request/receipt and live binding/generation with no transition, but does not inspect `PendingWrite`.

This exception is reachable through the ordinary application path: [UpdateApplication](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recoveryanchor/coordinator.go#L303) can set `PendingWrite=true` while preserving `LastTransition` and the target binding/generation. Repeating the completed activation request then returns its receipt; `Verify` also uses the same committed predicate.

A replacement following the prose would reject this retry, unlike the current implementation.

Small fix: qualify both clauses: **starting or resuming an uncommitted activation rejects pending application writes; an exact already-committed retry returns its receipt before that check.** Explain that receipt verification does not establish absence of a current application pending write. Preserve the separate application-write authority rules.

**R2-codebase_details-02 — Blocker — Shutdown is described as universally draining storage**

Affected clauses:

- [Hub C4, line 69](cross-area-go-contracts-2026-10-30.md?plain=1#L69): Node shutdown joins writers and then drains storage.
- [E16, line 32](cross-area-go-contracts-2026-10-30.md?plain=1#L32): combines writer shutdown, mutation-result checking, bucket close, and filesystem ownership without naming the distinct paths.

In ordinary `Shutdown`, [shutdownEngine](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L2036) calls `DrainMutations` only inside `if n.localOwner != nil`; see [node.go:2110](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L2110). An ordinary HA shutdown without that owner closes the bucket without performing this drain.

The separate [TerminalDrain path](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/fencing.go#L53) retains the bucket, runs `shutdownEngine(false)`, then explicitly drains tracked mutations and joins the bucket-close result before setting `StorageKnown`.

The [Node owner clause](node-lifecycle-go-reverse-spec-2026-10-30.md?plain=1#L170) appropriately qualifies its 30-second drain as Local; the [object-store clause](object-store-go-reverse-spec-2026-10-30.md?plain=1#L58) identifies terminal drain. The hub broadens these contracts.

Small fix: explicitly distinguish ordinary HA shutdown, filesystem-owner shutdown, and identity-bound `TerminalDrain`. Keep the owner-close-error/qlog-release behavior, which the inspected source supports.

**R2-codebase_details-03 — Major — E14 lacks a normative probe wire contract**

Affected clauses:

- [E14, line 30](cross-area-go-contracts-2026-10-30.md?plain=1#L30).
- [Operator probing, lines 182–187](operator-go-reverse-spec-2026-10-30.md?plain=1#L182).
- [HTTP inventory, line 33](request-http-go-reverse-spec-2026-10-30.md?plain=1#L33), which explicitly excludes the recovery protocol.
- [Operator appendix exclusions](operator-go-contracts-2026-10-30.md?plain=1#L237), which exclude dependency-owned network schemas.

These clauses establish GET, authentication, query names, and conceptual response checks, but leave the interoperable challenge/response contract to source inference.

The actual crossing requires:

- A random 32-byte nonce, encoded as lowercase hex by the operator.
- A handler nonce that hex-decodes to exactly 32 bytes.
- `RecoveryProbe` JSON containing `excluded,omitempty`, `nonce`, `status`, and `mac`.
- HMAC-SHA-256 keyed by the token over Go JSON serialization of that struct with `MAC=""`; the `mac` field remains present.
- Exact echoed nonce/exclusion and cluster/node checks.
- A three-second client context, 16 KiB decoder reader limit, and two-second handler context.

Evidence: [automatic_probe.go:16](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/automatic_probe.go#L16), [recovery_probe.go:15](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/recovery_probe.go#L15), and [recovery_status.go:12](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/recovery_status.go#L12).

Small fix: assign a short normative recovery-probe subsection to the network/request-http owner and link it from E14/operator. Specify these wire details, nested status fields, rejection behavior, and deadlines. This concerns the existing cross-area interface, not new engine-internal scope.

**17-edge coverage**

“Inspected” below means the stated boundary received document/source inspection; it is not a pass certification.

| Edge | Boundary inspected | Result |
|---|---|---|
| E01 | FFI dispatch → facade → SQL admission; mixed SQL fields | Inspected |
| E02 | Batch/proposal ownership, cancellation, detached contexts | Inspected |
| E03 | Recorder payload → QLog; Append versus Sync | Inspected |
| E04 | Learned installation/application and propagation quorum | Inspected; full authorization/control closure remains unread |
| E05 | WaitTip → ordered ApplyBatch → receipt | Inspected |
| E06 | Paired snapshot roles, matching tips, cleanup, upload lifetime | Inspected |
| E07 | Conditional upload, metric classification, extent readback | Inspected; provider closure remains unread |
| E08 | Candidate → prepare/seal → archive → CURRENT | Inspected |
| E09 | Portable recreation and immutable Local metadata | Inspected; complete prepared-resume path remains unread |
| E10 | Recovery guard, generation installation, base/suffix ownership | Inspected; full archive verification closure remains unread |
| E11 | Exact-base prepare/open/finalize before suffix replay | Inspected; import implementation closure remains unread |
| E12 | QLog manifest adoption before Core base installation | Inspected; S-02 supported by inspected branches |
| E13 | Learner sequential catch-up, durability/replay, readiness | Inspected |
| E14 | Probe request/response, nonce, MAC, limits | Major 03 |
| E15 | Request construction, live receipt verification, activation | Blocker 01 |
| E16 | Shutdown, terminal drain, filesystem-owner/qlog lifetime | Blocker 02 |
| E17 | Local status lookup versus retry durability barrier | Inspected |

**Actual read scope and completeness**

Read the supplied standard first, then persona; also read the EGM review skill. Read the index and **all hub lines 1–104**, with finding locations re-read at the end. Inspected boundary clauses across all 11 area bodies and their relevant contracts/formats appendices, including the QuePaxa protocol, membership, and durability supplements.

Source inspection covered bounded regions in the FFI bridge, Go facade, shared value types, request server/batcher/admission, peer server/transport/bounds, materializer snapshot/validation and Local restore APIs, checkpoint creation/upload/hash/promotion, QLog append/sync/compaction, Core prepare/compaction, Node startup/generation/catch-up/shutdown/Local backup, recovery snapshot publication/archive upload, operator probe/controller, recovery-anchor coordinator/types, and object-store metering/ownership.

**Missing reads prevent a complete audit.** These include linked implementation details in `pkg/recovery/anchor.go`, `fork.go`, `pkg/node/local_backup_staging.go`, checkpoint auto/Local-gate implementation, provider implementations, graph application implementation, and portions of Core replay/certification and peer authorization. Some initial bulk output was truncated; subsequent focused reads recovered relevant portions, but did not establish complete transitive link coverage.

Historical review files were not opened or used as normative evidence. An overly broad search inadvertently returned historical-review snippets; they were excluded from the assessment.

The bounded outcome is therefore **findings established; full requested review incomplete**. Neither prior fixes nor the inspected S-01/S-02 descriptions justify a clean result.

```json
{
  "mistake_candidates": [
    {
      "id": "R2-codebase_details-M01",
      "observation": "An unconditional rejection statement omitted an earlier idempotent-success branch.",
      "control": "Check early returns before documenting validation or rejection precedence."
    },
    {
      "id": "R2-codebase_details-M02",
      "observation": "The hub generalized Local and TerminalDrain guarantees to ordinary Shutdown.",
      "control": "Name the entry point and mode predicate for every drain or ownership guarantee."
    },
    {
      "id": "R2-codebase_details-M03",
      "observation": "A producer-consumer edge named a protocol while its linked owners excluded the detailed wire contract.",
      "control": "Assign one normative owner for each crossing's encoding, validation, errors, and deadlines."
    },
    {
      "id": "R2-codebase_details-M04",
      "observation": "Broad searches and oversized reads returned historical snippets or truncated output.",
      "control": "Use explicit normative-file allowlists and bounded line reads; never count truncated ranges as covered."
    }
  ]
}
```

## 3차 독립 검토

세 검토자는 모두 `fork_context:false`로 시작했다. A·B가 직접 경계 소스를 나눠 검토하고 C가 전체 흐름을 검토했다. A·C에는 지적이 없고 B에는 Major 1건이 있으므로 이 회차도 clean으로 계산하지 않는다.

### 3차 A

검토자: `01a18c09-9360-7840-9b38-6f4de1815ef5`.

**R3-A verdict: no Blocker, Major, or Nit findings within the assigned boundary.** Findings: `[]`; no R3-A-NN IDs issued.

Actual read scope is **complete within the assigned boundary**: both required references first, the index, all 105 hub lines, relevant owner clauses, and direct crossing sources/validators. Truncated output was not counted as read evidence.

| Coverage | Verified boundary |
|---|---|
| E01 | FFI decoding → facade → shared mutation API; mixed SQL precedence and returning restrictions |
| E02 | Admission, clone, lease transfer, queue rejection, proposal registration and cancellation ownership |
| E03 | QuePaxa WAL production versus QLog framing/CRC; Append versus Sync |
| E04 | Historical/current authorization, Record/Learned/Sync/ReadIndex, apply-before-ACK, propagation versus recorder quorum |
| E05 | WaitTip → ordered ApplyBatch; rejected receipts versus consensus success |
| E06 | Matching snapshot tips, fixed roles, caller-owned cleanup and source immutability |
| E07 | Capability/error classification, conditional-upload semantics, reader ownership and content verification |
| E08 | Candidate → validation/preparation → seal decision → archive synchronization → CURRENT promotion |
| E17 | Local retained status, expiry, matching retry and renewed durability wait |

C2 identity/fingerprint contracts, C3 SQL→checkpoint ordering, and C4 request cancellation/retry ownership were checked against their direct implementations.

Assigned C7 constants and validation sites were confirmed:

- **Request/admission:** FFI 16 MiB; HTTP 1 MiB; mutation count 128, aggregate charge 32 MiB, per-request charge 1 MiB, depth 32 and nodes 65,536.
- **Batch/value:** queue 4,096/8 MiB, eight concurrent batches per batcher, 64 items and 128 KiB replicated values; relevant admission/error ordering checked.
- **Peer:** 1 MiB frame, Sync limit 1..128, 16,384 object budget; decode preflight and first-item frame failure checked.
- **Timeouts:** FFI default 30 seconds; ordinary peer five seconds; quorum calls inherit caller deadlines; checkpoint preparation five minutes; catch-up rounds two seconds/pages 250 ms. Worker and caller lifetimes remain distinct.
- **Output:** 10,000 rows, 16 MiB aggregate, 1 MiB cell and retained SQL-result bounds; collection, encoding and rejection paths checked.

`mistake_candidates` considered and dismissed:

- Mixed SQL fields are accepted; unused legacy fields remain fingerprint-significant.
- A retained committed status does not establish before-ACK durability.
- Learned ACK follows application, but recorder membership alone selects the hint path.
- The 128 KiB proposal/staging limit is not a universal received-decision limit.
- Conditional-upload rejection and dedup metrics do not verify content equality.

E09–E16, probe wire/MAC verification, and recursive engine/provider/dependency internals remain scope exclusions. This verdict does not establish collective E01–E17 completion or reimplementation equivalence.

No edits, builds, tests, delegation, memory access, history inspection, or prior-review consultation occurred.

### 3차 B

검토자: `01a18c09-956a-7523-a2a7-a86412968a9e`.

**R3-B: one Major documentation finding. Assigned boundary review complete; no clean verdict.**

**R3-B-01 — Major: independent-attestation timestamp acceptance is underspecified.**

- **Document:** [operator contracts, lines 148–153](operator-go-contracts-2026-10-30.md?plain=1#L148), reached through hub C5/C7, gives “clock skew is 2 minutes” and broadly says stale/malformed evidence rejects fencing.
- **Source:** [validateIndependentEvidence](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/fencing_attestor.go#L221) checks top-level `ObservedAt` and barrier `DeniedAt` only for nonempty strings. [validateEvidenceStorage](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/fencing_attestor.go#L302) parses the storage timestamps, requires the declared quiet-window duration, and rejects only an end time later than `now + 2 minutes`. It imposes no maximum past age. [independentFence](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/fencing_executor.go#L563) separately rechecks ledger hash and Pod inventory.
- **Rationale:** A reimplementer must guess whether “stale” means changed scope, expired observations, or both, and which timestamps receive format validation. Implementing a conventional freshness check would change accepted boundary inputs.
- **Small fix:** State the exact timestamp predicates, explicitly distinguish stale scope from observation age, and document that the top-level/barrier timestamps receive nonempty checks only. No runtime change is requested.

**Edge coverage**

| Assigned edge | Direct boundary checked |
|---|---|
| E09 | Portable recreation → immutable metadata → archived seal → snapshot HEAD; S-01 generation/hash conflict |
| E10 | Archive integrity versus Core authority; startup protection, generation-anchor verification, base installation and suffix replay |
| E11 | Exact WAL authority → Prepare → Open → Finalize → replay; import-journal cleanup ordering |
| E12 | Validated archive floor, prepared/sealed root, QLog Commit adoption versus Core installation; S-02 |
| E13 | Voter quorum versus learner sequential-source catch-up; durability/replay before readiness |
| E14 | Complete probe/status schema, bearer, nonce, MAC preimage, verification order, errors and deadlines |
| E15 | Lineage versus live anchor authority; request reconstruction, committed retry, PendingWrite exception, receipt verification |
| E16 | Ordinary shutdown versus TerminalDrain; captured bucket, identity-bound StorageKnown, owner poison and lock-release ordering |

**C7 verification**

Assigned constants and validation sites were checked:

- Portable checkpoint: 32 GiB, 64 MiB blocks, 512 blocks, 64 KiB root — `checkpoint.go` creation/read validators and `local.go`.
- Local/archive: 16 KiB descriptor, 4 MiB HEAD/snapshot, 8 MiB extent, 1–1024 decisions, 1–128 KiB values; binary HEAD 8,388,632 bytes.
- Leases: archive 2 minutes/40 seconds; fork 10 minutes/200 seconds.
- Local RPC: 30 seconds; release/readback independently 5 seconds; held gate has no expiry takeover.
- Fence: 64 targets, 10-second HTTP cap, 64 KiB response; independent response 512 KiB.
- Probe: exactly 32 decoded nonce bytes, client 3 seconds, handler 2 seconds, decoder reader 16 KiB.
- Checkpoint peer preparation: 5 minutes per remote call.

Error precedence, publication-before-error cases, cancellation boundaries and decoder-versus-body limits were checked alongside these constants.

**mistake_candidates**

- S-01/S-02 are documented current limitations, not new findings.
- Committed-anchor retry can succeed with PendingWrite; current C5 accurately records this.
- Probe 16 KiB is not a total-body/EOF check; current owner text accurately distinguishes it.
- Ordinary shutdown is not terminal-drain proof; current E16/C4 distinguishes them.
- QuePaxa baseline difference was not treated as a defect.

Read scope includes the full 105-line hub, index, assigned normative owner clauses and direct crossing source/validators. Truncated portions needed for the review were reread. E01–E08/E17 received hub reading only; their independent source coverage belongs to the other reviewers. Engine/provider internals remain excluded. No edits, builds, tests, delegation, memory or historical review reports were used.

### 3차 C

검토자: `01a18c09-9710-7ac3-bb75-c2c700791e38`.

## What this proposes

The hub documents 17 producer/consumer contracts across 11 Go areas. It connects request admission, consensus, application, receipts, checkpoint publication, archive recovery, and fencing while distinguishing their success and trust boundaries.

## How the existing system works, per this document

A mutation passes adapter validation and admission before independently owned consensus work can continue beyond caller cancellation. Decisions reach the materializer in order; application outcomes and retained receipts precede the configured durability barrier. Checkpoint publication and recovery require additional object, certification, lineage, ownership, and replay checks. Readiness, probe quorum, stored receipts, and successful shutdown each establish narrower facts than recovery authority or process termination.

**Verdict: no Blocker/Major/Nit findings within the assigned cross-area boundary.** No `R3-C-NN` finding IDs were issued.

Read scope is **complete within that boundary**: the index; the full 105-line hub, C0–C7; linked owning normative clauses for all 17 edges; relevant identity/hash and limit clauses; and direct source conditions needed to adjudicate boundary claims. Truncated output was not counted as completed reading; affected required portions were subsequently read in bounded ranges.

| Edge coverage | Comprehension and consistency result |
|---|---|
| E01–E02 | Adapter validation, SQL field precedence, admission/clone ownership, worker transfer, cancellation and unknown outcomes connect coherently. |
| E03–E05 | WAL framing versus semantic validation, recorder versus propagation quorum, Learned application-before-ACK, and deterministic rejection boundaries are distinguished. |
| E06–E08 | Paired snapshot ownership, immutable capture interval, provider conditions, candidate/prepare/seal ordering, archive durability and CURRENT promotion are distinguished. |
| E09–E12 | Local seal versus portable identity, protected recovery, exact-base installation, journal cleanup, suffix replay and partial compaction transition are documented consistently. |
| E13 | Learner single-source catch-up readiness is explicitly separated from voter quorum and voting authority. |
| E14 | Complete probe request/status schema, MAC serialization and verification, acceptance order, errors and deadlines were read and checked directly. |
| E15–E16 | Immutable lineage versus live anchor authority, committed-retry exception, ordinary shutdown versus terminal drain, and identity-bound storage evidence are consistent. |
| E17 | Retained status lacks a durability barrier; matching mutation retry waits again; expiry does not authorize safe re-execution. |

**C7 confirmation:** all 11 rows were checked against their owners and relevant source constants/validation sites:

| Boundary | Direct source checked |
|---|---|
| FFI / HTTP input | `cmd/rhiza-ffi/main.go`: input cap and timeout conversion; `pkg/network/server.go`: body cap and decoder |
| Mutation admission | `pkg/network/mutation_admission.go`: count, bytes, depth, nodes, rejection order |
| Batch / replicated value | `pkg/network/batcher.go`, `internal/types/value.go`, `pkg/quepaxa/core.go`: queue/batch limits and proposal/staging checks |
| Peer frame / page | `peer_codec.go`, `peer_bounds.go`, `peer_server.go`: frame/materialization budgets, Sync range and encoded-page fitting |
| Deadlines / catch-up | FFI timeout; transport ordinary/quorum/checkpoint paths; Node quorum/page contexts |
| Portable checkpoint | `pkg/checkpoint/checkpoint.go`: constants, file validation, post-upload aggregate/root checks |
| Local / archive bounds | `local_checkpoint.go`, recovery `local.go`, `archive.go`, `archive_codec.go`: descriptor/read limits, extent/value limits and binary HEAD calculation |
| Lease / mutation timeout | Archive lock renewal, fork evidence renewal, Local RPC dispatch and release/readback budgets |
| Fence / anchor limits | `fencing.go`, `fencing_attestor.go`: target capacity, timeout and response bounds |
| Recovery probe | Full handler/client/status/MAC paths; 16 KiB decoder reader correctly described as distinct from whole-body validation |
| Materialized SQL output | Materializer constants, row/cell/aggregate budgets and retained-result encoding limits |

`mistake_candidates: []`

Candidate concerns about empty SQL IDs, probe whole-body enforcement, committed-anchor retries with pending writes, and shutdown drain guarantees were resolved against the actual layer-specific checks; they are not findings.

Engine/provider/dependency internals and recursive dependency audits remain explicit scope exclusions. No edits, builds, tests, delegation, memory operations, or historical review reports were used. The differing QuePaxa baseline identifiers were not treated as a defect.

## 4차 독립 검토

A·C에는 지적이 없고 B에는 Major `R4-B-01`이 있어 이 회차도 clean이 아니다. A의 담당 경계는 3·4차에 연속 지적이 없었으며, 이 뒤 그 경계를 변경하지 않으면 판정을 유지한다.

### 4차 A

검토자: `01a18c11-6f45-76d2-94ed-fabc04dd408c`, `fork_context:false`.

Round4 A: **0 Blocker, 0 Major, 0 Nit findings.** No cross-area contradiction or unowned contract was established within the assigned scope.

**Read scope: complete within assigned scope.** Read the persona and standard first, the full [hub, lines 1–107](cross-area-go-contracts-2026-10-30.md?plain=1#L1), index, assigned original owner clauses, and direct source call/validation sites.

| Coverage | Result |
|---|---|
| E01–E08, E17 | Reviewed producer/consumer boundaries and success conditions |
| C2 | Checked identity distinctions, fingerprints, prefix hashes, checkpoint roots, archive/anchor hashes |
| C3 | Checked SQL admission → proposal → apply/receipt → durability → checkpoint certification/publication |
| C4 request cancellation/retry ownership | Checked lease transfer/release, detached batch/proposal contexts, ambiguity mapping, retained retry boundary |
| Assigned C7 rows, lines 95–99 and 105 | Checked constants, rejection predicates, deadlines, and relevant ordering |

Direct evidence included FFI dispatch/input handling; HTTP mutation, status, and error paths; batcher/admission ownership; materializer validation, application, receipts, output budgets, and snapshot capture; QuePaxa recorder/quorum/preparation paths; QLog framing/Append/Sync; peer Learned/Sync/frame validation; checkpoint upload/promotion; and Node archive-before-CURRENT publication.

The checked boundaries preserve these distinctions:

- Retained status visibility does not establish before-ACK durability.
- Identical retained mutation retries recheck the durability barrier.
- Learned ACK follows application through the requested slot.
- Candidate upload, checkpoint preparation, seal decision, archive durability, and CURRENT promotion are separate stages.
- Cancellation can end caller waiting while owned work continues.

E09–E16 were read in the full hub but receive no independent A verdict. Excluded engine/provider/dependency internals were not treated as missing reads. Historical review reports were not consulted as authority.

All commands used the explicit assigned cwd. No edits, builds, tests, memory operations, history inspection, or delegation occurred.

`mistake_candidates: []`

### 4차 B

검토자: `01a18c11-70dc-7350-9fc6-94c3a99a9944`, `fork_context:false`.

**Round 4 B: 0 Blocker, 1 Major, 0 Nit. No clean verdict.**

**R4-B-01 — Major: quiet-window duration overflow is omitted from the acceptance contract.**

- **Document:** hub [C5, line 81](cross-area-go-contracts-2026-10-30.md?plain=1#L81), C7 line 103, and operator owner [lines 158–160](operator-go-contracts-2026-10-30.md?plain=1#L158).
- **Source:** `QuietWindowSeconds` is an unrestricted `int` in [fencing.go:160](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/fencing.go#L160). Validation checks the minimum, then compares elapsed time against `time.Duration(QuietWindowSeconds)*time.Second` without checking overflow in [fencing_attestor.go:309](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/fencing_attestor.go#L309) and [line 322](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/fencing_attestor.go#L322).
- **Rationale:** on a 64-bit target, `quietWindowSeconds=4611686018427387904` passes the minimum check but its nanosecond multiplication wraps to zero. Equal, valid before/after timestamps therefore pass the duration predicate, provided the other evidence checks pass. The documented requirement that elapsed time cover the declared window—and its unconditional minimum-30-second implication—is stronger than actual acceptance.
- **Small fix:** document the exact duration conversion/multiplication and unchecked-overflow exception in the operator owner clause; qualify C5/C7 accordingly. This review requests documentation of current behavior, not a code change. The example is statically derived; no test was run.

**Read scope: complete within assigned scope.**

Read the persona and standard first, the index, and the full hub at freshly resolved lines **1–107**. Read assigned original owner clauses covering node lifecycle, operator fencing/anchor/attestation, HTTP probe schema, archive recovery/formats, checkpoint Local/limits, QLog compaction, QuePaxa recovery/durability, materializer exact-base restore, object-store drain/ownership, and peer catch-up.

Direct source reads covered the corresponding call, validation, constant, timeout, error, and ordering sites.

**Edge coverage:** E09–E16 all reviewed; C5/C6 and assigned C7 checkpoint/Local/archive/lease/fence/probe limits reviewed. No additional findings in those boundaries. E01–E08/E17 received full-hub reading only; their independent source coverage belongs to A/C. Excluded engine/provider/dependency internals are not missing assigned reads.

No edits, builds, tests, memory access, history inspection, or delegation. Historical review reports were not used as authority.

`mistake_candidates: []`

### 4차 C

검토자: `01a18c11-7261-71e2-aa11-afefde0d0f3a`, `fork_context:false`.

## What this proposes

The hub documents the current contracts connecting 11 Go areas through E01–E17. It assigns detailed behavior to the original owner clauses and explains where submission, consensus, application results, durability, restoration, and recovery authority have different success conditions.

## How the existing system works, per this document

HTTP and FFI mutations enter shared admission, cloning, validation, and retry handling before batch or proposal submission. QuePaxa decides values, QLog persists records, and the materializer applies contiguous decisions and produces retained outcomes; configured durability barriers govern acknowledgment separately. Checkpoint publication connects paired snapshots, object verification, preparation, seal decisions, archive durability, and CURRENT promotion. Recovery preserves ownership and pins while installing an exact base and replaying its suffix; operator fencing and live external-anchor verification supply separate authority.

**Verdict: no Blocker, Major, or Nit findings within assigned scope.**

- **Counts:** Blocker **0**, Major **0**, Nit **0**.
- **Read scope:** complete within assigned scope. Read the full hub, lines **1–107**, the index, assigned original normative boundary clauses, the complete HTTP probe appendix, and direct source call/validation sites needed to adjudicate boundary claims. Truncated required ranges were recovered.
- **Edge coverage:** **E01–E17, 17/17**—adapters/admission; proposal ownership; WAL; peer acknowledgment; materialization; snapshot capture; object-store conditions/readers; checkpoint certification/publication; Local backup; recovery protection; exact-base restoration; compaction; catch-up; probe; external anchor; shutdown/drain; receipt retry.
- **C7:** checked constants, rejection boundaries, timeout contexts, and ordering predicates across all 11 rows. This includes decoder-limit versus body-size semantics, cooperative probe deadlines, independent-attestation timestamp predicates, and Local release/readback budgets.
- **Contract assessment:** no unresolved boundary ambiguity established for inputs, outputs, side effects, success/error meanings, trust prerequisites, or context/resource ownership. Direct source supports both documented partial-failure scenarios, S-01 and S-02.

This is an independent static boundary review, not whole-engine or dependency-internal qualification. No edits, builds, tests, memory access, history inspection, or delegation occurred. Historical review reports were not used as authority.

`mistake_candidates: []`

## 5차 독립 검토

B의 Blocker 1건이 있으므로 clean이 아니다. C에는 지적이 없다. `R5-B-01`은 exported HTTP client 주입에서 음수 timeout을 그대로 두는 예외다. 원문에 인용된 Go 1.27.2 외에 현재 실행 도구체인 Go 1.27.3의 `net/http.Client.deadline`도 양수 timeout에만 deadline을 만드는 것을 통합자가 확인했다.

### 5차 B

검토자: `01a18c16-d518-7512-b270-aef662d44962`, `fork_context:false`.

Found **1 Blocker**, using the persona’s rule that a current-behavior claim contradicting source is a Blocker. No additional Major or Nit findings.

**R5-B-01 — Blocker: fencing HTTP’s 10-second cap has an undocumented exception**

- **Docs:** [hub C7, line 103](cross-area-go-contracts-2026-10-30.md?plain=1#L103) and [operator appendix, line 140](operator-go-contracts-2026-10-30.md?plain=1#L140) describe a 10-second HTTP timeout cap.
- **Source:** [cloneClient, lines 279–295](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/fencing.go#L279) replaces only zero or greater-than-10-second timeouts. An injected negative `http.Client.Timeout` survives. Both `FencingClient.Fence` and `HTTPSAttestor.Attest` use this helper. The HTTP client deadline implementation (local-only evidence: `/opt/homebrew/Cellar/go/1.27.2/libexec/src/net/http/client.go:199`) creates a client deadline only for positive timeouts.
- **Rationale:** With a negative injected timeout and an unbounded parent context, these calls have no client-enforced 10-second deadline. Reimplementing the documented unconditional cap would change current behavior. The normal CLI configuration supplies positive 10-second timeouts; this finding concerns the exported client-injection boundary.
- **Small fix:** Qualify C7 and the owner appendix: nil clients default to 10 seconds; zero or larger timeouts become 10 seconds; positive shorter values remain; negative values remain and disable the client timeout, leaving cancellation/deadlines to the caller context. This is a documentation correction, not a requested runtime change.

Coverage:

| Assigned boundary | Result |
|---|---|
| E09 | Checked Local portable creation → immutable metadata → archived seal → snapshot HEAD; S-01 generation/hash conflict matches source. |
| E10–E11 | Checked archive integrity versus Core authority, startup protection, exact-base restore ordering, suffix replay, and separate import-journal cleanup. |
| E12 | Checked archive trim prerequisites, prepared/sealed identity, QLog manifest adoption, Commit error return, and deferred Abort; S-02 matches source. |
| E13 | Checked voter quorum versus learner single-source catch-up, durable completion, replay, and readiness. |
| E14 | Checked complete probe/status schema, HMAC bytes, validation order, decoder limit, errors, and deadlines. |
| E15 / C5 | Checked lineage verification, reconstructed anchor request, live receipt verification, committed retry exception, manual fence assertion, and attestation predicates. |
| E16 | Checked ordinary shutdown’s conditional drain, terminal background drain, identity-bound `StorageKnown`, owner poisoning, and subsequent qlog release attempt. |
| C7 assigned limits | Checked checkpoint/Local/archive sizes, peer frame/page and catch-up deadlines, leases, Local RPC/release/readback budgets, fence capacities/transports, and probe limits. Exception above remains. |

**Actual read scope: complete within the assigned scope.** Read the full hub and index, assigned normative owner clauses across node, operator, HTTP probe, archive, checkpoint, materializer restore, QLog, QuePaxa, object-store, and peer documents, plus the direct call sites, validators, constants, errors, and deadline conditions supporting those claims. Required truncated boundary reads were recovered with bounded follow-up reads. Excluded engine/provider internals were not counted as missing reads.

No edits, tests, builds, history inspection, memory access, or delegation occurred. Historical review reports were not consulted. The baseline identifier difference was not treated as a defect. This is **not a clean verdict**.

`mistake_candidates: []`

### 5차 C

검토자: `01a18c16-d64d-7e72-b1a7-f18cb8b4dbcc`, `fork_context:false`.

## What this proposes

The hub connects eleven implementation areas through seventeen producer/consumer contracts. It identifies who owns each contract, which values cross each boundary, and what success, failure, cancellation, retry, and cleanup mean. Detailed behavior remains with the linked original owner clauses.

## How the existing system works, per this document

HTTP and FFI mutations enter the common Go API, pass admission and validation, and transfer work to a batch or proposal owner. Consensus certification, contiguous learning, materializer application, receipt visibility, and the configured durability barrier are separate completion points. Checkpoint capture, seal preparation/decision, archive durability, CURRENT promotion, restoration, and compaction likewise have separate authority and failure boundaries. Recovery combines protected evidence and exact identities; neither readiness, Pod deletion, a stored receipt, nor a clean storage drain independently proves complete fencing.

**Round5 C verdict: no new Blocker, Major, or Nit findings within the assigned scope.** No `R5-C-NN` IDs assigned. This is a scoped document/source review, not whole-engine approval or runtime qualification.

## Edge coverage

Each row was checked for inputs, outputs, side effects, ordering, success/error meaning, trust, context, and ownership where applicable.

| Edge | Boundary checked |
|---|---|
| E01 | FFI dispatch to common mutation API; mixed SQL fields and returning restrictions |
| E02 | Charge/reservation/clone, worker transfer, cancellation ambiguity and work deadlines |
| E03 | QuePaxa WAL payload versus QLog framing/CRC; Append versus Sync |
| E04 | Learned acceptance/application before ACK; propagation quorum versus recorder quorum |
| E05 | WaitTip and ordered ApplyBatch; rejected receipts versus infrastructure errors |
| E06 | Equal-tip SQLite/graph capture, fixed roles, caller cleanup and snapshot stability |
| E07 | Bucket capability/error classification, conditional-write meaning, content verification and reader Close |
| E08 | Candidate, validator/prepare, seal decision, archive durability and CURRENT promotion |
| E09 | Portable creation, immutable metadata, archived seal and HEAD publication; S-01 |
| E10 | Protected recovery evidence, archive integrity versus Core authentication, base installation and suffix replay |
| E11 | Exact-authority Prepare/Open/Finalize ordering; separate import-journal cleanup |
| E12 | Authoritative recovery floor and exact roots; QLog/Core partial transition and ineffective completed Abort |
| E13 | Voter quorum catch-up versus learner single-source catch-up, durability/replay and readiness |
| E14 | Authenticated GET probe, nonce/HMAC bytes, exclusion/identity/quorum checks, errors and deadlines |
| E15 | Immutable lineage versus live application authority; request reconstruction and live receipt verification |
| E16 | Conditional ordinary shutdown drain versus terminal drain; identity, bucket lifetime and lock-release behavior |
| E17 | Retained local outcomes, expiry, retry barrier and absence of a RequestStatus durability barrier |

The source specifically supports the important exceptions:

- Anchor `Activate` and `Verify` can succeed for the exact committed request while `PendingWrite` is true; application `CheckBinding` remains separate. [Coordinator](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recoveryanchor/coordinator.go#L224)
- Ordinary shutdown drains mutations only under `localOwner != nil`; terminal drain captures and drains the bucket separately. [Shutdown](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L2110), [TerminalDrain](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/fencing.go#L53)
- The automatic probe checks exclusion, nonce/MAC, expected identities and quorum; its decoder limit is not a whole-body/EOF validation. [Probe client](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/automatic_probe.go#L26)

## C7 checks

All eleven rows were checked against original owner clauses and relevant constants, validators, calls, or deadline construction.

| Boundary | Result |
|---|---|
| FFI input / HTTP body | 16 MiB / 1 MiB; input and output limits remain distinct |
| Mutation admission | 128, 32 MiB, 1 MiB charge, depth 32, nodes 65,536 |
| Batch / replicated value | 4,096 / 8 MiB queue, 8 batches, 64 SQL items, 128 KiB proposal limit |
| Peer frame / page | 1 MiB encoded frame, 1–128 items, 16,384 objects/slots |
| Call / catch-up deadlines | 30 s, 5 s, caller deadline, 5 min, 2 s and 250 ms at their stated boundaries |
| Portable checkpoint | 32 GiB total, 64 MiB blocks, 512 blocks, 64 KiB root |
| Local descriptor / archive | 16 KiB descriptor; 4 MiB JSON objects; stated extent/value limits; distinct binary HEAD bound |
| Lease / mutation timeout | Archive/fork renewal periods and Local 30 s RPC / separate 5 s release/readback budgets |
| External fence / anchor | 64 targets, 10 s / 64 KiB fencing response, 512 KiB attestation response |
| Recovery probe | Exactly 32 decoded nonce bytes, 3 s client / 2 s cooperative handler context, 16 KiB decoder reader |
| Materialized output | 10,000 rows, 16 MiB aggregate, 1 MiB cell and retained SQL result limits |

C5/C7 accurately describe the timestamp predicates, including unchecked duration multiplication. The validator checks the declared integer minimum, then compares elapsed time against the potentially wrapped duration; it does not establish an unconditional thirty-second elapsed minimum or maximum past observation age. [Validator](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/fencing_attestor.go#L302)

## Actual read scope

**Complete within the assigned scope:** full [hub, lines 1–107](cross-area-go-contracts-2026-10-30.md?plain=1#L1), full index, C0–C7, E01–E17, and their original normative owner boundary clauses—including the complete HTTP probe appendix. This included the relevant FFI, request/HTTP, materializer, checkpoint, object-store, archive/recovery, QuePaxa, QLog, peer, node-lifecycle and operator clauses and their identity/hash/format supplements.

Direct source reads covered the crossing calls, validators, constants, error mappings, deadlines, restore ordering, publication paths, anchor predicates, shutdown/drain paths, and both documented partial-failure scenarios. Required truncated excerpts were recovered with bounded rereads; source symbols were re-resolved rather than trusting stale link line numbers.

Excluded engine/provider/dependency internals were not counted as missing reads. No historical review report was consulted as evidence, and prior verdicts supplied no proof. No edits, tests, builds, history commands, memory access, or delegation occurred.

`mistake_candidates: []`

## 최종 고정 스냅샷 독립 검토 묶음 6·7

같은 명세에 대해 두 묶음을 병렬 실행했다. 둘 사이에 명세를 고치거나 다른 검토 결과를 제공하지 않았다. 각 묶음의 B는 변경된 복구 경계, C는 전체 흐름과 소유 규범 절을 확인했다. 두 묶음 모두 Blocker/Major가 없었다. 7차 C의 Nit 1건은 E03 Append 소스 링크의 행 번호이며 별도 보정 대상으로 남긴다. A 경계의 3·4차 clean 판정과 불변 비교는 통합 기록을 따른다.

### 최종 6 B

검토자: `01a18c1e-6622-73e1-aad2-e9b5fc2b8e09`, `fork_context:false`.

Batch6 B: **no findings**. Counts: **Blocker 0 / Major 0 / Nit 0**.

Reviewed current HEAD `008f8e5`, the full [hub](cross-area-go-contracts-2026-10-30.md), index, assigned original owner clauses, and direct source conditions. Required read scope is **complete within the assigned boundary**; truncated required reads were recovered.

| Coverage | Result |
|---|---|
| E09–E11 | Local publication ordering, portable identity conflict, recovery protection, exact-base restore and journal cleanup match source. |
| E12 | Archive authority, prepared/sealed-root requirements and QLog/Core partial transition match source. |
| E13 | Voter quorum catch-up and single-source learner readiness remain correctly distinguished. |
| E14 | Probe schema, HMAC, validation order, decoder limit and cooperative deadlines match source. |
| E15 / C5 | Anchor request reconstruction, committed retry exception, live verification and application-authority distinction match source. |
| E16 | Ordinary shutdown, terminal drain, storage-known result and owner/qlog-lock lifetimes match source. |
| C6 | S-01 and S-02 remain supported static derivations, without execution claims. |
| Assigned C7 | Checkpoint, Local/archive, lease, peer/catch-up, fence/attestation and probe limits match their direct validation predicates and constants. |

This conclusion covers the assigned crossings, not recursive engine/provider internals or overall EGM approval. The older QuePaxa baseline was not treated as a defect or prior-verdict evidence.

No edits, builds, tests, history inspection, memory access or delegation performed.

`mistake_candidates: []`

### 최종 6 C

검토자: `01a18c1e-6773-7cd2-84a1-8b152ab421d3`, `fork_context:false`.

## What this proposes

The hub connects 11 areas through 17 producer/consumer contracts. It assigns detailed behavior to the original owner clauses and explains the ordering, identifiers, ownership, trust assumptions, and success boundaries needed to follow a request through persistence and recovery.

## How the existing system works, per this document

HTTP and FFI mutations enter shared admission, cloning, validation, and proposal paths; QuePaxa decides values, and the materializer applies them in order. Application receipts, durability barriers, checkpoint publication, archive protection, restoration, and compaction complete at separate boundaries and can leave partial effects on failure. Recovery combines verified lineage and content with separately established fencing and live activation authority; readiness, stored receipts, and timeout results do not substitute for those checks.

**Batch6 C result: no findings.** Blocker **0**, Major **0**, Nit **0**.

| Coverage | Assessment |
|---|---|
| C0–C7 | Full hub read; index read; current clauses assessed independently |
| E01–E05 | Adapter → admission/proposal → WAL/peer → ordered apply boundaries checked |
| E06–E09 | Snapshot ownership, portable publication, certification, and Local immutable metadata checked |
| E10–E13 | Protected recovery, exact-base restore, compaction transition, and learner readiness checked |
| E14 | Full HTTP probe appendix and direct handler/client/HMAC predicates checked |
| E15–E17 | Live anchor verification, shutdown versus terminal drain, and retained-receipt retry boundaries checked |
| C7 | All rows checked against owner clauses and relevant direct constants, validation, error, or deadline predicates |

The assigned boundary read scope is **complete**: full hub, linked original normative owner clauses, and direct source needed to adjudicate the crossing claims. Required truncated reads were recovered. Excluded engine/provider/dependency internals are outside this verdict.

No unresolved ambiguity was established for boundary inputs, outputs, side effects, success/error behavior, trust, context lifetime, or ownership. S-01/S-02 and the documented anchor, attestation, and timeout exceptions remain descriptions of current behavior, not newly discovered documentation defects.

Strictly read-only: no edits, builds, tests, history, memory, or delegation. No other review report supplied evidence for this verdict. This is a scoped comprehension/boundary result, not whole-engine or reimplementation certification.

`mistake_candidates: []`

### 최종 7 B

검토자: `01a18c1e-69cd-70b2-bebc-bcb47fa28b35`, `fork_context:false`.

Batch7 B: **0 Blocker, 0 Major, 0 Nit.** No confirmed documentation discrepancy within the assigned boundary.

Read scope complete: full hub, index, assigned original owner clauses, and direct source calls, validation branches, constants, errors, and deadline predicates needed for E09–E16, C5, C6, and assigned C7 limits. Required truncated reads were recovered through focused reads. Excluded engine/provider internals were not treated as missing coverage.

| Coverage | Result |
|---|---|
| E09 | Local metadata → archived seal → HEAD ordering and portable-root retry conflict supported. |
| E10 | Archive integrity versus Core authority, startup protection, generation-base installation and suffix replay supported. |
| E11 | Exact-base prepare/open/finalize ordering and separate import-journal cleanup supported. |
| E12 | Archive floor prerequisites and QLog/Core partial transition supported. |
| E13 | Voter quorum versus single-source learner catch-up and readiness distinction supported. |
| E14 | GET, nonce, exclusion, HMAC, identity/quorum acceptance and decoder/deadline qualifications supported. |
| E15 | Request reconstruction, live receipt verification and immutable lineage distinction supported. |
| E16 | Conditional ordinary drain, terminal drain, identity-bound `StorageKnown`, and lock-release behavior supported. |
| C5 | Committed retry precedes `PendingWrite` rejection; Verify and CheckBinding remain distinct. Timestamp and overflow qualifications match source. |
| C6 | S-01 and S-02 remain supported static scenarios, without execution claims. |
| C7 | Assigned checkpoint, Local/archive, lease, fence and probe bounds match their direct validation points. |

Key adjudication sources included [Local publication](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L533), [QLog adoption](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L1232), [Core Commit boundary](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/compaction.go#L349), [committed-anchor predicate](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recoveryanchor/coordinator.go#L224), and [attestation duration predicate](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/fencing_attestor.go#L302).

This is an independent, boundary-scoped static result—not whole-engine or execution qualification. No edits, builds, tests, history inspection, memory access, delegation, or other review-report reads were performed.

`mistake_candidates: []`

### 최종 7 C

검토자: `01a18c1e-6e25-73f1-ab11-1e770b9907ac`, `fork_context:false`.

## What this proposes

The hub connects eleven owner specifications through E01–E17 without introducing new runtime behavior. It explains which component owns each input, validation, output, side effect, and success boundary, then follows SQL submission through consensus, materialization, durability, checkpoint publication, and recovery.

## How the existing system works, per this document

HTTP and FFI reach shared mutation APIs, which reserve capacity, clone requests, and transfer work to batch/proposal owners. Consensus certification, contiguous tip, application outcome, and durability-barrier completion are distinct; cancellation can leave work running, and receipt lookup does not establish durability. Recovery combines protected archive/checkpoint evidence, Core authority validation, exact store installation, and ordered suffix replay; fencing and live application-anchor authority remain separate prerequisites.

## Findings

**R7-C-01 — Nit — E03’s `Append` source link lands on another function.**

- Document: [hub, line 19](cross-area-go-contracts-2026-10-30.md?plain=1#L19).
- Source: its `Append` link targets [wal.go:500](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L500), which starts `createSegment`. Current `Append` starts at [wal.go:662](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L662).
- Rationale: the behavioral explanation is supported, but the labeled evidence link sends a fresh reader to rollover implementation instead of the append boundary.
- Small fix: change that source link’s line suffix from `:500` to `:662`.

**Counts: Blocker 0; Major 0; Nit 1.** No behavioral or comprehension Blocker/Major established within the assigned boundary.

## Edge coverage

| Edge | Independently checked boundary |
|---|---|
| E01 | Strict FFI dispatch; shared API; statements precedence; returning rejection |
| E02 | Reservation/clone ownership transfer; detached work; cancellation ambiguity |
| E03 | WAL payload versus framing responsibility; Append versus Sync |
| E04 | Historical/current authorization; Learned apply-before-ACK; propagation quorum |
| E05 | WaitTip and ordered materialization; rejected receipts and advancing tip |
| E06 | Equal-tip two-store snapshot; cleanup ownership; stable upload inputs |
| E07 | Provider capabilities, condition classification, reader ownership, content validation |
| E08 | Candidate/prepare/seal/archive/CURRENT ordering; promotion’s limited validation |
| E09 | Local metadata/seal/HEAD publication; portable-generation retry mismatch |
| E10 | Archive integrity versus Core authority; protection and recovery ordering |
| E11 | Exact authority journal; installation/finalization before suffix replay |
| E12 | Prepared/sealed compaction prerequisites; QLog/Core partial transition |
| E13 | Voter quorum versus learner single-source catch-up and readiness |
| E14 | Complete HTTP probe appendix: schema, HMAC, acceptance, errors, deadlines |
| E15 | Request reconstruction; committed retry; live receipt verification; application authority |
| E16 | Conditional ordinary drain; terminal drain identity and background lifetime |
| E17 | Retained outcome versus durability; retry barrier; expiry ambiguity |

## C7 checks

All thirteen rows were checked against their owner clauses and relevant direct constants/predicates:

- FFI/HTTP input limits.
- Mutation count, bytes, charge depth, and nodes.
- Queue, concurrent batches, SQL items, and replicated-value limits.
- Peer frame, Sync page, and materialization bounds.
- FFI, peer, checkpoint, and catch-up deadlines.
- Portable checkpoint file/block/root limits.
- Local descriptor and HA/Local archive limits.
- Lease renewal and Local RPC/release/readback budgets.
- Fence capacity, response limits, injected-client timeout branches, and attestation timestamp/duration predicates.
- Probe nonce, decoder-read limit, and cooperative handler deadline.
- Materialized result and retained-result limits.

The negative HTTP-client timeout branch and positive-only standard-library deadline predicate agree with the current wording. The quiet-window overflow example follows the actual unchecked predicate; it was not executed.

## Actual read scope and completion

**Complete within the assigned boundary:** full hub C0–C7, index, linked original normative owner boundary clauses across all eleven areas, relevant format/contract appendices, and direct crossing-call, validation, constant, error, and deadline source predicates. Required truncated passages were recovered through narrower reads.

Excluded whole-engine/provider/dependency internals are outside this assignment, not missing target reads. Historical verdicts and concurrent-batch conclusions were not used as proof. HEAD was `008f8e5` at both checks; the older QuePaxa baseline was not treated as a defect.

No edits, builds, tests, history inspection, memory access, or delegation occurred.

## 초기 W 검토

검토자: `01a18bf5-38f7-74b0-b5af-32ab81990eb7`, `fork_context:false`.

Found **2 Blockers and 1 Major** in the assigned mutation lane. All require prose corrections; no runtime changes are proposed.

**W-01 — Blocker: FFI SQL schema implies mutually exclusive input forms**

- **Document clauses:** [c-ffi contracts:53](c-ffi-go-contracts-2026-10-30.md?plain=1#L53) says `ExecuteRequest` contains either legacy fields or statements. [request-http contracts:16](request-http-go-contracts-2026-10-30.md?plain=1#L16) explicitly permits their coexistence.
- **Source adjudication:** FFI `dispatch` decodes the same request and calls `DB.Execute` at [main.go:412](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/cmd/rhiza-ffi/main.go#L412). `ValidateSQLCommand` selects legacy SQL only when `len(Statements)==0` at [materializer.go:1653](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1653); `executeSQLCommand` follows the same precedence at [materializer.go:1847](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1847).
- **Required correction:** State that mixed fields are accepted, nonempty `statements` supplies the executed SQL/arguments, and returning methods reject nonempty statements. Preserve the additional distinction that unused legacy fields still participate in `SQLFingerprint`, through [value.go:290](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/types/value.go#L290) and `EncodeSQLBatchItem`. Removing them on retry can therefore cause a conflict.
- **Classification:** Document error. A replacement must not introduce exclusive-form validation.

**W-02 — Blocker: Go cancellation clause loses commit ambiguity**

- **Document clauses:** [c-ffi contracts:75](c-ffi-go-contracts-2026-10-30.md?plain=1#L75) promises that cancellation returns the underlying context error. [request-http main:76](request-http-go-reverse-spec-2026-10-30.md?plain=1#L76) correctly distinguishes cancellation before admission from cancellation after submission.
- **Source adjudication:** `mutationBatcher.submitEncoded` returns `CommitUnknownError` when caller cancellation wins after queue submission at [batcher.go:141](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/batcher.go#L141). Its `Unwrap` exposes **both** `ErrCommitUnknown` and the context cause at [server.go:59](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L59). FFI `errorCode` prioritizes `commit_unknown` over `timeout` and `canceled` at [main.go:624](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/cmd/rhiza-ffi/main.go#L624).
- **Required correction:** Specify stage-dependent cancellation. Before submission, a context error can return directly; afterward, the error can preserve the context cause while carrying commit ambiguity. FFI then returns `commit_unknown`; detached work may continue. Keep the original request ID and retention-window qualification.
- **Classification:** Document error. Do not flatten the existing wrapper into a raw context error.

**W-03 — Major: FFI status operation overstates durability**

- **Document clauses:** [c-ffi main:72](c-ffi-go-reverse-spec-2026-10-30.md?plain=1#L72) describes `request_status` as reporting a “durable request outcome.” [request-http contracts:45](request-http-go-contracts-2026-10-30.md?plain=1#L45) explicitly says committed status is not a durability proof.
- **Source adjudication:** FFI delegates directly at [main.go:493](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/cmd/rhiza-ffi/main.go#L493). `Server.RequestStatus` reads the materialized receipt and tip without a consensus or durability barrier at [server.go:1672](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1672). Mutation execution applies before waiting for configured durability in `runProposalAttempt` at [server.go:923](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L923).
- **Required correction:** Describe a **locally retained materialized outcome**. Explicitly exclude proof of the configured before-ACK durability barrier. Explain that an identical mutation retry rechecks that barrier, while status lookup does not.
- **Classification:** Missing cross-area qualification. The lookup’s weaker behavior should be documented, not changed.

Each lane edge reviewed:

| Edge | Result |
|---|---|
| HTTP / FFI → shared SQL request validation | W-01 |
| Caller cancellation → batch/proposal ownership → FFI errors | W-02 |
| Materialized receipt → status lookup → durability/retry | W-03 |
| JSON/blob encoding → fingerprints → replicated-size limits | No additional finding |
| Charge/reservation → cloning → lease transfer/release | No finding |
| Batching → replay deduplication → retained receipts | No finding; documented kind-specific replay differences remain implementation limitations |
| QuePaxa proposal success → QLog Append/Sync → local application | No finding; these completion boundaries remain distinct |
| QuePaxa ↔ peer Learned ACK → materializer | No finding; current source applies before remote ACK |
| Go receipts → C envelope → Rust result handling | No additional finding; rejected receipts require explicit status inspection |

Read scope: persona and standard first; the index and all 11 areas’ main/contract/format documents in full; additionally QuePaxa protocol and durability evidence. Historical review logs and memory were not consulted. Source inspection stayed within the assigned mutation boundary and relevant adjacent functions.

HEAD was verified as `008f8e576ad76425182e10a69b987f10cfb48b2a`. QuePaxa’s explicit `8acb` baseline was respected; no finding rests solely on baseline differences. Broader engine internals and other areas’ source contracts were not exhaustively audited.

No writes, builds, tests, commits, delegation, or environment qualification were performed. No finding treats an unperformed test as a failure.

`mistake_candidates: []`

## 초기 S 검토

검토자: `01a18bf5-3ac0-7671-a80f-a59191d42bb2`, `fork_context:false`.

Found **2 Major cross-area contract omissions**. No Blockers or Nits established. Source HEAD is `008f8e576ad76425182e10a69b987f10cfb48b2a`.

**S-01 — Major — Local checkpoint retry can conflict with already-published immutable snapshot metadata**

Both document clauses:

- [checkpoint main:100](checkpoint-go-reverse-spec-2026-10-30.md?plain=1#L100): Local creation has no CURRENT dedup; an existing base block causes generation-1 fallback.
- [archive-recovery main:273](archive-recovery-go-reverse-spec-2026-10-30.md?plain=1#L273), together with [formats:123](archive-recovery-go-formats-2026-10-30.md?plain=1#L123): immutable snapshot metadata binds a seal-root key to a portable root and requires exact readback.

The missing crossing contract is that **repeating publication of the same Local checkpoint need not reproduce its portable identity**.

Source evidence:

- `Manager.createLocalFiles` always uploads with generation 1 and no known-block map: [local.go:43](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/local.go#L43).
- `Manager.uploadFile` initially uses generation 0, but switches to generation 1 after a conditional collision: [checkpoint.go:784](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L784), [checkpoint.go:804](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L804).
- `fileDescriptorHash` includes block generation: [checkpoint.go:1630](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1630).
- `uploadLocalBackupSnapshot` recreates the portable checkpoint before storing metadata: [local_backup.go:550](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L550).
- `StoreLocalSnapshot` rejects different bytes under the existing seal-root key with `Local snapshot publication mismatch`: [local.go:709](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L709).
- Prepared-checkpoint resumption calls publication again: [node.go:1349](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L1349).

Concrete boundary: with initially absent block keys, the first upload can publish generation-0-backed metadata. If execution stops after metadata publication but before compaction, resumption encounters those blocks, generates different portable descriptor hashes, and conflicts with the immutable metadata.

**Required prose correction:** document this conditional retry failure, its persistence across reopen, and the distinction between unchanged Local seal identity and changed portable identity. This is a **current code weakness that the specification must preserve**, not a request for a runtime fix. The scenario is statically derived, not executed.

**S-02 — Major — Compaction failure lacks an explicit QLog-to-Core state contract**

Both document clauses:

- [QLog main:167](qlog-go-reverse-spec-2026-10-30.md?plain=1#L167): a post-rename directory-sync failure adopts the new segment list while retaining fatal state.
- [QuePaxa durability:114](quepaxa-go-durability-evidence-2026-10-30.md?plain=1#L114), followed by [118](quepaxa-go-durability-evidence-2026-10-30.md?plain=1#L118): describes final validation and base installation, but omits the intervening partial transition.

Source evidence:

- `publishManifestLocked` advances generation after publication and records fatal durability uncertainty: [wal.go:581](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L581).
- `Compaction.Commit` adopts the new segments, marks the operation done, then returns the publication error: [wal.go:1232](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L1232).
- `Core.compactThroughOwned` returns immediately on that error, before `installVerifiedBaseLocked`: [compaction.go:349](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/compaction.go#L349).

Consequently, an error can leave **QLog selecting the compacted generation while the live Core still exposes its previous floor and retained state**. Deferred `Abort` does not reverse the completed QLog transition.

**Required prose correction:** add this outcome to the QuePaxa compaction contract and cross-reference QLog’s uncertainty/reopen rules. Distinguish manifest selection, durability confirmation, and Core base installation; a failed call does not establish that the old WAL remains selected. This is a documentation omission around implemented failure behavior.

Lane edges reviewed:

| Edge | Result |
|---|---|
| Materializer capture → checkpoint roles/files | 0 additional findings |
| Checkpoint blob/hash/name → object-store conditional writes | 0 additional findings |
| Local portable root → immutable archive snapshot → prepared resume | **S-01** |
| Checkpoint preparation → seal → archive durability → CURRENT | 0 additional findings |
| Archive trim → Core compaction → QLog manifest | **S-02** |
| Archive/root pins → restore → GC | 0 additional findings |
| Local restore authority/journal → suffix replay → deferred cleanup | 0 additional findings |
| Receipt replay/durability → crash or whole-loss boundary | 0 additional findings |
| Missing objects/uncertain mutations → maintenance and terminal drain | 0 additional findings |

Read scope: persona and standard first; index; all eight primary main/format/contract documents; materializer and node-lifecycle main/contract documents; QuePaxa main, formats, and durability evidence. Source inspection covered the relevant crossing functions cited above, startup/live restore, Local pin/GC, checkpoint verification, and mutation tracking.

Limitations: this was the assigned lane review, not an audit of all 11 areas or every engine internal. Historical review logs and memory were not consulted. No writes, builds, tests, commits, or delegation occurred. QuePaxa’s explicit `8acb` baseline was not itself treated as a defect; findings were checked against current crossing source. No environment-test finding is raised.

`mistake_candidates: []`

## 초기 L 검토

검토자: `01a18bf5-3d44-7171-abda-468076bb3185`, `fork_context:false`.

Found **5 cross-area documentation findings: 4 Blocker, 1 Major**. “Blocker” follows the supplied persona’s rule for prose that contradicts current code; it does not imply a demonstrated runtime vulnerability.

**L-01 — Blocker — C-FFI publishes an invalid durability literal**

- **Both clauses:** [c-ffi contracts:30](c-ffi-go-contracts-2026-10-30.md?plain=1#L30) lists `before_ack`; [node-lifecycle contracts:33](node-lifecycle-go-contracts-2026-10-30.md?plain=1#L33) specifies `before-ack`.
- **Source:** [`ObjectStoreDurabilityBeforeAck`, config.go:27](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/types/config.go#L27) is `"before-ack"`. [`Node.open`, node.go:233](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L233) rejects other nonempty values.
- **Correction:** Replace `before_ack` with `before-ack` in the C-FFI appendix. A host following that appendix currently supplies a configuration that opening rejects.
- **Classification:** Document error; no runtime correction requested.

**L-02 — Blocker — Readiness restoration omits the non-voting learner exception**

- **Both clauses:** [node-lifecycle main:128](node-lifecycle-go-reverse-spec-2026-10-30.md?plain=1#L128) says only a fresh quorum round reasserts readiness; [peer-transport main:148](peer-transport-go-reverse-spec-2026-10-30.md?plain=1#L148) describes the admin-readable Sync transfer used by learners.
- **Source:** [`Node.catchUpMembership`, node.go:1768](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L1768) sends voters through quorum catch-up, but a configured non-voting learner succeeds after **one peer** catches it up, followed by `EnsureDurableThrough` and replay. [`Node.observeCatchUp`, node.go:1874](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L1874) sets readiness on that success. The not-ready recovery loop uses this same dispatch.
- **Correction:** Restrict the quorum-only statement to voters. Explicitly document the learner’s sequential single-source Sync path, durable-prefix/replay completion, and readiness restoration. Readiness still grants neither voting authority nor quorum proof.
- **Classification:** Document error about current behavior, not a proposed strengthening of learner admission.

**L-03 — Blocker — Shutdown overstates the dependency between filesystem-owner drain and qlog-lock release**

- **Both clauses:** [node-lifecycle main:163](node-lifecycle-go-reverse-spec-2026-10-30.md?plain=1#L163) says the lock releases only after filesystem ownership drains; [object-store main:76](object-store-go-reverse-spec-2026-10-30.md?plain=1#L76) correctly describes timeout/poison retaining filesystem-owner locks.
- **Source:** [`LocalFilesystemOwner.Close`, local_owner.go:117](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner.go#L117) can fail with active operations or poisoned ownership. [`Node.shutdownEngine`, node.go:2139](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L2139) joins that error but continues to [`n.lock.Release`, node.go:2148](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L2148).
- **Correction:** Distinguish the locks: shutdown **attempts** filesystem-owner close before attempting qlog-lock release; owner-close failure does not suppress the latter attempt. The filesystem owner can remain retained after shutdown returns an error.
- **Classification:** Current implementation limitation hidden by stronger prose. Document the existing behavior; this review requests no runtime fix.

**L-04 — Major — External-anchor receipt semantics fall between the operator and archive specifications**

- **Both clauses:** [operator contracts:194](operator-go-contracts-2026-10-30.md?plain=1#L194) names `Activate`/`Verify` and the backend, while excluding dependency schemas at line 213; [archive-recovery main:237](archive-recovery-go-reverse-spec-2026-10-30.md?plain=1#L237) defines immutable generation-lineage verification. Neither supplies the semantic bridge from that lineage to live external-anchor activation authority.
- **Source:** [`Controller.buildAnchorRequest`, controller.go:987](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/controller.go#L987) constructs the request and independently hashes the fence assertion. [`RequestHash`, types.go:66](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recoveryanchor/types.go#L66) hashes the **entire request**, including `FenceHash`. [`Coordinator.Activate`, coordinator.go:137](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recoveryanchor/coordinator.go#L137) rejects pending application writes and reserves `Transition`. [`committed`, coordinator.go:224](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recoveryanchor/coordinator.go#L224) and [`Verify`, coordinator.go:260](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recoveryanchor/coordinator.go#L260) bind the receipt to the request and live binding/generation.
- **Missing contract:** Add a concise boundary contract stating those rules, plus request reconstruction/equality and live verification in Starting/Restarted, including immediately before successor PUT ([controller.go:455](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/controller.go#L455)). Explicitly separate immutable archive lineage from the external application anchor. A persisted receipt alone grants no activation or future-write authority.
- **Classification:** Missing documentation; the inspected implementation contains these checks. Reproducing every dependency schema is unnecessary.

**L-05 — Blocker — Recovery-probe method contradicts the operator’s actual request**

- **Both clauses:** [request-http main:33](request-http-go-reverse-spec-2026-10-30.md?plain=1#L33) inventories `/recovery/probe` as POST; [operator main:180](operator-go-reverse-spec-2026-10-30.md?plain=1#L180) relies on authenticated workload quorum probes without specifying their method.
- **Source:** [`Controller.automaticQuorumProbe`, automatic_probe.go:28](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/automatic_probe.go#L28) sends **GET**, with `nonce` and `exclude` query parameters. [`Server.handleRecoveryProbe`, recovery_probe.go:40](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/recovery_probe.go#L40) rejects other methods.
- **Correction:** Move `/recovery/probe` into the GET inventory and identify that GET/query contract at the operator boundary. Following the current POST inventory produces method rejection.
- **Classification:** Document error; no runtime correction requested.

**Lane edges reviewed**

“Zero” means no additional cross-area finding within the inspected boundary.

| Lane edge | Result |
|---|---|
| C-FFI configuration → node/operator durability | L-01 |
| Node readiness → learner Sync and durable replay | L-02 |
| Node shutdown → object-store filesystem owner → qlog lock | L-03 |
| Operator activation → archive lineage → external-anchor receipt | L-04 |
| Operator quorum probing → request-http recovery probe | L-05 |
| Node startup → voter registration/WAL identity | Zero |
| Node generation installation → archive/checkpoint lineage | Zero |
| Node terminal drain → supervisor acknowledgement/operator fence proof | Zero; storage-known drain is distinguished from process termination and recreation prevention |
| Membership admission → learner WAL/prefix probe → voter activation | Zero beyond L-02 |
| Historical/current membership → peer certificate authorization | Zero |
| QuePaxa completion → ordinary/freeze/terminal transport ACK quorums | Zero |
| Catch-up → peer frame/page limits, deadlines and retry behavior | Zero beyond L-02 |
| Node checkpoint publication → certified archive → compaction | Zero |
| Local lifecycle → HTTP quiescence and sticky failure | Zero |
| C-FFI close → Go DB close → node/peer shutdown | Zero |

**Read scope and limitations**

Read the supplied persona and standard first; the index; all 11 indexed main/contract-or-format pairs; QuePaxa membership and durability evidence; and selected protocol-evidence sections. Historical per-area review logs were not consulted as authority.

Source inspection was limited to the crossing symbols and adjacent control flow cited above: node startup/catch-up/shutdown/fencing/identity, peer authorization/transfer/quorum handling, operator activation/probing/fence validation, external-anchor coordination, filesystem ownership, HTTP lifecycle, and C-FFI/Go close. Engine internals and provider SDK internals were not exhaustively audited. QuePaxa’s explicit `8acb` baseline was preserved; historical differences alone were not findings.

No writes, builds, tests, commits, history/memory retrieval, or delegation occurred. No environment-test absence was reported as a defect.

`mistake_candidates`:

- `MC-01`: Some initial searches guessed nonexistent source filenames. Existing implementations were subsequently located; missing guessed paths were not treated as findings.
- `MC-02`: Oversized document reads produced truncated output. Bounded follow-up reads recovered the omitted sections before conclusions were finalized.

## 소스 기준 및 기존 링크 확인

검증자: `01a18bf7-af36-73f1-81c8-33a3044a1c7f`.

Baseline compatible. **No current QuePaxa overlay is required from this commit comparison.**

- HEAD is `008f8e576ad76425182e10a69b987f10cfb48b2a`.
- Diff from `8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd` contains only six added QuePaxa Markdown documents: 1,272 added lines.
- Non-document diff is empty; current tracked non-document content also matches HEAD. Consequently, no source-contract change affects crossing clauses. Preserve the historical QuePaxa baseline.

| Area | Main | Contracts/formats | Review present |
|---|---|---|---|
| QuePaxa | ✓ | ✓ | ✓ |
| QLog | ✓ | ✓ | ✓ |
| Checkpoint | ✓ | ✓ | ✓ |
| Object store | ✓ | ✓ | ✓ |
| Archive/recovery | ✓ | ✓ | ✓ |
| Request/HTTP | ✓ | ✓ | ✓ |
| Materializer | ✓ | ✓ | ✓ |
| Node lifecycle | ✓ | ✓ | ✓ |
| Operator | ✓ | ✓ | ✓ |
| Peer transport | ✓ | ✓ | ✓ |
| C FFI | ✓ | ✓ | ✓ |

Link validation covered **26 files**: 22 normative main/appendix documents, three QuePaxa evidence appendices, and the hub.

- **895 local target occurrences:** all exist; numeric line suffixes are within target-file bounds.
- **192 actual reference-style uses:** all have definitions. One regex-shaped candidate at operator main line 31 is inline code, not a link.
- **Broken links: none found.**

Method: read-only Git diffs, in-memory R extraction of inline links/reference definitions, filesystem existence checks, and line-count checks. Gaps: no exhaustive Markdown-parser/fragment validation or semantic verification of every cited line; review-document outbound links were outside the normative link scan. No broad W/S/L audit, runtime tests, builds, writes, memory retrieval, or delegation performed.

`mistake_candidates: []`
