# 나머지 다섯 영역 EGM 독립 검토 원문

기준 HEAD: `008f8e576ad76425182e10a69b987f10cfb48b2a`.

각 검토자는 `fork_context:false`로 시작했다. 아래는 보정 전 발견 사항이며, 최종 판정은 각 영역의 `*-go-egm-review-2026-10-30.md`에 기록한다. 이 검토는 전체 5단계 승인 또는 무가정 재구현 인증이 아니다.

## 보정 후 최종 재검토 원문

초안의 지적을 보정한 뒤 같은 독립 검토자에게 해당 지적의 코드 대조를 다시 요청했다. 아래 판정은 발견한 지적의 해소에 한정된다. 중복 지적을 포함한 검토별 집계는 12건, 9건, 7건이며 고유 결함 28건을 뜻하지 않는다.

### Materializer·C FFI 최종

검토자: `01a18be7-f2f5-7943-b468-dc9cb01be18d`.

Confirmed: original **#4 is resolved**. [Line 121](materializer-go-reverse-spec-2026-10-30.md?plain=1#L121) now says “upstream-populated,” consistent with the source previously checked.

**Final bounded outcome: all 12 original findings resolved; none outstanding.** This closes those findings only. Previous coverage limitations remain; no broader audit, edits, or tests were performed.

### Node·Operator·Peer 최종

검토자: `01a18be8-9217-70a1-bc39-8fad3c505002`.

**Original #3 is resolved. All nine original findings are now resolved; no blockers remain from that set.**

The revised [stage/effect mapping:54](operator-go-reverse-spec-2026-10-30.md?plain=1#L54) matches the controller: reservation → seal → preserved target evidence → source termination → successor activation → completion.

This recheck read only that passage and corresponding sections of [controller.go](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/controller.go#L238). Findings #1–2 and #4–9 retain their previously verified resolved status. No broader audit, edits, or tests.

### 이해도 최종

검토자: `01a18be8-9396-7ef2-a1ee-657575269cc2`.

**All seven original findings are resolved.**

The final exact-hash correction matches source: literal NUL-terminated domains, full struct order/types/tags, Go `json.Marshal` bytes, both hash fields retained as empty strings, and ordered state-hash components with big-endian lengths and textual digests. [Corrected contract:99](node-lifecycle-go-contracts-2026-10-30.md?plain=1#L99), [source domains:55](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_checkpoint.go#L55), [source hashing:495](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_checkpoint.go#L495).

| Original finding | Final outcome |
|---|---|
| Stream second return | Resolved |
| Node `Start` cleanup | Resolved |
| Exact checkpoint hashes | Resolved |
| Operator persisted/proof schemas | Resolved |
| Literal peer ALPN | Resolved |
| C FFI KV spelling | Resolved |
| C FFI empty KV result | Resolved |

Scope: this pass checked only the remaining hash gap against source; the other six retain their previous rereview outcomes. No remaining gaps within these seven findings. No edits, tests, or broader audit; no full-system certification.

## 보정 과정의 추가 지적

첫 재검토에서는 Materializer의 `apply-populated` 잔존 문구, Node의 도메인 바이트·JSON 직렬화 규칙, Operator의 단계별 부수 효과 순서가 남아 있었다. 이를 추가 보정한 후 위 최종 판정을 받았다.


## Materializer·C FFI 코드 대조

검토자: `01a18be7-f2f5-7943-b468-dc9cb01be18d`.

Verified HEAD: `008f8e576ad76425182e10a69b987f10cfb48b2a`. **All four documents need corrections; none receives a clean result.** Severity follows the supplied persona: a statement contradicted by current code is a **Blocker**.

**Materializer reverse specification — Blocker**

1. **Blocker — Stream cursor is misidentified.** [Doc lines 148–151](materializer-go-reverse-spec-2026-10-30.md?plain=1#L148) describe `GraphReadStream`’s second return as the stream high-water sequence. It is the materializer’s applied slot, captured as `m.tip`.
   **Correction:** document `(records, appliedSlot, error)`; callers must not use that slot as their stream cursor. Evidence: [implementation lines 1094–1105](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/graph_enabled.go#L1094).

2. **Blocker — Apply does not uniformly enforce fingerprint conflicts.** [Doc lines 62–66](materializer-go-reverse-spec-2026-10-30.md?plain=1#L62) promise a hard conflict for another intent using the same ID. SQL apply skips existing/pending IDs without comparing fingerprints; KV and notify likewise skip existing receipts. Graph does compare fingerprints.
   **Correction:** separate admission/status matching from replay-time behavior, explicitly describing each mutation kind. Evidence: [SQL duplicate handling](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1359), [KV duplicate handling](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1540), [notify duplicate handling](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1320), [graph comparison](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/graph_enabled.go#L558).

3. **Blocker — SQL `Applied` semantics are false.** [Doc lines 97–99](materializer-go-reverse-spec-2026-10-30.md?plain=1#L97) say applied mutations set `Applied=true`. SQL constructs a committed receipt without setting that field; successful SQL therefore retains false. KV and graph explicitly set it.
   **Correction:** specify SQL’s actual field behavior separately. Evidence: [SQL receipt construction and aggregation](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1397).

4. **Blocker — Expiry and Bloom-filter responsibilities are misstated.** [Expiry section](materializer-go-reverse-spec-2026-10-30.md?plain=1#L117) implies materializer apply derives expiry from TTL. `applyKV` instead writes the supplied absolute expiry and uses supplied observation time; it does not compute TTL. [Receipt section](materializer-go-reverse-spec-2026-10-30.md?plain=1#L173) calls the Bloom filter expired-ID replay protection, but it only selects whether SQLite lookup might be necessary.
   **Correction:** assign timestamp population to the upstream boundary and describe the filter as a lookup optimization, not durable deduplication. Evidence: [KV apply](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1545), [Bloom lookup](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L579), [receipt pruning](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1052).

5. **Blocker — Restore failure does not guarantee the original remains open.** [Doc lines 186–188](materializer-go-reverse-spec-2026-10-30.md?plain=1#L186) promise reopening/preservation. Rollback can itself fail to recover or reopen and returns joined errors. After the journal reaches `committed`, recovery retains the replacement; a subsequent cleanup failure can therefore return an error with the replacement selected.
   **Correction:** distinguish precommit rollback, committed replacement with cleanup failure, and failed reopening. Evidence: [joined rollback errors](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L2807), [commit/finalization](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L2899), [phase-dependent recovery](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L2709).

**Materializer contracts — Blocker**

6. **Blocker — Exact replay rule contradicts older-slot handling.** [Doc lines 116–117](materializer-go-contracts-2026-10-30.md?plain=1#L116) require matching hashes for replay generally. `ApplyBatch` checks the hash only at the current tip; older slots are skipped after the initial execution-policy validation.
   **Correction:** distinguish older-slot skip, current-tip hash check, and pending graph recovery. Evidence: [implementation](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1146).

7. **Blocker — Local pair publication is described as atomic.** [State machine lines 145–153](materializer-go-contracts-2026-10-30.md?plain=1#L145) compresses staged→installed into atomic pair publication. Actual publication renames graph before SQLite and persists intermediate phases; validation and index reconciliation precede `installed`.
   **Correction:** document restart handling for `backup-sql`, `backup-graph`, `publish-graph`, and `published`, including partial filesystem state. Evidence: [recognized phases](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/local_restore.go#L379), [publication](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/local_restore.go#L789), [validation/install transition](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/local_restore.go#L175).

8. **Major — Retention policy lacks reconstruction-critical numbers and error distinctions.** [Limits](materializer-go-contracts-2026-10-30.md?plain=1#L83) omit the 256 MiB retained-result quota and `64 × window` receipt cap. Envelope-count exhaustion returns an apply error; result-byte exhaustion rolls back the command and records `result_retention_full`.
   **Correction:** state both limits, accounting validation, and their distinct outcomes. Evidence: [retention policy](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/result_retention.go#L13), [apply branches](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1433).

**C FFI reverse specification — Blocker**

9. **Blocker — Two operations bypass the documented strict request decoding.** [Doc line 48](c-ffi-go-reverse-spec-2026-10-30.md?plain=1#L48) requires an object and exactly `{}` for empty-request operations. `ready` and `object_store_stats` never decode `request`; any present, non-null JSON value passes that part of dispatch.
   **Correction:** mark these two exceptions explicitly in the operation matrix. Evidence: [dispatch](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/cmd/rhiza-ffi/main.go#L516).

**C FFI contracts — Blocker**

10. **Blocker — Lowercase KV request fields are accepted.** [Lines 37, 54 and 78](c-ffi-go-contracts-2026-10-30.md?plain=1#L37) claim a consequential capitalization difference. Go JSON decoding matches untagged exported fields case-insensitively; Rust itself sends `key` and `consistency`.
    **Correction:** document both accepted spellings; remove the claimed FFI/HTTP incompatibility. Evidence: [Go request type](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1734), [bridge decoder](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/cmd/rhiza-ffi/main.go#L355), [Rust caller](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/sdk/rust/src/lib.rs#L216).

11. **Blocker — Empty KV response handling contradicts Rust.** [Doc line 64](c-ffi-go-contracts-2026-10-30.md?plain=1#L64) says a found value requires `value`. Go omits empty bytes, and Rust deliberately interprets missing `value` with `found=true` as an empty vector. That line also omits the mandatory applied-slot/tip fields.
    **Correction:** reproduce the full Go response schema and Rust’s missing-value behavior. Evidence: [Go response](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1735), [Rust handling](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/sdk/rust/src/lib.rs#L219).

12. **Major — Public stream defaults remain unspecified.** [Request matrix](c-ffi-go-contracts-2026-10-30.md?plain=1#L42) leaves default-bearing fields without their behavior. The exposed Go method changes limit zero to 100 and rejects limit above 1,000 or wait above 30,000 ms.
    **Correction:** include those values and distinguish stream wait from the enclosing FFI call deadline. Evidence: [public stream implementation](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/graph.go#L241).

**Actual coverage and gaps**

- Read all **636 target-document lines**, the requested persona and standard, and EGM integration guidance.
- Read the C bridge’s exports, registry/lifetime accounting, timeout selection, decoder, complete operation dispatch and error mapping; checked Rust close/call/buffer decoding, KV handling, subscription lifecycle, and the complete build script.
- Materializer source reads covered batch apply, SQL/KV/notify duplicate handling, receipt expiry/Bloom behavior, retained-result accounting/codec, graph command transactions and partial-apply gating, stream reads, checkpoint creation, ordinary restore rollback, and Local restore preparation/publication/finalization.
- **Incomplete source coverage:** full open/schema initialization, SQL policy guards, graph validation/query/reachability, all nested membership/config schemas, and every referenced source path were not exhaustively audited. Subscription helper bodies also lacked complete returned-output coverage. These areas are **unreviewed**, not clean.
- No writes, tests, builds, memory/history retrieval, or delegation occurred. No Nit-only findings are reported.

## Node·Operator·Peer 코드 대조

검토자: `01a18be8-9217-70a1-bc39-8fad3c505002`.

Verified HEAD is `008f8e576ad76425182e10a69b987f10cfb48b2a`. **The documents do not pass the re-implementation standard.** Findings below concern documentation, with Blocker used for statements contradicted by source.

**Node lifecycle**

1. **Blocker — Prepared-checkpoint resume does not share reclaim’s cancellation/error behavior.**
   [Reverse spec:124](node-lifecycle-go-reverse-spec-2026-10-30.md?plain=1#L124) groups both paths under cancellation wrapping and `FailLocalLifecycle`. However, [resume implementation:1306](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L1306) returns `Quiesce` errors directly and only defers resume; [Quiesce:809](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L809) can return raw `ctx.Err()`. The wrapping and failure-handler defer belong to reclaim.
   **Correction:** Specify the two paths separately, including resume’s startup error propagation.

2. **Major — HTTP shutdown ownership is underspecified.**
   [Contracts:20](node-lifecycle-go-contracts-2026-10-30.md?plain=1#L20) omits consequential `Start` behavior. In [Start:1658](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L1658), caller-context cancellation triggers HTTP shutdown with a separate ten-second background deadline. Its error is discarded. A non-`ErrServerClosed` Serve failure returns directly without engine shutdown. [Node shutdown:2036](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L2036) has no reference to this HTTP server.
   **Correction:** Document HTTP timeouts, cancellation ownership, and which return paths require the caller to invoke `Node.Shutdown`.

**Operator**

3. **Blocker — The stage machine omits literal `Reserved`.**
   [Reverse spec:54](operator-go-reverse-spec-2026-10-30.md?plain=1#L54) lists “empty/reservation” and says other stages are blocked. [Reconcile:240](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/controller.go#L240) persists `"Reserved"`; [advance:257](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/controller.go#L257) resumes it by sealing the archive.
   **Correction:** State `"" → Reserved → Sealed → Stopping → Starting → Restarted → Complete`, with the durable side effect preceding each transition.

4. **Blocker — Status-recorded failures do not necessarily return errors to the loop.**
   [Contracts:179](operator-go-contracts-2026-10-30.md?plain=1#L179) says errors return after successful status recording. Many branches instead return `record(...)` or `autoStatus(...)`, whose return value is solely the Kubernetes status-write error: [record:907](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/controller.go#L907), [autoStatus:272](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/automatic.go#L272). A successful `Blocked` status write can therefore return nil.
   **Correction:** Distinguish status-only rejection from propagated dependency/CAS errors; describe their different logging behavior.

5. **Blocker — Manual recovery’s fence gate is overstated.**
   [Reverse spec:57](operator-go-reverse-spec-2026-10-30.md?plain=1#L57) describes fencing over a complete persisted identity set. Ordinary recovery actually calls [validateFence:911](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/controller.go#L911), which checks confirmation, nonblank evidence, and operation/cluster/StatefulSet UID equality. It does not validate a `FenceRequest` target set or cryptographic proof. Automatic-child authorization separately validates its persisted proof.
   **Correction:** Separate trusted manual fence assertions from automatic request/proof validation. Do not attribute automatic authority checks to the manual gate.

6. **Major — Claim ownership and completion acceptance remain too abstract to reproduce.**
   The reservation and completion descriptions at [reverse spec:43](operator-go-reverse-spec-2026-10-30.md?plain=1#L43) and [reverse spec:119](operator-go-reverse-spec-2026-10-30.md?plain=1#L119) omit:
   - The separate `recovery-owner` annotation claim, conflict rejection, and UID-plus-operation condition for release: [claim:775](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/controller.go#L775), [release:566](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/controller.go#L566).
   - Completion’s distinct membership and generation checks, including acceptance of legacy incomplete target-identity capture: [completion verification:25](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/automatic_completion.go#L25). Missing identities remain absent and prevent later single-voter repair.

   **Correction:** Add explicit ownership acquisition/release rules and separate completion predicates, including the legacy identity-deficit branch.

7. **Blocker — “No branch deploys a workload” contradicts activation.**
   [Reverse spec:178](operator-go-reverse-spec-2026-10-30.md?plain=1#L178) makes that blanket statement, but [activation:478](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/controller.go#L478) patches cluster identity, durability, credentials, and replicas, then PUTs the StatefulSet to start the successor.
   **Correction:** Describe the actual workload mutations and restrict any narrower non-deployment statement to the specific component that satisfies it.

**Peer transport**

8. **Blocker — The deadline table assigns five seconds to RPCs that inherit caller deadlines.**
   [Contracts:76](peer-transport-go-contracts-2026-10-30.md?plain=1#L76) says normal RPCs shorten the parent deadline to five seconds. [callQuorum:317](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/transport.go#L317) directly calls `callContext`, bypassing that timeout. `SendRecord`, `ReadTip`, and Learned fan-outs use this path.
   **Correction:** Give a per-operation deadline table: five-second `call`, caller-controlled `callQuorum`, and five-minute checkpoint calls. Explicitly state that connection invalidation does not replay the failed RPC.

9. **Blocker — VerifyPrefix can copy certified data into the learner.**
   [Reverse spec:160](peer-transport-go-reverse-spec-2026-10-30.md?plain=1#L160) says it “does not copy data,” while also describing catch-up. The [server handler:540](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/peer_server.go#L540) calls `catchUpFrom(..., true)` before computing the durable prefix.
   **Correction:** Say that the request carries proof parameters, can trigger durable certified catch-up, and does not itself change membership.

**Reads and limits**

Read all six target documents completely, plus the specified persona and standard. Source inspection covered the relevant Open/readiness/shutdown and fencing paths; operator reservation, activation, intent resume, child authorization, claim/release and completion paths; and peer TLS, configuration routing, authorization, framing, both materialization budgets, connection handling and ACK rules.

This was a completed bounded static audit, **not a clean certification**. Local checkpoint internals, learner retirement/escalation, attestation/backend implementations, and every cited reference were not exhaustively verified. No tests, writes, memory/history access, or delegation occurred.

## 다섯 영역 이해도 검토

검토자: `01a18be8-9396-7ef2-a1ee-657575269cc2`.

Reviewed all ten targets fully at HEAD `008f8e576ad76425182e10a69b987f10cfb48b2a`, using the requested comprehension persona and re-implementation standard. Findings below concern each area independently.

### Materializer

**What this proposes**

These documents describe the existing local application layer, rather than a new protocol. A replacement must preserve ordered replay, retained request outcomes, and consistency between SQLite and graph state.

**How the existing system works, per this document**

Certified decisions advance local stores in slot order. Deterministic command failures may become retained rejections; uncertain graph commits gate further access until recovery.

**Major — Stream read returns the applied slot, not the stream high-water sequence.** The [reverse specification, lines 148–151](materializer-go-reverse-spec-2026-10-30.md?plain=1#L148) identifies the second `GraphReadStream` result as the current high-water sequence. The implementation captures `m.tip` and returns it as `appliedSlot`: [graph_enabled.go:1098](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/graph_enabled.go#L1098). These are different coordinate systems. A caller following the document could incorrectly advance its stream cursor and skip records. Specify the result as the materializer applied slot; stream cursors come from record sequences.

### Node lifecycle

**What this proposes**

These documents specify opening, readiness, identity admission, recovery prerequisites, and shutdown. A replacement must retain durable evidence and refuse recovery when identity or authority cannot be established.

**How the existing system works, per this document**

Opening validates configuration and storage identity before enabling participation. Readiness signals completed local recovery, while shutdown drains owned work and preserves unresolved evidence.

**Major — `Start` does not have a sufficiently explicit lifetime/cleanup contract.** The [appendix, lines 20–23](node-lifecycle-go-contracts-2026-10-30.md?plain=1#L20) says that `Start` opens and listens, but omits that it blocks in `Serve`. More consequentially, context cancellation initiates HTTP shutdown with a ten-second deadline; the successful serving path does **not** call `Node.Shutdown`. Only listener creation failure explicitly does so: [node.go:1658](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L1658). State whether callers must arrange node cleanup after `Start` returns, including cancellation and serving errors.

**Major — Checkpoint hashes cannot be reproduced from the documented schema.** The [checkpoint JSON contract](node-lifecycle-go-contracts-2026-10-30.md?plain=1#L83) lists fields and requires matching hashes without defining their byte construction. Source clears **both** root and state hashes before hashing the marshaled descriptor; the state hash separately includes ordered role/name strings, NUL delimiters, eight-byte big-endian lengths, and textual file digests: [local_checkpoint.go:495](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_checkpoint.go#L495). Document those constructions and domain bytes. Otherwise independently generated roots will fail recovery validation despite containing identical files.

### Operator

**What this proposes**

These documents describe manual and automatic recovery as persisted workflows. Progress requires immutable bindings, complete fencing evidence, and verified successor installation.

**How the existing system works, per this document**

The controller reconciles Kubernetes resources and object-store journals. Automatic recovery resumes an existing intent before making fresh decisions, retaining learner lineage and completion evidence across incidents.

**Major — Operator-owned persisted and external proof schemas remain incomplete.** The [reverse specification, lines 88–125](operator-go-reverse-spec-2026-10-30.md?plain=1#L88) describes control/intent/completion contents, but neither target gives their complete tagged shapes, types, omission rules, or timestamp representation. These are durable resume contracts, defined directly in [automatic.go:57](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/automatic.go#L57). Likewise, the [independent-attestation contract](operator-go-contracts-2026-10-30.md?plain=1#L117) summarizes evidence instead of specifying its nested wire objects, which are operator-owned types in [fencing.go:108](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/fencing.go#L108). The dependency-schema exclusion does not explain these omissions. A replacement controller or attestor must currently guess schemas required for interoperability and recovery.

### Peer transport

**What this proposes**

These documents specify private QUIC RPC transport independently of public HTTP APIs. Compatibility includes framing, authentication, configuration selection, quorum acknowledgements, and cancellation.

**How the existing system works, per this document**

Each RPC uses one bidirectional stream on a cached connection. The server validates bounded FlatBuffers messages and authorizes identities against the relevant historical and current memberships before dispatch.

**Major — The TLS negotiation identifier remains symbolic.** The [reverse specification, line 40](peer-transport-go-reverse-spec-2026-10-30.md?plain=1#L40) names `sqlpolicy.PeerALPN`, but neither target supplies its exact value. Source defines it as `"rhiza-peer-v7"`: [policy.go:16](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/sqlpolicy/policy.go#L16). This is a wire compatibility requirement: a replacement cannot establish the intended QUIC connection using only the documents without resolving this constant from source.

### C FFI

**What this proposes**

These documents specify a synchronous C JSON bridge and its Rust ownership wrapper. Compatibility requires matching handle lifetime, response buffers, operation dispatch, cancellation, and JSON behavior.

**How the existing system works, per this document**

Native calls operate on registered Go database handles and return allocated JSON envelopes. Rust owns response cleanup and translates typed convenience methods into the same operation requests.

**Major — The claimed KV request capitalization distinction is misleading.** The [appendix, lines 37 and 78](c-ffi-go-contracts-2026-10-30.md?plain=1#L37) presents capitalized request fields as a consequential FFI/HTTP difference. The bridge uses Go’s case-insensitive struct-field matching, even with unknown fields disallowed: [main.go:355](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/cmd/rhiza-ffi/main.go#L355). Rust itself sends lowercase `key` and `consistency`: [lib.rs:216](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/sdk/rust/src/lib.rs#L216). Explicitly document acceptance of lowercase forms; a replacement enforcing capitalization would break the supplied wrapper.

**Major — Empty KV value behavior contradicts the Rust implementation.** The [appendix, line 64](c-ffi-go-contracts-2026-10-30.md?plain=1#L64) says Rust requires `value` whenever `found` is true. Go omits empty values through `omitempty`: [server.go:1735](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1735). Rust deliberately converts a missing value into `Some(Vec::new())`: [lib.rs:216](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/sdk/rust/src/lib.rs#L216). Specify that behavior so an existing empty value remains distinguishable from an absent key.

**Actual coverage limits:** Full reading of all ten documents; narrow source checks supporting the seven findings above. No writes, tests, builds, memory access, history inspection, or delegation. No exhaustive audit of transitive engines, archive/consensus implementations, CRDs, generated ABI headers, or every nested type. No entire section was incomprehensible; these are bounded contract findings, not full-system certification.
