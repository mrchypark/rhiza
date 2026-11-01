# 요청 admission·HTTP EGM 독립 검토 기록

기준 HEAD: `008f8e576ad76425182e10a69b987f10cfb48b2a`. 두 Goldfish를 fork_context=false로 시작했다. 전체 5단계 승인이나 재구현 인증은 아니다.

## 초안 검토 원문

### 01a18bd2-913f-75f1-bb9c-d58bca956419

**Fails the re-implementation standard.** HEAD matches `008f8e576ad76425182e10a69b987f10cfb48b2a`. Findings below concern the bounded public request surface.

1. **Blocker — SQL blob HTTP contract contradicts accepted input.**
   [Contracts:10](request-http-go-contracts-2026-10-30.md?plain=1#L10) says `{"$rhiza_blob":"…"}` is not an HTTP body schema. However, HTTP `args` decode into `map[string]any`, and [sqlArg:2035](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L2035) explicitly accepts that exact single-key object and base64-decodes it. **Correction:** document this accepted HTTP SQL argument representation, including rejection of extra keys, non-string values, and invalid base64.

2. **Blocker — Returning endpoints do not reject every statements array.**
   [Reverse spec:20](request-http-go-reverse-spec-2026-10-30.md?plain=1#L20) and [contracts:16](request-http-go-contracts-2026-10-30.md?plain=1#L16) say returning endpoints reject `statements`. [execute:1354](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1354) rejects only `len(command.Statements) != 0`; `[]` and `null` proceed using top-level SQL. **Correction:** say “nonempty statements arrays are rejected.”

3. **Blocker — The universal mutation ordering is false.**
   [Reverse spec:37](request-http-go-reverse-spec-2026-10-30.md?plain=1#L37) places validation/encoding/fingerprinting before request locking and receipt lookup for every domain. Notify performs matching and receipt lookup first, then encoding and the replicated-size check at [server.go:1995](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1995). Graph/KV batch encoding also occurs after lookup. **Correction:** document domain-specific sequences; this ordering determines which error or cached result wins.

4. **Major — SQL and Graph acceptance rules remain unspecified.**
   [Reverse spec:65](request-http-go-reverse-spec-2026-10-30.md?plain=1#L65) and [contracts:18](request-http-go-contracts-2026-10-30.md?plain=1#L18) summarize limits or defer to “Graph validator.” They omit externally observable rules:
   - SQL precondition compatibility/ranges; output-reference null placeholders, earlier row-returning targets, unique argument indices, exclusive column selectors, and plain `?` parameters; reserved SQL forms and argument conversions.
   - Graph parameter-name limits, stream/kind/consumer UTF-8 and length rules, reserved event streams, semicolon handling, and standalone `CREATE` restrictions.

   Sources: [SQL validator:1637](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1637), [parameter scanner:1749](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1749), [Graph validators:16](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/graph_common.go#L16). **Correction:** specify these boundary rules explicitly; engine internals need not be included.

5. **Major — Charge accounting cannot be reproduced from the document.**
   [Reverse spec:39](request-http-go-reverse-spec-2026-10-30.md?plain=1#L39) gives string/blob contributions but omits the additional 32 bytes per `add` invocation, collection overhead, command-specific contributions, and precise node/depth traversal. [mutation_admission.go:76](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/mutation_admission.go#L76) contains these rules. **Correction:** provide the complete accounting table and exact accepted scalar types; “built-in scalar” incorrectly suggests types such as `uintptr` or complex numbers are accepted.

6. **Major — Cancellation, ownership, and lifecycle coverage is insufficient.**
   [Reverse spec:41–49](request-http-go-reverse-spec-2026-10-30.md?plain=1#L41) omits the dispatch cancellation cutoff, worker-owned 30-second context, direct proposal deadline inheritance, and Local-mode sticky-failure behavior. Dispatch drops already-cancelled items; once dispatched, batch execution uses a context independent of individual callers. Local lifecycle waits continue after caller cancellation and can close readiness on failure. Sources: [batcher.go:311](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/batcher.go#L311), [batcher.go:360](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/batcher.go#L360), [server.go:855](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L855), [server.go:472](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L472). **Correction:** state cancellation boundaries and release points separately for batched requests, direct notify proposals, and Local lifecycle work.

7. **Major — HTTP error precedence and SSE failure contract are missing.**
   [Contracts:37–50](request-http-go-contracts-2026-10-30.md?plain=1#L37) presents independent mappings, but [writeAPIError:1604](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1604) uses ordered `errors.Is` checks, then forcibly writes `code:"commit_unknown"` for a typed `CommitUnknownError` while retaining the previously selected HTTP status. A typed unknown wrapping `ErrInvalidRequest`, for example, yields HTTP 400 with `commit_unknown`. Subscribe also has HTTP 500 `streaming_unsupported`, absent from the table. **Correction:** specify ordered classification, the typed override, and SSE’s Flusher requirement, initial comment, headers, and termination behavior. Source: [subscribe handler:2059](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L2059).

**Actual reads:** persona and standard fully; both target documents fully; `mutation_admission.go`, `batcher.go`, `graph.go`, and `graph_common.go` fully; scoped server dispatch, handlers, admission, receipt, migration, proposal/lifecycle, and SSE code; relevant value schemas/encoding/fingerprints, SQL validators/conversions, and replicated-value constant.

**Coverage limits:** no tests, writes, memory, history, or delegation. Full peer/admin protocols, engine execution, complete SQL tokenization, materializer receipt storage/retention, and external middleware were not audited. No Nit findings.

### 01a18bd2-cbcc-7a82-ba62-21546bdab357

## What this proposes

The two documents aim to specify Rhiza’s public data HTTP interface and its mutation/read admission boundary sufficiently for independent reimplementation. They cover decoding, request and response schemas, capacity limits, ownership transfer, receipts, retries, and migration reservations while excluding full peer, administration, recovery, and database engines.

## How the existing system works, per this document

HTTP handlers decode and sometimes validate requests before calling domain methods. Mutations reserve capacity and clone caller data, then validate, check retained intent/receipts, and submit work; reads use separate admission and optional consistency barriers. Outcomes include retained committed/rejected receipts, definite refusal, and commit uncertainty; status lookup cannot distinguish missing requests from expired ones.

**Verdict: the bounded review is complete, but the documents do not meet the zero-assumption reimplementation standard.**

1. **Blocker — Public input contracts still require reading validators.**
   [Contracts:16–22](request-http-go-contracts-2026-10-30.md?plain=1#L16) lists SQL output references without explaining their meaning, and explicitly delegates Graph combinations to “Graph validator.” [Spec:65](request-http-go-reverse-spec-2026-10-30.md?plain=1#L65) supplies only part of those rules.
   [SQL validation](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1637) requires references to earlier row-returning statements, null destination arguments, unique argument indices, exactly one column selector, and unnumbered positional parameters. [Graph validation](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/graph_common.go#L16) adds parameter, stream, consumer, event, reserved-prefix, and standalone-CREATE restrictions.
   **Correction:** specify these caller prerequisites, nested field types, and rejection rules directly. These are public admission contracts even though engine internals are excluded.

2. **Major — The shared mutation ordering is incorrect.**
   [Spec:37](request-http-go-reverse-spec-2026-10-30.md?plain=1#L37) places encoding/fingerprinting before request locking and replay lookup for every domain. [GraphExecute](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/graph.go#L104) checks retained intent/receipt before batch encoding; [NotifyPublish](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1999) checks replay before encoding and replicated-size validation. [KVMutate](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1865) checks replay before timestamp assignment and TTL overflow validation.
   **Correction:** provide domain-specific sequences. Their ordering determines whether a retry returns a receipt or encounters a later validation failure.

3. **Major — Retry advice omits the expiry boundary and possible reexecution.**
   [Spec:47–51](request-http-go-reverse-spec-2026-10-30.md?plain=1#L47) recommends retrying the same ID but does not define when retained identity protection ends. [Receipt lookup](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L689) treats the receipt as absent when materialized tip is **greater than** `retry_through_slot`; equality remains retained.
   **Correction:** explicitly bound replay guarantees to retention, identify the relevant tip, and state that an expired ID can be treated as a new submission. `unknown_or_expired` is not evidence that retrying cannot repeat a mutation.

4. **Major — KV fields lack operational meaning.**
   [Contracts:23](request-http-go-contracts-2026-10-30.md?plain=1#L23) names `expected`, `expected_exists`, and `ttl_ms` without specifying their effects. [KV application](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1548) returns a **committed** receipt with `applied=false` for CAS mismatch or deletion of an absent key; `omitempty` then removes `applied`. CAS compares existence and, only when present, bytes. [KV submission](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1880) anchors expiry to first submission time.
   **Correction:** define put/delete/CAS behavior, omitted `expected_exists=false`, expiry visibility, zero TTL, overflow rejection, and the distinction between committed no-op and rejected mutation.

5. **Major — Error classification omits durability uncertainty and sticky lifecycle failure.**
   [Contracts:43–50](request-http-go-contracts-2026-10-30.md?plain=1#L43) presents durability failure as `durability_unavailable`, but [receipt durability handling](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L472) wraps failures with a known slot as `CommitUnknownError`; the HTTP writer emits `code:"commit_unknown"`. [Local proposal completion](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L855) can latch a lifecycle failure requiring reopen, causing subsequent admission to return `not_ready`.
   **Correction:** distinguish the underlying error from the final HTTP classification and document the bounded local failure/reopen contract. Also specify the 30-second worker/proposal deadlines rather than only the 2-second admission and 5-second reserve.

6. **Major — SSE delivery and shutdown bounds are missing.**
   [Contracts:26](request-http-go-contracts-2026-10-30.md?plain=1#L26) describes only payload framing and keepalive. [Subscribe](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1497) enforces 64 subscribers and a one-item queue; the public delivery contract is live, at-most-once, with drops. [HTTP streaming](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L2059) requires `http.Flusher`, clears the write deadline, emits `: connected`, and exits on cancellation/channel closure. [Close](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L429) waits for active handlers.
   **Correction:** state capacity, loss/replay semantics, `streaming_unsupported`, and the caller’s responsibility for ending existing subscriptions during shutdown.

7. **Major — Admission charge is not reproducible from the stated formulas.**
   [Spec:39](request-http-go-reverse-spec-2026-10-30.md?plain=1#L39) gives string/blob charges but omits the per-visited-value 32-byte charge, collection overhead, and command-specific additions in the [walker](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/mutation_admission.go#L76). For example, a visited scalar string incurs both the base charge and string charge.
   **Correction:** specify the complete accounting recurrence and node/depth counting rules. Otherwise an implementation will accept and reject different requests at the advertised limits.

**Read coverage:** fully read both requested documents, the comprehension persona, and the reimplementation standard; verified HEAD equals `008f8e576ad76425182e10a69b987f10cfb48b2a`. Inspected public data handlers, mutation charging/cloning, read admission, migration tokens, receipt/error paths, and selected boundary validators and materializer operations. Some initial combined source output was truncated; focused follow-up reads recovered the evidence cited above. Full proposal/batcher internals, engine/build variants, peer/admin engines, and transitive dependencies were not exhaustively read. No writes, tests, memory access, history inspection, or delegation occurred.

## 실행 검증 원문

검증은 완료됐으며 막힌 작업은 없습니다. 지정한 HEAD `008f8e576ad76425182e10a69b987f10cfb48b2a`에서 **기본 선택 31개 중 30 PASS·1 SKIP**, 추가 Local lifecycle 테스트 **1 PASS**입니다. 탐색은 종료했습니다. 파일·문서·메모리·커밋을 변경하지 않았고, 기존 미추적 문서 12개를 보존했습니다.

**실행 명령과 결과**

모든 명령의 작업 디렉터리는 `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza`였습니다.

1. 테스트 목록 확인:

```sh
go test -mod=readonly -list 'Admission|HTTP|RequestID|Decode|Overload' ./pkg/network
```

종료 코드 0입니다. 최상위 테스트 32개가 매칭됐습니다. 목록에 표시된 benchmark와 fuzz는 실행하지 않았습니다.

2. POSIX `sh`에서 첫 실행을 시도했지만 `go: command not found`로 종료 코드 127이 났습니다. 테스트는 시작되지 않았습니다. 로그인 셸에서 확인한 `/opt/homebrew/bin/go` 절대 경로로 바로 재실행했습니다.

3. 실제 기본 검증 명령:

```sh
/opt/homebrew/bin/go test -mod=readonly \
  -list 'Admission|HTTP|RequestID|Decode|Overload' ./pkg/network \
  > "$task_tmp/list.txt"

run_regex=$(awk '/^Test/ && $0 != "TestMutationAdmissionPeakRSS" {
  if (n++) printf "|"; printf "%s", $0
} END { print "" }' "$task_tmp/list.txt")

/opt/homebrew/bin/go test -mod=readonly -count=1 -timeout=180s \
  ./pkg/network -run "^($run_regex)$" -v
```

실제 확장된 정규식은 다음과 같습니다. 이 목록이 실행된 최상위 테스트 이름 31개입니다.

```text
^(TestSQLBatcherRejectsOversizedItemAtAdmission|TestNotifyCancellationReportsUnknownWithRequestID|TestCheckpointBarrierRetriesRealPreAdmissionWithSamePayload|TestForegroundCheckpointBarrierRetriesOnlyTypedPreAdmissionOverload|TestGraphRequestIDRejectedBeforeConsensus|TestConcurrentGraphRequestIDConflict|TestVerifyLearnerFetchesAndPersistsAdmissionPrefix|TestChangeMembershipCallbackReceivesDecodedChange|TestMigrationAdmissionRejectsSubstitutionReleaseAndWrongServer|TestMigrationAdmissionIsSingleUseDuringConcurrentSubmission|TestMutationAdmissionCountAndAggregateByteCaps|TestDecodeWALIdentityRejectsNonCanonicalAndMismatchedValues|TestReadAdmissionLimitsValidateAndReserveLongPollCapacity|TestReadAdmissionRejectsSaturationAndHonorsCancellation|TestHTTPReadAdmissionHoldsSlotUntilResponseWriteCompletes|TestCrossIngressGraphRequestIDConflictDoesNotBlockApply|TestCrossIngressRequestIDConflictDoesNotBlockSQLKVOrNotify|TestHTTPAdapterDoesNotExposePeerRPC|TestDurabilityFailureIsRetryableWithSameRequestID|TestHTTPBodyLimitIsIndependentFromConsensusLimit|TestQuiesceTimeoutKeepsAdmissionClosedUntilDrain|TestKVRetryPreservesFirstAdmissionAndRejectsChangedIntent|TestLocalProposalAdmissionRejectsBeyondBackgroundLimit|TestLocalProposalAdmissionSharedWithCheckpointBarrierAndKV|TestInternalBatchAdmissionWaitsForReleaseWithoutChangingDirectRefusal|TestInternalBatchAdmissionByteReleaseAndQuiesce|TestInternalBatchAdmissionDoesNotWakeOnRegistrationAndJoinsInflight|TestInternalBatchAdmissionTaggedObserverAndDirectControlIsolation|TestProposeWithLeaseMarksOnlyPreAdmissionCapacityRejections|TestTempAdmissionNeverCertifies|TestTerminalDrainClosedHTTPPreservesNotReadyErrorCode)$
```

결과는 종료 코드 **0**, 패키지 실행 시간 **9.667초**입니다.

- **30 PASS, 1 SKIP, 0 FAIL**입니다. 하위 테스트는 이 개수에 별도로 합산하지 않았습니다.
- SKIP: `TestInternalBatchAdmissionTaggedObserverAndDirectControlIsolation` — `observer is compiled out`.
- 의도적으로 제외: `TestMutationAdmissionPeakRSS`. 저장소 지침이 성능 측정을 CI로 제한하므로 로컬 RSS 측정은 실행하지 않았습니다.
- `rhiza_local_testhooks` 전용 overload-site 테스트는 기본 빌드에 포함되지 않았습니다.
- `RHIZA_FOREGROUND_API_COST_SCENARIO`로 켜는 150초 측정은 실행하지 않았습니다.

4. 추가로 실행한 유일한 Local lifecycle 테스트:

```sh
/opt/homebrew/bin/go test -mod=readonly -count=1 -timeout=180s \
  ./pkg/network \
  -run '^TestExplicitLocalReadApplySurvivesCallerCancellation$' -v
```

**1 PASS**, 종료 코드 **0**, 패키지 실행 시간 **0.993초**입니다. 호출자 취소 뒤에도 Local apply가 permit을 유지하며 완료하고, 성공 시 readiness를 손상시키지 않는다는 것을 확인했습니다.

**문서 편집자에게 전달할 admission 계약**

| 항목 | 구현된 계약과 검증 범위 |
|---|---|
| 변경 요청 입구 | writable·Ready 확인 → context 확인 → charge 계산 → 예약 → 입력 복제 → 검증 → request stripe·기존 결과 확인 → 제출 순서입니다. 잘못된 요청도 의미 검증보다 먼저 용량 거절을 받을 수 있습니다. |
| 변경 요청 한도 | Server 전체 **128개 / charge 합계 32 MiB / 요청당 charge 1 MiB**, 재귀 깊이 **32**, 노드 **65,536**입니다. 요청 자체 한도 초과는 `ErrInvalidRequest`, 동시 용량 부족은 `ErrOverloaded`입니다. 개수·합계 한도 테스트가 통과했습니다. |
| charge 의미 | 실제 JSON 길이나 실제 RSS가 아닙니다. 각 방문 값에 32바이트를 더하고 문자열은 추가 `32+13×len`, blob은 `32+5×len` 등을 예약합니다. 허용된 내장 타입만 순회하며 사용자 marshal hook은 호출하지 않습니다. |
| 호출자 입력 | 복제 전 charge 순회도 호출자 입력을 읽습니다. 호출자는 메서드 반환 전까지 중첩 값·slice·map을 변경하면 안 됩니다. 복제했다고 동시 변경이 허용되는 계약은 아닙니다. |
| lease 소유권 | 상태는 caller `0` → worker `1` → released `2`이며 CAS로 한 번만 이전·해제합니다. 이전 전에는 caller defer가 해제합니다. batch 제출은 queue 예약 전에 이전하고, 예약 실패 시 worker 경로에서 즉시 해제합니다. direct proposal은 새 등록 또는 기존 proposal 합류 시 이전합니다. |
| 반환과 해제 시점 | batch의 `finish`는 **결과 채널 전송 후 lease를 해제**합니다. 따라서 성공 응답 수신만으로 해제 완료를 단정하면 안 됩니다. migration 동시 제출 테스트는 batcher를 join한 뒤 누수 여부를 확인합니다. direct proposal은 논리 작업 정리에서 lease를 해제한 뒤 완료 채널을 닫습니다. |
| 읽기 admission | 기본 **동시 64개 / long-poll 8개**입니다. `MaxConcurrent>0`, `0≤MaxLongPoll≤MaxConcurrent`여야 합니다. 대기 없이 거절하며 long-poll은 두 permit을 모두 사용합니다. HTTP는 **응답 encode/write가 끝날 때까지**, 직접 API는 메서드 반환까지 유지합니다. 관련 3개 테스트가 통과했습니다. |
| 설정 교체 | `SetReadAdmissionLimits`는 이후 시작하는 읽기에 적용됩니다. 기존 읽기는 이전 admission 객체를 계속 사용하므로 실행 중 재설정이 전체 활성 읽기 수를 새 한도로 즉시 낮추지는 않습니다. 노출 전 설정이 의도된 사용법입니다. |
| migration 예약 | `AdmitMigration`은 request ID와 checksum을 각각 **64자리 `"0"`**으로 가정해 예약합니다. `MigrateAdmitted`는 **같은 Server·정확히 같은 charge·미소비 상태**를 검사합니다. 원문이나 fingerprint 일치는 검사하지 않습니다. 같은 charge의 다른 내용까지 막는 예약으로 기술하면 안 됩니다. |
| migration 해제 책임 | 소비 전 실패·취소·잘못된 서버·charge 불일치에는 예약이 남으므로 caller가 `Release()`해야 합니다. 소비 후에는 내부 lease 경로가 해제합니다. `Release()`는 소비 후 호출해도 안전합니다. 두 migration admission 테스트가 통과했습니다. |

근거: [mutation_admission.go](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/mutation_admission.go#L12), [읽기 admission](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L390), [migration 예약](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1427), [batch 소유권](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/batcher.go#L113).

**취소·배치·오류 경계에서 반드시 남길 구분**

- **admission 전 취소**는 보통 원래 context 오류입니다. **queue 전송 후 또는 proposal 등록·합류 후 취소**는 commit 가능성이 남아 `CommitUnknownError` 또는 `ErrCommitUnknown` 래핑으로 반환합니다. 호출자 취소가 작업 취소나 lease 해제를 뜻하지 않습니다. Notify 취소 테스트가 request ID·slot 0·예약 유지·최종 해제를 확인했습니다.
- SQL·Graph·KV batch 작업에는 server/batcher 소유의 **30초 context**가 사용됩니다. direct proposal은 server context에 **최대 30초와 최초 등록 caller deadline 중 이른 시점**을 적용합니다. 이후 합류 caller의 deadline으로 작업 deadline을 갱신하지 않습니다.
- batcher별 queue 한도는 **4,096개 / 8 MiB**, item 예약은 encoded 길이+128입니다. batch는 최대 **64개**, 목표 **64 KiB**, worker 동시 **8개**, worker encoded 합계 **1 MiB**입니다. queue 예약은 dequeue 시 반환하지만 mutation lease는 작업 완료까지 남습니다.
- Server proposal 한도는 기본 **local permit 8개**, 명시적 Local mode에서는 **1개**, operation permit **12개**, encoded 한도는 각각 **1 MiB / 2 MiB**입니다. direct는 즉시 거절하고 internal batch는 **최대 2초**, 작업 deadline에서 **5초 reserve**를 뺀 시점까지 기다립니다. 이 대기·거절·합류 계약 관련 테스트들이 통과했습니다.
- 기존 receipt를 찾았어도 설정된 durability barrier를 다시 통과해야 ACK합니다. `RequestStatus`의 `committed`는 durability 증명이 아닙니다. 해당 재시도 테스트가 통과했습니다.

근거: [proposal admission](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L641), [작업 context](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L906), [receipt durability](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L472).

**HTTP·직렬화 계약과 검증 한계**

- 본문 제한은 **1 MiB**, 합의 값 제한은 framing을 포함한 **128 KiB**입니다. 서로 독립이라는 테스트가 경계값과 초과값에서 통과했습니다.
- 공통 decoder는 `UseNumber`, `DisallowUnknownFields`, 두 번째 decode의 EOF 검사를 사용합니다. 초과 본문도 handler에서는 **400 `invalid_request`**로 처리합니다. `Content-Type` 검사는 없습니다. duplicate key 거절이나 JSON 필드명의 엄격한 대소문자 일치는 추가하지 않습니다. 이 세부사항은 **소스 확인 결과이며 별도 실행 검증은 하지 않았습니다**.
- 공통 오류 매핑 순서는 invalid/consistency **400**, conflict **409**, not-ready·overloaded·durability·commit-unknown·quorum **503**, 그 밖에는 **500**입니다. 일반 context 오류를 위한 408/499 매핑은 없습니다.
- `CommitUnknownError`를 찾으면 JSON `code`를 `commit_unknown`으로 쓰지만 HTTP status는 앞선 오류 분류 결과를 유지합니다. 따라서 다중 원인 오류를 “code와 status가 항상 일대일 대응한다”고 문서화하면 안 됩니다.
- SQL/Graph batch framing은 `QBAT`/`QGRB`+정책 바이트 **7**+JSON 배열, KV batch는 `QKVB\x00`+배열, Notify는 `QNTF\x00`+객체입니다. SQL·Graph decode는 `UseNumber`를 사용합니다. SQL blob 인자는 `{"$rhiza_blob":"<base64>"}`, KV·Notify의 `[]byte`는 JSON base64입니다.
- 요청 ID 상한은 **64바이트**, 기본 idempotency window는 **65,536 slots**입니다. slot을 아는 commit-unknown의 retry bound는 `slot+65,535`이며 slot 0일 때는 설정하지 않습니다. status의 미발견 결과는 `unknown_or_expired`로, 미제출과 만료를 구분하지 않습니다.
- Graph stream read 기본 limit는 **100**, 최대 **1,000**, `wait_ms≤30,000`이며 `wait_ms!=0`이면 long-poll admission을 사용합니다. `/graph/changes`는 stream을 `__lattice_changes`로 덮어씁니다.

근거: [HTTP 오류·decode](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1587), [내부 wire types](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/types/value.go#L17), [Graph handler](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/graph.go#L277).

HTTP route inventory는 [routes()](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L496)가 기준입니다. SQL 실행·transaction·query, Graph 실행·query·changes·stream read/offset/trim, KV put/get/delete/cas, Notify publish/subscribe, request status를 포함합니다. offset은 **POST 조회 / PUT 변경**, trim은 POST이며 변경 성공은 **204**입니다. Notify subscribe는 GET SSE입니다. `GraphReachable`과 migration은 등록된 HTTP route가 없습니다. metrics·replica·recovery·membership·ready·healthz도 등록돼 있지만, peer/admin 복구 프로토콜 전체는 이번 명세 범위에서 제외했습니다.

독립 리뷰 편집 시 우선 확인할 항목은 **migration의 charge 일치와 내용 일치 구분**, **결과 반환과 lease 해제의 순서**, **admission 이후 취소와 실제 작업 종료의 구분**, **HTTP code/status 우선순위**, **decode 이전 메모리까지 32 MiB로 제한된다는 잘못된 주장 방지**입니다. HTTP JSON decode는 mutation admission보다 먼저 수행되므로 32 MiB는 전체 HTTP 수신 메모리 상한이 아닙니다.

테스트 fixture 이외의 서버·컨테이너·live cloud는 사용하지 않았습니다. 테스트 프로세스는 모두 종료됐고 잔존 `network.test` 프로세스가 없음을 확인했습니다. 자체 임시 로그 디렉터리도 삭제 후 부재를 확인했습니다. 정식 독립 EGM 완료 주장은 하지 않습니다. EGM 지침 (local-only evidence: `/Users/cypark/.codex/plugins/cache/egm/egm/0.1.0/skills/egm-review/SKILL.md`)은 별도 문맥의 Goldfish를 요구하며 “a review in the same context is not a Goldfish review”라고 명시합니다. 이번 결과는 진행 중인 독립 리뷰에 전달할 소스·실행 증거입니다.


## 1차 재점검

### 01a18bd2-913f-75f1-bb9c-d58bca956419

**Original findings: 4 resolved, 3 partially resolved.** The release and migration corrections match the inspected source; charge accounting still needs one clarification.

| Original finding | Rereview |
|---|---|
| 1. HTTP SQL blob representation | **Resolved.** Contracts now describe the accepted single-key object and its rejection cases; matches `sqlArg`. |
| 2. Returning endpoints and empty statements | **Resolved.** Both documents now say **nonempty** statements are rejected; matches `execute`. |
| 3. Domain-specific mutation ordering | **Resolved.** The new sequence table matches SQL, Graph, KV, and notify entry paths. |
| 4. Domain acceptance rules | **Partially resolved — Major residual below.** Output references, preconditions, conversions, and Graph restrictions are substantially covered. |
| 5. Exact charge accounting | **Partially resolved — Major residual below.** Contributions are present, but helper charging versus recursive visits remains ambiguous. |
| 6. Cancellation and lifecycle | **Partially resolved — Blocker residual below.** Dispatch cutoff, independent batch context, deadlines, and completion ordering are now covered. |
| 7. Error precedence and SSE | **Resolved.** Ordered classification, typed override, Flusher failure, headers, and termination now match the checked handlers. |

Residuals:

- **Major — SQL reserved-form rules remain missing.**
  [Contracts:37](request-http-go-contracts-2026-10-30.md?plain=1#L37) adds the keyword restrictions but still omits rejection of `CREATE TEMP/TEMPORARY`, `temp.` qualifiers, and any case-insensitive `_rhiza_` substring. These checks remain in [temp_policy.go:105](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/temp_policy.go#L105) and [materializer.go:1840](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1840). **Correction:** add those admission rules.

- **Major — “String charge” needs an explicit helper definition.**
  [Reverse spec:60](request-http-go-reverse-spec-2026-10-30.md?plain=1#L60) uses “string charge” for map keys and command fields, while the preceding string row includes the recursive visit’s base 32. Source uses `addString` directly for these fields: **`32+13*n`, without another visit/base/node/depth increment**. Only `add(...)` incurs those increments. See [mutation_admission.go:76](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/mutation_admission.go#L76). **Correction:** define `S(n)=32+13*n`, `B(n)=32+5*n`, distinguish helper calls from visits, and restore the explicit root depth of zero. Otherwise an implementation can overcharge requests.

- **Blocker — Local failure statement is overbroad.**
  [Reverse spec:76](request-http-go-reverse-spec-2026-10-30.md?plain=1#L76) says failure after core start latches sticky lifecycle failure. [runProposal:855](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L855) explicitly exempts trusted **prewrite admission denial**, permitting reclaim and one retry; clean maintenance refusal also avoids the blanket outcome. Conversely, [finishLocalLifecycle:1018](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1018) latches any lifecycle-work error without a core-start condition. **Correction:** separate proposal failures and their prewrite-denial exception from durability/apply lifecycle failures.

**Release/migration cross-check:** no direct contradiction found. Batch `finish` sends before releasing; direct proposal cleanup releases before closing completion. Migration tokens bind server and exact charge, not content; pre-consumption failure leaves an available reservation for caller release, while successful consumption delegates release internally. This matches [batcher.go:151](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/batcher.go#L151), [server.go:978](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L978), and [server.go:1449](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1449).

Read both revised targets fully and reread source slices supporting the seven findings and charge/release/migration corrections. No tests or writes occurred. Earlier gaps remain: full peer/admin protocols, engine execution, complete SQL tokenization, receipt storage/retention, and external middleware were not audited. Newly added retention, KV apply, and subscription-delivery claims were not independently qualified in this targeted rereview.

### 01a18bd2-cbcc-7a82-ba62-21546bdab357

**Targeted result: four findings resolved; three partially resolved. No new Blocker established.**

The revisions now describe separate domain admission sequences, bounded replay protection, and explicit worker lifetimes. The charge, release, and migration sections largely agree, but charge accounting still needs precision.

| Original finding | Result | Verification |
|---|---|---|
| 1. Public input contracts | **Residual — Major** | Validation rules are substantially expanded, but Graph nested input shapes remain unspecified: `events[]`, `stream_offset`, and `stream_trim` lack complete field/type definitions. [Contracts:18](request-http-go-contracts-2026-10-30.md?plain=1#L18) versus [source types:147](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/types/value.go#L147). Add `events:[{stream:string,kind:string,payload:any}]`, offset `{stream:string,consumer:string,sequence:uint64}`, and trim `{stream:string,through_sequence:uint64}`, including omission/default behavior. |
| 2. Shared mutation ordering | **Resolved** | The domain table correctly separates SQL’s pre-lookup encoding from Graph/KV/notify replay paths. Checked against [SQL:1365](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1365), [Graph:82](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/graph.go#L82), [KV:1838](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1838), and [notify:1974](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1974). |
| 3. Retry expiry/reexecution | **Resolved** | [Spec:78](request-http-go-reverse-spec-2026-10-30.md?plain=1#L78) now states the inclusive retention boundary, materialized tip, and possible reexecution after expiry. Matches [receipt lookup:689](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L689). |
| 4. KV operational meaning | **Resolved** | [Contracts:43](request-http-go-contracts-2026-10-30.md?plain=1#L43) explains CAS existence/byte comparison, committed no-op, TTL anchoring, overflow, and expiry visibility. Matches [submission:1880](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1880) and [application:1548](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1548). The existing `omitempty` rule establishes that false `applied` is absent on the wire. |
| 5. Error/lifecycle classification | **Residual — Major** | Durability wrapping and HTTP classification are corrected. However, [Spec:76](request-http-go-reverse-spec-2026-10-30.md?plain=1#L76) broadly says failure after core start latches lifecycle failure. [runProposal:855](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L855) explicitly distinguishes `prewriteDenied` and clean maintenance refusal. Qualify the statement so definite prewrite refusal is not documented as sticky failure/commit uncertainty. |
| 6. SSE bounds/shutdown | **Resolved** | [Contracts:47](request-http-go-contracts-2026-10-30.md?plain=1#L47) supplies capacity, queue depth, live delivery/no replay, streaming prerequisites, exit conditions, and handler joining. Matches [Subscribe:1497](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1497), [SSE:2059](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L2059), and [Close:429](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L429). |
| 7. Exact charge recurrence | **Residual — Major** | [Spec:52–64](request-http-go-reverse-spec-2026-10-30.md?plain=1#L52) adds most accounting, but “string charge” is ambiguous between a visited string’s `64+13*n` and helper-only `32+13*n`. Keys and command string fields use the latter, without node/depth increments. Root depth zero and the separate 65,536 length guards for SQL statements/output refs and Graph events also need explicit inclusion. These distinctions are visible in [walker:66](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/mutation_admission.go#L66). |

For the requested charge/release/migration consistency check:

- **Release ordering is consistent with source.** Batch `finish` sends the result before releasing the lease; direct proposal cleanup releases leases before closing completion. Thus batch method return need not imply release has completed. [Batch:151](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/batcher.go#L151), [proposal cleanup:980](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L980).
- **Migration token ownership is consistent.** Server binding, exact charge, single consumption, no content binding, and caller release after pre-consumption failure match [consume:1241](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1241) and [MigrateAdmitted:1448](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1448).
- **Nit — make the placeholder consequence explicit.** With unchanged name/statements and a valid 64-character checksum, an `AdmitMigration` reservation requires a 64-byte final request ID to preserve exact charge. A shorter otherwise-valid ID produces charge mismatch. [Spec:84](request-http-go-reverse-spec-2026-10-30.md?plain=1#L84) contains the ingredients but leaves this caller prerequisite implicit.

Both revised documents were read fully. Source checks stayed within the original findings and the requested consistency check. Prior gaps remain: no exhaustive proposal/batcher-engine, build-variant, peer/admin, or transitive-dependency review. No tests, writes, memory access, history inspection, or delegation occurred.

## 최종 재점검

### 01a18bd2-913f-75f1-bb9c-d58bca956419

**All my prior residuals are resolved. No remaining Blocker, Major, or Nit in this targeted rereview.**

- **SQL restrictions:** TEMP/TEMPORARY, `temp.` qualifiers, and case-insensitive `_rhiza_` rejection now match source.
- **Charge accounting:** S/B helpers, recursive base/node/depth accounting, root depth zero, and separate collection guards now match the walker.
- **Local lifecycle:** prewrite denial and maintenance exceptions are distinguished from lifecycle-work error latching.
- **Graph schemas:** nested field names, types, pointers, and omission behavior match the structs.
- **Migration:** the conditional requirement for a 64-byte final request ID follows from exact-charge matching with unchanged name/statements and a valid 64-character checksum.

No direct contradiction found among the corrected charge, release, and migration descriptions.

Read both revised documents fully and checked the relevant source slices only. No tests or writes. Earlier coverage gaps remain unchanged; this is not a broader qualification of engine, retention/storage, peer/admin, or middleware behavior.

### 01a18bd2-cbcc-7a82-ba62-21546bdab357

**All my remaining findings, including the migration Nit, are resolved.**

- **Input contracts:** Graph nested tags/types/defaults and SQL temporary/reserved forms now match [Graph types](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/types/value.go#L147), [temp policy](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/temp_policy.go#L105), and [namespace validation](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1840).
- **Charge accounting:** S/B helpers are distinguished from recursive visits, with root depth zero, base/node accounting, and separate collection guards matching the [walker](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/mutation_admission.go#L66).
- **Local lifecycle:** Prewrite denial and clean maintenance refusal are separated from proposal failures and lifecycle-work error latching, matching [proposal branches](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L855) and [lifecycle completion](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1018).
- **Migration Nit:** The conditional 64-byte final-ID requirement now matches [reservation and consumption](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1427).

No residual contradiction found among the targeted charge, release, and migration corrections.

Both canonical documents were read fully; source verification stayed targeted. Prior coverage gaps remain: no exhaustive engine, build-variant, peer/admin, or transitive review. No tests or writes occurred.

## 최종 상태

원 지적과 재점검 보완 사항을 수정했고 두 검토자가 해소를 확인했다. 전체 명세의 무결점 판정은 아니다. 선별 테스트 30 PASS·1 SKIP(9.667s), 추가 Local lifecycle 1 PASS(0.993s). 전체 network·race·태그 빌드·RSS·클라우드·Chaos는 미검증이다. 최초 sh Go PATH 실패는 테스트 시작 전 실패로 구분했다. 구현·의존성·기존 문서는 보존했다. 자동 승인 검토는 rm -f 스타일 정리를 거부했고 파일별 삭제 방식으로 정리했다.
