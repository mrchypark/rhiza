# Checkpoint EGM 독립 검토 기록

기준 HEAD: `008f8e576ad76425182e10a69b987f10cfb48b2a`. 두 Goldfish를 fork_context=false로 시작했다. 전체 5단계 승인이나 재구현 인증은 아니다.

## 초안 검토

### 01a18b90-dc30-7da0-8047-11fdb42841ff

**What this proposes**

These documents describe the checkpoint package as implemented at HEAD `008f8e576ad76425182e10a69b987f10cfb48b2a`, rather than proposing changes. Together they specify publication, recovery pins, garbage collection, Local maintenance, automatic checkpoints, and persisted formats. Consensus certification, fencing, and database installation remain caller responsibilities.

**How the existing system works, per this document**

HA writers acquire a shared lease, upload a candidate, obtain external certification, and conditionally advance CURRENT. Readers validate descriptors and block content, with successful verification reused through caches. Recovery pins preserve selected content during installation; HA GC uses maintenance claims, while Local operations use a persistent gate and cancellation-aware bucket wrapper.

Four **Major** contract gaps remain. No Blocker or Nit findings are asserted.

1. **Major — Promotion does not compare the supplied descriptor with the stored descriptor.**
   Affected: [reverse spec, lines 46–50](checkpoint-go-reverse-spec-2026-10-30.md?plain=1#L46). “OpenRoot … descriptor를 확인” obscures which descriptor is checked. [Source lines 661–663](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L661) discard the opened descriptor; successful promotion remembers the supplied `root` at lines 718 and 724. Thus valid stored bytes do not establish that the caller’s `Hash`, `Files`, or `Size` match them, and `Latest` can expose those supplied values.

   Corrective wording: “Promotion validates the canonical stored root identified by index/RootHash, but does not compare its descriptor with the supplied root. On upload success or an identical CURRENT pointer, it remembers the supplied descriptor through a shallow copy. The caller must supply the exact descriptor and preserve its shared slices.”

2. **Major — Local callback storage prerequisites are not explicit.**
   Affected: [reverse spec, lines 104–106](checkpoint-go-reverse-spec-2026-10-30.md?plain=1#L104). The text describes uncertainty tracking without explicitly requiring callback operations to use both `LocalBucket` and the callback context. [Source lines 80–84 and 123–128](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/local_gc.go#L80) wrap the gate’s own manager and pass a context to work; they do not replace the caller manager’s bucket. Tracking exists in [local_gc_bucket.go, lines 63–100](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/local_gc_bucket.go#L63).

   Corrective wording: “Callback storage operations must use a LocalBucket-wrapped bucket and the supplied callback context. WithLocalMaintenanceContext does not wrap the caller’s manager in place. Raw-bucket calls bypass RPC admission, timeout, and mutation-uncertainty tracking.”

3. **Major — Cache persistence failure leaves reusable in-memory verification state.**
   Affected: [reverse spec, line 80](checkpoint-go-reverse-spec-2026-10-30.md?plain=1#L80). The unconditional statement that verification success requires cache-save success omits a consequential retry path. [Source lines 1079–1083](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1079) populate the memory cache before persistence and retain it on error. A subsequent Verify can find no pending blocks and return success without persisting at [lines 1057–1059](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1057). Download similarly populates memory before saving at lines 946–953.

   Corrective wording: “Paths that attempt cache persistence return its error, but do not roll back populated memory entries. A later Verify on the same manager may reuse those entries and succeed without retrying persistence when no blocks remain pending.”

4. **Major — Local busy retries can replay the callback.**
   Affected: [reverse spec, line 104](checkpoint-go-reverse-spec-2026-10-30.md?plain=1#L104). The retry description reads as acquisition waiting, but [source lines 55–57](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/local_gc.go#L55) classify the entire attempt’s returned error. A callback error matching `ErrLocalMaintenanceBusy` is returned after release at lines 147–148, so the outer loop can reacquire and invoke the callback again—even after acknowledged side effects.

   Corrective wording: “The outer retry applies to any attempt error matching ErrLocalMaintenanceBusy, including a callback error returned after gate release. Such an error can cause callback replay; callback effects are not rolled back.”

Read coverage: persona and re-implementation standard completely; both target documents completely; all seven checkpoint production files completely. Narrow supporting reads covered SQL policy, materializer snapshot production, filesystem-owner prerequisites, production hook behavior, claim readback tests, Local release-ACK tests, and selected checkpoint helpers/cache/publication tests.

Missing coverage: the remaining checkpoint tests, provider implementations, tagged hook implementation in full, and end-to-end recovery/operator integration. No writes, tests, memory/history access, or delegation occurred. The recorded test pass was not independently verified; this is not a clean or exhaustive verdict.

### 01a18b90-d9f8-7483-b32a-74e539889f8b

Verified HEAD: `008f8e576ad76425182e10a69b987f10cfb48b2a`. Four bounded findings:

1. **Blocker — cache persistence is overstated.**
   [Spec line 80](checkpoint-go-reverse-spec-2026-10-30.md?plain=1#L80) says verification success requires successful cache persistence. However, entries enter memory before persistence at [checkpoint.go lines 1079–1083](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1079). After a save failure, retrying can reach the empty-pending success branch at lines 1057–1059 without saving again. Download cache-save failures can leave the same reusable entries.
   **Corrective wording:** “Cache-save errors are returned without rolling back in-memory block entries. A subsequent Verify may succeed using those entries without retrying persistence; the no-pending-block branch does not save the cache.”

2. **Blocker — cancellation contract omits gate acquisition.**
   [Spec lines 104–106](checkpoint-go-reverse-spec-2026-10-30.md?plain=1#L104) broadly says request cancellation prevents the next RPC dispatch. During acquisition, [local_gc.go lines 76–78](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/local_gc.go#L76) instead stores `context.WithoutCancel(ctx)` as the admission request. Subsequent acquisition reads, held-token upload, and confirmation can therefore continue after cancellation. The callback is separately suppressed at lines 123–126. The five-second contention timer does not interrupt that acquisition attempt.
   **Corrective wording:** “Cancellation is checked before each acquisition attempt. Once an attempt begins, acquisition RPC admission ignores original-request cancellation. After confirmation, cancellation suppresses the callback and the gate is released. Callback storage admission uses the original request.”

3. **Major — promotion does not establish equality with the supplied descriptor.**
   [Spec lines 46–50](checkpoint-go-reverse-spec-2026-10-30.md?plain=1#L46) omits an observable identity distinction. [checkpoint.go lines 661–663](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L661) validates the canonical stored root but discards the returned descriptor. Successful promotion remembers `*root`, the caller’s descriptor, at lines 718 and 724. It does not compare supplied Hash, Files, Size, Generation, or policy with the opened root. This affects subsequent Latest and dedup behavior.
   **Corrective wording:** “OpenRoot validates the stored object identified by Index and RootHash; its returned descriptor is discarded. Successful promotion stores a shallow copy of the supplied descriptor in certified memory without comparing its remaining fields with the stored object.”

4. **Major — automatic candidate retry has an earlier bypass.**
   [Spec line 122](checkpoint-go-reverse-spec-2026-10-30.md?plain=1#L122) describes retained candidates as retried on the next call. At [auto.go lines 177–181](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/auto.go#L177), `Latest.Index >= material.Tip()` returns success before candidate processing. Thus, after a callback promotes successfully but returns an error, a later call can bypass candidate publication, release, and clearing.
   **Corrective wording:** “Candidate retry occurs only after the initial Latest-versus-materializer-Tip shortcut. If that shortcut succeeds, create returns Latest.Index without processing, releasing, or clearing an existing candidate.”

Read completely: the supplied persona and standard, both target documents, and all seven checkpoint production files. Narrow supporting reads covered SQL policy, materializer snapshot/Tip APIs, filesystem-owner identity checks, category hooks, and claim/readback, release-ACK, listing, cancellation, stream, and observer test helpers.

**Coverage remains incomplete:** cost-test/Versity infrastructure and transitive snapshot/provider implementations were not fully audited; reference destinations were not exhaustively checked. No clean verdict. No writes, tests, memory, history inspection, or delegation were performed.

## 수정 후 재점검

### 01a18b90-dc30-7da0-8047-11fdb42841ff

All four prior Major findings are resolved in the revised [reverse spec](checkpoint-go-reverse-spec-2026-10-30.md):

- **Line 46:** distinguishes stored-root validation from the supplied descriptor retained in memory.
- **Line 106:** explicitly requires both `LocalBucket` wrapping and the callback context.
- **Line 80:** documents retained memory entries after cache-save failure and subsequent Verify success without persistence retry.
- **Line 104:** documents callback replay on matching busy errors and preservation of acknowledged effects.

Rechecked the exact corresponding source branches. No residual discrepancy among these four findings; no conflicting wording found in the appendix’s relevant passages.

Prior coverage gaps remain: remaining tests, provider implementations, full tagged hooks, and end-to-end recovery/operator integration. This targeted rereview does not establish an overall clean verdict. No tests or writes performed.

### 01a18b90-d9f8-7483-b32a-74e539889f8b

All four findings are resolved in the revised spec:

- **Cache persistence:** line 80 now documents retained memory entries and successful Verify retries without another save.
- **Gate cancellation:** lines 104–108 distinguish acquisition admission from callback admission.
- **Promotion identity:** line 46 states that the opened descriptor is discarded and certified memory retains the supplied descriptor.
- **Candidate retry:** line 124 documents the early Latest/Tip bypass and skipped candidate processing, release, and clearing.

These match the relevant branches in `checkpoint.go`, `local_gc.go`, and `auto.go`. No residual discrepancy for these four findings.

Earlier coverage gaps remain: cost-test/Versity infrastructure, transitive snapshot/provider implementations, and exhaustive reference validation. This is a targeted closure, not a whole-document clean verdict. No tests or writes performed.

## 최종 상태

두 검토의 지적 8건(중복 제외 6개 계약)을 문서에 반영하고 각 검토자가 해소를 재확인했다. 전체 명세의 무결점 판정은 아니다. 실제 실행은 `go test -mod=readonly -count=1 -timeout=180s ./pkg/checkpoint`, exit 0, 27.725s였다. 외부 클라우드·emulator·race·Chaos·성능 및 전체 복구 통합은 검증하지 않았다. 구현과 의존성은 변경하지 않았다. 실행 에이전트는 테스트 프로세스 정상 종료와 별도 서버·컨테이너·수동 임시 파일 미생성을 보고했다. 테스트 내부 임시 경로 삭제는 별도 실측하지 않았다. 작성용 임시 worktree는 두 문서 일치 확인 후 제거했다. 기존 QLog 문서 3개는 그대로 보존했다.
