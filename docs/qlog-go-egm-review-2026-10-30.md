# QLog EGM 독립 검토 기록

기준 HEAD: `008f8e576ad76425182e10a69b987f10cfb48b2a`. 두 Goldfish를 fork_context=false로 실행했다. 전체 5단계 승인이나 재구현 인증은 아니다.

## 초안 검토

### 01a18b82-9001-7800-a77a-baefadd37cb2

**What this proposes**

These documents specify the existing QLog implementation for independent reimplementation. The main document defines API behavior and lifecycle; the appendix defines entry and manifest bytes.

**How the existing system works, per this document**

The caller owns process locking, opening, syncing, recovery sequencing, and shutdown. A selected manifest determines the authoritative segments; compaction rewrites an older prefix while appends continue in a separate tail. Publication distinguishes rename success from confirmed directory durability, and some uncertain failures poison subsequent writes.

1. **Major — RestoreSegment’s readonly behavior is incomplete.**
   [Reverse spec, lines 143–147](qlog-go-reverse-spec-2026-10-30.md?plain=1#L143) explains that readonly objects lack a rejection check, but omits a significant consequence: `OpenReadOnly` leaves the I/O hooks nil ([wal.go:187](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L187)). Restoring a new index can publish the segment, change the in-memory list, then panic when manifest publication calls the nil `writeAt` hook ([wal.go:959–976](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L959), [wal.go:581](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L581)).
   **Correction:** Document this branch explicitly, distinguishing identical-data success, current-segment replacement, and new-index mutation followed by panic. “No readonly guard” alone does not reproduce the observable contract.

2. **Major — Finalization lacks a cross-phase prerequisite.**
   [Reverse spec, line 120](qlog-go-reverse-spec-2026-10-30.md?plain=1#L120) describes finalization without explaining interference with an unfinished compaction. Finalization holds only the WAL mutex and deletes unreferenced segments and internal temporary files ([wal.go:150–168](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L150), [wal.go:421–434](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L421)). After successful `Build`, the target remains unreferenced until `Commit`; finalization can delete it, causing Commit’s open to fail ([wal.go:1166–1185](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L1166)).
   **Correction:** State that finalization does not coordinate with `compactMu`; callers must finish or abort compaction before finalizing to preserve its files. Include the concrete Build → Finalize → Commit failure sequence.

3. **Nit — Estimate’s error return omits partial output semantics.**
   [Reverse spec, line 157](qlog-go-reverse-spec-2026-10-30.md?plain=1#L157) describes summation and overflow rejection but not the returned size on failure. The implementation returns accumulated `size` alongside the error ([wal.go:779–788](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L779)).
   **Correction:** Specify that failure can return a nonzero partial count, which is not a complete estimate.

Coverage: verified HEAD matches the requested commit. Read both target documents, persona, standard, and all four production files completely. Inspected selected cited test infrastructure, including the complete hard-kill test; did not read every cited test completely or independently verify the reported test execution. No writes, tests, memory, history, or delegation. No Blocker identified in the reviewed scope; this is not a clean verdict.

### 01a18b82-8cb8-7920-ad8c-0fc2e560d393

Verified HEAD: `008f8e576ad76425182e10a69b987f10cfb48b2a`.

1. **Major — Read-only restore can panic after publishing files.**
   [Spec line 143](qlog-go-reverse-spec-2026-10-30.md?plain=1#L143) notes the missing read-only guard, but lines 145–147 describe ordinary restore outcomes without this exception. `OpenReadOnly` leaves the I/O hooks nil ([wal.go:187](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L187)). Restoring a **new index** first installs the segment, then manifest publication invokes nil `writeAt` ([wal.go:960](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L960), [wal.go:1032](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L1032)). **Correction:** distinguish identical-data success, current-segment replacement, and new-index panic with preceding filesystem mutations; do not imply this path reliably returns an error.

2. **Major — Manifestless read-only restore has an undocumented rollback panic.**
   [Spec line 147](qlog-go-reverse-spec-2026-10-30.md?plain=1#L147) promises rollback on pre-publication failure. A manifestless read-only WAL has no segments. For valid nonempty data at index 0, restore installs `seg_000.log`; manifest encoding rejects index 0, and rollback indexes the now-empty segment list ([wal.go:976–984](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L976)). **Correction:** document the panic and the installed file remaining because execution never reaches removal.

3. **Major — Cited test infrastructure is named but not specified.**
   [Spec line 179](qlog-go-reverse-spec-2026-10-30.md?plain=1#L179) lists coverage without describing the mechanisms required by the supplied standard/persona. The crash harness passes JSON `{Dir, Phase, ReadyFile}` through `RHIZA_LOCAL_WAL_CRASH`, launches the same test binary, waits up to 10 seconds for a marker, then kills and waits for signal termination ([local_compaction_crash_test.go:14](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/local_compaction_crash_test.go#L14), [line 156](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/local_compaction_crash_test.go#L156)). **Correction:** add a bounded infrastructure contract covering both boundary names, subprocess handshake, fault hooks, and the distinction between the package callback and tagged `localtesthooks.Hit`.

Read coverage: persona, standard, both target documents, and all four requested production files read completely. Crash harness read completely; other cited tests inspected selectively for helpers and coverage anchors. Remaining test bodies and unrelated hook APIs were not fully audited; reported historical test execution was not independently verified. No writes, tests, memory, history, or delegation performed. **Review remains qualified, not a clean verdict.**

## 수정 후 재점검

### 01a18b82-9001-7800-a77a-baefadd37cb2

Targeted rereview complete. All three findings are resolved:

- **Major, readonly RestoreSegment:** [lines 149–151](qlog-go-reverse-spec-2026-10-30.md?plain=1#L149) now distinguish successful branches, mutation before panic, and missing rollback.
- **Major, Finalize/compaction sequencing:** [line 120](qlog-go-reverse-spec-2026-10-30.md?plain=1#L120) states the prerequisite and concrete failure sequence.
- **Nit, partial estimate:** [line 161](qlog-go-reverse-spec-2026-10-30.md?plain=1#L161) specifies partial size plus error.

No residual discrepancy in these findings. The formats appendix points to the revised behavior.

Broader coverage limitation remains: not every cited test was read completely, and reported test execution was not independently verified. This is not a whole-document clean verdict. No writes or tests performed.

### 01a18b82-8cb8-7920-ad8c-0fc2e560d393

Targeted rereview completed. All three prior findings are resolved:

- Read-only restore mutation and nil-hook panic: documented at [spec line 149](qlog-go-reverse-spec-2026-10-30.md?plain=1#L149).
- Manifestless index-0 rollback panic and remaining resources: documented at [line 151](qlog-go-reverse-spec-2026-10-30.md?plain=1#L151).
- Crash handshake, boundary ordering, tagged hooks, and I/O fault infrastructure: documented at [lines 177–183](qlog-go-reverse-spec-2026-10-30.md?plain=1#L177).

No residual discrepancy within those findings. No writes or tests performed.

Broader limitation remains: other test bodies and unrelated hook APIs were not fully audited, and historical test results were not independently verified. This is not a whole-document clean verdict.

## 최종 상태

지적 6건(중복 포함)을 반영하고 두 검토자가 모두 해소를 확인했다. readonly RestoreSegment panic 가능성은 코드에서 확인한 동작이며 실행 재현·구현 수정은 하지 않았다. 패키지 테스트 exit 0, 8.572s. 전체 테스트 재검토·race·전원 장애·Chaos 검증은 하지 않았다. 개별 테스트 임시 디렉터리 삭제는 독립 측정하지 않았다.
