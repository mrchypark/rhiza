# Archive·복구 EGM 독립 검토 기록

기준 HEAD: `008f8e576ad76425182e10a69b987f10cfb48b2a`. 두 Goldfish를 fork_context=false로 실행했다. 전체 5단계 승인이나 재구현 인증은 아니다.

## 초안 검토

### 01a18bc7-58c7-7200-a719-aa403034c305

## What this proposes

The documents provide a bounded specification for reproducing HA archive recovery and Local backup recovery, with a companion format reference. They distinguish storage integrity, consensus certification, external fencing, and application installation rather than treating archive loading as complete recovery.

## How the existing system works, per this document

HA archives publish immutable extents through a mutable HEAD; pins protect recovery inputs, while fork either copies certified history or materializes a new anchored generation. Local startup reconciles surviving history with the archive, stages replacement state, resumes journaled installation, and then verifies and replays the installed state before completing import cleanup.

**No Blocker established. Three Major corrections and one Nit follow.** Paths below are relative to `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza`.

1. **Major — Held-maintenance startup transitions omit the filesystem-owner prerequisite.**
   **Document:** `docs/archive-recovery-go-reverse-spec-2026-10-30.md:287` describes equal tips plus held maintenance as staged replay followed by control recovery.
   **Source:** `pkg/node/local_backup.go:250–255` rejects unresolved maintenance immediately when `n.localOwner == nil`, before reaching history comparison or staging.
   **Correction:** Add this earlier admission branch explicitly: unresolved maintenance without filesystem ownership fails startup, regardless of relative tips. Qualify the table’s held-maintenance recovery branches as filesystem-owned cases. Otherwise a reimplementation could attempt automatic remote gate recovery that production refuses.

2. **Major — Restore protection’s lifetime and error propagation are missing.**
   **Document:** main document `:295–312` describes acquisition, renewal, and installation, but does not specify when protection ends.
   **Source:** `pkg/node/local_backup.go:650–665` transfers successful staging protection into `n.localBackupRestorePins` during an open Node. `pkg/node/node.go:277–280` retains and closes it when `Open` returns, joining cancellation and close errors into the return value; `:300–302` connects protection cancellation to Node cancellation, and `:575–578` reconfirms protection before materializer installation.
   **Correction:** State that protection survives staging and file installation through normal startup. Document the transfer, reconfirmation, cancellation propagation, and deferred close-error handling. Successful file moves alone do not end the protected recovery interval or establish successful startup.

3. **Major — Installation’s durable commitment boundary and restart ordering need explicit treatment.**
   **Document:** main document `:301–305` says to persist `install.json`, reconfirm protection, then install, but leaves the consequence of failure between those steps implicit.
   **Source:** `pkg/node/local_backup.go:781–787` writes the journal **before** the final protection confirmation. On restart, `:197` calls `finishLocalBackupImport` before ordinary archive reconciliation; `:846–926` resumes a committed journal rather than discarding it. Owner verification reopens and compares the remote binding (`pkg/node/local_backup_staging.go:70–89`).
   **Correction:** Explain that failure after journal persistence can leave committed installation work even when the final confirmation failed and no moves began. The next startup resumes that journal before tip reconciliation; it does not rerun the original staging/pin-acquisition sequence. Distinguish this from owned pre-install payload, which may be discarded after binding verification.

4. **Nit — Trim’s sealed check precedes its already-trimmed fast path.**
   **Document:** main document `:188` presents `through <= base` success before sealed rejection.
   **Source:** `pkg/recovery/archive.go:466–473` rejects sealed HEAD first.
   **Correction:** Specify that even an already-covered trim returns `ErrArchiveSealed` on a sealed archive, after earlier admission and payload checks.

**Known incomplete coverage:** Local JSON/API contracts remain incomplete, as acknowledged at `docs/archive-recovery-go-formats-2026-10-30.md:3`; consolidate that completion work rather than counting each missing symbol separately.

**Actual coverage:** Read the supplied persona and standard and both target documents fully. Confirmed current HEAD matches `008f8e576ad76425182e10a69b987f10cfb48b2a`. Read `application_evidence.go`, Local staging ownership, Local Node pin management, and startup guard fully; inspected bounded production sections for archive trim/codec, Local open/publication, fork, anchor verification/materialization, import staging/install, and Node startup. Other production sections and transitive certification/materializer implementations were not exhaustively audited. No tests, writes, memory, history inspection, delegation, cloud SDK inspection, or transitive test exploration occurred.

### 01a18bc7-56d9-7482-aba2-137bc8651aea

Completed the bounded read-only audit at verified HEAD `008f8e576ad76425182e10a69b987f10cfb48b2a`.

- **Blocker — Trim’s no-op precedence contradicts code.** [Spec line 188](archive-recovery-go-reverse-spec-2026-10-30.md?plain=1#L188) says `through <= base` succeeds before mentioning sealed rejection. [archive.go:466](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L466) rejects sealed HEADs **before** that no-op check. Correction: explicitly order payload validation → sealed rejection → already-trimmed success → tip/generation checks. A sealed, already-trimmed request returns `ErrArchiveSealed`.

- **Major — Startup table omits an earlier remote-maintenance rejection.** [Spec line 275](archive-recovery-go-reverse-spec-2026-10-30.md?plain=1#L275) presents reconciliation branches without their ownership prerequisite. [local_backup.go:246](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L246) rejects unresolved maintenance whenever `localOwner == nil`, before policy inspection, recorder recovery, prefix comparison, or staging. Correction: add this admission gate before the table; held-maintenance replay branches apply only with filesystem ownership.

- **Major — Local synchronization’s durable partial progress is missing.** [Spec line 271](archive-recovery-go-reverse-spec-2026-10-30.md?plain=1#L271) describes extent/HEAD ordering but omits that [local.go:495](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L495) publishes HEAD **inside each extent iteration**. Correction: document that a multi-extent synchronization may return an error after publishing a shorter prefix; subsequent work resumes from that prefix. Sticky uncertainty applies to potentially dispatched HEAD failures, not every later failure.

- **Major — Local extent GC’s eligibility contract is absent.** [Spec line 315](archive-recovery-go-reverse-spec-2026-10-30.md?plain=1#L315) gives retention policy but omits the deletion algorithm in [local_gc.go:170](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local_gc.go#L170). Correction: distinguish Local GC from HA’s two-scan markers: Local selects unreachable extents directly using nonzero `LastModified` strictly before the grace cutoff; malformed/noncanonical extent names fail the scan, and active protection suppresses deletion. These choices affect observable errors and deletion timing.

- **Major — Anchored-source materialization omits a required authority transition.** [Spec line 226](archive-recovery-go-reverse-spec-2026-10-30.md?plain=1#L226) summarizes root restoration and suffix certification without specifying [anchor.go:352](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/anchor.go#L352): anchored sources first install `FencedGenerationBase` using bootstrap config ID and the source anchor’s tip, prefix, root, and anchor hash; a regular checkpoint base is restored afterward when present. Correction: add this ordered branch, including source-lineage verification at [anchor.go:259](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/anchor.go#L259). Root restoration alone does not reconstruct Observer authority.

The acknowledged Local JSON/API coverage gap remains one consolidated item at [formats line 3](archive-recovery-go-formats-2026-10-30.md?plain=1#L3); I have not expanded it into symbol-level findings.

Actual coverage: read the persona, standard, both targets fully (352 and 114 lines), complete `fork.go`, `seal.go`, and `membership.go`; inspected bounded production sections of archive loading/publication/trim/GC, anchor verification/materialization/resumption, Local opening/publication/GC, node reconciliation/staging, and startup protection. Source coverage was **partial**, not all of `pkg/recovery`: remaining codec/lease/pin/application-evidence paths and cited Core, network, checkpoint, and node integration details were not fully independently verified. No tests, writes, memory, history inspection, delegation, cloud SDK inspection, or transitive test review.

## 1차 재점검

### 01a18bc7-56d9-7482-aba2-137bc8651aea

All five previous findings are **resolved**:

- Trim rejection precedence: spec line 188.
- Remote maintenance admission gate: line 278.
- Local sync’s durable partial progress: line 274.
- Local extent GC eligibility/errors: line 323.
- Anchored-source Observer authority transition: line 223.

I rechecked each against its production branch. No residual discrepancy in those corrections.

**One consolidated Major remains in the new format supplement:** [formats lines 120–125](archive-recovery-go-formats-2026-10-30.md?plain=1#L120) still leaves material serialization choices implicit:

- Local format version is **2**, despite namespace `local/v1`; state the literal. Source: [config.go:28](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/types/config.go#L28).
- Nested `bootstrap`, `seal`, and `decision` schemas remain unspecified. In particular, `decision.value` is base64 JSON, while `certificate` and `descriptor` are embedded JSON (`json.RawMessage`). Sources: [types.go:72](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L72), [types.go:154](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L154), [local.go:34](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L34).

Correction: add these nested field/tag contracts and encoding distinctions. This is the remaining portion of the previously acknowledged format gap, not separate symbol-level findings.

The listed Local keys, top-level tags, bounds, previous-object rules, pin fields, and API signatures matched the declarations inspected.

Coverage: targeted revised passages, the complete Local format supplement and API table, and corresponding declarations/validation branches. No whole-codebase re-audit, runtime verification, tests, writes, or memory operations. Newly added Node protection-lifetime prose was not independently traced end-to-end.

### 01a18bc7-58c7-7200-a719-aa403034c305

**Targeted rereview complete: three original findings resolved; the lifetime finding has a wording residual. The Local supplement still has one material format gap.**

| Original finding | Result |
|---|---|
| Held-maintenance prerequisite | **Resolved.** Main document line 278 explicitly requires filesystem ownership, matching `local_backup.go:246–247`. |
| Restore-protection lifetime | **Mostly resolved.** Main line 306 documents transfer, cancellation, reconfirmation, and close errors. However, “Node가 열린 동안” implies the entire running lifetime. Protection ends when **`Node.Open` returns**, including successful return (`node.go:277–280`). Replace that phrase with “`Node.Open` 실행이 끝날 때까지.” |
| Durable install commitment/restart ordering | **Resolved.** Main line 312 correctly distinguishes committed journal resumption from disposable pre-install staging, matching `local_backup.go:197` and `:789–795`. |
| Sealed trim precedence | **Resolved.** Main line 188 matches `archive.go:466–473`. |

**Major — Local nested wire formats remain underspecified.**
[Format supplement line 123](archive-recovery-go-formats-2026-10-30.md?plain=1#L123) names `seal` and `decision` without defining their nested serialization. In particular, `decision.value` is a **base64 JSON string**, while `decision.certificate` and `descriptor` are **embedded JSON**, not base64 strings. See [DecidedValue declaration](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L154) and [LocalSnapshot declaration](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L34). Add the nested tags/omission rules, explicitly state archive format version **2**, and include the required `decision.slot > seal.index` constraint (`local.go:688`). These are necessary to reproduce compatible JSON without guessing.

**Nit — Disambiguate pin hashes.**
Supplement lines 121–124 define `head_hash` using original HEAD bytes, but pin creation hashes `json.Marshal(a.head)` (`local_gc.go:366–370`); pin binding validation likewise hashes marshaled binding (`:501–503`). Specify these separately from the raw-byte HEAD/BINDING hashes. They coincide for canonical writer output, but need not for accepted noncanonical JSON.

**Coverage:** Read the full revised format appendix and targeted revised main sections, including the API table. Compared Local declarations, JSON validation, pin hashes, and the four original source boundaries. Listed Local API signatures matched. Did not repeat the whole HA audit or inspect transitive implementations. No tests, writes, memory, history, or delegation.

## 최종 재점검

### 01a18bc7-56d9-7482-aba2-137bc8651aea

**Resolved.** My remaining consolidated Local format finding is closed: version **2**, nested bootstrap/seal/decision fields, omission tags, base64 versus embedded JSON, and snapshot constraints now match the inspected declarations.

The raw-versus-marshaled pin hash distinction and protection lifetime through `Node.Open` return also match the targeted source paths. No material contradiction remains in the checked passages.

Coverage was limited to the revised Local format section, related narrative, and corresponding declarations/hash/lifetime branches. No broader audit, tests, or writes.

### 01a18bc7-58c7-7200-a719-aa403034c305

**All findings in this targeted rereview are resolved.**

- **Lifetime:** Main document line 306 now correctly ends protection when `Node.Open` returns, matching `node.go:277–280`.
- **Local format:** Supplement lines 120–127 now specify version **2**, nested bootstrap/seal/decision fields, omission rules, base64 versus embedded JSON, and `decision.slot > seal.index`. These match the checked declarations and validation.
- **Pin hashes:** Supplement lines 121 and 124 correctly distinguish raw archive hashes from marshaled pin hashes, including binding creation versus validation.

No contradictory wording found for these contracts. Coverage was limited to these revised passages, focused contradiction searches in both canonical documents, and corresponding declarations/validation. No broad audit, tests, or writes.

## 검증·정리와 작업 범위

원 지적 9건(중복 포함)과 Local 형식 보완을 반영했다. 1차 재점검에서 발견한 lifetime 문구·중첩 JSON·pin hash 구분을 다시 수정했고 두 검토자가 최종 해소를 확인했다. 전체 명세의 무결점 판정은 아니다. 코드는 수정하지 않았다.

실행: `env RHIZA_VERSITYGW_BIN= go test -mod=readonly -count=1 -timeout=180s ./pkg/recovery` exit 0, 41.081s. 두 node 선별 테스트 명령과 정규식은 본문에 보존했으며 각각 exit 0, 5.204s와 14.415s였다. 전체 node 테스트·외부 SDK 세 provider 계약·Versity·실제 클라우드·Chaos·성능 검증은 실행하지 않았다. 테스트 프로세스는 정상 종료했고 별도 서버·emulator·컨테이너·수동 임시 리소스는 시작하지 않았다. 테스트 내부 임시 경로 삭제는 별도 실측하지 않았다.

첫 작성자는 탐색 후 파일을 생성하지 않아 중단했다. 부모가 두 근거 보고서를 통합해 초안을 저장했고, 두 번째 문서 편집자가 격리 체크아웃에서 Local 형식·API와 검토 지적을 보완했다. 작성 지연을 검토 완료로 계산하지 않았으며 이 사건은 작업 내 기록으로만 남긴다. 두 문서 일치 확인 후 작성용 임시 worktree를 제거했다. 기존 QLog·Checkpoint·Object-store 문서 아홉 개와 구현·의존성은 보존했다.
