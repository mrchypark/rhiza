# Object-store adapter EGM 독립 검토 기록

기준 HEAD: `008f8e576ad76425182e10a69b987f10cfb48b2a`. 두 Goldfish를 fork_context=false로 실행했다. 전체 5단계 승인이나 독립 재구현 인증은 아니다.

## 초안 검토 원문

### 01a18ba8-9d8b-7132-a200-829298896327

Reviewed HEAD `008f8e576ad76425182e10a69b987f10cfb48b2a`. **One Blocker and two Major gaps; no clean verdict.**

1. **Blocker — relative DataDir can bypass the documented overlap rejection.**
   [Reverse spec:62](object-store-go-reverse-spec-2026-10-30.md?plain=1#L62) and [contracts:238](object-store-go-contracts-2026-10-30.md?plain=1#L238) state that overlapping root/DataDir paths are rejected. Implementation makes only root absolute; DataDir receives `EvalSymlinks` without `Abs`. If it remains relative, `filepath.Rel` against absolute root fails, and `pathContains` returns false. See [local_owner.go:36–64](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner.go#L36).
   **Correction:** describe this actual limitation: overlap rejection depends on successful relative-path comparison; a relative resolved DataDir can evade it. Do not describe both paths as unconditionally canonical absolute paths.

2. **Major — body rejection and active-slot completion need separate rules.**
   [Reverse spec:50](object-store-go-reverse-spec-2026-10-30.md?plain=1#L50) and [contracts:134](object-store-go-contracts-2026-10-30.md?plain=1#L134) flatten several conditions into “unknown.” In [mutation_drain.go:34–52](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/mutation_drain.go#L34), exceeding Content-Length or the inspection buffer only sets `r.known=false`; it does **not** immediately decrement active or publish sticky drain unknown. Completion still requires EOF, exact positive length, a non-EOF error, or Close. Error-marker inspection likewise occurs only at the completion branch.
   **Correction:** explicitly distinguish local unknown candidacy from `finish(false)`/`end(false)`. Explain that an overlong body left open can keep drain waiting rather than immediately returning `ErrMutationOutcomeUnknown`.

3. **Major — documented test infrastructure omits the Expect/trace helper contract.**
   The fixture discussion at [reverse spec:118–122](object-store-go-reverse-spec-2026-10-30.md?plain=1#L118) does not cover the infrastructure underlying its cited Expect evidence. [providertransport_test.go:27–71](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/providertransport_test.go#L27) defines a **test-only** policy: PUT, nonnil body, declared size ≥1 MiB, and nonempty conditional header; clone request/headers and insert `Expect: 100-continue` only when absent. Lines 74–229 define bounded trace schemas, 64-attempt/64-event limits, dropped counters, and post-snapshot callback suppression.
   **Correction:** document these helper contracts and `RHIZA_VERSITY_HTTP_TRACE`, explicitly identifying them as test infrastructure rather than production provider behavior.

Coverage: read both targets fully, supplied persona/standard, all listed `internal/objstore` production files including every OS/build-tag variant, and `versityfixture/fixture.go`. Read cited caller sections for ownership, capabilities, extent attribution, and terminal drain; read the Expect/trace helper definitions and Azure startup fixture.

Missing reads: remaining test-helper implementations and complete cited caller files were not exhaustively audited; provider SDK internals were not audited. Consequently, test-infrastructure completeness and every caller claim remain unverified. No writes, tests, memory access, history inspection, or delegation occurred.

### 01a18ba8-9fc2-7fe2-a520-a432c2f84c12

**What this proposes**

These documents specify the existing object-store adapter for faithful reimplementation. They cover provider construction, metrics, mutation drain, filesystem ownership, optional diagnostics, and the Versity fixture without assigning SDK or cloud guarantees to the adapter.

**How the existing system works, per this document**

A metered Bucket delegates storage operations while tracking logical calls and HTTP attempts separately. Mutation drain permanently stops new mutations and retains unknown outcomes across retries. Filesystem ownership has a separate lifetime, including open readers, and poisoned ownership retains locks until process exit.

**Findings: 2 Major, 1 Nit. No Blocker identified within the inspected scope.**

1. **Major — Relative DataDir breaks the stated disjointness contract.**
   [Reverse spec, line 62](object-store-go-reverse-spec-2026-10-30.md?plain=1#L62) says canonical root and data paths are rejected whenever they overlap. However, [local_owner.go, lines 36–51](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner.go#L36) makes only root absolute. A relative DataDir can remain relative after EvalSymlinks; [pathContains, lines 62–64](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner.go#L62) treats a filepath.Rel error as false. Both containment checks can consequently fail to detect overlap.
   **Correction:** Describe the actual normalization asymmetry and its relative-path limitation. State that an absolute resolved DataDir is needed for the documented containment check; do not describe that prerequisite as adapter-enforced validation.

2. **Major — Other-platform runtime rejection is not an established executable contract.**
   [Reverse spec, line 70](object-store-go-reverse-spec-2026-10-30.md?plain=1#L70) and [contracts, line 248](object-store-go-contracts-2026-10-30.md?plain=1#L248) promise an unsupported-platform error. The fallback file defines that error path, but [local_owner.go, lines 154 and 161](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner.go#L154) unconditionally references unlockClose. Its sole definition is in [local_owner_unix.go, lines 1 and 48](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner_unix.go#L1), selected only for Darwin/Linux.
   **Correction:** Document that the fallback expresses intended rejection, while the package as written has an unresolved-symbol build limitation outside Darwin/Linux. This is a static source finding; I did not cross-compile.

3. **Nit — One lifetime sentence contradicts the precise completion rule.**
   [Reverse spec, line 48](object-store-go-reverse-spec-2026-10-30.md?plain=1#L48) says successful mutation bodies remain tracked until the SDK reads **and closes** them. In [mutation_drain.go, lines 44–52](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/mutation_drain.go#L44), EOF or exact positive Content-Length consumption already ends active tracking. A subsequent Close error can still make unknown sticky, as lines 57–65 implement and document line 50 explains.
   **Correction:** Replace “reads and closes” with the exact consumption boundary, followed by the separate late-Close-error rule. Distinguish this from replay readers, whose slots require Close.

**Coverage and limits**

- Confirmed HEAD `008f8e576ad76425182e10a69b987f10cfb48b2a`. Both target documents were untracked working-tree files.
- Fully read both targets, the supplied persona and standard, all production files directly under `internal/objstore` including every OS/build-tag variant, and `versityfixture/fixture.go`.
- Checked cited caller sections in Node shutdown/fencing, local backup construction, recovery binding/capabilities, and extent attribution.
- Read Azure startup testing fully; inspected selected S3 retry/multipart/conditional/Expect tests, fixture supervisor tests, and all four opt-in skip gates.
- **Incomplete test-infrastructure coverage:** remaining test bodies/helpers were not fully read. SDK implementations and raw execution logs were not inspected. Reported historical passes and cleanup therefore remain unverified here.

No writes, tests, memory access, history retrieval, or delegation were performed. This is not a clean or exhaustive reimplementation sign-off.

## 수정 후 재점검

### 01a18ba8-9d8b-7132-a200-829298896327

All three prior findings are **resolved in the documentation**:

- **Blocker: relative DataDir** — [reverse spec:62](object-store-go-reverse-spec-2026-10-30.md?plain=1#L62) and contracts:238,251 now accurately describe the overlap-check bypass in `local_owner.go:35–64`. The implementation limitation remains explicitly documented.
- **Major: body completion** — reverse spec:50 and [contracts:134](object-store-go-contracts-2026-10-30.md?plain=1#L134) now distinguish local `known=false`, completion, sticky unknown, and active-slot release, matching `mutation_drain.go:22–68`.
- **Major: Expect/trace infrastructure** — [reverse spec:146–152](object-store-go-reverse-spec-2026-10-30.md?plain=1#L146) and contracts:378–410 now cover eligibility, cloning, header insertion, opt-ins, schemas, limits, and snapshot suppression consistently with the relevant helpers.

**No residual discrepancy found within these three findings.** This is a targeted closure, not exhaustive sign-off. Previous limits remain: remaining test helpers, complete caller files, and SDK internals were not exhaustively audited. No writes or tests performed.

### 01a18ba8-9fc2-7fe2-a520-a432c2f84c12

All three prior findings are **resolved in the documentation**:

- **Major — Relative DataDir:** reverse spec line 62 and contracts lines 238/251 now describe the normalization asymmetry, missed-overlap possibility, and unenforced absolute-path prerequisite. Matches `local_owner.go:35–64`.
- **Major — Other-platform behavior:** reverse spec lines 70–72 and contracts lines 248/253 now distinguish the fallback error from the unresolved `unlockClose` build limitation. Matches `local_owner.go:154/161`, `local_owner_unix.go:1/48`, and `local_owner_unsupported.go:7–8`.
- **Nit — Mutation-body lifetime:** reverse spec lines 48–50 and contracts line 134 now distinguish completion on consumption from Close, including late Close errors becoming sticky unknown. Matches `mutation_drain.go:31–67`.

**No residual discrepancy in these three findings.** Source limitations remain documented, not fixed.

Targeted rereview only; prior coverage limits remain. No tests, cross-compilation, writes, or expanded review performed; this is not a full-document sign-off.

## 검증과 최종 상태

지적 6건(중복 포함)을 문서에 반영하고 각 검토자가 해소를 재확인했다. 상대 DataDir와 기타 OS의 빌드 한계는 소스 확인 결과이며 새 실행 재현·cross-compile·구현 수정을 하지 않았다. 전체 문서의 무결점 판정이 아니다.

로컬 테스트는 gateway/helper opt-in을 비우고 GOPROXY=off에서 `go test -mod=readonly -count=1 -timeout=180s -v ./internal/objstore ./internal/objstore/versityfixture`를 실행해 exit 0이었다. objstore 2.495s, fixture 41.962s. 최초 GOSUMDB=off 실행은 toolchain 검증에서 테스트 시작 전에 실패했고 정상 checksum 검증으로 재실행했다.

Versity의 ConditionalLargePutEvidence, GatewayCostEvidence, SDKRetryEvidence는 RHIZA_VERSITYGW_BIN 부재로, ConditionalLargePutExpectEvidence는 RHIZA_VERSITY_EXPECT_CONTINUE 미설정으로 건너뛰었다. S3 로컬 HTTP fixture와 Azure 로컬 TLS 생성 경로만 부분 확인했고 GCS 객체 연산 및 Floci S3/GCS/Azure 계약 테스트는 실행하지 않았다. 따라서 3-provider 계약·실제 클라우드 parity·Chaos·race·성능 qualification은 미검증이다.

검증 에이전트는 잔여 테스트 프로세스·관련 임시 디렉터리와 관측된 go-build 디렉터리가 남지 않았음을 확인했다. live service·gateway·emulator는 시작하지 않았다. 문서 작성용 격리 worktree는 두 파일 일치 확인 후 제거했다. 이전 QLog/Checkpoint 문서 여섯 개와 구현·의존성은 보존했다.
