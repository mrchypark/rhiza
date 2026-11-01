# Go QuePaxa EGM 독립 검토 기록

기준: `8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd`. 검토자는 `fork_context=false`로 시작했으며 아래는 초안에 대한 반환 결과이다. 수정 후 처리 상태는 문서 끝에 별도로 기록한다. 전체 EGM 5단계 승인이나 재구현 인증을 뜻하지 않는다.

## 1. Comprehension Goldfish: 초안 검토 원문

## What this proposes

This document records the behavior of the existing Go QuePaxa implementation and its direct network and node integrations. It distinguishes source-derived contracts, test expectations, and reported execution results, while excluding QLog’s physical format and the full database executor. It proposes no runtime changes.

## How the existing system works, per this document

Proposers collect durable recorder summaries, construct a certificate, and install a decision; ordinary proposal completion additionally establishes a contiguous prefix and disseminates the decision. Learning, local durability, execution, and remote acknowledgment are separate boundaries. Membership changes use freeze, drain, and terminal decisions, while checkpoints establish verified recovery floors; LocalMode adds exclusive execution ownership and capacity admission.

The overall flow is understandable. However, the following contracts still require implementation knowledge or guessing.

## Findings

**1. Major — Checkpoint preparation is described without the prerequisite that connects it to recorder acceptance.**

Document: [main specification, line 142](quepaxa-go-reverse-spec-2026-10-30.md?plain=1#L142); [durability appendix, line 98](quepaxa-go-durability-evidence-2026-10-30.md?plain=1#L98).

Source: [validator registration, core.go:1635](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1635), [recorder checkpoint gate, core.go:1641](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1641), [missing-validator failure, core.go:1781](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1781).

“Validator succeeds, then prepare persists” does not explain who supplies that validator or how preparation enables a seal vote. A fresh engineer cannot reconstruct this connection: `SetCheckpointValidator` registers the callback; an unprepared checkpoint requires it; `Record` recognizes a checkpoint value and requires its identity to be prepared.

**Concrete correction:** Document the sequence `SetCheckpointValidator → PrepareCheckpoint → propose encoded seal → recorder RequirePreparedCheckpoint → learned seal → CompactThrough`. State that preparation of a new identity fails when no validator is registered, while an already prepared matching identity can return successfully without rerunning it.

**2. Major — The described formats are insufficient to reproduce the serialized contracts.**

Document: [main specification, line 85](quepaxa-go-reverse-spec-2026-10-30.md?plain=1#L85), [line 133](quepaxa-go-reverse-spec-2026-10-30.md?plain=1#L133), [protocol appendix, line 105](quepaxa-go-protocol-evidence-2026-10-30.md?plain=1#L105).

Source: [certificate.go:10](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/certificate.go#L10), [types.go:87](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L87), [types.go:161](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L161), [compaction.go:214](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/compaction.go#L214).

The documents enumerate conceptual fields but omit exact JSON schemas for certificates, reconfiguration controls, checkpoint seals, and recovery bases. For example, certificate fields use snake-case names, hash and priority arrays serialize as numeric arrays, and `ReconfigurationID` belongs to individual summaries. Checkpoint seals additionally contain `StateHash`, two leader orders, and an optional generation anchor. Those distinctions affect interoperability and recovery.

**Concrete correction:** Add the exact field names, types, encoding and omission rules, control prefixes, and decoder acceptance rules to an appendix. Define root/state/prefix/anchor identities and distinguish checkpoint state index from seal decision slot. Express magic prefixes as explicit bytes ending in `0x00`, avoiding the ambiguous doubled-backslash notation currently used. Source links should support these contracts rather than substitute for them.

**3. Major — The recommended paging path omits its recovery-floor and return-value contracts.**

Document: [main specification, line 122](quepaxa-go-reverse-spec-2026-10-30.md?plain=1#L122); [protocol appendix, line 155](quepaxa-go-protocol-evidence-2026-10-30.md?plain=1#L155).

Source: [core.go:2992](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2992), [core.go:3017](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L3017), [core.go:3051](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L3051). The existing test explicitly preserves these errors at [core_bounded_test.go:67](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core_bounded_test.go#L67).

The documents recommend `DecisionsFrom` for lossless replay without stating that requests at or below `floor` return `ErrCompacted`. They also omit that the second return value is the current tip, potentially beyond the returned page—not the page’s last slot or next cursor.

**Concrete correction:** Include both signatures and state the normal, empty, compacted, oversized-first-item, and gap results. Explain that continuation follows the last returned item; a compacted request requires recovery-base handling outside this paging API.

**4. Major — Learner enrollment omits the persistent identity creation and reopen prerequisites.**

Document: [main specification, line 21](quepaxa-go-reverse-spec-2026-10-30.md?plain=1#L21); [protocol appendix, line 41](quepaxa-go-protocol-evidence-2026-10-30.md?plain=1#L41).

Source: [config.go:117](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/config.go#L117), [config.go:141](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/config.go#L141).

Activation requires matching WAL identities, but the constructor description never explains their origin. `NewLearner` uses an existing WAL identity or generates and binds 32 random bytes, then exposes lowercase hex. Reopening an enrolled voter requires its original matching WAL identity.

**Concrete correction:** Add these constructor side effects and failure conditions. Connect the identity returned by `WALIdentity()` to candidate registration, the target member, admission probing, and subsequent voter reopen.

**5. Major — Leader scheduling after membership activation is left undefined.**

Document: [main specification, line 81](quepaxa-go-reverse-spec-2026-10-30.md?plain=1#L81), [line 158](quepaxa-go-reverse-spec-2026-10-30.md?plain=1#L158); [membership appendix, line 15](quepaxa-go-membership-evidence-2026-10-30.md?plain=1#L15).

Source: [core.go:730](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L730), [core.go:767](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L767), [reconfiguration.go:397](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration.go#L397).

The scheduling explanation explicitly covers fixed membership. The membership explanation establishes `T+1` ownership and clears timing state, but never states that leader epochs restart there. Continuing the global `(slot−1)/16` calculation would select different leaders and schedule-control slots.

**Concrete correction:** Define generation start `S=T+1`, relative epoch `e=(slot−S)/16`, rotation during the first `2N+1` epochs of that configuration, schedule slots `S+16e`, and subsequent order lookup at `S+16(e−1)`.

**6. Major — Ordinary Record catch-up is incorrectly specified for slots 1–16.**

Document: [main specification, line 163](quepaxa-go-reverse-spec-2026-10-30.md?plain=1#L163); [membership appendix, line 42](quepaxa-go-membership-evidence-2026-10-30.md?plain=1#L42).

Source: [reconfiguration.go:440](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration.go#L440).

Both documents say ordinary requests require `slot−16`. The implementation returns zero when `slot<=16`. Since `Slot` is unsigned, implementing the written subtraction directly would underflow for slots 1–15 and request an enormous prefix.

**Concrete correction:** Specify the exact cases: disabled mode or slot zero → zero; terminal request → `slot−1`; ordinary request with `slot>16` → `slot−16`; otherwise → zero.

## Evidence and coverage

Confirmed HEAD: `8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd`.

Read completely: the supplied persona and standard; the main specification; all three evidence appendices; `go.mod`; package documentation. All three appendix links and all 44 distinct absolute source-file link targets exist. The initial path extraction also captured prose punctuation; those were not actual broken links.

Read source evidence included:

- Complete types, protocol definitions, certificate codec, configuration, group commit, WAL schema and codec.
- Core sections covering initialization, voting, proposals, hydration, reads, scheduling, recorder paths, checkpoint preparation, durability, completion, installation, certificate validation, certified ingestion, learning, paging, replay, and prefix advancement.
- Referenced production sections for reconfiguration, membership history/validation, checkpoint membership, compaction, Local reclamation/archive/costs, node membership management, transport acknowledgment rules, peer authentication, and learner admission.
- Test sections: `protocol_test.go:119–240` and `core_bounded_test.go:1–76`.

**Missing reads:** The remaining test evidence listed in protocol appendix lines 180–190, durability appendix lines 172–180, and membership appendix lines 100–112 was not independently read, except for the test sections identified above. Accordingly, this is **not a clean or exhaustive source-verification verdict**. No writes, builds, tests, shared-memory access, or history retrieval were performed.

Implementation-identical reimplementation is **not established**. The documents currently provide an operational reverse specification with useful boundaries and limitations, but the missing behavioral connections and serialization contracts prevent meeting the supplied document-only reimplementation standard.

## 2. Codebase Details Goldfish: 초안 검토의 핵심 반환 내용

검토자는 같은 기준 커밋의 초안을 대상으로 아래 여섯 항목을 반환했다. 다음 표는 원문 전체의 재현이 아닌 한국어 정리이다. 문서 행 번호는 수정 전 초안을 가리킨다.

| ID | 심각도 | 초안 위치 | 확인한 계약과 필요한 수정 |
|---|---|---|---|
| C1 | Blocker | 본문 49행 | 128KiB는 Propose·staging에서 검사하지만 일반 decision/certified 수신의 보편적 상한은 아니다. 입력 경로별로 상한을 한정해야 한다. 근거: `core.go:317,453,2591` |
| C2 | Major | 본문 103·122행 | floor 이하 AcceptDecision·EnsureDurable은 base coverage로 성공할 수 있고, Record·고정 구성 certified page·페이지 조회·CompleteDecision·비영 DurablePrefix는 다르게 거절한다. retained conflict 범위와 반환 tip도 명시해야 한다. 근거: `core.go:2197,2777,3017,3101` |
| C3 | Major | protocol 부록 30행 | certified page 수신은 transactional하지 않다. 사전 metadata 검사 뒤 순차 검증·설치로 앞부분이 남을 수 있다. 재구성 페이지는 정렬하며 control이 있으면 전체를 durable 처리한다. 근거: `core.go:2763,2785,2825` |
| C4 | Major | 본문 112행·membership 부록 71행 | 실제 OperationLearned ACK는 readiness·실행 정책 확인, 결정 설치, 연속 tip 대기 및 해당 slot까지 apply 성공 뒤에 나온다. certificate에 자기 ID가 있으면 hint를 고르는 handler와 live ISR까지 보는 AcceptCertifiedValueForAck를 구별해야 한다. 근거: `peer_server.go:387,437,452`, `server.go:2226` |
| C5 | Major | 본문 142행 | InstallFencedGenerationBase는 외부에서 검증된 관리 권한으로 fresh target의 floor를 설치한다. source quorum certificate나 checkpoint validator를 대신하지 않으며, 입력·empty WAL·재시도·anchor 보존·초기 membership 예외 조건이 필요하다. 근거: `types.go:175`, `compaction.go:66,703` |
| C6 | Major | 본문 134·157행·durability 부록 63행 | QCKP/QRCF prefix, 정확한 JSON tag·생략·배열/base64 규칙, certificate/reconfiguration의 strict decoder와 checkpoint/leader의 json.Unmarshal 차이를 명시해야 한다. 근거: `types.go:12,87,161`, `certificate.go:10` |

이 검토자는 persona·재구현 표준·본문·세 근거 부록과 QuePaxa production 파일을 읽었다. node·network의 관련 인증·라우팅·전파·probe·apply 경로, fixture 및 일부 compaction/fenced-generation/hard-kill 테스트도 읽었다. 그러나 부록이 참조한 모든 테스트, 전체 peer wire codec, 하위 실행기·QLog를 독립적으로 재점검하지 않았다. 과거 테스트 출력과 cleanup도 이 검토자가 다시 실행해 확인한 것은 아니다. 따라서 clean 또는 exhaustive 판정은 내리지 않았다.

## 3. 작업 범위 교정과 검토 한계

초기 부모 세션은 원래 작업 디렉터리의 Rust 브랜치를 먼저 대상으로 선택했다. 사용자가 Go를 명시한 뒤 세 분석자를 중단했고, 모두 파일 변경·테스트 실행이 없음을 확인했다. 이후 Go `origin/main`에서 별도 worktree를 만들고 모든 분석자에게 `go.mod`, `pkg/quepaxa`, HEAD 확인을 먼저 요구했다. 같은 체크아웃의 유효한 Go 이웃 사례에서 이 확인이 통과했다. 이 사건은 작업 내 범위 교정으로만 보존하며 전역 규칙이나 공유 메모리로 승격하지 않았다.

완료된 에이전트 여섯 개가 열려 있어 첫 Goldfish dispatch가 agent limit에 걸렸다. 완료 에이전트를 닫고 새 문맥 두 명을 실제로 dispatch해 반환 결과를 받았다. 실패한 dispatch를 검토 완료로 계산하지 않았다.

이 작업은 기존 구현의 역명세이며 새 설계의 다섯 단계 승인 흐름은 실행하지 않았다. 테스트 실행 결과는 별도의 실행 에이전트가 제공했고 부모가 소스·작업 트리·프로세스 종료를 확인했다. 문서만으로 완전히 동일한 구현을 만들 수 있다는 인증, 형식적 합의 증명, Kubernetes·객체 저장소·Chaos qualification은 하지 않았다.

## 4. 수정 및 재점검 상태

### 수정 반영

문서 편집자는 본문·세 근거 부록을 수정하고 `quepaxa-go-formats-2026-10-30.md`를 추가했다. 12개 지적 항목(직렬화 관련 중복 포함)에 대해 두 원 검토자가 자신의 범위만 재점검하여 모두 해소되었다고 반환했다. 이는 전체 명세의 무결점 판정이 아니다. 구현 파일과 의존성을 변경하지 않아 패키지 테스트를 반복 실행하지 않았다.

### Comprehension Goldfish 재점검 원문

All six original comprehension findings are **resolved** within this targeted rereview.

| # | Finding | Result |
|---|---|---|
| 1 | Checkpoint preparation prerequisites | **Resolved.** [Main line 151](quepaxa-go-reverse-spec-2026-10-30.md?plain=1#L151) connects validator registration, preparation, recorder acceptance, seal learning, and compaction. Missing-validator and prepared-identity retry behavior match the source. |
| 2 | Serialization contracts | **Resolved.** The [formats appendix](quepaxa-go-formats-2026-10-30.md?plain=1#L5) supplies exact declarations, JSON encodings, omission rules, byte prefixes, decoder distinctions, and checkpoint identities. Relevant declarations and codecs match the source. |
| 3 | Paging return values and recovery floor | **Resolved.** [Protocol lines 227–245](quepaxa-go-protocol-evidence-2026-10-30.md?plain=1#L227) specify signatures, tip versus cursor, normalization, empty results, `ErrCompacted`, and error returns consistently with `core.go:2992–3051`. |
| 4 | Learner persistent identity | **Resolved.** [Main line 24](quepaxa-go-reverse-spec-2026-10-30.md?plain=1#L24) and membership line 11 explain generation/binding, reuse, errors, enrollment connections, and voter reopen requirements, matching `config.go:117–147`. |
| 5 | Leader scheduling after activation | **Resolved.** [Membership line 9](quepaxa-go-membership-evidence-2026-10-30.md?plain=1#L9) defines generation-relative epochs, rotation, control slots, and prior-order lookup, matching the scheduling and activation source. |
| 6 | Catch-up subtraction for early slots | **Resolved.** [Main line 174](quepaxa-go-reverse-spec-2026-10-30.md?plain=1#L174) and membership line 48 now specify every branch, including zero for ordinary slots 1–16, matching `reconfiguration.go:440–458`. |

No residual issue remains among these six findings. Relevant revised sections, the formats appendix, and corresponding source were reread at HEAD `8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd`. No writes or tests were performed.

This closes these six findings only; it does not certify full reimplementation equivalence or remove the earlier broader coverage limits.

### Codebase Details Goldfish 재점검 원문

Targeted rereview completed against HEAD `8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd`. **C1–C6 are individually resolved; no material discrepancy remains within these six findings.**

| Finding | Result |
|---|---|
| C1 — Value-size enforcement | **Resolved.** Limits are scoped to proposal/staging; receiving-path differences are explicit. |
| C2 — Compaction-floor behavior | **Resolved.** The API matrix documents covered success, rejection, retained conflict checks, and pagination cursor semantics. |
| C3 — Partial batch failures | **Resolved.** Sequential mutation, prechecks, sorting, durability promotion, and absence of rollback are documented. |
| C4 — Production ACK contract | **Resolved.** Readiness, policy checks, apply-before-ACK, failure after installation, and differing hint-selection checks are explicit. |
| C5 — Fenced recovery authority | **Resolved.** External verification responsibility, certificate/validator absence, freshness conditions, retries, and the anchored-base exception are documented. |
| C6 — Serialization contracts | **Resolved.** The [formats appendix](quepaxa-go-formats-2026-10-30.md?plain=1#L1) supplies schemas, prefixes, encoding rules, and decoder differences. |

I reread the corresponding corrections in the main document and three evidence appendices, the new formats text, and the relevant source sections for these findings.

The earlier incomplete-wide-coverage limitation remains: this closes C1–C6 only, not the whole specification. No tests, builds, or file writes were performed.
