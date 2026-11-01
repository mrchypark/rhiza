# Operator Go EGM 보정 기록

- 기준: `HEAD 008f8e576ad76425182e10a69b987f10cfb48b2a`, 2026-10-30. 이는 현재 Go 소스에서 도출한 제한된 계약이다.
- 상태: **보정·독립 재검토 완료(발견 지적 해소; 검토 범위 한정)**. 원본 지적 집합에 남은 항목은 없다. 전체 EGM 5단계 승인이나 재구현 동등성 인증은 아니다.

## 원본 발견 목록과 증거

코드 대조 검토자 `01a18be8-9217-70a1-bc39-8fad3c505002`, 이해도 검토자 `01a18be8-9396-7ef2-a1ee-657575269cc2`, 모두 `fork_context:false`였다. [원문](remaining-areas-egm-review-evidence-2026-10-30.md)을 보존 근거로 삼는다.

1. Blocker: stage machine에는 literal `Reserved`가 있어야 한다. 정확한 순서는 `"" → Reserved → Sealed → Stopping → Starting → Restarted → Complete`다. 단계별 부수 효과도 구분한다. `Sealed`에서 target evidence를 보존한 뒤 `Stopping`에 진입하고, `Stopping`에서 source 종료를 확인한 뒤 `Starting`에 진입하며, `Starting`에서 successor를 활성화한 뒤 `Restarted`에 진입한다. [reconcile](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/controller.go#L240), [advance](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/controller.go#L257).
2. Blocker: status-recorded failure는 항상 reconciliation loop 오류를 반환하지 않는다. `record`/`autoStatus` 성공은 `Blocked` 상태를 쓰고 nil을 반환할 수 있으므로 status-only rejection과 dependency/CAS 오류를 구분해야 한다. [record](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/controller.go#L907), [autoStatus](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/automatic.go#L272).
3. Blocker: manual recovery fence gate은 전체 identity set 또는 암호학적 proof를 검증하지 않는다. confirmation, nonblank evidence, operation/cluster/StatefulSet UID만 검사하며 automatic child의 persisted proof 검증과 분리된다. [validateFence](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/controller.go#L911).
4. Major: `recovery-owner` annotation claim, conflict rejection, UID+operation release 조건 및 membership/generation completion predicate와 legacy identity-deficit branch가 누락되었다. [claim](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/controller.go#L775), [release](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/controller.go#L566), [completion](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/automatic_completion.go#L25).
5. Blocker: “No branch deploys a workload”는 activation과 모순된다. activation은 cluster identity, durability, credentials, replicas를 patch하고 StatefulSet을 PUT해 successor를 시작한다. [activation](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/controller.go#L478).
6. Major: operator-owned control/intent/completion 및 independent-attestation nested wire objects의 tag, type, omission, timestamp 표현이 완전하지 않아 재개·상호운용 구현이 추측에 의존한다. [automatic types](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/automatic.go#L57), [fencing types](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/fencing.go#L108).

## 읽기 범위와 제한

코드 대조 검토자는 6개 대상 문서와 reservation, activation, intent resume, child authorization, claim/release, completion 경로를 읽었다. attestation/backend 구현과 모든 인용 참조는 완전 검증하지 않았다. 이해도 검토는 10개 대상 문서를 모두 읽고 위 발견을 지지하는 좁은 소스만 확인했다. 테스트·빌드·쓰기·기억 조회·위임은 없었다.

## 원본 항목 처리 체크리스트

정본의 최종 독립 재검토 결과를 기록한다. B는 코드 대조 검토자, C는 이해도 검토자이며 위 ID와 대응한다. 이 문서 작성자는 재검토를 실행하지 않았다.

- [x] B3: `Reserved`를 포함한 단계와 부수 효과 순서가 보정되었다. 검토자는 reservation → seal → target evidence 보존 → source termination → successor activation → completion 대응을 소스와 대조해 해소를 확인했다. 위 원본 목록은 보정 전 지적을 보존한다.
- [x] B4: status-only rejection과 실제 반환 오류 구분이 해소되었다.
- [x] B5: manual fence assertion과 automatic proof 검증 구분이 해소되었다.
- [x] B6: claim/release 및 completion·legacy identity 조건이 해소되었다.
- [x] B7: activation의 workload 변경 설명이 해소되었다.
- [x] C-Operator: persisted/external proof의 nested schema 누락이 해소되었다.

최종 판정의 근거와 범위는 아래 최종 재검토 기록을 따른다.

## 최종 독립 재검토

[정본의 최종 재검토 원문](remaining-areas-egm-review-evidence-2026-10-30.md?plain=1#L7)을 확인했다. A(`01a18be7-f2f5-7943-b468-dc9cb01be18d`)는 12/12, B(`01a18be8-9217-70a1-bc39-8fad3c505002`)는 9/9, 이해도 C(`01a18be8-9396-7ef2-a1ee-657575269cc2`)는 7/7 지적의 해소를 확정했다. 세 검토자는 모두 `fork_context:false`로 시작했다. 이 집계에는 중복이 있으므로 고유 결함 28건으로 합산하지 않는다. 위 체크리스트가 이 영역에 해당하는 원본 지적을 구분한다.

첫 재검토에서는 Materializer의 `apply-populated` 잔존 문구, Node hash의 도메인 바이트·JSON 규칙, Operator 단계별 부수 효과 순서가 남았다. 이는 보정 과정의 이력이다. 추가 보정 후 세 검토자가 원본 지적 집합에 남은 항목이 없다고 확정했다. 최종 재검토는 해당 지적과 대응 소스의 범위에 한정되며, 앞서 기록한 미검토 범위를 확대하지 않는다. 검토자는 추가 테스트·수정·전체 감사를 수행하지 않았다.
