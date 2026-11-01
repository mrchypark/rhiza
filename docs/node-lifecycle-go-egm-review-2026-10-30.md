# Node lifecycle Go EGM 보정 기록

- 기준: `HEAD 008f8e576ad76425182e10a69b987f10cfb48b2a`, 2026-10-30. 현재 소스에서 도출한 제한된 계약이다.
- 상태: **보정·독립 재검토 완료(발견 지적 해소; 검토 범위 한정)**. 원본 지적 집합에 남은 항목은 없다. 전체 EGM 5단계 승인이나 재구현 동등성 인증은 아니다.

## 원본 발견 목록과 증거

코드 대조 검토자 `01a18be8-9217-70a1-bc39-8fad3c505002`, 이해도 검토자 `01a18be8-9396-7ef2-a1ee-657575269cc2`, 모두 `fork_context:false`였다. [원문](remaining-areas-egm-review-evidence-2026-10-30.md)을 기준으로 한다.

1. Blocker: prepared-checkpoint resume은 reclaim의 cancellation wrapping 및 `FailLocalLifecycle` 동작을 공유하지 않는다. resume은 `Quiesce` 오류를 직접 반환하고 resume만 defer한다. [resume](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L1306), [Quiesce](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L809).
2. Major: `Start`의 HTTP shutdown ownership이 불완전하다. caller context 취소는 별도 10초 background deadline으로 HTTP shutdown을 시도하고 오류를 버리며, `ErrServerClosed`가 아닌 Serve 오류는 engine shutdown 없이 직접 반환한다. [Start](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L1658), [Node shutdown](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L2036).
3. Major: `Start`는 `Serve`에서 block하며 성공 serving 경로는 `Node.Shutdown`을 호출하지 않는다. caller가 cancellation·serving 오류 뒤 cleanup을 준비해야 하는지 계약에 명시해야 한다. [Start](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L1658).
4. Major: checkpoint hash 재현에 필요한 byte construction이 문서에 없다. root/state hash를 비운 descriptor marshal, role/name 순서, NUL, 8-byte big-endian length, textual file digest를 명시해야 한다. [구현](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_checkpoint.go#L495).

## 읽기 범위와 제한

코드 대조 검토자는 대상 6개 문서를 전부 읽고 Open/readiness/shutdown 및 fencing 관련 경로를 확인했다. local checkpoint internals, learner retirement/escalation, 모든 인용 참조는 완전 검증하지 않았다. 이해도 검토는 10개 대상 문서를 전부 읽되 위 발견의 좁은 소스만 확인했다. 테스트·빌드·쓰기·기억 조회·위임은 없었다.

## 원본 항목 처리 체크리스트

정본의 최종 독립 재검토 결과를 기록한다. B는 코드 대조 검토자, C는 이해도 검토자이며 위 ID와 대응한다. 이 문서 작성자는 재검토를 실행하지 않았다.

- [x] B1: prepared resume과 reclaim의 오류·취소 구분이 해소되었다.
- [x] B2 / C-Node-1: `Start`의 blocking 및 HTTP/node cleanup 소유권이 해소되었다.
- [x] C-Node-2: 정확한 checkpoint hash 계약이 보정되었고 이해도 검토자가 소스와 일치함을 확인했다. NUL로 끝나는 literal domain, struct 순서·타입·tag, Go `json.Marshal` 바이트, 빈 문자열로 유지하는 두 hash field, 순서가 있는 state component·big-endian 길이·textual digest를 포함한다.

최종 판정의 근거와 범위는 아래 최종 재검토 기록을 따른다.

## 최종 독립 재검토

[정본의 최종 재검토 원문](remaining-areas-egm-review-evidence-2026-10-30.md?plain=1#L7)을 확인했다. A(`01a18be7-f2f5-7943-b468-dc9cb01be18d`)는 12/12, B(`01a18be8-9217-70a1-bc39-8fad3c505002`)는 9/9, 이해도 C(`01a18be8-9396-7ef2-a1ee-657575269cc2`)는 7/7 지적의 해소를 확정했다. 세 검토자는 모두 `fork_context:false`로 시작했다. 이 집계에는 중복이 있으므로 고유 결함 28건으로 합산하지 않는다. 위 체크리스트가 이 영역에 해당하는 원본 지적을 구분한다.

첫 재검토에서는 Materializer의 `apply-populated` 잔존 문구, Node hash의 도메인 바이트·JSON 규칙, Operator 단계별 부수 효과 순서가 남았다. 이는 보정 과정의 이력이다. 추가 보정 후 세 검토자가 원본 지적 집합에 남은 항목이 없다고 확정했다. 최종 재검토는 해당 지적과 대응 소스의 범위에 한정되며, 앞서 기록한 미검토 범위를 확대하지 않는다. 검토자는 추가 테스트·수정·전체 감사를 수행하지 않았다.
