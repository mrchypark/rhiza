# Peer transport Go EGM 보정 기록

- 기준: `HEAD 008f8e576ad76425182e10a69b987f10cfb48b2a`, 2026-10-30. 이는 현재 Go 소스에서 도출한 제한된 계약이다.
- 상태: **보정·독립 재검토 완료(발견 지적 해소; 검토 범위 한정)**. 원본 지적 집합에 남은 항목은 없다. 전체 EGM 5단계 승인이나 재구현 동등성 인증은 아니다.

## 원본 발견 목록과 증거

코드 대조 검토자 `01a18be8-9217-70a1-bc39-8fad3c505002`, 이해도 검토자 `01a18be8-9396-7ef2-a1ee-657575269cc2`, 모두 `fork_context:false`였다. [원문](remaining-areas-egm-review-evidence-2026-10-30.md)을 보존 근거로 삼는다.

1. Blocker: normal RPC 전체가 5초 deadline을 받는 것은 아니다. `call`은 5초, `callQuorum`은 caller deadline, checkpoint call은 5분이며 connection invalidation은 실패 RPC를 replay하지 않는다. `SendRecord`, `ReadTip`, Learned fan-out은 `callQuorum` 경로다. [구현](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/transport.go#L317).
2. Blocker: `VerifyPrefix`는 certified data를 learner로 durable catch-up할 수 있다. proof parameter를 전달하지만 membership은 직접 변경하지 않는다. [handler](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/peer_server.go#L540).
3. Major: TLS negotiation identifier의 실제 wire 값이 누락되었다. `sqlpolicy.PeerALPN`은 `"rhiza-peer-v7"`다. [정의](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/sqlpolicy/policy.go#L16).

## 읽기 범위와 제한

코드 대조 검토자는 6개 대상 문서와 peer TLS, configuration routing, authorization, framing, 두 materialization budget, connection handling, ACK rule을 확인했다. learner retirement/escalation과 모든 인용 참조는 완전 검증하지 않았다. 이해도 검토는 10개 대상 문서를 모두 읽고 위 발견의 좁은 소스만 확인했다. 테스트·빌드·쓰기·기억 조회·위임은 없었다.

## 원본 항목 처리 체크리스트

정본의 최종 독립 재검토 결과를 기록한다. B는 코드 대조 검토자, C는 이해도 검토자이며 위 ID와 대응한다. 이 문서 작성자는 재검토를 실행하지 않았다.

- [x] B8: RPC별 deadline과 실패 RPC의 비재실행 계약이 해소되었다.
- [x] B9: `VerifyPrefix`의 certified catch-up 부작용이 해소되었다.
- [x] C-Peer: ALPN `rhiza-peer-v7` 누락이 해소되었다.

최종 판정의 근거와 범위는 아래 최종 재검토 기록을 따른다.

## 최종 독립 재검토

[정본의 최종 재검토 원문](remaining-areas-egm-review-evidence-2026-10-30.md?plain=1#L7)을 확인했다. A(`01a18be7-f2f5-7943-b468-dc9cb01be18d`)는 12/12, B(`01a18be8-9217-70a1-bc39-8fad3c505002`)는 9/9, 이해도 C(`01a18be8-9396-7ef2-a1ee-657575269cc2`)는 7/7 지적의 해소를 확정했다. 세 검토자는 모두 `fork_context:false`로 시작했다. 이 집계에는 중복이 있으므로 고유 결함 28건으로 합산하지 않는다. 위 체크리스트가 이 영역에 해당하는 원본 지적을 구분한다.

첫 재검토에서는 Materializer의 `apply-populated` 잔존 문구, Node hash의 도메인 바이트·JSON 규칙, Operator 단계별 부수 효과 순서가 남았다. 이는 보정 과정의 이력이다. 추가 보정 후 세 검토자가 원본 지적 집합에 남은 항목이 없다고 확정했다. 최종 재검토는 해당 지적과 대응 소스의 범위에 한정되며, 앞서 기록한 미검토 범위를 확대하지 않는다. 검토자는 추가 테스트·수정·전체 감사를 수행하지 않았다.
