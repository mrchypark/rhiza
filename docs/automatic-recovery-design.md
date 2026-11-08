# 자동 세대 DR: 미구현 상태와 역사적 설계

현재 자동 세대 DR는 구현되거나 qualification을 통과한 기능이 아니다.
`RhizaRecovery`의 queue/status schema가 존재한다는 사실은 fencing, restore,
새 quorum 활성화를 실행하는 controller가 완성됐다는 뜻이 아니다.

alpha1·StatefulSet·legacy fencer의 실행 예제와 당시 CI 기록은
[v0.23.1 원본](https://github.com/mrchypark/rhiza/blob/2262531f266f2638eb21baea6f65de45465ffe90/docs/automatic-recovery-design.md)에
고정된 역사적 자료로 보존한다. 원본의 YAML, 환경 변수, migration 제안과 자동
복구 성공 기록을 현재 alpha2 설치 절차나 지원 보장으로 사용하지 않는다.
이 문서는 새로운 설계를 채택하거나 구현 완료를 선언하지 않는다.

## 현재 정상 HA와의 구분

genesis operator, healthy-quorum Host repair, certified engine membership의
역할은 [현재 operator 문서](../deploy/operator/README.md)를 따른다.
정상 HA에는 별도 상시 fencer나 외부 lease가 필수적이지 않다. quorum을 잃으면
쓰기와 linearizable read는 실패하며, 준비된 노드의 local read는 별개다.
원래 quorum 복구와 whole-generation DR를 같은 절차로 취급하지 않는다.

## 승인된 정책과 아직 없는 실행 기능

[승인된 automatic-attempt 정책](../deploy/operator/README.md#approved-automatic-attempt-policy-not-implemented-or-qualified)은
기본 OFF다. 명시적 정책이 있을 때 지속적인 실제 ReadIndex/quorum 실패를
준비 시도의 조건으로 삼을 수 있지만, 현재 runtime이 이 정책을 실행하거나
검증했다는 뜻은 아니다. Pod readiness나 시간 초과만으로 전환하지 않는다.

활성화에는 다음 증거가 모두 필요하다.

1. 모든 이전 voter·learner·sidecar·임베딩 프로세스·publisher 등 writer의
   정확한 identity와 전체 실행 범위를 확인한다.
2. 독립적인 실제 종료와 재등장 차단을 확인한다. Pod 삭제, API 응답이나
   네트워크 단절을 물리적 종료의 증거로 대체하지 않는다.
3. 이미 제출된 저장소 작업까지 완료되거나 안전하게 격리됐음을 확인한다.
   오래된 삭제 작업이 복구 자료를 손상시킬 가능성도 포함한다.
4. 이 자동 시도 설계의 BeforeAck 경로에서는 성공한 ACK를 모두 포함하는
   source와 checkpoint 및 연속 certified suffix를 검증한다. 별도로 승인된
   [managed Async 제안](managed-async-recovery.md)은 검증된 게시 cut 이후의
   미게시 tail 손실만 허용한다. 이 제안도 활성화 기능을 구현한 것은 아니다.
   archive 지연이나 알 수 없는 손실량을 0으로 기록하지 않고, 부족한 source를
   임의로 낮춘 tip, 빈 초기화 또는 새 bootstrap으로 대체하지 않는다.
5. 정확한 source·storage·binding·generation·operation에 대한 live authority와
   Transition을 검증하고, 새 ready quorum을 확인한 뒤 서비스를 재개한다.

증거가 불명확하거나 partition이면 Blocked 상태에서 수동 판단을 기다린다.
진행 중 애플리케이션 요청과 UNKNOWN 원본은 보존한다. 기존 receipt만으로
활성화하거나 미래 쓰기를 승인하지 않는다. 이 조건은 새 lease/epoch나
상시 권한 서비스를 추가하자는 설계가 아니다.

독립 복구의 [fencing 경계](kubernetes-fencing.md)와
[anchor 프로토콜의 한계](recovery-anchor.md)를 함께 확인한다.
