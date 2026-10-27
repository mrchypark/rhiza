# Deployment 기반 NoPVC HA 복구 설계 검토

- 검토일: 2026-10-27 (Asia/Seoul)
- 마지막 범위 갱신: 2026-10-28 (Asia/Seoul)
- 대상 초안: `docs/deployment-ha-recovery-design.md`
- 원본: task input의 동일 경로 `docs/deployment-ha-recovery-design.md` (복사본은 기준 문서와 byte-for-byte 일치)
- 원본 기준 HEAD: `a078cca81bd8c5a68d139eb2c12e04794cb44893`
- 독립 대조 기준: `main`의 `ffdc12c73e598b9f1694913310c4ec1e0d68f35b`
- 범위: Rhiza 설계 검토와 coherent Node/network recovery observation 및 해당 Operator 소비 경로. Deployment·initContainer 지원은 아직 구현되지 않았다.
- 복사본 SHA-256: `663a782d0b11aa8bf02e6f8c92c7c34a67fe7cd42cdb77f078db0bb51d720582` (원본과 `cmp` 일치)

## 요약

초안의 핵심 안전 원칙은 Rhiza의 현재 계약과 맞는다. 특히 빈 WAL에 기존 voter ID를 재사용하지 않고, quorum 상실을 Pod 상태나 타임아웃만으로 확정하지 않으며, 이전 publisher와 GC writer의 차단 증거가 없으면 복구를 막아야 한다.

다만 초안은 기존 구현과 새 제안을 섞어 읽기 쉽다. 현재 코드에는 이미 `RhizaRecovery`, 선택형 `RhizaCluster` 자동 복구, 외부 fencing, archive seal/fork, target quorum 확인, authenticated learner 및 멤버십 변경이 있다. 첫 구현에서 새 CRD·복구 엔진을 다시 만들거나 기존 자동 경로를 먼저 제거하지 말아야 한다. 현재 구현은 StatefulSet + emptyDir 중심이지만, 이는 최종 범위를 StatefulSet으로 제한한다는 뜻이 아니다. 목표는 Deployment와 initContainer까지 지원하는 것이며, 대체 경로가 검증될 때까지 StatefulSet 안전 경로를 유지해야 한다.

## 반드시 범위를 고정할 사항

1. **기존 StatefulSet 안전 경로를 유지하면서 Deployment/initContainer 목표로 진행한다.** 현재 controller는 StatefulSet API·UID·ordinal을 전제로 하고 `noPVC`는 해당 템플릿의 `emptyDir` 데이터 볼륨을 검사한다. 이를 Deployment 지원으로 오인하거나 기존 경로를 먼저 제거하면 안 된다. Deployment workload identity, rollout/recreation fencing, init과 본 프로세스 간 DataDir 소유권을 별도로 구현·검증하되 최종 목표에서 제외하지 않는다. 근거: [`pkg/operator/controller.go`](../pkg/operator/controller.go), [`pkg/operator/config.go`](../pkg/operator/config.go#L144-L185), [`deploy/operator/cluster-crd.yaml`](../deploy/operator/cluster-crd.yaml).

2. **일반 교체와 전체 세대 DR을 별도 단계로 둔다.** 기존 quorum이 있으면 새 identity의 learner를 붙이고, 따라잡은 뒤 기존 멤버를 제거하는 경로가 가능하다. 그러나 `pkg/node/membership.go`는 개별 add/remove 요청을 처리할 뿐, Pod 장애 판정·fencer 호출·교체 순서를 소유하는 내부 controller는 아니다. `MembershipFence.Confirmed`와 증거 문자열은 admin 요청의 입력이다. 해당 코드만으로 독립적인 종료·재등장 방지 증명을 만들어 내지 않는다. 따라서 신뢰 가능한 fence 공급자가 없는 자동 교체는 설계상 차단해야 하며, 현재 Operator 경로를 대체 경로 검증 전에 삭제하면 안 된다. 근거: [`pkg/node/membership.go`](../pkg/node/membership.go#L22-L130), [`pkg/node/membership.go`](../pkg/node/membership.go#L196-L210), [`pkg/operator/automatic_execution.go`](../pkg/operator/automatic_execution.go#L18-L48).

3. **`OpenLearner`와 멤버십 learner를 혼동하지 않는다.** 공개 `OpenLearner`는 비투표 read-replica이며 cluster membership이 아니다. 멤버가 될 learner는 `Node.Open`의 별도 구성으로 fresh identity/WAL을 열고, 기존 voter가 durable registration과 exact-prefix admission을 확인해야 한다. 복구를 위해 임의로 voter ID를 재사용하거나 bootstrap member list를 바꾸면 안 된다. 근거: [`replica.go`](../replica.go#L198-L218), [`pkg/node/node.go`](../pkg/node/node.go#L530-L565), [`pkg/node/membership.go`](../pkg/node/membership.go#L67-L108), [`pkg/node/voter_identity.go`](../pkg/node/voter_identity.go#L90-L155).

4. **initContainer API는 아직 계약되지 않았다.** 공개 `rhiza.Open`/`Node.Open`은 단발성 archive 복원 함수가 아니라 WAL·identity 검증, archive/checkpoint 복구, materializer 설치와 프로세스 시작을 포함하는 runtime open 경로다. `materializer.PrepareLocalRestore`는 Local exact-base 복원을 위한 하위 API이며 HA trust 또는 멤버십 권한을 주지 않는다. 따라서 초안의 “공통 Go 복구 API”는 아직 구현된 호출 계약으로 적지 말고, 입력 identity, source binding, 검증된 tip/base, 출력 증거, 재시작/idempotency, 앱 시작 전/후의 소유자를 먼저 정한다. 일반 learner의 peer catch-up으로 대체할 수 있는지 먼저 검증하고, 새 initContainer 복원 엔진은 필요한 경우에만 추가한다. 근거: [`rhiza.go`](../rhiza.go#L77-L100), [`pkg/node/node.go`](../pkg/node/node.go#L184-L220), [`pkg/node/node.go`](../pkg/node/node.go#L430-L480), [`pkg/node/node.go`](../pkg/node/node.go#L572-L645), [`pkg/materializer/local_restore.go`](../pkg/materializer/local_restore.go#L77-L100).

5. **외부 recovery anchor와 object-store generation anchor는 다른 권위다.** object-store `recovery/anchor.json`은 fork된 target의 불변 lineage/checkpoint 증거다. `recoveryanchor.Coordinator`의 외부 Record는 binding·generation·pending write·transition·receipt를 CAS로 관리한다. 두 기록을 하나의 원자적 저장으로 가정하지 말고, 어느 한쪽이 누락·불일치·미확정이면 activation을 막는다. 일반 `Verify`는 저장된 요청·receipt와 live binding/generation을 확인하지만 archive 복원 자체를 수행하지 않는다. 근거: [`docs/recovery-anchor.md`](recovery-anchor.md), [`pkg/recovery/anchor.go`](../pkg/recovery/anchor.go#L20-L44), [`pkg/recoveryanchor/coordinator.go`](../pkg/recoveryanchor/coordinator.go#L95-L221), [`pkg/recoveryanchor/coordinator.go`](../pkg/recoveryanchor/coordinator.go#L260-L331).

## 가장 작은 안전한 구현 순서

1. **첫 구현 slice는 coherent authenticated recovery observation이다.** 기존 `/recovery/probe`의 challenge/HMAC 응답에 Ready(local catch-up), current membership/config/WAL identity, pending/abort 상태, 그리고 그 snapshot에 묶인 fresh normal 또는 excluded ReadIndex 결과를 함께 담는다. 새 caller는 `version=1`을 요청하고 signed version, 모든 필수 typed field, challenge/scope, quorum state tuple, MAC을 strict decode한다. 버전이 없는 legacy response는 schema error로 분류하여 outage나 no-quorum으로 해석하지 않는다. 이전 caller는 version 없이 legacy JSON/MAC을 계속 받는다. 안정된 snapshot 아래 ReadIndex가 `ErrQuorumUnavailable`을 반환하면 `quorum_unavailable=true`로 관측하되, 이는 해당 시도의 quorum 확보 실패일 뿐 fencing 권한이나 process 종료 증거가 아니다. Timeout/cancellation, pending, snapshot 변경 또는 typed quorum-unavailable이 아닌 오류는 unknown이다. Operator의 timer는 정책상 안정된 unavailable/no-positive 상태를 관측할 때 진행할 수 있지만, independent fence 없이 recovery를 승인하지 않는다.

2. **현재 기능 경계를 문서화하고 유지한다.** 3개 이상 voter, StatefulSet, emptyDir, 공유 object store라는 현재 지원 범위를 유지하면서 Deployment/initContainer까지의 경로를 추가한다. 새 `RhizaCluster`/`RhizaRecovery` 스키마를 만들기보다 기존 API와 상태 전이를 기준으로 한다. `async`는 마지막으로 확인된 게시 prefix 뒤의 acknowledged write를 잃을 수 있고, 게시 주기를 최대 손실 시간으로 해석할 수 없다. `before-ack`는 성공 응답의 보장 범위이며 quorum 또는 외부 fencing을 대체하지 않는다.

3. **surviving quorum 아래 단일 교체만 먼저 닫는다.** 기존 Node membership API와 durable operation record를 사용해 새 learner identity 등록 → learner catch-up/admission → quorum에서 add commit → fenced old identity remove commit 순서를 한 번에 한 교체로 구현한다. old identity의 fence proof는 신뢰된 외부 공급자가 발급해야 한다. 장애 감지, 임의의 Pod 삭제, `NotReady`, timeout은 proof가 아니다. 재시도는 동일 operation/config/abort slot에 결합하고, 경쟁 controller·중간 프로세스 종료 뒤에도 다른 교체로 바뀌지 않아야 한다. 이 slice가 검증되기 전에는 현재 Operator 자동 경로를 제거하지 않는다.

4. **상태 판정과 교체 조율을 그 다음에 추가한다.** `Ready()`는 local startup/catch-up 완료를 나타내며 현재 quorum 증명이 아니다. 장애 상태가 `unknown`이면 기존 membership과 데이터를 보존하고 멈춘다. 단일 peer의 local read를 quorum으로 격상하지 않는다.

5. **initContainer는 Deployment 목표의 일부로 구현한다.** 검증된 learner catch-up/archive restore 경로를 재사용하되, init과 본 프로세스가 같은 DataDir을 동시에 열지 않도록 명시적 소유권과 산출물/재시작 계약을 둔다. restore 성공만으로 voter 자격을 부여하지 않는다.

6. **전체 세대 복구는 기존 외부 fencing + seal/fork 경로를 재사용한다.** `RhizaRecovery`의 durable reservation, 전체 source identity 범위 fence, `Seal`, 검증된 `Fork`, target membership/lineage 기록, 필요 시 external anchor 활성화, source workload 정지 후 target 시작/과반 검증의 순서를 바꾸지 않는다. fence proof는 모든 구세대 consensus writer, archive publisher와 GC writer의 범위·재등장 방지·진행 중 저장 작업의 안전성을 다뤄야 한다. 증거가 불완전하거나 provider가 필요한 보장을 못 하면 `Blocked`가 정답이다. 근거: [`pkg/operator/controller.go`](../pkg/operator/controller.go#L257-L295), [`pkg/operator/controller.go`](../pkg/operator/controller.go#L300-L340), [`pkg/operator/controller.go`](../pkg/operator/controller.go#L360-L520), [`pkg/recovery/seal.go`](../pkg/recovery/seal.go), [`pkg/recovery/fork.go`](../pkg/recovery/fork.go#L45-L145).

## 인수 테스트 기준

- 동일 Pod의 앱 재시작은 원래 WAL/identity를 유지하고, fresh emptyDir에 기존 voter ID만 넣으면 `ErrVoterStateLost`로 시작하지 않는다.
- learner는 새로운 identity와 durable registration을 사용하며, exact certified prefix를 따라잡기 전에는 membership commit이 되지 않는다.
- add 후 remove의 각 경계에서 controller/runtime 프로세스를 종료·재시작해도 같은 operation이 재개되고 중복 투표권이나 quorum 축소가 발생하지 않는다.
- peer partition, 일시적인 `ReadIndex` 실패, stale Pod 목록, timeout만으로 fence·remove·새 세대 활성화가 일어나지 않는다.
- 한 surviving quorum은 보존되고, 단일 peer read를 확정 데이터나 quorum으로 간주하지 않는다.
- 전체 DR은 fencer가 source cluster/workload UID/Pod UID/WAL identity 및 publisher/GC 범위에 결합된 완료 증거를 주지 않으면 멈춘다. 오래된 process 재개, 이미 전송된 upload/delete, operator 재시작을 포함한다.
- target은 source와 분리된 generation identity를 쓰며, target anchor/lineage/membership/archive/checkpoint 검증과 필요한 external receipt 확인 전에는 사용자 쓰기를 받지 않는다.
- `async`와 `before-ack`의 데이터 손실/ACK oracle을 각각 검증한다. 모호한 복구점은 손실량을 추정하지 않고 `unknown`으로 보고한다.
- object-store 어댑터 변경은 VersityGW가 제공하는 S3 로컬 개발 경로와 Floci S3, GCS, Azure 실제 SDK/adapter contract CI를 구분한다. MinIO 또는 S3 단독 통과를 GCS/Azure 보장으로 확장하지 않는다. 실제 Kubernetes 장애 주입과 Chaos Mesh 자격은 별도의 release 증거다.

## 구현 상태와 관찰 계약

현재 작업 트리에는 초안의 첫 단계인 coherent recovery observation과 Operator 소비 경로가 반영되어 있다. Node는 ReadIndex 전후의 config, voter/WAL identity, pending/abort 상태가 동일한지 확인하고, 정상 또는 제외-voter ReadIndex 결과를 같은 snapshot에 묶는다. Authenticated `/recovery/probe?version=1` 응답은 전체 tuple, nonce, exclusion scope를 HMAC으로 보호하며 strict decoder는 필수 필드와 상태 조합을 검증한다. 구버전 비인증 status 응답은 기존의 제한된 필드만 유지한다. 근거: [`pkg/node/node.go`](../pkg/node/node.go), [`pkg/network/recovery_probe.go`](../pkg/network/recovery_probe.go), [`pkg/network/recovery_status.go`](../pkg/network/recovery_status.go).

Operator는 `QuorumKnown`인 일치된 성공만 positive quorum으로 취급한다. 인증된 no-positive 결과(안정된 typed `ErrQuorumUnavailable`, 유효한 unknown/pending 관측 또는 응답 없음)는 정책상 grace timer에 기여할 수 있다. 그러나 outer cancellation, 미실행 요청, 인증/schema/identity/scope 오류는 timer 근거가 아니며 차단 또는 대기한다. grace 후에도 positive quorum이 없으면 Operator는 독립 fencer에 generation-scope fence request를 예약할 수 있지만, 관측 자체로 fence 완료나 target activation을 승인하지 않는다. Activation에는 별도의 유효한 fencer 증명과 기존 복구 검증이 계속 필요하다. 근거: [`pkg/operator/automatic_probe.go`](../pkg/operator/automatic_probe.go), [`pkg/operator/automatic_escalation.go`](../pkg/operator/automatic_escalation.go), [`pkg/operator/automatic_execution.go`](../pkg/operator/automatic_execution.go).

이 구현은 설계의 관찰/정책 입력 경계만 진전시킨다. StatefulSet 중심 경로는 유지되어야 하며, Deployment workload identity/fencing, initContainer/DataDir 소유권 계약 및 Deployment end-to-end 검증은 아직 구현되지 않았다. 최종 CI와 provider 계약 검증은 대기 중이며, 이 문서는 통과를 주장하지 않는다. Kubernetes, GCS, Chaos Mesh 또는 실제 배포 복구 자격도 검증되지 않았다. 기존 `automatic-operator-chaos` 작업은 직접 Pod 삭제를 사용하며 Chaos Mesh 실험이 아니다. 원본 설계 초안은 변경하지 않았다. Pro 검토의 출처는 2026-10-27에 제공된 독립 관찰 검토이며, 그 provenance는 [Pro 원본 검토 기록](#pro-원본-검토-기록)에 남긴다.

### Pro 원본 검토 기록

2026-10-27 task input으로 전달된 Pro 독립 검토를 바탕으로 작성했다. 검토 원문은 task evidence로 보존되며, private task-state 경로는 repository 문서에 노출하지 않는다. 기준 문서는 위에 기록한 초안 복사본이며 원본 바이트를 수정하지 않았다.

## WIP 중복 확인 및 검증 한계

읽기 전용으로 확인한 별도 `ha-independent-evidence-v2` WIP는 HEAD `63f31a85d7225635a7f7a01fcd9e359c2f122f45`에서 dirty 상태였다. 변경은 fencing source mutation transport, automatic logical replacement assignment, mutation execution/compaction과 경쟁·publication 테스트에 집중되어 있었다. 해당 WIP의 `docs/independent-fencing-v2-plan.md`는 trusted provider, source routing, 실제 CRI/mutation provider가 아직 연결되지 않았고 live qualification도 미완료라고 적는다. 따라서 이 WIP를 현 HA 설계의 작동하는 fencer나 승인된 fence proof 공급자로 취급할 수 없다. HA 변경이 `pkg/operator/automatic.go` 또는 fencing 경로와 겹치기 전에는 WIP 소유자와 조정해야 한다. 해당 checkout은 수정하지 않았다.

이 slice의 검증 범위는 focused Node/network/Operator unit tests다. 최종 CI와 Floci provider contract 결과는 아직 대기 중이다. Kubernetes/Chaos Mesh, 실제 GCS 또는 운영 환경 복구는 이 slice에서 실행·승인하지 않는다.
