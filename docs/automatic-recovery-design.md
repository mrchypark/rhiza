# 완전 자동 복구 설계 초안

상태: 논의용 제안, 미구현. 기준 코드: main `854ed1c`.
현재 구현된 `RhizaRecovery`와 구분한다. 아래 CRD/필드/API는 아직 적용할 수 없다.

## 목표와 한계

3-voter no-PVC StatefulSet의 장애 감지부터 구세대 차단, 데이터 복구, 새 3-voter
세대의 활성화까지 사고마다 사람의 승인을 요구하지 않는다. 운영자는 배포 시
자동 전환·async 유실·계획 중단·fencing 범위를 사전 승인한다.

완전 자동은 모든 외부 장애 중에도 무조건 복구한다는 뜻이 아니다. Kubernetes,
공유 저장소, fencing authority, 새 Pod 실행 용량이 사용 가능하고 증거가 유효할
때 무인 진행한다. 그 전제가 깨지면 안전하게 대기하고 전제가 돌아오면 재개한다.
현재 archive 없는 빈 클러스터, archive 손상/롤백, 알 수 없는 소스 모드, 임의로
복제된 VM/WAL, Kubernetes와 원격 ledger가 함께 유실되는 사고를 자동 초기화로
우회하지 않는다.

사용자 결정: async는 평상시 성능 우선이며 일부 쓰기 유실을 허용한다.
before-ack도 같은 Operator에서 지원한다. 따라서 최초 구현은 평상시 모든
쓰기에 추가 원격 lease/CAS를 요구하지 않는 인프라 fencing 방식을 우선한다.
배포 provider는 아직 미확정이며, 실제 provider를 정하기 전까지 자동 fencing의
구체적인 안전성·권한·복구 시간은 확정할 수 없다.

## 구성

- `RhizaCluster` (신규): 기존 StatefulSet을 채택하는 논리 클러스터와 장기 정책.
  최초 DB/StatefulSet 생성까지 책임을 넓히지 않는다.
- `RhizaRecovery` (기존 확장): controller가 생성하는 사고별 실행 저널.
  기존 수동 실행 경로도 유지한다.
- Fencing adapter: 실제 인프라의 종료/격리와 재등장 방지 완료를 검증한다.
  첫 구현은 검증 가능한 provider 하나만 지원한다.
- 기존 archive seal/Fork/credential 생성/StatefulSet 전환: 그대로 재사용한다.

Kubernetes Lease는 controller 중복 실행을 줄일 수 있지만 안전성의 근거는 아니다.
source successor의 원격 CAS와 세대/operation 식별자 검사로 중복·늦은 실행을 막는다.

## 정책 CRD 예시

아래는 설계용 YAML이다. 시간은 예시이며 부하·재시작·네트워크 특성 측정 후 정한다.

```yaml
apiVersion: rhiza.mrchypark.dev/v1alpha1
kind: RhizaCluster
metadata:
  name: myapp
spec:
  logicalID: myapp-production           # 불변, generation과 별개
  workloadRef:
    statefulSet: myapp
    container: application
  adoptClusterID: initial-generation    # 최초 채택에만 사용
  recovery:
    mode: Automatic                    # Manual도 유지
    noReachableQuorumFor: 120s
    asyncDataLoss: AllowArchivedPrefix  # before-ack 소스에는 적용하지 않음
    degradedRepair:
      strategy: RotateGeneration       # Wait도 지원; 온라인 교체가 아님
      after: 30m                       # 전체 중단을 허용하는 사전 정책
    fencing:
      providerRef: production-fencer
    cooldown: 30m
    maxAutomaticRecoveriesPerDay: 3
```

`RhizaCluster`에는 평상시 실행 설정 전체를 복제하지 않는다. 실제 소스 mode는
불변 원격 membership에서 확인한다. 자동 복구의 target mode는 기본적으로
source를 유지한다. 모드 변경은 별도의 명시적 계획 작업으로 처리한다.
`adoptClusterID`는 최초 source 확인값이며 사고 때마다 사람이 수정하지 않는다.

`logicalID`, 최초 binding, workload/container 변경은 일반 patch로 허용하지 않는다.
StatefulSet UID 변경도 같은 이름이라는 이유만으로 채택하지 않는다. 정책 변경은
다음 사고에 적용하며 진행 중 operation은 시작할 때의 정책 snapshot/hash에 결합한다.
`mode: Manual` 또는 suspend는 새 사고의 시작을 막는다. 이미 봉인한 세대를 되살리는
취소로 해석하지 않는다.

status는 엄격한 schema와 Kubernetes Conditions를 둔다. 최소 항목:
`observedGeneration`, `activeEpoch`, `activeClusterID`, `workloadUID`,
`activeRecoveryRef`, `lastHealthyTime`, `suspectedSince`, `lastCompletedRecovery`,
`conditions`(Healthy, Degraded, FencingReady, ArchiveReady, RecoveryInProgress, Blocked).

## 세대와 실행 저널의 권위

논리 클러스터 ID는 고정이고 매 복구마다 epoch/실제 ClusterID가 증가·변경된다.
원격 저장소의 고정 키 `clusters/<logicalID>/control.json`을 CAS로 관리한다.

```text
logicalID, bindingUID, activeEpoch, activeClusterID,
state: Active | Recovering,
operation: {id, sourceEpoch, targetEpoch, recoveryRecord, policyHash}
```

`Active(e)` → `Recovering(e, operation)` 예약은 첫 fencing 같은 불가역 작업보다
먼저 수행한다. 기존 source `recovery/successor.json`도 같은 operation에 결합한다.
두 키 사이 원자성을 가정하지 않는다. 하나라도 실패/불명확하면 동일 intent를
읽어 조정하고, 필요한 예약이 모두 확인될 때까지 fencing하지 않는다.

새 세대 설치 후 `Active(e+1)` 승인은 같은 control 키의 CAS로 수행한다.
Kubernetes status는 관찰/표시용이며 재시작 시 원격 저널과 대조한다.
operation의 권위를 CR UID에만 두지 않는다. 현재 `CR UID:recoveryID` 결합은
논리 root/epoch에 결합된 durable operation ID로 확장해야 CR 재생성도 처리할 수 있다.
원격 operation 기록에 복구점·새 membership·Secret 참조·fence 증거를 남기고,
자격 증명 원문은 로그/status에 노출하지 않는다. Secret까지 유실되면 동일 토큰을
임의로 새로 만들어 재개하지 않고 차단한다.

진행 중 CR에는 finalizer를 두어 일반 삭제로 저널이 사라지지 않게 한다.
최종 안전성은 finalizer가 아니라 원격 기록과 세대 검증에 의존한다.
늦게 깨어난 controller는 새 Pod/VM을 옛 작업으로 죽이지 못하도록 모든 외부
작업을 source epoch·Pod UID·provider의 불변 instance ID에 결합한다.

## 장애 판정

HTTP timeout은 영구 디스크 유실이나 전역 quorum 소멸의 증거가 아니다.
관찰 상태를 `Healthy`, `Degraded`, `NoReachableQuorum`, `Unknown`으로 분리한다.
신뢰되는 같은 세대 voter에서 성공한 quorum probe가 있으면 실제 quorum을
관측한 것으로 취급한다. Pod Ready 개수로 이를 대체하지 않는다.

관찰 endpoint는 자동화 전 인증/identity 검증을 보강한다. peer의 nonce challenge,
cluster/membership/version 식별과 운영 API 인증을 사용하여 stale/잘못된 Pod의
응답을 현재 세대의 증거로 채택하지 않는다. 시작 실패 전에도 진단 가능한 경로를
추가해 `WALStateLost`, `ArchiveUnavailable`, `ConfigMismatch` 등을 구분한다.
시작 오류 로그 문자열을 파싱해 영구 유실을 판정하지 않는다.

| 상황 | 자동 정책 |
| --- | --- |
| 컨테이너 재시작, 동일 emptyDir/WAL 유지 | 원래 세대로 재시도. Pod 삭제를 유도하지 않음 |
| 1 voter 일시 불가용, quorum 정상 | Degraded 관찰, 정상 quorum 유지 |
| 1 voter WAL 유실, 나머지 2개 정상 | 사전 RotateGeneration 정책이면 유예 후 계획된 전체 세대 전환 |
| 2개 불가용, 1개 응답하지만 quorum 없음 | 유예 동안 원본 복귀 우선. 이후 fencing 가능한 경우 전체 DR |
| 3개 모두 불가용 | 같은 DR. 살아 있는 voter의 추가 자료를 수집할 수 없다는 차이 |
| Operator에만 안 보이는 기존 majority | 죽었다고 가정하지 않음. 자동 takeover 정책에 따라 그 majority까지 fencing해야 전환 가능 |
| quorum 정상, before-ack 저장소만 불가용 | 저장소 복원을 기다림. 세대 교체로 해결하려 하지 않음 |
| 제어면/저장소/fencer 불가용 또는 증거 충돌 | Blocked/대기, 임의 강등·초기화 없음 |

Automatic takeover는 감시 경로만 끊어진 정상 majority도 의도적으로 중단시킬 수
있다는 운영 선택이다. grace는 오탐을 줄일 뿐 안전성 증명이 아니다. fencing의
첫 불가역 동작 전 정상 quorum이 다시 보이면 취소할 수 있다. fencing을 시작한
뒤에는 원본 세대 자동 복귀 대신 같은 전환을 끝낸다.

## fencing의 실제 계약

첫 구현은 cloud instance 종료/전원 차단, 또는 검증 가능한 bare-metal 전원 제어를
사용한다. 구체적 provider가 다음을 보장해야 한다.

1. 모든 이전 실행 identity를 포괄한다. voter 3개뿐 아니라 이전 읽기 replica,
   archive publisher/GC와 관리되는 임베딩 프로세스가 범위에 들어간다.
2. 옛 workload의 재스케줄·컨테이너 재시작·VM 복원으로 새 구세대 프로세스가
   생기는 경합을 차단한다. 한번 조회한 Pod UID 목록만 종료하는 것으로 끝내지 않는다.
3. 완료 확인은 exact instance/generation에 결합되며 재실행해도 같은 결과를 낸다.
   API가 종료 요청을 받았다는 응답은 실제 종료/재등장 방지 완료가 아니다.
4. 차단은 새 세대 활성화 뒤에도 유지한다. 원래 노드를 재사용하려면 구 workload와
   자격을 제거·검증한 별도 절차가 필요하다.
5. 기존 전송 중 저장소 요청은 남을 수 있으므로 archive HEAD seal과 증거 pin/
   불변 보존을 함께 사용한다. 오래된 GC가 이미 전송한 삭제까지 안전하게 끝나거나
   versioned/보존된 복구 자료를 훼손할 수 없다는 저장소 계약이 필요하다.

fencer 결과에는 logicalID/sourceEpoch/operationID, workload UID, 대상 instance 목록,
차단 범위, restart-prevention 정보, provider task ID, 검증 결과와 proof digest를
기록한다. 일반 앱 계정은 proof를 쓰지 못한다. 단순 `confirmed: true`를 controller
자신이 채워 넣는 것은 자동 fencing 구현이 아니다.

공유 노드를 끄면 다른 앱도 중단된다. 최초 지원 환경은 전용 노드 풀 또는 동등한
격리 범위가 적절하다. Operator·fencing authority·오브젝트 저장소와 새 Pod용 용량이
문제의 3개 노드와 함께 사라지지 않도록 분리한다.

인프라에 독립적인 data-plane fencing은 별도 후속 설계다. 주기적 epoch 조회만으로는
조회 직후 process pause/in-flight 작업을 막지 못한다. 지원하려면 모든 읽기·쓰기·
peer 응답·storage/GC·임베딩 경로가 유효 권한을 강제해야 한다. lease의 시계/중단
가정과 만료 후 늦은 효과를 검증해야 하며, 제어 저장소 장애 중 async 쓰기의
가용성에도 영향을 줄 수 있다. 이를 첫 구현의 간단한 대체물로 넣지 않는다.

## 복구 실행 순서

```text
Observe → Suspect → Preflight → Reserve
  → [계획 전환이면 Quiesce + Drain + Certified Flush]
  → Fence → Seal → Verify/Fork → Replace
  → VerifyQuorum → Activate → Stabilize
```

Preflight는 source identity/mode, 자동 정책, fencer 권한/상태, 저장소 CAS·증거
접근성, target 용량을 확인한다. 최종 archive 검증은 seal 뒤 다시 수행한다.
사전 점검이 성공했다고 fencing 후 장애가 없다고 보장하지는 않는다.

계획 전환은 모든 애플리케이션 쓰기 admission을 실제로 닫고, 진행 중 요청을
quorum으로 정리한 뒤 확정 prefix를 원격 저장한다. Service 트래픽만 끊는 것으로는
직접 임베딩 API 호출을 막지 못한다. HTTP와 Go/Rust 인프로세스 호출 모두에
동일한 gate를 걸고, background proposal도 포함한다. 확인할 수 없는 노드가 있으면
ACK 무손실 계획 전환이라고 주장하지 않고 DR의 소스 mode 정책을 적용하거나 대기한다.

2~3 voter DR에서는 존재하지 않는 원래 quorum의 drain을 기다리지 않는다.
접근 가능한 인증 suffix를 best-effort 보존하고, 강제 fencing 뒤 봉인된 archive를
기준으로 복구한다. before-ack는 저장소가 보존한 성공 ACK prefix를 복원한다.
async는 마지막 검증 가능 prefix 이후 성공 쓰기가 유실될 수 있다. 낮은 tip을
선택하는 대신 인증된 checkpoint+연속 suffix를 검증한다. 손실량을 모르면 unknown을
기록하며 0이나 게시 주기로 추정하지 않는다.

Fork는 기존 구현처럼 소스 Pod의 emptyDir를 없애기 전에 완료·검증한다.
전체 세대 전환에는 같은 이름/DNS와 새 ClusterID/namespace/peer/admin 토큰을 쓴다.
새 voter 둘의 matching quorum과 recovered tip 적용은 최소 검증이고, 여기에
활성화 전 쓰기 admission gate가 필요하다. 새 세대가 remote Active CAS 이전에
사용자 쓰기를 받지 않도록 관리 대상 엔진이 Prepared/Active 상태를 이해해야 한다.
기존 ReadIndex/합의 초기화용 peer traffic과 사용자 쓰기 준비 상태를 혼동하지 않는다.

완료 후에도 이전 세대의 direct connection·읽기 replica가 현재 서비스로 응답하면
안 된다. 임베딩 호스트/replica의 범위와 라우팅을 검증하고, 외부 업무 부작용은
애플리케이션의 종료·재개 계약에 따라 처리한다. DB 복구가 외부 이메일/결제/작업
실행의 exactly-once를 보장하지는 않는다.

## 무한 복구 루프와 보안

cooldown과 단위시간 최대 자동 전환 횟수를 원격 저널에서 계산해 controller 재시작으로
리셋되지 않게 한다. 횟수 초과는 새로운 전환만 막고 진행 중 복구의 재개를 막지 않는다.
오래된 controller의 fence 명령은 epoch/instance가 다르면 거부한다.

대상 2개 quorum을 얻은 뒤 3개 전체 복원/안정화도 관찰한다. 새 세대 Pod가 다시 WAL을
잃었다면 같은 bootstrap을 반복해 투표 상태를 초기화하지 않는다. 새 사고로 취급하되
루프 제한과 Degraded 정책을 적용한다.

기존 namespaced RBAC는 유지/최소 확장하며, 노드/클라우드 전원 권한은 별도 adapter
서비스 계정에 한정한다. 정상 애플리케이션은 정책/operation/proof를 수정할 수 없다.
복구를 완료하기 위해 임의 cluster-admin 권한을 요구하지 않는다.

## 구현 순서와 통과 기준

1. concrete fencer 한 개를 선택하고 partition/restart/늦은 명령 반례부터 검증.
2. 논리 root 원격 저널 + RhizaCluster 정책 + 기존 RhizaRecovery idempotent 재사용.
3. 인증된 진단, 자동 사고 판정, 재시작해도 유지되는 grace/cooldown.
4. 관리 대상 엔진의 quiesce/활성화 gate와 자동 DR 연동. Go/Rust 임베딩 경로 포함.
5. 정상 quorum이 있는 단일 voter 유실의 계획 rotation 및 2~3 voter 유실 검증.

필수 테스트:
- 구 B/C는 서로·클라이언트와 통신하지만 Operator에서는 안 보임: 확인된 fence 전
  새 세대 활성화 금지. fence 불가 시 영구 대기해도 안전성을 유지.
- 모든 Pod 유실 + before-ack: 각 성공 ACK 쓰기 보존, 새 quorum의 새 쓰기 확인.
- 같은 async 사고: 승인 정책 없이 자동 전환 금지; 허용 시 실제 복구 prefix 기록.
- live majority + 한 Pod emptyDir 유실: 기존 ID 빈 WAL 재가입 금지, 계획 rotation.
- 인프라 fence 직후 옛 노드 자동 재기동/새 구세대 Pod 생성 시도 차단.
- archive publish/GC와 fence/seal 경합, 늦게 도착한 ACK와 이미 보낸 저장소 작업.
- 각 단계의 controller 종료, 두 controller 경쟁, CR 삭제 시도, 오래된 작업 재개.
- target 복원 후 Activate CAS 전 직접 Go/Rust 쓰기 거절, Activate 이후 성공.
- source/target UID 교체, 저장소 장애·오염, fencer 권한 상실, 새 노드 용량 부족.
- generation/cooldown이 controller·CR status 재생성으로 초기화되지 않음.

## 근거와 미확정 사항

현재 소스: `pkg/operator/controller.go`, `pkg/operator/types.go`,
`pkg/recovery/seal.go`, `pkg/recovery/fork.go`, `pkg/node/node.go`.

Kubernetes 공식 근거:
- https://kubernetes.io/docs/tasks/run-application/force-delete-stateful-set-pod/
  강제 삭제만으로 실제 이전 프로세스 종료를 보장할 수 없다.
- https://kubernetes.io/docs/concepts/cluster-administration/node-shutdown/
  out-of-service taint는 전원 차단이 확인된 뒤 적용한다.
- https://pkg.go.dev/k8s.io/client-go/tools/leaderelection
  leader election 자체는 fencing을 보장하지 않는다.
- https://kubernetes.io/docs/concepts/services-networking/network-policies/
  정책 변경의 기존 연결 차단은 구현에 따라 다르다.

미확정: 실제 배포 환경/fencing provider, 전용 노드 범위, 자동 takeover 유예,
단일 voter 유실 때 계획 중단 허용 시간. 앞의 정책 값은 제안이며 합의된 기본값이 아니다.
Pro 신규 상담은 지정 프로젝트 페이지 로딩 실패로 수행하지 못했다.

## Fencing 소유권과 backend 경계 — 2026-09-11 결정

Operator가 장애 판정, quorum 확인, fencing 범위 선정, immutable operation ID와
identity 저널, 재시도 및 복구 상태 전환을 소유한다. `Fencer`는 그 요청을 실행하는
경계이며 HTTP 서비스가 복구 정책을 결정하지 않는다. 구현체는 HTTPS
`FencingClient`와 Kubernetes `RhizaFence` backend이다. 독립 서비스 배포는 인터페이스의 필수 조건이 아니며, 동일한
증거 계약을 만족하는 backend를 Operator 내부에 연결할 수 있다.

모든 backend의 결과는 Operator에서 다시 검증한다. operation ID, source generation,
binding UID 및 전체 요청 해시가 일치하고, 프로세스 종료·재생성 차단·저장소 작업
정지가 모두 확인되어야 복구를 진행한다. 성공 응답이나 proof ID만으로 충분하지 않다.
이 검증은 증거의 연결 관계를 확인하는 것이며, backend가 실제 격리를 수행했다는
신뢰를 대신하지 않는다. backend별 실제 장애 주입 검증이 별도로 필요하다.

현재 대상은 공유 Kubernetes 노드의 StatefulSet + emptyDir이다. Pod DELETE와
admission 차단만으로 단절된 노드의 기존 프로세스 또는 이미 전송된 저장소 쓰기의
종료를 증명할 수 없다. 관찰 가능한 Pod만 사라진 상태에서도 보이지 않는 프로세스가
남을 수 있으므로 이를 완료 증거로 낮추지 않는다. Kubernetes backend의 실행기는
실제 런타임 종료와 재실행 차단, 저장소 quiescence를 확인할 수 있는 수단이 필요하다.
확인할 수 없으면 pending/blocked로 남긴다. 공유 노드 전체의 전원 차단은 다른
workload에 영향을 주므로 일반 Pod 복구의 기본 동작으로 삼지 않는다.

Kubernetes backend와 CI 전용 실행기의 구현 및 무인 검증 범위는 아래에 기록한다.
운영 환경의 실행기 연동과 물리 장애 검증은 별도 자격 조건이다.

## Kubernetes backend 실행 계약

`RHIZA_AUTOMATIC_RECOVERY=true`, `RHIZA_FENCER_BACKEND=kubernetes`를 설정하고
`deploy/operator/cluster-crd.yaml`, `deploy/operator/fence-crd.yaml`, RBAC를 적용한다.
HTTP backend는 `RHIZA_FENCER_BACKEND=http` 및 기존 URL/토큰/CA 설정으로 선택한다.

Kubernetes backend는 `RhizaFence`에 고정된 요청을 생성한다. 요청 이름은 binding UID와
operation ID로 결정되어 대상이 바뀐 재시도를 별도 작업으로 숨길 수 없다. CRD가 spec
변경을 금지하며 backend도 전체 요청 해시를 비교한다. Operator의 권한은 요청 get/create
뿐이다. 별도 실행 권한을 가진 실행기가 `/status`에 완료 증거를 기록해야 한다.
이 요청 전달만으로 프로세스를 종료하지는 않는다. 실행기가 없거나 증거가 불완전하면
자동 복구는 계속 대기한다. 배포 환경별 실행기는 동일 프로세스에 구현할 수도 있으나
권한과 책임은 이 계약을 유지해야 한다.

실행기는 요청을 영속화한 뒤 재생성 차단을 먼저 설정하고, 해당 voter/generation의
실제 실행 중인 모든 incarnation을 조사·종료해야 한다. 관찰 목록인 targets만 믿고
다른 incarnation을 누락해서는 안 된다. Pod 이름을 다시 해석한 늦은 종료 명령이
새 generation을 중단하지 않도록 UID/컨테이너 ID를 고정한다. 저장소에 이미 전송된
작업까지 정지한 뒤에만 완료 증거를 기록한다. 완료 이후에도 이전 identity의 재생성
차단은 유지되어야 하므로 완료 CR 또는 barrier를 자동으로 삭제하지 않는다.

CI의 `kind_fencer.py`는 이 계약을 실제 kind 런타임과 admission webhook으로 실행하는
테스트 전용 구현이다. 단일 폐기형 노드와 독점 MinIO를 사용하며 공유 운영 클러스터에
배포하는 node agent가 아니다. 운영 환경의 종료/저장소 격리 수단은 별도 연동이 필요하다.

### 확인된 무인 CI 근거

2026-09-11, candidate `ef65375`, CI run `34554367214`, job `103123753569`에서
기본 무인 카오스 6개 시나리오가 최종 `PASS`를 기록했다.
https://github.com/mrchypark/rhiza/actions/runs/34554367214/job/103123753569

- 동일 Pod/WAL의 SIGKILL 재시작: 불필요한 fencing 없이 복귀.
- emptyDir 유실: Operator가 voter fencing 요청과 learner 교체를 생성·완료.
- 실행기 중단 중 복구 대기 및 Operator 재시작 후 동일 operation 재사용.
- 실행기 재시작 후 완료된 구 voter identity의 admission 차단 유지.
- 살아 있는 구 프로세스의 QUIC 단절: proof 전 새 세대 복구 금지.
- generation fencing 이후 새 세대로 자동 복구: ACK 349행 전부 보존,
  중복 요청 처리 유지 및 새 쓰기 성공.

Operator가 fencing status를 갱신할 권한이 없음과 요청 spec의 API 불변성도 확인했다.
이 결과는 단일 폐기형 kind 노드와 독점 MinIO에서의 실제 실행 근거다. 물리 호스트
단절, 공유 운영 저장소의 credential fencing, 운영 node agent를 검증한 결과는 아니다.

### Learner 유실을 포함한 7개 시나리오

Candidate `12fe7f27b7461a2f9e78a4eaad316226a3ebf541`의 CI run
`34556748732`, job `103130932550`에서 무인 7개 시나리오가 `PASS`했다.
https://github.com/mrchypark/rhiza/actions/runs/34556748732/job/103130932550

추가 도중 learner Pod를 실제 삭제한 뒤 Operator가 failed incarnation fencing,
abort revision **384**, 새 learner `-learner-1` 승격을 자동 수행했다.
이후 live-process QUIC 단절과 generation fencing을 거쳐 **ACK 430행 / 복구 430행**,
중복 요청 처리 보존, 새 세대 쓰기를 확인했다. 같은 후보의 Go/race, Rust,
컨테이너, 멤버십 E2E와 수동 카오스도 통과했다. Artifact는
`automatic-operator-chaos-34556748732`이다.

테스트는 learner 시작 전 QUIC 장애를 설치하며, Operator의 자동 재시도로 생성된
새 Pod도 추적해야 한다. 테스트의 시작 대기 장치가 재시도 Pod를 막던 문제를 수정했다.
승격 이후 live abort marker는 초기화되므로 성공 판정은 새 addition의 영속
`expectedAbortSlot`을 사용한다.

앞선 candidate `2a1a0b8`은 제거 단계의 HTTP 500으로 제한 시간 내 완료되지 않았다.
당시 응답 본문이 보존되지 않아 정확한 원인은 미확정이다. 이후 bounded/redacted
오류 본문과 멤버십 상태 수집을 추가했다. 위 성공 실행은 이 간헐적 실패의 원인이
해결됐다는 증거가 아니며, 운영 자격 검증에서는 재현 여부와 원인을 계속 확인해야 한다.
