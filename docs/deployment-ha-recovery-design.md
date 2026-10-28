# NoPVC HA 복구 설계 초안

- 작성일: 2026-10-27 (Asia/Seoul)
- 출처: 사용자와의 사이드 대화에서 논의한 요구와 설계
- 상태: 설계 초안과 현재 검증 경계. 준비 API/CLI 및 cooperative-fencing evidence는 전체 HA 복구 완료·안전성 검증·Pro 승인 기록이 아니다. 원래 목표는 Deployment+NoPVC이며, 현재 StatefulSet 결합 구현은 제한 사항이지 목표 변경이 아니다.
- 최초 설계 기준선: `a078cca81bd8c5a68d139eb2c12e04794cb44893`. HA 계약/상태 검토 기준선: `31e58ce` 및 `ccdafbe`.
- 당시 문서 작성 범위: 해당 설계 초안 작성 외에 코드, 실행 중인 작업, 다른 브랜치와 워커는 변경하지 않았다. 이는 현재 체크아웃의 변경 상태를 나타내지 않는다.

## 현재 구현 상태 — 2026-10-28

아래는 공개 foundation과 미출시 후보를 구분한 상태 요약이다. 목표 계약을 구현 사실로 읽지 않는다.

- Go API는 `PrepareRestore(ctx, Config)`이며, 실행 파일은 `rhiza --prepare-restore`로 이를 호출하고 일반 서버 시작 전에 종료한다.
- `Config.PreparedPodUID`는 `RHIZA_PREPARED_POD_UID`에서 읽는다. 이는 initContainer가 Downward API의 현재 Pod UID로 설정해야 하는 incarnation binding이다.
- HA membership learner의 새 멤버 정보는 `RHIZA_LEARNER` JSON에서 `Config.Learner`로 파싱한다. 이는 앱 역할을 고르는 `RHIZA_ROLE`과 다른 설정이다. `RHIZA_ROLE=learner`는 read-replica 실행 경로이며 consensus membership learner나 voter 교체 역할이 아니다.
- 준비 단계는 shared object store의 인증된 checkpoint와 연속 suffix를 DataDir에 오프라인 materialize하고, source/Pod-bound marker를 쓴다. 이는 재구성 가능한 HA learner 전용 경로이며 `Local` 모드가 아니다. Local archive restore와 HA certified archive view는 동등하거나 상호 교환 가능한 형식으로 보장되지 않으며, Local archive로 기존 voter identity/WAL을 되살리는 경로도 아니다. 이후 정상 Node open은 설정된 Pod UID와 일치하는 marker를 요구한다. 준비 API 자체는 Node를 열거나 listener를 띄우거나 투표권·멤버십을 부여하지 않는다.
- marker intent, observer 생성, pending marker 기록 단계의 다섯 SIGKILL 경계에서 프로세스 재시작 테스트가 추가되었다. 종료된 자식 프로세스를 회수한 뒤 동일 Pod 준비 재개와 다른 Pod UID 거부를 검사한다. 실제 rename 이후의 별도 crash point나 전원 손실, Kubernetes initContainer, Chaos Mesh 검증을 뜻하지 않는다.
- 현재 ordinary 흐름은 StatefulSet ordinal/authority에 결합되어 있고, manifest/init wiring, 정상 quorum의 learner catch-up 및 Node coordinator, 분산 HA의 old-writer 게시/GC 차단은 아직 완료되지 않았다. Deployment+NoPVC 목표는 구현·검증되지 않았다. 아래 authority 경계를 모의 coordinator로 대체하지 않는다.
- 실제 CI Kind + Chaos Mesh 흐름과 ACK readback end-to-end 결과는 아직 없다. `operator-chaos` 또는 직접 Pod 삭제 테스트를 해당 증거로 간주하지 않는다.
- 조건부 설계 검토 기록과 기존 안전 조건은 이 문서의 원문대로 유지한다. 전체 HA 구현·운영 자격에 대한 최종 Pro 승인은 아직 없다. 이 변경은 미출시 feature branch 작업이며 Rhiza 0.21 release에 포함되지 않았다.

## 1. 목표와 합의한 방향

Rhiza는 오브젝트 스토리지를 복구 데이터의 진실 원천으로 사용하며, HA에서는
QuePaxa 합의를 사용한다. Kubernetes 배포는 NoPVC를 전제로 한다.

원래 ordinary replacement 목표는 NoPVC Deployment다. 건강한 quorum에서의 일반 Pod
교체는 Rhiza 내부에서 처리되어야 하며, 이 acceptance scenario에서는 오퍼레이터와
standalone authority 모두 사용할 수 없어도 완료되어야 한다. 오퍼레이터는 명시적으로
승인된 전체 세대 DR만 담당한다. 이는 현재 구현이 아니라 목표 계약이다. 현재 코드는
StatefulSet/ordinal 및 원격 authority 경로에 결합되어 있어 이 목표를 충족하지 않는다.

Deployment 구성원은 Pod ordinal이나 이름이 아니라 실제 Pod UID 기반 incarnation과
Deployment/ReplicaSet 소유 관계로 판별해야 한다. 각 후보의 owner chain, source configuration,
assignment를 정확히 검증하고 desired voter 수 및 unavailable 범위를 제한해야 한다. rollout 중
여러 ReplicaSet이 동시에 존재할 때의 허용 정책은 미확정이며, 세대 혼합을 일률적으로 금지하는
계약으로 정하지 않는다. 정확한 API/schema 변경은 아직 미확정이므로 이 문서는 새 프로토콜이나
호환성 보장을 정의하지 않는다.

데이터 복원과 투표권 획득은 별개의 절차다. initContainer가 데이터를 복원했다고
기존 voter의 투표권을 즉시 되살려서는 안 된다. 이전 identity의 합의 참여와
정식 저장소 게시를 차단하는 안전 조건은 유지한다.

이 문서는 Local filesystem maintenance 복구와 HA 복구를 구분한다. Local 모드의
OS 파일 잠금만으로 분산 HA의 이전 writer 차단이 해결되는 것은 아니다.

## 2. 책임 분리

| 구성 요소 | 책임 | 맡지 않는 일 |
|---|---|---|
| Kubernetes Deployment + ReplicaSet (목표) | 원하는 Pod 수와 rollout 유지, 새 Pod 인스턴스 생성 | Pod 이름을 identity로 간주, voter 투표권 부여, 합의 상태 복원 판단 |
| mandatory initContainer | trusted assignment를 확인하고 공통 Go `PrepareRestore` 경로로 같은 Pod의 emptyDir DataDir을 준비 | quorum 대기, identity 승인, 멤버십 변경, 자체 fencing 증명 |
| Rhiza Node 런타임 | 새 Pod UID의 정확한 assignment로 시작, learner catch-up, 정상 quorum의 REMOVE/ADD 조율, 쓰기·readiness 판단 | 증거 없는 identity 재사용이나 새 세대 생성 |
| authority / fencing ledger | immutable workload·credential binding, 정확한 incarnation 승인, old identity closure와 proof/status 보존 | 합의 mutation 대행 또는 데이터 복원 |
| 오퍼레이터 (목표) | 명시적으로 승인된 quorum-loss 전체 세대 DR 조율 | ordinary 교체의 필수 의존성 또는 정상 quorum에서의 단일 voter 교체 |

일반 장애와 quorum-loss DR은 서로 다른 실행 경로다. 기존 오퍼레이터의 ordinary
membership mutation은 실제 Node 경로와 회귀를 통합·검증한 뒤에만 제거한다.
이 표와 아래 단계는 목표 책임 계약이며, 현재 구현 상태는 §12에 별도로 표시한다.
ordinary 목표에서는 별도 fencer/authority 서비스가 장애 교체의 선행조건이 아니다. 이 목표는
old-writer 차단이나 미확정 저장소 작업의 안전 조건을 약화하지 않는다.

## 3. identity와 현재 코드의 제약

### Cooperative fencing admission의 현재 호환성 경계

새 supervisor 등록은 기존 fencing ledger의 exact enrollment를 전제로 한다.
ledger가 없거나 해당 StatefulSet slot의 이력이 없으면 registration은 거부되며,
Pod 이름만으로 legacy NodeID를 신규 등록하지 않는다. 빈 ConfigMap을 미리 만들어도
enrollment 증거가 되지 않는다. 같은 ordinal에 새 Pod UID가 오면 기존 terminal
history와 managed root/store의 검증을 통과해야 UID-derived replacement identity를
받는다. legacy NodeID는 기존 exact Pod UID 및 독립 관측된 WAL identity의 managed
binding 아래에서만 유지되며, WAL bind 시 다시 대조한다.

이에 따라 unmanaged 환경의 cold first registration은 현재 지원되지 않는다. 빈
ledger, 다른 slot의 run, `CloseNodes` 기록만으로 admission을 허용하지 않는다.
`Bucket`가 없거나 root/store/evidence를 확인할 수 없는 경우 이를 unmanaged로
분류하지 않고 새 run admission을 거부한다. 이미 등록된 exact run의 stop 조회와
drain/exit receipt, 기존 fencing reconciliation은 계속 진행할 수 있지만, 그 경로로
ledger를 재생성하지 않는다. 최초 unmanaged bootstrap의 명시적 승인 절차는 별도
미완료 작업이며, 이 slice를 전체 HA 완료나 bootstrap 지원으로 해석하면 안 된다.

| 값 | 의미 |
|---|---|
| ClusterID | 클러스터 또는 복구 세대를 구분하는 ID |
| NodeID | 합의 구성원의 논리적 이름 |
| ConfigID | 해당 세대 안의 멤버십 구성 번호 |
| PublicKey | peer 통신 상대가 구성원의 자격을 소유했는지 확인하는 공개키 |
| WALIdentity | WAL에 결합한 고유값. 데이터만 복원한 새 실행과 원래 합의 상태를 구분한다. |
| PodUID | Kubernetes의 실제 Pod 인스턴스 ID |

현재 합의 코어의 NodeID는 문자열이며 StatefulSet ordinal 자체를 요구하지 않는다.
그러나 현재 오퍼레이터는 초기 NodeID와 Pod 이름을 `<StatefulSet>-<ordinal>`로
생성하고, `RHIZA_NODE_ID`가 `metadata.name`을 사용하도록 요구한다. 이는 현재
StatefulSet 구현의 제약이며 Deployment 목표의 identity 규칙이 아니다.

현재 voter 등록은 로컬 `qlog/voter-identity.json`, WAL binding과 원격 등록을
대조한다. 원격 등록은 있지만 원래 WAL 또는 로컬 identity가 없으면 시작을 거부한다.
확정 아카이브가 유실된 voter의 미완료 promise 상태까지 복원하는 것은 아니기 때문이다.

현재 멤버십 변경은 한 번에 한 voter를 추가하거나 제거하며, 남아 있는 구성원의
주소·공개키 등 identity 변경과 retired ID 재사용을 거부한다. learner의 승격도
해당 WALIdentity에 결합된다.

따라서 기존 voter ID를 새 emptyDir에 넣는 것만으로 안전하게 재시작할 수 없다.
이 설계는 새 실행을 새 learner로 편입하는 방향이며, 동일 ID의 무조건적인 인계는
제안하지 않는다. NodeID·주소·Pod 이름의 분리는 구현이 필요한 변경이다.

관련 코드:

- [Member와 NodeID](../pkg/quepaxa/types.go)
- [시작 구성 검증](../pkg/quepaxa/config.go)
- [투표 자격 검사](../pkg/quepaxa/core.go)
- [voter 등록과 WAL 연속성](../pkg/node/voter_identity.go)
- [멤버십 변경 제한](../pkg/quepaxa/reconfiguration.go)
- [peer 인증](../pkg/network/peer_server.go)
- [현재 오퍼레이터의 Pod·ID 결합](../pkg/operator/automatic.go)

## 4. 컨테이너 재시작과 Pod 교체

| 상황 | PodUID | emptyDir | 일반 initContainer | 제안하는 Rhiza 동작 |
|---|---|---|---|---|
| 같은 Pod에서 앱 컨테이너 재시작 | 유지 | 유지 | 완료된 initContainer는 보통 재실행하지 않음 | 원래 WAL·identity 검증 후 기존 구성원으로 재개 |
| Pod 삭제 후 새 Pod 생성 | 변경 | 새로 생성 | 실행 | 확정 데이터 복원 후 새 learner로 시작 |
| 다른 Node에 교체 Pod 생성 | 변경 | 새로 생성 | 실행 | 위와 동일 |

이 표는 Pod의 emptyDir에 DataDir을 두는 구성을 전제로 한다. 동일 Pod라도 WAL이
손상·삭제됐다면 기존 구성원으로 재개할 수 있다고 판단해서는 안 된다.

### 정상 quorum: Node 소유의 Deployment Pod 교체 (목표)

이 목표 경로에서 Pod 이름은 Kubernetes의 조회 키일 뿐 identity가 아니다. 새 Pod는 새 UID와
emptyDir을 가진 별도 incarnation이며, 이전 NodeID/WAL을 재사용하지 않는다. 현재
StatefulSet 중심 구현으로는 이 Deployment 경로를 지원하지 않는다. 기존 assignment가
없거나 불일치·unknown인 경우에는 새로운 승인 신호로 해석하지 않고 차단한다.

아래는 필요한 목표 동작을 설명한다. 현재 후보의 authority assignment 절차가 operator와
standalone authority에 의존한다는 점은 이 목표와의 미해결 차이다. 둘 다 없는 조건에서
사용할 승인·증거 경로는 아직 정의되지 않았으므로, 아래 현재 후보 절차를 해당 경로의
완성된 설계로 간주하지 않는다.

1. Kubernetes가 새 UID의 Pod를 만들고 mandatory init은 앱과 같은 image 및 DataDir에
   `rhiza --prepare-restore`를 실행한다. init은 고정 profile과 downward-API Pod UID를
   사용하고, authority의 projected token/CA를 통해 자기 assignment를 읽기만 한다.
   런타임 admin token, 전체 private peer-token map, store credential은 init에 전달하지 않는다.
2. 현재 후보에서는 authority가 bootstrap 이후의 정확한 ordinal/PodUID, retained assignment, immutable
   generated credential Secret UID, source/workload/profile binding을 검증한다. 실제
   credential handoff는 해당 assignment로 제한하며, Secret 404나 UID 교체는 재발급 허가가
   아니다. init은 shared Go `PrepareRestore`를 사용해 checkpoint와 인증된 suffix를 준비하고
   settlement를 끝낸 뒤 종료한다. init은 voter를 만들거나 멤버십을 바꾸지 않는다.
3. Node는 새 incarnation을 learner로 등록하고 certified prefix를 따라잡는다. 각 surviving
   voter는 같은 immutable repair/phase request를 재개할 수 있지만 각 Node에는 단일 worker만
   둔다. 별도 coordinator lease나 새 서비스는 필요하지 않다.
4. ordinary 목표는 remote authority가 없는 상태에서도 이전 identity가 합의와 canonical
   storage에 영향을 줄 수 없음을 보장한 뒤 교체를 진행하는 것이다. 이를 입증하는 데 물리적
   프로세스 종료나 drain이 항상 필요한 구현 조건이라고 정하지 않는다. 그런 증거는 효과
   배제가 독립적으로 보장되는 경우 사용할 수 있다. 관련 저장소 I/O가 진행 중이거나 결과가
   unknown이면 해결될 때까지 성공 처리나 다음 단계 진행을 보류한다. 현재 후보의 authority 기반
   closure/proof 절차는 목표와 다르며, Node는 기존 합의 프로토콜로 3→2 REMOVE, 이후 2→3 ADD를
   실행한다.
   각 단계의 operation, request hash, abort revision, candidate assignment와 terminal ACK를
   분리해 보존하며 conflicting retry는 거부한다.
5. 실제 voter 구성과 SQL/Graph ACK readback, archive durability가 확인된 뒤 readiness를
   허용한다. Pod 삭제·부재, 이름 재사용, init 성공, tip 증가 어느 하나만으로 old writer가
   종료되었다고 간주하지 않는다.

init 실행은 logical operation/assignment와 별도 per-execution nonce를 가지며, 응답 유실
재시도만 같은 execution을 재사용한다. materializer drain/close, qlog unlock, 무신호 namespace
quiescence, durable outcome, 정확한 init container terminal 관측을 모두 결합한다. 미확정
outcome은 main admission과 fencing proof에서 Pending으로 남는다. 완료 marker no-op는 이번
실행의 원격 작업이 없었다는 뜻이지 과거 실행 정산을 대신하지 않는다.

## 5. initContainer 계약

초기 설계의 “단독 노드 복구 API 재사용”은 API/코드 경로 재사용 목표로 유지하되, 이를
Local archive 형식과 HA certified archive 형식의 호환성으로 해석하지 않는다. 그 재사용
범위와 구현 가능성은 아직 검증 대상이며, 이 문서는 변환 경로나 새 복구 프로토콜을 정의하지 않는다.

HA 전용 Go API `PrepareRestore(ctx, Config)`와 실행 파일의 `--prepare-restore`
early-exit 경로는 unreleased feature candidate에 있다. 이 경로는 `Local` 모드를 거부하고,
shared object store, bootstrap membership, 재구성 설정과 명시적 `ClusterID`, `NodeID`,
`PreparedPodUID`를 사용한다. 이것은 데이터 준비 API이지 identity admission이나 voter 승격
API가 아니다. ordinary replacement 및 fresh bootstrap의 실제 manifest wiring, trusted
assignment/status 연결, generated credential handoff, settlement authority 연계는 아직
통합되지 않았다.

복구 대상의 공개 learner identity는 선택 사항이며, 제공되면 `NodeID`와 일치하고 유효한
공개 키를 포함해야 한다. 새 복원과 중단된 복원 재개는 bootstrap voter ID/키를 불변
`voters/membership.json` 등록과 대조한다. 누락·구버전·불일치 시 복원 데이터를 쓰거나
등록을 고치지 않고 거부하며, 새 DataDir에는 소유권 잠금 항목만 남을 수 있다. 완료 marker가
일치하는 재호출은 원격 저장소에 접근하지 않으며 최신 runtime WAL을 되감지 않는다. 실제
프로덕션 init profile은 일반 init 전체가 아니라 정확한 승인 assignment와 Pod UID에 한해
authority 조회를 허용해야 한다. init 성공만으로 런타임 `Config.Learner`, admin token 또는
peer token을 허가하지 않는다. 단독 노드용 공통 Local 복구 API로 해석해서는 안 된다.

완료 marker는 materializer 동기화, recovery-pin 취소 및 종료, archive 종료, bucket mutation
drain과 종료, 임시 observer WAL 정리가 모두 성공한 뒤 DataDir qlog 잠금을 보유한 상태에서만
기록한다. 이 정리 단계에서 오류나 미확정 mutation이 있으면 완료 marker 기록을 시작하지 않고
`preparing` 상태를 유지하며, 그 상태를 완료로 간주하거나 원격 상태를 강제로 정리하지 않는다.
완료 marker의 rename/fsync 또는 qlog 잠금 해제에서 오류나 결과 미확정이 발생한 경우에는
marker가 존재하더라도 settlement 증거로 단독 간주하지 않는다. 재시도는 기존 marker와 원격
source를 다시 검증해야 하며, 원격 상태를 강제로 정리하지 않는다.

실제 initContainer에서 API를 호출하고 DataDir 및 복구 설정을 앱 컨테이너와 동일하게
공유하는 ordinary/fresh-start manifest wiring은 아직 미통합이다. initContainer는
`RHIZA_PREPARED_POD_UID`에 Downward API의 현재 `metadata.uid`를 전달해야 한다. HA 런타임의
consensus learner identity는
`RHIZA_LEARNER` JSON으로 설정되는 `Config.Learner`이며, `RHIZA_ROLE=learner`는 read-replica
분기다. 이를 멤버십 learner 실행 설정으로 사용하면 안 된다.

준비 단계는 현재 세대의 인증된 복구 지점만 materialize하며 quorum 구성·투표권 부여·멤버십
변경을 하지 않는다. 설정된 `PreparedPodUID`에 대한 marker 검증은 정상 Node open에
연결되어 있지만, 이 연결만으로 learner catch-up, authority 확인 또는 voter 승격이 완료되는
것은 아니다. 전체 quorum을 기다리지 않아 앱 시작과 순환 대기를 만들지 않는 계약은 그대로
유지한다.

데이터가 없거나 복구 근거가 누락된 경우 이를 신규 bootstrap 신호로 해석하지 않는다.
클러스터 최초 생성은 별도의 명시적 controller/authority 승인 절차다. 그 절차는
`bootstrap: true`만으로 실행되지 않는다. 초기 `members`와 private peer-token map이 비어
있고, 기존 root/source/store/profile binding과 generated credential을 확인한 뒤 authority가
ledger UID와 최초 assignment를 pin해야 한다. 순서는 store와 configured authority 준비 →
operator bootstrap intent → controller의 실제 Pod UID/assignment/credential 승인 → mandatory
init → Node의 첫 WAL bind/enrollment 순이어야 한다. 승인된 초기 init만 빈 SQL/Graph DataDir을
materialize하고 genesis provenance를 기록할 수 있다. 실제 Node WAL bind 뒤에는 사용자
SQL 없이 certified barrier를 만들고, durability/archive readback까지 확인해야 최초 bootstrap을
완료로 표시한다. 이 initializer와 Node의 certified-barrier 연결은 아직 미통합이므로 fresh
NoPVC bootstrap은 현재 지원 상태가 아니다.

이 최초 genesis는 ordinary Pod 교체나 DR의 base 검증과 구분한다. 승인된 최초 bootstrap만
빈 application state에서 시작할 수 있다. ordinary replacement는 기존 source의 exact
assignment와 인증된 복구 suffix를 요구한다. quorum-loss DR은 별도 Transition/reservation과
selected target archive view의 anchor/base/barrier 검증을 요구하며, `tip == 0`이나 빈 DataDir을
bootstrap 우회로 사용하지 않는다.

## 6. quorum 상실과 전체 복구 필요성은 다르다

3-voter를 기준으로 다음을 구분한다.

| 상태 | 우선 처리 |
|---|---|
| 2개 이상이 유효한 quorum 연산에 성공 | 일반 교체 경로 유지 |
| 1개 생존, 나머지가 원래 WAL로 재시작 중 | 생존 peer를 보존하고 복귀를 기다림 |
| 1개 생존, 나머지가 새 emptyDir로 교체됨 | learner 생성만으로 quorum이 회복된다고 보지 않음. 원래 합의 상태의 복귀 가능성 확인 |
| 일부 Node가 연결 단절 | 종료나 영구 유실로 단정하지 않음 |
| 모든 peer 응답 불가 | 우선 재시작·복원 가능성 확인. 즉시 새 세대를 만들지 않음 |
| 정상 경로로 quorum 회복 불가 | 이전 세대 차단 후 재해 복구 검토 |

생존 peer의 로컬 read 성공은 최신성 또는 quorum의 증명이 아니다. 하지만 그 peer가
제공하는 확정 로그·체크포인트는 보존하고 복구 지점 선정에 활용해야 한다.
단일 peer에 남은 미확정 값을 임의로 확정하지 않는다.

## 7. 오퍼레이터의 관측 근거

| 근거 | 확인 내용 |
|---|---|
| Pod 상태 | UID, 삭제 시각, 종료 이유, 재시작 횟수 |
| Node 상태 | Ready, 통신 단절, eviction |
| initContainer 상태 | 복원 진행·실패, 검증한 복구 지점 |
| 인증된 Rhiza 상태 API | 세대, ConfigID, NodeID, WALIdentity, voter/learner 자격, 확정·적용 위치 |
| 새로운 challenge 기반 확인 | 동일한 유효 구성에서 실제 quorum 연산을 완료할 수 있는지 |
| 로그와 이벤트 | WAL 유실·저장소 오류·인증 실패의 진단 |

자동 판단은 구조화된 상태와 검증 가능한 근거를 사용한다. 로그 문자열은 진단에
활용하되, 로그 파싱만으로 재해 복구나 구세대 종료를 승인하지 않는다.

Rhiza가 보고할 복구 상태의 의미는 다음처럼 구분한다. 필드명은 미확정이다.

- 원래 WAL·identity로 재개 가능
- 확정 데이터는 복원 가능하지만 새 learner로만 시작 가능
- 복원 실패
- 판단 불가

반복 관측과 유예 시간은 일시적 장애의 오탐을 줄인다. 그러나 타임아웃은 기존 quorum의
사망 증명이 아니다. 마지막 확정 멤버십을 기준으로 관측하고, 장애 중 오퍼레이터가
재시작해도 기록을 보존한다. 유효한 quorum 성공이 확인되면 차단 전 복구 판단을 재평가한다.

## 8. 전체 복구와 이전 writer 차단

정상 quorum이 복귀할 수 없고 명시적 정책/증거가 이를 승인한 경우에만 오퍼레이터가 다음
whole-generation DR 경로를 조율한다. 목표 운영에서 오퍼레이터는 DR 전용이며 ordinary
replacement의 필수 구성 요소가 아니다. 현재 remote-authority 의존 구현은 이 목표와 다르다.
timeout, NotReady, 단일 peer의 성공/실패만으로 DR을
선택하지 않는다. DR init은 ordinary public `PrepareRestore`의 learner/self-member 규칙을
전역 완화하지 않으며, 정확한 Transition/source successor reservation, immutable target
assignment/credential, selected archive view의 generation anchor, certified barrier를 대조한
내부 승인을 필요로 한다. 검증용 observer는 target initial membership에서 별도 synthetic ID로
시작하고, target DataDir·runtime WAL·listener·voter를 만들지 않는다. 이 검증은 첫 bootstrap의
base-zero/genesis 경로가 아니라 authority가 승인한 anchor가 있는 선택 archive view를 대상으로
한다. 실제 Prepare는 pin,
materializer, mutation drain/close, 임시 WAL 및 strict marker의 수명을 계속 소유한다. 검증된
anchor만으로 실제 materialization 또는 활성화를 완료했다고 간주하지 않는다.

전체 복구의 개념적 단계:

1. 정상 quorum 복귀가 불가능하거나 정책상 재해 복구 전환이 필요한 근거를 모은다.
2. 조건부 갱신으로 전체 복구 작업 하나를 예약한다.
3. 이전 세대의 유효한 게시 권한을 차단한다.
4. 진행 중인 저장소 작업과 GC가 안전 조건을 만족하는지 확인한다.
5. 검증된 확정 데이터에서 복구 지점을 선정한다.
6. 새 세대의 복구 지시와 identity·자격 증명을 확정한다.
7. 새 Pod들이 복원하고 새 합의 구성으로 시작한다.
8. 새 quorum, 읽기·쓰기와 이전 세대의 거부를 확인한 뒤 완료한다.

차단 이후는 단순한 관측 철회와 다르다. 오퍼레이터 재시작이나 실패 시에도 저장된
복구 단계에서 안전하게 재개해야 한다.

별도 standalone fencer 프로세스는 ordinary recovery의 목표상 필수 의존성이 아니다.
필수인 것은 이전 세대가 정식
HEAD·체크포인트 등 활성 상태를 게시하거나 새 세대의 데이터를 GC하지 못한다는 보장이다.
게시 전에 권한을 한 번 읽거나 오퍼레이터에게 중단 메시지를 보내는 것만으로는 부족하다.
멈췄다가 재개한 프로세스와 늦게 완료되는 업로드도 처리해야 한다.

저장소 기반 세대별 게시 권한을 우선 검토하되, 정확한 프로토콜은 미확정이다.
만료형 권한도 대안일 뿐 채택하지 않았다. 단순 로컬 TTL을 안전 근거로 삼지 않는다.
제공자 어댑터가 필요한 원자성과 차단 보장을 제공하지 못하면 자동 전체 복구를
지원한다고 주장하지 않는다. 차단 증거가 없으면 fail-closed로 유지한다.

Pod DELETE 성공, Pod 목록에서 사라짐, Node NotReady는 이전 프로세스의 종료나
쓰기 차단을 증명하지 않는다.

## 9. Deployment과 롤링 업데이트: 목표와 현재 제한

목표 workload는 NoPVC Deployment다. consensus identity는 ordinal이 아니라 canonical
NodeID와 실제 PodUID/incarnation에 결합한다. Deployment UID 및 각 후보의 ReplicaSet owner chain을
검증한다. 서로 다른 rollout generation의 후보를 함께 허용할지는 아직 정하지 않았다. 어느
정책에서든 exact source configuration과 candidate authorization을 확인하고 unavailable 범위를
제한해야 한다.
같은 PodName/new UID는 같은 identity가 아니며, 주소 검색 결과만으로 멤버를 인증하지 않는다.
승격된 UID-derived learner도 보존된 assignment와 현재 membership을 통해 다음 survivor
closure에 참여할 수 있어야 한다. 미등록 또는 retired UID를 PodName만으로 inventory에
합치지 않는다.

현재 automatic/bootstrap 경로는 StatefulSet UID, ordinal, PodName을 사용한다. 이 경로는
Deployment owner chain을 검증하지 않으며 Deployment 지원의 증거가 아니다. 이 문서는
StatefulSet을 목표로 재정의하지 않는다.

readiness는 프로세스 실행이 아니라 learner catch-up, 합의된 membership, 쓰기·readback과
복구 settlement를 반영한다. 롤링 업데이트 중 동시 replacement가 여러 개의 voter 손실을
만들지 않도록 동일 source repair를 직렬화하고, 같은 assignment/request만 재시도한다.
구체적인 Deployment rollout strategy와 readiness 수치는 별도 qualification 전까지 확정하지
않는다.

## 10. 기존 API와 durable state 경계

기존 immutable workload binding, auto-control 상태, authority ledger reservation과 Node
membership journal을 우선 재사용하며, 새 CRD나 일반 목적 framework를 전제하지 않는다. 현재
이 상태는 StatefulSet 중심이다. Deployment UID/ReplicaSet generation을 기존 경계에 안전하게
결합할 수 있는지는 검증해야 한다. 구체적인 API/state 확장은 특정 증거 요구로 꼭 필요한 최소
변경임이 확인된 경우에만 고려하며, schema와 호환성 정책은 아직 정해지지 않았다.

## 11. 구현 순서와 합격 기준

1. Bootstrap intent/ledger UID, mandatory init의 noncreating trusted assignment 조회,
   genesis materialization 및 certified barrier를 연결한다.
2. immutable NodeID/PodName/PodUID assignment를 통해 init settlement와 main admission을
   authority proof에 연결한다.
3. 정상 quorum의 Node-owned learner catch-up, old identity closure, 3→2 REMOVE→2→3 ADD,
   exact retry와 ACK/archive readback을 완성한다.
4. Deployment+NoPVC ordinary 경로를 operator와 standalone authority replicas 모두 0인
   조건에서 검증한다. 이 조건은 목표 합격 기준이며 현재 구현은 통과하지 못한다.
5. 별도로 quorum-loss DR의 source closure, selected anchor/base/barrier 검증, 세대별 게시/GC
   차단과 재개 프로토콜을 검증한다.

검증에는 다음을 포함한다.

- Deployment/ReplicaSet owner chain과 실제 Pod UID를 검증하고, 같은 Pod의 컨테이너 재시작과
  새 Pod의 emptyDir 유실을 구분한다. StatefulSet 리소스에 의존하지 않는다.
- healthy quorum의 Pod replacement가 operator와 standalone authority 모두 없는 상태에서
  완료되고, operator는 DR 전용 경로로 남는다.
- 단일 Pod 교체는 오퍼레이터의 멤버 교체 개입 없이 완료된다.
- 동시 교체·롤링 업데이트 중 투표권 중복과 quorum 손실을 방지한다.
- 생존 peer 하나의 데이터를 보존하며, 이를 정상 quorum으로 오인하지 않는다.
- 네트워크 단절만으로 이전 세대가 죽었다고 판정하지 않는다.
- 오래된 프로세스 복귀·늦은 업로드·GC가 새 세대에 영향을 주지 못한다. stale writer 차단,
  미확정/unknown I/O 결과, 저장소 작업 quiescence 및 durable evidence 요구는 어떤 구성에서도
  면제되지 않는다. 증거 부족이나 unknown은 성공으로 간주하지 않고 차단/Pending으로 남긴다.
- 오퍼레이터 재시작·복구 중단 후에도 전체 복구가 중복 실행되지 않는다.
- 실제 pinned Chaos Mesh 검증은 주입된 장애와 복구 양쪽을 확인한다.
- VersityGW fixture는 실제 S3 SDK 경로에 한정한다. S3/GCS/Azure emulator qualification은
  각 pinned Floci adapter와 실제 SDK로 별도 수행하고, 어느 emulator도 production-cloud parity로
  확대하지 않는다.
- NoPVC fresh bootstrap과 같은 PodName/new UID 교체를 실제 pinned Chaos Mesh 실험으로
  검증한다. init 완료, Pod DELETE, `NotReady`, 또는 새 UID의 생성은 old identity의 종료 증거가
  아니다.
- old incarnation의 정상 Stage/읽기 control과 replacement 뒤 실제 QUIC peer 인증 거부를
  구분한다. 정확한 request ID/value가 독립 archive에 없는 것은 그 범위의 기록 부재일 뿐,
  원격 WAL에 아무 효과도 없었다는 증거가 아니다.
- 3→2→3의 Node-driven membership 결과, init의 retained settlement, old identity의 consensus와
  canonical-storage 효과 배제, old credential의 write 거부, ACK된 SQL/Graph의 archive와
  readback을 각각 관측한다. 물리적 프로세스 종료는 독립된 효과 배제가 없는 경우 필요한
  증거로 취급할 수 있지만, 보편적인 구현 계약은 아니다. 관련 I/O 결과가 unknown이면
  qualification 결과는 Pending/Fail이다.

최종 목표는 다음 두 가지다.

1. quorum이 살아 있으면 Kubernetes와 Rhiza 내부에서 일반 복구한다.
2. 정상 quorum 복귀가 불가능하면 이전 세대를 확실히 차단한 경우에만 전체 복구한다.

## 12. 구현 상태와 아직 검증하지 않은 사항

2026-10-28 기준 공개 foundation `9238552`의 12-job CI 결과는 그 체크아웃에서 수행된 기존
cooperative-fencing/복구 검증에 한정한다. 이것은 아래의 unpublished Core/Node 작업, mandatory
init, ordinary coordinator, fresh genesis 또는 전체 DR qualification의 증거가 아니다. 이
checkout의 fencing qualification fixture도 실제 CI/Kind/Chaos Mesh 실행이 완료되기 전에는
계약 검증으로 간주하지 않는다. GCS/Azure의 실제 SDK 계약이나 production cloud parity를
로컬 VersityGW/S3 결과에서 추론하지 않는다.

현재 구현은 StatefulSet/ordinal 및 별도 remote authority 구성에 결합되어 있으며,
Deployment+NoPVC ordinary recovery와 operator 및 authority 부재 조건은 구현·검증되지 않았다.
이는 acceptance target의 변경이 아니라 구현 제한이다. 현재 fixture 후보는 기존
`examples/recovery-anchor` 서비스를 cooperative-fencing Kind job에
연결하고, completed recovery의 정확한 Request/Receipt, retained DR-successor assignment,
init 대기 중의 retained assignment/immutable credential membership 관측, init 종료, actual
runtime WAL binding 및 target quorum/readback을 확인하도록 확장 중이다.
이는 설계된 검증 범위일 뿐이다. 이 후보의 실제 CI 실행 전에는 DR 경로가 통과했다고
주장하지 않으며, 기존 ordinary replacement 단계와 그 operator-paused evidence는 별도로
유지한다.

그 뒤의 공개 후보 `70b7c009`에서 실행한 [CI 40606696235](https://github.com/mrchypark/rhiza/actions/runs/40606696235)은 실패했다. Provider job의 [S3](https://github.com/mrchypark/rhiza/actions/runs/40606696235/job/122547854271)·GCS·Azure actual-SDK 경로는 모두 `TestEmbeddedObjectStoragePrepareRestore`에서 `restore generation anchor: certified archive view is unavailable`로 실패했다. 별도로 선택 검토한 [automatic Go](https://github.com/mrchypark/rhiza/actions/runs/40606696235/job/122547854653), [automatic server](https://github.com/mrchypark/rhiza/actions/runs/40606696235/job/122547854449), [automatic Rust](https://github.com/mrchypark/rhiza/actions/runs/40606696235/job/122547854563), [operator chaos](https://github.com/mrchypark/rhiza/actions/runs/40606696235/job/122547854453), [anchor chaos](https://github.com/mrchypark/rhiza/actions/runs/40606696235/job/122547854631)도 initial voter 시작 timeout으로 끝났고, 보존된 각 Pod log는 동일한 certified-archive-view 오류를 보였다. 이는 알려진 fresh-empty baseline 회귀와 일치한다. 이 후보는 수정·Pro 재검토 및 실제 재현 전까지 실패 상태이며, mandatory init·ordinary replacement·DR qualification은 여전히 pending이다.

S3/Versity와 GCS/Azure provider qualification은 각각 actual-SDK/emulator evidence로 따로
기록한다. Chaos Mesh qualification도 pinned image와 target/recovery 증거가 있는 CI만 인정한다.
이 구분은 현재 fixture나 공개 foundation CI가 해당 신규 시나리오를 이미 통과했다는 뜻이 아니다.

원문 검토 근거는 2026-10-28 Pro 검토의 ordinary Node-owned replacement, trusted bootstrap,
init execution settlement/quiescence, authorized DR boundary 계약이다. 기준선은 각각
`31e58ce` 및 `ccdafbe`; 공개 코드 링크는 위 문단의 repository source links를 따른다. 이
설계 기록은 미게시 구현 승인이나 전체 HA 완료 선언이 아니다. 관련 통합 이슈는
[#246](https://github.com/mrchypark/rhiza/issues/246)에서 추적한다.

- ordinary Node coordinator와 모든 survivor 재개, immutable phase request/receipt, 실제
  3→2→3 membership, canonical UID-derived identity caller 경로는
  `feature/ha-coherent-recovery-probes` 통합 후보에 연결되었다. 실제 Pod 교체 qualification은
  새 후보의 GitHub Actions 결과를 확인하기 전까지 pending이다.
- fresh bootstrap 승인·ledger pin, generated credential 전달, 빈 DataDir genesis, 사용자 SQL
  없는 certified barrier 및 archive durability/readback도 통합 후보에 연결되었다. 실제 fresh
  NoPVC bootstrap 검증과 단위 회귀 검사 결과는 구분한다.
- init execution별 nonce/outcome, no-signal namespace quiescence, exact terminal-container
  binding과 retained main/init proof 결합은 통합 후보에 포함되었다. 실제 실행 증거가 없는
  상태를 완료로 인정하지 않는다. 모호한 실행은 Pending이며
  새 실행이 과거 unknown을 덮어쓰지 않는다.
- authorized DR용 내부 init admission, same selected archive view anchor/base 검증, barrier
  binding, target materialization과 complete marker 정산 경로도 통합 후보에 연결되었다.
  새 DR fixture는 init 이전 assignment 관측과 최종 runtime/quorum 증거를 요구하지만,
  fixture 단위 테스트 통과가 실제 DR 실행 통과를 뜻하지는 않는다.
- provider별 production consistency, 게시 권한의 원자적 fencing, late writer·GC 격리, 명시적
  quorum-loss 증거와 데이터 손실 정책은 별도 qualification 대상이다.

오프라인 준비 API/CLI와 crash-boundary 단위 테스트는 이 통합 feature 후보의 미출시 변경이다.
기존 cooperative fencing CI와 fixture의 일부 로컬 단위 테스트가 통과해도 실제 fresh
NoPVC bootstrap, ordinary Pod replacement, init settlement, Chaos Mesh, production object
storage 또는 whole-generation DR 통과를 뜻하지 않는다. 이 문서를 전체 HA 복구 구현 완료나
release qualification 근거로 사용해서는 안 된다.
