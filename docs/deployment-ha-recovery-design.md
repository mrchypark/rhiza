# Deployment 기반 NoPVC HA 복구 설계 초안

- 작성일: 2026-10-27 (Asia/Seoul)
- 출처: 사용자와의 사이드 대화에서 논의한 요구와 설계
- 상태: 설계 초안. 아래 구현 상태 갱신은 부분적인 준비 API/CLI만 기록하며, 전체 HA 복구 완료·안전성 검증·Pro 승인 기록이 아니다.
- 설계 기준선: `a078cca81bd8c5a68d139eb2c12e04794cb44893`. 구현 상태 갱신: 2026-10-28 unreleased feature candidate.
- 경계: 이 문서 작성 외에 코드, 실행 중인 작업, 다른 브랜치와 워커는 변경하지 않았다.

## 구현 상태 갱신 — 2026-10-28

이 갱신은 승인된 오프라인 준비 경로의 범위만 반영한다. 설계 목표·NoPVC 전제·기존 writer 차단 요구는 바꾸지 않는다.

- Go API는 `PrepareRestore(ctx, Config)`이며, 실행 파일은 `rhiza --prepare-restore`로 이를 호출하고 일반 서버 시작 전에 종료한다.
- `Config.PreparedPodUID`는 `RHIZA_PREPARED_POD_UID`에서 읽는다. 이는 initContainer가 Downward API의 현재 Pod UID로 설정해야 하는 incarnation binding이다.
- HA membership learner의 새 멤버 정보는 `RHIZA_LEARNER` JSON에서 `Config.Learner`로 파싱한다. 이는 앱 역할을 고르는 `RHIZA_ROLE`과 다른 설정이다. `RHIZA_ROLE=learner`는 read-replica 실행 경로이며 consensus membership learner나 voter 교체 역할이 아니다.
- 준비 단계는 shared object store의 인증된 checkpoint와 연속 suffix를 DataDir에 오프라인 materialize하고, source/Pod-bound marker를 쓴다. 이는 재구성 가능한 HA learner 전용 경로이며 `Local` 모드나 단독 로컬 복구를 지원하는 공통 API가 아니다. 이후 정상 Node open은 설정된 Pod UID와 일치하는 marker를 요구한다. 준비 API 자체는 Node를 열거나 listener를 띄우거나 투표권·멤버십을 부여하지 않는다.
- marker intent, observer 생성, pending marker 기록 단계의 다섯 SIGKILL 경계에서 프로세스 재시작 테스트가 추가되었다. 종료된 자식 프로세스를 회수한 뒤 동일 Pod 준비 재개와 다른 Pod UID 거부를 검사한다. 실제 rename 이후의 별도 crash point나 전원 손실, Kubernetes initContainer, Chaos Mesh 검증을 뜻하지 않는다.
- NoPVC Deployment/StatefulSet manifest에 initContainer와 공유 DataDir/Pod UID를 연결하는 작업, 정상 quorum의 learner catch-up 및 실제 coordinator에 의한 voter 교체·승격, 분산 HA의 old-writer 게시/GC 차단 구현은 아직 완료되지 않았다. 아래 설계의 authority 경계를 유지하며 이를 모의 coordinator로 대체하지 않는다.
- 실제 CI Kind + Chaos Mesh 흐름과 ACK readback end-to-end 결과는 아직 없다. `operator-chaos` 또는 직접 Pod 삭제 테스트를 해당 증거로 간주하지 않는다.
- 조건부 설계 검토 기록과 기존 안전 조건은 이 문서의 원문대로 유지한다. 전체 HA 구현·운영 자격에 대한 최종 Pro 승인은 아직 없다. 이 변경은 미출시 feature branch 작업이며 Rhiza 0.21 release에 포함되지 않았다.

## 1. 목표와 합의한 방향

Rhiza는 오브젝트 스토리지를 복구 데이터의 진실 원천으로 사용하며, HA에서는
QuePaxa 합의를 사용한다. Kubernetes 배포는 NoPVC를 전제로 한다.

일반 Pod 장애는 Kubernetes와 Rhiza 내부에서 처리하고, 오퍼레이터는 정상적인
quorum 복귀가 불가능할 때의 전체 세대 재해 복구만 제어한다. StatefulSet을 필수로
두지 않고 Deployment도 지원할 수 있도록 구성한다.

데이터 복원과 투표권 획득은 별개의 절차다. initContainer가 데이터를 복원했다고
기존 voter의 투표권을 즉시 되살려서는 안 된다. 이전 identity의 합의 참여와
정식 저장소 게시를 차단하는 안전 조건은 유지한다.

이 문서는 Local filesystem maintenance 복구와 HA 복구를 구분한다. Local 모드의
OS 파일 잠금만으로 분산 HA의 이전 writer 차단이 해결되는 것은 아니다.

## 2. 책임 분리

| 구성 요소 | 책임 | 맡지 않는 일 |
|---|---|---|
| Kubernetes Deployment | 원하는 Pod 수 유지, 컨테이너 재시작, Pod 교체 | voter 투표권 부여, 합의 상태 복원 판단 |
| initContainer | 공통 Go 복구 API를 사용한 확정 데이터 복원과 결과 전달 | quorum 구성 대기, 단독 멤버십 변경 |
| Rhiza 런타임 | identity 검증, learner 따라잡기, 정상 quorum을 통한 멤버 교체, 쓰기·readiness 판단 | 증거 없는 새 세대 자동 생성 |
| 오퍼레이터 | 장애 관측, 전체 복구 필요성 판단, 구세대 차단과 새 세대 활성화 조율 | 일반 단일 voter 교체의 단계별 제어 |

기존 오퍼레이터의 단일 voter 교체 기능은 내부 대체 경로를 검증한 뒤 제거한다.
먼저 삭제하여 복구 기능에 공백을 만들지 않는다.

## 3. identity와 현재 코드의 제약

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
생성하고, `RHIZA_NODE_ID`가 `metadata.name`을 사용하도록 요구한다.

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

정상 quorum이 있을 때의 목표 경로:

1. Kubernetes가 실패한 Pod를 대체한다.
2. initContainer가 검증 가능한 확정 데이터를 복원한다.
3. 새 Rhiza 프로세스가 새 identity의 learner로 실행된다.
4. 기존 quorum에서 최신 상태를 따라잡는다.
5. Rhiza 내부가 기존 구성원 제거와 새 구성원 편입을 합의한다.
6. 실제 서비스 가능 조건을 만족하면 Ready가 된다.

추가와 제거의 정확한 순서는 아직 확정하지 않았다. 현재 코드는 한 번에 한 구성원만
변경하므로 중간 구성의 quorum 크기와 실패 허용도를 검토해야 한다.

## 5. initContainer 계약

HA 전용 Go API `PrepareRestore(ctx, Config)`와 실행 파일의 `--prepare-restore`
early-exit 경로가 unreleased feature candidate에 구현되어 있다. 이 경로는
`Local` 모드를 거부하며, shared object store, bootstrap voters, 재구성 설정과 새
`Config.Learner` identity를 요구한다. 기존 certified archive/checkpoint 및 materializer
복구 helper를 재사용한다. 단독 노드용 공통 Local 복구 API로 해석해서는 안 된다.

실제 initContainer에서 API를 호출하고 DataDir 및 복구 설정을 앱 컨테이너와 동일하게
공유하는 manifest wiring은 아직 미구현이다. initContainer는 `RHIZA_PREPARED_POD_UID`에
Downward API의 현재 `metadata.uid`를 전달해야 한다. HA 런타임의 consensus learner identity는
`RHIZA_LEARNER` JSON으로 설정되는 `Config.Learner`이며, `RHIZA_ROLE=learner`는 read-replica
분기다. 이를 멤버십 learner 실행 설정으로 사용하면 안 된다.

준비 단계는 현재 세대의 인증된 복구 지점만 materialize하며 quorum 구성·투표권 부여·멤버십
변경을 하지 않는다. 설정된 `PreparedPodUID`에 대한 marker 검증은 정상 Node open에
연결되어 있지만, 이 연결만으로 learner catch-up, authority 확인 또는 voter 승격이 완료되는
것은 아니다. 전체 quorum을 기다리지 않아 앱 시작과 순환 대기를 만들지 않는 계약은 그대로
유지한다.

데이터가 없거나 복구 근거가 누락된 경우 이를 신규 bootstrap 신호로 해석하지 않는다.
클러스터 최초 생성은 별도의 명시적 절차로 남긴다.

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

별도 fencer 프로세스는 필수 구현 형태가 아니다. 필수인 것은 이전 세대가 정식
HEAD·체크포인트 등 활성 상태를 게시하거나 새 세대의 데이터를 GC하지 못한다는 보장이다.
게시 전에 권한을 한 번 읽거나 오퍼레이터에게 중단 메시지를 보내는 것만으로는 부족하다.
멈췄다가 재개한 프로세스와 늦게 완료되는 업로드도 처리해야 한다.

저장소 기반 세대별 게시 권한을 우선 검토하되, 정확한 프로토콜은 미확정이다.
만료형 권한도 대안일 뿐 채택하지 않았다. 단순 로컬 TTL을 안전 근거로 삼지 않는다.
제공자 어댑터가 필요한 원자성과 차단 보장을 제공하지 못하면 자동 전체 복구를
지원한다고 주장하지 않는다. 차단 증거가 없으면 fail-closed로 유지한다.

Pod DELETE 성공, Pod 목록에서 사라짐, Node NotReady는 이전 프로세스의 종료나
쓰기 차단을 증명하지 않는다.

## 9. Deployment와 롤링 업데이트

Deployment replica 수와 실제 voter 수는 별개다. 추가 Pod는 learner 또는 대기 상태로
시작하며 생성 즉시 투표하지 않는다. NodeID와 통신 주소를 분리하되 주소 검색 결과를
구성원 인증의 근거로 사용하지 않는다.

readiness는 프로세스 실행 성공만으로 결정하지 않는다. 따라잡기와 역할에 맞는
서비스 가능성을 반영해야 한다. 다만 새 Pod가 Ready가 되려면 기존 Pod 제거가 필요하고,
Deployment는 새 Pod가 Ready가 되어야 기존 Pod를 제거하는 교착도 피해야 한다.

따라서 `maxSurge`·`maxUnavailable`, 멤버 추가·제거 순서, learner와 voter의 readiness
조건을 함께 설계하고 검증해야 한다. 이 문서는 구체적인 롤아웃 설정을 아직 확정하지 않는다.

## 10. 최소 CRD 방향

장기 정책에는 워크로드 참조, 원하는 voterCount, 저장소·인증 참조, Manual/Automatic
전체 복구 정책과 데이터 손실 허용 정책을 둔다. 상태에는 활성 세대, 복구 단계,
복구 지점과 차단·실패 이유를 기록한다. 구체적인 스키마는 미확정이다.

일반 단일 voter 교체용 CR·Pod별 교체 절차는 Rhiza 내부 대체 경로의 검증 후 정리한다.

## 11. 구현 순서와 합격 기준

1. NodeID·주소·Pod 이름을 분리하고 인증·멤버십 검증은 유지한다.
2. 정상 quorum에서 Rhiza 내부의 단일 구성원 교체를 완성한다.
3. 공통 복구 API를 initContainer에 연결한다.
4. 세대별 게시·GC 차단과 전체 복구 재개 프로토콜을 검증한다.
5. 오퍼레이터를 전체 복구 책임으로 축소한다.

검증에는 다음을 포함한다.

- 같은 Pod의 컨테이너 재시작과 새 Pod의 emptyDir 유실을 구분한다.
- 단일 Pod 교체는 오퍼레이터의 멤버 교체 개입 없이 완료된다.
- 동시 교체·롤링 업데이트 중 투표권 중복과 quorum 손실을 방지한다.
- 생존 peer 하나의 데이터를 보존하며, 이를 정상 quorum으로 오인하지 않는다.
- 네트워크 단절만으로 이전 세대가 죽었다고 판정하지 않는다.
- 오래된 프로세스 복귀·늦은 업로드·GC가 새 세대에 영향을 주지 못한다.
- 오퍼레이터 재시작·복구 중단 후에도 전체 복구가 중복 실행되지 않는다.
- 실제 Chaos Mesh 검증은 주입된 장애와 복구 양쪽을 확인한다.
- 로컬 VersityGW 결과는 S3 인터페이스 증거로 한정하며 다른 제공자 검증으로 확대하지 않는다.

최종 목표는 다음 두 가지다.

1. quorum이 살아 있으면 Kubernetes와 Rhiza 내부에서 일반 복구한다.
2. 정상 quorum 복귀가 불가능하면 이전 세대를 확실히 차단한 경우에만 전체 복구한다.

## 12. 아직 결정·검증하지 않은 사항

- 정상 quorum 복귀 불가능을 확정하거나 수동 판단으로 전환하는 증거·정책
- 저장소별 원자적 게시 권한 차단, 진행 중 작업과 GC 처리
- Rhiza 내부 멤버 교체 조율자와 동시 교체 직렬화
- NodeID·주소 검색의 분리 및 현재 불변 Member와의 접점
- 롤링 업데이트 중 추가·제거 순서와 readiness 교착 방지
- 전체 복구의 확정 지점 선정과 데이터 손실 정책
- initContainer manifest wiring, 동일 DataDir/config 전달, Pod UID Downward API binding

초안의 최초 작성은 코드 변경이나 클러스터 실행을 포함하지 않았다. 위에 적은 오프라인
준비 API/CLI와 crash-boundary 단위 테스트는 별도의 unreleased candidate 변경이다.
이 문서 갱신은 source·CI·클러스터를 변경하지 않았다. 실제 initContainer, Chaos Mesh,
Deployment, GCS 또는 release qualification은 아직 검증되지 않았다. 이 초안을 전체 HA
복구 구현 완료 근거로 사용해서는 안 된다.
