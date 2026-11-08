# 임베딩 애플리케이션과 Rhiza Operator

이 문서는 현재 v1alpha2 managed HA의 역할과 운영 경계를 설명한다.
배포 계약은 [Operator 안내](../deploy/operator/README.md)를, 장애별 대응은
[복구 경계](recovery.md)를 먼저 확인한다. 라이브러리 버전만 올리면 임의의
애플리케이션이 자동으로 managed HA에 연결되는 구조는 아니다.

## 각 구성 요소가 맡는 일

| 구성 요소 | 현재 역할 |
| --- | --- |
| `rhiza-operator` | namespace의 초기화를 reconcile하고 설치된 private runtime profile과 Secret을 검증한다. generation 0 Deployment를 만들고 실제 Pod identity를 바탕으로 genesis와 bootstrap plan을 확정한다. |
| Kubernetes Deployment와 kubelet | 설정된 replica 수를 유지하고 컨테이너를 재시작한다. 새 Pod의 생성만으로 투표 권한이 생기지는 않는다. |
| 참조 managed host | background replacement loop에서 교체 후보를 관측하고, 살아 있는 quorum을 통해 ADD-first repair를 조정한다. |
| Rhiza 엔진 | WAL identity, 현재 membership, 인증된 로그를 검증하고 learner catch-up과 결정의 인증·적용을 수행한다. |

실제 경로는 [operator 진입점](../cmd/rhiza-operator/main.go),
[초기화 reconciler](../pkg/operator/initializer_v1alpha2.go),
[참조 host loop](../cmd/rhiza-ha-init/serve.go),
[교체 coordinator](../pkg/managedsql/replacement.go)에서 확인할 수 있다.
일반 요청마다 Kubernetes CR을 예약하고 완료하는 구조는 아니다.
초기화·profile·membership·repair authority와 정상 요청의 엔진 실행 경로는 구분한다.

## NoPVC Pod의 시작과 교체

managed Pod는 [고정 Pod/init 계약](../pkg/operator/bootstrap_members_v1alpha2.go)을 따른다.
두 init container와 main runtime은 같은 `emptyDir`를 사용한다.

1. `rhiza-prepare-identity`가 실제 Pod UID에 결합된 identity handoff를 준비한다.
2. `rhiza-await-bootstrap-plan`이 확정된 bootstrap plan 또는 repair 준비를 기다린다.
3. main runtime이 profile·identity·인증된 자료를 검증하고 startup/adoption과 catch-up을 수행한다.

현재 참조 profile은 main container에서 `rhiza-ha-init serve-managed-sql`을 실행한다.
이미지에는 필요한 실행 파일이 있어야 하고 private runtime profile, 일치하는 Secret,
ServiceAccount/RBAC, 공유 object store와 peer 네트워크가 준비되어야 한다.
profile을 설치하지 않은 기본 initializer의 inert candidate는 실행 가능한 managed DB가 아니다.
사용자 애플리케이션이 이 참조 host와 같은 통합을 구현했다고 자동으로 가정하지 않는다.

### CR로 새 배포를 시작하는 순서

CR에서 managed Deployment를 만드는 경로는 이미 구현되어 있다. 다만 기본
initializer만 설치하는 것과 managed profile을 설치하는 것은 다르다.

1. namespace, 공유 object-store namespace, 명시적 profile 9/10, peer 네트워크와
   private admin 인증을 배포 소유자가 결정하고 준비한다. CI emulator 자격 증명을
   실제 배포 설정으로 복사하지 않는다.
2. `before-ack`, reconfiguration, peer port와 실제 실행 파일을 포함한 `InitImage`를
   지정한 private runtime profile을 준비한다. Secret `runtime`의 key는
   `runtime.json`이며 설치된 profile과 Secret 내용이 정확히 같아야 한다.
3. 공개 [managed overlay](../deploy/operator-managed/)를 namespace와 검토한 image로
   렌더링·검토한 뒤 strict validation으로 적용한다. profile Secret과 storage는
   overlay가 대신 만들지 않는다. 상세 절차는 [설치 안내](../deploy/operator/README.md#installing-the-managed-reference-host)를 따른다.
4. 일치하는 protocol과 참조 host image, `genesisIntent: Create`를 지정한
   [RhizaCluster](../deploy/operator/automatic-example.yaml)를 생성한다. status나
   DB Deployment를 미리 심지 않는다.
5. operator가 만든 Deployment의 owner UID, init 완료, 확정 bootstrap plan과
   해당 CR label의 실제 Pod Ready를 확인한 뒤 사용한다. CR 생성 성공만으로
   데이터베이스가 준비되었다고 판단하지 않는다.

이 절차는 임의 애플리케이션을 자동 변환하거나 Service·autoscaling·전체 quorum DR를
제공하는 절차가 아니다. overlay의 namespace/RBAC와 private profile은 명시적으로
검토해야 하는 배포 입력이다.

같은 Pod의 컨테이너 재시작은 `emptyDir`를 유지하지만, Pod 교체는 디렉터리를 잃는다.
quorum이 살아 있으면 새 ID와 새 WAL의 learner를 먼저 추가하고 인증된 prefix와 admission을
검증한 뒤 이전 voter를 제거한다. 유실한 WAL의 ID를 빈 WAL에 재사용하지 않는다.
같은 identity로 재가입하려면 원래 WAL과 현재 인증된 membership이 필요하다.

## 설정과 지원 경계

- `RhizaCluster.spec.voterCount`는 생성 시 3–64로 정하며 이후 변경할 수 없다. autoscaling 설정이 아니다.
- 정상 managed HA는 명시적 append-only profile 9(SQL/KV) 또는 10(Graph)을 사용한다. 7/8 실행이나 자동 old-WAL migration은 지원하지 않는다.
- public managed Host는 BeforeAck durability를 요구한다. HA object data는 append-only이며 online delete GC를 제공하지 않는다.
- initializer는 Recreate 전략을 강제하거나 peer/client Service를 자동 관리하지 않는다. rollout, 접근 경로와 Service 구성은 배포에서 별도로 결정한다.
- 별도 상시 fencer Deployment나 외부 lease 서비스는 정상 quorum 교체의 필수 구성 요소가 아니다. Recreate나 Pod 삭제 응답은 partition 중 이전 프로세스의 종료 증거가 아니다.

설정의 범위와 불변 조건은 [cluster schema](../deploy/operator/cluster-crd.yaml)와
[Operator 안내](../deploy/operator/README.md)를 따른다. 요청 API와 UNKNOWN 처리 방법은
[managed request API](managed-internal-request-api.md), Graph의 추가 경계는
[managed Graph HA](managed-graph-ha.md)를 참고한다.

## quorum 상실과 DR

quorum을 잃으면 쓰기와 linearizable 읽기는 실패한다. 준비된 노드의 local 읽기는
계속 가능하지만, local Ready를 현재 quorum의 증거로 사용하면 안 된다.
우선 원래 노드·디스크·네트워크를 복원하며 pending state와 UNKNOWN의 원본을 보존한다.
UNKNOWN을 해결하려고 새 ID나 변경한 payload로 mutation을 다시 제출하지 않는다.

현재 operator에는 whole-generation DR executor가 없다. recovery 요청·status API나
`automaticRecovery: OFF`라는 schema 기본값이 있다는 사실은 실행 지원을 뜻하지 않는다.
다른 정책값을 받아들이더라도 자동 세대 전환을 수행한다는 뜻은 아니다.

별도로 승인된 DR 설계에서 자동 시도는 기본 OFF다. 실제 quorum 실패를 감지해도
활성화 권한이 생기지는 않는다. 이전 runtime·voter·sidecar 전체의 종료 또는 격리,
재생성 방지, 진행 중인 storage 작업의 완료, 모든 성공 ACK를 포함한 검증 소스,
live authority와 새 ready quorum이 필요하다. 증거가 불명확하면 Blocked로 남긴다.
이 설계를 현재 구현된 자동 복구 기능으로 설명하지 않는다.

## Local과 검증 자료

Local은 peer quorum이 없는 별도 single-writer 모드다.
[Local init-container recovery](local-init-container-recovery.md)의 배포 책임과
미해결 maintenance token의 fail-closed 조건을 managed HA 절차와 혼동하지 않는다.
[Chaos qualification](ha-managed-cluster-chaos.md)은 격리된 실험의 범위와 증거를 설명하며,
production cloud parity·network partition·전체 quorum DR의 보편적 보증이 아니다.

## 과거 예제

기존 alpha1/StatefulSet·fencer·anchor YAML과 긴 통합 예제는
[v0.23.1 원본 문서](https://github.com/mrchypark/rhiza/blob/v0.23.1/docs/embedded-operator.md#지원하지-않는-legacy-운영-예제)에서 추적할 수 있다.
해당 예제는 현재 설치 절차가 아니다. alpha1 변환 경로는 없으며 `spec.membership`,
`spec.fence`, `spec.anchorID`와 `RHIZA_AUTOMATIC_RECOVERY=true`를 현행 운영에 적용하지 않는다.

### 6. Recovery Anchor 연동

기존 링크를 위한 역사 안내다. [v0.23.1의 anchor 연동 예제](https://github.com/mrchypark/rhiza/blob/v0.23.1/docs/embedded-operator.md#6-recovery-anchor-연동)는
지원하지 않는 과거 통합에 속하며, 현재 v1alpha2의 DR executor 설치 절차가 아니다.
