# 임베딩 애플리케이션에서 Rhiza Operator 사용하기

Rhiza를 라이브러리로 사용하는 **Go·Rust 애플리케이션의 프로세스 자체**를
Operator가 복구할 수 있다. 별도의 Rhiza 서버 프로세스나 데이터베이스 sidecar는
필요하지 않다. Operator는 Kubernetes에 별도 Deployment로 실행한다.

이 기능은 기존 **3-voter StatefulSet + emptyDir(no-PVC) + 공유 오브젝트 저장소**를
대상으로 한다. 소스 세대의 복구 자료를 검증하고 새 세대로 복사한 다음,
애플리케이션 Pod 전체를 재시작한다. Pod 이름·peer 주소는 유지하지만
ClusterID와 peer/admin 토큰은 바뀐다. 애플리케이션의 다른 emptyDir 상태도
사라지므로, 필요한 업무 상태는 Rhiza 또는 별도 영속 저장소에 두어야 한다.

## 1. 임베딩 프로세스 연결

### Go

```go
config, err := rhiza.ConfigFromEnv()
if err != nil { return err }
db, err := rhiza.Open(ctx, config)
if err != nil { return err }
defer db.Close()

server := &http.Server{
    Addr: config.BindAddr, // 예: 0.0.0.0:9091
    Handler: db.OperatorHandler(),
    ReadHeaderTimeout: 5 * time.Second,
}
// 애플리케이션 종료 시 server.Shutdown도 호출한다.
return server.ListenAndServe()
```

[실행 가능한 Go 예제](../examples/operator-embedded/main.go)를 참고한다.
업무 처리는 같은 `db`의 인프로세스 API를 사용한다. 기존 애플리케이션 HTTP
서버가 있다면 별도 포트에 위 핸들러를 제공하거나, 아래 정확한 경로만 라우터에
연결한다. 접두 경로를 추가하지 않는다.

`OperatorHandler()`는 다음 여섯 경로만 제공한다. SQL·KV·Graph 등 일반 DB API와
`/ready`는 노출하지 않는다. 기존 `db.Handler()`를 사용하는 서버도 계속 호환된다.

| 경로 | 동작 |
| --- | --- |
| `GET /recovery/status` | 현재 세대·durability·복구 위치와 실제 quorum 확인. 로컬 Ready와 quorum은 다르다. |
| `GET /recovery/probe` | `Authorization: Bearer <RHIZA_ADMIN_TOKEN>`와 query nonce를 보내고, 응답 HMAC으로 새 recovery 상태를 검증. |
| `POST /recovery/archive` | `Authorization: Bearer <RHIZA_ADMIN_TOKEN>` 인증 후 로컬 확정 suffix를 저장소에 발행. 빈 토큰은 비활성화. |
| `GET /membership/status` | `Authorization: Bearer <RHIZA_ADMIN_TOKEN>` 인증 후 현재 membership 상태를 반환. |
| `POST /membership/change` | `Authorization: Bearer <RHIZA_ADMIN_TOKEN>` 인증 후 membership 변경을 요청. |
| `POST /membership/abort` | `Authorization: Bearer <RHIZA_ADMIN_TOKEN>` 인증 후 진행 중인 membership 추가를 중단. |

### Rust

```rust
let mut db = rhizadb::Db::open_from_env()?;
let address = db.start_operator("0.0.0.0:9091")?;
// db로 업무 처리를 수행한다. address는 실제 바인딩된 주소다.
// 종료 시 close 또는 Drop이 복구 HTTP listener도 닫는다.
db.close()?;
```

Rust SDK는 같은 Go 엔진과 환경 설정 파서를 사용한다. 프로세스를 실행하기
**전에** 환경 변수를 주입한다. 실행 중 Rust 환경 변수 변경으로 엔진 설정을
바꾸지 않는다. SDK 호출은 동기식이므로 async 런타임에서는 blocking 실행
영역에서 열기·닫기 등을 수행한다. 자세한 API는 [Rust SDK](../sdk/rust/README.md)를
참고한다.

이 API가 포함된 Rhiza 소스/SDK 버전을 사용해야 한다. 아직 이 API가 없는
이전 배포 버전에서 호출할 수 없으며, 개발 시에는 SDK의 로컬 path 의존성을
사용한다. Operator와 임베딩 엔진은 같은 소스 버전으로 빌드하는 것을 권장한다.

## 2. 환경 변수는 실행 시점의 원본으로 사용

Operator는 컨테이너의 `RHIZA_*` 환경 설정을 읽고 다음 항목을 새 세대로
교체한다. 애플리케이션은 매 시작마다 `ConfigFromEnv()` / `open_from_env()`를
호출하고 아래 값을 하드코딩하거나 다른 설정 파일로 덮어쓰지 않아야 한다.

| 환경 변수 | 의미 |
| --- | --- |
| `RHIZA_CLUSTER_ID` | 세대별 클러스터 ID |
| `RHIZA_CLUSTER_MEMBERS` | 3개 member의 JSON. 각 항목은 `node_id`, `url`, `peer_url`, `token`을 포함 |
| `RHIZA_ADMIN_TOKEN` | 복구 archive 발행 인증 토큰 |
| `RHIZA_OBJSTORE_DURABILITY` | `async` 또는 `before-ack` |
| `RHIZA_NODE_ID` | `metadata.name` downward API. 예: `myapp-0` |
| `RHIZA_DATA_DIR` | emptyDir에 위치한 절대 경로. 예: `/data` |
| `RHIZA_PEER_ADDR` | QUIC listener. 예: `0.0.0.0:9090` |
| `RHIZA_BIND_ADDR` | 예제의 복구 HTTP listener. 예: `0.0.0.0:9091` |
| `RHIZA_OBJSTORE_*` | Operator와 동일한 저장소·bucket·prefix. provider를 명시한다. |

ConfigMap/Secret 참조를 사용할 수 있다. `RHIZA_NODE_ID=metadata.name` 이외의
동적 fieldRef와 `$(...)` 환경 확장은 지원하지 않는다. 자체 Go 설정은 로더
호출 후 추가할 수 있지만, 세대·멤버·저장소·durability·데이터 경로 계약은 유지한다.
설정 오류나 `ErrVoterStateLost`가 나면 시작을 실패시켜야 한다. 빈 새 DB로
fallback하거나 peer 상태를 지우고 기존 세대로 재가입하면 안 된다.

## 3. StatefulSet 연결

아래는 기존 애플리케이션 StatefulSet에 반영할 핵심 설정이다. `application`
컨테이너의 image·command·업무 포트는 그대로 사용할 수 있다.

```yaml
spec:
  replicas: 3
  podManagementPolicy: Parallel
  updateStrategy:
    type: OnDelete
  template:
    spec:
      containers:
        - name: application
          image: YOUR_REGISTRY/myapp:YOUR_VERSION
          ports:
            - name: recovery
              containerPort: 9091
              protocol: TCP
            - name: peer
              containerPort: 9090
              protocol: UDP
          envFrom:
            - configMapRef:
                name: rhiza-config
            - secretRef:
                name: rhiza-object-store
          env:
            - name: RHIZA_NODE_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: RHIZA_DATA_DIR
              value: /data
            - name: RHIZA_BIND_ADDR
              value: 0.0.0.0:9091
            - name: RHIZA_PEER_ADDR
              value: 0.0.0.0:9090
            - name: RHIZA_CLUSTER_MEMBERS
              valueFrom:
                secretKeyRef:
                  name: rhiza-peer-credentials
                  key: members
            - name: RHIZA_ADMIN_TOKEN
              valueFrom:
                secretKeyRef:
                  name: rhiza-peer-credentials
                  key: admin
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          emptyDir: {}
```

`volumeClaimTemplates`와 데이터 경로의 PVC/subPath/중첩 mount는 제거한다.
기존 데이터를 가진 배포의 PVC를 단순히 제거해 전환하지 않는다. 위 계약은
이미 no-PVC로 준비된 배포의 복구용이다.

headless Service에는 `publishNotReadyAddresses: true`와 peer UDP 포트를 설정한다.
멤버 ID는 `myapp-0`부터 `myapp-2`, peer URL은 예를 들어
`quic://myapp-0.myapp-peers.NAMESPACE.svc.cluster.local:9090` 형태다.
초기 `rhiza-peer-credentials`는 고유한 member별 토큰과 admin 토큰을 담은 Secret으로
별도 생성한다. 실제 자격 증명을 Git에 기록하지 않는다.

Operator는 소유 StatefulSet UID에 속한 Pod IP로 직접 접근한다.
`recovery`라는 컨테이너 포트가 있으면 사용하고, 없으면 기존 `http` 포트,
그것도 없으면 8080을 사용한다. 복구 포트는 인터넷/Ingress로 공개하지 말고
NetworkPolicy로 Operator에서의 접근을 허용한다. HTTP이므로 신뢰할 수 있는
클러스터 네트워크에서 사용한다. 애플리케이션의 로그인 middleware로 관리
경로를 가로막지 않는다. readiness/liveness는 별도의 애플리케이션 경로를
사용하고, quorum 상실만으로 모든 Pod를 재시작하는 probe를 만들지 않는다.

## 4. Operator 배포와 관찰

[배포 파일](../deploy/operator/README.md)의 CRD·RBAC·Deployment를 사용한다.
애플리케이션 namespace로 모든 namespaced 리소스와 RoleBinding의 ServiceAccount
namespace를 맞춘다. Operator의 `rhiza-config`, `rhiza-object-store` 참조도
애플리케이션과 동일한 저장소를 가리키도록 설정한다.

Operator 이미지는 Rhiza 저장소 루트에서 직접 빌드할 수 있다.

```sh
docker build -f Dockerfile.operator -t YOUR_REGISTRY/rhiza-operator:YOUR_VERSION .
docker push YOUR_REGISTRY/rhiza-operator:YOUR_VERSION
```

Deployment의 image를 이 태그로 교체한다. 샘플 `rhiza-operator:latest`는
공개 레지스트리에 배포되었다는 의미가 아니다. CRD를 먼저 설치하고 해당
namespace에 RBAC와 Operator Deployment를 적용한다.

```yaml
apiVersion: rhiza.mrchypark.dev/v1alpha1
kind: RhizaRecovery
metadata:
  name: myapp-observe
  namespace: YOUR_NAMESPACE
spec:
  statefulSet: myapp
  container: application
  sourceClusterID: YOUR_CURRENT_CLUSTER_ID
  durability: async
  recoveryID: ""
  allowDataLoss: false
```

`container`는 Rhiza를 연 애플리케이션 컨테이너의 정확한 이름이다. 빈
`recoveryID`는 관찰만 한다. `kubectl get rhizarecoveries -n YOUR_NAMESPACE -o yaml`로
peer의 Ready/Quorum과 상태 메시지를 확인한다.

자동 복구는 [Operator 배포 안내](../deploy/operator/README.md#automatic-recovery-with-kubernetes-fencing)의
`RHIZA_AUTOMATIC_RECOVERY=true`, `RHIZA_FENCER_BACKEND=kubernetes`로 켜며 모든 voter의
opt-in이 필요하다. executor는 외부의 신뢰된 주체이고, learner는 애플리케이션 image와
command를 복제하며, 새 `RHIZA_*` 환경은 재시작 때 읽힌다.

## 5. 두 개 또는 전체 Pod 유실 후 복구

1. 기존 노드·네트워크를 복원할 수 있으면 먼저 복원한다. quorum이 없다는
   사실만으로 새 세대를 시작하지 않는다.
2. 복구하려면 고유한 `recoveryID`를 정한다. 모든 이전 voter·클라이언트·replica와
   저장소 writer가 다시 활동하지 못하도록 **외부 fencing**을 완료한다.
   Pod 삭제나 Kubernetes Ready=false만으로 fencing을 증명할 수 없다.
3. 현재 StatefulSet UID를 확인하고 복구 CR의 `spec.fence`에 동일한 recoveryID,
   sourceClusterID, UID, `confirmed: true`, 구체적 evidence를 기록한다.
   이 spec 수정 권한은 신뢰할 수 있는 운영자/자동화에만 부여한다.
4. 소스가 `async`이면 `allowDataLoss: true`가 필요하다. 저장소에 아직 발행되지
   않은 성공 응답 쓰기가 유실될 수 있다. 소스가 `before-ack`이면 검증된
   certified archive가 필요하며, archive가 없다고 데이터 유실 동의로 우회할 수 없다.
   `spec.durability`는 **복구 후 대상 세대**의 모드이며 소스의 보장을 바꾸지 않는다.
5. Operator는 가능한 소스 suffix 수집, 유일한 successor 예약, archive seal,
   대상 복사·검증을 거쳐 3개 Pod를 새 세대로 재시작한다. 중단되면 같은 CR로
   재개한다. 예약 후 CR을 삭제하거나 source/recoveryID를 바꾸지 않는다.
6. `Complete`와 두 개 이상의 일치하는 target quorum을 확인하고 애플리케이션의
   업무 읽기/쓰기를 검증한다. 다음 복구에는 완료된 대상 ClusterID를 소스로
   별도 CR을 생성한다.

before-ack는 성공 응답한 쓰기의 내구성 계약이다. 장애 직전 읽기로 관찰한
모든 미응답 쓰기까지 보존한다는 뜻은 아니다. 애플리케이션의 업무 토큰·외부
작업·캐시는 Operator가 갱신하지 않으므로 전체 프로세스 재시작에 맞게 처리한다.

복구 상태가 `AwaitingFence`면 fencing 증명이, `Blocked`면 정책/배포/자료 검증
문제가 남아 있다. 상태 메시지를 해결한 뒤 같은 요청을 재개한다. source archive를
수정하거나 seal을 제거해 강제로 진행하지 않는다. 상세 계약은
[복구 설계](recovery.md)와 [Operator 운영 설명](../deploy/operator/README.md)에 있다.
