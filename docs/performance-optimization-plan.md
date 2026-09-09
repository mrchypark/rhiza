# 성능 최적화 후속 작업

기준일: 2026-09-10. 브랜치: `feature/quorum-membership`.
현재 로컬의 미커밋 멤버십 구현을 기준으로 한다. 아래 완료 표시는 머지·릴리즈를 뜻하지 않는다.

## 현재 상태

| 항목 | 상태 | 확인된 결과 |
| --- | --- | --- |
| 코어 내부 설정 조회의 멤버 목록 복사 제거 | 완료 | 공개 API는 복사 유지. 작업당 할당량 7.4% 감소 |
| 1. WAL 기록 전 인증서 중복 해석 제거 | 완료 | 작업당 할당량 54,663 → 42,971 bytes, 21.4% 감소. 할당 횟수 299 → 240 |
| 2. 네트워크 설정 캐시 조회 개선 | 구현·기능 검증·CI 측정 완료 | 실제 QUIC 할당량 7.25% 감소. 시간 유의차 없음 |
| 3. RPC 인증용 임시 멤버 맵 제거 | 구현·기능 검증·CI 측정 완료 | 인증 시간 3명 29.45%, 16명 73.54% 감소 |
| 4. 중복 catch-up 합치기 | 구현·기능 검증·CI 측정 완료 | 같은 source fetch 2→1, 할당량 48.96% 감소. durable prefix 장벽 유지 |

1번의 시간 중앙값은 14.18 → 14.55ms로, 속도 개선은 확인되지 않았다.
수치는 Apple M3 / Go 1.27, 인프로세스 3-peer transport와 로컬 WAL,
1,000회 작업 × 3회 측정 결과다. 실제 QUIC·SQL·Kubernetes 성능을 대표하지 않는다.
이전 측정과 상세 제약은 [멤버십 문서](quorum-membership.md)의 성능 항목을 참고한다.
1번 적용 후 전체 Go 테스트, race, vet, CLI·Operator 빌드가 통과했다.

## 2. 네트워크 설정 캐시 조회 개선

대상: `pkg/network/transport.go`의 `transportForSlot`, `transportForCurrent`, `transportFor`.
기존 경로는 공개 `Core.ClusterForSlot`로 멤버 목록을 복사한 뒤 ConfigID 캐시를 조회했다.
이제 `ConfigIDForSlot`/`ConfigID`를 먼저 사용하고 캐시 miss에서만 전체 설정을 읽는다.
ID 조회와 snapshot 사이 설정이 바뀌면 실제 snapshot의 ID로 다시 조회·저장한다.

- 설정 ID만 먼저 조회하고 캐시에 있으면 기존 transport를 사용한다.
- 전체 설정 복사는 새 설정의 transport를 만들 때만 수행한다.
- 필요한 경우 Core에 멤버 목록을 노출하지 않는 설정 ID 조회를 추가한다.
- 초기 transport 재사용은 생성 당시 설정과 동일함을 확인할 수 있을 때만 한다.
  ID만 같다고 임의로 재사용하지 않는다.
- 과거 슬롯 라우팅, 종료 후 요청 거부, 설정별 연결 풀·TLS 세션 분리를 유지한다.

검증: 캐시 hit에서는 전체 설정 조회가 발생하지 않는지, 설정 전환 시 별도
transport가 생성되는지, 과거 슬롯이 올바른 설정을 쓰는지, Close 및 토큰 변경
격리가 유지되는지 확인한다. 실제 QUIC 경로의 할당과 지연을 전후 비교한다.

## 3. RPC 인증용 임시 멤버 맵 제거

대상: `pkg/network/peer_server.go`의 요청 인증 경로.
기존 `config.MemberSet()[sender]`와 `current.MemberSet()[sender]`를
`peerMember`의 직접 slice 조회로 대체했다. 역순 조회로 중복 ID에 대한 기존
MemberSet의 마지막 값 선택도 유지한다.

과거 설정과 현재 설정의 검증, 상수 시간 토큰 비교, 제거된 voter 차단,
learner의 관리자 권한 예외, ConfigID 검증을 그대로 유지한다.
Core 인증 스냅샷 API 추가 같은 큰 변경은 이 단계에 포함하지 않는다.

검증: 정상 voter, 제거된 voter, 잘못된 토큰, 과거 슬롯, learner 관리자 요청의
허용·거부 결과가 동일해야 한다. 인증 경로 벤치마크에서 할당 감소를 확인한다.

## 4. 중복 catch-up 합치기

대상: `pkg/network/server.go`의 `catchUpFrom`과 Record 호출 경로.
기존 전역 동시 실행 제한 2개 앞에 source별 cancellable 대기를 추가했다.
대기자는 이전 소유자의 종료를 관찰한 뒤 새 엔트리를 획득하고 prefix를 다시 확인한다.
소유자의 context나 오류는 대기자에게 전파하지 않으며, 대기자가 전역 용량을 차지하지 않는다.
`durable=true`는 `Core.EnsureDurableThrough`로 hint prefix도 append·공유 sync 후 완료한다.
기존 checkpoint의 prefix append helper를 재사용하고, sync 실패 시 durable 표시를 하지 않는다.
동시 compaction으로 floor 이하가 된 슬롯은 건너뛰며 제거된 상태를 다시 만들지 않는다.
공유 commit은 sync 시작 전에 batch를 분리하므로 이후 append가 이전 sync 완료에 합류하지 않는다.

한계: durable 확인은 floor 이후 retained prefix를 O(길이)로 순회한다. 긴 WAL에서
반복 순회가 병목으로 측정되면 durable watermark를 검토한다. 서로 다른 source의
중복 페이지까지 합치지는 않는다. 실행 중 파일 append/잠금 자체는 즉시 취소되지 않으며,
취소는 이미 append된 marker나 기존 공유 fsync를 롤백하지 않는다.

- 같은 source의 작업을 합치거나 직렬화하고, 대기 후 필요한 prefix를 다시 확인한다.
- 서로 다른 source는 계속 진행할 수 있어야 한다.
- 요청 취소가 다른 요청의 복구까지 취소하지 않도록 한다.
- `durable=true`와 hint 설치를 구분한다. Tip이 올랐다는 이유만으로
  hint 결과를 내구성 있는 완료로 취급하면 안 된다.
- 멤버십 control의 강제 영속화와 terminal prefix 장벽을 유지한다.
- 동시 실행 제한을 단순히 늘리는 변경은 하지 않는다.

검증: 동일 source의 동시 요청에서 fetch 중복 감소, 취소된 대기자,
서로 다른 source의 진행, durable/hint 혼합, control 포함 페이지를 결정적으로
시험한다. 지연된 follower에서 RPC 수·설치 횟수·지연을 측정한다.
안전한 병합이 커지면 먼저 중복량을 측정하고 비용 대비 적용 여부를 판단한다.

## 작업 순서와 통합

2 → 3 → 4 순서로 통합·측정한다. 병렬 구현 시 소유 파일을 나눈다:
transport 담당은 `transport.go`, 인증 담당은 `peer_server.go`, catch-up 담당은
`server.go`; 공통 Core 변경과 최종 통합은 주 작업에서 맡는다.
벤치마크는 다른 테스트나 벤치마크와 동시에 실행하지 않는다.

사용자 요청에 따라 2~4번 성능 측정은 CI에서 수행한다. 로컬 시간은 판정에 사용하지 않는다.
각 변경 전후 동일 조건으로 측정하고, 시간·bytes/op·allocs/op를 함께 기록한다.
안정적인 속도 개선이 없으면 할당 감소와 처리량 개선을 구분해서 보고한다.
기존 코어 기준 명령:

```sh
go test ./pkg/quepaxa -run '^$' -bench '^BenchmarkCoreProposeThreePeersParallelReconfigurationEnabled$' -benchtime=1000x -count=3 -timeout=180s
```

이 명령만으로 네트워크 최적화를 검증할 수는 없다. 변경 경로를 통과하는
QUIC 벤치마크와 동작 테스트를 별도로 사용해야 한다.
최종 후보에서 관련 테스트와 다음 검사를 실행한다:

```sh
CGO_ENABLED=0 go test ./...
go test -race ./...
go vet ./...
CGO_ENABLED=0 go build ./cmd/rhiza ./cmd/rhiza-operator
git diff --check
```

## 유지해야 할 조건

16슬롯 파이프라인, 쿼럼·인증서 검증, learner WAL identity,
설정 전환의 영속화 장벽, 오래된 voter 차단을 약화시키지 않는다.
no-PVC 자동 복구·Node·Operator 연동 완료와 성능 최적화를 혼동하지 않는다.
기존 미커밋 변경과 `docs/automatic-recovery-design.md`는 보존한다.

## MiMo 병렬 실행 확인

이전 작업의 `unreadable_encrypted_agent_task`는 이번 실행에서 재현되지 않았다.
`multi_agent_v1__spawn_agent`에 `model: "opencode-go/mimo-v2.5"`와 범위가 명확한
`message`를 지정하는 방식으로 실제 파일 읽기·수정·실행 및 테스트 작성을 확인했다.
추가 옵션 생략은 검증된 호출법이며, 옵션이 과거 장애 원인이라는 증거는 아니다.
`functions.exec`의 도구 결과는 `text(await tools.exec_command(...))`로 출력한다.
정상 작업이 약 7분 24초 걸린 사례가 있으므로 짧은 대기 timeout만으로 실패로 단정하지 않는다.
서브 에이전트 결과는 주 작업이 diff와 테스트로 검증한다. 이번에는 주 작업이 기존
구현을 통합하고, MiMo에 소유 파일이 분리된 테스트 작성 및 독립 검토를 맡겼다.
설정 변경이나 앱 재시작은 하지 않았다. 재사용법은 Yeoul `fact_006576`에 저장되어 있다.

## 최종 후보 기능 검증 (2026-09-10)

집중 검사에서 설정 캐시 hit의 복사·할당 0, 동시 miss 단일 생성, 설정 ID 전환,
토큰·TLS·연결 풀 격리, 인증 허용·거부 행렬을 확인했다. 실제 QUIC 테스트에서
같은 source hint/durable 혼합의 단일 fetch, 소유자·대기자 취소 격리,
다른 source의 병행 진행, control/terminal 페이지의 WAL 재시작 복구를 확인했다.
Core 테스트는 sync 실패 후 재시도, hint 여러 슬롯의 단일 sync, 동시에 복구 base를
설치했을 때 floor 이하 상태를 다시 만들지 않는 동작을 확인했다.

- `CGO_ENABLED=0 go test ./...`: 통과.
- `go test -race ./...`: 통과.
- `go vet ./...`: 통과.
- `CGO_ENABLED=0 go build ./cmd/rhiza ./cmd/rhiza-operator`: 통과.

기존 미커밋 변경을 포함한 최적화 전 snapshot과 파일 해시를 비교했다.
`docs/automatic-recovery-design.md` 및 이번 최적화 범위 밖의 기존 파일은 그대로다.
기능 검증 시점에는 커밋·푸시·릴리즈를 하지 않았다. 이후 CI 측정용 임시 브랜치 게시만
별도로 승인받았다. Node·Operator·no-PVC 연동은 여전히 미완성이고 릴리즈하지 않았다.

## CI 성능 측정

로컬에서 다른 작업의 테스트와 겹친 샘플은 폐기했다. 아래 CI 결과만 성능 판정에
사용한다. 기능 테스트의 무할당 캐시 hit와 단일 fetch 확인은 별도의 동작 검증 결과다.

기존 `Performance` workflow에 `network_optimization` 선택 입력을 추가했다.
`none`은 기존 전체 측정을 유지하고, 아래 모드는 해당 네트워크 벤치마크만 실행한다.
환경은 `ubuntu-24.04`, `CGO_ENABLED=0`, `GOMAXPROCS=2`, revision별 10회,
회당 1초이며 base/candidate 실행 순서를 교대로 바꾼다. 동일 benchmark fixture를
양쪽 revision에 사용하고, 양쪽 모두 benchmark가 존재하고 성공해야 한다.
측정 중 기능 테스트는 실행하지 않으며 `benchstat`, 원본 출력, runner 환경을 artifact로 남긴다.

| 순서 | CI 선택 | 비교 revision | 측정 경로 |
| --- | --- | --- | --- |
| 2 | `transport` | 기존 미커밋 구현+1번 → +2번 | warmed 실제 QUIC ReadTip, 시간/B/op/allocs/op |
| 3 | `auth` | +2번 → +3번 | 3·16명 설정의 PeerServer 인증+빈 Sync 응답, 시간/B/op/allocs/op |
| 4 | `catchup` | +3번 → +4번 | 실제 QUIC, 8슬롯 지연 follower, 동일 source 동시 2요청, 응답당 의도적 5ms 지연 |

catch-up은 시간/B/op/allocs/op 외에 fetch 수, 받은 decision 후보 수,
고유 설치 슬롯 수를 기록한다. `unique_installed/op`는 복구 후 Core의 실제 prefix에서
계수하며, 중복 후보 수와 구분한다. 초기 연결은 warm-up하고 follower/WAL 생성·종료는
측정 시간에서 제외한다. 이 통제된 지연 실험을 일반적인 QUIC 처리량으로 확대하지 않는다.

실행 형식 (SHA는 원격에 게시된 각 단계의 정확한 commit ID로 지정):

```sh
gh workflow run performance.yml --ref CI_BRANCH \
  -f base_ref=BEFORE_SHA -f candidate_ref=AFTER_SHA \
  -f network_optimization=transport
```

동일 workflow ref의 동시 실행은 기존 concurrency 정책으로 취소될 수 있으므로
2번 완료와 artifact 수집 후 3번(`auth`), 4번(`catchup`)을 순차 실행한다.
사용자 승인 후 `feature/quorum-membership-perf-ci`에 기준·2·3·4 스냅샷을
커밋·푸시했다. 기존 작업 브랜치와 작업 디렉터리의 미커밋 상태는 유지한다.
기준 `e63b66e` → transport `b989d0c` → auth `d6b0eec` → catch-up `72a35ff`이며,
각 단계는 바로 전 단계와 비교한다. CI 설정만 바꾸는 후속 commit은 측정 대상 Go 코드를 바꾸지 않는다.

CI 변경의 `bash -n`, `shellcheck`, `actionlint` 검사는 통과했다.
세 단계 결과와 원본 artifact를 아래에 기록했다. 전체 CI 결과는 별도로 확인한다.

### 단계별 CI 측정 결과 (2026-09-10)

아래는 각 revision 10회 중앙값이다. 각 행의 전후는 같은 runner에서 교대로 측정했다.
transport·catch-up runner는 AMD EPYC 9V74, auth runner는 AMD EPYC 7763이었다.
모두 Go 1.27.0 / linux amd64 / Ubuntu 24.04다. 서로 다른 단계의 수치를 합산해
전체 성능 개선율로 해석하지 않는다.

| 경로 | 시간/op 전 → 후 | 시간 변화 | B/op 전 → 후 | allocs/op 전 → 후 |
| --- | ---: | ---: | ---: | ---: |
| 2. 실제 QUIC ReadTip | 113.0 → 112.8 µs | 유의차 없음 (p=0.971) | 7,946.5 → 7,370 (−7.25%) | 92 → 90 |
| 3. 인증, 3명 | 444.2 → 313.4 ns | −29.45% | 720 → 720 | 3 → 3 |
| 3. 인증, 16명 | 3,347 → 885.7 ns | −73.54% | 12,000 → 3,728 (−68.93%) | 9 → 3 |
| 4. 동일 source catch-up | 5.835 → 5.680 ms | −2.65% | 212,005.5 → 108,199 (−48.96%) | 793 → 430 |

인증과 catch-up 시간 차이는 `benchstat p<0.001`이다. 3명 인증의 할당량·횟수는
변하지 않았으므로 임시 맵 제거가 모든 설정 크기에서 힙 할당 감소를 뜻하지는 않는다.
catch-up의 성공한 두 호출 한 쌍당 fetch는 **2→1회**, 받은 decision 후보는
**16→8개**, 고유 설치 슬롯은 **8→8개**였다. 줄인 것은 중복 전송·후보 처리이며,
필요한 prefix 설치량을 줄인 것이 아니다. 시간 개선 2.65%는 의도적 5ms 지연을 둔
이 실험 조건에 한정한다. tail latency나 일반 SQL/Kubernetes 처리량은 측정하지 않았다.

- [transport CI 성공](https://github.com/mrchypark/rhiza/actions/runs/34375805234), [benchstat](../benchmarks/results/2026-09-10-network-optimization/transport/benchstat.txt).
- [auth CI 성공](https://github.com/mrchypark/rhiza/actions/runs/34376322012), [benchstat](../benchmarks/results/2026-09-10-network-optimization/auth/benchstat.txt).
- [catch-up CI 성공](https://github.com/mrchypark/rhiza/actions/runs/34376555468), [benchstat](../benchmarks/results/2026-09-10-network-optimization/catchup/benchstat.txt).

각 결과 디렉터리에 `base.txt`, `candidate.txt`, `environment.txt`도 보존했다.
최초 [전체 CI](https://github.com/mrchypark/rhiza/actions/runs/34376849217)는 측정 종료 후
실행했다. 일반 Go 테스트·vet·빌드·실제 서버 E2E, 양 플랫폼 Rust, 컨테이너 빌드는
통과했지만 race 단계의 `TestQuorumMembershipQUIC`에서 UDP `address already in use`로
실패했다. 데이터 race 경고가 아니라 테스트의 예약 소켓 close/rebind 경쟁이었다.

`809d59e`에서 예약한 UDP 소켓을 `quic.Transport`가 계속 보유하고 listener만
재시작하도록 수정했다. `StartPeerServerOnTransport`는 기존 인증·TLS 검사를 공통으로
사용하며, 호출자가 transport/socket을 소유한다. 포트가 listener 종료 사이에도
점유되어 있고 재시작 후 실제 QUIC 요청이 성공하는 회귀 검사를 추가했다.
관련 두 테스트의 로컬 `go test -race`가 통과했다. 측정 대상 handler·캐시·catch-up
코드는 바뀌지 않았으며 위 성능 수치는 표시된 원래 revision 비교 결과다.
[수정 후보 전체 CI](https://github.com/mrchypark/rhiza/actions/runs/34378032446)가 통과했다.
검증한 commit은 `809d59e`다. Go 일반 테스트·race·vet·CLI/Operator 빌드·실제 서버
E2E·취약점 검사, Linux/macOS Rust SDK 검사와 패키징, 두 컨테이너 빌드가 모두
성공했다. [CI 결과 메타데이터](../benchmarks/results/2026-09-10-network-optimization/validation.json)를 보존했다.
최종 문서·측정 artifact 커밋은 검증된 Go 코드를 변경하지 않는다. `git diff --check`도 통과했다.
