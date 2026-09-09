# 성능 최적화 후속 작업

기준일: 2026-09-10. 브랜치: `feature/quorum-membership`.
현재 로컬의 미커밋 멤버십 구현을 기준으로 한다. 아래 완료 표시는 머지·릴리즈를 뜻하지 않는다.

## 현재 상태

| 항목 | 상태 | 확인된 결과 |
| --- | --- | --- |
| 코어 내부 설정 조회의 멤버 목록 복사 제거 | 완료 | 공개 API는 복사 유지. 작업당 할당량 7.4% 감소 |
| 1. WAL 기록 전 인증서 중복 해석 제거 | 완료 | 작업당 할당량 54,663 → 42,971 bytes, 21.4% 감소. 할당 횟수 299 → 240 |
| 2. 네트워크 설정 캐시 조회 개선 | 미구현 | 캐시 조회 전에 전체 멤버 목록을 복사하는 경로 확인 |
| 3. RPC 인증용 임시 멤버 맵 제거 | 미구현 | 요청마다 과거·현재 멤버 맵을 생성하는 경로 확인 |
| 4. 중복 catch-up 합치기 | 미구현 | 같은 source의 동시 호출이 동일 페이지를 요청할 수 있음. 실제 중복량 측정 필요 |

1번의 시간 중앙값은 14.18 → 14.55ms로, 속도 개선은 확인되지 않았다.
수치는 Apple M3 / Go 1.27, 인프로세스 3-peer transport와 로컬 WAL,
1,000회 작업 × 3회 측정 결과다. 실제 QUIC·SQL·Kubernetes 성능을 대표하지 않는다.
이전 측정과 상세 제약은 [멤버십 문서](quorum-membership.md)의 성능 항목을 참고한다.
1번 적용 후 전체 Go 테스트, race, vet, CLI·Operator 빌드가 통과했다.

## 2. 네트워크 설정 캐시 조회 개선 — 다음 작업

대상: `pkg/network/transport.go`의 `transportForSlot`, `transportForCurrent`, `transportFor`.
현재 공개 `Core.ClusterForSlot`로 멤버 목록을 복사한 뒤 ConfigID 캐시를 조회한다.

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
`config.MemberSet()[sender]`와 `current.MemberSet()[sender]`는 각각 단일 멤버를
찾으려고 전체 맵을 만든다. 기존 조회 helper가 있으면 재사용하고, 없으면
멤버 slice에서 직접 찾는 작은 helper로 대체한다.

과거 설정과 현재 설정의 검증, 상수 시간 토큰 비교, 제거된 voter 차단,
learner의 관리자 권한 예외, ConfigID 검증을 그대로 유지한다.
Core 인증 스냅샷 API 추가 같은 큰 변경은 이 단계에 포함하지 않는다.

검증: 정상 voter, 제거된 voter, 잘못된 토큰, 과거 슬롯, learner 관리자 요청의
허용·거부 결과가 동일해야 한다. 인증 경로 벤치마크에서 할당 감소를 확인한다.

## 4. 중복 catch-up 합치기

대상: `pkg/network/server.go`의 `catchUpFrom`과 Record 호출 경로.
현재 전역 동시 실행 제한 2개는 같은 source의 중복 fetch를 막지 못한다.

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

## 병렬 실행 장애 인계

Mimo 서브 에이전트는 실행 전에 `unreadable_encrypted_agent_task`로 실패했다.
`ocx v2 status`는 v1, `~/.codex/config.toml`의 `[features].multi_agent_v2`는
false였고, 앱 재시작 후 새 에이전트에서도 같은 오류가 발생했다.
따라서 2~4번은 에이전트가 수정하지 않았다. 새 대화에서의 재현 여부는 미확인이다.
설정 재변경이나 재시작만으로 해결된다고 가정하지 않는다.
