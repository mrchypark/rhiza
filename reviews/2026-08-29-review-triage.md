# Rhiza 전체 리뷰 통합 판정

- 기준: `main@2a118a28294dae48edc0b7af0a54b02230f69fae`
- 외부 원문: `reviews/2026-08-29-chatgpt-pro-main-2a118a2.md`
- 외부 재검토 원문: `reviews/2026-08-29-chatgpt-pro-rereview.md`
- 내부 검토: 합의, 복구, SQL/KV, Graph, 성능, 운영/Kubernetes, 전역 테스트의 7개 읽기 전용 리뷰
- 판정 원칙: 코드와 결정적 테스트로 재현되는 항목만 구현한다.

## 명시적 제외

이번 사이클에서는 사용자 지시에 따라 다음 신뢰 경계 변경을 하지 않는다.

- recovery certificate 서명 및 키 회전
- peer mTLS
- 위 두 변경에 종속되는 0-RTT 인증 구조 변경

## 확인되어 구현한 항목

- WAL compaction은 전체 value map이 아니라 checkpoint 이후 decision과 unresolved recorder가 참조하는 live hash만 rewrite한다.
- SQL/KV/Notify의 동일 request ID·상이한 fingerprint는 첫 결정만 적용하고 후속 결정은 deterministic no-op으로 전진시킨다. 서로 다른 peer의 정상 admission race가 이미 합의된 slot에서 materialization을 영구 정지시키면 안 되며, 호출자는 post-apply fingerprint 확인으로 `request conflict`를 받는다.
- 공개 `KVMutate`는 지원하지 않는 operation과 TTL overflow를 합의 전에 거부한다.
- `Core.CompleteDecision`은 해당 slot까지 연속 prefix가 형성되기 전에 성공하지 않는다.
- 정상 proposal은 별도 StageValue quorum 없이 첫 Record quorum에 value를 inline해 한 peer round trip을 제거한다.
- slow-path proposal priority는 recorder별이 아니라 round당 한 번 생성한다.
- production 3-peer Kubernetes 배포는 `Parallel` pod startup과 not-ready peer DNS publication을 사용한다.
- Graph query/stream payload와 `AppliedSlot`은 같은 LatticeDB read snapshot에서 캡처한다.
- checkpoint root listing과 GC의 remote I/O는 manager mutex 밖에서 실행하고 concurrent root를 병합한다.
- expected S3 conditional response는 HTTP failure로 집계하지 않으며 benchmark transport failure를 정확히 한 번 센다.
- 초기 publication의 missing-object HEAD probe는 논리 not-found로 유지하되 S3 HTTP failure로 오분류하지 않는다.
- archive GC는 marker prefix를 한 번 목록화해 객체별 marker HEAD를 제거하며, 검증된 extent cache는 prefix index로 suffix를 O(1) 검증한다.
- peer의 compacted-history 오류를 typed response로 보존하고, 실행 중 lagging node는 readiness fence와 proposal quiesce 아래에서 sealed checkpoint와 archive suffix로 복구한다.
- 한 peer가 compacted-history를 반환해도 다른 peer의 quorum suffix가 있으면 peer catch-up을 우선하고, 사용 가능한 suffix가 없을 때만 archive 복구로 전환한다.
- Notification은 bounded live at-most-once 계약이다. Graph journal 확인 성공 뒤에만 비동기 enqueue하고, 확인 실패나 포화로 전달하지 못한 건은 drop metric에 포함한다. durable cursor·ack·replay는 제공하지 않는다.

## 선행 검증 결과

- `GOEXPERIMENT=arenas,greenteagc go test ./...`, `go vet ./...`, graph-tag 전체 테스트 통과
- objstore/materializer/network/node/quepaxa/checkpoint/recovery race 테스트 통과
- Dory 실제 3-peer SQL/Graph의 async, before-ack, healthy, one-peer fault, 1초 checkpoint stress 완료
- 합계 78개 workload aggregate에서 client error 0
- 최신 checkpoint stress에서 예상 밖 S3 4xx/5xx 0, `history compacted` 반복 및 `unknown replicated command` 0
- killed Graph peer 재시작 뒤 consensus/materialized slot 396에서 linearizable read 성공
- 상세 결과: `benchmarks/results/2026-08-29-review-fixes/`

## 별도 제품 결정이 필요한 항목

- KV TTL의 node-local wall clock 의미와 linearizable read 계약
- archive publisher 단일화와 takeover timeout
- checkpoint/archive GC의 허용 API budget과 grace policy
- shutdown 뒤 보존된 public Go API handle의 호출 계약

## 최종 재검토 판정

최종 재검토의 13개 항목은 모두 코드에서 재현하거나 상태 경계를 확인했다. 아래 항목은 구현 및 결정적 회귀 테스트 대상이다.

1. 동일 kind·request ID의 교차 ingress 충돌은 첫 결정만 적용하고 후속 결정은 no-op으로 전진시킨다. SQL/KV/Notify/Graph 3-peer 경쟁 테스트로 loser conflict와 후속 write 진행을 검증한다.
2. `RestoreCheckpointBase`는 보유한 out-of-order suffix를 즉시 다시 연결하고 tip waiter와 slot allocator를 갱신한다.
3. live base 설치와 WAL compaction은 checkpoint 이후 unresolved recorder가 참조하는 proposal value를 재시작 뒤에도 보존한다.
4. checkpoint remote root 목록을 authoritative하게 사용하고 publication과 GC를 같은 cross-process CAS maintenance claim으로 직렬화한다.
5. slow path의 모든 Record phase가 proposal value를 전달해 최초 value holder 한 peer를 잃어도 남은 quorum이 진행한다.
6. compacted peer 오류는 foreground에서 503/`ErrNotReady`로 전환하고 single catch-up worker를 깨운다. archive restore 자체는 readiness를 올리지 않고 새 quorum catch-up 성공만 올린다.
7. live recovery는 immutable checkpoint/archive snapshot을 pin하고 단일 download+verify 경로를 사용한다. recovery base가 이동해도 선택한 suffix를 먼저 복원한 뒤 최신 head를 따라간다.
8. `Server.Quiesce` timeout 뒤 기존 proposal drain이 끝나기 전에는 admission을 다시 열지 않는다.
9. SQL/KV payload와 `AppliedSlot`은 동일 SQLite snapshot에서 읽고, 모든 Graph 응답의 `ConsensusTip`은 materializer read 뒤에 관측한다.
10. HTTP 409/412는 conditional PUT일 때만 expected CAS로 분류하고, 정상 missing HEAD 404는 호출자가 context로 명시한다.
11. archive GC는 대상 객체가 사라진 뒤 남은 orphan marker를 제거한다.
12. embedded `HedgeDelay`는 `nil=5ms`, 명시적 `0=eager`, 음수 거부의 브레이킹 계약을 사용한다.
13. WAL compaction은 decided suffix의 payload를 `EntryDecide`와 `EntryProposal`에 중복 보존하지 않고 unresolved recorder hash만 별도로 유지한다.

서명 recovery certificate와 peer mTLS는 사용자 지시로 이번 판정과 차단 조건에서 제외한다. 최종 통합 테스트와 Dory 재측정 결과는 구현 완료 후 이 문서와 `benchmarks/results`에 추가한다.

## 최종 통합 검증

- 위 13개 항목과 내부 재검토에서 확인한 잔여 항목을 모두 구현했다.
- 검증 중 추가로 재현된 checkpoint compaction 중복 실행, archive-base 선행 시 catch-up 반복, peer 재생성 뒤 0-RTT rejection을 각각 single-flight compaction, 즉시 pinned restore, 동일 deadline 내 1-RTT 1회 재전송으로 수정했다.
- 정상 초기 `CURRENT`/publisher object miss만 expected 404로 명시하고 immutable root/block 손실은 계속 unexpected failure로 유지했다.
- `GOEXPERIMENT=arenas,greenteagc go test ./...`, graph tag 전체, `go vet ./...`, 관련 전체 race test와 `git diff --check`가 통과했다.
- Dory 전체 매트릭스 151,200건, 독립 확인 50,400건, 최종 checkpoint 검증 12,600건에서 client error 0이었다.
- 최종 Graph checkpoint 검증은 예상 밖 S3 4xx/5xx, transport/S3 HTTP failure가 모두 0이며, 이전 내부 오류 로그 3종도 재발하지 않았다.
- `emptyDir` peer 삭제 후 object-storage-backed 복구는 SQL 3초(slot 406), Graph 3초(slot 415)에 ready 및 linearizable read까지 완료했다.
- 상세 원시 자료와 비교 보고서: `benchmarks/results/2026-08-29-final-rereview/`

## 20:18 최종 수정본 재검토 중 선제 확인

- 성공한 restore/reopen이 임시 Materializer의 notification dispatcher를 남기던 goroutine·queue 누수를 제거했다. storage ownership을 옮긴 뒤 임시 Materializer를 닫아 worker만 회수한다.
- `DB.Close` 뒤 Graph query와 request receipt/match 경로가 nil graph를 역참조하던 panic을 오류 반환으로 바꾸고, SQL/KV/Notify Apply·Snapshot·Subscribe도 종료 상태를 명시적으로 거부한다.
- restore가 live connection을 닫은 뒤 sidecar·rename·journal·graph install 단계에서 실패하면 `recoverRestore`만 수행하고 reopen하지 않던 P1을 수정했다. 모든 post-close 실패 분기는 원본 파일 복구 후 Materializer를 다시 열며, 기본·Graph 빌드에서 실패 뒤 Health, 다음 Apply, read를 검증한다.
- Graph journal 확인 전 notification이 조기 노출되지 않도록 enqueue를 확인 성공 뒤로 유지한다. 확인 실패는 at-most-once drop으로 계측한다.
- 위 변경 후 기본/Graph 전체 테스트, 관련 race, 양쪽 vet, diff check가 통과했다. Pro의 최종 판정과 최신 Dory 재측정은 아직 진행 중이다.
