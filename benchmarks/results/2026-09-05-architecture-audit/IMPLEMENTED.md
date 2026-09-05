# 성능 개선 적용 및 검증 — 2026-09-05

기준 커밋: `f6c01b19ea6957a4dbdfd5636a7fdb5ffaad2c35`. 이 문서는 그 위의 작업 트리 변경을 설명한다. 기존 `README.md` 감사 결과는 변경 전 기록이다.

## 기본 빌드에 적용한 변경

- **유휴 복제본 비용:** 성공적으로 검증한 아카이브 head의 버전, 적용 위치, compaction floor가 그대로면 HEAD 확인만 수행한다. 새 suffix·체크포인트·재시작은 기존 인증 및 pin 복구를 거친다. 원격 조회 실패를 캐시로 숨기지 않는다.
- **핀 수명:** 프로세스 내 성공한 복구 작업은 동일 owner를 재사용한다. 프로세스마다 owner는 다르며, pin 해제 실패 시 오류를 반환하고 owner와 no-op 상태를 폐기한다. 기존 CAS·토큰·GC 보호를 유지한다.
- **읽기 과부하 제어:** SQL·KV·Graph 임베디드 API와 HTTP가 인스턴스별 admission을 공유한다. 기본 동시 읽기 64개, 그중 기다리는 stream 읽기는 최대 8개다. 초과하면 `ErrOverloaded`/HTTP 503을 반환한다. HTTP는 응답 전송이 끝날 때까지 슬롯을 유지한다.
- **설정 및 API:** `Config`와 `ReplicaConfig`의 `MaxConcurrentReads`/`MaxLongPollReads`, CLI의 `RHIZA_MAX_CONCURRENT_READS`/`RHIZA_MAX_LONG_POLL_READS`를 연결했다. 둘 다 0은 기본값, 명시적 total과 long-poll 0은 대기 읽기 비활성화다. `ReadReplica.GraphReachable`도 제공한다.
- **오브젝트 지표:** 공급자 공통 HTTP 요청·실패 및 메서드별 계수를 노출하며 기존 S3 필드는 alias로 유지한다. SDK 재시도 `attempt=10` 파싱을 수정했다. 로컬 bucket 읽기 실패와 실제 HTTP body 실패를 구분하고, 한 요청의 HTTP 실패를 중복 집계하지 않는다.

## 측정 결과

Apple M3 / Go 1.27.0 / 로컬 filesystem 기준. 아래 호출 수는 bucket 경계의 논리 연산이며 클라우드 청구량 실측이 아니다.

| 유휴 복제본 동기화 1회 | 변경 전 | 변경 후 |
|---|---:|---:|
| HEAD | 8 | 1 |
| GET | 5 | 0 |
| PUT | 4 | 0 |
| 총 연산 | 17 | 1 (94.1% 감소) |
| B/op 중앙값 | 178,711 | 2,338 |
| allocs/op 중앙값 | 376 | 16 |

`BenchmarkReadReplicaIdleSync`, 200회 × 3회 측정. 변경 전은 동일 테스트를 Go overlay로 기준 커밋의 `replica.go`에 연결했다. 실제 시각 지연은 filesystem 동기 쓰기와 동시 작업 영향을 크게 받아 클라우드 레이턴시 개선율로 해석하지 않는다. 원시 기록은 `replica-idle-before.txt`, `replica-idle-after.txt`에 있다.

`BenchmarkHTTPQueryLoopback`은 실제 loopback TCP, keep-alive, SQL `SELECT 1`, 응답 전체 읽기, GOMAXPROCS=4로 실행했다. 3회 결과는 58,070 / 99,990 / 106,939 ns/op, 약 10.7KB/op, 150 allocs/op이다. 이는 병렬 처리의 요청당 평균 비용이며 p95/p99 또는 운영 서버 최대 성능이 아니다. 변경 전 HTTP 비교는 수행하지 않았다.

## 별도 검증한 Graph 의존성 패치

**`latticedb-cow.patch`는 기본 `go.mod`에 적용되지 않았다.** 기본 빌드의 LatticeDB v0.3.0은 그대로다. 최신 버전으로 단순 교체해도 전체 metadata map 복사 문제가 해결되지 않아, v0.3.0 기반의 shard별 copy-on-write 패치를 격리된 모듈에서 검증했다. 공개 API와 snapshot/WAL 형식은 유지한다.

Rhiza의 기존 `BenchmarkGraphApply`를 동일 1,000건 × 3회, GOMAXPROCS=4로 비교했다. 무관한 테스트와 겹친 최초 측정은 `graph-fixed-before-contended.txt`로 분리하고, 비교는 다른 검증이 종료된 뒤 순서대로 다시 실행했다.

| Graph 적용 중앙값 | 기존 v0.3.0 | 별도 COW 패치 |
|---|---:|---:|
| ns/op | 240,561 | 148,152 |
| B/op | 132,303 | 62,550 |
| allocs/op | 291 | 296 |

할당 바이트는 약 52.7% 줄었지만 할당 **횟수**는 소폭 늘었다. 이 결과는 고정 1,000건 로컬 Graph workload에 한정된다. 패치는 별도 upstream 버전으로 배포·채택하기 전까지 운영 성능에 반영되지 않는다. 상세 근거는 `latticedb-cow-evidence.md`, 원시 기록은 `graph-fixed-before.txt`와 `graph-fixed-cow.txt`에 있다.

## 검증과 남은 범위

- 기본 의존성: `go test ./...`, `go vet ./...` 통과.
- 동시성: root·network·objstore·recovery·node의 `go test -race ... -count=1` 통과.
- 복제본 회귀: 유휴 호출 수, 원격 조회 오류, suffix-only 갱신, 새 체크포인트, 재시작, archive/checkpoint pin 해제 실패 후 재시도 통과.
- HTTP 회귀: saturation 503, 취소, 롱폴 용량 분리, 느린 응답 전송 중 슬롯 유지 및 반환 통과.
- 별도 COW 모듈을 alternate modfile로 연결한 Rhiza Graph·체크포인트 복구 테스트 통과. 기본 modfile·의존성 캐시는 수정하지 않았다.

읽기 제한은 동시 실행 수를 제한한다. JSON 디코딩 전 유입량, 전체 RSS, 임베디드 호출자가 반환 결과를 보관하는 메모리까지 제한하지는 않는다. 합산 바이트 예산, request-ID 대기 취소, 대규모 다중 노드·실제 클라우드·장시간 HTTP tail latency 측정은 기존 감사 계획의 후속 범위로 남아 있다. 이번 변경은 합의·ACK·durability·idempotency 보장을 완화하지 않는다.
