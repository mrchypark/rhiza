# LatticeDB v0.5.0 업데이트 평가

Rhiza의 LatticeDB 의존성을 v0.3.0에서 v0.5.0으로 갱신했다. 전이 의존성 변경은 없다. 하위 호환을 위한 분기나 이전 버전 fallback은 추가하지 않았다.

현재 측정에서는 성능 개선을 확인하지 못했다. 그래프 읽기와 큰 데이터의 스냅샷 작업에서 지연 증가가 관측됐다. 정확성 수정 후 일반 테스트, race, vet, 빌드 및 최종 10회 비교는 통과했다. 이 결과를 전체 서비스 처리량이나 운영환경 성능 보장으로 해석하면 안 된다.

## 변경과 검증

- 일시적인 LatticeDB checkpoint WAL backpressure와 writer lock 충돌을 영구 `rejected` 영수증으로 기록하지 않고 apply 오류로 반환한다. 기존 로그/요청 ID를 통한 재적용 경로를 사용한다. Query resource limit 등 영구 거절은 유지한다.
- 공통 스냅샷 시작 경로가 내부 writer lock 충돌만 1ms 간격으로 재시도하고 호출자의 context 취소를 따른다. 실제 DB writer를 보유한 상태에서 deadline 반환, writer 해제 후 snapshot 성공을 검사한다.
- 벤치마크의 DB 크기 검증은 변경되는 WAL 디렉터리 대신 immutable backup 파일을 사용한다. 초기 실패 원인과 제외 데이터는 [failures.md](failures.md)에 남겼다.
- `CGO_ENABLED=0 go test ./...`, `go vet ./...`, `CGO_ENABLED=1 go test -race ./...`, 서버 빌드 통과. 생산 코드 최종 수정 후 일반/race/vet/build를 재실행했다.
- 실제 HTTP 서버의 `TestSQLServer` 통과. 이 E2E는 스냅샷 lock 재시도 수정 전에 실행했고, 이후 해당 변경은 전체 일반/race 테스트와 스냅샷 벤치마크로 검증했다.
- govulncheck: 호출되는 취약점 0개. 요구 모듈에 호출되지 않는 취약점 4개가 보고됐다. [원문](vuln.txt).
- 백그라운드 WAL backpressure의 실제 발생 시점을 외부 공개 API로 고정할 수 없어 해당 오류 분류는 단위 테스트로 검증했다. 영수증 미생성/재시도를 그 오류 상황에서 직접 재현한 통합 테스트는 없다.

## 동일 조건 10회 비교

동일한 최종 Rhiza 소스와 벤치마크에서 의존성만 바꾼 두 바이너리를 사용했다. Apple M3, Go 1.27.0, darwin/arm64, CGO=0, GOMAXPROCS=2. 실행 순서를 매 회 반대로 바꿨다. 측정 중 자체 빌드/테스트는 함께 실행하지 않았으나 Dory VM 등 다른 사용자 프로세스와 OS 작업은 유지됐다.

- GraphApply: 매 표본 100개 요청을 빈 DB에 순차 적용. 큰 그래프의 steady-state 쓰기 성능은 아니다.
- Metadata: 4,096개 고정 키 cohort에서 한 키 갱신, 100회. Rhiza 내부 metadata도 존재한다.
- GraphQuery: setup에서 단일 native transaction으로 4,096개 노드 생성 후, 인덱스 없는 property-match 단건 읽기 100회. 매번 반환값 검증.
- Snapshot: 1/16/64MiB metadata payload에서 dirty update를 timer 밖에서 수행하고 snapshot 시작/종료를 10회 측정. immutable backup 크기는 약 1.334/21.33/85.33MiB다. snapshot 파일 다운로드/업로드 시간은 제외한다.
- HTTP: loopback HTTP SQL `SELECT 1`, 병렬 2, 1,000회. LatticeDB를 직접 읽는 경로가 아닌 비교용 지표다.

| 작업 | v0.3 중앙값 | v0.5 중앙값 | benchstat 판정 |
| --- | ---: | ---: | --- |
| Graph Apply | 414.0µs | 423.6µs | 유의한 차이 없음, p=0.739 |
| Metadata 한 키 갱신 | 176.0µs | 196.4µs | 유의한 차이 없음, p=0.436 |
| 4,096 노드 Graph Query | 922.9µs | 965.6µs | +4.63%, p=0.009 |
| Snapshot 1MiB payload | 2.859ms | 2.895ms | 유의한 차이 없음, p=0.579 |
| Snapshot 16MiB payload | 10.51ms | 11.38ms | 유의한 차이 없음, p=0.218 |
| Snapshot 64MiB payload | 34.95ms | 57.88ms | +65.60%, p=0.009 |
| HTTP SQL loopback | 38.42µs | 36.79µs | 유의한 차이 없음, p=0.971 |

이는 각 반복의 평균 operation 시간에 대한 표본 중앙값이며 p95/p99가 아니다. Snapshot 측정 중 background checkpoint가 계속 실행될 수 있다. 그 할당과 CPU 경쟁, writer 대기 비용도 영향을 주므로 +65.60%를 순수 snapshot pin 알고리즘의 비용 증가로 단정하지 않는다. 특히 64MiB v0.5 할당량은 변동이 컸다. 공유 머신의 수치는 회귀 조사 신호이며 별도 부하 없는 서버의 용량 측정이 아니다.

## 메모리와 CPU

- GraphApply B/op: 52.83KiB → 53.47KiB (+1.20%).
- Metadata 갱신: 두 버전 모두 약 387.3KiB/op. v0.5에도 전체 metadata map 복사가 남아 있다.
- GraphQuery: 두 버전 모두 약 1.610MiB/op. +0.01% 수준으로 사실상 같은 할당 크기다.
- `/usr/bin/time -l`로 측정한 graph apply + metadata + query 프로세스의 CPU(user+sys) 중앙값은 0.435s → 0.450s, peak RSS 중앙값은 75.44 → 75.72MiB였다. setup/cleanup도 포함하며 별도 snapshot/HTTP 프로세스는 제외한다. per-request CPU나 장기 서비스 RSS/누수 지표가 아니다. [범위 및 원문 요약](resources-summary.json).
- 오브젝트 스토리지 요청량, 3-voter 처리량, 장애 시 p95/p99, 장기 RSS는 이번 비교에서 측정하지 않았다.

## Upstream에 확인한 문제

- [#157](https://github.com/mrchypark/latticedb-go/issues/157): 애플리케이션 writer 종료 후 내부 checkpoint 때문에 `BeginSnapshot`이 `ErrWriteTxActive`를 반환. LatticeDB 공개 API만 쓰는 재현기에서 20/20회 관측. 데이터 손실 주장은 하지 않았다.
- [#158](https://github.com/mrchypark/latticedb-go/issues/158): AppMetadata full-map copy는 v0.5 신규 회귀가 아니라 남아 있는 병목이다. 독립 공개 API 벤치마크에서 1/4,096/16,384키 상태의 한 키 갱신 할당 중앙값이 3,280 / 396,605 / 1,577,270 B/op였다. [벤치마크와 원문](upstream/metadata-bench.txt).

사용자의 하위 호환 작업 중단 지시 전에 완료한 교차 버전 검증은 `compat/`에 참고 기록으로만 보존했다. 하위 호환 보장이나 릴리스 게이트로 삼지 않는다.

## 재현

macOS에서 저장소 루트의 `bash benchmarks/results/2026-09-05-latticedb-0.5.0/run.sh`를 실행한다. 현재 결과 파일을 덮어쓰므로 기존 결과를 보존하려면 별도 checkout을 사용한다. 기본 모듈은 v0.5.0이고, 임시 alternate modfile만 v0.3.0으로 설정한다. raw 결과는 [030.txt](030.txt), [050.txt](050.txt), 통계는 [benchstat.txt](benchstat.txt), 환경은 [environment.json](environment.json)에 있다.

의존성 릴리스 원문: [LatticeDB v0.5.0](https://github.com/mrchypark/latticedb-go/releases/tag/v0.5.0). 이번 작업에서 Rhiza를 커밋·게시·릴리스하지 않았다.
