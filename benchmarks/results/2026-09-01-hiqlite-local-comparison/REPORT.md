# Hiqlite local benchmark and Rhiza comparison

> Update: Rhiza's async catch-up collapse was fixed and remeasured. The
> three-run median is 141.61 HTTP writes/s with 0/15,000 errors, still 66.0
> times below Hiqlite's 9,340 leader-local writes/s. See
> `../2026-09-01-sql-async-fix/REPORT.md`. The original invalid-result warning
> below is retained as the chronology of the first comparison.

## 결론

Hiqlite의 공식 3노드 벤치를 같은 Apple M3 호스트에서 원본 그대로 세 번
실행했다. 100,000건 단건 INSERT의 중앙값은 **9,340 ops/s**, 트랜잭션 배치는
**398,406 rows/s**였다. 세 실행 모두 정상 종료했고 각 단계의 행 수가
100,000건으로 확인됐다.

현재 Rhiza SQL async의 100-write 중앙값은 **63.1 ops/s**였지만 정상 capacity
수치가 아니다. 후속 5,000-write 진단에서 993 HTTP 503과 peer decision-sync
실패가 발생해 성공 처리량이 14.8 ops/s로 붕괴했다. 따라서 이전의 148.0배
관측 비율을 제품 성능 비교에서 철회한다. async catch-up overload를 해결한 뒤
동일한 장기 조건으로 다시 측정해야 한다.

| Run | Rhiza SQL async write | p50 | errors | foreground S3 calls |
|---:|---:|---:|---:|---:|
| 1 | 27.9 ops/s | 376.955 ms | 0 | 0 |
| 2 | 105.8 ops/s | 110.302 ms | 0 | 0 |
| 3 | 63.1 ops/s | 182.403 ms | 0 | 0 |
| **Median** | **63.1 ops/s** | **182.403 ms** | **0** | **0** |

## Hiqlite 공식 명령 결과

명령:

```sh
cargo run --release -- cluster -c 16 -r 100000
```

대상은 `sebadob/hiqlite` commit
`c3ff2536ac985ecb9f77201d1b58dab66c7b256e` (`hiqlite` crate 0.14.0)이다.
기본 snapshot threshold 10,000과 benchmark 기본 jemalloc 설정을 유지했다.

| Run | 단건 INSERT | 트랜잭션 배치 | cache PUT | fresh single SELECT | peak RSS |
|---:|---:|---:|---:|---:|---:|
| 1 | 9,340 ops/s | 363,636 rows/s | 46,838 ops/s | 236 us | 524 MiB |
| 2 | 8,862 ops/s | 403,225 rows/s | 54,112 ops/s | 132 us | 526 MiB |
| 3 | 10,888 ops/s | 398,406 rows/s | 62,111 ops/s | 221 us | 518 MiB |
| **Median** | **9,340 ops/s** | **398,406 rows/s** | **54,112 ops/s** | **221 us** | **524 MiB** |

첫 실행의 wall time 635.12초에는 clean release 컴파일 약 10분이 포함된다.
공식 프로그램 내부의 timed section만 처리량에 사용했으며, warm-build wall time
중앙값은 36.03초다. cache는 benchmark 설정상 `cache_storage_disk = false`여서
Rhiza의 durable KV 경로와 비교하지 않았다.

## 비교 경계

| 조건 | Hiqlite official bench | Rhiza current qualification |
|---|---|---|
| 배치 형태 | 호스트 프로세스 3개 | 단일-node k3s의 voter pod 3개 + MinIO |
| client 경로 | 리더의 로컬 Rust `Client` | host → port-forward → HTTP server |
| 쓰기 의미 | Raft SQL execute | Raft SQL execute + HTTP; object publish는 background |
| concurrency | 16 | 16 |
| 표본 크기 | 100,000 writes × 3 | 100 writes × 3 |
| snapshot/checkpoint | 10,000 Raft logs | async object sync interval 10분 |

이번 결과로 확정할 수 있는 것은 공식 Hiqlite workload가 이 호스트에서
재현된다는 점까지다. 현재 Rhiza async는 장기 부하에서 peer catch-up overload가
발생하므로 유효한 제품 비교 대상이 아니다. 공정한 비교에는 이 결함을 해결한
뒤 동일한 HTTP 또는 remote-client 경로, 동일한 durability contract,
100,000건 이상의 Rhiza 표본이 필요하다.

## 재현 및 증거

- `environment.json`: commit, 명령, 호스트, toolchain
- `summary.csv`: 세 실행과 중앙값
- `raw/hiqlite-cluster-c16-r100000-run{1,2,3}.txt`: 원본 stdout/stderr와
  `/usr/bin/time -lp` 결과
- Rhiza 기준: `../2026-09-01-latticedb-0.2.1-current/REPORT.md`
- Async 진단: `../2026-09-01-sql-async-diagnosis/REPORT.md`

현재 세션에는 기존 Dory k3s의 `kubectl` context가 없어 Rhiza 100,000건 장기
재측정은 수행하지 않았다. 기존 qualification과 호스트 하드웨어는 동일하다.
