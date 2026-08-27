# Rhiza P0/P1 재검토 구현 및 전체 벤치마크

## 판정

첨부 리뷰의 버저닝·마이그레이션 권고를 제외한 P0/P1 항목을 구현했다. 버저닝은 사용자 정책에 따라 추가하지 않았다. Go 1.27.0과 `arenas,greenteagc` 조합에서 SQL/Graph 일반 테스트, race detector, vet가 모두 통과했다. Dory K3s의 3-peer SQL·Graph 10개 프로필에서 총 126,000개 요청을 실행했고 최종 client error는 0건이다.

장애 pod가 adaptive leader epoch 경계를 포함한 하나의 archive page를 복구할 때, 페이지 전체를 현재 상태로 선검증하여 뒤쪽 결정을 검증하는 데 필요한 앞쪽 leader schedule을 볼 수 없는 결함도 측정 중 발견해 수정했다. 인증서 binding과 compact-floor preflight는 페이지 전체에 대해 먼저 수행하고, leader schedule 의존 검증은 결정 적용 순서대로 수행한다. 160개 proposal을 한 페이지로 복구하는 회귀 테스트와 SQL·Graph 실제 Chaos Mesh pod failure 복구를 모두 통과했다.

## 구현 범위

- Graph request ID를 전역 64-byte 경계에서 consensus 전에 거부하고 내부 인덱스 길이를 varint로 전환했다.
- checkpoint candidate와 certified `CURRENT`를 분리하고, seal 뒤 CAS promotion과 stale writer 방지를 추가했다.
- KV/Notify를 SQL/Graph와 같은 전역 mutation admission에 포함하고 Server가 consensus → apply → durability operation의 context와 lifecycle을 소유하게 했다.
- WAL recovery를 streaming scan으로 바꾸고 archive는 extent ref + bounded lazy cache로 전환했다.
- verified checkpoint block cache와 bounded streaming verifier/downloader를 추가했다.
- Graph/SQL checkpoint가 전용 DB snapshot 경로를 사용하도록 바꾸고, Graph의 global stop-the-world 구간을 tip 확인으로 축소했다.
- checkpoint 생성자를 첫 configured member로 고정했다. service consensus는 계속 leaderless다.
- object store가 없는 single-node는 WAL 4 GiB 상한을 적용하고, multi-node는 shared object store 없이 시작하지 않는다.
- KV TTL을 mutation당 최대 256개씩 물리 정리하고 SQL 결과는 최종 JSON encoded byte 상한으로 검사한다.
- S3 logical operation, HTTP request, conflict/dedup, retry, transport, 4xx, 5xx 지표를 분리했다.
- 업로드 reader가 `io.SectionReader.Size`를 전달하도록 해 MinIO unknown-size multipart 64 MiB buffer 증폭을 제거했다.

## E2E 조건

- Apple M3 / Dory 0.4.5 / K3s 1.36.2 / MinIO / 3 peers
- read sample: 1,000 requests, write sample: 100 requests, concurrency 16, 각 workload 3회
- durability: `async`, `before-ack`; topology: healthy, one-fault; checkpoint stress: 1초 interval
- local read와 linearizable read를 SQL/Graph 및 공통 KV에서 각각 측정
- `before-ack`은 publication을 ACK 전 완료하며, `async` sync interval은 1분

## 최종 처리량과 latency

아래 값은 세 반복의 median throughput/p50이고 p99도 같은 방식으로 집계했다.

| Profile | Primary local read ops/s · p50 | Primary linearizable read ops/s · p50 | Primary write ops/s · p50 | KV write ops/s · p50 |
|---|---:|---:|---:|---:|
| Graph async healthy | 2,526 · 5.15 ms | 2,012 · 6.86 ms | 391 · 33.78 ms | 105 · 125.77 ms |
| Graph async 1 fault | 6,866 · 1.98 ms | 2,498 · 5.55 ms | 636 · 20.58 ms | 363 · 37.29 ms |
| Graph before-ack healthy | 5,675 · 2.34 ms | 3,201 · 4.16 ms | 449 · 29.78 ms | 255 · 54.11 ms |
| Graph before-ack 1 fault | 5,706 · 2.23 ms | 4,502 · 3.03 ms | 338 · 40.86 ms | 173 · 85.58 ms |
| SQL async healthy | 6,456 · 2.10 ms | 3,264 · 3.22 ms | 835 · 16.90 ms | 393 · 33.09 ms |
| SQL async 1 fault | 7,297 · 1.79 ms | 2,931 · 3.66 ms | 548 · 22.84 ms | 290 · 33.24 ms |
| SQL before-ack healthy | 3,482 · 3.57 ms | 1,422 · 6.95 ms | 150 · 84.69 ms | 132 · 71.23 ms |
| SQL before-ack 1 fault | 6,266 · 2.15 ms | 2,283 · 5.84 ms | 399 · 38.51 ms | 176 · 83.88 ms |

한 번의 fault run이 healthy보다 빠른 항목은 결함 개선을 뜻하지 않는다. 각 프로필이 독립적으로 pod를 재생성하고, 3회 반복도 같은 deployment 안에서 수행하는 closed-loop micro workload이므로 scheduler, catch-up, adaptive epoch 위치의 영향이 크다. production SLO나 fault speedup 근거로 사용하면 안 된다.

## 이전 구현과 비교

이전 구현은 `6329620` 작업 트리의 저장된 Dory 결과다. 주요 throughput 변화는 다음과 같다.

| Path | 변화 |
|---|---:|
| Graph checkpoint local / linearizable / write | +171% / +64% / +28% |
| Graph before-ack local / linearizable / write | +45% / +1% / +50% |
| Graph before-ack 1-fault write | +40% |
| SQL async local / linearizable / write | +153% / -8% / +11% |
| SQL checkpoint local / linearizable / write | +34% / +3% / +38% |
| SQL before-ack local / linearizable / write | -26% / -46% / -40% |
| Graph async healthy local / linearizable / write | -51% / -48% / -34% |

Checkpoint와 Graph before-ack은 개선됐지만 Graph async healthy와 SQL before-ack은 명확한 재측정 대상이다. 현재 결과만으로 원인을 특정하거나 회귀가 제거됐다고 주장하지 않는다. 다음 성능 gate는 pod별 역할을 고정하고 독립 deployment 반복, open-loop arrival, HDR histogram으로 수행해야 한다.

## CPU와 memory

CPU는 workload window 동안 cgroup `usage_usec` 증가량 합계, memory는 pod별 `memory.peak` 합계다.

| Profile | 측정 CPU nodes | CPU seconds | peak sum MiB |
|---|---:|---:|---:|
| Graph checkpoint 1s | 3 | 12.95 | 146.2 |
| Graph async healthy | 2 | 19.74 | 191.9 |
| Graph async 1 fault | 2 | 10.94 | 147.5 |
| Graph before-ack healthy | 3 | 9.85 | 140.5 |
| Graph before-ack 1 fault | 2 | 10.90 | 138.5 |
| SQL checkpoint 1s | 3 | 11.40 | 140.7 |
| SQL async healthy | 3 | 9.34 | 137.4 |
| SQL async 1 fault | 2 | 10.60 | 151.8 |
| SQL before-ack healthy | 3 | 14.92 | 145.5 |
| SQL before-ack 1 fault | 2 | 9.83 | 134.9 |

fault profile의 CPU node 수 2는 한 pod가 Chaos Mesh로 정지되어 정상이다. Graph async healthy는 한 pod의 before snapshot 획득 실패로 CPU가 2-node만 측정됐으므로 cluster CPU 비교에 사용하면 안 된다. Graph checkpoint leader peak는 unknown-size multipart 수정 전 약 739 MiB에서 최종 pod별 최대 약 58 MiB, cluster peak 합계 146.2 MiB로 내려갔다.

동일 contacted pod 기준 이전 대비 checkpoint CPU/peak memory는 Graph -8.0%/-38.3%, SQL -8.8%/-40.7%다. Graph before-ack은 -12.1%/-28.1%, SQL before-ack은 +40.1%/-17.5%였다. Graph async의 CPU +139%는 catch-up timeout 로그와 측정 누락이 겹쳐 독립 재현이 필요하다.

## Object storage API와 bytes

아래는 각 18-sample profile 전체의 logical SDK operation과 실제 S3 HTTP request 합계다.

| Profile | PUT | GET | HEAD | LIST | S3 HTTP | failures | uploaded | downloaded |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Graph checkpoint 1s | 39 | 66 | 17 | 0 | 122 | 0 | 1.93 MiB | 3.78 MiB |
| SQL checkpoint 1s | 40 | 63 | 21 | 0 | 124 | 0 | 1.09 MiB | 1.60 MiB |
| Graph before-ack healthy | 276 | 138 | 138 | 0 | 552 | 0 | 0.87 MiB | 0.03 MiB |
| Graph before-ack 1 fault | 278 | 139 | 139 | 0 | 556 | 0 | 0.91 MiB | 0.03 MiB |
| SQL before-ack healthy | 334 | 167 | 167 | 0 | 668 | 0 | 1.10 MiB | 0.04 MiB |
| SQL before-ack 1 fault | 284 | 344 | 143 | 0 | 771 | 1 | 0.85 MiB | 1.17 MiB |
| async healthy | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

두 async one-fault profile에서 fresh prefix의 absent recovery metadata를 확인하는 GET/HEAD가 각각 한 번씩 404였고 LIST 한 번을 포함해 S3 HTTP 3회, 4xx 2회가 기록됐다. SQL before-ack fault 복구 중에도 추가 404 1건이 있었지만 client error, retry, transport failure, 5xx는 없었다. 현재 지표는 정상 초기화 not-found도 `http_4xx_unexpected`로 분류하므로 운영 alarm에는 바로 쓰지 말고 not-found semantics를 별도 분리해야 한다.

## Go microbench

순차 실행 5회의 median이다.

| Benchmark | time/op | bytes/op | allocs/op |
|---|---:|---:|---:|
| QuePaxa propose, 3 peers | 24.57 ms | 63.6 KiB | 378 |
| QuePaxa propose, 1 peer down | 20.90 ms | 48.1 KiB | 290 |
| ReadIndex, 3 peers | 1.66 µs | 400 B | 6 |
| ReadIndex, 1 peer down | 1.64 µs | 416 B | 7 |
| Local tip read | 16.94 ns | 0 | 0 |
| SQL checkpoint file snapshot | 0.78 ms | 9.2 KiB | 120 |
| Graph apply | 0.35 ms | 356.6 KiB | 459 |

이 microbench transport는 in-process test transport이고 fsync·QUIC·MinIO latency를 대표하지 않는다. 실제 사용자 경로 판단에는 위 K3s E2E 결과를 사용한다.

## 검증과 남은 한계

- PASS: `GOEXPERIMENT=arenas,greenteagc go test ./...`
- PASS: `GOEXPERIMENT=arenas,greenteagc go test -tags graph ./...`
- PASS: 양쪽 `go vet`와 `go test -race`
- PASS: SQL·Graph async/before-ack one-peer Chaos Mesh fault 중 50,400요청, client error 0
- PASS: 양쪽 fault 해제 후 3/3 Ready, shared archive recovery, adaptive leader epoch archive page regression
- 확인 필요: Graph async healthy CPU 측정 누락과 throughput 변동
- 확인 필요: SQL before-ack throughput/CPU 회귀
- 범위 밖: 사용자가 금지한 data/wire format versioning, legacy migration, mixed-version compatibility

원시 NDJSON, cgroup snapshot, Go benchmark output, 집계 JSON/CSV와 이전 비교는 이 디렉터리에 함께 보존한다.
