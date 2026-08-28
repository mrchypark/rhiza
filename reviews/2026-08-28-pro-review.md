# 2026-08-28 Pro 코드 리뷰 처리 기록

## 기준과 원본

- 대상: `main` commit `256474498d6f00eae131a03c08f5c144347a5c9d`
- 원 대화: <https://chatgpt.com/c/6a866d25-19e4-83e8-8a60-b18c208fa0c6>
- 확보한 원문 SHA-256: `a6d7f9b55ec84634df6e4515166b71a9b29522b7514bde98deab11b2b44c9333`
- 원문은 P0 5건, P1 7건, S3/CPU/memory/latency 및 배포 후속 권고를 포함한다.

이 문서는 원문을 다시 해석한 설계 문서가 아니라, 각 지적을 현재 코드에서 재검증하고 처리한 ledger다.

## 처리 ledger

| ID | 심각도 | 지적 | 판정 및 처리 | 검증 |
|---|---:|---|---|---|
| R-01 | P0 | 새 WAL segment의 parent directory fsync 누락 | 수정. segment를 active set에 넣기 전에 directory sync가 성공해야 한다. 실패 시 생성 파일을 닫고 제거하며 append가 실패한다. | `TestWALRolloverRequiresDirectorySync` |
| R-02 | P0 | checkpoint recovery가 WAL 실패 전에 memory base를 변경 | 수정. certificate와 decision을 검증하고 WAL compact를 먼저 내구화한 뒤 memory base를 교체한다. | `TestRestoreCheckpointBaseReplacesLaggingPrefix`의 capacity failure cut |
| R-03 | P0 | Go 1.27 timer Stop/drain 교착 | 수정. one-shot timer wake 경로의 blocking drain을 제거했다. | `TestGroupCommitWakeTimerRaceMakesProgress`, 전체 network tests |
| R-04 | P0 | replicated SQL multi-statement/comment/PRAGMA 우회 | 수정. leading comment를 건너뛴 첫 keyword 검증, NUL 거부, driver prepare-tail 검증 후 실행을 적용했다. | `TestReplicatedSQLRejectsTailPragmaAndNullByte` |
| R-05 | P0 | 공개 HTTP client 인증 부재 | 제품 범위상 보류. embedded Go API가 기본이고 HTTP는 선택 adapter다. 사용자가 client token과 SSL을 명시적으로 제외했다. 기본 bind는 loopback이며 배포 경계 인증은 별도 승인 없이는 추가하지 않는다. | 범위 결정 |
| R-06 | P1 | Graph offset/trim이 node-local이라면서 checkpoint에 전파 | 수정. offset과 trim을 idempotent `GraphCommand`로 합의·QLog·materializer에 복제한다. direct local mutation API는 제거했다. | graph-tag 전체 tests, root graph API test |
| R-07 | P1 | catch-up이 가장 느린 peer를 page마다 2초 대기 | 수정. quorum 도달 후 10ms grace 동안 더 높은 tip만 수집하고 남은 RPC를 취소한다. | default 전체 tests |
| R-08 | P1 | 긴 Graph query가 global materializer lock을 점유 | 수정. DB와 tip을 캡처한 뒤 global lock을 해제하고 query lifetime은 wait group으로 close와 조정한다. | graph-tag 전체 tests, race gate |
| R-09 | P1 | Graph stream long poll의 25ms polling | 수정. graph apply/trim이 generation channel을 깨우는 event-driven wait로 전환했다. | graph-tag stream tests |
| R-10 | P1 | embedded close가 parent context를 먼저 cancel | 수정. `Node.Shutdown`의 archive sync/checkpoint가 끝난 뒤 child context를 cancel한다. | default 전체 tests |
| R-11 | P1 | local read의 staleness가 응답에서 보이지 않음 | 수정. SQL/KV/Graph/stream/offset 응답에 `applied_slot`과 `consensus_tip`을 반환한다. local read 자체는 의도한 저지연 API로 유지한다. | default 및 graph-tag 전체 tests |
| R-12 | P2 | mutation batch size 확인이 반복 assemble로 O(n²) allocation | 수정. 고정 framing overhead와 running encoded size로 admission을 계산하고 dispatch 때 한 번만 assemble한다. | batcher 및 전체 network tests |
| R-13 | P2 | WAL append마다 모든 segment byte 합산 | 수정. mutex가 보호하는 `totalBytes`를 append/restore/compact에서 증감한다. | qlog capacity/restore/compact tests |
| R-14 | 품질 | typed SQL/Graph decoder가 trailing JSON을 허용 | 수정. 첫 JSON 값 뒤 EOF를 요구한다. | `TestTypedBatchDecodersRejectTrailingData` |
| R-15 | 비용 | archive 2ms linger가 고정 | 수정. embedded `ObjStoreBatchDelay`와 server `RHIZA_OBJSTORE_BATCH_DELAY`를 추가했다. 기본은 2ms이며 cost profile은 5~20ms로 설정할 수 있다. | recovery 및 전체 tests |
| R-16 | 도구 | 실패 요청의 0 latency가 benchmark percentile을 오염 | 수정. 성공 latency와 오류 latency를 별도 집계하고 오류 p50/p95/p99를 출력한다. | build 및 full tests |
| R-17 | 품질 | restore sidecar 삭제 오류 무시 및 concurrent health/restore race | 수정. `-wal`/`-shm` 삭제 실패를 fail-closed하고, health ping 동안 DB lifetime을 materializer read lock으로 보호한다. | materializer tests 및 race tests |
| R-18 | P0 | WAL compaction이 전체 proposal pipeline을 장시간 정지 | 수정. active segment를 manifest로 fence한 뒤 immutable prefix만 잠금 밖에서 재기록하고, 최종 manifest가 compacted prefix와 모든 post-fence tail을 원자적으로 결합한다. 복구는 최고 generation manifest만 사용하고 orphan segment를 무시한다. | `TestWALCompactionRetainsConcurrentTailExactlyOnce`, `TestWALRecoveryIgnoresUncommittedCompactionSegment`, `TestWALFailsClosedOnCorruptHighestManifest` |
| R-19 | P1 | archive JSON allocation과 wire 표현의 비결정성 | 수정. head/extent를 big-endian 고정 폭·length-prefix·CRC32C binary로 교체하고 JSON layout을 거부한다. head generation과 post-CAS stable reread token을 한 상태로 설치한다. | `TestArchiveExtentCodecIsCanonicalAndStrict`, `TestArchivePostCASFailureKeepsLocalHeadAndTokenTogether`, recovery 전체 tests |

## 이미 반영됐거나 현재 설계와 일치한 항목

- SQLite와 LatticeDB의 동일 applied index online snapshot 및 background file copy
- checkpoint block content addressing, seal 검증, shared archive hash chain/CAS
- Graph durable stream publish가 graph mutation과 같은 LatticeDB transaction에 포함됨
- object-store SDK retry를 포함한 실제 HTTP request/bytes/condition-conflict 계측
- request/body/result/inflight/WAL 용량 제한과 backpressure
- local read와 `ReadIndex` 기반 linearizable read 분리

## 이번 변경에서 의도적으로 하지 않은 항목

- client mTLS/token, peer certificate rotation, TLS 기반 workload identity: 사용자의 명시적 제외 범위다.
- experimental GC/arena 이미지 분리: 사용자는 최신 Go와 arenas/greenteagc의 적극 사용을 명시했다.
- post-CAS head reread 제거: 현재 object-store interface는 successful conditional upload의 version/ETag를 반환하지 않는다. 이를 생략하면 concurrent publisher가 전진시킨 head와 local CAS token을 잘못 결합할 수 있다.
- immutable shared value buffer, FlatBuffers direct table access: profile로 copy/allocation 우선순위를 확정한 뒤 적용한다. 소유권을 먼저 바꾸면 use-after-pool과 data race 위험이 더 크다.
- archive compression: canonical binary 효과를 먼저 측정한다. compressor 고정, decompression output/window 상한, content-hash 재현성 계약 없이 wire 변경과 동시에 넣지 않는다.

## 유지해야 할 불변식

1. recorder 성공은 WAL file과 그 새 directory entry가 모두 durable한 뒤에만 가능하다.
2. checkpoint restore 실패는 memory floor/tip/decision/value 상태를 바꾸지 않는다.
3. 하나의 replicated SQL statement는 SQLite prepare tail이 whitespace/comment뿐일 때만 실행된다.
4. Graph stream offset과 trim은 일반 graph mutation과 같은 consensus, idempotency, archive, checkpoint 경로를 탄다.
5. local read 응답은 관측한 applied slot과 해당 노드의 consensus tip을 함께 제공한다.
6. archive batching 설정은 acknowledgement durability를 바꾸지 않고 publication linger만 바꾼다.
7. compaction manifest가 durable해지기 전에는 old layout이, 이후에는 compacted prefix와 전체 fenced tail만 권위가 있다.
8. archive local head와 CAS token은 항상 같은 stable `Attributes → Get → Attributes` 읽기에서 유래한다.
