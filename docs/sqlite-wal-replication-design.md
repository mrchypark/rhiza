# Rhiza SQLite QWAL v3 복제 계약

> 상태: **클린 설치 전용 QWAL v3 구현 완료**
>
> 범위: QuePaxa가 결정한 SQLite 물리 효과의 준비, 배치, 적용, 복구
>
> 호환성: QWAL v2, control/QCTL generation 5, 이전 QSNP를 읽거나 마이그레이션하지 않는다.

## 1. 결정

Rhiza SQL의 내구성 원본은 SQLite 파일이 아니라 **2/3 Recorder quorum이 내구화한
`StoredCommand` 전체**다. command에는 QWAL page after-image와 request receipt가 함께
들어간다. winning proposer만 SQL을 실행하고 follower는 SQL 문장을 재실행하지 않는다.

```text
exact-base SQLite에서 SQL 실행
  -> native SQLite WAL frame 캡처
  -> QWAL v3(changed pages + receipts + base/target state root)
  -> 2/3 Recorder WAL 내구화 및 결정
  -> 로컬 SQLite 적용과 read visibility
  -> ACK
```

Recorder quorum WAL이 strict ACK의 유일한 필수 디스크 장벽이다. canonical SQLite,
control sidecar와 file qlog는 `synchronous=OFF` 또는 buffered로 유지하는 **폐기 가능한
materialized view**다. 로컬 apply는 ACK 전에 끝나지만 별도 로컬 내구성 경계는 아니다.

이 모델은 trigger, foreign-key cascade, rowid/`AUTOINCREMENT`와 허용된 비결정적 함수의
winning 결과를 exact replay한다. 패배한 제안은 canonical DB를 바꾸지 않는다.

## 2. 구현 경계

| 책임 | 구현 기준점 |
| --- | --- |
| native WAL parser와 checksum/commit 검증 | [`wal_capture.rs`](../crates/rhiza-sql/src/wal_capture.rs) |
| `StateIdentityV3` Merkle page state | [`page_state.rs`](../crates/rhiza-sql/src/page_state.rs) |
| QWAL v3 codec과 in-place page apply | [`qwal.rs`](../crates/rhiza-sql/src/qwal.rs) |
| 준비, inode seal, exact promotion, foreign apply | [`SqliteStateMachine`](../crates/rhiza-sql/src/lib.rs) |
| generation 6 control, receipt, embedded qlog | [`control.rs`](../crates/rhiza-sql/src/control.rs) |
| runtime recovery와 readiness | [`NodeRuntime`](../crates/rhiza-node/src/lib.rs) |
| checkpoint publish, restore와 compaction | [`durability.rs`](../crates/rhiza-node/src/durability.rs) |
| physical replication 계약 테스트 | [`qwal_contracts.rs`](../crates/rhiza-sql/tests/qwal_contracts.rs) |

현재 포맷 이름과 framing은 다음과 같다.

- QWAL v3: `QWAL\0\x04`
- QSNP v3: `QSNP\0\x04`
- control/QCTL generation 6: `RHIZA-SQL-CONTROL\0\x06`, `QCTL\0\x06`
- canonical client command는 계속 QSQL v2다. QSQL은 입력 포맷이고 QWAL 버전과 무관하다.
- archive API의 checkpoint v2도 별도 외부 checkpoint 프로토콜이며 QWAL v2를 뜻하지 않는다.

## 3. QWAL v3와 page-state identity

```text
QwalEnvelopeV3 {
  cluster_id, epoch, configuration_id, recovery_generation,
  base_index, base_hash,
  base_state:   { page_size, page_count, state_root },
  target_state: { page_size, page_count, state_root },
  materializer_fingerprint,
  receipts: [{ request_id, request_digest, result_blob }, ...],
  pages: [{ page_no, after_image }, ...]
}
```

`state_root`는 page number와 page bytes의 leaf, canonical empty subtree, internal node,
page size와 page count를 domain-separated hash로 묶는다. dense `PageStateCacheV3`는
authoritative data가 아니라 닫힌 SQLite 파일에서 언제든 재생성할 수 있는 cache다. 정상
write에서는 changed page만 overlay해 target root를 계산하고 설치 뒤 같은 patch로 cache를
갱신한다. 매 요청 전체 DB hash나 전체 page diff는 하지 않는다.

QWAL 검증은 다음을 fail closed로 강제한다.

- canonical postcard bytes와 정확한 v3 magic
- 1..=1,024개의 고유 receipt, 512 KiB 이하의 envelope/result/page image
- 엄격히 증가하는 page number와 정확한 page 크기
- grow 시 base EOF 뒤의 모든 새 page 포함
- SQLite header page size와 target page count 일치
- cached base root 일치와 changed-page overlay로 계산한 target root 일치
- cluster/epoch/configuration/recovery generation/materializer fingerprint 일치

로컬 path, WAL salt, `-shm`, staging 이름과 entry hash는 payload에 넣지 않는다.

## 4. 준비: native WAL capture

`prepare_sql_batch_effect`는 materialized tip, control의 `user_state`, pending 부재를 먼저
확인한다. lifecycle gate 아래에서 canonical connection을 닫고 Unix에서는 열린 file과
pathname이 같은 regular inode인지 `dev/ino/len/mtime/ctime` seal로 고정한다.

speculative target은 같은 filesystem에서 만든다. 256 KiB 이상은 macOS `clonefile` 또는
Linux `FICLONE`을 우선하고, 지원되지 않으면 full copy로 fallback한다. 이 clone/copy는
winning SQL을 격리하기 위한 로컬 준비물이지 QWAL correctness 증명이나 follower apply
방식이 아니다.

staging connection은 WAL mode, `synchronous=OFF`, checkpoint-on-close disabled로 실행한다.
commit 뒤 connection이 살아 있을 때 `-wal` descriptor를 열고 metadata를 seal한 다음,
connection을 닫아도 보존된 descriptor에서 frame을 직접 파싱한다. parser는 WAL magic/version,
page size, salts, rolling checksums, complete frames, 하나의 최종 commit marker와 target page
count를 검증한다. capture 전후 descriptor seal이 달라지면 거부한다.

마지막 committed frame의 page after-image만 보존하고 base와 같은 image는 제거한다. 새 page는
누락 없이 포함한다. custom recording VFS, checkpoint 뒤 full-file diff, full target digest는
production 준비 경로에 없다. full-file diff와 byte digest는 테스트 oracle 또는 checkpoint
용도로만 남아 있다.

ordered batch는 member별 savepoint를 사용한다. 실패 member만 rollback하고 성공 subset의
page effect와 receipt를 하나의 decided entry에 묶는다. 전부 실패하면 effect, consensus와
qlog가 모두 없다. retry는 같은 request ID와 canonical bytes를 사용하며 ID가 같고 bytes가
다르면 conflict다. foreign winner 뒤에는 stale effect를 재사용하지 않고 새 exact base에서
unseen member만 다시 준비한다.

## 5. 적용: exact promotion과 foreign patch

적용 전 lifecycle/read-write gate를 닫고 entry chain, identity, receipt와 page-state transition을
모두 검증한다.

- **exact local winner**: prepared effect digest, base/target roots, node identity와 configuration이
  모두 같고 base/target inode seal이 유지된 경우에만 prepared target을 canonical path로
  rename한다. Unix에서 inode 동일성을 증명할 수 없거나 seal이 달라지면 이 최적화는 사용하지
  않는다.
- **foreign winner 또는 promotion 불가**: connection을 checkpoint/close한 뒤 seal된 canonical
  inode에 changed page만 in-place write하고 target 길이를 맞춘다. 설치한 page bytes와 갱신된
  Merkle target root를 다시 확인한다. 전체 DB temp reconstruction/rename은 하지 않는다.

물리 설치 뒤 control generation 6의 한 비내구 transaction이 applied tip, target state,
receipts와 embedded qlog entry를 같은 anchor로 게시한다. connection은 exclusive lifecycle
gate 안에서 먼저 reopen될 수 있지만, control commit이 성공하기 전에는 read와 ACK를 허용하지
않는다. commit 실패 시 다시 닫는다.

DB 설치 뒤 control 게시가 실패하면 connection을 닫고 모든 read/receipt API를 차단한다.
같은 process에서는 exact payload만 재시도할 수 있다. 중간 page write 뒤 crash 또는 재시작 시
DB/control root가 맞지 않으므로 로컬 복구를 추측하지 않고 Recorder 기반 rebuild를 요구한다.

## 6. Recorder-authoritative recovery와 GC

시작과 restore는 다음 불변조건을 지킨다.

1. SQLite와 control pair를 함께 검증하고 closed DB에 예상 밖 `-wal`/`-shm`이 있으면 거부한다.
2. control tip/state root와 실제 DB의 rebuilt `StateIdentityV3`가 정확히 같아야 한다.
3. recovery anchor가 없고 materializer가 손상됐으면 로컬 `sqlite/`를 sibling quarantine한 뒤
   genesis view를 만들고 qlog와 certified Recorder decisions를 재생한다.
4. recovery anchor가 있으면 손상된 view를 임의 genesis로 만들지 않고 verified checkpoint를
   요구한다. restore는 target node identity로 QSNP를 설치하고 checkpoint suffix를 적용한다.
5. restored materializer tip과 local qlog tip/hash는 checkpoint root와 정확히 같아야 하며,
   qlog가 그 root hash를 실제 entry 또는 compaction anchor로 포함해야 한다.
6. 그 뒤 peer candidate를 certified decision과 대조하고 Recorder tail을 적용한다. 이 과정이
   끝나기 전 runtime은 생성되지 않으므로 `/readyz`가 열리지 않는다. runtime 중 invariant
   오류는 readiness를 latch-off한다.
7. quorum이 command나 decision certificate를 증명하지 못하면 `Unavailable` 또는
   reconciliation error로 fail closed한다.

checkpoint compaction은 materialized tip을 고정하고 local committed tail을 archive에 먼저
publish한다. snapshot을 publish한 뒤 archive에서 다시 restore해 **anchor와 bytes가 동일함**을
확인한 다음에만 local file qlog와 embedded qlog prefix를 compact한다.

Recorder WAL rotation은 frame 안의 full command를 checksum이 있는 QCMD 파일로 승격한 뒤 WAL을
비운다. 현재 QCMD 개별 GC는 구현하지 않았다. 따라서 checkpoint가 충분한 복구 근거가 되기
전에 Recorder command를 지우는 위험은 없지만, Recorder disk 사용량을 제한하는 별도
checkpoint-certificate 기반 GC 프로토콜은 남은 작업이다.

## 7. 클린 설치와 거부 범위

이 전환은 rolling upgrade가 아니다. 모든 voter를 같은 binary와 fingerprint로 구성한 빈 data
directory에서 시작한다. 다음 입력은 fallback 없이 거부한다.

- QWAL v2(`QWAL\0\x03`)와 더 오래된 QWAL payload
- control/QCTL generation 5와 이전 sidecar/snapshot
- 이전 QSNP와 legacy `__rhiza_meta`/`__rhiza_requests` DB
- old QEFX/QBCH history, dual writer/decoder, statement replay fallback
- 기존 DB/control의 in-place migration과 자동 downgrade

SQL page replay는 main DB 내부 final bytes만 다룬다. `ATTACH`/`DETACH`, TEMP schema,
extension/임의 virtual table, file/network/process side effect, `VACUUM INTO`, user-controlled
journal/checkpoint/page-size/`writable_schema`는 계속 admission 또는 authorizer에서 차단한다.

## 8. strict 보장과 한계

- ACK된 command의 전체 QWAL과 receipt는 3-peer 중 2 Recorder에 먼저 내구화된다.
- 한 voter의 영구 손실까지 quorum progress와 RPO=0를 유지할 수 있다. 두 voter의 durable
  data를 동시에 잃으면 외부 durable checkpoint 없이는 RPO=0를 증명할 수 없다.
- leaderless consensus는 failover와 병렬 proposal에 유리하지만 단건 latency의 하한인 network
  RTT와 quorum flush를 제거하지 않는다. 처리량은 concurrent outstanding request와 logical
  batch가 group commit을 채울 때 올라간다.
- SQLite/control/qlog만 가진 한 node의 peer 없는 독립 재개는 strict 계약이 아니다.
- QWAL은 512 KiB 상한이다. 더 큰 transaction은 durable content-addressed payload protocol
  없이는 지원하지 않는다.
- exact prepared promotion의 inode proof는 Unix에서만 활성화된다.

## 9. 구현 완료와 남은 검증

구현 및 자동화 테스트가 확인하는 항목:

- native WAL checksum/commit parsing과 changed-page reproduction
- Merkle base/target transition, grow/shrink와 corrupt page/root 거부
- exact prepared promotion, foreign in-place patch, inode/symlink race 거부
- partial apply 및 post-control failure의 read 차단과 exact retry
- QSNP restore, checkpoint-root equality, tail catch-up와 readiness gate
- QWAL v2 및 control generation 5 rejection

아직 release gate로 남은 항목:

- clean Linux NVMe에서 Recorder `fdatasync`, native capture와 follower apply를 분리한 반복 benchmark
- 3-peer Kubernetes에서 emptyDir/no-PVC failure 및 checkpoint+Recorder recovery 재측정
- 손상 종류와 1/2/3 peer loss를 포함한 전체 HTTP rejoin/recovery E2E
- 큰 DB startup은 입력을 page 단위로 읽지만 모든 page hash를 다시 계산한다. 측정상 필요할 때만
  authenticated persisted cache 등으로 O(DB size) startup hashing을 줄인다.
- checkpoint certificate에 묶인 bounded Recorder QCMD GC

벤치마크는 durability mode, voter 수, concurrency, logical batch 크기, payload, host/filesystem을
함께 기록한다. 검증되지 않은 수치를 이 계약의 근거로 사용하지 않는다.

## 10. 공식 SQLite 자료

- [Write-Ahead Logging](https://www.sqlite.org/wal.html)
- [WAL-mode File Format](https://www.sqlite.org/walformat.html)
- [Database File Format](https://www.sqlite.org/fileformat.html)
- [Atomic Commit](https://www.sqlite.org/atomiccommit.html)
- [How To Corrupt An SQLite Database File](https://www.sqlite.org/howtocorrupt.html)
