# ChatGPT Pro 전체 트리 재검토 원문

- 대상: 현재 전체 소스 스냅샷과 `origin/main` 대비 전체 diff
- 회수일: 2026-08-30
- 세션: https://chatgpt.com/c/6a866d25-19e4-83e8-8a60-b18c208fa0c6

## 최종 판정

- P0: 0건
- P1: 2건
- P2: 2건

직전 지적 중 다음 항목은 현재 코드에서 닫힌 것으로 판정했습니다.

- Notification dispatcher 포화 시 payload 복사 증폭
- Restore/reopen 과정의 notification dispatcher ownership 누수
- Notification subscriber cardinality와 queue 메모리 무제한 증가
- QUIC 0-RTT에서 Record, Learned, StageValue, PrepareCheckpoint 등 durable/mutating RPC 실행
- WAL compaction 중 logged=false decision의 proposal value 유실
- DB.Close 이후 Graph API nil dereference
- Recovery pin 갱신이 전역 GC/publisher lock과 직접 충돌하던 경로
- Request-ID가 다른 intent로 경쟁할 때 materializer 전체가 정지하던 경로
- SQL/KV/Graph 응답 payload와 AppliedSlot snapshot 불일치

## P1 — 1. WAL segment 생성 도중 crash가 나면 노드가 재시작 불가 또는 영구 write-unavailable 상태가 된다

위치: `pkg/qlog/wal.go`의 `Open`, `(*WAL).loadSegments`, `(*WAL).createSegment`, `(*WAL).publishManifestLocked`; 간접 호출자는 `Append`, `BeginCompaction`, `RestoreSegment`입니다.

최초 WAL 생성에서 `seg_001.log`를 최종 파일명으로 생성하고 directory fsync한 뒤 manifest를 publish합니다. segment 생성 후 manifest publish 전 crash가 발생하면 재시작 시 manifest 없이 `seg_*.log`가 존재해 `loadSegments`가 실패합니다.

정상 rollover에서도 `seg_(N+1).log` 생성·fsync 후 manifest publish 전 crash가 발생하면 기존 manifest로 시작은 되지만 orphan segment가 남습니다. 다음 rollover에서 같은 index를 `O_EXCL`로 생성하면서 `EEXIST`가 반복되어 mutation이 영구 중단됩니다. `BeginCompaction`, `RestoreSegment`, compaction target에서도 같은 종류의 orphan이 생길 수 있습니다.

근본 원인은 manifest를 authoritative commit record로 사용하면서 startup에서 manifest가 참조하지 않는 segment를 reconcile하지 않는 것입니다.

최소 수정 방향:

1. 최신 valid manifest가 참조하는 segment set 구성
2. `seg_*.log` 전체 열거
3. manifest에 없는 segment 검증
4. authoritative manifest보다 나중 index인 unreferenced segment quarantine 또는 삭제
5. no-manifest 상태에서 zero-length `seg_001`만 존재하면 제거 후 초기 세대 재생성
6. nonempty orphan은 record validation 후 quarantine하고 최신 manifest 기준으로 계속 기동
7. directory fsync

최신 manifest에 없는 segment를 active WAL에 임의로 합치면 안 됩니다.

결정적 회귀 테스트:

- `TestOpenRecoversInitialSegmentCreatedBeforeManifest`
- `TestOpenRemovesUnreferencedRolloverSegment`
- `TestOpenQuarantinesAbortedCompactionTarget`
- `TestRolloverCrashMatrix`

## P1 — 2. Object-store GC renewable lease가 fencing token으로 사용되지 않아 stale GC가 새 root/head가 참조하는 object를 삭제할 수 있다

위치:

- Checkpoint: `pkg/checkpoint/checkpoint.go`의 `withMaintenanceClaim`, `garbageCollect`, `uploadFile`, `RecoveryPin.Renew`, active recovery-pin 처리
- Archive: `pkg/recovery/archive.go`의 `withGCLock`, `cleanup`, `syncNow`, `RecoverySnapshot.Renew`, active archive recovery-pin 처리

만료된 GC holder가 block/extent Delete 직전 멈춘 뒤 lease가 만료되고 새 publisher가 같은 content-addressed object를 재사용하여 root/head를 publish할 수 있습니다. 이후 stale GC의 Delete가 완료되면 현재 root/head가 missing object를 가리킵니다. `context.Cancel`과 heartbeat는 이미 시작했거나 재개한 외부 side effect를 무효화하지 못합니다.

Recovery pin renewal과 expired-pin 조건 없는 Delete도 `renew success + pin deleted` 경쟁을 만들 수 있습니다.

근본 원인은 lock generation, GC candidate generation, recovery-pin version, content-addressed object, root/head publication generation, Delete operation이 서로 연결되지 않은 것입니다.

최소 수정 방향은 CAS 가능한 `LIVE/CANDIDATE/DELETING` object state/tombstone protocol입니다. Publisher가 candidate를 LIVE로 되돌리고 payload 존재·hash를 검증한 뒤 root/head를 publish하며, GC는 candidate를 DELETING으로 CAS한 세대만 Delete해야 합니다. Expired pin도 `pin V -> tombstone V+1` CAS에 성공한 GC만 inactive로 간주해야 합니다.

결정적 회귀 테스트:

- `TestStaleCheckpointGCCannotDeleteRepublishedBlock`
- `TestStaleArchiveCleanupCannotDeleteRepublishedExtent`
- `TestRecoveryPinRenewVsExpirySweep`

## P2 — 1. KVGetAt optimistic retry는 write가 지속되면 progress bound가 없다

위치: `pkg/materializer/materializer.go`의 `(*Materializer).KVGetAt`.

성공 시 value와 AppliedSlot snapshot 정합성은 유지되지만 global applied tip이 계속 변하면 retry 상한이 없어 unrelated SQL/KV/Graph/Notify/control decision만으로도 읽기가 무한 재시도될 수 있습니다. 큰 값에서는 실패 attempt마다 BLOB copy·allocation이 반복됩니다.

최소 수정 방향은 attempt를 제한하고 contention 시 SQLite read transaction 하나에서 applied slot과 KV value를 같은 snapshot으로 읽는 fallback을 사용하는 것입니다. 하나의 CTE/scalar-subquery SQL도 가능하지만 not-found에서도 slot을 반환하고 query-plan 회귀를 측정해야 합니다.

결정적 회귀 테스트:

- `TestKVGetAtFallsBackAfterBoundedTipChurn`
- `TestKVGetAtProgressUnderContinuousWrites`
- `TestKVGetAtRestoreAndCloseDuringOptimisticAttempt`

## P2 — 2. WAL manifest와 atomic-write temp 파일 수가 세대 수에 따라 무한 증가한다

위치: `pkg/qlog/wal.go`의 `publishManifestLocked`, `writeFileAtomically`, `loadSegments`, `Compaction.Commit`.

segment 또는 compaction generation마다 새 manifest를 만들지만 과거 manifest를 정리하지 않습니다. loader는 전체 manifest를 glob/sort하므로 inode 수, directory entry 수, startup memory/CPU, backup/filesystem metadata 작업량이 계속 증가합니다. process crash로 defer가 실행되지 않으면 atomic temp와 compaction temp도 누적됩니다.

새 manifest가 temp fsync → rename → directory fsync를 완료한 뒤 현재 manifest(선택적으로 직전 하나)만 보존해야 합니다. committed manifest cleanup 실패는 이미 durable한 foreground Append를 실패로 오인시키면 안 되며 startup retry 또는 health/metric으로 관측해야 합니다. Startup은 오래된 manifest, unreferenced temp, incomplete compaction temp, unreferenced segment를 정리하고 directory fsync해야 합니다.

결정적 회귀 테스트:

- `TestManifestGenerationsRemainBounded`
- `TestCommittedManifestCleanupFailureDoesNotFailAppend`
- `TestOpenCleansAtomicWriteTemps`

## 나머지 영역 판정

다음 경로에서는 추가 P0/P1/P2를 확정하지 않았습니다.

- QuePaxa fast/slow path proposal value availability
- One-peer-down record/learner quorum
- CompleteDecision contiguous-prefix 조건
- WAL compaction concurrent tail fencing 및 logged=false proposal retention
- concurrent multi-ingress Request-ID
- SQL/KV/Notify/Graph deterministic replay
- Graph/SQLite crash journal 및 snapshot metadata
- Live checkpoint restore와 suffix replay
- Recovery snapshot/root pin 정상 갱신
- Notification dispatcher FIFO/cardinality/close ownership
- QUIC read-only 0-RTT allowlist
- Peer typed compacted-history error
- Archive extent prefix validation
- Checkpoint root/block hash 검증
- Normal uncontended before-ACK archive publication
- Embedded API normal close error
- Kubernetes 3-peer parallel bootstrap

Pro 런타임에는 Go 1.27 toolchain이 없어 테스트와 race suite를 독립 재실행하지 못했습니다. 판정은 첨부 current source/tests의 전체 정적 추적 결과입니다.

최종 개수: P0 0, P1 2, P2 2.
