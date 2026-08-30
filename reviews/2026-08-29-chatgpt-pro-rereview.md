ChatGPT의 말:53m 2s 동안 처리함재검토 판정
이번 검토에서는 요청대로 recovery certificate 서명·키 회전, peer mTLS, 이에 종속된 0-RTT 구조는 제외했다. 2026-08-29-review-triage
정상 경로의 개선은 확인된다. 별도 StageValue 왕복 제거, slow-path priority 일관화, Graph snapshot metadata, Kubernetes 병렬 bootstrap, S3 marker HEAD 제거 등은 방향이 맞다. 특히 Parallel 기동과 not-ready 주소 발행은 manifest와 독립 테스트에 모두 반영됐다. manifest_test
다만 현재 변경에는 병합 전 수정이 필요한 correctness/recovery 차단 항목 4개가 남아 있다. 제공된 벤치마크는 78개 aggregate에서 오류가 없고 정상 latency와 S3 총량도 개선됐지만, 아래 문제들은 주로 서로 다른 ingress의 동시 요청, out-of-order suffix, compaction 도중 crash, fault-transition 중간 상태, 장시간 checkpoint download에서 발생하므로 현재 벤치 범위로는 검출되지 않는다. REPORT

병합 차단 항목
1. [P0] request-ID fingerprint 충돌이 합법적인 leaderless 경쟁을 영구적인 materializer poison pill로 만든다
패치는 SQL·KV·Notify에서 이미 존재하는 request ID의 fingerprint가 다르면 ApplyBatch 자체를 실패시키도록 변경했다. transaction이 rollback되므로 해당 slot은 적용되지 않고 materializer tip도 전진하지 않는다. rhiza-review-fixes
이 변경은 단일 ingress 시스템에서는 “불가능한 replay를 corruption으로 처리”하는 것처럼 보이지만, 3-peer leaderless multi-ingress에서는 fingerprint 충돌이 정상적인 client race로 발생할 수 있다.
구체적인 재현 순서는 다음과 같다.


Client A가 node 1에 request_id=X, value=A를 보낸다.


동시에 Client B가 node 2에 request_id=X, value=B를 보낸다.


각 node의 request-ID mutex는 프로세스 로컬이므로 서로를 보지 못한다.


두 node 모두 아직 receipt가 없다고 판단한다.


두 값이 서로 다른 consensus slot에 정상적으로 결정된다.


먼저 적용된 값이 receipt를 저장한다.


두 번째 slot 적용 시 fingerprint conflict가 발생한다.


모든 replica가 동일 slot에서 materialization을 멈춘다.


이후 모든 replay와 mutation이 같은 slot에서 반복 실패한다.


서버의 request lock은 256개 프로세스 로컬 stripe일 뿐 cluster-global admission이 아니다.
Graph profile은 현재 이 상황을 다르게 처리한다. applyCommand의 fingerprint conflict를 applyGraph가 deterministic command failure로 받아 recordFailure를 수행하고 slot metadata를 전진시킨다. 기존 request가 이미 있으면 새 receipt를 쓰지 않지만 slot은 소비된다. 즉 SQL/KV/Notify와 Graph의 replay semantics도 달라졌다.
권장 수정
materializer에서는 두 번째 intent를 deterministic rejected no-op으로 처리하고 반드시 slot을 전진시켜야 한다.
request ID 없음
    → 실행하고 receipt 저장

request ID 있음 + fingerprint 동일
    → exact replay no-op

request ID 있음 + fingerprint 상이
    → 첫 receipt 유지
    → user state 변경 없음
    → slot은 정상 적용
두 번째 요청의 origin server는 이미 submit 이후 RequestMatches를 다시 확인하므로 ErrRequestConflict를 반환할 수 있다. 정말로 fingerprint conflict를 “절대 불가능한 corruption”으로 만들려면 materializer가 아니라 consensus admission 전에 cluster-global request-ID reservation을 구현해야 한다.
필수 테스트는 서로 다른 두 Server 또는 node에서 동일 ID·상이한 payload를 동시에 제출한 후 다음 mutation과 restart/replay가 정상 진행되는지를 확인해야 한다.

2. [P0] live RestoreCheckpointBase가 이미 보유한 out-of-order suffix를 tip에 다시 연결하지 않는다
RestoreCheckpointBase는 checkpoint base를 WAL에 설치하고 installBaseLocked를 호출하지만, 일반 CompactThrough와 달리 그 뒤에 advanceTipLocked와 slot allocator 정리를 호출하지 않는다. rhiza-review-fixes
이 문제는 다음 상태에서 발생한다.
local contiguous tip = 10
local decided map     = {12}
shared checkpoint base = 11
archive tip             = 12
leaderless 합의에서는 gap 때문에 tip은 10이지만 slot 12가 먼저 결정되어 있는 상태가 정상적으로 가능하다.
checkpoint base 11을 설치하면:


installBaseLocked가 tip을 11로 설정한다.


기존 slot 12 decision은 retained된다.


하지만 slot 12를 따라가며 tip을 전진시키지 않는다.


그 뒤 archive catch-up은 slot 12를 다시 받는다. acceptDecision은 동일 decision이 이미 있으므로 조기 반환하고 역시 advanceTipLocked를 호출하지 않는다. 결과적으로 다음 루프가 영구 반복될 수 있다.
Gofor core.Tip() < archive.Tip() {
    // 같은 slot 12를 계속 읽고 기존 decision으로 처리
}
일반 compaction 경로는 installBaseLocked 뒤에 명시적으로 advanceTipLocked와 pruneSlotAllocatorLocked를 수행한다.
추가로 base 설치 자체가 tipChanged를 깨우지 않는다. suffix가 하나도 없더라도 기존 WaitTip(ctx, 11) 호출은 tip이 11에 도달했음에도 다음 tip 변경이나 context timeout까지 잠들 수 있다.
권장 수정
base 설치를 하나의 공통 helper로 만들어 다음을 원자적으로 수행해야 한다.
Gobefore := c.tip

c.installBaseLocked(base)
c.advanceTipLocked()
c.pruneSlotAllocatorLocked()

if c.tip != before && advanceTipLocked가 signal하지 않은 경우 {
    signalTipChangedLocked()
}
필수 테스트:
tip=1, decided[3] 보유
checkpoint base=2 설치
→ tip이 즉시 3이어야 함
→ WaitTip(2), WaitTip(3)이 모두 깨어나야 함
→ archive catch-up loop가 종료되어야 함

3. [P0] live checkpoint base 설치가 unresolved recorder의 proposal value를 WAL에서 삭제한다
일반 CompactThrough는 패치 이후 floor 위의 decision과 unresolved recorder가 참조하는 hash를 keep에 넣는다. 이 수정 자체는 필요하다. rhiza-review-fixes
그러나 live recovery의 RestoreCheckpointBase는 여전히 다음과 같이 nil keep set으로 WAL을 compact한다.
Goc.wal.Compact(baseEntry, nil)
WAL compaction은 EntryProposal에 대해 hash가 keep set에 없으면 제거하지만, floor 위의 EntryReceipt 같은 non-proposal entry는 유지한다.
따라서 다음 상태가 만들어진다.
slot 20:
  durable recorder state는 WAL에 남음
  recorder가 참조하는 proposal hash도 남음
  proposal payload EntryProposal은 삭제됨
현재 프로세스에서는 c.values 메모리에 값이 남아 있으므로 복구가 성공해 보인다. 하지만 checkpoint 설치 직후 crash/restart가 발생하면:


WAL recovery는 recorder state를 복구한다.


해당 hash의 proposal bytes는 WAL에 없다.


archive에는 아직 결정되지 않은 proposal이 없으므로 복구할 수 없다.


다른 peer가 unavailable하거나 이미 같은 값을 버렸다면 RecoverThrough가 실패한다.


이는 local recorder durability를 깨뜨린다.
권장 수정
CompactThrough와 RestoreCheckpointBase가 같은 helper를 사용해야 한다.
Gofunc (c *Core) liveProposalHashesAboveLocked(floor Slot) map[ValueHash]struct{}
최소한 floor 위 unresolved recorder의 FirstCurrent, AggregateCurrent, AggregatePrior가 참조하는 hash는 모두 보존해야 한다.
필수 crash test:


checkpoint index보다 높은 slot의 recorder receipt를 WAL에 기록한다.


아직 decision은 만들지 않는다.


live RestoreCheckpointBase를 수행한다.


즉시 WAL을 close/reopen한다.


RecoverThrough가 local WAL만으로 해당 proposal을 복구해야 한다.



4. [P0] checkpoint root reconciliation과 block GC가 concurrent object publication에 대해 안전하지 않다
4-1. 삭제된 remote root를 local memory에서 다시 살린다
새 loadAll은 remote root 목록을 읽은 뒤, remote 목록에 없는 모든 m.checkpoints 항목을 다시 합친다. rhiza-review-fixes
이 로직은 “목록을 읽는 동안 현재 프로세스가 만든 root”를 보존하려는 의도지만, 이전에 다른 node가 삭제한 stale root도 구분 없이 되살린다.
3-peer 환경에서 다음이 가능하다.


node A가 과거 root R1, R2, R3를 메모리에 갖고 있다.


이후 GC leader가 된 node B가 remote object R1을 삭제한다.


node A가 나중에 GC leader가 되어 loadAll을 실행한다.


remote LIST 결과는 R2, R3이다.


merge가 local memory의 R1을 다시 추가한다.


GarbageCollect가 R1에 Attributes를 호출한다.


remote에는 이미 없으므로 GC가 오류로 종료한다.


다음 GC에서도 같은 stale root가 다시 merge된다.


object-storage-first 모델에서는 이전 세대 remote object의 부재가 authoritative해야 한다. 모든 local root를 무조건 merge해서는 안 된다.
4-2. 첨부 테스트는 실제 LIST snapshot race를 검증하지 않는다
추가된 테스트 bucket은 underlying Iter를 수행하기 전에 대기한다. checkpoint 생성이 끝난 뒤에야 실제 LIST가 시작되므로 새 root는 그냥 remote LIST 결과에 포함된다. 즉, 이 테스트는 “LIST가 이미 snapshot을 잡은 뒤 root가 생성되는 경우”를 검증하지 않는다. rhiza-review-fixes
더구나 CreateFiles는 root upload가 끝나도 candidate를 m.checkpoints에 추가하지 않는다. root는 certification 후 rememberCertified에서야 local list에 들어간다.
따라서 실제 S3 LIST가 새 candidate root를 보지 못한 경우 local merge로도 복구되지 않는다.
4-3. 놓친 candidate가 재사용한 오래된 block을 GC가 삭제할 수 있다
새 candidate root가 LIST snapshot에서 빠지면 그 root의 block은 live set에 없다. 새 block은 grace period가 보호하지만, content-addressed dedup으로 재사용한 block은 LastModified가 오래되었을 수 있다. 이 경우 유효한 candidate가 참조하는 block을 GC가 삭제할 수 있다.
candidate가 이후 seal되고 CURRENT로 승격되면 checkpoint root는 존재하지만 block이 없는 recovery point가 된다.
권장 수정
단순한 m.checkpoints merge 대신 root 상태를 구분해야 한다.
remoteRoots        remote LIST에서 관측한 authoritative roots
pendingRoots       현재 프로세스가 성공적으로 upload했지만 아직 LIST에 없을 수 있는 roots
certifiedRoot      CURRENT가 가리키는 root
pinnedRecoveryRoot active recovery가 사용 중인 root
loadAll은 remote 결과에 LIST 시작 이후 생성된 pending root만 merge해야 한다. generation counter나 pendingRoots map을 사용할 수 있다.
block deletion은 다음 중 하나가 필요하다.


publication과 sweep을 조정하는 GC barrier


root/block에 대한 active pin


두 번의 독립 root snapshot을 거치는 mark-then-delete


candidate marker를 먼저 만들고 다음 GC 세대에만 삭제


필수 테스트는 두 개의 Manager를 사용해 remote deletion과 concurrent publication을 재현해야 한다.

높은 우선순위
5. [P1] 첫 Record에만 value를 inline하면 slow path 도중 한 peer 장애에서 quorum liveness가 깨진다
정상 경로에서 별도 StageValue quorum을 없애고 첫 Record에 value를 넣은 것은 latency 측면에서 유효하다. 다만 현재 구현은 step % 4 != 0인 모든 후속 phase에서 value를 제거한다. rhiza-review-fixes
recordQuorum은 quorum을 받는 즉시 나머지 RPC context를 취소한다. 따라서 첫 round에서 value를 실제로 durable하게 받은 node가 정확히 quorum 2개뿐일 수 있다.
다음 fault transition을 고려해야 한다.
step 4:
  n1, n2가 Record 성공 → quorum
  n3 요청은 취소되어 value를 저장하지 못함

step 5 직전:
  n2 장애

현재 live nodes:
  n1: value 보유
  n3: value 미보유
step 5는 hash-only이므로 n3의 Record는 “proposal value is unavailable”로 실패한다. n1+n3는 수적으로 quorum인데도 slot을 진행할 수 없다. 현재 benchmark test는 “StageValue 호출 0, inline value 호출 1 이상”만 확인하고 phase 간 fault transition은 확인하지 않는다. rhiza-review-fixes
권장 수정
가장 단순하고 안전한 방법은 slow path 모든 Record phase에 value를 포함하는 것이다. 정상 fast path는 여전히 한 phase이므로 정상 경로 비용은 변하지 않는다.
더 최적화하려면 proposer가 hash별 valueHolders를 추적해, 성공 summary를 돌려준 recorder에는 hash-only, 아직 확인되지 않은 recorder에는 value를 전송할 수 있다.
필수 테스트:
step 4는 n1+n2 성공, n3 delivery 차단
step 5부터 n2 장애
n1+n3만으로 proposal이 완료되어야 함
첨부 벤치 runner는 peer 장애를 넣은 뒤 linearizable probe가 성공할 때까지 기다린 후 workload를 시작하므로, 이와 같은 장애가 phase 사이에 발생하는 구간은 측정하지 않는다. rhiza-review-fixes

6. [P1] compacted-history recovery 상태 머신이 불필요한 전체 restore와 잘못된 readiness를 만든다
6-1. 한 peer의 ErrCompacted가 사용 가능한 정상 peer 응답보다 우선한다
catchUpQuorum은 어느 peer 하나라도 ErrCompacted를 반환하면, 다른 peer가 정상적인 contiguous page를 반환했더라도 무조건 restoreArchiveCatchUp을 실행한다. rhiza-review-fixes
3-peer에서 local node + 정상 응답 peer 하나면 이미 quorum이다. 다른 peer 하나가 조금 먼저 compact했을 뿐인데도 다음 비용이 발생한다.


readiness fence


checkpoint root/block download


WAL compaction


materializer restore


archive suffix replay


peer별 compaction 시점이 다를 수 있으므로 정상 운영에서도 발생 가능한 S3·latency 증폭이다.
정상 page가 applied+1을 제공한다면 그것을 우선 사용하고, 사용 가능한 page가 하나도 없을 때만 archive recovery를 선택해야 한다.
6-2. archive restore가 끝났다는 이유만으로 quorum 확인 전에 ready=true가 된다
restoreArchiveCatchUp은 끝에서 직접 n.ready.Store(true)를 수행한다. 하지만 호출자인 catchUpQuorum은 그 뒤 다시 peer quorum round를 실행한다. 다음 round가 실패해도 observeCatchUp은 오류를 로그만 하고 기존 ready=true를 유지한다. rhiza-review-fixes
이는 기존 multi-node startup 계약인 “quorum catch-up 성공 후 ready”와 다르다.
restoreArchiveCatchUp은 readiness를 결정하지 말고 local recovery만 수행해야 한다. 최종 ready=true는 완전한 catchUpQuorum 성공 지점 한 곳에서만 설정해야 한다.
6-3. foreground read/write 경로의 ErrCompacted는 live recovery를 유발하지 않고 HTTP 500이 된다
typed ErrCompacted는 Node의 periodic catch-up에서만 특별 처리된다. Server.readBarrier → catchUpFrom이나 hedged proposal accept 경로에서 같은 오류가 발생하면 그대로 반환된다. writeAPIError에는 ErrCompacted 분류도 없으므로 HTTP adapter에서는 500이 된다.
foreground 경로에서는 대규모 checkpoint restore를 request context에 직접 수행하지 말고:


node를 not-ready로 전환


singleflight recovery worker를 깨움


caller에는 ErrNotReady 또는 명시적인 recovery-in-progress 503 반환


이 흐름이 안정적이다.

7. [P1] live recovery가 checkpoint를 중복 다운로드하고, recovery base가 움직이면 무한히 폐기할 수 있다
현재 경로는 checkpoint 하나에 대해 다음 I/O를 수행한다.


OpenRoot — root GET


DownloadRootFiles


내부에서 다시 OpenRoot — 두 번째 root GET


모든 block GET 및 SHA-256 검증




Core.RestoreCheckpointBase


checkpoint validator가 Manager.Verify 호출


Verify가 다시 OpenRoot — 세 번째 root GET


local verified cache에 없는 block을 다시 모두 GET




DownloadRootFiles의 성공은 verified-block cache에 기록되지 않으므로, 최신 checkpoint를 검증하지 못했던 lagging peer에서는 checkpoint bytes를 사실상 두 번 다운로드한다. live recovery 함수의 호출 순서는 첨부 패치에 명확히 나타난다. rhiza-review-fixes
또한 checkpoint download를 끝낸 뒤 archive base가 처음 선택한 base와 정확히 같은지 검사한다. download 중 새 checkpoint가 archive base로 승격되면 모든 다운로드를 버리고 오류로 종료한다. checkpoint 생성 주기가 download 시간보다 짧으면 다음이 반복될 수 있다.
base N 선택
checkpoint N 다운로드
base N+1로 변경
N 폐기

base N+1 선택
checkpoint N+1 다운로드
base N+2로 변경
N+1 폐기
...
최대 checkpoint 크기가 큰 환경에서는 recovery starvation과 막대한 S3 GET/egress가 발생할 수 있다.
권장 수정
checkpoint.Manager에 다음과 같은 통합 API가 필요하다.
GoDownloadAndVerifyRootFiles(
    ctx,
    rootIdentity,
    destination,
) (root, files, verificationToken, error)
이 API는:


root를 한 번만 GET


각 block을 한 번만 GET


download 중 hash 검증


검증 성공 block을 local verified cache에 기록


Core validator가 같은 token/cache를 사용해 원격 GET 없이 검증


하도록 해야 한다.
archive 쪽에는 BeginRecoverySnapshot과 같은 pin이 필요하다.
head version
base seal/decision
tail extent refs
reader/GC pin
선택한 snapshot을 이용해 복구를 끝낸 뒤 최신 head를 다시 따라가면, base가 움직여도 이미 받은 수 GiB 데이터를 폐기할 필요가 없다.

8. [P1] Server.Quiesce의 context 취소 경로가 WaitGroup.Wait 종료 전에 admission을 다시 연다
Quiesce는 별도 goroutine에서 proposalWG.Wait()를 호출한다. context가 먼저 취소되면 quiescing=false로 되돌리고 즉시 반환하지만, Wait goroutine은 계속 실행된다. rhiza-review-fixes
그 상태에서 새 proposal이 들어오면 기존 Wait가 반환하기 전에 같은 WaitGroup에 새로운 Add가 발생할 수 있다. Go의 WaitGroup은 독립된 새 작업 세트를 추가할 때 이전 Wait가 반환된 후에 Add해야 한다고 명시한다. 이 패턴은 race에 따라 misuse panic, 잘못된 drain 완료, orphan Wait goroutine으로 이어질 수 있다. Go Packages
권장 수정
context 취소 시에도 기존 Wait가 끝날 때까지 quiescing=true를 유지해야 한다. 더 좋은 구조는 WaitGroup을 context-aware admission gate로 사용하지 않고, proposeMu 아래의 명시적인 inflight counter와 generation/condition channel을 사용하는 것이다.
필수 테스트:


proposal 하나를 의도적으로 block한다.


짧은 deadline으로 Quiesce를 호출한다.


timeout 직후 새 proposal을 제출한다.


-race, 수천 회 반복에서 panic·goroutine leak·조기 restore가 없어야 한다.



중간 우선순위
9. [P2] Graph metadata 수정이 AppliedSlot > ConsensusTip이라는 새 불가능 상태를 만들 수 있다
Graph payload와 AppliedSlot을 같은 LatticeDB snapshot에서 얻도록 한 변경은 맞다. 그러나 Server는 ConsensusTip을 materializer operation 이전에 캡처한다. rhiza-review-fixes
다음 interleaving이 가능하다.
core tip 캡처: 100
다른 goroutine이 slot 101을 materialize
Graph snapshot 시작: applied slot 101
응답: AppliedSlot=101, ConsensusTip=100
long-poll GraphStreamRead에서는 wait 도중 여러 slot이 적용될 수 있어 더 쉽게 발생한다.
ConsensusTip은 operation 뒤에 읽고 최소한 다음 invariant를 보장해야 한다.
GoConsensusTip >= AppliedSlot
SQL과 KV는 반대 방향의 기존 문제가 남아 있다. query/KV 값을 읽은 뒤 별도의 material.Tip()을 읽으므로, 응답의 AppliedSlot이 실제 payload snapshot보다 새로울 수 있다.
embedded API 계약을 정확하게 만들려면 다음과 같은 API가 필요하다.
GoQueryResultAt(...) (result, appliedSlot, error)
KVGetAt(...)       (value, found, appliedSlot, error)
SQLite read transaction 안에서 _rhiza_meta.applied_slot과 사용자 값을 함께 읽어야 한다.

10. [P2] S3 HTTP metric이 expected condition이 아니라 모든 409/412와 모든 HEAD 404를 숨긴다
transport classifier는 현재 다음을 전부 failure가 아닌 것으로 처리한다.


모든 HTTP 409


모든 HTTP 412


모든 HEAD 404


conditional header나 object class를 확인하지 않는다. 첨부 테스트조차 conditional header가 전혀 없는 일반 PUT에 412를 반환하고 이를 정상으로 기대한다. rhiza-review-fixes
따라서 다음도 Unexpected4xx=0, S3HTTPFailures=0으로 숨을 수 있다.


unconditional PUT의 409


잘못된 precondition 구성의 412


기존 archive head가 사라진 HEAD 404


필수 checkpoint root/block의 HEAD 404


provider-specific conflict


이 상태에서는 보고서의 “예상 밖 S3 4xx/5xx 0”이 실제 transport 오류 부재를 완전히 증명하지 못한다. REPORT
권장 수정
다음 counter를 분리하는 것이 가장 명료하다.
http_non_2xx_total
expected_condition_total
unexpected_4xx_total
logical_not_found_total
409/412는 If-Match 또는 If-None-Match가 실제 request에 있고 해당 logical operation이 CAS/dedup으로 표시된 경우에만 expected로 분류해야 한다. HEAD 404도 caller가 존재성 probe로 표시한 요청만 expected로 분류해야 한다.

11. [P2] archive GC marker map이 crash orphan을 영구 누적시킨다
marker prefix를 한 번 LIST하여 object별 marker HEAD를 제거한 것은 S3 호출 수 관점에서 좋은 개선이다. 그러나 모든 marker를 map[string]time.Time으로 메모리에 올리고, target object 목록을 순회하면서 발견된 marker만 처리한다. rhiza-review-fixes
다음 crash window가 있다.
target block DELETE 성공
process crash
marker DELETE 미실행
다음 GC에서는:


target이 이미 없으므로 blocks/manifests LIST에 나오지 않는다.


marker는 marker map에 들어간다.


해당 target iteration이 없으므로 marker가 소비되지 않는다.


GC 종료 시 남은 map entry를 아무 작업 없이 버린다.


그 marker는 영구히 남고, 이후 모든 GC가 다시 LIST하고 메모리에 올린다. 장기 운영에서는 marker object 수, LIST 응답, heap 사용량이 계속 증가할 수 있다.
권장 수정
target loop가 끝난 뒤 남은 marker를 처리해야 한다. marker body에 이미 원래 object name을 저장하므로, leftover marker에 대해서만 body를 GET하고 target 부재와 grace를 확인한 뒤 marker를 삭제할 수 있다. 더 나은 형식은 target key를 marker key에서 복원 가능하게 만드는 것이다.

남은 성능 문제
12. [P2] embedded API의 HedgeDelay=0 기본값이 inline-value 변경과 결합해 최대 3-way proposal amplification을 만든다
CLI는 hedge delay 기본값을 설정하지만, 공개 rhiza.Open은 zero-value HedgeDelay를 그대로 Server에 넘긴다.
Server는 member마다 goroutine을 만들고 rank * hedgeDelay 후 proposal을 시작하므로 embedded 사용자가 값을 생략하면 세 proposer가 동시에 실행된다. inline-value 변경 후에는 각 proposer가 첫 Record에서 전체 payload를 quorum replica에 전송한다.
3-peer에서 하나의 client batch가 최악의 경우 다음으로 증폭될 수 있다.
3 proposer attempts × 3 Record destinations × full payload
Kubernetes benchmark는 CLI/env 기본 hedge delay를 사용하므로 이 zero-value embedded 경로를 측정하지 않는다.
권장 수정
공개 Config에서 “미설정”과 “의도적인 0”을 구분해야 한다.
Goif config.HedgeDelay == 0 {
    config.HedgeDelay = 5 * time.Millisecond
}
정말 eager hedge를 지원해야 한다면 별도 boolean 또는 optional duration을 두는 편이 안전하다.

13. [P2] WAL live-set 수정 후에도 decided suffix payload를 두 번 보존한다
CompactThrough은 floor 위 모든 decision hash를 proposal keep set에 추가한다. 그러나 WAL compaction은 floor 위 EntryDecide를 이미 자동으로 보존하고, decision record에는 전체 value가 들어 있다. rhiza-review-fixes
따라서 suffix의 결정된 값은 compacted WAL에 다음 두 형태로 남는다.
EntryProposal: raw value
EntryDecide:   full value + certificate
checkpoint tail이 수백 MiB이면 compaction write bytes, restart scan, transient allocation이 크게 증가한다.
proposal keep set에는 원칙적으로 retained decision record만으로 복구할 수 없는 unresolved recorder hash만 넣으면 된다. retained decision hash와 중복되는 proposal entry는 제거할 수 있다.
추가된 test는 recovery 후 values map 항목 수만 확인한다. EntryDecide만으로도 그 map은 동일하게 복구되므로 WAL byte 감소 여부는 검증하지 않는다. rhiza-review-fixes

테스트·벤치 증거에 대한 해석
제공된 결과는 다음을 잘 뒷받침한다.


정상 proposal에서 별도 peer round trip이 제거됐다.


안정화된 healthy/one-fault workload latency가 개선됐다.


짧은 single-host MinIO 실행에서 aggregate S3 호출 수가 감소했다.


한 번의 Graph live recovery가 성공했다. REPORT


그러나 현재 runner는 fault injection 직후 linearizable probe가 성공할 때까지 기다린 뒤 workload를 시작한다. 따라서 mid-proposal peer loss, readiness 전환, compacted error를 처음 만나는 foreground request의 결과는 78개 aggregate 오류율에 포함되지 않는다. rhiza-review-fixes
또한 제공된 보고서 자체도 결과가 짧은 단일 호스트 Dory/MinIO 측정이며 WAN·capacity 주장이 아니라고 제한한다. REPORT
이 세션에서는 첨부 patch와 코드, 첨부 테스트 및 보고서를 정적으로 대조했다. 보고서가 참조하는 object-store-totals.json, raw NDJSON 전체는 첨부되지 않았으므로 per-method 요청 예산은 독립적으로 재계산하지 않았다.

수정 우선순위
병합 순서는 다음이 적절하다.


request-ID conflict를 fatal apply error가 아닌 deterministic no-op으로 복원


RestoreCheckpointBase의 suffix advance·tip wakeup·slot allocator 정리


live base compaction에서도 unresolved recorder proposal 보존


checkpoint remote/local root reconciliation과 GC publication fence 수정


inline Record slow-path value availability 보장


compacted recovery의 quorum 우선순위·readiness 단일화


checkpoint download/verify 단일-pass와 pinned recovery snapshot


Quiesce drain을 WaitGroup 재사용 없는 admission gate로 교체


snapshot metadata, S3 classifier, marker orphan 정리


embedded hedge 기본값과 compacted WAL 중복 payload 최적화


현재 patch의 정상 경로 성능 개선은 유지할 가치가 크다. 다만 위 1~4는 각각 client-triggered cluster wedge, recovery loop, crash 후 recorder value 손실, certified checkpoint block 손실로 이어질 수 있으므로 먼저 수정해야 한다.출처 
