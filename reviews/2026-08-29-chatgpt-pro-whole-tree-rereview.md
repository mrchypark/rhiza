# ChatGPT Pro whole-tree re-review

Source session: https://chatgpt.com/c/6a866d25-19e4-83e8-8a60-b18c208fa0c6

Completed: 2026-08-29 KST

## Raw response

판정

P0 0건, P1 2건, P2 4건입니다.

직전의 QUIC 0-RTT P2는 닫혔습니다. Client는 Sync, ReadIndex, FetchValue 외 operation에서 handshake 완료를 강제하고, server도 동일 allowlist 밖의 early operation을 operation switch와 core 호출 전에 거부합니다. Allowlist 전체 enum과 server-side 선차단을 검사하는 테스트도 있습니다. 

rhiza-pro-final-fix-rereview-20… +2

Notification은 subscriber fan-out 측의 무제한 메모리 증폭은 닫혔지만, dispatcher 이전 복사와 materializer 교체 수명 관리에서 P2가 남았습니다. 3-peer bootstrap의 Parallel 기동과 not-ready peer DNS publication도 테스트로 고정되어 있습니다. 

manifest_test

첨부 tar의 현재 소스와 patch의 역적용 일치는 확인했습니다. 다만 현재 실행 환경은 Go 1.23.2이고 작업 트리는 Go 1.27을 요구하며 외부 toolchain 취득이 불가능해, 제공된 go test·race·Dory 실행 결과를 이 환경에서 독립 재실행하지는 못했습니다.

[P1] 1. 기존 checkpoint/PUBLISHER 객체와 호환되지 않아 업그레이드 후 checkpoint·GC가 영구 중단된다

위치

pkg/checkpoint/checkpoint.go

PublisherClaim

(*Manager).readPublisherClaim

(*Manager).acquireClaim

(*Manager).AcquirePublisherClaim

(*Manager).GarbageCollect

확정된 실패 조건

현재 변경은 기존 PublisherClaim JSON에 Purpose 필드를 추가했습니다.

Go
Purpose string `json:"purpose,omitempty"`

그리고 readPublisherClaim은 Purpose가 정확히 "publisher" 또는 "maintenance"가 아니면 객체를 invalid로 거부합니다. 

rhiza-pro-final-fix-rereview-20… +1

그러나 origin/main@2a118a2가 이미 작성한 checkpoint/PUBLISHER 객체에는 purpose 필드가 없습니다. Publisher claim은 release 시 삭제되지 않고 lease만 만료 상태로 갱신되므로, 한 번이라도 checkpoint publisher가 동작한 기존 bucket에는 이 객체가 남습니다.

업그레이드 후 첫 AcquirePublisherClaim 또는 maintenance claim은 다음 순서로 실패합니다.

기존 PUBLISHER GET
→ JSON decode 성공, Purpose=""
→ readPublisherClaim validation 실패
→ acquireClaim은 expired claim takeover를 시도하지 못함
→ 모든 후속 checkpoint publication/GC 실패

모든 노드를 업그레이드해도 자동 복구되지 않습니다. 새로운 checkpoint가 생성되지 않으므로 WAL·archive compaction도 정체되고, 장기적으로 MaxWALBytes 도달 후 mutation이 중단될 수 있습니다.

Rolling upgrade도 양방향 호환되지 않습니다. 현재 persisted JSON decoder는 unknown field를 거부하므로, 구버전 node는 신버전이 작성한 purpose 필드를 읽지 못합니다. 신버전 node는 구버전 claim의 빈 Purpose를 거부합니다.

최소 수정 방향

가장 안전한 수정은 checkpoint publisher와 maintenance lock을 별도 object key와 별도 schema로 분리하는 것입니다.

checkpoint/PUBLISHER   // 기존 schema 그대로 유지
checkpoint/GC_LOCK     // 신규 maintenance schema

현재 구조를 유지한다면 최소한 legacy migration이 필요합니다.

Go
if claim.Purpose == "" && claim.ReservedIndex > 0 {
    claim.Purpose = "publisher"
}

다만 이 fallback만으로는 구버전 reader가 신버전 purpose 필드를 거부하는 rolling-upgrade 문제는 해결되지 않습니다. 따라서 기존 PUBLISHER format을 변경하지 않는 별도 lock object가 적절합니다.

필수 테스트:

- origin/main 형식의 purpose 없는 expired PUBLISHER object 설치
- 신버전 AcquirePublisherClaim 성공
- 신버전 GarbageCollect 성공
- 구버전·신버전 claim format 교차 읽기 또는 명시적 upgrade fence 검증
[P1] 2. Recovery pin 갱신이 publisher/GC 전역 lock과 경쟁하여 40초 이상 복구를 즉시 취소한다

위치

pkg/node/node.go

(*Node).restoreArchiveCatchUp

pkg/checkpoint/checkpoint.go

(*RecoveryPin).Renew

(*Manager).withMaintenanceClaim

pkg/recovery/archive.go

(*RecoverySnapshot).Renew

(*Manager).withGCLock

확정된 실패 시나리오

Live recovery는 checkpoint pin과 archive snapshot pin에 2분 lease를 설정하고, 40초마다 순차적으로 갱신합니다. 갱신 중 하나라도 실패하면 workCtx를 취소해 checkpoint download·restore 전체를 중단합니다. 

rhiza-pro-final-fix-rereview-20…

그러나 checkpoint pin 갱신은 pin 자체를 CAS 갱신하는 데 그치지 않고 withMaintenanceClaim을 통해 공용 checkpoint/PUBLISHER claim을 먼저 취득합니다. 

rhiza-pro-final-fix-rereview-20…

Archive snapshot 갱신도 같은 방식으로 전체 archive GC lock을 먼저 취득합니다. 

rhiza-pro-final-fix-rereview-20…

각 lock acquisition은 기존 lease가 살아 있으면 기다리지 않고 즉시 ErrPublisherBusy 또는 ErrArchiveBusy를 반환합니다. 따라서 다음이 재현 가능합니다.

T=0:
  lagging node가 recovery pin 2분 lease로 checkpoint download 시작

T=35:
  다른 node가 checkpoint publication 또는 archive GC 시작
  작업이 전역 lock을 보유

T=40:
  recovery heartbeat 실행
  pin 자체 lease는 아직 약 80초 남음
  그러나 전역 lock acquisition이 Busy 반환
  restoreArchiveCatchUp이 즉시 cancel
  이미 받은 checkpoint bytes 폐기

두 lagging node가 동시에 40초 이상 recovery하는 경우에도 heartbeat가 같은 전역 maintenance lock과 충돌하여 한쪽이 취소될 수 있습니다.

제공된 5초 내 emptyDir recovery 검증으로는 이 경로가 실행되지 않습니다. 대규모 checkpoint, 저속 S3 endpoint, cross-region 복구에서 발생하는 liveness 문제입니다. 또한 heartbeat마다 pin CAS 외에 전역 lock의 HEAD/GET/conditional PUT/release가 추가되어 S3 control-plane 호출도 불필요하게 증가합니다.

최소 수정 방향

초기 pin 등록만 GC/publication lock 아래에서 수행해 deletion race를 닫고, 갱신은 기존 unexpired pin에 대한 직접 CAS로 처리해야 합니다.

Pin registration:
  global lock → root/chain 존재 확인 → pin IfNotExists/CAS → global lock release

Pin renewal:
  pin HEAD/GET → owner/identity/expiry 확인 → pin IfMatch 갱신
  global publisher/GC lock 취득하지 않음

40초 heartbeat와 2분 lease라면 GC가 이전 unexpired version을 읽어도 해당 GC cycle에서 object를 유지하므로 직접 CAS가 안전한 여유를 가집니다.

보조 방어로 Busy를 즉시 fatal 처리하지 말고 lease expiry safety margin까지 backoff retry해야 합니다.

필수 테스트:

- checkpoint publisher claim을 40초 heartbeat 시점에 보유
- RecoveryPin.Renew는 정상 성공해야 함

- archive GC lock을 heartbeat 시점에 보유
- RecoverySnapshot.Renew는 정상 성공하거나 lease margin 내 retry해야 함

- 두 recovery를 동시에 40초 이상 실행
- 어느 쪽도 단순 lock contention으로 취소되지 않아야 함
[P2] 3. Dispatcher queue가 가득 차도 notification payload가 이미 materializer write lock 아래에서 복제된다

위치

pkg/materializer/materializer.go

(*Materializer).ApplyBatch

(*Materializer).applyValueLocked

(*Materializer).enqueueNotification

TestMaterializerDropsFullNotificationWithoutPayloadCopy

현재 개선된 부분

dispatcher queue는 64로 bounded

subscriber는 최대 64개

subscriber queue depth는 1

subscriber queue가 full이면 clone 전에 drop

실제 fan-out은 단일 dispatcher에서 처리

이 부분은 확인됩니다. 

rhiza-pro-final-fix-rereview-20…

남은 문제

ApplyBatch는 전체 함수 동안 Materializer.mu write lock을 보유합니다. Notification command를 처리할 때 applyValueLocked는 dispatcher 수용 여부를 알기 전에 다음 추가 복사를 수행합니다.

Go
pendingNotification{
    topic:   notifyCommand.Topic,
    payload: append([]byte(nil), notifyCommand.Payload...),
}

SQLite commit 후에야 enqueueNotification이 nonblocking send를 시도하고, dispatcher queue가 full이면 drop합니다. Dispatcher send 자체는 bounded지만, drop 판단은 이미 payload-sized clone 이후입니다. 

rhiza-pro-final-fix-rereview-20…

즉, 포화 상태에서는 다음 순서입니다.

JSON decode가 payload 할당
→ payload 전체를 다시 clone
→ SQLite commit
→ dispatcher full 확인
→ clone한 payload drop

Catch-up의 ApplyBatch는 최대 256 decisions를 처리할 수 있습니다. Replicated value 상한에 가까운 notification이 연속된 경우 dispatcher에는 64개만 들어가지만, 그보다 훨씬 많은 payload가 materializer write lock 아래에서 먼저 복제되어 수십 MiB의 transient allocation·copy·GC pressure를 만들 수 있습니다.

추가된 allocation 테스트는 dispatcher queue가 아니라 subscriber channel을 직접 채운 뒤 publishNotification을 호출합니다. 따라서 subscriber-side clone은 검증하지만 ApplyBatch → pending → enqueueNotification의 dispatcher overflow는 검증하지 않습니다. 

rhiza-pro-final-fix-rereview-20…

최소 수정 방향

DecodeNotifyCommand가 반환한 notifyCommand.Payload는 이미 독립 slice이므로 pending 목록에서 다시 clone할 필요가 없습니다.

Go
*pending = append(*pending, pendingNotification{
    topic:   notifyCommand.Topic,
    payload: notifyCommand.Payload,
})

또는 commit 후 dispatcher slot을 먼저 reserve한 뒤, 성공한 항목만 clone해야 합니다.

필수 테스트:

- notifyQueue를 완전히 포화
- 최대 크기에 가까운 notification batch 적용
- full dispatcher와 empty dispatcher의 추가 allocation 차이가 payload 크기에 비례하지 않는지 확인
- ApplyBatch latency가 drop 수 × payload 크기로 증가하지 않는지 확인
[P2] 4. Checkpoint restore와 rollback reopen마다 notification dispatcher goroutine이 하나씩 누수된다

위치

pkg/materializer/materializer.go

openMaterializer

(*Materializer).restoreParts

(*Materializer).reopen

(*Materializer).Close

openMaterializer는 호출될 때마다 새로운 notifyQueue, notifyStop, notifyWG를 만들고 즉시 go m.runNotifications()를 시작합니다. 

rhiza-pro-final-fix-rereview-20…

Dispatcher를 종료하는 유일한 정상 경로는 Materializer.Close입니다. 

rhiza-pro-final-fix-rereview-20…

하지만 current tar의 다음 경로는 임시 materializer의 DB handles만 기존 materializer로 옮긴 후 donor를 닫지 않습니다.

Go
restored, err := openMaterializer(...)
m.db, m.writer, m.readers, m.graph =
    restored.db, restored.writer, restored.readers, restored.graph
restored.db, restored.writer, restored.readers, restored.graph =
    nil, nil, nil, nil
return nil

reopen()도 동일한 ownership transfer를 수행합니다.

따라서 donor의 dispatcher goroutine은 자신의 notifyStop 또는 notifyQueue를 기다리며 영구 생존하고, goroutine이 donor Materializer를 참조하므로 다음도 회수되지 않습니다.

notifyQueue

notifyStop

subscriber map

WaitGroup과 기타 materializer metadata

Live object-storage checkpoint restore가 같은 process에서 반복될 때마다 하나씩 누적됩니다. Restore rollback의 reopen() 경로에서도 같은 누수가 발생합니다.

최소 수정 방향

Handles를 nil로 옮긴 뒤 donor를 닫으면 됩니다.

Go
restored.db, restored.writer, restored.readers, restored.graph = nil, nil, nil, nil
_ = restored.Close()

또는 openMaterializerResources와 dispatcher start를 분리해 transplant용 materializer는 worker를 시작하지 않게 해야 합니다.

필수 테스트:

- RestoreCheckpoint를 같은 Materializer에 반복 실행
- rollback/reopen 경로 반복
- active notification dispatcher 수가 1로 유지되는지 확인
- Close 이후 모든 dispatcher가 종료되는지 확인
[P2] 5. WAL compaction live-set이 logged=false인 decided hint의 proposal을 보존하지 않는다

위치

pkg/quepaxa/compaction.go

(*Core).liveProposalHashesAboveLocked

pkg/quepaxa/core.go

(*Core).acceptDecision

(*Core).AcceptCertifiedHints

Helper의 주석은 WAL에 도달하지 않은 local decision의 proposal을 보존해야 한다고 명시합니다. 그러나 구현은 다음만 수행합니다.

logged=true인 retained decision hash를 별도 set에 넣음

현재 c.recorders가 참조하는 proposal만 keep

logged=false인 c.decided hash를 keep에 직접 넣지 않음 

rhiza-pro-final-fix-rereview-20…

AcceptCertifiedHints는 decision을 local decision WAL marker 없이 설치합니다. acceptDecision은 decision 설치 시 해당 slot의 in-memory recorder state를 삭제합니다.

따라서 다음 상태가 가능합니다.

WAL:
  EntryProposal(hash H, value V)
  EntryReceipt(slot S, hash H)

memory:
  decided[S] = V
  logged[S] = false
  recorders[S] 없음    // acceptDecision이 삭제

더 낮은 checkpoint를 통해 compaction하면:

liveProposalHashesAboveLocked는 H를 keep하지 않음

EntryProposal(H) 삭제

EntryReceipt(S,H)는 floor 위이므로 유지

EntryDecide(S,V)는 원래 없으므로 유지할 것이 없음

Crash 후 WAL은 hash를 참조하는 receipt만 보유하고 value를 잃습니다. 복구는 원래 로컬 WAL만으로 가능했던 proposal을 peer FetchValue에 의존하게 됩니다. Peer fetch가 일시 실패하거나 필요한 peer가 unavailable하면 node rejoin이 실패합니다.

최소 수정 방향

Unlogged retained decision은 직접 keep해야 합니다.

Go
for slot, value := range c.decided {
    if slot <= through {
        continue
    }
    if !c.logged[slot] {
        keep[value.Hash] = struct{}{}
        continue
    }
    decided[value.Hash] = struct{}{}
}

logged=true인 decision만 EntryDecide가 payload를 대신하므로 raw proposal 중복 제거 대상입니다.

필수 테스트:

- proposal Record로 Proposal+Receipt를 WAL에 기록
- 동일 slot을 AcceptCertifiedHints로 결정하여 logged=false 및 recorder map 삭제
- 더 낮은 checkpoint로 compaction
- close/reopen
- peer FetchValue를 실패시키더라도 local proposal value가 복구되어야 함
[P2] 6. DB.Close 후 Graph embedded API가 error가 아니라 nil dereference panic을 낸다

위치

rhiza.go

(*DB).Close

(*DB).GraphQuery

(*DB).RequestStatus

pkg/materializer/graph_enabled.go

(*Materializer).GraphQuery

(*Materializer).GraphMutationReceipt

(*Materializer).GraphRequestMatches

(*Materializer).graphRequestExists

DB.Close는 Node.Shutdown을 실행하지만 db.api handle은 그대로 보존합니다. 이후 exported methods는 계속 같은 Server를 호출합니다. 

rhiza-pro-final-fix-rereview-20…

Shutdown은 materializer의 graph를 닫고 m.graph=nil로 만듭니다. 그러나 GraphQuery는 다음과 같이 nil 확인 전에 g.tip을 읽습니다.

Go
g := m.graph
if g.tip != m.tip {
    ...
}

현재 Graph query 변경 hunk에도 g를 바로 사용해 read transaction을 시작하는 구조가 남아 있습니다. 

rhiza-pro-final-fix-rereview-20…

GraphMutationReceipt, GraphRequestMatches, graphRequestExists도 같은 방식으로 m.graph.mu를 직접 사용합니다.

따라서 다음 public API는 deterministic panic이 가능합니다.

Go
db.Close()
db.GraphQuery(...)
db.RequestStatus(ctx, RequestStatusRequest{
    Kind: "graph",
    ...
})

보존된 db.Handler()로 /graph/query를 호출해도 동일합니다.

SQL materializer에는 close 후 sql.ErrConnDone을 확인하는 테스트가 추가됐지만 Graph public path는 포함되지 않았습니다. 

rhiza-pro-final-fix-rereview-20…

최소 수정 방향

DB 수준에서 모든 public API에 하나의 atomic closed gate를 적용하는 것이 가장 일관적입니다.

Go
if db.closed.Load() {
    return ..., ErrNotReady // 또는 명시적 ErrClosed
}

그리고 Graph materializer 함수에도 방어적으로 다음 검사를 둬야 합니다.

Go
g := m.graph
if g == nil || g.db == nil {
    return ..., sql.ErrConnDone // 또는 graph closed error
}

필수 테스트:

- db.Close 이후 모든 SQL/KV/Graph/Notify/RequestStatus API 호출
- panic 없이 일관된 closed error
- 보존된 Handler의 모든 route도 panic 없이 503/closed response
- Close와 GraphQuery/RequestStatus 동시 race
최종 요약

직전 지적 중 QUIC 0-RTT 경계는 완전히 닫혔습니다.

Notification은 다음 개선은 유효합니다.

subscriber 수와 queue가 bounded

subscriber full 판단이 clone 이전

fan-out이 materializer write 경로 밖의 단일 dispatcher로 이동

drop count 노출

그러나 dispatcher overflow 이전 추가 clone과 restore/reopen worker leak 때문에 해당 P2는 완전히 닫히지 않았습니다.

현재 최종 판정은 다음과 같습니다.

P0: 0
P1: 2
P2: 4

가장 먼저 고칠 순서는 다음입니다.

1. checkpoint/PUBLISHER legacy format 및 maintenance lock 분리
2. recovery pin renewal을 전역 lock 밖의 직접 CAS로 변경
3. unlogged decided proposal을 WAL compaction live-set에 포함
4. notification pending clone 제거
5. restore donor dispatcher 종료
6. DB closed gate와 Graph nil guard 추가

