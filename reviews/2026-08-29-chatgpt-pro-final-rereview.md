# ChatGPT Pro final re-review

Source session: https://chatgpt.com/c/6a866d25-19e4-83e8-8a60-b18c208fa0c6

Requested at: 2026-08-29 14:44 KST

Completed at: 2026-08-29 16:07 KST

## Raw response

판정

요청 범위 안에서 재현 가능하거나 코드상 확정되는 P0/P1은 남아 있지 않습니다. P2는 2건 남아 있습니다.

Recovery certificate 서명·키 회전, peer mTLS 및 이에 종속되는 인증 구조는 판정에서 제외했습니다. 문서와 벤치 결과는 회귀 범위를 확인하는 보조 자료로만 사용했고, 아래 판정은 실제 호출·복구·동시성 경로를 기준으로 내렸습니다.

이전 재검토에서 지적했던 request-ID 충돌에 의한 materializer 정지, live checkpoint base 설치 후 suffix 미전진, unresolved recorder value 유실, checkpoint/GC publication 경쟁, slow-path value availability, compacted-history readiness 전이, quiesce drain 경쟁, snapshot metadata 불일치는 현재 코드에서 다시 지적할 상태가 아니었습니다.

### [P2] 느린 notification subscriber가 materializer apply 경로의 메모리·CPU를 비례 증폭시킨다

위치

- `pkg/materializer/materializer.go`
- `(*Materializer).Subscribe`
- `(*Materializer).publishNotification`
- `(*Materializer).ApplyBatch`

실패 시나리오

Notification payload는 최대 1MiB이고, 각 subscription은 깊이 64의 `chan []byte`를 가진다. Subscriber 수에 대한 전역 또는 topic별 상한과 queued-byte 상한은 없다.

`publishNotification`이 각 subscriber에게 payload 복사본을 보내는 구조라면 느린 subscriber 하나가 보유할 수 있는 payload backing memory가 최대 약 64MiB다. Subscriber가 여러 개면 이 상한은 선형 증가한다.

더 중요한 문제는 nonblocking send가 다음 형태일 때다.

```go
select {
case ch <- append([]byte(nil), payload...):
default:
}
```

`select` 진입 시 send의 RHS가 평가되므로, channel이 이미 가득 차서 `default`가 선택되더라도 payload 복사와 allocation이 먼저 일어난다. 즉, 큐가 가득 찬 subscriber도 이후 모든 publish마다 최대 1MiB의 일회성 allocation과 copy를 발생시킨다.

예를 들어 1MiB payload, 가득 찬 slow subscriber 100개, publish 1회라면 실제 enqueue는 하나도 되지 않더라도 약 100MiB의 transient allocation/copy가 발생할 수 있다. 반복 publish에서는 GC 부하와 CPU 사용량이 급증한다.

이 fan-out이 `ApplyBatch`의 materializer write critical section 안에서 실행되면 영향은 notification 경로에 국한되지 않는다.

```text
SQLite commit
→ notification fan-out/copy
→ graph journal confirmation
→ Materializer.mu 해제
```

Subscriber 수나 payload 크기에 따라 consensus decision의 local apply 완료 시간, 후속 mutation, read metadata 접근까지 지연될 수 있다.

최소 수정 방향

내부 queue를 `chan []byte`가 아니라 immutable notification reference로 바꾸고, payload backing storage는 publish당 한 번만 만들어 공유하는 편이 적절하다. 외부 `<-chan []byte` API에서 subscriber 간 mutable slice 공유를 허용할 수 없다면, subscriber가 실제 dequeue할 때 복사하도록 forwarding 계층을 둬야 한다.

최소한 다음 세 가지는 필요하다.

- Subscriber별 message count가 아니라 queued byte budget을 둔다.
- Topic별·전역 subscriber 수 상한을 둔다.
- Queue가 가득 찬 경우 payload를 복사하기 전에 drop 여부를 결정한다.

가능하면 SQLite·Graph 상태 commit과 pending notification 목록 확정까지만 `Materializer.mu` 아래에서 수행하고, 실제 fan-out은 materializer write lock을 해제한 뒤 실행해야 한다. 다만 기존의 graph-journal confirmation 실패 시 notification 누락 방지 순서는 유지해야 하므로, pending notification을 별도 durable/committed delivery state로 넘기는 방식이 안전하다.

필요한 회귀 테스트

- 1MiB payload
- 읽지 않는 subscriber N개
- subscriber channel을 모두 채움
- 추가 publish 반복
- allocations/op과 heap growth가 N × payload에 비례하지 않는지 확인
- publish 중 SQL/KV/Graph `ApplyBatch` latency가 subscriber 수에 비례하지 않는지 확인
- drop count 또는 gap 표시가 관측 가능한지 확인

### [P2] QUIC 0-RTT에서 Record가 재실행되면 동일 receipt가 WAL과 fsync를 반복 소비한다

위치

- `pkg/network/transport.go`
- `(*Transport).SendRecord`
- `(*Transport).call / callWithTimeout`
- `pkg/network/peer_server.go`
- `StartPeerServer`
- `(*PeerServer).handle`
- `pkg/quepaxa/core.go`
- `(*Core).Record`

이 항목은 peer identity나 mTLS 문제가 아니라 0-RTT replay에 대한 durable operation의 실행 의미이므로 제외 범위와 독립적이다.

실패 시나리오

Peer QUIC listener는 0-RTT를 허용한다. Transport가 `Record` RPC에 대해 handshake 완료를 기다리지 않으면 session resumption 시 `Record` frame이 early data로 전송될 수 있다.

`Propose`만 handshake completion을 요구하고 다음 durable/mutating operation이 early data로 허용되는 구조라면 경계가 일관되지 않다.

- `Record`
- `Learned`
- `StageValue`
- `PrepareCheckpoint`

이 중 `Record`가 가장 문제가 크다.

동일한 slot, step, proposal의 `Record`가 재전송되면 per-slot record lock 때문에 메모리 state corruption은 방지된다. 그러나 ISR.Record 결과가 기존 상태와 실질적으로 같아도 현재 경로가 매번 다음 작업을 수행하면:

```text
EntryReceipt WAL append
→ group commit wait
→ WAL fsync
→ response
```

동일 early-data replay가 WAL record와 fsync를 반복 소비한다.

inline-value 변경 이후 `Record` frame에는 최대 128KiB의 proposal value도 포함될 수 있으므로 다음이 함께 증가한다.

- peer ingress bytes와 FlatBuffer allocation
- SHA-256 및 value 검증
- duplicate receipt WAL bytes
- group-commit waiter와 fsync
- WAL compaction 대상과 startup scan 비용

반복 replay가 충분하면 `MaxWALBytes`에 도달해 정상 quorum의 recorder가 새 Record를 수락하지 못하는 availability failure로 이어질 수 있다.

`StageValue`, `Learned`, `PrepareCheckpoint`는 현재 상태 검사를 통해 대부분 idempotent하게 짧게 종료할 수 있지만, durable write operation을 0-RTT 허용 목록에 두는 정책 자체는 이후 코드 변경 때 같은 문제를 다시 만들기 쉽다.

최소 수정 방향

0-RTT 허용 operation을 명시적인 read-only allowlist로 제한하는 것이 가장 단순하다.

0-RTT 허용:

- `Sync`
- `ReadIndex`
- `FetchValue`

handshake 완료 후에만 허용:

- `Propose`
- `Record`
- `Learned`
- `StageValue`
- `PrepareCheckpoint`

구현 방법은 둘 중 하나면 충분하다.

- Transport에서 durable operation 호출 시 `waitHandshake=true`
- Peer server에서 operation을 분류하고, durable operation인데 `HandshakeComplete == false`이면 실행하지 않고 handshake 이후 재시도를 요구

추가 방어로 `Core.Record`에서 next ISR state가 현재 state와 동일하고 동일 proposal durability가 이미 확립된 경우에는 duplicate `EntryReceipt` append와 fsync를 생략할 수 있다. 이 최적화는 네트워크 retry에도 유효하다.

필요한 회귀 테스트

- resumption/early connection으로 동일 `Record`를 여러 번 전달
- 반환 Summary는 동일해야 함
- `EntryReceipt` 수와 WAL bytes가 1회 실행 이상 증가하지 않아야 함
- fsync 횟수가 replay 횟수에 비례하지 않아야 함
- mutating operation은 handshake 완료 전에 handler에 진입하지 않아야 함
- `Sync`, `ReadIndex`, `FetchValue`의 early-data fast path는 유지

### 나머지 영역 판정

다음 영역에서는 현재 코드 기준으로 추가적인 P0/P1/P2를 확정하지 못했습니다.

- 3-peer leaderless quorum의 fast/slow path
- `CompleteDecision`의 contiguous-prefix 조건
- checkpoint base 설치와 retained suffix 전진
- unresolved recorder proposal의 WAL 생존성
- WAL compaction 후 close/reopen 복구
- request-ID 동일 intent replay와 상이 intent conflict
- compacted peer history에서 object-storage recovery로의 전환
- recovery 중 proposal admission 및 apply quiescence
- checkpoint root·block publication과 GC pin
- archive head·extent generation 검증
- checkpoint/archive GC의 live object 판정
- SQL·KV·Graph payload와 AppliedSlot snapshot 일치
- S3 conditional conflict, missing probe 및 실제 failure의 계측 분리
- embedded API의 close/recovery 중 nil·closed connection 접근
- 정상 before-ACK archive publication의 불필요한 extent 재다운로드
- 결정된 suffix value와 unresolved proposal의 WAL 보존 구분

따라서 현재 남은 코드상 판정은 P0 0건, P1 0건, P2 2건이다.
