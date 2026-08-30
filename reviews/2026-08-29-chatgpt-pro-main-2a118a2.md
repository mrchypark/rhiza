# ChatGPT Pro 전체 코드 리뷰 — main@2a118a2

- Source: https://chatgpt.com/c/6a866d25-19e4-83e8-8a60-b18c208fa0c6
- Reviewed commit: `2a118a28294dae48edc0b7af0a54b02230f69fae`
- Completed: 2026-08-29 KST
- Note: 아래 내용은 외부 리뷰 원문이며, 구현 지시가 아니라 검증 대상 증거입니다.

https://github.com/mrchypark/rhiza 

메인 브랜치의 코드들을 모두 정교하게 검토하고 리뷰해줘. 문서들을 가능한한 배제하고 모두 코드를 검토해야 해. 제프딘 같은 뛰어난 프린시플엔지니어 로서 프로젝트의 본질을 이해하고 목표와 설계를 파악해서 장단점과 더 효율적이고 cpu, memory 사용량, 레이턴시, s3 api 사용량의 비용최적화 모두를 정교하게 고려해서 리뷰해줘. 리뷰 내용은 모두 파일을 만들지 말고 직접 출력해야 해.
Worked for 79m 57s
검토 기준

2026년 8월 29일 기준 GitHub의 실시간 main 브랜치 HEAD인 **2a118a28294dae48edc0b7af0a54b02230f69fae**를 기준으로 검토했다. 저장소 설명 문서나 과거 벤치마크 보고서보다는 다음의 실제 실행 코드에 집중했다.

공개 Go API와 실행 진입점

node 런타임 조립 및 복구

quepaxa 합의 코어

qlog WAL

materializer의 SQLite·KV·Graph 적용 경로

network의 HTTP·QUIC·배처

recovery의 S3 공유 decision archive

checkpoint의 S3 체크포인트

객체 저장소 추상화 및 계측

직렬화·프로토콜 경계

생성된 FlatBuffers 코드는 생성물 자체보다 schema·codec·호출 경계를 검토했다. 문서와 생성된 벤치마크 결과는 설계를 추정하는 주 근거로 사용하지 않았다.

총평

이 프로젝트의 본질은 “로컬 WAL과 결정론적 materializer를 빠른 실행 계층으로 두고, 고정 멤버십 합의를 통해 명령을 순서화하며, S3 호환 객체 저장소를 공유 복구 계층으로 사용하는 임베디드 분산 데이터베이스”다.

아키텍처 방향은 상당히 좋다.

특히 다음 판단이 정확하다.

SQLite나 LatticeDB를 진실의 원장으로 취급하지 않고 재구축 가능한 materialized state로 취급한다.

합의 WAL을 로컬 내구성의 기준으로 두고 SQLite를 synchronous=NORMAL로 운영한다.

체크포인트는 content-addressed immutable block으로 구성한다.

공유 archive는 immutable extent와 CAS 가능한 작은 mutable head로 구성한다.

요청 큐, 바이트 수, 합의 동시성, peer 동시성을 각각 제한한다.

catch-up 값에 certificate와 prefix-chain 검증을 적용한다.

체크포인트를 합의로 seal하기 전에 quorum replica가 실제 객체를 검증하게 한다.

손상된 materializer를 억지로 수리하지 않고 인증된 로그에서 재구축한다.

이는 단순한 프로토타입보다 훨씬 높은 수준의 설계다. 여러 파일에서 crash consistency, torn write, stale writer, concurrent publisher를 의식한 흔적이 보인다.

그러나 현재 상태에는 장기 운영 시 자원 회수가 실패하는 compaction 문제, S3 복구 데이터의 진위성을 암호학적으로 검증하지 못하는 신뢰 경계, 객체 저장소 GC의 선형 비용 증폭, materializer의 불필요한 slot별 SQL 작업, QUIC admission 및 0-RTT 경계가 남아 있다.

제 판단은 다음과 같다.

설계 골격은 강하지만, 현재 구현 그대로 다중 노드·공유 S3 프로덕션 시스템에 투입하기 전에 반드시 해결해야 할 P0/P1 문제가 있다.

가장 먼저 고쳐야 할 것은 성능 튜닝이 아니라 다음 네 가지다.

인증되지 않은 recovery certificate

WAL compaction의 live-value 계산 오류

peer 연결의 handshake 단계 인증과 0-RTT 제한

materializer에서 idempotency fingerprint 불일치를 조용히 무시하는 경로

1. 아키텍처 이해

실제 데이터 흐름은 대략 다음과 같다.

Client mutation
    ↓
HTTP / in-process API
    ↓
request-id validation and batching
    ↓
hedged proposer selection
    ↓
QuePaxa record quorum and decision
    ↓
local decision WAL durability
    ↓
SQLite / LatticeDB materialization
    ↓
(optional before-ACK)
shared S3 decision archive
    ↓
checkpoint creation, quorum verification, seal decision
    ↓
local WAL + shared archive compaction

복구는 반대 방향이다.

local WAL checkpoint base
    +
S3 certified checkpoint
    +
S3 decision archive suffix
    +
local recorder state
    ↓
reconstruct consensus prefix
    ↓
restore/replay materializer
    ↓
quorum catch-up
    ↓
ready

이 구조가 좋은 이유는 로컬 처리 레이턴시와 원격 저장소 비용을 분리할 수 있기 때문이다.

async 모드에서는 S3가 foreground ACK 경로에 없다.

before-ack 모드에서는 필요한 사용자만 S3 복구 내구성을 ACK 계약에 포함할 수 있다.

체크포인트 블록은 내용 기반 중복 제거가 가능하다.

immutable extent는 S3의 약한 rename semantics에 의존하지 않는다.

Node.Open의 복구 순서도 대체로 안전하다. WAL과 archive를 먼저 확립하고, materializer의 tip을 인증된 decision과 대조한 뒤 suffix를 적용하고, 다중 노드는 quorum catch-up 전까지 공개 mutation을 막는다.

2. 반드시 먼저 수정해야 할 문제
P0-1. S3 recovery certificate에 암호학적 서명이 없다
현재 동작

합의 certificate는 다음 정보를 JSON으로 저장한다.

config ID

slot과 step

proposal priority, proposer ID, hash

recorder ID별 summary

하지만 recorder가 해당 summary를 실제로 생성했다는 서명 또는 MAC이 없다.

복구 검증은 다음을 확인한다.

recorder가 cluster member인지

recorder ID가 중복되지 않았는지

quorum 수를 만족하는지

step과 proposal 형태가 QuePaxa 규칙에 맞는지

value hash가 일치하는지

즉, certificate의 문법과 내부 일관성은 검증하지만 출처의 진위는 검증하지 않는다.

결과

객체 저장소가 단순히 손상되는 경우에는 SHA-256, CRC, prefix chain이 잘 방어한다. 그러나 S3 write 권한을 가진 공격자나 잘못된 내부 서비스는 다음을 만들 수 있다.

임의의 application command

그 값에 맞는 proposal hash

존재하는 member ID를 나열한 가짜 quorum certificate

그 certificate를 담은 유효한 archive extent

일관된 prefix hash와 head

필요하면 가짜 checkpoint root와 seal

모든 hash를 다시 계산하면 현재 검증을 통과할 수 있다.

이는 “S3가 완전히 신뢰된 단일 보안 영역”이라는 매우 강한 가정을 숨겨 둔 것이다. 특히 backup bucket replication, 운영자 권한, CI credential, S3-compatible vendor까지 포함하면 위험한 가정이다.

수정안

각 cluster member에 독립된 장기 Ed25519 공개키를 cluster configuration에 넣어야 한다.

각 recorder는 다음 canonical record에 서명해야 한다.

domain_separator
cluster_id
config_id
slot
step
recorder_id
first_current proposal ref
aggregate_prior proposal ref

certificate에는 각 summary와 서명을 포함하고, 다음 지점에서 검증해야 한다.

peer RPC 수신 시

AcceptDecision

AcceptCertifiedValues

WAL recovery

S3 archive recovery

checkpoint seal recovery

현재 peer TLS private key는 token으로 결정론적으로 생성된다. 이 키를 그대로 archive 서명에 재사용하는 것은 권하지 않는다. 공유 admin token이나 노드 token 노출 시 다른 key material을 유추할 수 있는 구성이 될 수 있기 때문이다. 독립된 node signing key와 rotation 가능한 config epoch를 두는 편이 맞다. Peer 인증용 key와 durable-log 인증용 key도 분리해야 한다.

P0-2. 합의 WAL compaction이 제거해야 할 proposal payload를 다시 보존한다

CompactThrough는 compacted WAL에 남길 proposal hash의 집합을 다음과 같이 만든다.

Go
keep := make(map[[32]byte]struct{}, len(c.values))
for hash := range c.values {
    keep[hash] = struct{}{}
}

즉, 현재 c.values에 존재하는 값을 모두 보존한다. 그 뒤 WAL rewrite와 commit이 끝난 다음에야 installBaseLocked가 floor 이하에서 더 이상 참조하지 않는 값을 메모리에서 제거한다.

문제는 이미 compact된 proposal payload까지 새 WAL에 다시 쓰인다는 것이다.

장기적인 결과

checkpoint compaction 직후 현재 프로세스의 메모리에서는 오래된 values가 지워진다.

하지만 compacted WAL에는 오래된 proposal entry가 남는다.

재시작하면 WAL recovery가 해당 proposal들을 전부 c.values에 다시 적재한다.

recovery 말미에는 결정/recorder state에서 도달 불가능한 value를 mark-and-sweep하지 않는다.

다음 compaction도 다시 전체 c.values를 keep한다.

따라서 장기적으로 다음이 발생할 수 있다.

WAL 크기가 checkpoint 후에도 기대만큼 줄지 않는다.

startup WAL scan 시간이 계속 증가한다.

재시작 직후 메모리 사용량이 계속 증가한다.

MaxWALBytes에 더 빨리 도달한다.

compaction 자체의 디스크 read/write 비용이 커진다.

수정안

keep은 모든 c.values가 아니라 새 floor 이후에도 실제로 필요한 hash만 포함해야 한다.

1. decided slot > through 인 모든 decision hash
2. unresolved recorder state가 참조하는 proposal hash
3. 복구 중인 기타 명시적 durable reference

그리고 recovery 말미에도 동일한 mark-and-sweep을 수행해야 한다.

반드시 다음 회귀 테스트를 추가해야 한다.

- 큰 payload로 N개 slot 결정
- checkpoint seal
- CompactThrough
- WAL close/reopen
- values map의 live count 확인
- WAL byte 수가 retained suffix 크기에 비례하는지 확인
- 이 과정을 여러 세대 반복

이 문제는 가장 먼저 수정할 가치가 있다. correctness를 즉시 깨지는 않더라도 장기 운영 가능성을 직접 훼손한다.

P0-3. peer 인증이 QUIC handshake가 아니라 application request 이후에 일어난다

QUIC listener는 최대 64개 연결과 1,024개 stream을 전역으로 허용한다. 연결을 받은 뒤 각 request frame을 읽고 나서야 sender ID와 token을 검증한다.

즉, token을 모르는 외부 주체도 다음 자원을 선점할 수 있다.

64개의 peer connection slot

1,024개의 stream slot

각 stream의 최대 30초 deadline

TLS·QUIC handshake CPU

frame buffer와 goroutine

peer UDP port가 네트워크상 노출되면 저비용 availability 공격이 가능하다.

수정안

현재 client가 server key를 pinning하는 것처럼 server도 client certificate public key를 handshake에서 검증하는 mTLS로 바꿔야 한다.

cluster config에 노드별 peer public key

ClientAuth: RequireAnyClientCert

VerifyConnection에서 exact public-key pin

handshake 성공 시 authenticated node ID를 connection context에 binding

이후 request의 SenderId가 connection identity와 일치하는지만 확인

노드별 connection·stream quota 적용

이렇게 하면 application frame을 읽기 전에 비인가 연결을 거부할 수 있다.

P0-4. Graph와 달리 SQL/KV/Notify replay는 동일 request ID의 fingerprint 불일치를 fatal로 취급하지 않는다

API ingress에서는 request ID가 이미 존재할 경우 fingerprint를 비교한다. 그러나 materializer의 실제 apply 경로에서는 기존 receipt를 찾으면 다음과 같이 처리한다.

SQL: 기존 record가 있으면 command를 건너뜀

KV: 기존 record가 있으면 command를 건너뜀

Notify: 기존 fingerprint가 다르면 publish만 하지 않음

즉, 인증된 로그나 recovery input 안에 동일 request ID와 다른 내용이 들어오면 오류를 발생시키지 않고 조용히 무시한다. Graph 구현은 이 경우 명시적으로 오류를 반환한다.

admission layer의 검사는 신뢰 경계가 아니다. materializer가 마지막 결정론적 실행 경계여야 한다.

수정안

모든 mutation 종류에서 다음 invariant를 강제해야 한다.

request ID가 존재하지 않음
    → 실행 후 fingerprint와 receipt 저장

request ID가 존재하고 fingerprint가 동일
    → exact replay, 기존 결과 사용

request ID가 존재하고 fingerprint가 다름
    → fatal deterministic corruption/conflict

복구 중 이런 오류가 나면 노드는 ready가 되어서는 안 된다.

3. S3 API 사용량과 비용 최적화
3.1 현재 before-ACK archive publish의 요청 수

경합이 없는 일반적인 archive batch 한 번은 대략 다음 순서를 가진다.

immutable extent PUT

conditional head PUT

head HEAD

head GET

head HEAD

따라서 보통 batch당 5개의 S3 요청이 발생한다. readStableHead가 변경 전후 version을 대조하기 때문이다.

이 검증 자체는 generic S3-compatible store에서 안전한 선택이다. 단순히 제거해서는 안 된다. 하지만 AWS S3 또는 ETag/version을 반환하는 저장소에서는 더 효율적으로 만들 수 있다.

가장 큰 비용 절감안: conditional PUT 결과에서 새 version을 받기

현재 generic objstore.Bucket.Upload abstraction은 성공한 PUT의 새 ETag/version을 반환하지 않는다. 그 때문에 방금 쓴 head의 version을 알아내려고 HEAD/GET/HEAD를 다시 수행한다.

S3 전용 fast path를 추가해 다음을 반환하도록 한다.

Go
type PutResult struct {
    ETag      string
    VersionID string
}

성공한 conditional PutObject가 새 version을 반환하면 uncontended writer는 다음만 하면 된다.

extent PUT

conditional head PUT, 새 version 확보

로컬 headCAS를 바로 갱신

즉, 정상 경로를 5회에서 2회로 줄일 수 있다.

API request 수 약 60% 감소

순차적 원격 RTT 3개 제거

before-ACK p50/p99 latency 개선

SDK retry 기회 감소

generic store에서는 현재 strict readback path를 유지하고, capability로 분기하면 된다.

3.2 immutable extent PUT에도 If-None-Match를 사용해야 한다

extent key는 내용 hash로 결정된다. 같은 extent는 같은 key를 가진다. 그런데 archive는 extent를 unconditional upload한다. CAS head 충돌이나 동시 publisher가 있으면 동일 extent를 다시 올릴 수 있다.

다음으로 바꿔야 한다.

PUT archive/blocks/<sha256>.bin
If-None-Match: *

412는 정상 dedup hit로 처리한다.

이는 특히 다음에서 도움이 된다.

여러 노드가 같은 suffix를 동시 publish

head CAS conflict retry

네트워크 timeout 후 결과 불확실 재시도

archive GC가 동일 compacted block을 재생성

3.3 archive GC가 전체 retained chain을 반복 재작성한다

Cleanup은 여러 extent가 있으면 전체 retained extent를 읽어 다시 최대 크기로 pack한다.

모든 ref를 읽는다.

모든 decision을 재조립한다.

compacted extent를 전부 다시 upload한다.

head를 갱신한다.

새 verifier manager를 만들어 chain을 다시 load한다.

obsolete objects를 순회한다.

이 방식은 안전하지만, 새 작은 tail extent가 조금 추가될 때마다 오래된 안정된 prefix까지 다시 읽고 쓸 수 있다.

수정안: incremental tail compaction

다음 invariant를 이용할 수 있다.

이미 최대 크기나 최대 item 수에 가까운 immutable prefix extent는 다시 pack해도 결과가 바뀌지 않는다.

실제로 merge 가치가 있는 부분은 마지막 몇 개의 작은 tail extent다.

checkpoint trim으로 첫 extent가 partial이 된 경우 first boundary extent도 대상이다.

따라서 다음만 재작성한다.

[optional partial first extent]
+
[last compactable tail extents]

중간의 안정된 full extent는 hash와 previous chain을 그대로 유지한다.

더 좋은 형태는 extent format에 generation-independent predecessor 대신 Merkle/index root를 사용하는 것이지만, 현재 설계를 유지하면서도 incremental tail compaction으로 대부분의 증폭을 제거할 수 있다.

3.4 live archive block마다 marker DELETE를 수행한다

archive GC는 object마다 별도의 gc-candidates/<hash> marker를 사용한다. 현재 head에서 reachable한 live block을 발견할 때마다 marker를 Delete한다. marker가 없더라도 요청은 발생한다.

따라서 안정된 정상 상태에서도 GC 한 번에 대략 다음 비용이 생긴다.

O(number of live extents) DELETE requests

live extent가 많을수록 “아무것도 지우지 않는 GC”가 비싸진다.

수정안

체크포인트 GC가 이미 사용하는 접근처럼 IterWithAttributes와 LastModified를 활용하는 편이 낫다.

1. 현재 reachable hash set 작성
2. blocks prefix를 LIST with attributes
3. reachable이면 아무 API 호출도 하지 않음
4. unreachable이고 LastModified가 grace 이전이면 DELETE

스토어가 list timestamp를 제공하지 않으면 다음 대안이 있다.

하나의 generation-level candidate manifest

S3 Inventory + Batch Operations

lifecycle tag

bucket versioning과 noncurrent-version lifecycle

object별 marker는 object count를 두 배 가까이 늘리고 request 수도 크게 증가시킨다.

3.5 checkpoint GC는 매번 모든 root를 GET하고 각 root를 HEAD한다

GarbageCollect는 다음 순서를 가진다.

roots prefix LIST

모든 root object GET 및 검증

각 root의 age 확인을 위한 Attributes/HEAD

block prefix LIST

stale block DELETE

root 수가 checkpoint 역사에 비례해 늘어나면 GC 비용도 선형 증가한다.

개선

root 파일명에 index와 root hash가 이미 있으므로 다음 순서가 더 효율적이다.

roots를 IterWithAttributes로 LIST

index와 age만으로 유지 후보를 결정

유지되는 root와 실제 제거 대상 root만 GET

live block set 구축

stale root와 block 삭제

오래된 checkpoint root 전부를 매 GC마다 재검증할 필요는 없다.

3.6 checkpoint publisher lease가 S3 request를 과도하게 쓴다

publisher claim read는 안정화 확인을 위해 HEAD → GET → HEAD를 수행한다. Acquire, Bind, Renew가 각각 read와 conditional PUT, 때로는 다시 read를 수행한다. 2분 lease를 40초마다 갱신하므로 큰 checkpoint upload가 길어질수록 control-plane 요청이 누적된다.

앞서 제안한 “conditional PUT 결과 version 반환”이 여기에도 적용된다.

acquire PUT 성공 결과로 claim version 확보

bind/renew PUT 결과로 새 version 확보

같은 writer가 불필요하게 다시 claim을 GET하지 않음

안정화 read는 fencing conflict나 process recovery 때만 수행

3.7 archive extent 압축

archive certificate는 JSON이고 proposal·summary field 이름 및 hex/array 표현 때문에 압축 효율이 좋을 가능성이 높다. Extent를 deterministic zstd level 1 정도로 압축하면 다음이 줄어들 수 있다.

S3 upload/download bytes

S3 storage

network latency

extent cache memory

다만 canonical hash가 compressed bytes를 기준으로 계산되므로 format version을 추가해야 한다. CPU가 병목인 환경에서는 손해일 수 있으므로 다음을 실제 측정해야 한다.

archive bytes saved / compression CPU ns / allocation / p99 publish latency

체크포인트 DB block은 데이터 성격에 따라 다르므로 archive부터 적용하는 것이 낫다.

4. CPU 최적화
4.1 materializer batch 안에서 slot마다 동일 maintenance SQL을 반복한다

ApplyBatch는 최대 256개 decision을 하나의 SQLite transaction으로 처리한다. 좋은 설계다. 그러나 applyValueLocked는 각 slot마다 다음을 실행한다.

idempotency receipt prune DELETE

applied_slot UPSERT

applied_hash UPSERT

state mutation이면 state_slot UPDATE

256-slot replay라면 한 transaction 안에서도 수백 개의 불필요한 B-tree lookup과 statement execution이 발생한다.

수정안

ApplyBatch에서 다음을 계산한다.

Go
finalSlot
finalHash
finalStateSlot
pruneFloor

그리고 batch가 끝날 때 한 번만 실행한다.

DELETE idempotency below pruneFloor       // 필요할 때만
UPDATE applied_slot = finalSlot
UPDATE applied_hash = finalHash
UPDATE state_slot = finalStateSlot        // 변경된 경우

receipt prune은 모든 batch가 아니라 예를 들어 1,024 slot마다 실행하거나, 마지막 prune floor를 metadata에 저장해 일정 delta 이상일 때만 수행해도 된다.

이 변경은 정상 mutation latency뿐 아니라 다음을 크게 개선한다.

startup replay

archive catch-up

node rejoin

checkpoint 이후 suffix replay

4.2 KV expiry 정리를 매 mutation마다 수행한다

모든 KV command가 최대 256개 expired row를 삭제한다. write-heavy workload에서는 매 command마다 partial index scan과 delete가 붙는다.

더 나은 방법은 다음 중 하나다.

apply batch당 한 번

일정 slot 간격당 한 번

별도 low-priority local maintenance

expired row count 또는 oldest expiry를 기반으로 adaptive 실행

읽기 쿼리가 expiry predicate를 적용하므로 cleanup이 즉시 실행되지 않아도 논리적 정확성은 유지된다.

4.3 query 결과를 여러 번 직렬화한다

SQL query는 결과의 대략적 byte budget을 세면서 row를 materialize하고, 그 후 encodedJSONSize가 전체 결과를 다시 JSON encoding한다. HTTP adapter가 최종 응답을 또 encoding한다.

최대 16MiB 결과에서 다음이 발생할 수 있다.

값 스캔과 객체 생성

size 계산용 JSON 전체 순회

실제 응답용 JSON 전체 순회

여러 임시 slice와 interface allocation

수정안

응답을 한 번만 encode해야 한다.

제한된 counting writer와 실제 response writer를 결합

또는 pooled buffer에 한 번 encode한 뒤 크기를 확인하고 write

in-process API는 typed result를 유지하되 HTTP API는 streaming encoder 경로 분리

row 단위 streaming API를 별도로 제공하면 큰 query 결과의 peak memory도 줄일 수 있다.

4.4 row마다 scan slice 두 개를 새로 할당한다

collectRowsWithBudget는 각 row마다 다음을 새로 만든다.

Go
values := make([]any, len(columns))
pointers := make([]any, len(columns))

최대 10,000행에서 GC pressure가 커진다.

column 수가 고정되므로 scan buffer와 pointer slice를 재사용하고, 결과에 append할 때 필요한 값만 copy하는 편이 낫다.

4.5 graph recovery journal append가 batch 내에서 O(n²)가 될 수 있다

Graph build는 각 slot을 적용할 때 journal 전체를 다음처럼 처리한다.

metadata에서 journal 전체 읽기

전체 decode

새 entry append

journal 전체 encode

metadata에 다시 저장

ApplyBatch의 마지막에 SQLite commit이 성공한 뒤 journal을 정리한다. 따라서 한 batch에서 graph slot이 많으면 journal 크기가 1, 2, 3, …, N으로 증가하며 반복 복사된다.

수정안

가장 좋은 방법은 materializer에 graph용 batch API를 추가해 여러 command를 한 LatticeDB transaction 또는 batch journal append로 처리하는 것이다.

최소 변경으로는 journal을 다음처럼 나눌 수 있다.

journal_start_slot
journal_entries append-oriented blob/pages

매 slot마다 전체 blob을 재작성하지 않도록 해야 한다.

4.6 peer sync 응답 크기를 찾기 위해 전체 FlatBuffer를 반복 생성한다

Peer server는 최대 frame에 들어가는 decision 수를 이진 탐색한다. 각 probe에서 encodePeerResponse를 호출해 전체 FlatBuffer를 새로 생성한다. 최대 128개 decision에 대해 여러 번 1MiB에 가까운 직렬화·할당이 발생할 수 있다.

수정안

각 encoded decision 크기를 한 번 계산

envelope overhead를 계산

prefix cumulative size로 fit count 결정

최종 response만 한 번 pack

또는 FlatBuffers object API 대신 table builder에 직접 append하며 size cap에서 멈춰야 한다.

5. 메모리 최적화
5.1 notification subscriber는 무제한이고 payload를 subscriber마다 복제한다

각 subscription channel의 capacity는 64다. publish 시 동일 topic의 각 subscriber에게 payload를 새로 복사한다. payload는 최대 1MiB다.

이론적 상한은 subscriber 한 명당 약 64MiB queued payload다. subscriber 수에도 전역 제한이 없다.

SSE client가 느리거나 읽지 않으면 channel이 찬 뒤 drop되지만, 그 전까지 큰 메모리를 차지한다.

수정안

topic별 subscriber index를 둬 전체 subscriber scan 제거

immutable reference-counted payload 또는 shared byte slice 전달

subscriber별 byte budget

전체 subscriber 수와 topic별 수 제한

큐 capacity를 message 수가 아니라 byte 수로 제한

drop counter와 last-gap marker 노출

cancellation 시 channel close 또는 명시적 lifecycle state

SSE write와 flush error도 확인해야 한다. 현재 write 실패 후에도 루프가 계속될 수 있다.

5.2 archive extent decode가 raw object와 decoded 복사본을 동시에 가진다

최대 extent는 8MiB다. readObject가 전체 raw bytes를 읽고, decodeExtent가 각 value와 certificate를 다시 복사한다. 순간적으로 raw 8MiB와 decoded payload가 동시에 존재한다. cache에는 최대 2 extent가 유지된다.

catch-up concurrency까지 고려하면 peak memory가 예상보다 커질 수 있다.

개선

immutable raw backing buffer를 extent가 소유

decision value/certificate가 raw buffer의 subslice를 참조

외부 API로 반환할 때만 필요한 범위 copy

동일 hash concurrent load를 singleflight

cache를 단순 2개가 아니라 byte-budget LRU로 구성

다만 현재 DecisionsFrom 반환값이 caller에 의해 변경될 가능성을 고려해 ownership contract를 명확히 해야 한다.

5.3 WAL scan도 payload를 이중 복사한다

WAL scan은 segment arena로 record를 읽고, DecodeEntry가 payload를 별도 slice로 복사한다. 큰 WAL startup에서 allocation과 GC가 증가한다.

ScanView처럼 callback lifetime 동안만 유효한 read-only view API를 만들고, 장기 보관이 필요한 entry만 caller가 copy하도록 하는 편이 낫다.

5.4 checkpoint upload는 최대 약 256MiB의 병렬 read working set을 만든다

64MiB block을 최대 4개 병렬로 S3 SDK에 전달한다. SDK가 streaming을 유지하더라도 OS page cache와 SDK buffering을 합치면 전면 query workload와 경쟁할 수 있다. 체크포인트를 만들기 전에 파일 전체를 한 번 hash하고, 이후 다시 전부 읽어 upload하므로 로컬 disk traffic은 최소 2배다.

수정안

가장 실용적인 방향은 리소스 governor다.

block size 8~32MiB 검토

upload concurrency를 disk latency와 network throughput에 따라 1~4 사이 조정

Linux posix_fadvise(DONTNEED) 또는 sequential hint

checkpoint I/O bandwidth limiter

foreground WAL fsync/query latency가 상승하면 자동 throttle

이전 certified root의 block hash를 활용해 변경 가능성이 높은 영역만 우선 처리

단일 pass hash-and-upload는 conditional dedup 판단을 위해 hash가 먼저 필요하므로 완전한 streaming은 어렵다. 임시 block spool 또는 rolling content-defined chunking은 복잡성을 크게 늘리므로, 먼저 bandwidth와 concurrency 제어가 낫다.

5.5 SnapshotAt은 전체 DB를 메모리에 반환할 수 있다

SnapshotAt은 bytes.Buffer로 전체 SQLite snapshot을 수집한다. DB 크기가 커지면 프로세스 OOM 위험이 있다. 스트리밍 API인 SnapshotTo와 파일 기반 checkpoint API가 이미 있으므로, 메모리 반환 API에는 명시적인 size cap을 두거나 deprecated 처리하는 편이 안전하다.

6. 레이턴시 최적화
6.1 embedded API의 HedgeDelay=0은 모든 proposer를 즉시 실행한다

CLI는 기본 hedge delay를 5ms로 설정한다.

반면 rhiza.Open은 빈 profile·address 등은 defaulting하지만 HedgeDelay의 0값은 그대로 전달한다.

Server는 member마다 goroutine을 만들고 rank * hedgeDelay 후 proposal을 시작한다. 따라서 embedded 사용자가 값을 설정하지 않으면 모든 member가 동시에 실작업을 시작한다.

결과는 다음과 같다.

동일 client mutation에 대해 여러 합의 시도

record RPC와 WAL 작업 증폭

CPU와 네트워크 증가

slot contention 가능성

장애 시 tail latency 개선 대신 정상 시 부하 급증

수정안

library layer에서 안전한 기본값을 강제해야 한다.

Go
if config.HedgeDelay == 0 {
    config.HedgeDelay = 5 * time.Millisecond
}

다만 “명시적으로 zero hedge를 원함”과 “미설정”을 구분하려면 pointer/optional config 또는 별도 EagerHedging 필드가 필요하다.

더 나은 방법은 adaptive hedge다.

primary proposer 시작
p95 observed proposer latency 또는 최소 2~5ms 후 1개 hedge
그 후에도 완료되지 않으면 최대 1개 추가

모든 member goroutine을 미리 만들지 말고 동시 hedge 수를 2 정도로 제한하는 편이 좋다.

6.2 before-ACK는 S3 API 수뿐 아니라 순차 RTT가 문제다

앞서 설명한 5-call 경로에서 extent PUT과 head PUT, HEAD/GET/HEAD 안정화가 순차적으로 이어진다. 같은 region에서 요청당 수 ms만 걸려도 p50에 큰 고정 비용이 추가되고 p99는 retry와 함께 급격히 늘어난다.

다음 순서가 효과가 가장 크다.

PUT 결과 version을 받아 readback 제거

SDK retry budget을 client deadline과 연동

immutable extent conditional dedup

batch delay를 arrival rate에 맞게 adaptive 조정

archive batch size와 mutation batch size를 공동 관찰

현재 2ms group delay는 합리적인 시작값이지만 고정값이다. 낮은 트래픽에서는 2ms가 그대로 latency가 되고, 높은 트래픽에서는 더 큰 batch가 비용상 유리할 수 있다.

6.3 QUIC stream 오류가 전체 connection 재연결로 확대될 수 있다

transport는 다음 오류에서 connection을 invalidate한다.

OpenStreamSync 오류

frame write 오류

stream close 오류

frame read 오류

이 중 상당수는 stream deadline, stream cancellation, stream limit 같은 connection 자체가 살아 있는 오류일 수 있다.

한 요청의 timeout이 persistent QUIC connection 폐기와 TLS/QUIC handshake 재수립으로 번지면 부하가 높을수록 connection churn이 악화된다.

수정안

다음일 때만 connection을 invalidate한다.

conn.Context().Err() != nil

QUIC transport/application connection close error

peer identity 또는 protocol framing의 connection-wide corruption

명시적인 idle/handshake failure

request context timeout이나 stream reset은 해당 stream만 종료해야 한다.

6.4 single request ID stripe lock가 최대 30초간 unrelated 요청을 막는다

request ID는 SHA-256 첫 byte로 256개 mutex에 매핑된다. lock은 receipt 확인부터 합의와 materialization이 끝날 때까지 유지된다.

서로 다른 request ID라도 같은 stripe면 최대 30초까지 head-of-line blocking이 발생한다.

수정안

bounded keyed singleflight를 사용한다.

requestID → in-flight call
동일 ID만 합류
완료 후 즉시 map에서 제거
전체 entry 수 제한

이미 proposal value hash에 대해서는 유사한 inflight dedup 구조가 있으므로 같은 패턴을 request ID에도 적용할 수 있다.

6.5 NotifyPublish는 mutation batcher를 우회한다

SQL, graph, KV는 batching되지만 notification은 직접 proposeHedged를 호출한다. 작은 notification이 많은 workload에서는 합의 round와 before-ACK archive batch가 지나치게 잘게 쪼개질 수 있다.

Notify batch wire format을 추가해 SQL/KV와 같은 adaptive batcher를 사용하는 것이 좋다.

6.6 SQLite WAL checkpoint를 명시적으로 관리하지 않는다

SQLite writer는 WAL mode와 synchronous=NORMAL을 사용한다. 올바른 선택이지만 WAL autocheckpoint의 기본 동작에 의존하면 어느 request가 checkpoint 비용을 떠안는지 불명확해진다.

다음 metric과 제어가 필요하다.

SQLite WAL pages/bytes

checkpoint duration

busy reader count

frames checkpointed

longest reader transaction

foreground apply latency

자동 threshold가 아니라 low-priority background passive checkpoint와 주기적 truncate checkpoint를 분리하는 것이 p99 안정성에 도움이 된다.

7. 네트워크 및 보안
7.1 0-RTT 허용 범위가 너무 넓다

Peer server는 Allow0RTT: true다. Propose만 TLS handshake complete를 확인해 early data를 거부한다. 그러나 다음 operation도 replay되거나 비용이 큰 mutation이다.

Record

Learned

StageValue

PrepareCheckpoint

이들은 별도 early-data 검사가 없다.

QuePaxa operation 일부가 논리적으로 idempotent하더라도 replay가 WAL append, fsync, object verification, materializer apply 같은 비용을 재발생시킬 수 있다.

권장

가장 안전한 초기 선택은 0-RTT 비활성화다.

0-RTT가 실질적으로 필요한 경우에만 다음 read-only operation으로 제한한다.

Sync

ReadIndex

FetchValue

그리고 request nonce 또는 operation별 replay policy를 명시해야 한다.

7.2 public HTTP API에 인증이 없다

Peer plane에는 token과 TLS pinning이 있지만 public HTTP mutation/query API에는 인증 계층이 보이지 않는다. 기본 bind는 loopback이지만 production에서 0.0.0.0으로 바꾸는 순간 누구나 SQL/KV/graph mutation, notification, metrics를 호출할 수 있다.

제품이 “애플리케이션이 앞단에서 인증하는 embedded engine”을 목표로 한다 해도 HTTP adapter에는 다음이 필요하다.

explicit unsafe flag 없이 non-loopback bind 금지

bearer/mTLS middleware hook

metrics endpoint 분리 또는 제한

per-client rate limit

tenant/request-size quota

audit identity 전달

7.3 HTTP slowloris 방어가 불충분하다

서버에는 ReadTimeout, WriteTimeout, IdleTimeout은 있지만 ReadHeaderTimeout과 MaxHeaderBytes가 명시되지 않는다.

추가할 값의 예시는 다음과 같다.

Go
ReadHeaderTimeout: 5 * time.Second
MaxHeaderBytes:    32 << 10

SSE endpoint는 write deadline을 해제하므로 연결 수 제한과 per-client idle 정책도 별도로 필요하다.

7.4 peer frame 경로에서 payload copy가 많다

현재 경로는 대략 다음 복사를 가진다.

application value clone

FlatBuffers object API의 Pack

FinishedBytes를 다시 append-copy

writePeerFrame이 header+payload 통합 buffer로 다시 copy

수신 측 frame buffer

UnPack object copy

proposal/value copy

최대 1MiB frame에서 CPU와 allocator 부담이 크다.

개선

FlatBuffers builder pool

object API 대신 table view/direct builder

net.Buffers와 유사한 vectored header+payload write

frame buffer pool

receive side에서 immutable backing buffer view

value staging RPC에서 별도 large-value streaming protocol 검토

8. WAL과 로컬 디스크
장점

WAL은 이 저장소에서 가장 신중하게 작성된 부분 중 하나다.

CRC32C

sealed segment 정확한 크기 검증

active tail에서만 torn record truncation

immutable generation manifest

temp file, rename, file sync, directory sync

compaction build와 commit 분리

append 중 compaction barrier

이런 설계는 crash consistency를 실제로 이해하고 작성한 코드다.

개선점
8.1 manifest 세대 파일이 계속 누적된다

segment rollover 또는 compaction마다 새 manifest_%020d.bin을 만들지만 이전 manifest generation을 정리하는 경로가 보이지 않는다.

장기적으로 다음이 증가한다.

inode 수

directory entry 수

startup glob/sort 시간

backup/monitoring metadata I/O

새 manifest가 durable하게 rename되고 directory fsync된 후 최근 2개 정도만 남기도록 정리할 수 있다. 삭제 후 directory fsync도 해야 한다.

8.2 startup이 retained WAL 전체를 CRC scan한다

강한 검증이지만 WAL이 크면 restart time이 파일 크기에 선형이다. compaction bug와 결합하면 특히 나빠진다.

sealed segment manifest에 다음을 넣을 수 있다.

file size

segment content hash

final slot

record count

신뢰된 local manifest와 filesystem metadata가 일치할 때는 전체 record decode 없이 hash 또는 sampled verification 경로를 사용할 수 있다. 다만 silent bit rot 방어를 유지하려면 background scrub을 별도로 두는 것이 좋다.

8.3 restore/compare 시 segment 전체를 메모리에 읽는다

최대 segment 크기에 근접한 기존 파일을 비교하기 위해 전체 allocation하는 경로는 streaming hash 비교로 바꿀 수 있다.

8.4 old segment 삭제 오류를 무시하지 말아야 한다

compaction 후 obsolete segment 삭제 실패는 correctness를 깨지 않지만 storage leak이 된다. orphan count metric과 재시도 cleanup이 필요하다.

9. 체크포인트 설계
좋은 점

체크포인트 설계는 전반적으로 우수하다.

64MiB content-addressed block

기존 certified root의 known block skip

IfNotExists 중복 억제

root와 state hash 분리

최대 size/block count 제한

block 다운로드 후 SHA-256 검증

fixed-role SQLite/graph 파일

restore journal과 directory fsync

publisher lease와 CAS fencing

seal이 결정된 뒤에만 CURRENT 승격

특히 “candidate가 객체 저장소에 존재한다”와 “그 candidate가 합의로 인증됐다”를 분리한 점이 좋다.

개선점
9.1 hash pass와 upload pass로 파일을 두 번 읽는다

uploadFile은 모든 block hash를 먼저 계산한 후 병렬 upload를 위해 같은 파일을 다시 읽는다.

대규모 DB에서 다음 비용이 생긴다.

디스크 read 2배

page cache 오염

checkpoint 동안 foreground read latency 증가

EBS/네트워크 블록 스토리지 비용 증가

앞서 설명한 I/O governor가 우선이고, 장기적으로는 SQLite page-level 변경 정보나 snapshot block hash cache를 활용할 수 있다.

9.2 checkpoint manager mutex가 원격 I/O 동안 잡힌다

CreateFiles는 manager mutex를 잡은 채 hashing과 upload를 수행한다. loadAll도 lock을 잡은 채 LIST와 root GET을 수행한다. 긴 원격 I/O가 Latest, promotion, GC 등과 불필요하게 직렬화된다.

상태 snapshot만 lock 안에서 복사하고 I/O는 lock 밖에서 수행한 후 generation을 대조해 commit하는 구조가 낫다.

9.3 shutdown에서 큰 checkpoint를 만들려 한다

Node shutdown은 10초 context로 archive sync를 수행하고, checkpointer가 있으면 checkpoint-on-shutdown도 시도한다.

수 GiB DB의 snapshot·hash·upload는 10초 안에 끝나기 어렵다. 정상 shutdown latency가 길어지고, 어차피 timeout으로 실패할 가능성이 높다.

shutdown 계약은 다음으로 제한해야 한다.

신규 admission 중지

진행 중 합의 정리

local WAL sync

설정된 내구성 계약에 따라 archive tail sync

빠른 close

큰 checkpoint는 주기적 background 작업이어야 한다. shutdown에서는 이미 만들어진 candidate의 작은 promotion만 허용하는 편이 낫다.

9.4 restore가 큰 SQLite 파일을 추가로 한 번 복사한다

다운로드한 checkpoint 파일을 restore temp file로 다시 복사한 뒤 검사하고 rename한다. 동일 filesystem이면 reflink 또는 안전한 rename workflow를 고려할 수 있다. 최소한 copy bandwidth metric이 필요하다.

10. 합의 코어
장점

value를 별도 quorum stage한 후 hash reference로 protocol을 운용한다.

recorder state가 WAL에 남아 leader crash 후 old slot을 재구동할 수 있다.

decision certificate 검증을 catch-up과 recovery에서도 반복한다.

prefix hash를 통해 연속 decision history를 인증한다.

prepared checkpoint를 WAL에 sync한 뒤에만 seal 투표를 허용한다.

compaction barrier가 proposal pipeline과 record stripes를 모두 막는다.

결정과 local fsync, learner quorum, object-store ACK를 별도 단계로 구분한다.

durability failure를 commit_unknown으로 표현해 클라이언트가 request ID로 결과를 재조회하게 한다.

이는 분산 시스템 API 의미론을 상당히 잘 이해한 구현이다.

개선점
10.1 static group commit

recorder WAL group commit은 200µs 또는 64 waiter 고정이다.

스토리지별 fsync latency와 arrival rate가 다르므로 다음을 관찰해 adaptive하게 해야 한다.

waiter arrival rate

last fsync duration

current queue age

p99 latency budget

batch size

NVMe에서는 200µs가 불필요한 지연일 수 있고, network disk에서는 더 긴 coalescing이 유리할 수 있다.

10.2 EnsureDurable의 fsync가 caller context를 무시한다

ensureDurableLocked는 c.commits.Sync(context.Background())를 호출한다.

fsync 자체를 OS 수준에서 취소하기 어렵더라도 waiter가 context timeout 후 반환하도록 하고, group flush는 계속 완료되게 할 수 있다. 지금은 request cancellation 의미가 흐려진다.

10.3 Learn은 순서가 비결정적이고 event를 조용히 drop한다

기존 decision을 Go map 순서로 순회하고, channel이 차면 nonblocking send로 drop한다.

이 API가 정확한 decision stream이라면 잘못된 계약이다. 단순 wakeup hint라면 이름과 문서를 그렇게 바꾸고 consumer가 DecisionsFrom으로 다시 읽도록 해야 한다.

10.4 2-node cluster를 허용한다

2-node quorum은 2이므로 노드 하나가 내려가면 mutation이 모두 중지된다. 기술적으로 유효하지만 운영자가 실수하기 쉽다.

production profile에서는 다음을 권고해야 한다.

홀수 노드

최소 3노드

2노드는 명시적 override 필요

11. materializer 정확성과 실행 비용
좋은 점

SQLite writer 1개, reader pool 분리

SQL length, variable, result row, result byte, cell size 제한

defensive mode 및 extension/attach 차단

replica에서 비결정적 SQL 함수 차단

idempotency receipt를 replicated state에 저장

SQL command별 SAVEPOINT로 deterministic rejection 기록

state tip과 applied tip 분리

restored SQLite quick_check

SQLite와 graph tip을 함께 검증

graph-ahead crash window를 recovery journal로 처리

개선점
11.1 SQL determinism 정책은 SQLite authorizer에 더 가까이 두어야 한다

현재 함수 이름 denylist는 유용하지만 새로운 extension/function이나 구문이 추가되면 빠질 수 있다. SQL text keyword 검사와 authorizer를 함께 사용하고, 실제 prepare 시 authorizer가 write API용 허용 operation만 통과시키도록 강화해야 한다.

11.2 Cypher determinism은 substring denylist로 충분하지 않다

Graph command는 random(, uuid(, load from 등의 문자열을 찾아 막는다.

이는 다음 문제가 있다.

주석·문자열 literal에 의한 false positive

alternate syntax, alias, procedure로 우회

LatticeDB 버전에서 새 비결정적 함수 추가

external side effect procedure 누락

Cypher parser AST 또는 LatticeDB transaction capability layer에서 다음을 허용 목록으로 제한해야 한다.

deterministic read/write operators

deterministic functions

local graph state only

external I/O 없음

nondeterministic clock/random 없음

11.3 reader 수와 cache가 하드코딩이다

Node는 reader count를 4로 고정하고 graph cache를 32MiB로 고정한다.

CPU core 수, workload, memory limit에 따라 크게 달라진다. configuration과 auto-sizing이 필요하다.

예:

reader conns = min(max(2, GOMAXPROCS/2), 16)
graph cache = min(memory_limit * 5%, configured cap)

무조건 자동 설정하기보다 metric을 노출하고 명시적 override를 지원하는 것이 좋다.

12. Node lifecycle 및 운영성
12.1 archive sync와 maintenance를 한 goroutine에서 직렬 실행한다

주기적 archive 루프가 다음을 순서대로 수행한다.

archive catch-up

local decisions sync

certified checkpoint compaction

checkpoint compaction이나 object-store latency가 길어지면 다음 archive sync가 지연된다. before-ACK 호출은 별도 SyncThrough로 동작하지만 async durability의 lag는 커질 수 있다.

다음 queue로 분리하는 편이 좋다.

high priority: required archive tail sync

medium priority: archive catch-up

low priority: checkpoint compaction

very low priority: archive/checkpoint GC

동시에 object-store request budget을 공유해야 한다.

12.2 readiness가 degraded 상태를 충분히 표현하지 않는다

한 번 quorum catch-up에 성공하면 ready=true가 되고 이후 periodic sync failure가 ready를 되돌리지 않는다. mutation은 합의 자체에서 실패하므로 안전성 문제는 아니지만 운영자는 다음을 구분할 필요가 있다.

locally healthy

quorum available

archive current

archive lagged

checkpoint current

before-ACK durability available

GC degraded

/ready와 /healthz 외에 structured status와 metrics가 필요하다.

12.3 shutdown 순서

현재 shutdown은 background context를 취소하기 전에 archive sync와 checkpoint를 시도한다. 이 동안 주기 worker, request handling, checkpoint worker와 경합할 수 있다.

권장 순서는 다음과 같다.

ready=false, 신규 mutation admission 중지

proposal/batcher close

periodic maintenance cancel

in-flight apply 종료

local WAL sync

필요한 archive barrier

transports, materializer, WAL close

큰 checkpoint는 앞서 말했듯 shutdown 경로에서 제외한다.

13. 객체 저장소 계측

현재 계측은 논리 operation과 HTTP-level retry/status를 구분하려는 방향이 좋다. 특히 SDK retry 수와 실제 HTTP request 수를 별도로 보는 것은 비용 분석에 중요하다.

하지만 현재 누적 counter만으로는 다음 질문에 답하기 어렵다.

어느 key class가 비용을 만들었는가?

head, extent, checkpoint block, root, GC marker 중 무엇인가?

GET p50/p99는 얼마인가?

SDK queueing인가, network인가, server latency인가?

retry가 timeout, 5xx, throttling, connection reset 중 무엇 때문인가?

before-ACK latency 중 S3가 차지한 비율은?

dedup hit로 절약한 bytes와 requests는?

GC 한 회가 발생시킨 API 수와 bytes는?

최소 metric 집합은 다음이어야 한다.

objstore_requests_total{
  logical_op,
  http_method,
  key_class,
  outcome,
  status_class
}

objstore_request_duration_seconds{
  logical_op,
  key_class
}

objstore_bytes{
  direction,
  key_class,
  logical_or_wire
}

objstore_retries_total{
  key_class,
  reason
}

archive_batch_slots
archive_batch_bytes
archive_publish_duration
archive_head_conflicts
archive_lag_slots

checkpoint_hash_bytes
checkpoint_upload_bytes
checkpoint_dedup_bytes
checkpoint_duration
checkpoint_io_throttle_seconds

gc_listed_objects
gc_deleted_objects
gc_requests
gc_bytes_rewritten

고카디널리티 실제 key는 label에 넣지 말고 정해진 key class만 사용해야 한다.

14. 권장 수정 우선순위
단계 0: 안전성과 장기 운영 가능성
1. certificate signature 도입

member별 durable public key

canonical signed summary

WAL/archive/checkpoint recovery에서 quorum signature 검증

config epoch와 key rotation

2. compaction live-set 수정

floor 이후 decision과 unresolved recorder state만 keep

recovery 후 mark-and-sweep

repeated compact/restart resource-bound test

3. peer mTLS admission

handshake에서 client public key 검증

application token 이전에 연결 거부

per-peer/IP quota

0-RTT 우선 비활성화

4. materializer fingerprint invariant

SQL/KV/Notify/Graph 모두 동일하게 처리

불일치 시 startup/replay fatal

이 네 가지가 해결되기 전에는 프로덕션 신뢰성을 주장하기 어렵다.

단계 1: S3 비용과 foreground latency
5. S3 conditional PUT result abstraction

새 ETag/version 반환

정상 archive publish 5 requests → 2 requests

claim readback 제거

6. incremental archive compaction

안정된 prefix 재사용

tail fragment만 merge

content-addressed PUT에 IfNotExists

7. marker-free GC

live object에 아무 요청도 하지 않기

attributes 기반 grace

large deployment는 Inventory/Batch Delete 고려

8. materializer batch metadata 축약

batch당 final applied metadata 1회

receipt/TTL prune amortization

graph journal batch append

단계 2: CPU와 memory
9. peer serialization 복사 제거

buffer/builder pool

vectored frame write

sync response 단일 encode

read-only view decode

10. query encoding 단일화

size check와 실제 JSON encoding 통합

streaming query API

row scan buffer 재사용

11. checkpoint I/O governor

upload concurrency/configuration

foreground latency 기반 throttle

disk bandwidth 제한

shutdown checkpoint 제거

12. notification resource control

topic index

global/topic/subscriber quota

byte-budget queue

shared payload

drop metric

단계 3: 운영성
13. 하드코딩된 값들을 configuration 및 auto-tuning 대상으로 전환

SQLite reader count

graph cache

mutation batch target

group commit delay

archive group delay

catch-up page size/timeouts

checkpoint block/upload concurrency

hedge policy

14. latency histogram과 subsystem 상태 노출

consensus stages

WAL append/fsync

materializer apply

QUIC RPC

S3 logical/HTTP

checkpoint/GC

archive lag

15. HTTP adapter security

authentication hook

non-loopback unsafe opt-in

ReadHeaderTimeout

header limit

SSE/session caps

15. 반드시 추가할 테스트
Compaction resource-bound test
N번:
  큰 value 여러 개 결정
  checkpoint 생성 및 seal
  compact
  close/reopen

assert:
  WAL bytes <= recovery base + retained suffix + bounded metadata
  values count <= live suffix + unresolved recorder refs
  startup scan time가 전체 역사에 비례하지 않음
Malicious archive test

다음 조작된 archive가 반드시 거부되어야 한다.

존재하는 member ID만 사용한 가짜 unsigned quorum

valid hash와 CRC를 가진 forged extent

rewritten head와 prefix chain

다른 config epoch의 signature

revoked node의 signature

duplicate signer

Object-store request budget test

mock bucket에서 정상 uncontended operation의 정확한 upper bound를 검증한다.

before-ACK one archive batch:
  1 immutable extent PUT
  1 conditional head PUT
  0 readback with S3 PutResult fast path

GC가 아무것도 삭제하지 않을 때 live object 수에 비례한 DELETE가 발생하지 않는지도 검증해야 한다.

Materializer batch amplification test

256 decision 적용 시 다음 statement 횟수를 계측한다.

applied_slot update: 1
applied_hash update: 1
state_slot update: <=1
receipt prune: <=1
SQLite commit: 1
Fingerprint corruption test

동일 request ID에 다른 SQL/KV/Notify/Graph command를 포함한 certified replay는 모든 profile에서 동일하게 실패해야 한다.

QUIC availability test

비인가 client가 connection cap을 점유하지 못함

stream timeout 하나가 connection을 invalidate하지 않음

Record/Learned/StageValue가 0-RTT replay로 실행되지 않음

한 peer의 connection churn이 다른 peer를 방해하지 않음

Notification memory test

느린 subscriber와 1MiB payload를 반복해도 설정된 global byte budget을 넘지 않고 gap/drop이 관측 가능해야 한다.

Chaos recovery matrix

각 persistence boundary마다 process kill을 주입해야 한다.

WAL append 전/후

group fsync 전/후

SQLite commit 전/후

graph commit 전/후

archive extent PUT 후/head PUT 전

head PUT 후/local state 갱신 전

checkpoint root PUT 후/seal 전

seal 결정 후/CURRENT 승격 전

compaction build/commit 각 단계

restore journal 각 phase

16. 잘된 점을 명확히 평가하면

이 코드는 일반적인 개인 프로젝트 수준보다 상당히 높다.

특히 높게 평가할 부분은 다음이다.

복구 가능성을 중심으로 한 데이터 계층 분리

SQLite와 graph DB를 재생 가능한 projection으로 본 결정은 옳다. 이 덕분에 database-specific corruption repair보다 인증된 로그 replay라는 일관된 복구 모델을 사용할 수 있다.

객체 저장소의 immutable-data/mutable-pointer 분리

content-addressed block과 CAS head는 S3에서 구현하기 좋은 기본 패턴이다. rename이나 directory atomicity를 흉내 내지 않은 점이 좋다.

체크포인트 인증 절차

“업로드 성공”만으로 checkpoint를 신뢰하지 않고, quorum replica가 root와 block을 검증한 후 작은 seal을 합의하는 절차는 강하다.

overload protection

요청 수만 제한하지 않고 encoded byte, local/peer proposal, total operation, per-peer count를 각각 제한한다. 이는 실제 memory safety와 noisy-neighbor 방어에 효과적이다.

commit outcome semantics

합의와 local apply는 성공했지만 before-ACK S3 barrier가 실패한 경우를 단순 실패로 반환하지 않고 commit_unknown과 request ID, slot, retry window로 표현한다. 분산 mutation API로서 올바른 접근이다.

crash-consistent restore

restore journal, backup rename, directory fsync, rollback path를 단계별로 둔 것은 세심하다.

bounded interfaces

SQL, query result, cell, request body, peer frame, checkpoint, extent의 상한을 대부분 명시했다. 이는 예측 가능한 리소스 사용을 위한 중요한 기반이다.

최종 판단

이 프로젝트는 본질적으로 좋은 아이디어를 가진 작은 분산 데이터베이스다. 코드의 여러 부분은 다음 질문을 실제로 고민한 사람이 작성했다.

crash가 정확히 어느 write 사이에 날 수 있는가?

stale writer가 mutable pointer를 되돌릴 수 있는가?

checkpoint가 실제로 quorum에서 읽히는가?

합의는 됐지만 응답을 못 받은 클라이언트에게 무엇을 말해야 하는가?

materializer가 원장보다 앞서면 어떻게 감지하는가?

객체 저장소의 논리 호출과 실제 retry HTTP 호출을 어떻게 구분하는가?

그 점은 분명한 강점이다.

다만 현재 가장 중요한 약점도 분명하다.

무결성 hash는 충분히 많지만, durable recovery certificate의 진위성이 없다.

그리고:

compaction이 메모리에서는 값을 지우면서 compacted WAL에는 다시 보존하기 때문에, checkpoint가 장기 자원 회수 경계로 제대로 작동하지 않을 수 있다.

이 두 문제를 해결한 뒤, archive 정상 publish를 5회에서 2회의 S3 요청으로 줄이고, 전체-chain GC를 incremental tail compaction으로 바꾸며, materializer의 slot별 반복 SQL을 batch별 1회로 축약하면 프로젝트의 비용·레이턴시·운영 특성이 크게 개선될 것이다.

현재 상태를 항목별로 평가하면 다음 정도다.

영역	평가
기본 아키텍처	강함
crash consistency	강함
합의·복구 코드의 방어성	대체로 강함
malicious storage에 대한 인증	미흡
장기 WAL·메모리 boundedness	주요 결함 존재
foreground overload 제어	강함
S3 정상 publish 비용	최적화 여지 큼
S3 GC 비용	장기적으로 비쌈
CPU 효율	materializer·serialization 개선 필요
메모리 상한	일부 잘 제한됐으나 notification·decode 경로 취약
운영 관측성	누적 counter 이상으로 확장 필요
public/peer 보안 경계	강화 필요

우선순위를 한 문장으로 압축하면 다음과 같다.

서명된 recovery certificate → compaction live-set 수정 → peer handshake 인증/0-RTT 차단 → materializer invariant 강화 → S3 2-request publish → incremental GC → batch apply 최적화 순서가 가장 높은 투자 대비 효과를 낸다.

ChatGPT는 실수를 할 수 있습니다. 중요한 정보는 재차 확인하세요.

Pro

