# Rhiza HTTP 및 요청 수락 역명세

기준 소스는 `008f8e576ad76425182e10a69b987f10cfb48b2a`이다. 모든 링크는 정본 [`server.go`](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1181), [`graph.go`](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/graph.go#L63), [`mutation_admission.go`](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/mutation_admission.go#L13), [`value.go`](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/types/value.go#L24)를 가리킨다.

## 범위 및 증거

HTTP 클라이언트 표면, SQL/Graph/KV/notify 변이 수락, batch/read/migration/request-receipt 수명주기를 다룬다. DB 엔진 내부, QuePaxa 합의 구현, 피어 인증과 전체 관리·복구 프로토콜은 제외한다. 모든 단정은 **소스 판독**이며 테스트·런타임 변경은 하지 않았다. materializer/QuePaxa의 내부, 외부 미들웨어·TLS·프록시는 **미검증**이다.

## HTTP 공통 계약

`ServeHTTP`는 직접 `ServeMux`로 dispatch한다. 종료 시작 뒤 `/recovery/drain` 외의 새 요청은 `not_ready`로 거절하고 기존 handler를 추적한다. 클라이언트 routes에는 `Authorization`/역할 검사가 없다. membership/recovery routes만 별도 bearer-token 인증을 갖지만 본문 범위 밖이다. [dispatch](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1181), [route 등록](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L497), [recovery 인증 참조](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/recovery_probe.go#L40).

JSON body는 `MaxBytesReader`로 정확히 1 MiB 제한, `UseNumber`, `DisallowUnknownFields`, 단일 JSON value(뒤 값 금지)를 적용한다. 요청 `Content-Type`과 POST query parameter는 검사/사용하지 않는다. 본문 초과도 413이 아니라 decoder 실패로 400 `invalid_request`다. 이 decoder는 중복 key, 대소문자만 다른 field name, `null` 자체를 일괄 금지하지 않는다. 성공 JSON은 `Content-Type: application/json`; 204는 body/Content-Type 없음. 메서드 오류는 405, `Allow: METHOD[, METHOD]`, JSON 오류; fallback은 404 `not_found` JSON이다. [decoder](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1716), [writers](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1594).

## 라우트 매트릭스

| 경로 | 허용 메서드 | 종류 | 성공 |
|---|---|---|---|
| `/sql/execute`, `/sql/transaction` | POST | SQL 변이 | 200 receipt+statements |
| `/sql/execute-returning`, `-one` | POST | SQL 변이 | 200 receipt+statements; `want_rows=true` 강제, **nonempty** `statements`만 거절; one은 `require_one=true`도 강제 |
| `/sql/query` | POST | 읽기 | 200 query result |
| `/graph/execute` | POST | Graph 변이 | 200 receipt |
| `/graph/query`, `/graph/changes`, `/graph/streams/read` | POST | 읽기; wait stream은 long-poll | 200 Graph result |
| `/graph/streams/offset` | POST, PUT | POST 읽기; PUT Graph 변이 | 200 JSON, 204 |
| `/graph/streams/trim` | POST | Graph 변이 | 204 |
| `/kv/put`, `/kv/delete`, `/kv/cas` | POST | KV 변이 | 200 receipt |
| `/kv/get` | POST | 읽기 | 200 KV result |
| `/notify/publish` | POST | notify 변이 | 200 receipt |
| `/notify/subscribe?topic=…` | GET | SSE 구독 | 200 SSE; read-admission 미사용 |
| `/request/status` | POST | receipt 조회 | 200 상태; read-admission 미사용 |
| `/metrics/object-store`, `/replica/status` | GET | 상태 | 200 JSON 또는 기능 없음 404 |

관리·복구 인벤토리는 GET `/metrics/object-store`, `/replica/status`, `/recovery/status`, `/membership/status`, `/ready`, `/healthz`, `/recovery/probe`와 POST `/membership/change`, `/membership/abort`, `/recovery/archive`, `/recovery/drain`이다. protocol은 재구현 대상이 아니다. `/ready`는 readiness·materializer·WAL, `/healthz`는 materializer·WAL을 검사한다. handler 근거는 [SQL/KV](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1289), [Graph](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/graph.go#L63), [stream offset/trim](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/graph.go#L330)에 있다.

## 변이 admission, clone, release

HTTP handler의 형식 검증은 도메인 호출 이전이다. 모든 변이는 `writable && Ready()` → caller context → charge → 전역 reservation → clone까지 공통이다. 그 다음 순서는 도메인마다 다르며, 아래 표가 오류/재시도 우선순위 계약이다.

| 도메인 | clone 뒤 순서 |
|---|---|
| SQL | nonempty statements/returning 제약 → SQL validation → fingerprint → encode·replicated-size → request stripe → intent/receipt lookup → batch submit |
| Graph | Graph admission validation → request stripe → intent/receipt lookup → batch encoding·submit |
| KV | operation/request/key/TTL/value validation → request stripe → intent/receipt lookup → TTL overflow 검사와 최초 관찰/만료시각 설정 → batch encoding·submit |
| notify | request/topic/payload validation → request stripe → intent/receipt lookup → notify encoding·replicated-size → direct proposal |

따라서 Graph/KV/notify의 retained 동일 요청은 뒤쪽 encode·size·TTL-overflow 검사를 다시 만나지 않고 receipt/durability 경로를 택할 수 있다. SQL만 encode/fingerprint가 lookup보다 앞선다. [SQL](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1354), [Graph](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/graph.go#L82), [KV](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1838), [notify](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1965).

전역 admission 상한은 **128 mutations**, 총 **32 MiB**, 요청당 **1 MiB**다. 요청당 초과/charge 실패는 `invalid_request`; count/byte budget 부족은 `overloaded`다. 이는 HTTP decoder 전 메모리까지 제한하지 않는다.

### 정확한 charge recurrence

루트는 `add(root, 0)`이다. `add(value, depth)` 호출만 node를 하나 증가하고 `nodes>65,536` 또는 `depth>32`면 실패한 뒤 **항상 32 bytes**를 더한다. helper `S(n)=32+13*n`과 `B(n)=32+5*n`은 visit가 아니며 node/depth/base-32를 추가하지 않는다. 각각 `n>1MiB/13`, `n>1MiB/5`면 실패한다. overflow 또는 총계가 1 MiB 초과면 실패한다. 허용 scalar는 정확히 `nil`, `bool`, 모든 signed `int` 폭, `uint`/`uint8`/`uint16`/`uint32`/`uint64`, `float32`, `float64`뿐이다(`uintptr`, complex, struct, pointer, arbitrary marshaler는 불가). `json.Number`는 `S(len)`처럼 취급한다.

| 방문 값 | 추가 charge와 재귀 |
|---|---|
| scalar | base 32만 |
| string 또는 `json.Number` | visit base 32 + `S(len)` |
| `[]byte` | visit base 32 + `B(len)` |
| `[]any` | base 32 + `64+16*len`, 각 element를 `depth+1`로 방문; len>65,536 실패 |
| `map[string]any` | base 32 + `64+32*len`, 각 key에 **helper만** `S(len)` (visit 없음), 각 value를 `depth+1`로 방문; len>65,536 실패 |
| `SQLCommand` | base 32; request ID/SQL은 helper `S`, args를 `depth+1`, `64+64*statements`, 각 statement SQL은 `S`, args(`depth+1`), `32*outputRefs`, 각 column name `S`, migration name/checksum `S`. `statements`와 각 statement `output_refs`는 각각 len>65,536이면 별도로 실패 |
| `GraphCommand` | base 32; request ID/cypher `S`, args(`depth+1`), `64+64*events`, event stream/kind `S`, payload(`depth+1`), optional offset stream/consumer `S`, optional trim stream `S`. `events`는 len>65,536이면 별도로 실패 |
| `KVCommand` | base 32; request ID/operation/key `S`, value/expected `B` |
| `NotifyCommand` | base 32; request ID/topic `S`, payload `B` |

charge 순회는 clone 전 호출자 값을 읽으므로, 메서드가 반환할 때까지 caller는 nested map/slice/value를 변경하면 안 된다. [walker](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/mutation_admission.go#L59).

clone은 strings, byte slices, `[]any`, `map[string]any`, SQL statement/refs, Graph events/offset/trim을 독립 소유로 만든다. admission lease는 owner state `caller(0) → worker(1) → released(2)` 또는 `caller → released`만 허용한다. batch submit은 queue reservation 전에 worker로 transfer하고, reservation 실패도 worker path가 즉시 해제한다. `finish`는 result channel에 결과를 **먼저 전송한 후** worker lease를 release한다. direct proposal은 새 registration 또는 inflight proposal join 때 transfer하고 logical proposal 정리에서 lease를 release한 뒤 completion channel을 닫는다. [clone](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/mutation_admission.go#L202), [batch transfer/finish](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/batcher.go#L100).

batcher는 별도로 queue 4,096건/8 MiB, 동시 batch 8개, SQL batch item 64, 복제값 128 KiB, value/proposal 자원 상한을 적용한다. 내부 batch admission은 최대 2초 기다리고 execution reserve 5초를 남긴다. 이는 128/32 MiB와 독립된 뒤쪽 거절 지점이고 public code는 `overloaded`일 수 있다. [상수](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/batcher.go#L13), [internal batch 예산](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L587), [복제값 상한](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L19).

## 영수증, retry, deadline/cancel

요청 ID는 4,096 hash-striped locks로 직렬화한다. 기존 request ID가 다른 intent/fingerprint면 `request_conflict`; 같은 intent의 receipt는 새 proposal 없이 반환하지만 durability barrier는 다시 기다린다. [lock](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L251), [SQL path](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1484).

admission 전 cancel은 context error이고, queue send 뒤 또는 proposal registration/join 뒤 caller cancel은 `CommitUnknownError`가 될 수 있다. batch dispatch는 이미 cancelled item을 drop/release하며, dispatch된 SQL/Graph/KV job은 caller와 독립적인 batcher-owned 30초 context로 실행한다. direct proposal은 server context에 `min(30초, 최초 registration caller deadline)`을 쓴다; joiner의 deadline은 바꾸지 않는다. Local **proposal**에서 trusted prewrite admission denial은 core-start failure로 latch하지 않는다. reclaim handler가 있으면 permit을 반납하고 한 번 retry할 수 있으며, clean `ErrLocalMaintenanceRefused`도 blanket latch 대상이 아니다; retry의 core-start 후 non-prewrite failure 또는 비정상 maintenance failure는 lifecycle failure를 latch한다. 반대로 Local durability/apply **lifecycle work**는 `proposalCtx` 기반 30초 작업이고 caller cancel 뒤 계속할 수 있으며, `finishLocalLifecycle`은 lifecycle work의 모든 non-nil error를 core-start 조건 없이 latch하여 이후 admission을 `not_ready`로 만든다(명시적 reopen 전까지). [batch dispatch/worker](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/batcher.go#L311), [proposal branches](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L855), [lifecycle finish](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1018).

slot이 0이 아니면 `retry_through_slot=slot+65,536-1`; materialized tip이 그 값과 **같을 때까지는** receipt가 retained이며, `tip > retry_through_slot`에서 lookup은 absent다. 따라서 같은 ID retry 보호는 이 retention window 안에서만 성립한다. `unknown_or_expired`는 미제출/진행 중/만료를 구분하지 않으며, 만료된 ID retry는 새 submission으로 실행될 수 있다. [계산](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L45), [retention lookup](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L674).

`/request/status`는 `kind`가 `sql|kv|notify|graph`, request ID가 1..64 bytes여야 한다. local lifecycle failure를 먼저 검사한다. consensus barrier나 durability 확인은 수행하지 않는다. receipt 부재는 진행 중·미도착·만료를 구분하지 않는 `{state:"unknown_or_expired",tip:N}`이다. [구현](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1666).

## migration token

`MigrationRequest`는 HTTP body가 아닌 in-process 구조체다. `Migrate`는 일반 SQL admission을 한다. embedded wrapper가 checksum/validation 전에 자리를 확보할 때 `AdmitMigration(version,name,statements)`가 request ID와 checksum을 각각 64개의 `"0"`으로 놓고 charge를 예약한다. token은 server-bound, exact-charge, single-use다. **원문/statement/fingerprint 내용 binding은 없으므로 같은 charge의 다른 content를 막지 않는다.** 다만 unchanged name/statements와 valid 64-character checksum이라는 조건에서는 final request ID도 64 bytes여야 reservation charge와 맞는다; 더 짧은 ID는 charge mismatch로 invalid다. nil/different server/charge mismatch/이미 release·consume된 token은 invalid. consume 전 context/서버/charge 실패에는 caller가 `Release`해야 예약이 남지 않는다; consume 뒤 내부 lease 경로가 release하며 `Release` 재호출은 안전하다. [정의](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1216), [acquire/consume](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1427).

## 읽기 admission 및 consistency

기본은 concurrent 64, long-poll 8이고 노출 전에 `SetReadAdmissionLimits`로 바꿀 수 있다. acquire는 local lifecycle failure → context 오류 → concurrent token nonblocking → long-poll token nonblocking 순서다. 부족하면 기다리지 않고 overloaded; long-poll token 실패 시 concurrent token을 반환한다. Graph stream에서 `wait_ms != 0`일 때만 long-poll이다. [limits](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L126), [acquire](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L397), [stream](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/graph.go#L228).

HTTP handler는 response encode/write가 끝날 때까지 permit을 유지하고, 직접 Go API는 method 반환까지 유지한다. `SetReadAdmissionLimits`는 이후 시작 read만 새 object를 쓰며 이전 read는 이전 object를 유지한다. 따라서 실행 중 재설정은 활성 read 수를 즉시 새 limit 아래로 강제하지 않는다. [HTTP query 예](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1536).

`consistency`는 빈 값/`local`이 local, `linearizable`은 ReadIndex·필요 시 catch-up·결정 적용을 수행하며 다른 값은 invalid다. SQL `sql`, KV `key`, Graph query `cypher`는 필수. stream `limit=0`은 100, 최대 1,000이며 `wait_ms>30000`은 invalid다. 일반 reads는 `Ready()` callback을 검사하지 않고 sticky local-lifecycle failure만 검사한다. [barrier](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1910), [stream defaults](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/graph.go#L238).

## 독립 EGM 한계 판정

dispatch, decoder, 공개 schema, status, 수락 순서, 한도, clone/release, retry, migration token, read admission의 계약을 연결했다. materializer 내부, 외부 미들웨어와 QuePaxa internals는 범위 밖이다. 실제 테스트와 skip, 두 독립 Goldfish의 원문·재점검은 [EGM 검토 기록](request-http-go-egm-review-2026-10-30.md)에 남겼다. 지적 사항을 수정했고 최종 재점검에서 해당 항목의 해소를 확인했다. 전체 명세의 무결점 판정이나 독립 재구현 인증은 아니다.
