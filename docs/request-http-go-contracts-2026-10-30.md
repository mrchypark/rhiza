# Rhiza HTTP 요청 계약 부록

기준 커밋 `008f8e576ad76425182e10a69b987f10cfb48b2a`. 태그의 정본은 [`server.go`](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1199), [`graph.go`](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/graph.go#L15), [`value.go`](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/types/value.go#L40)다. 생략된 request field는 Go zero value이며, response `omitempty` field는 zero이면 없다.

## JSON, base64, RawMessage

- body는 1 MiB 이하의 하나의 JSON value이며 unknown field는 불가하다. 초과도 413이 아니라 400이다. `any`/`map[string]any` number는 `json.Number`다. 중복 key, 대소문자만 다른 field name, `null` 자체를 전면 거절하지는 않는다.
- 모든 `[]byte` body/response는 Go `encoding/json` 표준 base64 문자열이다. 생략 또는 `null`은 nil slice, `""`은 빈 slice다.
- 이 HTTP/request scope의 공개 body/query 타입에는 `json.RawMessage` 필드가 없다. RawMessage 보존/재직렬화 기본값은 **해당 없음**이다.
- SQL `args` HTTP JSON에서는 `{ "$rhiza_blob": "<base64>" }`라는 **정확히 하나의 key만 가진 object**가 blob으로 허용된다. extra key, `$rhiza_blob`의 non-string, invalid base64는 invalid SQL argument다. Go in-process `[]byte`도 blob으로 허용되고 replicated SQL wire는 이를 같은 object로 인코드한다. [HTTP decode](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1716), [SQL arg](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L2035), [wire](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/types/value.go#L346).

## body/query 및 성공 응답

| API | 요청 body 또는 query | 성공 body |
|---|---|---|
| SQL execute 계열 | `request_id` 필수; optional `sql`, `args`, `want_rows`, `require_one`, `statements`; statement=`sql`, optional `args`,`want_rows`,`expected_rows_affected`,`expected_returned_rows`,`output_refs`; ref=`arg_index`,`statement_index`, optional `column_name`,`column_index`. `statements`는 top-level SQL/args와 배타적이지 않음 | receipt + optional `statements`; result=`rows_affected`,`last_insert_id`,`columns`,`rows`. returning 계열은 **nonempty** statements만 거절; one은 incoming `require_one`도 false로 바꾸지 않음 |
| SQL query | `sql` 필수, optional `args`,`consistency` | `columns`,`rows`,`applied_slot`,`consensus_tip` |
| Graph execute | `request_id:string`, optional `cypher:string`,`args:map[string]any`,`events:[]GraphStreamEvent`,`stream_offset:*GraphStreamOffsetMutation`,`stream_trim:*GraphStreamTrimMutation`; 아래 Graph admission 규칙을 모두 만족 | receipt |
| Graph query | `cypher` 필수, optional `args`,`consistency` | optional `columns`,`rows`,`error`,`applied_slot`,`consensus_tip` |
| Graph changes/read | optional `stream`,`after_sequence`,`limit`,`wait_ms`,`consistency`; changes는 stream을 `__lattice_changes`로 덮음. limit 0=100, max 1,000; wait max 30,000ms | `records` (`sequence`,`kind`,`payload`)[], `applied_slot`,`consensus_tip` |
| Graph offset | `request_id` optional, `stream`,`consumer`, optional `sequence`,`consistency`; PUT은 mutation이므로 valid request_id 필요 | POST JSON=`sequence`,`found`,`applied_slot`,`consensus_tip`; PUT 204 |
| Graph trim | `request_id`,`stream`,`through_sequence` | 204 |
| KV mutation | `request_id`,`key`, optional base64 `value`,`expected`, optional `expected_exists`,`ttl_ms` | receipt |
| KV get | `key` 필수, optional `consistency` | `found`, optional base64 `value`, `applied_slot`,`consensus_tip` |
| notify publish | `request_id`,`topic`, `payload` (base64; `null` decode 가능) | receipt |
| notify subscribe | query `topic` 필수 | SSE `data: <base64 JSON string>`; 15초 keepalive |
| request status | `kind`=`sql|kv|notify|graph`, `request_id` | `state`,`tip`, optional `receipt` |

모든 tag의 근거: [receipt/SQL](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/types/value.go#L29), [Graph](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/types/value.go#L122), [HTTP types](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1199), [Graph HTTP types](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/graph.go#L15).

`MutationReceipt`는 최상위로 embed된다: 필수 JSON fields는 `slot:uint64`, `status:"committed"|"rejected"`, `retry_through_slot:uint64`; optional은 `error_code`, `rows_affected`, `last_insert_id`, `applied`. receipt에는 `request_id`가 없다. known rejection error code는 `execution_failed`, `precondition_failed`, `result_retention_full`이다. HTTP 200과 `status:"rejected"`는 함께 가능하며 HTTP 오류가 아니다.

## SQL 및 Graph admission 계약

SQL command request ID는 최대 64 bytes, statement는 1..64개, SQL은 trim 후 비어 있지 않고 NUL 없고 최대 256 KiB, args와 output refs는 각각 최대 999다. `require_one`은 top-level `want_rows=true` 및 statements 길이 0일 때만 가능하다. statement `expected_rows_affected`는 non-row statement에서만 0 이상, `expected_returned_rows`는 `want_rows=true`에서만 0..10,000이다. migration은 top-level SQL/args/want_rows를 허용하지 않고 version>0, name nonempty, 64 hex checksum을 요구하며 statement args/rows/precondition/output refs를 금지한다.

`output_refs`가 있으면 SQL의 각 positional parameter에 args 하나가 있어야 하고 plain unnumbered `?`만 세며 quoted identifier/string, comments, bracket identifier 안의 `?`는 세지 않는다. `?NNN`와 named `:x`, `@x`, `$x`는 불가하다. 각 ref는 현재 statement args 범위의 null destination, statement 내 unique `arg_index`, 더 이른 `want_rows` statement, 그리고 nonnegative `column_index` 또는 nonempty `column_name` 중 정확히 하나를 요구한다. `PRAGMA`, `ATTACH`, `DETACH`, `VACUUM`, `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`는 replicated write API에서 거절된다. tokenized SQL에서 `CREATE TEMP`/`CREATE TEMPORARY`와 `temp.` qualifier도 case-insensitively 거절한다. SQL text 어디든 case-insensitive `_rhiza_` substring이 있으면 reserved namespace로 거절한다. SQL JSON args의 허용값은 null/bool/string/`$rhiza_blob` object/JSON number다; `json.Number`는 int64로 먼저, 불가하면 float64로 바꾸며 invalid number와 unsigned overflow는 거절된다. [SQL validator](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1637), [parameter scanner](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1749), [temp policy](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/temp_policy.go#L105), [namespace](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1840), [conversion](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L2035).

Graph nested JSON은 `events:[{"stream":string,"kind":string,"payload":any}]`, `stream_offset:{"stream":string,"consumer":string,"sequence":uint64}`, `stream_trim:{"stream":string,"through_sequence":uint64}`다. omission은 nil/zero value이고 `events`는 `omitempty`다. Graph mutation은 request ID 1..64 bytes이고 trim한 cypher, stream offset, stream trim 중 정확히 하나다. cypher는 최대 256 KiB이고 끝의 하나만 제외하고 다른 semicolon이 있으면 거절된다. args는 최대 999 key이며 key는 nonempty·최대 256 bytes다. JSON numbers는 int64를 먼저 시도 후 float64, recursive array/object도 허용하며 unsupported value/overflow는 거절된다. events는 cypher mutation에만 붙고 event stream은 valid UTF-8/nonempty/최대 255 bytes이며 `__lattice_` prefix가 금지된다; kind도 valid UTF-8/nonempty/최대 255 bytes다. offset/trim stream은 same UTF-8/length rule이지만 reserved prefix를 허용하고, offset consumer는 valid UTF-8/nonempty/최대 255 bytes다. standalone `CREATE (`는 node 하나만 만들 수 있고 node 뒤에는 빈 문자열 또는 `RETURN `만 올 수 있다; 관계 생성은 `MATCH ... CREATE`가 필요하다. Charge admission은 root depth 0에서 시작하고 Graph events len>65,536, SQL statements len>65,536, 그리고 각 SQL statement output refs len>65,536을 node 총계와 별도로 거절한다. [Graph schema](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/types/value.go#L147), [Graph validator](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/graph_common.go#L16), [charge guards](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/mutation_admission.go#L66).

## KV, retention 및 SSE의 운영 의미

KV `put`은 value와 expiry를 upsert한다. `delete`는 key가 없으면 `status:"committed", applied:false` receipt이고, `cas`는 current existence가 `expected_exists`(생략=false)와 같고 존재할 때 bytes가 `expected`와 같을 때만 write한다. mismatch도 rejected가 아니라 committed `applied:false`다. `ttl_ms=0`은 expiry 없음; 양수 TTL은 **첫 제출** 때 `ObservedAtUnixMS`로 anchor된 `ExpiresAtUnixMS`가 되고 동일 ID retry는 그 시각을 바꾸지 않는다. `ttl_ms > MaxInt64-now`는 invalid이다. expired value는 lookup 시 존재하지 않는 것으로 취급된다. [submission](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1880), [apply](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1548).

receipt의 replay/fingerprint 보호는 materializer tip `<= retry_through_slot`인 동안만 유지되고, tip이 더 크면 receipt/status lookup은 absent다. 따라서 `unknown_or_expired`는 retry가 반복 실행되지 않는다는 보장이 아니다. cached receipt ACK도 configured durability barrier를 다시 기다리며 RequestStatus의 `committed`는 durability proof가 아니다. [lookup](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L674).

SSE subscribe는 `http.Flusher`가 아니면 500 `streaming_unsupported`; write deadline을 해제한 뒤 `Content-Type:text/event-stream`, `Cache-Control:no-cache`, `X-Accel-Buffering:no`를 설정하고 `: connected\n\n`을 flush한다. subscriber capacity는 64, 각 subscription queue depth는 1이다. delivery는 live·at-most-once이고 full/racing queue의 event는 drop count만 증가하며 replay 없다. handler는 request context cancel 또는 channel close에서 끝나고 `Server.Close`는 active handler 종료를 기다린다. [HTTP SSE](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L2059), [subscription](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1497).

## 실패 envelope와 status

보통 실패는 다음 형태다: `{"code":"<code>","error":"<현재 error 문자열>"}`. encoder newline은 가능하다.

| 조건 | HTTP | code |
|---|---:|---|
| JSON/HTTP validation, unsupported consistency | 400 | `invalid_request` |
| 동일 ID의 다른 intent | 409 | `request_conflict` |
| writable/readiness/local lifecycle/handler shutdown | 503 | `not_ready` |
| mutation/read/queue capacity | 503 | `overloaded` |
| unwrapped durability barrier | 503 | `durability_unavailable` |
| proposal 뒤 commit 불명 | 503 | `commit_unknown` |
| QuePaxa quorum error | 503 | `quorum_unavailable` |
| 매핑 밖 오류(직접 context cancel/deadline 포함) | 500 | `internal_error` |
| wrong method | 405 | `method_not_allowed` + `Allow` |
| no route | 404 | `not_found` |

`writeAPIError`는 순서대로 invalid/consistency → conflict → not-ready → overloaded → durability → commit-unknown → quorum을 `errors.Is`로 분류한다. 그 뒤 `errors.As(*CommitUnknownError)`면 **선택된 HTTP status를 유지**하면서 JSON code만 `commit_unknown` 및 slot/request fields로 덮어쓴다. 예를 들어 typed unknown이 invalid를 wrap하면 HTTP 400 `commit_unknown`가 가능하다. known-slot durability failure도 `commitUnknown` wrapping 뒤에는 이 typed override를 탄다. slot이 0이면 숫자 fields는 `omitempty`로 사라진다. [mapping](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1587), [receipt durability](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L472).

검증 증거는 별도 [EGM review](request-http-go-egm-review-2026-10-30.md)에 기록된 source-checked/test-executed 구분을 따른다. 이 보정본은 재검토 대기 중이며, 이 부록은 테스트 결과를 새로 주장하지 않는다.

## 전단 validation의 좁은 범위

KV handler는 request ID 1..64 bytes, key 1..1024, `ttl_ms>=0`, decoded value <=16 MiB를 확인한다. notify는 request ID 1..64, topic 1..256, payload <=1 MiB다. HTTP body 1 MiB가 먼저이므로 16 MiB KV 한계는 HTTP에는 사실상 도달 불가하지만 in-process API에는 적용된다. SQL/Graph은 decode 뒤 도메인 validator가 상세 validation을 한다. [KV](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1809), [notify](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1955).

`MigrationRequest`는 HTTP route가 아닌 in-process struct이며 JSON tag가 없다. 따라서 migration HTTP wire schema는 존재한다고 문서화하지 않는다. migration wrapper의 전체 목록 reservation은 개별 migration의 순차 제출과 별개이므로 목록 전체 원자성을 뜻하지 않는다. [정의](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1216).

## Recovery probe wire contract

이 절은 operator가 직접 소비하는 `GET /recovery/probe`와 그 안의 recovery status schema를 소유한다. 다른 recovery protocol 전체나 엔진 내부의 명세로 범위를 넓히지 않는다. 기준은 `008f8e576ad76425182e10a69b987f10cfb48b2a`의 [recovery_probe.go](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/recovery_probe.go#L15), [recovery_status.go](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/recovery_status.go#L19), [automatic_probe.go](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/automatic_probe.go#L16)다. 이 절은 소스 판독 결과이며 새 실행 검증 결과를 주장하지 않는다.

### Request and response schema

요청은 body 없는 GET이다. operator는 난수 32바이트를 소문자 hex 64자로 encode하여 `nonce`에 넣고, `exclude`는 첫 optional excluded voter 또는 빈 문자열을 `url.QueryEscape`하여 보낸다. bearer header는 정확한 `Bearer ` prefix와 설정 token 원문이다. handler는 `recoveryArchiveToken`이 비어 있지 않아야 하며 prefix 뒤 나머지 bytes가 token과 constant-time 비교로 같아야 한다. token을 hex/base64 decode하거나 공백을 trim하지 않는다. 이 token은 `SetRecoveryArchive`에서 설치하며 peer TLS credential과 구별한다.

handler는 query의 첫 `nonce` 값을 hex decode하여 정확히 32바이트인지만 확인한다. 소문자만 강제하지 않고 입력 문자열을 그대로 echo한다. `exclude`도 첫 query 값을 그대로 사용한다. 이 handler는 excluded ID의 별도 형식·소속 검사를 먼저 하지 않으며 Core의 `ReadIndexExcluding` 결과를 사용한다. 알 수 없는 query parameter나 GET body를 별도로 검증하지 않는다. 성공 응답은 HTTP 200, `Content-Type: application/json`, `Cache-Control: no-store`이며 다음 구조를 Go `json.Encoder.Encode`로 쓰므로 마지막 LF가 붙는다.

```go
type RecoveryProbe struct {
    Excluded string              `json:"excluded,omitempty"`
    Nonce    string              `json:"nonce"`
    Status   VoterRecoveryStatus `json:"status"`
    MAC      string              `json:"mac"`
}
type VoterRecoveryStatus struct {
    NodeID       string `json:"node_id"`
    ClusterID    string `json:"cluster_id"`
    Durability   string `json:"durability"`
    Ready        bool   `json:"ready"`
    Quorum       bool   `json:"quorum"`
    CertifiedTip uint64 `json:"certified_tip"`
    AppliedTip   uint64 `json:"applied_tip"`
    ArchiveTip   uint64 `json:"archive_tip"`
}
```

`excluded`만 빈 값이면 생략한다. 나머지 probe/status 필드는 zero여도 모두 존재한다. string은 JSON 문자열, bool은 JSON boolean, tip은 uint64 JSON 정수다. 이 구조에는 hash byte array나 base64 byte field가 없다. Node의 기본 status callback은 `ReadIndex`를 시도하고 그 오류 유무를 `quorum`에 넣는다. 나머지는 로컬 NodeID/ClusterID/durability, ready bit, Core tip, materializer tip, archive tip(archive 없으면 0)을 각각 읽은 관측값이며 하나의 원자 snapshot은 아니다. [Node callback](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L699).

### HMAC byte contract and acceptance

MAC 계산은 probe 구조체의 복사본에서 `MAC=""`로 바꾼 뒤 Go `json.Marshal`한 **전체 bytes**를 대상으로 한다. struct declaration 순서대로 `excluded`(nonempty일 때만), `nonce`, `status`, `mac`을 쓰며 nested status도 위 선언 순서다. `mac`에는 `omitempty`가 없으므로 preimage의 마지막 필드는 항상 `"mac":""`다. compact Go JSON이므로 separator 공백과 마지막 LF는 없다. 문자열은 Go encoding/json의 escaping을 적용하며 기본 HTML escaping도 적용한다. 응답 원문 bytes를 그대로 hash하거나 map key를 재정렬하지 않는다.

```text
key = []byte(token) 그대로 (UTF-8 text token이면 그 UTF-8 bytes; hex/base64 decoding·재인코딩 없음)
preimage = json.Marshal(probe with MAC = "")
mac = lowercase_hex(HMAC-SHA-256(key, preimage))
```

`VerifyRecoveryProbe`는 nonempty token과 nonce 문자열의 정확한 일치를 요구하고, 받은 `mac`을 hex decode한 뒤 다시 계산한 digest와 `hmac.Equal`로 비교한다. 인코더는 소문자 hex를 만들지만 검증기는 hex decode 가능한 대문자도 허용한다. nonce도 bytes 동치가 아니라 요청 challenge 문자열과 같아야 한다. MAC은 nonce·excluded·모든 status 필드를 포함한다. 반환 JSON의 공백·필드 순서 자체를 MAC 검증 대상으로 삼지는 않는다.

operator의 `automaticQuorumProbe`는 200 응답 하나를 `io.LimitReader(body, 16<<10)` 위의 일반 `json.Decoder`로 decode한다. `DisallowUnknownFields`, 뒤 JSON/EOF 검사, 전체 body 길이 검사는 하지 않는다. 따라서 이 16 KiB는 decoder가 읽을 수 있는 상한이지 전체 HTTP 응답이 반드시 그 이하라는 별도 검사 결과가 아니다. 이 응답 decoder를 일반 mutation request의 strict decoder와 혼동하지 않는다. operator는 순서대로 echoed `excluded` 일치, nonce/MAC 검증, expected cluster/node 일치, `status.quorum==true`를 요구한다. 이 자동 quorum probe 자체는 `ready`, durability나 세 tip의 하한을 검사하지 않는다. 준비/완료 판정의 더 강한 조건을 이 helper의 반환에 추가하지 않는다. body는 반환 시 닫는다.

### Order, errors and deadlines

handler는 method → bearer → nonce → status callback 존재 → read-admission 순으로 검사한다. status callback과 optional exclusion ReadIndex는 같은 handler context를 공유한다. 먼저 callback이 status를 만들고, exclusion이 비어 있지 않으면 Core가 없을 때 `quorum=false`, 그 외에는 `ReadIndexExcluding(ctx, excluded)`의 성공 여부로 `quorum`만 덮어쓴다. 실패·quorum 부재 자체는 HTTP 오류가 아니라 서명된 200 응답의 `quorum=false`다. 다른 status 필드는 그대로 둔다.

| 조건 | 응답/동작 |
|---|---|
| GET 이외 method | 405, `Allow: GET`, `method_not_allowed` / `method not allowed` |
| 빈 token 또는 bearer 불일치 | 403 `forbidden` / `recovery probe is not authorized` |
| nonce decode 실패 또는 길이 !=32 | 400 `invalid_request` / `32-byte challenge required` |
| status callback 없음 | 404 `not_a_voter` / `not a voter` |
| read admission 실패 | 공통 `writeAPIError` mapping: 예를 들어 503 `not_ready`/`overloaded`, 직접 context 오류는 500 `internal_error`다. |
| callback의 ReadIndex 실패 | 초기 status의 `quorum=false`로 표현한다. nonempty exclusion이 있으면 아래 검사로 덮어쓴다. |
| nonempty exclusion 검사 | Core가 없거나 exclusion ReadIndex가 실패하면 `quorum=false`, 성공하면 `true`로 덮어쓴다. 이 결과만으로 HTTP 오류를 만들지 않는다. |
| response encode/write 실패 | 반환 오류를 무시하며 추가 error envelope를 보내지 않는다. client가 완전한 유효 응답을 받는다는 보장은 없다. |

오류 JSON은 공통 `{ "code": string, "error": string }` envelope다. `ServeHTTP`의 종료 admission이 먼저 요청을 막으면 handler 검사 전에 503 `not_ready`가 될 수 있다. [dispatch](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1181), [error writers](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1594).

operator는 parent context에 3초 timeout을 추가한다. 난수·request 생성·transport·non-200·decode·검증 실패는 모두 `false`이며 상세 오류를 반환하지 않는다. controller HTTP client는 redirect를 따라가지 않는다. handler는 request context에 2초 timeout을 추가하고 일반 read permit을 응답 encode가 끝날 때까지 유지한다. 이 2초는 callback/Core에 전달하는 협력적 기한이지 handler goroutine 강제 종료 보장이 아니다. 기한 만료 뒤 callback이 반환하면 `quorum=false`의 200이 나올 수도 있다. timeout이나 false 결과는 process death·fence·재생성 방지·storage quiescence 증명이 아니다. [client deadline/checks](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/automatic_probe.go#L28), [HTTP client](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/operator/controller.go#L710).

`GET /recovery/status`는 같은 `VoterRecoveryStatus`를 envelope 없이 반환하지만 nonce/MAC이 없고 이 handler 자체에는 bearer 검사가 없다. GET 이외는 405이고 callback 부재는 404 `not_a_voter`다. 같은 2초 callback context와 read admission을 사용하며 성공 Content-Type은 JSON이다. probe의 `no-store` header를 이 status handler가 별도로 설정하지는 않는다. status 관측은 인증된 challenge response나 복구 권한을 대신하지 않는다. [status handler](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/recovery_status.go#L45).
