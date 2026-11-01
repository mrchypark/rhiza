# C FFI Go EGM 보정 기록

- 기준: `HEAD 008f8e576ad76425182e10a69b987f10cfb48b2a`, 2026-10-30. 이는 현재 Go 소스에서 도출한 제한된 계약이다.
- 상태: **보정·독립 재검토 완료(발견 지적 해소; 검토 범위 한정)**. 원본 지적 집합에 남은 항목은 없다. 전체 EGM 5단계 승인이나 재구현 동등성 인증은 아니다.

## 원본 발견 목록과 증거

코드 대조 검토자 `01a18be7-f2f5-7943-b468-dc9cb01be18d`, 이해도 검토자 `01a18be8-9396-7ef2-a1ee-657575269cc2`, 모두 `fork_context:false`였다. [원문](remaining-areas-egm-review-evidence-2026-10-30.md)을 보존 근거로 삼는다.

1. Blocker: `ready`와 `object_store_stats`는 strict request decoding을 거치지 않는다. 존재하고 non-null인 JSON 값이면 request 부분은 통과한다. [dispatch](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/cmd/rhiza-ffi/main.go#L516).
2. Blocker: KV request field는 lowercase도 허용한다. untagged exported field의 Go JSON decoding은 case-insensitive이며 Rust wrapper도 `key`, `consistency`를 보낸다. [Go type](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1734), [decoder](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/cmd/rhiza-ffi/main.go#L355), [Rust](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/sdk/rust/src/lib.rs#L216).
3. Blocker: `found=true`인 empty KV 값은 `value`가 누락될 수 있고 Rust는 이를 empty vector로 해석한다. Go response의 applied-slot/tip field도 명시해야 한다. [Go response](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1735), [Rust handling](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/sdk/rust/src/lib.rs#L219).
4. Major: public stream defaults가 빠져 있다. limit 0은 100이 되고 1,000 초과 또는 wait 30,000 ms 초과는 거부된다. stream wait와 FFI call deadline을 구분해야 한다. [구현](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/graph.go#L241).
5. Major: 이해도 검토는 lowercase KV request 허용과 empty-value Rust 해석을 독립적으로 확인했다. [decoder](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/cmd/rhiza-ffi/main.go#L355), [Rust](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/sdk/rust/src/lib.rs#L216).

## 읽기 범위와 제한

코드 대조 검토자는 636개 대상 문서 줄, C bridge exports, registry/lifetime, timeout, decoder, complete dispatch/error mapping, Rust close/call/buffer/KV/subscription lifecycle, complete build script를 읽었다. subscription helper body의 반환 출력, 모든 nested membership/config schema와 모든 인용 경로는 완전 검증하지 않았다. 이해도 검토는 10개 대상 문서를 모두 읽고 위 발견의 좁은 소스만 확인했다. 테스트·빌드·쓰기·기억 조회·위임은 없었다.

## 원본 항목 처리 체크리스트

정본의 최종 독립 재검토 결과를 기록한다. A는 코드 대조 검토자, C는 이해도 검토자이며 위 ID와 대응한다. 이 문서 작성자는 재검토를 실행하지 않았다.

- [x] A9: `ready`/`object_store_stats` decoding 예외가 해소되었다.
- [x] A10 / C-C-FFI-1: lowercase KV request 허용 문제가 해소되었다.
- [x] A11 / C-C-FFI-2: empty value, Rust 해석 및 응답 필드 문제가 해소되었다.
- [x] A12: stream limit/wait 기본값과 deadline 구분이 해소되었다.

A의 전체 12개 지적 중 이 영역은 A9–A12이다. 최종 판정의 근거와 범위는 아래 최종 재검토 기록을 따른다.

## 최종 독립 재검토

[정본의 최종 재검토 원문](remaining-areas-egm-review-evidence-2026-10-30.md?plain=1#L7)을 확인했다. A(`01a18be7-f2f5-7943-b468-dc9cb01be18d`)는 12/12, B(`01a18be8-9217-70a1-bc39-8fad3c505002`)는 9/9, 이해도 C(`01a18be8-9396-7ef2-a1ee-657575269cc2`)는 7/7 지적의 해소를 확정했다. 세 검토자는 모두 `fork_context:false`로 시작했다. 이 집계에는 중복이 있으므로 고유 결함 28건으로 합산하지 않는다. 위 체크리스트가 이 영역에 해당하는 원본 지적을 구분한다.

첫 재검토에서는 Materializer의 `apply-populated` 잔존 문구, Node hash의 도메인 바이트·JSON 규칙, Operator 단계별 부수 효과 순서가 남았다. 이는 보정 과정의 이력이다. 추가 보정 후 세 검토자가 원본 지적 집합에 남은 항목이 없다고 확정했다. 최종 재검토는 해당 지적과 대응 소스의 범위에 한정되며, 앞서 기록한 미검토 범위를 확대하지 않는다. 검토자는 추가 테스트·수정·전체 감사를 수행하지 않았다.
