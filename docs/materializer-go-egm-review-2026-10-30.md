# Materializer Go EGM 보정 기록

- 기준: `HEAD 008f8e576ad76425182e10a69b987f10cfb48b2a`, 2026-10-30.
- 성격: 원하는 설계가 아니라 현재 Go 소스에서 도출한 제한된 계약이다.
- 상태: **보정·독립 재검토 완료(발견 지적 해소; 검토 범위 한정)**. 원본 지적 집합에 남은 항목은 없다. 전체 EGM 5단계 승인이나 재구현 동등성 인증은 아니다.

## 원본 발견 목록과 증거

코드 대조 검토자 `01a18be7-f2f5-7943-b468-dc9cb01be18d`와 이해도 검토자 `01a18be8-9396-7ef2-a1ee-657575269cc2`는 모두 `fork_context:false`였다. 원문은 [검토 증거](remaining-areas-egm-review-evidence-2026-10-30.md)에서 보존한다.

1. Blocker: `GraphReadStream`의 두 번째 반환값은 스트림 high-water가 아니라 `appliedSlot`이다. [원 구현](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/graph_enabled.go#L1094).
2. Blocker: Apply는 모든 mutation kind에서 fingerprint 충돌을 일관되게 강제하지 않는다. SQL, KV, notify는 기존 receipt를 건너뛰고 graph만 비교한다. [SQL](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1359), [KV](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1540), [notify](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1320), [graph](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/graph_enabled.go#L558).
3. Blocker: SQL 성공 receipt의 `Applied`는 true로 설정되지 않으며 false로 남는다. [구현](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1397).
4. Blocker: `applyKV`는 TTL에서 만료를 계산하지 않고 제공된 절대 만료와 관찰 시각을 쓴다. Bloom filter는 durable deduplication이 아니라 SQLite 조회 필요성 최적화다. [KV](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1545), [Bloom](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L579), [pruning](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1052).
5. Blocker: restore 실패가 원본의 재개방·보존을 보장하지 않는다. precommit rollback, committed replacement의 cleanup 실패, 재개방 실패를 분리해야 한다. [rollback](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L2807), [commit](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L2899), [recovery](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L2709).
6. Blocker: 일반적 hash 일치 replay 규칙은 older-slot 처리와 다르다. older slot은 실행 정책 검증 뒤 skip되고 current tip에서만 hash를 확인한다. [구현](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1146).
7. Blocker: local pair publication은 atomic하지 않다. graph와 SQLite rename 사이의 `backup-sql`, `backup-graph`, `publish-graph`, `published` restart 상태를 보존해야 한다. [phase](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/local_restore.go#L379), [publication](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/local_restore.go#L789), [install](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/local_restore.go#L175).
8. Major: retained-result 한도 `256 MiB`, receipt cap `64 × window`, envelope-count와 result-byte 고갈의 서로 다른 오류 결과가 누락되었다. [policy](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/result_retention.go#L13), [apply](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L1433).
9. Major: 이해도 검토도 stream read의 `appliedSlot`/stream high-water 혼동을 독립적으로 확인했다. [원 구현](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/graph_enabled.go#L1098).

## 읽기 범위와 제한

코드 대조 검토자는 대상 문서 636줄, batch apply, 중복 처리, expiry/Bloom, retained result, graph transaction, stream, checkpoint, ordinary/local restore를 읽었다. open/schema 초기화, SQL policy guard, graph validation/query/reachability, 모든 nested membership/config schema와 인용 경로 전체는 검토하지 않았다. 이해도 검토는 10개 대상 문서를 모두 읽고 이 발견을 뒷받침하는 좁은 소스만 확인했으며, 전이 엔진과 모든 nested type은 미검토다. 두 검토 모두 테스트·빌드·쓰기·기억 조회·위임을 수행하지 않았다.

## 원본 항목 처리 체크리스트

정본의 최종 독립 재검토 결과를 기록한다. A는 코드 대조 검토자, C는 이해도 검토자이며 위 ID와 대응한다. 이 문서 작성자가 재검토를 실행한 것은 아니다.

- [x] A1 / C-Materializer: stream 반환값 구분이 해소되었다.
- [x] A2: mutation별 fingerprint 및 중복 처리 구분이 해소되었다.
- [x] A3: SQL `Applied=false` 계약이 해소되었다.
- [x] A4: expiry 입력과 Bloom 조회 최적화 구분이 해소되었다.
- [x] A5: restore 실패 단계와 재개방 한계가 해소되었다.
- [x] A6: older-slot/current-tip replay 구분이 해소되었다.
- [x] A7: local pair publication의 중간 단계가 해소되었다.
- [x] A8: retention 수치와 오류 결과 구분이 해소되었다.

A의 전체 12개 지적 중 이 영역은 A1–A8이다. 후속 결과는 해당 항목만 닫으며, 최종 판정의 근거와 범위는 아래 최종 재검토 기록을 따른다.

## 최종 독립 재검토

[정본의 최종 재검토 원문](remaining-areas-egm-review-evidence-2026-10-30.md?plain=1#L7)을 확인했다. A(`01a18be7-f2f5-7943-b468-dc9cb01be18d`)는 12/12, B(`01a18be8-9217-70a1-bc39-8fad3c505002`)는 9/9, 이해도 C(`01a18be8-9396-7ef2-a1ee-657575269cc2`)는 7/7 지적의 해소를 확정했다. 세 검토자는 모두 `fork_context:false`로 시작했다. 이 집계에는 중복이 있으므로 고유 결함 28건으로 합산하지 않는다. 위 체크리스트가 이 영역에 해당하는 원본 지적을 구분한다.

첫 재검토에서는 Materializer의 `apply-populated` 잔존 문구, Node hash의 도메인 바이트·JSON 규칙, Operator 단계별 부수 효과 순서가 남았다. 이는 보정 과정의 이력이다. 추가 보정 후 세 검토자가 원본 지적 집합에 남은 항목이 없다고 확정했다. 최종 재검토는 해당 지적과 대응 소스의 범위에 한정되며, 앞서 기록한 미검토 범위를 확대하지 않는다. 검토자는 추가 테스트·수정·전체 감사를 수행하지 않았다.
