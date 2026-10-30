# Go 역명세 문서 색인

날짜는 2026-10-30이다. QuePaxa의 독립 기준은 `8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd`이며, 나머지 10개 영역의 기준은 `008f8e576ad76425182e10a69b987f10cfb48b2a`다. 기준과 판정은 각 영역 기록을 우선한다. 각 문서는 원하는 설계가 아닌 코드에서 도출한 제한된 계약이다. 문서 링크는 상대 경로이며, 소스 코드 링크는 커밋 `8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd`에 고정된 GitHub URL이다.

| 영역 | 본문 | 부록 | EGM 기록 |
| --- | --- | --- | --- |
| quepaxa | [main](quepaxa-go-reverse-spec-2026-10-30.md) | [appendix](quepaxa-go-formats-2026-10-30.md) | [review](quepaxa-go-egm-review-2026-10-30.md) |
| qlog | [main](qlog-go-reverse-spec-2026-10-30.md) | [appendix](qlog-go-formats-2026-10-30.md) | [review](qlog-go-egm-review-2026-10-30.md) |
| checkpoint | [main](checkpoint-go-reverse-spec-2026-10-30.md) | [appendix](checkpoint-go-formats-2026-10-30.md) | [review](checkpoint-go-egm-review-2026-10-30.md) |
| object-store | [main](object-store-go-reverse-spec-2026-10-30.md) | [appendix](object-store-go-contracts-2026-10-30.md) | [review](object-store-go-egm-review-2026-10-30.md) |
| archive-recovery | [main](archive-recovery-go-reverse-spec-2026-10-30.md) | [appendix](archive-recovery-go-formats-2026-10-30.md) | [review](archive-recovery-go-egm-review-2026-10-30.md) |
| request-http | [main](request-http-go-reverse-spec-2026-10-30.md) | [appendix](request-http-go-contracts-2026-10-30.md) | [review](request-http-go-egm-review-2026-10-30.md) |
| materializer | [main](materializer-go-reverse-spec-2026-10-30.md) | [appendix](materializer-go-contracts-2026-10-30.md) | [보정·독립 재검토 완료](materializer-go-egm-review-2026-10-30.md) |
| node-lifecycle | [main](node-lifecycle-go-reverse-spec-2026-10-30.md) | [appendix](node-lifecycle-go-contracts-2026-10-30.md) | [보정·독립 재검토 완료](node-lifecycle-go-egm-review-2026-10-30.md) |
| operator | [main](operator-go-reverse-spec-2026-10-30.md) | [appendix](operator-go-contracts-2026-10-30.md) | [보정·독립 재검토 완료](operator-go-egm-review-2026-10-30.md) |
| peer-transport | [main](peer-transport-go-reverse-spec-2026-10-30.md) | [appendix](peer-transport-go-contracts-2026-10-30.md) | [보정·독립 재검토 완료](peer-transport-go-egm-review-2026-10-30.md) |
| c-ffi | [main](c-ffi-go-reverse-spec-2026-10-30.md) | [appendix](c-ffi-go-contracts-2026-10-30.md) | [보정·독립 재검토 완료](c-ffi-go-egm-review-2026-10-30.md) |

앞선 6개 영역의 기존 기록은 읽기 전용으로 유지했다. 마지막 5개 영역은 정본의 최종 독립 재검토 결과를 반영했다.

QuePaxa의 추가 근거 부록은 [protocol](quepaxa-go-protocol-evidence-2026-10-30.md), [durability](quepaxa-go-durability-evidence-2026-10-30.md), [membership](quepaxa-go-membership-evidence-2026-10-30.md)이다. 표의 부록은 실제 파일에 따라 formats 또는 contracts를 가리킨다.

마지막 5개 영역의 상태는 **보정·독립 재검토 완료(발견 지적 해소; 검토 범위 한정)**다. A 12/12, B 9/9, 이해도 7/7 지적이 해소되었으며 원본 지적 집합에 남은 항목은 없다. 이 집계는 중복을 포함하므로 고유 결함 28건을 뜻하지 않는다. [최종 재검토 및 원본 리뷰](remaining-areas-egm-review-evidence-2026-10-30.md)와 [읽기 전용 검증 기록](remaining-areas-verification-2026-10-30.md)을 함께 확인한다. 이 색인은 전체 EGM 5단계 승인이나 재구현 동등성 인증을 부여하지 않는다.

## 영역 간 연결 계약 — 통합 경계 검토 완료

현재 기준 `008f8e576ad76425182e10a69b987f10cfb48b2a`의 [영역 간 계약 허브](cross-area-go-contracts-2026-10-30.md)는 11개 영역을 17개 실제 producer/consumer edge로 연결한다. 타입·identity·hash, 호출 순서와 성공 경계, 취소·retry·자원 소유권, restore·fencing·generation, 크기·timeout 검증 위치와 대표 SQL 쓰기→checkpoint 흐름을 제공한다.

상태는 **보정·독립 통합 경계 검토 완료(지정 범위 한정)**다. 고유 Blocker/Major 16건을 원문과 연결 계약에 보정했다. A의 E01–E08/E17 및 해당 C7 조항은 내용이 유지된 상태에서 R3/R4 소스 검토의 추가 지적이 0건이었다. 최종 동일 규범 snapshot을 대상으로 한 두 독립 병렬 batch R6/R7의 B 소스 검토와 C 전체 흐름 이해도 검토는 모두 Blocker/Major 0건이었다. R7-C의 Nit 1건은 E03 Append URL을 실제 `wal.go:662` 선언 위치로 보정했다. 두 batch를 직렬 검토로 표현하지 않는다.

[통합 검토 기록](cross-area-go-egm-review-2026-10-30.md)과 [원본 검토 증거](cross-area-go-review-evidence-2026-10-30.md)에 지적·보정·검토 범위를 기록한다. 직접 연결의 [probe wire 규범](request-http-go-contracts-2026-10-30.md#recovery-probe-wire-contract)은 HTTP 부록이 소유한다. 범위는 E01–E17과 C7의 직접 owner/source 연결이며 전체 엔진의 재귀적 감사, 전체 EGM 5단계 인증 또는 재구현 동등성 인증이 아니다. 코드·런타임은 변경하지 않았고 새 테스트 통과를 주장하지 않는다. S-01/S-02 등 정적으로 확인한 구현 제한을 명세에 보존했다.

QuePaxa의 역사적 `8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd` 기준과 과거 영역별 검토·검증 기록은 유지한다. 전달받은 baseline 비교에서 현재 `008f8e576ad76425182e10a69b987f10cfb48b2a`와 비문서 차이는 없었으며, 허브는 현재 연결 보정을 기록한다. 이번 완료 판정은 위 표의 역사적 영역별 판정과 구별한다.

## Publication links

Markdown URL normalization only: source/code links are pinned to `8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd`. Local SDK, skill, and test-log references remain local-only evidence with their original literal paths; no raw logs were uploaded. Historical source findings were not re-audited, and no new test claims are made.
