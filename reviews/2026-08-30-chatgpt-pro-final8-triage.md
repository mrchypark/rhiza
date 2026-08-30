# final8 Pro 재검토 로컬 대조

| 지적 | 로컬 판정 | 상태 |
|---|---|---|
| WAL manifest 전 crash orphan | 재현 가능한 P1 | startup reconcile/테스트 필요 |
| stale GC의 재사용 block/extent Delete | 재현 가능한 P1 | object deletion fencing 필요 |
| expired pin renewal vs sweep | 재현 가능한 P1 하위 항목 | CAS tombstone과 회귀 테스트 구현 완료 |
| KVGetAt 무한 optimistic retry | 재현 가능한 P2 | 단일 SQLite snapshot query로 제거, 회귀/race 통과 |
| WAL manifest/temp 무한 증가 | 재현 가능한 P2 | bounded cleanup/startup reconcile 필요 |
| SSE close 후 zero-payload loop | 독립 검토에서 추가 확인 | channel close 처리와 회귀/race 구현 완료 |
| manifest rename 후 dir-fsync 오류 rollback | 독립 검토에서 추가 확인 | published generation 보존과 reopen 회귀/race 구현 완료 |

명시적 제외 범위는 recovery certificate signing/key rotation과 peer mTLS입니다. 그 외 항목은 P0/P1/P2가 0이 될 때까지 구현·검증합니다.
