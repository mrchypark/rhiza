# 현재 전체 트리 Pro 재검토 로컬 대조

| 지적 | 로컬 판정 | 최소 처리 |
|---|---|---|
| stale `m.certified` block 재사용 | P1 확인 | publisher claim 뒤 authoritative CURRENT/root refresh 후 그 block만 재사용 |
| archive HEAD base 미보존 | P1 확인 | authoritative archive base index를 GC retention floor로 사용 |
| recovery pin 뒤 stale root delete | P1 확인 | pin record에 immutable descriptor 저장, active pin block을 GC live set에 포함 |

명시적 제외 범위는 recovery certificate signing/key rotation, peer mTLS, 모든
legacy/versioned compatibility/migration이다. 그 밖의 P0/P1/P2는 0이 될 때까지
수정·검증한다.
