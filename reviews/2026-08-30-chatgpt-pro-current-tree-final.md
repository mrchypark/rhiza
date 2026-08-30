# ChatGPT Pro 현재 전체 트리 최종 재검토 회수본

- 회수일: 2026-08-30
- 세션: https://chatgpt.com/g/g-p-69ecdc42175c819186cf485b225c0e46-codex-request/c/6a93243b-f0b0-83ee-9749-588750073a04
- 소스 SHA-256: `6f9b9edfc145e9ca66621ce72fa14e17dc504b8f8c7c2fc5bae51d2bc6ffb105`
- 패치 SHA-256: `b05602eabe3df5420eebffe1120201f0eabda9486abe2056e57bb37e95a2d86f`
- 처리 시간: 145분 26초

## 판정

현재 트리는 릴리스 가능 상태가 아니다. 기존 네 건 중 WAL 복구, KVGetAt,
WAL 보존 상한은 닫혔지만 stale checkpoint GC 문제는 완전히 닫히지 않았다.
체크포인트 객체 재사용, 아카이브 복구 기준점 보존, 복구 핀과 물리 삭제의
선형화에서 재현 가능한 P1 세 건이 남아 있다.

Pro 환경에는 Go 1.23.2만 있고 저장소는 Go 1.27과
`GOEXPERIMENT=arenas,greenteagc`를 요구해 테스트/race/vet를 독립 재실행하지
못했다. 아래 판정은 첨부 소스의 상태 전이와 object-store interleaving을 직접
추적한 결과이며, `IfNotExists`와 `PutIfMatch`가 키 단위로 선형화된다는 전제를
유지했다.

## P1-1: stale `m.certified` block 재사용

위치:

- `pkg/checkpoint/checkpoint.go`: `Manager.CreateFiles`, `uploadFile`
- `pkg/checkpoint/auto.go`: publisher claim 획득 후 `CreateFiles` 경로

새 publisher claim을 얻은 뒤 원격 `CURRENT`를 다시 읽지 않고 프로세스
메모리의 `m.certified`로 `knownBlocks`를 만든다. 알려진 hash/size이면 기존
generation과 물리 키를 새 root에 그대로 기록하고 upload를 생략한다. canonical
충돌 때 generation 전용 key를 쓰는 fencing은 이 fast path에서 실행되지 않는다.

재현 interleaving:

1. A는 C1/B1을 `m.certified`로 기억한다.
2. 다른 peer가 C2/C3를 게시해 공유 `CURRENT`를 전진시킨다.
3. GC가 C1 root를 제거하고 다음 주기에 B1 delete 직전 정지한다.
4. lease 만료 뒤 A가 새 publisher claim을 획득한다.
5. A가 B1 내용을 포함한 C4를 만들면서 stale known-block fast path로 B1을 재참조한다.
6. C4가 인증·승격된 뒤 stale GC가 B1을 삭제한다.

결과적으로 `CURRENT`의 인증 root가 없는 block을 참조해 복원이 실패한다.

권고: claim 획득 후 authoritative `CURRENT`와 root를 다시 읽고 그 root의 block만
재사용한다. 집합 밖의 block은 canonical `IfNotExists`를 거쳐 충돌 시 현재 claim
generation 전용 key에 upload한다. root 게시 직전 claim을 다시 검증한다. 테스트는
stale manager, 두 번의 GC, block delete pause, lease takeover, republish, stale delete
resume 뒤 복원 성공과 물리 key 분리를 검증한다.

## P1-2: archive HEAD base checkpoint 미보존

위치:

- `pkg/node/node.go`: `runCheckpointGC`, 시작 복구 경로
- `pkg/recovery/archive.go`: `RecoveryBase`
- `pkg/checkpoint/checkpoint.go`: `GarbageCollect`

checkpoint GC retain 집합에는 로컬 core의 latest/recovery seal만 있고 공유 archive
HEAD의 `BaseSeal.RootHash`가 없다. archive trim/HEAD 갱신이 실패해 base가 C1에
남는 동안 C2/C3/C4와 checkpoint `CURRENT`가 전진하면 keep=2 GC가 C1 root와
후속 주기의 block을 삭제할 수 있다. 빈 디스크 복구는 archive HEAD가 가리키는
C1 descriptor를 열지 못해 object-storage-first recovery가 실패한다.

Pro 권고는 archive generation별 immutable retain marker였다. 로컬 재검증에서는
archive base index가 단조 증가하므로, GC 직전 authoritative archive HEAD를 load하고
그 base index 이상인 모든 checkpoint root를 retention floor로 보존하는 더 작은
동등 안전 해법을 채택한다. concurrent trim은 base를 전진만 시키므로 이미 읽은
floor 이상에 머문다. 테스트는 C1 archive base, C2-C4 current 전진, GC/emptyDir
복구와 C1->C2 HEAD 전환 경쟁을 검증한다.

## P1-3: recovery pin 이후 stale root delete

위치:

- `pkg/checkpoint/checkpoint.go`: `PinRecoveryRoot`, `activeRecoveryRoots`,
  `GarbageCollect`의 root delete 경로

GC가 maintenance claim을 검증한 뒤 무조건부 `Delete(root)` 직전에 정지하고 lease를
잃을 수 있다. 새 owner가 같은 root를 성공적으로 pin한 뒤 stale GC가 재개하면
canonical root를 삭제한다. 현재 pin은 root hash만 보존하므로 descriptor가 사라진
뒤 block live set을 재구성할 수 없고 후속 GC가 block도 삭제할 수 있다. delete 직전
claim 재검증은 같은 TOCTOU 창을 다시 만들 뿐이다.

Pro의 최소 권고와 로컬 검증 결론은 pin-owned immutable descriptor다.
`PinRecoveryRoot`가 maintenance claim 아래 canonical root bytes를 읽고 token/CAS로
보호되는 pin record에 descriptor를 저장한다. 복구는 pin descriptor를 사용하고,
GC는 active pin descriptor의 root hash와 모든 block key를 live로 취급한다. canonical
root가 stale GC에 의해 삭제돼도 pin lease 동안 복구에 필요한 descriptor와 blocks는
남는다. 테스트는 root delete pause, lease takeover, pin 성공, stale delete 재개,
두 번째 GC 뒤 pinned descriptor로 전체 파일 복원을 검증한다.

## 기존 지적 폐쇄 판정

- WAL orphan/manifest crash recovery: 닫힘.
- stale checkpoint/archive GC fencing: 부분적으로만 닫힘.
- KVGetAt 무한 optimistic retry: 닫힘.
- WAL manifest/temp 무제한 보존: 닫힘.

## 성능 판정

Final13에서 object-store HTTP 호출은 Graph write -22.7%, Graph KV -30.3%, SQL
write -8.7%, SQL KV -17.9%였다. Graph CPU/메모리는 비회귀, SQL CPU/메모리는
개선으로 기록됐으며, 이에 반하는 코드 확정 steady-state 회귀는 찾지 못했다.

수정 비용은 checkpoint 생성당 authoritative `CURRENT` refresh 1회, archive base
전진당 상수 연산, recovery 시작 시 pin descriptor 1회로 제한해야 한다. 모든 이전
block을 매 checkpoint마다 재upload하는 구현은 허용하지 않는다.

## 최종 판정 및 다음 조치

`P0 0, P1 3, P2 0`.

세 interleaving을 deterministic fake-bucket 테스트로 먼저 고정하고 claim-bound
authoritative block reuse, archive-base retention, pin-owned root descriptor를 구현한
뒤 Go 1.27 기본/Graph 전체 테스트, focused race, vet, qlog crash matrix, Dory
emptyDir 복구, 1초 checkpoint stress와 Final13 A/B를 같은 트리에서 재실행한다.
