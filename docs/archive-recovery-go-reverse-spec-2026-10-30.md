# Archive·복구 Go 구현 역명세

기준 HEAD: `008f8e576ad76425182e10a69b987f10cfb48b2a`, 기준일 2026-10-30. 이 문서는 pkg/recovery와 직접 연결된 node의 backup·staging·startup 경로를 코드에서 역도출한다. 전체 Operator·실제 클라우드·합의의 형식적 증명은 범위 밖이다. 두 분석자의 근거를 HA와 Local로 통합했다. 형식은 [형식 부록](archive-recovery-go-formats-2026-10-30.md), 검토는 별도 기록을 따른다. 코드 확인과 테스트 실행은 각 절에서 구분한다.

## HA archive·세대 복구

기준은 rhiza **`008f8e576ad76425182e10a69b987f10cfb48b2a`**입니다. 아래는 지정한 7개 파일과 인증·fencing 경계를 확인하는 좁은 호출 경로에 근거한 역명세입니다. 클라우드 서비스의 일관성이나 형식적 안전성은 추론하지 않았습니다.

**1. 소유권과 신뢰 경계**

Archive는 연속된 결정의 바이트와 복구 기준점을 객체 저장소에 보관합니다. `Manager.configID`는 **불변 archive 식별자**이며, 각 인증서가 속한 consensus configuration과 구별됩니다. voter 교체 시에도 archive 식별자는 유지하도록 정의되어 있습니다. [archive.go:170](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L170)

검증 책임은 다음과 같이 나뉩니다.

| 경로 | 실제 검사 및 책임 |
|---|---|
| Archive `Load`·extent 읽기 | 포맷, CRC, 객체 SHA-256, 값 SHA-256, slot 연속성, prefix 연결, archive config, 객체 generation 관계를 검사합니다. |
| Archive 인증서 취급 | 인증서가 비어 있지 않은지는 검사하지만 voter 권한과 quorum을 직접 검증하지 않습니다. |
| Fork 검증·materialization | 호출자가 제공한 bootstrap으로 Observer Core를 만들고 `RestoreCheckpointBase`, `AcceptCertifiedValues`를 호출합니다. |
| Generation anchor | 외부에서 fenced된 세대 전환의 계보를 나타냅니다. consensus 인증서가 아닙니다. |
| Seal·Fork | 모든 기존 writer의 fencing을 호출자에게 요구합니다. 프로세스 종료나 storage I/O 차단을 직접 수행하지 않습니다. |

근거: [archive.go:2354](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L2354), [archive.go:2406](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L2406), [fork.go:346](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/fork.go#L346), [seal.go:18](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/seal.go#L18)

Core의 checkpoint 복원은 seal과 certified decision의 일치, membership 및 leader 정보, recovery decision 검증, 등록된 checkpoint validator, 기존 floor·prefix·lineage 일치를 확인합니다. 따라서 Archive 로딩 성공만으로 인증된 복구가 완료되지는 않습니다. [compaction.go:159](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/compaction.go#L159)

**2. 저장 키와 JSON 계약**

아래 키는 namespace prefix 아래의 상대 경로입니다. 일반 `Manager`는 prefix를 정규화하지 않고 `/`로 연결합니다. Fork는 별도로 경로를 정규화하며 빈 경로, `..` segment, 동일 경로 및 서로 포함되는 source/target을 거부합니다. [archive.go:2451](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L2451), [fork.go:452](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/fork.go#L452)

| 키 | 내용 |
|---|---|
| `archive/head.bin` | 현재 공개된 archive HEAD입니다. |
| `archive/blocks/<sha256>_<generation:020d>.bin` | extent 바이트입니다. 해시는 전체 extent 바이트를 대상으로 합니다. |
| `archive/PUBLISH_LOCK` | publication lease입니다. |
| `archive/GC_LOCK` | snapshot 생성·trim·GC를 조정하는 lease입니다. |
| `archive/recovery-pins/<SHA256(owner)>` | 고정한 HEAD의 recovery pin입니다. |
| `archive/gc-candidates/<SHA256(full-object-key)>` | 삭제 후보 marker입니다. 본문은 대상 객체 키입니다. |
| `archive/seal.json` | seal 작업의 불변 intent입니다. |
| `fork/intent.json` | fork 요청과 source HEAD digest를 결합합니다. |
| `fork/result.json` | fork 결과입니다. |
| `recovery/anchor.json` | materialized target 세대의 anchor입니다. |

근거: [archive.go:1536](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L1536), [archive.go:2423](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L2423), [fork.go:492](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/fork.go#L492), [anchor.go:42](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/anchor.go#L42)

주요 JSON 필드는 다음과 같습니다.

- Lock: `owner_id`, `generation`, `lease_until_unix_ms`.
- Recovery pin: `owner_id`, `token`, `base`, `tip`, `tail_hash`, `tail_object`, `lease_until_unix_ms`. token은 난수 16바이트의 hex 표현입니다. `[32]byte` 해시 필드는 Go JSON의 숫자 배열로 직렬화됩니다.
- Seal intent: `version:1`, `operation_id`.
- Fork intent: `version`, `operation_id`, `source_prefix`, `target_prefix`, 정렬된 `member_ids`, `manifest_hash`. v2는 `source_bootstrap_hash`, `target_membership_hash`를 추가합니다.
- Fork result: `tip`, `prefix_hash`, `manifest_hash`.
- Generation anchor: `version:1`, `operation_id`, `source_prefix`, `target_prefix`, `source_manifest`, `source_tip`, `source_prefix_hash`, `source_bootstrap_hash`, `target_membership`, `checkpoint`.
- Anchor의 checkpoint: `index`, `root_hash`, `state_hash`. `index == source_tip`이어야 합니다.

Lock·pin의 객체 version은 메모리에서만 유지합니다. Seal intent, fork intent/result, anchor는 읽은 바이트가 해당 Go 구조체의 `json.Marshal` 결과와 정확히 같아야 합니다. Lock·pin에는 이 canonical-byte 비교가 없습니다. `validForkHash`는 32바이트로 hex decode되는지를 검사하며 소문자만 강제하지는 않습니다. [archive.go:98](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L98), [fork.go:568](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/fork.go#L568), [fork.go:644](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/fork.go#L644), [anchor.go:44](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/anchor.go#L44)

**3. 바이너리 포맷·해시·상한**

정수는 big-endian이며, CRC는 Castagnoli CRC32입니다.

Extent는 144바이트 header, decision records, 마지막 4바이트 CRC로 구성됩니다.

| 바이트 구간 | 내용 |
|---|---|
| `[0,8)` | `RHZAEXT!` |
| `[8,12)` | 전체 길이 `u32` |
| `[12,16)` | 예약 영역, 0 |
| `[16,24)` | archive config ID `u64` |
| `[24,32)` | 시작 slot `u64` |
| `[32,36)` | decision 개수 `u32` |
| `[36,40)` | 예약 영역, 0 |
| `[40,72)` | 시작 prefix |
| `[72,104)` | 끝 prefix |
| `[104,136)` | 이전 extent hash |
| `[136,144)` | 이전 객체 generation |

각 decision은 `value_length:u32`, `certificate_length:u32`, value, certificate 순서입니다. slot은 시작 slot과 record 순서로 복원하며 value hash는 다시 계산합니다. 별도 slot·value hash 필드는 저장하지 않습니다. [archive_codec.go:39](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive_codec.go#L39)

HEAD는 128바이트 header, 가변 payload, 4바이트 CRC로 구성됩니다.

| 바이트 구간 | 내용 |
|---|---|
| `[0,8)` | `RHZAHD2!` |
| `[8,12)` | 전체 길이 `u32` |
| `[12,16)` | flags: checkpoint base=1, sealed=2, anchor=4 |
| `[16,24)` | archive config ID |
| `[24,32)` | HEAD generation |
| `[32,40)` | base |
| `[40,72)` | base prefix |
| `[72,80)` | tip |
| `[80,112)` | tail hash |
| `[112,120)` | tail 객체 generation |
| `[120,124)` | 0이어야 하는 seal length |
| `[124,128)` | payload length |

Checkpoint base payload는 `decision_slot:u64`, value/certificate 길이 각 `u32`, value, certificate입니다. seal은 value에서 decode합니다. Anchor base는 32바이트 anchor hash를 저장합니다. Checkpoint base와 lineage가 함께 있으면 base decision 뒤에 32바이트 lineage hash를 추가합니다. 알 수 없는 flag는 오류입니다. [archive_codec.go:126](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive_codec.go#L126)

해시 계약은 다음과 같습니다.

- Value hash: `SHA256(value)`.
- Prefix: `SHA256("rhiza/decision-prefix\x00" || previous_prefix || slot_BE64 || value_hash)`. 최초 prefix는 0입니다.
- Extent hash: CRC를 포함한 전체 extent 바이트의 SHA-256.
- Fork `manifest_hash`: sealed source HEAD를 encode한 전체 바이트의 SHA-256.
- Anchor hash: canonical anchor JSON 바이트의 SHA-256.
- Source bootstrap hash: `json.Marshal(Cluster)`의 SHA-256이며 별도 정렬 처리는 없습니다.
- Target membership hash: `json.Marshal(MembershipRecord)`의 SHA-256.

근거: [types.go:33](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L33), [fork.go:138](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/fork.go#L138), [anchor.go:72](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/anchor.go#L72)

MembershipRecord v2는 `version`, `cluster`, `membership`, `durability`로 구성됩니다. 각 member의 `[ID, public_key_hex]`를 JSON 문자열로 만든 뒤 문자열 목록을 정렬하고 JSON encode합니다. `membership`은 `"rhiza-membership-v2\x00"`와 그 결과를 이어 붙인 SHA-256입니다. private peer token은 이 계산에 포함하지 않습니다. 빈 durability는 `async`가 됩니다. 생성 함수 자체가 member 중복이나 임의 durability 문자열을 검증하지는 않습니다. [membership.go:13](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/membership.go#L13)

기본값과 상한은 다음과 같습니다.

| 항목 | 값 |
|---|---|
| Extent 전체 크기 | 최대 8 MiB |
| Extent decision 수 | 1~1024개 |
| Value 크기 | 1~128 KiB |
| Value+certificate | 최대 8,388,452바이트, 둘 다 비어 있으면 안 됨 |
| Binary HEAD | 최대 8,388,632바이트 |
| JSON 제어 객체 읽기 | 최대 64 KiB |
| 기본 read page | `limit <= 0`이면 256개 |
| Extent LRU cache | 2개 |
| Group delay | 2ms, setter는 양수만 반영 |
| Batch sync timeout | 5분 |
| 주요 bounded retry | 8회 |
| Archive lock lease/갱신 주기 | 2분/40초 |
| Fork evidence lease/갱신 주기 | 10분/200초 |
| Lock 대기 간격 | 50ms |
| Background release/close 제한 | 5초 |
| GC 삭제 동시 실행 수 | 4개 |

64 KiB는 binary HEAD 상한이 아닙니다. [archive.go:29](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L29), [archive_codec.go:18](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive_codec.go#L18), [types.go:17](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L17)

**4. Load와 HEAD publication**

`Load`는 HEAD의 Attributes→Get→Attributes로 version 안정성을 확인합니다. 이미 알고 있는 non-nil version이 같으면 본문을 재다운로드하지 않습니다. Tail에서 이전 객체를 역추적하고 base까지 연결한 뒤 정방향 slot·prefix 연속성을 확인하며, 마지막 HEAD version과 로컬 CAS 상태도 다시 확인합니다. 재시도 동안 검증한 immutable extent 참조를 재사용합니다. [archive.go:198](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L198)

검증 조건은 다음과 같습니다.

- 공개 HEAD의 generation은 0이 아니어야 합니다.
- `base <= tip`이어야 합니다.
- `tip > base`와 tail 존재 여부가 일치해야 합니다.
- Tail hash가 0인 것과 tail object가 0인 것이 일치해야 합니다.
- Tail object는 HEAD generation보다 클 수 없습니다.
- 관측한 sealed 상태의 해제, generation·base·tip 감소, 같은 base에서 recovery base 내용 변경은 거부합니다.
- Trim 경계가 기존 extent 내부에 있으면 객체를 다시 쓰지 않고 논리적 시작점과 prefix를 조정하여 suffix를 검증합니다.

처음 HEAD가 없고 과거 tip도 0이면 빈 archive로 취급할 수 있습니다. 다만 `archive/extents`가 발견되면 지원하지 않는 레이아웃으로 거부합니다. 이미 nonzero tip을 관측했다면 HEAD 소실은 오류입니다. 이 빈 archive 처리를 generation anchor 누락 시 bootstrap 허용으로 확대할 수는 없습니다. [archive.go:217](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L217)

Publication은 **extent upload → HEAD upload → HEAD readback 및 로컬 상태 갱신** 순서입니다.

- CAS 지원 여부는 bucket이 `IfMatch`와 `IfNotExists`를 모두 광고하는지로 판단합니다.
- HEAD 최초 생성은 `IfNotExists`, 갱신은 기존 version의 `IfMatch`를 사용합니다.
- HEAD 쓰기 직전에 publication lease의 owner·generation·만료를 검사합니다. 작업 성공 후에도 lease를 다시 확인합니다.
- Extent는 다음 HEAD generation을 객체 이름에 사용하며 CAS 모드에서는 `IfNotExists`로 생성합니다.
- Extent upload가 typed condition 오류 또는 `EPIPE`를 반환하면, context가 살아 있고 같은 객체를 읽어 해시·포맷·내용 검증에 성공한 경우에만 성공으로 전환합니다.
- Sync·trim·GC는 CAS 모드에서 HEAD publication의 일반 오류도 재로드 후 재시도할 수 있습니다. Seal은 typed condition 오류만 재시도합니다.

근거: [archive.go:938](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L938), [archive.go:1001](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L1001), [archive.go:1044](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L1044), [seal.go:50](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/seal.go#L50)

`publishHead`에는 CAS 미지원 시 조건 없이 쓰는 분기가 있지만, 이것만으로 해당 bucket에서 안전한 동시 publication을 보장하지 않습니다. 별도 lock 경로도 조건부 쓰기를 사용합니다.

**5. Batching과 lease 상태 전이**

`SyncThrough(through)`는 `through == 0` 또는 로컬 tip이 이미 충분하면 즉시 성공합니다. 이 fast path는 closed·sealed 원격 상태를 확인하기 전입니다.

나머지 요청은 같은 batch에 합류하여 최대 target을 공유합니다. flush 시 source의 현재 tip도 포함하므로 요청 slot보다 더 멀리 공개할 수 있습니다. Publication admission 도중 합류한 요청도 작업 진입 후 다시 반영합니다. 한 호출자의 context 취소는 그 대기만 종료하며, 공유 flush는 Manager context와 5분 timeout으로 계속될 수 있습니다. `Close`는 Manager context를 취소하고 batch 종료를 기다립니다. [archive.go:812](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L812)

Lock은 absent→generation 1 또는 expired→generation+1로 조건부 획득하고, 저장된 레코드를 readback하여 확인합니다. 갱신은 동일 owner·generation의 살아 있는 lease에만 허용합니다. 해제는 삭제가 아니라 만료시각을 현재로 바꾸는 CAS입니다.

Publication lock이 작업 진입 전에 Busy를 반환하면 deadline이 있는 호출만 50ms 간격으로 기다립니다. 작업에 진입한 뒤에는 전체 작업을 자동 반복하지 않습니다. Sync의 대기 경로는 원격의 검증된 unsealed HEAD가 target을 이미 덮었는지 재확인하여 성공할 수 있습니다. 갱신 실패는 작업 context를 취소합니다. [archive.go:1759](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L1759), [archive.go:1801](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L1801)

**6. Snapshot pin·trim·GC**

Recovery snapshot은 GC lock 아래에서 HEAD와 extent 참조를 고정하고 owner별 pin을 생성합니다. 살아 있는 같은 owner의 pin이 있으면 Busy입니다. 생성은 조건부 upload 후 **전체 레코드 readback**으로 확인합니다. GC lock 획득만 lease 길이까지 기다리며, pin 쓰기가 시작됐을 수 있는 작업은 반복하지 않습니다. [archive.go:604](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L604)

Snapshot 읽기는 고정 tip을 반환합니다. `from <= base`는 `ErrCompacted`이며 decision gap은 오류입니다. `DecisionsFrom` 자체는 pin 생존 여부를 매번 확인하지 않습니다. 호출자가 갱신 책임을 집니다.

`Renew`는 owner·token·base·tip·tail이 일치하고 아직 만료되지 않은 pin만 CAS로 연장합니다. 갱신 ACK 뒤에는 readback하지 않습니다. `Close`도 같은 identity를 확인한 뒤 현재 시각으로 만료시키며 삭제하거나 readback하지 않습니다. Close 이후 객체를 사용할 수 없도록 로컬 상태를 지우는 동작도 없습니다. [archive.go:709](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L709), [archive.go:744](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L744)

Trim은 다음 순서로 진행합니다.

1. GC lock을 획득하고 active pin이 있으면 `ErrActiveRecoveryPin`을 반환합니다.
2. Publication lock을 획득하고 HEAD를 다시 로드합니다.
3. seal encoding과 decision value, decision slot의 일치를 확인합니다.
4. payload admission·seal/decision 일치 확인 뒤 sealed HEAD는 먼저 `ErrArchiveSealed`로 거부합니다. 그 다음에만 `through <= base`가 성공합니다. 따라서 sealed archive의 이미-covered trim도 `ErrArchiveSealed`입니다. 이후 `through > tip`과 generation 한계를 검사합니다.
5. base·base prefix·checkpoint seal·base decision을 교체하고 HEAD generation을 증가시킵니다. `BaseAnchor`는 지우지만 lineage는 유지합니다.
6. covered extent 참조를 제거하고 경계 extent의 논리적 시작점을 조정합니다. 실제 객체 삭제는 하지 않습니다.

Trim 자체는 quorum 인증과 checkpoint 바이트를 검증하지 않습니다. 좁은 Node 호출자는 먼저 Core checkpoint 검증, seal decision까지 archive sync, certified value 조회를 수행합니다. 단독 `ErrActiveRecoveryPin`·`ErrActiveGCLock`만 trim 연기로 처리하며, 결합된 다른 실패는 전파합니다. [archive.go:432](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L432), [node.go:1147](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L1147)

GC는 음수 grace를 거부하고 GC lock 아래에서 실행합니다.

- 먼저 upload 없이 packing을 계산합니다. extent 수가 줄어들 때만 publication lock 아래에서 재로드·재계산하고 새 체인을 공개합니다.
- Packing은 1024개 및 8 MiB 경계를 따릅니다. 동일 recovery base와 기존 참조 prefix가 유지된 경우 이전 packing을 재사용할 수 있습니다.
- 공개 후 HEAD readback과 별도 Manager의 `Load`로 검증합니다.
- 삭제 단계는 publication lock 밖에서 수행하되 GC lock은 유지합니다.
- 현재 참조 객체, GC snapshot보다 새로운 객체 generation, active pin의 최대 `TailObject` 이하 generation은 보존합니다. Pin별 정확한 reachable set 대신 generation 범위를 보수적으로 보호합니다.
- 첫 발견 시 candidate marker만 생성합니다. 이후 scan에서 marker의 수정시각이 grace cutoff를 지났을 때 객체와 marker를 삭제합니다. grace 0도 첫 발견 즉시 삭제하지 않습니다.
- 대상은 `archive/manifests`, `archive/blocks`입니다. 파싱할 수 없는 block 이름은 건너뜁니다. 해시만 있는 구형 block 이름은 GC 이름 파서에서 generation 0으로 인식합니다.
- 만료 pin은 CAS로 lease 0의 tombstone으로 바꾸며, 충돌은 Busy입니다. 삭제 전에는 해당 Manager의 일반 reader 종료도 기다립니다.

근거: [archive.go:1243](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L1243), [archive.go:1296](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L1296), [archive.go:1421](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L1421), [archive.go:2098](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L2098)

**7. Seal과 두 Fork 경로**

Seal은 CAS 지원 bucket, operation ID, authoritative HEAD를 요구합니다. 내부 Manager의 archive config는 1입니다. `seal.json`을 `IfNotExists`로 선점하며 기존 intent는 같은 operation이어야 합니다. Publication lock 아래에서 HEAD의 sealed bit를 설정합니다. **Seal은 HEAD generation을 증가시키지 않습니다.** 이미 sealed라면 matching intent를 확인합니다. 이 패키지에 unseal API는 없습니다. [seal.go:21](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/seal.go#L21)

Fork는 `SourceBootstrap.Members` 또는 `TargetMembers`가 하나라도 있으면 anchored 경로를 선택합니다. 그 경로는 두 값 모두와 일치하는 target membership을 요구합니다.

| 구분 | Legacy copy fork | Anchored materialization fork |
|---|---|---|
| Intent | v1 | v2 |
| Source authority | `Members`, config 1의 fixed-mode Observer | 독립 제공 `SourceBootstrap` |
| Target 구성 | source extent·checkpoint 복사 | 검증된 source 상태를 target 전용 checkpoint로 재구성 |
| Target HEAD | source HEAD에서 sealed만 해제 | generation 1, `base == tip == source tip`, tail 없음, anchor base·lineage 설정 |
| Result manifest | sealed source HEAD digest | sealed source HEAD digest |

Legacy는 source snapshot이 sealed이고 tip이 0이 아니어야 합니다. Checkpoint가 있으면 root를 열고 검증·pin한 뒤, Core로 base와 suffix를 검증합니다. 그 다음 checkpoint와 참조 extent를 복사하고 unsealed target HEAD를 생성합니다. 복사한 객체는 readback 검증합니다. Source HEAD 불변을 다시 확인하고 checkpoint CURRENT를 승격한 다음 result를 기록합니다. Evidence guard는 archive/root pin을 갱신하며, 마지막 guard 확인은 result 쓰기 **이후**입니다. [fork.go:98](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/fork.go#L98)

Anchored 경로는 sealed source snapshot의 HEAD digest와 bootstrap·membership digest를 v2 intent에 결합합니다. `MaterializeGeneration`은 별도 source snapshot을 만들고 요청한 tip·manifest와 일치하는지 확인합니다. anchored source면 먼저 저장된 anchor hash와 source lineage를 검증하고, Observer Core에 `FencedGenerationBase{ConfigID: sourceBootstrap.ConfigID, Index: sourceAnchor.SourceTip, PrefixHash: sourceAnchor.SourcePrefixHash, RootHash: sourceAnchor.Checkpoint.RootHash, AnchorHash: anchorHash}`를 설치합니다. checkpoint가 있으면 그 **뒤에** regular checkpoint base를 복원하고 suffix를 인증하여 materializer에 적용합니다. 즉 root 복원만으로 Observer authority가 재구성되지는 않습니다. Base-zero source에서는 reconfiguration 검증을 켜며, checkpoint가 있으면 seal의 membership history 존재 여부를 따릅니다. [fork.go:200](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/fork.go#L200), [anchor.go:259](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/anchor.go#L259), [anchor.go:352](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/anchor.go#L352)

Target publication 순서는 다음과 같습니다.

`target checkpoint 생성·CURRENT 승격 → source HEAD 재확인 → anchor 생성·readback 일치 확인 → anchored archive 초기화 → fork result 기록`

기존 target CURRENT가 있으면 materialized tip과 파일 role별 SHA-256을 비교합니다. Anchor까지 기록된 부분 완료 상태는 요청·intent·root를 다시 검증하고 archive 초기화를 재개합니다. `MaterializeGeneration` 단독 호출에는 source sealed 여부를 직접 검사하는 조건이 없으므로, fencing 및 호출 전제는 그대로 필요합니다. [anchor.go:394](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/anchor.go#L394), [anchor.go:509](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/anchor.go#L509)

**8. 재시도·lineage·복구 결과의 의미**

Fork는 intent가 없을 때 target 전체가 비어 있어야 합니다. 기존 intent가 있으면 같은 요청만 계속할 수 있습니다. Intent/result는 조건부 생성하며 충돌 시 기존 내용을 비교합니다.

- Legacy cached result는 target HEAD를 다시 sealed 형태로 encode하여 source manifest와 비교하고, checkpoint CURRENT·인증된 suffix·최종 prefix·HEAD 불변을 재검증합니다. Target이 result tip 이후로 진행했다면 이 exact-result 재검증은 실패할 수 있습니다.
- Anchored cached result는 target HEAD의 anchor hash로 `VerifyGenerationAnchor`를 수행하고 result와 source 정보를 비교합니다.
- `VerifyGenerationAnchor`는 호출자가 제공한 expected anchor hash와 immutable membership을 요구합니다. Anchor, v2 intent, result, archive lineage를 결합하며 store의 anchor만으로 권한을 정하지 않습니다.
- Archive base는 source tip 이상이어야 합니다. Base가 source tip과 같고 아직 anchor base이면 원래 anchor checkpoint도 열어 검증합니다. 더 새로운 checkpoint로 base가 전진한 경우 원래 root의 존재를 요구하지 않습니다.
- 좁은 Node 시작 경로는 regular checkpoint seal의 `GenerationAnchorHash`와 index를 추가로 확인하고, 필요한 경우 fresh Core에 fenced base를 설치합니다.

근거: [fork.go:395](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/fork.go#L395), [anchor.go:154](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/anchor.go#L154), [generation.go:15](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/generation.go#L15)

`RecoverApplicationEvidence`는 expected anchor hash·membership·fork result·source prefix·operation ID 및 callback을 요구합니다. 검증한 anchor의 **원래 checkpoint**를 pin하고 다운로드·복원하여 tip 일치를 확인한 뒤 `Query`와 `Tip`을 제공하는 callback을 호출합니다. 최신 archive suffix를 재생하지 않으므로 반환 tip은 anchor/fork 시점입니다. [application_evidence.go:55](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/application_evidence.go#L55)

Callback 뒤에는 renewal goroutine을 멈추고 parent cancellation·guard 오류를 확인한 다음 최종 pin renewal에 성공해야 evidence를 반환합니다. Callback의 bucket 쓰기·voter 시작 금지는 호출 계약이며 callback을 격리하는 실행 장치는 아닙니다. `Query`는 materializer의 SQL validation과 reader 경로로 위임합니다. [application_evidence.go:172](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/application_evidence.go#L172), [materializer.go:2303](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/materializer/materializer.go#L2303)

**9. 오류·부분 완료 계약과 검토 한계**

이 구현은 여러 객체를 묶는 원자적 transaction이나 전체 rollback을 제공하지 않습니다.

| 관측 결과 | 해석 |
|---|---|
| Extent upload 후 HEAD 실패 | 도달 불가능한 객체가 남을 수 있습니다. |
| HEAD upload 후 readback·lease·release 실패 | 호출이 실패해도 HEAD가 이미 변경됐을 수 있습니다. |
| Seal intent 기록 후 seal 실패 | intent가 남아 다른 operation을 거부할 수 있습니다. |
| Fork 중간 실패 | intent, copied objects, checkpoint CURRENT, anchor, HEAD 또는 result 일부가 남을 수 있습니다. |
| Result 기록 후 guard 실패 | result가 존재해도 이번 호출은 실패할 수 있습니다. |
| GC 실패 | 일부 삭제는 이미 완료됐을 수 있습니다. |
| Snapshot/root 종료 실패 | Fork helper는 5초 제한으로 Close를 시도하지만 오류를 버립니다. 즉시 해제 완료를 반환값으로 증명하지 않습니다. |

따라서 일반 I/O 오류나 context 취소를 “아무것도 쓰이지 않았다”로 해석하면 안 됩니다. `ErrActiveRecoveryPin`과 `ErrActiveGCLock`은 `errors.Is(..., ErrArchiveBusy)`와 호환되지만 원인을 구분합니다. GC lock release의 Busy는 일부 경로에서 억제되는 반면 publication release 실패는 작업 성공 뒤에도 오류가 될 수 있습니다. [archive.go:1875](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L1875), [fork.go:673](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/fork.go#L673)

같은 세션에서 근거를 재검토하며 특히 다음 과장을 배제했습니다: Archive 무결성 검사를 quorum 인증으로 표현하는 것, Seal을 실제 writer fencing으로 표현하는 것, anchor를 consensus 인증서로 표현하는 것, 실패를 rollback 완료로 표현하는 것, application evidence를 최신 tip 복구로 표현하는 것입니다.



## Local backup·복구와 실행 검증

**Local archive identity and publication (Local archive 정체성과 발행)**

- Namespace는 `<prefix>/<cluster>/local/v1`입니다. `BINDING`은 UUID, configuration digest, policy version, bootstrap cluster, 선택적 filesystem location hash를 기록합니다. 이는 identity이지 **분산 소유권 주장**이 아니므로, 원격 writer의 배타 소유권은 외부 요구사항입니다. [local_backup.go:32](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L32), [local.go:21](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L21)
- Genesis는 local attachment intent를 먼저 persist하고 `BINDING`과 `local-gc-control`을 조건부 생성한 뒤, compacted base가 있으면 upload하고 `HEAD`를 발행합니다. partial genesis는 다시 열 때 오류입니다. 이미 binding된 archive가 없어진 경우 silent bootstrap은 허용하지 않습니다. [local_backup.go:456](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L456), [local.go:241](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L241), [local_backup.go:210](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L210)
- Backup 발행 순서는 portable checkpoint 생성 → download/file 검증 → immutable snapshot metadata → archived seal decision → snapshot HEAD 발행입니다. extent는 HEAD advancement 전에 조건부 upload 및 readback을 거칩니다. snapshot 발행은 extent chain을 snapshot index로 rebase하고 그 뒤 suffix를 보존합니다. [local_backup.go:508](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L508), [local_backup.go:533](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L533), [local.go:441](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L441), [local.go:510](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L510)
- Local `SyncThrough`는 각 extent iteration 안에서 HEAD를 발행합니다. 따라서 multi-extent sync는 짧은 prefix를 이미 publish한 뒤 오류를 반환할 수 있고, 다음 작업은 그 published prefix에서 재개합니다. sticky uncertainty는 potentially dispatched HEAD failure에만 적용되며 그 이후의 모든 오류에는 적용되지 않습니다. [local.go:482](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L482), [local.go:495](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L495), [local.go:628](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L628)

**S-01 — immutable publication retry boundary (static derivation; not executed).** Portable checkpoint recreation precedes `StoreLocalSnapshot`; the latter binds `snapshots/<seal.root_hash>.json` to exact metadata bytes. If the initial upload used absent base block keys (generation 0), metadata publication followed by interruption before compaction can leave prepared resumption to encounter those keys and upload generation 1 blocks. Because block generation enters the descriptor hash, the portable root can change even though the Local seal is unchanged. The existing seal-keyed metadata then causes `Local snapshot publication mismatch`; it is not replaced or accepted merely because the seal matches. Persisted blocks and metadata can preserve this conflict across reopen. See [Local creation and retry](checkpoint-go-reverse-spec-2026-10-30.md) and the [format boundary](archive-recovery-go-formats-2026-10-30.md). Sources: [local_backup.go:550](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L550), [local.go:709](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L709), [node.go:1349](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L1349), with the generation/hash derivation cited in the checkpoint contract.

**Startup admission and ancestry (시작 admission과 계보)**

이 표보다 먼저, 원격 maintenance가 unresolved인데 `localOwner == nil`이면 relative tip, policy, recorder, staging과 무관하게 startup을 실패시킵니다. held-maintenance 행의 recovery/replay는 filesystem owner를 held한 경우에만 적용됩니다. [local_backup.go:246](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L246)

| 관측 상태 | 구현된 전이 |
|---|---|
| 원격 binding 없음 | 이전 attachment marker, HEAD, namespace 내용이 모두 없을 때만 첫 attachment를 허용합니다. |
| 원격 binding 존재, local policy 없음 | 남은 local data를 거부하고, 없으면 archive import를 staging합니다. |
| Store UUID/configuration 불일치 | 시작을 실패시킵니다. |
| Recorder tip이 decided tip 초과 | history 비교 전에 원래 recorder를 복구합니다. |
| common-prefix 증명 없음 또는 불일치 | 실패하며 원래 history를 교체하지 않습니다. |
| 원격이 앞섬 | 남은 SQLite evidence를 사본에서 검증한 뒤 import를 staging합니다. |
| tip 동일, maintenance held | filesystem-owned control recovery 전에 staged archive replay를 요구합니다. |
| local이 앞섬, maintenance held | 시작을 실패시킵니다. |
| local이 앞섬, maintenance resolved | snapshot 필요 상태로 표시하고 normal startup이 extension을 sync/checkpoint합니다. |

이 분기는 [local_backup.go:193](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L193)에 구현되어 있으며, 특히 recorder recovery는 299행, prefix 비교는 325행, staging 결정은 344행입니다. 남은 SQLite evidence에는 WAL도 포함됩니다. certified tip을 넘는 materialized slot, 없는 decision, 상충하는 hash는 교체를 막습니다. [local_backup.go:374](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L374)

Archive open은 stable HEAD version, binding hash, content-addressed extent hash, cycle, 연속 endpoint/prefix/configuration, declared origin에서의 종결을 독립적으로 검사합니다. snapshot ancestry도 archived history와 일치해야 합니다. [local.go:95](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L95)

**Restore staging and ownership (복구 staging과 소유권)**

1. Archive 교체 전에 unresolved original materializer restore evidence를 거부합니다.
2. Staging 전에 archive protection을 획득합니다.
3. `.local-backup-import/next`를 만들기 전에 `.local-backup-import-owner.json`을 persist합니다.
4. Staged policy/WAL/checkpoints를 만들고 snapshot base를 복원한 뒤 최대 256 decision 단위로 suffix를 import하고 최종 prefix를 검증합니다.
5. Staged WAL을 sync/close하고 staged materializer pair를 만들며 binding과 `install.json`을 persist한 후 protection을 재확인하고 install합니다.

Owner는 absolute DataDir, cluster, node, Local mode, 전체 archive binding을 묶습니다. foreign/unowned/unexpected pre-install evidence는 오류입니다. owned pre-install state를 폐기하려면 원격 binding을 다시 열어 일치시켜야 합니다. [local_backup.go:650](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L650), [local_backup_staging.go:19](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup_staging.go#L19), [local_backup_staging.go:112](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup_staging.go#L112)

Staging 성공 시 protection은 `n.localBackupRestorePins`로 이전되어 `Node.Open` 실행이 끝날 때까지 유지됩니다. Node `Open`은 protection context cancellation을 Node cancellation에 연결하고, return 때 context cause와 deferred `pins.close()` 오류를 함께 반환합니다. writable Core가 materializer를 install하기 직전에도 `pins.confirm()`을 다시 수행합니다. 그러므로 file move 성공은 protected recovery interval의 종료나 startup 성공을 뜻하지 않습니다. [local_backup.go:650](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L650), [node.go:277](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L277), [node.go:300](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L300), [node.go:575](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L575)

`.local-backup-import-owner.json`은 `version`, `data_dir`(absolute), `cluster`, `node`, `local:true`, `binding`(형식 부록의 `LocalBinding`) JSON입니다. 최대 64 KiB regular file만 읽고 현재 Node configuration에서 재계산한 값과 `reflect.DeepEqual`해야 합니다. `.local-backup-import/install.json`은 항목 배열이며 각 항목은 `name`, `old`, `new`입니다. `name`은 `sqlite.db`, SQLite WAL/SHM, `latticedb`, `local-checkpoints`, `local-store.json`, `local-backup.json`, 및 `qlog/lock.qlog`를 제외한 qlog entry입니다. committed install journal에 owner가 있으면 위 owner 검증도 필수이고, 없으면 legacy retry semantics를 유지합니다. [local_backup_staging.go:19](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup_staging.go#L19), [local_backup.go:798](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L798), [local_backup.go:869](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L869)

Committed installation moves old entries into `previous`, then staged entries into their targets, syncing affected directories. It excludes `qlog/lock.qlog` and retries using existing `previous`/`next` entries. Legacy committed journals may lack the new owner; when an owner exists, it must verify. Installation retains its journal and owner until normal startup verifies/replays installed state and initializes backup successfully. [local_backup.go:804](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L804), [local_backup.go:846](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L846), [local_backup_staging.go:99](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup_staging.go#L99)

`install.json` persist가 durable commitment boundary입니다. final protection confirmation은 그 **뒤**에 있으므로, confirmation 실패와 file move 시작 전에도 committed install work가 남을 수 있습니다. 다음 startup은 ordinary archive tip reconciliation 전에 `finishLocalBackupImport`로 그 journal을 resume하며, 원래 staging/pin-acquisition sequence를 다시 실행하지 않습니다. 이는 remote binding 재검증 뒤 폐기할 수 있는 owned pre-install payload와 다릅니다. [local_backup.go:197](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L197), [local_backup.go:781](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L781), [local_backup.go:846](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L846), [local_backup_staging.go:70](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup_staging.go#L70)

Normal Local restore derives authority from read-only WAL evidence, then rechecks the writable Core’s recovery root/index/prefix before `OpenLocalFromBase`. Replay and backup initialization precede import-journal deletion. [node.go:317](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L317), [node.go:572](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L572), [node.go:838](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L838)

**Pins, maintenance gates, and failure states (pin·maintenance gate·실패 상태)**

- Filesystem restore는 filesystem owner를 held 상태로 두고 archive read-only view로 읽습니다. restore pin을 발행하지 않고 ownership을 검사합니다. 다른 Local restore는 snapshotless tail과 suffix seal이 참조하는 모든 portable checkpoint dependency를 포함한 captured HEAD를 pin합니다. [local_backup.go:136](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L136), [local_gc.go:133](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_gc.go#L133), [recovery/local_gc.go:277](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local_gc.go#L277)
- Local restore lease는 2분이고 renewal은 20초 budget으로 20초마다 실행합니다. Admission은 gate release 내부와 **이후**에 남은 lease margin을 검증합니다. renewal 실패는 restore를 취소합니다. 만료된 view pin은 다른 token의 replacement를 renew/close할 수 없습니다. Close는 maintenance gate 아래에서 component pin과 view를 retire합니다. [local_gc.go:162](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_gc.go#L162), [recovery/local_gc.go:375](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local_gc.go#L375), [recovery/local_gc.go:421](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local_gc.go#L421), [recovery/local_gc.go:525](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local_gc.go#L525)
- Maintenance 전이는 free → conditional token acquisition → exact stable readback → work → conditional free state입니다. 불확실한 mutation은 gate를 held 상태로 남깁니다. lost release ACK는 stable readback이 정확한 free bytes와 변경된 non-nil version을 보일 때만 성공합니다. held-token timeout takeover는 자동으로 일어나지 않습니다. [checkpoint/local_gc.go:72](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/local_gc.go#L72)
- dispatch되었을 수 있는 write 뒤 HEAD publication이 오류면 archive handle은 sticky-failed가 됩니다. 즉 “publication uncertain; reopen required”입니다. 알려진 pre-dispatch 및 conditional-write 거절은 이 poisoning을 피합니다. 외부에서 advanced된 HEAD도 reopen이 필요합니다. [local.go:628](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L628), [recovery/local_gc.go:242](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local_gc.go#L242)
- GC는 current/latest configured snapshot과 suffix checkpoint dependency를 보존합니다. active recovery/view protection은 extent deletion을 막고 catalog pruning은 acknowledged deletion 뒤에 실행됩니다. 기본 retention은 3입니다. [recovery/local_gc.go:30](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local_gc.go#L30)
- Local extent GC는 HA의 two-scan marker와 다릅니다. live chain에서 닿지 않는 extent를 직접 후보로 고르고, `LastModified`가 0이 아니며 grace cutoff보다 **엄격히 이전**일 때만 삭제합니다. malformed/noncanonical extent name은 scan을 실패시키며 active protection은 deletion을 억제합니다. [recovery/local_gc.go:170](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local_gc.go#L170), [recovery/local_gc.go:542](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local_gc.go#L542)

별도 startup guard는 비어 있어도 CAS-capable shared archive를 snapshot하고 archive/root protection을 renew하며 `ErrRecoveryAdmissionBusy`만 재시도하고 다른 pin 오류는 즉시 실패합니다. close는 pin-close 오류를 버립니다. non-CAS archive는 이 shared snapshot fence를 건너뜁니다. [startup_recovery.go:33](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/startup_recovery.go#L33), [startup_recovery.go:71](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/startup_recovery.go#L71), [startup_recovery.go:153](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/startup_recovery.go#L153)

**Mutation ACK behavior (mutation ACK 동작)**

Before-ACK mode는 `EnsureDurable(slot)`을 호출하고 현재 Core tip까지 Local backup을 synchronize합니다. Proposal execution은 이 barrier 전에 decision을 적용합니다. cached receipt도 durability barrier를 통과하며 실패한 Local receipt lifecycle은 sticky failure를 기록하고 readiness/admission을 닫습니다. caller timeout은 server-owned receipt barrier가 계속되는 동안 unknown outcome을 반환할 수 있습니다. Async mode는 이 ACK barrier를 설치하지 않습니다. [node.go:724](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L724), [server.go:914](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L914), [server.go:470](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L470), [server.go:1018](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L1018)

**공개 API 표면 (HA·Local·fork)**

| 영역 | Go 서명 | 계약상 핵심 |
|---|---|---|
| HA | `func Seal(ctx context.Context, bucket objstore.Bucket, prefix, operationID string) error` | 호출자가 기존 writer를 fence한 뒤 seal intent를 기록합니다. |
| HA/fork | `func Fork(ctx context.Context, bucket objstore.Bucket, options ForkOptions) (ForkResult, error)` | 검증·pin된 source generation을 isolated target으로 복사합니다. target은 성공 반환 전 시작하면 안 됩니다. |
| Local | `func OpenLocalArchive(ctx context.Context, bucket objstore.Bucket, prefix string) (*LocalArchive, error)` | remote Local archive를 열고 binding/HEAD/extent 계보를 검증합니다. |
| Local filesystem | `func OpenLocalArchiveOwned(ctx context.Context, bucket objstore.Bucket, prefix string, owner *internalobjstore.LocalFilesystemOwner) (*LocalArchive, error)` | held process-lifetime filesystem owner가 없으면 거부합니다. |
| Local genesis | `func CreateLocalArchive(ctx context.Context, bucket objstore.Bucket, prefix string, binding LocalBinding, base *LocalSnapshot) (*LocalArchive, error)` | conditional identity/control 생성 후 검증된 base와 HEAD를 발행합니다. |
| Local snapshot | `func StoreLocalSnapshot(ctx context.Context, bucket objstore.Bucket, prefix string, snapshot LocalSnapshot) error` / `func (a *LocalArchive) PublishSnapshot(ctx context.Context, snapshot LocalSnapshot) error` | immutable snapshot metadata를 먼저 기록한 뒤 HEAD snapshot을 발행합니다. |
| Local restore pin | `func (a *LocalArchive) PinRestore(ctx context.Context, snapshot LocalSnapshot, owner string, lease time.Duration) (*LocalRestorePin, error)` / `func (a *LocalArchive) PinView(ctx context.Context, owner string, lease time.Duration) (*LocalRestorePin, error)` | captured HEAD와 suffix checkpoint dependencies를 보호합니다. |

근거: [seal.go:21](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/seal.go#L21), [fork.go:60](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/fork.go#L60), [local.go:76](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L76), [local.go:201](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L201), [local.go:502](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L502), [local_gc.go:268](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local_gc.go#L268)

**Verification**

All commands used explicit workdir `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza`. Opt-in inspection found the recovery Versity test gated by `RHIZA_VERSITYGW_BIN`; it was disabled.

```sh
env RHIZA_VERSITYGW_BIN= go test -mod=readonly -count=1 -timeout=180s ./pkg/recovery
```

Output: `ok github.com/mrchypark/rhiza/pkg/recovery 41.081s`

```sh
env RHIZA_VERSITYGW_BIN= go test -mod=readonly -count=1 -timeout=180s ./pkg/node -run '^(TestLocalBackupStagingPreinstallCrashPhases|TestLocalBackupStagingForeignEvidenceUnchanged|TestLocalBackupStagingCommittedRetryAndOpen|TestStartupRecoveryGuardReloadsFirstArchiveHead|TestStartupRecoveryGuardSkipsSingleNodeArchiveWithoutCAS|TestStartupRecoveryPinRefusesForeignLivePublisher|TestStartupRecoveryAdmissionCancellation|TestStartupRecoveryAdmissionDoesNotRetryPinFailures)$'
```

Output: `ok github.com/mrchypark/rhiza/pkg/node 5.204s`

```sh
env RHIZA_VERSITYGW_BIN= go test -mod=readonly -count=1 -timeout=180s ./pkg/node -run '^TestLocalBackup(WholeLossCheckpointTailAndRestart|HeldMaintenanceEqualTipReplaysBeforeRelease|DivergencePreservesWAL|MissingBoundRemoteFails|ReconcilesRemoteAheadAndLocalExtension|FirstAttachCompactedStoreAndRestore|RemoteAheadPreservesRestoreIntent|RemoteAheadPreservesPartialMaterializedEvidence|RestartRecoversOriginalPendingRecorder|RestartPublishesBeforeResumedCompaction)$'
```

Output: `ok github.com/mrchypark/rhiza/pkg/node 14.415s`

All three processes exited successfully. No servers, emulators, containers, or manually created temporary resources were started. Test fixtures used test-owned temporary directories; no cleanup failures were reported. One exploratory `rg` named nonexistent `pkg/node/local_restore_pins.go`; subsequent inspection located the implementation in `pkg/node/local_gc.go`.



## 검증 한계

S3/GCS/Azure 실제 SDK 계약·실제 클라우드·emulator parity·성능·Chaos qualification은 이번에 검증하지 않았다. 위 테스트 실행은 별도 검증 에이전트가 수행했다. 코드 근거와 테스트 정의가 모든 장애 스케줄의 보장을 뜻하지 않는다. 두 독립 Goldfish의 지적을 수정했고 최종 재점검에서 해당 항목의 해소를 확인했다. 원문·수정 이력·검토 범위는 [EGM 검토 기록](archive-recovery-go-egm-review-2026-10-30.md)에 남겼다. 전체 명세의 무결점 판정이나 독립 재구현 인증은 아니다.
