# Archive·복구 형식 부록

기준 HEAD: `008f8e576ad76425182e10a69b987f10cfb48b2a`. 다음은 HA와 Local의 키·바이트·해시 계약이다. Local BINDING/HEAD/snapshot/pin JSON 세부 필드는 4절을 따른다. staging/install journal과 상위 호출 규칙은 [본문](archive-recovery-go-reverse-spec-2026-10-30.md)을 따른다. 외부 제공자 wire 형식을 대신하지 않는다.

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

**4. Local archive JSON·키·해시 계약**

Local namespace는 `<prefix>/<cluster>/local/v1`이며, 아래는 namespace 아래 상대 키입니다. JSON은 unknown field와 trailing JSON을 거부합니다. `[32]byte`는 Go JSON에서 정수 배열이고, hex로 명시된 필드만 hex 문자열입니다. Local extent는 HA와 같은 binary extent encoding을 쓰지만 key에 generation을 넣지 않습니다.

| 키 | 값과 제약 |
|---|---|
| `BINDING` | `version`, `archive_location_hash`, `store_uuid`, `config_digest`, `policy_version`, `bootstrap`의 immutable JSON. `version`은 **2**, `policy_version`은 `1`; `store_uuid`는 정확히 16 byte hex, `config_digest`는 정확히 32 byte hex, 비어 있지 않은 `archive_location_hash`는 canonical lower-case 32 byte hex입니다. `bootstrap.config_id != 0`, member는 정확히 하나이며 그 ID는 비어 있지 않아야 합니다. |
| `HEAD` | `version`, `binding_hash`, `tip`, `prefix`, `tail`, `origin?`, `snapshot?`, `catalog?`; version은 **2**입니다. `binding_hash = SHA256(BINDING 원본 bytes)`이고 archive handle의 raw `head_hash = SHA256(HEAD 원본 bytes)`입니다. 최대 4 MiB이며 stable attributes read와 conditional overwrite/readback을 사용합니다. |
| `extents/<hex(SHA256(bytes))>` | HA binary extent bytes. Local chain에서 `previous_object`는 previous hash가 없으면 `0`, 있으면 반드시 `1`입니다. `tail`은 마지막 extent 전체 bytes의 SHA-256이고 key hex와 같습니다. |
| `snapshots/<hex(seal.root_hash)>.json` | 아래 중첩 schema의 `seal`, `decision`, `portable_root`, `descriptor`를 가진 `LocalSnapshot` JSON입니다. seal canonical encoding은 `decision.value`와 같고 `decision.hash = SHA256(decision.value)`입니다. `decision.slot > seal.index`, certificate/descriptor는 비어 있으면 안 되며 descriptor는 최대 16 KiB, portable root는 0이 아니고 seal config ID는 binding bootstrap과 같아야 합니다. if-not-exists 후 정확한 bytes readback을 요구합니다. |
| `view-pins/<hex(SHA256(owner))>.json` | `version`, `binding_hash`, `head_hash`, `token`, `lease_until_unix_ms`. version은 `1`, token은 정확히 16 byte hex, head hash는 0이 아니고 lease는 0 이상입니다. pin의 `head_hash = SHA256(json.Marshal(a.head))`이며 raw HEAD bytes hash가 아닙니다. read validation의 `binding_hash`는 `SHA256(json.Marshal(a.Binding))`와 비교합니다. 생성은 `a.head.BindingHash`를 기록하므로 canonical writer output에서는 raw BINDING hash와 같지만, 허용된 noncanonical JSON에서는 다를 수 있습니다. 갱신·close는 같은 token과 object version에 conditional write합니다. |
| `local-gc-control` | `version`, `token`, `location_hash`, `binding_hash` maintenance JSON입니다. BINDING 뒤 조건부 생성되고 filesystem Local은 owner location hash와 binding hash를 재확인합니다. |

`bootstrap`은 `Cluster{config_id, members}`이고 각 member는 `Member{node_id, url, peer_url?, log_url, public_key, wal_identity?}`입니다. `peer_url`과 `wal_identity`만 `omitempty`이고 `public_key`는 canonical standard base64 JSON string입니다. `seal`은 `CheckpointSeal{config_id, index, root_hash, state_hash, prefix_hash, next_leader_order, following_leader_order?, membership?, generation_anchor_hash?}`이며, `following_leader_order`와 `membership`은 `omitempty`, `generation_anchor_hash`는 `omitzero`입니다. `decision`은 `DecidedValue{slot, hash, value, certificate?}`이며 `certificate`만 `omitempty`; `value []byte`는 base64 JSON string, `certificate json.RawMessage`와 `descriptor json.RawMessage`는 embedded JSON입니다. `origin`, `snapshot`, `catalog` 안의 snapshot은 이 `LocalSnapshot` schema를 그대로 사용합니다. Open은 HEAD binding hash와 extent hash, cycle, endpoint/prefix/configuration, declared origin까지의 종결, snapshot ancestry를 함께 검증합니다. prefix 계산과 extent binary header/CRC 규칙은 위 HA extent 계약과 같습니다. [types.go:49](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L49), [types.go:72](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L72), [types.go:82](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L82), [types.go:154](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L154), [local.go:34](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L34), [local.go:95](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L95), [local.go:676](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L676), [local_gc.go:481](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local_gc.go#L481)

**S-01 — identity boundary.** The snapshot key uses the Local seal root, while `portable_root` identifies the separately generated portable checkpoint. Exact readback rejects different bytes under the same seal key with `Local snapshot publication mismatch`; equal seal identity does not imply equal portable identity. Block-generation fallback on recreation can change the portable root after metadata publication and before compaction, including on prepared resumption after reopen. This conditional failure is statically derived, not executed; see the [publication scenario and sources](archive-recovery-go-reverse-spec-2026-10-30.md) and [generation/hash rules](checkpoint-go-reverse-spec-2026-10-30.md). The format provides no replacement of the original metadata on such a mismatch. Source: [local.go:709](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L709).

Local object bounds는 `BINDING` 64 KiB, `HEAD`와 snapshot JSON 각 4 MiB, snapshot descriptor 16 KiB, extent 8 MiB입니다. Local HEAD는 JSON이므로 HA의 128-byte binary HEAD header/CRC 계약을 적용하지 않습니다. [local.go:41](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L41), [local.go:709](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L709), [local.go:744](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L744)
