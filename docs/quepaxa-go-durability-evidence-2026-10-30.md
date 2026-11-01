편집 주: 아래의 실행·무수정 진술은 최초 근거 수집 당시의 기록입니다. 이번 문서 보정은 허용된 문서만 수정하고 테스트를 실행하지 않았습니다.

분석 대상은 `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza`이며, 시작과 종료 시 HEAD가 `8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd`임을 확인했습니다. `go.mod`와 `pkg/quepaxa`도 확인했습니다. 지정한 Go 체크아웃만 분석했으며 파일 수정, 테스트 실행, 위임, 커밋, 공유 메모리 접근은 하지 않았습니다.

아래 내용은 **현재 구현에서 역도출한 durability/recovery/storage 계약**입니다. 코드 근거와 테스트에 정의된 검증을 구분하며, 논문과의 동등성이나 과거 설계 의도는 주장하지 않습니다.

**1. 영속성 상태와 ACK 경계**

Core는 값의 보유, recorder 상태, 결정의 WAL 기록, 결정의 Sync 완료를 따로 관리합니다. 따라서 “결정이 메모리에 있다”와 “로컬 WAL에서 결정 자체를 재생할 수 있다”는 서로 다른 상태입니다.

| 상태 | 코드에서 의미하는 조건 | 보장 범위 |
|---|---|---|
| staged value | `values[hash]`에 값이 있음 | Append 이후일 수 있으나 Sync 완료를 뜻하지 않음 |
| durable value | `valueDurable[hash] == true` | `Value()`가 값을 반환할 수 있음 |
| recorder state | `recorders[slot]`에 ISR이 있음 | receipt Append 직후에도 존재하므로 이것만으로 Sync 완료를 뜻하지 않음 |
| learned decision | `decided[slot]`에 결정이 있음 | 인증된 결정의 메모리 설치 상태 |
| logged decision | `logged[slot] == true` | QDEC Append 완료 상태 |
| durable decision | `durable[slot] == true` | 해당 결정에 필요한 Sync 완료 상태 |
| base-covered slot | `slot <= floor` | 개별 결정 marker 대신 설치된 recovery base로 덮임 |

근거는 [core.go:462](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L462)의 staging 경로, 같은 파일 496–520행의 durable value 공개 조건, 1355–1368행의 receipt 기록 순서, 2016–2076행의 결정 영속화 경로입니다.

Recorder의 정상 응답은 receipt Append와 group Sync 성공 뒤에 반환됩니다. 반면 Sync가 실패하면 이미 Append한 receipt와 갱신한 ISR이 남을 수 있습니다. 따라서 실패 반환은 “아무것도 기록되지 않았다”는 증거가 아닙니다.

API별 계약은 다음과 같습니다.

| 진입점 | 성공 반환 전 수행하는 작업 | 주의할 경계 |
|---|---|---|
| `StoreValue` | 값 staging 후 Sync, 이후 durable 표시 | 값의 영속성만 다루며 결정 marker를 만들지는 않음 |
| `Record` | receipt Append → ISR 갱신 → Sync | Local 모드에서는 외부 호출 거부 |
| `AcceptDecision` | 결정 설치 후 QDEC 영속화 | 일반 hint 경로보다 강한 로컬 영속성 |
| `AcceptDecisionHint` | 인증된 결정을 설치 | 일반 결정은 추가 디스크 barrier 없음. 재구성 control은 durable 경로로 전환 |
| `EnsureDurable(slot)` | 필요한 QDEC Append와 Sync | 일반적으로 한 slot을 다룸. terminal control은 retained prefix까지 영속화 |
| `EnsureDurableThrough` | retained prefix의 누락 QDEC를 기록하고 한 Sync로 승격 | 중간 결정이 없으면 오류 |
| `AcceptCertifiedValueForAck` | 조건을 충족하면 recorder 영속성을 재사용 | 다중 voter 구성, certificate에 자기 ID 포함, 일치하는 live ISR, durable value가 모두 필요 |

근거: [core.go:1900](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1900), 1900–2076행 및 [core.go:2714](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2714), 2714–2743행.

`ProposeCertified`는 durable recorder quorum 이후 반환하는 API이며, 주석은 client ACK 전에 반환된 결정을 다른 voter에 설치하도록 호출자에게 요구합니다. 일반 `Propose`는 completion 경로를 추가로 수행합니다. 다만 clustered 경로는 recorder quorum의 영속성을 재사용하므로 **성공한 모든 제안에 즉시 로컬 QDEC Sync가 존재한다는 계약으로 해석하면 안 됩니다.** 근거: [core.go:302](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L302), 302–306행과 415–435행.

**2. Group commit의 기본값과 취소 의미**

기본 대기 시간은 **25µs**, 조기 wake 기준은 **64 waiters**입니다. 이것은 최대 ACK 지연이나 배치 크기 상한이 아닙니다. 실제 Sync 시간과 스케줄링 시간이 더해지며, flush가 pending batch를 분리하기 전까지 추가 waiter가 들어올 수 있습니다.

`Sync(ctx)`는 먼저 pending batch에 참가한 다음 완료 채널과 context 취소를 기다립니다. 취소된 waiter를 batch에서 제거하거나 이미 시작된 Sync를 취소하는 코드는 없습니다. 따라서 취소 오류가 반환된 뒤에도 해당 batch의 디스크 작업은 진행될 수 있습니다. 완료와 취소가 동시에 준비되면 두 select 분기 중 어느 쪽이 선택될지도 고정되지 않습니다.

Flush는 pending을 분리한 뒤 실제 Sync를 호출합니다. Sync 도중 들어온 호출은 다음 batch에 배정됩니다. 한 batch의 Sync 오류는 그 batch의 모든 완료 대기자에게 전달되며, 이후 batch의 진행을 영구 차단하는 상태는 이 계층에 없습니다. 근거: [group_commit.go:9](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/group_commit.go#L9), 9–11행, 34–81행.

Local recorder quorum은 더 강한 취소 경계를 둡니다. `context.WithoutCancel(ctx)`로 자기 receipt 작업을 끝낸 뒤 원래 context의 취소를 확인합니다. 즉, **취소 오류를 반환하더라도 Local receipt의 Sync는 이미 완료될 수 있습니다.** 근거: [core.go:1163](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1163), 1163–1180행.

**3. WAL payload 형식과 재생 시 거부 조건**

여기서는 QuePaxa payload와 그 소비 경로를 확인했습니다. 하위 qlog의 물리 파일 형식, segment 기본값, checksum·잘린 tail 처리 전체는 분석하지 않았습니다.

| 레코드 | 현재 payload 계약 | 주요 검사 |
|---|---|---|
| Proposal | 원시 value bytes | 복구 시 비어 있지 않아야 하며 SHA-256이 entry hash와 일치 |
| Receipt | `51 49 53 52 00` + FlatBuffers | 내부 identifier `QISR`, slot 비영(非零), proposal 필드 길이 검사 |
| 재구성 Receipt | `51 49 53 52 32 00` + 같은 FlatBuffers | 재구성 비활성 Core에서는 복구 거부 |
| 재구성 capability marker | 정확히 `51 49 53 52 32 00`만 존재 | slot 0의 marker로 처리 |
| Decide | `51 44 45 43 00` + JSON | value와 raw JSON certificate를 포함 |
| Checkpoint | JSON `consensusBase` | floor, prefix, root, leader order, membership, lineage 등 |
| CheckpointVerified | encoded checkpoint seal | entry slot/hash와 seal index/root의 일치 검사 |

Magic 값은 [core.go:32](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L32), 스키마는 [wal.fbs:3](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/wal.fbs#L3), base 필드는 [compaction.go:214](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/compaction.go#L214)에 정의되어 있습니다.

ISR 스키마의 `slot`과 `step`은 `ulong`이며 별도 기본값 선언이 없습니다. proposal 참조는 nil로 디코딩될 수 있습니다. 인코더는 세 proposal이 같으면 FlatBuffers object를 공유합니다. 디코더는 짧은 buffer와 잘못된 identifier를 거부하고, FlatBuffers 접근 중 panic을 오류로 바꿉니다. priority/hash 길이를 검사하며, **비어 있지 않은 inline value에 대해서만** hash를 대조합니다. Codec 자체가 step의 프로토콜 유효성이나 ISR 전체 불변식을 검증하는 것은 아닙니다. 근거: [wal_codec.go:12](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/wal_codec.go#L12), 12–41행, 44–110행.

QDEC는 value를 JSON `[]byte`로 직렬화하므로 base64 비용이 발생하고, certificate는 `json.RawMessage`로 삽입됩니다. Strict JSON decoder는 알 수 없는 필드와 뒤따르는 JSON 데이터를 거부합니다. 다만 이것을 중복 key까지 거부하는 canonical JSON 계약으로 확대할 근거는 없습니다. 근거: [certificate.go:32](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/certificate.go#L32), 32–35행, 93–117행.

**4. Replay와 corruption 처리**

`New`는 `recover()` 실패 시 Core 생성을 실패시킵니다. 복구는 다음 단계로 진행됩니다.

1. WAL scan에서 base, prepared marker, proposal, ISR, QDEC를 읽습니다.
2. 결정들을 slot 순서로 정렬하고 검증합니다.
3. 연속된 prefix를 구성합니다.
4. prepared seal의 prefix identity를 확인하고 prepare 상태를 재구성합니다.

근거: [config.go:152](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/config.go#L152), [core.go:3162](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L3162), 3162–3334행.

| 복구 입력 | 처리 |
|---|---|
| 잘못된 base JSON, config/slot/root/epoch/order 불일치 | 오류 |
| 잘못된 prepared marker identity | 오류 |
| 비어 있거나 hash가 맞지 않는 Proposal | 오류 |
| 지원하지 않는 ISR payload | 오류 |
| 재구성 WAL을 재구성 비활성 Core로 열기 | 오류 |
| QDEC certificate/value identity 불일치 | 오류 |
| 같은 slot에 서로 다른 결정 hash | 오류 |
| 인증된 sparse decision | 보존하되 빈 slot을 건너 tip을 전진시키지 않음 |
| 같은 index의 서로 다른 prepared root | 오류 |

QDEC는 value를 inline으로 복원하고 logged/durable 표시를 설정하며 같은 slot의 recorder를 제거합니다. ISR은 이미 결정이 존재하는 slot에는 설치하지 않습니다. 재구성 모드에서는 정렬된 결정을 검증하고 연속된 부분에만 구성 변경을 적용합니다.

검사 범위에도 경계가 있습니다. Receipt replay는 decoded payload의 slot을 사용하며 이 분기에서 outer entry slot/hash와의 대조를 수행하지 않습니다. QDEC replay도 certificate의 slot/hash를 사용하며 outer entry metadata를 직접 대조하는 조건은 보이지 않습니다. 또한 switch에 열거되지 않은 entry type은 이 계층에서 오류 없이 통과합니다. 이는 **검사 위치에 관한 코드 사실**이며, 하위 qlog를 포함한 전체 손상 방어의 취약성 판정은 아닙니다.

**5. Checkpoint와 compaction 계약**

Checkpoint prepare는 단순한 “root 등록”이 아닙니다. 먼저 seal의 config, prefix, leader orders, lineage, membership를 로컬 상태와 대조합니다. 최신 prepared index보다 낮은 checkpoint는 거부하고, 같은 index에 다른 root도 거부합니다. 이미 같은 root를 준비한 경우에는 검증된 상태로 반환할 수 있습니다. 근거: [core.go:1671](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1671), 1671–1739행.

새 prepare의 기록 순서는 `validator → 미영속 prefix QDEC Append → CheckpointVerified Append → Sync → prepared 상태 설치`입니다. 성공 시 메모리의 prepared map은 새 root 하나로 교체되지만, 이전 marker가 WAL에서 즉시 삭제되는 것은 아닙니다. 근거: [core.go:1771](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1771), 1771–1833행.

`CompactThrough(T,R)`의 주요 전제는 다음과 같습니다.

- `T`와 `R`이 비영이어야 하고 `floor < T <= tip`이어야 합니다.
- `R`은 index `T`의 sealed root여야 합니다.
- `preparedCheckpoints[T]`도 정확히 `R`이어야 합니다.
- prefix와 leader orders가 있어야 합니다.
- 재구성 모드에서는 active reconfiguration이 없어야 하고 seal의 membership history가 현재 이력과 일치해야 합니다.

근거: [compaction.go:238](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/compaction.go#L238), 238–305행.

압축은 barrier 아래에서 시작하고, Build 동안 barrier를 놓은 다음, Commit 직전에 다시 barrier를 획득합니다. 이때 floor와 membership version, retained suffix를 다시 검증합니다. Suffix 검증은 별도 Core에 base와 결정을 재구성해 tip/prefix 및 membership가 유지되는지 확인합니다. Local 모드에서는 정확한 rewrite 예상 크기가 WAL limit 이내인지도 검사합니다. 근거: 같은 파일 306–354행, 404–458행.

값 보존은 hash 단위입니다. floor 위의 logged decision은 value를 inline으로 보유하므로 같은 hash의 raw Proposal을 중복 보존하지 않습니다. 반면 unlogged decision과 unresolved ISR이 필요로 하는 값은 보존합니다. 필요한 값이 비었거나 hash가 맞지 않으면 압축을 거부합니다. Build 중 재사용되는 기존 값도 `compactionValues`를 통해 새 WAL에 필요한 Proposal 기록을 추가할 수 있습니다. 근거: [compaction.go:476](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/compaction.go#L476), 476–534행 및 core.go 462–483행.

Base 설치 후에는 `<= floor` 결정·recorder를 제거하고, `< floor` prepared/sealed root를 제거하며, 남은 결정과 ISR에서 사용하지 않는 value를 삭제합니다. vacant slot도 floor 위만 남깁니다. **Checkpoint state index와 seal decision slot은 별개**이므로 seal 결정은 suffix에 남을 수 있습니다. 근거: compaction.go 609–675행, 741–755행.

**S-02 — failed Commit can leave a partial transition (static derivation; not executed).** If QLog publishes the compacted manifest by rename and then directory sync fails, it advances generation, adopts the new segments, marks compaction done, and returns an error with fatal durability uncertainty. `compactThroughOwned` returns immediately, before `installVerifiedBaseLocked` and allocator pruning. Thus QLog can select the compacted generation while the live Core still exposes its previous floor and retained state. The deferred `Abort` cannot reverse the completed QLog transition. A failed call does not establish selection of the old WAL: manifest selection, durability confirmation, and Core base installation are distinct. Reopen uses QLog's manifest selection and validation rules for files actually present; persistence of the renamed manifest across a crash is not confirmed by this failure. See [QLog uncertainty, reopen, and compaction rules](qlog-go-reverse-spec-2026-10-30.md). Sources: [wal.go:581](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L581), [wal.go:1232](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L1232), [compaction.go:349](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/compaction.go#L349).

**6. Local reclamation, root 목록, archive**

Local 모드는 명시적 옵션입니다. 자기 자신인 단일 voter, transport 없음, 재구성 비활성이라는 조건이 필요합니다. WAL은 호출자 소유지만 Core가 활성인 동안 외부 Append·Compact·limit 변경·Bind·공유를 금지하는 계약이 있습니다. 양의 유한 WAL limit도 필요합니다. 근거: [config.go:19](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/config.go#L19), 19–28행, 64–80행.

Local execution owner는 용량 1인 channel로 구현됩니다. Reclamation은 owner를 끝까지 유지하므로 apply 콜백은 owner를 다시 획득하는 Core 작업으로 재진입하면 안 됩니다. 콜백에 전달되는 slot은 checkpoint index가 아니라 **seal 결정을 포함해 apply할 slot**입니다. 근거: core.go 2444–2456행 및 [local_reclamation.go:149](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/local_reclamation.go#L149), 149–171행.

| Reclamation 단계 | 동작 | 이후 실패의 의미 |
|---|---|---|
| 초기 검증 | exact root validator와 seal identity 확인 | 아직 protocol 기록 없음 |
| 사전 용량 검사 | history, unknown recovery, checkpoint reserve 계산 | 전용 admission refusal은 쓰기 전 거부 |
| 선택적 물리 공간 검사 | rewrite bytes와 remaining allowance 전달 | 실제 공간 판단은 콜백 책임 |
| Prepare | prefix와 prepared marker 영속화 | prepared 상태가 남을 수 있음 |
| Seal 확보 | 새 제안 또는 이미 결정된 동일 seal 재사용 | 새 slot을 항상 할당하지 않음 |
| Durable barrier | seal slot까지 prefix 영속화 | apply 이전 영속 경계 |
| Apply·재검증 | apply 후 context와 root 재확인 | 실패 시 자동 롤백 없음 |
| Compact | seal의 **index**까지 압축 | 성공 후 recovery floor 설치 |

근거: local_reclamation.go 175–275행. 기존 seal 재사용 분기는 199–205행과 245–258행에 있습니다. Apply가 여러 번 호출되어도 안전한지는 호출자 구현에 달려 있으며 Core 자체의 exactly-once 계약은 없습니다.

`LocalCheckpointRoots`는 live base/sealed state, 결정 value, ISR이 참조하는 value와 WAL의 base/prepared/QDEC를 수집합니다. tuple은 `(RootHash, Index, PrefixHash, StateHash, ConfigID)`이며 base에서는 StateHash가 없을 수 있습니다. 충돌·decode·scan 오류가 나면 부분 결과 없이 실패하고 root hash 순으로 정렬합니다. WAL scan 자체는 `c.mu`를 놓고 실행합니다. 근거: [local_reclamation.go:13](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/local_reclamation.go#L13), 13–146행. 이 API는 root 보존 근거를 제공하며 실제 snapshot 파일을 삭제하지 않습니다.

Archive import는 전체 페이지를 임시 validation view에서 검증한 뒤 쓰기를 시작합니다. 페이지는 `tip+1`부터 연속되어야 하며, pending recorder나 unlogged decision이 있는 미정착 prefix에는 import할 수 없습니다. 전체 QDEC 크기와 checkpoint reserve를 승인받고 Append 전부와 직접 `wal.Sync()`를 마친 뒤 live Core에 설치합니다. 원본 certificate bytes를 보존합니다. Append·Sync·설치 실패는 재개방을 요구하는 불확실한 결과입니다. 근거: [local_archive.go:14](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/local_archive.go#L14), 14–113행.

Archive base restore는 fresh Core와 빈 staging WAL을 요구합니다. 정확한 base 하나만 이미 설치된 경우에는 rewrite 없이 재시도를 허용합니다. Membership가 있거나 seal decision slot이 index보다 크지 않으면 거부합니다. 근거: 같은 파일 118–164행.

**7. Local 비용 모델과 admission**

이 Local 비용 모델이 사용하는 제안·staging value 상한은 **128KiB**, qlog entry header 가정은 **53 bytes**입니다. 이 상한은 모든 Decision 수신 경로의 보편적인 크기 검사가 아닙니다. 비용 모델 생성 시 실제 빈 entry 인코딩 길이가 53인지 검사하므로 header가 바뀌면 조용히 계산을 계속하지 않습니다. Node ID는 유효한 UTF-8이어야 하며, JSON escaping까지 반영해 schedule과 checkpoint가 value 한도에 들어가는지 확인합니다.

`V=128KiB`, `R=receipt bound`, `C=certificate bound`라고 하면:

| 비용 | 현재 계산 |
|---|---|
| Proposal `P(v)` | `53+v` |
| Decision `D(v)` | `85+4×ceil(v/3)+C`, `v=0`이면 2 추가 |
| Slot `B(v)` | `P(v)+3R+D(v)` |
| Foreground | `B(v)+B(V)` |
| Recovery | `B(V)` |
| Checkpoint | `P(V)+2B(V)` |

근거: [local_cost.go:72](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/local_cost.go#L72), 72–180행, 268–348행. Receipt bound는 지원하는 step 4–6의 합성 ISR을 실제 codec으로 인코딩해 구하며 certificate bound도 최대 숫자 폭을 반영한 합성 certificate로 계산합니다.

이 수치는 공간 예약이나 I/O를 수행하지 않는 추정치입니다. 특히 `RecoveryBytes`는 **한 slot 상한**이며 임의 길이 pending suffix 상한이 아닙니다. 전체 복구 계획은 알려진 unlogged decision의 실제 QDEC 크기와 미결정 hole 수에 `B(V)`를 곱한 값을 합산합니다. Pending ISR도 쓰기 전에 검사합니다. 근거: local_cost.go 41–46행, 419–493행.

Admission 조건은 `used + immediate + protected <= limit`입니다. 정확히 맞는 경우 허용하며 overflow나 초과는 전용 `localAdmissionDenial`을 반환합니다. Fatal WAL 상태와 잘못된 limit는 별도 오류입니다. 일반 capacity 오류나 중첩 콜백 오류를 “쓰기 전 무변경 거부”로 분류해서는 안 됩니다. 근거: [local_cost.go:18](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/local_cost.go#L18), 18–38행, 496–512행.

길이 0의 비용 계산은 nil과 빈 slice의 JSON 차이를 덮지만, 실제 proposal staging은 빈 값을 거부합니다. 따라서 비용 검증 성공은 전체 프로토콜 입력 검증 성공을 뜻하지 않습니다.

**8. 테스트 정의와 미입증 보장**

아래는 **테스트 코드가 정의하는 검사**이며 이번 분석에서 통과를 확인한 결과가 아닙니다.

| 테스트 근거 | 정의된 검증 |
|---|---|
| [group_commit_test.go:12](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/group_commit_test.go#L12), 12–145행 | Sync 공유, timer/wake 경합 진행, 오류 전파, Sync 중 다음 batch 분리, 취소 후 진행 |
| [wal_codec_test.go:10](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/wal_codec_test.go#L10), 10–72행 | Round trip, 짧은 buffer·identifier 손상 거부, `QISR1` JSON 거부, 특정 입력의 크기 비교 |
| [durable_prefix_test.go:13](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/durable_prefix_test.go#L13), 13–148행 | Sync 실패 시 durable 미승격, 재시도 한 Sync, gap 거부, base 설치와의 경합 |
| [local_record_lifecycle_test.go:69](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/local_record_lifecycle_test.go#L69), 69–179행 | 취소 후 receipt 완료 경계, Sync 이전 반환 금지, 결정 후 live recorder 제거 |
| [local_reclamation_test.go:12](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/local_reclamation_test.go#L12), 12–240행 | prepare/seal/apply/compact 순서, 거부 시 무변경, historical root 보존, 기존 seal 재사용, 공간 추정 전달 |
| [local_reclamation_crash_test.go:26](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/local_reclamation_crash_test.go#L26), 26–115행, 193–219행 | 두 명시적 경계에서 child process 강제 종료 후 exact root로 재개 |
| [local_archive_test.go:39](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/local_archive_test.go#L39), 39–174행 | base 재시도, suffix 재개방, certificate 보존, 잘못된 페이지·pending 상태 거부 |
| [local_cost_test.go:18](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/local_cost_test.go#L18), 18–85행, 268–290행, 958–999행 | 비용 공식, 단일 slot/suffix 구분, nil/empty 차이, overflow |
| [local_reserve_test.go:199](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/local_reserve_test.go#L199), 199–260행, 454–504행 | 1 byte 부족 거부·정확한 fit 허용, finite limit 요구, Append/Sync 실패 후 남는 상태 |

Hard-kill 테스트는 prepared marker Sync 후와 seal durable 후의 두 경계만 대상으로 합니다. Apply marker를 멱등하게 처리하는 코드도 테스트가 직접 제공합니다(같은 파일 166–190행). 따라서 이 정의에서 임의 instruction 경계의 crash safety, 전원 장애, 하위 qlog 교체의 전체 원자성, 실제 snapshot의 정합성, 사용자 apply의 exactly-once를 도출할 수 없습니다. 실제 객체 저장소의 내구성·삭제 안전성·Kubernetes·Chaos qualification 역시 이번 근거에 포함되지 않습니다.

`mistake_candidates: []`

**문서 보정: checkpoint 준비와 외부 generation base**

정확한 seal/base 필드·JSON 태그·root/state/prefix/anchor 정의·decoder 차이는 [형식 부록](quepaxa-go-formats-2026-10-30.md)을 따른다. 특히 seal은 strict JSON decoder가 아니라 `json.Unmarshal`을 사용하며, base는 strict decoder를 사용한다.

준비·투표·압축 순서는 `SetCheckpointValidator → PrepareCheckpoint → EncodeCheckpointSeal·제안 → Record의 RequirePreparedCheckpoint → seal 학습 → CompactThrough`다. 각 투표 recorder는 자신의 준비가 필요하다. 호출자가 등록하는 validator는 consensus 이전에 객체를 검증한다. 새 identity를 준비할 때 validator가 없으면 `checkpoint validation is unavailable`로 실패한다. 이미 준비한 같은 index/root는 현재 config·prefix·orders·membership·anchor 등 identity 검사를 통과한 뒤 validator를 다시 실행하지 않고 성공할 수 있다. `Record`의 검사는 객체 검증 callback을 대신 실행하지 않는다. node 객체 저장 경로는 `checkpoints.Verify(index, root, state)`를 callback으로 등록한다. [등록·Record 검사](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1635), [prepared 확인](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1742), [Prepare](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1771), [node 등록](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L603)

`InstallFencedGenerationBase(ctx, FencedGenerationBase)`는 일반 quorum-sealed checkpoint restore와 별개의 관리 권한 전환이다. Core는 source quorum certificate를 받거나 checkpoint validator callback을 호출하지 않는다. caller가 외부 anchor·fencing·객체의 권한과 정합성을 검증해야 한다. 실제 node 경로는 peer listener 이전에 공유 archive anchor와 WAL lineage를 대조하고 `VerifyGenerationAnchor` 및 필요한 checkpoint 객체 검증 후 호출한다. [API](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/compaction.go#L66), [caller 검증](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/generation.go#L14)

이 API는 LocalMode를 거부하고 context, nonzero ConfigID/Index/PrefixHash/RootHash/AnchorHash, Index가 최대 Slot이 아님, target bootstrap ConfigID 일치를 검사한다. 첫 설치는 tip/floor=0, 결정·ISR·prepared history 없음, pending reconfiguration 없음, configHistory가 초기 하나인 Core를 요구한다. WAL에는 최초 재구성 capability marker(`EntryReceipt`, slot=0, reconfiguration=true)만 허용한다. 같은 anchor로 재호출하면 floor/index·root·저장된 prefix가 정확히 같아야 성공하며 다른 anchor나 base는 거부한다. WAL Compact로 base를 저장한 뒤 anchor와 floor를 설치한다. [검사·retry·WAL](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/compaction.go#L73)

anchor는 base에 저장되어 replay되고 이후 base에도 유지된다. 이미 설치된 nonzero lineage와 다른 anchor는 거부한다. 재구성 base의 일반 규칙은 membership history 필수지만, **nonzero anchor, 초기 configHistory 하나, bootstrap ConfigID와 base.ConfigID 일치**인 초기 anchored base에는 history 없는 설치 예외가 있다. 이 예외를 일반 checkpoint seal의 history 요구 해제로 확대하지 않는다. [초기 base 예외·lineage](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/compaction.go#L695), [replay base 검사](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L3164)

floor 이하의 호출별 차이와 certified page의 부분 설치 가능성은 [합의·API 부록의 보정](quepaxa-go-protocol-evidence-2026-10-30.md)에 명시했다. 이 보정은 소스 대조이며 새 테스트 실행·클라우드 qualification·독립 재구현 검증을 뜻하지 않는다.
