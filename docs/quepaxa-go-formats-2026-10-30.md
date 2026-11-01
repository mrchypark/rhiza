# Go QuePaxa 직렬화 형식 부록

기준은 HEAD `8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd`다. 이 부록은 certificate, membership control, checkpoint와 recovery base의 현재 형식을 기록한다. 전체 peer wire나 QLog 물리 형식을 재현하지 않으며, 독립 재구현 인증을 주장하지 않는다. 아래 Go 선언은 기준 소스에서 발췌했다.

## 공통 인코딩

| Go 형식·태그 | JSON 표현과 생략 규칙 |
|---|---|
| `Slot`, `Step` | `uint64`의 JSON 정수다. |
| `uint`, `uint64` | JSON 정수다. ConfigID의 Go 범위는 호스트의 uint 폭을 따른다. |
| `NodeID`, `string` | JSON 문자열이다. |
| `ValueHash`, `Priority`, `[32]byte` | 32개 바이트의 JSON 숫자 배열이다. hex나 base64 문자열이 아니다. |
| `PublicKey` | 전용 Marshaler가 표준 base64 문자열을 출력한다. Unmarshaler는 표준 base64로 디코딩한 길이가 정확히 32바이트인지 검사한다. canonical 재인코딩 비교는 하지 않는다. |
| `[]byte` | Go `encoding/json`의 base64 문자열이다. nil이면 null, 비어 있는 non-nil slice이면 빈 문자열이다. |
| `json.RawMessage` | 인용·base64 변환 없이 중첩 JSON으로 삽입한다. |
| `[]NodeID`, `[]Member`, `[]summaryRef` 등 | JSON 배열이다. 생략 태그가 없으면 nil slice는 null이다. |
| `omitempty` | false, 빈 문자열·slice, nil pointer 등 해당 Go 값이 비어 있으면 인코더가 필드를 생략한다. |
| `omitzero`의 `[32]byte` | 32바이트가 모두 0이면 필드를 생략한다. |
| 태그에 생략 옵션 없음 | 인코더는 영 값도 필드를 출력한다. 필드 출력과 디코더의 필수 필드 검사는 별개다. |

근거: [기본 형식·PublicKey codec](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L23), [Priority](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/protocol.go#L11), [JSON codec](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/certificate.go#L51).

## Control prefix와 디코더

hex는 바이트 순서이며 마지막 `00`은 실제 trailing `0x00` 한 바이트다. 문자열에 역슬래시와 숫자 0을 붙이는 형식이 아니다.

| 값 | 정확한 prefix (hex) | 뒤따르는 payload |
|---|---|---|
| leader schedule (`QLDR`) | `51 4c 44 52 00` | `[]NodeID` JSON 배열 |
| read barrier (`QRDB`) | `51 52 44 42 00` | 16바이트 raw nonce; 전체 길이는 21바이트 |
| checkpoint seal (`QCKP`) | `51 43 4b 50 00` | 아래 CheckpointSeal JSON |
| reconfiguration (`QRCF`) | `51 52 43 46 00` | 아래 reconfigurationValue JSON |

각 control 디코더는 prefix가 다르면 해당 형식이 아니라는 false와 nil error를 반환한다. prefix가 맞고 본문이 잘못되면 true와 error를 반환한다. [prefix·codec](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L11)

| 디코더 | 수용·거부 규칙 |
|---|---|
| certificate | 비어 있는 입력은 거부한다. `decodeStrictJSON`으로 unknown field와 후속 JSON을 거부한다. decoding 후의 quorum·hash·slot·phase 검증은 별도다. |
| decision WAL record | strict JSON이며 certificate raw bytes가 비어 있으면 거부한다. 중첩 certificate의 합의 유효성은 별도 검증한다. |
| reconfiguration | strict JSON이다. `Abort && !Terminal`, `Freeze==0`, `TerminalSlot!=Freeze+16`, `Target.ConfigID==0`, 빈 target members를 거부한다. uint64 덧셈 자체에 별도 overflow 검사는 없다. 목표 구성·prefix·현재 전환과의 binding은 적용 경로에서 추가 검사한다. |
| checkpoint seal | `json.Unmarshal`을 사용하므로 unknown field를 허용하지만 추가 JSON 값·쓰레기는 거부한다. 전체 control 길이 128KiB 초과, Index=0, 영 RootHash/StateHash/PrefixHash, 빈 NextLeaderOrder를 거부한다. 이 codec만으로 config·membership·leader order·anchor를 검증하지 않는다. encoder는 Marshal과 총 길이만 검사한다. |
| leader schedule | `json.Unmarshal`로 문자열 배열을 읽고 빈 배열을 거부한다. 구성원 일치·중복·제어 slot 검사는 Core가 수행한다. |
| read barrier | prefix가 맞으면 전체 길이가 정확히 21바이트여야 한다. |
| consensus base | strict JSON이다. ClosedThrough=0, 영 PrefixHash/RecoveryRoot, 빈 NextLeaderOrder를 거부한다. config·epoch·membership·entry slot/hash의 binding은 replay·설치 경로가 추가 검사한다. |

strict는 `DisallowUnknownFields`와 두 번째 Decode의 EOF 확인을 뜻한다. 중복 key 거부나 canonical JSON 바이트 계약을 추가하지 않는다. 모든 JSON codec은 해당 Go decoder의 필드 매칭·영 값 처리 규칙을 그대로 사용하며, 누락된 필드 전부를 자동으로 거부하지 않는다. [strict 구현](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/certificate.go#L108), [control decode](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L107), [base decode](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/compaction.go#L758)

## Certificate와 결정 WAL JSON

다음 선언에서 각 필드의 `json` 태그가 정확한 직렬화 이름이다. `ReconfigurationID`는 최상위 certificate가 아니라 개별 summary의 선택적 필드다. 인코더는 영 ReconfigurationID와 nil proposal 참조를 생략한다. proposal 참조에는 payload가 없다. [선언·인코더](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/certificate.go#L10)

```go
type proposalRef struct {
	Priority   Priority  `json:"priority"`
	ProposerID NodeID    `json:"proposer_id"`
	Hash       ValueHash `json:"hash"`
}

type summaryRef struct {
	RecorderID        NodeID       `json:"recorder_id"`
	ReconfigurationID *ValueHash   `json:"reconfiguration_id,omitempty"`
	Step              Step         `json:"step"`
	FirstCurrent      *proposalRef `json:"first_current,omitempty"`
	AggregatePrior    *proposalRef `json:"aggregate_prior,omitempty"`
}

type certificate struct {
	ConfigID  uint         `json:"config_id"`
	Slot      Slot         `json:"slot"`
	Step      Step         `json:"step"`
	Proposal  proposalRef  `json:"proposal"`
	Summaries []summaryRef `json:"summaries"`
}

type decisionRecord struct {
	Value       []byte          `json:"value"`
	Certificate json.RawMessage `json:"certificate"`
}

```

## Cluster·재구성·membership history

`Terminal=false`는 freeze, `Terminal=true, Abort=false`는 성공 terminal, 둘 다 true이면 addition abort다. freeze PrefixHash는 F−1, terminal PrefixHash는 T−1의 결정 prefix를 결합한다. `T=F+16`이며 목표 검증·적용 규칙은 [멤버십 부록](quepaxa-go-membership-evidence-2026-10-30.md)을 따른다. [선언](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L69), [binding](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration.go#L350)

```go
type Member struct {
	ID          NodeID    `json:"node_id"`
	URL         string    `json:"url"`
	PeerURL     string    `json:"peer_url,omitempty"`
	LogURL      string    `json:"log_url"`
	PublicKey   PublicKey `json:"public_key"`
	WALIdentity string    `json:"wal_identity,omitempty"`
}

// Cluster describes one voter configuration.
type Cluster struct {
	ConfigID uint     `json:"config_id"`
	Members  []Member `json:"members"`
}

type reconfigurationValue struct {
	Terminal     bool     `json:"terminal,omitempty"`
	Abort        bool     `json:"abort,omitempty"`
	Freeze       Slot     `json:"freeze"`
	TerminalSlot Slot     `json:"terminal_slot"`
	Target       Cluster  `json:"target"`
	PrefixHash   [32]byte `json:"prefix_hash"`
}

type DecidedValue struct {
	Slot        Slot            `json:"slot"`
	Hash        ValueHash       `json:"hash"`
	Value       []byte          `json:"value"`
	Certificate json.RawMessage `json:"certificate,omitempty"`
}

type ConfigTransition struct {
	Freeze   DecidedValue `json:"freeze"`
	Terminal DecidedValue `json:"terminal"`
}

// MembershipRecord is the ordered genesis cluster followed by every
// completed config transition certified by this Core.
type MembershipRecord struct {
	Genesis     Cluster            `json:"genesis"`
	Transitions []ConfigTransition `json:"transitions"`
	// Abort retains the latest certified addition abort. It is evidence for a
	// management retry round, never authority to activate its target.
	Abort *ConfigTransition `json:"abort,omitempty"`
}

```

`DecidedValue.certificate`는 RawMessage이고 `value`는 base64다. membership의 `transitions`는 성공 전환의 인증된 freeze/terminal 쌍을 순서대로 담으며 `abort`는 최신 abort의 증거다. abort는 target 활성화 권한을 주지 않는다. [history 선언](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/membership_history.go#L8)

## Checkpoint seal과 recovery base

[CheckpointSeal·관련 API 타입](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L161):

```go
type CheckpointSeal struct {
	ConfigID             uint     `json:"config_id"`
	Index                Slot     `json:"index"`
	RootHash             [32]byte `json:"root_hash"`
	StateHash            [32]byte `json:"state_hash"`
	PrefixHash           [32]byte `json:"prefix_hash"`
	NextLeaderOrder      []NodeID `json:"next_leader_order"`
	FollowingLeaderOrder []NodeID `json:"following_leader_order,omitempty"`
	// Membership is required when the checkpoint can span reconfiguration.
	// Fixed-membership checkpoints retain their compact legacy encoding.
	Membership           *MembershipRecord `json:"membership,omitempty"`
	GenerationAnchorHash [32]byte          `json:"generation_anchor_hash,omitzero"`
}

// FencedGenerationBase is an externally verified immutable recovery anchor
// for a fresh target generation. It carries no source quorum certificate.
type FencedGenerationBase struct {
	ConfigID   uint
	Index      Slot
	PrefixHash [32]byte
	RootHash   [32]byte
	AnchorHash [32]byte
}

type SealedCheckpoint struct {
	CheckpointSeal
	DecisionSlot Slot
}

```

`SealedCheckpoint`와 `FencedGenerationBase`는 위 API 상태/입력 타입이다. 이 경로가 별도 control JSON으로 Marshal하는 타입은 아니다. 이 두 타입에는 JSON 태그가 없으며, WAL에 실제로 직렬화되는 recovery base는 아래 `consensusBase`다. `SealedCheckpoint.DecisionSlot`은 인밴드 seal JSON의 필드가 아니다. [base 선언](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/compaction.go#L214):

```go
type consensusBase struct {
	ConfigID             uint              `json:"config_id"`
	ClosedThrough        Slot              `json:"closed_through"`
	PrefixHash           [32]byte          `json:"prefix_hash"`
	RecoveryRoot         [32]byte          `json:"recovery_root"`
	LeaderEpoch          uint64            `json:"leader_epoch"`
	NextLeaderOrder      []NodeID          `json:"next_leader_order"`
	FollowingLeaderOrder []NodeID          `json:"following_leader_order,omitempty"`
	Membership           *MembershipRecord `json:"membership,omitempty"`
	GenerationAnchorHash [32]byte          `json:"generation_anchor_hash,omitzero"`
}
```

| 필드·identity | 정의와 관계 |
|---|---|
| seal `Index` / base `ClosedThrough` | snapshot이 나타내는 state의 결정 index 및 설치할 log floor다. seal 자체가 결정된 slot과 다르다. |
| `DecisionSlot` | seal 값이 합의된 실제 slot이다. floor가 Index까지 전진해도 seal 결정이 suffix에 남을 수 있다. |
| `ConfigID` | checkpoint 뒤 `Index+1`을 소유하는 구성과 대조한다. |
| `RootHash` / base `RecoveryRoot` | 복구 객체의 정확한 root identity다. Core는 외부 validator에 객체 검증을 맡긴다. 객체 저장 checkpoint 경로는 직렬화한 root JSON bytes의 SHA-256을 사용한다. |
| `StateHash` | 외부 checkpoint state identity이며 root hash와 별개다. 객체 저장 checkpoint 경로는 순서대로 각 file의 `Role + ":" + Hash + ":" + decimal(Size) + "\n"`를 이어 SHA-256한다. base JSON에는 StateHash 필드가 없다. |
| `PrefixHash` | Index까지의 인증된 결정 prefix다. `SHA256(domain || previous32 || uint64-big-endian(slot) || valueHash32)`로 갱신한다. domain은 ASCII `rhiza/decision-prefix` 뒤 실제 `0x00`이다. 초기 prefix는 영 32바이트이며 외부 base 설치는 검증한 prefix를 시작점으로 사용한다. |
| `NextLeaderOrder` | Index+1이 속한 epoch의 leader 순서다. 모든 해당 voter가 정확히 한 번 나와야 한다. |
| `FollowingLeaderOrder` | 해당 epoch 안에서 다음 schedule이 이미 결정되어 필요한 경우 그 다음 epoch용 순서다. 불필요하면 빈 slice가 허용되고 인코더가 생략한다. |
| base `LeaderEpoch` | Index+1의 내부 key다. 고정 구성은 `(slot−1)/16`, 재구성 모드는 구성 시작 S를 기준으로 `S+16*((slot−S)/16)`이다. 상대 epoch 번호와 구분한다. |
| `Membership` | 재구성 가능한 seal에는 검증된 history가 필요하다. 고정 구성 seal은 history를 거부한다. 외부 anchored 초기 base의 좁은 예외는 영속성 부록에 기술했다. |
| `GenerationAnchorHash` / API `AnchorHash` | 외부에서 검증한 generation lineage identity다. source quorum certificate를 대체하지 않는다. 일반 genesis에서는 영 값이며 seal/base 인코더가 생략한다. node 경로는 저장된 generation anchor JSON bytes의 SHA-256을 사용한다. |

근거: [객체 root·state 생성](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L566), [stateHash](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1620), [prefix 함수](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L36), [로컬 identity 검사](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1671), [leader orders](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L833), [anchor digest](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/anchor.go#L44).

Core가 root/state 객체 형식을 직접 구현하거나 generic validator 없이 그 객체의 존재·내구성을 보장하는 것은 아니다. Local store와 객체 저장 경로의 검증 책임을 섞지 않는다. [node 객체 validator 등록](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L603), [Local validator 등록](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L548)
