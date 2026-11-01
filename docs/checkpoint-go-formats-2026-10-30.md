# Go checkpoint 형식과 API 부록

이 부록은 커밋 `008f8e576ad76425182e10a69b987f10cfb48b2a`의 저장 bytes와 공개 API를 정의한다. 동작 순서와 외부 호출자 책임은 [구현 명세](checkpoint-go-reverse-spec-2026-10-30.md)에 따른다.

## 1. JSON schema와 Go 대응

다음 선언은 persisted JSON 태그와 Go 자료형을 그대로 나타낸다. JSON 숫자는 Go 대상 타입 범위를 따라 decoding된다. `[32]byte`는 hex 문자열이 아니라 숫자 배열이다. 생성 JSON은 `json.Marshal`의 compact bytes이며 아래 선언 순서로 필드를 기록한다. `omitempty`는 0 또는 빈 문자열일 때만 해당 필드를 생략한다. `json:"-"`와 소문자 비공개 필드는 저장하지 않는다. 엄격 decoder는 unknown field와 trailing JSON을 거절하지만 duplicate key에 대한 별도 거절은 없으며 `encoding/json`의 동작을 따른다. 누락 필드는 zero value가 되고 이후 validation을 적용한다. [자료형][types], [decoder][json]

```go
type Block struct {
	Hash       string `json:"hash"`
	Generation uint64 `json:"generation,omitempty"`
	Size       int64  `json:"size"`
}

type File struct {
	Role   string  `json:"role"`
	Hash   string  `json:"hash"`
	Size   int64   `json:"size"`
	Blocks []Block `json:"blocks"`
	Path   string  `json:"-"`
}

type Source struct{ Role, Path string }

type Checkpoint struct {
	SQLExecutionPolicy int      `json:"sql_execution_policy"`
	ConfigID           uint     `json:"config_id"`
	Generation         uint64   `json:"generation"`
	Index              uint64   `json:"index"`
	Hash               [32]byte `json:"hash"`
	Size               int64    `json:"size"`
	Files              []File   `json:"files"`
	RootHash           [32]byte `json:"-"`
	claim              *PublisherClaim
}

type PublisherClaim struct {
	ConfigID      uint   `json:"config_id"`
	Generation    uint64 `json:"generation"`
	OwnerID       string `json:"owner_id"`
	Purpose       string `json:"purpose,omitempty"`
	LeaseUntilMS  int64  `json:"lease_until_unix_ms"`
	ReservedIndex uint64 `json:"reserved_index"`
	BoundIndex    uint64 `json:"bound_index,omitempty"`
	RootHash      string `json:"root_hash,omitempty"`
	version       *objstore.ObjectVersion
}

type currentPointer struct {
	Index    uint64 `json:"index"`
	RootHash string `json:"root_hash"`
}

type verifiedRoot struct {
	index uint64
	state [32]byte
}

type recoveryPinRecord struct {
	ConfigID     uint       `json:"config_id"`
	OwnerID      string     `json:"owner_id"`
	Token        string     `json:"token"`
	Index        uint64     `json:"index"`
	RootHash     string     `json:"root_hash"`
	Root         Checkpoint `json:"root"`
	LeaseUntilMS int64      `json:"lease_until_unix_ms"`
	version      *objstore.ObjectVersion
}

```

`version *objstore.ObjectVersion`과 `claim *PublisherClaim`은 저장 필드가 아니다. object version은 provider Attributes에서 얻는 별도 CAS identity다. Source는 파일 입력용이며 persisted root에 저장하지 않는다. recovery pin의 root에는 Checkpoint 전체 descriptor가 중첩된다.

Local control의 schema는 다음과 같다. [Local control][lgc]

```go
type localMaintenanceControl struct {
	Version      uint   `json:"version"`
	Token        string `json:"token"`
	LocationHash string `json:"location_hash"`
	BindingHash  string `json:"binding_hash"`
}
```

현재 `version`은 2이다. free 상태는 token `""`이며 token 필드는 생략하지 않는다. location/binding hash가 빈 문자열이어도 두 필드는 존재한다. pin token과 gate token은 각각 16바이트 난수의 32자리 소문자 hex다.

### 의미 검증

| 자료 | 거절 조건 및 검증 |
|---|---|
| Checkpoint | policy!=7, config 불일치, generation/index=0, size<=0 또는 >32 GiB, 파일 수가 1..2 밖이면 거절한다. |
| File | 역할 중복·허용되지 않은 역할, size<=0, 빈 blocks, 블록 합계와 size 불일치, descriptor hash 불일치를 거절한다. SQLite가 필수다. 읽기는 역할 순서를 강제하지 않는다. |
| Block | hash가 SHA-256 hex로 decode되지 않음, size<=0 또는 >64 MiB, 마지막 외 블록이 64 MiB가 아님을 거절한다. generation=0을 허용한다. |
| root 전체 | 합계 size 불일치, 총 블록>512, 상태 hash 불일치를 거절한다. |
| PublisherClaim | config 불일치, generation=0, 빈 owner, purpose가 publisher/maintenance/generation 이외, publisher/generation의 reserved_index=0을 거절한다. lease·bound/root 관계는 해당 연산에서 추가 검사한다. |
| recoveryPinRecord | config 불일치, 빈 owner/token, index=0을 거절한다. root의 index/config 일치, descriptor validation, RootHash와 marshal(root)의 SHA-256 일치를 요구한다. token의 길이나 hex 문법을 읽기에서 별도 검사하지 않는다. |
| currentPointer | 사용 경로에서 root_hash를 32바이트 hex로 decode하고 canonical root를 연다. 별도 schema version은 없다. |
| 검증 캐시 | JSON object `{물리상대키: int64 size}`다. 키 parse 및 1..64 MiB size를 검증한다. |

[전체 root validation][validate], [claim 읽기][claimread], [pin 읽기][pinread]

## 2. 해시의 정확한 preimage

모든 digest는 SHA-256이다. digest 문자열을 새로 생성할 때는 소문자 64자리 hex를 사용한다. `decodeHash`는 hex decoding 결과가 32바이트인지 검사하므로 대문자 hex도 받아들인다. 단, file descriptor hash 비교는 문자열의 정확한 일치를 요구한다. [해시 구현][hashes], [decode][decode]

```text
block.Hash = hex(SHA256(블록 원본 bytes))
file.Hash = hex(SHA256(concat_in_block_order(
    block.Hash + ":" + decimal(block.Generation) + ":" + decimal(block.Size) + "\n")))
root.Hash = SHA256(concat_in_file_order(
    file.Role + ":" + file.Hash + ":" + decimal(file.Size) + "\n"))
root.RootHash = SHA256(json.Marshal(root))
pin_key_owner_digest = hex(SHA256(raw owner bytes))
NamespaceDigest = SHA256(raw prefix bytes)
VersionDigest = SHA256(decimal(int64(version.Type)) + ":" + raw version.Value bytes)
```

decimal은 선행 0 없는 Go 정수 표현이다. 해시 preimage의 줄바꿈은 LF 1바이트이며 마지막 항목에도 붙는다. RootHash와 File.Path, 내부 claim/version은 JSON preimage에 포함하지 않는다. root.Hash는 generation과 index를 직접 포함하지 않지만 file descriptor 안의 블록 generation을 포함한다. generation/index/config/policy를 포함한 전체 root identity는 RootHash로 구분한다. marshal JSON과 같은 의미의 다른 공백·필드 순서를 가진 bytes라도 RootHash는 달라진다. pin은 descriptor를 다시 marshal하여 비교하므로 exact 생성 descriptor에 맞아야 한다.

## 3. 키와 로컬 파일

아래 상대키에 prefix가 비어 있지 않으면 raw `prefix + "/"`를 붙인다. 경로 정규화·URL escaping은 없다. `%020d`는 20자리 0-padding unsigned decimal이다. `%x`는 digest 소문자 hex다. [키 구현][keys]

| 대상 | 상대키/경로 |
|---|---|
| root | `checkpoint/roots/%020d_%x.json` (index, RootHash) |
| generation=0 블록 | `checkpoint/blocks/%x.block` (block hash) |
| generation>0 블록 | `checkpoint/blocks/%x_%020d.block` (block hash, generation) |
| certified pointer | `checkpoint/CURRENT` |
| 공유 claim | `checkpoint/PUBLISHER` |
| owner별 pin | `checkpoint/recovery-pins/<SHA256(owner) hex>` |
| Local gate | `local-gc-control` |
| 검증 캐시 | `path.Join(localDir, "checkpoint-verified-blocks.json")` |
| 다운로드 SQLite | `path.Join(dir, "sqlite.db")` |
| 다운로드 graph | `path.Join(dir, "graph-data.db")` |

root parser는 basename에서 `.json` suffix를 선택적으로 제거하고 첫 `_`로 index/hash를 나눈다. index는 uint64로 parse하며 그 자체에서 20자리 canonical 표기나 suffix 존재를 요구하지 않는다. 블록 parser는 `.block` suffix를 요구하고 generation suffix가 있으면 비영 uint64이며 정확한 20자리 표현인지 검사한다. 두 parser 모두 basename을 보므로 prefix·부모 경로 canonicality를 증명하지 않는다. Local GC는 완전한 key를 재생성하여 정확히 비교한다. HA GC의 열거 경로는 이 Local 수준의 canonical key 비교를 하지 않는다.

## 4. 읽기 상한과 직렬화 경계

| 경로 | 한 번에 읽는 최대 bytes / 성공 조건 |
|---|---|
| root | 65537 / 실제 65536 이하와 expected SHA-256 일치 |
| PUBLISHER 안정 읽기 | 65537 / 65536 이하와 앞뒤 동일 nonnil version |
| pin 안정 읽기 | 66561 / 66560 이하와 앞뒤 동일 nonnil version |
| Local gate 안정 읽기 | 4097 / 4096 이하와 앞뒤 동일 nonnil version |
| CURRENT Load/refresh/promote GET | 65537 / 별도 `len>65536` 검사 없이 strict JSON decode. root와 같은 명시적 초과 오류를 적용하지 않는다. |
| 블록 다운로드·Verify | descriptor size+1 / 정확한 descriptor size와 SHA-256 일치 |
| export source/target | size+1 / size 상한과 SHA-256 일치; root는 size=65536, 블록은 descriptor size |
| 디스크 검증 캐시 | `os.ReadFile` / 명시적인 bytes 상한이 없다. |

JSON upload는 publisher claim에 별도 크기 검사가 없고, pin은 66560바이트, 생성 root는 65536바이트 상한을 검사한다. stable lease read는 최대 4회 시도하며 limit+1 bytes로 초과를 탐지한다. [안정 읽기][stable], [root 읽기][readroot], [캐시][cache], [export][export]

## 5. 공개 API 시그니처

아래는 일곱 운영 파일에 선언된 exported 함수의 전체 시그니처다. `Manager`와 `RecoveryPin`의 private 필드는 직접 구성하지 않고 생성·획득 API로 얻는다. 함수별 행동과 오류 계약은 본문을 따른다.

```go
func NewManager(bucket objstore.Bucket, prefix, localDir string, configID ...uint) *Manager
func (m *Manager) Load(ctx context.Context) error
func (m *Manager) AcquirePublisherClaim(ctx context.Context, owner string, minExclusive uint64, lease time.Duration) (*PublisherClaim, error)
func (m *Manager) AcquireGenerationClaim(ctx context.Context, owner string, index uint64, lease time.Duration) (*PublisherClaim, error)
func (m *Manager) BindPublisherClaim(ctx context.Context, claim *PublisherClaim, index uint64, root [32]byte, lease time.Duration) (*PublisherClaim, error)
func (m *Manager) RenewPublisherClaim(ctx context.Context, claim *PublisherClaim, lease time.Duration) (*PublisherClaim, error)
func (m *Manager) ValidatePublisherClaim(ctx context.Context, owner string, index uint64, root [32]byte) error
func (m *Manager) ReleasePublisherClaim(ctx context.Context, claim *PublisherClaim) error
func (m *Manager) CreateFiles(ctx context.Context, claim *PublisherClaim, sources []Source, index uint64) (*Checkpoint, error)
func (m *Manager) PromoteCertifiedCurrent(ctx context.Context, root *Checkpoint) error
func (m *Manager) Latest() *Checkpoint
func (m *Manager) OpenRoot(ctx context.Context, index uint64, rootHash [32]byte) (*Checkpoint, error)
func (m *Manager) DownloadRootFiles(ctx context.Context, index uint64, rootHash [32]byte, dir string) ([]File, error)
func (m *Manager) DownloadAndVerifyRootFiles(ctx context.Context, root *Checkpoint, dir string) ([]File, error)
func (m *Manager) Verify(ctx context.Context, index uint64, rootHash, state [32]byte) error
func (m *Manager) PinRecoveryRoot(ctx context.Context, root *Checkpoint, owner string, lease time.Duration) (*RecoveryPin, error)
func (p *RecoveryPin) Renew(ctx context.Context, lease time.Duration) error
func (p *RecoveryPin) Close(ctx context.Context) error
func (p *RecoveryPin) Root() (*Checkpoint, error)
func (m *Manager) GarbageCollectFrom(ctx context.Context, retain map[[32]byte]struct{}, keep int, floor uint64, grace time.Duration) error
func (m *Manager) CreateLocalFiles(ctx context.Context, sources []Source, index uint64) (*Checkpoint, error)
func (m *Manager) SetLocalControlBinding(locationHash, bindingHash string)
func (m *Manager) LocalRootExists(ctx context.Context, index uint64, hash [32]byte) (bool, error)
func (m *Manager) LocalRecoveryActive(ctx context.Context) (bool, error)
func (m *Manager) WithLocalMaintenanceContext(ctx context.Context, work func(context.Context) error) error
func (m *Manager) RecoverLocalMaintenanceControl(ctx context.Context, owner *internalobjstore.LocalFilesystemOwner) error
func (m *Manager) LocalMaintenanceResolved(ctx context.Context) (bool, error)
func (m *Manager) GarbageCollectLocal(ctx context.Context, retired, retain map[[32]byte]struct{}, grace time.Duration) error
func LocalRecoveryPinDeadline(ctx context.Context, pins []*RecoveryPin) (time.Time, error)
func LocalReadbackContext(ctx context.Context) (context.Context, context.CancelFunc)
func LocalBucket(bucket objstore.Bucket) objstore.Bucket
func LocalBeforeDispatch(err error) bool
func (a *AutoCheckpointer) ConfigureTail(bytes func() int64, budget int64)
func (a *AutoCheckpointer) ConfigurePublication(eligible func() bool, publish func(context.Context, *Checkpoint) error)
func (a *AutoCheckpointer) ConfigurePublisher(owner string, floor func() uint64, advance func(context.Context, uint64) error)
func NewAutoCheckpointer(manager *Manager, material *materializer.Materializer, interval int64, duration time.Duration) *AutoCheckpointer
func (a *AutoCheckpointer) Start(ctx context.Context, tipFunc func() uint64, before func(context.Context) error)
func (a *AutoCheckpointer) Stop()
func (a *AutoCheckpointer) CheckpointOnShutdown(ctx context.Context, tip uint64) error
func NewManagerFromEnv(bucket objstore.Bucket, localDir string) *Manager
func CopyRoot(ctx context.Context, bucket objstore.Bucket, sourcePrefix, targetPrefix string, root *Checkpoint) error
func WithPublisherBusyObserver(ctx context.Context, observe func(PublisherBusyObservation)) context.Context
```

공개 role 상수는 `RoleSQLite="sqlite"`, `RoleGraphData="graph-data"`다. 공개 sentinel은 `ErrStaleCheckpoint`, `ErrAuthoritativeRecoveryBaseRequired`, `ErrPublisherBusy`, `ErrRecoveryAdmissionBusy`, `ErrPublisherFenced`, `ErrLocalMaintenanceBusy`이며 본문의 각 오류 분기를 따른다. `ErrRecoveryAdmissionBusy`는 `ErrPublisherBusy`를 `%w`로 감싼 값이다. 감싼 stale/busy 오류는 `errors.Is`로 판별한다. 저장소의 불명확한 mutation 오류를 fenced나 busy로 임의 변환하지 않는다. [sentinel][limits], [Local sentinel][lgc]

## 6. 관찰 이벤트 schema

이 구조체는 persisted JSON 형식이 아니다. context callback으로 동기 전달하는 진단 API이며 JSON 태그가 없다. 필드가 채워지는 조건은 본문의 진단 계약을 따른다. [observer][busy]

```go
type PublisherBusyObservation struct {
	Source               string
	RequestedPurpose     string
	Attempt              int
	ActiveClaim          bool
	Purpose              string
	PurposeTruncated     bool
	OwnerID              string
	OwnerIDTruncated     bool
	Generation           uint64
	ReservedIndex        uint64
	BoundIndex           uint64
	LeaseRemainingMillis int64
	HolderCategory       string
	NamespaceDigest      [32]byte
	VersionDigest        [32]byte
	VersionPresent       bool
	ObservedAtMillis     int64
}
```

busy source는 `publisher_claim_active`, `publisher_claim_conditional_exhausted`, `generation_claim_active`, `generation_claim_conditional_exhausted`다. 조건 충돌 소진 이벤트는 attempt=4이며 ActiveClaim과 holder 필드는 기본 zero value다. lifecycle source는 본문의 여섯 maintenance 이벤트를 사용한다. digest와 이벤트는 승인 토큰이나 외부 fencing 증거를 대신하지 않는다.

[core]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1
[local]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/local.go#L1
[lgc]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/local_gc.go#L1
[lb]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/local_gc_bucket.go#L1
[auto]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/auto.go#L1
[export]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/export.go#L15
[busy]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/busy_observation.go#L17
[constructor]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L134
[env]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/auto.go#L343
[acquire]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L255
[promote]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L652
[limits]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L29
[sources]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L864
[upload]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L748
[validate]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1584
[create]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L509
[lease]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L636
[parallel]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L825
[load]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L143
[refresh]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L592
[latest]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L884
[generation]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L197
[stable]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L439
[confirm]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L364
[bind]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L318
[renew]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L339
[claimvalidate]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L380
[release]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L391
[readroot]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1563
[json]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1705
[download]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L905
[verify]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1020
[cache]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1638
[pin]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1091
[pinlife]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1155
[pinscan]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1291
[deadline]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/local_gc.go#L299
[gc]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1328
[maintenance]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1340
[sweep]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1403
[gate]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/local_gc.go#L72
[localgc]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/local_gc.go#L207
[autocreate]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/auto.go#L176
[autorenew]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/auto.go#L293
[shutdown]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/auto.go#L145
[types]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L50
[claimread]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L423
[pinread]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1230
[hashes]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1620
[decode]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1717
[keys]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1727
[policy]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/sqlpolicy/policy.go#L17
