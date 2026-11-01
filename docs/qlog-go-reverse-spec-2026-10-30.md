# QLog Go 구현 역명세

기준일: 2026-10-30. 기준 HEAD: `008f8e576ad76425182e10a69b987f10cfb48b2a`.
대상은 `pkg/qlog/{entry,manifest,lock,wal}.go`이다. 이 문서는 현재 동작을 재현하기 위한 명세이며 합의 프로토콜, Local 복구 정책, 클라우드 SDK, Kubernetes 런타임은 범위 밖이다. 바이트 형식은 [형식 부록](qlog-go-formats-2026-10-30.md)에 정의한다.

## 근거 수준과 리뷰 상태

본문의 동작은 **코드 확인** 결과이다. 별도 검증자가 아래 테스트 실행 결과를 전달했으며, 작성자가 실행한 것으로 표기하지 않는다. 정형 증명이나 전체 EGM 절차를 수행했다는 주장은 하지 않는다. 독립 리뷰의 지적 사항은 수정 후 두 검토자가 해소를 재확인했다. 전체 명세에 대한 무결점 판정은 아니다. [독립 리뷰 기록](qlog-go-egm-review-2026-10-30.md)을 참조한다. 소스 링크는 기준 프로젝트의 절대 경로를 사용한다.

## 공개 API

아래 선언의 receiver와 인자는 Go 구현 그대로이다. 이 HEAD의 `Segment` 필드는 `file`, `index`, `offset`, `mu`, `allowTornTailReadOnly`로 모두 비공개이며 공개 `Index`, `Path`, `FD` 필드나 메서드는 없다. `WAL`, `LockFile`, `Compaction`의 필드도 비공개이다. `EntryType`은 `uint8`이고 값은 Proposal=0, Receipt=1, Decide=2, Checkpoint=3, CheckpointVerified=4이다.

```go
type Entry struct {
    Slot uint64
    Hash [32]byte
    Type EntryType
    Payload []byte
}
type CapacitySnapshot struct { Used int64; Limit int64; Fatal error }
var ErrCapacity = errors.New("WAL capacity reached")
func (e Entry) Encode() []byte
func DecodeEntry(data []byte) (Entry, int, error)
func Acquire(dir string) (bool, *LockFile, error)
func (lf *LockFile) Release() error
func Open(dir string) (*WAL, error)
func OpenLocalDeferredCleanup(dir string) (*WAL, error)
func OpenReadOnly(dir string) (*WAL, error)
func (w *WAL) FinalizeDeferredCleanup() error
func (w *WAL) Capacity() CapacitySnapshot
func (w *WAL) Bytes() int64
func (w *WAL) SetMaxBytes(max int64) error
func (w *WAL) Identity() ([]byte, bool)
func (w *WAL) BindIdentity(identity []byte) error
func (w *WAL) Append(entry Entry) error
func (w *WAL) Sync() error
func (w *WAL) Read() ([]Entry, error)
func (w *WAL) Scan(visit func(Entry) error) error
func (w *WAL) RestoreSegment(index uint32, data []byte) error
func (w *WAL) EstimateCompactedBytes(base Entry, retainedValues map[[32]byte][]byte) (uint64, error)
func (w *WAL) BeginCompaction(base Entry, retainedValues map[[32]byte][]byte) (*Compaction, error)
func (c *Compaction) Build() error
func (c *Compaction) Commit() error
func (c *Compaction) Abort()
func (w *WAL) Compact(base Entry, retainedValues map[[32]byte][]byte) error
func (w *WAL) Close() error
```

근거: [entry.go:23](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/entry.go#L23), [lock.go:11](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/lock.go#L11), [wal.go:19](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L19). 아래 각 계약의 소스 링크가 나머지 선언을 포함한다.

## 전체 흐름과 파일 소유권

```text
호출자가 Acquire → Open 계열로 manifest 선택 → 참조 세그먼트 검증
  → Append → 필요 시 rollover와 manifest 발행 → Sync
  → BeginCompaction(기존 prefix 봉인, 별도 tail 활성화)
      → Build(prefix 재작성) → Commit(manifest 전환) 또는 Abort(tail 유지)
  → Close → 호출자가 Release
Local 복구: OpenReadOnly로 검사 → OpenLocalDeferredCleanup
  → 호출자 exact-base 복구 및 suffix replay → FinalizeDeferredCleanup
```

이 흐름의 호출 순서는 호출자 책임이다. `Open`은 `Acquire`를 호출하지 않으며 `Close`도 `Release`를 호출하지 않는다. WAL이 관리하는 파일은 정규 세그먼트, manifest, 두 내부 임시 파일 패턴이다. 미참조 정규 파일은 정리 대상이므로 다른 용도의 파일을 같은 이름으로 두면 안 된다. 비정규 파일은 mutable 정리에서 대체로 보존되지만 읽기 전용 검사에서는 오류가 될 수 있다. OS 사용자 소유자를 변경하는 `chown`은 없다.

## 열기, 권한, 권위 선택

기본 세그먼트 전환 기준은 64 MiB이고 전체 용량 제한은 0(미설정)이다. mutable 열기는 디렉터리를 `0700`으로 생성하고 기존 경로에도 `Chmod(0700)`을 실행한다. 참조 세그먼트는 `O_RDWR`로 열고 `0600`으로 변경한다. 실패 전에 수행된 권한 변경이나 tail 복구를 되돌리지 않는다. mutable 경로에는 읽기 전용과 동일한 symlink 방어가 없다.

`manifest_*.bin` 중 비정규 이름이 있으면 실패한다. 정규 이름을 문자열 정렬한 마지막 파일만 디코딩하고 파일명과 내부 generation을 비교한다. 최신 manifest가 손상되면 이전 manifest로 fallback하지 않는다. 참조된 sealed 파일은 실제 크기와 manifest length가 같아야 하며, active 파일은 실제 크기를 스캔한다. 참조 목록 순서가 스캔 순서이고 슬롯 정렬은 없다.

Manifest가 없으면 정규 세그먼트가 없거나 유일한 정규 세그먼트가 비어 있는 index 1이어야 한다. 후자는 삭제하고 새 `seg_001.log`와 초기 manifest를 만든다. 다른 정규 배치는 `unsupported WAL layout without manifest`이다. 이 경로는 비정규 세그먼트를 검증하지 않고 내부 임시 파일도 지운다. deferred 열기도 이 초기화 경로와 권한 변경 및 active tail 복구를 그대로 수행한다.

근거: [wal.go:107](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L107), [wal.go:299](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L299), [wal.go:375](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L375).

`OpenReadOnly`는 디렉터리 자체가 symlink이거나 디렉터리가 아니면 거부한다. 모든 발견 manifest와 참조 세그먼트는 symlink가 아닌 일반 파일이어야 한다. 선택 manifest 검증 후 미참조 `seg_*.log`도 이름 및 파일 종류를 검증한다. manifest가 없으면 발견 세그먼트의 정규 이름, symlink 여부와 크기 0을 요구하지만 새 파일은 만들지 않는다. 이 무manifest 분기에는 일반 파일 여부를 별도로 강제하는 조건이 없다. 읽기 전용 열기는 생성, chmod, truncate, 삭제를 수행하지 않는다.

근거: [wal.go:179](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L179).

## 프로세스 잠금과 객체 동시성

`Acquire`는 `MkdirAll(0755)`, `lock.qlog`의 `O_CREATE|O_RDWR` 열기(0644), 비차단 `flock(LOCK_EX|LOCK_NB)` 순서로 실행한다. 충돌은 `another process is using this WAL`이다. 기존 lock 파일 크기가 0이면 cleanStart=true이며 PID 생존 여부나 WAL 무결성을 뜻하지 않는다. 파일을 truncate하고 seek 후 PID를 기록한다. PID 쓰기 오류는 검사하지 않으며 획득 시 sync도 없다.

`Release`는 동일 inode를 유지하며 truncate(0), sync, unlock, close 순으로 실행한다. 중간 오류는 후속 단계를 중단한다. close까지 도달하면 오류 여부와 무관하게 내부 포인터를 nil로 만든다. nil 포인터 상태의 Release는 성공한다. 파일을 unlink하지 않는다.

WAL 변경은 객체 쓰기 잠금, Capacity/Bytes/Identity/Scan은 읽기 잠금을 사용한다. 세그먼트 접근에는 별도 mutex가 있다. compaction은 별도 mutex를 Begin부터 Commit/Abort까지 보유한다. 공개 API에 context, deadline 또는 I/O 취소는 없다. 객체 잠금은 프로세스 잠금을 대체하지 않는다.

근거: [lock.go:16](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/lock.go#L16), [wal.go:746](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L746), [wal.go:1081](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L1081).

## 용량과 identity

`Capacity`는 동일 잠금 아래 Used/Limit/Fatal을 반환한다. `Bytes`는 같은 totalBytes 카운터이며 디스크 재측정값이 아니다. 선택된 세그먼트의 논리 offset만 계산하고 manifest, lock, 임시 파일, 미참조 파일은 제외한다. 읽기 전용에서 제외한 torn tail과 실패한 append의 물리 바이트도 반영되지 않을 수 있다. Fatal이 있으면 이 수치로 새 쓰기를 허가하면 안 된다.

`SetMaxBytes`는 양수만 허용하며 Used>max이면 ErrCapacity를 감싸 반환하고 기존 제한을 유지한다. 같으면 성공한다. 제한은 메모리에만 저장되며 다시 열면 사라진다. 읽기 전용/fatal 여부는 검사하지 않는다. 0으로 해제하는 API는 없다.

`Identity`는 없으면 `(nil,false)`, 있으면 복사본과 true를 반환한다. `BindIdentity`는 길이 1~64바이트를 요구한 뒤 readOnly와 fatal을 검사한다. 같은 기존 값은 성공하고 다른 값은 `WAL identity is already bound`이다. 새 값을 복사하여 같은 세그먼트 목록으로 manifest를 발행한다. 발행 전 실패는 identity를 nil로 되돌리고 rename 후 실패는 유지한다. compaction도 바인딩을 보존한다. 이 바이트열의 의미는 QLog에서 검증하지 않는다.

근거: [wal.go:67](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L67), [wal.go:599](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L599).

## Append, rollover, Sync와 불확실한 오류

`Append`는 readOnly, fatal 순으로 검사하고 Encode 후 용량을 검사한다. `encoded length > Limit-Used`이면 ErrCapacity이며 fatal을 만들지 않는다. current offset+record size가 64 MiB보다 클 때만 rollover한다. 같으면 유지한다. 단일 레코드를 분할하지 않으므로 큰 레코드는 기준보다 큰 세그먼트를 만들 수 있고 빈 current에서도 전환할 수 있다.

rollover는 이전 current를 sync하고 dirty=false로 만든 뒤 새 index 파일을 `O_CREATE|O_EXCL|O_RDWR`, 0600으로 만든다. 목록에 없는 같은 index 파일이 있으면 삭제하고 디렉터리를 sync한 뒤 재생성할 수 있다. 새 파일 생성 뒤 디렉터리 sync와 새 manifest 발행을 수행한다. 발행 전 실패는 새 세그먼트를 목록에서 제거하고 파일을 닫아 삭제하지만, rename 후 오류에서는 새 목록과 generation을 유지한다. 일반 append rollover에는 compaction과 같은 명시적 index 고갈 검사가 없다.

레코드는 current offset에 한 번 WriteAt한다. 완전 성공이면 offset/Used를 증가시키고 dirty=true로 만든다. Append 성공은 sync 완료를 뜻하지 않는다. 오류 또는 short write는 `WAL append at <offset> is uncertain: …`를 fatal에 저장한다. short write에 오류가 없으면 io.ErrShortWrite를 사용한다. 전체 길이와 오류가 함께 반환되어도 fatal이며 offset/Used는 증가하지 않는다. 짧은 재시도로 덮어쓰지 않고 다시 열어 검증해야 한다.

`Sync`는 readOnly에서 성공한다. mutable에서는 fatal을 우선 반환하고 current가 없거나 dirty=false이면 아무 작업도 하지 않는다. 그 외 current만 sync하고 성공 시 dirty를 해제한다. Sync나 rollover 이전 파일 sync의 실패는 직접 반환하며 fatal로 저장하지 않는다. Sync 실패 시 dirty를 유지한다.

근거: [wal.go:500](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L500), [wal.go:662](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L662).

## 원자 파일 발행과 cleanup debt

원자 발행은 같은 디렉터리에 `.rhiza-segment-*` 임시 파일을 만들고 chmod(0600), 단일 WriteAt 및 short-write 검사, 파일 sync, close, rename, 디렉터리 sync 순서로 수행한다. 임시 경로 삭제를 defer한다. rename 성공부터 published=true이며 마지막 디렉터리 sync 성공과 구별한다. manifest 발행은 published이면 generation을 전진시키고 이후 오류에 `WAL manifest durability is uncertain: …` fatal을 저장한다. rename 전 오류는 generation을 유지한다.

성공한 manifest 발행 뒤 이전 manifest 삭제 실패는 로그만 남긴다. 일반 Open의 참조 복구 후 정리 실패도 로그만 남기고 열기를 성공시킨다. 정리는 미참조 정규 세그먼트, 이전 정규 manifest, `.rhiza-segment-*`, `.rhiza-compact-*`를 삭제하고 변경 시 디렉터리를 sync한다. 선택 manifest 자체는 지우지 않는다.

`OpenLocalDeferredCleanup`은 기존 manifest 복구 후 pendingCleanup=true로 남기며 이 시작 정리를 생략한다. `FinalizeDeferredCleanup`은 현재 목록과 generation으로 정리한다. 호출자가 exact-base 복구와 suffix replay에 성공했는지는 검사하지 않는다. 정리 오류를 반환하고 상태를 유지하므로 재시도할 수 있다. 성공하면 pendingCleanup/deferCleanup을 해제한다. pending이 없어도 deferCleanup을 해제하며 이후 반복 호출은 `WAL cleanup was not deferred`이다. deferred 상태라도 일반 manifest 발행의 이전 manifest 정리까지 생략하는 것은 아니다.

Finalize는 WAL mutex만 잡고 `compactMu`와 조정하지 않는다. 호출자는 compaction을 Commit 또는 Abort로 끝낸 뒤 Finalize해야 해당 작업의 파일을 보존할 수 있다. 예를 들어 pendingCleanup 상태에서 `Build → FinalizeDeferredCleanup → Commit`을 실행하면 Build의 target은 아직 manifest에 참조되지 않아 Finalize가 삭제하고 Commit의 target open이 실패한다. Build 진행 중이면 내부 임시 파일도 정리 대상이다.

근거: [wal.go:150](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L150), [wal.go:411](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L411), [wal.go:567](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L567), [wal.go:1023](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L1023), [wal.go:1180](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L1180).

## Scan, scratch 수명과 tail 복구

`Scan(nil)`은 오류이다. 파일 순서와 offset 순서로 헤더 및 레코드 CRC를 통과한 Entry를 전달한다. 타입 범위, 슬롯 순서/중복, payload와 Hash 관계는 검증하지 않는다. 헤더용 53바이트 scratch는 해당 세그먼트 스캔 안에서 재사용하며 필요한 최대 레코드 크기까지 증가한다. DecodeEntry는 payload를 별도로 복사하므로 콜백은 Entry를 보관할 수 있다. scratch 자체는 콜백에 노출되지 않는다. Scan은 전체 tail을 집계하지 않지만 엔트리당 payload 복사와 큰 레코드용 scratch는 필요하다. 열기 경로의 `scan`은 검증하면서 Entry 목록도 일시적으로 집계한다.

콜백 실행 동안 WAL 읽기 잠금과 세그먼트 mutex가 유지된다. 같은 WAL을 변경하거나 재귀 Scan하는 콜백은 교착될 수 있다. 콜백 오류는 즉시 반환하고 앞서 전달한 엔트리는 취소하지 않는다. `Read`는 Scan 결과를 모으므로 오류 이전 부분 결과와 오류를 함께 반환할 수 있다. Scan은 fatal을 검사하거나 새 fatal을 저장하지 않으며 성공 끝에 내부 offset을 갱신한다.

| 상태 | mutable Open의 active | sealed 및 일반 mutable Scan | readonly active |
|---|---|---|---|
| 불완전 헤더 | UnexpectedEOF이면 레코드 시작까지 truncate+sync | 오류 | 남은 길이<53이면 검사 전 prefix만 노출 |
| 정상 헤더가 선언한 payload 부족 | 시작까지 truncate+sync | UnexpectedEOF | 파일 수정 없이 prefix만 노출 |
| 잘못된 marker | 오류 | 오류 | 완전 헤더일 때 오류 |
| Header/Record CRC 오류 | 오류 | 오류 | 오류 |

특히 mutable의 45~52바이트 tail은 marker를 먼저 검사하므로 잘못된 marker이면 복구하지 않는다. readonly active는 같은 짧은 tail을 marker 검사 전에 제외한다. truncate 성공 후 sync 실패도 열기 실패이며 offset은 sync 성공 후에만 갱신한다. 완전한 손상 레코드를 torn write로 삭제하지 않는다. readonly의 Bytes는 유효 prefix 길이이며 물리 파일은 그대로 남는다.

근거: [entry.go:76](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/entry.go#L76), [wal.go:735](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L735), [wal.go:839](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L839), [wal.go:1323](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L1323).

## RestoreSegment

입력은 비어 있으면 안 되고 전체 바이트가 DecodeEntry로 끝까지 해석되어야 한다. 검증 후 WAL 잠금과 fatal 검사를 수행한다. **별도의 readOnly 거부 조건이 없다.** 따라서 이름과 주석만으로 읽기 전용 객체의 모든 API가 비변경이라고 가정하면 안 된다.

같은 index가 있으면 현재 논리 offset만큼 읽어 비교한다. 완전히 같으면 성공한다. sealed가 다르면 충돌이다. current는 비어 있거나 기존 바이트가 입력의 prefix이면 확장할 수 있고, delta 용량을 검사한다. 이는 주석의 “non-empty must match”보다 넓은 실제 동작이다. 같은 index의 current 교체는 원자 파일 교체와 새 descriptor 열기, 기존 descriptor close, offset/Used 차이 반영으로 끝나며 manifest를 새로 발행하지 않는다.

새 index는 current보다 커야 하며 연속일 필요는 없다. 전체 입력의 용량을 검사하고 원자 발행 후 목록 끝과 current로 추가하여 새 manifest를 발행한다. manifest 발행 전 실패는 목록/Used를 되돌리고 새 파일을 제거한다. rename 후 오류는 새 상태와 fatal을 유지한다. 세그먼트 자체의 rename 후 디렉터리 sync 오류도 `WAL segment durability is uncertain: …` fatal을 만든다. 새 index 복원은 이전 current를 별도로 sync하지 않으며 dirty를 명시적으로 바꾸지 않는다. 교체 후 descriptor 열기/close 실패의 롤백을 추가로 보장하지 않는다. index=0 전용 선행 검사는 없고 새 참조 발행의 manifest 검증에 걸릴 수 있다.

위의 오류 반환과 롤백 설명에는 다음 readonly 예외가 있다. `OpenReadOnly`는 `writeAt`, `syncFile`, `syncDir`, `remove` 훅을 초기화하지 않는다. 동일 데이터 복원은 쓰기 없이 성공할 수 있다. current 교체는 훅 대신 실제 OS 함수를 사용하는 원자 세그먼트 쓰기 경로이므로 권한과 I/O가 허용하면 readonly 객체에서도 파일을 교체하고 descriptor/offset/Used를 바꿀 수 있다. 반면 유효한 새 양수 index는 세그먼트 파일을 발행하고 목록/Used/current를 바꾼 뒤 manifest 임시 파일 생성과 chmod가 성공하면 nil `writeAt` 호출로 panic한다. 정상 오류 반환이나 롤백을 보장하지 않으며 발행한 세그먼트가 남는다. manifest 임시 파일의 defer 삭제는 실행되지만 panic 지점 전에 열린 파일의 close에는 도달하지 않는다.

manifest 없는 readonly 객체에는 세그먼트가 없다. 여기서 유효한 비어 있지 않은 data를 index=0으로 복원하면 `seg_000.log`를 설치한 뒤 manifest 인코딩이 index 0을 거부한다. 롤백이 목록을 빈 상태로 줄인 직후 마지막 원소를 current로 지정하려다 범위 초과 panic을 일으킨다. 따라서 Used 차감, descriptor close, 파일 삭제에 도달하지 못하며 설치된 파일이 남는다. 이 분기는 nil 훅 호출 전의 별도 실패 경로이다.

근거: [wal.go:187](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L187), [wal.go:911](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L911), [wal.go:976](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L976), [wal.go:990](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L990), [wal.go:1032](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L1032).

## Compaction의 선택 규칙과 수명

base는 Type=EntryCheckpoint이며 Slot>0이어야 한다. retainedValues의 각 value는 비어 있지 않고 SHA-256(value)==map key여야 한다. 어떤 proposal을 보존할지는 호출자 책임이며 QLog가 기존 로그에서 필요한 map을 계산하거나 완전성을 증명하지 않는다.

선택 결과는 (1) base, (2) hash 바이트 사전순으로 정렬한 retained proposal(각 Slot=0), (3) prefix의 기존 순서대로 Type!=EntryProposal이고 Slot>base.Slot인 모든 엔트리이다. 기존 proposal은 슬롯과 무관하게 모두 제외한다. 비proposal은 중복 제거하지 않는다. base payload의 인증이나 체크포인트 의미는 검사하지 않는다.

`EstimateCompactedBytes`는 같은 선택 규칙으로 각 Encode 길이를 합산하고 uint64 overflow를 거부한다. 입력 검증 오류는 0과 오류를 반환한다. 합산/스캔 중 오류는 그때까지 누적한 size와 오류를 반환하므로 0이 아닌 부분값일 수 있으며 완전한 추정치로 사용하면 안 된다. 세그먼트 목록만 읽기 잠금으로 복사한 뒤 스캔하므로 호출자가 이후 compaction까지 변경을 직렬화해야 정확한 preflight가 된다. readOnly/fatal 검사는 없다.

`BeginCompaction`은 retained value를 복사하지만 base.Payload를 별도로 복사하지 않는다. 호출자는 작업 동안 base payload를 변경하지 않아야 한다. compaction mutex와 WAL 잠금을 잡고 readOnly/fatal/current를 검사한다. current index가 MaxUint32-2보다 크면 고갈 오류이다. 기존 세그먼트를 prefix로 고정하고 current+1을 target, current+2를 tail로 예약한다. tail 생성과 manifest 발행을 완료한 뒤 WAL 잠금을 풀며 compaction mutex는 유지한다. 이후 Append는 독립 tail에 기록하므로 Build가 복사하지 않는다.

`Build`는 nil/done/built 상태를 거부하고 `.rhiza-compact-*`에 선택 결과를 쓴다. Begin에서 잡은 maxBytes로 compacted prefix만 검사한다. WriteAt 오류와 short write, sync, close, rename, 디렉터리 sync 실패를 반환한다. 전부 성공하면 offset과 built=true를 설정한다. 실패가 compaction mutex를 풀지는 않으므로 호출자는 Abort해야 한다. Build의 rename 후 sync 실패는 built=false로 남고 WAL fatal을 자동 설정하지 않는다. 이 파일은 아직 선택된 manifest의 참조가 아니다.

`Commit`은 built이고 미완료인 작업만 허용한다. target을 열고 WAL 잠금 아래 fatal/current를 확인한 뒤 current를 sync하고 dirty=false로 만든다. tailIndex 이상의 모든 세그먼트를 수집하고 마지막이 current인지 확인한다. compacted prefix+tail의 실제 합계가 현재 maxBytes를 넘으면 ErrCapacity이다. 새 manifest를 발행하고 성공하면 새 목록/Used로 전환한다. **manifest rename 후 디렉터리 sync 실패에서도 새 목록을 메모리에 채택하고 fatal을 유지한다.** 작업을 done으로 하고 compaction mutex를 풀며 이전 prefix descriptor를 닫는다. 이 실패에서는 이전 파일들을 삭제하지 않고 오류를 반환한다.

**S-02 — QLog-to-Core partial transition (static derivation; not executed).** After manifest rename succeeds but directory sync fails, QLog advances the generation, adopts the compacted segment list, retains fatal durability uncertainty, marks the compaction done, and returns the publication error. `Core.compactThroughOwned` returns that error before `installVerifiedBaseLocked`, so the live Core retains its previous floor and retained state while QLog selects the compacted generation. Deferred `Abort` is a no-op for this completed operation. Manifest selection, durability confirmation, and Core base installation are separate boundaries: failure does not establish that the old WAL remains selected. On reopen, the manifest selection/validation rules above apply to files actually present; directory-sync failure does not prove crash persistence of the new manifest. See the [Core compaction contract](quepaxa-go-durability-evidence-2026-10-30.md). Sources: [wal.go:581](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L581), [wal.go:1232](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L1232), [compaction.go:349](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/compaction.go#L349).

정상 Commit은 deferred이면 이전 prefix 파일을 남기고 pendingCleanup=true로 만든다. 일반 모드에서는 이전 prefix 삭제 및 디렉터리 sync 오류를 무시한다. 이 오류는 커밋 실패로 바꾸지 않으며 재열기 정리 대상으로 남는다. manifest 전환 전 Commit 실패는 작업을 종료하지 않으므로 Abort가 필요하다.

`Abort`는 nil/done이면 아무 작업도 하지 않는다. 그 외 done=true, target 삭제(오류 무시), compaction mutex 해제를 수행한다. 이미 발행한 tail과 그 manifest는 유지하므로 concurrent append는 되돌리지 않는다. Abort에는 디렉터리 sync가 없다. `Compact`는 Begin 후 Abort를 defer하고 Build와 Commit을 차례로 호출하는 편의 API이다. Compaction 객체의 메서드를 여러 goroutine에서 동시에 호출하는 계약은 제공하지 않는다.

근거: [wal.go:767](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L767), [wal.go:1081](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L1081), [wal.go:1128](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L1128), [wal.go:1180](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L1180), [wal.go:1274](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L1274).

## 테스트 하네스 계약

crash 하네스는 `after-compaction-build-synced-before-manifest`와 `after-manifest-switch-before-old-cleanup` 두 경계를 사용한다. 전자는 target rename과 디렉터리 sync 후 built 상태를 설정하기 전이고, 후자는 manifest 전환과 메모리 목록 변경 및 compaction mutex 해제 후 이전 descriptor/file 정리 전이다. 경계 함수는 먼저 `localtesthooks.Hit("qlog:"+name)`을 호출하고, 다음에 nil이 아닌 패키지 비공개 `localWALCrashBoundary(name)`을 호출한다. 두 경로는 별개이며 비공개 callback은 production에서 nil이다. `localtesthooks.Hit`는 기본 빌드에서 no-op이고 `rhiza_local_testhooks` 태그 빌드에서는 등록된 프로세스 로컬 callback을 호출한다. 기본 패키지 hard-kill 테스트는 태그 훅 대신 비공개 callback을 사용한다.

부모 테스트는 초기 로그를 sync/close한 뒤 `{Dir, Phase, ReadyFile}` JSON을 `RHIZA_LOCAL_WAL_CRASH` 환경변수로 전달하고 같은 테스트 바이너리를 `-test.run=^TestLocalCompactionHardKillHelper$`로 시작한다. 자식은 WAL을 열고 16 MiB 제한을 설정한 뒤 compaction을 시작하고 별도 tail receipt를 append/sync한다. 해당 Phase에 도달한 callback은 ReadyFile에 경계 이름을 0600으로 기록하고 대기한다. 부모는 marker 존재를 10ms 간격으로 최대 10초 기다린 다음 Process.Kill과 Wait를 호출하며 ExitError의 ExitCode=-1로 signal 종료를 확인한다. marker 검사 오류나 timeout에서도 Kill/Wait를 시도한다. 이후 재열기에서 manifest 전환 전이면 원래 기록과 durable tail, 전환 후이면 checkpoint/보존 proposal/상위 receipt/tail을 확인한다.

I/O fault 테스트는 객체의 비공개 `writeAt`, `syncFile`, `syncDir`, `remove` 훅을 교체하고 복구한다. Build의 부분 쓰기+ENOSPC, 오류 없는 short write, 파일 sync EIO, Commit의 active sync EIO, manifest rename 후 디렉터리 sync EIO, 정리 삭제/디렉터리 sync 실패를 주입한다. 이 훅들은 공개 설정 API가 아니며 실제 전원 손실이나 저장 장치 fault를 대체하지 않는다.

근거: [wal.go:53](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L53), [wal.go:1172](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L1172), [wal.go:1251](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L1251), [local_compaction_crash_test.go:14](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/local_compaction_crash_test.go#L14), [local_compaction_crash_test.go:156](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/local_compaction_crash_test.go#L156), [local_compaction_io_fault_test.go:12](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/local_compaction_io_fault_test.go#L12). 태그 훅의 최소 구분 근거: [hooks_disabled.go:126](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/localtesthooks/hooks_disabled.go#L126), [hooks_enabled.go:260](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/localtesthooks/hooks_enabled.go#L260).

## 종료와 검증 상태

`Close`는 WAL 잠금 아래 모든 세그먼트에 대해 mutable이면 Sync, 그 다음 Close를 시도하고 errors.Join으로 오류를 합친다. readOnly는 Sync를 생략한다. fatal을 검사하지 않고 closed 상태를 기록하거나 목록을 비우지 않으므로 반복 Close나 종료 후 호출의 성공을 보장하지 않는다. compaction mutex를 대신 정리하지도 않는다.

근거: [wal.go:1306](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L1306).

**테스트 실행 확인(별도 검증자 전달):** 지정 HEAD에서 `go test -mod=readonly -count=1 -timeout=120s ./pkg/qlog`가 exit 0으로 PASS했으며 소요 시간은 8.572초였다. 검증 세션 93558은 종료되었고 남은 qlog.test/go test 프로세스가 없음을 검증자가 확인했다. 개별 t.TempDir 삭제는 독립 측정하지 않았다. 작성자는 이 실행을 재수행하지 않았다.

소스에 존재하는 검증 항목은 append 불확실성, framing/CRC 손상 보존, tail truncate의 sync 필요성, readonly 불변성, identity 유지, concurrent compaction tail, manifest 전환 및 cleanup fault, subprocess hard-kill 복구 등이다. 근거: [append_failure_test.go:13](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/append_failure_test.go#L13), [tail_framing_test.go:15](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/tail_framing_test.go#L15), [local_compaction_io_fault_test.go:12](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/local_compaction_io_fault_test.go#L12), [local_compaction_crash_test.go:20](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/local_compaction_crash_test.go#L20), [qlog_test.go:991](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/qlog_test.go#L991).

**미검증:** race 실행, 실제 클라우드/세 제공자 계약, Chaos Mesh, 전원 손실, 파일시스템별 영속성, 성능 및 모든 입력의 정형 증명은 이번 실행으로 확립하지 않았다. subprocess hard-kill 테스트가 실제 전원 손실을 증명하지 않는다. [독립 리뷰](qlog-go-egm-review-2026-10-30.md)의 지적 사항을 수정했고 두 검토자가 해당 항목의 해소를 재확인했다. 이는 전체 명세의 무결점 판정이 아니다. 위 panic과 Finalize 간섭 설명은 코드 확인 결과이며 해당 분기를 새로 실행 검증했다는 주장은 하지 않는다.
