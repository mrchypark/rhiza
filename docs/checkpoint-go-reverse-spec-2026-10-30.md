# Go checkpoint 구현 명세

## 1. 범위와 근거

이 문서는 커밋 `008f8e576ad76425182e10a69b987f10cfb48b2a`의 `pkg/checkpoint` 운영 파일 7개를 기준으로 checkpoint 생성, 게시, 읽기, pin, GC, Local gate, 자동 실행 및 export의 관찰 가능한 동작을 정의한다. 저장 형식, 전체 공개 함수 시그니처와 JSON 태그는 [형식 부록](checkpoint-go-formats-2026-10-30.md)에 수록한다. 코드가 동작의 기준이다. 이 패키지는 합의 seal 결정, 외부 fencing 증거, 복구 승인, 실행 중 데이터베이스 설치를 자체 구현하지 않는다.

근거는 [checkpoint.go][core], [local.go][local], [local_gc.go][lgc], [local_gc_bucket.go][lb], [auto.go][auto], [export.go][export], [busy_observation.go][busy]이다. 외부 저장소의 실제 내구성·일관성이나 합의의 형식적 안전성을 이 문서의 조건부 쓰기 설명으로 보장하지 않는다.

## 2. manager와 저장소 의존성

`NewManager(bucket, prefix, localDir, configID ...uint)`는 I/O 없이 설정과 검증 root map을 만든다. config ID는 생략하면 0, 여러 값이면 첫 값이다. prefix는 정규화하지 않고 비어 있지 않으면 `prefix + "/" + 상대키`를 사용한다. `localDir`는 검증 블록 캐시 위치이며 snapshot 다운로드 디렉터리와는 별개다. `NewManagerFromEnv`는 `RHIZA_OBJSTORE_PREFIX`만 읽고 config ID는 0으로 둔다. [생성자][constructor], [환경 설정][env]

의존 객체는 `github.com/thanos-io/objstore`의 `Bucket`이다. 이 패키지가 소비하는 계약은 다음과 같다.

| 연산 | 소비하는 의미 |
|---|---|
| `Get(ctx, name) (io.ReadCloser, error)` | 본문 읽기와 Close 오류가 각각 발생할 수 있다. |
| `Attributes(ctx, name) (ObjectAttributes, error)` | `Version *ObjectVersion`, `LastModified`를 사용한다. version은 비교 가능하고 CAS에 전달할 수 있어야 한다. |
| `Upload(ctx, name, io.Reader, ...ObjectUploadOption) error` | `WithIfNotExists()`와 `WithIfMatch(version)`를 사용한다. |
| `SupportedObjectUploadOptions()` | 일반 claim 획득과 CURRENT 승격은 `IfNotExists`, `IfMatch` 모두를 요구한다. generation 획득에는 같은 선행 검사가 없다. |
| `IsConditionNotMetErr(error) bool`, `IsObjNotFoundErr(error) bool` | 조건 거절과 부재를 다른 오류와 구분한다. |
| `Iter(ctx, dir, func(string) error, ...IterOption) error` | root와 pin을 열거한다. Local 목록에는 recursive 옵션을 사용한다. |
| `IterWithAttributes(ctx, dir, func(IterObjectAttributes) error, ...IterOption) error` | HA 블록 sweep에서 `WithUpdatedAt`, `WithRecursiveIter`를 사용한다. 항목에 시간이 없으면 Attributes로 보충한다. |
| `Exists(ctx, name) (bool, error)`, `Delete(ctx, name) error` | Local root 존재 조회와 GC 삭제에 사용한다. |

위 표는 소비 연산의 형태를 설명한다. 타입의 정확한 전체 선언은 의존 라이브러리의 책임이다. 이 패키지는 provider SDK를 직접 선택하거나 endpoint를 설정하지 않는다. 버킷이 보고하는 조건부 쓰기 능력과 실제 오류 분류가 올바르게 동작해야 한다. [claim 능력 검사][acquire], [CURRENT 검사][promote], [Local wrapper][lb]

## 3. 입력·크기·정책

생성 source는 `Source{Role, Path string}`이며 한 개 또는 두 개다. 첫 항목은 `sqlite`, 두 번째가 있으면 `graph-data`여야 한다. 빈 경로, 중복 역할, 다른 역할을 거절한다. 각 파일은 1바이트 이상 32 GiB 이하여야 한다. 블록은 64 MiB 단위이며 마지막 블록만 더 작을 수 있다. 전체 파일 합계는 32 GiB 이하, 전체 블록 수는 512 이하, root JSON은 64 KiB 이하다. 한도를 넘는 사실을 업로드 이후 확인하는 경로가 있으므로 생성 오류가 객체 미생성을 뜻하지 않는다. [상수][limits], [source 검증][sources], [업로드][upload]

SQL execution policy는 7이다. HA 생성과 다운로드는 SQLite에 `sqlpolicy.CheckFile`, Local 생성은 `CheckSnapshotFile`을 호출한다. 정책 검사는 읽기 전용으로 `_rhiza_meta`의 `sql_execution_policy` 값이 `"7"`인지 확인하고 persistent virtual/shadow table을 거절한다. snapshot 검사는 `immutable=1`을 추가하여 source 옆 WAL/SHM 생성을 피한다. marker 부재·불일치와 조회 오류는 `sqlpolicy.ErrIncompatible`로 감싼다. [정책 구현][policy]

root 읽기 검증은 생성 source 검증과 다르다. 역할 중복을 거절하고 SQLite를 요구하지만 파일의 고정 역할 순서는 강제하지 않는다. 저장된 순서대로 상태 해시를 계산한다. 블록 generation은 0도 허용한다. [root 검증][validate]

## 4. HA 후보 생성과 CURRENT

`CreateFiles(ctx, claim, sources, index)`는 SQLite 정책 검사, index 비영 확인, 현재 lease 검사, CURRENT 갱신, source 검사 순으로 진행한다. 그 뒤 manager mutex를 보유하며 파일을 차례로 처리한다. 유효한 publisher 또는 generation claim이 필요하다. 이 생성 경로의 lease 검사는 purpose·generation·owner와 만료를 비교하지만 예약 index나 bound index를 비교하지 않는다. 예약 index와 실제 snapshot index의 관계를 확인하는 책임은 bind 및 상위 호출 경로에 있다. [생성][create], [lease 검사][lease]

중복 제거는 저장소에서 갱신한 certified CURRENT의 블록만을 기준으로 한다. 같은 hash와 size가 있으면 기존 generation을 재사용하고 본문 GET·업로드를 생략한다. 그 외에는 기본 hash 키에 IfNotExists 업로드를 시도한다. 조건 거절 시 기존 객체를 검증해서 채택하지 않고 현재 generation 키에 무조건 업로드한다. 조건부 생성 미지원 분기는 generation 키에 바로 업로드한다. 파일별 hash scan은 순차적이며 업로드는 최대 4개 작업을 병렬로 실행한다. 첫 오류는 context를 취소하고 worker들을 기다린 뒤 반환한다. [블록 처리][upload], [병렬 처리][parallel]

source 파일은 hash scan부터 업로드 완료까지 일관된 snapshot이어야 한다. 코드는 파일을 먼저 읽어 해시를 계산한 뒤 같은 파일의 section을 다시 읽어 업로드하며 그 사이 변경을 탐지하는 재검사를 하지 않는다. 로컬 scan 자체에는 명시적인 context 취소 검사가 없다.

파일 descriptor와 상태 해시를 만든 뒤 compact JSON을 직렬화하고 그 바이트의 SHA-256을 RootHash로 사용한다. root는 지원 시 IfNotExists로 업로드하며 조건 거절을 성공 경로로 받아들인다. HA 생성은 이때 실제 저장 root를 다시 읽지 않는다. 반환 root에 내부 claim을 연결하고 후보 게시 로그를 남긴다. `CURRENT`와 certified 메모리는 이 단계에서 갱신하지 않는다. 실패 시 앞서 업로드한 블록·root를 롤백하지 않는다. [생성][create]

호출 순서는 **후보 생성 → 외부 seal 인증/결정 → CURRENT 승격**이다. `PromoteCertifiedCurrent` 자체는 합의 결정이나 인증서를 확인하지 않는다. root에 내부 claim이 있을 때만 lease를 검사하며 claim 없는 root도 받을 수 있다. OpenRoot는 전달된 Index와 RootHash가 가리키는 canonical 저장 객체를 검증하지만 반환 descriptor는 버린다. 전달된 root의 Hash·Files·Size·Generation·정책 등 나머지 필드가 저장 descriptor와 같은지는 비교하지 않으며 블록 본문도 검사하지 않는다. upload 성공 또는 동일 CURRENT pointer 관찰 시 certified 메모리에 기억하는 값은 호출자가 전달한 descriptor의 얕은 복사다. 따라서 Latest와 후속 dedup이 전달된 값을 사용할 수 있다. 외부 호출자는 올바른 seal 결정과 정확한 descriptor를 제공하고 공유되는 Files·Blocks slice를 변경하지 않아야 한다. [승격][promote]

CURRENT 승격은 최대 4회 수행한다. 부재면 IfNotExists를 사용하고, 존재하면 Attributes의 version과 GET한 pointer를 사용하여 IfMatch한다. version이 없으면 오류다. 현재 index가 후보보다 높으면 그 root를 읽어 certified로 기억하고 `ErrStaleCheckpoint`로 감싼 오류를 반환한다. 같은 index에 다른 hash이면 오류다. 같은 index·hash이면 재업로드 없이 성공한다. 낮은 index면 CAS한다. 조건 거절만 재시도하며 다른 upload 오류는 반환한다. 네 번 충돌하면 `concurrent writers did not converge` 오류다. 성공은 upload ACK 또는 동일 pointer 관찰이며 별도의 성공 후 readback은 없다. [승격][promote]

`Load`는 CURRENT GET와 가리킨 root GET를 한다. CURRENT 부재는 certified를 nil로 만들고 성공하지만 기존 checkpoint 목록을 지우지는 않는다. 참조 root 부재는 오류다. 성공하면 목록을 해당 root 하나로 바꾸고 certified를 기억한다. `refreshCertifiedCurrent`는 이미 아는 동일 pointer이면 root GET를 생략한다. `Latest`는 메모리의 certified 구조체를 얕게 복사해 반환한다. slice까지 독립 복사하지 않으므로 반환 descriptor를 변경하지 않는 것이 호출자 계약이다. [Load][load], [refresh][refresh], [Latest][latest]

## 5. PUBLISHER 상태와 실패

모든 claim은 하나의 `checkpoint/PUBLISHER` 객체를 공유한다. lease는 Unix millisecond 기준이며 `LeaseUntilMS > now`일 때 활성이다. lease 시간은 로컬 wall clock을 사용하며 clock skew를 보정하지 않는다. [claim 획득][acquire]

| purpose | 예약 및 재획득 |
|---|---|
| `publisher` | 최초 generation 1. 기존 만료 claim이 있으면 generation+1, 예약 index=`max(minExclusive, 이전 ReservedIndex, 이전 BoundIndex)+1`. generation 또는 index uint64 소진은 오류다. |
| `maintenance` | 같은 claim 객체를 사용하지만 예약 index는 0이다. 만료된 이전 claim의 예약·bound 값을 새 maintenance record에 보존하지 않는다. 일반 publisher 호출자는 외부 floor를 올바르게 공급해야 한다. |
| `generation` | 지정 index를 정확히 예약한다. CURRENT를 먼저 갱신한다. 기존 claim은 같은 purpose·owner·ReservedIndex를 가지며 BoundIndex가 요청 index보다 크지 않아야 한다. CURRENT가 있다면 CURRENT index와 기존 bound index 모두 요청 index여야 한다. 부재 claim과 기존 CURRENT 조합은 fenced다. |

획득은 owner 비영 및 lease 양수를 요구하며 generation 획득은 index 비영도 요구한다. 활성 claim은 busy다. 일반 acquire에서 maintenance의 첫 시도가 활성 claim을 확인하면 `ErrRecoveryAdmissionBusy`를 반환한다. 이는 `ErrPublisherBusy`를 감싼 값이며 다른 read/CAS 실패에 확대 적용하지 않는다. 조건 충돌은 최대 4회 시도하고 소진하면 busy다. generation claim의 identity 거절은 busy 검사보다 먼저 수행한다. [일반 획득][acquire], [generation 획득][generation]

lease 객체는 `Attributes → Get → 제한 읽기 → Close → Attributes`로 읽는다. 앞뒤 version이 모두 존재하고 동일해야 한다. version이 없거나 바뀌면 최대 4회 반복한다. 본문 초과, 읽기·Close·Attributes 오류는 즉시 반환한다. 네 번 불안정하면 `checkpoint lease object did not stabilize`다. PUBLISHER 본문은 64 KiB, pin은 64 KiB+1024, Local control은 4096바이트 이하다. [안정 읽기][stable]

획득·bind·renew는 upload 뒤 안정 읽기로 confirmation한다. version을 제외한 claim 전체와 기대 record가 정확히 같고 lease가 여전히 미래여야 한다. 부재 또는 불일치는 fenced다. 일반 저장소 오류는 원래 오류를 반환하며 성공 여부가 불명확한 mutation을 자동 성공·재획득으로 바꾸지 않는다. [confirmation][confirm]

`BindPublisherClaim`은 config·reserved·purpose·generation·owner와 현재 유효 lease를 비교한다. publisher는 `index >= ReservedIndex`, generation은 정확한 일치를 요구한다. 현재 bound/root와 전달 handle의 이전 bound/root를 비교하지 않고 새 index/root로 바인딩하며 lease를 연장한다. 양수가 아닌 lease와 조건 거절은 fenced다. `RenewPublisherClaim`은 이 identity에 현재 bound index와 root hash의 일치까지 요구한다. [bind][bind], [renew][renew]

`ValidatePublisherClaim`은 publisher purpose만 받으며 manager config, owner, bound index, root hash, 유효 lease를 확인한다. generation claim을 승인하지 않는다. `ReleasePublisherClaim`은 purpose·generation·owner만 비교하고 현재 record의 lease를 현재 시각으로 바꾸어 CAS한다. 만료된 같은 identity도 release할 수 있다. 객체를 삭제하지 않는다. 조건 거절은 fenced, 그 밖의 오류는 그대로 반환하며 exact readback은 없다. [validate claim][claimvalidate], [release][release]

## 6. 읽기·다운로드·검증 캐시

`OpenRoot(index, rootHash)`는 canonical key에서 최대 65537바이트를 읽고 65536 초과, SHA-256 불일치, schema·정책·index 불일치를 거절한다. root reader Close 오류는 이 경로에서 반환하지 않는다. JSON은 알 수 없는 필드와 두 번째 JSON 값을 거절한다. 필드 누락은 Go zero value로 decoding되며 이후 개별 validation에 따라 거절 여부가 정해진다. [root 읽기][readroot], [JSON][json]

`DownloadRootFiles`는 OpenRoot 후 `DownloadAndVerifyRootFiles`를 호출한다. 후자는 이미 열린 root를 받는 계약으로 descriptor 구조를 검증하지만 root JSON을 다시 hash하여 RootHash와 비교하지 않는다. 출력 디렉터리는 호출자가 미리 준비한다. `sqlite.db`, `graph-data.db`를 `O_CREATE|O_EXCL|O_RDWR`, 0600으로 만들어 기존 파일을 덮어쓰지 않는다. 파일을 descriptor 크기로 truncate하고 최대 4개 블록을 병렬 GET하여 offset에 쓴다. 각 본문은 크기+1까지만 읽고 정확한 크기, SHA-256, read·Close 성공을 확인한다. 파일 Sync와 Close를 완료해야 성공이다. [다운로드][download]

실패한 파일은 제거를 시도하며 앞서 완성한 파일도 다운로드 오류 시 제거를 시도한다. 이후 SQL 정책 검사 또는 캐시 저장 오류에는 완성된 파일을 제거하지 않는다. 다운로드 성공은 실행 중 materializer 설치나 원자적 전환을 뜻하지 않는다. 설치와 pin 수명, 실패 파일 정리는 호출자 책임이다.

`Verify(index, rootHash, state)`는 메모리의 verified root identity가 같으면 I/O 없이 성공한다. 그 외에는 OpenRoot와 state 비교 후 검증 블록 캐시에 없는 키·크기만 본문 검증한다. 캐시는 물리 object key→size이며 원격 현재 존재 여부를 매번 확인하지 않는다. localDir가 비어 있으면 메모리만 사용한다. 디스크 캐시는 엄격 JSON으로 읽고 키 형식과 1..64 MiB 크기를 검사한다. 저장은 localDir 내 임시 파일, chmod 0600, write, file Sync, Close, rename, directory Sync/Close 순서다. 임시 파일 제거 오류는 무시한다. 다운로드와 Verify에서 캐시 저장을 시도하는 경로는 저장 오류를 반환하지만, 저장 전에 추가한 메모리 블록 항목을 롤백하지 않는다. 같은 manager의 후속 Verify는 이 항목을 재사용할 수 있다. pending 블록이 없으면 캐시 저장을 다시 시도하지 않고 verified root를 기억한 뒤 성공한다. 따라서 이전 저장 오류 이후 Verify 성공이 디스크 캐시 저장 성공을 뜻하지 않는다. [Verify][verify], [다운로드][download], [캐시][cache]

## 7. Recovery pin과 HA GC

`PinRecoveryRoot`는 root·owner·양수 lease를 요구하고 root descriptor를 직렬화한 hash가 RootHash와 정확히 같은지 검사한다. 16바이트 난수 token을 생성한다. maintenance claim을 획득한 안에서 canonical root Attributes로 존재를 확인하고 owner SHA-256 키에 descriptor를 포함한 pin을 조건부 게시한다. 활성 pin은 busy이며 만료 pin은 현재 version으로 교체한다. 최대 4번 조건 충돌을 시도한다. exact confirmation과 maintenance release 뒤에도 pin lease가 미래인지 확인한다. canonical root의 블록 검증은 선행 호출자 책임이다. [pin 생성][pin]

pin `Renew`는 config·owner·token·index·root hash와 현재 유효 lease를 검사하고 lease를 늘려 CAS한다. upload 후 exact readback은 없다. `Close`는 nil handle 또는 부재 객체이면 성공한다. 같은 identity를 요구하지만 아직 활성인지는 요구하지 않으며 현재 시각 lease로 CAS한다. 두 연산의 조건 거절은 fenced다. `Root()`는 pin에 캡처된 descriptor를 검증해 반환하므로 canonical root가 사라져도 descriptor를 얻을 수 있다. 단, 블록 존재를 보장하지는 않는다. [pin 생명주기][pinlife]

pin scan은 lease 0을 무시한다. 만료 pin은 lease 0으로 CAS하여 tombstone으로 남기며 충돌이면 busy다. 유효 pin의 캡처 descriptor를 root 보존 집합에 넣는다. `LocalRecoveryPinDeadline`은 모든 pin의 identity를 다시 확인해 가장 이른 deadline을 반환하고 이미 만료되었으면 fenced다. 빈 목록은 zero time이다. 갱신하지 않으며 다른 admission RPC 이후 반환 deadline을 다시 현재 시각과 비교해야 한다. [scan][pinscan], [deadline][deadline]

`GarbageCollectFrom(retain, keep, floor, grace)`는 floor 0을 `ErrAuthoritativeRecoveryBaseRequired`로, keep<1 또는 음수 grace를 정책 오류로 거절한다. 호출자는 검증·게시된 외부 archive RecoveryBase의 floor와 retain root를 제공해야 한다. 함수는 그 외부 권위를 조회하거나 retain에 실제 base가 있는지 증명하지 않는다. [HA GC 진입][gc]

maintenance claim owner는 `checkpoint-gc`, lease는 2분이다. 40초마다 갱신하고 실패 시 work context를 취소한다. 종료 시 background 5초 context로 release한다. work 오류가 우선하며, work 성공 시 fenced 이외 release 오류를 반환한다. 그 뒤 저장된 renew 오류가 있으면 반환한다. pin 등록과 HA sweep은 같은 maintenance claim으로 직렬화한다. [maintenance 실행][maintenance]

HA sweep은 active pin scan, Load, 전체 root 열거 순이다. root를 index 오름차순, 동률이면 hash byte 오름차순으로 정렬한다. retain, active pin, CURRENT, 마지막 keep개 root를 보존하고 index>=floor 또는 수정 시각이 cutoff 이후인 root도 보존한다. 나머지는 삭제 직전 현재 maintenance purpose·generation·owner·유효 lease를 확인하고 root를 삭제한다. 부재 삭제는 허용한다. root 목록을 갱신한 다음 블록 sweep을 한다. [HA sweep][sweep]

블록 live 집합에는 **보존 root뿐 아니라 이번 sweep에서 제거한 root의 블록도 포함**한다. 따라서 같은 sweep에서 root와 그 블록을 함께 제거하지 않는다. pin descriptor 블록도 live다. verified cache는 보존 root와 pin 블록만 남기도록 줄인다. 블록 목록 중 parse 가능한 키에서 live가 아니고 cutoff 이후가 아닌 객체를 삭제 직전 claim 확인 후 삭제한다. 알 수 없는 블록 이름은 건너뛴다. HA에서는 cutoff와 같은 시각도 삭제 가능하고 zero LastModified를 별도 보호하지 않는다. 중간 오류 시 이미 끝난 삭제는 유지된다. [HA sweep][sweep]

## 8. Local 경로

`CreateLocalFiles`는 `WithLocalMaintenanceContext` 안에서 LocalBucket을 쓰는 worker manager로 생성한다. index·source를 검증하고 SQLite snapshot 정책을 검사한다. root generation과 새 generation 블록은 1을 사용하며 known CURRENT dedup은 없다. 기본 키 IfNotExists 거절 시 generation 1 키로 업로드하는 공통 블록 규칙을 따른다. root 업로드도 IfNotExists를 쓰고 거절을 허용하되 실제 저장 root를 읽어 hash와 descriptor를 검증한다. CURRENT, certified 메모리, voter claim은 게시하지 않는다. 외부적으로 독점적인 Local writer가 전제다. [Local 생성][local]

**S-01 — conditional retry weakness (static derivation; not executed).** Recreating the same Local checkpoint does not guarantee the same portable identity. With initially absent block keys, the first upload can use generation 0; a retry encounters those keys and uses generation 1. Block generation participates in `fileDescriptorHash`, then the state hash and portable root hash. The Local seal identity can remain unchanged while the portable identity changes. If immutable archive snapshot metadata was published before interruption and compaction has not completed, prepared-checkpoint resumption publishes again and can fail with `Local snapshot publication mismatch`. Reopening does not remove the stored blocks or immutable metadata, so the same conflict can persist across reopen. This is current behavior, not a guaranteed idempotent retry. See the [archive publication contract](archive-recovery-go-reverse-spec-2026-10-30.md) and [snapshot format contract](archive-recovery-go-formats-2026-10-30.md). Sources: [local.go:43](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/local.go#L43), [checkpoint.go:784](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L784), [checkpoint.go:1630](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/checkpoint/checkpoint.go#L1630), [local_backup.go:550](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L550), [local.go:709](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L709), [node.go:1349](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L1349).

Local gate는 `local-gc-control`의 token 비영이면 held, 빈 문자열이면 free다. lease 자동 만료가 없다. format version은 2이며 location/binding hash를 manager 설정과 비교한다. binding이 비어 있지 않은데 객체가 없으면 오류이며 bootstrap하지 않는다. unbound 부재만 IfNotExists로 만들 수 있다. token은 난수 16바이트의 hex다. free에서 held로 CAS하고 저장 bytes를 정확히 읽어 확인한 후 callback을 실행한다. [Local gate][lgc]

바깥 재시도 loop는 획득 경쟁뿐 아니라 한 번의 시도에서 반환한 모든 오류를 `errors.Is(err, ErrLocalMaintenanceBusy)`로 분류한다. 해당 오류이면 10ms 뒤 재시도하며 5초 대기 타이머 또는 요청 취소 시 종료한다. callback이 gate 해제 후 반환되는 busy 오류를 만들면 gate를 다시 획득하고 callback을 재실행할 수 있다. 이미 ACK된 부수 효과를 롤백하지 않는다. 이 타이머는 획득 시도나 callback 실행을 중단하는 timeout이 아니다. 각 획득 시도에 들어가기 전에는 요청 취소를 검사하지만, 시도가 시작되면 acquisition admission의 request는 `context.WithoutCancel(ctx)`이므로 이후 읽기·held token 업로드·confirmation은 원래 요청 취소와 무관하게 계속될 수 있다. confirmation 이후 취소가 관찰되면 callback을 생략하고 gate 해제를 진행한다. callback context는 원래 취소 신호를 제거하되 admission 상태의 request에는 원래 요청을 보관한다. [gate 실행][gate]

callback의 storage 작업은 반드시 `LocalBucket`으로 감싼 bucket과 전달받은 callback context를 함께 사용해야 한다. WithLocalMaintenanceContext는 gate 자체 manager만 감싸며 호출자의 manager bucket을 제자리에서 바꾸지 않는다. raw bucket 호출은 이 wrapper의 RPC admission·timeout·mutation uncertainty 추적을 우회한다. callback은 모든 storage 작업의 완료를 기다려야 한다. mutation uncertainty가 있으면 gate를 held로 남기고 오류를 반환한다. 자동 takeover는 없다. 정상 해제는 획득 당시 held version으로 free bytes를 CAS한다. release upload 오류 시 background readback으로 exact free bytes와 held version과 다른 nonnil version을 확인한 경우만 성공으로 인정한다. readback 및 release는 각각 5초 예산이다. work 오류가 있으면 해제 후 반환하고 그 외에는 원래 request context 오류를 반환한다. [gate 실행][gate], [RPC wrapper][lb]

`LocalBucket`은 중복 래핑하지 않으며 gate context가 없는 일반 reader·HA 호출의 동작을 바꾸지 않는다. 래핑은 gate 획득 증명이 아니다. callback 작업의 admission에서는 원래 요청 취소가 다음 RPC dispatch를 막고 이미 dispatch한 RPC는 취소를 분리한 30초 timeout으로 진행한다. 획득 단계의 admission은 위에서 설명한 취소 분리 규칙을 따른다. mutation의 일반 오류 또는 timeout은 uncertainty를 설정한다. Upload 조건 거절과 Delete 부재는 변경 없음이 확인된 ACK로 분류한다. timeout은 underlying operation이나 goroutine 종료를 증명하지 않는다. `LocalBeforeDispatch`는 dispatch 전에 거절된 오류만 식별한다. [RPC wrapper][lb]

`Iter`는 먼저 이름을 모은 뒤 callback을 동기 실행하고 각 callback 전에 원래 요청 취소를 확인한다. Get은 본문 Close까지 RPC context를 유지하고 늦게 도착한 reader도 닫기를 시도한다. Read는 독립 버퍼를 사용한다. Close는 RPC를 취소한 뒤 최대 30초 기다리며 readback context이면 공유 예산 만료도 따른다. `LocalReadbackContext`는 gate 상태가 없으면 원래 context와 no-op cancel을 반환하고, 있으면 원래 취소와 분리된 30초 읽기 예산을 준다. mutation dispatch는 여전히 원래 request 취소 조건을 따른다. [stream 및 readback][lb]

`RecoverLocalMaintenanceControl`은 nonempty binding, held filesystem owner, prefix에 대한 location 일치를 요구한다. control을 검증하고 held이면 free로 CAS한 뒤 exact readback한다. writer 종료, 재생성 방지, outstanding storage 작업 해소는 이 함수가 증명하지 않으며 호출자의 사전 검증이다. `LocalMaintenanceResolved`는 unbound면 I/O 없이 true, bound면 control을 검증해 token이 빈지 반환한다. `LocalRootExists`는 canonical root의 Exists만 수행한다. `LocalRecoveryActive`는 active pin scan 결과가 비어 있지 않은지 반환하며 만료 pin tombstone 쓰기가 발생할 수 있다. [Local 보조 API][lgc]

`GarbageCollectLocal`은 gate를 직접 획득하지 않는다. 호출자가 gate 안에서 실행해야 한다. 음수 grace는 오류다. 먼저 recursive root 목록의 모든 키가 canonical인지 검증하며 알 수 없는 root는 오류다. pin과 전체 root를 읽고, retired에 명시되어 있고 retain·pin에 없으며 수정 시각이 0이 아니고 cutoff보다 엄격히 이전인 root만 삭제 후보로 둔다. prepared root를 자동으로 retired로 간주하지 않는다. [Local GC][localgc]

보존 root와 pin의 블록을 live로 표시하고 recursive block 목록을 끝까지 검사·수집한다. canonical이며 live가 아니고 nonzero 수정 시각이 cutoff보다 이전인 블록만 후보로 둔다. 비정규 block 이름은 건너뛴다. 목록과 후보 수집이 성공한 뒤 root부터, 그다음 블록을 삭제한다. HA와 달리 이번에 제거할 root만 참조하던 블록도 같은 호출에서 삭제할 수 있다. 부재 삭제는 허용한다. 트랜잭션이나 롤백이 없으므로 중간 실패는 부분 삭제를 남길 수 있다. 이 경로는 HA verified-cache pruning을 하지 않는다. [Local GC][localgc]

## 9. 자동 checkpoint와 export

`AutoCheckpointer`의 설정 함수는 값을 그대로 보관한다. interval 또는 duration이 0 이하이면 Start는 시작하지 않는다. ticker 주기는 min(duration, 1분)이다. 이미 시작했거나 Stop한 객체는 다시 시작하지 않는다. 성공 index는 초기 Latest에서, 성공 시각은 시작 시각에서 출발한다. tick에서 duration 경과 또는 양수 tailBudget 도달, eligible 승인, tip 비영, 마지막 index보다 증가 및 interval 이상 진행을 모두 만족해야 생성한다. `before` callback이 실패하면 로그를 남기고 다음 tick을 기다린다. 생성 실패도 로그만 남기고 성공 기준은 갱신하지 않는다. [자동 loop][auto]

생성은 materializer Tip과 Latest로 생략 여부를 먼저 판단한다. owner 설정 시 floor callback 또는 0으로 2분 publisher claim을 얻고 선택적 advance(ReservedIndex), materializer.CheckpointFilesAt, CreateFiles, reserved index 하한 확인, bind 순으로 진행한다. snapshot cleanup은 defer한다. snapshot의 실제 index가 결과 index다. owner 미설정 경로도 CreateFiles를 호출하므로 이 패키지의 lease 검사를 우회하는 Local 경로가 아니다. [자동 생성][autocreate]

후보를 저장한 뒤 publish callback을 호출한다. callback이 없으면 오류다. callback 후 Latest의 index가 후보 이상이고 root hash가 후보와 같아야 한다. 일반 publish 오류는 후보를 남기지만 후속 호출의 재시도는 맨 앞의 `Latest.Index >= material.Tip()` 생략 분기를 통과한 경우에만 진행한다. 이 분기가 성립하면 기존 후보를 처리·release·초기화하지 않고 Latest.Index를 반환한다. 예를 들어 callback이 승격에 성공한 뒤 오류를 반환하면 다음 호출에서 이 생략 경로를 탈 수 있다. 후보 처리에 진입하여 fenced이면 후보를 버리고 생성 경로를 다시 호출한다. stale이면 후보를 버리고 Latest가 있으면 그 index로 성공한다. 성공 뒤 release의 fenced 오류는 무시하지만 다른 release 오류는 반환한다. 새 작업 실패의 defer release는 background 5초로 시도하며 오류를 무시한다. [후보 처리][autocreate]

생성 및 publish 작업 중 lease는 40초마다 갱신한다. 실패하면 작업 context를 취소하고 작업 오류가 없거나 context.Canceled이면 renew 오류를 우선 반환한다. Stop은 loop context와 stop channel을 취소하고 goroutine을 join한다. callback 및 provider가 종료에 협조하지 않으면 즉시 종료를 보장하지 않는다. `CheckpointOnShutdown(tip)`은 tip 0 또는 Latest>=tip이면 생략하고 그 외에는 create한다. eligible·interval·duration·before 검사를 하지 않으며 tip 인자가 snapshot index를 지정하지 않는다. [갱신][autorenew], [종료][shutdown]

`CopyRoot`는 같은 bucket의 sourcePrefix에서 targetPrefix로 복사한다. source root를 다시 열고 상태 hash를 비교한다. 각 블록을 순차 복사한 뒤 root를 마지막에 복사한다. CURRENT·claim·pin·gate는 복사하지 않는다. 각 객체는 OS 임시 파일에 최대 size+1바이트를 받아 SHA-256과 상한을 검사하고 대상에 IfNotExists 업로드한다. 대상 충돌도 허용하지만 대상 GET으로 다시 hash와 상한을 검사한다. 블록 상한은 descriptor size, root 상한은 64 KiB이며 구현은 `read > size`를 거절하고 `read == size`를 별도 요구하지 않는다. 앞서 복사한 대상 객체는 후속 실패 시 남는다. 임시 파일은 defer로 close·remove하며 cleanup 오류는 무시한다. [export][export]

export 중 source GC 보호, 대상의 seal·활성화·CURRENT 승격은 호출자 책임이다. export 성공이 게시 또는 복구 완료를 뜻하지 않는다.

## 10. 진단과 검증 수준

`WithPublisherBusyObserver`의 nil observer는 no-op이다. observer는 context별 opt-in이며 동기 호출된다. 기본 경로는 이벤트를 보관하지 않는다. busy 이벤트는 획득 source·요청 purpose·attempt·활성 holder identity·남은 lease를 담는다. owner/purpose는 최대 128바이트의 유효 UTF-8로 제한하고 잘못된 UTF-8은 replacement rune으로 바꾼다. 초과·변환 여부를 truncated 필드로 표시한다. [진단][busy]

maintenance lifecycle source는 `acquire_upload_ack`, `acquire_confirmed`, `renew_confirmed`, `release_upload_ack`, `release_error`, `release_fenced`다. lifecycle 이벤트는 purpose·generation·holder category·관찰 시각·남은 lease와 identity digest를 담으며 일반 busy의 모든 필드를 채우지는 않는다. category는 startup/node_catchup/checkpoint_gc만 인정하고 나머지는 source_context_unknown이다. namespace digest는 raw prefix bytes의 SHA-256, version digest는 decimal type + `:` + raw value의 SHA-256이다. 일반 acquire 활성 busy에는 observer가 있을 때 namespace/version digest와 시각을 추가하지만 generation 활성 busy 경로에는 이 추가 단계가 없다. 이벤트는 진단 정보이며 release·fencing의 추가 승인 조건이 아니다. [진단 구현][busy]

검증 결과는 다음처럼 구분한다.

| 근거 종류 | 상태와 범위 |
|---|---|
| 구현 근거 | 지정 HEAD의 운영 코드에서 위 계약을 확인했다. |
| 전달받은 로컬 테스트 결과 | 같은 HEAD에서 `go test -mod=readonly -count=1 -timeout=180s ./pkg/checkpoint` 종료 코드 0, `ok github.com/mrchypark/rhiza/pkg/checkpoint 27.725s`라는 검증 결과가 전달되었다. 이 문서 작성 과정에서 다시 실행하지 않았다. |
| 독립 문서 검토 | 두 Goldfish의 지적을 소스와 대조하여 수정했고 각 검토자가 해당 항목의 해소를 재확인했다. 원문·재점검·범위 제한은 [checkpoint Go 검토 기록](checkpoint-go-egm-review-2026-10-30.md)을 참조한다. 전체 명세의 무결점 판정이나 독립 재구현 인증은 아니다. |
| 미검증 | 외부 cloud, 3-provider emulator 계약, 실제 chaos, 성능, 전체 recovery/operator 통합 및 형식적 증명은 위 로컬 테스트 결과로 검증되지 않는다. |


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
