# Object-store Go 어댑터 역명세

기준은 rhiza 커밋 `008f8e576ad76425182e10a69b987f10cfb48b2a`이다. 이 문서는 `internal/objstore`의 실제 구현을 재현하기 위한 명세이며, [계약 부록](object-store-go-contracts-2026-10-30.md)과 함께 읽는다. 코드가 권위 있는 근거다. 아래 동작은 SDK 내부 구현이나 실제 클라우드의 원자성·내구성·일관성을 보장하지 않는다.

## 1. 범위와 의존 경계

대상 production 파일은 `provider.go`, `metrics.go`, `mutation_drain.go`, `local_owner.go`, OS별 `local_owner_{unix,linux,darwin,unsupported}.go`, 빌드 태그별 `transport_observer_{enabled,disabled}.go`이다. 하위 `versityfixture/fixture.go`는 일반 애플리케이션 경로가 아닌 opt-in 측정 지원 코드로 별도 기술한다. 테스트 파일은 구현 계약의 권위로 삼지 않는다.

어댑터는 provider를 생성하고, Bucket 인터페이스를 내장한 `MeteredBucket`을 반환하며, 논리 호출·HTTP 호출을 계측한다. 소유권 래퍼와 terminal mutation drain은 서로 다른 수명 관리 기능이다. 객체 이름, 업로드 내용, range, 반복 옵션은 의존 Bucket에 전달한다. `Config.Prefix`는 이 패키지에서 객체 이름에 적용하지 않는다. 객체 내용의 JSON/바이너리 형식이나 실제 S3/GCS/Azure wire schema도 이 패키지에서 정의하지 않는다. 의존 모듈은 `github.com/thanos-io/objstore v0.0.0-20260817070452-e42d91c337ee`이다. [provider.go:90](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/provider.go#L90), [metrics.go:268](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L268), [go.mod:14](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/go.mod#L14)

## 2. 설정 검증과 provider 생성

지원 문자열은 대소문자를 변환하지 않은 `filesystem`, `s3`, `gcs`, `azure`이다. `LoadConfig()`만 빈 provider를 filesystem으로 바꾼다. 직접 `NewBucket(Config{})`를 호출하면 unsupported provider 오류가 난다. 검증은 provider 생성 전에 수행하며, 아래 검증 외의 인증 조합·음수 재시도 등의 검증은 이 어댑터가 추가하지 않는다. [provider.go:20](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/provider.go#L20), [provider.go:138](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/provider.go#L138)

| Provider | 어댑터 검증 | 생성 시 전달·변경 |
|---|---|---|
| filesystem | 추가 검증 없이 통과한다. | 빈 FilesystemDir을 `./objstore`로 바꾸어 `filesystem.NewBucket`에 전달한다. 다른 필드를 적용하지 않는다. |
| s3 | Bucket이 필수다. | Bucket, Endpoint, Region, Insecure, AccessKey, SecretKey, SessionToken, MaxRetries를 전달한다. `AWSSDKAuth = (AccessKey == "")`이다. |
| gcs | Bucket이 필수다. Endpoint가 비어 있지 않거나 Insecure이면 거부한다. | `gcs.DefaultConfig`에서 Bucket, ServiceAccount, MaxRetries를 덮어쓴다. `context.Background()`로 생성한다. |
| azure | Bucket과 AzureStorageAccount가 필수다. 연결 문자열이 있어도 동일하다. Insecure는 거부한다. | `azure.DefaultConfig`에서 tenant/client/secret/account/key/connection/container/user-assigned ID/retries를 덮어쓴다. Endpoint는 비어 있지 않을 때만 덮어쓴다. supervised 생성은 StorageCreateContainer를 false로 바꾼다. |

모든 원격 생성에는 nop logger와 component 문자열 `rhiza`를 전달한다. SDK 생성 오류는 그대로 반환한다. Azure 일반 생성은 `StorageCreateContainer`의 의존 기본값을 유지한다. supervised 생성의 명시적 차이는 Azure 자동 컨테이너 생성을 끄는 것이며, 어댑터가 모든 provider의 부작용을 포괄적으로 차단하는 별도 계층은 없다. provider 기본값의 숫자나 SDK의 최종 retry 횟수는 여기서 새로 정의하지 않는다. [provider.go:97](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/provider.go#L97), [provider.go:166](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/provider.go#L166)

`NewBucket`은 replay grouping을 끈다. `NewBucketWithContext`와 `NewBucketForSupervisedStartup`은 ctx에서 `WithReplayObservation` 표식만 읽는다. nil 생성 ctx는 허용하며 표식이 없는 것으로 본다. 생성 ctx의 취소·기한은 provider 생성에 전달하지 않는다. 표식이 있어도 filesystem에서는 grouping을 끈다. 각 Bucket 연산의 ctx는 해당 연산과 transport에 전달한다. 연산용 nil ctx를 허용하는 별도 계약은 없다. [provider.go:54](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/provider.go#L54)

환경변수·JSON 필드·오류 문자열의 전체 대응은 부록에 있다. `LoadConfig`는 AccessKey, SecretKey, SessionToken, MaxRetries를 설정하지 않으므로 각각 빈 문자열·0이다. 이 사실만으로 SDK의 ambient 인증이나 retry 동작을 확정할 수 없다.

## 3. Bucket 연산, 조건부 쓰기와 reader

`MeteredBucket`은 Upload/Delete/Get/GetRange/Iter/IterWithAttributes/Exists/Attributes를 재정의한다. 그 밖의 인터페이스 메서드와 capability 조회·오류 판별·Close는 내장 Bucket으로 전달한다. 따라서 `SupportedObjectUploadOptions`는 provider가 보고하는 능력이며 어댑터가 모든 provider에 동일한 CAS를 구현하는 것은 아니다. 내장된 공개 `Bucket` 필드를 직접 호출하면 바깥 MeteredBucket의 논리 계층을 우회할 수 있다. 소유권·계측 보장은 올바른 래핑 경로를 사용한다는 조건에 한정된다. [metrics.go:268](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L268)

Upload 옵션을 의존 함수로 적용한 결과 IfNotExists가 true이거나 Condition이 nil이 아니면 내부 expected-condition 문맥 표식을 넣는다. 옵션은 변경하지 않고 그대로 전달하며 HTTP 조건 헤더를 이 래퍼가 직접 생성하지 않는다. 제공자가 `IsConditionNotMetErr`로 분류한 오류는 이름에 `/blocks/`, `/extents/`, `/roots/` 중 하나가 있으면 DedupHits, 아니면 ConditionConflicts를 증가시킨다. 이는 이름 부분 문자열에 따른 계수이며 내용 일치 검증이 아니다. 조건부 문맥에서 `minio.ErrorResponse`가 status 409, Code `ConditionalRequestConflict`이면 mutation의 논리 결과를 known으로 처리하지만 오류는 그대로 반환한다. 이 특례만으로 dedup 계수를 올리지 않는다. [metrics.go:255](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L255), [metrics.go:306](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L306)

GetRange는 off와 length를 그대로 전달한다. 최대 읽기 크기 제한, 자동 전체 읽기, range 유효성 추가 검증은 없다. 성공한 Get/GetRange는 counting reader를 반환한다. 호출자는 EOF 도달 여부와 관계없이 Close해야 replay 추적 슬롯을 반환한다. 일반 counting reader의 Close 오류는 반환하되 일반 failure 계수에는 더하지 않는다. [metrics.go:346](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L346), [metrics.go:638](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L638)

Upload reader는 Read와 가능한 ReaderAt/Seeker 조합을 보존한다. Read/ReadAt의 n을 누적하고 Seek는 그대로 전달한다. 크기는 먼저 의존 `TryToGetSize`, 실패하면 `Size() int64`에서 얻는다. 성공한 Upload만 BytesPublished를 증가시키며, 미리 구한 크기가 있으면 그 값, 없으면 소비 바이트를 쓴다. 임의의 다른 reader 인터페이스까지 모두 보존하는 래퍼는 아니다. [metrics.go:561](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L561)

## 4. mutation의 admission, 완료와 terminal drain

상태는 active 개수, sticky unknown, irreversible stopped이다. begin은 stopped이면 `ErrMutationsStopped`를 반환하고 아니면 active를 하나 늘린다. end는 active를 하나 줄이고 `unknown = unknown || !known`으로 합치며 대기자에게 변경을 알린다. 논리 Upload/Delete와 각 변경 HTTP 시도를 별도로 센다. 따라서 논리 호출이 끝나도 그 응답 본문이 끝나지 않았으면 HTTP 추적이 남을 수 있다. [mutation_drain.go:73](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/mutation_drain.go#L73)

| 추적 대상 | known으로 끝내는 조건 |
|---|---|
| 논리 Upload | nil 오류, provider가 분류한 조건 불충족, 또는 위의 조건부 S3 409 특례다. |
| 논리 Delete | nil 오류, provider가 분류한 object-not-found 또는 조건 불충족이다. |
| 변경 HTTP | 상태가 500 미만이고 408·202가 아니어야 한다. 필요하면 아래 본문 완료 조건도 만족해야 한다. |

HTTP 변경 요청은 GET·HEAD·OPTIONS 이외의 모든 메서드다. transport 오류는 unknown이다. 응답 Body가 nil 또는 `http.NoBody`이거나 status가 400 이상이면 즉시 위 status 판정으로 종료한다. 그러므로 오류 상태 본문에는 mutationResponse의 framing 검사가 적용되지 않는다. 나머지 본문은 EOF 또는 정확한 양수 Content-Length 소비 시 active 추적을 끝내며, 그 전에 Read 오류나 Close가 발생해도 종료한다. 이후 Close 오류는 별도로 sticky unknown을 만들 수 있다. 이는 Close가 있어야 슬롯을 반환하는 replay reader와 다르다. 202는 HTTP 실패 계수가 아니어도 mutation 결과는 unknown이다. status 408·500 이상도 unknown이며, 이후 retry 성공으로 지워지지 않는다. [metrics.go:429](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L429), [metrics.go:450](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L450)

본문 추적은 받은 바이트 수와 최대 64 KiB의 누적 내용을 사용한다. 양수 Content-Length 초과나 누적 버퍼 한도 초과는 우선 reader 내부의 `known=false`만 설정한다. 이 단계만으로 active를 줄이거나 drain의 sticky unknown을 갱신하지 않는다. EOF 또는 양수 Content-Length에 정확히 도달한 완료 분기에서 길이 불일치와 누적 내용의 대소문자 구분 literal `<Error`·`"error"`를 검사하고 `finish(r.known)`을 호출한다. EOF 이외 Read 오류는 즉시 `finish(false)`를 호출한다. 따라서 길이를 초과한 본문을 EOF·오류·Close 없이 열어 두면 drain은 unknown 오류를 즉시 반환하지 않고 active 종료를 기다릴 수 있다. 이는 XML/JSON parser가 아닌 바이트 휴리스틱이다. Content-Length가 0이거나 음수이면 EOF가 필요하다. 조기 Close는 `end(false)`로 종료한다. `finish(false)`는 sticky unknown을 갱신하고 active는 once로 한 번만 줄인다. 이미 완료한 뒤의 Close 오류도 sticky unknown을 설정한다. 이 64 KiB 한도는 mutation 응답 검사 한도이며 Get 객체 크기 제한이 아니다. [mutation_drain.go:22](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/mutation_drain.go#L22)

DrainMutations는 먼저 stopped를 true로 만들고 active가 0이 될 때까지 ctx 취소 또는 변경을 기다린다. 취소되면 ctx.Err를 반환하고 admission은 다시 열지 않는다. active가 0이면 unknown일 때 `ErrMutationOutcomeUnknown`, 아니면 ctx.Err를 반환한다. 즉 이미 취소된 ctx에서도 중단 상태는 유지되고, 무작업 상태의 unknown 오류는 ctx 오류보다 우선한다. Drain은 Read/Head 자체를 막거나 provider Close를 호출하지 않는다. 이미 admission된 논리 쓰기의 후속 HTTP 시도는 drain이 시작된 뒤 admission에서 거부될 수 있다. [mutation_drain.go:107](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/mutation_drain.go#L107)

### unknown과 not-started의 구분

이 패키지는 범용 dispatch receipt나 `NotStarted` 타입을 제공하지 않는다. 자신의 begin이 stopped로 거부되면 그 경계의 하위 호출은 실행하지 않는다. 그러나 logical admission 이후의 일반 오류, 취소, transport 오류를 근거로 원격 작업이 시작되지 않았다고 판정하지 않는다. 이들은 위 known 예외가 아니면 보수적으로 unknown으로 남는다. 생성 ctx의 취소 역시 원격 생성 취소의 증거가 아니다. 이 구분은 애플리케이션의 retry·복구 판정에서 유지해야 한다. [metrics.go:306](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L306), [metrics.go:450](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L450)

Drain의 nil 결과는 현재 살아 있는 Bucket 수명에서의 완료 판정이다. 영속 incarnation ID, crash 후 재구성 가능한 drain receipt, 원격 프로세스 종료 증명은 없다. 호출자는 별도로 cluster/node/WAL 및 프로세스 incarnation을 묶어야 한다. Node terminal drain은 identity를 검사하고 writer shutdown 이후 drain과 Bucket.Close 결과를 합친다. caller ctx 취소는 이 백그라운드 절차의 완료를 증명하지 않는다. [mutation_drain.go:104](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/mutation_drain.go#L104), [fencing.go:11](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/fencing.go#L11)

## 5. 로컬 filesystem 소유권

AcquireLocalFilesystemOwner는 rootDir만 Abs로 바꾸고 MkdirAll(0700) 후 symlink를 해석한다. dataDir에는 Abs 없이 EvalSymlinks만 적용하므로 존재하지 않으면 오류이고, 결과가 상대경로로 남을 수 있다. 두 pathContains는 filepath.Rel이 성공하면서 동일·하위 관계를 발견한 경우에만 겹침을 거부한다. 절대 root와 상대 resolved DataDir을 비교하면 Rel 오류를 false로 처리하여 실제 겹침을 놓칠 수 있다. 문서가 설명하는 포함관계 검사에는 절대경로로 해석된 DataDir이 필요하지만, 어댑터가 이 전제조건을 강제하지 않는다. 잠금 범위는 prefix별이 아닌 root 전체다. 디렉터리 생성은 이후 검증 실패 시에도 되돌리지 않는다. 기존 권한을 모두 재설정하는 기능은 없다. [local_owner.go:35](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner.go#L35)

Unix에서는 filesystem 검사 후 canonical root의 SHA-256 앞 8바이트를 소문자 hex로 바꿔 부모 디렉터리에 `.rhiza-local-owner-<16hex>` anchor를 둔다. anchor는 O_CREAT|O_RDWR|O_CLOEXEC|O_NOFOLLOW, 생성 mode 0600으로 열고 Fstat에서 일반 파일이며 group/other 권한 비트가 없는지 검사한다. 사용자 UID 소유 여부를 추가 검사하지는 않는다. anchor에 LOCK_EX|LOCK_NB를 획득한 뒤 root를 O_RDONLY|O_DIRECTORY|O_CLOEXEC로 열고 같은 flock을 획득한다. 두 번째 잠금은 경로 alias의 inode 중복 소유를 막기 위한 것이다. 실패 시 이미 연 descriptor와 잠금을 정리한다. 성공한 Close는 anchor를 먼저 unlock/close하고 root를 마지막으로 unlock/close하며 오류는 errors.Join으로 합친다. anchor 파일을 삭제하지 않는다. 이는 협조적인 advisory locking이다. [local_owner.go:188](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner.go#L188), [local_owner_unix.go:12](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner_unix.go#L12)

| 빌드 대상 | 허용 filesystem |
|---|---|
| linux | statfs Type `0xef53`, `0x58465342`, `0x9123683e`, `0x794c7630`, `0x01021994` (ext, XFS, Btrfs, overlayfs, tmpfs) |
| darwin | Fstypename의 끝 NUL을 제거한 값이 정확히 `apfs` 또는 `hfs` |
| 그 밖 | fallback 소스에는 unsupported 오류 경로가 있지만, 공통 Close가 참조하는 unlockClose의 정의가 Darwin/Linux 파일에만 있어 패키지에 unresolved-symbol 빌드 한계가 있다. 실행 가능한 거부 계약으로 확정하지 않는다. |

근거: [local_owner_linux.go:11](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner_linux.go#L11), [local_owner_darwin.go:12](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner_darwin.go#L12), [local_owner_unsupported.go:7](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner_unsupported.go#L7), [local_owner.go:154](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner.go#L154), [local_owner_unix.go:1](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner_unix.go#L1). 기타 OS 한계는 정적 소스 판독 결과이며 cross-compile을 실행하지 않았다.

Wrap은 실제 하위 호출의 active 수명을 관리한다. Upload/Delete/Attributes/Exists/두 Iter는 호출 반환까지, Get/GetRange는 성공 reader의 Close까지 active를 유지한다. wrapper의 Close나 capability 메서드는 내장 Bucket에 위임되므로 owner.Close를 대신하지 않는다. Upload의 조건 불충족 이외 오류, Delete의 not-found 이외 오류는 Poison한다. read 호출 자체의 오류는 poison하지 않지만 reader Close 오류는 poison한다. [local_owner.go:193](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner.go#L193)

Poison은 플래그를 세우고 전역 보관 집합에 owner를 유지하여 descriptor가 프로세스 종료 전까지 남게 한다. Poison만으로 enter를 거부하지는 않는다. enter는 closing만 검사한다. Held는 `!closing && !poison && root != nil && anchor != nil`이다. Close는 closing을 영구히 true로 설정하고 active가 0이 될 때까지 주어진 timeout을 기다린다. active가 남아 timeout이 지나면 poison하고 오류를 반환한다. active가 없어도 poison이면 잠금을 풀지 않고 오류를 반환한다. active가 이미 0이면 0/음수 timeout이어도 정상 release 경로에 들어갈 수 있다. [local_owner.go:67](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner.go#L67), [local_owner.go:108](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner.go#L108)

localOwnerReader는 Close 시작 후 Read를 `os.ErrClosed`로 거부한다. Close는 underlying Close를 한 번 호출한 뒤 진행 중 Read를 기다리고 active를 한 번 해제한다. EOF만으로 active를 해제하지 않는다. 따라서 caller timeout만으로 실제 filesystem 호출 종료나 owner release를 가정할 수 없다. [local_owner.go:274](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner.go#L274)

LocationHash는 다음 바이트열의 SHA-256을 소문자 64자리 hex로 반환한다: `rhiza/local-filesystem-archive/v2` + NUL + canonical root + NUL + `filepath.ToSlash(filepath.Clean(namespace))`. namespace를 root와 Join하지 않는다. 빈 namespace는 Clean 결과 `.`을 사용한다. RootDir은 canonical root를 반환한다. 이 hash는 위치·namespace 식별값이며 새 소유자 incarnation이나 영속 fencing receipt가 아니다. 호출자는 Held와 binding hash를 함께 검사해야 한다. [local_owner.go:91](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner.go#L91), [local.go:234](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L234)

## 6. metrics와 retry 관측

논리 계수는 Uploads, Gets(Get/GetRange), Lists(두 Iter), Heads(Exists/Attributes), Deletes로 나눈다. Upload/Delete는 admission 성공 뒤 증가한다. nil 아닌 반환 오류는 Failures에 포함하므로 정상 제어 결과인 조건 충돌도 논리 실패로 집계된다. 반환 reader의 EOF 이외 Read 오류는 해당 reader에서 한 번만 Failures를 올린다. Close 오류는 일반 계수 래퍼에서 세지 않는다. Stats는 각 atomic을 차례로 읽으며 전체 필드가 같은 순간으로 고정된 snapshot은 아니다. [metrics.go:277](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L277), [metrics.go:376](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L376)

BytesUploaded는 source Read/ReadAt 소비량으로 재읽기도 포함한다. BytesPublished는 성공한 Upload의 알려진 크기 또는 소비량이며 영구 저장량 증거가 아니다. BytesDownloaded는 반환 reader에서 caller가 읽은 바이트다. HTTPRequestBodyBytes/HTTPResponseBodyBytes는 transport wrapper의 본문 Read 바이트이며 protocol overhead를 제외한다. [metrics.go:19](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L19)

S3 transport 순서는 바깥부터 metrics → tagged observer → provider transport다. GCS/Azure에는 metrics만 설치하고 filesystem에는 HTTP 래퍼를 설치하지 않는다. RoundTrip에서 admission이 통과한 요청을 HTTPRequests에 센다. GET/PUT/HEAD/DELETE는 각 계수, 나머지는 Other다. S3HTTPRequests/S3HTTPFailures는 모든 provider HTTP 계수의 호환 별칭이다. [provider.go:101](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/provider.go#L101), [metrics.go:450](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L450)

HTTP 분류는 부록의 status 표와 같다. expected-condition 표식만으로는 부족하고 PUT과 실제 If-Match/If-None-Match 헤더가 있어야 409/412의 HTTP 실패 집계를 제외한다. WithExpectedNotFound는 해당 문맥의 404 집계만 제외하며 오류 반환은 바꾸지 않는다. 응답 본문 Read 오류는 한 번만 TransportFailures를 올리고 상태 코드로 이미 실패를 셌으면 HTTPFailures를 추가하지 않는다. 일반 transport counting wrapper는 Close 오류를 HTTP 실패로 세지 않는다. [metrics.go:511](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L511)

`amz-sdk-request`를 `;`로 나누고 TrimSpace한 조각에서 `attempt=`를 찾는다. 첫 해당 값의 Atoi 결과가 1 이상이면 recognized, 2 이상이면 retry다. 누락·해석 실패·1 미만이면 unknown이다. 이 계수는 재시도를 실행하지 않는다. GCS/Azure의 공통 retry header를 구현하지 않으므로 SDKRetries가 0이라고 재시도가 없었다고 해석해서는 안 된다. [metrics.go:542](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L542)

## 7. 선택적 replay grouping

최대 동시 논리 연산 128개, 연산별 digest 64개를 보관한다. 슬롯 부족은 TrackerCapacityMisses와 IncompleteOperations를 올리고 그 연산의 HTTP 요청은 grouping unknown으로 남는다. identity 한도 초과는 새 digest마다 unknown을 올리고 해당 연산의 IdentityCapacityMisses/IncompleteOperations는 한 번만 올린다. 이미 보관한 digest의 반복은 한도 이후에도 repeat로 센다. done/released 뒤 요청은 unknown 및 incomplete로 표시한다. [metrics.go:92](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L92)

digest는 부록의 정확한 길이-prefix encoding을 SHA-256한 값이다. 본문·Authorization·기타 헤더는 포함하지 않는다. 따라서 같은 논리 연산 안에서 동일 식별값을 가진 RoundTrip 반복을 뜻하며, 본문 동일성이나 실제 SDK retry를 증명하지 않는다. 연산 종료 시 active RoundTrip이 남으면 incomplete로 표시하고, 마지막 active 요청이 끝나야 슬롯을 반환한다. RoundTrip active는 헤더 반환까지이며 mutation 본문 완료 추적과 별개다. Get/GetRange는 반환 reader의 Close까지 연산을 열어 두고 ReplayOpenReaders를 올린다. [metrics.go:147](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L147), [metrics.go:208](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L208), [metrics.go:376](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L376)

## 8. 빌드 태그 진단 observer

일반 빌드(`!rhiza_local_testhooks`)에서 BeginExtentUploadAttribution은 입력 ctx와 nil을 반환하고 Complete는 아무 일도 하지 않는다. `rhiza_local_testhooks`일 때 S3 observer API가 추가된다. 일반 metrics/replay/drain은 이 태그에 종속되지 않는다. [transport_observer_disabled.go:1](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/transport_observer_disabled.go#L1)

Register는 정확한 endpoint 문자열로 등록하며 빈 endpoint, 빈 owners, 중복 endpoint를 거부한다. owners를 복사하고 초기 phase는 `setup`이다. transport 생성 순서로 owner를 배정하고 소진하면 빈 문자열을 사용한다. unregister는 현재 등록이 자기 observer일 때만 map에서 제거한다. 이미 만들어진 transport의 observer 참조를 제거하지 않는다. 요청 시작 시 phase와 method를 캡처하고 total/owner·phase·method 계수를 올린다. [transport_observer_enabled.go:204](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/transport_observer_enabled.go#L204), [transport_observer_enabled.go:307](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/transport_observer_enabled.go#L307)

observer는 RoundTrip 오류, nil response, 예상 제어 결과를 제외한 400 이상 status, EOF 이외 body Read 오류, body Close 오류를 기록한다. 본문 오류를 한 번으로 제한하지 않으므로 한 요청에서 여러 이벤트가 생길 수 있다. 오류 집계와 최초로 record 잠금을 얻은 이벤트만 상세 보관하며 최소 sequence를 재선별하지 않는다. URL, 객체명, 헤더 값, 본문, 원문 오류 문자열은 이벤트에 보관하지 않는다. owner/phase/guard는 caller 입력이므로 임의 문자열을 제한하는 검증은 없다. nil response 처리는 진단 observer 내부 동작이며 바깥 metrics가 nil 응답을 지원한다는 뜻은 아니다. [transport_observer_enabled.go:332](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/transport_observer_enabled.go#L332), [transport_observer_enabled.go:414](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/transport_observer_enabled.go#L414)

Snapshot은 복사한 상세·집계를 반환한다. requests는 phase/owner/method, error aggregates는 phase/owner/method/outcome/status-family/error-class 순으로 정렬한다. error 집계의 resource/conditional 필드는 정렬 tie-break에 포함되지 않으므로 그 동률 순서를 보장하지 않는다. total은 map 복사 잠금 해제 후 atomic으로 읽는다. ExtentAttributionSnapshot은 phase/owner/physical/final/guard 순으로 정렬한다. 성공별 상세 이력은 없다. [transport_observer_enabled.go:234](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/transport_observer_enabled.go#L234)

Extent attribution은 문맥에 probe를 넣고 `archive_block` PUT + 정확히 `if_none_match` 분류인 요청만 연결한다. RoundTrip 오류와 412 관측을 수집하며 body 오류나 다른 HTTP status를 물리 오류로 직접 합치지 않는다. 최초 observer/owner/phase와 다른 관측 또는 빈 owner는 unknown으로 표시한다. Complete는 한 번만 실행하고 물리 오류가 하나도 없으면 집계하지 않는다. final SDK 오류와 typedCondition, guard는 caller가 제공한다. `verified`는 observer 자체의 검증이 아니다. 호출부 uploadExtent는 조건 오류 또는 EPIPE 후 ctx가 살아 있을 때 exact extent readback을 시도하여 guard를 정한다. 그 성공은 lifetime drain의 과거 unknown을 지우지 않는다. [transport_observer_enabled.go:89](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/transport_observer_enabled.go#L89), [archive.go:1044](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/archive.go#L1044)

## 9. 호출자 의무와 로컬 fixture 경계

Node는 로컬 filesystem 모드에서 먼저 owner를 획득하고 canonical RootDir으로 Bucket을 생성한 뒤 `bucket.Bucket = owner.Wrap(bucket.Bucket)`으로 내부를 감싼다. Prefix 적용·namespace 검증은 caller 책임이다. shutdown은 writer를 먼저 종료하고 mutation drain이 실패하거나 maintenance가 unresolved이면 owner를 Poison한다. 단순 Bucket.Close를 소유권 해제나 terminal receipt로 대체해서는 안 된다. [local_backup.go:143](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/local_backup.go#L143), [node.go:2110](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/node.go#L2110)

recovery caller는 IfNotExists/IfMatch capability, Held, LocationHash와 binding을 검사한다. 이 패키지는 binding의 애플리케이션 schema를 소유하지 않는다. 따라서 해당 schema를 이 문서에서 새 wire 계약으로 정의하지 않는다. [local.go:234](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/recovery/local.go#L234)

하위 versityfixture는 별도 테스트 지원 패키지다. Start는 실행 가능한 제공 binary를 검사하고 3초 ctx로 `--version`을 읽고 SHA-256을 계산한다. 버전 일치나 digest allowlist를 자체 검증하지 않으므로 pin 선택은 caller 책임이다. 임시 root/data/iam을 0700으로 만들고 loopback 임의 포트를 잠시 예약한 뒤 닫아 gateway에 전달한다. nonce 16바이트의 hex로 run ID와 정적 테스트 자격을 생성한다. 필수 `RHIZA_VERSITY_EVIDENCE_DIR`는 절대경로이며 임시 parent와 양방향으로 겹치지 않아야 하고 symlink 해석 후에도 검사한다. 새 evidence 하위 디렉터리에는 raw log/output/completion을 보관한다. [fixture.go:147](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/versityfixture/fixture.go#L147)

fixture는 `sh` lifetime supervisor에 fd 3 pipe를 넘기고 env를 ROOT_ACCESS_KEY, ROOT_SECRET_KEY, PATH로 한정한다. gateway 인자는 `--port <addr> --iam-dir <iam> --access-log <log> --quiet posix <data>`다. pipe EOF는 supervisor가 child에 INT를 보내고 최대 20×0.1초 확인 후 필요하면 KILL하고 wait하도록 한다. completion 파일은 임시 파일 작성 후 rename하며 gateway_reaped/child_status/forced를 기록한다. 강제 종료는 실패로 처리한다. Close는 pipe를 한 번 닫고 supervisor 종료를 기다리며 caller deadline이 지나도 supervisor를 죽이지 않는다. Start의 readiness 실패는 별도 7초 cleanup ctx를 사용한다. Start의 더 이른 실패 경로가 모든 생성 디렉터리를 자동 삭제한다고 보장하지 않는다. [fixture.go:51](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/versityfixture/fixture.go#L51), [fixture.go:248](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/versityfixture/fixture.go#L248), [fixture.go:413](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/versityfixture/fixture.go#L413)

WaitReady는 25ms ticker와 500ms dial timeout으로 TCP만 확인하고 조기 child 종료를 감지한다. 인증된 S3 readiness는 caller 책임이다. NewS3Client는 ambient auth 없이 생성 자격·HTTP·us-east-1·path lookup·MaxRetries=1을 사용하고 attempts/응답 수를 분리한다. AccessRecords는 공백을 다듬은 비어 있지 않은 log line을 반환하고 RequestCount는 빈 줄과 `log starts ` prefix만 제외한다. fixture Close는 디렉터리 삭제 API가 아니다. [fixture.go:316](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/versityfixture/fixture.go#L316)

## 10. 근거 수준과 검증 결과

위의 동작은 코드로 확인한 계약이다. 별도 검증 담당자가 같은 HEAD에서 제공한 실행 결과는 다음과 같다. 문서 작성 과정에서 테스트를 추가 실행한 것은 아니다.

```sh
go test -mod=readonly -count=1 -timeout=180s -v \
  ./internal/objstore ./internal/objstore/versityfixture
```

Gateway/helper opt-in을 비우고 GOPROXY=off로 실행한 정상 retry는 exit 0이며 objstore 2.495초, fixture 41.962초로 보고되었다. 초기 GOSUMDB=off 실행은 toolchain 검증에서 테스트 시작 전에 실패했다. 초기 실행은 통과 근거가 아니며 정상 checksum 검증으로 재시도한 결과만 위 실행 근거다. 원시 실행 로그는 이 문서에 첨부되지 않았으므로 이 수치는 전달된 검증 보고로 구분한다.

| Skip된 테스트 | 보고된 이유 |
|---|---|
| TestVersityConditionalLargePutEvidence | RHIZA_VERSITYGW_BIN이 비어 있었다. |
| TestVersityGatewayCostEvidence | RHIZA_VERSITYGW_BIN이 비어 있었다. |
| TestVersitySDKRetryEvidence | RHIZA_VERSITYGW_BIN이 비어 있었다. |
| TestVersityConditionalLargePutExpectEvidence | RHIZA_VERSITY_EXPECT_CONTINUE가 `1`이 아니었다. |

S3는 로컬 HTTP fixture에서 retry, multipart replay, 조건부 upload, Expect 경로를 부분 실행했다. Azure는 로컬 TLS 생성·provisioning 경로를 실행했다. GCS는 설정 검증만 다루었으며 객체 연산 계약 실행은 없었다. Floci S3/GCS/Azure 계약 suite는 모두 실행하지 않았다. 따라서 3-provider 계약 적합성, 실제 cloud parity, cloud 내구성·일관성·원자성, 성능·chaos qualification은 미검증이다. 기본 명령에 rhiza_local_testhooks 태그가 없으므로 tagged observer 실행 검증도 이 명령으로 확립되지 않는다.

검증 담당자는 실제 gateway/emulator/live service를 시작하지 않았고 fixture fake-process 종료 검사가 통과했으며 잔여 테스트 프로세스·임시 디렉터리가 없음을 확인했다고 보고했다. 테스트 파일의 존재나 unit test 통과를 3-cloud qualification으로 확장하지 않는다. 두 Goldfish의 지적을 문서에 수정 반영했고 각 검토자가 해당 항목의 해소를 재확인했다. 원문·재점검·범위 제한은 [EGM 리뷰 기록](object-store-go-egm-review-2026-10-30.md)에 남겼다. 전체 명세의 무결점 판정이나 독립 재구현 인증을 뜻하지 않는다. SDK 내부와 남은 테스트 도우미 전체는 감사하지 않았다.

## 11. 테스트 전용 Expect·HTTP trace 도우미

이 절은 인용된 Expect 증거의 테스트 인프라 계약이다. production NewBucket에 Expect 정책이 있다는 뜻은 아니다. `conditionalExpectContinueRoundTripper`는 PUT, nonnil Body, 선언 ContentLength ≥ `1<<20` (1 MiB), 비어 있지 않은 If-Match 또는 If-None-Match를 모두 만족할 때만 개입한다. 요청과 Header를 clone하고 기존 Expect가 비어 있을 때만 `100-continue`를 삽입한다. 원래 요청을 변경하지 않으며 이미 있는 Expect를 덮어쓰지 않는다. 조건을 만족하지 않으면 바로 하위 transport를 호출한다. 테스트 생성 함수는 enabled=false이면 NewBucket을 사용하고, enabled=true이면 하위 `*http.Transport`를 clone하여 ExpectContinueTimeout을 1초로 설정한다. 타입이 다르면 호출 시 `conditional Expect test requires net/http.Transport` 오류를 반환하는 transport를 설치한다. Expect 삽입은 transport 단계에 있으므로 SDK 서명 이후의 테스트 정책이다. [providertransport_test.go:27](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/providertransport_test.go#L27), [providertransport_test.go:318](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/providertransport_test.go#L318)

Versity Expect 증거는 RHIZA_VERSITY_EXPECT_CONTINUE=`1`이어야 선택되고 gateway binary opt-in도 필요하다. 해당 경로에서만 RHIZA_VERSITY_HTTP_TRACE=`1`이면 trace를 생성한다. 두 플래그는 production 설정이 아니다. trace는 논리 Upload 반환 직후 closeAndSnapshot하여 `VERSITY_HTTP_TRACE <JSON>`으로 test log에 기록한다. closeAndSnapshot은 closed를 세우고 events를 복사하며 이후 callback과 새 attempt를 무시한다. transport goroutine을 join하거나 모든 비동기 callback이 끝났다고 증명하지 않는다. [versity_conditional_put_test.go:34](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/versity_conditional_put_test.go#L34), [versity_conditional_put_test.go:99](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/versity_conditional_put_test.go#L99)

trace는 전체에서 최대 64 attempt를 추적하고 최대 64 event를 보관한다. 닫히기 전 eligible attempt는 모두 attempts를 증가시키되 65번째부터 dropped_attempts를 올리고 callback 추적을 설치하지 않는다. 보관 한도 이후 event는 dropped_events만 올린다. order는 보관된 event마다 증가하며 attempt 번호와 별개다. elapsed_ns는 attempt 시작 이후 경과다. 정확한 JSON schema와 이벤트·오류 enum은 부록 J절에 있다. 이 상한과 snapshot 이후 억제로 누락된 이벤트는 네트워크 활동 부재의 증거가 아니다. [providertransport_test.go:74](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/providertransport_test.go#L74)
