# Object-store Go 어댑터 계약 부록

기준 커밋은 `008f8e576ad76425182e10a69b987f10cfb48b2a`이다. [역명세 본문](object-store-go-reverse-spec-2026-10-30.md)의 동작 설명과 함께 사용한다. 아래 Go 선언은 어댑터 소유 계약이다. `objstore`/`thanosobjstore` 타입은 `github.com/thanos-io/objstore`를 뜻한다. 의존 Bucket의 HTTP URL, 인증 서명, XML/JSON 응답, multipart schema, GCS generation, Azure ETag의 wire encoding은 이 어댑터가 정의하지 않으므로 여기서 발명하지 않는다. 의존 버전은 [go.mod:14](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/go.mod#L14)에 고정되어 있다.

## A. 설정 schema와 생성 API

다음 선언의 JSON 태그를 그대로 사용한다. Provider 외 필드는 omitempty이고, credentials도 JSON 필드에서 자동으로 숨기거나 redact하지 않는다. 이 패키지에 별도 설정 JSON decoder나 YAML schema는 없다. 직접 Config를 전달하는 것과 LoadConfig의 환경변수 기본값을 구분해야 한다. [provider.go:18](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/provider.go#L18)

```go
type Provider string

const (
	ProviderS3         Provider = "s3"
	ProviderGCS        Provider = "gcs"
	ProviderAzure      Provider = "azure"
	ProviderFilesystem Provider = "filesystem"
)

// Config holds object storage configuration.
type Config struct {
	Provider Provider `json:"provider"`

	// Filesystem configuration
	FilesystemDir          string `json:"filesystem_dir,omitempty"`
	Endpoint               string `json:"endpoint,omitempty"`
	Bucket                 string `json:"bucket,omitempty"`
	Region                 string `json:"region,omitempty"`
	Insecure               bool   `json:"insecure,omitempty"`
	MaxRetries             int    `json:"max_retries,omitempty"`
	AccessKey              string `json:"access_key,omitempty"`
	SecretKey              string `json:"secret_key,omitempty"`
	SessionToken           string `json:"session_token,omitempty"`
	ServiceAccount         string `json:"service_account,omitempty"`
	AzureTenantID          string `json:"azure_tenant_id,omitempty"`
	AzureClientID          string `json:"azure_client_id,omitempty"`
	AzureClientSecret      string `json:"azure_client_secret,omitempty"`
	AzureStorageAccount    string `json:"azure_storage_account,omitempty"`
	AzureStorageAccountKey string `json:"azure_storage_account_key,omitempty"`
	AzureConnectionString  string `json:"azure_connection_string,omitempty"`
	AzureUserAssignedID    string `json:"azure_user_assigned_id,omitempty"`

	// Prefix for all objects
	Prefix string `json:"prefix,omitempty"`
}
```

```go
func LoadConfig() Config
func ValidateConfig(cfg Config) error
func NewBucket(cfg Config) (*MeteredBucket, error)
func NewBucketWithContext(ctx context.Context, cfg Config) (*MeteredBucket, error)
func NewBucketForSupervisedStartup(ctx context.Context, cfg Config) (*MeteredBucket, error)
func WithReplayObservation(ctx context.Context) context.Context
func WithExpectedNotFound(ctx context.Context) context.Context
```

생성 성공은 metered bucket과 nil, 검증·provider 생성 실패는 nil과 오류를 반환한다. WithReplayObservation은 생성 시 관측 선택만 바꾸며 WithExpectedNotFound는 연산 HTTP 404 계측만 바꾼다. 생성·오류·기본값은 [provider.go:60](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/provider.go#L60), [provider.go:138](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/provider.go#L138), [metrics.go:246](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L246)에 근거한다.

| 환경변수 | Config 필드 | 빈 값·조건 |
|---|---|---|
| RHIZA_OBJSTORE_PROVIDER | Provider | filesystem으로 바꾼다. |
| RHIZA_OBJSTORE_PREFIX | Prefix | 빈 문자열이다. 이름에 자동 적용하지 않는다. |
| RHIZA_OBJSTORE_ENDPOINT | Endpoint | 빈 문자열이다. |
| RHIZA_OBJSTORE_BUCKET | Bucket | 빈 문자열이다. |
| AWS_REGION | Region | 빈 문자열이다. |
| RHIZA_OBJSTORE_SERVICE_ACCOUNT | ServiceAccount | 빈 문자열이다. |
| RHIZA_OBJSTORE_AZURE_TENANT_ID | AzureTenantID | 빈 문자열이다. |
| RHIZA_OBJSTORE_AZURE_CLIENT_ID | AzureClientID | 빈 문자열이다. |
| RHIZA_OBJSTORE_AZURE_CLIENT_SECRET | AzureClientSecret | 빈 문자열이다. |
| RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT | AzureStorageAccount | 빈 문자열이다. |
| RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT_KEY | AzureStorageAccountKey | 빈 문자열이다. |
| RHIZA_OBJSTORE_AZURE_CONNECTION_STRING | AzureConnectionString | 빈 문자열이다. |
| RHIZA_OBJSTORE_AZURE_USER_ASSIGNED_ID | AzureUserAssignedID | 빈 문자열이다. |
| RHIZA_FILESYSTEM_DIR | FilesystemDir | filesystem에서만 읽는다. 빈 값은 ./objstore다. |
| RHIZA_OBJSTORE_INSECURE | Insecure | s3에서만 읽고 정확히 `true`인 경우에만 true다. |

AccessKey/SecretKey/SessionToken/MaxRetries는 LoadConfig에서 읽지 않는다. 각각 Go zero value를 유지한다. 다른 환경변수를 SDK가 내부적으로 읽는지는 이 어댑터의 환경변수 계약에 포함하지 않는다. [provider.go:175](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/provider.go#L175)

| ValidateConfig 조건 | 정확한 오류 문자열 |
|---|---|
| S3 Bucket 빈 값 | `S3 bucket is required` |
| GCS Bucket 빈 값 | `GCS bucket is required` |
| GCS Endpoint 비어 있지 않음 또는 Insecure | `GCS endpoint and insecure overrides are unsupported` |
| Azure Bucket 또는 AzureStorageAccount 빈 값 | `Azure container and storage account are required` |
| Azure Insecure | `Azure insecure transport is unsupported` |
| 알 수 없는 Provider | `unsupported provider: %s` |

검사 순서는 각 provider에서 위 표의 필수 필드 검사가 override 검사보다 먼저다. filesystem 검증은 항상 nil이다. 의존 생성 오류는 새 문자열로 정규화하지 않는다.

## B. Bucket 요청·응답 인터페이스

[metrics.go:268](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L268)의 내장 Bucket과 다음 재정의가 어댑터 경계다.

```go
type MeteredBucket struct {
    thanosobjstore.Bucket
    // metrics는 비공개다.
}
func (b *MeteredBucket) Stats() Stats
func (b *MeteredBucket) Upload(ctx context.Context, name string, reader io.Reader, opts ...thanosobjstore.ObjectUploadOption) error
func (b *MeteredBucket) Get(ctx context.Context, name string) (io.ReadCloser, error)
func (b *MeteredBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error)
func (b *MeteredBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...thanosobjstore.IterOption) error
func (b *MeteredBucket) IterWithAttributes(ctx context.Context, dir string, f func(thanosobjstore.IterObjectAttributes) error, options ...thanosobjstore.IterOption) error
func (b *MeteredBucket) Exists(ctx context.Context, name string) (bool, error)
func (b *MeteredBucket) Attributes(ctx context.Context, name string) (thanosobjstore.ObjectAttributes, error)
func (b *MeteredBucket) Delete(ctx context.Context, name string) error
func (b *MeteredBucket) DrainMutations(ctx context.Context) error
```

name/dir/off/length/옵션/callback은 추가 schema로 바꾸지 않고 그대로 전달한다. Get/GetRange의 provider 오류는 nil reader와 함께 반환하며 성공 reader는 계수 래퍼로 감싼다. Exists의 bool과 Attributes의 값은 하위 결과를 그대로 반환한다. Iter의 callback 오류를 별도로 가공하지 않는다. 조건 capability와 IsConditionNotMetErr/IsObjNotFoundErr, 그 밖의 Bucket 메서드는 내장 인터페이스로 위임된다. 의존 ObjectAttributes·IterObjectAttributes·옵션 타입을 어댑터 자체 wire schema로 다시 정의하지 않는다. [metrics.go:306](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L306), [metrics.go:346](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L346), [metrics.go:393](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L393)

조건 Upload의 expected 표식은 의존 ApplyObjectUploadOptions 결과 `IfNotExists || Condition != nil`이다. typed condition 오류는 provider 판별 결과를 쓴다. 별도 S3 특례는 expected 표식 AND errors.As(minio.ErrorResponse) AND StatusCode=409 AND Code=`ConditionalRequestConflict`이다. 이 특례는 known 판정만 바꾸며 반환 오류를 nil로 바꾸지 않는다. [metrics.go:255](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L255), [metrics.go:338](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L338)

## C. HTTP 상태와 mutation 완료 계약

아래 분류는 SDK가 실제 보낸 요청이 설치된 RoundTripper를 통과할 때 적용한다. URL/path/header/body 생성 계약은 SDK 소유다. [metrics.go:450](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L450)

| 입력·결과 | HTTP 계측 | 변경 시도 완료 |
|---|---|---|
| stopped에서 변경 HTTP admission 거부 | HTTP 요청 계수를 올리지 않고 ErrMutationsStopped를 반환한다. | 하위 RoundTrip을 호출하지 않는다. |
| RoundTrip 오류 | TransportFailures +1, HTTPFailures +1이다. 응답·오류를 그대로 반환한다. | unknown으로 즉시 종료한다. |
| PUT + expected condition + 비어 있지 않은 If-Match/If-None-Match + 409/412 | status failure에서 제외한다. | known으로 즉시 종료한다. |
| expected not-found + 404 | status failure에서 제외한다. | 변경 요청이면 known으로 즉시 종료한다. |
| 나머지 400–499 | Unexpected4xx +1, HTTPFailures +1이다. | 408은 unknown, 나머지는 known이다. 본문 완료 검사를 하지 않는다. |
| 500 이상 | HTTP5xx +1, HTTPFailures +1이다. | unknown으로 즉시 종료한다. |
| 400 미만 | status failure가 아니다. | 202는 unknown 후보, 그 밖은 known 후보이며 body가 있으면 완료 검사가 필요하다. |
| nil Body 또는 http.NoBody | body byte 계측 대상이 없거나 비어 있다. | status known 판정으로 즉시 종료한다. |
| 응답 EOF 이외 Read 오류 | 해당 일반 counting wrapper에서 한 번 TransportFailures +1이다. status로 이미 실패를 셌으면 HTTPFailures를 중복 가산하지 않는다. | mutationResponse가 적용된 응답은 unknown이다. |
| 일반 counting wrapper Close 오류 | 오류를 반환하며 일반 HTTP failure 계수를 더하지 않는다. | mutationResponse Close 오류는 sticky unknown이다. |

변경 메서드 판정은 `method != GET && method != HEAD && method != OPTIONS`이다. HTTP status known 식은 `status < 500 && status != 408 && status != 202`이다. 응답이 정상이라는 RoundTripper 전제를 사용하며 metrics는 `(nil, nil)` 응답을 방어하지 않는다.

mutationResponse는 로컬 후보 판정과 active 완료를 분리한다. `expected > 0 && received > expected`와 누적 64 KiB 초과는 `r.known=false`만 설정하며 sticky drain unknown이나 active를 즉시 바꾸지 않는다. EOF 이외 Read 오류는 `finish(false)`를 호출한다. EOF 또는 `expected > 0 && received == expected` 분기에서는 양수 expected와 received 불일치 및 누적 내용의 literal `<Error`/`"error"`를 검사한 뒤 `finish(r.known)`을 호출한다. marker 검사는 이 완료 분기에서만 수행한다. 따라서 overlong body가 EOF·오류·Close 없이 열려 있으면 active가 남아 drain이 기다릴 수 있다. `finish(false)`는 sticky unknown을 갱신하고 once로 active를 한 번 줄인다. early Close는 once를 통해 `end(false)`를 호출하며 Close 오류는 이미 완료한 뒤에도 sticky unknown을 설정한다. 양수 length의 정확한 소비는 Close나 EOF 없이도 완료되지만 0/음수 length는 EOF가 필요하다. 이 판정은 응답 status가 400 미만이며 Body가 존재할 때만 적용한다. [mutation_drain.go:22](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/mutation_drain.go#L22)

```go
var ErrMutationsStopped = errors.New("object-store mutations stopped for terminal drain")
var ErrMutationOutcomeUnknown = errors.New("object-store mutation outcome remains unknown")
```

Drain은 admission을 영구 중단하고 active가 0일 때 unknown이면 위 unknown 오류, 아니면 ctx.Err를 반환한다. 대기 중 취소는 ctx.Err다. read 계열 호출이나 provider Close를 수행하지 않는다. live Bucket 밖의 persistent receipt schema와 범용 not-started 결과 schema는 제공하지 않는다. [mutation_drain.go:70](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/mutation_drain.go#L70)

## D. Stats JSON schema

모든 필드는 아래 타입과 JSON 이름을 그대로 사용하며 omitempty가 없다. 여러 atomic을 순차 조회하므로 필드 간 동일 순간의 일관성은 보장하지 않는다. S3HTTPRequests/S3HTTPFailures는 HTTPRequests/HTTPFailures의 같은 조회값이다. [metrics.go:26](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L26), [metrics.go:277](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L277)

```go
type Stats struct {
	Uploads         uint64 `json:"uploads"`
	Gets            uint64 `json:"gets"`
	Lists           uint64 `json:"lists"`
	Heads           uint64 `json:"heads"`
	Deletes         uint64 `json:"deletes"`
	Failures        uint64 `json:"failures"`
	BytesUploaded   uint64 `json:"bytes_uploaded"`
	BytesPublished  uint64 `json:"bytes_published"`
	BytesDownloaded uint64 `json:"bytes_downloaded"`
	HTTPRequests    uint64 `json:"http_requests"`
	// HTTP body bytes are read by the transport wrappers; they exclude protocol overhead.
	HTTPRequestBodyBytes  uint64 `json:"http_request_body_bytes"`
	HTTPResponseBodyBytes uint64 `json:"http_response_body_bytes"`
	HTTPFailures          uint64 `json:"http_failures"`
	S3HTTPRequests        uint64 `json:"s3_http_requests"`
	S3HTTPFailures        uint64 `json:"s3_http_failures"`
	HTTPGetRequests       uint64 `json:"http_get_requests"`
	HTTPPutRequests       uint64 `json:"http_put_requests"`
	HTTPHeadRequests      uint64 `json:"http_head_requests"`
	HTTPDeleteRequests    uint64 `json:"http_delete_requests"`
	HTTPOtherRequests     uint64 `json:"http_other_requests"`
	ConditionConflicts    uint64 `json:"condition_conflicts"`
	DedupHits             uint64 `json:"dedup_hits"`
	SDKRetries            uint64 `json:"sdk_retries"`
	// RetryMetadataRequests counts recognized attempt metadata; unknown counts missing or unusable metadata.
	RetryMetadataRequests        uint64 `json:"retry_metadata_requests"`
	RetryMetadataUnknownRequests uint64 `json:"retry_metadata_unknown_requests"`
	TransportFailures            uint64 `json:"transport_failures"`
	Unexpected4xx                uint64 `json:"http_4xx_unexpected"`
	HTTP5xx                      uint64 `json:"http_5xx"`
	ReplayGroupingEnabled        bool   `json:"replay_grouping_enabled"`
	// These counters describe same-identity HTTP RoundTrip repeats, not proven
	// SDK retries. Unknown requests preserve attempts that could not be grouped.
	ObservedRequestIdentities     uint64 `json:"observed_request_identities"`
	ObservedRequestRepeats        uint64 `json:"observed_request_repeats"`
	RequestGroupingUnknown        uint64 `json:"request_grouping_unknown"`
	ReplayTrackerCapacityMisses   uint64 `json:"replay_tracker_capacity_misses"`
	ReplayIdentityCapacityMisses  uint64 `json:"replay_identity_capacity_misses"`
	ReplayIncompleteOperations    uint64 `json:"replay_incomplete_operations"`
	ReplayTrackedOperationsActive uint64 `json:"replay_tracked_operations_active"`
	ReplayOpenReaders             uint64 `json:"replay_open_readers"`
}
```

Uploads/Deletes는 admission된 논리 호출 수다. Gets는 Get/GetRange, Lists는 두 Iter, Heads는 Exists/Attributes다. Failures는 논리 반환 오류와 reader별 최초 EOF 이외 Read 오류를 센다. ConditionConflicts/DedupHits는 provider typed condition 오류를 이름 부분 문자열로 나눈다. 바이트 계수는 본문 설명의 source/returned-reader/transport 경계를 따르며 성공·영속성의 증거로 해석하지 않는다.

HTTP method 계수는 GET/PUT/HEAD/DELETE를 개별 집계하고 나머지를 Other로 센다. SDKRetries는 `amz-sdk-request`의 첫 `attempt=` 조각이 양수 정수로 해석되고 2 이상일 때만 증가한다. recognized 요청은 RetryMetadataRequests, 그 밖은 RetryMetadataUnknownRequests다. `SDKRetries == 0`은 retry 부재 증명이 아니다. [metrics.go:468](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L468), [metrics.go:542](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L542)

## E. replay identity encoding과 용량 계약

문자열 필드마다 UTF-8 string의 바이트 길이를 uint64 big endian 8바이트로 앞에 붙인 뒤 원래 바이트를 붙인다. 필드 순서는 다음과 같다.

1. `rhiza-request-identity-v1`
2. URL Scheme, Host, EscapedPath(), RawQuery. URL이 nil이면 네 개 모두 빈 문자열이다.
3. request.Method
4. Header.Get의 Range, If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since
5. uint64(request.ContentLength)를 big endian 8바이트로 쓴 값. 이 마지막 숫자에는 문자열 길이 prefix를 붙이지 않는다.

전체를 SHA-256한다. 음수 ContentLength도 uint64 cast 결과를 인코딩한다. 본문·인증은 포함하지 않는다. 최대 128 live operations × 64 digest이며 이 digest는 영속 포맷이나 공개 요청 ID가 아닌 프로세스 내 관측 키다. [metrics.go:208](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L208)

| 계수 | 증가·감소 규칙 |
|---|---|
| ObservedRequestIdentities | 연산의 새 digest를 슬롯에 보관할 때 +1이다. |
| ObservedRequestRepeats | 같은 연산에서 이미 보관한 digest일 때 +1이다. |
| RequestGroupingUnknown | 연산 표식 없음, done/released 요청, 새 digest의 capacity 초과일 때 요청별 +1이다. |
| ReplayTrackerCapacityMisses | 동시 128개 한도로 새 논리 연산을 추적하지 못할 때 +1이다. |
| ReplayIdentityCapacityMisses | 연산의 새 identity 한도 초과 시 연산당 한 번 +1이다. |
| ReplayIncompleteOperations | tracker 한도 누락, identity 한도 초과, done 이후 요청, 종료 시 active 요청 잔존을 표시한다. 추적 연산 내부에서는 한 번만 증가한다. |
| ReplayTrackedOperationsActive | 추적 시작 시 +1, done이며 active RoundTrip이 0일 때 한 번 -1이다. |
| ReplayOpenReaders | 추적되는 Get/GetRange 성공 reader마다 +1, Close에서 한 번 -1이다. EOF로 감소하지 않는다. |

근거: [metrics.go:114](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L114), [metrics.go:147](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L147), [metrics.go:376](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/metrics.go#L376)

## F. 로컬 ownership API와 식별값

```go
func AcquireLocalFilesystemOwner(rootDir, dataDir string) (*LocalFilesystemOwner, error)
func (o *LocalFilesystemOwner) Wrap(bucket objstore.Bucket) objstore.Bucket
func (o *LocalFilesystemOwner) LocationHash(namespace string) string
func (o *LocalFilesystemOwner) RootDir() string
func (o *LocalFilesystemOwner) Held() bool
func (o *LocalFilesystemOwner) Poison()
func (o *LocalFilesystemOwner) Close(timeout time.Duration) error
```

owner 구조는 비공개 상태를 가지며 JSON/wire 직렬화 계약이 없다. LocationHash는 `hex(SHA256("rhiza/local-filesystem-archive/v2" + NUL + canonicalRoot + NUL + filepath.ToSlash(filepath.Clean(namespace))))`다. anchor는 `filepath.Join(filepath.Dir(root), ".rhiza-local-owner-" + hex(SHA256(root)[0:8]))`이다. prefix별 lock이나 incarnation nonce를 추가하지 않는다. canonical root와 namespace hash가 같아도 현재 lock을 보유한다는 뜻은 아니므로 Held를 별도로 확인한다. [local_owner.go:35](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner.go#L35), [local_owner.go:91](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner.go#L91), [local_owner.go:188](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner.go#L188)

| 조건 | 로컬 오류 문자열·반환 |
|---|---|
| filepath.Rel이 성공하고 root/DataDir의 동일·포함관계가 감지됨 | `Local archive root and DataDir must be disjoint` |
| closing 중 새 entry | `Local filesystem owner is closing` |
| active drain timeout | `Local filesystem operations did not drain` |
| poison 상태 Close | `Local filesystem ownership remains poisoned until process exit` |
| anchor open 실패 | `open Local owner anchor: %w` |
| anchor가 일반 파일이 아니거나 group/other 권한 있음 | `unsafe Local owner anchor` |
| anchor flock 실패 | `Local filesystem archive is already owned: %w` |
| root flock 실패 | `Local filesystem archive alias is already owned: %w` |
| Linux 허용 목록 외 | `unsupported Local archive filesystem type %#x` |
| Darwin 허용 목록 외 | `unsupported Local archive filesystem %q` |
| 기타 OS fallback 소스에 정의된 오류 | `filesystem-backed Local recovery is unsupported on this platform` (아래 빌드 한계로 실행 가능한 계약은 미확립) |
| reader Close 시작 후 Read | `(0, os.ErrClosed)` |

root만 Abs 후 EvalSymlinks하며 DataDir에는 EvalSymlinks만 적용한다. DataDir이 상대경로로 남으면 절대 root와의 filepath.Rel 오류가 pathContains에서 false가 되어 실제 겹침을 놓칠 수 있다. 절대 resolved DataDir은 포함관계 검사의 전제이며 어댑터가 강제하는 검증이 아니다. [local_owner.go:35](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner.go#L35)

기타 OS fallback은 오류 경로를 정의하지만 공통 Close는 unlockClose를 무조건 참조하며 그 유일한 정의에는 `darwin || linux` 태그가 붙는다. 따라서 그 밖의 OS에서는 unresolved-symbol 빌드 한계가 있고 unsupported 오류를 실행 계약으로 약속할 수 없다. 이는 정적 소스 판독 결과이며 cross-compile은 실행하지 않았다. [local_owner.go:154](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner.go#L154), [local_owner_unix.go:1](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner_unix.go#L1), [local_owner_unsupported.go:7](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner_unsupported.go#L7)

나머지 path·OS 오류는 반환하며 unlock과 Close 오류는 errors.Join한다. wrapper의 Upload는 provider condition 이외 오류에 Poison하고 Delete는 not-found 이외 오류에 Poison한다. capability 및 reader 밖의 위임 메서드 자체는 owner admission을 추가하지 않는다. [local_owner.go:193](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner.go#L193), [local_owner_unix.go:12](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/local_owner_unix.go#L12)

## G. rhiza_local_testhooks 진단 계약

다음 observer API는 태그가 있을 때만 존재한다. 공통 BeginExtentUploadAttribution/Complete는 일반 빌드에도 무동작 형태로 존재한다. [transport_observer_enabled.go:89](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/transport_observer_enabled.go#L89), [transport_observer_disabled.go:13](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/transport_observer_disabled.go#L13)

```go
func RegisterS3TransportObserver(endpoint string, owners []string) (*S3TransportObserver, func(), error)
func (o *S3TransportObserver) SetPhase(phase string)
func (o *S3TransportObserver) Snapshot() (total uint64, requests []S3TransportRequestCount, aggregates []S3TransportAggregate, first *S3TransportEvent)
func (o *S3TransportObserver) ExtentAttributionSnapshot() []ExtentUploadAttributionAggregate
func BeginExtentUploadAttribution(ctx context.Context) (context.Context, *ExtentUploadAttribution)
func (p *ExtentUploadAttribution) Complete(uploadErr error, typedCondition bool, guard string)
```

등록 오류는 `S3 transport observer requires endpoint and owners`, `S3 transport observer already registered for endpoint`다. 성공하면 observer와 unregister closure, nil을 반환한다. observer와 attribution 객체 자체는 직렬화하지 않으며 다음 결과 구조만 JSON 태그를 정의한다. [transport_observer_enabled.go:22](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/transport_observer_enabled.go#L22)

```go
type S3TransportEvent struct {
	Sequence              uint64        `json:"sequence"`
	Owner                 string        `json:"owner"`
	Phase                 string        `json:"phase"`
	Method                string        `json:"method"`
	StatusCode            int           `json:"status_code,omitempty"`
	Outcome               string        `json:"outcome"`
	ErrorClass            string        `json:"error_class,omitempty"`
	ResourceClass         string        `json:"resource_class,omitempty"`
	ConditionalPresence   string        `json:"conditional_presence,omitempty"`
	DeclaredContentLength int64         `json:"declared_content_length,omitempty"`
	Elapsed               time.Duration `json:"elapsed_ns"`
}

// S3TransportAggregate summarizes error observations by phase and request
// class; it retains no per-success history.
type S3TransportAggregate struct {
	Owner               string `json:"owner"`
	Phase               string `json:"phase"`
	Method              string `json:"method"`
	StatusFamily        string `json:"status_family,omitempty"`
	Outcome             string `json:"outcome"`
	ErrorClass          string `json:"error_class,omitempty"`
	ResourceClass       string `json:"resource_class,omitempty"`
	ConditionalPresence string `json:"conditional_presence,omitempty"`
	Count               uint64 `json:"count"`
}

// S3TransportRequestCount aggregates all actual RoundTrip calls without
// retaining per-request success history.
type S3TransportRequestCount struct {
	Owner  string `json:"owner"`
	Phase  string `json:"phase"`
	Method string `json:"method"`
	Count  uint64 `json:"count"`
}
type ExtentUploadAttributionAggregate struct {
	Owner                 string `json:"owner"`
	Phase                 string `json:"phase"`
	PhysicalClass         string `json:"physical_class"`
	FinalSDKClass         string `json:"final_sdk_class"`
	GuardOutcome          string `json:"guard_outcome"`
	Operations            uint64 `json:"operations"`
	PhysicalErrorAttempts uint64 `json:"physical_error_attempts"`
	Observed412           uint64 `json:"observed_412"`
}
```

Elapsed는 time.Duration의 정수 nanoseconds로 JSON에 들어간다. 원문 URL/객체명/본문/오류 문자열을 이벤트에 넣지 않는다. owner/phase/guard는 caller 제공 문자열이며 이를 고정 enum으로 검증하지 않는다.

| 분류 | 정확한 값과 규칙 |
|---|---|
| Method | GET, HEAD, PUT, POST, DELETE, PATCH, OPTIONS만 그대로 두고 나머지는 OTHER다. |
| Outcome | round_trip_error, nil_response, http_status, response_body_read_error, response_body_close_error다. 성공은 별도 이벤트로 보관하지 않는다. |
| StatusFamily | 100–199=1xx, 200–299=2xx, 300–399=3xx, 400–499=4xx, 500–599=5xx, 나머지=none이다. |
| ConditionalPresence | 양쪽 헤더가 있으면 both, If-Match만 있으면 if_match, If-None-Match만 있으면 if_none_match, 없으면 none이다. 값 자체를 보관하지 않는다. |
| ErrorClass | errors.Is 순서로 context_canceled, context_deadline, broken_pipe, connection_reset, unexpected_eof, closed_pipe, net_closed, body_read_after_close를 검사하고 그 뒤 net.Error.Timeout이면 net_timeout, 나머지 other다. status 이벤트는 http_status, nil response는 other다. |

리소스 분류는 request.URL.Path의 끝 segment를 검사한다. 아래 hash는 소문자 `[0-9a-f]` 64자리이며 gen은 정확히 20자리 십진 uint64 중 0보다 큰 값, index는 정확히 20자리 십진 uint64로 0도 허용한다. 경로 앞부분은 제한하지 않는다. 내용 검증이 아니다.

| 경로 suffix | ResourceClass |
|---|---|
| archive/head.bin | archive_head |
| checkpoint/CURRENT | checkpoint_current |
| archive/blocks/<hash>.bin 또는 <hash>_<gen>.bin | archive_block |
| checkpoint/blocks/<hash>.block 또는 <hash>_<gen>.block | checkpoint_block |
| checkpoint/roots/<index>_<hash>.json | checkpoint_root |
| 나머지 | other |

근거: [transport_observer_enabled.go:384](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/transport_observer_enabled.go#L384), [transport_observer_enabled.go:425](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/transport_observer_enabled.go#L425), [transport_observer_enabled.go:451](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/transport_observer_enabled.go#L451)

Extent PhysicalClass는 unknown이 우선이며 아니면 물리 오류가 전부 broken_pipe/connection_reset/other일 때 각각 해당 값, 혼합이면 mixed다. FinalSDKClass는 nil=success, typedCondition=typed_condition, context canceled/deadline=context_done, EPIPE=broken_pipe, ECONNRESET=connection_reset, 나머지=other_error 순이다. GuardOutcome은 Complete에 전달한 guard를 그대로 사용하며 observer가 `verified`를 검증하지 않는다. Operations는 집계된 연산 수, PhysicalErrorAttempts는 broken+reset+other, Observed412는 probe에 연결한 412 수다. 물리 오류 0이면 Complete는 집계하지 않는다. [transport_observer_enabled.go:129](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/transport_observer_enabled.go#L129)

## H. fixture의 지원 API와 증거 형식

versityfixture는 별도 측정 지원 패키지이며 애플리케이션 Bucket 인터페이스를 대체하지 않는다. [fixture.go:29](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/versityfixture/fixture.go#L29)

```go
func Start(ctx context.Context, binary, parentDir string) (*Server, error)
func (s *Server) WaitReady(ctx context.Context) error
func (s *Server) NewS3Client() (*minio.Client, *ClientCounts, error)
func (s *Server) AccessRecords() ([]string, error)
func RequestCount(records []string) uint64
func (s *Server) Close(ctx context.Context) error
func (c *ClientCounts) Snapshot() (attempts, responses uint64)
```

Server의 공개 string 필드는 Endpoint, AccessKey, SecretKey, Root, LogPath, OutputPath, CompletionPath, EvidenceDir, Version, BinarySHA256, RunID이다. JSON 태그는 정의하지 않는다. EvidenceDir 하위 이름은 `s3-access.log`, `server-output.log`, `gateway-complete`다. RunID는 16 random bytes의 32자리 hex, 자격은 `versity-<RunID>`/`versity-secret-<RunID>`다. Endpoint는 `http://<loopback addr>`다. 필수 RHIZA_VERSITY_EVIDENCE_DIR는 절대경로이며 tempParent와 canonical 경로에서 disjoint여야 한다. [fixture.go:147](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/versityfixture/fixture.go#L147), [fixture.go:272](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/versityfixture/fixture.go#L272)

completion은 JSON이 아닌 newline text다. supervisor cleanup에서 다음 키를 순서대로 기록한 임시 파일을 CompletionPath로 rename한다.

```text
gateway_reaped=true
child_status=<shell wait status>
forced=<0 or 1>
```

이 형식은 fixture supervisor 증거이며 application mutation drain receipt가 아니다. raw access log 형식은 Versity 소유다. AccessRecords는 빈 줄을 제거하고 TrimSpace한 문자열을 반환한다. RequestCount는 `log starts `로 시작하는 metadata 줄과 빈 줄만 제외하며 다른 미인식 줄도 센다. caller는 인증 readiness/setup traffic과 client transport attempts/응답 수를 구분해야 한다. [fixture.go:51](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/versityfixture/fixture.go#L51), [fixture.go:372](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/versityfixture/fixture.go#L372)

## I. 검증 경계

코드 계약과 실제 실행 결과를 구분한다. 별도 담당자의 정상 재시도 보고는 기본 빌드의 objstore/versityfixture 테스트 exit 0이다. 초기 toolchain 검증 실패는 통과로 세지 않는다. VersityConditionalLargePutEvidence/GatewayCostEvidence/SDKRetryEvidence는 RHIZA_VERSITYGW_BIN 부재로, ConditionalLargePutExpectEvidence는 EXPECT_CONTINUE opt-in 부재로 skip되었다. 정확한 테스트명과 실행 조건은 본문 10절에 있다.

S3 로컬 HTTP fixture와 Azure 로컬 TLS 생성 검증은 부분 근거다. GCS 객체 연산, rhiza_local_testhooks observer 실행, Floci 기반 S3/GCS/Azure 계약 suite, production cloud parity와 성능·chaos qualification은 이 증거로 검증되지 않았다. 문서의 schema·상태 표는 어댑터 코드 계약이며 세 cloud의 공식 보장으로 해석하지 않는다.

## J. 테스트 전용 Expect·trace schema

이 절은 production provider 계약이 아닌 테스트 도우미 계약이다. PUT + Body nonnil + ContentLength ≥ 1 MiB + 비어 있지 않은 If-Match/If-None-Match일 때 요청·Header를 clone하고 빈 Expect에만 `100-continue`를 추가한다. 다른 요청은 그대로 위임한다. enabled 테스트 생성 경로는 `*http.Transport` clone에 ExpectContinueTimeout=1초를 설정한다. Versity 증거 경로의 trace 선택은 `expectContinue && RHIZA_VERSITY_HTTP_TRACE == "1"`이며 Expect 경로 자체는 RHIZA_VERSITY_EXPECT_CONTINUE=`1` opt-in과 RHIZA_VERSITYGW_BIN을 요구한다. [providertransport_test.go:27](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/providertransport_test.go#L27), [providertransport_test.go:318](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/providertransport_test.go#L318), [versity_conditional_put_test.go:34](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/versity_conditional_put_test.go#L34)

JSON schema는 다음 테스트 선언을 따른다. 로그 접두사는 `VERSITY_HTTP_TRACE `이다. URL·객체명·원문 오류는 필드에 포함하지 않는다. [providertransport_test.go:74](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/providertransport_test.go#L74)

```go
type conditionalPutHTTPTraceEvent struct {
	Attempt       uint64 `json:"attempt"`
	Order         uint64 `json:"order"`
	ElapsedNS     int64  `json:"elapsed_ns"`
	Event         string `json:"event"`
	Status        int    `json:"status,omitempty"`
	ErrorClass    string `json:"error_class,omitempty"`
	ContentLength int64  `json:"content_length"`
	Expect        bool   `json:"expect_100_continue"`
	ProtoMajor    int    `json:"proto_major,omitempty"`
	ProtoMinor    int    `json:"proto_minor,omitempty"`
	ResponseClose bool   `json:"response_close,omitempty"`
}

type conditionalPutHTTPTraceSnapshot struct {
	Attempts        uint64                         `json:"attempts"`
	DroppedAttempts uint64                         `json:"dropped_attempts"`
	DroppedEvents   uint64                         `json:"dropped_events"`
	Events          []conditionalPutHTTPTraceEvent `json:"events"`
}

```

Event는 round_trip_started, wrote_headers, got_100_continue, got_1xx_response, got_first_response_byte, wrote_request, round_trip_result다. got_100_continue의 status는 100이고 got_1xx_response는 callback code, round_trip_result는 nonnil 응답의 StatusCode다. 그 밖은 0이다. ErrorClass는 오류를 받지 않는 이벤트에서 빈 값이고, wrote_request/round_trip_result에서는 nil=none, 이후 errors.Is 순서로 context_canceled, context_deadline, broken_pipe, connection_reset, connection_refused, unexpected_eof, closed_pipe, connection_closed, 그 뒤 net.Error.Timeout이면 network_timeout, 나머지 other다. ProtoMajor/ProtoMinor/ResponseClose는 round_trip_result에 응답이 있을 때만 채운다. Expect bool은 헤더가 정확히 `100-continue`인지 나타낸다. [providertransport_test.go:49](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/providertransport_test.go#L49), [providertransport_test.go:151](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/providertransport_test.go#L151)

최대 64 attempt를 추적하고 전체 trace에 최대 64 event를 보관한다. 닫히기 전 beginAttempt는 attempts를 먼저 늘리며 64 초과이면 dropped_attempts를 올리고 nil을 반환한다. event capacity 초과는 dropped_events만 늘리며 order는 보관하는 event마다 증가한다. elapsed_ns는 attempt 시작 기준이다. closeAndSnapshot은 closed=true와 event slice 복사를 같은 잠금 아래 수행하고 이후 beginAttempt/record를 계수 증가 없이 무시한다. 비동기 callback 완료를 기다리는 계약은 아니다. [providertransport_test.go:117](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/providertransport_test.go#L117), [providertransport_test.go:172](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/internal/objstore/providertransport_test.go#L172)

두 Goldfish 검토의 지적을 수정 반영했으며 [EGM 리뷰 기록](object-store-go-egm-review-2026-10-30.md)의 상태는 수정 후 재검토 대기다. clean 또는 exhaustive sign-off는 아직 없다. 남은 테스트 도우미 전체와 SDK 내부를 감사한 것으로 해석하지 않는다.
