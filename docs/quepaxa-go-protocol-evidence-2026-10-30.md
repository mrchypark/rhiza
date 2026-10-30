편집 주: 아래의 실행·무수정 진술은 최초 근거 수집 당시의 기록이다. 이번 문서 보정은 허용된 문서만 수정하고 테스트를 실행하지 않았다. 기존 결과를 재실행 결과로 해석하지 않는다.

분석 대상은 **Go 체크아웃 `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza`의 HEAD `8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd`**이다. 첫 shell 호출에서 `go.mod`와 `pkg/quepaxa`의 존재를 확인했고, 마지막에도 HEAD가 일치함을 확인했다. `go.mod`의 module은 `github.com/mrchypark/rhiza`, Go 선언은 `1.27.3`이다.

아래 내용은 현재 구현을 역으로 정리한 합의·프로토콜 명세의 근거 자료다. **코드에서 확인한 동작**, **테스트가 정의한 기대**, **실행으로 검증한 결과**를 구분한다. 테스트·빌드·벤치마크는 실행하지 않았으며 파일 수정, 커밋, 위임, 공유 메모리 접근도 수행하지 않았다. WAL·compaction·membership의 내부 구현이나 논문과의 동등성, 과거 설계 의도는 이 분석의 결론에 포함하지 않는다.

**1. 자료형과 공개 API의 계약**

`Slot`과 `Step`은 `uint64`, `NodeID`는 문자열, `ValueHash`는 32바이트 SHA-256 해시다. `Proposal`은 `Priority`, `ProposerID`, `Hash`, `Value`를 보유한다. 제안의 비교에는 payload가 직접 참여하지 않으며, **priority → proposer ID의 바이트열 → hash** 순서로 사전식 비교한다. `nil` 제안은 모든 실재 제안보다 작다. 따라서 `sameProposal`도 payload의 바이트 일치가 아니라 이 튜플의 일치를 뜻한다. [types.go:23](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L23), [protocol.go:23–75](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/protocol.go#L23)

주요 메시지 계약은 다음과 같다.

| 자료형 | 필드와 의미 |
|---|---|
| `RecordRequest` | `Slot`, `Step`, `ConfigID`, `ReconfigurationID`, `Proposal`을 recorder에 전달한다. |
| `Summary` | `RecorderID`, `ReconfigurationID`, `Step`, `FirstCurrent`, `AggregatePrior`를 반환한다. `AggregateCurrent`는 응답에 없다. |
| `Decision` | `ConfigID`, `Slot`, `Step`, 결정한 `Proposal`, quorum의 `Summaries`를 묶는다. |
| `DecidedValue` | slot·hash·실제 value·JSON certificate를 묶는다. |
| `Receipt` | slot·hash·recorder ID와 `Accepted`를 표현한다. 서명 필드는 없다. |
| `SlotValue` | slot과 hash만 전달하며 payload와 certificate는 포함하지 않는다. |

근거는 [protocol.go:114–137](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/protocol.go#L114), [types.go:143–158](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L143), [types.go:216–220](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L216)이다.

`Transport`는 `SendRecord`, `SendDecision`, `ReadTip`, `StageValue`, `FetchValue`의 다섯 메서드로 구성된다. 특히 `SendDecision`은 ACK 목록이나 quorum 수치를 반환하지 않고 `error`만 반환한다. 따라서 Core가 그 성공의 구체적인 전파 범위를 직접 계산하지는 않는다. [core.go:52–58](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L52)

공개 API는 역할별로 다음과 같이 구분된다.

| 역할 | API와 완료 조건 |
|---|---|
| 제안 | `Propose`는 인증한 결정을 설치하고 연속 prefix 및 decision 전파까지 완료한다. `ProposeCertified`는 후속 completion을 생략한다. |
| 결정 수신 | `AcceptDecision`은 검증·설치 후 로컬 내구성 확보를 요청한다. `AcceptDecisionHint`는 일반 결정에 대해 추가 디스크 장벽을 생략한다. |
| 인증된 값 수신 | `AcceptCertifiedValue(s)`는 certificate와 외부 metadata를 결합해 검증한다. `AcceptCertifiedHints`는 추가 디스크 장벽을 생략한다. |
| 완료 재시도 | `CompleteDecision`은 이미 알려진 slot의 completion과 decision 전파를 다시 수행한다. |
| 조회 | `CertifiedValue`는 앞선 gap이 있어도 해당 결정을 반환한다. `Tip`은 연속 결정 prefix의 끝이다. |
| 읽기 위치 | `ReadIndex`, `ReadIndexExcluding`은 quorum에서 관찰한 tip과 그 응답자의 ID를 반환한다. |
| 학습·페이지 | `Learn`은 slot/hash 알림을 제공하고, `DecisionsFrom(Bounded)`는 연속 prefix의 실제 값과 certificate를 복사해 반환한다. |
| 진행 대기·복구 | `WaitTip`은 prefix 진행을 기다리고, `RecoverThrough`는 다음 미결정 slot부터 합의를 다시 수행한다. |

이 구분의 직접 근거는 [core.go:298–306](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L298), [core.go:1900–1934](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1900), [core.go:2707–2760](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2707), [core.go:2951–3004](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2951)이다.

**2. 생성 조건과 기본값**

`New`는 voter, `NewObserver`는 수동 verifier, `NewLearner`는 비투표 replica를 생성한다. 생성 시 node ID, WAL, 한 명 이상의 member가 필요하다. member ID는 비어 있거나 중복될 수 없다. 일반 voter의 ID는 구성에 포함되어야 하고 observer와 learner의 ID는 기존 voter 구성에 없어야 한다. 복수 member 구성에서는 observer를 제외하고 transport가 필요하다. caller가 전달한 member slice는 복사한다. [config.go:40–112](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/config.go#L40)

| 항목 | 현재 기본값 또는 제한 |
|---|---|
| quorum | `floor(N/2)+1`이다. |
| 최초 예약 slot | 1이다. |
| 제안 pipeline | 동시에 최대 16개가 permit을 보유한다. |
| slot 실행 시작 step | 4이다. |
| leader epoch | 16개 slot이다. |
| 재구성 | `EnableReconfiguration=false`가 기본이다. |
| LocalMode | 명시적으로 설정해야 하며 singleton 구성만으로 활성화되지 않는다. |
| 제안·staging value 상한 | 128 KiB이며 보편적인 결정 수신 한도는 아니다. |
| read barrier nonce | 16바이트이다. |

근거는 [types.go:17–19](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L17), [types.go:121](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L121), [core.go:38](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L38), [core.go:140–146](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L140), [config.go:19–36](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/config.go#L19)이다.

LocalMode는 matching voter 한 명, transport 없음, 재구성 비활성화라는 조건을 요구한다. 유한한 양의 WAL capacity도 필요하다. 외부에서 `Record`, `StageValue`, 결정 수신 API 등을 호출하면 실행 소유권 오류로 거부한다. 일반 singleton Core와 별개의 실행 계약이다. [config.go:64–79](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/config.go#L64), [core.go:1241–1245](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1241)

**3. recorder의 ISR 상태 전이**

각 slot의 ISR은 현재 step과 세 개의 제안 참조를 유지한다. `Record`의 상태 전이는 다음과 같다.

| 입력 step `s`와 현재 `t`의 관계 | Step | FirstCurrent | AggregateCurrent | AggregatePrior |
|---|---|---|---|---|
| `s<t` | 유지 | 유지 | 유지 | 유지 |
| `s=t` | 유지 | 유지 | 기존 값과 입력 제안의 최댓값 | 유지 |
| `s=t+1` | `s` | 입력 제안 | 입력 제안 | 이전 AggregateCurrent |
| `s>t+1` | `s` | 입력 제안 | 입력 제안 | `nil` |

반환 summary에는 변경 후 `Step`, `FirstCurrent`, `AggregatePrior`가 들어간다. 같은 step의 후속 제안은 최초 제안을 바꾸지 않지만 aggregate를 높일 수 있다. [protocol.go:88–110](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/protocol.go#L88)

Core의 일반 recorder는 slot 0과 step 4 미만을 거부한다. 신규 payload는 검증·staging하고, hash만 받은 경우에는 이미 알려진 value 또는 ISR의 기존 제안인지 확인한다. ISR에 넣는 proposal에서는 payload를 제거한다. 상태 기록 후 `commits.Sync(ctx)`가 성공해야 summary를 반환한다. 이는 코드에 나타난 내구성 호출 순서이며 저장장치의 실제 보장을 시험한 결과는 아니다. [core.go:1371–1376](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1371), [core.go:1598–1632](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1598)

이미 결정한 slot에 대한 요청은 결정한 proposal을 `FirstCurrent`와 `AggregatePrior`에 넣고, `max(결정 step, 요청 step)`을 반환한다. 이 응답은 경쟁 proposer가 기존 결정으로 수렴하는 입력이 된다. [core.go:1552–1596](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1552)

**4. 정상 제안과 경쟁 proposer의 경로**

`proposeOwned`는 pipeline permit을 얻고 slot을 예약한다. 실패 후 반환된 vacant slot을 먼저 재사용하며, 재사용 여부를 `runSlot`에 전달한다. 결정한 값이 자신이 요청한 값과 hash 및 실제 바이트 모두 일치할 때만 성공을 반환한다. 다른 proposer의 값이 결정되면 그 결정을 설치하고 반복하여 자신의 요청을 다시 제안한다. [core.go:353–439](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L353), [core.go:683–706](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L683)

`runSlot`은 초기 proposal에 최댓값 priority인 `ff…ff`를 부여하고 step 4에서 시작한다. 그러나 최초 leader가 아니거나 재사용 slot이면 랜덤 priority로 바꾼다. 이후 round의 phase 0에서도 새 priority를 뽑는다. 랜덤 값은 `crypto/rand`를 사용하며 전부 0이거나 전부 `ff`이면 마지막 바이트를 1로 바꾼다. [protocol.go:34–42](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/protocol.go#L34), [core.go:985–1013](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L985)

각 step에서 구성원들에게 병렬 Record를 보내고 **처음 성공한 quorum만** 수집한다. 모든 응답을 기다리지 않으며 반환 시 자식 context를 취소한다. 수집 단계는 성공 응답 수를 세고, 최종 결정 설치 단계에서 recorder ID의 중복·membership 등을 검증한다. [core.go:1182–1229](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1182)

| phase | 모든 summary가 요청 step에 도달했을 때 수행하는 동작 |
|---|---|
| `step%4=0` | step 4이고 모든 `FirstCurrent`가 같은 최고 priority 제안이면 즉시 결정한다. 그렇지 않으면 `max(FirstCurrent)`를 채택한다. |
| `step%4=1` | phase별 추가 동작 없이 다음 step으로 이동한다. |
| `step%4=2` | 현재 proposal이 `max(AggregatePrior)`와 같으면 결정한다. |
| `step%4=3` | `max(AggregatePrior)`를 채택하고 다음 round로 이동한다. |

최댓값 후보 집합이 비어 있으면 오류다. step 증가가 overflow하여 4 미만이 되면 오류를 반환한다. [core.go:1087–1135](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1087)

summary의 step이 서로 맞지 않으면 가장 높은 step의 summary를 고르고 그 `FirstCurrent`를 채택하여 해당 step에서 다시 시작한다. 적절한 상위 summary나 최초 제안이 없으면 catch-up 오류다. payload가 없는 제안은 로컬 staged value를 먼저 찾고, 없으면 recorder들에 병렬 Fetch를 보낸다. hash가 맞는 첫 비어 있지 않은 결과를 저장하고 나머지 fetch를 취소한다. [core.go:1070–1084](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1070), [core.go:530–585](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L530)

**5. leader 순서와 내부 제어 slot**

고정 구성에서 epoch는 `(slot-1)/16`이다. 최초 `2N+1`개 epoch에서는 member 배열을 epoch 번호에 따라 회전하여 leader 순서를 정한다. 이후 epoch에서는 직전 epoch의 첫 slot에서 합의한 순서를 사용한다. 다음 순서를 담는 제어 slot은 `epoch+1 >= 2N+1`인 epoch의 첫 slot부터 예약된다. 따라서 3-node 구성에서는 slot 97이 첫 schedule 제어 slot이고, 그 값은 slot 113부터 시작하는 epoch에 적용된다. [core.go:709–738](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L709), [core.go:794–813](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L794)

이 제어 slot에서는 사용자 value 대신 `51 4c 44 52 00`과 JSON 배열을 합의하고, 사용자 요청은 이후 slot에서 계속 제안한다. 순서는 모든 member가 정확히 한 번씩 나오는 배열이어야 한다. 성능 표본이 있는 두 후보의 순서는 평균 epoch 완료 시간이 짧은 쪽을 앞세운다. epoch 완료 시간의 갱신식은 최초 표본 이후 `(기존 평균+새 표본)/2`다. [core.go:387–410](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L387), [core.go:938–960](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L938), [core.go:3345–3366](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L3345)

재구성 모드의 구성 시작을 S(genesis 1, 성공 terminal 이후 T+1), 구성원 수를 N이라 하면 상대 epoch는 `e=(slot−S)/16`이다. `e<2N+1` 동안 `e mod N`부터 member 배열을 회전한다. 이후에는 `S+16(e−1)`에서 결정한 순서를 조회한다. 제어 slot은 `e+1>=2N+1`을 만족하는 `S+16e`다. 압축으로 제어 값이 사라지면 해당 epoch key와 일치하는 검증된 base order를 사용하며, 사용할 순서가 없거나 잘못되면 오류다. 재구성 모드의 내부 epoch key는 상대 e가 아니라 `S+16e`이고 고정 구성의 key는 `(slot−1)/16`이다. [상대 epoch·순서](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L730), [epoch key](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L3337)

이는 현재 코드가 계산하는 preference다. 이 계산이 특정 환경에서 최적의 leader를 선택한다는 성능 보장은 도출하지 않는다.

**6. certificate의 형식과 검증**

certificate는 config ID, slot, step, 결정 proposal의 참조, summary 참조들을 JSON으로 직렬화한다. proposal 참조에는 priority·proposer ID·hash만 있으며 payload는 없다. 0이 아닌 `ReconfigurationID`만 선택적으로 포함한다. 디코더는 unknown field와 뒤따르는 JSON 데이터를 거부한다. [certificate.go:10–29](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/certificate.go#L10), [certificate.go:51–90](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/certificate.go#L51), [certificate.go:108–117](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/certificate.go#L108)

주요 검증 계약은 다음과 같다.

| 검증 대상 | 조건 |
|---|---|
| 결정의 값 | slot이 0이 아니고 payload의 SHA-256이 proposal hash와 같아야 한다. |
| 구성 | 직접 전달한 Decision의 ConfigID는 0 또는 해당 slot의 config ID여야 한다. |
| recorder 집합 | 모두 해당 구성원이어야 하며 ID 중복이 없어야 한다. |
| quorum | 고유 recorder 수가 `floor(N/2)+1` 이상이어야 한다. |
| step | 모든 summary의 step이 decision step과 같아야 한다. |
| 빠른 결정 | 정확히 step 4, 지정 leader의 최고 priority proposal이어야 하며 모든 `FirstCurrent`가 그 proposal과 같아야 한다. |
| phase 2 결정 | `max(AggregatePrior)`가 결정 proposal과 같아야 한다. |
| 다른 phase | 거부한다. |

근거는 [core.go:2591–2627](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2591), [core.go:2659–2690](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2659)이다.

`AcceptCertifiedValue(s)` 경로는 추가로 외부 `DecidedValue`의 slot·hash·payload가 certificate와 일치하고 certificate config ID가 정확히 일치해야 한다. `decodeCertificate` 자체는 config ID를 별도 반환하므로 이 결합 검사가 중요하다. [core.go:2933–2947](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2933)

현재 검사 범위를 다음과 같이 제한해서 해석해야 한다.

- certificate에는 서명이 없고 이 검증 함수도 서명을 확인하지 않는다. 인증된 송신자와 신뢰 경계는 이 slice 밖의 계약이다.
- phase 2 검사에는 `Step>=4`라는 독립 조건이 없다. 정상 생성기는 4부터 시작하지만 validator의 수용 조건은 그보다 넓다.
- phase 2 검사에는 proposer가 member인지 확인하는 독립 조건이 없다.
- `IsQuorum(receipts)`는 현재 구성의 고유한 `Accepted=true` ID 수만 센다. receipt들 사이의 slot·hash 일치는 검사하지 않는다. [core.go:3148–3159](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L3148)

이 항목들은 관찰한 구현 범위이며, 이 단계에서 취약점 판정이나 수정 권고로 확장하지 않는다.

**7. 결정·전파·연속 prefix의 구분**

floor 위의 결정을 새로 설치하면 `decided[slot]`과 value를 보관하고 해당 recorder 상태를 삭제한다. 처음에는 `durable=false`, `logged=false`다. 보존된 결정과 같은 slot의 다른 hash 또는 다른 payload는 거부하고, 같은 값의 재수신은 허용한다. floor 이하의 검증된 직접 Decision은 재설치 없이 성공할 수 있다. 고정 구성의 tip은 `tip+1` 결정이 존재하는 동안에만 전진한다. 따라서 **알려진 결정, 연속 tip, 로컬 내구성, 전파 완료는 별개의 상태**다. [core.go:2197–2234](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2197), [core.go:3370–3384](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L3370)

`Propose`는 앞선 gap을 복구하고 `completeDecision`을 호출한다. completion은 `WaitTip(slot)` 후 복수 member라면 `SendDecision`의 성공을 요구한다. `ProposeCertified`는 이 단계를 생략하며, 주석은 client ACK 전에 다른 voter에 결정을 설치해야 한다고 명시한다. [core.go:302–306](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L302), [core.go:422–435](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L422), [core.go:2157–2177](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2157)

`AcceptCertifiedValueForAck`는 로컬 노드가 certificate에 포함되고, 해당 proposal을 실제 ISR에서 찾으며 value도 durable로 표시되어 있으면 hint 설치 경로를 사용한다. 그렇지 않으면 일반 durable 설치를 수행한다. certificate에 ID가 있다는 사실만으로 최적화하지는 않는다. [core.go:2717–2743](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2717)

**8. ReadIndex의 정확한 반환 의미**

`ReadIndex`는 observer, 비투표 상태, 진행 중인 재구성 freeze 상태에서 참여를 거부한다. singleton이면 로컬 tip을 반환한다. 복수 member에서는 자기 tip과 병렬 `ReadTip` 결과 중 처음 성공한 quorum을 모은 뒤 **그 표본에서 가장 큰 tip과 해당 응답자 ID**를 반환한다. 전체 cluster의 최댓값을 기다리지 않으며 로컬 replica를 catch-up시키지도 않는다. 동률일 때 source ID는 응답 처리 순서에 영향을 받는다. [core.go:233–239](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L233), [core.go:603–659](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L603)

`ReadIndexExcluding`은 지정 voter에 연락하지 않고 quorum에도 세지 않는다. **원래 구성의 quorum 크기는 유지**한다. 자기 자신을 제외하면 즉시 실패한다. 따라서 3-node에서 한 노드를 제외한 경우 남은 두 노드가 모두 성공해야 한다. [core.go:597–601](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L597), [core.go:625–655](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L625)

완료된 쓰기를 놓치지 않는다는 주석은 write의 learner quorum과 read quorum의 교집합에 의존한다. 그러나 Core는 `SendDecision`의 ACK 집합을 받지 않는다. 테스트용 transport는 `AcceptDecision` 성공을 과반수까지 세지만, 이것은 **테스트 모델의 정의**다. production transport의 같은 보장은 이번 범위에서 입증하지 않았다. [core.go:588–590](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L588), [protocol_test.go:362–412](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/protocol_test.go#L362)

read barrier는 `51 52 44 42 00` 뒤에 16바이트 nonce를 붙인 복제 value다. `ReadIndex` 자체는 barrier를 제안하지 않는다. [types.go:245–256](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/types.go#L245)

**9. Learn, 페이지, 재시도와 취소**

`Learn(ctx, from)`은 크기 100의 채널을 만들고 기존 결정 중 `slot>=from`인 항목을 map 순회 순서로 넣는다. 버퍼가 차면 항목을 버린다. 이후 알림도 nonblocking 전송이므로 유실될 수 있다. 신규 알림에는 `from` 필터를 다시 적용하지 않으며 context 취소 시 listener만 제거하고 채널은 닫지 않는다. 따라서 이 API만으로 순서 있는 무손실 재생을 계약할 수 없다. [core.go:2965–2989](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2965), [core.go:2235–2242](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2235)

`DecisionsFrom`은 `from=0`을 1로, 비양수 limit를 256으로 바꾼다. bounded API는 양수 item·byte limit를 요구한다. byte budget은 value와 certificate의 합이며 첫 항목부터 budget을 넘으면 오류다. 뒤의 항목이 넘으면 그 직전까지의 연속 prefix를 반환한다. 반환 payload와 certificate는 복사한다. [core.go:2992–3051](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2992)

오류와 재시도의 구체적인 경계는 다음과 같다.

| 상황 | 현재 동작 |
|---|---|
| quorum 부족 | `ErrQuorumUnavailable`을 반환한다. Record 수집은 첫 하위 오류도 `%w`로 보존한다. |
| decision 전파 실패 | quorum 오류로 감싸지만 하위 오류는 `%v`로 출력한다. |
| 제안 실패 후 slot | 미결정 slot은 vacant 목록에 돌려주며 다음 시도는 최고 priority 경로를 사용하지 않는다. |
| 경쟁 값 결정 | 같은 호출 내부에서 자신의 값을 다시 제안한다. 일반적인 round 상한이나 backoff는 없다. |
| 동일 payload 재호출 | 별도 slot에서 다시 결정될 수 있다. exactly-once 계약이 없다. |
| 대기 취소 | pipeline, quorum, fetch, prefix 대기에서 context 취소를 관찰한다. |
| LocalMode Record | 소유한 receipt를 `WithoutCancel`로 끝낸 후 원래 context 취소를 확인한다. 기본 시도 한도는 3회다. |
| context 없는 수신·저장 API | 일부 sync는 `context.Background()`를 사용하므로 호출자 취소 계약이 없다. |

근거는 [core.go:963–982](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L963), [core.go:1163–1229](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1163), [core.go:2458–2467](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2458), [core.go:2811](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2811), [core.go:3119–3133](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L3119)이다.

일반 `Propose`는 빈 value를 입구에서 명시적으로 거부하지 않지만 staging은 빈 value를 거부하고 hydration도 빈 결과를 성공으로 취급하지 않는다. 따라서 빈 payload를 지원하는 일관된 정상 계약은 이 코드에서 확인되지 않는다. [core.go:313–324](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L313), [core.go:453–455](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L453)

**10. 테스트 정의와 아직 입증하지 않은 보장**

아래는 읽어서 확인한 테스트의 기대다. **이번 분석에서 통과 여부를 확인한 테스트는 없다.**

| 테스트 정의 | 확인하도록 작성된 조건 |
|---|---|
| [protocol_test.go:119, 146](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/protocol_test.go#L119) | ISR의 동일·인접·과거·도약 step 전이와 고정 seed 10,000개 입력의 단조성을 검사한다. |
| [protocol_test.go:181, 195, 215](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/protocol_test.go#L181) | 튜플 비교 순서, certificate의 payload 제외, unknown `version` 필드 거부를 검사한다. |
| [protocol_test.go:300](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/protocol_test.go#L300) | epoch leader의 빠른 결정을 허용하고 다른 proposer의 빠른 결정을 거부한다. |
| [protocol_test.go:735, 915, 926](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/protocol_test.go#L735) | leader 장애 시 진행, 3개 중 1개만으로 결정 불가, 실패 slot 재사용을 검사한다. |
| [protocol_test.go:939](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/protocol_test.go#L939) | 두 경쟁 명령이 slot 1·2에 보존되고 replica들의 hash가 일치하는지 검사한다. |
| [core_test.go:241, 266, 284](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core_test.go#L241) | 초기 holder 장애 이후 phase 2 결정, 남은 ReadTip 취소, completion 재호출을 검사한다. |
| [core_test.go:391](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core_test.go#L391) | `ProposeCertified`가 `SendDecision`을 호출하지 않는지 검사한다. |
| [protocol_test.go:1028](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/protocol_test.go#L1028) | stale replica가 다른 source의 index를 얻되 로컬 tip은 그대로이며, 추가 장애 시 읽기가 실패하는지 검사한다. |
| [read_index_excluding_test.go:55, 104, 129, 158](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/read_index_excluding_test.go#L55) | 제외 voter 미접촉, 원래 quorum 유지, 정상 잔여 quorum 성공, 자기 제외 실패를 검사한다. |
| [core_bounded_test.go:11–76](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core_bounded_test.go#L11) | 연속 페이지·budget·복사 분리·최대 slot·오류 경계를 검사한다. |
| [core_memory_test.go:32–78](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core_memory_test.go#L32) | 1 MiB 페이지 budget에 대한 반환 payload와 allocation 상한을 검사한다. |

이 증거만으로 임의의 메시지 순서에서의 합의 안전성, 경쟁 하의 유한 시간 종료, Byzantine 내성, production transport를 포함한 선형화 가능 읽기, crash·실제 저장장치의 내구성, 전체 메모리 상한을 입증하지 않는다. 후속 EGM 독립 검토에서는 위의 수용 조건과 외부 의존 계약을 검토 입력으로 사용할 수 있다.

`mistake_candidates: []`

**문서 보정: identity·크기·floor·페이지의 정확한 경계**

이 보정은 HEAD `8acb6da`의 소스 대조 결과이며 새 실행 결과가 아니다. 직렬화 필드·태그·codec 규칙은 [형식 부록](quepaxa-go-formats-2026-10-30.md)에 모았다. certificate의 선택적 `reconfiguration_id`는 각 summary에 속한다.

`NewLearner`는 WAL의 기존 identity를 재사용하거나 무작위 32바이트를 생성해 `BindIdentity`로 저장한다. random·bind 실패 또는 길이 불일치는 생성 오류다. `WALIdentity()`의 소문자 hex를 후보 등록·목표 member·admission probe에 사용한다. 편입된 voter의 재개방은 member의 WALIdentity와 일치하는 원래 WAL identity를 요구한다. [생성·재개방](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/config.go#L117), [등록·probe 상세](quepaxa-go-membership-evidence-2026-10-30.md)

128KiB 검사는 `proposeOwned`와 `stageValue`에 있다. staging은 빈 값과 hash 불일치도 거부한다. 일반 Decision 검증의 SHA-256 검사와 certified-value metadata binding에는 보편적인 최대 길이 검사가 없다. 따라서 이 상수를 모든 수신 경로의 상한으로 구현하면 현재 수용 동작과 달라진다. checkpoint codec의 별도 길이 검사와도 구분한다. [제안](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L317), [staging](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L453), [일반 결정 검증](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2591), [binding](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2933)

다음 표는 다른 사전조건을 만족한 호출의 floor 경계다. LocalMode의 외부 변이 금지는 별도로 적용된다.

| API | `0 < slot <= floor`의 동작 |
|---|---|
| `AcceptDecision` / 일반 hint | covered 결정을 검증한 뒤 재설치하지 않고 성공할 수 있다. 개별 QDEC를 다시 쓰지 않는다. 과거 payload와의 충돌 비교는 보존된 `decided` 항목에만 적용된다. |
| `EnsureDurable` | 검증된 base coverage를 성공으로 인정한다. slot=0은 오류다. |
| `EnsureDurableThrough` | base로 덮인 prefix에는 개별 marker가 필요 없다. context·그 밖의 사전조건은 여전히 적용된다. |
| `Record` | `ErrCompacted`로 거부한다. |
| 고정 구성 `AcceptCertifiedValues` / hints | 모든 metadata binding 후 floor 사전검사에서 거부한다. |
| `CompleteDecision` | `ErrCompacted`로 거부한다. |
| `DecisionsFrom(Bounded)` | 정규화한 from이 floor 이하이면 `(nil, tip, ErrCompacted)`다. |
| `DurablePrefix` | nonzero through가 floor 이하이면 `ErrCompacted`다. through=0은 영 해시와 성공을 반환한다. |

근거: [covered 설치](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2180), [EnsureDurable](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2016), [prefix durability](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1965), [Record](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1436), [page precheck](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2777), [completion](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2151), [paging](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L3017), [DurablePrefix](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L3092).

certified page 설치는 transaction이 아니다. 고정 구성 경로는 전체 항목의 certificate/metadata binding과 floor 사전검사를 먼저 하지만, 전체 합의 검증과 설치는 입력 순서대로 `acceptDecision`에서 수행한다. durable 경로는 각 항목 설치 뒤 QDEC를 Append하고 페이지 마지막에 한 번 Sync한다. 뒤 항목의 검증·Append 실패나 마지막 Sync 실패는 앞 항목의 설치·Append를 롤백하지 않는다. hints도 순차 설치 중 부분 변경이 가능하다. [고정 구성 페이지](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2763)

재구성 경로는 입력을 복사해 slot순으로 정렬하고 control decode를 사전검사한다. 하나라도 재구성 control이면 hints를 포함한 페이지 전체를 durable 경로로 승격한다. 그 뒤 각 항목을 binding·검증·설치·Append하며 terminal에는 추가 durability 처리가 있다. 이 경로 역시 뒤 항목 실패 전에 앞 항목이 바뀔 수 있으며 고정 구성의 전체 metadata/floor 사전검사와 동일하지 않다. [재구성 페이지](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2825)

정확한 paging signatures는 다음과 같다.

```go
func (c *Core) DecisionsFrom(from Slot, limit int) ([]DecidedValue, Slot, error)
func (c *Core) DecisionsFromBounded(from Slot, itemLimit, payloadByteLimit int) ([]DecidedValue, Slot, error)
```

| 조건 | 반환 계약 |
|---|---|
| 정상 페이지 | `(연속 page, 현재 tip, nil)`이다. value·certificate는 복사한다. tip은 페이지 마지막이나 다음 cursor가 아니며 페이지 끝보다 클 수 있다. |
| 정규화 | 두 API 모두 from=0을 1로 바꾼다. 일반 API의 비양수 limit는 256이다. |
| bounded limit 비양수 | `(nil, 0, error)`다. |
| from > tip | 빈 non-nil page와 현재 tip, nil error를 반환한다. |
| 정규화한 from <= floor | `(nil, tip, ErrCompacted를 감싼 error)`다. recovery base 처리는 이 API 밖의 책임이다. |
| 첫 항목의 value+certificate가 byte budget 초과 | `(nil, tip, error)`다. |
| 뒤 항목이 budget 초과 | 앞선 연속 page와 tip, nil error를 반환한다. |
| 순회 중 결정 누락 | `(nil, 0, error)`이며 이전에 모은 부분 page도 반환하지 않는다. |

계속 읽을 때는 마지막 반환 항목의 slot+1을 사용한다. 빈 페이지에서는 전진할 cursor가 없다. [paging 구현](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2992)
