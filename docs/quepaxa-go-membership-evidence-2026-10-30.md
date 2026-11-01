편집 주: 아래의 실행·정리·무수정 진술은 최초 근거 수집 당시의 기록입니다. 이번 문서 보정은 허용된 문서만 수정하고 테스트를 실행하지 않았으며, 기존 패키지 PASS 기록을 그대로 보존했습니다.

분석 대상은 `/Users/cypark/.codex/worktrees/quepaxa-go-reverse-spec/rhiza`, HEAD `8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd`입니다. `go.mod`와 `pkg/quepaxa` 존재를 먼저 확인했고, 종료 시에도 HEAD와 깨끗한 작업 트리를 확인했습니다. 아래는 현재 코드에서 도출한 멤버십·통합 계약이며, 역사적 의도나 논문과의 동등성은 주장하지 않습니다.

**멤버십 전환 계약**

기존 구성을 C, 목표 구성을 C′, freeze 슬롯을 F, terminal 슬롯을 T라고 하면 `T=F+16`입니다. C′의 슬롯 소유권은 성공 terminal 다음인 T+1부터 시작합니다.

새 구성의 leader epoch도 `S=T+1`에서 다시 시작합니다. 상대 epoch는 `e=(slot−S)/16`이며 첫 `2N+1`개 epoch는 `e mod N`부터 member 배열을 회전합니다. `e+1>=2N+1`인 epoch의 `S+16e`가 schedule 제어 slot이고, exploration 이후에는 `S+16(e−1)`의 순서를 사용합니다. abort는 새 구성을 시작하지 않습니다. [leader 계산](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L730), [순서 조회](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L767), [성공 전환](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration.go#L397)

learner의 persistent identity는 `NewLearner`에서 생성됩니다. 기존 WAL identity가 없으면 `crypto/rand`로 32바이트를 만들고 `BindIdentity`를 호출합니다. random 생성·bind 실패와 32바이트가 아닌 기존 identity는 오류입니다. 기존 identity가 있으면 재사용하며 `WALIdentity()`는 소문자 64자리 hex를 노출합니다. 이 값을 외부 후보 등록, 목표 member의 WALIdentity, 아래 admission probe에 일관되게 전달해야 합니다. 편입 후 voter constructor로 재개방할 때는 member에 기록된 값과 일치하는 원래 WAL identity가 반드시 있어야 합니다. [constructor](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/config.go#L117), [voter 재개방](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/config.go#L141)

| 상태·구간 | 코드에서 확인되는 계약 | 근거 |
|---|---|---|
| 전환 시작 전 | 재구성을 활성화한 비관찰자만 시작할 수 있습니다. 이미 freeze 상태이면 거절합니다. 파이프라인을 독점한 뒤 F를 `tip+1`로 선정합니다. | [reconfiguration.go:23–64][r23] |
| 목표 구성 검사 | ConfigID는 정확히 `C.ConfigID+1`이어야 합니다. 멤버는 비어 있지 않고 ID가 유일해야 하며, 정확히 한 명만 추가하거나 제거합니다. 유지되는 멤버의 전체 구조체 값은 변경할 수 없습니다. | [reconfiguration.go:280–315][r280] |
| 추가 대상 | 퇴역한 NodeID를 재사용할 수 없습니다. WALIdentity는 정규화된 소문자 64자리 hex, 즉 32바이트여야 합니다. 코어의 이 검사 자체는 PublicKey·PeerURL의 유효성을 보장하지 않습니다. | [reconfiguration.go:301–310][r301] |
| F | freeze는 직전 prefix, 목표 구성, T를 담습니다. 연속 prefix에 진입할 때 슬롯·prefix·목표를 검사하고, 인증서 recorder 집합을 `safe`로 보관합니다. | [reconfiguration.go:350–367][r350] |
| F+1…T−1 | 기존 구성으로 15개 슬롯을 drain합니다. `Finish`는 `RecoverThrough(T−1)`와 각 슬롯의 `completeDecision(..., true)`를 수행합니다. | [reconfiguration.go:208–218][r208] |
| T | terminal은 동일한 F·T·목표와 T−1 prefix를 결합해야 합니다. summaries는 freeze hash를 `ReconfigurationID`로 가져야 합니다. | [reconfiguration.go:370–377][r370] |
| 성공 terminal | 제거된 ID를 퇴역 집합에 넣고, C′를 `start=T+1`로 이력에 추가합니다. 이전 abort revision과 leader timing 상태를 지웁니다. | [reconfiguration.go:391–401][r391] |
| abort terminal | 추가 작업만 중단할 수 있습니다. 기존 구성을 유지하고, freeze·terminal 쌍을 최신 abort 증거로 남기며 pending 상태를 해제합니다. 제거 작업은 abort할 수 없습니다. | [reconfiguration.go:379–389][r379] |

F 또는 T가 leader schedule 슬롯과 겹치면 시작 API는 앞선 슬롯을 복구·완료한 뒤 F를 다시 선정합니다. 적용 시에도 해당 경계를 거절합니다. 이미 알려진 슬롯이 `F+15`를 넘는 경우에도 시작을 거절합니다. [reconfiguration.go:65–85][r65], [reconfiguration.go:332–335][r332]

freeze 이후 일반 `Propose`를 차단하는 테스트가 존재합니다. drain 내부에 이미 인증된 추가 freeze control이 나타나는 경우에는 새 전환을 시작하지 않고 no-op으로 처리합니다. [reconfiguration_test.go:68–83][rt68], [reconfiguration.go:350–352][r350]

**구성 반영·투표 활성화·전파 완료는 별도 조건입니다**

| 구분 | 필요한 조건 |
|---|---|
| 구성 이력 반영 | terminal이 연속 결정 prefix에 들어오면서 `applyReconfigurationLocked`가 실행됩니다. sparse 인증 슬롯만으로는 앞선 빈 구간을 건너뛰지 않습니다. [core.go:2260–2293][c2260] |
| 새 구성에서 투표 | 해당 epoch의 terminal이 로컬에서 durable해야 합니다. compact된 terminal은 검증된 membership base에 해당 전환이 남아 있어야 합니다. [core.go:185–220][c185] |
| learner의 투표 | NodeID가 구성에 포함되고, 구성의 WALIdentity와 로컬 WALIdentity가 일치해야 합니다. observer는 항상 비투표자입니다. [core.go:172–202][c172] |
| API 완료 | `Finish`는 terminal의 `completeDecision(..., true)` 결과를 반환합니다. 이 함수는 로컬 durability를 확보하고 연속 tip을 기다립니다. [reconfiguration.go:237–248][r237], [core.go:2115–2162][c2115] |
| 네트워크 terminal 전파 완료 | 기존 구성 quorum과 다음 구성 quorum의 ACK를 모두 요구하며, 새로 추가된 모든 멤버의 ACK도 명시적으로 요구합니다. terminal envelope의 ConfigID는 기존 구성에 속합니다. [transport.go:749–807][t749], [transport.go:810–851][t810] |

따라서 `CurrentCluster`가 새 구성을 보여 준다는 사실만으로 로컬 투표 가능 여부나 관리 작업 완료를 판정하면 안 됩니다. 실제로 terminal을 학습했지만 디스크 barrier를 통과하지 않은 상태에서 투표를 거절하는 테스트가 있습니다. [recovery_admission_test.go:34–83][ra34]

terminal 제안의 recorder 후보는 목표 구성과의 교집합이 아니라 freeze 인증서의 `safe` 집합으로 제한됩니다. 복원용 history 검증도 terminal recorder가 freeze recorder 집합에 포함되는지 검사합니다. [core.go:1019–1035][c1019], [membership_validation.go:62–69][mv62]

**catch-up과 추가 승인**

| 경계 | 구체적인 계약 | 근거 |
|---|---|---|
| 일반 Record | 재구성 모드에서 `slot>tip+16`을 거절합니다. 요청 ConfigID가 해당 슬롯 구성과 일치해야 합니다. | [core.go:1386–1405][c1386] |
| frozen Record | T 이후 요청을 거절하고, F 이후 요청에는 freeze hash와 같은 `ReconfigurationID`를 요구합니다. T에는 terminal control만 허용합니다. | [core.go:1407–1433][c1407] |
| Record catch-up 요구량 | 비활성 모드 또는 slot=0이면 0입니다. 그 외 terminal 요청은 `slot−1`, 일반 요청은 `slot>16`일 때만 `slot−16`, 나머지는 0입니다. 본문이 생략되면 저장된 hash 대응 값을 조회합니다. 로컬 pending terminal 슬롯 또는 디코딩 결과의 terminal 표시로 terminal을 식별합니다. | [reconfiguration.go:440–458][r440] |
| 네트워크 catch-up 연결 | 수신 Record가 요구하는 prefix가 로컬 tip보다 앞서면 송신자에게서 catch-up한 뒤 `Core.Record`를 호출합니다. | [peer_server.go:371–382][ps371] |
| terminal 투표 전 추가 승인 | 추가 성공 terminal에는 admission callback이 필수입니다. callback에 목표 구성, T−1, 그 prefix hash를 전달합니다. abort에는 이 추가 승인 경로를 적용하지 않습니다. | [core.go:1417–1429][c1417] |
| 노드 admission | 현재 구성 대신 `ClusterForSlot(through)`와 비교하여 새 후보를 찾습니다. 각 새 후보에 `VerifyLearner`를 수행합니다. | [node/membership.go:213–223][n213] |
| learner probe | 후보 ID·공개키·WALIdentity를 검사하고, 후보 QUIC endpoint에 through·prefix·WALIdentity를 전송합니다. 응답 cluster·후보 ID·ConfigID·through가 정확히 일치해야 합니다. | [transport.go:931–959][t931] |
| probe 수신 | WALIdentity를 맞춘 뒤 인증된 송신자로부터 durable catch-up을 수행하고 `DurablePrefix(through)`를 비교합니다. prefix 확인 자체는 투표권을 부여하지 않습니다. | [peer_server.go:526–550][ps526] |

**관리 작업·fencing의 신뢰 경계**

노드 관리 작업은 mutex로 직렬화합니다. 재구성 활성화, bucket 존재, 로컬 voter 자격을 요구하며 요청에는 OperationID, ClusterID, 예상 ConfigID, 추가·제거 중 정확히 하나가 필요합니다. 새 작업은 예상 abort revision도 확인합니다. [node/membership.go:22–55][n22]

작업 journal의 키는 OperationID가 아니라 예상 ConfigID와 선택적인 abort 슬롯으로 정해집니다. 같은 키의 저장된 요청과 전체 요청이 다르면 conflict입니다. 목표 검증과 journal 저장 후 guarded `BeginReconfigurationAt`·`FinishReconfigurationAt`를 호출하고, 완료 시 로컬 prefix 내구성과 선택적인 archive 동기화를 확보합니다. [node/membership.go:89–138][n89], [node/membership.go:185–193][n185]

| 요청 종류 | 노드가 추가로 검사하는 조건 |
|---|---|
| 추가 | Fence가 없어야 하며 ID·PublicKey·WALIdentity·PeerURL이 필요합니다. 외부 voter registration의 version·NodeID·ClusterID·nonce·공개키가 후보와 일치해야 합니다. [node/membership.go:67–79][n67] |
| 제거 | 대상이 존재하고 결과가 비어 있지 않아야 하며, 처리 노드 자신을 제거할 수 없습니다. [node/membership.go:81–86][n81] |
| 제거 fence | `Confirmed=true`, 대상 NodeID 일치, WorkloadUID·WALIdentity·Evidence의 비어 있지 않음을 요구합니다. 외부 등록의 nonce와 WALIdentity를 비교합니다. [node/membership.go:196–210][n196] |
| abort·재시도 | 원래 journal에 기록된 추가 작업과 operation/config/abort revision이 맞아야 합니다. 이미 같은 목표가 abort된 경우 persistence 재시도를 허용합니다. 오래된 revision은 conflict입니다. [node/membership.go:150–182][n150] |

**이 코드의 fence 검사는 대상 프로세스 종료, 재생성 방지, 진행 중인 스토리지 쓰기의 종료를 직접 검증하지 않습니다.** WorkloadUID와 Evidence도 이 함수에서는 비어 있지 않은지만 검사합니다. 외부 관리 주체의 확인을 신뢰하는 경계입니다. `MembershipStatus` 역시 관찰용 스냅샷이며 fencing 권한이 아니라는 계약이 명시되어 있습니다. [network/membership.go:114–158][m114]

HTTP 변경 요청은 POST와 membership bearer token을 요구하고, JSON 본문은 64KiB로 제한하며 알 수 없는 필드와 뒤따르는 추가 데이터를 거절합니다. 콜백은 shutdown 추적에 포함되고 30초 제한 및 서버 취소 컨텍스트를 적용받습니다. [network/membership.go:56–104][m56]

**QuePaxa 네트워크 신뢰 경계**

- Record는 해당 슬롯의 구성으로 라우팅하고 ConfigID를 검사합니다. Sync는 현재 voter로 라우팅하되 반환 데이터의 cluster·source·ConfigID를 검사합니다. [transport.go:488–541][t488]
- 송신 연결은 단일 Ed25519 인증서의 공개키를 구성에 고정된 키와 비교합니다. TLS handshake 완료를 기다린 뒤 RPC를 전송합니다. [transport.go:258–299][t258], [transport.go:321–329][t321]
- 수신 voter 권한은 송신자가 **해당 요청의 역사적 구성과 로컬 현재 구성 양쪽에 속하고**, handshake 공개키가 양쪽 공개키와 일치해야 성립합니다. admin token만 가진 비투표자는 Sync만 사용할 수 있습니다. [peer_server.go:334–352][ps334]
- freeze 전파는 기존 구성 과반 ACK에 더하여 freeze 인증서에 포함된 recorder 모두의 ACK를 요구합니다. terminal 전파의 기존·다음 quorum 및 신규 멤버 ACK 요구와 구분해야 합니다. [transport.go:683–735][t683]
- 재구성 control은 `AcceptDecisionHint`로 전달되어도 durable `AcceptDecision` 경로를 사용합니다. [core.go:1907–1928][c1907]
- 저장된 certificate는 ConfigID·slot·step·proposal 참조·summary 목록을 가진 구조입니다. 이 구조에 recorder별 서명 필드는 없으며, 이력 검증은 값 hash·구성·recorder 소속·중복·quorum 등을 확인합니다. 따라서 이를 독립 서명 quorum 증명이나 Byzantine 보장으로 확대할 근거는 확보하지 않았습니다. [certificate.go:10–29][cert10], [membership_validation.go:134–147][mv134], [core.go:2608–2627][c2608]

**history·checkpoint 계약**

`MembershipRecord`는 genesis, 성공한 freeze→terminal 전환 목록, 선택적인 최신 abort 쌍으로 구성됩니다. abort는 목표 구성 활성화 권한이 아닙니다. [membership_history.go:10–22][mh10]

`MembershipHistory`는 성공 전환의 freeze와 terminal이 모두 존재하고 durable한지 검사하며, 값 hash와 terminal 목표 구성을 확인합니다. 반환값은 깊은 복사입니다. pending freeze는 성공 전환 목록에 들어가지 않습니다. [membership_history.go:25–35][mh25], [membership_history.go:80–139][mh80]

history 검증은 명시적으로 신뢰한 bootstrap과 genesis의 정확한 일치에서 시작합니다. 각 전환의 ConfigID·슬롯·hash·quorum·freeze binding·terminal binding을 검증하고 퇴역 ID를 재구성합니다. **이 함수만으로 log floor를 설치하거나 learner를 승인하지는 않습니다.** [membership_validation.go:8–78][mv8]

checkpoint에는 다음 조건을 적용합니다.

| 조건 | 근거 |
|---|---|
| 활성 freeze 중이거나 index가 0 또는 tip 이후이면 membership 추출을 거절합니다. | [membership_history.go:41–46][mh41] |
| 검토하는 성공 전환의 terminal이 checkpoint index 이상이면 거절합니다. | [membership_history.go:80–83][mh80] |
| 고정 구성 모드는 membership history가 포함된 seal을 거절하고, 재구성 모드는 history가 없는 seal을 거절합니다. | [checkpoint_membership.go:54–63][cm54] |
| history 검증 후 `Index+1`의 ConfigID, leader orders, 모든 성공 terminal이 Index보다 앞선다는 조건을 확인합니다. | [checkpoint_membership.go:64–74][cm64] |

history 검증은 전환 쌍으로 구성 권한을 재구성합니다. 이것만으로 모든 drain 값의 존재·실행 결과나 checkpoint 객체의 실제 보존을 증명했다고 볼 수는 없습니다.

**테스트 정의와 실행 결과**

아래는 읽은 테스트가 정의하는 관찰 대상입니다. 개별 테스트별 PASS 로그를 수집한 목록은 아닙니다.

| 테스트 정의 | 확인하도록 작성된 사항 |
|---|---|
| [reconfiguration_test.go:68][rt68] | freeze 중 일반 쓰기 거절, 제거 후 새 구성 쓰기, 제거된 노드의 제안 거절 |
| [reconfiguration_adversarial_test.go:92][rat92] | terminal recorder가 목표 교집합 대신 freeze quorum에 속함 |
| [reconfiguration_adversarial_test.go:267][rat267] | 16-slot 창 안의 terminal도 전체 drain catch-up을 요구하며 hash-only 요청에도 적용됨 |
| [recovery_admission_test.go:34][ra34] | terminal 학습과 로컬 내구성 사이에서 새 구성 투표 거절 |
| [membership_history_test.go:9][mht9] | 미완료 전환 제외, 깊은 복사, 미내구 terminal·누락 freeze 거절 |
| [membership_validation_test.go:10][mvt10] | bootstrap·값·슬롯·quorum·freeze binding·순서 변조 거절 |
| [checkpoint_membership_test.go:79][cmt79], [116][cmt116] | 전환 후 compaction·restart, cold observer restore와 변조 거절 |
| [reconfiguration_abort_test.go:13][rab13], [157][rab157], [189][rab189] | abort 후 기존 구성과 revision 보존, 오래된 관리 라운드 거절 |
| [node/membership_operation_test.go:27][not27], [127][not127] | journal 재시도·충돌·fence 검사, abort 이후 새 revision |
| [network/learner_admission_test.go:12][nat12] | 실제 QUIC prefix catch-up·내구성 확인, 잘못된 WALIdentity·prefix 거절 |
| [network/peer_auth_test.go:13][pat13] | voter·admin·잘못된 키·cluster·config 인증 행렬 |

코어의 주요 재구성 fixture는 로컬 WAL과 메모리 `clusterTransport`를 사용합니다. 따라서 그 결과를 실제 네트워크 통합 결과와 동일시하지 않습니다. [reconfiguration_test.go:12–24][rt12]

실제로 실행한 명령은 지정 체크아웃을 명시적 workdir로 사용한 다음 한 번입니다.

```sh
go test -mod=readonly -count=1 -timeout=120s ./pkg/quepaxa
```

정확한 출력과 종료 결과:

```text
ok  	github.com/mrchypark/rhiza/pkg/quepaxa	101.932s
exit_code=0
```

이는 캐시를 재사용하지 않은 **`pkg/quepaxa` 패키지 실행 통과**입니다. 정적으로 센 207개 Test/Fuzz/Benchmark 정의가 모두 실행·통과했다는 뜻은 아닙니다. `pkg/node`·`pkg/network` 테스트, race 검사, fuzz campaign, 클라우드·클러스터·전체 CI는 실행하지 않았습니다. `scripts/e2e-fast.sh`와 `scripts/check-ci.sh`는 이 Go 체크아웃에 없음을 확인했습니다.

정리 확인 결과, 시작한 Go 프로세스 PID 7807과 테스트 바이너리 PID 8042는 종료되었고, 소유한 `go-build681683637` 임시 빌드 디렉터리도 제거되었습니다. 테스트의 WAL 임시 디렉터리는 기존 `t.TempDir`·cleanup 경로로 관리됩니다. 별도 서버·컨테이너·클러스터나 수동 임시 파일을 만들지 않았으며, 실패 로그를 보존할 필요도 없었습니다. 파일 수정·커밋·위임·공유 메모리 쓰기는 수행하지 않았습니다.

`mistake_candidates: []`

[r23]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration.go#L23
[r280]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration.go#L280
[r301]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration.go#L301
[r350]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration.go#L350
[r208]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration.go#L208
[r370]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration.go#L370
[r391]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration.go#L391
[r379]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration.go#L379
[r65]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration.go#L65
[r332]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration.go#L332
[r237]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration.go#L237
[r440]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration.go#L440
[c2260]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2260
[c185]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L185
[c172]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L172
[c2115]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2115
[c1019]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1019
[c1386]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1386
[c1407]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1407
[c1417]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1417
[c1907]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1907
[c2608]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2608
[t749]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/transport.go#L749
[t810]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/transport.go#L810
[t931]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/transport.go#L931
[t488]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/transport.go#L488
[t258]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/transport.go#L258
[t321]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/transport.go#L321
[t683]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/transport.go#L683
[ps371]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/peer_server.go#L371
[ps526]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/peer_server.go#L526
[ps334]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/peer_server.go#L334
[n213]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/membership.go#L213
[n22]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/membership.go#L22
[n89]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/membership.go#L89
[n185]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/membership.go#L185
[n67]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/membership.go#L67
[n81]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/membership.go#L81
[n196]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/membership.go#L196
[n150]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/membership.go#L150
[m114]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/membership.go#L114
[m56]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/membership.go#L56
[mh10]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/membership_history.go#L10
[mh25]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/membership_history.go#L25
[mh80]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/membership_history.go#L80
[mh41]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/membership_history.go#L41
[mv8]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/membership_validation.go#L8
[mv62]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/membership_validation.go#L62
[mv134]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/membership_validation.go#L134
[cm54]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/checkpoint_membership.go#L54
[cm64]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/checkpoint_membership.go#L64
[cert10]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/certificate.go#L10
[rt68]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration_test.go#L68
[rt12]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration_test.go#L12
[rat92]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration_adversarial_test.go#L92
[rat267]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration_adversarial_test.go#L267
[ra34]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/recovery_admission_test.go#L34
[mht9]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/membership_history_test.go#L9
[mvt10]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/membership_validation_test.go#L10
[cmt79]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/checkpoint_membership_test.go#L79
[cmt116]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/checkpoint_membership_test.go#L116
[rab13]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration_abort_test.go#L13
[rab157]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration_abort_test.go#L157
[rab189]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/reconfiguration_abort_test.go#L189
[not27]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/membership_operation_test.go#L27
[not127]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/node/membership_operation_test.go#L127
[nat12]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/learner_admission_test.go#L12
[pat13]: https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/peer_auth_test.go#L13

**문서 보정: 운영 learned ACK 경계**

운영 peer handler의 `OperationLearned`는 Ready 상태를 요구하고, 비어 있지 않은 payload에 execution policy를 검사한다. 재구성 control은 필요한 선행 prefix를 durable catch-up하고 기다린다. handler는 decision의 summaries에 자기 recorder ID가 있으면 `AcceptDecisionHint`, 없으면 `AcceptDecision`을 선택한다. 일반 hint 선택에 live ISR·valueDurable 검사를 추가하지 않는다. 재구성 control hint는 Core에서 durable 경로로 승격된다. [peer handler](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/peer_server.go#L387), [hint 선택](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/peer_server.go#L437), [control 승격](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L1917)

설치 후 handler는 `applyDecisions(ctx, decision.Slot)`이 성공해야 ACK한다. 이 함수는 연속 tip을 기다린 뒤 materializer가 해당 slot까지 도달하도록 `ApplyBatch`를 호출한다. 따라서 앞선 gap·apply·context 오류로 ACK가 실패할 수 있고, 그때 합의 상태는 이미 변경되었을 수 있다. 이 경로는 certificate의 자기 ID 외에도 live ISR의 일치 proposal과 durable value를 확인하는 별도 API `AcceptCertifiedValueForAck`와 다르다. [ACK 전 apply](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/peer_server.go#L452), [연속 prefix·apply](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/network/server.go#L2226), [별도 Ack API](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/quepaxa/core.go#L2717)

이 보정은 소스 대조이며 위의 과거 패키지 실행 결과를 확대하지 않는다. 독립 검토자가 나머지 열거 테스트와 peer wire 전체를 읽었다는 증거는 없으며, clean/full certification이나 독립 재구현 검증을 뜻하지 않는다. 정확한 membership/control JSON은 [형식 부록](quepaxa-go-formats-2026-10-30.md)을 따른다.
