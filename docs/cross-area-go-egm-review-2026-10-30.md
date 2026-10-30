# Go 11개 영역 통합 EGM 검토 기록

상태: **11개 영역의 통합 계약 검토·보정·독립 재검토 완료(지정 경계 범위 한정)**. 고유 Blocker/Major 16건과 참조 Nit 1건을 보정했다. 최종 두 독립 스냅샷 검토 묶음에서 Blocker/Major는 0건이며, Nit도 원 검토자의 후속 확인으로 해소됐다.

## 구성과 범위

대상은 [11개 영역 색인](reverse-spec-index-2026-10-30.md)과 본문·계약·형식 부록 사이의 계약이다. 현재 소스 기준은 `008f8e576ad76425182e10a69b987f10cfb48b2a`다. QuePaxa 문서의 원래 검토 기준은 `8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd`이며, 이력을 지우지 않고 현재 소스에 대한 영역 간 연결을 별도로 확인한다.

EGM의 `codebase_details`와 `comprehension` persona 및 `re-implementation-standard`를 사용한다. 초기 검토는 요청·실행(W), 저장·복구(S), 수명주기·소유권(L) 경계로 나눈다. 검토자는 모두 `fork_context:false`로 시작하며, 문서 작성자와 분리한다. 작성자는 격리된 체크아웃에서 명세만 보정한다. 검토자가 지적한 코드의 한계는 현재 동작으로 문서화하며 원하는 설계로 바꾸지 않는다.

보정 뒤 새 문맥의 독립 검토자가 통합 계약과 소유 문서의 경계 절을 다시 확인했다. 초기 계획은 EGM review-loop의 연속 clean 기준을 참고했으나, 보정 과정 뒤에는 불변인 A 경계 판정을 유지하고 같은 최종 스냅샷의 B 경계·전체 흐름 C를 두 묶음으로 독립 병렬 검토했다. 아래에 그 실제 순서를 구분한다. 전체 5회 직렬 review-loop나 EGM 5단계 승인, 전체 저장소 무결함, 외부 환경 동등성 인증을 뜻하지 않는다. 과거 영역별 검토 기록과 테스트 결과는 보존했다.

## 1차 검토

W `01a18bf5-38f7-74b0-b5af-32ab81990eb7`, S `01a18bf5-3ac0-7671-a80f-a59191d42bb2`, L `01a18bf5-3d44-7171-abda-468076bb3185`가 검토를 완료했다. 문서가 현재 코드와 모순되는 경우 persona 기준으로 Blocker를 부여했다. 이는 실행으로 입증한 취약점의 심각도와 다르다.

총 10건(Blocker 6, Major 4)을 모두 `PROPOSE FIX`로 분류했다. 코드 근거와 원문은 [독립 검토 증거](cross-area-go-review-evidence-2026-10-30.md)를 따른다.

| ID | 심각도 | 경계와 보정 사항 | 상태 |
| --- | --- | --- | --- |
| W-01 | Blocker | SQL 혼합 입력 우선순위와 fingerprint | 보정·해소 확인 |
| W-02 | Blocker | 취소와 commit-unknown·FFI 오류 우선순위 | 보정·해소 확인 |
| W-03 | Major | status 조회와 durability barrier의 구분 | 보정·해소 확인 |
| S-01 | Major | Local checkpoint 재게시의 portable identity 충돌 | 보정·해소 확인 |
| S-02 | Major | QLog 선택과 Core base 설치 사이의 compaction 실패 | 보정·해소 확인 |
| L-01 | Blocker | FFI 설정의 `before-ack` literal | 보정·해소 확인 |
| L-02 | Blocker | learner 단일-source catch-up과 readiness | 보정·해소 확인 |
| L-03 | Blocker | filesystem owner와 QLog 잠금 해제 시도의 구분 | 보정·해소 확인 |
| L-04 | Major | archive lineage와 외부 anchor의 live authority | 보정·해소 확인 |
| L-05 | Blocker | Operator·HTTP recovery probe의 GET 계약 | 보정·해소 확인 |

## 기준 호환성

검증자 `01a18bf7-af36-73f1-81c8-33a3044a1c7f`는 두 기준 커밋 사이의 non-document diff가 비었음을 확인했다. 차이는 QuePaxa Markdown 문서 6개 추가뿐이다. 원래 QuePaxa 기준을 보존하며, 기준 커밋의 차이만으로 소스 동작이 달라졌다고 주장하지 않는다.

## 2차 독립 검토

새 코드 대조 검토자 `01a18bff-b2c2-7b32-9b1c-6ba49123414c`와 이해도 검토자 `01a18bff-b4bf-7950-9db0-60ed5dacaf06`가 각각 독립 검토했다. 코드 대조는 Blocker 2·Major 1, 이해도는 Major 2를 보고했으며 뒤의 2건은 앞의 지적과 중복된다. 두 보고는 모두 읽기 범위가 미완료라고 명시했으므로 clean round로 계산하지 않는다.

| 원본 ID | 관계 | 조치 | 상태 |
| --- | --- | --- | --- |
| R2-codebase_details-01 / R2-comprehension-02 | 같은 anchor committed-retry 예외 | PROPOSE FIX: 먼저 성공하는 분기와 이후 PendingWrite 거절을 구분 | 보정·해소 확인 |
| R2-codebase_details-02 / R2-comprehension-01 | 같은 일반 shutdown·TerminalDrain 구분 | PROPOSE FIX: entry point와 Local owner 조건 명시 | 보정·해소 확인 |
| R2-codebase_details-03 | recovery probe의 규범 소유권 누락 | PROPOSE FIX: HTTP 부록에 wire·검사·오류·기한 계약 배치 | 보정·해소 확인 |

심각도는 각 검토자의 원문을 보존한다. 중복을 제외하면 최초 10건과 추가 3건이다. 이 과정에서 작성한 연결 설명의 과도한 일반화를 재개방해 보정하며, 과거 검토 통과를 근거로 무시하지 않는다.

다음 검토의 범위는 통합 문서의 E01–E17·C0–C7 및 관련 소유 문서의 경계 절과 직접적인 검증·호출 지점이다. 각 엔진·공급자·의존 라이브러리 내부 전체를 재귀적으로 감사하는 일은 이 영역 간 검토에 포함하지 않는다. 이 구분은 원래 작업 범위를 명확히 한 것이며, 2차 보고의 미완료 상태를 소급해 바꾸지 않는다. 요청한 경계의 읽기가 빠지면 이후 검토에서도 clean으로 계산하지 않는다.

## 3차 독립 검토

A `01a18c09-9360-7840-9b38-6f4de1815ef5`, B `01a18c09-956a-7523-a2a7-a86412968a9e`, C `01a18c09-9710-7ac3-bb75-c2c700791e38`가 담당 경계를 완료했다. A·C에는 추가 지적이 없고 B에는 Major `R3-B-01`이 있다. 따라서 이 회차도 clean이 아니다.

`R3-B-01`은 독립 attestation 시각 검사의 과도한 일반화다. `PROPOSE FIX`로 분류하고 operator 부록과 통합 계약에 top-level/barrier 시각의 nonempty 검사, storage의 RFC3339·quiet-window·미래 종료 상한, 최대 과거 나이 제한의 부재, 별도의 ledger/Pod scope 재확인을 구분했다. 당시 고유 지적은 14건이었으며, 이후 최종 독립 검토에서 보정 결과를 확인했다.

세 보고의 실제 읽기 범위와 담당 edge 및 C7 검증 지점은 [원문 증거](cross-area-go-review-evidence-2026-10-30.md)에 보존한다.

## 4차 독립 검토와 남은 보정

A `01a18c11-6f45-76d2-94ed-fabc04dd408c`와 C `01a18c11-7261-71e2-aa11-afefde0d0f3a`에는 지적이 없었다. B `01a18c11-70dc-7350-9fc6-94c3a99a9944`가 Major `R4-B-01`을 보고했으므로 회차 전체를 clean으로 계산하지 않는다.

`R4-B-01`은 `QuietWindowSeconds`의 unchecked duration 변환이다. `PROPOSE FIX`로 분류하고 `time.Duration(QuietWindowSeconds)*time.Second`의 정확한 비교식과 overflow 예외, 64-bit 정적 예시를 operator 부록과 통합 계약에 보정했다. 선언값의 최소 30초 검사가 언제나 실제 30초 관측 간격을 보장한다고 쓰지 않는다. 이 예시는 실행하지 않았다. 최초 10건과 2차 3건, 3차 1건, 4차 1건으로 고유 지적은 15건이다.

A 담당 E01–E08/E17, C2–C4 및 배정된 C7 행은 3·4차에 연속 지적이 없었다. 4차 이후의 시각·duration 보정에서 그 절들의 내용을 추출해 비교했고 바뀌지 않았음을 확인했다. 따라서 이후에는 그 판정을 유지하고 변경한 B 경계 및 전체 흐름 C의 새 문맥 검토를 이어간다. 전체 엔진·의존성 감사를 통과했다고 확대하지 않는다.

## 5차 독립 검토와 timeout 보정

B `01a18c16-d518-7512-b270-aef662d44962`는 Blocker `R5-B-01`, C `01a18c16-d64d-7e72-b1a7-f18cb8b4dbcc`는 지적 없음으로 보고했다. 따라서 이 회차 역시 clean이 아니다.

`R5-B-01`은 주입한 `http.Client.Timeout`이 음수일 때의 예외다. `PROPOSE FIX`로 분류하고 nil·zero·양수·음수별 실제 정규화와 caller context 경계를 operator 부록과 통합 계약에 보정했다. 현재 Go 1.27.3의 `net/http.Client.deadline`도 양수 timeout에만 client deadline을 만드는 것을 확인했다. 고유 지적은 16건이다.

그 보정 뒤 같은 고정 스냅샷을 두 개의 별도 fresh-context 검토 묶음(6·7차, 각각 B 코드 경계와 C 전체 흐름)에 병렬로 제공했다. 두 묶음 사이에는 명세를 수정하지 않는다. 이는 기본 5회 직렬 review-loop를 그대로 실행한 기록이 아니며, 지적 해소와 두 독립 최종 스냅샷 점검을 마치기 위한 제한된 EGM 적용이다. 전체 EGM 5단계 승인이나 전체 저장소 무결함 인증으로 표현하지 않는다. A 담당 절은 다시 추출 비교해 3·4차 이후 내용이 바뀌지 않았음을 확인했다.

## 구조·소스 검증 스냅샷

검증자 `01a18bf7-af36-73f1-81c8-33a3044a1c7f`가 29개 문서(11개 규범 쌍 22개, QuePaxa 근거 부록 3개, 통합 계약·색인, 이 기록·원문 증거)를 확인했다. 당시 스냅샷에서 로컬 대상 1,136개, numeric line 참조 928개, heading fragment 63개에 누락·범위 초과·깨진 앵커가 없었다. reference-style 사용 192개도 모두 정의돼 있었다. 통합 문서의 edge는 중복 없는 E01–E17이며 모든 영역이 owner 링크로 연결됐다.

확인 방법은 R/Sh의 메모리 내 파싱, 문서를 기준으로 한 상대경로 해석, 절 앵커 생성·중복 처리 및 파일·행수 대조다. GitHub renderer 자체를 실행한 검증이나 모든 인용 행의 의미 검증은 아니다. `HEAD` 대비 작업 트리의 non-document diff, `8acb…`에서 `008f…`까지의 non-document diff가 모두 비었다. 과거 테스트 결과를 새 실행으로 계산하지 않았다. 이 스냅샷 뒤 보고서가 추가되면 최종 통합에서 링크 대상을 다시 확인한다.



## 최종 독립 판정

| 경계·묶음 | 검토자와 결과 | 완료 범위 |
| --- | --- | --- |
| A 3·4차 | `01a18c09-9360-7840-9b38-6f4de1815ef5`, `01a18c11-6f45-76d2-94ed-fabc04dd408c`: 모두 지적 0 | E01–E08/E17, C2–C4 및 담당 C7; 이후 규범 서술 불변 확인 |
| B 6·7차 | `01a18c1e-6622-73e1-aad2-e9b5fc2b8e09`, `01a18c1e-69cd-70b2-bebc-bcb47fa28b35`: 모두 지적 0 | E09–E16, C5/C6 및 담당 C7의 직접 소스·검사 조건 |
| C 6·7차 | `01a18c1e-6773-7cd2-84a1-8b152ab421d3`: 지적 0; `01a18c1e-6e25-73f1-ab11-1e770b9907ac`: B/M 0, Nit 1 후 해소 | 전체 C0–C7, E01–E17 및 소유 규범 절·필요한 직접 소스 |

두 최종 묶음은 같은 명세를 병렬로 읽었고 서로의 결과를 증거로 사용하지 않았다. 둘 사이에 규범을 바꾸지 않았다. 이후 변경은 완료 상태·탐색 링크, Nit의 소스 행 번호, 표 안의 literal pipe escaping뿐이다. 마지막 형식 보정은 C2의 `||`를 GFM 표에서 같은 식으로 표시하도록 escape한 것이며 식의 의미를 바꾸지 않았다. 검증자는 hub 표 3개·42개 표 텍스트 행에 잘못된 cell 구분자가 없고 hub의 로컬 링크 120개가 존재함을 확인했다. `R7-C-01`은 `Append` 링크를 `wal.go:500`에서 실제 선언인 `:662`로 보정했고 원 검토자가 확인했다. 그 검토자의 “C7 13 rows”는 데이터 11행과 header/separator를 합한 표 텍스트 13행이었다고 후속 확인했으며 원문은 보존했다.

| 추가 ID | 조치 | 최종 상태 |
| --- | --- | --- |
| R3-B-01 | PROPOSE FIX: timestamp acceptance의 정확한 조건 | 해소 확인 |
| R4-B-01 | PROPOSE FIX: quiet-window duration overflow 예외 | 해소 확인 |
| R5-B-01 | PROPOSE FIX: 주입 HTTP client의 음수 timeout 예외 | 해소 확인 |
| R7-C-01 | PROPOSE FIX: E03 Append 소스 참조 위치 | 원 검토자 후속 확인 완료 |

이 범위에서 미해소 지적은 없다. S-01/S-02, attestation 시각·overflow 및 timeout 예외는 현재 코드의 한계나 허용 동작을 명세로 밝힌 것이며 런타임에서 고쳤다는 뜻이 아니다. 정적으로 도출한 장애·overflow 시나리오는 실행하지 않았다.

## 검증·이력·정리

이번 작업은 명세만 바꿨다. 새 Go 테스트·빌드·race·provider SDK/emulator·실제 cloud·Kubernetes·Chaos Mesh·성능 시험은 실행하지 않았다. 이전 작업의 425 pass·2 skip을 이번 결과로 집계하지 않는다. 검증은 독립 소스 대조, 규범 링크·행·절 앵커, 변경 범위, 과거 기록 보존에 한정한다.

이번 작업에서 만든 격리 체크아웃 4개와 그 임시 부모 디렉터리를 모두 제거하고 경로 부재를 확인했다. 별도 테스트 서버·컨테이너·데이터를 생성하지 않았다. 이전 작업의 조사용 로그와 다른 작업자의 자원은 건드리지 않았다.

## 최종 구조 검증 결과

위 상태·원문·형식 보정 뒤 검증자가 같은 29개 문서를 다시 확인했다. Inline link 979개, reference definition/use 203/192개, 로컬 대상 1,182개, numeric line 참조 963개, 절 앵커 65개가 모두 유효했다. hub의 3개 표·42개 표 텍스트 행에 잘못된 cell pipe가 없었다. Edge E01–E17, 11개 영역, C7 데이터 11행과 실제 Append 선언 `wal.go:662`도 확인했다. GitHub renderer 실행은 검증 범위 밖이다.

HEAD 대비 작업 트리와 `8acb…` 대비 `008f…`의 non-document diff는 모두 비었다. Untracked 36개 파일은 모두 docs 아래에 있다. 시작 시점 SHA-256과 비교해 과거 영역별 EGM·기존 검증 기록이 바뀌지 않은 것도 확인했다. 독립 소스 검토를 제외한 이 마지막 결과는 문서의 구조·변경 범위 검증이며 런타임 테스트 통과가 아니다.

## 실수 후보 처리

초기 W/S는 후보 없음이었다. 초기 L·작성자의 추측 경로와 잘린 출력, W 편집자의 잘못된 검색 경로는 실제 파일·필요한 범위를 다시 읽은 뒤 결론을 냈으며 누락 근거로 쓰지 않았다. S 편집자의 불필요한 Ruby version 확인은 helper 실행이나 파일 변경으로 이어지지 않은 작업 한정 실수로 분류했다. 부모의 문맥이 맞지 않는 patch 요청은 도구가 거절했고 실제 파일을 읽은 뒤 수정했다.

2차 검토의 조건 없는 cleanup 일반화, 이른 idempotent-success 분기 누락, 규범 소유권 누락은 위 발견 ID로 처리했다. 3차 이후 검토의 후보 우려는 각 원문에 판정과 함께 남겼다. 후속 확인이 없는 추정이나 범용 예방 규칙을 새 코드·테스트·공유 memory로 승격하지 않았다. 문서가 현재 증거의 정본이며 과거 영역별 EGM·검증 기록은 수정하지 않는다.
