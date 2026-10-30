# 나머지 영역 읽기 전용 검증 기록

기준은 `HEAD 008f8e576ad76425182e10a69b987f10cfb48b2a`, 2026-10-30이다. 실행 환경은 default tag, Darwin/arm64, cgo, Go `1.27.3`이며 의존성은 `-mod=readonly`로 offline 상태에서 사용했다. cloud 서비스는 사용하지 않았다.

## 실행 명령과 결과

`/tmp/rhiza-readonly-verify.o1cRgE/commands.sh`에 보존된 실제 명령은 다음과 같다.

```sh
/opt/homebrew/bin/go test -mod=readonly -count=1 -timeout=180s -json ./pkg/materializer
/opt/homebrew/bin/go test -mod=readonly -count=1 -timeout=180s -json ./pkg/operator
/opt/homebrew/bin/go test -mod=readonly -count=1 -timeout=180s -json ./cmd/rhiza-ffi
/opt/homebrew/bin/go test -mod=readonly -count=1 -timeout=180s -json ./pkg/node -run '^(TestLocalMode.*|TestLocalLifecycleFailureReopensAndReplaysMultiCommandBatchExactlyOnce|TestStartupRecovery.*|TestFence.*|TestTerminalDrain.*|TestOpenSupervisedAzureDoesNotCreateContainerBeforeRejectedBind|TestLocalRestorePinCloseDrainsRenewal|TestLocalGCInterruptionDrainsBeforeGateReleaseAndClose|TestLocalStartupRestoreJournalGuardPreservesWAL|TestReadonlyStartupValidatesNonLatestCheckpointRoot)$'
/opt/homebrew/bin/go test -mod=readonly -count=1 -timeout=180s -json ./pkg/network -run '^(TestPeer.*|TestRetiredPeerTokenCannotAuthenticate|TestQUICFlatBuffersRecordRoundTrip|TestDecisionHasRecorder|TestStartLearnerPeerServerValidatesIdentity|TestNonMemberLearnerMayOnlyFetchCertifiedDecisions|TestRecordEnvelopeCarriesConfigurationAndReconfigurationID|TestSummaryCarriesReconfigurationID|TestBoundTransport.*|TestVerifyLearnerRejectsCurrentVoter|TestDecodeWALIdentityRejectsNonCanonicalAndMismatchedValues|TestDecisionCatchUpPageIsBoundedByEncodedBytes|TestFetchDecisionsPreservesCompactedError|TestTransportCache.*)$'
```

| 범위 | 결과 |
| --- | --- |
| materializer | 전체 148 pass, helper child 1 skip, exit 0, 64초 |
| operator | 전체 216 pass, exit 0, 27초 |
| Go FFI | 전체 9 pass, S3 opt-in three-node 1 skip, exit 0, 13초 |
| node | SELECTED 22 pass, exit 0, 7초 |
| network | SELECTED 30 pass, exit 0, 10초 |
| 합계 | top-level 425 pass, 2 skip |

최종 테스트 실행에서 assertion 실패는 없었다. 초기 before-test launcher 오류와 Go `1.27.2` toolchain mismatch 기록은 남겨 두었고, 보정한 최종 실행은 모두 exit 0이었다. 원시 로그는 재현과 조사 근거로 `/tmp/rhiza-readonly-verify.o1cRgE`에 보존했다.

## 검증 공백

external C caller 또는 Rust compile ABI, cloud의 실제 all-three-provider, 실제 Kubernetes, Chaos Mesh, race, 성능 검증은 수행하지 않았다. 따라서 이 기록은 해당 항목의 통과 또는 production parity를 주장하지 않는다.

## 증거 위치와 집계 기준

commands.sh (local-only evidence: `/tmp/rhiza-readonly-verify.o1cRgE/commands.sh`), counts.txt (local-only evidence: `/tmp/rhiza-readonly-verify.o1cRgE/counts.txt`), results.txt (local-only evidence: `/tmp/rhiza-readonly-verify.o1cRgE/results.txt`)를 읽어 위 명령·개수·종료 코드를 대조했다. 이 문서 작업에서는 테스트를 실행하지 않았다. 환경의 `GOPROXY=off`, default tags 및 Go 1.27.3은 전달된 실행 조건이다.

425는 `148 + 216 + 9 + 22 + 30`으로 계산한 top-level pass 합계다. nested subtest pass event를 중복 합산하지 않았다. skip은 materializer의 `TestGraphPartialApplyProcessCrashChild` helper와 Go FFI의 S3 opt-in three-node `TestNotificationFFIThreeNode`다. node 22개와 network 30개는 선택 실행 결과이며 전체 패키지 통과를 뜻하지 않는다.

원시 로그에는 테스트 시작 전 실패 두 건(launcher 실패, Go 1.27.2의 최소 버전 미충족)이 남아 있다. 이를 테스트 assertion 실패나 최종 실행 실패로 집계하지 않는다. 수정 후 다섯 최종 실행의 exit code는 모두 0이다.

## 최종 통합과 정리

다섯 영역의 본문·계약 부록·독립 검토 기록을 정본 작업 트리에 통합했다. 발견 사항을 보정한 후 코드 대조 검토 A 12/12, B 9/9, 이해도 검토 7/7의 해소를 확인했다. 이 집계에는 중복이 포함된다. 재검토의 실제 범위와 원문은 [검토 증거](remaining-areas-egm-review-evidence-2026-10-30.md)를 따른다.

이번 작업에서 만든 임시 체크아웃 다섯 개는 제거하고 경로가 없음을 확인했다. 검증용 임시 데이터 디렉터리도 남아 있지 않으며, 관찰한 테스트 실행·자식 프로세스가 종료된 것을 확인했다. 테스트용 서버나 컨테이너를 별도로 유지하지 않았다. 원시 로그 디렉터리는 위에 밝힌 이유로 보존한다.

기존 다섯 영역의 문서 15개는 시작 시점 SHA-256과 일치한다. QuePaxa 문서와 실행 코드의 tracked diff는 없다. 새 문서의 로컬 링크 대상과 후행 공백도 확인했다. 새 문서만 변경했으므로 이 보정 뒤 런타임 테스트를 반복 실행하지 않았다.

## 문서 독립 재검토 최종 상태

다섯 영역은 **보정·독립 재검토 완료(발견 지적 해소; 검토 범위 한정)**다. [정본의 최종 원문](remaining-areas-egm-review-evidence-2026-10-30.md?plain=1#L7)에서 A 12/12, B 9/9, 이해도 7/7 지적의 해소를 확인했다. Node hash와 Operator 단계별 부수 효과 순서도 최종 확인을 받았으며 원본 지적 집합에 남은 항목은 없다. 검토별 집계에는 중복이 있으므로 고유 결함 28건을 뜻하지 않는다.

문서 재검토와 실행 증거는 별도 범위다. 실제 테스트 결과는 위의 **425 pass·2 skip**이며, 최종 문서 재검토에서 테스트를 재실행하지 않았다. 앞서 명시한 실행 검증 공백과 검토 범위 제한은 그대로 적용된다.
