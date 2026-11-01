# QLog Go 저장 형식 부록

기준 HEAD: `008f8e576ad76425182e10a69b987f10cfb48b2a`. 이 문서는 [동작 역명세](qlog-go-reverse-spec-2026-10-30.md)의 바이트 계약을 분리한다. 아래 형식은 코드 확인 결과이며 형식 호환성 전체에 대한 정형 증명은 아니다. [독립 리뷰](qlog-go-egm-review-2026-10-30.md)를 수행했고 지적 사항을 수정했으며 재검토 대기 중이다.

## 파일 이름과 세그먼트 구조

세그먼트는 `fmt.Sprintf("seg_%03d.log", index)`와 정확히 같은 이름이다. `%03d`는 최소 자릿수이므로 1000 이상도 허용한다. manifest는 `fmt.Sprintf("manifest_%020d.bin", generation)`이다. 이름 파싱 후 동일 문자열로 재포맷하여 정규성을 검사한다. manifest의 참조 index는 0이 아니어야 한다. 세그먼트는 아래 Entry 레코드를 간격, 정렬 padding, 세그먼트 헤더 없이 이어 붙인다. 빈 active나 sealed 세그먼트도 형식상 허용된다. `lock.qlog`는 별도 manifest 참조가 없고 PID의 10진수 텍스트를 기록하며 clean Release 후 크기 0이 된다. 내부 임시 파일은 `.rhiza-segment-*`, `.rhiza-compact-*`이다.

근거: [wal.go:475](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L475), [wal.go:563](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L563), [lock.go:16](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/lock.go#L16).

## Entry 형식

모든 Entry 정수는 **Little Endian**이다. L은 하위 30비트로 디코딩한 payload 길이이며 레코드 길이는 53+L이다. 오프셋 구간은 시작 포함, 끝 제외이다.

| 구간 | 바이트 수 | 타입과 의미 |
|---|---:|---|
| `[0,8)` | 8 | Slot uint64 |
| `[8,40)` | 32 | Hash 원시 바이트 |
| `[40,41)` | 1 | EntryType uint8 |
| `[41,45)` | 4 | uint32(L) OR `0x80000000` |
| `[45,49)` | 4 | Record CRC uint32 |
| `[49,53)` | 4 | Header CRC uint32 |
| `[53,53+L)` | L | Payload 원시 바이트 |

길이 표식은 상위 2비트가 정확히 `10`이어야 한다. mask는 `0xc0000000`, marker는 `0x80000000`, 길이 mask는 `0x3fffffff`이다. 디코더 길이 범위는 0~2³⁰−1이다. Encode는 길이 범위를 검사하지 않고 `uint32(len(Payload))|marker`를 기록하므로 초과 입력의 정상 왕복을 보장하지 않는다.

CRC는 두 필드 모두 Go `crc32.MakeTable(crc32.Castagnoli)`를 사용한다. Record CRC는 `[0,45)`와 payload를 이어 붙인 바이트열의 CRC이며 두 CRC 필드 자체는 제외한다. 구현은 헤더 checksum을 초기값으로 payload에 `crc32.Update`한다. Header CRC는 이미 Record CRC가 채워진 `[0,49)` 전체의 checksum이다. 두 값 모두 LE uint32로 저장한다. Slot, Hash, Type, 길이, payload가 Record CRC에 포함된다. Header CRC 검증 전에는 길이를 외부에 노출하지 않는다.

타입 값은 `EntryProposal=0`, `EntryReceipt=1`, `EntryDecide=2`, `EntryCheckpoint=3`, `EntryCheckpointVerified=4`이다. 일반 Encode/Decode는 열거 범위, Slot, Hash와 payload의 해시 관계를 의미적으로 검증하지 않는다.

근거: [entry.go:10](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/entry.go#L10), [entry.go:45](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/entry.go#L45).

### 디코딩 순서와 오류

1. 입력이 45바이트 이상이면 marker를 먼저 검사한다. 틀리면 `unsupported WAL entry format; preserve old data and migrate logically`이다.
2. 입력이 53바이트 미만이면 io.ErrUnexpectedEOF이다.
3. Header CRC 불일치는 `WAL header CRC mismatch: stored=%08x actual=%08x`이다.
4. 검증된 길이로 전체 53+L 바이트를 요구하며 부족하면 io.ErrUnexpectedEOF이다.
5. Record CRC 불일치는 `CRC mismatch: stored=%08x actual=%08x`이다.
6. 성공하면 payload를 새 슬라이스에 복사하고 Entry와 소비 길이 53+L을 반환한다. 뒤의 추가 레코드 바이트는 이 호출에서 소비하지 않는다. 오류는 빈 Entry, 소비 길이 0과 함께 반환한다.

active readonly 스캔은 53바이트 미만 tail을 이 디코더에 넘기기 전에 제외한다. mutable Open은 45~52바이트 tail도 marker부터 검사한다. 완전한 CRC 손상과 unsupported 형식을 tail 절단으로 수리하지 않는다.

근거: [entry.go:76](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/entry.go#L76), [wal.go:839](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L839).

## Manifest 형식

Manifest 정수와 마지막 CRC 저장 순서는 **Big Endian**이다. magic은 ASCII `RHZAWAL2`(8바이트)이다. N은 reference 수, R=20+16N이다.

| 구간 | 바이트 수 | 의미 |
|---|---:|---|
| `[0,8)` | 8 | magic |
| `[8,16)` | 8 | generation uint64 |
| `[16,20)` | 4 | N uint32 |
| `[20,R)` | 16N | reference 배열 |
| `[R,R+2)` | 2, 선택 | identity 길이 I uint16 |
| `[R+2,R+2+I)` | I, 선택 | identity 원시 바이트 |
| 마지막 4바이트 | 4 | 앞의 모든 바이트에 대한 CRC32 Castagnoli |

identity가 없으면 길이 필드 자체도 없고 CRC는 R에서 시작한다. 총길이는 `24+16N`이다. 있으면 I는 1~64이며 CRC는 R+2+I에서 시작하고 총길이는 `26+16N+I`이다. 길이 0인 명시적 identity와 여분 바이트는 거부한다.

각 reference의 상대 오프셋은 다음과 같다.

| 구간 | 타입 | 의미 |
|---|---|---|
| `[0,4)` | uint32 BE | segment index |
| `[4,12)` | uint64 BE | sealed 실제 길이 또는 active의 0 |
| `[12,16)` | uint32 BE | active=1, sealed=2 |

generation>0, N>0이어야 한다. index>0이고 디코더는 중복 없는 엄격한 증가를 요구하지만 연속성은 요구하지 않는다. 마지막 reference만 active이고 active length는 반드시 0이다. sealed length에는 양수 조건이 없다. 참조 목록이 전체 권위이며 파일 크기가 active length=0이라는 뜻은 아니다. 인코더는 index 중복과 증가 순서를 별도로 검사하지 않지만 디코더는 검사한다.

근거: [manifest.go:9](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/manifest.go#L9), [manifest.go:27](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/manifest.go#L27), [manifest.go:66](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/manifest.go#L66).

### 검증 순서와 발행

최소 40바이트와 magic을 검사한 뒤 generation/count를 읽는다. count가 파일 크기로 수용 가능한 reference 수를 넘으면 `invalid WAL manifest count`이다. generation=0, count=0, 부족한 길이는 `invalid WAL manifest length`이다. identity 경계와 길이 검증 후 CRC를 검사하며 불일치는 `WAL manifest checksum mismatch`이다. 이후 reference flag/active 위치/index/length를 검사한다. 잘못된 참조는 `invalid WAL manifest reference`, 중복은 `duplicate WAL segment %d`, 역순은 `WAL segments are not strictly ordered`이다. 초기 magic/최소길이 오류는 `invalid or unsupported WAL manifest; preserve old data and migrate logically`이다.

선택한 파일명 generation은 내부 generation과 같아야 하며 다르면 `WAL manifest generation mismatch`이다. 최신 정규 이름이 손상되어도 이전 파일로 fallback하지 않는다. 발행은 generation+1의 새 파일을 원자 rename하고 디렉터리를 sync한다. rename 뒤 sync 오류는 세대 발행 여부와 내구성 확정 여부를 분리하며 fatal로 남긴다. 이전 파일의 존재는 fallback 허가가 아니다. 이전 WAL 형식에 대한 자동 migration은 구현하지 않는다.

근거: [wal.go:299](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L299), [wal.go:567](https://github.com/mrchypark/rhiza/blob/8acb6da35a4c6fc83fa14ecec2fe2b64370fa4bd/pkg/qlog/wal.go#L567).

## 검증 기록

별도 검증자가 지정 HEAD에서 `go test -mod=readonly -count=1 -timeout=120s ./pkg/qlog`를 실행하여 PASS, exit 0, 8.572초를 보고했다. 작성자가 재실행한 결과는 아니다. 검증 세션 93558 종료 후 qlog.test/go test 프로세스 잔존 없음이 확인되었지만 개별 t.TempDir 삭제는 독립 측정하지 않았다. race, 실제 cloud, Chaos Mesh, 전원 손실, 성능 검증은 수행하지 않았다. 패키지 테스트 성공만으로 모든 형식 입력이나 저장 장치의 영속성을 증명하지 않는다. [독립 리뷰 기록](qlog-go-egm-review-2026-10-30.md)의 지적 사항은 수정했으며 재검토 대기 중이다. 테스트 하네스 및 Restore/Finalize의 예외 경로는 동작 역명세를 참조한다.
