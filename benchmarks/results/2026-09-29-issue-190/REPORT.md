# Issue 190 bounded paired measurements

This is a local measurement run, not a production-bottleneck finding. The CI
workflow remains the release-quality comparison: fixed `ubuntu-24.04`, ten
interleaved samples per revision. This local run used three interleaved samples
and 100 ms benchmark calibration to verify the new cases in the paired harness.

Environment: Apple M1, Darwin arm64, Go 1.27.0, `GOMAXPROCS=2`,
`CGO_ENABLED=0`. Base: `ade2139c9dd2deb101edc58797fc19987910c839`; candidate:
`f69d27d69840fca1603b9be40a101d9d8198d8f0`. The raw collector log confirms
three passes, and each selected codec and read-index benchmark has three raw
samples on both revisions. Raw Go outputs, environment, benchstat output and
warnings, collector log, and whole-process timer are retained in `paired-local/`
and adjacent files.

| Measurement | Base | Candidate | Interpretation |
| --- | ---: | ---: | --- |
| SQL codec JSON value | 1.853 µs/op | 1.881 µs/op | inconclusive (`n=3`, `p=0.700`) |
| SQL codec 1 KiB blob | 15.163 µs/op | 8.229 µs/op | inconclusive (`n=3`, `p=0.700`); high within-run variance |
| `CoreReadIndexThreePeers` | 914.9 ns/op | 906.2 ns/op | inconclusive (`n=3`, `p=1.000`) |

The codec allocation counts were unchanged (24/op for the JSON case, 43/op for
the blob case); the read-index case was unchanged at 496 B/op and 8 allocs/op.
`benchstat` marks these comparisons with unbounded confidence intervals and
notes at least six samples are needed for a 95% interval. Its parser also emits
warnings for unrelated package benchmark log lines in the existing harness.
Treat this run as harness/scale evidence only; it supports no optimization claim.

Coverage is deliberately partial. `SQLBatchCodec` measures Go SQL batch JSON
encoding/decoding and the explicit BLOB base64 representation, not Rust
`serde_json`, Go/C buffer copies, or the complete Rust-to-native FFI call.
`CoreReadIndexThreePeers` measures the quorum read-index path; it does not test
read coalescing, whose barrier freshness and linearizability constraints remain
unchanged. No structured `commit_unknown` ABI or retry change was made. The
remaining issue candidates—FFI boundary cost, operator reconciliation,
ApplyBatch locking, expiry scans, statement prepares, receipt-cache clearing,
notification accounting, and cancel-after-reservation—remain unmeasured here.
