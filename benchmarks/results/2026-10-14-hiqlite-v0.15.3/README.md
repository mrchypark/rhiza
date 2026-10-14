# Hiqlite v0.15.3 reference refresh

The [CI measurement](https://github.com/mrchypark/rhiza/actions/runs/38506102319)
used Hiqlite commit `493f980562766f0ce5645e015557bc39501f9a5b`, Rust 1.97.1,
`ubuntu24-20261004.327.1`, 100,000 SQL inserts and concurrency 16 in each
scenario. The existing benchmark patch and measurement procedure were unchanged.

The measured values and checksums are stored in
[`hiqlite-reference.json`](../../hiqlite-reference.json). Byte-identical
compressed logs and runner environment from the CI artifact are retained in
[`evidence/`](evidence/). Run `bash benchmarks/verify-hiqlite-reference.sh` to
validate pinned provenance, evidence digests, and throughput parsed from logs.

The stopped-peer cases measure post-election steady state, not the interruption
during failure. These results are diagnostic references, not a direct Rhiza
speed ratio. Prior v0.15.2 evidence remains in its existing result directory.
