# Hiqlite v0.15.2 reference refresh

The [CI measurement](https://github.com/mrchypark/rhiza/actions/runs/38002472835)
used Hiqlite commit `18e3e71be1b40465f82e1f26f17b7d1bfcc6995d`, Rust 1.97.1,
`ubuntu24-20261004.327.1`, 100,000 SQL inserts and concurrency 16 in each
scenario.

The canonical measured values and checksums are stored in
[`hiqlite-reference.json`](../../hiqlite-reference.json). The byte-identical
compressed logs and runner environment from the CI artifact are retained in
[`evidence/`](evidence/). Run `bash benchmarks/verify-hiqlite-reference.sh` to
validate the pinned provenance, evidence digests, and throughput parsed from
the retained logs.

The stopped-peer cases measure post-election steady state, not the interruption
during failure. These results are diagnostic references, not a direct Rhiza
speed ratio.
