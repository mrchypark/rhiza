# Hiqlite v0.16.0 reference refresh

The [CI measurement](https://github.com/mrchypark/rhiza/actions/runs/42081996466)
used Hiqlite commit `fd45e758af9ab2ff976813a6a511772d676562f1`, Rust 1.97.1,
`ubuntu24-20261101.360.1`, 100,000 SQL inserts and concurrency 16 in each
of the five existing scenarios. The existing benchmark patch was unchanged.
All five commands and artifact verification completed successfully. The source
workflow commit was `92354d42de66d44dd4fb999156093867cf900b0c`.

The measured values and checksums are stored in
[`hiqlite-reference.json`](../../hiqlite-reference.json). Byte-identical
compressed logs and runner environment from artifact `13441033218` are retained
in [`evidence/`](evidence/). Run `bash benchmarks/verify-hiqlite-reference.sh`
to validate pinned provenance, evidence digests, and throughput parsed from logs.
The UTC measurement timestamp is November 5; the directory date is November 6
in the project's Asia/Seoul timezone.

The stopped-peer cases measure post-election steady state, not interruption
during failure. These are diagnostic references, not a direct Rhiza speed ratio
or live-cloud qualification. Prior v0.15.3 evidence remains unchanged in its
existing result directory. No local Rust build was used for this refresh.
