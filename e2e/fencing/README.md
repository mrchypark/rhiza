# Generic cooperative fencing runtime qualification

GitHub Actions builds the server, operator and supervisor from the same source
and includes them in the disposable `Dockerfile.fencing-test` image, tagged
`rhiza-fencing-host:local` and `rhiza-fencing-operator:local`. The repository's
production Dockerfiles also include `rhiza-fencer`; this fixture image is never
published and does not substitute for their image-build checks. Then run:

```sh
CGO_ENABLED=0 go build -o "$RUNNER_TEMP/rhiza-fencing-e2e" ./e2e/fencing
FENCING_E2E_BIN="$RUNNER_TEMP/rhiza-fencing-e2e" sh e2e/fencing/run.sh
```

This creates only the disposable `rhiza-generic-fencing` Kind cluster and
`rhiza-fencing-e2e` namespace. A pre-existing cluster with that name is rejected.
The script uses the canonical authority manifest and supervised StatefulSet
patch. Writers start only after the authority, projected token, TLS trust and
dedicated Versity bucket are ready. No external cloud or existing bucket is used.

The Go driver verifies each running voter/authority/operator image against the
built image configuration through the CRI manifest/configuration digest mapping,
preserves healthy incarnations across an authority outage, acknowledges one SQL
write, then partitions all three live voters'
QUIC input, waits for the real generic executor proof, and verifies automatic
generation recovery with a linearizable ACK read and readback of a new write.
Proof identity/hash and clean drain/exit receipts are checked against the durable
generation ledger. This scenario qualifies generation recovery, not learner
replacement or runtime injection of an ambiguous storage mutation. The executor
gets no Docker, CRI, node or host privileges. The separate test driver uses CRI
only to target the injected packet loss and independently check old runtime
absence. No storage restart or manually supplied successful proof is used.

The shell always collects diagnostics and deletes its owned Kind cluster.
`generic-fencing-results/` preserves the actual exit code, events, resource
snapshots and process logs, including failed runs. This is local Kubernetes/S3
qualification, not GCS or hard-crash recovery qualification. Unknown mutation,
stale receipt and crash history rejection are covered by focused Go regressions.
