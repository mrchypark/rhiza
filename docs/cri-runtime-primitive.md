# Exact-target CRI stop primitive

Tracking: [issue #246](https://github.com/mrchypark/rhiza/issues/246).
Basis: [the preserved Pro review](reviews/2026-10-21-dead-pod-autohealing-pro.md),
especially immutable pre-writer enrollment and execution-scope termination.

`pkg/fencing/cri` implements the local CRI RPC boundary. It uses the official
`k8s.io/cri-api` bindings pinned at `v0.35.2`, plus the repository's existing gRPC
and protobuf versions. The API pin is not a qualification claim for Kubernetes,
containerd, a kernel, or a cloud cluster.

**This is not yet the production node agent.** It exposes no HTTP endpoint,
starts no daemon and grants no host privileges. It is not wired to
`FencingExecutor.CollectRuntimeEvidence` or `VerifyRuntimeEvidence`. Its return
value is a `StopObservation`, not a durable/authenticated terminal receipt or
storage-quiescence proof. No real runtime/kernel combination is qualified by the
current fake-runtime tests.

## Implemented boundary

The administrator supplies one absolute local Unix socket and a profile with
namespace, application container name, exact runtime/API versions and kernel
release. Requests cannot select sockets, TCP endpoints, shell commands, PIDs or
process paths. The caller must already be trusted and must have verified root
enrollment, ownership, closure, dedicated placement and the supported no-escape
Pod security profile. This package does not implement that authorization layer.

The host identity callback must inspect the actual enrolled host, not the
node-agent container's own machine/boot files. The command is rejected before any
stop RPC if node name/UID, machine ID, boot ID, kernel or enrolled runtime identity
differs. `RuntimeIdentity` hashes the runtime-name/version/API tuple without
ambiguous string concatenation. Enrollment must capture that exact tuple before
writers start; old name-only runtime records are not retroactively upgraded.

The client inspects the exact sandbox/container IDs, verifies Pod UID and
namespace, image identity, application container name and sandbox membership,
and rejects host/shared/unknown namespace modes or unrelated application scopes.
It then calls `StopContainer` and `StopPodSandbox`, retaining which acknowledgements
were actually received. The [pinned CRI contract](https://github.com/kubernetes/cri-api/blob/v0.35.2/pkg/apis/runtime/v1/api.proto)
requires forced termination after the container grace period and says sandbox
stop covers running sandbox processes/containers. Those are provider contracts,
not proof that every implementation obeys them.

After stopping, the client rechecks container exit/finish state, sandbox not-ready
state, every listed sandbox container's terminal state, runtime version and host
identity. Acknowledgements alone are insufficient. Metadata `NotFound`, RPC
timeout/cancellation, a remaining live container, or a changed boot returns
`ErrUnverified`; partial acknowledgements are preserved and never relabelled a
successful observation. False observation flags mean unverified, not proof that
a process is alive.

All CRI calls share a 30-second context, shortened by the caller's earlier context.
The container grace is bounded at 0–10 seconds, Unix connection setup at 5 seconds,
received RPC messages at 1MiB and sent messages at 64KiB. Configured gRPC retries
are disabled; CRI's stop operations are idempotent for the same captured IDs.
The trusted host-reader callback must itself respect its context. A successful
observation still requires durable persistence, issuer verification, qualified
descendant containment and prevention of old-runtime return before fencing can
be authorized.

## Deterministic evidence

Tests start the official gRPC service over an actual private Unix socket, with
an explicitly fake CRI runtime. They verify:

- exact-ID container/sandbox stop and post-stop inspection;
- zero stop commands for wrong node/machine/boot/kernel/runtime, Pod, namespace,
  sandbox, container, image, private-namespace mode or application scope;
- no success from stop acknowledgements with a remaining nonterminal container;
- uncertainty for metadata removal, NotFound, timeout and cancellation;
- preserving partial container acknowledgement when sandbox stop is unknown;
- deterministic cancellation after the stop RPC is reached, without replay;
- rejected unsupported configuration and a fail-closed unconfigured provider;
- version-tuple identity binding and verified cleanup of the owned socket directory.

The "another container alive" case represents another CRI container in the same
sandbox, **not a Linux child process escaping a container**. Actual cgroup/process
containment, metadata-GC recovery from durable pre-fault capture, node reboot,
stale-runtime return, authenticated command/receipt handling, production service
wiring and real GCS/Chaos qualification remain required work. Safe rejection of
those incomplete cases is not counted as positive autohealing completion.

## Toolchain security repair found during verification

The initial whole-repository vulnerability scan on Go1.27.2 found reachable
`GO-2026-6809` in `crypto/x509.Certificate.Verify`, with a trace from the existing
node HTTP server. The [official advisory](https://pkg.go.dev/vuln/GO-2026-6809)
identifies Go1.27.3 as the fixed release on this Go line. The repository minimum
and both pinned Docker builder images were updated to1.27.3; CI image-generation
matchers were updated together. The new builder manifest's SHA-256 was verified
against the public registry digest, not guessed from a tag.

The same complete repository scan under Go1.27.3 reports zero affected symbols
and zero imported-package findings. It still reports the uncalled required-module
advisory `GO-2026-5932` for `golang.org/x/crypto/openpgp` (no fixed version listed).
That residual finding and the original failed scan are retained; the result must
not be described as "no vulnerabilities anywhere." This repair is not a fresh
Pro architecture verdict or a substitute for independent review.
