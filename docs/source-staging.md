# Source mutation payload staging (experimental, disabled by default)

`rhiza-fencer serve` can expose an authenticated, bounded source-stage endpoint
when explicitly configured with a pre-provisioned local spool. This is only a
payload-reservation and sealing surface. The bootstrap consumer, provider
execution, and acknowledgement path are not enabled by this change. It does
not make source mutation safe to activate or qualify an independent-evidence
deployment.

## Storage profile and provisioning

The implemented profile is one authority process on one trusted Linux host
using a local POSIX filesystem. The process holds an exclusive nonblocking
`flock` on `.lock` for its lifetime. This prevents a second process using the
same lock implementation from serving the same directory concurrently. It is
not a distributed lease: no cross-node or network/shared-PV locking, fencing,
failover, or split-brain guarantee is claimed. Do not mount this directory into
multiple authority replicas or move it between hosts. A missing directory,
manifest, payload, or mismatched incarnation fails closed; startup does not
initialize or reconstruct state.

Before enabling the flags, provision an existing directory owned by the fencer
account with mode `0700` (group/other permissions are rejected). Provision
`stages.json` there as a regular, non-symlink file with mode `0600` or stricter.
The fencer creates `.lock` if absent; it must be a regular file with no
group/other permissions. Payloads are stored as `payload-<ticket>` with mode
`0400`. The directory may contain only `.lock`, `stages.json`, payload files
accounted for in the manifest, and recognized interrupted `CreateTemp` files.
On reopen, under the exclusive lock, the fencer removes regular
`.manifest-<random>` files and `.receiving-<ticket>-<random>` files only when
that ticket has a durable receiving descriptor; it never reads or adopts their
bytes. Symlinks, unsafe temp permissions, receiving temps without a matching
durable descriptor, and all other unknown entries fail closed. Preserve the
directory and its manifest across process restarts. Do not delete entries to
recover capacity.

The initial manifest is version 1, has an empty record list, and pins the
operator-selected lowercase SHA-256 incarnation digest:

```json
{"version":1,"incarnation":"<64 lowercase hex chars>","records":[],"digest":"<manifest digest>"}
```

`digest` is the lowercase SHA-256 of the compact JSON encoding of the same
manifest with `digest` set to the empty string. Record field names and shapes
are the JSON forms of `sourceStageRecord` and `fencing.MutationIntent` in the
implementation. Provision this initial empty manifest with a trusted local
provisioning procedure; no manifest-generation or repair command is provided.
The configured incarnation must exactly equal the manifest's value on every
start. When the source-stage store is configured during producer binding, the
authority also pins this exact digest in the root ledger before WAL activation
and returns it in the producer binding. This detects replacement by another
configured incarnation; it is not a cross-host identity guarantee or a
distributed lock.

For example, after provisioning a real manifest and directory (the values
below are placeholders), configure the fencer process with:

```sh
export FENCER_SOURCE_STAGE_DIR=/var/lib/rhiza/source-stages
export FENCER_SOURCE_STAGE_INCARNATION=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
rhiza-fencer serve --namespace=rhiza-system --tls-cert=/etc/rhiza/tls/tls.crt --tls-key=/etc/rhiza/tls/tls.key
```

The example digest is illustrative and must be replaced with the exact value
provisioned in `stages.json`. Both settings must be present; with neither set,
staging is disabled and its route returns `503`. Supplying only one, or any
invalid/missing store state, prevents service startup. Use one process and one
host for this qualified profile.

## HTTP boundary

All routes are served by the fencer's existing TLS listener and authentication
path. A request carries the existing bearer token and a base64url-encoded
`fencing.Identity` in `X-Rhiza-Fencing-Identity`; that header is only a claim,
not authentication. Existing TokenReview and Pod/runtime enrollment checks
authenticate the bearer token and bind it to the requested identity. The route
derives the producer UID and scope from the authority's enrolled ledger and
rejects closed, stopped, unjournaled, or mismatched producers.

* `POST /v1/source-stages/reserve` accepts a JSON ticket and upload draft, up to
  64 KiB.
* `POST /v1/source-stages/{ticket}?digest=<sha256>&size=<bytes>` accepts an
  unencoded `application/octet-stream` body, at most 64 MiB per ticket. It
  pins digest and length before reading bytes, verifies both, fsyncs and
  atomically renames the payload, then records the sealed descriptor.
* `DELETE /v1/source-stages/{ticket}` records cancellation only before sealing.

The store retains at most 64 ticket records and 4 GiB of payload/receiving
inventory. Records and sealed bytes are retained; there is no automatic
eviction, retirement, TTL, or deletion policy. Cancellation does not reclaim a
ticket slot. The existing authority server read/write timeouts also apply;
clients must treat timeout or connection loss as an unknown response and retry
the same ticket and descriptor, never mint a replacement identity.

## Deliberate limits

This interface does not execute a provider mutation, create provider evidence,
acknowledge an owner step, enable the bootstrap consumer, or enable Node
mutation. No production caller is authorized to infer success from a sealed
payload. Authentication of the existing authority transport is not evidence
that a future provider transport or result is authenticated. Deployment must
remain disabled until the complete provider/result/ACK contract and storage
profile are independently implemented, reviewed, and qualified.

The original Pro contract remains the source of mutation identity, retry,
unknown-outcome, and result-retention requirements. The later user waiver
allowed implementation to continue without another Pro consultation; it does
not approve provider activation, shared-volume use, or deployment. This
document records the bounded local spool and immutable root-ledger pin above.
