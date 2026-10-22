# Qualified node runtime agent

`rhiza-runtime-agent` is a narrow, opt-in runtime provider for Linux nodes
running the specifically configured containerd CRI and cgroup-v2 profile. It
is not a generic process-kill endpoint and does not independently verify
authority closure. Its trust boundary is the pinned `rhiza-fencer` Deployment:
the agent accepts only a TokenReview-authenticated Pod-bound token with the
`rhiza-runtime-agent` audience, then checks the live Pod UID, service account,
ReplicaSet owner, and configured immutable Deployment UID. The request body
cannot claim an authority identity.

Before enabling the DaemonSet, qualify the exact containerd version/runtime
type, kernel release, and private cgroup-v2 profile, and provision the agent
TLS certificate/CA. The profile values in
`deploy/operator/fencing/authority.yaml` are intentional placeholders, not
defaults. Deploy the authority and obtain its immutable Deployment UID, then
set `authority-deployment-uid` in `rhiza-runtime-agent-profile`; the agent
rejects any token not originating from that workload. Restrict node TCP port
9445 to the authority network path. The runtime-agent CA secret must contain
`ca.crt`, and its TLS certificate must identify `runtime-agent`.

Authority registration synchronously enrolls the exact Kubernetes container
with the node agent and persists the returned full runtime binding before
register returns and before the supervised child is launched. For stop, the
agent keys a durable fsync'd receipt by the exact binding and complete
StopRequest. It checks this receipt before querying CRI, allowing a lost HTTP
response or later CRI metadata collection to recover the original receipt.
Without a matching saved receipt, unavailable/missing CRI metadata and any
unverified or live cgroup descendant remain pending; Pod deletion or an empty
sample is not terminal proof. This retained receipt alone does not establish
that the old run cannot return; terminal authority still verifies the closed
ledger/cohort and separate mutation evidence.

The cgroup profile is an administrator qualification assertion, not a
software proof that the workload cannot escape its assigned subtree. Do not
claim broader runtime, node, cloud, or crash-durability qualification from
unit tests or the cooperative CI job.
