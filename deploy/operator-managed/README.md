# Explicit managed reference-host installation

This overlay selects the existing generation-zero managed initialization path.
The base does not install a managed profile; its operator CLI refuses new
genesis without a common-plan provider. Neither overlay nor base creates a
RhizaCluster, provisions storage, or enables whole-generation disaster recovery.

1. Copy `profile.example.json` to a private file outside the repository. Replace
   every `REPLACE_WITH_` value with your existing storage configuration and a
   private admin token; do not commit the resulting file. The example is a
   schema illustration, not usable credentials or an automatically provisioned
   endpoint. For S3, use `host[:port]`, not a URL; TLS is the default
   (`objstore_insecure: false`). Use the supported provider-specific fields for
   GCS/Azure instead of the S3 fields when appropriate. Managed HA requires
   `before-ack` and rejects explicit `async`.
2. Create namespace `rhiza` if absent, then install the required Secret:

   ```sh
   kubectl apply -f deploy/operator-managed/namespace.yaml
   kubectl create secret generic runtime -n rhiza \
     --from-file=runtime.json=/absolute/path/to/private/runtime.json
   ```

3. Render and apply the overlay from the repository root:

   ```sh
   kubectl kustomize deploy/operator-managed
   kubectl apply --validate=strict -k deploy/operator-managed
   kubectl rollout status deployment/rhiza-operator -n rhiza
   kubectl apply --validate=strict -f deploy/operator/automatic-example.yaml
   ```

The operator and generated hosts must read the **same** Secret
`runtime`, key `runtime.json`. `ProfileSecretName` must match that
name. The file is mounted read-only at `/etc/rhiza/managed/runtime.json`, with
mode 0440 and group 65532. Missing or inconsistent configuration blocks startup;
there is no fallback to unknown storage or the inert path.
For a different namespace, adjust the overlay's Namespace and namespaced
resources (including RoleBinding subjects), Secret and CR together. For an
image override, review the operator image, profile `InitImage` and CR workload
image separately; changing one does not replace the others.

The example CR selects policy 9 and the public v0.25.0 reference-host image.
The managed reconciler requires exactly one workload container, although the
CRD admits a list of up to 64. Its image must provide `rhiza-ha-init`; arbitrary
app command/env/ports are not supported.
Pods require direct peer connectivity on the installed `PeerPort`; the reference
HTTP host listens on 8080. This overlay does not create client/peer Services or
grant service-controller behavior. Observe the generated Deployment and Pod
readiness before using an explicitly configured client endpoint. The overlay
installs the existing namespace-scoped Roles, including Service creation for
the operator and CR status updates for the Host; it does not start a Service
controller.

## First-install checks

Operator rollout only checks the operator Deployment. After creating the
example CR, inspect its status and the workload resources selected by its
actual UID:

```sh
uid=$(kubectl -n rhiza get rhizacluster rhiza -o jsonpath='{.metadata.uid}')
kubectl -n rhiza get rhizacluster rhiza -o jsonpath='{.status.bootstrapPlan.clusterID}{"\n"}'
kubectl -n rhiza get deployments,pods -l "rhiza.mrchypark.dev/cluster-uid=$uid"
kubectl -n rhiza get deployment -l "rhiza.mrchypark.dev/cluster-uid=$uid" \
  -o jsonpath='{range .items[*]}{.metadata.name}{" owner="}{.metadata.ownerReferences[?(@.controller==true)].uid}{"\n"}{end}'
```

Check that the Deployment owner is the CR UID, the bootstrap plan is present,
and the expected three Pods become Ready. If a Pod stalls, use
`kubectl -n rhiza describe pod POD_NAME` and
`kubectl -n rhiza logs deployment/rhiza-operator --tail=100`; inspect the two
init containers' logs for identity or bootstrap errors. Do not dump the
runtime Secret. Workload readiness checks initial admission and local health,
not current quorum on every probe. Verify current quorum with an authenticated
linearizable query through your configured client endpoint.

One operator process uses one installed profile (protocol, provider, bucket,
base prefix and admin token) for the namespace's managed CRs. Existing code
derives per-CR/generation storage namespaces; this is not independent per-CR
credentials or tenant isolation. Use separate installations/namespaces for
different profiles. After genesis/bootstrap, no running operator is required for
healthy-quorum join or replacement, which remains the Host/engine's job. The
operator is retained for separately authorized DR work, but no generation DR
executor, fencer or lease is added here.

See the [operator contract](../operator/README.md) for ownership and recovery limits.
