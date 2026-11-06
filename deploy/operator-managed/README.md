# Explicit managed reference-host installation

This overlay selects the existing generation-zero managed initialization path.
The parent base remains profileless/inert. Neither installation creates a
RhizaCluster, provisions storage, or enables whole-generation disaster recovery.

1. Copy `profile.example.json` to a private file outside the repository. Replace
   every `REPLACE_WITH_` value with your existing storage configuration and a
   private admin token; do not commit the resulting file. The example is a
   schema illustration, not usable credentials or an automatically provisioned
   endpoint. Use the supported provider-specific fields for GCS/Azure instead
   of the S3 fields when appropriate.
2. Create namespace `rhiza` if absent, then install the required Secret:

   ```sh
   kubectl apply -f deploy/operator-managed/namespace.yaml
   kubectl create secret generic runtime -n rhiza \
     --from-file=runtime.json=/absolute/path/to/private/runtime.json
   ```

3. Render and apply the overlay from the repository root:

   ```sh
   kubectl kustomize deploy/operator-managed
   kubectl apply -k deploy/operator-managed
   kubectl rollout status deployment/rhiza-operator -n rhiza
   kubectl apply --validate=strict -f deploy/operator/automatic-example.yaml
   ```

The operator and generated hosts must read the **same** Secret
`runtime`, key `runtime.json`. `ProfileSecretName` must match that
name. The file is mounted read-only at `/etc/rhiza/managed/runtime.json`, with
mode 0440 and group 65532. Missing or inconsistent configuration blocks startup;
there is no fallback to unknown storage or the inert path.

The CR selects policy 9 and the public v0.23.2 reference-host image. Its image
must provide `rhiza-ha-init`; arbitrary app command/env/ports are not supported.
Pods require direct peer connectivity on the installed `PeerPort`; the reference
HTTP host listens on 8080. This overlay does not create client/peer Services or
grant service-controller behavior. Observe the generated Deployment and Pod
readiness before using an explicitly configured client endpoint.

One operator process uses one installed profile (protocol, provider, bucket,
base prefix and admin token) for the namespace's managed CRs. Existing code
derives per-CR/generation storage namespaces; this is not independent per-CR
credentials or tenant isolation. Use separate installations/namespaces for
different profiles. Healthy-quorum replacement remains the Host/engine's job;
no generation DR executor, fencer or lease is added here.

See the [operator contract](../operator/README.md) for ownership and recovery limits.
