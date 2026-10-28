# Local recovery with an init container

Rhiza v0.21.0 supports continuous object-storage backup and recovery through
the public Go SDK in single-node `Local` mode. This is not peer replication or
HA recovery. HA recovery work is separate and is not part of this released
Local feature.

An application can use an init container to call `rhiza.Open`, wait for it to
return, and close the DB. The main container then calls `Open` with the same
configuration and the **same mounted DataDir**. The init container must not
start application SQL or other services. Rhiza has no dedicated official init
CLI or image: the application owns the entrypoint and image in this example.
Both opens require object-store access. The second open validates the binding
and reconciles history; equal histories do not blindly download again. Keep
object storage configured in the main process and do not switch to `Async`.

## Shared SDK configuration

The following complete helper functions use v0.21.0 public types. Both modes
must call `sharedConfig` with the same values. Supply the service-account JSON
securely, for example by reading a mounted secret file; do not log it. These
functions are application code, not a Rhiza command or a complete server.

```go
package app

import (
    "context"
    "fmt"
    "os"

    "github.com/mrchypark/rhiza"
)

func sharedConfig(bucket, prefix, credentialFile string) (rhiza.Config, error) {
    credentials, err := os.ReadFile(credentialFile)
    if err != nil {
        return rhiza.Config{}, fmt.Errorf("read GCS credentials: %w", err)
    }
    return rhiza.Config{
        Local:                  true,
        ClusterID:              "my-application",
        NodeID:                 "node-1",
        DataDir:                "/var/lib/myapp/rhiza",
        ObjStoreProvider:       rhiza.ObjectStoreProviderGCS,
        ObjStoreBucket:         bucket,
        ObjStorePrefix:         prefix,
        ObjStoreServiceAccount: string(credentials),
        ObjStoreDurability:     rhiza.ObjectStoreDurabilityBeforeAck,
    }, nil
}

func restore(ctx context.Context, cfg rhiza.Config) error {
    db, err := rhiza.Open(ctx, cfg)
    if err != nil {
        return fmt.Errorf("open for recovery: %w", err)
    }
    if err := db.Close(); err != nil {
        return fmt.Errorf("close after recovery: %w", err)
    }
    return nil
}
```

The consumer's `restore` command exits nonzero on either error, so Kubernetes
does not start the main container. The `serve` command calls `rhiza.Open` with
the same config, fails startup on error, and signals readiness only after Open
and its own service initialization succeed. On termination it stops accepting
work, drains requests, closes the DB, and reports close errors. Do not report
ready merely because the init container succeeded.

## Deployment shape

This illustrative Deployment assumes the consumer builds `/app/service` with
`restore` and `serve` subcommands implementing the contract above. They are
not Rhiza-provided commands. Both modes obtain the same bucket, prefix, and
credential file from application configuration; configure those and mount the
credential securely in both containers before using this example. The image
name is a placeholder, not an official Rhiza image.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: myapp
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: myapp
  template:
    metadata:
      labels:
        app: myapp
    spec:
      initContainers:
        - name: restore
          image: example.invalid/myapp:replace-me
          command: ["/app/service", "restore"]
          volumeMounts:
            - name: data
              mountPath: /var/lib/myapp/rhiza
      containers:
        - name: app
          image: example.invalid/myapp:replace-me
          command: ["/app/service", "serve"]
          volumeMounts:
            - name: data
              mountPath: /var/lib/myapp/rhiza
      volumes:
        - name: data
          emptyDir: {}
```

An `emptyDir` survives container restarts within the same Pod, but is lost when
the Pod is removed or replaced; a new Pod restores from the independent object
store. An existing PVC can be mounted at the same path instead: its data can
survive Pod replacement, subject to storage and failure-domain guarantees.
If both local data and a filesystem archive reside on the failed host or disk,
neither is an independent backup for a new node. A filesystem archive's
same-host owner lock does not fence remote object stores or another host.

## Safety and limits

- Establish that the previous writer has stopped, cannot be recreated, and
  has no outstanding storage work before starting recovery. One replica and
  `Recreate` reduce ordinary overlap but do not prove exclusion during a
  partition; a Pod deletion response is not fencing. External quiescence is
  the deployment's responsibility.
- Use the same `ClusterID`, `NodeID`, bucket, archive prefix, and DataDir
  identity for both opens and recovery. A held or uncertain remote GC or
  maintenance token fails closed; do not clear it based on age or presumed
  process death. Preserve evidence and investigate startup failures.
- `BeforeAck` makes successful mutations wait for archive durability;
  explicit `Async` has a loss window. Recovery still depends on an intact,
  accessible archive and correct writer exclusion.
- v0.21.0 Local archive format is incompatible with older formats, including
  v0.19.5. It does not automatically migrate them. Nor does it automatically
  convert a three-node HA installation into Local mode. Preserve the old data
  and use a separately validated migration plan.

The SDK's `RepeatedTotalLoss` and `Equal` tests exercise loss and matching
history reconciliation respectively; see the [test contract](embedded-object-storage.md).
They do not establish that this init-container Deployment, Kubernetes fencing,
or a production cloud provider has been tested or qualified.
