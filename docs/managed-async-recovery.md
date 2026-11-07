# Rehearse a read-only restore from a published archive

The existing Go recovery API reconstructs a verified published cut into a new
local directory for offline inspection. It does not create a consensus WAL,
start a Host, grant write authority, or activate a new generation. There is no
operator or CLI procedure for doing those things from this rehearsal output.

Managed `before-ack` remains the default. The
[initializer](../pkg/operator/initializer_v1alpha2.go) and
[managed Host](../pkg/managedsql/host.go) reject explicit `async`. A proposed
Async ACK after certification and quorum WAL durability would not prove
object-store publication. The accepted loss boundary for that proposal is
only the unpublished tail beyond the verified cut after loss of the old WALs;
it is not permission to activate a writer without independent fencing and
live authority. This rehearsal neither enables Async nor tests that ACK path.

## Inputs and execution

1. Independently establish the source's storage identity and exact final
   archive prefix, archive config ID, bootstrap configuration with member IDs
   and **public verification keys**, and whether reconfiguration was enabled.
   Use trusted deployment/bootstrap records, not the archive under inspection
   or replacement identities. Supply the source's exact append-only protocol:
   `rhiza-managed-request-append-only/v1` (policy 9, SQL/KV) or
   `rhiza-managed-request-graph-append-only/v1` (policy 10, SQL/KV/Graph). The
   [protocol mapping](../internal/types/managed_bootstrap.go) fixes the matching
   policy and retry window (65,536 slots for both). Do not infer these settings
   from the archive, `CURRENT`, or restored files.
2. Open the source provider bucket with **read-only credentials** scoped to the
   source objects, including the archive, locator, and referenced checkpoint
   objects. Use a bounded caller context and close the bucket in the caller.
   Choose an **absent** target path beneath a private parent you control. Keep
   that parent protected against rename or replacement, and never target a
   live data directory or an earlier rehearsal output.
3. Open a verified view once and pass that **same view** to local preparation.
   `CapturedHead()` is the canonical encoded HEAD verified by that view. Keep
   the bytes and successful result together with the independently trusted
   input record; record `HeadHash`, `PrefixHash`, `Base`, and `Tip`. If a specific
   cut was approved beforehand, compare its expected hashes and tip against the
   view before preparing. Do not silently substitute a later open. A bare
   captured HEAD is not a generic replay recipe for a locator-backed cut; in
   particular the policy-9 `AtHead` path does not rediscover `CURRENT`.

The following helper can be used from within the Rhiza source module because
the protocol-to-policy mapping is internal. The caller supplies the already
configured source bucket and trusted inputs. See the
[view API](../pkg/recovery/readonly_view.go) and
[local preparation API](../pkg/recovery/prepare_readonly_restore.go).

```go
package rehearsal

import (
	"context"
	"fmt"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

func restorePublished(ctx context.Context, bucket objstore.Bucket, prefix string,
	configID uint, bootstrap quepaxa.Cluster, protocol string, reconfiguration bool,
	target string,
) (recovery.PreparedReadOnlyRestore, []byte, error) {
	policy, window, err := types.ManagedApplicationPolicy(protocol)
	if err != nil {
		return recovery.PreparedReadOnlyRestore{}, nil, fmt.Errorf("source protocol: %w", err)
	}
	view, err := recovery.OpenReadOnlyViewForApplicationProtocol(ctx, bucket, prefix,
		configID, bootstrap, policy, window, protocol, reconfiguration)
	if err != nil {
		return recovery.PreparedReadOnlyRestore{}, nil, fmt.Errorf("verify published cut: %w", err)
	}
	// If a cut was approved in advance, check its hashes and tip here.
	result, err := recovery.PrepareReadOnlyRestoreForPolicy(ctx, view, target, policy, window)
	if err != nil {
		return recovery.PreparedReadOnlyRestore{}, nil, fmt.Errorf("prepare local restore: %w", err)
	}
	return result, view.CapturedHead(), nil
}
```

On success the result describes local SQLite state and the certified suffix;
for Graph the reconstructed native Graph state must stay with its SQLite pair.
The result is rehearsal evidence, not a mutation receipt or an activation
receipt. Check expected data and retained originals offline. For each retained
original, compare the kind, raw request ID, fingerprint, canonical slot,
original receipt/result, and retry boundary. An absent or expired original,
including an operation beyond the published cut, is not proof that it never
executed. Do not replay it transparently.

Any error means no successful restore. A claimed directory and partial files
may remain. Keep them isolated for diagnosis or remove only those files you own;
use a different absent target for another attempt. Do not publish, serve, or
adopt the output, even when local files look complete.

## Reproduce the local rehearsals

From the Rhiza repository root, use the Go version in `go.mod`. These fixtures
use an in-memory object store and temporary directories; they do not need cloud
credentials or start a managed Host.

```sh
go test ./pkg/recovery -run '^TestManagedRequestReadOnlyLocatorAndFixedHandoff$/^pinned-restore-excludes-unpublished-certified-tail$' -count=1 -timeout=60s
go test ./pkg/recovery -run '^TestManagedNoPVCGraphProtocolOpenPublishLoadReconstructs$/^published-only-excludes-unpublished-graph-tail$' -count=1 -timeout=60s
```

The [KV fixture](../pkg/recovery/managed_request_locator_view_test.go) checks the
exact published hashes and tip, compares retained originals with the producer,
and excludes a later locally certified, WAL-durable but unpublished operation
without mutating the source. The [Graph fixture](../pkg/recovery/managed_nopvc_protocol_restore_test.go)
checks that the restored tip and hashes match the published cut, published
Graph nodes and retained originals with their receipts are intact, and a later
WAL-only Graph node and its original are absent. The source bucket is unchanged.
Neither fixture qualifies old-writer fencing, live authority, or write
activation after quorum loss.
