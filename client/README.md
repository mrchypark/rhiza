# HTTP service client

This optional Go client sends HTTP requests to Rhiza adapters on separate
application instances. It does **not** intercept embedded `DB` calls: the
calling application must adopt the client to obtain endpoint failover. It can
also connect to `cmd/rhiza` servers.

## Expose the adapter in each instance

Each HA application instance embeds Rhiza as usual and can serve `db.Handler()`
on an application-managed HTTP server. For example, after `rhiza.Open` succeeds:

```go
// Supply authenticateAndAuthorize from your application's security layer.
// Configure TLS on the listener or on a trusted proxy before serving traffic.
mux.Handle("/rhiza/", authenticateAndAuthorize(http.StripPrefix("/rhiza", db.Handler())))
```

`db.Handler()` does not provide application authentication or authorization.
Do not expose it directly to untrusted networks. Secure the listener and
storage credentials, manage server shutdown with your application, and
advertise only endpoints you trust. The client currently accepts origin URLs
only, not path prefixes; use a dedicated origin or configure your proxy to map
the client's `/sql`, `/kv`, and `/graph` paths to the adapter. See
[embedded lifecycle](../docs/embedded-lifecycle.md) for opening and closing a
`DB` and [HA deployment](../docs/embedded-operator.md) for the
deployment contract.

## Use the client

Configure one to 32 trusted origins for the **same Rhiza cluster**. Endpoint
discovery, cluster identity, TLS, and authentication are the application's
responsibility. The configured `http.Client` transport must attach credentials
and validate server identities as appropriate. Redirects are disabled.
For endpoint-level failover, configure independently reachable instance
origins; a single Kubernetes Service origin leaves backend selection to that
Service and does not give this client a second endpoint to try. Ensure that
instance discovery and certificates remain valid as Pods are replaced.

The example below uses local HTTP origins only to show the API. In production,
replace them and configure a protected `http.Client` before sending requests.

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "net/http"
    "time"

    "github.com/mrchypark/rhiza/client"
)

func main() {
    c, err := client.New([]string{
        "http://127.0.0.1:8081",
        "http://127.0.0.1:8082",
        "http://127.0.0.1:8083",
    }, &http.Client{}, 250*time.Millisecond)
    if err != nil {
        panic(err)
    }

    ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
    defer cancel()
    result, err := c.Read(ctx, "/sql/query", []byte(`{"sql":"SELECT 1","consistency":"linearizable"}`))
    if err != nil {
        panic(fmt.Sprintf("query failed: %v (HTTP %d: %s)", err, result.StatusCode, result.Body))
    }
    if result.StatusCode != http.StatusOK {
        panic(fmt.Sprintf("query rejected: HTTP %d: %s", result.StatusCode, result.Body))
    }
    fmt.Println(string(result.Body))

    // Generate a unique ID per genuinely new operation and durably retain the
    // exact original request before submission if recovery may be needed.
    original := []byte(`{"request_id":"replace-with-unique-id","sql":"CREATE TABLE example (id INTEGER)"}`)
    result, err = c.Mutate(ctx, "/sql/execute", original)
    if errors.Is(err, client.ErrOutcomeUnknown) {
        // Do not resubmit: preserve original and resolve its exact outcome.
        fmt.Println("write outcome unknown; manual resolution required")
        return
    }
    if err != nil {
        panic(err)
    }
    fmt.Println(string(result.Body)) // Inspect the returned receipt/status.
}
```

`Read` supports only `/sql/query`, `/kv/get`, and `/graph/query`. It tries
another configured endpoint on transport errors, HTTP 408, or any HTTP 5xx
response, and returns `ErrReadUnavailable` if all attempts fail. Other
non-success HTTP responses, including other 4xx responses and redirects, are
not retried: `Read` returns `ErrReadRejected` together with the response.
Callers should check `StatusCode` and decode the response body. The timeout
supplied to `New` bounds **each attempt**, including a
mutation; an earlier caller context deadline takes precedence. A local read
can differ across replicas after failover. Use a linearizable read when the
application requires quorum-backed consistency.

`Mutate` makes one application-level request to the preferred endpoint. A
transport failure, non-success HTTP response, or missing/invalid receipt
returns `ErrOutcomeUnknown`; the server may already have applied the write.
The client does not retry that mutation. It may choose a different endpoint
for a later, genuinely new operation. Even a successful response can report a
rejected mutation, so inspect its receipt/status.

## Recover an uncertain mutation

Persist the unique request ID and the exact original operation before sending
it. For the raw Go `Execute` API, use `ResolveExecute` with that exact original
request. The HTTP `/request/status` endpoint alone does **not** verify the
original payload or recover SQL `RETURNING` results; this client does not
provide a remote exact-original resolver. Applications need an appropriate
trusted recovery path instead of blindly submitting the mutation again.

Receipts have a bounded 65,536-slot retention window. `unknown_or_expired`
does not distinguish a request that never ran from one whose receipt expired,
and a separately observed tip does not ensure retention at a later retry.
Never create a new ID or resubmit an uncertain request merely to resolve it.
See the [request ID and recovery contract](../README.md#consistency-and-failure-model)
and [managed request recovery](../docs/managed-internal-request-api.md).

Subscriptions and streaming responses are not supported by this client.
