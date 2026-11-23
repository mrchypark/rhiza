# HTTP service client

This optional client targets the HTTP adapter exposed by `cmd/rhiza` or an
application explicitly serving `DB.Handler()`. It does not intercept the
embedded `DB` API, and an application must adopt it to obtain failover.

Configure trusted endpoints for the **same Rhiza cluster** and an HTTP client
whose transport supplies TLS and authentication as appropriate. Redirects are
disabled. `Read` retries only the SQL, KV and graph read endpoints, with a
per-attempt timeout; a local-consistency read can differ across replicas. The
last reachable endpoint is preferred for later requests.
If all endpoints fail or return 502, 503 or 504, `Read` returns
`ErrReadUnavailable`, together with the final HTTP response when available.

`Mutate` sends a new write once to the preferred endpoint. A transport error,
HTTP error or missing/invalid receipt is reported as `ErrOutcomeUnknown` and is
never retried automatically. The returned HTTP response, if any, is retained.
The preferred endpoint can change for a *subsequent, genuinely new* operation;
this does not authorize replaying an uncertain request. The configured attempt
timeout applies to writes too, and a caller may supply an earlier context deadline.
Follow the root README's
request ID, retention and exact-original recovery contract. The HTTP status
endpoint alone cannot verify the original mutation and recover its SQL result.

This package does not support subscriptions or streaming responses. Endpoint
discovery, service identity validation and caller credentials remain the
application's responsibility.
