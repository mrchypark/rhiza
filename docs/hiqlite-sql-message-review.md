# Hiqlite/Haqlite SQL Compatibility Review

> Status: architecture reference. Reviewed against Hiqlite 0.14 commit
> `c8316c53799c509990475ea8e2aa2ef8679e070e` and Haqlite commit
> `db6db54b555a56c7352a5c9d57f1b998da05d199`.

## What Hiqlite Sends

Hiqlite does not replicate a SQLite file mutation produced by one node. It
serializes a typed request and applies that request through OpenRaft:

```text
remote Client
  -> bincode ApiStreamRequest { request_id, payload }
  -> WebSocket connected to the current Raft leader
  -> QueryWrite::{Execute, ExecuteReturning, Transaction, Batch, Migration}
  -> OpenRaft EntryPayload::Normal(QueryWrite)
  -> state-machine apply
  -> local writer actor
  -> prepared SQLite statement / transaction
  -> typed Response
```

A client embedded in the leader skips the client WebSocket and calls
`client_write` directly. A remote client tracks the leader and retries after a
leader update. The API stream and Raft transport both use serde types encoded by
bincode. The SQL log payload is a `Query { sql, params }`; `Param` preserves
NULL, integer, real, text, and blob values. Transactions can also reference the
first returned row of an earlier statement by column index or name.

Reads do not normally enter the replicated log. Hiqlite offers local reads and
a more expensive consistent read routed through the leader. Writes, migrations,
backups, and an RTT marker are separate `QueryWrite` variants.

## Useful Patterns for rhiza sql

Adopt or retain these patterns:

- A typed, versioned write envelope, separate from transport and consensus.
- Explicit operation variants instead of inferring execute, transaction,
  migration, or maintenance semantics from SQL text.
- Typed parameters and result cells.
- A transaction message containing an ordered statement list.
- A single SQLite writer boundary with prepared-statement caching.
- Separate local and consistency-barrier read paths.
- A response envelope correlated to the client request ID.

QSQL v2 carries a stable request ID and ordered typed `statements[]`. Execution
produces a statement-level result with `rows_affected` plus bounded typed rows.
The rows may come from DML `RETURNING`, `SELECT`, `VALUES`, or an observational
PRAGMA executed through the replicated execute API. QWAL v3 binds the executor
fingerprint, physical page effect and exact result receipt, so retries and
follower apply never regenerate it. QuePaxa still orders opaque bytes and
SQLite provides the command transaction boundary.

Do not adopt these Hiqlite-specific choices:

- Leader forwarding. A QuePaxa preferred proposer is only a latency hint and
  must not become an exclusive write authority.
- Panicking a replica when a forbidden function reaches the writer. Admission
  must reject before proposal, and apply must fail closed with diagnosable
  recovery behavior.
- Treating a nondeterministic-function denylist as proof that arbitrary SQL text
  is deterministic.
- Coupling SQL messages to Kubernetes or object storage. Those stay outside the
  consensus and state-machine contracts.

## Physical Replay Instead of Statement Determinism

Hiqlite executes the same write SQL independently on every SQLite state
machine. Its writer overwrites time/date/random functions with failures and its
documentation forbids non-deterministic functions in modifying statements.
That still leaves SQLite version, compile options, collation, connection state,
implicit ROWID edge cases, TEMP and attached-file state as compatibility
boundaries.

Rhiza does not use statement replay. The winning proposer executes once against
the exact base, captures committed native-WAL page after-images, and puts those
pages and the typed receipt in QWAL v3. Followers verify the base/target Merkle
state and apply the pages without re-executing SQL. This supports trigger and
foreign-key indirect changes, `AUTOINCREMENT`, time/random functions, general
DDL/DML and exact returned rows within the bounded QWAL contract.

Haqlite also works below SQL by shipping WAL state. Its leader exposes a normal
`rusqlite::Connection`; without a caller-supplied authorizer, an unfenced
connection has no SQL filter. Hrana adds connection-scoped transaction sessions.
That broad parser surface is not itself a strict durability guarantee: Haqlite's
default Continuous mode sends WAL to storage in the background, whereas Rhiza
ACKs only after Recorder quorum durability.

## Practical Compatibility Matrix

| Surface | Hiqlite 0.14 | Haqlite | Rhiza QWAL v3 |
| --- | --- | --- | --- |
| General `main` DDL/DML | execute and migrations | raw connection/Hrana | supported and exact page replay |
| `RETURNING` | dedicated execute API | raw query/Hrana `want_rows` | typed exact receipt |
| Atomic multi-statement work | prepared `txn()` list | native/Hrana transaction | atomic `statements[]` command |
| Read in replicated execute | transaction output is limited | Hrana `want_rows` | `SELECT`/`VALUES` exact receipt |
| Raw multi-statement string | unprepared `batch()`, partial statement errors | native/Hrana sequence | one statement per array item |
| Raw transaction session | not the documented model | supported by Hrana baton | denied; command is the transaction |
| PRAGMA | no product allowlist; connection semantics vary | unfenced raw leader; fenced default denies PRAGMA | curated observation; two header setters and bounded optimize masks |
| TEMP | writer-connection local, not read-pool HA state | connection/session local | create/use/drop within one command only |
| Virtual tables | engine-dependent, no cluster module registry | engine/registration-dependent | bundled FTS/RTree/DBSTAT nine-module allowlist |
| Non-deterministic write | explicitly forbidden | physical WAL mode can capture it | exact pages and receipt supported |
| ATTACH/extension/external state | not a verified HA contract | raw SQL may run but files are outside registered DB replication | denied |

The compatibility target is the safe application surface in this table, not
every statement that another product happens not to filter. Rhiza therefore
keeps raw transaction/savepoint control, `ATTACH`/`DETACH`, persistent TEMP,
`VACUUM`/`VACUUM INTO`, arbitrary UDF/collation/extension/vtable modules, and
storage-changing PRAGMA setters outside the contract.

## Current Decision

1. Keep QSQL v2 as the typed command envelope and QWAL v3 as the only replicated
   SQLite effect; no statement-replay or QEFX fallback remains.
2. Keep QuePaxa unaware of SQL. It orders a versioned opaque state-machine
   command.
3. Match Hiqlite/Haqlite on ordinary `main` DDL/DML, exact returned rows,
   ordered atomic statements, observational metadata, and bundled
   FTS/RTree/DBSTAT.
4. Admit connection-local or external state only after it has a bounded,
   recoverable QWAL/snapshot contract; parser acceptance alone is insufficient.

## Sources

- [Hiqlite execute API](https://github.com/sebadob/hiqlite/blob/c8316c53799c509990475ea8e2aa2ef8679e070e/hiqlite/src/client/execute.rs#L9-L227)
- [Hiqlite transaction API](https://github.com/sebadob/hiqlite/blob/c8316c53799c509990475ea8e2aa2ef8679e070e/hiqlite/src/client/transaction.rs#L8-L99)
- [Hiqlite batch API](https://github.com/sebadob/hiqlite/blob/c8316c53799c509990475ea8e2aa2ef8679e070e/hiqlite/src/client/batch.rs#L8-L92)
- [Hiqlite non-deterministic write limitation](https://github.com/sebadob/hiqlite/blob/c8316c53799c509990475ea8e2aa2ef8679e070e/README.md#L631-L643)
- [Hiqlite crate documentation](https://docs.rs/crate/hiqlite/latest)
- [Haqlite raw connection and authorizer contract](https://github.com/russellromney/haqlite/blob/db6db54b555a56c7352a5c9d57f1b998da05d199/src/database.rs#L339-L357)
- [Haqlite leader/follower authorizer behavior](https://github.com/russellromney/haqlite/blob/db6db54b555a56c7352a5c9d57f1b998da05d199/src/database.rs#L916-L1024)
- [Haqlite raw rusqlite connection API](https://github.com/russellromney/haqlite/blob/db6db54b555a56c7352a5c9d57f1b998da05d199/src/database.rs#L1475-L1497)
- [Haqlite Hrana baton transaction test](https://github.com/russellromney/haqlite/blob/db6db54b555a56c7352a5c9d57f1b998da05d199/tests/test_hrana.rs#L254-L291)
- [Haqlite durability modes](https://github.com/russellromney/haqlite/blob/db6db54b555a56c7352a5c9d57f1b998da05d199/README.md#L75-L85)
- [SQLite ROWID allocation](https://www.sqlite.org/autoinc.html)
- [SQLite WAL format](https://www.sqlite.org/walformat.html)
