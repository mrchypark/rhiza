# SQL execution policy 4 compatibility boundary

The reconfiguration schedule repair uses peer ALPN `rhiza-peer-v7`, with no
v6 fallback; SQL policy remains 4. Upgrade all participants while quiesced.
Do not downgrade after activation. This transport gate does not mechanically
prevent reopening WALs with older binaries.

Certified reconfigurations whose freeze or terminal occupies a source-generation
leader-schedule slot are incompatible. Preserve the installation, WAL, ISR and
certificates; do not rewrite controls, truncate to the pre-freeze prefix, or
invent a schedule. Migrate verified committed application state (including
committed drain writes) into a fresh cluster with a separate lineage. This change
does not perform that migration automatically.

The SQL counter/default, TEMP, ROWID and PRAGMA fixes are incompatible execution-policy changes. This is
not an in-place upgrade. Certified SQL uses `QBAT\x04`; policy-3 `QBAT\x03`, policy-2 `QBAT\x02`, policy-1 `QBAT\x01`, unversioned `QBAT\x00`,
unknown policy versions and raw SQL decisions cannot be applied by this binary.
Unsupported history is an error, not a rejected SQL receipt. Already-applied
slots are checked too. Do not rewrite stored decision bytes.

Existing unmarked, policy-1, policy-2 or policy-3 SQLite materializations and checkpoints are refused, even if TEMP was never knowingly used. The
`sql_execution_policy=4` metadata value identifies newly initialized state; it
is not a migration certificate. Do not manually insert it into old databases or
stamp old checkpoint descriptors. Peer ALPN v7 separates this binary from v2/v3/v4/v5/v6
peers. Mixed-version clusters and downgrades are unsupported.

Replicated writes cannot create TEMP/TEMPORARY objects or use `temp` as a dotted
qualifier (including quoted spellings). This also reserves `temp.id` when `temp`
would otherwise be a table alias; use another alias. Persistent objects named
`temp`, ordinary string values, and SQLite's internal sort/group workspace remain
supported. Writer authorization also refuses resolved TEMP objects. Main-schema
ALTER operations retain their narrowly scoped internal catalog maintenance.

Policy 3 reserves the maximum actual SQLite ROWID (`9223372036854775807`).
Effective INSERT/UPDATE of that key rejects and rolls back the entire command,
including trigger writes and earlier statements. An implicit insert after
`9223372036854775806` also rejects. Maximum integers in ordinary columns or
WITHOUT ROWID keys remain supported. AUTOINCREMENT tables use the same restriction,
although SQLite already handles their exhaustion deterministically.

Persistent virtual tables (including every FTS5 storage mode), direct sequence or
statistics table mutations, and ANALYZE are unsupported by replicated writes.
Native preupdate hooks cannot cover those storage paths. Normal engine-managed
AUTOINCREMENT bookkeeping and narrowly scoped ALTER/DROP maintenance remain supported.
Old virtual-table materializations/checkpoints are refused before installation.

Policy-4 physical state must originate from guarded execution or its supported
checkpoint path. The marker does not validate arbitrary externally manufactured or
relabeled files. Logical imports must use the guarded SQL API. A preexisting maximum
application key needs an explicit application remapping decision, never automatic
rewriting. If a later SQL error ends the whole transaction, application fails without
a durable rejection receipt or tip advancement; it is not converted into success.

Policy 4 denies user PRAGMA evaluation inside replicated SQL, including table-valued sources in SELECT, INSERT, views, triggers and returned results. Ordinary tables and CTEs named `pragma_*` remain valid; this is an execution restriction, not an identifier ban. The read API retains PRAGMA access. Internal initialization and ROWID checks run outside the user statement scope. SQLite's internal ADD COLUMN constraint validation may run `quick_check` only for the exact main-schema target authorized by a single ALTER; this does not permit user PRAGMA queries.

Policy versions identify SQL envelopes, SQLite provenance and live peers; they do not universally identify every non-SQL historical artifact after its surrounding provenance is removed. Never reuse old history or physical namespaces.

For an existing installation:

1. Keep the old compatible binary, data directories, object namespace and
   recovery evidence. Quiesce writes and retain consistent backups.
2. Export the selected actual application state under the old binary. If voters
   disagree, reconcile their application state before choosing the export.
3. Initialize a distinct cluster with fresh directories, object namespace and
   peer identities. Review schema defaults and triggers for denied functions, TEMP dependencies and PRAGMA reads. Review previously stored node-local values; copying them does not repair their meaning.
4. Import explicit application keys and stored values through new SQL commands.
   Do not import old QLog history, `_rhiza_` receipts, applied slots or the old
   migration ledger. Use new import request IDs; old retry guarantees do not
   transfer. A physical checkpoint copy is not a logical migration.
5. Compare imported application data and validate reads/writes before switching
   traffic. Retain the old installation to resolve outstanding old outcomes.

No migration or deployment is performed automatically by this change.

## Graph engine upgrade

The new-cluster requirement also covers the upgrade from latticedb-go v0.7.0
to v0.9.0. Export actual application nodes, relationships, and properties with
the old compatible binary, then import them with new graph request IDs into
the fresh cluster. Do not copy graph files, checkpoints, receipts, or replay
old graph decision history, and do not mix graph engine versions among peers.

The underlying storage format is unchanged, but query grammar and resource
accounting are not: a previously rejected certified query can commit under the
new engine. Graph batches retain their existing envelope and are not universally
fenced by an engine version when replayed without materializations. The supported
migration is logical import, not cross-version history replay. This also applies
to earlier development builds carrying SQL policy 2. SQL policy metadata alone
is not proof of graph replay compatibility.

## WAL framing compatibility

The local WAL now uses a 53-byte header with independent CRC32C protection for
its length and record checksum, and the `RHZAWAL2` manifest discriminator.
All previous 49-byte WAL artifacts, including development builds of policy 2,
are rejected before scanning or modifying their segments. Old raw segments are
also rejected by restore. Preserve the old binary and data for the logical
migration above; never relabel a manifest or mix old and new records.

Only a physically incomplete final append under a current-format manifest can
be truncated automatically. A complete header must pass its checksum before a
short payload is repairable. Repairs are synced before startup succeeds;
checksum or format failures preserve the evidence and fail closed. This does
not distinguish an incomplete write from later physical deletion of identical
tail bytes, and does not provide automatic migration of previous WAL formats.
