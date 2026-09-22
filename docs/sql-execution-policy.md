# SQL execution policy 2 compatibility boundary

The SQL counter/default and TEMP fixes are incompatible execution-policy changes. This is
not an in-place upgrade. Certified SQL uses `QBAT\x02`; policy-1 `QBAT\x01`, unversioned `QBAT\x00`,
unknown policy versions and raw SQL decisions cannot be applied by this binary.
Unsupported history is an error, not a rejected SQL receipt. Already-applied
slots are checked too. Do not rewrite stored decision bytes.

Existing unmarked or policy-1 SQLite materializations and checkpoints are refused, even if TEMP was never knowingly used. The
`sql_execution_policy=2` metadata value identifies newly initialized state; it
is not a migration certificate. Do not manually insert it into old databases or
stamp old checkpoint descriptors. Peer ALPN v4 separates this binary from v2/v3
peers. Mixed-version clusters and downgrades are unsupported.

Replicated writes cannot create TEMP/TEMPORARY objects or use `temp` as a dotted
qualifier (including quoted spellings). This also reserves `temp.id` when `temp`
would otherwise be a table alias; use another alias. Persistent objects named
`temp`, ordinary string values, and SQLite's internal sort/group workspace remain
supported. Writer authorization also refuses resolved TEMP objects. Main-schema
ALTER operations retain their narrowly scoped internal catalog maintenance.

For an existing installation:

1. Keep the old compatible binary, data directories, object namespace and
   recovery evidence. Quiesce writes and retain consistent backups.
2. Export the selected actual application state under the old binary. If voters
   disagree, reconcile their application state before choosing the export.
3. Initialize a distinct cluster with fresh directories, object namespace and
   peer identities. Review schema defaults and triggers for denied functions and TEMP dependencies.
4. Import explicit application keys and stored values through new SQL commands.
   Do not import old QLog history, `_rhiza_` receipts, applied slots or the old
   migration ledger. Use new import request IDs; old retry guarantees do not
   transfer. A physical checkpoint copy is not a logical migration.
5. Compare imported application data and validate reads/writes before switching
   traffic. Retain the old installation to resolve outstanding old outcomes.

No migration or deployment is performed automatically by this change.
