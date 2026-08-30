# SQL chaos recovery failure

- Image: `rhiza-sql-kv-e2e:review-final8`
- Prefix: `bench-final8-sql-checkpoint-one-fault-1788014460526034000`
- Scenario request ID: `no-quorum-1788014533`
- Two-peer failure response: HTTP `503`
- Final all-emptyDir recovery check: failed because the rejected write was present.

Recovered node query:

```json
{"columns":["id","value"],"rows":[[1,"before-fault"],[2,"during-fault"],[3,"must-not-commit"],[4,"after-quorum"]],"applied_slot":789,"consensus_tip":789}
```

The cluster remained internally converged, but the operation reported unavailable during loss of quorum and committed after quorum recovery.

Request-status query after recovery:

```json
{"state":"committed","tip":789,"receipt":{"slot":781,"status":"committed","rows_affected":1,"last_insert_id":3,"retry_through_slot":66316}}
```
