if length == 0 or any(.[]; (.total_count | type != "number") or (.workflow_runs | type != "array")) then error("malformed pages") else . end |
.[0].total_count as $total |
if any(.[]; .total_count != $total) or ([.[].workflow_runs[]] | length != $total) then error("incomplete pages")
else .[].workflow_runs[] | {id,run_attempt,status,created_at,run_started_at,updated_at} end
