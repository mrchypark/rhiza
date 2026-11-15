def timestamp:
  if type != "string" then false else
    . as $raw | try ((fromdateiso8601 | todateiso8601) == $raw) catch false
  end;
if ((length > 0) and any(.[]; (.id | tostring) == $own) and
    ($start | timestamp) and ($end | timestamp) and ($start <= $end) and ($own | test("^[0-9]+$")) and
    all(.[]; (.id | type == "number") and
      (.status | IN("queued", "in_progress", "completed", "waiting", "requested", "pending")) and
      (.created_at | timestamp) and (.updated_at | timestamp) and (.created_at <= .updated_at) and
      ((.run_started_at == null) or ((.run_started_at | timestamp) and (.created_at <= .run_started_at) and (.run_started_at <= .updated_at))))) | not then
  error("malformed performance workflow inventory")
else
  {start_utc: $start, end_utc: $end,
   overlaps: [ .[] | select((.id | tostring) != $own) |
     select((.run_started_at // .created_at) <= $end) |
     select(.status != "completed" or .updated_at >= $start) |
     {id,status,created_at,run_started_at,updated_at} ] | unique_by(.id)}
end
