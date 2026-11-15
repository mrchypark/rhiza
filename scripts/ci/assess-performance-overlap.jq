def timestamp:
  if type != "string" then false else . as $raw | try ((fromdateiso8601 | todateiso8601) == $raw) catch false end;
def shape:
  (.id | type == "number") and (.run_attempt | type == "number" and . >= 1 and floor == .) and
  (.status | IN("queued", "in_progress", "completed", "waiting", "requested", "pending")) and
  (.created_at | timestamp) and (.updated_at | timestamp) and
  (.run_started_at == null or (.run_started_at | timestamp));
def ordered:
  .created_at <= .updated_at and
  (.run_started_at == null or (.created_at <= .run_started_at and .run_started_at <= .updated_at));
def old_completed:
  .status == "completed" and .created_at < $start and .updated_at < $start and
  (.run_started_at == null or .run_started_at < $start);
def inventory:
  length > 0 and (map(.id) | length == (unique | length)) and
  any(.[]; (.id | tostring) == $own) and all(.[]; shape and (ordered or old_completed));
. as $current |
if (($start | timestamp) and ($end | timestamp) and $start <= $end and ($own | test("^[0-9]+$")) and ($current | inventory)) | not then
  error("malformed performance workflow inventory")
elif $phase == "pre" then
  {start_utc:$start, end_utc:$end, overlaps:[$current[] | select((.id | tostring) != $own) | select(.status != "completed" or (old_completed | not))]}
elif $phase == "post" then
  $before as $pre |
  if ($pre | inventory | not) or
    any($current[] | select(ordered | not); . as $p | ([$pre[] | select(. == $p and .status == "completed")] | length != 1)) or
    any($current[] | select(.created_at < $start); . as $p | ([$pre[] | select(.id == $p.id)] | length != 1)) or
    any($pre[]; . as $p | ([$current[] | select(.id == $p.id)] | length != 1)) or
    any($pre[] | select(.status == "completed"); . as $p |
      any($current[] | select(.id == $p.id); .status != "completed" or .run_attempt != $p.run_attempt or ((($p | ordered) | not) and . != $p))) then
    error("performance workflow inventory changed or incomplete")
  else
    {start_utc:$start, end_utc:$end, overlaps:[$current[] | select((.id | tostring) != $own) |
      select((.run_started_at // .created_at) <= $end) |
      select(.status != "completed" or .updated_at >= $start)]}
  end
else error("invalid inventory phase") end
