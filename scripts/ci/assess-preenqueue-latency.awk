# Assess bounded tagged pre-enqueue latency without retaining request metadata.
function field(line, name,    i, pair) {
  for (i = 1; i <= NF; i++) {
    split($i, pair, "=")
    if (pair[1] == name) return pair[2]
  }
  return ""
}
function show(value) { return value == "" ? "NA" : value }
function duration(start, finish) { return (start == "" || finish == "") ? "" : finish - start }
function ordered(tag) {
  if (admission_end[tag] > clone_start[tag] + 2048 || clone_start[tag] > clone_end[tag] + 2048) return 0
  if (clone_end[tag] > validate_start[tag] + 2048 || validate_start[tag] > validate_end[tag] + 2048) return 0
  if (validate_end[tag] > fingerprint_start[tag] + 2048 || fingerprint_start[tag] > fingerprint_end[tag] + 2048) return 0
  if (fingerprint_end[tag] > encode_start[tag] + 2048 || encode_start[tag] > encode_end[tag] + 2048) return 0
  if (encode_end[tag] > lock_start[tag] + 2048 || lock_start[tag] > lock_end[tag] + 2048) return 0
  if (lock_end[tag] > receipt_start[tag] + 2048 || receipt_start[tag] > receipt_end[tag] + 2048) return 0
  if (receipt_end[tag] > enqueue_start[tag] + 2048 || enqueue_start[tag] > enqueue_end[tag] + 2048) return 0
  if (enqueue_end[tag] > proposal_start[tag] + 2048) return 0
  return 1
}
function overlaps(start_a, end_a, start_b, end_b) {
  return start_a + 2048 < end_b && end_a - 2048 > start_b
}
BEGIN {
  max_client_tags = 64
  max_pair_checks = max_client_tags * max_client_tags
  if (client_file == "") { print "# status=invalid missing_client_file"; exit 2 }
  while ((getline line < client_file) > 0) {
    split(line, cols, "\t")
    tag = cols[1]
    if (tag !~ /^[1-9][0-9]*$/ || cols[8] == "") {
      print "# status=invalid malformed_client_row"
      exit 2
    }
    if (tag in selected) { print "# status=invalid duplicate_client_tag"; exit 2 }
    if (++tag_count > max_client_tags) { print "# status=invalid too_many_client_tags"; exit 2 }
    selected[tag] = 1
    client_slot[tag] = cols[2]
    client_latency[tag] = cols[7]
    succeeded[tag] = cols[8]
  }
  close(client_file)
}
/^rhiza_latency dropped=/ { split($2, pair, "="); server_drops += pair[2]; next }
/^rhiza_latency pre_enqueue_dropped=/ { split($2, pair, "="); pre_enqueue_drops += pair[2]; next }
/^rhiza_latency pre_enqueue_dropped_capacity=/ { split($2, pair, "="); pre_enqueue_capacity_drops += pair[2]; next }
/^rhiza_latency pre_enqueue_dropped_trylock=/ { split($2, pair, "="); pre_enqueue_trylock_drops += pair[2]; next }
/^rhiza_latency pre_enqueue_dropped_reason_incomplete=/ { split($2, pair, "="); pre_enqueue_reason_incomplete += pair[2]; next }
/^rhiza_latency stage=/ {
  stage = field($0, "stage")
  tag = field($0, "tag")
  if (tag == "0" || !(tag in selected)) next
  if (stage !~ /^(pre_enqueue_clone|pre_enqueue_validate|pre_enqueue_fingerprint|pre_enqueue_encode|request_lock_wait|request_lock_hold|materializer_receipt_pre_enqueue|batcher_enqueue|mutation_admission|batch_proposal)$/) next
  key = tag SUBSEP stage
  if (seen[key]++) { duplicate_events++; fatal++; next }
  start = field($0, "start_unix_ns")
  finish = field($0, "end_unix_ns")
  outcome = field($0, "outcome")
  count = field($0, "count")
  if (start !~ /^[0-9]+$/ || finish !~ /^[0-9]+$/ || finish < start) { negative_duration++; fatal++; next }
  if ((stage == "request_lock_wait" || stage == "request_lock_hold") && (count !~ /^[0-9]+$/ || count + 0 > 4095)) { shard_out_of_range++; fatal++; next }
  if (stage == "pre_enqueue_clone") { clone_start[tag] = start; clone_end[tag] = finish }
  else if (stage == "pre_enqueue_validate") { validate_start[tag] = start; validate_end[tag] = finish }
  else if (stage == "pre_enqueue_fingerprint") { fingerprint_start[tag] = start; fingerprint_end[tag] = finish }
  else if (stage == "pre_enqueue_encode") { encode_start[tag] = start; encode_end[tag] = finish }
  else if (stage == "request_lock_wait") { lock_start[tag] = start; lock_end[tag] = finish; lock_outcome[tag] = outcome; lock_shard[tag] = count; lock_file[tag] = FILENAME; wait_tags[tag] = 1 }
  else if (stage == "request_lock_hold") { hold_start[tag] = start; hold_end[tag] = finish; hold_shard[tag] = count; hold_file[tag] = FILENAME; hold_tags[tag] = 1 }
  else if (stage == "materializer_receipt_pre_enqueue") { receipt_start[tag] = start; receipt_end[tag] = finish }
  else if (stage == "batcher_enqueue") { enqueue_start[tag] = start; enqueue_end[tag] = finish; enqueue_outcome[tag] = outcome }
  else if (stage == "mutation_admission") { admission_end[tag] = finish }
  else if (stage == "batch_proposal") { proposal_start[tag] = start }
}
END {
  print "# Pre-enqueue stages use fixed numeric fields; missing values are NA and are never zero-filled."
  print "# tag\tslot\tlock_shard\tclone_ns\tvalidate_ns\tfingerprint_ns\tencode_ns\tlock_wait_ns\tlock_outcome\treceipt_ns\tenqueue_ns\tenqueue_outcome\tadmission_to_enqueue_ns\tclient_latency_ns\tcorrelation\treason"
  for (tag in selected) {
    correlation = "valid"; reason = "complete"
    if (succeeded[tag] != "true") {
      correlation = "request_error"; reason = "client_failure"; client_failures++
    } else {
      expected = "pre_enqueue_clone pre_enqueue_validate pre_enqueue_fingerprint pre_enqueue_encode request_lock_wait request_lock_hold materializer_receipt_pre_enqueue batcher_enqueue mutation_admission batch_proposal"
      count_expected = split(expected, names, " ")
      missing = 0
      for (j = 1; j <= count_expected; j++) if (!((tag SUBSEP names[j]) in seen)) { missing_stage[names[j]]++; missing++ }
      if (missing) { correlation = "incomplete"; reason = "missing_server_stage"; incomplete++ }
      else if ((tag in lock_shard) && (tag in hold_shard) && lock_shard[tag] != hold_shard[tag]) { correlation = "invalid"; reason = "lock_shard_mismatch"; shard_mismatches++; fatal++ }
      else if ((tag in lock_outcome) && lock_outcome[tag] != "ok") { correlation = "invalid"; reason = "lock_outcome"; lock_failures++; fatal++ }
      else if ((tag in enqueue_outcome) && enqueue_outcome[tag] != "ok") { correlation = "invalid"; reason = "enqueue_outcome"; enqueue_failures++; fatal++ }
      else if (!ordered(tag)) { correlation = "invalid"; reason = "boundary_order"; order_invalid++; fatal++ }
      else {
        valid++
        lock_wait = duration(lock_start[tag], lock_end[tag])
        if (lock_wait > 2048) positive_lock_waits++
      }
    }
    admission_to_enqueue = (tag in admission_end && tag in enqueue_end) ? enqueue_end[tag] - admission_end[tag] : ""
    print tag "\t" client_slot[tag] "\t" show(lock_shard[tag]) "\t" show(duration(clone_start[tag], clone_end[tag])) "\t" show(duration(validate_start[tag], validate_end[tag])) "\t" show(duration(fingerprint_start[tag], fingerprint_end[tag])) "\t" show(duration(encode_start[tag], encode_end[tag])) "\t" show(duration(lock_start[tag], lock_end[tag])) "\t" show(lock_outcome[tag]) "\t" show(duration(receipt_start[tag], receipt_end[tag])) "\t" show(duration(enqueue_start[tag], enqueue_end[tag])) "\t" show(enqueue_outcome[tag]) "\t" show(admission_to_enqueue) "\t" show(client_latency[tag]) "\t" correlation "\t" reason
  }
  # gawk 5.2.1 faults on the old indexed tags[] pair traversal.
  for (waiter in wait_tags) {
    if (succeeded[waiter] != "true") continue
    for (holder in hold_tags) {
      if (holder == waiter || succeeded[holder] != "true") continue
      if (++pair_checks > max_pair_checks) { pair_limit_exceeded = 1; fatal++; break }
      if (lock_file[waiter] != hold_file[holder]) continue
      if (lock_shard[waiter] == hold_shard[holder] && overlaps(lock_start[waiter], lock_end[waiter], hold_start[holder], hold_end[holder])) same_shard_overlaps++
    }
    if (pair_limit_exceeded) break
  }
  drop_reason_mismatch = pre_enqueue_capacity_drops + pre_enqueue_trylock_drops > pre_enqueue_drops
  if (drop_reason_mismatch || pre_enqueue_reason_incomplete > 0) fatal++
  pre_enqueue_unclassified_drops = pre_enqueue_drops - pre_enqueue_capacity_drops - pre_enqueue_trylock_drops
  status = fatal > 0 ? "invalid" : (client_failures > 0 ? "errors" : ((incomplete + pre_enqueue_drops > 0) ? "incomplete" : "valid"))
  print "# status=" status
  print "# client_tags=" tag_count
  print "# valid=" valid + 0
  print "# incomplete=" incomplete + 0
  print "# client_failures=" client_failures + 0
  print "# duplicate_events=" duplicate_events + 0
  print "# negative_durations=" negative_duration + 0
  print "# lock_shard_mismatches=" shard_mismatches + 0
  print "# lock_shard_out_of_range=" shard_out_of_range + 0
  print "# lock_outcome_failures=" lock_failures + 0
  print "# enqueue_outcome_failures=" enqueue_failures + 0
  print "# order_invalid=" order_invalid + 0
  print "# positive_lock_waits_over_2048ns=" positive_lock_waits + 0
  print "# pair_checks=" pair_checks + 0
  print "# pair_check_limit=" max_pair_checks
  print "# pair_limit_exceeded=" pair_limit_exceeded + 0
  print "# same_shard_wait_hold_overlaps=" same_shard_overlaps + 0
  print "# hold_end_semantics=timestamp_before_release_lower_bound"
  print "# overlap_rule=wait_overlaps_recorded_conservative_hold"
  print "# pre_enqueue_drops=" pre_enqueue_drops + 0
  print "# pre_enqueue_dropped_capacity=" pre_enqueue_capacity_drops + 0
  print "# pre_enqueue_dropped_trylock=" pre_enqueue_trylock_drops + 0
  print "# pre_enqueue_dropped_unclassified=" pre_enqueue_unclassified_drops + 0
  print "# drop_reason_mismatch=" drop_reason_mismatch + 0
  print "# pre_enqueue_dropped_reason_incomplete=" pre_enqueue_reason_incomplete + 0
  print "# server_drops=" server_drops + 0
  expected = "pre_enqueue_clone pre_enqueue_validate pre_enqueue_fingerprint pre_enqueue_encode request_lock_wait request_lock_hold materializer_receipt_pre_enqueue batcher_enqueue mutation_admission batch_proposal"
  count_expected = split(expected, names, " ")
  for (j = 1; j <= count_expected; j++) print "# missing_" names[j] "=" missing_stage[names[j]] + 0
  if (fatal > 0) exit 1
}
