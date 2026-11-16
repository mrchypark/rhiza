# Join fixed-schema client tag samples to tagged server boundaries.
# Duplicate or foreign tags and mismatched slots fail closed. Missing events stay NA.
function field(line, name,    i, pair) {
  for (i = 1; i <= NF; i++) {
    split($i, pair, "=")
    if (pair[1] == name) return pair[2]
  }
  return ""
}
function show(value) { return value == "" ? "NA" : value }
function in_sample_domain(tag) {
  return tag_sample_stride == 16 && tag_sample_max ~ /^[0-9]+$/ && tag ~ /^[1-9][0-9]*$/ && tag <= tag_sample_max && tag % tag_sample_stride == 0
}
function ordered(tag) {
  if (cstart[tag] > csend[tag] + 2048 || csend[tag] > receive[tag] + 2048) return 0
  if (receive[tag] > entry[tag] + 2048 || entry[tag] > admission_start[tag] + 2048) return 0
  if (admission_start[tag] > admission_end[tag] + 2048 || admission_end[tag] > proposal_start[tag] + 2048) return 0
  if (proposal_start[tag] > proposal_end[tag] + 2048 || proposal_end[tag] > result_end[tag] + 2048) return 0
  if (result_end[tag] > handler_end[tag] + 2048) return 0
  return 1
}
BEGIN {
  while ((getline line < client_file) > 0) {
    split(line, cols, "\t")
    tag = cols[1]
    if (tag !~ /^[1-9][0-9]*$/ || cols[8] == "") {
      malformed_client_rows++
      fatal++
      continue
    }
    if (tag in cslot) {
      duplicate_client_tags++
      invalid[tag] = 1
      fatal++
      continue
    }
    tags[++count] = tag
    cslot[tag] = cols[2]; cstart[tag] = cols[3]; cend[tag] = cols[4]
    csend[tag] = cols[5]; cheader[tag] = cols[6]; clatency[tag] = cols[7]
    csucceeded[tag] = cols[8]
  }
  close(client_file)
}
/^rhiza_latency dropped=/ {
  split($2, pair, "="); drops += pair[2]
  next
}
/^rhiza_latency stage=/ {
  stage = field($0, "stage"); tag = field($0, "tag"); slot = field($0, "slot")
  if (tag == "0" || stage == "") next
  if (!(tag in cslot)) {
    if (in_sample_domain(tag)) {
      key = tag SUBSEP stage
      if (event_seen[key]++) { duplicate_server_events++; fatal++ }
      else if (!(tag in unpaired_tag)) { unpaired_tag[tag] = 1; unpaired_sampled_tags++ }
      next
    }
    foreign_server_events++
    fatal++
    next
  }
  if (stage !~ /^(http_receive|http_response|mutation_entry|mutation_admission|batch_proposal|batch_result)$/) next
  key = tag SUBSEP stage
  if (event_seen[key]++) {
    duplicate_server_events++
    invalid[tag] = 1
    fatal++
    next
  }
  start = field($0, "start_unix_ns"); finish = field($0, "end_unix_ns")
  if ((stage == "batch_proposal" || stage == "batch_result") && (slot == "0" || slot != cslot[tag])) {
    if (!slot_mismatch[tag]) { slot_mismatches++; fatal++ }
    slot_mismatch[tag] = 1
  }
  if (stage == "http_receive") receive[tag] = start
  else if (stage == "http_response") {
    handler_end[tag] = finish
    http_outcome[tag] = field($0, "outcome"); http_status[tag] = field($0, "count")
  } else if (stage == "mutation_entry") entry[tag] = start
  else if (stage == "mutation_admission") { admission_start[tag] = start; admission_end[tag] = finish }
  else if (stage == "batch_proposal") { proposal_start[tag] = start; proposal_end[tag] = finish; proposal_slot[tag] = slot }
  else if (stage == "batch_result") { result_end[tag] = finish; result_slot[tag] = slot }
}
END {
  print "# epoch_ns floats have finite precision; differences within 2048 ns are unknown"
  print "# tag\tclient_start\tclient_end\tlatency_ns\tsend\tresponse_headers\tslot\tsucceeded\tserver_outcome\tserver_status\tserver_receive\tmutation_entry\tadmission_start\tadmission_end\tproposal_start\tproposal_end\tproposal_slot\tresult_end\tresult_slot\thandler_end\tsend_to_receive_ns\treceive_to_entry_ns\tentry_to_admission_end_ns\tadmission_to_proposal_ns\tready_to_client_headers_ns\tcorrelation\treason"
  for (i = 1; i <= count; i++) {
    tag = tags[i]
    missing = 0
    if (!(tag in receive)) { missing_receive++; missing++ }
    if (!(tag in entry)) { missing_entry++; missing++ }
    if (!(tag in admission_end)) { missing_admission++; missing++ }
    if (!(tag in proposal_start)) { missing_proposal++; missing++ }
    if (!(tag in result_end)) { missing_result++; missing++ }
    if (!(tag in handler_end)) { missing_response++; missing++ }

    correlation = "valid"; reason = "complete"
    if (csend[tag] == "0" || cheader[tag] == "0") {
      correlation = "transmit_failure"; reason = "no_response_headers"
      transmit_failures++
    } else if (csucceeded[tag] != "true" || (tag in http_outcome && http_outcome[tag] != "ok")) {
      correlation = "request_error"; reason = "client_or_http_failure"
      if (tag in http_outcome && http_outcome[tag] != "ok") http_failures++; else client_failures++
    } else if (invalid[tag] || slot_mismatch[tag]) {
      correlation = "invalid"; reason = "duplicate_or_slot_mismatch"
    } else if (missing) {
      correlation = "incomplete"; reason = "missing_server_event"
      incomplete_correlations++
    } else if (!ordered(tag)) {
      correlation = "invalid"; reason = "boundary_order"
      order_invalid++
      fatal++
    } else if (cslot[tag] == "0" || proposal_slot[tag] != cslot[tag] || result_slot[tag] != cslot[tag]) {
      correlation = "invalid"; reason = "slot_mismatch"
      slot_mismatches++
      fatal++
    } else {
      valid_correlations++
    }

    s2r = (tag in receive) ? receive[tag] - csend[tag] : ""
    r2e = (tag in receive && tag in entry) ? entry[tag] - receive[tag] : ""
    e2a = (tag in entry && tag in admission_end) ? admission_end[tag] - entry[tag] : ""
    a2p = (tag in admission_end && tag in proposal_start) ? proposal_start[tag] - admission_end[tag] : ""
    cross = (tag in handler_end && cheader[tag] != "0") ? cheader[tag] - handler_end[tag] : ""
    r2c = (cross != "" && cross > 2048) ? cross : ""
    if (correlation == "valid" && r2c == "") { cross_process_order_unknown++; reason = "complete_cross_process_order_unknown" }
    print tag "\t" cstart[tag] "\t" cend[tag] "\t" clatency[tag] "\t" csend[tag] "\t" cheader[tag] "\t" cslot[tag] "\t" csucceeded[tag] "\t" show(http_outcome[tag]) "\t" show(http_status[tag]) "\t" show(receive[tag]) "\t" show(entry[tag]) "\t" show(admission_start[tag]) "\t" show(admission_end[tag]) "\t" show(proposal_start[tag]) "\t" show(proposal_end[tag]) "\t" show(proposal_slot[tag]) "\t" show(result_end[tag]) "\t" show(result_slot[tag]) "\t" show(handler_end[tag]) "\t" show(s2r) "\t" show(r2e) "\t" show(e2a) "\t" show(a2p) "\t" show(r2c) "\t" correlation "\t" reason
  }
  status = fatal > 0 ? "invalid" : ((transmit_failures + http_failures + client_failures) > 0 ? "errors" : (incomplete_correlations > 0 ? "incomplete" : "valid"))
  print "# correlation_status=" status
  print "# client_tags=" count
  print "# valid_correlations=" valid_correlations + 0
  print "# incomplete_correlations=" incomplete_correlations + 0
  print "# transmit_failures=" transmit_failures + 0
  print "# http_failures=" http_failures + 0
  print "# client_failures=" client_failures + 0
  print "# malformed_client_rows=" malformed_client_rows + 0
  print "# duplicate_client_tags=" duplicate_client_tags + 0
  print "# foreign_server_events=" foreign_server_events + 0
  print "# unpaired_sampled_tags=" unpaired_sampled_tags + 0
  print "# duplicate_server_events=" duplicate_server_events + 0
  print "# cross_process_order_unknown=" cross_process_order_unknown + 0
  print "# slot_mismatches=" slot_mismatches + 0
  print "# order_invalid=" order_invalid + 0
  print "# missing_receive=" missing_receive + 0
  print "# missing_mutation_entry=" missing_entry + 0
  print "# missing_admission=" missing_admission + 0
  print "# missing_proposal=" missing_proposal + 0
  print "# missing_result=" missing_result + 0
  print "# missing_response=" missing_response + 0
  print "# server_drops=" drops + 0
  if (fatal > 0) exit 1
}
