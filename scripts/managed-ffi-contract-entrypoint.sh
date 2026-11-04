#!/bin/sh
# CI image only. Do not run beside another process owning this Pod's WAL.
set -eu
umask 077
diag_dir=/var/lib/rhiza/contract-diagnostics
emit_first_contract() {
  if [ -f "$diag_dir/first.log" ]; then
    printf '%s\n' 'FIRST_CONTRACT_DIAGNOSTIC_BEGIN' >&2
    sed -n '1,200p' "$diag_dir/first.log" >&2
    for first_stage in go-initial rust-initial internal-profile-initial graph-initial graph-profile-initial graph-qualification-initial; do
      if [ -f "$diag_dir/$first_stage.exit" ]; then
        first_code=$(cat "$diag_dir/$first_stage.exit")
        case "$first_code" in ''|*[!0-9]*) first_code=unknown ;; esac
        printf 'FIRST_CONTRACT_EXIT stage=%s code=%.3s\n' "$first_stage" "$first_code" >&2
      fi
    done
    printf '%s\n' 'FIRST_CONTRACT_DIAGNOSTIC_END' >&2
  else
    printf '%s\n' 'FIRST_CONTRACT_DIAGNOSTIC_UNAVAILABLE' >&2
  fi
}
run_contract() {
  stage=$1
  shift
  case "$stage" in
    go-initial|rust-initial|internal-profile-initial|graph-initial|graph-profile-initial|graph-qualification-initial)
      [ ! -e "$diag_dir/$stage.exit" ] || { emit_first_contract; return 125; }
      printf 'FIRST_CONTRACT_STAGE %s started\n' "$stage" >> "$diag_dir/first.log"
      sync
      ;;
    go-restart|rust-restart|internal-profile-restart|graph-restart|graph-profile-restart|graph-qualification-restart) rm -f "$diag_dir/$stage.exit" ;;
    *) echo 'FAIL: invalid contract diagnostic stage' >&2; return 125 ;;
  esac
  # Drain all child output without SIGPIPE or limits on its WAL/SQLite files.
  # Persist only fixed markers, error classes and source line numbers—not raw
  # messages, profile data, SQL values, endpoints or credentials.
  (
    set +e
    "$@"
    code=$?
    printf '%s\n' "$code" > "$diag_dir/$stage.exit"
  ) 2>&1 | awk '
    function emit(s) { if (used+length(s)+1<=65536) { print s; used+=length(s)+1 } }
    function knowncode(s) { return s ~ /^(internal|commit_unknown|kv_recovery_required|kv_not_submitted|sql_recovery_required|sql_not_submitted|timeout|canceled|invalid_request|not_ready|quorum_unavailable|request_conflict|durability_unavailable|overloaded|unclassified)$/ }
    function knownadmissioncause(s) { return s ~ /^(canceled|deadline|not_ready|invalid_request|request_conflict|quorum_unavailable|other)$/ }
    function uint(s) { return s ~ /^[0-9]+$/ && length(s)<=20 }
    function bit(s) { return s ~ /^(true|false)$/ }
    function stripgotimestamp(s) { sub(/^[^ ]+ [0-9][0-9]:[0-9][0-9]:[0-9][0-9] /,"",s); return s }
    /^GRAPH_STARTUP_DIAGNOSTIC last_stage=(target_inspect|core_open|archive_recover|pair_select|pair_reconstruct|pair_install|runtime_verify|open_complete|not_observed)$/ { emit($0); next }
    {
      refusal=$0
      diagnostic=stripgotimestamp($0)
      if (diagnostic ~ /^MANAGED_GRAPH_ADMISSION_FAILURE/) {
        if (diagnostic ~ /^MANAGED_GRAPH_ADMISSION_FAILURE route=(initial|admitted) stage=(runtime_pre|pre_control|read_index|owned_wait|wait_tip|replay|runtime_post|fixed_replay|authority) read_target_known=false cause=[a-z_]+$/) {
          split(diagnostic, admission_fields, " ")
          cause=admission_fields[5]; sub(/^cause=/,"",cause)
          if (knownadmissioncause(cause)) emit(diagnostic)
        } else if (diagnostic ~ /^MANAGED_GRAPH_ADMISSION_FAILURE route=(initial|admitted) stage=(owned_wait|wait_tip|replay|runtime_post|fixed_replay|authority) read_target_known=true target=[0-9]+ cause=[a-z_]+$/) {
          split(diagnostic, admission_fields, " ")
          target=admission_fields[5]; sub(/^target=/,"",target)
          cause=admission_fields[6]; sub(/^cause=/,"",cause)
          if (uint(target) && knownadmissioncause(cause)) emit(diagnostic)
        } else if (diagnostic ~ /^MANAGED_GRAPH_ADMISSION_FAILURE route=admitted stage=(pre_control|read_index) round=2 read_target_known=false cause=[a-z_]+$/) {
          split(diagnostic, admission_fields, " ")
          cause=admission_fields[6]; sub(/^cause=/,"",cause)
          if (knownadmissioncause(cause)) emit(diagnostic)
        } else if (diagnostic ~ /^MANAGED_GRAPH_ADMISSION_FAILURE route=admitted stage=(wait_tip|replay|runtime_post) round=2 read_target_known=true target=[0-9]+ cause=[a-z_]+$/) {
          split(diagnostic, admission_fields, " ")
          target=admission_fields[6]; sub(/^target=/,"",target)
          cause=admission_fields[7]; sub(/^cause=/,"",cause)
          if (uint(target) && knownadmissioncause(cause)) emit(diagnostic)
        }
        next
      }
      if (diagnostic ~ /^MANAGED_GRAPH_ADMISSION_REFUSED stage=history_compare/) {
        if (diagnostic ~ /^MANAGED_GRAPH_ADMISSION_REFUSED stage=history_compare route=(initial|admitted) read_target_known=true target=[0-9]+ recorded=[0-9]+ certified=[0-9]+ applied=[0-9]+ c_below_target=(true|false) applied_differs_certified=(true|false) recorded_above_certified=(true|false)$/) {
          split(diagnostic, history_fields, " ")
          target=history_fields[5]; sub(/^target=/,"",target)
          recorded=history_fields[6]; sub(/^recorded=/,"",recorded)
          certified=history_fields[7]; sub(/^certified=/,"",certified)
          applied=history_fields[8]; sub(/^applied=/,"",applied)
          if (uint(target) && uint(recorded) && uint(certified) && uint(applied)) emit(diagnostic)
        }
        next
      }
      if (diagnostic ~ /^MANAGED_HOST_REQUEST_RECOVERY_FAILURE/) {
        if (diagnostic == "MANAGED_HOST_REQUEST_RECOVERY_FAILURE stage=host_precheck cause=not_ready") emit(diagnostic)
        next
      }
      if (diagnostic ~ /^MANAGED_REQUEST_RESOLUTION_FAILURE/) {
        if (diagnostic ~ /^MANAGED_REQUEST_RESOLUTION_FAILURE stage=(resolve_borrow|resolve_admit) cause=[a-z_]+$/) {
          split(diagnostic, request_fields, " ")
          cause=request_fields[3]; sub(/^cause=/,"",cause)
          if (knownadmissioncause(cause)) emit(diagnostic)
        } else if (diagnostic ~ /^MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=error cause=[a-z_]+$/) {
          split(diagnostic, request_fields, " ")
          cause=request_fields[4]; sub(/^cause=/,"",cause)
          if (knownadmissioncause(cause)) emit(diagnostic)
        } else if (diagnostic == "MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=not_found" || diagnostic == "MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=found slot_known=false") {
          emit(diagnostic)
        } else if (diagnostic ~ /^MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=found slot_known=true slot=[1-9][0-9]*$/) {
          split(diagnostic, request_fields, " ")
          slot=request_fields[5]; sub(/^slot=/,"",slot)
          if (uint(slot)) emit(diagnostic)
        }
        next
      }
      if (diagnostic ~ /^MANAGED_GRAPH_ADMISSION_REFUSED stage=initial_admission/) {
        if (diagnostic ~ /^MANAGED_GRAPH_ADMISSION_REFUSED stage=initial_admission target=[0-9]+ recorded=[0-9]+ certified=[0-9]+ applied=[0-9]+$/) {
          split(diagnostic, initial_fields, " ")
          target=initial_fields[3]; sub(/^target=/,"",target)
          recorded=initial_fields[4]; sub(/^recorded=/,"",recorded)
          certified=initial_fields[5]; sub(/^certified=/,"",certified)
          applied=initial_fields[6]; sub(/^applied=/,"",applied)
          if (uint(target) && uint(recorded) && uint(certified) && uint(applied))
            emit(diagnostic)
        }
        next
      }
      request_admission=diagnostic
      if (request_admission ~ /^MANAGED_REQUEST_RESOLUTION_ADMISSION_FAILURE/) {
        if (request_admission ~ /^MANAGED_REQUEST_RESOLUTION_ADMISSION_FAILURE stage=request_recovery_admission cause=[a-z_]+$/) {
          split(request_admission, request_fields, " ")
          cause=request_fields[3]; sub(/^cause=/,"",cause)
          if (knownadmissioncause(cause)) emit(request_admission)
        }
        next
      }
      refusal=diagnostic
      if (refusal ~ /^[0-9][0-9][0-9][0-9]\/[0-9][0-9]\/[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9] MANAGED_GRAPH_ADMISSION_REFUSED /)
        sub(/^[0-9][0-9][0-9][0-9]\/[0-9][0-9]\/[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9] /,"",refusal)
      if (refusal ~ /^MANAGED_GRAPH_ADMISSION_REFUSED/) {
        if (refusal ~ /^MANAGED_GRAPH_ADMISSION_REFUSED target=[0-9]+ recorded=[0-9]+ certified=[0-9]+ applied=[0-9]+$/) {
          split(refusal, refusal_fields, " ")
          target=refusal_fields[2]; sub(/^target=/,"",target)
          recorded=refusal_fields[3]; sub(/^recorded=/,"",recorded)
          certified=refusal_fields[4]; sub(/^certified=/,"",certified)
          applied=refusal_fields[5]; sub(/^applied=/,"",applied)
          if (uint(target) && uint(recorded) && uint(certified) && uint(applied))
            emit("MANAGED_GRAPH_ADMISSION_REFUSED target=" target " recorded=" recorded " certified=" certified " applied=" applied)
        }
        next
      }
    }
    /managed operation unavailable / {
      tuple=$0; sub(/^.*managed operation unavailable /,"",tuple)
      if (tuple ~ /^operation=(execute|query|ready|request_status|recover_pending|recover_pending_detailed|kv_put|kv_delete|kv_cas|kv_get|graph_execute|graph_query|graph_reachable|graph_changes|graph_stream_read|graph_stream_offset|unsupported) code=[a-z_]+ cause=(unclassified|deadline|canceled|quorum_unavailable|durability_unavailable|not_ready|overloaded|request_conflict) context_known=(true|false) slot=[0-9]+ retry=[0-9]+$/) {
        split(tuple,fields," "); code=fields[2]; sub(/^code=/,"",code)
        slot=fields[5]; sub(/^slot=/,"",slot); retry=fields[6]; sub(/^retry=/,"",retry)
        if (knowncode(code) && uint(slot) && uint(retry)) emit("MANAGED_OPERATION_DIAGNOSTIC " tuple)
      }
      next
    }
    /^(GRAPH_STEP_DIAGNOSTIC |FAIL: managed-ha-contract )graph_step / {
      tuple=$0; sub(/^(GRAPH_STEP_DIAGNOSTIC |FAIL: managed-ha-contract )graph_step /,"",tuple)
      if (tuple ~ /^stage=(graph_execute|graph_recover|graph_query|graph_stream_read|graph_stream_offset|graph_status|graph_encode_original|graph_encode_execution|sql_schema|sql_tuple|sql_query|close|unclassified) ordinal=[0-9]+ code=[a-z_]+ context_known=(true|false) slot=[0-9]+ retry=[0-9]+$/) {
        split(tuple,fields," "); ordinal=fields[2]; sub(/^ordinal=/,"",ordinal); code=fields[3]; sub(/^code=/,"",code)
        slot=fields[5]; sub(/^slot=/,"",slot); retry=fields[6]; sub(/^retry=/,"",retry)
        if ((knowncode(code) || code ~ /^mixed_sql_(ack_missing|slot_missing|slot_zero)$/) && uint(ordinal) && uint(slot) && uint(retry)) emit("RUST_GRAPH_OPERATION_DIAGNOSTIC " tuple)
      }
      next
    }
    /managed startup unavailable / {
      tuple=$0; sub(/^.*managed startup unavailable /,"",tuple)
      if (tuple ~ /^stage=(load_config|host_open|ready) code=[a-z_]+ cause=(unclassified|deadline|canceled|bootstrap_pending|learner_pending|application_pending|required_root_unavailable|source_prefix_mismatch|identity_mismatch|unresolved_certified_history|pending_graph_state|graph_commit_unknown|budget_refusal)$/) {
        split(tuple,fields," "); code=fields[2]; sub(/^code=/,"",code)
        if (knowncode(code)) emit("MANAGED_STARTUP_DIAGNOSTIC " tuple)
      }
      next
    }
    /^=== RUN   TestManagedFFIInCluster(SQL|Mixed|Graph)Lifecycle$/ { emit($0); next }
    /^--- (PASS|FAIL): TestManagedFFIInCluster(SQL|Mixed|Graph)Lifecycle / { emit(substr($0,1,160)); next }
    /^=== RUN   TestManagedGraphMaterializationMarker$/ { emit($0); next }
    /^--- (PASS|FAIL): TestManagedGraphMaterializationMarker / { emit(substr($0,1,160)); next }
    /^=== RUN   TestManagedFFIInClusterGraphQualification$/ { emit($0); next }
    /^--- (PASS|FAIL): TestManagedFFIInClusterGraphQualification / { emit(substr($0,1,160)); next }
    /^[[:space:]]*managed_graph_qualification_ci_test.go:[0-9]+:/ { emit("GO_QUALIFICATION_SOURCE " $1); next }
    /^GRAPH_CABI_QUALIFICATION=/ { if (length($0)<=2048) emit($0); next }
    /^PASS: actual Go CABI Graph native\/root\/counter qualification$/ { emit($0); next }
    /^[[:space:]]*managed_incluster_test.go:[0-9]+:/ {
      emit("GO_CONTRACT_SOURCE " $1)
      if (match($0,/code=[a-z_]+/)) emit(substr($0,RSTART,RLENGTH))
      detail=$0; sub(/^.*real managed FFI open failed, code=/,"",detail)
      split(detail,fields," "); code=fields[1]
      if (knowncode(code) && detail ~ /^[a-z_]+ message=managed startup (inputs rejected|rejected)$/) emit("GO_MANAGED_OPEN code=" code " " substr(detail,length(code)+2))
      if (index($0,"actual in-cluster Go FFI -> managed Host -> SQL ACK readback -> close passed")) emit("actual in-cluster Go FFI -> managed Host -> SQL ACK readback -> close passed")
      if (index($0,"actual in-cluster Go FFI -> mixed managed Host -> SQL KV ACK readback -> close passed")) emit("actual in-cluster Go FFI -> mixed managed Host -> SQL KV ACK readback -> close passed")
      if (index($0,"actual in-cluster Go FFI -> Graph managed Host -> raw original ACK/readback/status/recovery -> close passed")) emit("actual in-cluster Go FFI -> Graph managed Host -> raw original ACK/readback/status/recovery -> close passed")
      next
    }
    /^FAIL: managed-ha-contract [a-z_]+$/ { emit(substr($0,1,160)); next }
    /^FAIL: managed-ha-contract graph_(open|ready) / {
      detail=$0; sub(/^FAIL: managed-ha-contract /,"",detail)
      split(detail,fields," "); code=fields[2]; sub(/:$/,"",code)
      if (knowncode(code) && detail ~ /^graph_(open|ready) [a-z_]+: managed (startup inputs rejected|startup rejected|operation rejected)$/) emit("FAIL: managed-ha-contract " detail)
      next
    }
    /^PASS: managed Rust FFI SQL contract$/ { emit($0); next }
    /^PASS: prior managed Rust ACK verified$/ { emit($0); next }
    /^PASS: managed Rust FFI mixed (initial|verify) contract$/ { emit($0); next }
    /^RUST_MIXED_ROW_ACK_SLOT=[0-9]+$/ { emit($0); next }
    /^RUST_MIXED_CURRENT_UID=[0-9a-fA-F-]+$/ { emit($0); next }
    /^RUST_MIXED_TUPLE=/ { if (length($0)<=16384) emit($0); next }
    /^RHIZA_ACTUAL_MATERIALIZATION_PROFILE=9$/ { emit($0); next }
    /^RHIZA_ACTUAL_MATERIALIZATION_PROFILE=10$/ { emit($0); next }
    /^PASS: managed Rust FFI Graph (initial|verify) state contract$/ { emit($0); next }
    /^RUST_GRAPH_CURRENT_UID=[0-9a-fA-F-]+$/ { emit($0); next }
    /^RUST_GRAPH_TUPLE=/ { if (length($0)<=32768) emit($0); next }
    /^panic: test timed out after [0-9hms.]+$/ { emit($0); next }
    /^(PASS|FAIL)$/ { emit($0) }
  ' > "$diag_dir/active.log"
  code=$(cat "$diag_dir/$stage.exit")
  case "$code" in ''|*[!0-9]*) echo 'FAIL: contract exit outcome unavailable' >&2; return 125 ;; esac
  case "$stage:$code" in
    graph-qualification-*:0)
      if ! grep -Fx '=== RUN   TestManagedFFIInClusterGraphQualification' "$diag_dir/active.log" >/dev/null ||
         ! grep -E '^--- PASS: TestManagedFFIInClusterGraphQualification ' "$diag_dir/active.log" >/dev/null ||
         [ "$(grep -c '^GRAPH_CABI_QUALIFICATION=' "$diag_dir/active.log")" != 1 ] ||
         ! grep -Fx 'PASS: actual Go CABI Graph native/root/counter qualification' "$diag_dir/active.log" >/dev/null; then
        code=1
        printf '%s\n' "$code" > "$diag_dir/$stage.exit"
        printf '%s\n' 'FAIL: managed-ha-contract graph_qualification_not_executed' >> "$diag_dir/active.log"
      fi
      ;;
    graph-profile-*:0)
      if ! grep -Fx '=== RUN   TestManagedGraphMaterializationMarker' "$diag_dir/active.log" >/dev/null ||
         ! grep -E '^--- PASS: TestManagedGraphMaterializationMarker ' "$diag_dir/active.log" >/dev/null ||
         ! grep -Fx 'RHIZA_ACTUAL_MATERIALIZATION_PROFILE=10' "$diag_dir/active.log" >/dev/null; then
        code=1
        printf '%s\n' "$code" > "$diag_dir/$stage.exit"
        printf '%s\n' 'FAIL: managed-ha-contract graph_profile_test_not_executed' >> "$diag_dir/active.log"
      fi
      ;;
  esac
  case "$stage" in
    go-initial|rust-initial|internal-profile-initial|graph-initial|graph-profile-initial|graph-qualification-initial)
      remaining=32600
      awk -v remaining="$remaining" '{ if (remaining>0) { text=substr($0,1,remaining-1); print text; remaining-=length(text)+1 } }' "$diag_dir/active.log" >> "$diag_dir/first.log"
      ;;
  esac
  sync
  cat "$diag_dir/active.log"
  if [ "$code" != 0 ]; then emit_first_contract; return "$code"; fi
}
if [ "${1-}" = --diagnostic-self-test ]; then
  diag_test_dir=$(mktemp -d)
  trap 'rm -f "$diag_test_dir/first.log" "$diag_test_dir/active.log" "$diag_test_dir/go-initial.exit" "$diag_test_dir/rust-initial.exit" "$diag_test_dir/rust-restart.exit" "$diag_test_dir/graph-profile-restart.exit" "$diag_test_dir/graph-qualification-restart.exit"; rmdir "$diag_test_dir"' EXIT HUP INT TERM
  diag_dir=$diag_test_dir
  : > "$diag_dir/first.log"
  if run_contract go-initial sh -c 'printf "%s\n" "private-profile-DO-NOT-RETAIN" "    managed_incluster_test.go:123: private-profile-DO-NOT-RETAIN"; exit 7'; then exit 1; else [ "$?" = 7 ]; fi
  grep -Fx 'GO_CONTRACT_SOURCE managed_incluster_test.go:123:' "$diag_dir/first.log" >/dev/null
  if grep -F 'DO-NOT-RETAIN' "$diag_dir/first.log"; then exit 1; fi
  before=$(cksum "$diag_dir/first.log")
  if run_contract rust-restart sh -c 'echo "FAIL: managed-ha-contract prior_ack_marker_missing"; exit 1'; then exit 1; fi
  [ "$before" = "$(cksum "$diag_dir/first.log")" ]
  [ "$(cat "$diag_dir/go-initial.exit")" = 7 ]
  run_contract rust-restart sh -c 'printf "%s\n" \
    "2026/11/03 12:00:00 managed startup unavailable stage=host_open code=internal cause=required_root_unavailable" \
    "    managed_incluster_test.go:73: real managed FFI open failed, code=internal message=managed startup rejected" \
    "FAIL: managed-ha-contract graph_open internal: managed startup rejected" \
    "FAIL: managed-ha-contract graph_ready not_ready: managed operation rejected" \
    "managed startup unavailable stage=ready code=internal cause=source_prefix_mismatch" \
    "managed startup unavailable stage=ready code=internal cause=source_prefix_mismatch secret-DO-NOT-RETAIN" \
    "managed startup unavailable stage=other code=internal cause=unclassified" \
    "managed startup unavailable stage=host_open code=internal cause=unclassified secret-DO-NOT-RETAIN" \
    "FAIL: managed-ha-contract graph_open internal: managed startup rejected secret-DO-NOT-RETAIN" \
    "    managed_incluster_test.go:74: real managed FFI open failed, code=internal message=managed startup rejected secret-DO-NOT-RETAIN"'
  grep -Fx 'MANAGED_STARTUP_DIAGNOSTIC stage=host_open code=internal cause=required_root_unavailable' "$diag_dir/active.log" >/dev/null
  grep -Fx 'GO_MANAGED_OPEN code=internal message=managed startup rejected' "$diag_dir/active.log" >/dev/null
  grep -Fx 'FAIL: managed-ha-contract graph_open internal: managed startup rejected' "$diag_dir/active.log" >/dev/null
  grep -Fx 'FAIL: managed-ha-contract graph_ready not_ready: managed operation rejected' "$diag_dir/active.log" >/dev/null
  grep -Fx 'MANAGED_STARTUP_DIAGNOSTIC stage=ready code=internal cause=source_prefix_mismatch' "$diag_dir/active.log" >/dev/null
  if grep -E 'DO-NOT-RETAIN|stage=other' "$diag_dir/active.log"; then exit 1; fi
  for cause in unclassified deadline canceled bootstrap_pending learner_pending application_pending required_root_unavailable source_prefix_mismatch identity_mismatch unresolved_certified_history pending_graph_state graph_commit_unknown budget_refusal; do
    run_contract rust-restart sh -c 'printf "managed startup unavailable stage=load_config code=internal cause=%s\n" "$1"' sh "$cause"
    grep -Fx "MANAGED_STARTUP_DIAGNOSTIC stage=load_config code=internal cause=$cause" "$diag_dir/active.log" >/dev/null
  done
  for startup_stage in target_inspect core_open archive_recover pair_select pair_reconstruct pair_install runtime_verify open_complete not_observed; do
    run_contract rust-restart sh -c 'printf "GRAPH_STARTUP_DIAGNOSTIC last_stage=%s\n" "$1"' sh "$startup_stage"
    grep -Fx "GRAPH_STARTUP_DIAGNOSTIC last_stage=$startup_stage" "$diag_dir/active.log" >/dev/null
  done
  run_contract rust-restart sh -c 'printf "%s\n" \
    "2026/11/03 12:00:00 managed operation unavailable operation=graph_execute code=commit_unknown cause=canceled context_known=true slot=17 retry=1041" \
    "GRAPH_STEP_DIAGNOSTIC graph_step stage=graph_execute ordinal=2 code=commit_unknown context_known=true slot=0 retry=18446744073709551615" \
    "FAIL: managed-ha-contract graph_step stage=sql_tuple ordinal=0 code=commit_unknown context_known=false slot=0 retry=0" \
    "GRAPH_STARTUP_DIAGNOSTIC last_stage=pair_install secret-DO-NOT-RETAIN" \
    "GRAPH_STARTUP_DIAGNOSTIC last_stage=invalid" \
    "managed operation unavailable operation=graph_execute code=commit_unknown cause=canceled context_known=true slot=17 retry=1041 secret-DO-NOT-RETAIN" \
    "FAIL: managed-ha-contract graph_step stage=sql_tuple ordinal=0 code=commit_unknown context_known=false slot=0 retry=0 secret-DO-NOT-RETAIN" \
    "GRAPH_STEP_DIAGNOSTIC graph_step stage=private ordinal=1 code=internal context_known=false slot=0 retry=0"'
  grep -Fx 'MANAGED_OPERATION_DIAGNOSTIC operation=graph_execute code=commit_unknown cause=canceled context_known=true slot=17 retry=1041' "$diag_dir/active.log" >/dev/null
  grep -Fx 'RUST_GRAPH_OPERATION_DIAGNOSTIC stage=graph_execute ordinal=2 code=commit_unknown context_known=true slot=0 retry=18446744073709551615' "$diag_dir/active.log" >/dev/null
  grep -Fx 'RUST_GRAPH_OPERATION_DIAGNOSTIC stage=sql_tuple ordinal=0 code=commit_unknown context_known=false slot=0 retry=0' "$diag_dir/active.log" >/dev/null
  if grep -E 'DO-NOT-RETAIN|stage=private|last_stage=invalid|last_stage=pair_install' "$diag_dir/active.log"; then exit 1; fi
  run_contract rust-restart sh -c 'printf "%s\n" \
    "MANAGED_GRAPH_ADMISSION_REFUSED target=17 recorded=15 certified=15 applied=14" \
    "2026/11/03 12:00:00 MANAGED_GRAPH_ADMISSION_REFUSED target=17 recorded=15 certified=15 applied=14" \
    "MANAGED_GRAPH_ADMISSION_REFUSED target=17 recorded=15 certified=15 applied=14 secret-DO-NOT-RETAIN" \
    "2026/11/03 12:00:00 MANAGED_GRAPH_ADMISSION_REFUSED target=17 recorded=15 certified=15 applied=14 secret-DO-NOT-RETAIN" \
    "MANAGED_GRAPH_ADMISSION_REFUSED target=-1 recorded=15 certified=15 applied=14" \
    "MANAGED_GRAPH_ADMISSION_REFUSED target=17 recorded=15x certified=15 applied=14" \
    "garbage-prefix MANAGED_GRAPH_ADMISSION_REFUSED target=17 recorded=15 certified=15 applied=14"'
  [ "$(grep -Fc 'MANAGED_GRAPH_ADMISSION_REFUSED target=17 recorded=15 certified=15 applied=14' "$diag_dir/active.log")" = 2 ]
  if grep -E 'DO-NOT-RETAIN|MANAGED_GRAPH_ADMISSION_REFUSED.*(target=-1|recorded=15x)|^garbage-prefix MANAGED_GRAPH_ADMISSION_REFUSED' "$diag_dir/active.log"; then exit 1; fi
  run_contract rust-restart sh -c 'printf "%s\n" \
    "MANAGED_GRAPH_ADMISSION_REFUSED stage=initial_admission target=17 recorded=15 certified=14 applied=13" \
    "2026/11/03 12:00:00 MANAGED_GRAPH_ADMISSION_REFUSED stage=initial_admission target=18 recorded=16 certified=15 applied=14" \
    "MANAGED_GRAPH_ADMISSION_REFUSED stage=initial_admission target=17 recorded=15 certified=14 applied=13 secret-DO-NOT-RETAIN" \
    "MANAGED_GRAPH_ADMISSION_REFUSED stage=other target=17 recorded=15 certified=14 applied=13" \
    "MANAGED_GRAPH_ADMISSION_REFUSED stage=initial_admission target=17 recorded=15x certified=14 applied=13" \
    "MANAGED_REQUEST_RESOLUTION_ADMISSION_FAILURE stage=request_recovery_admission cause=not_ready" \
    "2026/11/03 12:00:00 MANAGED_REQUEST_RESOLUTION_ADMISSION_FAILURE stage=request_recovery_admission cause=quorum_unavailable" \
    "MANAGED_REQUEST_RESOLUTION_ADMISSION_FAILURE stage=other cause=not_ready" \
    "MANAGED_REQUEST_RESOLUTION_ADMISSION_FAILURE stage=request_recovery_admission cause=private_cause" \
    "MANAGED_REQUEST_RESOLUTION_ADMISSION_FAILURE stage=request_recovery_admission cause=not_ready secret-DO-NOT-RETAIN"'
  grep -Fx 'MANAGED_GRAPH_ADMISSION_REFUSED stage=initial_admission target=17 recorded=15 certified=14 applied=13' "$diag_dir/active.log" >/dev/null
  grep -Fx 'MANAGED_GRAPH_ADMISSION_REFUSED stage=initial_admission target=18 recorded=16 certified=15 applied=14' "$diag_dir/active.log" >/dev/null
  [ "$(grep -Fc 'MANAGED_REQUEST_RESOLUTION_ADMISSION_FAILURE stage=request_recovery_admission cause=not_ready' "$diag_dir/active.log")" = 1 ]
  grep -Fx 'MANAGED_REQUEST_RESOLUTION_ADMISSION_FAILURE stage=request_recovery_admission cause=quorum_unavailable' "$diag_dir/active.log" >/dev/null
  if grep -E 'DO-NOT-RETAIN|stage=other|private_cause|stage=initial_admission.*recorded=15x' "$diag_dir/active.log"; then exit 1; fi
  run_contract rust-restart sh -c 'printf "%s\n" \
    "MANAGED_GRAPH_ADMISSION_FAILURE route=initial stage=pre_control read_target_known=false cause=quorum_unavailable" \
    "2026/11/03 12:00:00 MANAGED_GRAPH_ADMISSION_FAILURE route=initial stage=read_index read_target_known=false cause=deadline" \
    "MANAGED_GRAPH_ADMISSION_FAILURE route=admitted stage=wait_tip read_target_known=true target=29 cause=deadline" \
    "MANAGED_GRAPH_ADMISSION_FAILURE route=admitted stage=read_index round=2 read_target_known=false cause=quorum_unavailable" \
    "MANAGED_GRAPH_ADMISSION_FAILURE route=admitted stage=replay round=2 read_target_known=true target=31 cause=not_ready" \
    "MANAGED_GRAPH_ADMISSION_REFUSED stage=history_compare route=initial read_target_known=true target=17 recorded=15 certified=14 applied=13 c_below_target=true applied_differs_certified=true recorded_above_certified=true" \
    "MANAGED_HOST_REQUEST_RECOVERY_FAILURE stage=host_precheck cause=not_ready" \
    "MANAGED_REQUEST_RESOLUTION_FAILURE stage=resolve_borrow cause=canceled" \
    "MANAGED_REQUEST_RESOLUTION_FAILURE stage=resolve_admit cause=other" \
    "MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=error cause=not_ready" \
    "MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=not_found" \
    "MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=found slot_known=true slot=7" \
    "MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=found slot_known=false" \
    "MANAGED_GRAPH_ADMISSION_FAILURE route=private stage=pre_control read_target_known=false cause=not_ready" \
    "MANAGED_GRAPH_ADMISSION_FAILURE route=initial stage=private read_target_known=false cause=not_ready" \
    "MANAGED_GRAPH_ADMISSION_FAILURE route=initial stage=pre_control read_target_known=false cause=private_cause" \
    "MANAGED_GRAPH_ADMISSION_FAILURE route=initial stage=pre_control read_target_known=true target=17 cause=not_ready" \
    "MANAGED_GRAPH_ADMISSION_FAILURE route=admitted stage=read_index round=3 read_target_known=false cause=not_ready" \
    "MANAGED_GRAPH_ADMISSION_REFUSED stage=history_compare route=private read_target_known=true target=17 recorded=15 certified=14 applied=13 c_below_target=true applied_differs_certified=true recorded_above_certified=true" \
    "MANAGED_GRAPH_ADMISSION_REFUSED stage=history_compare route=initial read_target_known=true target=17x recorded=15 certified=14 applied=13 c_below_target=true applied_differs_certified=true recorded_above_certified=true" \
    "MANAGED_HOST_REQUEST_RECOVERY_FAILURE stage=host_precheck cause=private_cause" \
    "MANAGED_HOST_REQUEST_RECOVERY_FAILURE stage=host_precheck cause=not_ready secret-DO-NOT-RETAIN" \
    "MANAGED_REQUEST_RESOLUTION_FAILURE stage=private cause=not_ready" \
    "MANAGED_REQUEST_RESOLUTION_FAILURE stage=resolve_admit cause=private_cause" \
    "MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=other" \
    "MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=found slot_known=true slot=0" \
    "MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=not_found secret-DO-NOT-RETAIN"'
  for fixed in \
    'MANAGED_GRAPH_ADMISSION_FAILURE route=initial stage=pre_control read_target_known=false cause=quorum_unavailable' \
    'MANAGED_GRAPH_ADMISSION_FAILURE route=initial stage=read_index read_target_known=false cause=deadline' \
    'MANAGED_GRAPH_ADMISSION_FAILURE route=admitted stage=wait_tip read_target_known=true target=29 cause=deadline' \
    'MANAGED_GRAPH_ADMISSION_FAILURE route=admitted stage=read_index round=2 read_target_known=false cause=quorum_unavailable' \
    'MANAGED_GRAPH_ADMISSION_FAILURE route=admitted stage=replay round=2 read_target_known=true target=31 cause=not_ready' \
    'MANAGED_GRAPH_ADMISSION_REFUSED stage=history_compare route=initial read_target_known=true target=17 recorded=15 certified=14 applied=13 c_below_target=true applied_differs_certified=true recorded_above_certified=true' \
    'MANAGED_HOST_REQUEST_RECOVERY_FAILURE stage=host_precheck cause=not_ready' \
    'MANAGED_REQUEST_RESOLUTION_FAILURE stage=resolve_borrow cause=canceled' \
    'MANAGED_REQUEST_RESOLUTION_FAILURE stage=resolve_admit cause=other' \
    'MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=error cause=not_ready' \
    'MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=not_found' \
    'MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=found slot_known=true slot=7' \
    'MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=found slot_known=false'; do
    grep -Fx "$fixed" "$diag_dir/active.log" >/dev/null
  done
  if grep -E 'DO-NOT-RETAIN|route=private|stage=private|private_cause|round=3|target=17x|slot_known=true slot=0|lookup=other' "$diag_dir/active.log"; then exit 1; fi
  [ "$before" = "$(cksum "$diag_dir/first.log")" ]
  if run_contract rust-initial sh -c 'i=0; while [ "$i" -lt 4000 ]; do echo "FAIL: managed-ha-contract prior_ack_marker_missing"; i=$((i+1)); done; exit 9' >/dev/null 2>&1; then exit 1; else [ "$?" = 9 ]; fi
  [ "$(wc -c < "$diag_dir/first.log")" -le 65536 ]
  [ "$(cat "$diag_dir/rust-initial.exit")" = 9 ]
  grep -Fx 'GO_CONTRACT_SOURCE managed_incluster_test.go:123:' "$diag_dir/first.log" >/dev/null
  if run_contract graph-profile-restart sh -c 'echo PASS'; then exit 1; else [ "$?" = 1 ]; fi
  run_contract graph-profile-restart sh -c 'printf "%s\n" "=== RUN   TestManagedGraphMaterializationMarker" "--- PASS: TestManagedGraphMaterializationMarker (0.01s)" "RHIZA_ACTUAL_MATERIALIZATION_PROFILE=10" "PASS"'
  if run_contract graph-profile-restart sh -c 'printf "%s\n" "=== RUN   TestManagedGraphMaterializationMarker" "--- PASS: TestManagedGraphMaterializationMarker (0.01s)" "RHIZA_ACTUAL_MATERIALIZATION_PROFILE=10"; exit 7'; then exit 1; else [ "$?" = 7 ]; fi
  if run_contract graph-qualification-restart sh -c 'echo PASS'; then exit 1; else [ "$?" = 1 ]; fi
  run_contract graph-qualification-restart sh -c 'printf "%s\n" "=== RUN   TestManagedFFIInClusterGraphQualification" "--- PASS: TestManagedFFIInClusterGraphQualification (0.01s)" "GRAPH_CABI_QUALIFICATION={}" "PASS: actual Go CABI Graph native/root/counter qualification" "PASS"'
  if run_contract graph-qualification-restart sh -c 'echo PASS; exit 7'; then exit 1; else [ "$?" = 7 ]; fi
  exit 0
fi
if [ "${1-}" = serve-managed-sql ]; then
  marker_output=$(RHIZA_RUST_CONTRACT_MARKER=1 /usr/local/libexec/rhiza-managed-ffi-contract \
    -test.run='^TestRustContractRunStartedMarker$' -test.count=1 -test.v -test.timeout=10s)
  fixture_profile=$(printf '%s\n' "$marker_output" | sed -n 's/^RHIZA_RUST_FIXTURE_PROFILE=//p')
  case "$fixture_profile" in sql|mixed|internal|graph) ;; *) echo 'FAIL: explicit fixture profile unavailable' >&2; exit 1 ;; esac
  if [ "$fixture_profile" = mixed ] || [ "$fixture_profile" = internal ] || [ "$fixture_profile" = graph ]; then
    mixed_mode=$(printf '%s\n' "$marker_output" | sed -n 's/^RHIZA_RUST_CONTRACT_MODE=//p')
    producer_uid=$(printf '%s\n' "$marker_output" | sed -n 's/^RHIZA_RUST_CONTRACT_PRODUCER_UID=//p')
    printf '%s\n' "$producer_uid" | grep -Eq '^[0-9a-fA-F-]{16,64}$' || { echo 'FAIL: invalid mixed producer UID' >&2; exit 1; }
    export RHIZA_RUST_PRODUCER_UID="$producer_uid"
    case "$mixed_mode" in
      skip) ;;
      initial|verify)
        if [ -d "$diag_dir" ]; then
          [ "$mixed_mode" = verify ] || { emit_first_contract; exit 125; }
          go_stage=go-restart rust_stage=rust-restart
        else
          mkdir -m 700 "$diag_dir"
          (set -C; : > "$diag_dir/first.log")
          sync
          go_stage=go-initial rust_stage=rust-initial
        fi
        if [ -e /var/lib/rhiza/repair.json ]; then
          [ "$mixed_mode" = verify ] || { echo 'FAIL: replacement cannot run initial mixed writes' >&2; exit 1; }
          lifecycle=Mixed
          [ "$fixture_profile" != graph ] || lifecycle=Graph
          RHIZA_FFI_IN_CLUSTER_CONTRACT=1 run_contract "$go_stage" /usr/local/libexec/rhiza-managed-ffi-contract \
            -test.run="^TestManagedFFIInCluster${lifecycle}Lifecycle$" -test.count=1 -test.v -test.timeout=120s
        fi
        run_contract "$rust_stage" /usr/local/libexec/rhiza-rust-managed-contract "--mixed-$mixed_mode"
        if [ "$fixture_profile" = graph ]; then
          run_contract "graph-${rust_stage#rust-}" /usr/local/libexec/rhiza-rust-managed-contract "--graph-$mixed_mode"
          run_contract "graph-profile-${rust_stage#rust-}" /usr/local/libexec/rhiza-managed-materializer-contract \
            -test.run='^TestManagedGraphMaterializationMarker$' -test.count=1 -test.v -test.timeout=10s
          RHIZA_FFI_GRAPH_QUALIFICATION=1 run_contract "graph-qualification-${rust_stage#rust-}" /usr/local/libexec/rhiza-managed-ffi-contract \
            -test.run='^TestManagedFFIInClusterGraphQualification$' -test.count=1 -test.v -test.timeout=120s
        fi
        if [ "$fixture_profile" = internal ]; then
          RHIZA_INTERNAL_MATERIALIZATION_MARKER=1 run_contract "internal-profile-${rust_stage#rust-}" /usr/local/libexec/rhiza-managed-ffi-contract \
            -test.run='^TestManagedInternalMaterializationMarker$' -test.count=1 -test.v -test.timeout=10s
        fi
        ;;
      *) echo 'FAIL: mixed contract phase unavailable' >&2; exit 1 ;;
    esac
  elif [ -e /var/lib/rhiza/repair.json ]; then
  case "$marker_output" in
    *RHIZA_RUST_CONTRACT_MODE=initial*) rust_initial=1 ;;
    *RHIZA_RUST_CONTRACT_MODE=restart*) rust_initial=0 ;;
    *) echo 'FAIL: Rust run-start marker result missing' >&2; exit 1 ;;
  esac
  if [ "$rust_initial" = 1 ]; then
    mkdir -m 700 "$diag_dir"
    (set -C; : > "$diag_dir/first.log")
    sync
  elif [ ! -d "$diag_dir" ]; then
    # Older runs may lack diagnostics; this is not initial-run permission.
    mkdir -m 700 "$diag_dir"
  fi
  if [ "$rust_initial" = 0 ]; then
    # A prior start with a lost ACK never authorizes another initial mutation.
    run_contract rust-restart /usr/local/libexec/rhiza-rust-managed-contract
  fi
  RHIZA_FFI_IN_CLUSTER_CONTRACT=1 run_contract "go-$([ "$rust_initial" = 1 ] && printf initial || printf restart)" /usr/local/libexec/rhiza-managed-ffi-contract \
    -test.run='^TestManagedFFIInClusterSQLLifecycle$' -test.count=1 -test.v -test.timeout=120s
  if [ "$rust_initial" = 1 ]; then
    run_contract rust-initial /usr/local/libexec/rhiza-rust-managed-contract --initial-run
  fi
  fi
fi
# The contract closes its real FFI Host before normal same-Pod adopted startup.
exec /usr/local/libexec/rhiza-ha-init "$@"
