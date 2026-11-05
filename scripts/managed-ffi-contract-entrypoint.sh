#!/bin/sh
# CI image only. Do not run beside another process owning this Pod's WAL.
set -eu
umask 077
diag_dir=/var/lib/rhiza/contract-diagnostics
valid_fixture_profile() {
  case "$1" in internal|graph) return 0 ;; *) return 1 ;; esac
}
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
    function knowncontrolreason(s) { return s ~ /^(runtime_role_or_profile|reference_validation|validated_prefix_advance|slot_not_next|isr_shape|voter_configuration|proposal_value|authority_changed|refs_uncovered|slot_changed|step_regressed|final_snapshot_mismatch)$/ }
    function knowncheckpointreason(s) { return s ~ /^(selection_unavailable|local_seal_read|current_read|refreshed_local_seal_read|current_without_certified_qckp|current_ahead|current_rank_conflict|archive_load|archive_sealed|archive_tip_behind|archive_base_ahead|archive_base_invalid|archive_decision_unavailable|core_certificate_mismatch|archive_certificate_invalid|candidate_validation|root_open|root_state_policy_mismatch|current_certificate_hash_mismatch|prefix_refresh_unavailable|prefix_archive_load|prefix_unexpected_base|prefix_page_incomplete|prefix_decision_identity|prefix_decision_policy|prefix_certificate_accept|prefix_anchor_invalid|prefix_overlap_mismatch|prefix_anchor_changed)$/ }
    function knownquerystage(s) { return s ~ /^(host_closed|host_initial_admission|node_borrow|node_local_read|node_admission|node_server|admission_runtime_pre|admission_pre_control|admission_owned_wait|admission_read_index|admission_wait_tip|admission_replay|admission_runtime_post|admission_fixed_replay|admission_unresolved_history|server_read_acquire|server_read_index|server_catchup|server_apply|server_materialize)$/ }
    function knownopenstage(s) { return s ~ /^(host_config|host_binding|host_plan|host_bootstrap|host_authority|host_prepared_identity|host_source|host_node_open|host_initialization|prepared_config|prepared_authority|prepared_identity|prepared_lock|prepared_origin|prepared_adoption|prepared_wal|prepared_materializer|prepared_restore|prepared_runtime_open|adoption_identity|adoption_wal_history|adoption_sql_reconstruct|adoption_sql_digest|adoption_sql_restore_state|adoption_sql_actual_inspect|adoption_sql_applied_below_floor|adoption_sql_applied_above_tip|adoption_sql_base_binding|adoption_sql_base_missing|adoption_sql_adopted_prefix|adoption_sql_bucket_open|adoption_sql_temp_create|adoption_sql_cache_create|adoption_sql_reader_open|adoption_sql_root_open|adoption_sql_root_state_hash|adoption_sql_checkpoint_dir_create|adoption_sql_checkpoint_download|adoption_sql_certified_suffix|adoption_sql_expected_open|adoption_sql_checkpoint_restore|adoption_sql_suffix_apply|adoption_sql_tip_mismatch|adoption_sql_expected_inspect|adoption_sql_bucket_close|adoption_sql_temp_remove|adoption_sql_expected_close|runtime_config|runtime_optional_supervisor|runtime_policy|runtime_wal_lock|runtime_wal_open|runtime_source_open|runtime_registration|runtime_checkpoint_load|runtime_archive_load|runtime_materializer|runtime_core_open|runtime_archive_recover|runtime_pair_reconstruct|runtime_peer_open|runtime_post_checkpoint|runtime_post_prepared_view|runtime_post_prepared_base|runtime_post_prepared_validate_base|runtime_post_prepared_root|runtime_post_prepared_prefix|runtime_post_local_resume|runtime_post_recorder|runtime_post_adopted_tip|runtime_post_recover|runtime_post_restore|runtime_post_material_tip|runtime_post_material_decision|runtime_post_material_validate_tip|runtime_post_replay_graph|runtime_post_replay_adopted|runtime_post_replay_local|runtime_post_recovery_evidence|runtime_post_selected_seal|runtime_post_checkpoint_promote|runtime_post_local_backup|prepared_runtime_adoption)$/ }
    function uint(s) { return s ~ /^[0-9]+$/ && length(s)<=20 }
    function uint64(s) { return uint(s) && (length(s)<20 || "x" s<="x18446744073709551615") }
    function bit(s) { return s ~ /^(true|false)$/ }
    function stripgotimestamp(s) { sub(/^[0-9][0-9][0-9][0-9]\/[0-9][0-9]\/[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9] /,"",s); return s }
    /^GRAPH_STARTUP_DIAGNOSTIC last_stage=(target_inspect|core_open|archive_recover|pair_select|pair_reconstruct|pair_install|runtime_verify|open_complete|not_observed)$/ { emit($0); next }
    {
      refusal=$0
      diagnostic=stripgotimestamp($0)
      if (diagnostic ~ /^MANAGED_SQL_QUERY_HISTORY_REFUSED/) {
        if (diagnostic ~ /^MANAGED_SQL_QUERY_HISTORY_REFUSED target=[0-9]+ recorded=[0-9]+ certified=[0-9]+ applied=[0-9]+$/) {
          split(diagnostic, history_fields, " ")
          valid_history=1
          for (history_i=2;history_i<=5;history_i++) {
            history_value=history_fields[history_i]; sub(/^[a-z_]+=/,"",history_value)
            if (!uint64(history_value)) valid_history=0
          }
          if (valid_history) emit(diagnostic)
        }
        next
      }
      if (diagnostic ~ /^MANAGED_INITIAL_AUTHORITY_FAILURE/) {
        if (diagnostic ~ /^MANAGED_INITIAL_AUTHORITY_FAILURE leaf=(identity|live|pending_closed|evidence)$/) emit(diagnostic)
        next
      }
      if (diagnostic ~ /^MANAGED_EVIDENCE_RUNTIME_FAILURE/) {
        valid_evidence=0
        if (diagnostic ~ /^MANAGED_EVIDENCE_RUNTIME_FAILURE reason=unresolved_history recorded=[0-9]+ certified=[0-9]+ applied=[0-9]+$/) {
          split(diagnostic,evidence_fields," "); valid_evidence=1; evidence_last=5
        } else if (diagnostic ~ /^MANAGED_EVIDENCE_RUNTIME_FAILURE reason=snapshot_changed before_recorded=[0-9]+ before_certified=[0-9]+ before_applied=[0-9]+ after_recorded=[0-9]+ after_certified=[0-9]+ after_applied=[0-9]+$/) {
          split(diagnostic,evidence_fields," "); valid_evidence=1; evidence_last=8
        }
        if (valid_evidence) {
          for(evidence_i=3;evidence_i<=evidence_last;evidence_i++) {
            evidence_value=evidence_fields[evidence_i]; sub(/^[a-z_]+=/,"",evidence_value)
            if (!uint64(evidence_value)) valid_evidence=0
          }
          if (valid_evidence) emit(diagnostic)
        }
        next
      }
      if (diagnostic ~ /^MANAGED_CHECKPOINT_CURRENT_READ_FAILURE/) {
        if (diagnostic ~ /^MANAGED_CHECKPOINT_CURRENT_READ_FAILURE stage=(attributes_before|get|body|close|attributes_after|version_changed|decode|policy)$/) emit(diagnostic)
        next
      }
      if (diagnostic ~ /^MANAGED_HOST_OPEN_FAILURE/) {
        if (diagnostic ~ /^MANAGED_HOST_OPEN_FAILURE stage=[a-z_]+$/) {
          open_stage=diagnostic; sub(/^MANAGED_HOST_OPEN_FAILURE stage=/,"",open_stage)
          if (knownopenstage(open_stage)) emit(diagnostic)
        }
        next
      }
      if (diagnostic ~ /^MANAGED_SQL_QUERY_FAILURE/) {
        if (diagnostic ~ /^MANAGED_SQL_QUERY_FAILURE stage=[a-z_]+$/) {
          stage=diagnostic; sub(/^MANAGED_SQL_QUERY_FAILURE stage=/,"",stage)
          if (knownquerystage(stage)) emit(diagnostic)
        }
        next
      }
      if (diagnostic ~ /^MANAGED_CHECKPOINT_SELECT_FAILURE/) {
        if (diagnostic ~ /^MANAGED_CHECKPOINT_SELECT_FAILURE reason=[a-z_]+ prepared_readonly_learner=(true|false) local_voter=(true|false) local_config=[0-9]+$/) {
          split(diagnostic, select_fields, " ")
          reason=select_fields[2]; sub(/^reason=/,"",reason)
          prepared=select_fields[3]; sub(/^prepared_readonly_learner=/,"",prepared)
          voter=select_fields[4]; sub(/^local_voter=/,"",voter)
          config=select_fields[5]; sub(/^local_config=/,"",config)
          if (!knowncheckpointreason(reason)) reason="unclassified"
          if (uint64(config)) emit("MANAGED_CHECKPOINT_SELECT_FAILURE reason=" reason " prepared_readonly_learner=" prepared " local_voter=" voter " local_config=" config)
        }
        next
      }
      if (diagnostic ~ /^MANAGED_CONTROL_OBSERVATION_REJECTED/) {
        if (diagnostic ~ /^MANAGED_CONTROL_OBSERVATION_REJECTED reason=[a-z_]+$/) {
          split(diagnostic, control_fields, " ")
          reason=control_fields[2]; sub(/^reason=/,"",reason)
          if (knowncontrolreason(reason)) emit(diagnostic)
        } else if (diagnostic ~ /^MANAGED_CONTROL_OBSERVATION_REJECTED reason=[a-z_]+ prefix_recheck=(true|false) authority_same_known=(true|false) authority_same=(true|false) refs_covered_known=(true|false) refs_covered=(true|false) old_known=true old_c=[0-9]+ old_r=[0-9]+ old_step=[0-9]+ fresh_known=(true|false) fresh_c=[0-9]+ fresh_r=[0-9]+ fresh_step_known=(true|false) fresh_step=[0-9]+$/) {
          split(diagnostic, control_fields, " ")
          reason=control_fields[2]; sub(/^reason=/,"",reason)
          old_c=control_fields[9]; sub(/^old_c=/,"",old_c)
          old_r=control_fields[10]; sub(/^old_r=/,"",old_r)
          old_step=control_fields[11]; sub(/^old_step=/,"",old_step)
          fresh_c=control_fields[13]; sub(/^fresh_c=/,"",fresh_c)
          fresh_r=control_fields[14]; sub(/^fresh_r=/,"",fresh_r)
          fresh_step=control_fields[16]; sub(/^fresh_step=/,"",fresh_step)
          if ((knowncontrolreason(reason) || reason=="fresh_reference_invalid" || reason=="qckp_local_guard") && uint(old_c) && uint(old_r) && uint(old_step) && uint(fresh_c) && uint(fresh_r) && uint(fresh_step)) emit(diagnostic)
        }
        next
      }
      if (diagnostic ~ /^MANAGED_CONTROL_WAIT_FAILURE/) {
        if (diagnostic ~ /^MANAGED_CONTROL_WAIT_FAILURE leaf=(binding|budget|context|observe|prefix_recheck_error|prefix_recheck_false|worker_propose|worker_wait_tip|worker_certificate|worker_replay|waiter_error|waiter_slot) cause=(canceled|deadline|not_ready|invalid_request|request_conflict|quorum_unavailable|other)$/)
          emit(diagnostic)
        next
      }
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
        if (diagnostic ~ /^MANAGED_HOST_REQUEST_RECOVERY_FAILURE stage=request_recovery_admission cause=[a-z_]+$/) {
          split(diagnostic, host_fields, " ")
          cause=host_fields[3]; sub(/^cause=/,"",cause)
          if (knownadmissioncause(cause)) emit(diagnostic)
        }
        next
      }
      if (diagnostic ~ /^MANAGED_GRAPH_RESOLUTION_FAILURE/) {
        if (diagnostic ~ /^MANAGED_GRAPH_RESOLUTION_FAILURE leaf=(original_certificate|durability_result|runtime_post|original_recheck) original_slot=[0-9]+ cut=[0-9]+ floor=[0-9]+ recorded=[0-9]+ certified=[0-9]+ applied=[0-9]+ certificate_present=(true|false) certificate_matches=(true|false) binding_current=(true|false) voter=(true|false) cut_covered=(true|false) cause=[a-z_]+$/) {
          split(diagnostic, resolution_fields, " ")
          resolution_valid=1
          for (resolution_i=3;resolution_i<=8;resolution_i++) {
            resolution_value=resolution_fields[resolution_i]; sub(/^[a-z_]+=/,"",resolution_value)
            if (!uint64(resolution_value)) resolution_valid=0
          }
          resolution_cause=resolution_fields[14]; sub(/^cause=/,"",resolution_cause)
          if (resolution_valid && (knownadmissioncause(resolution_cause) || resolution_cause=="durability_unavailable")) emit(diagnostic)
        }
        next
      }
      if (diagnostic ~ /^MANAGED_REQUEST_RESOLUTION_FAILURE/) {
        if (diagnostic ~ /^MANAGED_REQUEST_RESOLUTION_FAILURE stage=(resolve_borrow|resolve_admit|resolve_original_certificate|resolve_durability_result|resolve_runtime_post|resolve_original_recheck) cause=[a-z_]+$/) {
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
  for profile in internal graph; do valid_fixture_profile "$profile"; done
  for profile in "" sql mixed legacy 7 8 rhiza-managed-sql-kv/v1 rhiza-managed-request/v1 rhiza-managed-request-graph/v1; do
    if valid_fixture_profile "$profile"; then echo 'FAIL: retired fixture profile accepted' >&2; exit 1; fi
  done
  diag_test_dir=$(mktemp -d)
  trap 'rm -f "$diag_test_dir/first.log" "$diag_test_dir/active.log" "$diag_test_dir/go-initial.exit" "$diag_test_dir/rust-initial.exit" "$diag_test_dir/rust-restart.exit" "$diag_test_dir/graph-profile-restart.exit" "$diag_test_dir/graph-qualification-restart.exit"; rmdir "$diag_test_dir"' EXIT HUP INT TERM
  diag_dir=$diag_test_dir
  : > "$diag_dir/first.log"
  if run_contract go-initial sh -c 'printf "%s\n" "private-profile-DO-NOT-RETAIN" "    managed_incluster_test.go:123: private-profile-DO-NOT-RETAIN" "2026/11/03 12:00:00 MANAGED_CHECKPOINT_SELECT_FAILURE reason=current_without_certified_qckp prepared_readonly_learner=true local_voter=false local_config=1"; exit 7'; then exit 1; else [ "$?" = 7 ]; fi
  grep -Fx 'GO_CONTRACT_SOURCE managed_incluster_test.go:123:' "$diag_dir/first.log" >/dev/null
  grep -Fx 'MANAGED_CHECKPOINT_SELECT_FAILURE reason=current_without_certified_qckp prepared_readonly_learner=true local_voter=false local_config=1' "$diag_dir/first.log" >/dev/null
  if grep -F 'DO-NOT-RETAIN' "$diag_dir/first.log"; then exit 1; fi
  before=$(cksum "$diag_dir/first.log")
  if run_contract rust-restart sh -c 'echo "FAIL: managed-ha-contract prior_ack_marker_missing"; exit 1'; then exit 1; fi
  [ "$before" = "$(cksum "$diag_dir/first.log")" ]
  for reason in selection_unavailable local_seal_read current_read refreshed_local_seal_read current_without_certified_qckp current_ahead current_rank_conflict archive_load archive_sealed archive_tip_behind archive_base_ahead archive_base_invalid archive_decision_unavailable core_certificate_mismatch archive_certificate_invalid candidate_validation root_open root_state_policy_mismatch current_certificate_hash_mismatch prefix_refresh_unavailable prefix_archive_load prefix_unexpected_base prefix_page_incomplete prefix_decision_identity prefix_decision_policy prefix_certificate_accept prefix_anchor_invalid prefix_overlap_mismatch prefix_anchor_changed; do
    run_contract rust-restart sh -c 'printf "2026/11/03 12:00:00 MANAGED_CHECKPOINT_SELECT_FAILURE reason=%s prepared_readonly_learner=false local_voter=true local_config=3\n" "$1"' sh "$reason"
    grep -Fx "MANAGED_CHECKPOINT_SELECT_FAILURE reason=$reason prepared_readonly_learner=false local_voter=true local_config=3" "$diag_dir/active.log" >/dev/null
    [ "$(grep -c '^MANAGED_CHECKPOINT_SELECT_FAILURE ' "$diag_dir/active.log")" = 1 ]
    [ "$before" = "$(cksum "$diag_dir/first.log")" ]
  done
  [ "$(cat "$diag_dir/go-initial.exit")" = 7 ]
  for authority_leaf in identity live pending_closed evidence; do
    run_contract rust-restart sh -c 'printf "MANAGED_INITIAL_AUTHORITY_FAILURE leaf=%s\n" "$1"' sh "$authority_leaf"
    grep -Fx "MANAGED_INITIAL_AUTHORITY_FAILURE leaf=$authority_leaf" "$diag_dir/active.log" >/dev/null
    [ "$before" = "$(cksum "$diag_dir/first.log")" ]
  done
  run_contract rust-restart sh -c 'printf "%s\n" "MANAGED_EVIDENCE_RUNTIME_FAILURE reason=unresolved_history recorded=13 certified=12 applied=12" "MANAGED_EVIDENCE_RUNTIME_FAILURE reason=snapshot_changed before_recorded=12 before_certified=12 before_applied=12 after_recorded=18446744073709551615 after_certified=12 after_applied=12"'
  grep -Fx "MANAGED_EVIDENCE_RUNTIME_FAILURE reason=unresolved_history recorded=13 certified=12 applied=12" "$diag_dir/active.log" >/dev/null
  grep -Fx "MANAGED_EVIDENCE_RUNTIME_FAILURE reason=snapshot_changed before_recorded=12 before_certified=12 before_applied=12 after_recorded=18446744073709551615 after_certified=12 after_applied=12" "$diag_dir/active.log" >/dev/null
  run_contract rust-restart sh -c 'printf "%s\n" "MANAGED_INITIAL_AUTHORITY_FAILURE leaf=evidence payload=DO-NOT-RETAIN" "MANAGED_EVIDENCE_RUNTIME_FAILURE reason=unresolved_history recorded=18446744073709551616 certified=12 applied=12" "MANAGED_EVIDENCE_RUNTIME_FAILURE reason=private recorded=1 certified=1 applied=1"'
  if grep -E 'MANAGED_INITIAL_AUTHORITY_FAILURE|MANAGED_EVIDENCE_RUNTIME_FAILURE|DO-NOT-RETAIN' "$diag_dir/active.log"; then exit 1; fi
  for current_stage in attributes_before get body close attributes_after version_changed decode policy; do
    run_contract rust-restart sh -c 'printf "2026/11/03 12:00:00 MANAGED_CHECKPOINT_CURRENT_READ_FAILURE stage=%s\n" "$1"' sh "$current_stage"
    grep -Fx "MANAGED_CHECKPOINT_CURRENT_READ_FAILURE stage=$current_stage" "$diag_dir/active.log" >/dev/null
    [ "$before" = "$(cksum "$diag_dir/first.log")" ]
  done
  run_contract rust-restart sh -c 'printf "%s\n" "MANAGED_CHECKPOINT_CURRENT_READ_FAILURE stage=unknown" "MANAGED_CHECKPOINT_CURRENT_READ_FAILURE stage=get payload=DO-NOT-RETAIN" "MANAGED_CHECKPOINT_CURRENT_READ_FAILURE stage=https://private.invalid"'
  if grep -E 'MANAGED_CHECKPOINT_CURRENT_READ_FAILURE|DO-NOT-RETAIN|private.invalid' "$diag_dir/active.log"; then exit 1; fi
  for open_stage in host_config host_binding host_plan host_bootstrap host_authority host_prepared_identity host_source host_node_open host_initialization prepared_config prepared_authority prepared_identity prepared_lock prepared_origin prepared_adoption prepared_wal prepared_materializer prepared_restore prepared_runtime_open adoption_identity adoption_wal_history adoption_sql_reconstruct adoption_sql_digest adoption_sql_restore_state adoption_sql_actual_inspect adoption_sql_applied_below_floor adoption_sql_applied_above_tip adoption_sql_base_binding adoption_sql_base_missing adoption_sql_adopted_prefix adoption_sql_bucket_open adoption_sql_temp_create adoption_sql_cache_create adoption_sql_reader_open adoption_sql_root_open adoption_sql_root_state_hash adoption_sql_checkpoint_dir_create adoption_sql_checkpoint_download adoption_sql_certified_suffix adoption_sql_expected_open adoption_sql_checkpoint_restore adoption_sql_suffix_apply adoption_sql_tip_mismatch adoption_sql_expected_inspect adoption_sql_bucket_close adoption_sql_temp_remove adoption_sql_expected_close runtime_config runtime_optional_supervisor runtime_policy runtime_wal_lock runtime_wal_open runtime_source_open runtime_registration runtime_checkpoint_load runtime_archive_load runtime_materializer runtime_core_open runtime_archive_recover runtime_pair_reconstruct runtime_peer_open runtime_post_checkpoint runtime_post_prepared_view runtime_post_prepared_base runtime_post_prepared_validate_base runtime_post_prepared_root runtime_post_prepared_prefix runtime_post_local_resume runtime_post_recorder runtime_post_adopted_tip runtime_post_recover runtime_post_restore runtime_post_material_tip runtime_post_material_decision runtime_post_material_validate_tip runtime_post_replay_graph runtime_post_replay_adopted runtime_post_replay_local runtime_post_recovery_evidence runtime_post_selected_seal runtime_post_checkpoint_promote runtime_post_local_backup prepared_runtime_adoption; do
    run_contract rust-restart sh -c 'printf "2026/11/03 12:00:00 MANAGED_HOST_OPEN_FAILURE stage=%s\n" "$1"' sh "$open_stage"
    grep -Fx "MANAGED_HOST_OPEN_FAILURE stage=$open_stage" "$diag_dir/active.log" >/dev/null
    [ "$before" = "$(cksum "$diag_dir/first.log")" ]
  done
  run_contract rust-restart sh -c 'printf "%s\n" \
    "2026/11/03 12:00:00 MANAGED_SQL_QUERY_HISTORY_REFUSED target=0 recorded=18446744073709551615 certified=3 applied=2" \
    "MANAGED_SQL_QUERY_HISTORY_REFUSED target=-1 recorded=4 certified=3 applied=2" \
    "MANAGED_SQL_QUERY_HISTORY_REFUSED target=1 recorded=18446744073709551616 certified=3 applied=2" \
    "MANAGED_SQL_QUERY_HISTORY_REFUSED target=1 recorded=4 certified=3.5 applied=2" \
    "MANAGED_SQL_QUERY_HISTORY_REFUSED target=1 recorded=4 certified=3 applied=2 payload=DO-NOT-RETAIN"'
  grep -Fx 'MANAGED_SQL_QUERY_HISTORY_REFUSED target=0 recorded=18446744073709551615 certified=3 applied=2' "$diag_dir/active.log" >/dev/null
  [ "$(grep -c '^MANAGED_SQL_QUERY_HISTORY_REFUSED ' "$diag_dir/active.log")" = 1 ]
  [ "$before" = "$(cksum "$diag_dir/first.log")" ]
  run_contract rust-restart sh -c 'printf "%s\n" "MANAGED_HOST_OPEN_FAILURE stage=private_stage" "MANAGED_HOST_OPEN_FAILURE stage=adoption_identity payload=DO-NOT-RETAIN" "MANAGED_HOST_OPEN_FAILURE stage=https://private.invalid" "prefix MANAGED_HOST_OPEN_FAILURE stage=host_config"'
  if grep -E "MANAGED_HOST_OPEN_FAILURE|DO-NOT-RETAIN|private.invalid|private_stage" "$diag_dir/active.log"; then exit 1; fi
  [ "$before" = "$(cksum "$diag_dir/first.log")" ]
  for query_stage in host_closed host_initial_admission node_borrow node_local_read node_admission node_server admission_runtime_pre admission_pre_control admission_owned_wait admission_read_index admission_wait_tip admission_replay admission_runtime_post admission_fixed_replay admission_unresolved_history server_read_acquire server_read_index server_catchup server_apply server_materialize; do
    run_contract rust-restart sh -c 'printf "2026/11/03 12:00:00 MANAGED_SQL_QUERY_FAILURE stage=%s\n" "$1"' sh "$query_stage"
    grep -Fx "MANAGED_SQL_QUERY_FAILURE stage=$query_stage" "$diag_dir/active.log" >/dev/null
    [ "$before" = "$(cksum "$diag_dir/first.log")" ]
  done
  run_contract rust-restart sh -c 'printf "%s\n" \
    "MANAGED_SQL_QUERY_FAILURE stage=private_stage" \
    "MANAGED_SQL_QUERY_FAILURE stage=server_apply payload=DO-NOT-RETAIN" \
    "MANAGED_SQL_QUERY_FAILURE stage=https://private.invalid" \
    "prefix MANAGED_SQL_QUERY_FAILURE stage=server_apply"'
  if grep -E 'MANAGED_SQL_QUERY_FAILURE|DO-NOT-RETAIN|private.invalid|private_stage' "$diag_dir/active.log"; then exit 1; fi
  [ "$before" = "$(cksum "$diag_dir/first.log")" ]
  run_contract rust-restart sh -c 'printf "%s\n" \
    "2026/11/03 12:00:00 MANAGED_CHECKPOINT_SELECT_FAILURE reason=current_without_certified_qckp prepared_readonly_learner=false local_voter=true local_config=3" \
    "MANAGED_CHECKPOINT_SELECT_FAILURE reason=current_without_certified_qckp prepared_readonly_learner=true local_voter=false local_config=1" \
    "MANAGED_CHECKPOINT_SELECT_FAILURE reason=private_reason prepared_readonly_learner=false local_voter=false local_config=2" \
    "MANAGED_CHECKPOINT_SELECT_FAILURE reason=current_without_certified_qckp prepared_readonly_learner=false local_voter=false local_config=18446744073709551615" \
    "MANAGED_CHECKPOINT_SELECT_FAILURE reason=current_without_certified_qckp prepared_readonly_learner=unknown local_voter=false local_config=3" \
    "MANAGED_CHECKPOINT_SELECT_FAILURE reason=current_without_certified_qckp prepared_readonly_learner=false local_voter=1 local_config=3" \
    "MANAGED_CHECKPOINT_SELECT_FAILURE reason=current_without_certified_qckp prepared_readonly_learner=false local_voter=true local_config=-1" \
    "MANAGED_CHECKPOINT_SELECT_FAILURE reason=current_without_certified_qckp prepared_readonly_learner=false local_voter=true local_config=3.5" \
    "MANAGED_CHECKPOINT_SELECT_FAILURE reason=current_without_certified_qckp prepared_readonly_learner=false local_voter=true local_config=18446744073709551616" \
    "MANAGED_CHECKPOINT_SELECT_FAILURE reason=current_without_certified_qckp prepared_readonly_learner=false local_voter=true local_config=999999999999999999999" \
    "MANAGED_CHECKPOINT_SELECT_FAILURE reason=current_without_certified_qckp prepared_readonly_learner=false local_voter=true local_config=3 extra=DO-NOT-RETAIN" \
    "prefix 2026/11/03 12:00:00 MANAGED_CHECKPOINT_SELECT_FAILURE reason=current_without_certified_qckp prepared_readonly_learner=false local_voter=true local_config=3" \
    "MANAGED_CHECKPOINT_SELECT_FAILURE reason=https://private.invalid prepared_readonly_learner=false local_voter=true local_config=3"'
  for fixed in \
    'MANAGED_CHECKPOINT_SELECT_FAILURE reason=current_without_certified_qckp prepared_readonly_learner=false local_voter=true local_config=3' \
    'MANAGED_CHECKPOINT_SELECT_FAILURE reason=current_without_certified_qckp prepared_readonly_learner=true local_voter=false local_config=1' \
    'MANAGED_CHECKPOINT_SELECT_FAILURE reason=unclassified prepared_readonly_learner=false local_voter=false local_config=2' \
    'MANAGED_CHECKPOINT_SELECT_FAILURE reason=current_without_certified_qckp prepared_readonly_learner=false local_voter=false local_config=18446744073709551615'; do
    grep -Fx "$fixed" "$diag_dir/active.log" >/dev/null
  done
  [ "$(grep -c '^MANAGED_CHECKPOINT_SELECT_FAILURE ' "$diag_dir/active.log")" = 4 ]
  if grep -E 'DO-NOT-RETAIN|private_reason|private.invalid|local_config=-1|local_config=3.5|18446744073709551616|999999999999999999999|^prefix ' "$diag_dir/active.log"; then exit 1; fi
  [ "$before" = "$(cksum "$diag_dir/first.log")" ]
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
    "MANAGED_CONTROL_OBSERVATION_REJECTED reason=reference_validation" \
    "2026/11/03 12:00:00 MANAGED_CONTROL_OBSERVATION_REJECTED reason=refs_uncovered prefix_recheck=false authority_same_known=true authority_same=true refs_covered_known=true refs_covered=false old_known=true old_c=4 old_r=5 old_step=6 fresh_known=true fresh_c=4 fresh_r=5 fresh_step_known=true fresh_step=7" \
    "MANAGED_CONTROL_OBSERVATION_REJECTED reason=private_cause" \
    "MANAGED_CONTROL_OBSERVATION_REJECTED reason=reference_validation extra=DO-NOT-RETAIN" \
    "MANAGED_CONTROL_OBSERVATION_REJECTED reason=refs_uncovered prefix_recheck=false authority_same_known=yes authority_same=true refs_covered_known=true refs_covered=false old_known=true old_c=4 old_r=5 old_step=6 fresh_known=true fresh_c=4 fresh_r=5 fresh_step_known=true fresh_step=7" \
    "MANAGED_CONTROL_OBSERVATION_REJECTED reason=refs_uncovered prefix_recheck=false authority_same_known=true authority_same=true refs_covered_known=true refs_covered=false old_known=true old_c=4 old_r=5 old_step=6 fresh_known=true fresh_c=4 fresh_r=5 fresh_step=7x fresh_step_known=true fresh_step=7" \
    "MANAGED_CONTROL_OBSERVATION_REJECTED reason=refs_uncovered prefix_recheck=false authority_same_known=true authority_same=true refs_covered_known=true refs_covered=false old_known=true old_c=4 old_r=5 old_step=6 fresh_known=true fresh_c=4 fresh_r=5 fresh_step_known=true fresh_step=7 secret-DO-NOT-RETAIN"'
  grep -Fx 'MANAGED_CONTROL_OBSERVATION_REJECTED reason=reference_validation' "$diag_dir/active.log" >/dev/null
  grep -Fx 'MANAGED_CONTROL_OBSERVATION_REJECTED reason=refs_uncovered prefix_recheck=false authority_same_known=true authority_same=true refs_covered_known=true refs_covered=false old_known=true old_c=4 old_r=5 old_step=6 fresh_known=true fresh_c=4 fresh_r=5 fresh_step_known=true fresh_step=7' "$diag_dir/active.log" >/dev/null
  if grep -E 'private_cause|extra=DO-NOT-RETAIN|authority_same_known=yes|fresh_step=7x|secret-DO-NOT-RETAIN' "$diag_dir/active.log"; then exit 1; fi
  for reason in fresh_reference_invalid qckp_local_guard; do
    detailed="MANAGED_CONTROL_OBSERVATION_REJECTED reason=$reason prefix_recheck=false authority_same_known=true authority_same=true refs_covered_known=true refs_covered=false old_known=true old_c=4 old_r=5 old_step=6 fresh_known=true fresh_c=4 fresh_r=5 fresh_step_known=true fresh_step=7"
    run_contract rust-restart sh -c 'printf "%s\n" "$1" "$1 secret-DO-NOT-RETAIN" "$2"' sh "$detailed" "MANAGED_CONTROL_OBSERVATION_REJECTED reason=$reason"
    [ "$(grep -Fxc "$detailed" "$diag_dir/active.log")" = 1 ]
    if grep -Fx "MANAGED_CONTROL_OBSERVATION_REJECTED reason=$reason" "$diag_dir/active.log"; then exit 1; fi
    if grep -F 'secret-DO-NOT-RETAIN' "$diag_dir/active.log"; then exit 1; fi
  done
  run_contract rust-restart sh -c 'printf "%s\n" \
    "MANAGED_CONTROL_WAIT_FAILURE leaf=observe cause=quorum_unavailable" \
    "2026/11/03 12:00:00 MANAGED_CONTROL_WAIT_FAILURE leaf=waiter_slot cause=deadline" \
    "MANAGED_CONTROL_WAIT_FAILURE leaf=private_leaf cause=not_ready" \
    "MANAGED_CONTROL_WAIT_FAILURE leaf=observe cause=private_cause" \
    "MANAGED_CONTROL_WAIT_FAILURE leaf=observe cause=not_ready extra=DO-NOT-RETAIN" \
    "forged 2026/11/03 12:00:00 MANAGED_CONTROL_WAIT_FAILURE leaf=observe cause=not_ready" \
    "MANAGED_CONTROL_WAIT_FAILURE leaf=observe cause=not_ready secret-DO-NOT-RETAIN"'
  grep -Fx 'MANAGED_CONTROL_WAIT_FAILURE leaf=observe cause=quorum_unavailable' "$diag_dir/active.log" >/dev/null
  grep -Fx 'MANAGED_CONTROL_WAIT_FAILURE leaf=waiter_slot cause=deadline' "$diag_dir/active.log" >/dev/null
  if grep -E 'private_leaf|private_cause|extra=DO-NOT-RETAIN|forged 2026|secret-DO-NOT-RETAIN' "$diag_dir/active.log"; then exit 1; fi
  run_contract rust-restart sh -c 'printf "%s\n" \
    "MANAGED_GRAPH_ADMISSION_FAILURE route=initial stage=pre_control read_target_known=false cause=quorum_unavailable" \
    "2026/11/03 12:00:00 MANAGED_GRAPH_ADMISSION_FAILURE route=initial stage=read_index read_target_known=false cause=deadline" \
    "MANAGED_GRAPH_ADMISSION_FAILURE route=admitted stage=wait_tip read_target_known=true target=29 cause=deadline" \
    "MANAGED_GRAPH_ADMISSION_FAILURE route=admitted stage=read_index round=2 read_target_known=false cause=quorum_unavailable" \
    "MANAGED_GRAPH_ADMISSION_FAILURE route=admitted stage=replay round=2 read_target_known=true target=31 cause=not_ready" \
    "MANAGED_GRAPH_ADMISSION_REFUSED stage=history_compare route=initial read_target_known=true target=17 recorded=15 certified=14 applied=13 c_below_target=true applied_differs_certified=true recorded_above_certified=true" \
    "MANAGED_HOST_REQUEST_RECOVERY_FAILURE stage=request_recovery_admission cause=not_ready" \
    "2026/11/03 12:00:00 MANAGED_HOST_REQUEST_RECOVERY_FAILURE stage=request_recovery_admission cause=quorum_unavailable" \
    "prefix 12:00:00 MANAGED_HOST_REQUEST_RECOVERY_FAILURE stage=request_recovery_admission cause=not_ready" \
    "MANAGED_HOST_REQUEST_RECOVERY_FAILURE stage=request_recovery_admission cause=private_cause" \
    "MANAGED_HOST_REQUEST_RECOVERY_FAILURE stage=request_recovery_admission cause=not_ready secret-DO-NOT-RETAIN" \
    "MANAGED_HOST_REQUEST_RECOVERY_FAILURE stage=host_precheck cause=not_ready" \
    "MANAGED_REQUEST_RESOLUTION_FAILURE stage=resolve_borrow cause=canceled" \
    "MANAGED_REQUEST_RESOLUTION_FAILURE stage=resolve_admit cause=other" \
    "MANAGED_REQUEST_RESOLUTION_FAILURE stage=resolve_original_certificate cause=other" \
    "MANAGED_REQUEST_RESOLUTION_FAILURE stage=resolve_durability_result cause=other" \
    "MANAGED_REQUEST_RESOLUTION_FAILURE stage=resolve_runtime_post cause=other" \
    "MANAGED_REQUEST_RESOLUTION_FAILURE stage=resolve_original_recheck cause=canceled" \
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
    'MANAGED_HOST_REQUEST_RECOVERY_FAILURE stage=request_recovery_admission cause=not_ready' \
    'MANAGED_HOST_REQUEST_RECOVERY_FAILURE stage=request_recovery_admission cause=quorum_unavailable' \
    'MANAGED_REQUEST_RESOLUTION_FAILURE stage=resolve_borrow cause=canceled' \
    'MANAGED_REQUEST_RESOLUTION_FAILURE stage=resolve_admit cause=other' \
    'MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=error cause=not_ready' \
    'MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=not_found' \
    'MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=found slot_known=true slot=7' \
    'MANAGED_REQUEST_RESOLUTION_FAILURE stage=original_lookup lookup=found slot_known=false'; do
    grep -Fx "$fixed" "$diag_dir/active.log" >/dev/null
  done
  [ "$(grep -Fc 'MANAGED_HOST_REQUEST_RECOVERY_FAILURE stage=request_recovery_admission cause=not_ready' "$diag_dir/active.log")" = 1 ]
  [ "$(grep -Fc 'MANAGED_HOST_REQUEST_RECOVERY_FAILURE stage=request_recovery_admission cause=quorum_unavailable' "$diag_dir/active.log")" = 1 ]
  if grep -E 'DO-NOT-RETAIN|route=private|stage=private|private_cause|round=3|target=17x|slot_known=true slot=0|lookup=other|host_precheck|^prefix 12:00:00 MANAGED_HOST_REQUEST_RECOVERY_FAILURE' "$diag_dir/active.log"; then exit 1; fi
  [ "$before" = "$(cksum "$diag_dir/first.log")" ]
  resolution_tuple='original_slot=77 cut=77 floor=1 recorded=81 certified=80 applied=77 certificate_present=true certificate_matches=true binding_current=true voter=true cut_covered=true cause=durability_unavailable'
  for resolution_leaf in original_certificate durability_result runtime_post original_recheck; do
    run_contract rust-restart sh -c 'printf "MANAGED_GRAPH_RESOLUTION_FAILURE leaf=%s %s\n" "$1" "$2"' sh "$resolution_leaf" "$resolution_tuple"
    grep -Fx "MANAGED_GRAPH_RESOLUTION_FAILURE leaf=$resolution_leaf $resolution_tuple" "$diag_dir/active.log" >/dev/null
  done
  run_contract rust-restart sh -c 'printf "%s\n" \
    "MANAGED_GRAPH_RESOLUTION_FAILURE leaf=private original_slot=77 cut=77 floor=1 recorded=81 certified=80 applied=77 certificate_present=true certificate_matches=true binding_current=true voter=true cut_covered=true cause=other" \
    "MANAGED_GRAPH_RESOLUTION_FAILURE leaf=runtime_post original_slot=18446744073709551616 cut=77 floor=1 recorded=81 certified=80 applied=77 certificate_present=true certificate_matches=true binding_current=true voter=true cut_covered=true cause=other" \
    "MANAGED_GRAPH_RESOLUTION_FAILURE leaf=runtime_post original_slot=77 cut=77 floor=1 recorded=81 certified=80 applied=77 certificate_present=invalid certificate_matches=true binding_current=true voter=true cut_covered=true cause=other" \
    "MANAGED_GRAPH_RESOLUTION_FAILURE leaf=runtime_post original_slot=77 cut=77 floor=1 recorded=81 certified=80 applied=77 certificate_present=true certificate_matches=true binding_current=true voter=true cut_covered=true cause=private" \
    "MANAGED_GRAPH_RESOLUTION_FAILURE leaf=runtime_post original_slot=77 cut=77 floor=1 recorded=81 certified=80 applied=77 certificate_present=true certificate_matches=true binding_current=true voter=true cut_covered=true cause=other payload=DO-NOT-RETAIN"'
  if grep -E 'MANAGED_GRAPH_RESOLUTION_FAILURE|DO-NOT-RETAIN' "$diag_dir/active.log"; then exit 1; fi
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
  valid_fixture_profile "$fixture_profile" || { echo 'FAIL: explicit append-only fixture profile unavailable' >&2; exit 1; }
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
fi
# The contract closes its real FFI Host before normal same-Pod adopted startup.
exec /usr/local/libexec/rhiza-ha-init "$@"
