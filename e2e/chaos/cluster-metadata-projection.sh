# shellcheck shell=sh
# Sourced by the bootstrap and runtime harnesses; callers provide jq and a private output directory.
project_cluster_metadata() {
  exec jq -e '
    . as $cluster |
    select(([.. | objects | to_entries[] |
      select(
        ((.key=="clientKey" or .key=="clientCertificate" or .key=="token" or .key=="username" or .key=="password") and
         ((.value|type)!="string" or (.value|length)>0)) or
        .key=="exec" or .key=="authProvider")
    ] | length)==0) |
    {name:$cluster.name,location:$cluster.location,endpoint:$cluster.endpoint,
     masterAuth:{clusterCaCertificate:$cluster.masterAuth.clusterCaCertificate},
     nodePools:[$cluster.nodePools[]? | {name,config:{workloadMetadataConfig:{mode:.config.workloadMetadataConfig.mode}}}]}
  '
}
consume_cluster_metadata_projection() {
  projected=$1; projection_status=$2
  projector_pid=
  # shellcheck disable=SC2329 # Invoked by trap.
  cleanup_cluster_metadata_projector() {
    [ -n "${projector_pid:-}" ] || return 0
    kill -TERM "$projector_pid" 2>/dev/null || :
    cleanup_wait=0
    while kill -0 "$projector_pid" 2>/dev/null && [ "$cleanup_wait" -lt 2 ]; do
      sleep 1
      cleanup_wait=$((cleanup_wait + 1))
    done
    kill -KILL "$projector_pid" 2>/dev/null || :
    wait "$projector_pid" 2>/dev/null || :
  }
  trap cleanup_cluster_metadata_projector 0
  trap 'exit 130' HUP INT TERM
  exec 3<&0
  projection_exit=0
  project_cluster_metadata <&3 3<&- > "$projected" 2>/dev/null &
  projector_pid=$!
  wait "$projector_pid" || projection_exit=$?
  projector_pid=
  # Keep the producer's pipe open through EOF so a projection error cannot
  # turn its real exit into a misleading SIGPIPE 141.
  cat <&3 >/dev/null
  exec 3<&-
  printf '%s\n' "$projection_exit" > "$projection_status"
}
write_cluster_metadata_failure() {
  failure=$1; stage=$2; code=$3
  [ ! -e "$failure" ] && [ ! -L "$failure" ] && [ ! -e "$failure.tmp" ] && [ ! -L "$failure.tmp" ] || return 1
  jq -n --arg stage "$stage" --argjson exit "$code" \
    '{stage:$stage,exit:$exit,reason:"cluster metadata projection failed; payload and diagnostics omitted"}' > "$failure.tmp" || return 1
  chmod 600 "$failure.tmp"
  mv "$failure.tmp" "$failure"
}
capture_cluster_metadata_projection() (
  output=$1; failure=$2; shift 2
  private_dir=${output%/*}/.cluster-projection.$$
  fifo=$private_dir/full-json.pipe
  projected=$private_dir/projected.json
  projection_status=$private_dir/projection.exit
  # shellcheck disable=SC2329 # Invoked by trap.
  cleanup_cluster_metadata_capture() {
    for cleanup_pid in ${producer_pid:-} ${consumer_pid:-}; do kill -TERM "$cleanup_pid" 2>/dev/null || :; done
    cleanup_wait=0
    while [ "$cleanup_wait" -lt 3 ]; do
      cleanup_running=false
      for cleanup_pid in ${producer_pid:-} ${consumer_pid:-}; do
        if kill -0 "$cleanup_pid" 2>/dev/null; then cleanup_running=true; fi
      done
      [ "$cleanup_running" = true ] || break
      sleep 1
      cleanup_wait=$((cleanup_wait + 1))
    done
    for cleanup_pid in ${producer_pid:-} ${consumer_pid:-}; do kill -KILL "$cleanup_pid" 2>/dev/null || :; done
    for cleanup_pid in ${producer_pid:-} ${consumer_pid:-}; do wait "$cleanup_pid" 2>/dev/null || :; done
    rm -f "$fifo" "$projected" "$projection_status"
    rmdir "$private_dir" 2>/dev/null || :
  }
  trap cleanup_cluster_metadata_capture 0
  trap 'exit 130' HUP INT TERM
  [ ! -e "$output" ] && [ ! -L "$output" ] && [ ! -e "$failure" ] && [ ! -L "$failure" ] || return 1
  mkdir -m 700 "$private_dir" || return 1
  mkfifo -m 600 "$fifo" || return 1
  "$@" --format=json > "$fifo" 2>/dev/null &
  producer_pid=$!
  consume_cluster_metadata_projection "$projected" "$projection_status" < "$fifo" &
  consumer_pid=$!
  consumer_exit=0
  wait "$consumer_pid" || consumer_exit=$?
  consumer_pid=
  projection_exit=$consumer_exit
  if [ -f "$projection_status" ]; then projection_exit=$(cat "$projection_status"); fi
  producer_exit=0
  wait "$producer_pid" || producer_exit=$?
  producer_pid=
  if [ "$producer_exit" != 0 ]; then
    write_cluster_metadata_failure "$failure" gcloud "$producer_exit" || return 1
    return "$producer_exit"
  fi
  if [ "$projection_exit" != 0 ]; then
    write_cluster_metadata_failure "$failure" jq "$projection_exit" || return 1
    return "$projection_exit"
  fi
  chmod 600 "$projected"
  mv "$projected" "$output"
)
