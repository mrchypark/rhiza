# shellcheck shell=sh
# Sourced by the bootstrap and runtime harnesses; callers provide jq and a private output directory.
project_cluster_metadata() {
  jq -e '
    . as $cluster |
    select(([.. | objects | to_entries[] |
      select(
        ((.key=="clientKey" or .key=="clientCertificate" or .key=="token") and
         ((.value|type)!="string" or (.value|length)>0)) or
        .key=="exec" or .key=="authProvider")
    ] | length)==0) |
    {name:$cluster.name,location:$cluster.location,endpoint:$cluster.endpoint,
     masterAuth:{clusterCaCertificate:$cluster.masterAuth.clusterCaCertificate},
     nodePools:[$cluster.nodePools[]? | {name,config:{workloadMetadataConfig:{mode:.config.workloadMetadataConfig.mode}}}]}
  '
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
  # shellcheck disable=SC2329 # Invoked by trap.
  cleanup_cluster_metadata_capture() {
    if [ -n "${producer_pid:-}" ]; then kill "$producer_pid" 2>/dev/null || :; wait "$producer_pid" 2>/dev/null || :; fi
    rm -f "$fifo" "$projected"
    rmdir "$private_dir" 2>/dev/null || :
  }
  trap cleanup_cluster_metadata_capture 0
  trap 'exit 130' HUP INT TERM
  [ ! -e "$output" ] && [ ! -L "$output" ] && [ ! -e "$failure" ] && [ ! -L "$failure" ] || return 1
  mkdir -m 700 "$private_dir" || return 1
  mkfifo -m 600 "$fifo" || return 1
  "$@" --format=json > "$fifo" 2>/dev/null &
  producer_pid=$!
  projection_exit=0
  project_cluster_metadata < "$fifo" > "$projected" 2>/dev/null || projection_exit=$?
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
