#!/bin/sh
# shellcheck disable=SC2154
# Functional scale qualification invoked by test-ha-managed-bootstrap.sh after
# the existing three-voter bootstrap and SQL/XKV readback have passed.
functional_replica_watch_pid=

managed_functional_node_id() {
  printf 'rhiza-managed-node-v1\000%s' "$1" | sha256sum | awk '{print "rhiza-n-"$1}'
}

managed_functional_bootstrap_roster() {
  mfbr_file=$1 mfbr_watch=${2-false}
  jq -sc --argjson watch "$mfbr_watch" '
    def normalize: if has("object") then .object else . end;
    (if $watch then
       (first(.[] | normalize |
          select((.status|type)=="object" and
            (((.status|has("appliedVoterCount")) and (.status.appliedVoterCount != null)) or
             (.spec.replicas? == 3 and
              ((.status|has("replicaStep"))|not) and
              ((.status|has("replicaCheck"))|not))))) // error("bootstrap event not found"))
     else
       (.[0] | normalize)
     end)
    | .status.bootstrapPlan.members
    | map(.member.node_id)
    | sort
    | if all(.[]; type=="string" and length>0) then . else error("bootstrap roster lacks member.node_id") end
  ' "$mfbr_file"
}

managed_functional_roster_oracle() {
  mfr_cr=$1 mfr_pods=$2 mfr_dep=$3 mfr_eps=$4 mfr_target=$5 mfr_out=$6
  mfr_sequence=${7-}
  [ "$(jq -r '.spec.voterCount' "$mfr_cr")" = "$mfr_target" ] || return 1
  [ "$(jq -r '.items|length' "$mfr_dep")" = 1 ] || return 1
  [ "$(jq -r '.items[0].spec.replicas' "$mfr_dep")" = "$mfr_target" ] || return 1
  mfr_ready_uids=$(jq -r '.items[] | select(any(.status.conditions[]?; .type=="Ready" and .status=="True")) | .metadata.uid' "$mfr_pods" | sort)
  [ "$(printf '%s\n' "$mfr_ready_uids" | sed '/^$/d' | wc -l | tr -d ' ')" = "$mfr_target" ] || return 1
  mfr_ready_ids=
  for mfr_uid in $mfr_ready_uids; do mfr_ready_ids="$mfr_ready_ids\n$(managed_functional_node_id "$mfr_uid")"; done
  mfr_ready_ids=$(printf '%b\n' "$mfr_ready_ids" | sed '/^$/d' | sort)
  if [ -z "$mfr_sequence" ] && [ "$mfr_target" = 3 ]; then
    mfr_sequence=bootstrap
  fi
  [ -n "$mfr_sequence" ] || mfr_sequence=add
  if [ "$mfr_sequence" = bootstrap ]; then
    mfr_roster=$(managed_functional_bootstrap_roster "$mfr_cr") || return 1
  else
    jq -e --argjson target "$mfr_target" --arg sequence "$mfr_sequence" '.status.repair.state=="Completed" and .status.repair.plan.sequence==$sequence and .status.repair.completion.kind=="Terminal" and (.status.repair.completion.finalVoterIDs|length)==$target and ((.status.repair.completion.finalVoterIDs|unique|length)==$target)' "$mfr_cr" >/dev/null || return 1
    mfr_roster=$(jq -c '.status.repair.completion.finalVoterIDs|sort' "$mfr_cr")
  fi
  [ "$mfr_roster" = "$(printf '%s\n' "$mfr_ready_ids" | jq -R . | jq -sc 'map(select(.!=""))|sort')" ] || return 1
  mfr_endpoint_uids=$(jq -r '[.subsets[]?.addresses[]?.targetRef.uid]|sort|.[]' "$mfr_eps" | sort)
  [ "$(printf '%s\n' "$mfr_endpoint_uids" | sed '/^$/d' | wc -l | tr -d ' ')" = "$mfr_target" ] || return 1
  [ "$(printf '%s\n' "$mfr_endpoint_uids" | sort)" = "$(printf '%s\n' "$mfr_ready_uids" | sort)" ] || return 1
  jq -n --argjson target "$mfr_target" --argjson roster "$mfr_roster" --argjson ready "$(printf '%s\n' "$mfr_ready_ids" | jq -R . | jq -sc 'map(select(.!=""))|sort')" '{target:$target,certifiedRoster:$roster,readyIDs:$ready,readyMatchesCertified:($roster==$ready),endpointCount:$target}' > "$mfr_out"
}

managed_functional_observe_roster() {
  mfo_target=$1 mfo_tag=$2 mfo_sequence=${3-}
  k -n "$namespace" get rhizacluster sql -o json > "$logs/functional-$mfo_tag-cr.json"
  k -n "$namespace" get deployment -l "$selector" -o json > "$logs/functional-$mfo_tag-deployment.json"
  k -n "$namespace" get pods -l "$selector" -o json > "$logs/functional-$mfo_tag-pods.json"
  k -n "$namespace" get endpoints bootstrap-client -o json > "$logs/functional-$mfo_tag-endpoints.json"
  managed_functional_roster_oracle "$logs/functional-$mfo_tag-cr.json" "$logs/functional-$mfo_tag-pods.json" "$logs/functional-$mfo_tag-deployment.json" "$logs/functional-$mfo_tag-endpoints.json" "$mfo_target" "$logs/functional-$mfo_tag-roster.json" "$mfo_sequence"
}

managed_functional_enrollment_ready() {
  mfer_target=$1 mfer_tag=$2 mfer_sequence=${3:-add}
  jq -e --argjson target "$mfer_target" --arg sequence "$mfer_sequence" '.spec.voterCount==$target and .status.repair.state=="Completed" and .status.repair.plan.sequence==$sequence and .status.repair.completion.kind=="Terminal" and (.status.repair.completion.finalVoterIDs|length)==$target' "$logs/functional-$mfer_tag-poll.json" >/dev/null 2>&1 \
    && jq -e --argjson target "$mfer_target" '(.items|length)==1 and .items[0].spec.replicas==$target' "$logs/functional-$mfer_tag-deployment-poll.json" >/dev/null 2>&1 \
    && jq -e --argjson target "$mfer_target" '[.items[]|select(any(.status.conditions[]?; .type=="Ready" and .status=="True"))]|length==$target' "$logs/functional-$mfer_tag-pods-poll.json" >/dev/null 2>&1 \
    && jq -e --argjson target "$mfer_target" '[.subsets[]?.addresses[]?]|length==$target' "$logs/functional-$mfer_tag-endpoints-poll.json" >/dev/null 2>&1
}

managed_functional_wait_enrollment() {
  mfw_target=$1 mfw_tag=$2 mfw_sequence=${3:-add}
  mfw_deadline=$(( $(date +%s) + 300 ))
  while :; do
    k -n "$namespace" get rhizacluster sql -o json > "$logs/functional-$mfw_tag-poll.json" 2>/dev/null || :
    k -n "$namespace" get deployment -l "$selector" -o json > "$logs/functional-$mfw_tag-deployment-poll.json" 2>/dev/null || :
    k -n "$namespace" get pods -l "$selector" -o json > "$logs/functional-$mfw_tag-pods-poll.json" 2>/dev/null || :
    k -n "$namespace" get endpoints bootstrap-client -o json > "$logs/functional-$mfw_tag-endpoints-poll.json" 2>/dev/null || :
    if managed_functional_enrollment_ready "$mfw_target" "$mfw_tag" "$mfw_sequence"; then
      managed_functional_observe_roster "$mfw_target" "$mfw_tag" "$mfw_sequence"
      return 0
    fi
    [ "$(date +%s)" -lt "$mfw_deadline" ] || return 1
    sleep 2
  done
}

managed_functional_operator_stopped() {
  mfops_tag=${1-}
  if [ -n "$mfops_tag" ]; then
    mfops_deployment="$logs/functional-operator-$mfops_tag-deployment.json"
    mfops_pods="$logs/functional-operator-$mfops_tag-pods.json"
  else
    mfops_deployment="$logs/functional-operator-deployment.json"
    mfops_pods="$logs/functional-operator-pods.json"
  fi
  jq -e '.spec.replicas==0 and .status.observedGeneration==.metadata.generation and (.status.replicas//0)==0 and (.status.updatedReplicas//0)==0 and (.status.availableReplicas//0)==0' "$mfops_deployment" >/dev/null 2>&1 \
    && jq -e '(.items|length)==0' "$mfops_pods" >/dev/null 2>&1
}

managed_functional_stop_operator() {
  mfso_operator_pod=$(k -n "$namespace" get pods -l app.kubernetes.io/name=rhiza-operator -o json | jq -er 'if (.items|length)==1 then .items[0].metadata.name else error("expected one operator Pod") end')
  mfso_operator_uid=$(k -n "$namespace" get pod "$mfso_operator_pod" -o json | jq -er '.metadata.uid')
  kind_runtime_before "$mfso_operator_pod" "$mfso_operator_uid" functional-operator operator
  k -n "$namespace" scale deployment/rhiza-operator --replicas=0 > "$logs/functional-operator-scale-zero.txt"
  mfso_deadline=$(( $(date +%s) + 90 ))
  while :; do
    k -n "$namespace" get deployment rhiza-operator -o json > "$logs/functional-operator-deployment.json" 2>/dev/null || :
    k -n "$namespace" get pods -l app.kubernetes.io/name=rhiza-operator -o json > "$logs/functional-operator-pods.json" 2>/dev/null || :
    managed_functional_operator_stopped && break
    [ "$(date +%s)" -lt "$mfso_deadline" ] || { echo 'operator did not stop before managed scale test' >&2; return 1; }
    sleep 1
  done
  kind_runtime_after delete "$mfso_operator_uid" || { echo 'operator container did not stop before managed scale test' >&2; return 1; }
}

managed_functional_commit() {
  mfc_job=$1 mfc_id=$2 mfc_sql=$3 mfc_expected=$4
  mfc_body=$(jq -cn --arg id "$mfc_id" --arg sql "$mfc_sql" '{request_id:$id,sql:$sql}')
  run_client "$mfc_job" execute "$mfc_body" || return 1
  jq -e '.status=="committed" and .rows_affected==1 and .slot>0 and .retry_through_slot>=.slot' "$logs/$mfc_job.json" >/dev/null || return 1
  run_client "$mfc_job-read" query "$(jq -cn --arg sql "SELECT id,value FROM bootstrap_ci WHERE value='$mfc_expected'" '{sql:$sql,consistency:"linearizable"}')" || return 1
  jq -e --arg value "$mfc_expected" 'any(.rows[]; .[1]==$value)' "$logs/$mfc_job-read.json" >/dev/null || return 1
}

managed_functional_load_oracle() {
  mfl_write=$1 mfl_read=$2 mfl_out=$3
  for mfl_file in "$mfl_write" "$mfl_read"; do
    [ -f "$mfl_file" ] || return 1
    awk -F '\t' 'NF!=7 || $1 !~ /^[0-9]+$/ || $3 !~ /^(write|linearizable-read)$/ || $5 !~ /^[0-9][0-9][0-9]$/ || $6 !~ /^[0-9]+$/ {exit 1}' "$mfl_file" || return 1
  done
  awk -F '\t' '$5==503' "$mfl_write" > "$logs/functional-write-503.tsv"
  awk -F '\t' '$5==503' "$mfl_read" > "$logs/functional-read-503.tsv"
  mfl_up3_writes=$(awk -F '\t' '$2=="up-3" && $3=="write"{n++} END{print n+0}' "$mfl_write")
  mfl_up3_reads=$(awk -F '\t' '$2=="up-3" && $3=="linearizable-read"{n++} END{print n+0}' "$mfl_read")
  mfl_up3_503=$(awk -F '\t' '$2=="up-3" && $5==503{n++} END{print n+0}' "$mfl_write" "$mfl_read")
  mfl_down2_503=$(awk -F '\t' '$2=="down-2" && $5==503{n++} END{print n+0}' "$mfl_write" "$mfl_read")
  mfl_one_writes=$(awk -F '\t' '$2=="one-survivor" && $3=="write"{n++} END{print n+0}' "$mfl_write")
  mfl_one_reads=$(awk -F '\t' '$2=="one-survivor" && $3=="linearizable-read"{n++} END{print n+0}' "$mfl_read")
  mfl_one_linear_200=$(awk -F '\t' '$2=="one-survivor" && $3=="linearizable-read" && $5==200{n++} END{print n+0}' "$mfl_read")
  mfl_one_acked=$(awk -F '\t' '$2=="one-survivor" && $3=="write" && index($7,"\"status\":\"committed\"")>0{n++} END{print n+0}' "$mfl_write")
  mfl_write_200_noncommit=$(awk -F '\t' '$5==200 && $3=="write" && index($7,"\"status\":\"committed\"")==0{n++} END{print n+0}' "$mfl_write")
  mfl_write_timeout=$(awk -F '\t' '$5=="000" && $6==28{n++} END{print n+0}' "$mfl_write")
  mfl_read_timeout=$(awk -F '\t' '$5=="000" && $6==28{n++} END{print n+0}' "$mfl_read")
  mfl_write_other=$(awk -F '\t' '$5!="200" && $5!="503" && !($5=="000" && $6==28){n++} END{print n+0}' "$mfl_write")
  mfl_read_other=$(awk -F '\t' '$5!="200" && $5!="503" && !($5=="000" && $6==28){n++} END{print n+0}' "$mfl_read")
  [ "$mfl_up3_writes" -gt 0 ] && [ "$mfl_up3_reads" -gt 0 ] || return 1
  [ "$mfl_up3_503" = 0 ] || return 1
  [ "$mfl_one_writes" -gt 0 ] && [ "$mfl_one_reads" -gt 0 ] || return 1
  [ "$mfl_one_linear_200" = 0 ] || return 1
  [ "$mfl_one_acked" = 0 ] || return 1
  [ "$mfl_write_200_noncommit" = 0 ] || return 1
  awk -F '\t' '{n[$5]++} END{for(code in n) print code"\t"n[code]}' "$mfl_write" "$mfl_read" | sort -n > "$logs/functional-http-codes.tsv"
  jq -n \
    --argjson up3w "$mfl_up3_writes" --argjson up3r "$mfl_up3_reads" \
    --argjson onew "$mfl_one_writes" --argjson oner "$mfl_one_reads" --argjson onelinear200 "$mfl_one_linear_200" --argjson down2503 "$mfl_down2_503" \
    --argjson w503 "$(wc -l < "$logs/functional-write-503.tsv" | tr -d ' ')" \
    --argjson r503 "$(wc -l < "$logs/functional-read-503.tsv" | tr -d ' ')" \
    --argjson wtimeout "$mfl_write_timeout" --argjson rtimeout "$mfl_read_timeout" \
    --argjson wother "$mfl_write_other" --argjson rother "$mfl_read_other" --argjson wnoncommit "$mfl_write_200_noncommit" \
    '{stages:{down2:{http503:$down2503},up3:{writes:$up3w,reads:$up3r,http503:0},oneSurvivor:{writes:$onew,reads:$oner,committedWrites:0,linearizableHTTP200:$onelinear200}},failures:{http503:{writes:$w503,reads:$r503,total:($w503+$r503)},deadline:{writes:$wtimeout,reads:$rtimeout,total:($wtimeout+$rtimeout)},other:{writes:$wother,reads:$rother,total:($wother+$rother)},http200WriteWithoutCommit:$wnoncommit},all503RowsRetained:true}' > "$mfl_out"
}

managed_functional_write_load_script() {
  cat > "$1" <<'EOF'
#!/bin/sh
set -u
mode=$1
n=0
stage=initial
while [ ! -f /work/stop ]; do
  n=$((n+1))
  next_stage=$(cat /work/stage 2>/dev/null || :)
  [ -z "$next_stage" ] || stage=$next_stage
  if [ "$mode" = write ]; then
    case "$stage" in
      one-survivor) row_base=900000 ;;
      *) row_base=100000 ;;
    esac
    row=$((row_base+n))
    value="functional-$n-$$"
    body="{\"request_id\":\"functional-$stage-write-$n-$$\",\"sql\":\"INSERT INTO bootstrap_ci VALUES ($row,'$value')\"}"
    route=execute
  else
    body='{"sql":"SELECT id FROM bootstrap_ci ORDER BY id","consistency":"linearizable"}'
    route=query
  fi
  op=$mode
  [ "$mode" != linear ] || op=linearizable-read
  body_file="/work/$stage-$mode.body"
  error_file="/work/$stage-$mode.curl.stderr"
  : > "$body_file"
  code=$(printf '%s' "$body" | curl --silent --show-error --output "$body_file" --write-out '%{http_code}' --max-time 8 --noproxy '*' --config /auth/config --header 'Content-Type: application/json' --data-binary @- "http://bootstrap-client.managed-bootstrap.svc.cluster.local:8080/$route" 2>> "$error_file")
  rc=$?
  response=$(tr '\n' ' ' < "$body_file" 2>/dev/null | head -c 2048 || :)
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$(date +%s)" "$stage" "$op" "$n" "${code:-000}" "$rc" "$response"
  sleep 0.15
done
EOF
}

managed_functional_load_job() {
  jq -n --arg image "$1" '
    {
      apiVersion: "batch/v1",
      kind: "Job",
      metadata: {
        name: "functional-load",
        labels: {"rhiza-ci-owner": "harness"}
      },
      spec: {
        backoffLimit: 0,
        activeDeadlineSeconds: 720,
        template: {
          metadata: {labels: {"rhiza-ci-owner": "harness"}},
          spec: {
            restartPolicy: "Never",
            automountServiceAccountToken: false,
            securityContext: {runAsUser: 65532, runAsGroup: 65532, fsGroup: 65532, runAsNonRoot: true},
            containers: [{
              name: "client",
              image: $image,
              command: ["sh", "-c", "sh /load/load.sh write & w=$!; sh /load/load.sh linear & r=$!; wait $w; wait $r; touch /work/load-done; while [ ! -f /work/probe-done ]; do sleep 1; done"],
              securityContext: {allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: {drop: ["ALL"]}},
              volumeMounts: [
                {name: "auth", mountPath: "/auth", readOnly: true},
                {name: "load", mountPath: "/load", readOnly: true},
                {name: "work", mountPath: "/work"}
              ]
            }],
            volumes: [
              {name: "auth", secret: {secretName: "functional-client-auth", defaultMode: 288}},
              {name: "load", configMap: {name: "functional-load", defaultMode: 365}},
              {name: "work", emptyDir: {}}
            ]
          }
        }
      }
    }
  '
}

managed_functional_start_load() {
  mfs_script="$private/functional-load.sh"
  managed_functional_write_load_script "$mfs_script"
  k -n "$namespace" create secret generic functional-client-auth --from-file="config=$private/curl.conf"
  k -n "$namespace" create configmap functional-load --from-file="load.sh=$mfs_script"
  managed_functional_load_job 'curlimages/curl:8.12.1@sha256:94e9e444bcba979c2ea12e27ae39bee4cd10bc7041a472c4727a558e213744e6' > "$private/functional-load-job.json"
  k -n "$namespace" create -f "$private/functional-load-job.json" > "$logs/functional-load-create.txt"
  k -n "$namespace" wait --for=condition=Ready pod -l job-name=functional-load --timeout=120s > "$logs/functional-load-ready.txt"
  functional_client_pod=$(k -n "$namespace" get pod -l job-name=functional-load -o jsonpath='{.items[0].metadata.name}')
  test -n "$functional_client_pod"
}

managed_functional_stage() {
  printf '%s' "$1" > "$private/functional-stage"
  k -n "$namespace" exec -i "$functional_client_pod" -c client -- sh -c 'cat > /work/stage' < "$private/functional-stage"
}

managed_functional_wait_up3_samples() {
  mfu_deadline=$(( $(date +%s) + 30 ))
  while :; do
    k -n "$namespace" logs job/functional-load --all-containers=true > "$logs/functional-load-live.log" 2>/dev/null || :
    mfu_writes=$(awk -F '\t' '$2=="up-3" && $3=="write"{n++} END{print n+0}' "$logs/functional-load-live.log")
    mfu_reads=$(awk -F '\t' '$2=="up-3" && $3=="linearizable-read"{n++} END{print n+0}' "$logs/functional-load-live.log")
    [ "$mfu_writes" -gt 0 ] && [ "$mfu_reads" -gt 0 ] && return 0
    [ "$(date +%s)" -lt "$mfu_deadline" ] || { echo 'no up-3 concurrent load samples' >&2; return 1; }
    sleep 1
  done
}

managed_functional_wait_single_survivor() {
  mfwss_uid=
  mfwss_ip=
  mfwss_samples=0
  mfwss_deadline=$(( $(date +%s) + 120 ))
  while :; do
    k -n "$namespace" get pods -l "$selector" -o json > "$logs/functional-one-pods.json" 2>/dev/null || :
    mfwss_ready=$(jq '[.items[]|select(any(.status.conditions[]?; .type=="Ready" and .status=="True"))]|length' "$logs/functional-one-pods.json" 2>/dev/null || printf 0)
    mfwss_total=$(jq '.items|length' "$logs/functional-one-pods.json" 2>/dev/null || printf 0)
    if [ "$mfwss_ready" = 1 ] && [ "$mfwss_total" = 1 ]; then
      mfwss_next_uid=$(jq -r '.items[0].metadata.uid // ""' "$logs/functional-one-pods.json")
      mfwss_next_ip=$(jq -r '.items[0].status.podIP // ""' "$logs/functional-one-pods.json")
      mfwss_live=$(jq -r '.items[0].metadata.deletionTimestamp==null' "$logs/functional-one-pods.json")
      if [ "$mfwss_live" = true ] && [ -n "$mfwss_next_uid" ] && [ -n "$mfwss_next_ip" ] && [ "$mfwss_next_uid" = "$mfwss_uid" ] && [ "$mfwss_next_ip" = "$mfwss_ip" ]; then
        mfwss_samples=$((mfwss_samples+1))
      else
        mfwss_uid=$mfwss_next_uid
        mfwss_ip=$mfwss_next_ip
        mfwss_samples=1
      fi
    else
      mfwss_uid=
      mfwss_ip=
      mfwss_samples=0
    fi
    if [ "$mfwss_samples" -ge 5 ]; then
      functional_survivor_ip=$mfwss_ip
      jq -n --arg uid "$mfwss_uid" --arg ip "$mfwss_ip" --argjson samples "$mfwss_samples" '{uid:$uid,ip:$ip,totalPods:1,readyPods:1,consecutiveReadyObservations:$samples}' > "$logs/functional-one-convergence.json"
      return 0
    fi
    [ "$(date +%s)" -lt "$mfwss_deadline" ] || { echo 'single-voter readiness did not converge' >&2; return 1; }
    sleep 1
  done
}

managed_functional_stop_load() {
  k -n "$namespace" exec "$functional_client_pod" -c client -- touch /work/stop >/dev/null 2>&1 || :
  mfst_deadline=$(( $(date +%s) + 30 ))
  while :; do
    if k -n "$namespace" exec "$functional_client_pod" -c client -- test -f /work/load-done >/dev/null 2>&1; then break; fi
    [ "$(date +%s)" -lt "$mfst_deadline" ] || { echo 'functional load did not stop' >&2; return 1; }
    sleep 1
  done
  k -n "$namespace" logs job/functional-load --all-containers=true > "$logs/functional-load.log" 2> "$logs/functional-load-errors.txt" || :
  awk -F '\t' '$3=="write"' "$logs/functional-load.log" > "$logs/functional-writes.tsv"
  awk -F '\t' '$3=="linearizable-read"' "$logs/functional-load.log" > "$logs/functional-reads.tsv"
  for mfsl_stage in initial down-2 up-3; do managed_functional_capture_work_raw "$mfsl_stage"; done
}

managed_functional_start_survivor_load() {
  # shellcheck disable=SC2016 # The remote shell owns $! and wait targets.
  k -n "$namespace" exec "$functional_client_pod" -c client -- sh -c '
    rm -f /work/stop /work/one-done
    sh /load/load.sh write > /work/one-write.tsv 2> /work/one-write.script.stderr & w=$!
    sh /load/load.sh linear > /work/one-read.tsv 2> /work/one-read.script.stderr & r=$!
    wait $w
    wait $r
    touch /work/one-done
  ' > "$logs/functional-survivor-client.log" 2>&1 &
  functional_survivor_pid=$!
  sleep 2
}

managed_functional_start_survivor_phase() {
  managed_functional_wait_single_survivor || return 1
  managed_functional_stage one-survivor
  managed_functional_start_survivor_load
}

managed_functional_capture_one_stream() {
  mfcos_name=$1 mfcos_remote=$2
  mfcos_candidate="$logs/functional-one-$mfcos_name.capture.tsv"
  if k -n "$namespace" exec "$functional_client_pod" -c client -- cat "$mfcos_remote" > "$mfcos_candidate" 2>/dev/null && [ -s "$mfcos_candidate" ]; then
    mv "$mfcos_candidate" "$logs/functional-one-$mfcos_name.tsv"
  fi
}

managed_functional_capture_file() {
  mfcf_dest=$1 mfcf_remote=$2
  mfcf_candidate="$mfcf_dest.capture"
  if k -n "$namespace" exec "$functional_client_pod" -c client -- cat "$mfcf_remote" > "$mfcf_candidate" 2>/dev/null; then
    mv "$mfcf_candidate" "$mfcf_dest"
  else
    rm -f "$mfcf_candidate"
  fi
}

managed_functional_capture_work_raw() {
  mfcwr_tag=$1
  for mfcwr_mode in write linear; do
    managed_functional_capture_file "$logs/functional-load-$mfcwr_tag-$mfcwr_mode.body" "/work/$mfcwr_tag-$mfcwr_mode.body"
    managed_functional_capture_file "$logs/functional-load-$mfcwr_tag-$mfcwr_mode.curl.stderr" "/work/$mfcwr_tag-$mfcwr_mode.curl.stderr"
  done
}

managed_functional_stop_survivor_load() {
  k -n "$namespace" exec "$functional_client_pod" -c client -- touch /work/stop >/dev/null 2>&1 || :
  mfssl_deadline=$(( $(date +%s) + 30 ))
  while :; do
    k -n "$namespace" exec "$functional_client_pod" -c client -- test -f /work/one-done >/dev/null 2>&1 && break
    [ "$(date +%s)" -lt "$mfssl_deadline" ] || { echo 'one-survivor load did not stop' >&2; return 1; }
    sleep 1
  done
  if [ -n "${functional_survivor_pid-}" ]; then
    wait "$functional_survivor_pid" 2>/dev/null || :
    functional_survivor_pid=
  fi
  managed_functional_capture_one_stream writes /work/one-write.tsv
  managed_functional_capture_one_stream reads /work/one-read.tsv
  managed_functional_capture_work_raw one-survivor
  managed_functional_capture_file "$logs/functional-load-one-survivor-write.script.stderr" /work/one-write.script.stderr
  managed_functional_capture_file "$logs/functional-load-one-survivor-read.script.stderr" /work/one-read.script.stderr
  k -n "$namespace" exec "$functional_client_pod" -c client -- touch /work/probe-done >/dev/null 2>&1 || :
  k -n "$namespace" wait --for=condition=complete job/functional-load --timeout=90s > "$logs/functional-load-complete.txt" 2>&1 || :
  cat "$logs/functional-writes.tsv" "$logs/functional-one-writes.tsv" > "$logs/functional-all-writes.tsv"
  cat "$logs/functional-reads.tsv" "$logs/functional-one-reads.tsv" > "$logs/functional-all-reads.tsv"
}

managed_functional_capture_voter_logs() {
  mfcvl_phase=$1
  mfcvl_result=0
  mfcvl_pods="$logs/functional-voter-$mfcvl_phase-pods.json"
  if ! k -n "$namespace" get pods -l "$selector" -o json > "$mfcvl_pods" 2> "$logs/functional-voter-$mfcvl_phase-pods-errors.txt"; then
    return 1
  fi
  for mfcvl_pod in $(jq -r '.items[].metadata.name' "$mfcvl_pods"); do
    if ! k -n "$namespace" logs "$mfcvl_pod" -c sql > "$logs/functional-voter-$mfcvl_phase-$mfcvl_pod-current.log" 2> "$logs/functional-voter-$mfcvl_phase-$mfcvl_pod-current-errors.txt"; then
      mfcvl_result=1
    fi
    k -n "$namespace" logs "$mfcvl_pod" -c sql --previous > "$logs/functional-voter-$mfcvl_phase-$mfcvl_pod-previous.log" 2> "$logs/functional-voter-$mfcvl_phase-$mfcvl_pod-previous-errors.txt" || :
  done
  return "$mfcvl_result"
}

managed_functional_probe() {
  mfp_name=$1 mfp_route=$2 mfp_body=$3 mfp_timeout=$4
  printf '%s\n' "$mfp_body" > "$private/functional-probe.body"
  set +e
  k -n "$namespace" exec -i "$functional_client_pod" -c client -- curl --silent --show-error --write-out '\nRHIZA_HTTP_CODE=%{http_code}\n' --max-time "$mfp_timeout" --noproxy '*' --config /auth/config --header 'Content-Type: application/json' --data-binary @- "http://$functional_survivor_ip:8080/$mfp_route" < "$private/functional-probe.body" > "$logs/functional-$mfp_name.raw" 2> "$logs/functional-$mfp_name.stderr"
  mfp_rc=$?
  set -e
  mfp_code=$(sed -n 's/^RHIZA_HTTP_CODE=\([0-9][0-9][0-9]\)$/\1/p' "$logs/functional-$mfp_name.raw")
  [ -n "$mfp_code" ] || mfp_code=000
  sed '/^RHIZA_HTTP_CODE=[0-9][0-9][0-9]$/d' "$logs/functional-$mfp_name.raw" > "$logs/functional-$mfp_name.json"
  printf '%s\t%s\t%s\n' "$mfp_code" "$mfp_rc" "$mfp_name" > "$logs/functional-$mfp_name.meta.tsv"
}

managed_functional_service_readback() {
  mfsr_body='{"sql":"SELECT id FROM bootstrap_ci ORDER BY id","consistency":"linearizable"}'
  printf '%s\n' "$mfsr_body" > "$private/functional-readback.body"
  set +e
  k -n "$namespace" exec -i "$functional_client_pod" -c client -- curl --silent --show-error --write-out '\nRHIZA_HTTP_CODE=%{http_code}\n' --max-time 15 --noproxy '*' --config /auth/config --header 'Content-Type: application/json' --data-binary @- "http://bootstrap-client.$namespace.svc.cluster.local:8080/query" < "$private/functional-readback.body" > "$logs/functional-ack-readback.raw" 2> "$logs/functional-ack-readback.stderr"
  mfsr_rc=$?
  set -e
  printf '%s\n' "$mfsr_rc" > "$logs/functional-ack-readback.curl-rc"
  mfsr_code=$(sed -n 's/^RHIZA_HTTP_CODE=\([0-9][0-9][0-9]\)$/\1/p' "$logs/functional-ack-readback.raw")
  [ "$mfsr_code" = 200 ] || return 1
  sed '/^RHIZA_HTTP_CODE=[0-9][0-9][0-9]$/d' "$logs/functional-ack-readback.raw" > "$logs/functional-ack-readback.json"
  awk -F '\t' '$5==200 && index($7,"\"status\":\"committed\"")>0{print $4}' "$logs/functional-writes.tsv" | sort -n > "$logs/functional-acked-write-numbers.txt"
  [ -s "$logs/functional-acked-write-numbers.txt" ] || return 1
  while IFS= read -r mfsr_n; do
    jq -e --argjson id "$((100000+mfsr_n))" 'any(.rows[]; .[0]==$id)' "$logs/functional-ack-readback.json" >/dev/null || { echo "ACKed write $mfsr_n is absent from readback" >&2; return 1; }
  done < "$logs/functional-acked-write-numbers.txt"
  printf 'ack_rows=%s\ncurl_rc=%s\nstatus=PASS\n' "$(wc -l < "$logs/functional-acked-write-numbers.txt" | tr -d ' ')" "$mfsr_rc" > "$logs/functional-ack-readback.txt"
}

managed_functional_record_leave_before() {
  mflb_roster=${1:-"$logs/functional-scale5-roster.json"}
  mflb_pods=${2:-"$logs/functional-scale5-pods.json"}
  jq -e '.target==5 and .readyMatchesCertified==true and .endpointCount==5 and (.certifiedRoster|length)==5 and ((.certifiedRoster|unique|length)==5) and (.readyIDs|length)==5 and ((.readyIDs|unique|length)==5) and .certifiedRoster==.readyIDs' "$mflb_roster" >/dev/null || return 1
  jq -e '(.items|length)==5' "$mflb_pods" >/dev/null || return 1
  mflb_uids=$(jq -r '.items[] | select(.metadata.deletionTimestamp==null) | select(any(.status.conditions[]?; .type=="Ready" and .status=="True")) | .metadata.uid' "$mflb_pods" | sort)
  [ "$(printf '%s\n' "$mflb_uids" | sed '/^$/d' | wc -l | tr -d ' ')" = 5 ] || return 1
  [ "$(printf '%s\n' "$mflb_uids" | sort -u | sed '/^$/d' | wc -l | tr -d ' ')" = 5 ] || return 1
  mflb_live_ids=
  for mflb_uid in $mflb_uids; do mflb_live_ids="$mflb_live_ids\n$(managed_functional_node_id "$mflb_uid")"; done
  mflb_live_ids=$(printf '%b\n' "$mflb_live_ids" | sed '/^$/d' | sort)
  mflb_live_ids_json=$(printf '%s\n' "$mflb_live_ids" | jq -R . | jq -sc 'map(select(.!=""))|sort')
  mflb_uids_json=$(printf '%s\n' "$mflb_uids" | jq -R . | jq -sc 'map(select(.!=""))|sort')
  jq -e --argjson live "$mflb_live_ids_json" '.readyIDs==$live' "$mflb_roster" >/dev/null || return 1
  jq -n --argjson target 5 \
    --argjson certified "$(jq -c '.certifiedRoster' "$mflb_roster")" \
    --argjson live "$mflb_live_ids_json" \
    --argjson uids "$mflb_uids_json" \
    '{target:$target,certifiedIDs:$certified,liveIDs:$live,podUIDs:$uids,certifiedMatchesLive:($certified==$live)}' > "$logs/functional-leave-before.json"
}

managed_functional_leave_scale_ready() {
  mflsr_before=$1 mflsr_cr=$2 mflsr_dep=$3 mflsr_pods=$4 mflsr_eps=$5 mflsr_target=$6 mflsr_out=$7
  [ "$(jq -r '.spec.voterCount' "$mflsr_cr")" = "$mflsr_target" ] || return 1
  jq -e --argjson target "$mflsr_target" '(.items|length)==1 and .items[0].spec.replicas==$target' "$mflsr_dep" >/dev/null || return 1
  jq -e --argjson target "$mflsr_target" '(.items|length)==$target and all(.items[]; .metadata.deletionTimestamp==null and any(.status.conditions[]?; .type=="Ready" and .status=="True"))' "$mflsr_pods" >/dev/null || return 1
  jq -e '[.subsets[]?.notReadyAddresses[]?]|length==0' "$mflsr_eps" >/dev/null || return 1
  mflsr_old_uids=$(jq -r '.podUIDs[]' "$mflsr_before" | sort)
  mflsr_current_uids=$(jq -r '.items[].metadata.uid' "$mflsr_pods" | sort)
  [ "$(printf '%s\n' "$mflsr_current_uids" | sed '/^$/d' | wc -l | tr -d ' ')" = "$mflsr_target" ] || return 1
  [ "$(printf '%s\n' "$mflsr_current_uids" | sort -u | sed '/^$/d' | wc -l | tr -d ' ')" = "$mflsr_target" ] || return 1
  for mflsr_uid in $mflsr_current_uids; do
    printf '%s\n' "$mflsr_old_uids" | grep -Fx "$mflsr_uid" >/dev/null || return 1
  done
  mflsr_removed_uids=
  for mflsr_uid in $mflsr_old_uids; do
    if ! printf '%s\n' "$mflsr_current_uids" | grep -Fx "$mflsr_uid" >/dev/null; then
      mflsr_removed_uids="$mflsr_removed_uids $mflsr_uid"
    fi
  done
  [ "$(printf '%s\n' "$mflsr_removed_uids" | wc -w | tr -d ' ')" = 1 ] || return 1
  mflsr_removed_uid=$(printf '%s\n' "$mflsr_removed_uids" | awk '{print $1}')
  jq -e --arg uid "$mflsr_removed_uid" 'all(.items[]; .metadata.uid!=$uid)' "$mflsr_pods" >/dev/null || return 1
  mflsr_endpoint_uids=$(jq -r '[.subsets[]?.addresses[]?.targetRef.uid]|sort|.[]' "$mflsr_eps")
  [ "$(printf '%s\n' "$mflsr_endpoint_uids" | sed '/^$/d' | wc -l | tr -d ' ')" = "$mflsr_target" ] || return 1
  [ "$(printf '%s\n' "$mflsr_endpoint_uids" | sort)" = "$(printf '%s\n' "$mflsr_current_uids" | sort)" ] || return 1
  mflsr_removed_node=$(managed_functional_node_id "$mflsr_removed_uid")
  mflsr_ready_ids=
  for mflsr_uid in $mflsr_current_uids; do mflsr_ready_ids="$mflsr_ready_ids\n$(managed_functional_node_id "$mflsr_uid")"; done
  mflsr_ready_ids_json=$(printf '%b\n' "$mflsr_ready_ids" | sed '/^$/d' | sort | jq -R . | jq -sc 'map(select(.!=""))|sort')
  mflsr_current_uids_json=$(printf '%s\n' "$mflsr_current_uids" | jq -R . | jq -sc 'map(select(.!=""))|sort')
  jq -n --argjson target "$mflsr_target" --arg removedUID "$mflsr_removed_uid" --arg removedNodeID "$mflsr_removed_node" \
    --argjson remainingPodUIDs "$mflsr_current_uids_json" --argjson readyIDs "$mflsr_ready_ids_json" \
    '{target:$target,removedPodUID:$removedUID,removedNodeID:$removedNodeID,previousPodUIDAbsent:true,remainingPodUIDs:$remainingPodUIDs,readyIDs:$readyIDs,endpointCount:$target}' > "$mflsr_out"
}

managed_functional_wait_leave_scale_down() {
  mflw_before=$1 mflw_target=$2 mflw_tag=$3
  mflw_deadline=$(( $(date +%s) + 180 ))
  while :; do
    k -n "$namespace" get rhizacluster sql -o json > "$logs/functional-$mflw_tag-cr-poll.json" 2>/dev/null || :
    k -n "$namespace" get deployment -l "$selector" -o json > "$logs/functional-$mflw_tag-deployment-poll.json" 2>/dev/null || :
    k -n "$namespace" get pods -l "$selector" -o json > "$logs/functional-$mflw_tag-pods-poll.json" 2>/dev/null || :
    k -n "$namespace" get endpoints bootstrap-client -o json > "$logs/functional-$mflw_tag-endpoints-poll.json" 2>/dev/null || :
    if managed_functional_leave_scale_ready "$mflw_before" "$logs/functional-$mflw_tag-cr-poll.json" "$logs/functional-$mflw_tag-deployment-poll.json" "$logs/functional-$mflw_tag-pods-poll.json" "$logs/functional-$mflw_tag-endpoints-poll.json" "$mflw_target" "$logs/functional-$mflw_tag.json"; then
      cp "$logs/functional-$mflw_tag-cr-poll.json" "$logs/functional-$mflw_tag-cr.json"
      cp "$logs/functional-$mflw_tag-deployment-poll.json" "$logs/functional-$mflw_tag-deployment.json"
      cp "$logs/functional-$mflw_tag-pods-poll.json" "$logs/functional-$mflw_tag-pods.json"
      cp "$logs/functional-$mflw_tag-endpoints-poll.json" "$logs/functional-$mflw_tag-endpoints.json"
      return 0
    fi
    [ "$(date +%s)" -lt "$mflw_deadline" ] || { echo '5->4 scale-down did not leave exactly four certified live voters' >&2; return 1; }
    sleep 1
  done
}

managed_functional_leave_http_ok() {
  [ "$1" = 204 ] && [ "$2" = 0 ] && [ "$3" = 0 ]
}

managed_functional_leave_voter() {
  mflv_node=$1
  jq -cn --arg node "$mflv_node" '{node_id:$node}' > "$logs/functional-leave-voter-request.json"
  k -n "$namespace" create secret generic functional-leave-voter-request \
    --from-file="config=$private/curl.conf" \
    --from-file="body=$logs/functional-leave-voter-request.json" > "$logs/functional-leave-voter-secret.txt" || return 1
  jq -n --arg route "http://bootstrap-client.$namespace.svc.cluster.local:8080/leave-voter" '
    {
      apiVersion:"batch/v1",kind:"Job",metadata:{name:"functional-leave-voter",labels:{"rhiza-ci-owner":"harness"}},
      spec:{backoffLimit:0,activeDeadlineSeconds:90,template:{metadata:{labels:{"rhiza-ci-owner":"harness"}},spec:{restartPolicy:"Never",automountServiceAccountToken:false,securityContext:{runAsUser:65532,runAsGroup:65532,fsGroup:65532,runAsNonRoot:true},containers:[{name:"client",image:"curlimages/curl:8.12.1@sha256:94e9e444bcba979c2ea12e27ae39bee4cd10bc7041a472c4727a558e213744e6",command:["curl"],args:["--silent","--show-error","--fail-with-body","--write-out","\nRHIZA_HTTP_CODE=%{http_code}\n","--max-time","60","--noproxy","*","--config","/request/config","--header","Content-Type: application/json","--data-binary","@/request/body",$route],securityContext:{allowPrivilegeEscalation:false,readOnlyRootFilesystem:true,capabilities:{drop:["ALL"]}},volumeMounts:[{name:"request",mountPath:"/request",readOnly:true}]}],volumes:[{name:"request",secret:{secretName:"functional-leave-voter-request",defaultMode:288}}]}}}}
  ' | k -n "$namespace" create -f - -o json > "$logs/functional-leave-voter-job.json" || return 1
  set +e
  k -n "$namespace" wait --for=condition=complete job/functional-leave-voter --timeout=90s > "$logs/functional-leave-voter-wait.txt" 2>&1
  mflv_wait_rc=$?
  set -e
  k -n "$namespace" get job functional-leave-voter -o json > "$logs/functional-leave-voter-job-state.json" 2>/dev/null || :
  k -n "$namespace" get pods -l job-name=functional-leave-voter -o json > "$logs/functional-leave-voter-pods.json" 2>/dev/null || :
  k -n "$namespace" logs job/functional-leave-voter --all-containers=true > "$logs/functional-leave-voter.raw" 2> "$logs/functional-leave-voter.stderr" || :
  mflv_code=$(sed -n 's/^RHIZA_HTTP_CODE=\([0-9][0-9][0-9]\)$/\1/p' "$logs/functional-leave-voter.raw")
  [ -n "$mflv_code" ] || mflv_code=000
  mflv_pod_rc=$(jq -r '[.items[].status.containerStatuses[]?|select(.name=="client")|.state.terminated.exitCode]|if length==1 then .[0] else -1 end' "$logs/functional-leave-voter-pods.json" 2>/dev/null || printf '%s' -1)
  sed '/^RHIZA_HTTP_CODE=[0-9][0-9][0-9]$/d' "$logs/functional-leave-voter.raw" > "$logs/functional-leave-voter.json"
  printf '%s\t%s\t%s\t%s\n' "$mflv_code" "$mflv_pod_rc" "$mflv_wait_rc" leave-voter > "$logs/functional-leave-voter.meta.tsv"
  managed_functional_leave_http_ok "$mflv_code" "$mflv_pod_rc" "$mflv_wait_rc"
}

managed_functional_leave_completion_oracle() {
  mflc_cr=$1 mflc_target=$2 mflc_removed=$3 mflc_expected=$4 mflc_out=$5
  jq -e --argjson target "$mflc_target" --arg removed "$mflc_removed" --slurpfile expected "$mflc_expected" '
    ($expected|length)==1 and ($expected[0]|type)=="array" and .spec.voterCount==$target and
    .status.repair.state=="Completed" and .status.repair.plan.sequence=="leave" and
    .status.repair.plan.oldNodeID==$removed and
    .status.repair.completion.kind=="Terminal" and
    .status.repair.completion.operationID==.status.repair.plan.operationID and
    (.status.repair.plan.finalVoterIDs|sort)==($expected[0]|sort) and
    (.status.repair.completion.finalVoterIDs|sort)==($expected[0]|sort) and
    (.status.repair.completion.finalVoterIDs|length)==$target and
    ((.status.repair.completion.finalVoterIDs|unique|length)==$target) and
    (.status.repair.completion.finalVoterIDs|index($removed))==null
  ' "$mflc_cr" >/dev/null || return 1
  jq -n --argjson target "$mflc_target" --arg removed "$mflc_removed" --slurpfile expected "$mflc_expected" '{target:$target,removedNodeID:$removed,finalVoterIDs:($expected[0]|sort),sequence:"leave",state:"Completed"}' > "$mflc_out"
}

managed_functional_leave_scale_down() {
  managed_functional_observe_roster 5 leave-before add || { echo 'live five-voter roster was not certified exactly before leave' >&2; return 1; }
  managed_functional_record_leave_before "$logs/functional-leave-before-roster.json" "$logs/functional-leave-before-pods.json" || { echo 'live five-voter roster was not recorded exactly' >&2; return 1; }
  k -n "$namespace" get deployment rhiza-operator -o json > "$logs/functional-operator-leave-before-deployment.json"
  k -n "$namespace" get pods -l app.kubernetes.io/name=rhiza-operator -o json > "$logs/functional-operator-leave-before-pods.json"
  managed_functional_operator_stopped leave-before || { echo 'operator was not stopped before 5->4 leave scale-down' >&2; return 1; }
  k -n "$namespace" patch rhizacluster sql --type merge -p '{"spec":{"voterCount":4}}' > "$logs/functional-leave-target-4.txt" || return 1
  k -n "$namespace" scale "deployment/$mfsu_deployment" --replicas=4 > "$logs/functional-leave-scale-deployment-4.txt" || return 1
  managed_functional_wait_leave_scale_down "$logs/functional-leave-before.json" 4 leave-scale-down || return 1
  k -n "$namespace" get deployment rhiza-operator -o json > "$logs/functional-operator-leave-after-scale-deployment.json"
  k -n "$namespace" get pods -l app.kubernetes.io/name=rhiza-operator -o json > "$logs/functional-operator-leave-after-scale-pods.json"
  managed_functional_operator_stopped leave-after-scale || { echo 'operator was not stopped after 5->4 scale-down' >&2; return 1; }
  functional_leave_removed_node_id=$(jq -er '.removedNodeID' "$logs/functional-leave-scale-down.json")
  jq -c --arg removed "$functional_leave_removed_node_id" '.certifiedIDs|map(select(.!=$removed))|sort' "$logs/functional-leave-before.json" > "$logs/functional-leave-expected-final.json"
  jq -e --slurpfile expected "$logs/functional-leave-expected-final.json" '.readyIDs==$expected[0]' "$logs/functional-leave-scale-down.json" >/dev/null || { echo 'remaining live voters do not match certified five-voter roster minus the removed Pod' >&2; return 1; }
}

managed_functional_verify_leave() {
  managed_functional_wait_enrollment 4 leave4 leave || { echo '5->4 explicit leave did not complete a certified four-voter roster' >&2; return 1; }
  managed_functional_leave_completion_oracle "$logs/functional-leave4-cr.json" 4 "$functional_leave_removed_node_id" "$logs/functional-leave-expected-final.json" "$logs/functional-leave4-certified.json" || { echo 'leave repair is not Completed with the exact certified final roster' >&2; return 1; }
  jq -e --slurpfile expected "$logs/functional-leave-expected-final.json" '.certifiedRoster==$expected[0] and .readyIDs==$expected[0] and .endpointCount==4' "$logs/functional-leave4-roster.json" >/dev/null || { echo 'completed leave roster differs from certified/live/endpoint voters' >&2; return 1; }
  k -n "$namespace" get deployment rhiza-operator -o json > "$logs/functional-operator-leave4-after-deployment.json"
  k -n "$namespace" get pods -l app.kubernetes.io/name=rhiza-operator -o json > "$logs/functional-operator-leave4-after-pods.json"
  managed_functional_operator_stopped leave4-after || { echo 'operator was started during explicit leave' >&2; return 1; }
  managed_functional_commit functional-scale-write-after-leave leave-functional-4 "INSERT INTO bootstrap_ci VALUES (6,'functional-after-leave-4')" functional-after-leave-4 || { echo 'write/readback failed after explicit leave' >&2; return 1; }
}

managed_functional_capture() {
  if [ -n "$functional_replica_watch_pid" ]; then
    managed_functional_stop_replica_watch
  fi
  if [ -n "${functional_client_pod-}" ]; then
    k -n "$namespace" get job,pod -l rhiza-ci-owner=harness -o json > "$logs/functional-client-state.json" 2>/dev/null || :
    k -n "$namespace" exec "$functional_client_pod" -c client -- touch /work/stop >/dev/null 2>&1 || :
    if [ -n "${functional_survivor_pid-}" ]; then
      wait "$functional_survivor_pid" 2>/dev/null || :
      functional_survivor_pid=
    fi
    k -n "$namespace" logs job/functional-load --all-containers=true > "$logs/functional-load.log" 2> "$logs/functional-load-errors.txt" || :
    awk -F '\t' '$3=="write"' "$logs/functional-load.log" > "$logs/functional-writes.tsv" 2>/dev/null || :
    awk -F '\t' '$3=="linearizable-read"' "$logs/functional-load.log" > "$logs/functional-reads.tsv" 2>/dev/null || :
    managed_functional_capture_one_stream writes /work/one-write.tsv
    managed_functional_capture_one_stream reads /work/one-read.tsv
    for mfc_stage in initial down-2 up-3 one-survivor; do managed_functional_capture_work_raw "$mfc_stage"; done
    managed_functional_capture_file "$logs/functional-load-one-survivor-write.script.stderr" /work/one-write.script.stderr
    managed_functional_capture_file "$logs/functional-load-one-survivor-read.script.stderr" /work/one-read.script.stderr
    cat "$logs/functional-writes.tsv" "$logs/functional-one-writes.tsv" > "$logs/functional-all-writes.tsv" 2>/dev/null || :
    cat "$logs/functional-reads.tsv" "$logs/functional-one-reads.tsv" > "$logs/functional-all-reads.tsv" 2>/dev/null || :
    k -n "$namespace" exec "$functional_client_pod" -c client -- touch /work/probe-done >/dev/null 2>&1 || :
  fi
}
managed_functional_external_target_valid() {
  case "$1" in ''|*[!0-9]*) return 1 ;; esac
  [ "$1" -ge 3 ] && [ "$1" -le 64 ]
}

managed_functional_external_high_sequence='3 4 5 6 7 8 9 10 9 8 7 6 5 4 3'

managed_functional_external_opt_in() {
  mfeo_file=$1
  case "$functional_scenario" in external-scale|external-high) ;; *) return 0 ;; esac
  jq -e '.spec | type=="object" and ((has("replicas")|not) or .replicas==3)' "$mfeo_file" >/dev/null || return 1
  jq '.spec.replicas=3' "$mfeo_file" > "$mfeo_file.external-scale.tmp" || return 1
  mv "$mfeo_file.external-scale.tmp" "$mfeo_file"
}

managed_functional_external_roster_oracle() {
  mfero_cr=$1 mfero_pods=$2 mfero_dep=$3 mfero_eps=$4 mfero_target=$5 mfero_out=$6 mfero_sequence=$7
  managed_functional_external_target_valid "$mfero_target" || return 1
  jq -e --argjson target "$mfero_target" --arg sequence "$mfero_sequence" '
    .spec.replicas==$target and
    (if $sequence=="bootstrap" then
       (.status.appliedVoterCount // .spec.voterCount)==$target and
       (.status.repair|not) and (.status.bootstrapPlan.members|length)==$target
     else
       .status.appliedVoterCount==$target and
       .status.repair.state=="Completed" and
       .status.repair.plan.sequence==$sequence and
       .status.repair.completion.kind=="Terminal" and
       .status.repair.completion.configID==(.status.repair.plan.expectedConfigID+1) and
       .status.repair.completion.operationID==.status.repair.plan.operationID and
       .status.repair.plan.finalVoterIDs==.status.repair.completion.finalVoterIDs and
       (.status.repair.completion.finalVoterIDs|length)==$target and
       (.status.repair.completion.finalVoterIDs|unique|length)==$target
     end)
  ' "$mfero_cr" >/dev/null || return 1
  if [ "$mfero_sequence" = bootstrap ]; then
    mfero_certified=$(managed_functional_bootstrap_roster "$mfero_cr") || return 1
  else
    mfero_certified=$(jq -c '.status.repair.completion.finalVoterIDs|sort' "$mfero_cr")
  fi
  jq -e --argjson target "$mfero_target" '(.items|length)==1 and .items[0].spec.replicas==$target' "$mfero_dep" >/dev/null || return 1
  jq -e --argjson target "$mfero_target" '
    (.items|length)==$target and
    all(.items[]; .metadata.deletionTimestamp==null and any(.status.conditions[]?; .type=="Ready" and .status=="True"))
  ' "$mfero_pods" >/dev/null || return 1
  mfero_ready_uids=$(jq -r '.items[].metadata.uid' "$mfero_pods" | sort)
  [ "$(printf '%s\n' "$mfero_ready_uids" | sed '/^$/d' | wc -l | tr -d ' ')" = "$mfero_target" ] || return 1
  [ "$(printf '%s\n' "$mfero_ready_uids" | sed '/^$/d' | sort -u | wc -l | tr -d ' ')" = "$mfero_target" ] || return 1
  mfero_ready_ids=
  for mfero_uid in $mfero_ready_uids; do mfero_ready_ids="$mfero_ready_ids\n$(managed_functional_node_id "$mfero_uid")"; done
  mfero_ready_json=$(printf '%b\n' "$mfero_ready_ids" | sed '/^$/d' | sort | jq -R . | jq -sc 'map(select(.!=""))|sort')
  [ "$mfero_certified" = "$mfero_ready_json" ] || return 1
  jq -e '[.subsets[]?.notReadyAddresses[]?]|length==0' "$mfero_eps" >/dev/null || return 1
  mfero_endpoint_uids=$(jq -r '[.subsets[]?.addresses[]?.targetRef.uid]|sort|.[]' "$mfero_eps")
  [ "$(printf '%s\n' "$mfero_endpoint_uids" | sed '/^$/d' | wc -l | tr -d ' ')" = "$mfero_target" ] || return 1
  [ "$mfero_endpoint_uids" = "$mfero_ready_uids" ] || return 1
  mfero_endpoint_uids_json=$(printf '%s\n' "$mfero_endpoint_uids" | sed '/^$/d' | jq -R . | jq -sc 'sort')
  mfero_endpoint_ids=
  for mfero_uid in $mfero_endpoint_uids; do mfero_endpoint_ids="$mfero_endpoint_ids\n$(managed_functional_node_id "$mfero_uid")"; done
  mfero_endpoint_json=$(printf '%b\n' "$mfero_endpoint_ids" | sed '/^$/d' | sort | jq -R . | jq -sc 'map(select(.!=""))|sort')
  [ "$mfero_endpoint_json" = "$mfero_ready_json" ] || return 1
  mfero_deployment_replicas=$(jq -er '.items[0].spec.replicas' "$mfero_dep")
  mfero_applied=$(jq -er '.status.appliedVoterCount // .spec.voterCount' "$mfero_cr")
  mfero_operation_id=$(jq -r '.status.repair.plan.operationID // .status.repair.completion.operationID // ""' "$mfero_cr")
  mfero_plan_config=$(jq -r '.status.repair.plan.expectedConfigID // ""' "$mfero_cr")
  mfero_completion_config=$(jq -r '.status.repair.completion.configID // ""' "$mfero_cr")
  jq -n --argjson target "$mfero_target" --arg sequence "$mfero_sequence" \
    --argjson certified "$mfero_certified" --argjson ready "$mfero_ready_json" \
    --argjson endpoints "$mfero_endpoint_json" --argjson endpointPodUIDs "$mfero_endpoint_uids_json" --argjson applied "$mfero_applied" \
    --argjson replicas "$mfero_deployment_replicas" --arg operationID "$mfero_operation_id" \
    --arg planConfigID "$mfero_plan_config" --arg completionConfigID "$mfero_completion_config" \
    '{target:$target,sequence:$sequence,appliedVoterCount:$applied,deploymentReplicas:$replicas,finalVoterIDs:$certified,certifiedRoster:$certified,readyIDs:$ready,endpointIDs:$endpoints,endpointPodUIDs:$endpointPodUIDs,readyMatchesCertified:($certified==$ready),endpointsMatchReady:($endpoints==$ready),operationID:$operationID,planExpectedConfigID:$planConfigID,completionConfigID:$completionConfigID,endpointCount:$target}' > "$mfero_out"
}

managed_functional_external_observe() {
  mfeoo_target=$1 mfeoo_tag=$2 mfeoo_sequence=$3
  k -n "$namespace" get rhizacluster sql -o json > "$logs/functional-$mfeoo_tag-cr.json"
  k -n "$namespace" get deployment -l "$selector" -o json > "$logs/functional-$mfeoo_tag-deployment.json"
  k -n "$namespace" get pods -l "$selector" -o json > "$logs/functional-$mfeoo_tag-pods.json"
  k -n "$namespace" get endpoints bootstrap-client -o json > "$logs/functional-$mfeoo_tag-endpoints.json"
  managed_functional_external_roster_oracle "$logs/functional-$mfeoo_tag-cr.json" "$logs/functional-$mfeoo_tag-pods.json" \
    "$logs/functional-$mfeoo_tag-deployment.json" "$logs/functional-$mfeoo_tag-endpoints.json" \
    "$mfeoo_target" "$logs/functional-$mfeoo_tag-roster.json" "$mfeoo_sequence"
}

managed_functional_operator_running() {
  mfor_tag=$1
  mfor_expected_pod=
  mfor_expected_deployment=
  mfor_expected_containers=
  [ "$#" -lt 2 ] || mfor_expected_pod=$2
  [ "$#" -lt 3 ] || mfor_expected_deployment=$3
  [ "$#" -lt 4 ] || mfor_expected_containers=$4
  mfor_deployment="$logs/functional-operator-$mfor_tag-deployment.json"
  mfor_pods="$logs/functional-operator-$mfor_tag-pods.json"
  jq -e '.spec.replicas==1 and .status.observedGeneration==.metadata.generation and (.status.availableReplicas//0)==1' "$mfor_deployment" >/dev/null || return 1
  jq -e '(.items|length)==1 and .items[0].metadata.deletionTimestamp==null and any(.items[0].status.conditions[]?; .type=="Ready" and .status=="True")' "$mfor_pods" >/dev/null || return 1
  jq -e '(.items[0].status.containerStatuses|length)>0 and all(.items[0].status.containerStatuses[]; (.name|type)=="string" and (.name|length)>0 and (.containerID|type)=="string" and (.containerID|length)>0 and (.restartCount|type)=="number")' "$mfor_pods" >/dev/null || return 1
  mfor_pod_uid=$(jq -er '.items[0].metadata.uid' "$mfor_pods")
  mfor_deployment_uid=$(jq -er '.metadata.uid' "$mfor_deployment")
  [ -z "$mfor_expected_pod" ] || [ "$mfor_pod_uid" = "$mfor_expected_pod" ] || return 1
  [ -z "$mfor_expected_deployment" ] || [ "$mfor_deployment_uid" = "$mfor_expected_deployment" ] || return 1
  if [ -n "$mfor_expected_containers" ]; then
    mfor_containers=$(managed_functional_operator_container_identity "$mfor_pods") || return 1
    [ "$mfor_containers" = "$mfor_expected_containers" ] || return 1
  fi
}

managed_functional_operator_container_identity() {
  jq -cer '[.items[0].status.containerStatuses[] | {name:.name,containerID:.containerID,restartCount:.restartCount}] | sort_by(.name)' "$1"
}

managed_functional_start_replica_watch() {
  functional_replica_watch_log="$logs/functional-external-replicas-watch.jsonl"
  : > "$functional_replica_watch_log"
  k -n "$namespace" get rhizacluster sql --watch -o json > "$functional_replica_watch_log" 2> "$logs/functional-external-replicas-watch.stderr" &
  functional_replica_watch_pid=$!
  mfsrw_deadline=$(( $(date +%s) + 30 ))
  while ! jq -s -e 'any(.[]; (.status.appliedVoterCount // .spec.voterCount)==3)' "$functional_replica_watch_log" >/dev/null 2>&1; do
    [ "$(date +%s)" -lt "$mfsrw_deadline" ] || return 1
    sleep 1
  done
}

managed_functional_stop_replica_watch() {
  [ -n "$functional_replica_watch_pid" ] || return 0
  kill "$functional_replica_watch_pid" 2>/dev/null || :
  wait "$functional_replica_watch_pid" 2>/dev/null || :
  functional_replica_watch_pid=
}

managed_functional_replica_progress_oracle() {
  mfrp_watch=$1 mfrp_out=$2 mfrp_expected=$3
  mfrp_bootstrap=$(managed_functional_bootstrap_roster "$mfrp_watch" true) || return 1
  jq -s --arg expected "$mfrp_expected" --argjson bootstrap "$mfrp_bootstrap" '
    def event: if has("object") then .object else . end;
    ($expected | split(" ") | map(select(length>0) | tonumber)) as $want |
    (map(event |
      select((.status|type)=="object" and
        (((.status|has("appliedVoterCount")) and (.status.appliedVoterCount != null)) or
         (.spec.replicas? == 3 and
          ((.status|has("replicaStep"))|not) and
          ((.status|has("replicaCheck"))|not)))))
     | reduce .[] as $e ({sequence:[],events:[]};
         ($e.status.appliedVoterCount // $e.spec.voterCount) as $n |
         if (.sequence|length)==0 or .sequence[-1] != $n
         then .sequence += [$n] | .events += [$e]
         else . end)) as $r |
    (($r.sequence|length)==($want|length)) and
    ($r.sequence == $want) and
    ([range(0; $r.sequence|length) as $i |
      if $i == 0 then true
      else ($r.sequence[$i]) as $to |
        ($r.sequence[$i-1]) as $from |
        ($r.events[$i].status.repair) as $rep |
        ($rep.state=="Completed") and
        ($rep.completion.kind=="Terminal") and
        (($rep.plan.expectedConfigID|type)=="number") and
        (($rep.completion.configID|type)=="number") and
        $rep.completion.configID == ($rep.plan.expectedConfigID + 1) and
        ($i==1 or $rep.plan.expectedConfigID == $r.events[$i-1].status.repair.completion.configID) and
        (($rep.plan.operationID|type)=="string" and ($rep.plan.operationID|length)>0) and
        $rep.plan.operationID == $rep.completion.operationID and
        ($i==1 or $rep.plan.operationID != $r.events[$i-1].status.repair.plan.operationID) and
        $rep.plan.finalVoterIDs == $rep.completion.finalVoterIDs and
        ($rep.completion.finalVoterIDs|length)==$to and
        ($rep.completion.finalVoterIDs|unique|length)==$to and
        (($to>$from and $rep.plan.sequence=="add") or
         ($to<$from and $rep.plan.sequence=="leave"))
      end] | all)
    | if . then
        {baseline:$r.sequence[0],sequence:$r.sequence,steps:$r.sequence[1:],exact:true,maxMove:1,minimum:3,
         events:[range(0;($r.events|length)) as $i |
           ($r.events[$i].status.repair) as $rep |
           {index:$i,from:(if $i==0 then null else $r.sequence[$i-1] end),to:$r.sequence[$i],
            state:($rep.state // "bootstrap"),planSequence:($rep.plan.sequence // "bootstrap"),
            operationID:($rep.plan.operationID // ""),planExpectedConfigID:($rep.plan.expectedConfigID // null),
            completionConfigID:($rep.completion.configID // null),completionKind:($rep.completion.kind // "bootstrap"),
            finalVoterIDs:($rep.completion.finalVoterIDs // $bootstrap)}]
        }
      else error("replica progress did not match the exact certified ramp")
      end
  ' "$mfrp_watch" > "$mfrp_out"
}

managed_functional_wait_external_scale() {
  mfwes_target=$1 mfwes_tag=$2 mfwes_sequence=$3 mfwes_pod_uid=$4 mfwes_deployment_uid=$5
  mfwes_containers=${6-}
  mfwes_deadline=$(( $(date +%s) + 300 ))
  while :; do
    k -n "$namespace" get rhizacluster sql -o json > "$logs/functional-$mfwes_tag-cr-poll.json" 2>/dev/null || :
    k -n "$namespace" get deployment -l "$selector" -o json > "$logs/functional-$mfwes_tag-deployment-poll.json" 2>/dev/null || :
    k -n "$namespace" get pods -l "$selector" -o json > "$logs/functional-$mfwes_tag-pods-poll.json" 2>/dev/null || :
    k -n "$namespace" get endpoints bootstrap-client -o json > "$logs/functional-$mfwes_tag-endpoints-poll.json" 2>/dev/null || :
    k -n "$namespace" get deployment rhiza-operator -o json > "$logs/functional-operator-$mfwes_tag-deployment.json" 2>/dev/null || :
    k -n "$namespace" get pods -l app.kubernetes.io/name=rhiza-operator -o json > "$logs/functional-operator-$mfwes_tag-pods.json" 2>/dev/null || :
    if managed_functional_external_roster_oracle "$logs/functional-$mfwes_tag-cr-poll.json" "$logs/functional-$mfwes_tag-pods-poll.json" \
      "$logs/functional-$mfwes_tag-deployment-poll.json" "$logs/functional-$mfwes_tag-endpoints-poll.json" \
      "$mfwes_target" "$logs/functional-$mfwes_tag-roster.json" "$mfwes_sequence" &&
      managed_functional_operator_running "$mfwes_tag" "$mfwes_pod_uid" "$mfwes_deployment_uid" "$mfwes_containers"; then
      cp "$logs/functional-$mfwes_tag-cr-poll.json" "$logs/functional-$mfwes_tag-cr.json"
      cp "$logs/functional-$mfwes_tag-deployment-poll.json" "$logs/functional-$mfwes_tag-deployment.json"
      cp "$logs/functional-$mfwes_tag-pods-poll.json" "$logs/functional-$mfwes_tag-pods.json"
      cp "$logs/functional-$mfwes_tag-endpoints-poll.json" "$logs/functional-$mfwes_tag-endpoints.json"
      return 0
    fi
    [ "$(date +%s)" -lt "$mfwes_deadline" ] || { echo "external replicas did not converge to $mfwes_target" >&2; return 1; }
    sleep 2
  done
}

managed_functional_external_rejection_oracle() {
  mfro_before_cr=$1 mfro_after_cr=$2 mfro_before_dep=$3 mfro_after_dep=$4
  mfro_target=$5 mfro_stdout=$6 mfro_stderr=$7 mfro_marker=$8
  case "$mfro_target" in 1|2) ;; *) return 1 ;; esac
  test -f "$mfro_stdout" && test -f "$mfro_stderr" && test -f "$mfro_marker" || return 1
  grep -E 'greater than or equal to 3|minimum.*3|Invalid value.*replicas' "$mfro_stderr" "$mfro_stdout" >/dev/null || return 1
  jq -e --argjson target "$mfro_target" '
    .target==$target and .command==("kubectl scale rhizacluster/sql --replicas="+($target|tostring)) and
    .result=="rejected" and .schemaValidation==true and .exitCode!=0
  ' "$mfro_marker" >/dev/null || return 1
  mfro_cr_before=$(jq -S -c '{specReplicas:.spec.replicas,status:.status}' "$mfro_before_cr") || return 1
  mfro_cr_after=$(jq -S -c '{specReplicas:.spec.replicas,status:.status}' "$mfro_after_cr") || return 1
  mfro_dep_before=$(jq -S -c '{uid:.metadata.uid,resourceVersion:.metadata.resourceVersion,generation:.metadata.generation,spec:{replicas:.items[0].spec.replicas},status:.items[0].status}' "$mfro_before_dep") || return 1
  mfro_dep_after=$(jq -S -c '{uid:.metadata.uid,resourceVersion:.metadata.resourceVersion,generation:.metadata.generation,spec:{replicas:.items[0].spec.replicas},status:.items[0].status}' "$mfro_after_dep") || return 1
  [ "$mfro_cr_before" = "$mfro_cr_after" ] || return 1
  [ "$mfro_dep_before" = "$mfro_dep_after" ] || return 1
  jq -e '.spec.replicas==3' "$mfro_before_cr" "$mfro_after_cr" >/dev/null || return 1
  jq -e '(.items|length)==1 and .items[0].spec.replicas==3' "$mfro_before_dep" "$mfro_after_dep" >/dev/null || return 1
}

managed_functional_external_reject() {
  mfer_target=$1 mfer_tag=$2
  mfer_cr_before="$logs/functional-$mfer_tag-before-cr.json"
  mfer_dep_before="$logs/functional-$mfer_tag-before-deployment.json"
  mfer_cr_after="$logs/functional-$mfer_tag-after-cr.json"
  mfer_dep_after="$logs/functional-$mfer_tag-after-deployment.json"
  mfer_stdout="$logs/functional-$mfer_tag.stdout"
  mfer_stderr="$logs/functional-$mfer_tag.stderr"
  mfer_marker="$logs/functional-$mfer_tag.result.json"
  k -n "$namespace" get rhizacluster sql -o json > "$mfer_cr_before" || return 1
  k -n "$namespace" get deployment -l "$selector" -o json > "$mfer_dep_before" || return 1
  mfer_rc=0
  if k -n "$namespace" scale rhizacluster/sql --replicas="$mfer_target" > "$mfer_stdout" 2> "$mfer_stderr"; then
    mfer_rc=0
  else
    mfer_rc=$?
  fi
  jq -n --argjson target "$mfer_target" --argjson exitCode "$mfer_rc" \
    '{target:$target,command:("kubectl scale rhizacluster/sql --replicas="+($target|tostring)),exitCode:$exitCode,result:(if $exitCode==0 then "accepted" else "rejected" end),schemaValidation:($exitCode!=0)}' \
    > "$mfer_marker" || return 1
  k -n "$namespace" get rhizacluster sql -o json > "$mfer_cr_after" || return 1
  k -n "$namespace" get deployment -l "$selector" -o json > "$mfer_dep_after" || return 1
  managed_functional_external_rejection_oracle "$mfer_cr_before" "$mfer_cr_after" \
    "$mfer_dep_before" "$mfer_dep_after" "$mfer_target" "$mfer_stdout" "$mfer_stderr" "$mfer_marker" || return 1
  jq -n --argjson target "$mfer_target" \
    --arg stdout "$mfer_stdout" --arg stderr "$mfer_stderr" --arg result "$mfer_marker" \
    --argjson stepBefore "$(jq '(.status|has("replicaStep"))' "$mfer_cr_before")" \
    --argjson stepAfter "$(jq '(.status|has("replicaStep"))' "$mfer_cr_after")" \
    --argjson checkBefore "$(jq '(.status|has("replicaCheck"))' "$mfer_cr_before")" \
    --argjson checkAfter "$(jq '(.status|has("replicaCheck"))' "$mfer_cr_after")" \
    '{target:$target,rawStdout:$stdout,rawStderr:$stderr,resultMarker:$result,specReplicas:3,deploymentReplicas:3,observedStateUnchanged:true,replicaStepBefore:$stepBefore,replicaStepAfter:$stepAfter,replicaCheckBefore:$checkBefore,replicaCheckAfter:$checkAfter}' \
    > "$logs/functional-$mfer_tag-state.json"
}

managed_functional_external_scale() {
  managed_functional_external_observe 3 external-baseline bootstrap
  k -n "$namespace" get deployment rhiza-operator -o json > "$logs/functional-operator-external-baseline-deployment.json"
  k -n "$namespace" get pods -l app.kubernetes.io/name=rhiza-operator -o json > "$logs/functional-operator-external-baseline-pods.json"
  managed_functional_operator_running external-baseline || return 1
  mfes_operator_pod_uid=$(jq -er '.items[0].metadata.uid' "$logs/functional-operator-external-baseline-pods.json")
  mfes_operator_deployment_uid=$(jq -er '.metadata.uid' "$logs/functional-operator-external-baseline-deployment.json")
  mfes_operator_containers=$(managed_functional_operator_container_identity "$logs/functional-operator-external-baseline-pods.json") || return 1
  managed_functional_commit external-scale-write-7 external-scale-7 \
    "INSERT INTO bootstrap_ci VALUES (7,'external-scale-7')" external-scale-7 || return 1
  managed_functional_start_replica_watch || { managed_functional_stop_replica_watch; return 1; }
  if ! k -n "$namespace" scale rhizacluster/sql --replicas=5 > "$logs/functional-external-scale-5.txt"; then
    managed_functional_stop_replica_watch
    return 1
  fi
  if ! managed_functional_wait_external_scale 5 external-5 add "$mfes_operator_pod_uid" "$mfes_operator_deployment_uid" "$mfes_operator_containers"; then
    managed_functional_stop_replica_watch
    return 1
  fi
  managed_functional_commit external-scale-write-8 external-scale-8 \
    "INSERT INTO bootstrap_ci VALUES (8,'external-scale-8')" external-scale-8 || { managed_functional_stop_replica_watch; return 1; }
  if ! k -n "$namespace" scale rhizacluster/sql --replicas=3 > "$logs/functional-external-scale-3.txt"; then
    managed_functional_stop_replica_watch
    return 1
  fi
  if ! managed_functional_wait_external_scale 3 external-3 leave "$mfes_operator_pod_uid" "$mfes_operator_deployment_uid" "$mfes_operator_containers"; then
    managed_functional_stop_replica_watch
    return 1
  fi
  managed_functional_external_reject 1 external-reject-1 || { managed_functional_stop_replica_watch; return 1; }
  managed_functional_external_reject 2 external-reject-2 || { managed_functional_stop_replica_watch; return 1; }
  managed_functional_external_observe 3 external-after-reject leave || { managed_functional_stop_replica_watch; return 1; }
  run_client external-scale-readback query \
    '{"sql":"SELECT id,value FROM bootstrap_ci WHERE id IN (1,7,8) ORDER BY id","consistency":"linearizable"}' || { managed_functional_stop_replica_watch; return 1; }
  jq -e '.rows==[[1,"real-kubelet"],[7,"external-scale-7"],[8,"external-scale-8"]]' \
    "$logs/external-scale-readback.json" >/dev/null || { managed_functional_stop_replica_watch; return 1; }
  mfes_watch_deadline=$(( $(date +%s) + 30 ))
  while ! jq -s -e 'last.status.appliedVoterCount==3' "$functional_replica_watch_log" >/dev/null 2>&1; do
    [ "$(date +%s)" -lt "$mfes_watch_deadline" ] || { managed_functional_stop_replica_watch; return 1; }
    sleep 1
  done
  managed_functional_stop_replica_watch
  managed_functional_replica_progress_oracle "$logs/functional-external-replicas-watch.jsonl" \
    "$logs/functional-external-replicas-progress.json" '3 4 5 4 3' || return 1
  printf '%s\n' 'PASS: external replicas 3->4->5->4->3 certified one step at a time; literal targets 1 and 2 rejected with unchanged CR status/Deployment; operator Pod/container identity stayed pinned; committed rows 7 and 8 read back' \
    > "$logs/functional-external-scale-result.txt"
}

managed_functional_external_high_evidence_oracle() {
  mfehe_checkpoints=$1 mfehe_progress=$2 mfehe_out=$3
  jq -s --slurpfile progress "$mfehe_progress" '
    . as $checks | ($progress[0]) as $prog | ($prog.events[1:]) as $events |
    (($checks|length)==($events|length)) and
    ([range(0;($checks|length)) as $i |
      ($checks[$i].step)==($i+1) and
      ($checks[$i].from)==($events[$i].from) and
      ($checks[$i].target)==($events[$i].to) and
      ($checks[$i].operationID)==($events[$i].operationID) and
      (($checks[$i].planExpectedConfigID|tostring)==($events[$i].planExpectedConfigID|tostring)) and
      (($checks[$i].completionConfigID|tostring)==($events[$i].completionConfigID|tostring)) and
      ($checks[$i].finalVoterIDs)==($events[$i].finalVoterIDs) and
      ($checks[$i].finalVoterIDs)==($checks[$i].readyIDs) and
      ($checks[$i].finalVoterIDs)==($checks[$i].endpointIDs) and
      ($checks[$i].finalVoterIDs|length)==($checks[$i].target) and
      ($checks[$i].deploymentReplicas)==($checks[$i].target) and
      (($checks[$i].target>$checks[$i].from and $checks[$i].sequence=="add") or
       ($checks[$i].target<$checks[$i].from and $checks[$i].sequence=="leave"))] | all)
    | if . then
        {exactSequence:$prog.sequence,steps:$prog.steps,transitionEvents:$events,checkpoints:$checks,
         target7:[$checks[]|select(.target==7)],target10:[$checks[]|select(.target==10)],
         finalVoterIDs:$checks[-1].finalVoterIDs}
      else error("high-scale checkpoints did not correlate with certified transition events")
      end
    | .target7Visible=((.target7|length)==2) |
      .target10Visible=((.target10|length)==1) |
      if ((.target7Visible and .target10Visible) and
          all(.target7[]; .finalVoterIDs==.readyIDs and .finalVoterIDs==.endpointIDs and (.finalVoterIDs|length)==7) and
          all(.target10[]; .finalVoterIDs==.readyIDs and .finalVoterIDs==.endpointIDs and (.finalVoterIDs|length)==10))
      then . else error("target 7 or 10 evidence is incomplete") end
  ' "$mfehe_checkpoints" > "$mfehe_out"
}

managed_functional_external_sequence_artifacts() {
  mfesa_sequence=$1 mfesa_text=$2 mfesa_json=$3
  printf '%s\n' "$mfesa_sequence" > "$mfesa_text" || return 1
  printf '%s\n' "$mfesa_sequence" \
    | jq -R -s '[splits("\\s+") | select(length>0) | tonumber]' > "$mfesa_json"
}

managed_functional_external_high() {
  mfesh_checkpoints="$logs/functional-external-high-checkpoints.jsonl"
  mfesh_progress="$logs/functional-external-high-progress.json"
  mfesh_ramp="$logs/functional-external-high-ramp.json"
  managed_functional_external_sequence_artifacts "$managed_functional_external_high_sequence" \
    "$logs/functional-external-high-sequence.txt" "$logs/functional-external-high-sequence.json" || return 1
  : > "$mfesh_checkpoints"
  managed_functional_external_observe 3 external-high-baseline bootstrap || return 1
  k -n "$namespace" get deployment rhiza-operator -o json > "$logs/functional-operator-external-high-baseline-deployment.json" || return 1
  k -n "$namespace" get pods -l app.kubernetes.io/name=rhiza-operator -o json > "$logs/functional-operator-external-high-baseline-pods.json" || return 1
  managed_functional_operator_running external-high-baseline || return 1
  mfesh_operator_pod_uid=$(jq -er '.items[0].metadata.uid' "$logs/functional-operator-external-high-baseline-pods.json")
  mfesh_operator_deployment_uid=$(jq -er '.metadata.uid' "$logs/functional-operator-external-high-baseline-deployment.json")
  mfesh_operator_containers=$(managed_functional_operator_container_identity "$logs/functional-operator-external-high-baseline-pods.json") || return 1
  managed_functional_start_replica_watch || { managed_functional_stop_replica_watch; return 1; }
  mfesh_previous=3
  mfesh_step=0
  mfesh_committed_7=0
  mfesh_committed_10=0
  for mfesh_target in $managed_functional_external_high_sequence; do
    if [ "$mfesh_step" = 0 ]; then
      mfesh_step=1
      continue
    fi
    mfesh_tag=$(printf 'external-high-%02d-%s' "$mfesh_step" "$mfesh_target")
    if [ "$mfesh_target" -gt "$mfesh_previous" ]; then mfesh_sequence=add; else mfesh_sequence=leave; fi
    if ! k -n "$namespace" scale rhizacluster/sql --replicas="$mfesh_target" \
      > "$logs/functional-$mfesh_tag-scale.stdout" 2> "$logs/functional-$mfesh_tag-scale.stderr"; then
      managed_functional_stop_replica_watch
      return 1
    fi
    if ! managed_functional_wait_external_scale "$mfesh_target" "$mfesh_tag" "$mfesh_sequence" \
      "$mfesh_operator_pod_uid" "$mfesh_operator_deployment_uid" "$mfesh_operator_containers"; then
      managed_functional_stop_replica_watch
      return 1
    fi
    jq --argjson step "$mfesh_step" --argjson from "$mfesh_previous" --arg tag "$mfesh_tag" \
      '. + {step:$step,from:$from,tag:$tag}' "$logs/functional-$mfesh_tag-roster.json" >> "$mfesh_checkpoints" || {
        managed_functional_stop_replica_watch
        return 1
      }
    if [ "$mfesh_target" = 7 ] && [ "$mfesh_committed_7" = 0 ]; then
      managed_functional_commit external-high-write-7 external-high-7 \
        "INSERT INTO bootstrap_ci VALUES (7,'external-high-7')" external-high-7 || {
          managed_functional_stop_replica_watch
          return 1
        }
      mfesh_committed_7=1
    fi
    if [ "$mfesh_target" = 10 ]; then
      managed_functional_commit external-high-write-10 external-high-10 \
        "INSERT INTO bootstrap_ci VALUES (10,'external-high-10')" external-high-10 || {
          managed_functional_stop_replica_watch
          return 1
        }
      mfesh_committed_10=1
    fi
    mfesh_previous=$mfesh_target
    mfesh_step=$((mfesh_step + 1))
  done
  mfesh_watch_deadline=$(( $(date +%s) + 30 ))
  while ! jq -s -e 'last.status.appliedVoterCount==3' "$functional_replica_watch_log" >/dev/null 2>&1; do
    [ "$(date +%s)" -lt "$mfesh_watch_deadline" ] || { managed_functional_stop_replica_watch; return 1; }
    sleep 1
  done
  managed_functional_stop_replica_watch
  managed_functional_replica_progress_oracle "$functional_replica_watch_log" "$mfesh_progress" \
    "$managed_functional_external_high_sequence" || return 1
  managed_functional_external_high_evidence_oracle "$mfesh_checkpoints" "$mfesh_progress" "$mfesh_ramp" || return 1
  jq '{target:7,occurrences:.target7,targetVisible:.target7Visible,finalVoterIDs:[.target7[].finalVoterIDs]}' "$mfesh_ramp" \
    > "$logs/functional-external-high-target-7.json" || return 1
  jq '{target:10,occurrences:.target10,targetVisible:.target10Visible,finalVoterIDs:[.target10[].finalVoterIDs]}' "$mfesh_ramp" \
    > "$logs/functional-external-high-target-10.json" || return 1
  managed_functional_external_observe 3 external-high-final leave || return 1
  jq -n --slurpfile ramp "$mfesh_ramp" --slurpfile live "$logs/functional-external-high-final-roster.json" '
    {target:3,rampFinalVoterIDs:$ramp[0].finalVoterIDs,liveFinalVoterIDs:$live[0].finalVoterIDs,
     liveReadyIDs:$live[0].readyIDs,liveEndpointIDs:$live[0].endpointIDs,deploymentReplicas:$live[0].deploymentReplicas,
     matchesRamp:($ramp[0].finalVoterIDs==$live[0].finalVoterIDs),matchesReady:($live[0].finalVoterIDs==$live[0].readyIDs),matchesEndpoints:($live[0].finalVoterIDs==$live[0].endpointIDs)}
  ' > "$logs/functional-external-high-final-evidence.json" || return 1
  jq -e '.target==3 and .matchesRamp==true and .matchesReady==true and .matchesEndpoints==true and .deploymentReplicas==3 and (.liveFinalVoterIDs|length)==3' \
    "$logs/functional-external-high-final-evidence.json" >/dev/null || return 1
  managed_functional_external_reject 1 external-high-reject-1 || return 1
  managed_functional_external_reject 2 external-high-reject-2 || return 1
  managed_functional_external_observe 3 external-high-after-reject leave || return 1
  [ "$mfesh_committed_7" = 1 ] && [ "$mfesh_committed_10" = 1 ] || return 1
  run_client external-high-readback query \
    '{"sql":"SELECT id,value FROM bootstrap_ci WHERE id IN (1,7,10) ORDER BY id","consistency":"linearizable"}' || return 1
  jq -e '.rows==[[1,"real-kubelet"],[7,"external-high-7"],[10,"external-high-10"]]' \
    "$logs/external-high-readback.json" >/dev/null || return 1
  printf '%s\n' 'PASS: external control exact 3->4->5->6->7->8->9->10->9->8->7->6->5->4->3; every move had one certified terminal and exact CR/Deployment/ReadyPod/Endpoint roster; target 7 and target 10 are in target evidence; literal targets 1 and 2 were rejected with unchanged CR status/Deployment; operator Deployment/Pod/container identity stayed pinned' \
    > "$logs/functional-external-high-result.txt"
}

managed_functional_scale_up() {
  mfsu_deployment=$(k -n "$namespace" get deployment -l "$selector" -o jsonpath='{.items[0].metadata.name}')
  test -n "$mfsu_deployment"
  managed_functional_commit functional-scale-write-3 scale-functional-3 "INSERT INTO bootstrap_ci VALUES (3,'functional-3')" functional-3
  managed_functional_stop_operator || return 1
  k -n "$namespace" patch rhizacluster sql --type merge -p '{"spec":{"voterCount":4}}' > "$logs/functional-scale-to-4.txt"
  k -n "$namespace" scale "deployment/$mfsu_deployment" --replicas=4 > "$logs/functional-scale-deployment-4.txt"
  managed_functional_wait_enrollment 4 scale4 || { echo '3->4 enrollment did not certify a four-voter roster' >&2; return 1; }
  managed_functional_commit functional-scale-write-4 scale-functional-4 "INSERT INTO bootstrap_ci VALUES (4,'functional-4')" functional-4
  k -n "$namespace" patch rhizacluster sql --type merge -p '{"spec":{"voterCount":5}}' > "$logs/functional-scale-to-5.txt"
  k -n "$namespace" scale "deployment/$mfsu_deployment" --replicas=5 > "$logs/functional-scale-deployment-5.txt"
  managed_functional_wait_enrollment 5 scale5 || { echo '4->5 enrollment did not certify a five-voter roster' >&2; return 1; }
  managed_functional_commit functional-scale-write-5 scale-functional-5 "INSERT INTO bootstrap_ci VALUES (5,'functional-5')" functional-5
  managed_functional_leave_scale_down || return 1
  managed_functional_leave_voter "$functional_leave_removed_node_id" || { echo 'authenticated leave-voter POST did not return 204' >&2; return 1; }
  managed_functional_verify_leave || return 1
  printf '%s\n' 'PASS: 3->4->5 certified managed enrollment and explicit 5->4 leave; operator stayed zero; exact certified final roster plus write/readback' > "$logs/functional-result.txt"
}

managed_functional_quorum() {
  mfq_repair_ok=0
  mfq_result=0
  mfq_deployment=$(k -n "$namespace" get deployment -l "$selector" -o jsonpath='{.items[0].metadata.name}')
  test -n "$mfq_deployment"
  managed_functional_stop_operator || return 1
  managed_functional_start_load
  managed_functional_stage down-2
  k -n "$namespace" scale "deployment/$mfq_deployment" --replicas=2 > "$logs/functional-scale-down-2.txt"
  mfq_deadline=$(( $(date +%s) + 120 ))
  while :; do
    k -n "$namespace" get pods -l "$selector" -o json > "$logs/functional-down-2-pods.json"
    [ "$(jq -r '[.items[]|select(any(.status.conditions[]?;.type=="Ready" and .status=="True"))]|length' "$logs/functional-down-2-pods.json")" = 2 ] && break
    [ "$(date +%s)" -lt "$mfq_deadline" ] || { echo '3->2 scale did not retain two Ready voters' >&2; return 1; }
    sleep 1
  done
  managed_functional_stage up-3
  k -n "$namespace" scale "deployment/$mfq_deployment" --replicas=3 > "$logs/functional-scale-up-3.txt"
  mfq_deadline=$(( $(date +%s) + 75 ))
  while :; do
    k -n "$namespace" get rhizacluster sql -o json > "$logs/functional-replacement-poll.json" 2>/dev/null || :
    k -n "$namespace" get pods -l "$selector" -o json > "$logs/functional-replacement-pods.json" 2>/dev/null || :
    if jq -e '.status.repair.state=="Completed" and .status.repair.plan.sequence=="addThenRemove" and .status.repair.completion.kind=="Terminal" and (.status.repair.completion.finalVoterIDs|length)==3' "$logs/functional-replacement-poll.json" >/dev/null 2>&1 \
      && jq -e '[.items[]|select(any(.status.conditions[]?;.type=="Ready" and .status=="True"))]|length==3' "$logs/functional-replacement-pods.json" >/dev/null 2>&1; then
      mfq_repair_ok=1
      break
    fi
    [ "$(date +%s)" -lt "$mfq_deadline" ] || break
    sleep 1
  done
  if ! managed_functional_wait_up3_samples; then mfq_result=1; fi
  if ! managed_functional_stop_load; then mfq_result=1; fi
  if ! managed_functional_service_readback; then mfq_result=1; fi
  if ! managed_functional_capture_voter_logs up-3-before-scale-down; then mfq_result=1; fi
  # Remove a gated replacement before reducing to one, so the survivor is a
  # previously Ready data-bearing voter rather than an unstarted candidate.
  k -n "$namespace" scale "deployment/$mfq_deployment" --replicas=2 > "$logs/functional-scale-down-before-one.txt"
  mfq_deadline=$(( $(date +%s) + 120 ))
  while :; do
    k -n "$namespace" get pods -l "$selector" -o json > "$logs/functional-before-one-pods.json"
    mfq_ready=$(jq '[.items[]|select(any(.status.conditions[]?;.type=="Ready" and .status=="True"))]|length' "$logs/functional-before-one-pods.json")
    mfq_total=$(jq '.items|length' "$logs/functional-before-one-pods.json")
    [ "$mfq_ready" = 2 ] && [ "$mfq_total" = 2 ] && break
    [ "$(date +%s)" -lt "$mfq_deadline" ] || { echo 'could not isolate two Ready voters before scale-to-one' >&2; return 1; }
    sleep 1
  done
  k -n "$namespace" scale "deployment/$mfq_deployment" --replicas=1 > "$logs/functional-scale-one.txt"
  managed_functional_start_survivor_phase || { echo 'single-survivor load phase did not start' >&2; return 1; }
  managed_functional_probe local-read query '{"sql":"SELECT id,value FROM bootstrap_ci WHERE id=1","consistency":"local"}' 15
  managed_functional_probe failed-write execute '{"request_id":"functional-no-quorum-write","sql":"INSERT INTO bootstrap_ci VALUES (999999,'\''must-not-ack'\'')"}' 8
  managed_functional_probe failed-linear query '{"sql":"SELECT id FROM bootstrap_ci ORDER BY id","consistency":"linearizable"}' 8
  managed_functional_probe local-after-failed query '{"sql":"SELECT id,value FROM bootstrap_ci WHERE id=999999","consistency":"local"}' 15
  if ! managed_functional_stop_survivor_load; then mfq_result=1; fi
  managed_functional_load_oracle "$logs/functional-all-writes.tsv" "$logs/functional-all-reads.tsv" "$logs/functional-load-summary.json" || { echo 'invalid or incomplete concurrent load evidence' >&2; mfq_result=1; }
  [ "$(cut -f1 "$logs/functional-local-read.meta.tsv")" = 200 ] || { echo 'single survivor did not serve an explicit local read' >&2; mfq_result=1; }
  jq -e '.rows==[[1,"real-kubelet"]]' "$logs/functional-local-read.json" >/dev/null || mfq_result=1
  if jq -e '.status=="committed"' "$logs/functional-failed-write.json" >/dev/null 2>&1; then echo 'single survivor acknowledged a write' >&2; mfq_result=1; fi
  if jq -e 'has("rows") and .rows!=null' "$logs/functional-failed-linear.json" >/dev/null 2>&1; then echo 'single survivor served a linearizable read' >&2; mfq_result=1; fi
  jq -e '.rows==null or .rows==[]' "$logs/functional-local-after-failed.json" >/dev/null || { echo 'failed write appeared in local state' >&2; mfq_result=1; }
  if [ "$mfq_repair_ok" != 1 ]; then
    jq '.status.repair' "$logs/functional-replacement-poll.json" > "$logs/functional-replacement-stalled.json" 2>/dev/null || :
    echo 'PARTIAL: one-survivor checks captured; replacement certification missing' > "$logs/functional-partial-result.txt"
    echo 'FAIL: 3->2->3 replacement did not reach a certified three-voter roster' >&2
    mfq_result=1
  fi
  [ "$mfq_result" = 0 ] || return 1
  printf '%s\n' 'PASS: forced physical Deployment 3->2->3 replacement followed by 3->2->1; concurrent HTTP code inventory and ACK/local checks retained; one survivor served only explicit local reads while writes and linearizable reads failed closed; this is a forced disaster test, not an ordinary 1->3 auto-repair claim' > "$logs/functional-result.txt"
}

managed_functional_qualification() {
  [ "${fixture_case-}" = internal ] && [ "${fixture_provider-}" = s3 ] || {
    echo 'functional qualification requires the internal S3 fixture' >&2
    return 1
  }
  case "$1" in
    scale-up) managed_functional_scale_up ;;
    quorum) managed_functional_quorum ;;
    external-scale) managed_functional_external_scale ;;
    external-high) managed_functional_external_high ;;
    *) return 2 ;;
  esac
}

managed_functional_self_test() (
  set -eu
  test "$(managed_functional_node_id test-uid)" = rhiza-n-a48858a1777167efb162008f1b8cdf11efbcd0aea8145193ba000e7c6d1c7381
  scratch=$(mktemp -d)
  trap 'find "$scratch" -depth -delete' EXIT HUP INT TERM
  jq -n '{spec:{voterCount:1},status:{repair:{state:"Completed",plan:{sequence:"add"},completion:{kind:"Terminal",finalVoterIDs:["rhiza-n-a48858a1777167efb162008f1b8cdf11efbcd0aea8145193ba000e7c6d1c7381"]}}}}' > "$scratch/cr.json"
  jq -n '{items:[{metadata:{uid:"test-uid"},status:{conditions:[{type:"Ready",status:"True"}]}}]}' > "$scratch/pods.json"
  jq -n '{items:[{spec:{replicas:1}}]}' > "$scratch/deploy.json"
  jq -n '{subsets:[{addresses:[{targetRef:{uid:"test-uid"}}]}]}' > "$scratch/endpoints.json"
  managed_functional_roster_oracle "$scratch/cr.json" "$scratch/pods.json" "$scratch/deploy.json" "$scratch/endpoints.json" 1 "$scratch/roster.json"
  cp "$scratch/cr.json" "$scratch/functional-enroll-poll.json"
  cp "$scratch/deploy.json" "$scratch/functional-enroll-deployment-poll.json"
  cp "$scratch/pods.json" "$scratch/functional-enroll-pods-poll.json"
  cp "$scratch/endpoints.json" "$scratch/functional-enroll-endpoints-poll.json"
  logs=$scratch managed_functional_enrollment_ready 1 enroll
  jq '.items += .items' "$scratch/deploy.json" > "$scratch/functional-enroll-deployment-poll.json"
  if logs=$scratch managed_functional_enrollment_ready 1 enroll; then return 1; fi
  cp "$scratch/deploy.json" "$scratch/functional-enroll-deployment-poll.json"
  jq -n '{metadata:{generation:2},spec:{replicas:0},status:{observedGeneration:2,replicas:0,updatedReplicas:0,availableReplicas:0}}' > "$scratch/functional-operator-deployment.json"
  jq -n '{items:[]}' > "$scratch/functional-operator-pods.json"
  logs=$scratch managed_functional_operator_stopped
  jq '.status.observedGeneration=1' "$scratch/functional-operator-deployment.json" > "$scratch/operator-stale.json"
  mv "$scratch/operator-stale.json" "$scratch/functional-operator-deployment.json"
  if logs=$scratch managed_functional_operator_stopped; then return 1; fi
  jq -n '{metadata:{generation:2},spec:{replicas:0},status:{observedGeneration:2,replicas:0,updatedReplicas:0,availableReplicas:0}}' > "$scratch/functional-operator-deployment.json"
  jq -n '{items:[{}]}' > "$scratch/functional-operator-pods.json"
  if logs=$scratch managed_functional_operator_stopped; then return 1; fi
  printf '1\tup-3\twrite\t1\t200\t0\t{"status":"committed"}\n2\tone-survivor\twrite\t2\t503\t0\toperation unavailable\n' > "$scratch/writes.tsv"
  printf '1\tup-3\tlinearizable-read\t1\t200\t0\t{"rows":[[1]]}\n2\tone-survivor\tlinearizable-read\t2\t000\t28\t\n' > "$scratch/reads.tsv"
  logs=$scratch managed_functional_load_oracle "$scratch/writes.tsv" "$scratch/reads.tsv" "$scratch/summary.json"
  jq -e '.stages.up3.http503==0 and .failures.http503.total==1 and .failures.deadline.total==1' "$scratch/summary.json" >/dev/null
  cat "$scratch/reads.tsv" > "$scratch/bad-reads.tsv"
  printf '3\tone-survivor\tlinearizable-read\t3\t200\t0\t{"rows":[[1]]}\n' >> "$scratch/bad-reads.tsv"
  if logs=$scratch managed_functional_load_oracle "$scratch/writes.tsv" "$scratch/bad-reads.tsv" "$scratch/bad-summary.json" 2>/dev/null; then return 1; fi
  managed_functional_write_load_script "$scratch/load.sh"
  sh -n "$scratch/load.sh"
  managed_functional_load_job test-image > "$scratch/load-job.json"
  jq -e --arg image test-image '.apiVersion=="batch/v1" and .kind=="Job" and .metadata.name=="functional-load" and .spec.template.spec.containers[0].image==$image and (.spec.template.spec.containers[0].volumeMounts|length)==3 and (.spec.template.spec.volumes|length)==3' "$scratch/load-job.json" >/dev/null
  grep -F 'op=linearizable-read' "$scratch/load.sh" >/dev/null
  # shellcheck disable=SC2016 # Verify the generated script retains its variables.
  grep -F 'functional-$stage-write-$n-$$' "$scratch/load.sh" >/dev/null
  grep -F 'one-survivor) row_base=900000' "$scratch/load.sh" >/dev/null
  # shellcheck disable=SC2016 # Verify the generated script retains its variables.
  grep -F '"$op" "$n"' "$scratch/load.sh" >/dev/null
  mkdir "$scratch/work"
  printf 'initial' > "$scratch/work/stage"
  printf 'stale-body-from-previous-attempt' > "$scratch/work/up-3-write.body"
  sed "s#/work#$scratch/work#g" "$scratch/load.sh" > "$scratch/load-test.sh"
  cat > "$scratch/curl" <<'EOF'
#!/bin/sh
out=
while [ "$#" -gt 0 ]; do
  case "$1" in --output) out=$2; shift ;; esac
  shift
done
count=$(cat "$LOAD_WORK/count" 2>/dev/null || printf 0)
count=$((count + 1))
printf '%s' "$count" > "$LOAD_WORK/count"
case "$count" in
  1) printf 'down-2' > "$LOAD_WORK/stage" ;;
  2) printf 'up-3' > "$LOAD_WORK/stage" ;;
  3) : > "$LOAD_WORK/stop" ;;
esac
[ "$count" = 2 ] && printf '%s\n' 'curl: simulated timeout in stderr only' >&2
[ "$count" = 3 ] || [ -z "$out" ] || printf '%s' '{"slot":1,"status":"committed","rows_affected":1,"last_insert_id":100001,"retry_through_slot":1}' > "$out"
[ "$count" = 3 ] && printf '000' || printf '200'
EOF
  chmod 0755 "$scratch/curl"
  PATH="$scratch:$PATH" LOAD_WORK="$scratch/work" sh "$scratch/load-test.sh" write > "$scratch/stage-per-request.tsv"
  [ "$(awk -F '\t' '{printf "%s%s",sep,$2; sep=","}' "$scratch/stage-per-request.tsv")" = initial,down-2,up-3 ]
  awk -F '\t' 'NF!=7{exit 1}' "$scratch/stage-per-request.tsv"
  if grep -F 'curl:' "$scratch/stage-per-request.tsv" >/dev/null; then return 1; fi
  grep -Fx 'curl: simulated timeout in stderr only' "$scratch/work/down-2-write.curl.stderr" >/dev/null
  [ ! -s "$scratch/work/up-3-write.body" ]
  printf 'not-a-row\n' > "$scratch/writes.tsv"
  if managed_functional_load_oracle "$scratch/writes.tsv" "$scratch/reads.tsv" "$scratch/bad.json" 2>/dev/null; then return 1; fi
  logs=$scratch
  leave_uids='leave-u1
leave-u2
leave-u3
leave-u4
leave-u5'
  leave_uids_json=$(printf '%s\n' "$leave_uids" | jq -R . | jq -sc 'map(select(.!=""))|sort')
  leave_ids=
  for leave_uid in $leave_uids; do leave_ids="$leave_ids\n$(managed_functional_node_id "$leave_uid")"; done
  leave_ids_json=$(printf '%b\n' "$leave_ids" | sed '/^$/d' | sort | jq -R . | jq -sc 'map(select(.!=""))|sort')
  jq -n --argjson ids "$leave_ids_json" '{target:5,certifiedRoster:$ids,readyIDs:$ids,readyMatchesCertified:true,endpointCount:5}' > "$scratch/functional-scale5-roster.json"
  jq -n --argjson uids "$leave_uids_json" '{items:[$uids[]|{metadata:{uid:.},status:{conditions:[{type:"Ready",status:"True"}]}}]}' > "$scratch/functional-scale5-pods.json"
  managed_functional_record_leave_before
  jq -e --argjson uids "$leave_uids_json" --argjson ids "$leave_ids_json" '.target==5 and .podUIDs==$uids and .certifiedIDs==$ids and .liveIDs==$ids and .certifiedMatchesLive==true' "$scratch/functional-leave-before.json" >/dev/null
  jq -n '{spec:{voterCount:4}}' > "$scratch/leave-cr.json"
  jq -n '{items:[{spec:{replicas:4}}]}' > "$scratch/leave-deploy.json"
  leave_current_json=$(printf '%s\n' leave-u1 leave-u2 leave-u3 leave-u4 | jq -R . | jq -sc 'map(select(.!=""))|sort')
  jq -n --argjson uids "$leave_current_json" '{items:[$uids[]|{metadata:{uid:.},status:{conditions:[{type:"Ready",status:"True"}]}}]}' > "$scratch/leave-pods.json"
  jq -n --argjson uids "$leave_current_json" '{subsets:[{addresses:[$uids[]|{targetRef:{uid:.}}]}]}' > "$scratch/leave-endpoints.json"
  managed_functional_leave_scale_ready "$scratch/functional-leave-before.json" "$scratch/leave-cr.json" "$scratch/leave-deploy.json" "$scratch/leave-pods.json" "$scratch/leave-endpoints.json" 4 "$scratch/leave-scale-down.json"
  jq -e --arg node "$(managed_functional_node_id leave-u5)" '.target==4 and .removedPodUID=="leave-u5" and .removedNodeID==$node and .previousPodUIDAbsent==true and .endpointCount==4' "$scratch/leave-scale-down.json" >/dev/null
  jq -c --arg removed "$(managed_functional_node_id leave-u5)" '.certifiedIDs|map(select(.!=$removed))|sort' "$scratch/functional-leave-before.json" > "$scratch/leave-expected-final.json"
  jq -e --slurpfile expected "$scratch/leave-expected-final.json" '.readyIDs==$expected[0]' "$scratch/leave-scale-down.json" >/dev/null
  jq '.spec.voterCount=5' "$scratch/leave-cr.json" > "$scratch/leave-bad-cr.json"
  if managed_functional_leave_scale_ready "$scratch/functional-leave-before.json" "$scratch/leave-bad-cr.json" "$scratch/leave-deploy.json" "$scratch/leave-pods.json" "$scratch/leave-endpoints.json" 4 "$scratch/leave-bad.json"; then return 1; fi
  jq -n --argjson uids '["leave-u1","leave-u2","leave-u3","leave-u6"]' '{items:[$uids[]|{metadata:{uid:.},status:{conditions:[{type:"Ready",status:"True"}]}}]}' > "$scratch/leave-replacement.json"
  if managed_functional_leave_scale_ready "$scratch/functional-leave-before.json" "$scratch/leave-cr.json" "$scratch/leave-deploy.json" "$scratch/leave-replacement.json" "$scratch/leave-endpoints.json" 4 "$scratch/leave-bad.json"; then return 1; fi
  jq '.items[0].status.conditions[0].status="False"' "$scratch/leave-pods.json" > "$scratch/leave-not-ready.json"
  if managed_functional_leave_scale_ready "$scratch/functional-leave-before.json" "$scratch/leave-cr.json" "$scratch/leave-deploy.json" "$scratch/leave-not-ready.json" "$scratch/leave-endpoints.json" 4 "$scratch/leave-bad.json"; then return 1; fi
  jq '.subsets[0].addresses += [{targetRef:{uid:"leave-u5"}}]' "$scratch/leave-endpoints.json" > "$scratch/leave-bad-endpoints.json"
  if managed_functional_leave_scale_ready "$scratch/functional-leave-before.json" "$scratch/leave-cr.json" "$scratch/leave-deploy.json" "$scratch/leave-pods.json" "$scratch/leave-bad-endpoints.json" 4 "$scratch/leave-bad.json"; then return 1; fi
  jq -n --argjson finalRoster "$(cat "$scratch/leave-expected-final.json")" --arg removed "$(managed_functional_node_id leave-u5)" '{spec:{voterCount:4},status:{repair:{state:"Completed",plan:{sequence:"leave",operationID:"leave-op",oldNodeID:$removed,finalVoterIDs:$finalRoster},completion:{kind:"Terminal",operationID:"leave-op",finalVoterIDs:$finalRoster}}}}' > "$scratch/leave-complete.json"
  managed_functional_leave_completion_oracle "$scratch/leave-complete.json" 4 "$(managed_functional_node_id leave-u5)" "$scratch/leave-expected-final.json" "$scratch/leave-certified.json"
  jq -e --arg removed "$(managed_functional_node_id leave-u5)" --slurpfile expected "$scratch/leave-expected-final.json" '.removedNodeID==$removed' "$scratch/leave-certified.json" >/dev/null
  for leave_mutation in add closed wrong-operation removed-in-final extra-final missing-final; do
    case "$leave_mutation" in
      add) filter='.status.repair.plan.sequence="add"' ;;
      closed) filter='.status.repair.state="Closed"' ;;
      wrong-operation) filter='.status.repair.completion.operationID="other-op"' ;;
      removed-in-final) filter='.status.repair.completion.finalVoterIDs += [.status.repair.plan.oldNodeID]' ;;
      extra-final) filter='.status.repair.completion.finalVoterIDs += ["extra"]' ;;
      missing-final) filter='.status.repair.completion.finalVoterIDs |= .[1:]' ;;
    esac
    jq "$filter" "$scratch/leave-complete.json" > "$scratch/leave-bad-complete.json"
    if managed_functional_leave_completion_oracle "$scratch/leave-bad-complete.json" 4 "$(managed_functional_node_id leave-u5)" "$scratch/leave-expected-final.json" "$scratch/leave-bad.json"; then return 1; fi
  done
  managed_functional_leave_http_ok 204 0 0
  for leave_http in '200 0 0' '204 1 0' '204 0 1' '503 0 0'; do
    leave_http_code=${leave_http%% *}
    leave_http_rest=${leave_http#* }
    leave_http_pod=${leave_http_rest%% *}
    leave_http_wait=${leave_http_rest#* }
    if managed_functional_leave_http_ok "$leave_http_code" "$leave_http_pod" "$leave_http_wait"; then return 1; fi
  done
  managed_functional_roster_oracle "$scratch/leave-complete.json" "$scratch/leave-pods.json" "$scratch/leave-deploy.json" "$scratch/leave-endpoints.json" 4 "$scratch/leave-roster.json" leave
  if managed_functional_roster_oracle "$scratch/leave-complete.json" "$scratch/leave-pods.json" "$scratch/leave-deploy.json" "$scratch/leave-endpoints.json" 4 "$scratch/leave-roster-bad.json" add; then return 1; fi
  managed_functional_external_target_valid 3
  managed_functional_external_target_valid 5
  if managed_functional_external_target_valid 2; then return 1; fi
  if managed_functional_external_target_valid 65; then return 1; fi
  for external_scenario in external-scale external-high; do
    jq -n '{spec:{voterCount:3}}' > "$scratch/requested-cluster.json"
    functional_scenario=$external_scenario
    managed_functional_external_opt_in "$scratch/requested-cluster.json"
    jq -e '.spec.replicas==3' "$scratch/requested-cluster.json" >/dev/null
  done
  jq -n '{spec:{voterCount:3,replicas:4}}' > "$scratch/requested-cluster.json"
  if managed_functional_external_opt_in "$scratch/requested-cluster.json"; then return 1; fi
  functional_scenario=scale-up
  jq -n '{spec:{voterCount:3}}' > "$scratch/requested-cluster.json"
  managed_functional_external_opt_in "$scratch/requested-cluster.json"
  jq -e '.spec.replicas==null' "$scratch/requested-cluster.json" >/dev/null
  functional_scenario=
  external3_uids_json=$(printf '%s\n' leave-u1 leave-u2 leave-u3 | jq -R . | jq -sc 'map(select(.!=""))|sort')
  external3_ids_json=$(printf '%s\n' "$leave_ids_json" | jq -c '.[0:3]')
  jq -n --argjson uids "$external3_uids_json" '{items:[$uids[]|{metadata:{uid:.},status:{conditions:[{type:"Ready",status:"True"}]}}]}' > "$scratch/external3-pods.json"
  jq -n --argjson uids "$external3_uids_json" '{subsets:[{addresses:[$uids[]|{targetRef:{uid:.}}]}]}' > "$scratch/external3-endpoints.json"
  jq -n '{items:[{spec:{replicas:3}}]}' > "$scratch/external3-deploy.json"
  jq -n --argjson roster "$external3_ids_json" '{spec:{replicas:3},status:{appliedVoterCount:3,bootstrapPlan:{members:[$roster[]|{member:{ID:.}}]}}}' > "$scratch/external3-old-id.json"
  if managed_functional_external_roster_oracle "$scratch/external3-old-id.json" "$scratch/external3-pods.json" \
    "$scratch/external3-deploy.json" "$scratch/external3-endpoints.json" 3 "$scratch/external3-old-roster.json" bootstrap 2> "$scratch/external3-old-id.err"; then return 1; fi
  jq -n --argjson roster "$external3_ids_json" '{spec:{replicas:3},status:{appliedVoterCount:3,bootstrapPlan:{members:[$roster[]|{member:{node_id:.}}]}}}' > "$scratch/external3-cr.json"
  managed_functional_external_roster_oracle "$scratch/external3-cr.json" "$scratch/external3-pods.json" \
    "$scratch/external3-deploy.json" "$scratch/external3-endpoints.json" 3 "$scratch/external3-roster.json" bootstrap
  jq -n --argjson roster "$leave_ids_json" '{spec:{replicas:5},status:{appliedVoterCount:5,repair:{state:"Completed",plan:{sequence:"add",operationID:"external-add",expectedConfigID:9,finalVoterIDs:$roster},completion:{kind:"Terminal",operationID:"external-add",configID:10,finalVoterIDs:$roster}}}}' > "$scratch/external5-cr.json"
  jq -n --argjson uids "$leave_uids_json" '{items:[$uids[]|{metadata:{uid:.},status:{conditions:[{type:"Ready",status:"True"}]}}]}' > "$scratch/external5-pods.json"
  jq -n --argjson uids "$leave_uids_json" '{subsets:[{addresses:[$uids[]|{targetRef:{uid:.}}]}]}' > "$scratch/external5-endpoints.json"
  jq -n '{items:[{spec:{replicas:5}}]}' > "$scratch/external5-deploy.json"
  managed_functional_external_roster_oracle "$scratch/external5-cr.json" "$scratch/external5-pods.json" \
    "$scratch/external5-deploy.json" "$scratch/external5-endpoints.json" 5 "$scratch/external5-roster.json" add
  jq -e '.target==5 and .readyMatchesCertified==true and .endpointsMatchReady==true and .endpointIDs==.readyIDs and .endpointIDs==.finalVoterIDs and (.endpointPodUIDs|length)==5 and .endpointPodUIDs!=.endpointIDs and .endpointCount==5' "$scratch/external5-roster.json" >/dev/null
  for external_mutation in spec applied closed sequence duplicate; do
    case "$external_mutation" in
      spec) filter='.spec.replicas=4' ;;
      applied) filter='.status.appliedVoterCount=4' ;;
      closed) filter='.status.repair.state="Closed"' ;;
      sequence) filter='.status.repair.plan.sequence="leave"' ;;
      duplicate) filter='.status.repair.plan.finalVoterIDs[1]=.status.repair.plan.finalVoterIDs[0] | .status.repair.completion.finalVoterIDs=.status.repair.plan.finalVoterIDs' ;;
    esac
    jq "$filter" "$scratch/external5-cr.json" > "$scratch/external5-bad.json"
    if managed_functional_external_roster_oracle "$scratch/external5-bad.json" "$scratch/external5-pods.json" \
      "$scratch/external5-deploy.json" "$scratch/external5-endpoints.json" 5 "$scratch/external5-bad-roster.json" add; then return 1; fi
  done
  jq '.items[0].status.conditions[0].status="False"' "$scratch/external5-pods.json" > "$scratch/external5-bad-pods.json"
  if managed_functional_external_roster_oracle "$scratch/external5-cr.json" "$scratch/external5-bad-pods.json" \
    "$scratch/external5-deploy.json" "$scratch/external5-endpoints.json" 5 "$scratch/external5-bad-roster.json" add; then return 1; fi
  jq '.subsets[0].addresses += [{targetRef:{uid:"extra"}}]' "$scratch/external5-endpoints.json" > "$scratch/external5-bad-endpoints.json"
  if managed_functional_external_roster_oracle "$scratch/external5-cr.json" "$scratch/external5-pods.json" \
    "$scratch/external5-deploy.json" "$scratch/external5-bad-endpoints.json" 5 "$scratch/external5-bad-roster.json" add; then return 1; fi
  jq --arg uid "$(managed_functional_node_id leave-u1)" '.subsets[0].addresses[0].targetRef.uid=$uid' \
    "$scratch/external5-endpoints.json" > "$scratch/external5-node-id-confusion.json"
  if managed_functional_external_roster_oracle "$scratch/external5-cr.json" "$scratch/external5-pods.json" \
    "$scratch/external5-deploy.json" "$scratch/external5-node-id-confusion.json" 5 "$scratch/external5-confusion-roster.json" add; then return 1; fi
  managed_functional_external_sequence_artifacts "$managed_functional_external_high_sequence" \
    "$scratch/external-high-sequence.txt" "$scratch/external-high-sequence.json"
  [ "$(cat "$scratch/external-high-sequence.txt")" = "$managed_functional_external_high_sequence" ]
  jq -e '.==[3,4,5,6,7,8,9,10,9,8,7,6,5,4,3]' "$scratch/external-high-sequence.json" >/dev/null
  external_spaced_sequence=$(printf ' 3\t4\n5  4 3 ')
  managed_functional_external_sequence_artifacts "$external_spaced_sequence" \
    "$scratch/external-spaced-sequence.txt" "$scratch/external-spaced-sequence.json"
  jq -e '.==[3,4,5,4,3]' "$scratch/external-spaced-sequence.json" >/dev/null
  managed_self_test_replica_watch() {
    mfs_trw_sequence=$1 mfs_trw_out=$2
    jq -cn --arg sequence "$mfs_trw_sequence" '
      ($sequence|split(" ")|map(tonumber)) as $s |
      [range(0;($s|length)) as $i |
        ($s[$i]) as $n |
        [range(0;$n) as $j | "fixture-\($n)-\($j)"] as $ids |
        if $i==0 then
          ({spec:{replicas:$n},status:{appliedVoterCount:$n,bootstrapPlan:{members:[$ids[]|{member:{node_id:.}}]}}})
        else
          ($s[$i] > $s[$i-1]) as $up |
          ({spec:{replicas:$n},status:{appliedVoterCount:$n,repair:{state:"Completed",
            plan:{sequence:(if $up then "add" else "leave" end),operationID:("fixture-op-"+($i|tostring)),
              expectedConfigID:$i,finalVoterIDs:$ids},
            completion:{kind:"Terminal",configID:($i+1),operationID:("fixture-op-"+($i|tostring)),
              finalVoterIDs:$ids}}}})
        end] | .[]
    ' | jq -c . > "$mfs_trw_out"
  }
  managed_self_test_replica_watch "$managed_functional_external_high_sequence" "$scratch/replica-watch.jsonl"
  managed_functional_replica_progress_oracle "$scratch/replica-watch.jsonl" "$scratch/replica-progress.json" \
    "$managed_functional_external_high_sequence"
  jq -e '.sequence==[3,4,5,6,7,8,9,10,9,8,7,6,5,4,3] and (.steps|length)==14 and .maxMove==1 and .minimum==3 and (.events|length)==15 and .events[0].finalVoterIDs==["fixture-3-0","fixture-3-1","fixture-3-2"]' "$scratch/replica-progress.json" >/dev/null
  jq -c 'if .status.bootstrapPlan then .status.bootstrapPlan.members |= map({member:{ID:.member.node_id}}) else . end' \
    "$scratch/replica-watch.jsonl" > "$scratch/replica-watch-old-bootstrap-id.jsonl"
  if managed_functional_replica_progress_oracle "$scratch/replica-watch-old-bootstrap-id.jsonl" "$scratch/replica-progress-bad.json" "$managed_functional_external_high_sequence" 2> "$scratch/progress-old-bootstrap-id.err"; then return 1; fi
  managed_self_test_replica_watch '3 4 5 4 3' "$scratch/replica-watch-quick.jsonl"
  managed_functional_replica_progress_oracle "$scratch/replica-watch-quick.jsonl" "$scratch/replica-progress-quick.json" '3 4 5 4 3'
  if managed_functional_replica_progress_oracle "$scratch/replica-watch.jsonl" "$scratch/replica-progress-bad.json" '3 4 5 4 3' 2> "$scratch/progress-wrong.err"; then return 1; fi
  jq -s -c '.[0:5] + .[6:] | .[]' "$scratch/replica-watch.jsonl" > "$scratch/replica-watch-skipped.jsonl"
  if managed_functional_replica_progress_oracle "$scratch/replica-watch-skipped.jsonl" "$scratch/replica-progress-bad.json" "$managed_functional_external_high_sequence" 2> "$scratch/progress-skipped.err"; then return 1; fi
  jq -s -c '.[0:4] + [.[2]] + .[4:] | .[]' "$scratch/replica-watch.jsonl" > "$scratch/replica-watch-duplicate.jsonl"
  if managed_functional_replica_progress_oracle "$scratch/replica-watch-duplicate.jsonl" "$scratch/replica-progress-bad.json" "$managed_functional_external_high_sequence" 2> "$scratch/progress-duplicate.err"; then return 1; fi
  jq -s -c '.[0:5] + [(.[5] | .status.repair.plan.expectedConfigID += 10 | .status.repair.completion.configID = (.status.repair.plan.expectedConfigID + 1))] + .[6:] | .[]' "$scratch/replica-watch.jsonl" > "$scratch/replica-watch-config-gap.jsonl"
  if managed_functional_replica_progress_oracle "$scratch/replica-watch-config-gap.jsonl" "$scratch/replica-progress-bad.json" "$managed_functional_external_high_sequence" 2> "$scratch/progress-config-gap.err"; then return 1; fi
  jq -s -c '.[0:5] + [(.[5] | .status.repair.state="Closed")] + .[6:] | .[]' "$scratch/replica-watch.jsonl" > "$scratch/replica-watch-stale.jsonl"
  if managed_functional_replica_progress_oracle "$scratch/replica-watch-stale.jsonl" "$scratch/replica-progress-bad.json" "$managed_functional_external_high_sequence" 2> "$scratch/progress-stale.err"; then return 1; fi
  jq -c '.events[1:][] | {step:.index,from:.from,target:.to,sequence:.planSequence,operationID:.operationID,planExpectedConfigID:.planExpectedConfigID,completionConfigID:.completionConfigID,finalVoterIDs:.finalVoterIDs,certifiedRoster:.finalVoterIDs,readyIDs:.finalVoterIDs,endpointIDs:.finalVoterIDs,endpointPodUIDs:[.finalVoterIDs[] as $id | "pod-uid-"+$id],deploymentReplicas:.to}' \
    "$scratch/replica-progress.json" > "$scratch/external-high-checkpoints.jsonl"
  managed_functional_external_high_evidence_oracle "$scratch/external-high-checkpoints.jsonl" \
    "$scratch/replica-progress.json" "$scratch/external-high-ramp.json"
  jq -e '.target7Visible==true and .target10Visible==true and (.target7|length)==2 and (.target10|length)==1 and all(.target7[]; (.finalVoterIDs|length)==7 and .finalVoterIDs==.readyIDs and .finalVoterIDs==.endpointIDs) and all(.target10[]; (.finalVoterIDs|length)==10 and .finalVoterIDs==.readyIDs and .finalVoterIDs==.endpointIDs) and (.finalVoterIDs|length)==3' "$scratch/external-high-ramp.json" >/dev/null
  jq -c 'select(.target!=7) | select(.step!=4)' "$scratch/external-high-checkpoints.jsonl" > "$scratch/external-high-checkpoints-missing-7.jsonl"
  if managed_functional_external_high_evidence_oracle "$scratch/external-high-checkpoints-missing-7.jsonl" "$scratch/replica-progress.json" "$scratch/external-high-ramp-bad.json" 2> "$scratch/external-high-missing-7.err"; then return 1; fi
  jq -c 'if .target==10 then .finalVoterIDs[0]="stale-target-10" else . end' "$scratch/external-high-checkpoints.jsonl" > "$scratch/external-high-checkpoints-bad-10.jsonl"
  if managed_functional_external_high_evidence_oracle "$scratch/external-high-checkpoints-bad-10.jsonl" "$scratch/replica-progress.json" "$scratch/external-high-ramp-bad.json" 2> "$scratch/external-high-target-10.err"; then return 1; fi
  jq -c 'if .step==1 then .endpointIDs=.endpointPodUIDs else . end' "$scratch/external-high-checkpoints.jsonl" > "$scratch/external-high-checkpoints-pod-uid-confusion.jsonl"
  if managed_functional_external_high_evidence_oracle "$scratch/external-high-checkpoints-pod-uid-confusion.jsonl" "$scratch/replica-progress.json" "$scratch/external-high-ramp-bad.json" 2> "$scratch/external-high-pod-uid-confusion.err"; then return 1; fi
  jq -n '{metadata:{uid:"operator-deploy",generation:2},spec:{replicas:1},status:{observedGeneration:2,availableReplicas:1}}' > "$scratch/functional-operator-external-baseline-deployment.json"
  jq -n '{items:[{metadata:{uid:"operator-pod"},status:{conditions:[{type:"Ready",status:"True"}],containerStatuses:[{name:"operator",containerID:"containerd://operator-1",restartCount:0}]}}]}' > "$scratch/functional-operator-external-baseline-pods.json"
  operator_containers=$(managed_functional_operator_container_identity "$scratch/functional-operator-external-baseline-pods.json")
  logs=$scratch managed_functional_operator_running external-baseline operator-pod operator-deploy "$operator_containers"
  if logs=$scratch managed_functional_operator_running external-baseline wrong-pod operator-deploy; then return 1; fi
  if logs=$scratch managed_functional_operator_running external-baseline operator-pod wrong-deploy "$operator_containers"; then return 1; fi
  jq '.items[0].status.containerStatuses[0].restartCount=1' "$scratch/functional-operator-external-baseline-pods.json" > "$scratch/functional-operator-external-restarted-pods.json"
  cp "$scratch/functional-operator-external-baseline-pods.json" "$scratch/functional-operator-external-baseline-pods.json.original"
  cp "$scratch/functional-operator-external-restarted-pods.json" "$scratch/functional-operator-external-baseline-pods.json"
  if logs=$scratch managed_functional_operator_running external-baseline operator-pod operator-deploy "$operator_containers"; then return 1; fi
  cp "$scratch/functional-operator-external-baseline-pods.json.original" "$scratch/functional-operator-external-baseline-pods.json"
  jq '.items[0].status.containerStatuses[0].containerID="containerd://operator-2"' "$scratch/functional-operator-external-baseline-pods.json" > "$scratch/functional-operator-external-replaced-pods.json"
  cp "$scratch/functional-operator-external-replaced-pods.json" "$scratch/functional-operator-external-baseline-pods.json"
  if logs=$scratch managed_functional_operator_running external-baseline operator-pod operator-deploy "$operator_containers"; then return 1; fi
  cp "$scratch/functional-operator-external-baseline-pods.json.original" "$scratch/functional-operator-external-baseline-pods.json"
  rejection_cr="$scratch/rejection-cr.json"
  rejection_dep="$scratch/rejection-deployment.json"
  jq -n '{metadata:{uid:"cluster"},spec:{replicas:3},status:{appliedVoterCount:3,replicaStep:{generation:1,deploymentUID:"deploy",deploymentResourceVersion:"7",configID:9,from:4,to:3,expiresAt:"2030-01-01T00:00:00Z",checkID:"check"},replicaCheck:{id:"check",deploymentResourceVersion:"7",expiresAt:"2030-01-01T00:00:00Z"},repair:{state:"Closed",plan:{sequence:"leave",expectedConfigID:9,finalVoterIDs:["a","b","c"]}},conditions:[{type:"Ready",status:"True"}]}}' > "$rejection_cr"
  jq -n '{metadata:{uid:"deploy",resourceVersion:"7",generation:1},items:[{spec:{replicas:3},status:{observedGeneration:1,availableReplicas:3}}]}' > "$rejection_dep"
  : > "$scratch/rejection.stdout"
  printf '%s\n' 'The RhizaCluster "sql" is invalid: spec.replicas: Invalid value: 1: must be greater than or equal to 3' > "$scratch/rejection.stderr"
  for rejection_target in 1 2; do
    jq -n --argjson target "$rejection_target" '{target:$target,command:("kubectl scale rhizacluster/sql --replicas="+($target|tostring)),exitCode:1,result:"rejected",schemaValidation:true}' > "$scratch/rejection-$rejection_target.result.json"
    managed_functional_external_rejection_oracle "$rejection_cr" "$rejection_cr" "$rejection_dep" "$rejection_dep" \
      "$rejection_target" "$scratch/rejection.stdout" "$scratch/rejection.stderr" "$scratch/rejection-$rejection_target.result.json"
  done
  jq '.spec.replicas=2' "$rejection_cr" > "$scratch/rejection-cr-spec-changed.json"
  if managed_functional_external_rejection_oracle "$rejection_cr" "$scratch/rejection-cr-spec-changed.json" "$rejection_dep" "$rejection_dep" 1 "$scratch/rejection.stdout" "$scratch/rejection.stderr" "$scratch/rejection-1.result.json"; then return 1; fi
  jq 'del(.status.replicaStep)' "$rejection_cr" > "$scratch/rejection-cr-step-lost.json"
  if managed_functional_external_rejection_oracle "$rejection_cr" "$scratch/rejection-cr-step-lost.json" "$rejection_dep" "$rejection_dep" 1 "$scratch/rejection.stdout" "$scratch/rejection.stderr" "$scratch/rejection-1.result.json"; then return 1; fi
  jq '.items[0].spec.replicas=2' "$rejection_dep" > "$scratch/rejection-deployment-changed.json"
  if managed_functional_external_rejection_oracle "$rejection_cr" "$rejection_cr" "$rejection_dep" "$scratch/rejection-deployment-changed.json" 1 "$scratch/rejection.stdout" "$scratch/rejection.stderr" "$scratch/rejection-1.result.json"; then return 1; fi
  jq '.exitCode=0 | .result="accepted" | .schemaValidation=false' "$scratch/rejection-1.result.json" > "$scratch/rejection-accepted.result.json"
  if managed_functional_external_rejection_oracle "$rejection_cr" "$rejection_cr" "$rejection_dep" "$rejection_dep" 1 "$scratch/rejection.stdout" "$scratch/rejection.stderr" "$scratch/rejection-accepted.result.json"; then return 1; fi
  printf '%s\n' 'some other failure' > "$scratch/rejection-bad.stderr"
  if managed_functional_external_rejection_oracle "$rejection_cr" "$rejection_cr" "$rejection_dep" "$rejection_dep" 1 "$scratch/rejection.stdout" "$scratch/rejection-bad.stderr" "$scratch/rejection-1.result.json"; then return 1; fi
  private=$scratch namespace=managed-bootstrap
  : > "$scratch/curl.conf"
  k() {
    case "$*" in
      *"create secret generic functional-leave-voter-request"*) ;;
      *"create -f - -o json"*) cat > "$scratch/leave-job-created.json"; printf '%s\n' '{"metadata":{"uid":"leave-job"}}' ;;
      *"wait --for=condition=complete job/functional-leave-voter"*) ;;
      *"get job functional-leave-voter -o json"*) printf '%s\n' '{"status":{"conditions":[{"type":"Complete","status":"True"}]}}' ;;
      *"get pods -l job-name=functional-leave-voter -o json"*) printf '%s\n' '{"items":[{"status":{"containerStatuses":[{"name":"client","state":{"terminated":{"exitCode":0}}}]}}]}' ;;
      *"logs job/functional-leave-voter"*) printf '\nRHIZA_HTTP_CODE=%s\n' "$leave_http_fixture" ;;
      *) return 97 ;;
    esac
  }
  leave_http_fixture=204
  managed_functional_leave_voter leave-node
  jq -e '.node_id=="leave-node"' "$scratch/functional-leave-voter-request.json" >/dev/null
  jq -e '.spec.backoffLimit==0 and .spec.template.spec.restartPolicy=="Never" and .spec.template.spec.containers[0].args[-1]=="http://bootstrap-client.managed-bootstrap.svc.cluster.local:8080/leave-voter" and (.spec.template.spec.containers[0].args|index("--config"))!=null and (.spec.template.spec.containers[0].volumeMounts[0].mountPath=="/request")' "$scratch/leave-job-created.json" >/dev/null
  [ "$(cut -f1-3 "$scratch/functional-leave-voter.meta.tsv" | tr '\t' ',')" = 204,0,0 ]
  leave_http_fixture=200
  if managed_functional_leave_voter leave-node 2>/dev/null; then return 1; fi
  namespace=managed-bootstrap selector=test-selector logs=$scratch
  mfsu_order="$scratch/scale-up-order.txt"
  k() {
    case "$*" in
      *"get deployment -l $selector -o jsonpath="*) printf '%s\n' workload-deployment ;;
      *'patch rhizacluster sql'*'voterCount":4'*) printf '%s\n' patch-4 >> "$mfsu_order" ;;
      *'patch rhizacluster sql'*'voterCount":5'*) printf '%s\n' patch-5 >> "$mfsu_order" ;;
      *"scale deployment/workload-deployment --replicas=4"*) printf '%s\n' scale-4 >> "$mfsu_order" ;;
      *"scale deployment/workload-deployment --replicas=5"*) printf '%s\n' scale-5 >> "$mfsu_order" ;;
      *) return 97 ;;
    esac
  }
  managed_functional_commit() { printf 'commit-%s\n' "$2" >> "$mfsu_order"; }
  managed_functional_stop_operator() { printf '%s\n' stop >> "$mfsu_order"; }
  managed_functional_wait_enrollment() { printf 'wait-%s\n' "$1" >> "$mfsu_order"; }
  managed_functional_leave_scale_down() { printf '%s\n' leave-scale-down >> "$mfsu_order"; functional_leave_removed_node_id=leave-removed; }
  managed_functional_leave_voter() { printf 'leave-post-%s\n' "$1" >> "$mfsu_order"; }
  managed_functional_verify_leave() { printf '%s\n' leave-verify >> "$mfsu_order"; }
  managed_functional_scale_up
  printf '%s\n' commit-scale-functional-3 stop patch-4 scale-4 wait-4 commit-scale-functional-4 patch-5 scale-5 wait-5 commit-scale-functional-5 leave-scale-down leave-post-leave-removed leave-verify > "$scratch/scale-up-expected.txt"
  cmp "$scratch/scale-up-expected.txt" "$mfsu_order"
  functional_client_pod=load-pod private=$scratch
  k() {
    [ "$1 $2 $3 $4 $5" = "-n managed-bootstrap exec -i load-pod" ] || return 97
    cat > "$scratch/stage-forwarded"
  }
  managed_functional_stage up-3
  [ "$(cat "$scratch/stage-forwarded")" = up-3 ]
  functional_survivor_pid=
  survivor_stopped=0
  sleep() { :; }
  convergence_polls=0
  k() {
    convergence_polls=$((convergence_polls+1))
    if [ "$convergence_polls" = 4 ]; then
      printf '%s\n' '{"items":[]}'
    else
      printf '%s\n' '{"items":[{"metadata":{"uid":"survivor-a"},"status":{"podIP":"10.0.0.8","conditions":[{"type":"Ready","status":"True"}]}}]}'
    fi
  }
  managed_functional_wait_single_survivor
  [ "$convergence_polls" = 9 ]
  [ "$functional_survivor_ip" = 10.0.0.8 ]
  jq -e '.uid=="survivor-a" and .ip=="10.0.0.8" and .totalPods==1 and .readyPods==1 and .consecutiveReadyObservations==5' "$scratch/functional-one-convergence.json" >/dev/null
  k() {
    case "$*" in
      *"exec load-pod -c client -- sh -c"*) printf '%s\n' "$*" > "$scratch/survivor-command" ;;
      *"exec load-pod -c client -- touch /work/stop"*) survivor_stopped=1 ;;
      *"exec load-pod -c client -- test -f /work/one-done"*) [ "$survivor_stopped" = 1 ] ;;
      *"exec load-pod -c client -- cat /work/one-write.tsv"*) printf '1\tone-survivor\twrite\t1\t503\t0\tunavailable\n' ;;
      *"exec load-pod -c client -- cat /work/one-read.tsv"*) printf '2\tone-survivor\tlinearizable-read\t2\t503\t0\tunavailable\n' ;;
      *"exec load-pod -c client -- touch /work/probe-done"*) ;;
      *"wait --for=condition=complete job/functional-load"*) ;;
      *) return 97 ;;
    esac
  }
  managed_functional_start_survivor_load
  wait "$functional_survivor_pid"
  grep -F '/load/load.sh write' "$scratch/survivor-command" >/dev/null
  grep -F '/load/load.sh linear' "$scratch/survivor-command" >/dev/null
  # shellcheck disable=SC2016 # Verify the remote shell retains its variables.
  grep -F 'wait $w' "$scratch/survivor-command" >/dev/null
  # shellcheck disable=SC2016 # Verify the remote shell retains its variables.
  grep -F 'wait $r' "$scratch/survivor-command" >/dev/null
  if grep -F 'nohup' "$scratch/survivor-command" >/dev/null; then return 1; fi
  : > "$scratch/functional-writes.tsv"
  : > "$scratch/functional-reads.tsv"
  managed_functional_stop_survivor_load
  [ "$(awk -F '\t' '$2=="one-survivor" && $3=="write"{n++} END{print n+0}' "$scratch/functional-one-writes.tsv")" -gt 0 ]
  [ "$(awk -F '\t' '$2=="one-survivor" && $3=="linearizable-read"{n++} END{print n+0}' "$scratch/functional-one-reads.tsv")" -gt 0 ]
  phase_order="$scratch/survivor-phase-order.txt"
  managed_functional_wait_single_survivor() { printf '%s\n' converge >> "$phase_order"; }
  managed_functional_stage() { printf 'stage-%s\n' "$1" >> "$phase_order"; }
  managed_functional_start_survivor_load() { printf '%s\n' start >> "$phase_order"; }
  managed_functional_start_survivor_phase
  printf '%s\n' converge stage-one-survivor start > "$scratch/survivor-phase-expected.txt"
  cmp "$scratch/survivor-phase-expected.txt" "$phase_order"
  printf 'preserved-write\n' > "$scratch/functional-one-writes.tsv"
  k() { printf 'partial-read\n'; return 1; }
  managed_functional_capture_one_stream writes /work/one-write.tsv
  grep -Fx 'preserved-write' "$scratch/functional-one-writes.tsv" >/dev/null
  k() { :; }
  managed_functional_capture_one_stream writes /work/one-write.tsv
  grep -Fx 'preserved-write' "$scratch/functional-one-writes.tsv" >/dev/null
  k() { printf 'new-write\n'; }
  managed_functional_capture_one_stream writes /work/one-write.tsv
  grep -Fx 'new-write' "$scratch/functional-one-writes.tsv" >/dev/null
  printf '%s\n' 'preserved-raw' > "$scratch/raw-capture"
  k() { printf 'partial-raw\n'; return 1; }
  managed_functional_capture_file "$scratch/raw-capture" /work/raw.body
  grep -Fx 'preserved-raw' "$scratch/raw-capture" >/dev/null
  k() { printf '%s\n' 'new-raw'; }
  managed_functional_capture_file "$scratch/raw-capture" /work/raw.body
  grep -Fx 'new-raw' "$scratch/raw-capture" >/dev/null
  k() {
    case "$*" in
      *"get pods -l test-selector -o json"*) printf '%s\n' '{"items":[{"metadata":{"name":"voter-a"}},{"metadata":{"name":"voter-b"}}]}' ;;
      *"logs voter-a -c sql --previous"*|*"logs voter-b -c sql --previous"*) return 1 ;;
      *"logs voter-a -c sql"*) printf 'unavailable-a\n' ;;
      *"logs voter-b -c sql"*) printf 'unavailable-b\n' ;;
      *) return 97 ;;
    esac
  }
  managed_functional_capture_voter_logs phase
  grep -Fx 'unavailable-a' "$scratch/functional-voter-phase-voter-a-current.log" >/dev/null
  grep -Fx 'unavailable-b' "$scratch/functional-voter-phase-voter-b-current.log" >/dev/null
  printf '%s\n' 'PASS: managed functional scale oracles'
)

if [ "${1-}" = --self-test ]; then
  managed_functional_self_test
fi
