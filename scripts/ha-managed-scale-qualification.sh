#!/bin/sh
# shellcheck disable=SC2154
# Functional scale qualification invoked by test-ha-managed-bootstrap.sh after
# the existing three-voter bootstrap and SQL/XKV readback have passed.

managed_functional_node_id() {
  printf 'rhiza-managed-node-v1\000%s' "$1" | sha256sum | awk '{print "rhiza-n-"$1}'
}

managed_functional_roster_oracle() {
  mfr_cr=$1 mfr_pods=$2 mfr_dep=$3 mfr_eps=$4 mfr_target=$5 mfr_out=$6
  [ "$(jq -r '.spec.voterCount' "$mfr_cr")" = "$mfr_target" ] || return 1
  [ "$(jq -r '.items|length' "$mfr_dep")" = 1 ] || return 1
  [ "$(jq -r '.items[0].spec.replicas' "$mfr_dep")" = "$mfr_target" ] || return 1
  mfr_ready_uids=$(jq -r '.items[] | select(any(.status.conditions[]?; .type=="Ready" and .status=="True")) | .metadata.uid' "$mfr_pods" | sort)
  [ "$(printf '%s\n' "$mfr_ready_uids" | sed '/^$/d' | wc -l | tr -d ' ')" = "$mfr_target" ] || return 1
  mfr_ready_ids=
  for mfr_uid in $mfr_ready_uids; do mfr_ready_ids="$mfr_ready_ids\n$(managed_functional_node_id "$mfr_uid")"; done
  mfr_ready_ids=$(printf '%b\n' "$mfr_ready_ids" | sed '/^$/d' | sort)
  if [ "$mfr_target" = 3 ]; then
    mfr_roster=$(jq -c '.status.bootstrapPlan.members|map(.member.ID)|sort' "$mfr_cr")
  else
    jq -e --argjson target "$mfr_target" '.status.repair.state=="Completed" and .status.repair.plan.sequence=="add" and .status.repair.completion.kind=="Terminal" and (.status.repair.completion.finalVoterIDs|length)==$target and ((.status.repair.completion.finalVoterIDs|unique|length)==$target)' "$mfr_cr" >/dev/null || return 1
    mfr_roster=$(jq -c '.status.repair.completion.finalVoterIDs|sort' "$mfr_cr")
  fi
  [ "$mfr_roster" = "$(printf '%s\n' "$mfr_ready_ids" | jq -R . | jq -sc 'map(select(.!=""))|sort')" ] || return 1
  mfr_endpoint_uids=$(jq -r '[.subsets[]?.addresses[]?.targetRef.uid]|sort|.[]' "$mfr_eps" | sort)
  [ "$(printf '%s\n' "$mfr_endpoint_uids" | sed '/^$/d' | wc -l | tr -d ' ')" = "$mfr_target" ] || return 1
  [ "$(printf '%s\n' "$mfr_endpoint_uids" | sort)" = "$(printf '%s\n' "$mfr_ready_uids" | sort)" ] || return 1
  jq -n --argjson target "$mfr_target" --argjson roster "$mfr_roster" --argjson ready "$(printf '%s\n' "$mfr_ready_ids" | jq -R . | jq -sc 'map(select(.!=""))|sort')" '{target:$target,certifiedRoster:$roster,readyIDs:$ready,readyMatchesCertified:($roster==$ready),endpointCount:$target}' > "$mfr_out"
}

managed_functional_observe_roster() {
  mfo_target=$1 mfo_tag=$2
  k -n "$namespace" get rhizacluster sql -o json > "$logs/functional-$mfo_tag-cr.json"
  k -n "$namespace" get deployment -l "$selector" -o json > "$logs/functional-$mfo_tag-deployment.json"
  k -n "$namespace" get pods -l "$selector" -o json > "$logs/functional-$mfo_tag-pods.json"
  k -n "$namespace" get endpoints bootstrap-client -o json > "$logs/functional-$mfo_tag-endpoints.json"
  managed_functional_roster_oracle "$logs/functional-$mfo_tag-cr.json" "$logs/functional-$mfo_tag-pods.json" "$logs/functional-$mfo_tag-deployment.json" "$logs/functional-$mfo_tag-endpoints.json" "$mfo_target" "$logs/functional-$mfo_tag-roster.json"
}

managed_functional_enrollment_ready() {
  mfer_target=$1 mfer_tag=$2
  jq -e --argjson target "$mfer_target" '.spec.voterCount==$target and .status.repair.state=="Completed" and .status.repair.plan.sequence=="add" and .status.repair.completion.kind=="Terminal" and (.status.repair.completion.finalVoterIDs|length)==$target' "$logs/functional-$mfer_tag-poll.json" >/dev/null 2>&1 \
    && jq -e --argjson target "$mfer_target" '(.items|length)==1 and .items[0].spec.replicas==$target' "$logs/functional-$mfer_tag-deployment-poll.json" >/dev/null 2>&1 \
    && jq -e --argjson target "$mfer_target" '[.items[]|select(any(.status.conditions[]?; .type=="Ready" and .status=="True"))]|length==$target' "$logs/functional-$mfer_tag-pods-poll.json" >/dev/null 2>&1 \
    && jq -e --argjson target "$mfer_target" '[.subsets[]?.addresses[]?]|length==$target' "$logs/functional-$mfer_tag-endpoints-poll.json" >/dev/null 2>&1
}

managed_functional_wait_enrollment() {
  mfw_target=$1 mfw_tag=$2
  mfw_deadline=$(( $(date +%s) + 300 ))
  while :; do
    k -n "$namespace" get rhizacluster sql -o json > "$logs/functional-$mfw_tag-poll.json" 2>/dev/null || :
    k -n "$namespace" get deployment -l "$selector" -o json > "$logs/functional-$mfw_tag-deployment-poll.json" 2>/dev/null || :
    k -n "$namespace" get pods -l "$selector" -o json > "$logs/functional-$mfw_tag-pods-poll.json" 2>/dev/null || :
    k -n "$namespace" get endpoints bootstrap-client -o json > "$logs/functional-$mfw_tag-endpoints-poll.json" 2>/dev/null || :
    if managed_functional_enrollment_ready "$mfw_target" "$mfw_tag"; then
      managed_functional_observe_roster "$mfw_target" "$mfw_tag"
      return 0
    fi
    [ "$(date +%s)" -lt "$mfw_deadline" ] || return 1
    sleep 2
  done
}

managed_functional_operator_stopped() {
  jq -e '.spec.replicas==0 and .status.observedGeneration==.metadata.generation and (.status.replicas//0)==0 and (.status.updatedReplicas//0)==0 and (.status.availableReplicas//0)==0' "$logs/functional-operator-deployment.json" >/dev/null 2>&1 \
    && jq -e '(.items|length)==0' "$logs/functional-operator-pods.json" >/dev/null 2>&1
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
  code=$(printf '%s' "$body" | curl --silent --show-error --output "/work/$mode.body" --write-out '%{http_code}' --max-time 8 --noproxy '*' --config /auth/config --header 'Content-Type: application/json' --data-binary @- "http://bootstrap-client.managed-bootstrap.svc.cluster.local:8080/$route")
  rc=$?
  response=$(tr '\n' ' ' < "/work/$mode.body" 2>/dev/null | head -c 2048 || :)
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
}

managed_functional_start_survivor_load() {
  # shellcheck disable=SC2016 # The remote shell owns $! and wait targets.
  k -n "$namespace" exec "$functional_client_pod" -c client -- sh -c '
    rm -f /work/stop /work/one-done
    sh /load/load.sh write > /work/one-write.tsv 2>&1 & w=$!
    sh /load/load.sh linear > /work/one-read.tsv 2>&1 & r=$!
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

managed_functional_capture() {
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
    cat "$logs/functional-writes.tsv" "$logs/functional-one-writes.tsv" > "$logs/functional-all-writes.tsv" 2>/dev/null || :
    cat "$logs/functional-reads.tsv" "$logs/functional-one-reads.tsv" > "$logs/functional-all-reads.tsv" 2>/dev/null || :
    k -n "$namespace" exec "$functional_client_pod" -c client -- touch /work/probe-done >/dev/null 2>&1 || :
  fi
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
  printf '%s\n' 'PASS: 3->4->5 certified managed enrollment, Deployment/Ready/endpoint rosters, ACK and SQL readback' > "$logs/functional-result.txt"
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
  printf '%s\n' 'PASS: 3->2->3 replacement, concurrent HTTP code inventory, ACK/local checks, and one-survivor fail-closed behavior' > "$logs/functional-result.txt"
}

managed_functional_qualification() {
  [ "${fixture_case-}" = internal ] && [ "${fixture_provider-}" = s3 ] || {
    echo 'functional qualification requires the internal S3 fixture' >&2
    return 1
  }
  case "$1" in
    scale-up) managed_functional_scale_up ;;
    quorum) managed_functional_quorum ;;
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
[ -z "$out" ] || printf '%s' '{"slot":1,"status":"committed","rows_affected":1,"last_insert_id":100001,"retry_through_slot":1}' > "$out"
printf '200'
EOF
  chmod 0755 "$scratch/curl"
  PATH="$scratch:$PATH" LOAD_WORK="$scratch/work" sh "$scratch/load-test.sh" write > "$scratch/stage-per-request.tsv"
  [ "$(awk -F '\t' '{printf "%s%s",sep,$2; sep=","}' "$scratch/stage-per-request.tsv")" = initial,down-2,up-3 ]
  printf 'not-a-row\n' > "$scratch/writes.tsv"
  if managed_functional_load_oracle "$scratch/writes.tsv" "$scratch/reads.tsv" "$scratch/bad.json" 2>/dev/null; then return 1; fi
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
  managed_functional_scale_up
  printf '%s\n' commit-scale-functional-3 stop patch-4 scale-4 wait-4 commit-scale-functional-4 patch-5 scale-5 wait-5 commit-scale-functional-5 > "$scratch/scale-up-expected.txt"
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
