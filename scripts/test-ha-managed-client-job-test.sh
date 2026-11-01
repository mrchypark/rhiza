#!/bin/sh
# Offline replay of the real common client Job helper; no API calls or SQL retries.
set -eu
cd "$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
fixture=scripts/ha-managed-bootstrap-fixture.sh
capture=$(sed -n '/^capture_client_job() {/,/^}/p' "$fixture")
waiter=$(sed -n '/^wait_client_job() {/,/^}/p' "$fixture")
client=$(sed -n '/^run_client() {/,/^}/p' "$fixture")
[ -n "$capture" ] && [ -n "$waiter" ] && [ -n "$client" ]
scratch=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-client-job.XXXXXX")
listener_pid=
cleanup() {
  if [ -n "$listener_pid" ]; then kill "$listener_pid" 2>/dev/null || :; wait "$listener_pid" 2>/dev/null || :; fi
  for scenario in complete failed failed-terminal init-failed missing-init api-error missing-pod foreign-pod pod-api-error replaced-job timeout; do
    for file in "$scratch/$scenario"/logs/* "$scratch/$scenario"/private/* "$scratch/$scenario"/*; do
      [ ! -f "$file" ] || rm -f "$file"
    done
    rmdir "$scratch/$scenario/logs" "$scratch/$scenario/private" "$scratch/$scenario" 2>/dev/null || :
  done
  for file in "$scratch"/*; do [ ! -f "$file" ] || rm -f "$file"; done
  rmdir "$scratch"
}
trap cleanup EXIT HUP INT TERM
for scenario in complete failed failed-terminal init-failed missing-init api-error missing-pod foreign-pod pod-api-error replaced-job timeout; do
  mkdir "$scratch/$scenario" "$scratch/$scenario/logs" "$scratch/$scenario/private"
  sh -c '
    set -eu
    scenario=$1 root=$2 namespace=test-ns
    logs=$root/logs private=$root/private
    printf "%s\n" NO_PRIVATE_TOKEN > "$private/curl.conf"
    printf 0 > "$root/polls"
    printf 0 > "$root/ticks"
    sleep() { :; }
    date() {
      tick=$(cat "$root/ticks")
      printf "%s\n" "$tick"
      if [ "$scenario" = timeout ]; then tick=$((tick+121)); else tick=$((tick+1)); fi
      printf "%s\n" "$tick" > "$root/ticks"
    }
    k() {
      [ "$1" = -n ] && [ "$2" = "$namespace" ] || return 9
      shift 2
      printf "%s\n" "$*" >> "$root/calls"
      case "$1/$2" in
        create/secret) return 0 ;;
        create/-f)
          cat > "$root/manifest.json"
          jq ".metadata.uid=\"job-uid\" | .metadata.namespace=\"test-ns\" | .spec.template.spec.containers[0].env=[{name:\"TOKEN\",value:\"NO_PRIVATE_TOKEN\"}]" "$root/manifest.json" ;;
        get/job/client-test)
          [ "$scenario" != api-error ] || return 7
          polls=$(cat "$root/polls"); polls=$((polls+1)); printf "%s\n" "$polls" > "$root/polls"
          condition=FailureTarget
          [ "$scenario" != failed-terminal ] || condition=Failed
          if [ "$scenario" = complete ]; then condition=Pending; [ "$polls" -lt 2 ] || condition=Complete; fi
          job_uid=job-uid; [ "$scenario" != replaced-job ] || job_uid=replaced-job-uid
          jq -n --arg condition "$condition" --arg uid "$job_uid" "{apiVersion:\"batch/v1\",kind:\"Job\",metadata:{name:\"client-test\",namespace:\"test-ns\",uid:\$uid,annotations:{secret:\"NO_PRIVATE_TOKEN\"}},status:{conditions:[{type:\$condition,status:\"True\"}]}}" ;;
        get/pods)
          [ "$scenario" != pod-api-error ] || return 8
          if [ "$scenario" = missing-pod ] || [ "$scenario" = timeout ]; then printf "%s\n" "{\"items\":[]}"; return 0; fi
          owner_uid=job-uid; [ "$scenario" != foreign-pod ] || owner_uid=other-job-uid
          jq -n --arg owner "$owner_uid" --arg scenario "$scenario" "{items:[{metadata:{name:\"client-pod\",namespace:\"test-ns\",uid:\"pod-uid\",annotations:{secret:\"NO_PRIVATE_TOKEN\"},ownerReferences:[{kind:\"Job\",uid:\$owner,controller:true}]},spec:{containers:[{name:\"client\",env:[{name:\"TOKEN\",value:\"NO_PRIVATE_TOKEN\"}]}]},status:{phase:\"Failed\",initContainerStatuses:(if \$scenario==\"missing-init\" then [] else [{name:\"ready\",state:{terminated:{exitCode:(if \$scenario==\"init-failed\" then 7 else 0 end),reason:\"Error\",message:\"NO_PRIVATE_TOKEN\"}}}] end),containerStatuses:[{name:\"client\",state:{terminated:{exitCode:22,reason:\"Error\",message:\"NO_PRIVATE_TOKEN\"}}}]}}]}" ;;
        logs/client-pod)
          case "$*" in *--tail=200*--limit-bytes=65536*) ;; *) return 9 ;; esac
          case "$*" in *--previous*) printf "%s\n" no-previous-container >&2; return 1 ;; esac
          case "$*" in *"-c ready "*) printf "%s\n" RHIZA_READY_HTTP_CODE=204; [ "$scenario" != init-failed ] || { printf "%s\n" "curl: (7) connection refused"; }; return 0 ;; esac
          if [ "$scenario" = complete ]; then
            printf "%s\n" "{\"rows\":[[1,\"ok\"]]}" RHIZA_HTTP_CODE=200
          else
            printf "%s\n" "{\"error\":\"commit outcome unknown\",\"request_id\":\"same-id\"}" "curl: (22) HTTP 503" RHIZA_HTTP_CODE=503
          fi ;;
        *) return 9 ;;
      esac
    }
    eval "$3"; eval "$4"; eval "$5"
    code=0
    run_client client-test execute "{\"request_id\":\"same-id\",\"sql\":\"NO_PRIVATE_BODY\"}" || code=$?
    case "$scenario" in
      complete)
        [ "$code" -eq 0 ] && [ "$(cat "$root/polls")" -eq 2 ]
        jq -e ".rows==[[1,\"ok\"]]" "$logs/client-test.json" >/dev/null
        if grep -q RHIZA_HTTP_CODE "$logs/client-test.json"; then exit 1; fi ;;
      failed|failed-terminal)
        [ "$code" -eq 1 ] && [ "$(cat "$root/polls")" -eq 1 ]
        grep -Fq "commit outcome unknown" "$logs/client-test-client-current.log"
        grep -Fxq RHIZA_HTTP_CODE=503 "$logs/client-test-client-current.log"
        jq -e ".items[0].status.containerStatuses[0].state.terminated.exitCode==22" "$logs/client-test-pods.json" >/dev/null ;;
      init-failed)
        [ "$code" -eq 1 ]
        grep -Fq "connection refused" "$logs/client-test-ready-current.log"
        jq -e ".items[0].status.initContainerStatuses[0].state.terminated.exitCode==7" "$logs/client-test-pods.json" >/dev/null ;;
      api-error) [ "$code" -eq 7 ] && [ -s "$logs/client-test-evidence-gap.txt" ] ;;
      missing-init|missing-pod|foreign-pod|pod-api-error|replaced-job)
        [ "$code" -eq 1 ] && [ -s "$logs/client-test-evidence-gap.txt" ] ;;
      timeout) [ "$code" -eq 124 ] && [ "$(cat "$root/polls")" -eq 0 ] ;;
    esac
    [ "$(cat "$logs/client-test-wait.exit")" -eq "$code" ]
    [ "$(grep -c "^create -f " "$root/calls")" -eq 1 ]
    jq -e ".spec.backoffLimit==0 and .spec.activeDeadlineSeconds==90 and (.spec.template.spec.containers[0].args|index(\"--fail-with-body\")!=null) and (.spec.template.spec.containers[0].args|index(\"--retry\")==null)" "$root/manifest.json" >/dev/null
    jq -e ".spec.template.spec as \$s | \$s.initContainers[0] as \$i | \$i.name==\"ready\" and \$i.image==\$s.containers[0].image and \$i.securityContext==\$s.containers[0].securityContext and (\$i.args|index(\"--retry-connrefused\")!=null) and (\$i.args|index(\"--retry-all-errors\")==null) and (\$i.args|index(\"--data-binary\")==null) and (\$i.args|last|endswith(\"/readyz\")) and (\$i.args|index(\"--retry-max-time\") as \$n | .[\$n+1]==\"15\")" "$root/manifest.json" >/dev/null
    if grep -R -E "NO_PRIVATE_TOKEN|NO_PRIVATE_BODY" "$logs"; then echo "private request or Pod spec leaked" >&2; exit 1; fi
  ' fixture "$scenario" "$scratch/$scenario" "$capture" "$waiter" "$client"
done
# Replay the actual init curl arguments against a disposable loopback server.
command -v nc >/dev/null
port=$((40000 + $$ % 10000))
if nc -z 127.0.0.1 "$port" 2>/dev/null; then echo 'loopback test port already owned' >&2; exit 1; fi
printf '%s\n' 'header = "Authorization: Bearer test-only"' > "$scratch/config"
ready_curl() {
  ready_test_mode=${1-}
  jq -r '.spec.template.spec.initContainers[0].args[]' "$scratch/complete/manifest.json" > "$scratch/args"
  set --
  while IFS= read -r arg; do
    case "$arg" in /request/config) arg=$scratch/config ;; http://bootstrap-client.*/readyz) arg="http://127.0.0.1:$port/readyz" ;; esac
    # jq renders the write-out newline as an empty extra line; no empty argv needed.
    [ -n "$arg" ] || continue
    set -- "$@" "$arg"
  done < "$scratch/args"
  case "$ready_test_mode" in
    dns) curl "$@" --connect-to "127.0.0.1:$port:rhiza-unresolvable.invalid:$port" ;;
    invalid) curl "$@" --invalid-option ;;
    *) curl "$@" ;;
  esac
}
(
  sleep 1
  printf 'HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n' | nc -l 127.0.0.1 "$port" > "$scratch/request"
) &
listener_pid=$!
ready_curl > "$scratch/ready.log" 2>&1
wait "$listener_pid"; listener_pid=
grep -Fq 'curl: (7)' "$scratch/ready.log"
grep -Fq 'GET /readyz HTTP/' "$scratch/request"
printf '%s\n' SQL-once > "$scratch/business"
[ "$(wc -l < "$scratch/business")" -eq 1 ]
(
  for status in 503 204; do
    printf 'HTTP/1.1 %s Result\r\nContent-Length: 0\r\nConnection: close\r\n\r\n' "$status" | nc -l 127.0.0.1 "$port" >> "$scratch/transient-request"
  done
) &
listener_pid=$!
sleep 1
ready_curl > "$scratch/transient.log" 2>&1
wait "$listener_pid"; listener_pid=
[ "$(grep -c 'GET /readyz HTTP/' "$scratch/transient-request")" -eq 2 ]
for status in 401 404; do
  printf 'HTTP/1.1 %s Rejected\r\nContent-Length: 0\r\nConnection: close\r\n\r\n' "$status" | nc -l 127.0.0.1 "$port" > "$scratch/request" &
  listener_pid=$!
  sleep 1
  : > "$scratch/business"
  code=0; ready_curl > "$scratch/rejected.log" 2>&1 || code=$?
  [ "$code" -ne 0 ] || printf '%s\n' SQL-once >> "$scratch/business"
  [ "$code" -eq 22 ]
  [ ! -s "$scratch/business" ]
  wait "$listener_pid"; listener_pid=
  [ "$(grep -c 'GET /readyz HTTP/' "$scratch/request")" -eq 1 ]
done
start=$(date +%s)
: > "$scratch/business"
code=0; ready_curl > "$scratch/refused.log" 2>&1 || code=$?
[ "$code" -ne 0 ] || printf '%s\n' SQL-once >> "$scratch/business"
[ "$code" -eq 7 ] && [ "$(( $(date +%s) - start ))" -le 20 ]
[ ! -s "$scratch/business" ]
# No successful prerequisite means no business command: the init/main barrier is Kubernetes-native.
for mode in dns invalid; do
  : > "$scratch/business"
  start=$(date +%s)
  code=0; ready_curl "$mode" > "$scratch/$mode.log" 2>&1 || code=$?
  [ "$code" -ne 0 ] || printf '%s\n' SQL-once >> "$scratch/business"
  if [ "$mode" = dns ]; then [ "$code" -eq 6 ]; else [ "$code" -eq 2 ]; fi
  [ "$(( $(date +%s) - start ))" -le 3 ] && [ ! -s "$scratch/business" ]
done
printf '%s\n' 'PASS: common client Job completion/failure/deadline/UID-bound diagnostics; original failures retained without request retries or secret contents'
