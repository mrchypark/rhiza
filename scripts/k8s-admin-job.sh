#!/usr/bin/env bash
set -euo pipefail

[ "$#" -ge 4 ] && [ "$#" -le 5 ] || {
  echo "usage: $0 SERVICE POD METHOD PATH [JSON_BODY]" >&2
  exit 64
}
service="$1" pod="$2" method="$3" path="$4" body="${5-}"
[ -n "$body" ] || body='{}'
namespace="${QUEQLITE_K8S_NAMESPACE:-queqlite-e2e}"
context="${QUEQLITE_KUBE_CONTEXT:-}"
auth_secret="${QUEQLITE_AUTH_SECRET:-queqlite-auth}"
curl_image="${QUEQLITE_CURL_IMAGE:-curlimages/curl:8.10.1}"
job="ql-admin-$(date +%s)-$$-${RANDOM}"
manifest="$(mktemp)"
response="$(mktemp)"
trap 'rm -f "$manifest" "$response"' EXIT

emit_single_json() {
  file="$1"
  if ! jq -e -s 'length == 1' "$file" >/dev/null; then
    echo "admin Job stdout must contain exactly one JSON document" >&2
    cat "$file" >&2
    return 1
  fi
  cat "$file"
}

case "$service$pod" in *[!a-z0-9-]*) exit 64;; esac
case "$method" in GET|POST|PUT) ;; *) exit 64;; esac
case "$path" in /*) ;; *) exit 64;; esac
printf '%s' "$body" | jq -e . >/dev/null

k=(kubectl)
[ -z "$context" ] || k+=(--context "$context")
k+=(-n "$namespace")
escape() { printf '%s' "$1" | sed 's/[&|\\]/\\&/g'; }
sed \
  -e "s|__JOB_NAME__|$job|g" \
  -e "s|__CURL_IMAGE__|$(escape "$curl_image")|g" \
  -e "s|__METHOD__|$method|g" \
  -e "s|__BODY__|$(escape "$body")|g" \
  -e "s|__POD__|$pod|g" \
  -e "s|__SERVICE__|$service|g" \
  -e "s|__PATH__|$(escape "$path")|g" \
  -e "s|__AUTH_SECRET__|$auth_secret|g" \
  deploy/k8s/queqlite-admin-job.yaml > "$manifest"

if [ -n "${QUEQLITE_ADMIN_JOB_RENDER_ONLY:-}" ]; then
  cp "$manifest" "$QUEQLITE_ADMIN_JOB_RENDER_ONLY"
  exit 0
fi
if [ -n "${QUEQLITE_ADMIN_JOB_RESPONSE_FILE:-}" ]; then
  emit_single_json "$QUEQLITE_ADMIN_JOB_RESPONSE_FILE"
  exit
fi

"${k[@]}" create -f "$manifest" >/dev/null
deadline=$((SECONDS + 130))
while :; do
  complete="$("${k[@]}" get "job/$job" \
    -o 'jsonpath={.status.conditions[?(@.type=="Complete")].status}')"
  if [ "$complete" = True ]; then
    if ! "${k[@]}" logs "job/$job" > "$response"; then
      cat "$response" >&2
      exit 1
    fi
    emit_single_json "$response"
    exit 0
  fi
  failed="$("${k[@]}" get "job/$job" \
    -o 'jsonpath={.status.conditions[?(@.type=="Failed")].status}')"
  if [ "$failed" = True ]; then
    "${k[@]}" logs "job/$job" >&2 || true
    exit 1
  fi
  [ "$SECONDS" -lt "$deadline" ] || {
    echo "timed out waiting for admin Job $job" >&2
    "${k[@]}" logs "job/$job" >&2 || true
    exit 1
  }
  sleep 1
done
