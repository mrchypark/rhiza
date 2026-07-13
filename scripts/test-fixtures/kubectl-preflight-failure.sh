#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >> "$QUEQLITE_KUBECTL_FIXTURE_LOG"

case " $* " in
  *" get statefulset queqlite-c3 "*) exit 0 ;;
  *" get secret queqlite-c4-bundle -o json "*) exit 1 ;;
  *" get secret missing-object-credentials "*) exit 1 ;;
  *" create -f "*)
    manifest="${*: -1}"
    args="$(yq eval -r '.spec.template.spec.containers[0].args | join(" ")' "$manifest")"
    [ "$args" = "checkpoint inspect" ]
    printf '%s\n' "$args" >> "$QUEQLITE_KUBECTL_FIXTURE_LOG"
    case "$QUEQLITE_KUBECTL_FIXTURE_PROFILE" in
      provider)
        [ "$(yq eval '[.spec.template.spec.containers[0].env[] |
          select(.name == "QUEQLITE_S3_ENDPOINT" or
            .name == "QUEQLITE_S3_ACCESS_KEY" or
            .name == "QUEQLITE_S3_SECRET_KEY")] | length' "$manifest")" = 0 ]
        ;;
      endpoint)
        [ "$(yq eval -r '.spec.template.spec.containers[0].env[] |
          select(.name == "QUEQLITE_S3_ENDPOINT") | .value' "$manifest")" = \
          http://127.0.0.1:1 ]
        ;;
      *) exit 99 ;;
    esac
    ;;
  *" get job/ql-object-"*"Complete"*) exit 0 ;;
  *" get job/ql-object-"*"Failed"*) printf 'True' ;;
  *" logs job/ql-object-"*) echo "fixture object-store preflight failed" >&2 ;;
  *) exit 99 ;;
esac
