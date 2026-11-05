#!/bin/sh
# Check a loaded native image, without Kubernetes or an object-store connection.
set -eu

[ "$#" -eq 4 ] || { echo 'usage: test-operator-image.sh IMAGE ARCH REVISION VERSION' >&2; exit 2; }
image=$1 arch=$2 revision=$3 version=$4
case "$arch" in amd64) machine='62 0' ;; arm64) machine='183 0' ;; *) exit 2 ;; esac
[ "${#revision}" -eq 40 ] || exit 2
case "$revision" in *[!0-9a-f]*) exit 2 ;; esac
[ -n "$version" ]
work=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-operator-image.XXXXXX")
files="rhiza-operator-files-$arch-$$"
runtime="rhiza-operator-runtime-$arch-$$"
files_id='' runtime_id=''
create_owned() {
    if created_id=$(docker create "$@"); then
        [ "${#created_id}" -eq 64 ] || { echo 'unverified container ID; cleanup requires inspection' >&2; return 1; }
        case "$created_id" in *[!0-9a-f]*) echo 'unverified container ID; cleanup requires inspection' >&2; return 1 ;; esac
        printf '%s\n' "$created_id"
    else
        return "$?"
    fi
}
cleanup() {
    status=$?
    cleanup_failed=0
    trap - EXIT HUP INT TERM
    for id in "$files_id" "$runtime_id"; do
        [ -n "$id" ] || continue
        if remaining=$(docker ps -aq --no-trunc --filter "id=$id"); then
            if [ -n "$remaining" ]; then
                if [ "$remaining" = "$id" ]; then
                    docker rm -f "$id" >/dev/null || cleanup_failed=1
                else
                    cleanup_failed=1
                fi
            fi
            if remaining=$(docker ps -aq --no-trunc --filter "id=$id"); then
                [ -z "$remaining" ] || cleanup_failed=1
            else
                cleanup_failed=1
            fi
        else
            cleanup_failed=1
        fi
    done
    rm -r "$work" || cleanup_failed=1
    [ "$cleanup_failed" -eq 0 ] || { echo 'owned container cleanup could not be verified' >&2; [ "$status" -ne 0 ] || status=1; }
    [ "$status" -eq 0 ] || echo 'operator image smoke failed; inspect the original failure and cleanup output' >&2
    exit "$status"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

docker image inspect "$image" > "$work/image.json"
jq -e --arg arch "$arch" --arg revision "$revision" --arg version "$version" '
  length == 1 and .[0].Os == "linux" and .[0].Architecture == $arch and
  .[0].Config.User == "65532:65532" and
  .[0].Config.Labels["org.opencontainers.image.revision"] == $revision and
  .[0].Config.Labels["org.opencontainers.image.version"] == $version
' "$work/image.json" >/dev/null
files_id=$(create_owned --name "$files" "$image")
for binary in rhiza-operator rhiza-ha-init; do
    docker cp "$files_id:/usr/local/bin/$binary" "$work/$binary"
    magic=$(od -An -tx1 -N6 "$work/$binary" | tr -d ' \n')
    [ "$magic" = 7f454c460201 ] || { echo "not a little-endian ELF64 binary: $binary" >&2; exit 1; }
    actual=$(od -An -tu1 -j18 -N2 "$work/$binary" | awk '{$1=$1;print}')
    [ "$actual" = "$machine" ] || { echo "wrong ELF machine: $binary" >&2; exit 1; }
done

runtime_id=$(create_owned -i --name "$runtime" --platform "linux/$arch" \
    --network none --read-only --tmpfs /tmp:rw,nosuid,nodev,noexec,size=1m,mode=1777 \
    --cap-drop ALL --security-opt no-new-privileges --entrypoint /bin/sh "$image" -seu)
timeout 30s docker start -ai "$runtime_id" <<'SMOKE'
test "$(id -u)" = 65532
test -s /etc/ssl/certs/ca-certificates.crt
rhiza-operator --help > /tmp/operator-help 2>&1
grep -q 'namespace' /tmp/operator-help
rhiza-ha-init prepare-identity --cr-uid image-smoke-cluster --generation 0 \
    --pod-uid image-smoke-pod --identity-file /tmp/identity --termination-message /tmp/termination
test -s /tmp/identity
test -s /tmp/termination
printf 'nonroot runtime: operator CLI startup and ha-init identity preparation passed\n'
SMOKE
printf 'verified linux/%s: two ELF64 binaries; image metadata revision %s, version %s\n' "$arch" "$revision" "$version"
