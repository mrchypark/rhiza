#!/bin/sh
# Exercise the real host-side smoke checks with a Docker fake, never a daemon.
set -eu
root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
smoke=${1:-"$root/scripts/test-operator-image.sh"}
work=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-operator-image-test.XXXXXX")
trap 'rm -r "$work"' EXIT HUP INT TERM
mkdir "$work/bin"
cat > "$work/bin/timeout" <<'TIMEOUT'
#!/bin/sh
shift
exec "$@"
TIMEOUT
cat > "$work/bin/docker" <<'DOCKER'
#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$MOCK_ROOT/calls"
case "$1:$2" in
    image:inspect)
        arch=$MOCK_ARCH
        [ "$MOCK_CASE" != platform ] || arch=unsupported
        user=65532:65532 revision=$MOCK_REVISION version=0.22.1
        [ "$MOCK_CASE" != user ] || user=0:0
        [ "$MOCK_CASE" != revision ] || revision=wrong
        [ "$MOCK_CASE" != version ] || version=wrong
        printf '[{"Os":"linux","Architecture":"%s","Config":{"User":"%s","Labels":{"org.opencontainers.image.revision":"%s","org.opencontainers.image.version":"%s"}}}]\n' "$arch" "$user" "$revision" "$version"
        ;;
    create:*)
        previous= name=
        for value do
            [ "$previous" != --name ] || name=$value
            previous=$value
        done
        case "$name" in
            *-files-*) id=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ;;
            *-runtime-*) id=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb ;;
            *) exit 99 ;;
        esac
        case "$MOCK_CASE:$name" in
            collision-files:*-files-*|collision-runtime:*-runtime-*)
                id=cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
                touch "$MOCK_ROOT/container-$id"
                printf '%s\n' "$id" > "$MOCK_ROOT/name-$name"
                printf '%s\n' "$id" # A failed response must not establish ownership.
                exit 23
                ;;
        esac
        touch "$MOCK_ROOT/container-$id"
        printf '%s\n' "$id" > "$MOCK_ROOT/name-$name"
        printf '%s\n' "$id"
        ;;
    cp:*)
        case "$2" in
            */rhiza-ha-init) [ "$MOCK_CASE" != missing ] || exit 1 ;;
        esac
        machine='\076'
        [ "$MOCK_ARCH" != arm64 ] || machine='\267'
        [ "$MOCK_CASE" != elf ] || machine='\003'
        # A real ELF header fixture, checked by the actual od logic; not executable.
        printf '\177ELF\002\001\001\000\000\000\000\000\000\000\000\000\002\000' > "$3"
        printf '%b\000' "$machine" >> "$3"
        ;;
    start:*)
        cat > "$MOCK_ROOT/runtime.sh"
        [ "$MOCK_CASE" != runtime ] || exit 17
        ;;
    ps:-aq)
        for filter do :; done
        case "$filter" in
            id=*) id=${filter#id=} ;;
            *) exit 99 ;;
        esac
        [ ! -f "$MOCK_ROOT/container-$id" ] || printf '%s\n' "$id"
        ;;
    rm:-f)
        id=$3
        [ "${#id}" -eq 64 ] || exit 99
        rm "$MOCK_ROOT/container-$id"
        ;;
    *) echo 'unexpected Docker fake command' >&2; exit 99 ;;
esac
DOCKER
chmod +x "$work/bin/docker" "$work/bin/timeout"
revision=0123456789abcdef0123456789abcdef01234567
for arch in amd64 arm64; do
    for scenario in success platform user revision version elf missing runtime collision-files collision-runtime; do
        run="$work/$arch-$scenario"
        mkdir "$run"
        if PATH="$work/bin:$PATH" MOCK_ROOT="$run" MOCK_ARCH="$arch" MOCK_CASE="$scenario" MOCK_REVISION="$revision" \
            sh "$smoke" fake:image "$arch" "$revision" 0.22.1 > "$run/output" 2>&1; then
            status=0
        else
            status=$?
        fi
        case "$scenario" in
            collision-*)
                [ "$status" -eq 23 ] || { echo 'create failure exit was not preserved' >&2; exit 1; }
                foreign=cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
                [ -f "$run/container-$foreign" ] || { echo 'pre-existing container was deleted' >&2; exit 1; }
                rm "$run/container-$foreign"
                ;;
        esac
        if [ "$scenario" = success ]; then
            [ "$status" -eq 0 ] || { cat "$run/output"; exit 1; }
            grep -q 'rhiza-operator --help' "$run/runtime.sh"
            if grep -q 'rhiza-fencer' "$run/runtime.sh" "$run/calls"; then
                echo 'normal operator smoke still requires a fencer' >&2; exit 1
            fi
            grep -q 'rhiza-ha-init prepare-identity' "$run/runtime.sh"
            grep -q -- '--network none --read-only' "$run/calls"
        else
            [ "$status" -ne 0 ] || { echo "unexpected pass: $arch/$scenario" >&2; exit 1; }
            if [ "$scenario" = runtime ]; then
                [ "$status" -eq 17 ] || { echo 'runtime exit was not preserved' >&2; exit 1; }
            fi
        fi
        for remaining in "$run"/container-*; do
            [ ! -e "$remaining" ] || { echo "container leaked: $arch/$scenario" >&2; exit 1; }
        done
    done
done
# Run the index guard from the actual workflow, not a copied predicate.
awk '
  $0 == "          jq -e \047" { capture=1; next }
  capture && $0 == "          \047 combined-index.json" { found++; exit }
  capture { sub(/^            /, ""); print }
  END { if (found != 1) exit 1 }
' "$root/.github/workflows/operator-release.yml" > "$work/index-check.jq"
jq -n '{manifests:[
  {digest:"sha256:a",platform:{os:"linux",architecture:"amd64"}},
  {digest:"sha256:b",platform:{os:"linux",architecture:"arm64"}},
  {platform:{os:"unknown",architecture:"unknown"},annotations:{"vnd.docker.reference.type":"attestation-manifest","vnd.docker.reference.digest":"sha256:a"}},
  {platform:{os:"unknown",architecture:"unknown"},annotations:{"vnd.docker.reference.type":"attestation-manifest","vnd.docker.reference.digest":"sha256:b"}}
]}' > "$work/index.json"
jq -e -f "$work/index-check.jq" "$work/index.json" >/dev/null
for mutation in missing wrong-subject fake-architecture duplicate-architecture; do
    case "$mutation" in
        missing) filter='.manifests |= .[0:3]' ;;
        wrong-subject) filter='.manifests[3].annotations["vnd.docker.reference.digest"] = "sha256:wrong"' ;;
        fake-architecture) filter='.manifests[3].platform = {os:"linux",architecture:"arm64"}' ;;
        duplicate-architecture) filter='.manifests[1].platform.architecture = "amd64"' ;;
    esac
    jq "$filter" "$work/index.json" > "$work/invalid-index.json"
    if jq -e -f "$work/index-check.jq" "$work/invalid-index.json" >/dev/null; then
        echo "invalid attestation index accepted: $mutation" >&2
        exit 1
    fi
done
printf 'operator image smoke regression passed: both architectures, seven rejection paths, two preserved name collisions and four attestation-index rejections\n'
