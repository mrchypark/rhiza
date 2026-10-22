#!/bin/sh
# Rebuild sdk/rust/native from the production Go sources required by cmd/rhiza-ffi,
# and vendor the Go module closure into it so a build of the packaged crate
# resolves every module from the crate instead of the network.
# Run from any directory; the output deliberately excludes Go tests and benchmarks.
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
sdk_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)
repo_dir=$(CDPATH= cd -- "$sdk_dir/../.." && pwd)
native_dir="$sdk_dir/native"
temporary_dir="$sdk_dir/.native.tmp.$$"

cleanup() { rm -rf "$temporary_dir"; }
trap cleanup EXIT HUP INT TERM

mkdir -p "$temporary_dir"
cp "$repo_dir/go.mod" "$repo_dir/go.sum" "$temporary_dir/"

# Ship only production packages in cmd/rhiza-ffi's actual local import graph
# across every OS/architecture supported by build.rs.
# Copying every package under pkg/ bloats the published crate with unrelated
# production code even though Go never compiles it for the FFI. Copy each
# package's direct non-test files (not only .go files) so direct cgo inputs
# remain available. The current FFI import graph has no nested or go:embed
# assets; extend this copy step if that changes. go list is metadata discovery;
# it does not build or run tests.
package_list="$temporary_dir/.package-dirs"
for target in darwin/amd64 darwin/arm64 linux/amd64 linux/arm64; do
    target_os=${target%/*}
    target_arch=${target#*/}
    if ! (cd "$repo_dir" && GOOS="$target_os" GOARCH="$target_arch" CGO_ENABLED=1 GOWORK=off go list -deps -f '{{if and .Module (eq .Module.Path "github.com/mrchypark/rhiza")}}{{.Dir}}{{end}}' ./cmd/rhiza-ffi) >> "$package_list"; then
        echo "FAIL unable to resolve the FFI package graph for $target" >&2
        exit 1
    fi
done
sort -u "$package_list" > "$package_list.sorted"
mv "$package_list.sorted" "$package_list"
if ! grep -F -x "$repo_dir/cmd/rhiza-ffi" "$package_list" >/dev/null; then
    echo "FAIL resolved package graph omits cmd/rhiza-ffi" >&2
    exit 1
fi
while IFS= read -r package_dir; do
    [ -n "$package_dir" ] || continue
    case "$package_dir" in
        "$repo_dir") relative=.;;
        "$repo_dir"/*) relative=${package_dir#"$repo_dir"/};;
        *) echo "FAIL local FFI dependency is outside the repository: $package_dir" >&2; exit 1;;
    esac
    for source in "$package_dir"/*; do
        [ -f "$source" ] || continue
        case "$source" in *_test.go) continue;; esac
        target="$temporary_dir/$relative/${source##*/}"
        mkdir -p "$(dirname "$target")"
        cp "$source" "$target"
    done
done < "$package_list"
rm -f "$package_list"

hash_files() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$@"
    else
        shasum -a 256 "$@"
    fi
}

before=$(hash_files "$temporary_dir/go.mod" "$temporary_dir/go.sum")
(cd "$temporary_dir" && GOWORK=off go mod vendor)
# Markdown is compiler-unread here: this generated native tree has no
# go:embed directive targeting Markdown. Preserve legal/notice Markdown names;
# keep any Markdown in a package containing go:embed conservatively rather than
# attempting to parse embed patterns.
NATIVE_VENDOR=$temporary_dir/vendor
export NATIVE_VENDOR
find "$temporary_dir/vendor" -type f -exec sh -eu -c '
    for source do
        base=${source##*/}
        case "$base" in
            *.[mM][dD]) ;;
            *) continue ;;
        esac
        lower=$(printf "%s" "$base" | tr "[:upper:]" "[:lower:]")
        case "$lower" in
            *license*|*notice*|*copying*|*copyright*) continue ;;
        esac
        dir=$(dirname "$source")
        keep=0
        while [ "$dir" != "$NATIVE_VENDOR" ] && [ "$dir" != "." ]; do
            for go_source in "$dir"/*.go; do
                [ -f "$go_source" ] || continue
                if grep -q "go:embed" "$go_source"; then
                    keep=1
                    break 2
                else
                    grep_status=$?
                    case "$grep_status" in
                        1) ;;
                        *)
                            echo "FAIL unable to inspect embed markers in $go_source" >&2
                            exit "$grep_status"
                            ;;
                    esac
                fi
            done
            parent=$(dirname "$dir")
            [ "$parent" = "$dir" ] && break
            dir=$parent
        done
        if [ "$keep" -eq 0 ]; then
            if rm -f "$source"; then
                :
            else
                rm_status=$?
                echo "FAIL unable to remove compiler-unread Markdown $source" >&2
                exit "$rm_status"
            fi
        fi
    done
' sh {} +
after=$(hash_files "$temporary_dir/go.mod" "$temporary_dir/go.sum")
if [ "$before" != "$after" ]; then
    echo "FAIL go.mod or go.sum changed while vendoring; run go mod tidy on the module and retry" >&2
    exit 1
fi

# Replace the previous generated snapshot only after source selection,
# vendoring, pruning, and module-integrity checks all succeed.
rm -rf "$native_dir"
mv "$temporary_dir" "$native_dir"
trap - EXIT HUP INT TERM
