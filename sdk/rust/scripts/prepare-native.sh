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
cp "$repo_dir/go.mod" "$repo_dir/go.sum" "$repo_dir/rhiza.go" "$repo_dir/replica.go" "$repo_dir/config_env.go" "$repo_dir/operator_handler.go" "$repo_dir/membership.go" "$temporary_dir/"

(
    cd "$repo_dir"
    export NATIVE_OUTPUT="$temporary_dir"
    # These two directories are used only by repository Go tests. The crate
    # builds cmd/rhiza-ffi; it does not run the repository qualification suite.
    find cmd/rhiza-ffi internal pkg -type f ! -name '*_test.go' \
        ! -path 'pkg/materializer/testdata/*' \
        ! -path 'internal/objstore/versityfixture/*' -exec sh -c '
        for source do
            # Retain all ordinary/platform sources and disabled counterparts.
            # Only the explicit repository qualification tag is excluded.
            first=
            IFS= read -r first < "$source" || :
            [ "$first" != "//go:build rhiza_local_testhooks" ] || continue
            target="$NATIVE_OUTPUT/$source"
            mkdir -p "$(dirname "$target")"
            cp "$source" "$target"
        done
    ' sh {} +
)

rm -rf "$native_dir"
mv "$temporary_dir" "$native_dir"
trap - EXIT HUP INT TERM

hash_files() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$@"
    else
        shasum -a 256 "$@"
    fi
}

before=$(hash_files "$native_dir/go.mod" "$native_dir/go.sum")
(cd "$native_dir" && GOWORK=off go mod vendor)
# Markdown is compiler-unread here: this generated native tree has no
# go:embed directive targeting Markdown. Preserve legal/notice Markdown names;
# keep any Markdown in a package containing go:embed conservatively rather than
# attempting to parse embed patterns. x/sys/unix/mkerrors.sh only regenerates
# the already bundled syscall Go tables; native compilation never runs it.
# Apply the same conservative embed preservation to that exact generator.
# BEGIN native-vendor compiler-unread input filter
NATIVE_VENDOR=$native_dir/vendor
export NATIVE_VENDOR
find "$native_dir/vendor" -type f -exec sh -eu -c '
    for source do
        # These pinned dependency doc.go files contain package prose plus a
        # package clause, with legal/build-tag preambles where applicable.
        # Retain the complete legal/build-tag preamble and package clause while omitting
        # only the documentation body. Unknown dependency contents are kept.
        expected_hash=
        expected_module_version=
        legal_header_lines=0
        package_line=0
        extra_lines=
        case "$source" in
            "$NATIVE_VENDOR/cloud.google.com/go/storage/doc.go")
                expected_hash=8b52b8b3c1f21aabacdd47b9bcd405d47f48503634d08e9e9ad184b8155145c5
                expected_module_version="cloud.google.com/go/storage v1.56.0"
                legal_header_lines=13; package_line=429 ;;
            "$NATIVE_VENDOR/github.com/Azure/azure-sdk-for-go/sdk/azcore/doc.go")
                expected_hash=cd41f51ee7acac26bf380f204b570980bd1537bf67bd091e3e1376e8f12e71f2
                expected_module_version="github.com/Azure/azure-sdk-for-go/sdk/azcore v1.18.1"
                legal_header_lines=6; package_line=264 ;;
            "$NATIVE_VENDOR/github.com/prometheus/client_golang/prometheus/doc.go")
                expected_hash=6e120e8402699d5f622c2b548ce23fba0b4b7d11eb95c15319ac83533a93e03f
                expected_module_version="github.com/prometheus/client_golang v1.17.0"
                legal_header_lines=12; package_line=210 ;;
            "$NATIVE_VENDOR/go.opentelemetry.io/otel/metric/doc.go")
                expected_hash=5f057f47f648b8641ed912d24b5342098a2dad58fc94c4952e6e8ed6a83f9263
                expected_module_version="go.opentelemetry.io/otel/metric v1.45.0"
                legal_header_lines=2; package_line=204 ;;
            "$NATIVE_VENDOR/github.com/aws/aws-sdk-go-v2/aws/doc.go")
                expected_hash=94825054f9598903f778e5d737f7d9a6c045ebd124b4d8443be188b8c4c4fe1d
                expected_module_version="github.com/aws/aws-sdk-go-v2 v1.30.4"
                package_line=56; extra_lines="60 61 62" ;;
            "$NATIVE_VENDOR/github.com/go-kit/log/doc.go")
                expected_hash=96dde80c6fd2fa551850526754a92cb94d9dfb83009a5f8cf96528a9af22c535
                expected_module_version="github.com/go-kit/log v0.2.1"
                package_line=116 ;;
            "$NATIVE_VENDOR/golang.org/x/net/ipv4/doc.go")
                expected_hash=fd086c081fb6e9cd14ce0358084337126f1586993d3daf866a23d07b7128f7d2
                expected_module_version="golang.org/x/net v0.60.0"
                legal_header_lines=4; package_line=238 ;;
            "$NATIVE_VENDOR/golang.org/x/net/ipv6/doc.go")
                expected_hash=9f809bba900ce3cd5fea64fc8f8b3bf98f198df0333788ba2fd85502ba367790
                expected_module_version="golang.org/x/net v0.60.0"
                legal_header_lines=4; package_line=237 ;;
            "$NATIVE_VENDOR/go.opentelemetry.io/otel/trace/doc.go")
                expected_hash=1e8208f56ff8741291081bffa1ed81bb252fac4b4602440b70873a0b65dadc19
                expected_module_version="go.opentelemetry.io/otel/trace v1.45.0"
                legal_header_lines=2; package_line=119 ;;
            "$NATIVE_VENDOR/github.com/aws/aws-sdk-go-v2/aws/retry/doc.go")
                expected_hash=72b7b3a08eba1ee553cb3c0db395bfc89f746663297a2521dc12f736f179d3d5
                expected_module_version="github.com/aws/aws-sdk-go-v2 v1.30.4"
                package_line=80 ;;
            "$NATIVE_VENDOR/go.opentelemetry.io/otel/sdk/metric/doc.go")
                expected_hash=a3a52c8d7f3dc66829a4a8cef945728c543f477bc66d41a1e15c342509049c68
                expected_module_version="go.opentelemetry.io/otel/sdk/metric v1.45.0"
                legal_header_lines=2; package_line=89 ;;
        esac
        if [ -n "$expected_hash" ]; then
            [ -f "$NATIVE_VENDOR/modules.txt" ] || continue
            grep -Fqx "# $expected_module_version" "$NATIVE_VENDOR/modules.txt" || continue
            if command -v sha256sum >/dev/null 2>&1; then
                actual_hash=$(sha256sum "$source" | awk "{print \$1}")
            else
                actual_hash=$(shasum -a 256 "$source" | awk "{print \$1}")
            fi
            [ "$actual_hash" = "$expected_hash" ] || continue
            trimmed="$source.package-doc.$$"
            if [ "$legal_header_lines" -gt 0 ]; then
                sed -n "1,${legal_header_lines}p" "$source" > "$trimmed"
            else
                : > "$trimmed"
            fi
            sed -n "${package_line}p" "$source" >> "$trimmed"
            for extra_line in $extra_lines; do
                sed -n "${extra_line}p" "$source" >> "$trimmed"
            done
            if ! grep -q "^package " "$trimmed"; then
                echo "FAIL pinned package documentation has no retained package clause: $source" >&2
                exit 1
            fi
            mv "$trimmed" "$source"
            continue
        fi
        base=${source##*/}
        case "$source" in
            "$NATIVE_VENDOR/golang.org/x/sys/unix/mkerrors.sh") ;;
            *)
                case "$base" in
                    *.[mM][dD]) ;;
                    *) continue ;;
                esac
                ;;
        esac
        lower=$(printf "%s" "$base" | tr "[:upper:]" "[:lower:]")
        case "$lower" in
            *license*|*legal*|*notice*|*copying*|*copyright*) continue ;;
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
# END native-vendor compiler-unread input filter
after=$(hash_files "$native_dir/go.mod" "$native_dir/go.sum")
if [ "$before" != "$after" ]; then
    echo "FAIL go.mod or go.sum changed while vendoring; run go mod tidy on the module and retry" >&2
    exit 1
fi
