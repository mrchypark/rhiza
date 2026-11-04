#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
prepare_script="$script_dir/prepare-native.sh"
scratch=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-native-filter.XXXXXX")
cleanup() { rm -rf "$scratch"; }
trap cleanup EXIT HUP INT TERM

cache_dir=$(go env GOMODCACHE)
filter_block="$scratch/filter-block.sh"
awk '
    /^# BEGIN native-vendor compiler-unread input filter$/ { active=1; next }
    /^# END native-vendor compiler-unread input filter$/ { active=0; exit }
    active { print }
' "$prepare_script" > "$filter_block"
[ -s "$filter_block" ] || { echo 'FAIL unable to extract actual prepare-native filter' >&2; exit 1; }
grep -Fq "! -path 'internal/foregroundcosttest/*'" "$prepare_script" || { echo 'FAIL test-only foreground cost package is staged into native snapshot' >&2; exit 1; }

run_filter() (
    native_dir=$1
    . "$filter_block"
)

add_candidate() {
    target="$matching/vendor/$1"
    source="$cache_dir/$2"
    [ -f "$source" ] || { echo "FAIL missing read-only cached source $source" >&2; exit 1; }
    mkdir -p "$(dirname "$target")"
    cp "$source" "$target"
}

assert_candidate() {
    target="$matching/vendor/$1"
    source="$cache_dir/$2"
    [ -f "$target" ] || { echo "FAIL pinned doc.go removed: $1" >&2; exit 1; }
    [ "$(wc -c < "$target" | tr -d ' ')" -lt "$(wc -c < "$source" | tr -d ' ')" ] || { echo "FAIL pinned doc.go not reduced: $1" >&2; exit 1; }
    grep -Fx "$3" "$target" >/dev/null || { echo "FAIL package clause not retained: $1" >&2; exit 1; }
    if [ -n "$4" ]; then
        grep -F "$4" "$target" >/dev/null || { echo "FAIL copyright/license preamble not retained: $1" >&2; exit 1; }
    fi
}

source_doc="$cache_dir/cloud.google.com/go/storage@v1.56.0/doc.go"
matching="$scratch/matching"
match_doc="$matching/vendor/cloud.google.com/go/storage/doc.go"
mkdir -p "$(dirname "$match_doc")" "$matching/vendor/docs" "$matching/vendor/embed" "$matching/vendor/golang.org/x/sys/unix"
printf '%s\n' \
    '# cloud.google.com/go/storage v1.56.0' \
    '# github.com/Azure/azure-sdk-for-go/sdk/azcore v1.18.1' \
    '# github.com/prometheus/client_golang v1.17.0' \
    '# go.opentelemetry.io/otel/metric v1.45.0' \
    '# github.com/aws/aws-sdk-go-v2 v1.30.4' \
    '# github.com/go-kit/log v0.2.1' \
    '# golang.org/x/net v0.60.0' \
    '# go.opentelemetry.io/otel/trace v1.45.0' \
    '# go.opentelemetry.io/otel/sdk/metric v1.45.0' > "$matching/vendor/modules.txt"
add_candidate cloud.google.com/go/storage/doc.go cloud.google.com/go/storage@v1.56.0/doc.go
add_candidate github.com/Azure/azure-sdk-for-go/sdk/azcore/doc.go github.com/\!azure/azure-sdk-for-go/sdk/azcore@v1.18.1/doc.go
add_candidate github.com/prometheus/client_golang/prometheus/doc.go github.com/prometheus/client_golang@v1.17.0/prometheus/doc.go
add_candidate go.opentelemetry.io/otel/metric/doc.go go.opentelemetry.io/otel/metric@v1.45.0/doc.go
add_candidate github.com/aws/aws-sdk-go-v2/aws/doc.go github.com/aws/aws-sdk-go-v2@v1.30.4/aws/doc.go
add_candidate github.com/go-kit/log/doc.go github.com/go-kit/log@v0.2.1/doc.go
add_candidate golang.org/x/net/ipv4/doc.go golang.org/x/net@v0.60.0/ipv4/doc.go
add_candidate golang.org/x/net/ipv6/doc.go golang.org/x/net@v0.60.0/ipv6/doc.go
add_candidate go.opentelemetry.io/otel/trace/doc.go go.opentelemetry.io/otel/trace@v1.45.0/doc.go
add_candidate github.com/aws/aws-sdk-go-v2/aws/retry/doc.go github.com/aws/aws-sdk-go-v2@v1.30.4/aws/retry/doc.go
add_candidate go.opentelemetry.io/otel/sdk/metric/doc.go go.opentelemetry.io/otel/sdk/metric@v1.45.0/doc.go
printf 'ordinary documentation\n' > "$matching/vendor/docs/README.md"
printf 'legal documentation\n' > "$matching/vendor/docs/legal.md"
printf 'license notice\n' > "$matching/vendor/docs/LICENSE.md"
printf 'package embedfixture\n//go:embed asset.md\nvar asset string\n' > "$matching/vendor/embed/embed.go"
printf 'embedded bytes\n' > "$matching/vendor/embed/asset.md"
printf '#!/bin/sh\n' > "$matching/vendor/golang.org/x/sys/unix/mkerrors.sh"
printf '#!/bin/sh\n' > "$matching/vendor/golang.org/x/sys/unix/mkerrors-neighbor.sh"
run_filter "$matching"

assert_candidate cloud.google.com/go/storage/doc.go cloud.google.com/go/storage@v1.56.0/doc.go 'package storage // import "cloud.google.com/go/storage"' 'Copyright 2016 Google LLC'
assert_candidate github.com/Azure/azure-sdk-for-go/sdk/azcore/doc.go github.com/\!azure/azure-sdk-for-go/sdk/azcore@v1.18.1/doc.go 'package azcore' 'Copyright 2017 Microsoft Corporation'
assert_candidate github.com/prometheus/client_golang/prometheus/doc.go github.com/prometheus/client_golang@v1.17.0/prometheus/doc.go 'package prometheus' 'Copyright 2014 The Prometheus Authors'
assert_candidate go.opentelemetry.io/otel/metric/doc.go go.opentelemetry.io/otel/metric@v1.45.0/doc.go 'package metric' 'SPDX-License-Identifier: Apache-2.0'
assert_candidate github.com/aws/aws-sdk-go-v2/aws/doc.go github.com/aws/aws-sdk-go-v2@v1.30.4/aws/doc.go 'package aws' ''
grep -Fx '//go:generate go run -tags codegen generate.go' "$matching/vendor/github.com/aws/aws-sdk-go-v2/aws/doc.go" >/dev/null || { echo 'FAIL go:generate directive was not retained' >&2; exit 1; }
grep -Fx '//go:generate go run -tags codegen logging_generate.go' "$matching/vendor/github.com/aws/aws-sdk-go-v2/aws/doc.go" >/dev/null || { echo 'FAIL second go:generate directive was not retained' >&2; exit 1; }
grep -Fx '//go:generate gofmt -w -s .' "$matching/vendor/github.com/aws/aws-sdk-go-v2/aws/doc.go" >/dev/null || { echo 'FAIL gofmt generation directive was not retained' >&2; exit 1; }
assert_candidate github.com/go-kit/log/doc.go github.com/go-kit/log@v0.2.1/doc.go 'package log' ''
assert_candidate golang.org/x/net/ipv4/doc.go golang.org/x/net@v0.60.0/ipv4/doc.go 'package ipv4 // import "golang.org/x/net/ipv4"' 'Copyright 2012 The Go Authors.'
assert_candidate golang.org/x/net/ipv6/doc.go golang.org/x/net@v0.60.0/ipv6/doc.go 'package ipv6 // import "golang.org/x/net/ipv6"' 'Copyright 2013 The Go Authors.'
assert_candidate go.opentelemetry.io/otel/trace/doc.go go.opentelemetry.io/otel/trace@v1.45.0/doc.go 'package trace' 'SPDX-License-Identifier: Apache-2.0'
assert_candidate github.com/aws/aws-sdk-go-v2/aws/retry/doc.go github.com/aws/aws-sdk-go-v2@v1.30.4/aws/retry/doc.go 'package retry' ''
assert_candidate go.opentelemetry.io/otel/sdk/metric/doc.go go.opentelemetry.io/otel/sdk/metric@v1.45.0/doc.go 'package metric' 'SPDX-License-Identifier: Apache-2.0'
if grep -F 'Package storage provides an easy way' "$match_doc" >/dev/null; then
    echo 'FAIL package documentation body remains' >&2
    exit 1
fi
[ ! -e "$matching/vendor/docs/README.md" ] || { echo 'FAIL ordinary Markdown was retained' >&2; exit 1; }
[ -f "$matching/vendor/docs/legal.md" ] && [ -f "$matching/vendor/docs/LICENSE.md" ] || { echo 'FAIL legal Markdown was removed' >&2; exit 1; }
[ -f "$matching/vendor/embed/asset.md" ] || { echo 'FAIL go:embed input was removed' >&2; exit 1; }
[ ! -e "$matching/vendor/golang.org/x/sys/unix/mkerrors.sh" ] || { echo 'FAIL exact generator was retained' >&2; exit 1; }
[ -f "$matching/vendor/golang.org/x/sys/unix/mkerrors-neighbor.sh" ] || { echo 'FAIL neighboring script was removed' >&2; exit 1; }

unknown="$scratch/unknown"
unknown_doc="$unknown/vendor/cloud.google.com/go/storage/doc.go"
mkdir -p "$(dirname "$unknown_doc")"
cp "$source_doc" "$unknown_doc"
cp "$matching/vendor/modules.txt" "$unknown/vendor/modules.txt"
chmod u+w "$unknown_doc"
printf '\n// unreviewed dependency contents\n' >> "$unknown_doc"
cp "$unknown_doc" "$scratch/unknown-before.go"
run_filter "$unknown"
cmp -s "$unknown_doc" "$scratch/unknown-before.go" || { echo 'FAIL unknown hash was modified' >&2; exit 1; }

upgraded="$scratch/upgraded"
upgraded_doc="$upgraded/vendor/cloud.google.com/go/storage/doc.go"
mkdir -p "$(dirname "$upgraded_doc")"
cp "$source_doc" "$upgraded_doc"
printf '%s\n' '# cloud.google.com/go/storage v1.56.1' > "$upgraded/vendor/modules.txt"
cp "$upgraded_doc" "$scratch/upgraded-before.go"
run_filter "$upgraded"
cmp -s "$upgraded_doc" "$scratch/upgraded-before.go" || { echo 'FAIL dependency-version change was modified' >&2; exit 1; }

rm -rf "$scratch"
trap - EXIT HUP INT TERM
[ ! -e "$scratch" ] || { echo 'FAIL fixture cleanup left temporary data' >&2; exit 1; }
echo 'PASS prepare-native hash filter, unknown-hash retention, legal/embed preservation, and cleanup'
