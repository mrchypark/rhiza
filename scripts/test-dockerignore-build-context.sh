#!/bin/sh
# Checks the real Docker build context for the `COPY . .` steps: host build
# output must be pruned, sources must survive. FROM scratch + tar export keeps
# this pull-free and tag-free (no image is created).
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
work=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-dockerignore.XXXXXX")
trap 'rm -rf -- "$work"' EXIT INT TERM

command -v docker >/dev/null 2>&1 || { echo "docker CLI not found" >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "docker daemon unavailable" >&2; exit 1; }
docker buildx version >/dev/null 2>&1 || { echo "docker buildx unavailable" >&2; exit 1; }

ctx=$work/context
mkdir -p "$ctx/nested/deep" "$ctx/sdk/rust/src" "$ctx/sdk/rust/target/debug" \
  "$ctx/sdk/rust/native" "$ctx/.git" "$ctx/cmd/rhiza" "$ctx/cmd/rhiza-operator" \
  "$ctx/pkg/node" "$ctx/internal/objstore"
cp "$repo_root/.dockerignore" "$ctx/.dockerignore"
for fixture in \
  node.test data.db rhiza .git/config \
  nested/nested.test nested/deep/x.test keep.go \
  pkg/node/node.test pkg/node/append_only_test.go \
  internal/objstore/local_owner.go \
  go.mod go.sum Dockerfile \
  cmd/rhiza/main.go cmd/rhiza-operator/main.go \
  sdk/rust/Cargo.toml sdk/rust/build.rs sdk/rust/src/lib.rs \
  sdk/rust/target/debug/blob sdk/rust/native/libfoo.a sdk/rust/native_notes.txt; do
  : > "$ctx/$fixture"
done

cat > "$work/Dockerfile" <<'EOF'
FROM scratch
COPY . /ctx
EOF

docker buildx build --no-cache --file "$work/Dockerfile" \
  --output "type=tar,dest=$work/context.tar" "$ctx"
tar -tf "$work/context.tar" | sed 's|^ctx/||' > "$work/files"

bad=0
expect_absent() {
  if grep -qxF -- "$1" "$work/files"; then
    echo "unexpected in build context: $1" >&2
    bad=$((bad + 1))
  fi
}
expect_present() {
  if ! grep -qxF -- "$1" "$work/files"; then
    echo "missing from build context: $1" >&2
    bad=$((bad + 1))
  fi
}

expect_absent node.test
expect_absent data.db
expect_absent rhiza
expect_absent .git/config
expect_absent sdk/rust/target/debug/blob
expect_absent sdk/rust/native/libfoo.a

# Docker's `*.test` is anchored to the whole relative path, so nested binaries
# are reported only; a future rule that prunes them is fine either way.
for nested in nested/nested.test nested/deep/x.test pkg/node/node.test; do
  if grep -qxF -- "$nested" "$work/files"; then
    echo "note: nested test binary currently in build context: $nested"
  else
    echo "note: nested test binary excluded from build context: $nested"
  fi
done

expect_present go.mod
expect_present go.sum
expect_present Dockerfile
expect_present keep.go
expect_present cmd/rhiza/main.go
expect_present cmd/rhiza-operator/main.go
expect_present sdk/rust/Cargo.toml
expect_present sdk/rust/build.rs
expect_present sdk/rust/src/lib.rs
expect_present pkg/node/append_only_test.go
expect_present internal/objstore/local_owner.go
# `/sdk/rust/native` is a directory rule, not a prefix rule.
expect_present sdk/rust/native_notes.txt

if [ "$bad" -ne 0 ]; then
  echo "dockerignore build-context regression: $bad check(s) failed" >&2
  exit 1
fi
echo "dockerignore build-context checks passed"
