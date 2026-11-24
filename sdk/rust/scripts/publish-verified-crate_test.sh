#!/bin/sh
set -eu
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-publish-guard-test.XXXXXX")
trap 'rm -rf "$scratch"' EXIT HUP INT TERM
mkdir -p "$scratch/repo/sdk/rust/scripts" "$scratch/bin" "$scratch/tmp"
scripts="$scratch/repo/sdk/rust/scripts"
cp "$script_dir/publish-verified-crate.sh" "$scripts/"
cat > "$scripts/ci-package-artifact.sh" <<'SH'
#!/bin/sh
set -eu
[ "$1" = fixturecommit ]
mkdir -p "$2/verified"
printf 'verified complete native snapshot\n' > "$2/verified/rhizadb-1.2.3.crate"
SH
cat > "$scripts/stage-crate.sh" <<'SH'
#!/bin/sh
set -eu
[ "$1" = fixturecommit ]
stage=$(mktemp -d "$TMPDIR/rhiza-crate-stage.XXXXXX")
printf '%s\n' "$stage" > "$RHIZA_CRATE_STATE_DIR/stage"
mkdir -p "$stage/target/package"
printf '[package]\nversion = "1.2.3"\n' > "$stage/Cargo.toml"
if [ "${MOCK_MISMATCH:-0}" = 1 ]; then printf 'incomplete vendor\n'; else printf 'verified complete native snapshot\n'; fi > "$stage/target/package/rhizadb-1.2.3.crate"
printf '%s\n' "$stage"
SH
cat > "$scratch/bin/git" <<'SH'
#!/bin/sh
printf 'fixturecommit\n'
SH
cat > "$scratch/bin/cargo" <<'SH'
#!/bin/sh
printf '%s\n' "$*" >> "$MOCK_CARGO_LOG"
[ "$1" = publish ] && [ "$2" = --manifest-path ] && [ "$4" = --locked ]
case "$3" in "$TMPDIR"/rhiza-crate-stage.*/Cargo.toml) exit 0;; *) exit 3;; esac
SH
cat > "$scratch/bin/curl" <<'SH'
#!/bin/sh
set -eu
for arg do last=$arg; done
if [ "${MOCK_REGISTRY_MISMATCH:-0}" = 1 ]; then printf 'different\n'; else printf 'verified complete native snapshot\n'; fi > "$last"
SH
chmod +x "$scratch/bin/git" "$scratch/bin/cargo" "$scratch/bin/curl"
PATH="$scratch/bin:$PATH" TMPDIR="$scratch/tmp" MOCK_CARGO_LOG="$scratch/cargo.log"
export PATH TMPDIR MOCK_CARGO_LOG
unset DOCS_RS RHIZA_NATIVE_LIB_DIR
sh "$scripts/publish-verified-crate.sh" ref "$scratch/good" > "$scratch/good.log" 2>&1
grep -F 'PASS published registry bytes match' "$scratch/good.log" >/dev/null
[ "$(wc -l < "$MOCK_CARGO_LOG" | tr -d ' ')" = 1 ]
if MOCK_MISMATCH=1 sh "$scripts/publish-verified-crate.sh" ref "$scratch/mismatch" > "$scratch/mismatch.log" 2>&1; then
    echo 'FAIL incomplete staged vendor accepted' >&2; exit 1
fi
grep -F 'staged package differs' "$scratch/mismatch.log" >/dev/null
[ "$(wc -l < "$MOCK_CARGO_LOG" | tr -d ' ')" = 1 ]
if MOCK_REGISTRY_MISMATCH=1 sh "$scripts/publish-verified-crate.sh" ref "$scratch/registry-mismatch" > "$scratch/registry-mismatch.log" 2>&1; then
    echo 'FAIL mismatched registry bytes accepted' >&2; exit 1
fi
grep -F 'registry bytes differ' "$scratch/registry-mismatch.log" >/dev/null
[ -z "$(find "$TMPDIR" -mindepth 1 -print)" ]
echo 'PASS publish uses verified stage, rejects incomplete vendor and checks registry bytes'
