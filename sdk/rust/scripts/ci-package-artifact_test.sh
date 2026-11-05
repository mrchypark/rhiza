#!/bin/sh
# Contract tests only: real Git/tar/hash, mock Cargo/Go (no compilation).
set -eu
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
REAL_GIT=$(command -v git)
REAL_RM=$(command -v rm)
export REAL_GIT REAL_RM
test_root=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-package-test.XXXXXX")
trap 'rm -rf "$test_root"' EXIT HUP INT TERM
mkdir -p "$test_root/repo/sdk/rust/scripts" "$test_root/bin" "$test_root/tmp"
fixture="$test_root/repo"
cp "$script_dir/ci-package-artifact.sh" "$script_dir/stage-crate.sh" "$fixture/sdk/rust/scripts/"
mkdir -p "$fixture/sdk/rust/src" "$fixture/cmd/rhiza-ffi" "$fixture/pkg/data"
printf 'module example.test/fixture\n' > "$fixture/go.mod"
printf 'locked dependencies\n' > "$fixture/go.sum"
printf 'package rhiza\n' > "$fixture/rhiza.go"
printf 'package main\n' > "$fixture/cmd/rhiza-ffi/main.go"
printf 'embedded bytes\000\377\n' > "$fixture/pkg/data/payload.bin"
printf '[package]\nname = "rhizadb"\nversion = "1.2.3"\n' > "$fixture/sdk/rust/Cargo.toml"
printf 'lock\n' > "$fixture/sdk/rust/Cargo.lock"
printf 'pub fn fixture() {}\n' > "$fixture/sdk/rust/src/lib.rs"
cat > "$fixture/sdk/rust/scripts/prepare-native.sh" <<'SH'
#!/bin/sh
set -eu
sdk=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
repo=$(CDPATH= cd -- "$sdk/../.." && pwd)
mkdir -p "$sdk/native/vendor/example.test" "$sdk/native/cmd" "$sdk/native/pkg"
mkdir -p "$sdk/native/vendor/example.test/empty-after-filter"
cp "$repo/go.mod" "$repo/go.sum" "$repo/rhiza.go" "$sdk/native/"
cp -R "$repo/cmd/rhiza-ffi" "$sdk/native/cmd/"
cp -R "$repo/pkg/data" "$sdk/native/pkg/"
printf 'vendor modules\n' > "$sdk/native/vendor/modules.txt"
printf 'vendor embedded bytes\n' > "$sdk/native/vendor/example.test/data.bin"
SH
cat > "$test_root/bin/cargo" <<'SH'
#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$MOCK_CARGO_LOG"
case "$1" in
    --version) echo 'cargo mock'; exit 0;;
    package)
        [ "$#" -eq 4 ] && [ "$2" = --manifest-path ] && [ "$4" = --locked ]
        [ "${MOCK_PACKAGE_FAIL:-0}" -eq 0 ] || exit 17
        stage=${3%/Cargo.toml}
        [ ! -e "$stage/../../go.mod" ]
        mkdir -p "$stage/target/package/rhizadb-1.2.3"
        package="$stage/target/package/rhizadb-1.2.3"
        (cd "$stage" && tar -cf - --exclude=./target .) | tar -xf - -C "$package"
        # Cargo packages file members, not empty source directories.
        rmdir "$package/native/vendor/example.test/empty-after-filter"
        cp "$package/Cargo.toml" "$package/Cargo.toml.orig"
        printf '# Cargo normalization\n' >> "$package/Cargo.toml"
        if [ "${MOCK_CORRUPT:-0}" -eq 1 ]; then echo corrupt >> "$package/native/pkg/data/payload.bin"; fi
        if [ "${MOCK_UNEXPECTED_FILE:-0}" -eq 1 ]; then echo unexpected > "$package/native/vendor/example.test/unexpected.bin"; fi
        if [ "${MOCK_OMIT_NONEMPTY:-0}" -eq 1 ]; then rm -rf "$package/native/pkg/data"; fi
        if [ "${MOCK_CHANGE_TYPE:-0}" -eq 1 ]; then
            rm "$package/native/pkg/data/payload.bin"
            ln -s missing "$package/native/pkg/data/payload.bin"
        fi
        if [ "${MOCK_CHANGE_MODE:-0}" -eq 1 ]; then chmod +x "$package/native/pkg/data/payload.bin"; fi
        (cd "$stage/target/package" && tar -czf rhizadb-1.2.3.crate rhizadb-1.2.3)
        ;;
    test)
        [ "$#" -eq 5 ] && [ "$2" = --manifest-path ] && [ "$4" = --locked ] && [ "$5" = --all-targets ]
        [ -f "${3%/Cargo.toml}/native/vendor/modules.txt" ]
        [ ! -e "${3%/Cargo.toml}/../../go.mod" ]
        [ "${MOCK_TEST_FAIL:-0}" -eq 0 ] || exit 23
        ;;
    *) exit 99;;
esac
SH
cat > "$test_root/bin/go" <<'SH'
#!/bin/sh
[ "$*" = version ] || exit 98
echo 'go version mock'
SH
cat > "$test_root/bin/git" <<'SH'
#!/bin/sh
case "$*" in
    *'worktree list --porcelain')
        if [ "${MOCK_GIT_LIST_FAIL:-0}" -eq 1 ]; then echo 'mock Git inventory unavailable' >&2; exit 31; fi;;
esac
exec "$REAL_GIT" "$@"
SH
cat > "$test_root/bin/rm" <<'SH'
#!/bin/sh
case "${2:-}" in
    */rhiza-ci-package.*)
        if [ "${MOCK_STATE_REMOVE_FAIL:-0}" -eq 1 ]; then echo 'mock owned-state removal refused' >&2; exit 32; fi;;
esac
exec "$REAL_RM" "$@"
SH
chmod +x "$test_root/bin/cargo" "$test_root/bin/go" "$test_root/bin/git" "$test_root/bin/rm"
git -C "$fixture" init -q
git -C "$fixture" add .
git -C "$fixture" -c user.name=Fixture -c user.email=fixture@example.invalid commit -qm fixture
commit=$(git -C "$fixture" rev-parse HEAD)
PATH="$test_root/bin:$PATH"
TMPDIR="$test_root/tmp"
MOCK_CARGO_LOG="$test_root/cargo.log"
export PATH TMPDIR MOCK_CARGO_LOG
unset DOCS_RS RHIZA_NATIVE_LIB_DIR
run_case() {
    label=$1 expected=$2
    : > "$MOCK_CARGO_LOG"
    if sh "$fixture/sdk/rust/scripts/ci-package-artifact.sh" "$commit" "$test_root/$label" > "$test_root/$label.log" 2>&1; then actual=0; else actual=$?; fi
    [ "$actual" -eq "$expected" ] || { cat "$test_root/$label.log"; echo "FAIL $label exit $actual expected $expected"; exit 1; }
    [ "$("$REAL_GIT" -C "$fixture" worktree list --porcelain | grep -c '^worktree ')" -eq 1 ]
    if [ "${MOCK_STATE_REMOVE_FAIL:-0}" -eq 1 ]; then
        [ -n "$(find "$TMPDIR" -name 'rhiza-ci-package.*' -type d -print)" ]
        grep -q 'retained cleanup state:' "$test_root/$label.log"
        grep -q '^cleanup_failed=1$' "$test_root/$label/diagnostics/cleanup.txt"
        # The fixture deliberately refused deletion. Only the fixture owner
        # removes these retained resources after checking the diagnostic.
        find "$TMPDIR" -name 'rhiza-ci-package.*' -type d -prune -exec "$REAL_RM" -rf {} +
    fi
    [ -z "$(find "$TMPDIR" -mindepth 1 -print)" ]
    if [ "$expected" -ne 0 ]; then [ ! -d "$test_root/$label/verified" ]; fi
    printf 'PASS %s actual_exit=%s owned_worktrees=0 owned_stages=0\n' "$label" "$actual"
}
run_case success 0
grep -q 'native/pkg/data/payload.bin' "$test_root/success/verified/native.sha256"
grep -q 'native/vendor/example.test/data.bin' "$test_root/success/verified/native.sha256"
grep -q '^commit=' "$test_root/success/verified/provenance.txt"
[ "$(grep -c '^package ' "$MOCK_CARGO_LOG")" -eq 1 ]
[ "$(grep -c '^test ' "$MOCK_CARGO_LOG")" -eq 1 ]
MOCK_CORRUPT=1; export MOCK_CORRUPT
run_case corrupted-embedded-data 1
grep -q '^phase=native_bytes_modes$' "$test_root/corrupted-embedded-data/diagnostics/failure.txt"
if grep -q '^test ' "$MOCK_CARGO_LOG"; then echo 'FAIL corrupt crate reached tests'; exit 1; fi
unset MOCK_CORRUPT
MOCK_UNEXPECTED_FILE=1; export MOCK_UNEXPECTED_FILE
run_case unexpected-native-file 1
grep -q '^phase=native_inventory$' "$test_root/unexpected-native-file/diagnostics/failure.txt"
unset MOCK_UNEXPECTED_FILE
MOCK_OMIT_NONEMPTY=1; export MOCK_OMIT_NONEMPTY
run_case omitted-nonempty-directory 1
grep -q '^phase=native_inventory$' "$test_root/omitted-nonempty-directory/diagnostics/failure.txt"
unset MOCK_OMIT_NONEMPTY
MOCK_CHANGE_TYPE=1; export MOCK_CHANGE_TYPE
run_case changed-native-type 1
grep -q '^phase=member_types$' "$test_root/changed-native-type/diagnostics/failure.txt"
unset MOCK_CHANGE_TYPE
MOCK_CHANGE_MODE=1; export MOCK_CHANGE_MODE
run_case changed-native-mode 1
grep -q '^phase=native_bytes_modes$' "$test_root/changed-native-mode/diagnostics/failure.txt"
unset MOCK_CHANGE_MODE
MOCK_PACKAGE_FAIL=1; export MOCK_PACKAGE_FAIL
run_case package-failure 17
unset MOCK_PACKAGE_FAIL
MOCK_TEST_FAIL=1; export MOCK_TEST_FAIL
run_case extracted-test-failure 23
unset MOCK_TEST_FAIL
MOCK_GIT_LIST_FAIL=1; export MOCK_GIT_LIST_FAIL
run_case inventory-failure 1
MOCK_PACKAGE_FAIL=1; export MOCK_PACKAGE_FAIL
run_case inventory-and-package-failure 17
unset MOCK_PACKAGE_FAIL MOCK_GIT_LIST_FAIL
MOCK_STATE_REMOVE_FAIL=1; export MOCK_STATE_REMOVE_FAIL
run_case state-cleanup-failure 1
MOCK_TEST_FAIL=1; export MOCK_TEST_FAIL
run_case cleanup-and-test-failure 23
unset MOCK_TEST_FAIL MOCK_STATE_REMOVE_FAIL
mkdir "$test_root/preexisting"
printf 'preserve\n' > "$test_root/preexisting/owner-marker"
run_case preexisting 1
[ "$(cat "$test_root/preexisting/owner-marker")" = preserve ]
DOCS_RS=1; export DOCS_RS
run_case bypass-refused 1
DOCS_RS=''; export DOCS_RS
run_case empty-docs-bypass-refused 1
unset DOCS_RS
RHIZA_NATIVE_LIB_DIR=''; export RHIZA_NATIVE_LIB_DIR
run_case empty-native-override-refused 1
echo 'PASS exact-source, complete manifest, bundled-native test contract, original failures and owned cleanup (mock Cargo/Go only)'
