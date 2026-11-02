#!/bin/sh
# Verify one exact-source staged crate; no registry credentials or publication.
set -eu
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH='' cd -- "$script_dir/../../.." && pwd)
commit=$(git -C "$repo_dir" rev-parse --verify "${1:?commit required}^{commit}")
output=${2:?new artifact directory required}
[ ! -e "$output" ] || { echo 'FAIL artifact directory already exists' >&2; exit 1; }
[ "${DOCS_RS+x}" != x ] && [ "${RHIZA_NATIVE_LIB_DIR+x}" != x ] || {
    echo 'FAIL package verification requires bundled native compilation' >&2; exit 1;
}
mkdir -p "$output/diagnostics"
output=$(CDPATH='' cd -- "$output" && pwd)
state=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-ci-package.XXXXXX")
cleanup() {
    result=$?
    trap - EXIT HUP INT TERM
    set +e
    original_result=$result
    cleanup_failed=0
    # Only paths recorded by our child stage invocation belong to this cleanup.
    if [ -f "$state/worktree" ]; then
        worktree=$(cat "$state/worktree")
        if [ -e "$worktree" ]; then
            if ! git -C "$repo_dir" worktree remove --force "$worktree" >&2; then
                echo "FAIL retained worktree: $worktree" >&2
                cleanup_failed=1
                [ "$result" -ne 0 ] || result=1
            fi
        fi
        if git -C "$repo_dir" worktree list --porcelain > "$output/diagnostics/worktrees.txt"; then
            grep -Fx "worktree $worktree" "$output/diagnostics/worktrees.txt" >/dev/null
            inventory_match=$?
        else
            inventory_match=2
        fi
        if [ "$inventory_match" -ne 1 ]; then
            echo "FAIL worktree absence unconfirmed: $worktree (inventory status $inventory_match)" >&2
            cleanup_failed=1
            [ "$result" -ne 0 ] || result=1
        fi
    fi
    if [ -f "$state/stage" ]; then
        stage=$(cat "$state/stage")
        if ! rm -rf "$stage" || [ -e "$stage" ]; then
            echo "FAIL retained package stage: $stage" >&2
            cleanup_failed=1
            [ "$result" -ne 0 ] || result=1
        fi
    fi
    if ! rm -rf "$state" || [ -e "$state" ]; then
        echo "FAIL retained cleanup state: $state" >&2
        cleanup_failed=1
        [ "$result" -ne 0 ] || result=1
    fi
    # A failed cleanup must not leave an uploadable success directory.
    if [ "$result" -ne 0 ]; then
        if ! rm -rf "$output/verified" || [ -e "$output/verified" ]; then
            echo "FAIL retained invalid artifact: $output/verified" >&2
            cleanup_failed=1
        fi
    fi
    if ! printf 'original_exit=%s\ncleanup_failed=%s\nexit=%s\n' "$original_result" "$cleanup_failed" "$result" > "$output/diagnostics/cleanup.txt"; then
        echo 'FAIL unable to record cleanup result' >&2
        [ "$result" -ne 0 ] || result=1
        if ! rm -rf "$output/verified" || [ -e "$output/verified" ]; then
            echo "FAIL retained invalid artifact: $output/verified" >&2
        fi
    fi
    exit "$result"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
RHIZA_CRATE_STATE_DIR=$state sh "$script_dir/stage-crate.sh" "$commit" > "$state/result" 2> "$output/diagnostics/stage.log"
stage=$(cat "$state/result")
[ "$stage" = "$(cat "$state/stage")" ]
version=$(sed -n 's/^version = "\(.*\)"/\1/p' "$stage/Cargo.toml" | head -n 1)
case "$version" in ''|*[!0-9.]*) echo 'FAIL invalid package version' >&2; exit 1;; esac
crate="$stage/target/package/rhizadb-$version.crate"
[ -f "$crate" ]
set -- "$stage"/target/package/*.crate
[ "$#" -eq 1 ] && [ "$1" = "$crate" ]
mkdir "$state/extracted"
tar -xf "$crate" -C "$state/extracted"
extracted="$state/extracted/rhizadb-$version"
[ -d "$extracted/native/cmd/rhiza-ffi" ]
[ ! -e "$extracted/../../go.mod" ]
[ ! -e "$extracted/.cargo_vcs_info.json" ]
[ "$(wc -c < "$crate" | tr -d ' ')" -le 10000000 ]
mkdir "$output/verified"
verified="$output/verified"
# Compare every packaged member with the staged original. Cargo normalizes its
# manifest; Cargo.toml.orig is the exact Git source, not fabricated VCS metadata.
find "$extracted" -type l -print > "$state/symlinks"
[ ! -s "$state/symlinks" ] || { echo 'FAIL symlink in crate' >&2; exit 1; }
diff -r "$stage/native" "$extracted/native"
(cd "$extracted" && find . -type f | LC_ALL=C sort) > "$state/members"
while IFS= read -r member; do
    relative=${member#./}
    case "$relative" in Cargo.toml) continue;; Cargo.toml.orig) original=Cargo.toml;; *) original=$relative;; esac
    cmp "$extracted/$relative" "$stage/$original"
    case "$relative" in
        native/vendor/*) origin=vendored-locked-module-closure;;
        native/*) origin=${relative#native/};;
        Cargo.toml.orig) origin=sdk/rust/Cargo.toml;;
        *) origin=sdk/rust/$relative;;
    esac
    if [ "$origin" != vendored-locked-module-closure ]; then
        git -C "$repo_dir" show "$commit:$origin" > "$state/git-blob"
        cmp "$extracted/$relative" "$state/git-blob"
    fi
    printf '%s\t%s\n' "$relative" "$origin" >> "$verified/source-origins.tsv"
done < "$state/members"
# All generated native files, including vendor/modules.txt and embedded data.
(cd "$extracted" && find native -type f -exec shasum -a 256 {} + | LC_ALL=C sort -k 2) > "$verified/native.sha256"
(cd "$extracted" && find . -type f -exec shasum -a 256 {} + | LC_ALL=C sort -k 2) > "$verified/members.sha256"
(cd "$extracted" && find . -type f | LC_ALL=C sort | while IFS= read -r member; do
    if [ -x "$member" ]; then mode=executable; else mode=regular; fi
    printf '%s\t%s\n' "$mode" "$member"
done) > "$verified/member-modes.tsv"
# This path is outside Git and has no adjacent module; build.rs uses native/.
cargo test --manifest-path "$extracted/Cargo.toml" --locked --all-targets > "$output/diagnostics/extracted-tests.log" 2>&1
(cd "$extracted" && shasum -a 256 -c "$verified/members.sha256") > "$output/diagnostics/source-recheck.log"
cp "$crate" "$verified/"
(cd "$verified" && shasum -a 256 "rhizadb-$version.crate") > "$verified/crate.sha256"
{
    printf 'commit=%s\ntree=%s\nversion=%s\nbytes=%s\n' "$commit" "$(git -C "$repo_dir" rev-parse "$commit^{tree}")" "$version" "$(wc -c < "$crate" | tr -d ' ')"
    printf 'event=%s\nrun_id=%s\nrun_attempt=%s\n' "${GITHUB_EVENT_NAME:-local}" "${GITHUB_RUN_ID:-none}" "${GITHUB_RUN_ATTEMPT:-none}"
    cargo --version
    go version
    printf 'native_source=bundled\nvendor_origin=generated-from-locked-go-mod-and-sum\n'
} > "$verified/provenance.txt"
