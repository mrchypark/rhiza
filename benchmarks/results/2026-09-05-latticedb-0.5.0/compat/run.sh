#!/bin/sh
set -eu

repo=${1:?repository path required}
work=${2:?work directory required}
baseline=${3:?v0.3 modfile required}
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$repo"
mkdir -p "$work"
cp "$baseline" "$work/baseline.mod"
cp "$repo/go.mod" "$work/current.mod"
go build -mod=mod -modfile="$work/baseline.mod" -o "$work/writer" "$script_dir/main.go"
go build -mod=mod -modfile="$work/current.mod" -o "$work/reader" "$script_dir/main.go"
data="$work/data-$$"
"$work/writer" write "$data"
"$work/reader" verify "$data"
"$work/reader" reopen "$data"
"$work/writer" reopen "$data"
store="$work/checkpoint-store-$$"
"$work/writer" checkpoint-write "$work/checkpoint-source-$$" "$store"
test -f "$store/compat-checkpoint/checkpoint/CURRENT"
"$work/reader" checkpoint-restore "$work/checkpoint-target-$$" "$store"
