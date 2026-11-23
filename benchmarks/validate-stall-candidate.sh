#!/bin/sh
set -eu

expected=${1-}
case "$expected" in
	*[!0-9a-f]*|'') printf '%s\n' 'expected a full lowercase commit SHA' >&2; exit 2 ;;
esac
if [ "${#expected}" -ne 40 ]; then
	printf '%s\n' 'expected a full 40-character commit SHA' >&2
	exit 2
fi
actual=$(git rev-parse --verify 'HEAD^{commit}')
if [ "$actual" != "$expected" ]; then
	printf '%s\n' 'checkout does not match triggering commit' >&2
	exit 2
fi
printf '%s\n' "$actual"
