#!/bin/sh
set -eu
cd "$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
. scripts/ha-managed-bootstrap-fixture.sh

assert_family() {
  selector=$1 expected=$2
  actual=$(managed_fixture_profile_family "$selector")
  [ "$actual" = "$expected" ] || {
    printf 'fixture %s selected family %s, want %s\n' "$selector" "$actual" "$expected" >&2
    return 1
  }
}

assert_family legacy other
assert_family mixed other
assert_family internal internal
assert_family internal-append-only internal
assert_family graph graph
assert_family graph-append-only graph

[ "$(managed_fixture_profile_protocol internal)" = rhiza-managed-request-append-only/v1 ]
[ "$(managed_fixture_profile_protocol internal-append-only)" = rhiza-managed-request-append-only/v1 ]
[ "$(managed_fixture_profile_protocol graph)" = rhiza-managed-request-graph-append-only/v1 ]
[ "$(managed_fixture_profile_protocol graph-append-only)" = rhiza-managed-request-graph-append-only/v1 ]
for retired in rhiza-managed-request/v1 rhiza-managed-request-graph/v1; do
  if managed_fixture_profile_protocol "$retired" >/dev/null 2>&1; then
    printf 'retired protocol was accepted as a fixture selector: %s\n' "$retired" >&2
    exit 1
  fi
done

if grep -Eq '\[ "\$fixture_case" = (internal|graph) \]' scripts/ha-managed-bootstrap-fixture.sh; then
  echo 'profile-specific fixture gates still compare only a legacy case name' >&2
  exit 1
fi
gate_count=$(grep -Ec 'if \[ "\$fixture_family" = (internal|graph) \]; then' scripts/ha-managed-bootstrap-fixture.sh)
[ "$gate_count" -eq 7 ] || {
  printf 'found %s profile-family gate sites, want 7 (setup and prefault/first/restart assertions)\n' "$gate_count" >&2
  exit 1
}

matrix=.github/workflows/ha-managed-bootstrap.yml
[ "$(grep -c 'fixture:' "$matrix")" -eq 10 ] || {
  echo 'bootstrap matrix must keep exactly the ten legacy/mixed/final-profile lanes' >&2
  exit 1
}
[ "$(grep -Fxc '          - fixture: internal' "$matrix")" -eq 3 ]
[ "$(grep -Fxc '          - fixture: graph' "$matrix")" -eq 3 ]
if grep -Eq 'fixture: (internal-append-only|graph-append-only)' "$matrix"; then
  echo 'duplicate append-only alias lanes remain in bootstrap matrix' >&2
  exit 1
fi

printf '%s\n' 'PASS: internal and Graph fixture selectors share policy-specific gates with their append-only profiles'
