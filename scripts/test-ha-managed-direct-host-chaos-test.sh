#!/bin/sh
# Offline guardrails for direct-host mode. No kubectl/API calls.
set -eu
cd "$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
direct=scripts/test-ha-managed-direct-host-chaos.sh
fixture=scripts/ha-managed-bootstrap-fixture.sh
candidate=.github/workflows/ha-managed-candidate-image.yml
chaos=.github/workflows/ha-managed-cluster-chaos.yml
operator_builder=pkg/operator/bootstrap_members_v1alpha2.go
operator_initializer=pkg/operator/initializer_v1alpha2.go
for source in "$direct" "$fixture" scripts/test-ha-managed-direct-host-chaos-test.sh; do sh -n "$source"; done
scratch=$(mktemp -d "${TMPDIR:-/tmp}/rhiza-direct-chaos-oracles.XXXXXX")
cleanup_test() {
  rm -f "$scratch/old-id" "$scratch/new-id" "$scratch/same-a" "$scratch/same-b" "$scratch/preflight-list.json" "$scratch/bucket-contract_test.go" "$scratch/bucket-overlay.json"
  rmdir "$scratch"
}
trap cleanup_test EXIT HUP INT TERM
identity_oracles=$(sed -n '/^managed_replacement_identity_oracle() {/,/^}/p' "$fixture")
[ -n "$identity_oracles" ]
printf '%s\n' 'old-voter-identity' > "$scratch/old-id"
printf '%s\n' 'new-voter-identity' > "$scratch/new-id"
sh -c 'set -eu; eval "$1"; managed_replacement_identity_oracle "$2" "$3"' fixture "$identity_oracles" "$scratch/old-id" "$scratch/new-id"
if sh -c 'set -eu; eval "$1"; managed_replacement_identity_oracle "$2" "$3"' fixture "$identity_oracles" "$scratch/old-id" "$scratch/old-id" 2>/dev/null; then
  echo 'replacement identity oracle accepted reused identity' >&2; exit 1
fi
same_pod_oracle=$(sed -n '/^managed_same_pod_identity_oracle() {/,/^}/p' "$fixture")
printf '%s\n' 'same-voter-identity' > "$scratch/same-a"
cp "$scratch/same-a" "$scratch/same-b"
sh -c 'set -eu; eval "$1"; managed_same_pod_identity_oracle "$2" "$3"' fixture "$same_pod_oracle" "$scratch/same-a" "$scratch/same-b"
printf '%s\n' 'changed-voter-identity' > "$scratch/same-b"
if sh -c 'set -eu; eval "$1"; managed_same_pod_identity_oracle "$2" "$3"' fixture "$same_pod_oracle" "$scratch/same-a" "$scratch/same-b" 2>/dev/null; then
  echo 'same-Pod oracle accepted changed identity' >&2; exit 1
fi
preflight_helper=$(sed -n '/^assert_names_absent() {/,/^}/p' "$direct")
[ -n "$preflight_helper" ]
run_preflight_case() {
  mode=$1 list_json=$2 expected=$3
  code=0
  sh -c 'set -eu; private=$1; mode=$2; test_list_json=$3; helper=$4
    hn() { [ "$mode" != error ] || return 41; printf "%s\n" "$test_list_json"; }
    eval "$helper"
    assert_names_absent services wanted
  ' fixture "$scratch" "$mode" "$list_json" "$preflight_helper" >/dev/null 2>&1 || code=$?
  if [ "$expected" = pass ]; then [ "$code" -eq 0 ]; else [ "$code" -ne 0 ]; fi
}
run_preflight_case ok '{"items":[{"metadata":{"name":"another"}}]}' pass
run_preflight_case collision '{"items":[{"metadata":{"name":"wanted"}}]}' fail
run_preflight_case error '{}' fail
grep -Fq 'operator) dockerfile=Dockerfile.operator' "$candidate"
grep -Fq 'managed-ffi-contract) dockerfile=Dockerfile.managed-ffi-contract' "$candidate"
grep -Fq 'rhiza-candidate-image/v2' "$candidate"
grep -Fq 'options: [amd64, arm64]' "$candidate"
grep -Fq "'ubuntu-24.04-arm'" "$candidate"
grep -Fq 'platforms: ${{ steps.variant.outputs.platform }}' "$candidate"
grep -Fq '"platform":"%s"' "$candidate"
mode_guard=$(sed -n '/^execution_mode=/,/^esac/p' "$direct")
[ -n "$mode_guard" ]
sh -c 'set -eu; git() { printf "%s\n" test-source; }; RHIZA_CHAOS_EXECUTION_MODE=local RHIZA_LOCAL_RUN_ID=123; unset GITHUB_ACTIONS; eval "$1"; [ "$execution_mode" = local ]; [ "$GITHUB_RUN_ID" = 123 ]; [ "$GITHUB_SHA" = test-source ]; [ "${GITHUB_ACTIONS-}" != true ]' guard "$mode_guard"
local_id_34=1234567890123456789012345678901234
bucket_assignment=$(sed -n '/^bucket=/p' "$fixture")
[ -n "$bucket_assignment" ]
generated_buckets=
for local_id in 123 "$local_id_34"; do
  generated_bucket=$(sh -c 'set -eu; git() { printf "%s\n" test-source; }; RHIZA_CHAOS_EXECUTION_MODE=local RHIZA_LOCAL_RUN_ID=$2; unset GITHUB_ACTIONS; eval "$1"; nonce=012345abcdef; eval "$3"; [ "${#bucket}" -le 63 ]; printf "%s\n" "$bucket"' guard "$mode_guard" "$local_id" "$bucket_assignment")
  generated_buckets="$generated_buckets $generated_bucket"
done
cat > "$scratch/bucket-contract_test.go" <<'GO'
package main

import (
  "os"
  "strings"
  "testing"
)

func TestGeneratedLocalChaosBucketContract(t *testing.T) {
  buckets := strings.Fields(os.Getenv("RHIZA_GENERATED_TEST_BUCKETS"))
  if len(buckets) != 2 {
    t.Fatal("expected short and maximum-length generated buckets")
  }
  for _, bucket := range buckets {
    if err := validate("http://127.0.0.1:1234", bucket); err != nil {
      t.Fatalf("driver-generated bucket %q rejected by real setup validator: %v", bucket, err)
    }
  }
}
GO
jq -n --arg source "$(pwd)/scripts/ha-managed-bucket-setup-src/main_test.go" --arg replacement "$scratch/bucket-contract_test.go" '{Replace:{($source):$replacement}}' > "$scratch/bucket-overlay.json"
(cd scripts/ha-managed-bucket-setup-src && RHIZA_GENERATED_TEST_BUCKETS="$generated_buckets" go test -mod=readonly -overlay "$scratch/bucket-overlay.json" -run '^TestGeneratedLocalChaosBucketContract$' -count=1 -timeout=30s)
for invalid_id in local-test "$(printf '123\nbad')" "$(printf '123\n456')" "$(printf '123\t456')"; do
  if sh -c 'set -eu; git() { printf "%s\n" test-source; }; RHIZA_CHAOS_EXECUTION_MODE=local RHIZA_LOCAL_RUN_ID=$2; unset GITHUB_ACTIONS; eval "$1"' guard "$mode_guard" "$invalid_id" 2>/dev/null; then
    echo 'local mode accepted a nonnumeric or multiline ID rejected by the bucket helper' >&2; exit 1
  fi
done
if sh -c 'set -eu; git() { printf "%s\n" test-source; }; RHIZA_CHAOS_EXECUTION_MODE=local RHIZA_LOCAL_RUN_ID=$2; unset GITHUB_ACTIONS; eval "$1"' guard "$mode_guard" "${local_id_34}5" 2>/dev/null; then
  echo 'local mode accepted an ID exceeding the bucket naming budget' >&2; exit 1
fi
if sh -c 'set -eu; RHIZA_CHAOS_EXECUTION_MODE=local RHIZA_LOCAL_RUN_ID=123 GITHUB_ACTIONS=true; eval "$1"' guard "$mode_guard" 2>/dev/null; then
  echo 'local mode accepted a GitHub Actions execution' >&2; exit 1
fi
if sh -c 'set -eu; unset GITHUB_ACTIONS RHIZA_CHAOS_EXECUTION_MODE; eval "$1"' guard "$mode_guard" 2>/dev/null; then
  echo 'default mode accepted a non-CI execution' >&2; exit 1
fi
grep -Fq 'io.rhiza.candidate.variant=' "$candidate"
grep -Fq 'options: [release, candidate, candidate-direct-host]' "$chaos"
grep -Fq 'candidate-direct-host) IMAGE_VARIANT=managed-ffi-contract' "$chaos"
dispatch_inputs=$(sed -n '/^    inputs:/,/^permissions:/p' "$chaos" | grep -Ec '^      [a-z_]+:')
[ "$dispatch_inputs" -le 10 ]
grep -Fq 'direct-host requires managed-ffi-contract image' "$direct"
grep -Fq '[ "$RHIZA_MANAGED_TEST_DOCKERFILE" = Dockerfile.managed-ffi-contract ]' "$direct"
grep -Fq 'export RHIZA_MANAGED_FIXTURE_NODE=$RHIZA_HOST_NODE' "$direct"
grep -Fq 'nodeSelector:$nodeSelector' "$fixture"
[ "$(grep -Fc 'nodeSelector:$nodeSelector' "$fixture")" -eq 2 ]
grep -Fq 'containers:[{name:"sql",image:$image}]' "$fixture"
grep -Fq 'ENTRYPOINT ["rhiza-operator"]' Dockerfile.managed-ffi-contract
grep -Fq 'COPY --chmod=0755 scripts/managed-ffi-contract-entrypoint.sh /usr/local/bin/rhiza-ha-init' Dockerfile.managed-ffi-contract
grep -Fq 'exec /usr/local/libexec/rhiza-ha-init "$@"' scripts/managed-ffi-contract-entrypoint.sh
grep -Fq 'main["command"] = []string{"/usr/local/bin/rhiza-ha-init"}' "$operator_builder"
grep -Fq 'podSpec, err := V1Alpha2ManagedSQLPodSpec' "$operator_initializer"
grep -Fq 'name=="rhizaclusters" and .kind=="RhizaCluster"' "$direct"
grep -Fq 'create podchaos.chaos-mesh.org' "$direct"
grep -Fq 'AllInjected' "$direct"
if grep -Fq 'AllRecovered' "$direct"; then echo 'one-shot kill waits for unsupported pre-delete recovery condition' >&2; exit 1; fi
grep -Fq 'metadata.uid!=$old_uid' "$direct"
grep -Fq 'containerID!=$old' "$direct"
grep -Fq 'voter-identity.json' "$direct"
grep -Fq 'rhiza-prepare-identity' "$direct"
grep -Fq 'rhiza-await-bootstrap-plan' "$direct"
if grep -Eq 'helm |kubectl apply .*cluster-crd|kubectl apply .*crd\.yaml' "$direct"; then
  echo 'direct-host runner contains forbidden vcluster/CRD installation path' >&2; exit 1
fi
if grep -Eq 'kind:(Namespace|ClusterRole|ClusterRoleBinding|CustomResourceDefinition)|persistentVolumeClaim' "$direct"; then
  echo 'direct-host fixture includes cluster-scoped or persistent storage resource' >&2; exit 1
fi
printf '%s\n' 'PASS: direct-host mode requires the provenance-bound FFI image, uses namespace-local exact-UID Chaos Mesh checks, and does not install CRDs or vcluster'
