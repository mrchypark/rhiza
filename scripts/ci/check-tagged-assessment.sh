#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || ! -s ${1:-} ]]; then
	printf 'usage: %s NONEMPTY_TAGGED_ASSESSMENT\n' "$0" >&2
	exit 2
fi

assessment=$1
grep -qx '# correlation_status=valid' "$assessment"
grep -Eq '^# valid_correlations=[1-9][0-9]*$' "$assessment"

for field in \
	incomplete_correlations transmit_failures http_failures client_failures \
	malformed_client_rows duplicate_client_tags foreign_server_events duplicate_server_events \
	slot_mismatches order_invalid missing_receive missing_mutation_entry missing_admission \
	missing_proposal missing_result missing_response missing_proposal_mu missing_proposal_worker \
	proposal_stage_bounds_invalid proposal_permit_code_invalid proposal_permit_outcome_invalid \
	proposal_worker_outcome_invalid proposal_gap_drops proposal_gap_dropped_capacity \
	proposal_gap_dropped_trylock proposal_gap_dropped_unclassified \
	proposal_gap_dropped_reason_incomplete proposal_gap_drop_reason_mismatch; do
	grep -qx "# $field=0" "$assessment"
done
