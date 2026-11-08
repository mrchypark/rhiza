#!/bin/sh
set -eu

here=$(CDPATH= cd "$(dirname "$0")" && pwd)
actual=$(awk -v bucket=rhiza -f "$here/gcs-request-classes.awk" <<'EOF'
unrelated Quarkus output
RHIZA_GCS_ACCESS GET /storage/v1/b 200
RHIZA_GCS_ACCESS POST /storage/v1/b 201
RHIZA_GCS_ACCESS POST /upload/storage/v1/b/rhiza/o 200
RHIZA_GCS_ACCESS POST /upload/storage/v1/b/rhiza/o 412
RHIZA_GCS_ACCESS PUT /upload/storage/v1/b/rhiza/o 308
RHIZA_GCS_ACCESS PUT /upload/storage/v1/b/rhiza/o 503
RHIZA_GCS_ACCESS GET /storage/v1/b/rhiza/o 200
RHIZA_GCS_ACCESS GET /storage/v1/b/rhiza 200
RHIZA_GCS_ACCESS GET /storage/v1/b/rhiza/o/prefix%2Fhead 404
RHIZA_GCS_ACCESS GET /download/storage/v1/b/rhiza/o/prefix%2Fhead 206
RHIZA_GCS_ACCESS GET /rhiza/prefix/head 200
RHIZA_GCS_ACCESS HEAD /rhiza/prefix/head 200
RHIZA_GCS_ACCESS DELETE /storage/v1/b/rhiza/o/prefix%2Fhead 204
RHIZA_GCS_ACCESS DELETE /storage/v1/b/rhiza 204
RHIZA_GCS_ACCESS POST /token 401
RHIZA_GCS_ACCESS GET /unknown/path 500
RHIZA_GCS_ACCESS PATCH /storage/v1/b/rhiza/o/prefix%2Fhead 200
RHIZA_GCS_ACCESS GET /storage/v1/b/rhiza/o/prefix%2Fhead/acl 200
RHIZA_GCS_ACCESS HEAD /rhiza/ 200
RHIZA_GCS_ACCESS GET /rhiza/prefix/head?upload_id=not-for-output 200
RHIZA_GCS_ACCESS GET /storage/v1/b/rhiza/o invalid-status
EOF
)
expected='{"http_attempts":19,"class_a_attempts":5,"class_b_attempts":5,"delete_attempts":2,"auth_attempts":1,"unknown_attempts":4,"failed_http_attempts":5,"malformed_access_lines":2,"upload_insert_or_resumable_init_attempts":2,"resumable_transfer_or_probe_attempts":2}'
if [ "$actual" != "$expected" ]; then
	printf 'GCS request attribution mismatch\nactual:   %s\nexpected: %s\n' "$actual" "$expected" >&2
	exit 1
fi

actual=$(awk -f "$here/gcs-request-classes.awk" </dev/null)
expected='{"http_attempts":0,"class_a_attempts":0,"class_b_attempts":0,"delete_attempts":0,"auth_attempts":0,"unknown_attempts":0,"failed_http_attempts":0,"malformed_access_lines":0,"upload_insert_or_resumable_init_attempts":0,"resumable_transfer_or_probe_attempts":0}'
[ "$actual" = "$expected" ]
printf '%s\n' 'GCS request attribution fixtures PASS'
