#!/bin/sh
set -eu

# Actions-only adapter contract test. The workflow owns the three pinned Floci
# service containers; this script never starts Docker or falls back to cloud.
if [ "${CI:-}" != "true" ] || [ "${RHIZA_FLOCI_INTEGRATION:-}" != "1" ]; then
	echo "refusing Floci test outside explicit CI opt-in" >&2
	exit 2
fi

export RHIZA_FLOCI_S3_ENDPOINT='http://floci-s3:4566'
export RHIZA_FLOCI_AZURE_BLOB_ENDPOINT='http://floci-az:4577/flocitest'
export STORAGE_EMULATOR_HOST='http://floci-gcs:4588'
export GOOGLE_CLOUD_PROJECT='floci-local'
unset GOOGLE_APPLICATION_CREDENTIALS

export AWS_ACCESS_KEY_ID='floci-test'
export AWS_SECRET_ACCESS_KEY='floci-test-secret'
export AWS_EC2_METADATA_DISABLED='true'
export AWS_REGION='us-east-1'

exec go test ./internal/objstore -run '^TestFlociProviderContracts$' -count=1 -timeout=3m -v
