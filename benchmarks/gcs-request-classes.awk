# Input: RHIZA_GCS_ACCESS <method> <path-without-query> <status>
# Pass -v bucket=NAME to recognize the configured bucket's XML object reads.
# https://cloud.google.com/storage/pricing#operations-pricing maps operation
# types to classes. These are HTTP attempts (including failures), not charges.
$1 != "RHIZA_GCS_ACCESS" { next }

NF != 4 || $2 !~ /^[A-Z]+$/ || $3 !~ /^\// || $3 ~ /[?#]/ || $4 !~ /^[1-5][0-9][0-9]$/ {
	malformed++
	next
}

{
	http++
	if ($4 >= 400) failed++
	method = $2
	path = $3
	upload = path ~ /^\/upload\/storage\/v1\/b\/[^\/]+\/o$/
	json_bucket = path ~ /^\/storage\/v1\/b\/[^\/]+$/
	json_object = path ~ /^\/storage\/v1\/b\/[^\/]+\/o\/.+/ && path !~ /\/acl(\/|$)/
	if (method == "POST" && path == "/token") {
		auth++
	} else if (upload && method == "PUT") {
		# Floci v0.11.0 uses this same path for upload_id PUTs, including probes.
		transfer++
	} else if (upload && method == "POST") {
		# uploadType is not in the log: insert and resumable initiation share a path.
		insert_or_init++
		class_a++
	} else if ((path == "/storage/v1/b" && (method == "GET" || method == "POST")) ||
	           (path ~ /^\/storage\/v1\/b\/[^\/]+\/o$/ && (method == "GET" || method == "POST"))) {
		class_a++
	} else if (method == "GET" && (json_bucket || json_object ||
	           path ~ /^\/download\/storage\/v1\/b\/[^\/]+\/o\/.+/)) {
		class_b++
	} else if ((method == "GET" || method == "HEAD") && bucket != "" &&
	           index(path, "/" bucket "/") == 1 && length(path) > length(bucket) + 2) {
		class_b++
	} else if (method == "DELETE" && (json_bucket || json_object)) {
		deletes++
	} else {
		unknown++
	}
}

END {
	printf "{\"http_attempts\":%.0f,\"class_a_attempts\":%.0f,\"class_b_attempts\":%.0f,", http, class_a, class_b
	printf "\"delete_attempts\":%.0f,\"auth_attempts\":%.0f,\"unknown_attempts\":%.0f,", deletes, auth, unknown
	printf "\"failed_http_attempts\":%.0f,\"malformed_access_lines\":%.0f,", failed, malformed
	printf "\"upload_insert_or_resumable_init_attempts\":%.0f,\"resumable_transfer_or_probe_attempts\":%.0f}\n", insert_or_init, transfer
}
