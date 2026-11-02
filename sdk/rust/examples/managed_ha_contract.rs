use base64::{engine::general_purpose::STANDARD, Engine as _};
use rhizadb::{
    ExecuteReturningResult, ManagedDb, ManagedExecuteRequest, ManagedKVReceipt, ManagedKVRequest,
    ManagedQueryRequest, ManagedRecoveryResult, ManagedRequestReceipt, MutationKind,
};
use serde_json::json;
use std::{
    env,
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const ACK_MARKER: &str = "/var/lib/rhiza/data/rust-managed-contract-ack.json";
const VALUE: &str = "rust-managed-ffi-ack";

fn main() {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments == ["--mixed-initial"] || arguments == ["--mixed-verify"] {
        if let Err(code) = run_mixed(arguments[0] == "--mixed-initial") {
            eprintln!("FAIL: managed-ha-contract {code}");
            std::process::exit(1);
        }
        return;
    }
    let initial_run = match env::args().skip(1).collect::<Vec<_>>().as_slice() {
        [] => false,
        [flag] if flag == "--initial-run" => true,
        _ => {
            eprintln!("FAIL: managed-ha-contract invalid_arguments");
            std::process::exit(2);
        }
    };
    if let Err(code) = run(initial_run) {
        eprintln!("FAIL: managed-ha-contract {code}");
        std::process::exit(1);
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct MixedReceipt {
    slot: u64,
    status: String,
    applied: bool,
    retry_through_slot: u64,
}
impl MixedReceipt {
    fn from_receipt(receipt: &ManagedKVReceipt) -> Result<Self, String> {
        receipt
            .require_committed()
            .map_err(|_| "mixed_receipt_not_committed")?;
        if receipt
            .error_code
            .as_deref()
            .is_some_and(|code| !code.is_empty())
        {
            return Err("mixed_receipt_error_code".into());
        }
        let retry = receipt
            .retry_through_slot
            .ok_or("mixed_receipt_window_missing")?;
        if receipt.slot == 0 || retry < receipt.slot {
            return Err("mixed_receipt_slot_invalid".into());
        }
        Ok(Self {
            slot: receipt.slot,
            status: receipt.status.clone(),
            applied: receipt.applied,
            retry_through_slot: retry,
        })
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct MixedCase {
    request_id_b64: String,
    key_b64: String,
    operation: String,
    value: Option<String>,
    expected: Option<String>,
    expected_exists: bool,
    wanted_value: Option<String>,
    receipt: MixedReceipt,
    recovered: bool,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct MixedTuple {
    version: u8,
    producer_uid: String,
    cases: Vec<MixedCase>,
}

fn mixed_requests() -> Vec<(&'static str, ManagedKVRequest, Option<Vec<u8>>, bool)> {
    vec![
        (
            "put",
            ManagedKVRequest {
                request_id: vec![0xff],
                key: vec![0xff],
                value: Some(b"ff-value".to_vec()),
                expected: None,
                expected_exists: true,
                ttl_ms: 0,
            },
            Some(b"ff-value".to_vec()),
            true,
        ),
        (
            "put",
            ManagedKVRequest {
                request_id: vec![0xfe],
                key: vec![0xfe],
                value: Some(b"fe-value".to_vec()),
                expected: Some(vec![]),
                expected_exists: false,
                ttl_ms: 0,
            },
            Some(b"fe-value".to_vec()),
            true,
        ),
        (
            "put",
            ManagedKVRequest {
                request_id: "\u{fffd}".as_bytes().to_vec(),
                key: "\u{fffd}".as_bytes().to_vec(),
                value: None,
                expected: Some(vec![0xff]),
                expected_exists: true,
                ttl_ms: 0,
            },
            Some(vec![]),
            true,
        ),
        (
            "cas",
            ManagedKVRequest {
                request_id: vec![0xff, 1],
                key: vec![0xff],
                value: Some(vec![]),
                expected: None,
                expected_exists: false,
                ttl_ms: 0,
            },
            Some(b"ff-value".to_vec()),
            false,
        ),
        (
            "delete",
            ManagedKVRequest {
                request_id: vec![0xfe, 1],
                key: vec![0xfe, 0],
                value: Some(vec![]),
                expected: None,
                expected_exists: false,
                ttl_ms: 0,
            },
            None,
            false,
        ),
    ]
}

fn mixed_mutation(
    db: &ManagedDb,
    operation: &str,
    request: &ManagedKVRequest,
) -> Result<(ManagedKVReceipt, bool), String> {
    let result = match operation {
        "put" => db.kv_put(request),
        "cas" => db.kv_cas(request),
        "delete" => db.kv_delete(request),
        _ => return Err("mixed_operation_invalid".into()),
    };
    match result {
        Ok(receipt) => Ok((receipt, false)),
        Err(error) if error.error().code == "commit_unknown" => {
            let slot = if let Some(context) = error.mutation_context() {
                if context.kind() != MutationKind::Kv || context.request_id() != request.request_id
                {
                    return Err("mixed_unknown_context_mismatch".into());
                }
                context.slot()
            } else {
                0
            };
            exact_mixed_recovery(
                db.recover_pending_detailed()
                    .map_err(|_| "mixed_unknown_unresolved")?,
                &request.request_id,
                slot,
            )
            .map(|receipt| (receipt, true))
        }
        Err(error) => Err(error.error().code.clone()),
    }
}

fn exact_mixed_recovery(
    result: ManagedRecoveryResult,
    id: &[u8],
    slot: u64,
) -> Result<ManagedKVReceipt, String> {
    match result {
        ManagedRecoveryResult::Kv {
            request_id,
            receipt,
        } if request_id == id && (slot == 0 || slot == receipt.slot) => Ok(receipt),
        _ => Err("mixed_unknown_original_unavailable".into()),
    }
}

fn mixed_sql_ack_slot(slot: Option<u64>) -> Result<u64, String> {
    match slot {
        Some(0) => Err("mixed_sql_slot_zero".into()),
        Some(slot) => Ok(slot),
        None => Err("mixed_sql_slot_missing".into()),
    }
}

fn mixed_sql(
    db: &ManagedDb,
    request: &ManagedExecuteRequest,
) -> Result<ExecuteReturningResult, String> {
    // The unchanged SQL facade returns Error without mutation context. Reject
    // any lost SQL ACK instead of inventing its slot or accepting row presence
    // as ACK proof. KV's detailed error path below retains exact slot context.
    let receipt = db.execute(request).map_err(|error| error.code)?;
    receipt
        .require_committed()
        .map_err(|_| "mixed_sql_ack_missing")?;
    mixed_sql_ack_slot(receipt.receipt.slot)?;
    Ok(receipt)
}

fn mixed_row(db: &ManagedDb) -> Result<String, String> {
    let row = db
        .query(
            &ManagedQueryRequest::new("SELECT tuple FROM rust_mixed_contract WHERE id=1")
                .consistency("linearizable"),
        )
        .map_err(|e| e.code)?;
    if row.rows.len() != 1 || row.rows[0].len() != 1 {
        return Err("mixed_ack_row_missing".into());
    }
    row.rows[0][0]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| "mixed_ack_row_invalid".into())
}

fn verify_mixed_case(
    db: &ManagedDb,
    case: &MixedCase,
    operation: &str,
    request: &ManagedKVRequest,
    wanted: &Option<Vec<u8>>,
    applied: bool,
) -> Result<(), String> {
    if case.operation != operation
        || case.request_id_b64 != STANDARD.encode(&request.request_id)
        || case.key_b64 != STANDARD.encode(&request.key)
        || case.value != request.value.as_ref().map(|v| STANDARD.encode(v))
        || case.expected != request.expected.as_ref().map(|v| STANDARD.encode(v))
        || case.expected_exists != request.expected_exists
        || case.wanted_value != wanted.as_ref().map(|v| STANDARD.encode(v))
        || case.receipt.applied != applied
    {
        return Err("mixed_original_tuple_mismatch".into());
    }
    let read = db
        .kv_get(&request.key, "linearizable")
        .map_err(|e| e.code)?;
    if read.found != wanted.is_some()
        || read.value != *wanted
        || read.applied_slot < case.receipt.slot
        || read.consensus_tip < case.receipt.slot
    {
        return Err("mixed_value_or_watermark_mismatch".into());
    }
    let status = db
        .request_status(MutationKind::Kv, &request.request_id)
        .map_err(|e| e.code)?;
    if status.state != "committed"
        || status.tip < case.receipt.slot
        || status.tip > case.receipt.retry_through_slot
    {
        return Err("mixed_status_expired_or_missing".into());
    }
    match status.receipt {
        Some(ManagedRequestReceipt::Kv(receipt))
            if MixedReceipt::from_receipt(&receipt)? == case.receipt =>
        {
            Ok(())
        }
        _ => Err("mixed_receipt_changed".into()),
    }
}

fn run_mixed(initial: bool) -> Result<(), String> {
    let uid = env::var("POD_UID").map_err(|_| "pod_identity_missing")?;
    let producer = env::var("RHIZA_RUST_PRODUCER_UID").map_err(|_| "producer_identity_missing")?;
    if uid.is_empty() || producer.is_empty() || initial && uid != producer {
        return Err("mixed_producer_identity_mismatch".into());
    }
    let mut db = ManagedDb::open_from_env().map_err(|e| e.code)?;
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err("readiness_timeout".into());
        }
        if db.ready_with_timeout(remaining).map_err(|e| e.code)? {
            break;
        }
        thread::sleep(remaining.min(Duration::from_millis(250)));
    }
    if initial {
        // No Host/WAL is opened by this subprocess. The actual Rust Host stays
        // open while the exact OTHER two original Pod UIDs become runtime Ready.
        let remaining = deadline
            .saturating_duration_since(Instant::now())
            .as_millis();
        if remaining == 0 {
            return Err("mixed_peer_budget_exhausted".into());
        }
        let output = std::process::Command::new("/usr/local/libexec/rhiza-managed-ffi-contract")
            .args([
                "-test.run=^TestRustMixedInitialPeersReady$",
                "-test.count=1",
                "-test.timeout=90s",
            ])
            .env("RHIZA_RUST_WAIT_ORIGINAL_PEERS", "1")
            .env(
                "RHIZA_RUST_PEER_BUDGET_MS",
                remaining.min(90000).to_string(),
            )
            .output()
            .map_err(|_| "mixed_peer_helper_failed")?;
        if !output.status.success()
            || !String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line == "PASS: exact original Rust peers normal Ready")
        {
            return Err("mixed_original_peers_not_ready".into());
        }
    }
    let requests = mixed_requests();
    let (tuple, row_ack_slot) = if initial {
        let mut cases = Vec::new();
        for (operation, request, wanted, applied) in &requests {
            let (receipt, recovered) = mixed_mutation(&db, operation, request)?;
            let case = MixedCase {
                request_id_b64: STANDARD.encode(&request.request_id),
                key_b64: STANDARD.encode(&request.key),
                operation: operation.to_string(),
                value: request.value.as_ref().map(|v| STANDARD.encode(v)),
                expected: request.expected.as_ref().map(|v| STANDARD.encode(v)),
                expected_exists: request.expected_exists,
                wanted_value: wanted.as_ref().map(|v| STANDARD.encode(v)),
                receipt: MixedReceipt::from_receipt(&receipt)?,
                recovered,
            };
            verify_mixed_case(&db, &case, operation, request, wanted, *applied)?;
            cases.push(case);
        }
        let tuple = MixedTuple {
            version: 1,
            producer_uid: producer.clone(),
            cases,
        };
        let raw = serde_json::to_string(&tuple).map_err(|_| "mixed_tuple_encode_failed")?;
        mixed_sql(
            &db,
            &ManagedExecuteRequest::sql(
                "rust-mixed-schema",
                "CREATE TABLE rust_mixed_contract(id INTEGER PRIMARY KEY,tuple TEXT NOT NULL)",
            ),
        )?;
        let ack = mixed_sql(
            &db,
            &ManagedExecuteRequest::sql(
                "rust-mixed-row",
                "INSERT INTO rust_mixed_contract VALUES(1,?)",
            )
            .args([json!(raw)]),
        )?;
        if mixed_row(&db)? != raw {
            return Err("mixed_sql_row_readback_mismatch".into());
        }
        // The row intentionally does not contain its own INSERT slot.
        (tuple, Some(mixed_sql_ack_slot(ack.receipt.slot)?))
    } else {
        let raw = mixed_row(&db)?;
        if raw.len() > 16384 {
            return Err("mixed_tuple_oversized".into());
        }
        (
            serde_json::from_str::<MixedTuple>(&raw).map_err(|_| "mixed_tuple_invalid")?,
            None,
        )
    };
    if tuple.version != 1 || tuple.producer_uid != producer || tuple.cases.len() != requests.len() {
        return Err("mixed_tuple_binding_mismatch".into());
    }
    for (case, (operation, request, wanted, applied)) in tuple.cases.iter().zip(&requests) {
        verify_mixed_case(&db, case, operation, request, wanted, *applied)?;
    }
    db.close().map_err(|e| e.code)?;
    // All output used as an ACK/PASS oracle is emitted AFTER successful close.
    if let Some(slot) = row_ack_slot {
        println!("RUST_MIXED_ROW_ACK_SLOT={slot}");
    }
    println!("RUST_MIXED_CURRENT_UID={uid}");
    println!(
        "RUST_MIXED_TUPLE={}",
        serde_json::to_string(&tuple).map_err(|_| "mixed_tuple_encode_failed")?
    );
    println!(
        "PASS: managed Rust FFI mixed {} contract",
        if initial { "initial" } else { "verify" }
    );
    Ok(())
}

fn run(initial_run: bool) -> Result<(), String> {
    let pod_uid = env::var("POD_UID").map_err(|_| "pod_identity_missing")?;
    if pod_uid.is_empty() {
        return Err("pod_identity_missing".into());
    }
    let mut db = ManagedDb::open_from_env().map_err(|error| error.code)?;
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err("readiness_timeout".into());
        }
        if db
            .ready_with_timeout(remaining)
            .map_err(|error| error.code)?
        {
            break;
        }
        thread::sleep(remaining.min(Duration::from_millis(250)));
    }

    let marker_path = Path::new(ACK_MARKER);
    let previous = match fs::read(marker_path) {
        Ok(bytes) => {
            if initial_run {
                return Err("initial_run_marker_exists".into());
            }
            let marker: AckMarker = serde_json::from_slice(&bytes).map_err(|_| "marker_invalid")?;
            if marker.pod_uid != pod_uid {
                return Err("marker_pod_mismatch".into());
            }
            Some(marker)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && initial_run => None,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err("prior_ack_marker_missing".into())
        }
        Err(_) => return Err("marker_read_failed".into()),
    };

    if let Some(marker) = &previous {
        let prior = query_row(&db, marker.row_id)?;
        if !row_matches(&prior, marker.row_id, VALUE) {
            return Err("prior_ack_row_missing_or_changed".into());
        }
    }

    let bootstrap = db
        .query(
            &ManagedQueryRequest::new("SELECT id, value FROM bootstrap_ci WHERE id = 1")
                .consistency("linearizable"),
        )
        .map_err(|error| error.code)?;
    if bootstrap.rows.len() != 1
        || bootstrap.rows[0].len() != 2
        || bootstrap.rows[0][0].as_i64() != Some(1)
        || bootstrap.rows[0][1].as_str() != Some("real-kubelet")
    {
        return Err("bootstrap_ack_mismatch".into());
    }

    let row_id = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "clock_error")?
            .as_nanos(),
    )
    .map_err(|_| "row_id_range")?;
    let (request_id, schema_request_id) = contract_request_ids(&pod_uid, row_id);
    let schema = ManagedExecuteRequest::sql(
        schema_request_id,
        "CREATE TABLE IF NOT EXISTS rust_managed_contract (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
    );
    execute_committed_or_resolve(&mut db, &schema)?;

    let insert = ManagedExecuteRequest::sql(
        request_id,
        "INSERT INTO rust_managed_contract (id, value) VALUES (?, ?)",
    )
    .args([json!(row_id), json!(VALUE)]);
    execute_committed_or_resolve(&mut db, &insert)?;

    let written = query_row(&db, row_id)?;
    if !row_matches(&written, row_id, VALUE) {
        return Err("managed_sql_ack_mismatch".into());
    }
    persist_marker(marker_path, &AckMarker { pod_uid, row_id })?;

    db.close().map_err(|error| error.code)?;
    println!("PASS: managed Rust FFI SQL contract");
    Ok(())
}

fn contract_request_ids(pod_uid: &str, row_id: i64) -> (String, String) {
    let pod_tag: String = pod_uid.chars().take(8).collect();
    let request_id = format!("rust-{pod_tag}-{row_id}");
    let schema_request_id = format!("{request_id}-schema");
    (request_id, schema_request_id)
}

#[derive(serde::Deserialize, serde::Serialize)]
struct AckMarker {
    pod_uid: String,
    row_id: i64,
}

fn query_row(db: &ManagedDb, row_id: i64) -> Result<rhizadb::QueryResult, String> {
    db.query(
        &ManagedQueryRequest::new("SELECT id, value FROM rust_managed_contract WHERE id = ?")
            .args([json!(row_id)])
            .consistency("linearizable"),
    )
    .map_err(|error| error.code)
}

fn row_matches(rows: &rhizadb::QueryResult, row_id: i64, value: &str) -> bool {
    rows.rows.len() == 1
        && rows.rows[0].len() == 2
        && rows.rows[0][0].as_i64() == Some(row_id)
        && rows.rows[0][1].as_str() == Some(value)
}

fn persist_marker(path: &Path, marker: &AckMarker) -> Result<(), String> {
    let mut temporary = path.as_os_str().to_os_string();
    temporary.push(format!(".{}.tmp", std::process::id()));
    let temporary = std::path::PathBuf::from(temporary);
    let bytes = serde_json::to_vec(marker).map_err(|_| "marker_encode_failed")?;
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|_| "marker_create_failed")?;
        file.write_all(&bytes).map_err(|_| "marker_write_failed")?;
        file.sync_all().map_err(|_| "marker_sync_failed")?;
        fs::rename(&temporary, path).map_err(|_| "marker_publish_failed")?;
        let directory = OpenOptions::new()
            .read(true)
            .open(path.parent().ok_or("marker_path_invalid")?)
            .map_err(|_| "marker_directory_open_failed")?;
        directory
            .sync_all()
            .map_err(|_| "marker_directory_sync_failed")
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map_err(str::to_owned)
}

fn execute_committed_or_resolve(
    db: &mut ManagedDb,
    request: &ManagedExecuteRequest,
) -> Result<(), String> {
    let receipt = match db.execute(request) {
        Ok(receipt) => receipt,
        Err(error) if error.code == "commit_unknown" => {
            // Resolve the retained original request, but do not claim an ACK
            // without an explicit committed receipt or submit a new request.
            db.recover_pending().map_err(|error| error.code)?;
            return Err("commit_unknown".into());
        }
        Err(error) => return Err(error.code),
    };
    receipt.require_committed().map_err(|error| error.code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_sql_ack_requires_present_nonzero_slot() {
        assert_eq!(
            mixed_sql_ack_slot(None).unwrap_err(),
            "mixed_sql_slot_missing"
        );
        assert_eq!(
            mixed_sql_ack_slot(Some(0)).unwrap_err(),
            "mixed_sql_slot_zero"
        );
        assert_eq!(mixed_sql_ack_slot(Some(17)).unwrap(), 17);
        assert_eq!(mixed_sql_ack_slot(Some(u64::MAX)).unwrap(), u64::MAX);
    }

    #[test]
    fn mixed_unknown_requires_exact_original_not_none_or_observation() {
        assert!(exact_mixed_recovery(ManagedRecoveryResult::None, &[0xff], 0).is_err());
        let result = || ManagedRecoveryResult::Kv {
            request_id: vec![0xff],
            receipt: ManagedKVReceipt {
                slot: 17,
                status: "committed".into(),
                applied: false,
                error_code: None,
                retry_through_slot: Some(100),
            },
        };
        assert!(exact_mixed_recovery(result(), &[0xfe], 0).is_err());
        assert!(exact_mixed_recovery(result(), &[0xff], 18).is_err());
        assert!(!exact_mixed_recovery(result(), &[0xff], 17).unwrap().applied);
        assert_eq!(exact_mixed_recovery(result(), &[0xff], 0).unwrap().slot, 17);
        let sql: ExecuteReturningResult =
            serde_json::from_value(json!({"slot":17,"status":"committed","statements":[]}))
                .unwrap();
        assert!(exact_mixed_recovery(
            ManagedRecoveryResult::Sql {
                request_id: vec![0xff],
                result: sql
            },
            &[0xff],
            17
        )
        .is_err());
    }

    #[test]
    fn mixed_intent_is_fixed_raw_nullable_and_ttl_zero() {
        let requests = mixed_requests();
        assert_eq!(requests.len(), 5);
        assert_eq!(requests[0].1.request_id, vec![0xff]);
        assert_eq!(requests[1].1.request_id, vec![0xfe]);
        assert_eq!(requests[2].1.request_id, "\u{fffd}".as_bytes());
        assert_eq!(requests[3].1.value, Some(vec![]));
        assert_ne!(requests[0].2, requests[2].2);
        assert_eq!(requests[2].1.value, None);
        assert!(requests.iter().all(|(_, r, _, _)| r.ttl_ms == 0));
        assert!(!requests[3].3 && !requests[4].3);
        assert!(requests[4].2.is_none());
    }

    #[test]
    fn managed_request_ids_fit_go_limit_and_are_pod_scoped() {
        let (write, schema) =
            contract_request_ids("12345678-1234-1234-1234-123456789abc", i64::MAX);
        assert!(write.len() <= 64, "write request ID exceeds Go limit");
        assert!(schema.len() <= 64, "schema request ID exceeds Go limit");
        assert_ne!(
            write,
            contract_request_ids("87654321-1234-1234-1234-123456789abc", i64::MAX).0
        );
    }
}
