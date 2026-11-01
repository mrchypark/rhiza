use rhizadb::{ManagedDb, ManagedExecuteRequest, ManagedQueryRequest};
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
    use super::contract_request_ids;

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
