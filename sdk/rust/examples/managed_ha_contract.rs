use rhizadb::{ManagedDb, ManagedExecuteRequest, ManagedQueryRequest};
use serde_json::json;
use std::{
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

fn main() {
    if let Err(code) = run() {
        eprintln!("FAIL: managed-ha-contract {code}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut db = ManagedDb::open_from_env().map_err(|error| error.code)?;
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if db.ready().map_err(|error| error.code)? {
            break;
        }
        if Instant::now() >= deadline {
            return Err("readiness_timeout".into());
        }
        thread::sleep(Duration::from_millis(250));
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
    let request_id = format!("rust-managed-contract-{row_id}");
    let schema = ManagedExecuteRequest::sql(
        format!("{request_id}-schema"),
        "CREATE TABLE IF NOT EXISTS rust_managed_contract (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
    );
    execute_committed_or_resolve(&mut db, &schema)?;

    let insert = ManagedExecuteRequest::sql(
        request_id,
        "INSERT INTO rust_managed_contract (id, value) VALUES (?, ?)",
    )
    .args([json!(row_id), json!("rust-managed-ffi-ack")]);
    execute_committed_or_resolve(&mut db, &insert)?;

    let written = db
        .query(
            &ManagedQueryRequest::new("SELECT id, value FROM rust_managed_contract WHERE id = ?")
                .args([json!(row_id)])
                .consistency("linearizable"),
        )
        .map_err(|error| error.code)?;
    if written.rows.len() != 1
        || written.rows[0].len() != 2
        || written.rows[0][0].as_i64() != Some(row_id)
        || written.rows[0][1].as_str() != Some("rust-managed-ffi-ack")
    {
        return Err("managed_sql_ack_mismatch".into());
    }

    db.close().map_err(|error| error.code)?;
    println!("PASS: managed Rust FFI SQL contract");
    Ok(())
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
