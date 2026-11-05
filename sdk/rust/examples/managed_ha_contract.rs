use base64::{engine::general_purpose::STANDARD, Engine as _};
use rhizadb::{
    ExecuteReturningResult, ManagedDb, ManagedExecuteRequest, ManagedKVReceipt, ManagedKVRequest,
    ManagedQueryRequest, ManagedRecoveryResult, ManagedRequestReceipt, MutationKind,
};
use rhizadb::{
    ManagedGraphCommand, ManagedGraphEvent, ManagedGraphOffset, ManagedGraphReceipt,
    ManagedGraphStreamRequest, ManagedGraphTrim, ManagedGraphValue,
};
use serde_json::json;
use std::collections::BTreeMap;
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
    if arguments == ["--graph-initial"] || arguments == ["--graph-verify"] {
        if let Err(code) = run_graph(arguments[0] == "--graph-initial") {
            eprintln!("FAIL: managed-ha-contract {code}");
            std::process::exit(1);
        }
        return;
    }
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

fn graph_commands() -> Vec<ManagedGraphCommand> {
    let event = |n| ManagedGraphEvent {
        stream: b"graph-ci-events".to_vec(),
        kind: b"ordered".to_vec(),
        payload: ManagedGraphValue::Integer(n),
    };
    vec![
        ManagedGraphCommand {
            request_id: vec![0xff, 1],
            cypher: b"CREATE (:GraphCI {id:'a', label:$label, n:$n})".to_vec(),
            args: Some(BTreeMap::from([
                (
                    b"label".to_vec(),
                    ManagedGraphValue::Bytes(b"alpha".to_vec()),
                ),
                (
                    b"n".to_vec(),
                    ManagedGraphValue::Float32Bits(0.1f32.to_bits()),
                ),
            ])),
            events: Some(vec![
                event(1),
                ManagedGraphEvent {
                    stream: b"graph-ci-trim".to_vec(),
                    kind: b"trimmed".to_vec(),
                    payload: ManagedGraphValue::Integer(1),
                },
            ]),
            ..Default::default()
        },
        ManagedGraphCommand {
            request_id: vec![0xfe, 2],
            cypher: b"CREATE (:GraphCI {id:'b', label:'beta', n:2})".to_vec(),
            events: Some(vec![
                event(2),
                ManagedGraphEvent {
                    stream: b"graph-ci-trim".to_vec(),
                    kind: b"trimmed".to_vec(),
                    payload: ManagedGraphValue::Integer(2),
                },
            ]),
            ..Default::default()
        },
        ManagedGraphCommand {
            request_id: "\u{fffd}".as_bytes().to_vec(),
            cypher: b"MATCH (a:GraphCI {id:'a'}), (b:GraphCI {id:'b'}) CREATE (a)-[:LINK]->(b)"
                .to_vec(),
            events: Some(vec![event(3)]),
            ..Default::default()
        },
        ManagedGraphCommand {
            request_id: b"graph-offset-max".to_vec(),
            stream_offset: Some(ManagedGraphOffset {
                stream: b"graph-ci-events".to_vec(),
                consumer: b"graph-ci-consumer".to_vec(),
                sequence: u64::MAX,
            }),
            ..Default::default()
        },
        ManagedGraphCommand {
            request_id: b"graph-trim-max".to_vec(),
            stream_trim: Some(ManagedGraphTrim {
                stream: b"graph-ci-trim".to_vec(),
                through_sequence: u64::MAX,
            }),
            ..Default::default()
        },
    ]
}

fn graph_receipt_json(receipt: &ManagedGraphReceipt) -> Result<serde_json::Value, String> {
    if receipt.slot == 0
        || receipt.status != "committed"
        || !receipt.applied
        || receipt.error_code.as_deref().is_some_and(|v| !v.is_empty())
        || receipt.retry_through_slot.is_none_or(|v| v < receipt.slot)
    {
        return Err("graph_receipt_not_ack".into());
    }
    Ok(
        json!({"slot":receipt.slot,"status":receipt.status,"applied":receipt.applied,"retry_through_slot":receipt.retry_through_slot}),
    )
}

fn graph_recover_exact(
    db: &ManagedDb,
    command: &ManagedGraphCommand,
    upper: u64,
    ordinal: usize,
) -> Result<ManagedGraphReceipt, String> {
    match db
        .recover_request(MutationKind::Graph, &command.request_id)
        .map_err(|e| {
            graph_step_error(
                "graph_recover",
                ordinal,
                &e.error().code,
                e.mutation_context()
                    .map(|c| (c.slot(), c.retry_through_slot())),
            )
        })? {
        ManagedRecoveryResult::Graph {
            request_id,
            receipt,
        } if request_id == command.request_id && (upper == 0 || receipt.slot <= upper) => {
            Ok(receipt)
        }
        _ => Err("graph_original_recovery_mismatch".into()),
    }
}

// A diagnostic must never interpolate Error.message, raw identity, or input.
// Missing context is not a known zero-slot outcome or permission to resubmit.
fn graph_step_error(
    stage: &str,
    ordinal: usize,
    code: &str,
    context: Option<(u64, u64)>,
) -> String {
    let stage = match stage {
        "graph_execute"
        | "graph_recover"
        | "graph_query"
        | "graph_stream_read"
        | "graph_stream_offset"
        | "graph_status"
        | "graph_encode_original"
        | "graph_encode_execution"
        | "sql_schema"
        | "sql_tuple"
        | "sql_query"
        | "close" => stage,
        _ => "unclassified",
    };
    let code = match code {
        "internal"
        | "commit_unknown"
        | "kv_recovery_required"
        | "kv_not_submitted"
        | "sql_recovery_required"
        | "sql_not_submitted"
        | "timeout"
        | "canceled"
        | "invalid_request"
        | "not_ready"
        | "quorum_unavailable"
        | "request_conflict"
        | "durability_unavailable"
        | "overloaded"
        | "mixed_sql_ack_missing"
        | "mixed_sql_slot_missing"
        | "mixed_sql_slot_zero" => code,
        _ => "unclassified",
    };
    let (slot, retry) = context.unwrap_or((0, 0));
    format!("graph_step stage={stage} ordinal={ordinal} code={code} context_known={} slot={slot} retry={retry}", context.is_some())
}

fn graph_state(db: &ManagedDb) -> Result<serde_json::Value, String> {
    let nodes = db
        .graph_query(
            "MATCH (n:GraphCI) RETURN n.id,n.label,n.n ORDER BY n.id",
            json!({}),
            "linearizable",
        )
        .map_err(|e| graph_step_error("graph_query", 1, &e.code, None))?;
    if nodes.rows
        != vec![
            vec![json!("a"), json!("alpha"), json!(0.1)],
            vec![json!("b"), json!("beta"), json!(2)],
        ]
    {
        return Err("graph_properties_mismatch".into());
    }
    let edges = db
        .graph_query(
            "MATCH (a:GraphCI)-[:LINK]->(b:GraphCI) RETURN a.id,b.id",
            json!({}),
            "linearizable",
        )
        .map_err(|e| graph_step_error("graph_query", 2, &e.code, None))?;
    if edges.rows != vec![vec![json!("a"), json!("b")]] {
        return Err("graph_edges_mismatch".into());
    }
    let events = db
        .graph_stream_read(&ManagedGraphStreamRequest {
            stream: "graph-ci-events".into(),
            after_sequence: 0,
            limit: 10,
            wait_ms: 0,
            consistency: "linearizable".into(),
        })
        .map_err(|e| graph_step_error("graph_stream_read", 1, &e.code, None))?;
    if events.records.len() != 3
        || events
            .records
            .windows(2)
            .any(|v| v[0].sequence >= v[1].sequence)
        || events
            .records
            .iter()
            .enumerate()
            .any(|(i, v)| v.kind != "ordered" || v.payload != json!(i + 1))
    {
        return Err("graph_events_mismatch".into());
    }
    let offset = db
        .graph_stream_offset("graph-ci-events", "graph-ci-consumer", "linearizable")
        .map_err(|e| graph_step_error("graph_stream_offset", 0, &e.code, None))?;
    if !offset.found || offset.sequence != u64::MAX {
        return Err("graph_offset_u64_mismatch".into());
    }
    let trimmed = db
        .graph_stream_read(&ManagedGraphStreamRequest {
            stream: "graph-ci-trim".into(),
            after_sequence: 0,
            limit: 10,
            wait_ms: 0,
            consistency: "linearizable".into(),
        })
        .map_err(|e| graph_step_error("graph_stream_read", 2, &e.code, None))?;
    if !trimmed.records.is_empty() {
        return Err("graph_trim_mismatch".into());
    }
    Ok(
        json!({"nodes":nodes.rows,"edges":edges.rows,"events":events.records.iter().map(|v|json!({"sequence":v.sequence,"kind":v.kind,"payload":v.payload})).collect::<Vec<_>>(),"offset":"18446744073709551615","trim_empty":true}),
    )
}

fn wait_managed_ready<E>(
    deadline: Instant,
    mut ready: impl FnMut(Duration) -> Result<bool, E>,
) -> Result<bool, E> {
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(false);
        }
        if ready(remaining)? {
            return Ok(true);
        }
        thread::sleep(
            deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(250)),
        );
    }
}

fn run_graph(initial: bool) -> Result<(), String> {
    let uid = env::var("POD_UID").map_err(|_| "pod_identity_missing")?;
    let producer = env::var("RHIZA_RUST_PRODUCER_UID").map_err(|_| "producer_identity_missing")?;
    if uid.is_empty() || producer.is_empty() || initial && uid != producer {
        return Err("graph_producer_identity_mismatch".into());
    }
    let mut db = ManagedDb::open_from_env().map_err(|e| format!("graph_open {}", e))?;
    let deadline = Instant::now() + Duration::from_secs(90);
    if !wait_managed_ready(deadline, |remaining| db.ready_with_timeout(remaining))
        .map_err(|e| format!("graph_ready {}", e))?
    {
        return Err("graph_not_ready".into());
    }
    let commands = graph_commands();
    let tuple = if initial {
        let mut cases = Vec::new();
        for (index, command) in commands.iter().enumerate() {
            let ordinal = index + 1;
            if command.request_id == b"graph-trim-max" {
                let before = db
                    .graph_stream_read(&ManagedGraphStreamRequest {
                        stream: "graph-ci-trim".into(),
                        after_sequence: 0,
                        limit: 10,
                        wait_ms: 0,
                        consistency: "linearizable".into(),
                    })
                    .map_err(|e| graph_step_error("graph_stream_read", ordinal, &e.code, None))?;
                if before.records.len() != 2
                    || before.records[0].sequence >= before.records[1].sequence
                    || before
                        .records
                        .iter()
                        .enumerate()
                        .any(|(i, r)| r.kind != "trimmed" || r.payload != json!(i + 1))
                {
                    return Err("graph_trim_nonempty_precondition_missing".into());
                }
            }
            let (receipt, recovered) = match db.graph_execute(command) {
                Ok(receipt) => (receipt, false),
                Err(e) if e.error().code == "commit_unknown" => {
                    eprintln!(
                        "GRAPH_STEP_DIAGNOSTIC {}",
                        graph_step_error(
                            "graph_execute",
                            ordinal,
                            &e.error().code,
                            e.mutation_context()
                                .map(|c| (c.slot(), c.retry_through_slot()))
                        )
                    );
                    let context = e
                        .mutation_context()
                        .ok_or("graph_unknown_context_missing")?;
                    if context.kind() != MutationKind::Graph
                        || context.request_id() != command.request_id
                    {
                        return Err("graph_unknown_context_mismatch".into());
                    }
                    (
                        graph_recover_exact(&db, command, context.slot(), ordinal)?,
                        true,
                    )
                }
                Err(e) => {
                    return Err(graph_step_error(
                        "graph_execute",
                        ordinal,
                        &e.error().code,
                        e.mutation_context()
                            .map(|c| (c.slot(), c.retry_through_slot())),
                    ))
                }
            };
            cases.push(json!({"request_id_b64":STANDARD.encode(&command.request_id),"expected_original_b64":STANDARD.encode(command.encode_original().map_err(|e|graph_step_error("graph_encode_original",ordinal,&e.code,None))?),"frame_b64":STANDARD.encode(command.encode_execution().map_err(|e|graph_step_error("graph_encode_execution",ordinal,&e.code,None))?),"receipt":graph_receipt_json(&receipt)?,"recovered":recovered}));
        }
        let tuple = json!({"version":1,"producer_uid":producer,"cases":cases,"trim_before_count":2,"state":graph_state(&db)?});
        mixed_sql(
            &db,
            &ManagedExecuteRequest::sql(
                "graph-ci-schema",
                "CREATE TABLE rust_graph_contract(id INTEGER PRIMARY KEY,tuple TEXT NOT NULL)",
            ),
        )
        .map_err(|code| graph_step_error("sql_schema", 0, &code, None))?;
        mixed_sql(
            &db,
            &ManagedExecuteRequest::sql(
                "graph-ci-row",
                "INSERT INTO rust_graph_contract VALUES(1,?)",
            )
            .args([json!(tuple.to_string())]),
        )
        .map_err(|code| graph_step_error("sql_tuple", 0, &code, None))?;
        tuple
    } else {
        let row = db
            .query(
                &ManagedQueryRequest::new("SELECT tuple FROM rust_graph_contract WHERE id=1")
                    .consistency("linearizable"),
            )
            .map_err(|e| graph_step_error("sql_query", 0, &e.code, None))?;
        let text = row
            .rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_str())
            .ok_or("graph_tuple_missing")?;
        if row.rows.len() != 1 || row.rows[0].len() != 1 || text.len() > 32768 {
            return Err("graph_tuple_invalid".into());
        }
        serde_json::from_str::<serde_json::Value>(text).map_err(|_| "graph_tuple_invalid")?
    };
    if tuple["version"] != json!(1)
        || tuple["trim_before_count"] != json!(2)
        || tuple["producer_uid"] != json!(producer)
        || tuple["cases"]
            .as_array()
            .is_none_or(|v| v.len() != commands.len())
        || tuple["state"] != graph_state(&db)?
    {
        return Err("graph_tuple_state_mismatch".into());
    }
    for (index, (command, case)) in commands
        .iter()
        .zip(tuple["cases"].as_array().ok_or("graph_cases_missing")?)
        .enumerate()
    {
        let ordinal = index + 1;
        if case["request_id_b64"] != json!(STANDARD.encode(&command.request_id))
            || case["expected_original_b64"]
                != json!(STANDARD.encode(command.encode_original().map_err(|e| {
                    graph_step_error("graph_encode_original", ordinal, &e.code, None)
                })?))
            || case["frame_b64"]
                != json!(STANDARD.encode(command.encode_execution().map_err(|e| {
                    graph_step_error("graph_encode_execution", ordinal, &e.code, None)
                })?))
        {
            return Err("graph_expected_original_changed".into());
        }
        let recovered = graph_recover_exact(&db, command, 0, ordinal)?;
        if case["receipt"] != graph_receipt_json(&recovered)? {
            return Err("graph_first_receipt_changed".into());
        }
        let status = db
            .request_status(MutationKind::Graph, &command.request_id)
            .map_err(|e| graph_step_error("graph_status", ordinal, &e.code, None))?;
        match status.receipt {
            Some(ManagedRequestReceipt::Graph(receipt))
                if graph_receipt_json(&receipt)? == case["receipt"] => {}
            _ => return Err("graph_status_receipt_mismatch".into()),
        }
    }
    db.close()
        .map_err(|e| graph_step_error("close", 0, &e.code, None))?;
    println!("RUST_GRAPH_CURRENT_UID={uid}");
    println!("RUST_GRAPH_TUPLE={tuple}");
    println!(
        "PASS: managed Rust FFI Graph {} state contract",
        if initial { "initial" } else { "verify" }
    );
    // This is state/receipt verification only. Forced UNKNOWN, canonical native
    // inspection and checkpoint/counter qualification are separate CI gates.
    Ok(())
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
    fn graph_readiness_waits_for_false_then_true() {
        let mut calls = 0;
        let deadline = Instant::now() + Duration::from_secs(2);
        let ready = wait_managed_ready(deadline, |remaining| {
            assert!(!remaining.is_zero());
            calls += 1;
            Ok::<bool, &str>(calls == 2)
        });
        assert_eq!(ready, Ok(true));
        assert_eq!(calls, 2);
    }

    #[test]
    fn graph_readiness_error_is_immediate() {
        let mut calls = 0;
        let ready = wait_managed_ready(Instant::now() + Duration::from_secs(2), |_| {
            calls += 1;
            Err::<bool, _>("original-error")
        });
        assert_eq!(ready, Err("original-error"));
        assert_eq!(calls, 1);
    }

    #[test]
    fn graph_readiness_exhausted_budget_does_not_call() {
        for deadline in [Instant::now(), Instant::now() - Duration::from_secs(1)] {
            let ready = wait_managed_ready(deadline, |_| -> Result<bool, &str> {
                panic!("readiness called after deadline")
            });
            assert_eq!(ready, Ok(false));
        }
    }

    #[test]
    fn graph_step_diagnostic_is_fixed_and_context_is_explicit() {
        assert_eq!(graph_step_error("graph_execute", 2, "commit_unknown", Some((0, u64::MAX))),
            "graph_step stage=graph_execute ordinal=2 code=commit_unknown context_known=true slot=0 retry=18446744073709551615");
        assert_eq!(graph_step_error("sql_tuple", 0, "commit_unknown", None),
            "graph_step stage=sql_tuple ordinal=0 code=commit_unknown context_known=false slot=0 retry=0");
        let private = "secret-SQL-https://private/path-raw-ID";
        let diagnostic = graph_step_error(private, 1, private, None);
        assert_eq!(diagnostic, "graph_step stage=unclassified ordinal=1 code=unclassified context_known=false slot=0 retry=0");
        assert!(!diagnostic.contains(private));
    }

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
