//! Synchronous embedded access to Rhiza. For async applications, call this SDK from
//! the runtime's blocking facility (for example, `tokio::task::spawn_blocking`).

use std::{ffi::c_void, fmt, path::Path};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Map, Value};

const MAX_INPUT_BYTES: usize = 16 << 20;
const DEFAULT_TIMEOUT_MS: u64 = 30_000;

#[repr(C)]
#[derive(Clone, Copy)]
struct RhizaBuffer {
    data: *mut c_void,
    len: usize,
}

extern "C" {
    fn RhizaOpen(data: *mut c_void, len: usize) -> RhizaBuffer;
    fn RhizaCall(handle: u64, data: *mut c_void, len: usize, timeout_ms: u64) -> RhizaBuffer;
    fn RhizaClose(handle: u64) -> RhizaBuffer;
    fn RhizaFree(buffer: RhizaBuffer);
}

/// An error returned by Rhiza. `code` preserves transport-independent Go API
/// semantics, including `commit_unknown`.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Error {
    pub code: String,
    pub message: String,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for Error {}

type Result<T> = std::result::Result<T, Error>;

/// JSON-backed Go `rhiza.Config`. Keys are Go exported field names such as
/// `DataDir`, `NodeID`, and `ObjStoreAccessKey`.
pub struct Config {
    fields: Map<String, Value>,
}
impl Config {
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        let mut fields = Map::new();
        fields.insert(
            "DataDir".into(),
            Value::String(data_dir.as_ref().display().to_string()),
        );
        Self { fields }
    }
    pub fn node_id(mut self, value: impl Into<String>) -> Self {
        self.fields
            .insert("NodeID".into(), Value::String(value.into()));
        self
    }
    pub fn cluster_id(mut self, value: impl Into<String>) -> Self {
        self.fields
            .insert("ClusterID".into(), Value::String(value.into()));
        self
    }
    pub fn bind_addr(mut self, value: impl Into<String>) -> Self {
        self.fields
            .insert("BindAddr".into(), Value::String(value.into()));
        self
    }
    pub fn peer_addr(mut self, value: impl Into<String>) -> Self {
        self.fields
            .insert("PeerAddr".into(), Value::String(value.into()));
        self
    }
    /// Sets another documented Go config field. Unknown field names are rejected
    /// by the native bridge when the DB is opened.
    pub fn set_option(mut self, field: impl Into<String>, value: Value) -> Self {
        self.fields.insert(field.into(), value);
        self
    }
    fn into_json(self) -> Value {
        Value::Object(self.fields)
    }
}
impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fields: Vec<_> = self.fields.keys().collect();
        f.debug_struct("Config")
            .field("field_names", &fields)
            .finish()
    }
}

/// An opened embedded database. It is not `Clone`; wrap it in `Arc` when a
/// shared handle is required. It is `Send + Sync` because the Go registry owns
/// the opaque handle and serializes lifetime transitions.
pub struct Db {
    handle: u64,
    closed: bool,
}
impl Db {
    pub fn open(config: Config) -> Result<Self> {
        let value = native_open(config.into_json())?;
        let handle = value
            .get("handle")
            .and_then(Value::as_u64)
            .ok_or_else(|| Error {
                code: "invalid_response".into(),
                message: "open response has no handle".into(),
            })?;
        Ok(Self {
            handle,
            closed: false,
        })
    }
    pub fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        let result = native_close(self.handle);
        self.closed = true;
        result
    }
    pub fn call<T: DeserializeOwned>(&self, operation: &str, request: Value) -> Result<T> {
        self.call_timeout(operation, request, DEFAULT_TIMEOUT_MS)
    }
    pub fn call_timeout<T: DeserializeOwned>(
        &self,
        operation: &str,
        request: Value,
        timeout_ms: u64,
    ) -> Result<T> {
        if self.closed {
            return Err(Error {
                code: "closed".into(),
                message: "Rhiza DB is closed".into(),
            });
        }
        native_call(
            self.handle,
            json!({"operation": operation, "request": request}),
            timeout_ms,
        )
    }
    pub fn execute(&self, request_id: &str, sql: &str, args: Value) -> Result<MutationReceipt> {
        self.mutation("execute", request_id, sql, args)
    }
    pub fn execute_returning(
        &self,
        request_id: &str,
        sql: &str,
        args: Value,
    ) -> Result<ExecuteReturningResult> {
        self.call(
            "execute_returning",
            json!({"request_id":request_id,"sql":sql,"args":args}),
        )
    }
    fn mutation(
        &self,
        operation: &str,
        request_id: &str,
        sql: &str,
        args: Value,
    ) -> Result<MutationReceipt> {
        self.call(
            operation,
            json!({"request_id":request_id,"sql":sql,"args":args}),
        )
    }
    pub fn query(&self, sql: &str, args: Value) -> Result<QueryResult> {
        self.call(
            "query",
            json!({"sql":sql,"args":args,"consistency":"local"}),
        )
    }
    pub fn kv_get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let response: KVGetResponse =
            self.call("kv_get", json!({"key":key,"consistency":"local"}))?;
        if !response.found {
            return Ok(None);
        }
        match response.value {
            Some(value) => STANDARD
                .decode(value)
                .map_err(|e| Error {
                    code: "invalid_response".into(),
                    message: e.to_string(),
                })
                .map(Some),
            None => Ok(Some(Vec::new())),
        }
    }
    pub fn kv_put(&self, request_id: &str, key: &str, value: &[u8]) -> Result<MutationReceipt> {
        self.kv_mutation("kv_put", request_id, key, value, None)
    }
    pub fn kv_delete(&self, request_id: &str, key: &str) -> Result<MutationReceipt> {
        self.call("kv_delete", json!({"request_id":request_id,"key":key}))
    }
    pub fn kv_cas(
        &self,
        request_id: &str,
        key: &str,
        expected: &[u8],
        value: &[u8],
    ) -> Result<MutationReceipt> {
        self.kv_mutation("kv_cas", request_id, key, value, Some(expected))
    }
    fn kv_mutation(
        &self,
        operation: &str,
        request_id: &str,
        key: &str,
        value: &[u8],
        expected: Option<&[u8]>,
    ) -> Result<MutationReceipt> {
        let mut request = json!({"request_id":request_id,"key":key,"value":STANDARD.encode(value)});
        if let Some(expected) = expected {
            request["expected"] = Value::String(STANDARD.encode(expected));
            request["expected_exists"] = Value::Bool(true);
        }
        self.call(operation, request)
    }
    pub fn graph_execute(
        &self,
        request_id: &str,
        cypher: &str,
        args: Value,
    ) -> Result<MutationReceipt> {
        self.call(
            "graph_execute",
            json!({"request_id":request_id,"cypher":cypher,"args":args}),
        )
    }
    pub fn graph_query(&self, cypher: &str, args: Value) -> Result<QueryResult> {
        self.call(
            "graph_query",
            json!({"cypher":cypher,"args":args,"consistency":"local"}),
        )
    }
    pub fn graph_reachable(&self, request: GraphReachableRequest) -> Result<Value> {
        self.call("graph_reachable", serde_json::to_value(request).unwrap())
    }
    pub fn request_status(&self, kind: &str, request_id: &str) -> Result<RequestStatus> {
        self.call(
            "request_status",
            json!({"kind":kind,"request_id":request_id}),
        )
    }
    pub fn ready(&self) -> Result<bool> {
        self.call("ready", json!({}))
    }
    pub fn object_store_stats(&self) -> Result<Value> {
        self.call("object_store_stats", json!({}))
    }
}
impl Drop for Db {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct MutationReceipt {
    pub slot: Option<u64>,
    pub status: Option<String>,
    pub applied: Option<bool>,
    pub error_code: Option<String>,
    pub rows_affected: Option<i64>,
    pub last_insert_id: Option<i64>,
    pub retry_through_slot: Option<u64>,
}
impl MutationReceipt {
    pub fn require_committed(&self) -> Result<()> {
        match self.status.as_deref() {
            Some("committed") => Ok(()),
            Some("rejected") => Err(Error {
                code: self.error_code.clone().unwrap_or_else(|| "rejected".into()),
                message: "mutation was rejected".into(),
            }),
            Some(status) => Err(Error {
                code: "invalid_response".into(),
                message: format!("unknown mutation status {status}"),
            }),
            None => Err(Error {
                code: "invalid_response".into(),
                message: "mutation response has no status".into(),
            }),
        }
    }
}
#[derive(Clone, Debug, Deserialize)]
pub struct ExecuteReturningResult {
    #[serde(flatten)]
    pub receipt: MutationReceipt,
    #[serde(default)]
    pub statements: Vec<SQLStatementResult>,
}
impl ExecuteReturningResult {
    pub fn require_committed(&self) -> Result<()> {
        self.receipt.require_committed()
    }
}
#[derive(Clone, Debug, Deserialize)]
pub struct SQLStatementResult {
    pub rows_affected: Option<i64>,
    pub last_insert_id: Option<i64>,
    #[serde(default, deserialize_with = "null_default")]
    pub columns: Vec<String>,
    #[serde(default, deserialize_with = "null_default")]
    pub rows: Vec<Vec<Value>>,
}
#[derive(Clone, Debug, Deserialize)]
pub struct QueryResult {
    #[serde(default, deserialize_with = "null_default")]
    pub columns: Vec<String>,
    #[serde(default, deserialize_with = "null_default")]
    pub rows: Vec<Vec<Value>>,
    pub applied_slot: Option<u64>,
    pub consensus_tip: Option<u64>,
}
#[derive(Clone, Debug, Deserialize)]
struct KVGetResponse {
    found: bool,
    value: Option<String>,
}
#[derive(Clone, Debug, Deserialize)]
pub struct RequestStatus {
    pub state: String,
    pub tip: u64,
    pub receipt: Option<MutationReceipt>,
}
#[derive(Clone, Debug, Serialize)]
pub struct GraphReachableRequest {
    pub start_label: String,
    pub start_property: String,
    pub start_value: Value,
    pub edge_type: String,
    pub node_label: Option<String>,
    pub node_filters: Option<Map<String, Value>>,
    pub result_property: String,
    pub max_depth: u32,
    pub max_results: u64,
    pub max_scanned_edges: u64,
    pub max_bytes: u64,
    pub require_applied_slot: Option<u64>,
}

struct BufferGuard(RhizaBuffer);
impl BufferGuard {
    unsafe fn bytes(&self) -> &[u8] {
        if self.0.data.is_null() {
            &[]
        } else {
            std::slice::from_raw_parts(self.0.data.cast(), self.0.len)
        }
    }
}
impl Drop for BufferGuard {
    fn drop(&mut self) {
        unsafe { RhizaFree(self.0) }
    }
}
fn encode(value: Value) -> Result<Vec<u8>> {
    let data = serde_json::to_vec(&value).map_err(internal)?;
    if data.len() > MAX_INPUT_BYTES {
        return Err(Error {
            code: "invalid_request".into(),
            message: "encoded FFI input exceeds 16 MiB".into(),
        });
    }
    Ok(data)
}
fn decode<T: DeserializeOwned>(buffer: RhizaBuffer) -> Result<T> {
    let guard = BufferGuard(buffer);
    let mut envelope: Map<String, Value> =
        serde_json::from_slice(unsafe { guard.bytes() }).map_err(internal)?;
    match (envelope.remove("data"), envelope.remove("error")) {
        (Some(data), None) => serde_json::from_value(data).map_err(internal),
        (None, Some(error)) => Err(serde_json::from_value(error).map_err(internal)?),
        _ => Err(Error {
            code: "invalid_response".into(),
            message: "native response must contain exactly one of data or error".into(),
        }),
    }
}
fn native_open(value: Value) -> Result<Value> {
    let mut input = encode(value)?;
    decode(unsafe { RhizaOpen(input.as_mut_ptr().cast(), input.len()) })
}
fn native_call<T: DeserializeOwned>(handle: u64, value: Value, timeout_ms: u64) -> Result<T> {
    let mut input = encode(value)?;
    decode(unsafe { RhizaCall(handle, input.as_mut_ptr().cast(), input.len(), timeout_ms) })
}
fn native_close(handle: u64) -> Result<()> {
    decode(unsafe { RhizaClose(handle) })
}
fn internal(error: impl fmt::Display) -> Error {
    Error {
        code: "invalid_response".into(),
        message: error.to_string(),
    }
}
fn null_default<'de, D, T>(deserializer: D) -> std::result::Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        path::PathBuf,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        thread,
        time::{SystemTime, UNIX_EPOCH},
    };
    fn path() -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "rhiza-rust-{}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }
    fn db() -> Db {
        Db::open(Config::new(path()).node_id("rust-test")).unwrap()
    }
    #[test]
    fn sql_kv_graph_and_reopen() {
        let mut db = db();
        db.execute(
            "schema",
            "CREATE TABLE values_test (id INTEGER PRIMARY KEY)",
            json!([]),
        )
        .unwrap()
        .require_committed()
        .unwrap();
        db.execute(
            "integer",
            "INSERT INTO values_test VALUES (?)",
            json!([9007199254740993_i64]),
        )
        .unwrap()
        .require_committed()
        .unwrap();
        assert_eq!(
            db.query("SELECT id FROM values_test", json!([]))
                .unwrap()
                .rows[0][0]
                .as_i64(),
            Some(9007199254740993)
        );
        assert!(db
            .query("SELECT id FROM values_test WHERE 1 = 0", json!([]))
            .unwrap()
            .rows
            .is_empty());
        db.kv_put("binary", "nul", &[0, 255])
            .unwrap()
            .require_committed()
            .unwrap();
        assert_eq!(db.kv_get("nul").unwrap(), Some(vec![0, 255]));
        db.kv_put("empty", "empty", &[])
            .unwrap()
            .require_committed()
            .unwrap();
        assert_eq!(db.kv_get("empty").unwrap(), Some(vec![]));
        let returning = db
            .execute_returning(
                "returning",
                "INSERT INTO values_test VALUES (?) RETURNING id",
                json!([7]),
            )
            .unwrap();
        returning.require_committed().unwrap();
        assert_eq!(returning.statements[0].rows[0][0].as_i64(), Some(7));
        db.graph_execute("graph", "CREATE (:Item {name: 'rust'})", json!({}))
            .unwrap()
            .require_committed()
            .unwrap();
        assert_eq!(
            db.graph_query("MATCH (n:Item) RETURN n.name", json!({}))
                .unwrap()
                .rows
                .len(),
            1
        );
        assert!(db
            .graph_query("MATCH (n:Item {name: 'missing'}) RETURN n.name", json!({}))
            .unwrap()
            .rows
            .is_empty());
        db.call::<()>(
            "set_graph_stream_offset",
            json!({"request_id":"offset","stream":"events","consumer":"rust","sequence":0}),
        )
        .unwrap();
        assert_eq!(
            db.request_status("sql", "integer").unwrap().state,
            "committed"
        );
        let rejected = db
            .execute("bad", "INSERT INTO no_table VALUES (1)", json!([]))
            .unwrap();
        assert!(rejected.require_committed().is_err());
        assert!(MutationReceipt {
            slot: None,
            status: None,
            applied: None,
            error_code: None,
            rows_affected: None,
            last_insert_id: None,
            retry_through_slot: None
        }
        .require_committed()
        .is_err());
        assert_eq!(
            db.call::<Value>("unknown", json!({})).unwrap_err().code,
            "invalid_request"
        );
        db.close().unwrap();
    }
    #[test]
    fn drop_and_reopen_preserve_committed_data() {
        let path = path();
        {
            let db = Db::open(Config::new(&path).node_id("rust-reopen")).unwrap();
            db.execute(
                "schema",
                "CREATE TABLE persisted (value INTEGER)",
                json!([]),
            )
            .unwrap()
            .require_committed()
            .unwrap();
            db.execute("value", "INSERT INTO persisted VALUES (1)", json!([]))
                .unwrap()
                .require_committed()
                .unwrap();
        }
        let reopened = Db::open(Config::new(path).node_id("rust-reopen")).unwrap();
        assert_eq!(
            reopened
                .query("SELECT value FROM persisted", json!([]))
                .unwrap()
                .rows[0][0]
                .as_i64(),
            Some(1)
        );
    }
    #[test]
    fn close_and_concurrent_calls() {
        let db = Arc::new(db());
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let db = db.clone();
                thread::spawn(move || assert!(db.ready().unwrap()))
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        let mut db = match Arc::try_unwrap(db) {
            Ok(db) => db,
            Err(_) => panic!("workers retained Db"),
        };
        db.close().unwrap();
        assert_eq!(db.ready().unwrap_err().code, "closed");
    }
}
