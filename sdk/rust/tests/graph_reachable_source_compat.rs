use rhizadb::{Db, Error, GraphReachableDirection, GraphReachableRequest, ManagedDb};
use serde_json::{json, Value};

#[test]
fn graph_reachable_additions_keep_existing_source_and_wire_signatures() {
    let request = GraphReachableRequest {
        start_label: "Concept".into(),
        start_property: "key".into(),
        start_value: json!("child"),
        edge_type: "REL".into(),
        node_label: Some("Concept".into()),
        node_filters: None,
        result_property: "key".into(),
        max_depth: 1,
        max_results: 10,
        max_scanned_edges: 10,
        max_bytes: 1024,
        require_applied_slot: None,
    };

    let _: fn(&Db, GraphReachableRequest) -> Result<Value, Error> = Db::graph_reachable;
    let _: fn(&ManagedDb, GraphReachableRequest) -> Result<Value, Error> =
        ManagedDb::graph_reachable;
    let _: fn(&Db, GraphReachableRequest, GraphReachableDirection) -> Result<Value, Error> =
        Db::graph_reachable_directed;
    let _: fn(&ManagedDb, GraphReachableRequest, GraphReachableDirection) -> Result<Value, Error> =
        ManagedDb::graph_reachable_directed;

    assert_eq!(serde_json::to_value(request).unwrap()["edge_type"], "REL");
}
