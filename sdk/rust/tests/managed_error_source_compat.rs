use rhizadb::{
    Error, ExecuteReturningResult, ManagedDb, ManagedExecuteRequest, ManagedQueryRequest,
    QueryResult,
};

#[test]
fn existing_error_literals_and_exhaustive_destructuring_compile() {
    let error = Error {
        code: "example".into(),
        message: "old source".into(),
    };
    let Error { code, message } = error;
    assert_eq!(code, "example");
    assert_eq!(message, "old source");
    let _: fn(&ManagedDb, &ManagedExecuteRequest) -> Result<ExecuteReturningResult, Error> =
        ManagedDb::execute;
    let _: fn(&ManagedDb, &ManagedQueryRequest) -> Result<QueryResult, Error> = ManagedDb::query;
    let _: fn(&ManagedDb) -> Result<(), Error> = ManagedDb::recover_pending;
    let _: fn(&ManagedDb) -> Result<bool, Error> = ManagedDb::ready;
}
