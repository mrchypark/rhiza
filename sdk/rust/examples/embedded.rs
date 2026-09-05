use rhizadb::{Config, Db};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Db::open(Config::new("./rhiza-rust-data").node_id("embedded-rust"))?;
    db.execute(
        "schema",
        "CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, body TEXT)",
        json!([]),
    )?
    .require_committed()?;
    db.execute(
        "note-1",
        "INSERT OR REPLACE INTO notes VALUES (?, ?)",
        json!([1, "hello from Rust"]),
    )?
    .require_committed()?;
    println!("{:?}", db.query("SELECT body FROM notes", json!([]))?.rows);
    db.close()?;
    Ok(())
}
