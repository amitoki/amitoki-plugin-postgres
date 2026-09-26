use stegrdb_relay_postgres::{manifest::manifest, PostgresPlugin};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match std::env::args().nth(1).as_deref() {
        Some("--schema") => print!("{}", include_str!("../schema.sql")),
        Some("--describe") => println!("{}", serde_json::to_string_pretty(&manifest())?),
        Some("--stdio") => stegrdb_plugin_sdk::serve(PostgresPlugin, manifest()).await?,
        _ => return Err("--stdio / --describe / --schemaを指定してください".into()),
    }
    Ok(())
}
