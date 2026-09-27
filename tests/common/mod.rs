use amitoki_relay::{Frame, Relay, RelayContext, RelayPlugin};
use amitoki_relay_postgres::PostgresPlugin;
use bytes::Bytes;
use serde_json::json;
use std::sync::Arc;
use tokio_postgres::Client;
mod database;
pub use database::database;
use database::CONNECTION_ENV;

pub async fn connect(channel: &str, node: &str) -> Result<Arc<dyn Relay>, amitoki_relay::RelayError> {
    connect_with_options(channel, node, json!({})).await
}

pub async fn connect_with_options(channel: &str, node: &str, mut options: serde_json::Value) -> Result<Arc<dyn Relay>, amitoki_relay::RelayError> {
    options["connection_env"] = json!(CONNECTION_ENV);
    PostgresPlugin
        .connect(
            RelayContext {
                channel: channel.into(),
                node_id: node.into(),
            },
            options,
        )
        .await
}

pub fn frame(value: u8) -> Frame {
    Frame::new(Bytes::from(vec![value; 1024])).unwrap()
}

pub async fn remove_channel(client: &Client, channel: &str) {
    client.execute("DELETE FROM stegrdb_relay.nodes WHERE channel = $1", &[&channel]).await.unwrap();
    client.execute("DELETE FROM stegrdb_relay.frames WHERE channel = $1", &[&channel]).await.unwrap();
}
