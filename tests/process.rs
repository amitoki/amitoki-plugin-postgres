mod common;

use amitoki_plugin_sdk::ProcessRelay;
use amitoki_relay::{Relay, RelayContext};
use amitoki_relay_postgres::manifest::manifest;
use common::{connect, database, frame, remove_channel};
use serde_json::json;
use std::path::Path;
use uuid::Uuid;

#[tokio::test]
#[ignore = "専用DBでscripts/test-postgres.shを実行する"]
async fn the_executable_delivers_payloads_and_acknowledges_opaque_receipts_over_ipc() {
    let channel = Uuid::new_v4().to_string();
    let sender = connect(&channel, "sender").await.unwrap();
    let receiver = ProcessRelay::connect(
        Path::new(env!("CARGO_BIN_EXE_amitoki-plugin-postgres")),
        &manifest(),
        (
            RelayContext {
                channel: channel.clone(),
                node_id: "receiver".into(),
            },
            json!({"connection_env": "AMITOKI_TEST_POSTGRES_URL"}),
        ),
    )
    .await
    .unwrap();
    let packets: Vec<_> = (0..128).map(frame).collect();
    sender.publish(&packets).await.unwrap();
    let deliveries = receiver.receive(128).await.unwrap();
    assert_eq!(deliveries.iter().map(|delivery| &delivery.frame).collect::<Vec<_>>(), packets.iter().collect::<Vec<_>>());
    let receipts: Vec<_> = deliveries.into_iter().map(|delivery| delivery.receipt).collect();
    receiver.acknowledge(&receipts).await.unwrap();
    receiver.acknowledge(&receipts).await.unwrap();
    sender.publish(&packets).await.unwrap();
    assert!(receiver.receive(128).await.unwrap().is_empty());
    drop(receiver);
    let (client, connection) = database().await;
    remove_channel(&client, &channel).await;
    connection.abort();
}
