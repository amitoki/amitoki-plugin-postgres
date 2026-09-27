mod common;

use common::{connect, connect_with_options, database, frame, remove_channel};
use serde_json::json;
use std::time::Duration;
use tokio_postgres::Client;
use uuid::Uuid;

// 実ワーカの完了を行の消滅で確認し、固定sleepで成功を推測しない。
const CLEANUP_DEADLINE: Duration = Duration::from_secs(15);
const CLEANUP_POLL: Duration = Duration::from_millis(10);

async fn wait_for_removal(client: &Client, channel: &str, id: Uuid) {
    tokio::time::timeout(CLEANUP_DEADLINE, async {
        while client.query_one("SELECT EXISTS(SELECT 1 FROM stegrdb_relay.frames WHERE channel=$1 AND id=$2)", &[&channel, &id]).await.unwrap().get::<_, bool>(0) {
            tokio::time::sleep(CLEANUP_POLL).await;
        }
    })
    .await
    .expect("設定した期限切れフレームが掃除されること");
}

#[tokio::test]
#[ignore = "専用DBでscripts/test-postgres.shを実行する"]
async fn background_cleanup_preserves_pending_and_allows_a_new_delivery_after_the_deduplication_window() {
    let channel = Uuid::new_v4().to_string();
    let settings = json!({"retention_ms": 60_000, "cleanup_interval_ms": 100, "cleanup_batch_size": 1});
    let sender = connect_with_options(&channel, "sender", settings.clone()).await.unwrap();
    let receiver = connect_with_options(&channel, "receiver", settings).await.unwrap();
    let (client, connection) = database().await;
    let packet = frame(1);
    sender.publish(std::slice::from_ref(&packet)).await.unwrap();
    let sentinel = frame(2);
    client
        .execute(
            "INSERT INTO stegrdb_relay.frames(channel,id,sender,payload,created_at) VALUES ($1,$2,'sender',$3,statement_timestamp()-interval '5 minutes')",
            &[&channel, &sentinel.id, &sentinel.bytes.as_ref()],
        )
        .await
        .unwrap();
    client
        .execute(
            "UPDATE stegrdb_relay.frames SET created_at=statement_timestamp()-interval '5 minutes' WHERE channel=$1",
            &[&channel],
        )
        .await
        .unwrap();
    wait_for_removal(&client, &channel, sentinel.id).await;
    let deliveries = receiver.receive(10).await.unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].frame, packet);
    let old_receipt = deliveries[0].receipt.clone();
    receiver.acknowledge(std::slice::from_ref(&old_receipt)).await.unwrap();
    wait_for_removal(&client, &channel, packet.id).await;
    sender.publish(std::slice::from_ref(&packet)).await.unwrap();
    receiver.acknowledge(std::slice::from_ref(&old_receipt)).await.unwrap();
    let renewed = receiver.receive(10).await.unwrap();
    assert_eq!(renewed[0].frame, packet);
    assert_ne!(renewed[0].receipt, old_receipt);
    receiver.acknowledge(&[renewed[0].receipt.clone()]).await.unwrap();
    assert!(receiver.receive(10).await.unwrap().is_empty());
    remove_channel(&client, &channel).await;
    connection.abort();
}

#[tokio::test]
#[ignore = "専用DBでscripts/test-postgres.shを実行する"]
async fn a_restarted_node_updates_its_retention_choice_without_replaying_acknowledged_frames() {
    let channel = Uuid::new_v4().to_string();
    let sender = connect(&channel, "sender").await.unwrap();
    let receiver = connect(&channel, "receiver").await.unwrap();
    let packet = frame(3);
    sender.publish(&[packet]).await.unwrap();
    let deliveries = receiver.receive(10).await.unwrap();
    receiver.acknowledge(&[deliveries[0].receipt.clone()]).await.unwrap();
    drop(receiver);
    let receiver = tokio::time::timeout(CLEANUP_DEADLINE, async {
        loop {
            if let Ok(receiver) = connect_with_options(&channel, "receiver", json!({"retention_ms": 60_000})).await {
                break receiver;
            }
            tokio::time::sleep(CLEANUP_POLL).await;
        }
    })
    .await
    .unwrap();
    assert!(receiver.receive(10).await.unwrap().is_empty());
    let (client, connection) = database().await;
    let retention: i64 = client.query_one("SELECT retention_ms FROM stegrdb_relay.nodes WHERE channel=$1 AND node_id='receiver'", &[&channel]).await.unwrap().get(0);
    assert_eq!(retention, 60_000);
    remove_channel(&client, &channel).await;
    connection.abort();
}
