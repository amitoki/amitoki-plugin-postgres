mod common;

use common::{connect, connect_with_options, database, frame, remove_channel};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
#[ignore = "専用DBでscripts/test-postgres.shを実行する"]
async fn first_registration_replays_only_recent_frames_and_zero_disables_history() {
    let channel = Uuid::new_v4().to_string();
    let sender = connect(&channel, "sender").await.unwrap();
    let (client, connection) = database().await;
    let packets = [frame(1), frame(2), frame(3)];
    sender.publish(&packets).await.unwrap();
    client
        .execute(
            "UPDATE stegrdb_relay.frames SET created_at = statement_timestamp() - interval '1 day' WHERE channel=$1 AND id=$2",
            &[&channel, &packets[0].id],
        )
        .await
        .unwrap();
    // DB時計のずれなどで未来の日付があっても、0なら履歴を取り込まない。
    client
        .execute(
            "UPDATE stegrdb_relay.frames SET created_at = statement_timestamp() + interval '1 day' WHERE channel=$1 AND id=$2",
            &[&channel, &packets[2].id],
        )
        .await
        .unwrap();
    let recent = connect_with_options(&channel, "recent", json!({"replay_window_ms": 60_000})).await.unwrap();
    let disabled = connect_with_options(&channel, "disabled", json!({"replay_window_ms": 0})).await.unwrap();
    let deliveries = recent.receive(10).await.unwrap();
    assert_eq!(
        deliveries.iter().map(|delivery| &delivery.frame).collect::<Vec<_>>(),
        packets[1..].iter().collect::<Vec<_>>()
    );
    assert!(disabled.receive(10).await.unwrap().is_empty());
    let fresh = frame(4);
    sender.publish(std::slice::from_ref(&fresh)).await.unwrap();
    assert_eq!(disabled.receive(1).await.unwrap()[0].frame, fresh);
    remove_channel(&client, &channel).await;
    connection.abort();
}

#[tokio::test]
#[ignore = "専用DBでscripts/test-postgres.shを実行する"]
async fn receive_respects_zero_and_page_limits_with_a_large_history() {
    let channel = Uuid::new_v4().to_string();
    let sender = connect(&channel, "sender").await.unwrap();
    let receiver = connect_with_options(&channel, "receiver", json!({"replay_window_ms": 0})).await.unwrap();
    let (client, connection) = database().await;
    client.execute("INSERT INTO stegrdb_relay.frames(channel,id,sender,payload) SELECT $1,md5(value::text)::uuid,'sender',decode(repeat('ab',64),'hex') FROM generate_series(1,10000) AS value", &[&channel]).await.unwrap();
    let packets: Vec<_> = (0..130).map(frame).collect();
    sender.publish(&packets).await.unwrap();
    assert!(receiver.receive(0).await.unwrap().is_empty());
    let first = receiver.receive(128).await.unwrap();
    assert_eq!(first.iter().map(|delivery| &delivery.frame).collect::<Vec<_>>(), packets[..128].iter().collect::<Vec<_>>());
    receiver.acknowledge(&first.into_iter().map(|delivery| delivery.receipt).collect::<Vec<_>>()).await.unwrap();
    let last = receiver.receive(128).await.unwrap();
    assert_eq!(last.iter().map(|delivery| &delivery.frame).collect::<Vec<_>>(), packets[128..].iter().collect::<Vec<_>>());
    remove_channel(&client, &channel).await;
    connection.abort();
}
