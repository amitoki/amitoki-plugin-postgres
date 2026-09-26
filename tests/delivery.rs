use amitoki_relay::{Frame, Relay, RelayContext, RelayPlugin};
use amitoki_relay_postgres::PostgresPlugin;
use bytes::Bytes;
use serde_json::json;
use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

const CONNECTION_ENV: &str = "AMITOKI_TEST_POSTGRES_URL";
const BATCH_SIZE: usize = 128;
const FRAME_COUNT: usize = 2048;

async fn database() -> (Client, tokio::task::JoinHandle<()>) {
    let connection_string = std::env::var(CONNECTION_ENV).expect("専用のPostgreSQLテスト接続を設定してください");
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls).await.expect("テスト用DBへの接続");
    let task = tokio::spawn(async move {
        connection.await.expect("テスト用DBの接続維持");
    });
    (client, task)
}

async fn connect(channel: &str, node: &str) -> Result<Arc<dyn Relay>, amitoki_relay::RelayError> {
    PostgresPlugin
        .connect(
            RelayContext {
                channel: channel.into(),
                node_id: node.into(),
            },
            json!({"connection_env": CONNECTION_ENV}),
        )
        .await
}

fn frame(value: u8) -> Frame {
    Frame::new(Bytes::from(vec![value; 1024])).unwrap()
}

async fn drain(relay: &Arc<dyn Relay>) -> HashSet<Uuid> {
    let mut received = HashSet::new();
    loop {
        let deliveries = relay.receive(BATCH_SIZE).await.unwrap();
        if deliveries.is_empty() {
            return received;
        }
        for delivery in &deliveries {
            assert!(received.insert(delivery.frame.id), "同一ノードへの重複配送");
        }
        let receipts: Vec<_> = deliveries.into_iter().map(|delivery| delivery.receipt).collect();
        relay.acknowledge(&receipts).await.unwrap();
        relay.acknowledge(&receipts).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "専用DBでscripts/test-postgres.shを実行する"]
async fn three_nodes_preserve_all_frames_across_retries_pagination_and_restarts() {
    let channel = Uuid::new_v4().to_string();
    let a = connect(&channel, "a").await.unwrap();
    let b = connect(&channel, "b").await.unwrap();
    let c = connect(&channel, "c").await.unwrap();
    assert!(connect(&channel, "b").await.is_err());
    let isolated = connect(&Uuid::new_v4().to_string(), "b").await.unwrap();
    let frames: Vec<_> = (0..FRAME_COUNT).map(|value| frame(value as u8)).collect();
    let expected: HashSet<_> = frames.iter().map(|frame| frame.id).collect();
    let started = Instant::now();
    for batch in frames.chunks(BATCH_SIZE) {
        a.publish(batch).await.unwrap();
    }
    let published_in = started.elapsed();
    a.publish(&frames[..BATCH_SIZE]).await.unwrap();
    let (client, connection) = database().await;
    client.execute("UPDATE stegrdb_relay.frames SET created_at = '2026-01-01' WHERE channel = $1", &[&channel]).await.unwrap();
    assert!(a.receive(BATCH_SIZE).await.unwrap().is_empty());
    assert!(isolated.receive(BATCH_SIZE).await.unwrap().is_empty());
    assert_eq!(b.receive(1).await.unwrap()[0].frame, frames[0]);
    assert_eq!(b.receive(1).await.unwrap()[0].frame, frames[0]);
    assert_eq!(drain(&b).await, expected);
    assert_eq!(drain(&c).await, expected);
    eprintln!(
        "PostgreSQL: {} x 1024B, publish={:?}, publish + two receivers + ACK={:?}",
        FRAME_COUNT,
        published_in,
        started.elapsed()
    );
    drop(b);
    let reconnect = async {
        loop {
            if let Ok(relay) = connect(&channel, "b").await {
                break relay;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    let b = tokio::time::timeout(Duration::from_secs(5), reconnect).await.unwrap();
    assert!(b.receive(1).await.unwrap().is_empty());
    let final_frame = frame(99);
    a.publish(std::slice::from_ref(&final_frame)).await.unwrap();
    assert_eq!(b.receive(1).await.unwrap()[0].frame, final_frame);
    remove_channel(&client, &channel).await;
    connection.abort();
}

#[tokio::test]
#[ignore = "専用DBでscripts/test-postgres.shを実行する"]
async fn a_late_commit_is_received_even_after_a_newer_frame_was_acknowledged() {
    let channel = Uuid::new_v4().to_string();
    let a = connect(&channel, "a").await.unwrap();
    let b = connect(&channel, "b").await.unwrap();
    let (mut client, connection) = database().await;
    let transaction = client.transaction().await.unwrap();
    let early = frame(1);
    let row = transaction
        .query_one(
            "INSERT INTO stegrdb_relay.frames (channel,id,sender,payload) VALUES ($1,$2,'a',$3) RETURNING position",
            &[&channel, &early.id, &early.bytes.as_ref()],
        )
        .await
        .unwrap();
    let position: i64 = row.get(0);
    transaction
        .execute(
            "INSERT INTO stegrdb_relay.pending (channel,node_id,frame_id,position) VALUES ($1,'b',$2,$3)",
            &[&channel, &early.id, &position],
        )
        .await
        .unwrap();
    let later = frame(2);
    a.publish(std::slice::from_ref(&later)).await.unwrap();
    let delivered = b.receive(10).await.unwrap();
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].frame, later);
    b.acknowledge(&[delivered[0].receipt.clone()]).await.unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(b.receive(10).await.unwrap()[0].frame, early);
    remove_channel(&client, &channel).await;
    connection.abort();
}

#[tokio::test]
#[ignore = "専用DBでscripts/test-postgres.shを実行する"]
async fn concurrent_node_registration_and_publish_do_not_lose_or_duplicate_delivery() {
    let channel = Uuid::new_v4().to_string();
    let a = connect(&channel, "a").await.unwrap();
    let packet = frame(1);
    let (published, connected) = tokio::join!(a.publish(std::slice::from_ref(&packet)), connect(&channel, "b"));
    published.unwrap();
    let b = connected.unwrap();
    let deliveries = b.receive(10).await.unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].frame, packet);
    let (client, connection) = database().await;
    remove_channel(&client, &channel).await;
    connection.abort();
}

async fn remove_channel(client: &Client, channel: &str) {
    client.execute("DELETE FROM stegrdb_relay.nodes WHERE channel = $1", &[&channel]).await.unwrap();
    client.execute("DELETE FROM stegrdb_relay.frames WHERE channel = $1", &[&channel]).await.unwrap();
}
