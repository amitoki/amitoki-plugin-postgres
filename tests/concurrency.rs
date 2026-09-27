mod common;

use amitoki_relay::{Frame, Relay};
use common::{connect, connect_with_options, database, frame, remove_channel};
use serde_json::json;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::task::JoinSet;
use tokio_postgres::Client;
use uuid::Uuid;

// 成功条件はDBが返す待機関係で判断し、期限は回帰時のハング検出だけに使う。
const TEST_DEADLINE: Duration = Duration::from_secs(15);
const LOCK_POLL_INTERVAL: Duration = Duration::from_millis(5);

async fn wait_for_blocked_backend(client: &Client, blocker: i32) -> i32 {
    tokio::time::timeout(TEST_DEADLINE, async {
        loop {
            if let Some(row) = client.query_opt("SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)) LIMIT 1", &[&blocker]).await.unwrap() {
                return row.get(0);
            }
            tokio::time::sleep(LOCK_POLL_INTERVAL).await;
        }
    })
    .await
    .expect("DBでロック待ちが発生すること")
}

#[tokio::test]
#[ignore = "専用DBでscripts/test-postgres.shを実行する"]
async fn publishers_can_commit_while_another_publisher_holds_the_channel_lock() {
    let channel = Uuid::new_v4().to_string();
    let a = connect(&channel, "a").await.unwrap();
    let b = connect(&channel, "b").await.unwrap();
    let receiver = connect(&channel, "receiver").await.unwrap();
    let (mut client, connection) = database().await;
    let transaction = client.transaction().await.unwrap();
    transaction.query_one("SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))", &[&channel]).await.unwrap();
    let packets = [frame(1), frame(2)];
    tokio::time::timeout(TEST_DEADLINE, async {
        let (a_sent, b_sent) = tokio::join!(a.publish(&packets[..1]), b.publish(&packets[1..]));
        a_sent.unwrap();
        b_sent.unwrap();
    })
    .await
    .expect("共有ロック中も別の送信が完了すること");
    transaction.rollback().await.unwrap();
    assert_eq!(receiver.receive(10).await.unwrap().len(), 2);
    remove_channel(&client, &channel).await;
    connection.abort();
}

#[tokio::test]
#[ignore = "専用DBでscripts/test-postgres.shを実行する"]
async fn a_publisher_waiting_for_registration_delivers_to_the_new_node_without_replay() {
    let channel = Uuid::new_v4().to_string();
    let sender = connect(&channel, "sender").await.unwrap();
    let (mut blocker, blocker_connection) = database().await;
    let (observer, observer_connection) = database().await;
    let blocker_pid: i32 = blocker.query_one("SELECT pg_backend_pid()", &[]).await.unwrap().get(0);
    let transaction = blocker.transaction().await.unwrap();
    // REGISTERを一意制約で待たせ、登録側がchannelの排他ロックを持つ状態を作る。
    transaction.execute("INSERT INTO stegrdb_relay.nodes VALUES ($1, 'receiver')", &[&channel]).await.unwrap();
    let joining_channel = channel.clone();
    let joining = tokio::spawn(async move { connect_with_options(&joining_channel, "receiver", json!({"replay_window_ms": 0})).await });
    let registration_pid = wait_for_blocked_backend(&observer, blocker_pid).await;
    let packet = frame(3);
    let sent = packet.clone();
    let publishing = tokio::spawn(async move { sender.publish(&[sent]).await });
    wait_for_blocked_backend(&observer, registration_pid).await;
    transaction.rollback().await.unwrap();
    let receiver = tokio::time::timeout(TEST_DEADLINE, joining).await.unwrap().unwrap().unwrap();
    tokio::time::timeout(TEST_DEADLINE, publishing).await.unwrap().unwrap().unwrap();
    let deliveries = receiver.receive(10).await.unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].frame, packet);
    remove_channel(&observer, &channel).await;
    blocker_connection.abort();
    observer_connection.abort();
}

#[tokio::test]
#[ignore = "専用DBでscripts/test-postgres.shを実行する"]
async fn registration_waits_for_a_publisher_then_replays_its_committed_frame() {
    let channel = Uuid::new_v4().to_string();
    let _sender = connect(&channel, "sender").await.unwrap();
    let (mut blocker, blocker_connection) = database().await;
    let (observer, observer_connection) = database().await;
    let blocker_pid: i32 = blocker.query_one("SELECT pg_backend_pid()", &[]).await.unwrap().get(0);
    let transaction = blocker.transaction().await.unwrap();
    transaction.query_one("SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))", &[&channel]).await.unwrap();
    let packet = frame(4);
    transaction
        .execute(
            "INSERT INTO stegrdb_relay.frames(channel,id,sender,payload) VALUES ($1,$2,'sender',$3)",
            &[&channel, &packet.id, &packet.bytes.as_ref()],
        )
        .await
        .unwrap();
    let joining_channel = channel.clone();
    let joining = tokio::spawn(async move { connect_with_options(&joining_channel, "receiver", json!({"replay_window_ms": 60_000})).await });
    wait_for_blocked_backend(&observer, blocker_pid).await;
    transaction.commit().await.unwrap();
    let receiver = tokio::time::timeout(TEST_DEADLINE, joining).await.unwrap().unwrap().unwrap();
    let deliveries = receiver.receive(10).await.unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].frame, packet);
    remove_channel(&observer, &channel).await;
    blocker_connection.abort();
    observer_connection.abort();
}

#[tokio::test]
#[ignore = "専用DBでscripts/test-postgres.shを実行する"]
async fn cancelling_a_blocked_insert_rolls_back_the_batch_and_releases_the_connection() {
    let channel = Uuid::new_v4().to_string();
    let sender = connect_with_options(&channel, "sender", json!({"max_connections": 1})).await.unwrap();
    let receiver = connect(&channel, "receiver").await.unwrap();
    let (mut blocker, blocker_connection) = database().await;
    let (observer, observer_connection) = database().await;
    let blocker_pid: i32 = blocker.query_one("SELECT pg_backend_pid()", &[]).await.unwrap().get(0);
    let transaction = blocker.transaction().await.unwrap();
    let packets = [frame(5), frame(6)];
    transaction
        .execute(
            "INSERT INTO stegrdb_relay.frames(channel,id,sender,payload) VALUES ($1,$2,'sender',$3)",
            &[&channel, &packets[0].id, &packets[0].bytes.as_ref()],
        )
        .await
        .unwrap();
    let sending = sender.clone();
    let publishing = tokio::spawn(async move { sending.publish(&packets).await });
    wait_for_blocked_backend(&observer, blocker_pid).await;
    publishing.abort();
    assert!(publishing.await.unwrap_err().is_cancelled());
    transaction.rollback().await.unwrap();
    let final_packet = frame(7);
    tokio::time::timeout(TEST_DEADLINE, sender.publish(std::slice::from_ref(&final_packet))).await.unwrap().unwrap();
    let deliveries = receiver.receive(10).await.unwrap();
    assert_eq!(deliveries.len(), 1, "キャンセルしたバッチが部分的に保存されないこと");
    assert_eq!(deliveries[0].frame, final_packet);
    remove_channel(&observer, &channel).await;
    blocker_connection.abort();
    observer_connection.abort();
}

#[tokio::test]
#[ignore = "専用DBでscripts/test-postgres.shを実行する"]
async fn concurrent_retries_of_the_same_batch_deliver_each_uuid_once_in_input_order() {
    let channel = Uuid::new_v4().to_string();
    let sender = connect(&channel, "sender").await.unwrap();
    let receiver = connect(&channel, "receiver").await.unwrap();
    let packets: Vec<_> = (0..128).map(frame).collect();
    let mut sending = JoinSet::new();
    for _ in 0..8 {
        let sender = sender.clone();
        let packets = packets.clone();
        sending.spawn(async move { sender.publish(&packets).await });
    }
    tokio::time::timeout(TEST_DEADLINE, async {
        while let Some(sent) = sending.join_next().await {
            sent.unwrap().unwrap();
        }
    })
    .await
    .unwrap();
    let deliveries = receiver.receive(1024).await.unwrap();
    assert_eq!(deliveries.iter().map(|delivery| &delivery.frame).collect::<Vec<_>>(), packets.iter().collect::<Vec<_>>());
    receiver.acknowledge(&deliveries.into_iter().map(|delivery| delivery.receipt).collect::<Vec<_>>()).await.unwrap();
    sender.publish(&packets).await.unwrap();
    assert!(receiver.receive(1024).await.unwrap().is_empty());
    let (client, connection) = database().await;
    remove_channel(&client, &channel).await;
    connection.abort();
}

async fn publish_until_committed(relay: Arc<dyn Relay>, packets: Vec<Frame>) {
    loop {
        match relay.publish(&packets).await {
            Ok(()) => return,
            // 重なるUUIDを逆順でINSERTするとDBが片方を中断し得る。
            // 呼び出し側の再送でバッチ全体が冪等に復旧することを確かめる。
            Err(error) => assert!(error.is_retryable(), "再送可能であること: {error}"),
        }
    }
}

#[tokio::test]
#[ignore = "専用DBでscripts/test-postgres.shを実行する"]
async fn overlapping_batches_from_different_senders_remain_idempotent_after_database_retries() {
    let channel = Uuid::new_v4().to_string();
    let a = connect(&channel, "a").await.unwrap();
    let b = connect(&channel, "b").await.unwrap();
    let receiver = connect(&channel, "receiver").await.unwrap();
    let packets: Vec<_> = (0..128).map(frame).collect();
    let reversed: Vec<_> = packets.iter().rev().cloned().collect();
    tokio::time::timeout(TEST_DEADLINE, async {
        tokio::join!(publish_until_committed(a, packets.clone()), publish_until_committed(b, reversed));
    })
    .await
    .expect("DBが中断した送信も再試行で完了すること");
    let delivered = receiver.receive(1024).await.unwrap();
    assert_eq!(delivered.len(), packets.len());
    let contents: HashMap<_, _> = delivered.into_iter().map(|delivery| (delivery.frame.id, delivery.frame.bytes)).collect();
    assert_eq!(contents, packets.into_iter().map(|packet| (packet.id, packet.bytes)).collect());
    let (client, connection) = database().await;
    remove_channel(&client, &channel).await;
    connection.abort();
}
