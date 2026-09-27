use super::prune;
#[path = "../../tests/common/database.rs"]
mod database;
use database::{database, CONNECTION_ENV};
use deadpool_postgres::{Manager, Pool};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

fn pool() -> Pool {
    let config = std::env::var(CONNECTION_ENV).unwrap().parse().unwrap();
    Pool::builder(Manager::new(config, NoTls)).max_size(1).build().unwrap()
}

async fn expired_channel(client: &Client, count: i32) -> String {
    let channel = Uuid::new_v4().to_string();
    client
        .execute(
            "INSERT INTO stegrdb_relay.nodes(channel,node_id,replay_window_ms,retention_ms) VALUES ($1,'node',0,60000)",
            &[&channel],
        )
        .await
        .unwrap();
    client.execute("INSERT INTO stegrdb_relay.frames(channel,id,sender,payload,created_at) SELECT $1,md5(value::text)::uuid,'source',decode(repeat('ab',64),'hex'),statement_timestamp()-interval '5 minutes' FROM generate_series(1,$2::int) AS value", &[&channel, &count]).await.unwrap();
    channel
}

#[tokio::test]
#[ignore = "専用DBでscripts/test-postgres.shを実行する"]
async fn cleanup_requires_every_node_to_enable_retention_and_stays_within_its_channel() {
    let (client, connection) = database().await;
    let channel = expired_channel(&client, 1).await;
    let other = expired_channel(&client, 1).await;
    client.execute("INSERT INTO stegrdb_relay.nodes(channel,node_id) VALUES ($1,'legacy')", &[&channel]).await.unwrap();
    let pool = pool();
    assert_eq!(prune(&pool, &channel, 10).await.unwrap(), 0);
    client.execute("UPDATE stegrdb_relay.nodes SET retention_ms=60000,replay_window_ms=0 WHERE channel=$1", &[&channel]).await.unwrap();
    assert_eq!(prune(&pool, &channel, 10).await.unwrap(), 1);
    let retained: i64 = client.query_one("SELECT count(*) FROM stegrdb_relay.frames WHERE channel=$1", &[&other]).await.unwrap().get(0);
    assert_eq!(retained, 1);
    connection.abort();
}

#[tokio::test]
#[ignore = "専用DBでscripts/test-postgres.shを実行する"]
async fn cleanup_preserves_unacknowledged_frames_and_the_longest_replay_or_retention_window() {
    let (client, connection) = database().await;
    let channel = expired_channel(&client, 4).await;
    client
        .execute(
            "INSERT INTO stegrdb_relay.nodes(channel,node_id,replay_window_ms,retention_ms) VALUES ($1,'long',180000,120000)",
            &[&channel],
        )
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO stegrdb_relay.pending SELECT channel,'node',id,position FROM stegrdb_relay.frames WHERE channel=$1 AND id=md5('1')::uuid",
            &[&channel],
        )
        .await
        .unwrap();
    client
        .execute(
            "UPDATE stegrdb_relay.frames SET created_at=statement_timestamp()-interval '2 minutes' WHERE channel=$1 AND id=md5('2')::uuid",
            &[&channel],
        )
        .await
        .unwrap();
    client
        .execute(
            "UPDATE stegrdb_relay.frames SET created_at=statement_timestamp() WHERE channel=$1 AND id=md5('3')::uuid",
            &[&channel],
        )
        .await
        .unwrap();
    let pool = pool();
    assert_eq!(prune(&pool, &channel, 10).await.unwrap(), 1);
    let remaining: Vec<Uuid> = client.query("SELECT id FROM stegrdb_relay.frames WHERE channel=$1", &[&channel]).await.unwrap().into_iter().map(|row| row.get(0)).collect();
    assert_eq!(remaining.len(), 3);
    let pending: i64 = client.query_one("SELECT count(*) FROM stegrdb_relay.pending WHERE channel=$1", &[&channel]).await.unwrap().get(0);
    assert_eq!(pending, 1);
    client.execute("DELETE FROM stegrdb_relay.pending WHERE channel=$1", &[&channel]).await.unwrap();
    assert_eq!(prune(&pool, &channel, 10).await.unwrap(), 1);
    connection.abort();
}

#[tokio::test]
#[ignore = "専用DBでscripts/test-postgres.shを実行する"]
async fn cleanup_limits_each_batch_skips_locked_rows_and_yields_to_registration() {
    let (mut client, connection) = database().await;
    let channel = expired_channel(&client, 5).await;
    let pool = pool();
    let transaction = client.transaction().await.unwrap();
    transaction.query_one("SELECT id FROM stegrdb_relay.frames WHERE channel=$1 AND id=md5('1')::uuid FOR UPDATE", &[&channel]).await.unwrap();
    assert_eq!(prune(&pool, &channel, 2).await.unwrap(), 2);
    assert_eq!(prune(&pool, &channel, 2).await.unwrap(), 2);
    assert_eq!(prune(&pool, &channel, 2).await.unwrap(), 0);
    transaction.rollback().await.unwrap();
    let transaction = client.transaction().await.unwrap();
    transaction.query_one(crate::queries::LOCK_CHANNEL, &[&channel]).await.unwrap();
    assert_eq!(prune(&pool, &channel, 2).await.unwrap(), 0);
    transaction.rollback().await.unwrap();
    assert_eq!(prune(&pool, &channel, 2).await.unwrap(), 1);
    assert_eq!(prune(&pool, &channel, 2).await.unwrap(), 0);
    connection.abort();
}
