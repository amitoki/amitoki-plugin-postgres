//! 専用DBで複数ノードの送信・受信・ACKを並行実行し、全フレームの内容を照合する。
use amitoki_relay::{Frame, Relay, RelayContext, RelayPlugin};
use amitoki_relay_postgres::PostgresPlugin;
use bytes::Bytes;
use serde_json::json;
use std::{collections::HashMap, error::Error, sync::Arc, time::Duration};
use tokio::{sync::Barrier, task::JoinSet, time::Instant};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

type BenchError = Box<dyn Error + Send + Sync>;
// 既存のDBを誤って測定しないよう、試験専用の変数を必須にする。
const CONNECTION_ENV: &str = "AMITOKI_BENCH_POSTGRES_URL";
const RECEIVE_BATCH: usize = 128;
// 送信開始前の空ポーリングだけを抑える。配送待ちには全体の期限を適用する。
const EMPTY_POLL_INTERVAL: Duration = Duration::from_millis(1);
const CASE_DEADLINE: Duration = Duration::from_secs(120);

#[derive(Clone, Copy)]
struct Settings {
    nodes: usize,
    batch_size: usize,
    frame_bytes: usize,
    frames_per_node: usize,
    warmup_frames: usize,
}

impl Settings {
    fn from_arguments() -> Result<Self, BenchError> {
        let values: Vec<usize> = std::env::args().skip(1).map(|value| value.parse()).collect::<Result<_, _>>()?;
        let [nodes, batch_size, frame_bytes, frames_per_node, warmup_frames] = values.as_slice() else {
            return Err("引数: ノード数 バッチ件数 フレーム長 ノードあたり送信件数 ウォームアップ件数".into());
        };
        if !(2..=8).contains(nodes) || !(1..=128).contains(batch_size) || !(14..=65535).contains(frame_bytes) || !(1..=100_000).contains(frames_per_node) || *warmup_frames > 10_000
        {
            return Err("測定条件が範囲外です".into());
        }
        Ok(Self {
            nodes: *nodes,
            batch_size: *batch_size,
            frame_bytes: *frame_bytes,
            frames_per_node: *frames_per_node,
            warmup_frames: *warmup_frames,
        })
    }
}

fn frames(settings: &Settings, node: usize) -> Vec<Frame> {
    (0..settings.frames_per_node)
        .map(|sequence| {
            let payload: Vec<u8> = (0..settings.frame_bytes).map(|offset| (offset ^ sequence ^ (node << 4)) as u8).collect();
            Frame::new(Bytes::from(payload)).expect("検証済みのフレーム長")
        })
        .collect()
}

async fn publish(relay: Arc<dyn Relay>, batches: (Vec<Frame>, usize), start: Arc<Barrier>) -> Result<Vec<f64>, BenchError> {
    let (frames, batch_size) = batches;
    let mut latencies = Vec::new();
    start.wait().await;
    for batch in frames.chunks(batch_size) {
        let started = Instant::now();
        relay.publish(batch).await?;
        latencies.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    Ok(latencies)
}

async fn receive(relay: Arc<dyn Relay>, mut expected: HashMap<Uuid, Bytes>, start: Arc<Barrier>) -> Result<(), BenchError> {
    start.wait().await;
    while !expected.is_empty() {
        let deliveries = relay.receive(RECEIVE_BATCH).await?;
        if deliveries.is_empty() {
            tokio::time::sleep(EMPTY_POLL_INTERVAL).await;
            continue;
        }
        let mut receipts = Vec::new();
        for delivery in deliveries {
            let Some(bytes) = expected.remove(&delivery.frame.id) else {
                return Err("不明またはACK済みのフレームを受信しました".into());
            };
            if bytes != delivery.frame.bytes {
                return Err("受信内容が送信内容と一致しません".into());
            }
            receipts.push(delivery.receipt);
        }
        relay.acknowledge(&receipts).await?;
    }
    Ok(())
}

async fn warm_up(relays: &[Arc<dyn Relay>], settings: &Settings, client: &Client) -> Result<(), BenchError> {
    if settings.warmup_frames == 0 {
        return Ok(());
    }
    let warmup = Settings {
        frames_per_node: settings.warmup_frames,
        ..*settings
    };
    let packets: Vec<Vec<Frame>> = (0..settings.nodes).map(|node| frames(&warmup, node)).collect();
    for (relay, packets) in relays.iter().zip(&packets) {
        for batch in packets.chunks(RECEIVE_BATCH) {
            relay.publish(batch).await?;
        }
    }
    // 空テーブルの統計に引きずられる初期状態と、統計更新後の測定を分ける。
    client.batch_execute("ANALYZE stegrdb_relay.frames; ANALYZE stegrdb_relay.pending").await?;
    for (node, relay) in relays.iter().enumerate() {
        receive(relay.clone(), expected_frames(&packets, node), Arc::new(Barrier::new(1))).await?;
    }
    Ok(())
}

fn expected_frames(packets: &[Vec<Frame>], receiver: usize) -> HashMap<Uuid, Bytes> {
    packets.iter().enumerate().filter(|(sender, _)| *sender != receiver).flat_map(|(_, frames)| frames.iter().map(|frame| (frame.id, frame.bytes.clone()))).collect()
}

async fn measure(settings: &Settings, channel: &str, client: &Client) -> Result<serde_json::Value, BenchError> {
    let mut relays = Vec::new();
    let packets: Vec<Vec<Frame>> = (0..settings.nodes).map(|node| frames(settings, node)).collect();
    for node in 0..settings.nodes {
        relays.push(
            PostgresPlugin
                .connect(
                    RelayContext {
                        channel: channel.into(),
                        node_id: format!("node-{node}"),
                    },
                    json!({"connection_env": CONNECTION_ENV, "replay_window_ms": 0}),
                )
                .await?,
        );
    }
    warm_up(&relays, settings, client).await?;
    let start = Arc::new(Barrier::new(settings.nodes * 2 + 1));
    let mut senders = JoinSet::new();
    let mut receivers = JoinSet::new();
    for (node, relay) in relays.iter().enumerate() {
        let expected = expected_frames(&packets, node);
        receivers.spawn(receive(relay.clone(), expected, start.clone()));
        senders.spawn(publish(relay.clone(), (packets[node].clone(), settings.batch_size), start.clone()));
    }
    let started = Instant::now();
    start.wait().await;
    let mut publish_latency_ms = Vec::new();
    while let Some(completed) = senders.join_next().await {
        publish_latency_ms.extend(completed??);
    }
    let publish_seconds = started.elapsed().as_secs_f64();
    while let Some(completed) = receivers.join_next().await {
        completed??;
    }
    let elapsed_seconds = started.elapsed().as_secs_f64();
    for relay in relays {
        if !relay.receive(RECEIVE_BATCH).await?.is_empty() {
            return Err("照合後も余分な配送が残っています".into());
        }
    }
    publish_latency_ms.sort_by(f64::total_cmp);
    let percentile = |percent: usize| publish_latency_ms[(publish_latency_ms.len() * percent).div_ceil(100).saturating_sub(1)];
    let frame_count = settings.nodes * settings.frames_per_node;
    Ok(json!({
        "nodes": settings.nodes, "batch_size": settings.batch_size, "frame_bytes": settings.frame_bytes,
        "frames_per_node": settings.frames_per_node, "published_frames": frame_count,
        "warmup_frames_per_node": settings.warmup_frames,
        "verified_deliveries": frame_count * (settings.nodes - 1),
        "publish_seconds": publish_seconds, "elapsed_seconds": elapsed_seconds,
        "published_frames_per_second": frame_count as f64 / publish_seconds,
        "completed_frames_per_second": frame_count as f64 / elapsed_seconds,
        "publish_batch_latency_ms": {"p50": percentile(50), "p95": percentile(95), "p99": percentile(99)},
        "errors": 0
    }))
}

#[tokio::main]
async fn main() -> Result<(), BenchError> {
    let settings = Settings::from_arguments()?;
    let channel = format!("bench-{}", Uuid::new_v4());
    let (client, connection) = tokio_postgres::connect(&std::env::var(CONNECTION_ENV)?, NoTls).await?;
    let connection_task = tokio::spawn(connection);
    let measured = tokio::time::timeout(CASE_DEADLINE, measure(&settings, &channel, &client)).await;
    let retained: i64 = client.query_one("SELECT count(*) FROM stegrdb_relay.frames WHERE channel=$1", &[&channel]).await?.get(0);
    let pending: i64 = client.query_one("SELECT count(*) FROM stegrdb_relay.pending WHERE channel=$1", &[&channel]).await?.get(0);
    // 例外時も、この測定が作ったchannelだけを掃除する。
    client.execute("DELETE FROM stegrdb_relay.nodes WHERE channel=$1", &[&channel]).await?;
    client.execute("DELETE FROM stegrdb_relay.frames WHERE channel=$1", &[&channel]).await?;
    connection_task.abort();
    let mut report = measured??;
    report["retained_frames_after_ack"] = json!(retained);
    report["pending_after_ack"] = json!(pending);
    println!("{report}");
    Ok(())
}
