use crate::{database_error, options::Options, pool_error};
use amitoki_relay::RelayError;
use deadpool_postgres::Pool;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_postgres::IsolationLevel;

// 掃除が統計不足や大量の未ACKに遭遇しても、接続と登録を占有し続けない。
const CLEANUP_STATEMENT_TIMEOUT: &str = "SET LOCAL statement_timeout = '250ms'";
const RETRY_INTERVAL: Duration = Duration::from_secs(60);
// 一時的なVACUUM競合などの後は、削除量を減らして早めに追従する。
const TIMEOUT_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const FAST_BATCHES_BEFORE_GROWTH: usize = 16;
const FAST_BATCH_DURATION: Duration = Duration::from_millis(50);
const TRY_CHANNEL_LOCK: &str = "SELECT pg_try_advisory_xact_lock_shared(hashtextextended($1, 0))";
// 1ノードでも無効を選んでいれば削除しない。停止中ノードの設定も保護する。
const CHANNEL_RETENTION: &str = "
SELECT CASE WHEN bool_and(retention_ms > 0)
       THEN greatest(max(retention_ms), max(replay_window_ms)) ELSE 0 END
FROM stegrdb_relay.nodes WHERE channel = $1";
// 相関NOT EXISTSのOFFSET 0は、古い統計でpending全体とのanti joinへ
// 展開されることを防ぐ。候補ごとにpending_frameの索引を調べる。
const PRUNE: &str = "
WITH expired AS MATERIALIZED (
    SELECT frames.channel, frames.id FROM stegrdb_relay.frames AS frames
    WHERE frames.channel = $1
      AND frames.created_at < statement_timestamp() - $2::bigint * INTERVAL '1 millisecond'
      AND NOT EXISTS (
          SELECT 1 FROM stegrdb_relay.pending
          WHERE pending.channel = frames.channel AND pending.frame_id = frames.id
          OFFSET 0
      )
    ORDER BY frames.created_at LIMIT $3
    FOR UPDATE OF frames SKIP LOCKED
)
DELETE FROM stegrdb_relay.frames AS frames USING expired
WHERE frames.channel = expired.channel AND frames.id = expired.id";

pub(crate) struct Maintenance(JoinHandle<()>);

#[derive(Debug)]
enum CleanupError {
    TimedOut,
    Database(RelayError),
}

impl From<tokio_postgres::Error> for CleanupError {
    fn from(error: tokio_postgres::Error) -> Self {
        if error.code() == Some(&tokio_postgres::error::SqlState::QUERY_CANCELED) {
            Self::TimedOut
        } else {
            Self::Database(database_error(error))
        }
    }
}

impl Maintenance {
    pub fn start(pool: Pool, channel: String, options: &Options) -> Option<Self> {
        if options.retention_ms == 0 {
            return None;
        }
        let interval = Duration::from_millis(options.cleanup_interval_ms);
        let maximum_batch_size = options.cleanup_batch_size as i64;
        Some(Self(tokio::spawn(async move {
            let mut next_interval = interval;
            let mut batch_size = maximum_batch_size;
            let mut fast_batches = 0;
            loop {
                tokio::time::sleep(next_interval).await;
                let started = tokio::time::Instant::now();
                match prune(&pool, &channel, batch_size).await {
                    Ok(removed) => {
                        next_interval = interval;
                        if removed == batch_size as u64 && started.elapsed() < FAST_BATCH_DURATION {
                            fast_batches += 1;
                        } else {
                            fast_batches = 0;
                        }
                        if fast_batches == FAST_BATCHES_BEFORE_GROWTH {
                            batch_size = (batch_size + (batch_size / 4).max(1)).min(maximum_batch_size);
                            fast_batches = 0;
                        }
                    },
                    Err(CleanupError::TimedOut) => {
                        batch_size = (batch_size / 2).max(1);
                        fast_batches = 0;
                        next_interval = interval.max(TIMEOUT_RETRY_INTERVAL);
                        eprintln!("PostgreSQLの掃除が期限を超えたため、次回の削除上限を{batch_size}件に減らします");
                    },
                    Err(CleanupError::Database(error)) => {
                        // SDKのstdoutはプロトコル専用。DB詳細や接続文字列をログに出さない。
                        eprintln!("PostgreSQLの期限切れフレーム掃除を延期します: {error}");
                        next_interval = RETRY_INTERVAL;
                    },
                }
            }
        })))
    }
}

impl Drop for Maintenance {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn prune(pool: &Pool, channel: &str, batch_size: i64) -> Result<u64, CleanupError> {
    let mut client = pool.get().await.map_err(|error| CleanupError::Database(pool_error(error)))?;
    let transaction = client.build_transaction().isolation_level(IsolationLevel::ReadCommitted).start().await?;
    transaction.batch_execute(CLEANUP_STATEMENT_TIMEOUT).await?;
    let locked: bool = transaction.query_one(TRY_CHANNEL_LOCK, &[&channel]).await?.get(0);
    if !locked {
        transaction.rollback().await?;
        return Ok(0);
    }
    // 登録とは排他、送信とは共有。ロック後に合意済み設定の最新スナップショットを読む。
    let retention_ms: i64 = transaction.query_one(CHANNEL_RETENTION, &[&channel]).await?.get(0);
    let removed = if retention_ms > 0 {
        transaction.execute(PRUNE, &[&channel, &retention_ms, &batch_size]).await?
    } else {
        0
    };
    transaction.commit().await?;
    Ok(removed)
}

#[cfg(test)]
mod tests;
