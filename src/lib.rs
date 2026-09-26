pub mod manifest;
mod options;
mod queries;
mod session;

use amitoki_relay::{Delivery, Frame, Receipt, Relay, RelayContext, RelayError, RelayPlugin};
use async_trait::async_trait;
use bytes::Bytes;
use deadpool_postgres::{Manager, Pool, PoolError};
use options::Options;
use serde_json::Value;
use session::NodeSession;
use std::{error::Error, io, sync::Arc};
use tokio_postgres_rustls::MakeRustlsConnect;
use uuid::Uuid;

pub struct PostgresPlugin;

struct PostgresRelay {
    context: RelayContext,
    pool: Pool,
    session: NodeSession,
}

fn database_error(error: tokio_postgres::Error) -> RelayError {
    // サーバのDETAILや接続文字列を含めず、再試行判断に必要なSQLSTATEだけ返す。
    let code = error.code().map(|code| code.code()).unwrap_or("connection");
    let message = format!("PostgreSQL中継に失敗しました (SQLSTATE: {code})");
    if is_connection_interrupted(&error) || code.starts_with("08") || code.starts_with("40") || code.starts_with("53") || code == "57P01" {
        RelayError::retryable(message)
    } else {
        RelayError::permanent(message)
    }
}

fn is_connection_interrupted(error: &tokio_postgres::Error) -> bool {
    if error.is_closed() {
        return true;
    }
    let mut source = error.source();
    while let Some(cause) = source {
        if let Some(error) = cause.downcast_ref::<io::Error>() {
            // TLS検証や型変換の失敗を無限に再試行しない。
            return matches!(
                error.kind(),
                io::ErrorKind::ConnectionRefused
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::NotConnected
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::TimedOut
                    | io::ErrorKind::Interrupted
                    | io::ErrorKind::UnexpectedEof
            );
        }
        source = cause.source();
    }
    false
}

fn tls_connector() -> Result<MakeRustlsConnect, RelayError> {
    let mut roots = rustls::RootCertStore::empty();
    for certificate in rustls_native_certs::load_native_certs().certs {
        roots.add(certificate).map_err(|_| RelayError::permanent("TLSのルート証明書を読み込めません"))?;
    }
    let config = rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
    Ok(MakeRustlsConnect::new(config))
}

#[async_trait]
impl RelayPlugin for PostgresPlugin {
    fn name(&self) -> &'static str {
        "postgres"
    }

    async fn connect(&self, context: RelayContext, options: Value) -> Result<Arc<dyn Relay>, RelayError> {
        context.validate()?;
        let options: Options = serde_json::from_value(options).map_err(|_| RelayError::permanent("postgresの設定が不正です"))?;
        let config = options.connection_config()?;
        let tls = tls_connector()?;
        let session = NodeSession::connect(&config, tls.clone(), &context).await?;
        let pool = Pool::builder(Manager::new(config, tls)).max_size(options.max_connections).build().map_err(|_| RelayError::permanent("PostgreSQL接続プールを作成できません"))?;
        let relay = PostgresRelay { context, pool, session };
        relay.register(options.replay_window_ms as i32).await?;
        Ok(Arc::new(relay))
    }
}

impl PostgresRelay {
    async fn acquire_client(&self) -> Result<deadpool_postgres::Object, RelayError> {
        self.session.ensure_connected()?;
        self.pool.get().await.map_err(|error| match error {
            PoolError::Backend(error) => database_error(error),
            PoolError::Timeout(_) => RelayError::retryable("PostgreSQL接続プールの待機がタイムアウトしました"),
            _ => RelayError::permanent("PostgreSQL接続プールから接続を取得できません"),
        })
    }

    async fn register(&self, replay_window_ms: i32) -> Result<(), RelayError> {
        let mut client = self.acquire_client().await?;
        let transaction = client.transaction().await.map_err(database_error)?;
        // 登録と送信が交差して、どちらからもpendingが作られない競合を防ぐ。
        transaction.query_one(queries::LOCK_CHANNEL, &[&self.context.channel]).await.map_err(database_error)?;
        let inserted = transaction.execute(queries::REGISTER, &[&self.context.channel, &self.context.node_id]).await.map_err(database_error)?;
        if inserted != 0 {
            transaction.execute(queries::REPLAY, &[&self.context.channel, &self.context.node_id, &replay_window_ms]).await.map_err(database_error)?;
        }
        transaction.commit().await.map_err(database_error)
    }
}

#[async_trait]
impl Relay for PostgresRelay {
    async fn publish(&self, frames: &[Frame]) -> Result<(), RelayError> {
        if frames.is_empty() {
            return Ok(());
        }
        for frame in frames {
            frame.validate()?;
        }
        let ids: Vec<_> = frames.iter().map(|frame| frame.id).collect();
        let payloads: Vec<&[u8]> = frames.iter().map(|frame| frame.bytes.as_ref()).collect();
        let mut client = self.acquire_client().await?;
        let transaction = client.transaction().await.map_err(database_error)?;
        let lock = transaction.prepare_cached(queries::LOCK_CHANNEL).await.map_err(database_error)?;
        transaction.query_one(&lock, &[&self.context.channel]).await.map_err(database_error)?;
        let statement = transaction.prepare_cached(queries::PUBLISH).await.map_err(database_error)?;
        transaction.execute(&statement, &[&self.context.channel, &self.context.node_id, &ids, &payloads]).await.map_err(database_error)?;
        transaction.commit().await.map_err(database_error)
    }

    async fn receive(&self, limit: usize) -> Result<Vec<Delivery>, RelayError> {
        let limit = i64::try_from(limit).map_err(|_| RelayError::permanent("受信上限が大きすぎます"))?;
        let client = self.acquire_client().await?;
        let statement = client.prepare_cached(queries::RECEIVE).await.map_err(database_error)?;
        let rows = client.query(&statement, &[&self.context.channel, &self.context.node_id, &limit]).await.map_err(database_error)?;
        rows.into_iter()
            .map(|row| {
                let id: Uuid = row.try_get("id").map_err(database_error)?;
                let payload: Vec<u8> = row.try_get("payload").map_err(database_error)?;
                Ok(Delivery {
                    frame: Frame { id, bytes: Bytes::from(payload) },
                    receipt: Receipt(id.to_string()),
                })
            })
            .collect()
    }

    async fn acknowledge(&self, receipts: &[Receipt]) -> Result<(), RelayError> {
        if receipts.is_empty() {
            return Ok(());
        }
        let ids: Result<Vec<_>, _> = receipts.iter().map(|receipt| Uuid::parse_str(&receipt.0)).collect();
        let ids = ids.map_err(|_| RelayError::permanent("postgresの受領情報が不正です"))?;
        let client = self.acquire_client().await?;
        let statement = client.prepare_cached(queries::ACKNOWLEDGE).await.map_err(database_error)?;
        client.execute(&statement, &[&self.context.channel, &self.context.node_id, &ids]).await.map_err(database_error)?;
        Ok(())
    }
}
