use stegrdb_relay::{RelayContext, RelayError};
use tokio::task::JoinHandle;
use tokio_postgres::{Client, Config};
use tokio_postgres_rustls::MakeRustlsConnect;

/// ノードの多重起動を防ぐ専用接続。プールへ返すとロックが残るため独立させる。
pub(crate) struct NodeSession {
    client: Client,
    connection_task: JoinHandle<()>,
}

impl NodeSession {
    pub async fn connect(config: &Config, tls: MakeRustlsConnect, context: &RelayContext) -> Result<Self, RelayError> {
        let (client, connection) = config.connect(tls).await.map_err(crate::database_error)?;
        let connection_task = tokio::spawn(async move {
            let _ = connection.await;
        });
        let session = Self { client, connection_task };
        let row =
            session.client.query_one("SELECT pg_try_advisory_lock(hashtext($1), hashtext($2))", &[&context.channel, &context.node_id]).await.map_err(crate::database_error)?;
        if !row.try_get::<_, bool>(0).map_err(crate::database_error)? {
            return Err(RelayError::permanent("同じchannel/node_idのPostgreSQL中継が既に動いています"));
        }
        Ok(session)
    }

    pub fn ensure_connected(&self) -> Result<(), RelayError> {
        if self.client.is_closed() {
            return Err(RelayError::permanent("ノードの占有接続が切れました。多重送信を防ぐため停止します。再起動してください"));
        }
        Ok(())
    }
}

impl Drop for NodeSession {
    fn drop(&mut self) {
        self.connection_task.abort();
    }
}
