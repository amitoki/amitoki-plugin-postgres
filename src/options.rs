use serde::Deserialize;
use std::time::Duration;
use stegrdb_relay::RelayError;
use tokio_postgres::{config::SslMode, Config};

// 送受信を並行処理でき、ノード数に比例して接続を増やしすぎない規定値。
const DEFAULT_CONNECTIONS: usize = 4;
// ノードごとの誤設定がDB全体の接続枠を使い切らないようにする。
const MAX_CONNECTIONS: usize = 64;
// 初回登録だけは、起動直前のパケットも取り込む。
const DEFAULT_REPLAY_WINDOW_MS: u64 = 4000;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Options {
    pub connection_env: String,
    pub max_connections: usize,
    pub replay_window_ms: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            connection_env: "STEGRDB_POSTGRES_URL".into(),
            max_connections: DEFAULT_CONNECTIONS,
            replay_window_ms: DEFAULT_REPLAY_WINDOW_MS,
        }
    }
}

impl Options {
    pub fn connection_config(&self) -> Result<Config, RelayError> {
        if self.max_connections == 0 || self.max_connections > MAX_CONNECTIONS || self.replay_window_ms > i32::MAX as u64 {
            return Err(RelayError::permanent("postgresの接続数または再生期間が範囲外です"));
        }
        let connection_string = std::env::var(&self.connection_env).map_err(|_| RelayError::permanent(format!("接続情報の環境変数が未設定です: {}", self.connection_env)))?;
        // 接続文字列を含むパースエラーは認証情報が混ざるため外へ出さない。
        let mut config: Config = connection_string.parse().map_err(|_| RelayError::permanent("PostgreSQL接続情報の書式が不正です"))?;
        if config.get_ssl_mode() == SslMode::Prefer {
            config.ssl_mode(SslMode::Require);
        }
        config.connect_timeout(CONNECT_TIMEOUT);
        Ok(config)
    }
}
