use amitoki_relay::RelayError;
use serde::Deserialize;
use std::time::Duration;
use tokio_postgres::{config::SslMode, Config};

// 送受信を並行処理でき、ノード数に比例して接続を増やしすぎない規定値。
const DEFAULT_CONNECTIONS: usize = 4;
// ノードごとの誤設定がDB全体の接続枠を使い切らないようにする。
pub(crate) const MAX_CONNECTIONS: usize = 64;
// 初回登録だけは、起動直前のパケットも取り込む。
const DEFAULT_REPLAY_WINDOW_MS: u64 = 4000;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
// 掃除のトランザクションを短くし、送信を長時間止めないための既定値。
const DEFAULT_CLEANUP_INTERVAL_MS: u64 = 1000;
const DEFAULT_CLEANUP_BATCH_SIZE: usize = 4096;
// PostgreSQLの整数ミリ秒パラメータと、掃除の負荷調整に許す範囲。
pub(crate) const MAX_WINDOW_MS: u64 = i32::MAX as u64;
pub(crate) const MIN_CLEANUP_INTERVAL_MS: u64 = 100;
pub(crate) const MAX_CLEANUP_INTERVAL_MS: u64 = 3_600_000;
pub(crate) const MAX_CLEANUP_BATCH_SIZE: usize = 65_536;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Options {
    pub connection_env: String,
    pub max_connections: usize,
    pub replay_window_ms: u64,
    pub retention_ms: u64,
    pub cleanup_interval_ms: u64,
    pub cleanup_batch_size: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            connection_env: "AMITOKI_POSTGRES_URL".into(),
            max_connections: DEFAULT_CONNECTIONS,
            replay_window_ms: DEFAULT_REPLAY_WINDOW_MS,
            retention_ms: 0,
            cleanup_interval_ms: DEFAULT_CLEANUP_INTERVAL_MS,
            cleanup_batch_size: DEFAULT_CLEANUP_BATCH_SIZE,
        }
    }
}

impl Options {
    pub fn connection_config(&self) -> Result<Config, RelayError> {
        self.validate()?;
        let connection_string = std::env::var(&self.connection_env).map_err(|_| RelayError::permanent(format!("接続情報の環境変数が未設定です: {}", self.connection_env)))?;
        // 接続文字列を含むパースエラーは認証情報が混ざるため外へ出さない。
        let mut config: Config = connection_string.parse().map_err(|_| RelayError::permanent("PostgreSQL接続情報の書式が不正です"))?;
        if config.get_ssl_mode() == SslMode::Prefer {
            config.ssl_mode(SslMode::Require);
        }
        config.connect_timeout(CONNECT_TIMEOUT);
        Ok(config)
    }

    fn validate(&self) -> Result<(), RelayError> {
        if self.connection_env.is_empty() || self.max_connections == 0 || self.max_connections > MAX_CONNECTIONS || self.replay_window_ms > MAX_WINDOW_MS {
            return Err(RelayError::permanent("postgresの接続数または再生期間が範囲外です"));
        }
        // replayと同じ整数ミリ秒範囲にし、DB側のinterval計算を正確に保つ。
        if self.retention_ms > MAX_WINDOW_MS || (self.retention_ms != 0 && self.retention_ms < self.replay_window_ms) {
            return Err(RelayError::permanent(
                "postgresの保持期間は0（無効）または再生期間以上の2147483647ミリ秒以下で指定してください",
            ));
        }
        if !(MIN_CLEANUP_INTERVAL_MS..=MAX_CLEANUP_INTERVAL_MS).contains(&self.cleanup_interval_ms) || !(1..=MAX_CLEANUP_BATCH_SIZE).contains(&self.cleanup_batch_size) {
            return Err(RelayError::permanent("postgresの掃除間隔は100〜3600000ミリ秒、1回の削除上限は1〜65536件で指定してください"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Options;

    #[test]
    fn retention_is_disabled_by_default_and_cannot_shorten_replay() {
        assert_eq!(Options::default().retention_ms, 0);
        assert!(Options::default().validate().is_ok());
        let options = Options {
            retention_ms: 1,
            ..Options::default()
        };
        assert!(options.validate().is_err());
        let options = Options {
            retention_ms: i32::MAX as u64 + 1,
            ..Options::default()
        };
        assert!(options.validate().is_err());
        let options = Options {
            cleanup_batch_size: 0,
            ..Options::default()
        };
        assert!(options.validate().is_err());
    }
}
