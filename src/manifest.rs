use crate::options::{Options, MAX_CLEANUP_BATCH_SIZE, MAX_CLEANUP_INTERVAL_MS, MAX_CONNECTIONS, MAX_WINDOW_MS, MIN_CLEANUP_INTERVAL_MS};
use amitoki_plugin_sdk::{PluginManifest, PROTOCOL_VERSION};

pub fn manifest() -> PluginManifest {
    let defaults = Options::default();
    PluginManifest {
        name: "postgres".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        protocol_version: PROTOCOL_VERSION,
        description: "PostgreSQLにフレームを保存し、ACKまで再配送する中継".into(),
        config_schema: serde_json::json!({
            "type":"object", "additionalProperties":false,
            "properties": {
                "connection_env":{"type":"string","minLength":1,"default":defaults.connection_env,"description":"接続文字列を読む環境変数名"},
                "max_connections":{"type":"integer","minimum":1,"maximum":MAX_CONNECTIONS,"default":defaults.max_connections,"description":"最大接続数"},
                "replay_window_ms":{"type":"integer","minimum":0,"maximum":MAX_WINDOW_MS,"default":defaults.replay_window_ms,"description":"初回登録時に過去のフレームを受信する期間（ミリ秒）"},
                "retention_ms":{"type":"integer","minimum":0,"maximum":MAX_WINDOW_MS,"default":defaults.retention_ms,"description":"ACK済みフレームの最低保持期間。0で自動削除を無効化。channel内の全ノードの合意が必要（ミリ秒）"},
                "cleanup_interval_ms":{"type":"integer","minimum":MIN_CLEANUP_INTERVAL_MS,"maximum":MAX_CLEANUP_INTERVAL_MS,"default":defaults.cleanup_interval_ms,"description":"保持期限を過ぎたフレームの掃除間隔（ミリ秒）"},
                "cleanup_batch_size":{"type":"integer","minimum":1,"maximum":MAX_CLEANUP_BATCH_SIZE,"default":defaults.cleanup_batch_size,"description":"1回の掃除で削除する最大フレーム数"}
            }
        }),
    }
}
