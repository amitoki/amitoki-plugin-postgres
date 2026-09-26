use amitoki_plugin_sdk::{PluginManifest, PROTOCOL_VERSION};

pub fn manifest() -> PluginManifest {
    PluginManifest {
        name: "postgres".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        protocol_version: PROTOCOL_VERSION,
        description: "PostgreSQLにフレームを保存し、ACKまで再配送する中継".into(),
        config_schema: serde_json::json!({
            "type":"object", "additionalProperties":false,
            "properties": {
                "connection_env":{"type":"string","minLength":1,"default":"AMITOKI_POSTGRES_URL","description":"接続文字列を読む環境変数名"},
                "max_connections":{"type":"integer","minimum":1,"maximum":64,"default":4,"description":"最大接続数"},
                "replay_window_ms":{"type":"integer","minimum":0,"maximum":2147483647,"default":4000,"description":"初回登録時に過去のフレームを受信する期間（ミリ秒）"}
            }
        }),
    }
}
