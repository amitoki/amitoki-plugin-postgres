use tokio_postgres::{Client, NoTls};

pub const CONNECTION_ENV: &str = "AMITOKI_TEST_POSTGRES_URL";

pub async fn database() -> (Client, tokio::task::JoinHandle<()>) {
    let connection_string = std::env::var(CONNECTION_ENV).expect("専用のPostgreSQLテスト接続を設定してください");
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls).await.expect("テスト用DBへの接続");
    let task = tokio::spawn(async move {
        connection.await.expect("テスト用DBの接続維持");
    });
    (client, task)
}
