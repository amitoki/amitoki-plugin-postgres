# amitoki-plugin-postgres

PostgreSQLを使うamitokiの外部プロセス型プラグイン。本体の再ビルドなしに追加・更新できる。共通SDKは公開リポジトリのrevisionへ固定しているため、このリポジトリだけでビルドできる。

## 利用する

amitoki 0.3以降を使う。公開リポジトリなので、公式配布物の取得にGitHubトークンは不要。

```bash
amitoki plugin add postgres
amitoki plugin configure postgres --set connection_env=AMITOKI_POSTGRES_URL --set max_connections=4
amitoki plugin validate postgres
read -r -s -p 'PostgreSQL接続文字列: ' AMITOKI_POSTGRES_URL
printf '\n'
export AMITOKI_POSTGRES_URL
```

接続文字列には`host=... port=5432 user=... dbname=... sslmode=require`と認証情報を入力する。TLSはOSの信頼ストアで証明書・ホスト名を検証する。平文接続を許可するのは`sslmode=disable`を明示した場合だけ。

初回は同梱の`schema.sql`をDBに適用する。Ubuntu/Debianでは`sudo apt-get install -y postgresql-client`でpsqlを導入できる。Gitを使わずにインストールした場合も、実行ファイルからSQLを取り出せる。

```bash
~/.local/share/amitoki/plugins/postgres/amitoki-plugin-postgres --schema > schema.sql
psql "$AMITOKI_POSTGRES_URL" --set ON_ERROR_STOP=1 -f schema.sql
```

`AMITOKI_PLUGIN_DIR`や`XDG_DATA_HOME`を指定した場合は、そのインストール先の実行ファイルを使う。初期化にはスキーマを作成できるDB権限が必要。amitoki側の設定は次のとおり。

```toml
[relay]
plugin = "postgres"
[relay.options]
connection_env = "AMITOKI_POSTGRES_URL"
max_connections = 4
replay_window_ms = 4000
```

`replay_window_ms`は初回登録前のフレームを何ミリ秒分受け取るか。再起動時は値に関係なく既存の未ACKキューを引き継ぐ。

## 配送の扱い

フレームと各ノードの未処理キューを同じトランザクションで保存する。receiveは非破壊で、NICへの注入後にACKする。同じUUIDの再送とACKは冪等。登録・送信の競合はchannel単位のadvisory lockで防ぎ、同一node_idの多重起動は専用DB接続で拒否する。

NIC注入とACKの間のクラッシュは重複し得る。DBの保存済みデータは再起動後も残るが、収集直後の本体メモリは永続化しない。DBの保存期限・掃除は自動化していない。

## 開発・試験

Rustの導入は[公式手順](https://rust-lang.github.io/rustup/installation/other.html)に従う。Ubuntu/Debianでは次で準備できる。

```bash
sudo apt-get install -y build-essential curl ca-certificates docker.io
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
. "$HOME/.cargo/env"
rustup component add rustfmt clippy
cargo test --locked
cargo clippy --all-targets --locked -- -D warnings
bash scripts/test-postgres.sh
cargo build --release --locked
```

Dockerを実行できるユーザで試験する。DB試験は一時コンテナを終了時に削除する。公開SDKのGit revisionとCargo.lockを固定している。

送信・受信・ACKの性能とSQL実行計画は[コンテナでの検証手順](docs/benchmark.md)で測定する。検証用の一時DBで、ノード数・パケット長・バッチサイズを変えて全フレームを照合する。

配布物は本体の`scripts/package-plugin.py target/release/amitoki-plugin-postgres dist`で生成する。CIも同じスクリプトを使用する。Linux x86_64向けの初回配布はUbuntu 24.04でビルド・試験した。

## stegrdb版から移行する

旧タグとリリースは保持している。amitoki版は実行ファイル・crate・環境変数の接頭辞をamitokiへ変更した。本体の[移行手順](https://github.com/amitoki/amitoki/blob/main/docs/amitoki-migration.md)に従って設定とプラグインを配置する。

DBスキーマ名は`stegrdb_relay`を維持し、既存の未ACKキューを引き継ぐ。既存接続変数を使う場合は`connection_env`へ元の環境変数名を明示する。
