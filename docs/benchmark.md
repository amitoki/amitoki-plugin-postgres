# コンテナで配送性能を検証する

検証用のPostgreSQLだけを作り、全フレームの内容とACK完了を照合する。既存DBの接続情報は使わない。プラグインのRust実装とDBの性能を測るため、NIC・本体のパイプライン・外部プラグインのMessagePack通信は測定に含まない。

## 実行

Rust・Python 3・Dockerを準備する。Rust/Dockerの導入は[README](../README.md#開発試験)を参照。

```bash
cargo build --release --example relay_bench --locked
python3 scripts/bench-postgres.py \
  --output artifacts/postgres-bench/result.json
```

各条件を3回測る。コンテナは4CPU・2GiB、ホストのloopbackのランダムポートを使う。既定イメージは`postgres:17-alpine`で、結果には実際のイメージIDとPostgreSQL版を記録する。比較を再現する場合は`--image postgres@sha256:...`で同じイメージを指定する。

`fsync`と`synchronous_commit`を無効化しない。コンテナの通常ストレージを使い、tmpfsへDBを置かない。ホストのディスクや同時負荷によって速度は変わる。例外・中断時も、このコマンドが作ったコンテナと匿名volumeを削除する。

## 測定条件と結果

- 2・3ノードすべてが同時に送信し、他ノードの全フレームを受信・照合・ACKする。
- フレーム長64・1400バイト、送信バッチ1・32・128件。受信は最大128件。
- 本測定前に各ノード1024件を送信し、統計を更新してから受信する。ウォームアップは速度に含めない。
- `--warmup-frames 0`では空テーブルの統計から始める。投入直後の実行計画の影響を含むため、通常の測定値と混ぜない。
- 速度は「元の送信フレーム数 ÷ 全ノードのACK完了時間」。複数宛先への配送数は`verified_deliveries`に別記する。
- バッチ送信のp50・p95・p99、ACK後にDBに残るフレーム数、未ACK数を保存する。保存フレーム数にはウォームアップを含む。

各条件の後、検証用データを削除する。この削除時間は配送速度に含めない。通常の比較では保持期限による自動削除を無効にする。

## 改善候補との比較

比較用ビルドを別の出力先で作り、両方の実行ファイルを指定する。

```bash
python3 scripts/bench-postgres.py \
  --binary /absolute/path/current/relay_bench \
  --compare-binary /absolute/path/candidate/relay_bench \
  --output artifacts/postgres-bench/comparison.json
```

比較ビルドでは`CARGO_TARGET_DIR`を分ける。同じpackage名・版のworktree同士で出力先を共有すると、成果物を取り違える場合がある。スクリプトは実行ファイルのSHA256を記録し、同一ファイル同士の比較を拒否する。実行順は繰り返しごとに交替する。

## SQL実行計画だけを調べる

```bash
python3 scripts/bench-postgres.py --plans-only \
  --baseline-queries /absolute/path/before/src/queries.rs \
  --output artifacts/postgres-bench/query-plans.json
```

受信SQLは1万件のフレームと128件の未ACKで、統計更新前後の計画を比較する。初回再生SQLは25万件の古い履歴で測定する。`--baseline-queries`には比較元の`src/queries.rs`を指定し、省略時は現行SQLだけを測る。比較元のファイルは`git show <revision>:src/queries.rs > before-queries.rs`で取得できる。配送するUUID・本文・順序の一致も確認する。

`EXPLAIN ANALYZE`の結果には実行時間・走査件数・バッファ参照を保存する。実際の稼働中はパラメータ、prepared statementの計画、データ分布、統計の鮮度で計画が変わるため、この固定データでの結果だけで常時高速化を保証しない。

初回再生の基準時刻にはSQL開始時刻で固定される`statement_timestamp()`を使う。[PostgreSQLの時刻関数](https://www.postgresql.org/docs/17/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT)の違いに従い、インデックスで期間を絞れるようにしている。

## 接続を維持した耐久試験

```bash
python3 scripts/bench-postgres.py --soak-seconds 300 --retention-ms 5000 \
  --output artifacts/postgres-bench/soak.json
```

同じ測定プロセス内で3つのRelayとDB接続を維持し、1400バイト・バッチ128件の送受信を繰り返す。すべての内容とACKを照合し、約10秒ごとのRSS、残存フレーム数、未ACK数、表と索引の使用量を記録する。上の5秒保持は掃除を短時間で試験するための値で、本番の推奨保持期間ではない。`--retention-ms 0`では履歴を残したまま測る。耐久試験では掃除間隔を100ミリ秒にする。

各ラウンドの速度と、生成・観測を含む全体時間を別に記録する。10秒のサンプルはラウンド完了後なので、負荷が高いと記録間隔は延びる。RSSには生成・照合用メモリを含み、DBの常駐メモリを含まない。5分の成功だけで日単位のメモリ安定性やDB容量上限は保証しない。
