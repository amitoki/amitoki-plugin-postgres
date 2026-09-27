# 保持期限と運用

既定の`retention_ms = 0`は自動削除を無効にする。フレームの保存期間を決めた場合だけ、channel内の全ノードで有効にする。期間は保存時刻から数え、ACK時刻からは数えない。

```toml
[relay.options]
connection_env = "AMITOKI_POSTGRES_URL"
max_connections = 4
replay_window_ms = 4000
retention_ms = 86400000 # 24時間
cleanup_interval_ms = 1000
cleanup_batch_size = 4096
```

CLIの設定も使える。保存した設定を稼働中のプラグインへ反映するには本体を再起動する。

```bash
amitoki plugin relay configure postgres --set retention_ms=86400000
```

## 削除する条件

登録済みノードが1つでも`retention_ms = 0`なら、そのchannelでは削除しない。全ノードが有効にした場合、最長の保持期間と再生期間を適用する。停止中ノードの設定もDBに残して保護する。設定変更は接続時の登録トランザクションで反映する。

期限を過ぎても、いずれかのノードに未ACKが残るフレームは削除しない。従って、停止したノードのキューや削除を無効にしたノードがあれば、DBの使用量は増え続ける。容量上限を保証する設定ではない。ノードの自動削除や未ACKの破棄は行わない。

保持期間内はUUIDの再送を重複排除する。実際に削除した後の同一UUIDは新規フレームとして配送する。保持期間を後から延ばしても、削除した履歴は復元されない。

受領情報にはUUIDと配送の世代（DBのposition）を含める。削除前のACKが遅れて届いても、新しい世代の未ACKを消さない。受領情報はプラグインが返した値をそのままACKに渡し、呼び出し側でUUIDから作らない。旧版のUUIDだけの受領情報は新しいプロセスでは受け付けない。

| 設定 | 範囲 | 既定 |
| --- | --- | --- |
| `retention_ms` | 0、または自ノードの再生期間以上〜2147483647ミリ秒 | 0 |
| `cleanup_interval_ms` | 100〜3600000ミリ秒 | 1000 |
| `cleanup_batch_size` | 1〜65536件 | 4096 |

掃除は送受信と同じ接続プールを利用する。登録とは排他、送信とは共有のロックで動き、登録中は待たずに次回へ回す。複数の掃除処理が重なった場合はロック済みの行をスキップする。1回の削除件数と各SQLの250ミリ秒の期限で処理を制限する。期限超過では削除件数を半減し、設定間隔と1秒の長い方で再試行する。50ミリ秒未満で上限件数を16回連続で削除できたら25%ずつ元の上限へ戻す。その他の失敗は60秒後に再試行する。stderrには認証情報を含まない診断を記録する。削除件数の上限は走査件数の上限ではない。

掃除能力より多く保存すると期限切れの行が残る。間隔とバッチ件数は実際の負荷で調整する。DELETE後の領域はPostgreSQLのVACUUMで再利用され、DBファイルが直ちに縮むわけではない。通常のautovacuum/autoanalyzeは無効にしない。

`schema.sql`は中継の2表だけにvacuum/analyzeのscale factorを0.02、vacuum cost limitを1000に設定する。末尾ページの縮小に伴う排他ロックを避けるため`vacuum_truncate=false`を指定し、領域は次のINSERTで再利用する。DB全体の設定は変更しない。[PostgreSQLの表単位の設定](https://www.postgresql.org/docs/17/sql-createtable.html#SQL-CREATETABLE-STORAGE-PARAMETERS)で調整できる。高負荷でVACUUMが追いつかない場合はDB管理者が起動間隔やI/O予算も確認する。

## 既存DBの更新

新しいバイナリの`--schema`を使って、旧ノードを動かしたまま事前に列と索引を追加できる。大きな表での索引作成は書き込みを待たせるため、下記のconcurrent作成を先に行う。

```bash
~/.local/share/amitoki/plugins/postgres/amitoki-plugin-postgres --schema > schema.sql
# 既存のpending表が大きい場合だけ先に実行する。トランザクションに入れない。
psql "$AMITOKI_POSTGRES_URL" --set ON_ERROR_STOP=1 \
  --command 'CREATE INDEX CONCURRENTLY IF NOT EXISTS pending_frame ON stegrdb_relay.pending (channel, frame_id)'
psql "$AMITOKI_POSTGRES_URL" --set ON_ERROR_STOP=1 -f schema.sql
```

新規DBは`schema.sql`だけで初期化する。concurrent索引作成を中断した場合は`pg_index.indisvalid`を確認し、無効な索引を作り直してから再適用する。`IF NOT EXISTS`は無効な索引の修復をしない。

旧ノードの列は削除無効で初期化される。各ノードが新しい版で再接続すると設定を更新するため、一部だけの更新では掃除を開始しない。停止中の登録も含めて移行する。旧版へ戻す場合は、先に新しい版で`retention_ms=0`へ変更して再接続し、掃除を無効にしてから旧版へ戻す。列と索引は残してよい。

DB再起動によるノード占有接続の切断は、従来どおり恒久エラーとして停止する。本体を明示的に再起動して占有を取り直す。自動再接続とOSサンドボックスはこの変更に含まない。
