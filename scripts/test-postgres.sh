#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# 初回のDB起動を最大60秒まで、readinessが返るまで待つ。
READY_ATTEMPTS=60
READY_INTERVAL_SECONDS=1
POSTGRES_IMAGE=${POSTGRES_IMAGE:-postgres:17-alpine}

# ランダムなホスト側ポートと一時コンテナで既存DBから隔離する。
container=$(docker run --detach --rm --publish 127.0.0.1::5432 \
  --env POSTGRES_HOST_AUTH_METHOD=trust "$POSTGRES_IMAGE")
trap 'docker rm --force --volumes "$container" >/dev/null' EXIT
# 初期化用の一時サーバはUnix socketだけで待受するため、本稼働のTCPを確認する。
for ((attempt=0; attempt<READY_ATTEMPTS; attempt++)); do
  if docker exec "$container" pg_isready --host 127.0.0.1 --username postgres >/dev/null 2>&1; then break; fi
  sleep "$READY_INTERVAL_SECONDS"
done
docker exec "$container" pg_isready --host 127.0.0.1 --username postgres >/dev/null
# 旧スキーマのデータと未ACKが更新で消えないことを先に確認する。
docker exec -i "$container" psql --username postgres --set ON_ERROR_STOP=1 < tests/fixtures/schema-v0.1.2.sql
docker exec -i "$container" psql --username postgres --set ON_ERROR_STOP=1 <<'SQL'
INSERT INTO stegrdb_relay.nodes VALUES ('schema-upgrade', 'receiver');
INSERT INTO stegrdb_relay.frames(channel,id,sender,payload)
VALUES ('schema-upgrade',md5('schema-upgrade')::uuid,'sender',decode(repeat('ab',64),'hex'));
INSERT INTO stegrdb_relay.pending
SELECT channel,'receiver',id,position FROM stegrdb_relay.frames WHERE channel='schema-upgrade';
SQL
docker exec -i "$container" psql --username postgres --set ON_ERROR_STOP=1 < schema.sql
port=$(docker port "$container" 5432/tcp | cut -d: -f2)
export AMITOKI_TEST_POSTGRES_URL="host=127.0.0.1 port=$port user=postgres dbname=postgres sslmode=disable"
# schema.sqlは既存データを維持したまま再適用できる必要がある。
docker exec -i "$container" psql --username postgres --set ON_ERROR_STOP=1 < schema.sql
docker exec -i "$container" psql --username postgres --set ON_ERROR_STOP=1 <<'SQL'
DO $$ BEGIN
    IF (SELECT count(*) FROM stegrdb_relay.pending WHERE channel='schema-upgrade') <> 1
       OR (SELECT encode(payload,'hex') FROM stegrdb_relay.frames WHERE channel='schema-upgrade') IS DISTINCT FROM repeat('ab',64)
       OR NOT EXISTS (SELECT 1 FROM stegrdb_relay.nodes WHERE channel='schema-upgrade' AND retention_ms=0 AND replay_window_ms=2147483647)
    THEN RAISE EXCEPTION 'Schema update changed existing delivery or retention'; END IF;
END $$;
DELETE FROM stegrdb_relay.nodes WHERE channel='schema-upgrade';
DELETE FROM stegrdb_relay.frames WHERE channel='schema-upgrade';
SQL
cargo test --tests --locked -- --ignored --nocapture
# DBの既定が強い分離レベルでも、登録/送信はロック後の最新状態を参照する。
docker exec "$container" psql --username postgres --set ON_ERROR_STOP=1 \
  --command "ALTER DATABASE postgres SET default_transaction_isolation TO 'repeatable read'"
cargo test --test concurrency --locked -- --ignored --nocapture
