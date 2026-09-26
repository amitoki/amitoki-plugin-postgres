#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# 初回のDB起動を最大60秒まで、readinessが返るまで待つ。
READY_ATTEMPTS=60
READY_INTERVAL_SECONDS=1

# ランダムなホスト側ポートと一時コンテナで既存DBから隔離する。
container=$(docker run --detach --rm --publish 127.0.0.1::5432 \
  --env POSTGRES_HOST_AUTH_METHOD=trust postgres:17-alpine)
trap 'docker stop "$container" >/dev/null' EXIT
# 初期化用の一時サーバはUnix socketだけで待受するため、本稼働のTCPを確認する。
for ((attempt=0; attempt<READY_ATTEMPTS; attempt++)); do
  if docker exec "$container" pg_isready --host 127.0.0.1 --username postgres >/dev/null 2>&1; then break; fi
  sleep "$READY_INTERVAL_SECONDS"
done
docker exec "$container" pg_isready --host 127.0.0.1 --username postgres >/dev/null
docker exec -i "$container" psql --username postgres --set ON_ERROR_STOP=1 < schema.sql
port=$(docker port "$container" 5432/tcp | cut -d: -f2)
export AMITOKI_TEST_POSTGRES_URL="host=127.0.0.1 port=$port user=postgres dbname=postgres sslmode=disable"
cargo test --test delivery --locked -- --ignored --nocapture
