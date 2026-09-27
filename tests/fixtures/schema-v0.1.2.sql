-- 旧packets/node_list/firewall_settingsには触れない、中継専用のスキーマ。
-- 既存の未ACKキューを引き継ぐためスキーマ名を維持する。
CREATE SCHEMA IF NOT EXISTS stegrdb_relay;

CREATE TABLE IF NOT EXISTS stegrdb_relay.nodes (
    channel TEXT NOT NULL,
    node_id TEXT NOT NULL,
    PRIMARY KEY (channel, node_id)
);

CREATE TABLE IF NOT EXISTS stegrdb_relay.frames (
    channel TEXT NOT NULL,
    id UUID NOT NULL,
    sender TEXT NOT NULL,
    position BIGINT GENERATED ALWAYS AS IDENTITY,
    payload BYTEA NOT NULL CHECK (octet_length(payload) BETWEEN 14 AND 65535),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (channel, id)
);
CREATE INDEX IF NOT EXISTS frames_channel_created_at ON stegrdb_relay.frames (channel, created_at);

CREATE TABLE IF NOT EXISTS stegrdb_relay.pending (
    channel TEXT NOT NULL,
    node_id TEXT NOT NULL,
    frame_id UUID NOT NULL,
    position BIGINT NOT NULL,
    PRIMARY KEY (channel, node_id, frame_id),
    FOREIGN KEY (channel, node_id) REFERENCES stegrdb_relay.nodes ON DELETE CASCADE,
    FOREIGN KEY (channel, frame_id) REFERENCES stegrdb_relay.frames ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS pending_delivery_order ON stegrdb_relay.pending (channel, node_id, position);
