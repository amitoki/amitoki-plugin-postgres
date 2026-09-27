pub(crate) const LOCK_CHANNEL: &str = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))";
pub(crate) const LOCK_CHANNEL_SHARED: &str = "SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))";

pub(crate) const PUBLISH: &str = "
WITH inserted AS (
    INSERT INTO stegrdb_relay.frames (channel, id, sender, payload)
    SELECT $1, frame.id, $2, frame.payload FROM unnest($3::uuid[], $4::bytea[]) AS frame(id, payload)
    ON CONFLICT (channel, id) DO NOTHING
    RETURNING channel, id, position
)
INSERT INTO stegrdb_relay.pending (channel, node_id, frame_id, position)
SELECT inserted.channel, nodes.node_id, inserted.id, inserted.position
FROM inserted JOIN stegrdb_relay.nodes AS nodes ON nodes.channel = inserted.channel
WHERE nodes.node_id <> $2";

// LIMITを本文JOINより前に適用し、OFFSET 0でLATERALの展開を防ぐ。
// 統計が古くても、小さな配送バッチのために全channel履歴をJOINしない。
pub(crate) const RECEIVE: &str = "
SELECT frame.id, frame.payload, delivery.position
FROM (
    SELECT frame_id, position FROM stegrdb_relay.pending
    WHERE channel = $1 AND node_id = $2
    ORDER BY position LIMIT $3
) AS delivery
CROSS JOIN LATERAL (
    SELECT id, payload FROM stegrdb_relay.frames
    WHERE channel = $1 AND id = delivery.frame_id
    OFFSET 0
) AS frame
ORDER BY delivery.position";

pub(crate) const ACKNOWLEDGE: &str = "
DELETE FROM stegrdb_relay.pending AS pending
USING unnest($3::uuid[], $4::bigint[]) AS acknowledged(id, position)
WHERE pending.channel = $1 AND pending.node_id = $2
AND pending.frame_id = acknowledged.id AND pending.position = acknowledged.position";

pub(crate) const REGISTER: &str = "
INSERT INTO stegrdb_relay.nodes (channel, node_id, replay_window_ms, retention_ms) VALUES ($1, $2, $3, $4)
ON CONFLICT (channel, node_id) DO NOTHING";

pub(crate) const UPDATE_NODE_OPTIONS: &str = "
UPDATE stegrdb_relay.nodes SET replay_window_ms = $3, retention_ms = $4
WHERE channel = $1 AND node_id = $2";

pub(crate) const REPLAY: &str = "
INSERT INTO stegrdb_relay.pending (channel, node_id, frame_id, position)
SELECT channel, $2, id, position FROM stegrdb_relay.frames
WHERE channel = $1 AND sender <> $2
AND created_at >= statement_timestamp() - $3::int * INTERVAL '1 millisecond'
ON CONFLICT DO NOTHING";
