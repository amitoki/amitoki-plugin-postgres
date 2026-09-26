pub(crate) const LOCK_CHANNEL: &str = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))";

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

pub(crate) const RECEIVE: &str = "
SELECT frames.id, frames.payload
FROM stegrdb_relay.pending AS pending
JOIN stegrdb_relay.frames AS frames ON frames.channel = pending.channel AND frames.id = pending.frame_id
WHERE pending.channel = $1 AND pending.node_id = $2
ORDER BY pending.position LIMIT $3";

pub(crate) const ACKNOWLEDGE: &str = "
DELETE FROM stegrdb_relay.pending
WHERE channel = $1 AND node_id = $2 AND frame_id = ANY($3::uuid[])";

pub(crate) const REGISTER: &str = "
INSERT INTO stegrdb_relay.nodes (channel, node_id) VALUES ($1, $2)
ON CONFLICT (channel, node_id) DO NOTHING";

pub(crate) const REPLAY: &str = "
INSERT INTO stegrdb_relay.pending (channel, node_id, frame_id, position)
SELECT channel, $2, id, position FROM stegrdb_relay.frames
WHERE channel = $1 AND sender <> $2
AND created_at >= clock_timestamp() - $3::int * INTERVAL '1 millisecond'
ON CONFLICT DO NOTHING";
