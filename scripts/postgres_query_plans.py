"""受信と初回再生のSQLを、初期統計・更新後の統計・蓄積済み履歴で比較する。"""

import json
import re

HISTORY_FRAMES = 250_000
RECEIVE_FRAMES = 10_000
PLAN_REPETITIONS = 3


def query(source, name, parameters):
    statement = re.search(rf'const {name}: &str = "(.*?)";', source, re.DOTALL).group(1)
    for position, value in enumerate(parameters, 1):
        statement = statement.replace(f"${position}", value)
    return statement


def compare_plans(sql, candidates):
    plans = {name: [] for name in candidates}
    for repetition in range(PLAN_REPETITIONS):
        order = list(candidates.items())
        if repetition % 2:
            order.reverse()
        for name, statement in order:
            plans[name].append(json.loads(sql(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {statement}"))[0])
    return plans


def measure_receive(sql, source):
    # 自動ANALYZEとの競合を避け、この診断中だけ試験テーブルを手動管理する。
    sql(f"""
        ALTER TABLE stegrdb_relay.frames SET (autovacuum_enabled=false);
        ALTER TABLE stegrdb_relay.pending SET (autovacuum_enabled=false);
        TRUNCATE stegrdb_relay.pending,stegrdb_relay.frames,stegrdb_relay.nodes RESTART IDENTITY;
        ANALYZE stegrdb_relay.frames; ANALYZE stegrdb_relay.pending;
        INSERT INTO stegrdb_relay.nodes VALUES ('bench-receive','receiver');
        INSERT INTO stegrdb_relay.frames(channel,id,sender,payload)
        SELECT 'bench-receive', md5(value::text)::uuid, 'source', decode(repeat('ab',64),'hex')
        FROM generate_series(1,{RECEIVE_FRAMES}) AS value;
        INSERT INTO stegrdb_relay.pending
        SELECT channel,'receiver',id,position FROM stegrdb_relay.frames ORDER BY position DESC LIMIT 128;
    """)
    current = query(source, "RECEIVE", ["'bench-receive'", "'receiver'", "128"])
    lookup = """
        SELECT frame.id,frame.payload FROM (
            SELECT frame_id,position FROM stegrdb_relay.pending
            WHERE channel='bench-receive' AND node_id='receiver' ORDER BY position LIMIT 128
        ) AS delivery CROSS JOIN LATERAL (
            SELECT id,payload FROM stegrdb_relay.frames
            WHERE channel='bench-receive' AND id=delivery.frame_id OFFSET 0
        ) AS frame ORDER BY delivery.position
    """
    received = sql(current).splitlines()
    assert len(received) == 128, "受信SQLが期待した128件を返しません"
    assert received == sql(lookup).splitlines(), "受信SQLの候補で結果が変わりました"
    candidates = {"current": current, "bounded_lookup": lookup}
    cold = compare_plans(sql, candidates)
    sql("ANALYZE stegrdb_relay.frames; ANALYZE stegrdb_relay.pending;")
    analyzed = compare_plans(sql, candidates)
    sql("ALTER TABLE stegrdb_relay.frames RESET (autovacuum_enabled); ALTER TABLE stegrdb_relay.pending RESET (autovacuum_enabled);")
    return {"frames": RECEIVE_FRAMES, "pending": 128, "equivalent_output": True, "initial_statistics": cold, "updated_statistics": analyzed}


def measure_replay(sql, source):
    sql(f"""
        INSERT INTO stegrdb_relay.nodes VALUES ('bench-history','late');
        INSERT INTO stegrdb_relay.frames(channel,id,sender,payload,created_at)
        SELECT 'bench-history',md5(value::text)::uuid,'source',decode(repeat('ab',64),'hex'),
               statement_timestamp()-interval '1 day'
        FROM generate_series(1,{HISTORY_FRAMES}) AS value;
        ANALYZE stegrdb_relay.frames;
    """)
    current = query(source, "REPLAY", ["'bench-history'", "'late'", "4000"])
    candidates = {"current": current, "statement_timestamp": current.replace("clock_timestamp()", "statement_timestamp()")}
    return {"history_frames": HISTORY_FRAMES, "plans": compare_plans(sql, candidates)}


def measure_query_plans(sql, root):
    source = (root / "src/queries.rs").read_text()
    return {"receive": measure_receive(sql, source), "replay": measure_replay(sql, source)}
