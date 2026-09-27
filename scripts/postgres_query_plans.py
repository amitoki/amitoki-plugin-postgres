"""受信と初回再生のSQLを、初期統計・更新後の統計・蓄積済み履歴で比較する。"""

import json
import re

HISTORY_FRAMES = 250_000
RECEIVE_FRAMES = 10_000
PLAN_REPETITIONS = 3
RECEIVE_LIMIT = 128


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


def frame_rows_examined(plan):
    rows = 0
    if plan.get("Relation Name") == "frames":
        rows = (plan["Actual Rows"] + plan.get("Rows Removed by Filter", 0)) * plan["Actual Loops"]
    return rows + sum(frame_rows_examined(child) for child in plan.get("Plans", []))


def check_receive_work(plans):
    # 時間の閾値ではなく、128件のために履歴を128回全走査する回帰を検出する。
    for execution in plans:
        examined = frame_rows_examined(execution["Plan"])
        assert examined <= RECEIVE_FRAMES + RECEIVE_LIMIT, f"受信が履歴を繰り返し走査しました: {examined}行"


def measure_receive(sql, sources):
    # 自動ANALYZEとの競合を避け、この診断中だけ試験テーブルを手動管理する。
    sql(f"""
        ALTER TABLE stegrdb_relay.frames SET (autovacuum_enabled=false);
        ALTER TABLE stegrdb_relay.pending SET (autovacuum_enabled=false);
        TRUNCATE stegrdb_relay.pending,stegrdb_relay.frames,stegrdb_relay.nodes RESTART IDENTITY;
        INSERT INTO stegrdb_relay.frames(channel,id,sender,payload)
        SELECT 'previous-channel',md5(value::text)::uuid,'source',decode(repeat('ab',1400),'hex')
        FROM generate_series(1,{RECEIVE_FRAMES}) AS value;
        ANALYZE stegrdb_relay.frames;
        TRUNCATE stegrdb_relay.pending,stegrdb_relay.frames,stegrdb_relay.nodes RESTART IDENTITY;
        ANALYZE stegrdb_relay.frames; ANALYZE stegrdb_relay.pending;
        INSERT INTO stegrdb_relay.nodes VALUES ('bench-receive','receiver');
        INSERT INTO stegrdb_relay.frames(channel,id,sender,payload)
        SELECT 'bench-receive', md5(value::text)::uuid, 'source', decode(repeat('ab',64),'hex')
        FROM generate_series(1,{RECEIVE_FRAMES}) AS value;
        INSERT INTO stegrdb_relay.pending
        SELECT channel,'receiver',id,position FROM stegrdb_relay.frames ORDER BY position DESC LIMIT {RECEIVE_LIMIT};
    """)
    candidates = {name: query(source, "RECEIVE", ["'bench-receive'", "'receiver'", str(RECEIVE_LIMIT)]) for name, source in sources.items()}
    # 受領情報の世代列が増えても、配送するUUID・本文・順序を比較する。
    def frames(statement):
        return [line.split("|")[:2] for line in sql(statement).splitlines()]

    received = frames(candidates["current"])
    assert len(received) == RECEIVE_LIMIT, "受信SQLが期待した128件を返しません"
    for statement in candidates.values():
        assert received == frames(statement), "受信SQLの比較元と結果が変わりました"
    cold = compare_plans(sql, candidates)
    check_receive_work(cold["current"])
    sql("ANALYZE stegrdb_relay.frames; ANALYZE stegrdb_relay.pending;")
    analyzed = compare_plans(sql, candidates)
    check_receive_work(analyzed["current"])
    sql("ALTER TABLE stegrdb_relay.frames RESET (autovacuum_enabled); ALTER TABLE stegrdb_relay.pending RESET (autovacuum_enabled);")
    return {"frames": RECEIVE_FRAMES, "pending": RECEIVE_LIMIT, "equivalent_output": True, "bounded_work": True, "initial_statistics": cold, "updated_statistics": analyzed}


def measure_replay(sql, sources):
    sql(f"""
        INSERT INTO stegrdb_relay.nodes VALUES ('bench-history','late');
        INSERT INTO stegrdb_relay.frames(channel,id,sender,payload,created_at)
        SELECT 'bench-history',md5(value::text)::uuid,'source',decode(repeat('ab',64),'hex'),
               statement_timestamp()-interval '1 day'
        FROM generate_series(1,{HISTORY_FRAMES}) AS value;
        ANALYZE stegrdb_relay.frames;
    """)
    candidates = {name: query(source, "REPLAY", ["'bench-history'", "'late'", "4000"]) for name, source in sources.items()}
    return {"history_frames": HISTORY_FRAMES, "plans": compare_plans(sql, candidates)}


def measure_query_plans(sql, root, *, baseline=None):
    sources = {"current": (root / "src/queries.rs").read_text()}
    if baseline:
        sources["baseline"] = baseline.read_text()
    return {"receive": measure_receive(sql, sources), "replay": measure_replay(sql, sources)}
