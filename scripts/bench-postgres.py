#!/usr/bin/env python3
"""一時コンテナで実配送を測り、任意の比較用バイナリとSQL実行計画を記録する。"""

import argparse
from datetime import datetime
import hashlib
from itertools import product
import json
import os
from pathlib import Path
import platform
import subprocess
import time

from postgres_query_plans import measure_query_plans

ROOT = Path(__file__).resolve().parents[1]
READY_TIMEOUT_SECONDS = 60
READY_POLL_SECONDS = 1
READINESS_COMMAND_TIMEOUT_SECONDS = 10
COMMAND_TIMEOUT_SECONDS = 180
# 3ノードの比較を一定条件で行い、ホストの資源を使い切らない。
DATABASE_CPUS = 4
DATABASE_MEMORY_MIB = 2048
# 小さいバッチでも秒単位で測り、大きいバッチでは配送件数を増やす。
FRAMES_PER_NODE = {1: 1024, 32: 4096, 128: 4096}


def command(arguments, **options):
    return subprocess.check_output(arguments, text=True, timeout=COMMAND_TIMEOUT_SECONDS, **options).strip()


def sql(container, statement):
    return command(["docker", "exec", "-i", container, "psql", "-U", "postgres", "-At", "-v", "ON_ERROR_STOP=1"], input=statement)


def await_database(container):
    deadline = time.monotonic() + READY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        # 初期化中のUnix socketではなく、本稼働のTCP待受を確認する。
        ready = subprocess.run(["docker", "exec", container, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=READINESS_COMMAND_TIMEOUT_SECONDS)
        if ready.returncode == 0:
            return
        time.sleep(READY_POLL_SECONDS)
    raise TimeoutError("PostgreSQLの起動が完了しません")


def benchmark(container, settings, *, report, destination):
    binaries = {"current": settings.binary.resolve()}
    if settings.compare_binary:
        binaries["candidate"] = settings.compare_binary.resolve()
    report["binaries"] = {name: {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for name, path in binaries.items()}
    if len({details["sha256"] for details in report["binaries"].values()}) != len(binaries):
        raise ValueError("比較元と候補の実行ファイルが同一です。ビルド先を分けてください")
    port = command(["docker", "port", container, "5432/tcp"]).rsplit(":", 1)[1]
    environment = dict(os.environ, AMITOKI_BENCH_POSTGRES_URL=f"host=127.0.0.1 port={port} user=postgres dbname=postgres sslmode=disable")
    for repetition, nodes, frame_bytes, batch in product(range(settings.repetitions), (2, 3), (64, 1400), FRAMES_PER_NODE):
        frames = FRAMES_PER_NODE[batch]
        order = list(binaries.items())
        if repetition % 2:
            order.reverse()
        for name, binary in order:
            # 前の条件の行・統計を引き継がず、試験DB内だけを初期化する。
            sql(container, "TRUNCATE stegrdb_relay.pending, stegrdb_relay.frames, stegrdb_relay.nodes RESTART IDENTITY; "
                           "ANALYZE stegrdb_relay.frames; ANALYZE stegrdb_relay.pending; CHECKPOINT;")
            measured = json.loads(command([str(binary), str(nodes), str(batch), str(frame_bytes), str(frames), str(settings.warmup_frames)], env=environment))
            assert measured["verified_deliveries"] == nodes * frames * (nodes - 1), measured
            assert measured["pending_after_ack"] == 0 and measured["errors"] == 0, measured
            report["cases"].append({"variant": name, "repetition": repetition, **measured})
            save_report(destination, report)
            print(f"{name} repeat={repetition + 1} nodes={nodes} bytes={frame_bytes} batch={batch}: "
                  f"{measured['completed_frames_per_second']:.0f} frames/s", flush=True)


def save_report(destination, report):
    destination.write_text(json.dumps(report, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/examples/relay_bench")
    parser.add_argument("--compare-binary", type=Path)
    parser.add_argument("--repetitions", type=int, choices=range(1, 11), default=3)
    parser.add_argument("--warmup-frames", type=int, default=1024)
    parser.add_argument("--plans-only", action="store_true", help="受信と初回再生のSQLだけを比較する")
    parser.add_argument("--image", default="postgres:17-alpine")
    parser.add_argument("--output", type=Path, required=True)
    settings = parser.parse_args()
    if not 0 <= settings.warmup_frames <= 10_000:
        parser.error("--warmup-framesは0〜10000件で指定してください")
    settings.output.parent.mkdir(parents=True, exist_ok=True)
    report = {"started_at": datetime.now().astimezone().isoformat(), "status": "running", "cases": [],
              "host": {"machine": platform.machine(), "logical_cpus": os.cpu_count()},
              "database_limits": {"cpus": DATABASE_CPUS, "memory_bytes": DATABASE_MEMORY_MIB * 1024**2},
              "plugin_head": command(["git", "-C", str(ROOT), "rev-parse", "HEAD"])}
    container = command(["docker", "run", "--detach", "--rm", "--publish", "127.0.0.1::5432",
                         "--cpus", str(DATABASE_CPUS), "--memory", f"{DATABASE_MEMORY_MIB}m", "--env", "POSTGRES_HOST_AUTH_METHOD=trust",
                         settings.image, "-c", "shared_preload_libraries=pg_stat_statements"])
    try:
        await_database(container)
        report["image"] = command(["docker", "inspect", "--format", "{{.Image}}", container])
        sql(container, (ROOT / "schema.sql").read_text() + "\nCREATE EXTENSION pg_stat_statements;")
        report["database"] = json.loads(sql(container, "SELECT json_build_object('version',version(),'fsync',current_setting('fsync'),"
                                           "'synchronous_commit',current_setting('synchronous_commit'),'shared_buffers',current_setting('shared_buffers'))"))
        if not settings.plans_only:
            benchmark(container, settings, report=report, destination=settings.output)
        report["query_plans"] = measure_query_plans(lambda statement: sql(container, statement), ROOT)
        report["sql_statistics"] = json.loads(sql(container, "SELECT coalesce(json_agg(summary),'[]'::json) FROM "
            "(SELECT query,calls,total_exec_time,rows,shared_blks_hit,shared_blks_read,wal_bytes "
            "FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 15) AS summary"))
        report["status"] = "passed"
    except BaseException as error:
        report["status"] = "interrupted" if isinstance(error, KeyboardInterrupt) else "failed"
        report["error"] = str(error)
        raise
    finally:
        report["finished_at"] = datetime.now().astimezone().isoformat()
        save_report(settings.output, report)
        # 匿名volumeも消し、この試験が作ったコンテナだけを片付ける。
        command(["docker", "rm", "--force", "--volumes", container])


if __name__ == "__main__":
    main()
