"""CLI benchmark: replay a seeded change workload and validate it.

Workload: full-load seed of rows/2 keys, then rows/2 CDC changes
(insert/update/delete mix, deterministic seed). Reports throughput,
LSN lag, wall-clock replication lag, and zero-missed-change validation
via reconciliation. Honest scale: default 1,000,000 total rows/changes;
pass --rows 10000000 for the 10M variant (much slower in pure Python).
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time

from .capture import CDCCapture, SourceDB
from .landing import Warehouse
from .monitor import ReplicationMonitor
from .reconcile import reconcile


def run(rows: int = 1_000_000, batch_size: int = 5000, seed: int = 42) -> dict:
    rng = random.Random(seed)
    table = "dbo.orders"
    src = SourceDB(table)
    wh = Warehouse(":memory:", fast=True)
    mon = ReplicationMonitor()
    seed_n = rows // 2
    changes_n = rows - seed_n

    t0 = time.perf_counter()
    # Generate workload on the source (full-load snapshot + CDC log).
    live: list[str] = []
    for i in range(seed_n):
        pk = str(i)
        src.insert(pk, {"id": i, "amount": rng.randint(1, 10000), "status": "new"})
        live.append(pk)
    snapshot_lsn = src.max_lsn
    snapshot = src.snapshot()
    next_id = seed_n
    for _ in range(changes_n):
        r = rng.random()
        if r < 0.40 or not live:
            pk = str(next_id); next_id += 1
            src.insert(pk, {"id": int(pk), "amount": rng.randint(1, 10000), "status": "new"})
            live.append(pk)
        elif r < 0.80:
            pk = rng.choice(live)
            src.update(pk, {"amount": rng.randint(1, 10000), "status": "paid"})
        else:
            idx = rng.randrange(len(live)); pk = live.pop(idx)
            src.delete(pk)
    gen_s = time.perf_counter() - t0
    mon.record_source(src.max_lsn, src.log[-1].commit_ts if src.log else None)

    # Replicate: full load, then incremental batches after the snapshot.
    t1 = time.perf_counter()
    wh.full_load_seed(table, snapshot, snapshot_lsn=snapshot_lsn)
    # Incremental phase starts exactly at the snapshot LSN.
    cap = CDCCapture(src, start_lsn=snapshot_lsn)
    applied = 0
    max_batch_lag = 0
    while True:
        recs = cap.poll(limit=batch_size)
        if not recs:
            break
        stats = wh.apply_batch(recs)
        applied += stats["applied"]
        max_batch_lag = max(max_batch_lag, src.max_lsn - recs[-1].lsn)
        mon.record_batch(recs[-1].lsn, len(recs))
    repl_s = time.perf_counter() - t1

    t2 = time.perf_counter()
    report = reconcile(src.snapshot(), wh, table)
    recon_s = time.perf_counter() - t2

    total_s = gen_s + repl_s
    result = {
        "rows_requested": rows, "seed_rows": seed_n, "change_events": src.max_lsn,
        "final_source_rows": len(src.rows), "final_target_rows": wh.count(table),
        "applied_events": applied, "batch_size": batch_size,
        "workload_gen_seconds": round(gen_s, 3), "replication_seconds": round(repl_s, 3),
        "reconcile_seconds": round(recon_s, 3),
        "throughput_events_per_sec": round(src.max_lsn / repl_s, 1) if repl_s else None,
        "end_lsn_lag": src.max_lsn - wh.last_applied_lsn(), "max_batch_lsn_lag": max_batch_lag,
        "replication_lag_seconds_wall": round(repl_s, 3),
        "zero_missed_changes": bool(report.passed and src.max_lsn - wh.last_applied_lsn() == 0),
        "reconcile_passed": report.passed, "source_checksum": report.source_checksum[:16],
        "target_checksum": report.target_checksum[:16], "monitor": mon.status(),
    }
    wh.close()
    return result


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="capflow-bench", description=__doc__)
    p.add_argument("--rows", type=int, default=1_000_000, help="total seeded rows+changes (default 1,000,000)")
    p.add_argument("--batch-size", type=int, default=5000)
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args(argv)
    result = run(a.rows, a.batch_size, a.seed)
    print(json.dumps(result, indent=2))
    return 0 if result["zero_missed_changes"] else 1


if __name__ == "__main__":
    sys.exit(main())
