# capflow

A change-data-capture (CDC) replication pipeline in Python, modeled on
SQL Server CDC / Qlik Replicate style log-based replication: read the change
log (inserts, updates, deletes), poll with a commit-LSN cursor, land changes
in a warehouse table, reconcile row-by-row against the source of record, and
monitor lag — with backfill/replay from a saved LSN when something breaks.

The source and warehouse are local stand-ins (an in-memory change log and
SQLite), so the whole pipeline — full load, incremental CDC, reconciliation,
backfill — runs and is tested end-to-end with no external services.

## Design

```
                        +----------------------+
                        |      SourceDB        |   source of record (stand-in)
                        |  rows + change log   |   every insert/update/delete
                        +----------+-----------+   appends a ChangeRecord
                                   |               lsn, seqval, op, pk,
                                   |               before, after, commit_ts
                                   v
                        +----------------------+
              poll()    |      CDCCapture      |   commit-LSN cursor
           -----------> |  bisect from cursor  |   (optionally persisted
                        +----------+-----------+    to a JSON cursor file)
                                   | ChangeRecord batches
                                   v
   full_load_seed()     +----------------------+
  --------------------> |       Warehouse      |   SQLite warehouse stand-in
   snapshot @ LSN       |  target_rows         |   pk, payload, last_lsn
                        |  applied_lsn ledger  |   same-txn exactly-once
                        +----------+-----------+
                                   |
                    +--------------+---------------+
                    v                              v
          +------------------+           +----------------------+
          |   reconcile.py   |           |     monitor.py       |
          | count / checksum |           | lag (LSN + seconds), |
          | sample diff      |           | error counts, alert  |
          | -> gate: batch   |           | thresholds, health   |
          |    complete only |           +----------------------+
          |    if passed     |           | backfill(): replay
          +------------------+           | from a saved LSN     |
                                         +----------------------+

   bench.py: seeds a workload on SourceDB, replicates it through the
   pipeline above, then validates zero missed changes via reconcile.
```

## Modules

| Module | Responsibility | Key pieces |
|---|---|---|
| `capflow/capture.py` | CDC capture modeled on SQL Server CDC. Appends structured change records to a log and polls from a commit-LSN cursor. | `SourceDB` (rows + append-only log, monotonic LSN), `ChangeRecord` (`lsn, seqval, table, op, pk, before, after, commit_ts`), `CDCCapture.poll()` / `peek()`, JSON cursor persistence |
| `capflow/landing.py` | Landing/upsert layer into a warehouse table (SQLite stand-in). Full-load seeding, then incremental upserts; deletes honored; stale/out-of-order records ignored. | `Warehouse.full_load_seed()` (idempotent, LSN-guarded), `apply_batch()` (per-LSN dedup via `applied_lsn` ledger in the same transaction as the upsert), `last_lsn` ordering guard |
| `capflow/reconcile.py` | Row-level reconciliation against the source of record that gates a batch before it is marked complete. | `checksum()` (SHA-256 over canonical rows), `reconcile()` → count, checksum, missing/extra/differing sample diff, `gate()` |
| `capflow/monitor.py` | Replication lag and error tracking with alert thresholds, plus backfill/replay from a saved LSN. | `ReplicationMonitor` (`lag_lsn`, `lag_seconds`, warn/critical thresholds, `health()`), `save_checkpoint()` / `load_checkpoint()`, `backfill()` (idempotent replay) |
| `capflow/bench.py` | CLI benchmark replaying a seeded change workload; reports lag, throughput, and zero-missed-change validation. | `python -m capflow.bench --rows N`, deterministic seed, JSON report |
| `tests/test_capflow.py` | Pytest coverage of the CDC invariants. | Exactly-once landing, deletes honored, update ordering, idempotent full load, crash/backfill recovery, reconcile gating, monitor thresholds, cursor persistence, small bench |

## How exactly-once works

1. Every change gets a monotonically increasing **LSN**.
2. `CDCCapture` only emits records with `lsn > cursor`, then advances the cursor.
3. `Warehouse.apply_batch()` writes each record and its LSN into the
   `applied_lsn` ledger **in the same transaction**. Replayed LSNs are
   skipped as duplicates; records older than a row's `last_lsn` are skipped
   as stale. A crash mid-batch rolls the whole batch back.
4. After landing, `reconcile()` compares count + checksum + a sample diff
   against the source. `gate(report)` is the only path to "batch complete".

## Run

```bash
python -m pytest -q
```

```
11 passed in 0.19s
```

## Benchmark

`bench.py` defaults to 1,000,000 total seeded rows/changes:
a 500k-row full-load seed plus 500k incremental CDC events
(insert/update/delete mix, seed 42). The 10M variant is available with
`--rows 10000000` — it is not the default because, honestly, pure-Python
workload generation and per-row SQLite upserts take minutes at that scale.

```bash
python -m capflow.bench --rows 1000000
```

Sanity run on this machine (Windows, Python 3.12; your numbers will vary):

```json
{
  "rows_requested": 1000000,
  "seed_rows": 500000,
  "change_events": 1000000,
  "final_source_rows": 600247,
  "final_target_rows": 600247,
  "applied_events": 500000,
  "batch_size": 5000,
  "workload_gen_seconds": 29.929,
  "replication_seconds": 9.521,
  "reconcile_seconds": 7.587,
  "throughput_events_per_sec": 105026.6,
  "end_lsn_lag": 0,
  "max_batch_lsn_lag": 495000,
  "replication_lag_seconds_wall": 9.521,
  "zero_missed_changes": true,
  "reconcile_passed": true,
  "source_checksum": "ea3279a17a188211",
  "target_checksum": "ea3279a17a188211",
  "monitor": {
    "health": "ok",
    "lag_lsn": 0,
    "lag_seconds": 0.0,
    "source_lsn": 1000000,
    "applied_lsn": 1000000,
    "errors": 0,
    "batches": 100,
    "alerts": []
  }
}
```

Notes on these numbers: `throughput_events_per_sec` is total change events
(500k delivered by the bulk full-load seed + 500k applied incrementally)
divided by replication wall time. `max_batch_lsn_lag` is expected — batches
are applied in 5k chunks while the source is already fully generated, so
early batches lag by design; `end_lsn_lag` is what matters, and it is 0.
Validation is reconciliation-based: equal counts, equal SHA-256 checksums,
`zero_missed_changes: true`.

Smaller/quicker runs:

```bash
python -m capflow.bench --rows 100000      # ~seconds
python -m capflow.bench --rows 10000000    # 10M variant, minutes; not run for this README
```

## Backfill / replay

```python
from capflow.monitor import backfill, load_checkpoint

saved = load_checkpoint("checkpoint.json")   # LSN saved before the crash
stats = backfill(capture, warehouse, from_lsn=saved)
# Already-applied LSNs are skipped by the ledger, so replay is safe to re-run.
```

## Limitations (honest)

- **Stand-ins, not real systems.** No live SQL Server connection, no
  `cdc.fn_cdc_get_all_changes_*` queries, no real cloud warehouse
  (BigQuery/Snowflake/etc.). SQLite and the in-memory log model the
  semantics — LSN ordering, before/after images, commit cursor — not wire
  protocols, permissions, or SQL Server CDC retention/cleanup jobs.
- **Single table, string primary keys, whole-row payloads.** No schema
  evolution / DDL capture, no multi-table transactions, no column-level
  filtering or masking, no composite keys beyond string coercion.
- **Polling, not streaming.** Lag is batch-shaped; there is no log tailing,
  no Debezium/Kafka-style streaming, and no parallel apply (single-threaded,
  one SQLite connection).
- **Exactly-once is scoped to this process's ledger.** The `applied_lsn`
  ledger gives idempotent replay against the same warehouse database; it is
  not a distributed transaction across a real source and warehouse.
- **Reconciliation is full-table.** Count + checksum over every row does not
  scale to very large tables unchanged; production systems reconcile in
  key-range chunks. The sample diff is capped (default 20 keys per class).
- **Benchmarks are synthetic and local.** In-memory/SQLite numbers say
  something about the pipeline logic and nothing about network, warehouse
  merge cost, or real SQL Server log-read throughput. The 10M run was not
  executed for this README; the 1M numbers above were.
- **No auth, encryption, or secrets handling** — there is nothing to connect
  to yet. Adding a real source/warehouse means adding all three.

## License

MIT — see [LICENSE](LICENSE). Copyright (c) 2026 Ayush Gawai.
