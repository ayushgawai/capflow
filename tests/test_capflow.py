import pytest

from capflow.bench import run as bench_run
from capflow.capture import CDCCapture, SourceDB
from capflow.landing import Warehouse
from capflow.monitor import ReplicationMonitor, backfill, load_checkpoint, save_checkpoint
from capflow.reconcile import gate, reconcile


@pytest.fixture()
def src():
    s = SourceDB()
    s.insert("1", {"id": 1, "amount": 10, "status": "new"})
    s.insert("2", {"id": 2, "amount": 20, "status": "new"})
    s.insert("3", {"id": 3, "amount": 30, "status": "new"})
    return s


def test_capture_poll_advances_cursor_without_reemit(src):
    cap = CDCCapture(src)
    first = cap.poll()
    assert [r.lsn for r in first] == [1, 2, 3]
    assert cap.poll() == []
    src.update("1", {"status": "paid"})
    assert [(r.op, r.pk) for r in cap.poll()] == [("update", "1")]


def test_every_change_lands_exactly_once(src):
    src.update("1", {"amount": 11})
    src.delete("2")
    cap, wh = CDCCapture(src), Warehouse()
    batch = cap.poll()
    s1 = wh.apply_batch(batch)
    s2 = wh.apply_batch(batch)  # replay of same batch
    assert s1["applied"] == 5
    assert s2["applied"] == 0 and s2["skipped_duplicate"] == 5
    report = reconcile(src.snapshot(), wh, src.table)
    assert report.passed and gate(report)


def test_deletes_are_honored(src):
    src.delete("2")
    wh = Warehouse()
    wh.apply_batch(CDCCapture(src).poll())
    assert wh.count(src.table) == 2
    assert "2" not in wh.rows(src.table)


def test_update_ordering_stale_record_does_not_regress(src):
    wh = Warehouse()
    cap = CDCCapture(src)
    wh.apply_batch(cap.poll())
    src.update("1", {"amount": 99})
    newer = cap.poll()
    wh.apply_batch(newer)
    # Replay an older insert/update manually via a fresh stale batch:
    stale = [r for r in CDCCapture(src).peek(0) if r.pk == "1" and r.op == "insert"]
    wh.apply_batch(stale)  # duplicate LSN, skipped
    assert wh.rows(src.table)["1"]["amount"] == 99


def test_full_load_seeds_idempotently(src):
    wh = Warehouse()
    snap = src.snapshot()
    wh.full_load_seed(src.table, snap, snapshot_lsn=src.max_lsn)
    before = wh.rows(src.table)
    wh.full_load_seed(src.table, snap, snapshot_lsn=src.max_lsn)
    wh.full_load_seed(src.table, snap, snapshot_lsn=src.max_lsn)
    assert wh.rows(src.table) == before == snap
    assert wh.count(src.table) == 3


def test_full_load_then_incremental(src):
    wh = Warehouse()
    wh.full_load_seed(src.table, src.snapshot(), snapshot_lsn=src.max_lsn)
    src.update("3", {"status": "shipped"})
    src.insert("4", {"id": 4, "amount": 40, "status": "new"})
    src.delete("1")
    cap = CDCCapture(src, start_lsn=3)
    wh.apply_batch(cap.poll())
    assert reconcile(src.snapshot(), wh, src.table).passed


def test_reconcile_detects_missing_and_diff(src):
    wh = Warehouse()
    wh.full_load_seed(src.table, {"1": {"id": 1, "amount": 999, "status": "new"}},
                      snapshot_lsn=1)
    report = reconcile(src.snapshot(), wh, src.table)
    assert not report.passed and not gate(report)
    assert report.missing_in_target == ["2", "3"]
    assert report.differing == ["1"]


def test_backfill_recovers_from_crash(src, tmp_path):
    src.update("1", {"amount": 111})
    src.delete("3")
    src.insert("4", {"id": 4, "amount": 40, "status": "new"})
    wh = Warehouse()
    cap = CDCCapture(src)
    # Simulate crash after only the first batch (2 records) was applied.
    wh.apply_batch(cap.peek(0, limit=2))
    checkpoint = tmp_path / "checkpoint.json"
    save_checkpoint(checkpoint, wh.last_applied_lsn())
    assert load_checkpoint(checkpoint) == 2
    # Recovery: replay from saved LSN; first records must not double-apply.
    stats = backfill(cap, wh, from_lsn=0)
    assert stats["skipped_duplicate"] == 2
    assert reconcile(src.snapshot(), wh, src.table).passed
    # Re-running the whole backfill is a no-op.
    again = backfill(cap, wh, from_lsn=0)
    assert again["applied"] == 0


def test_monitor_alert_thresholds():
    mon = ReplicationMonitor(max_lag_lsn_warn=5, max_lag_lsn_critical=10)
    mon.record_source(20)
    mon.record_batch(3)
    assert mon.lag_lsn == 17 and mon.health() == "critical"
    mon.record_error("boom")
    assert mon.status()["errors"] == 1
    mon.record_batch(20)
    assert mon.lag_lsn == 0


def test_cursor_persists_across_poller_restart(src, tmp_path):
    path = tmp_path / "cursor.json"
    cap = CDCCapture(src, cursor_path=path)
    assert len(cap.poll()) == 3
    src.insert("9", {"id": 9, "amount": 90, "status": "new"})
    cap2 = CDCCapture(src, cursor_path=path)
    assert [r.pk for r in cap2.poll()] == ["9"]


def test_bench_small_zero_missed():
    result = bench_run(rows=2000, batch_size=500)
    assert result["zero_missed_changes"] is True
    assert result["end_lsn_lag"] == 0
    assert result["final_source_rows"] == result["final_target_rows"]
    assert result["throughput_events_per_sec"] > 0
