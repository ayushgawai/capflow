"""Landing / upsert layer: warehouse table backed by SQLite (warehouse stand-in).

Exactly-once is enforced with an ``applied_lsn`` ledger written in the same
transaction as the row upsert/delete. Stale records (LSN <= row's last_lsn)
are ignored, so out-of-order or replayed batches cannot regress a row.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Iterable

from .capture import ChangeRecord


class Warehouse:
    def __init__(self, path: str = ":memory:", fast: bool = False):
        self.conn = sqlite3.connect(path)
        if fast:
            self.conn.execute("PRAGMA journal_mode=MEMORY")
            self.conn.execute("PRAGMA synchronous=OFF")
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS target_rows(
              table_name TEXT NOT NULL, pk TEXT NOT NULL, payload TEXT,
              last_lsn INTEGER NOT NULL, PRIMARY KEY(table_name, pk));
            CREATE TABLE IF NOT EXISTS applied_lsn(
              lsn INTEGER PRIMARY KEY, table_name TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            """
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    # -- full load ---------------------------------------------------------
    def full_load_seed(self, table: str, rows: dict[str, dict[str, Any]], snapshot_lsn: int = 0) -> int:
        """Idempotent seeding: re-running with the same snapshot changes nothing.

        Rows are only overwritten by a snapshot at least as new as the row
        already landed (``excluded.last_lsn >= target_rows.last_lsn``), so a
        stale re-seed cannot regress rows advanced by incremental CDC.
        """
        cur = self.conn.cursor()
        cur.executemany(
            """INSERT INTO target_rows(table_name,pk,payload,last_lsn) VALUES(?,?,?,?)
               ON CONFLICT(table_name,pk) DO UPDATE SET payload=excluded.payload,last_lsn=excluded.last_lsn
               WHERE excluded.last_lsn >= target_rows.last_lsn""",
            [(table, str(pk), json.dumps(row, sort_keys=True), snapshot_lsn)
             for pk, row in rows.items()],
        )
        self._set_meta_no_commit("snapshot_lsn", str(snapshot_lsn))
        # The snapshot covers the source up to snapshot_lsn, so the applied
        # watermark advances to it (rows themselves guard by last_lsn).
        if snapshot_lsn > self.last_applied_lsn():
            self._set_meta_no_commit("last_applied_lsn", str(snapshot_lsn))
        self.conn.commit()
        return len(rows)

    # -- incremental -------------------------------------------------------
    def apply_batch(self, records: Iterable[ChangeRecord]) -> dict[str, int]:
        stats = {"applied": 0, "skipped_duplicate": 0, "skipped_stale": 0, "deletes": 0}
        cur = self.conn.cursor()
        last = self.last_applied_lsn()
        try:
            for r in sorted(records, key=lambda x: (x.lsn, x.seqval)):
                cur.execute("SELECT 1 FROM applied_lsn WHERE lsn=?", (r.lsn,))
                if cur.fetchone():
                    stats["skipped_duplicate"] += 1
                    continue
                cur.execute("SELECT last_lsn FROM target_rows WHERE table_name=? AND pk=?", (r.table, r.pk))
                row = cur.fetchone()
                if row and row[0] > r.lsn:
                    stats["skipped_stale"] += 1
                elif r.op == "delete":
                    cur.execute("DELETE FROM target_rows WHERE table_name=? AND pk=?", (r.table, r.pk))
                    stats["deletes"] += 1
                    stats["applied"] += 1
                elif r.op in ("insert", "update"):
                    if r.after is None:
                        raise ValueError(f"{r.op} at lsn {r.lsn} has no after-image")
                    cur.execute(
                        """INSERT INTO target_rows(table_name,pk,payload,last_lsn) VALUES(?,?,?,?)
                           ON CONFLICT(table_name,pk) DO UPDATE SET payload=excluded.payload,last_lsn=excluded.last_lsn
                           WHERE excluded.last_lsn >= target_rows.last_lsn""",
                        (r.table, r.pk, json.dumps(r.after, sort_keys=True), r.lsn),
                    )
                    stats["applied"] += 1
                else:
                    raise ValueError(f"unknown op {r.op!r} at lsn {r.lsn}")
                cur.execute("INSERT INTO applied_lsn(lsn,table_name) VALUES(?,?)", (r.lsn, r.table))
                last = max(last, r.lsn)
            self._set_meta_no_commit("last_applied_lsn", str(last))
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return stats

    # -- reads -------------------------------------------------------------
    def rows(self, table: str) -> dict[str, dict[str, Any]]:
        cur = self.conn.execute("SELECT pk,payload FROM target_rows WHERE table_name=?", (table,))
        return {pk: json.loads(p) for pk, p in cur.fetchall()}

    def count(self, table: str) -> int:
        return int(self.conn.execute("SELECT COUNT(*) FROM target_rows WHERE table_name=?", (table,)).fetchone()[0])

    def last_applied_lsn(self) -> int:
        v = self._get_meta("last_applied_lsn")
        return int(v) if v else 0

    def _get_meta(self, key: str):
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def _set_meta(self, key: str, value: str) -> None:
        self._set_meta_no_commit(key, value)
        self.conn.commit()

    def _set_meta_no_commit(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
