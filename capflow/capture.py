"""CDC capture layer modeled on SQL Server Change Data Capture.

A source table's changes are appended to an in-memory change log with a
monotonically increasing LSN (log sequence number), mirroring how SQL
Server CDC exposes ``cdc.fn_cdc_get_all_changes_*`` rows ordered by
``__$start_lsn`` / ``__$seqval``. A capture poller reads from a committed
commit-LSN cursor, so re-polling never re-emits a record past the cursor
and never misses one after it.
"""
from __future__ import annotations

import bisect
import json
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Iterable, Optional


@dataclass(frozen=True)
class ChangeRecord:
    lsn: int
    seqval: int
    table: str
    op: str  # insert | update | delete
    pk: str
    before: Optional[dict[str, Any]]
    after: Optional[dict[str, Any]]
    commit_ts: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class SourceDB:
    """A toy source-of-record plus its CDC change log (SQL Server CDC analogue)."""

    def __init__(self, table: str = "dbo.orders"):
        self.table = table
        self.rows: dict[str, dict[str, Any]] = {}
        self.log: list[ChangeRecord] = []
        self._lsn = 0

    @property
    def max_lsn(self) -> int:
        return self._lsn

    def _append(self, op: str, pk: str, before, after) -> ChangeRecord:
        self._lsn += 1
        rec = ChangeRecord(
            lsn=self._lsn, seqval=self._lsn, table=self.table, op=op, pk=str(pk),
            before=dict(before) if before is not None else None,
            after=dict(after) if after is not None else None,
            commit_ts=time.time(),
        )
        self.log.append(rec)
        return rec

    def insert(self, pk: Any, row: dict[str, Any]) -> ChangeRecord:
        pk = str(pk)
        if pk in self.rows:
            raise KeyError(f"duplicate pk {pk}")
        self.rows[pk] = dict(row)
        return self._append("insert", pk, None, row)

    def update(self, pk: Any, changes: dict[str, Any]) -> ChangeRecord:
        pk = str(pk)
        if pk not in self.rows:
            raise KeyError(f"missing pk {pk}")
        before = dict(self.rows[pk])
        self.rows[pk].update(changes)
        return self._append("update", pk, before, self.rows[pk])

    def delete(self, pk: Any) -> ChangeRecord:
        pk = str(pk)
        if pk not in self.rows:
            raise KeyError(f"missing pk {pk}")
        before = self.rows.pop(pk)
        return self._append("delete", pk, before, None)

    def snapshot(self) -> dict[str, dict[str, Any]]:
        return {k: dict(v) for k, v in self.rows.items()}


class CDCCapture:
    """Polls a SourceDB change log with a commit-LSN cursor."""

    def __init__(self, source: SourceDB, cursor_path: str | Path | None = None, start_lsn: int = 0):
        self.source = source
        self.cursor_path = Path(cursor_path) if cursor_path else None
        self.cursor = self.load_cursor() if self.cursor_path and self.cursor_path.exists() else start_lsn

    def _from(self, since_lsn: int, limit: int | None = None) -> list[ChangeRecord]:
        # The log is append-only and LSN-ordered, so bisect to the cursor
        # instead of scanning the whole log on every poll.
        log = self.source.log
        start = bisect.bisect_right(log, since_lsn, key=lambda r: r.lsn)
        recs = log[start:] if limit is None else log[start:start + limit]
        return list(recs)

    def poll(self, limit: int | None = None) -> list[ChangeRecord]:
        recs = self._from(self.cursor, limit)
        if recs:
            self.cursor = recs[-1].lsn
            self.save_cursor()
        return recs

    def peek(self, since_lsn: int, limit: int | None = None) -> list[ChangeRecord]:
        """Read from an explicit LSN without moving this poller's cursor (replay)."""
        return self._from(since_lsn, limit)

    def save_cursor(self) -> None:
        if self.cursor_path:
            self.cursor_path.write_text(json.dumps({"commit_lsn": self.cursor}), encoding="utf-8")

    def load_cursor(self) -> int:
        if self.cursor_path and self.cursor_path.exists():
            return int(json.loads(self.cursor_path.read_text(encoding="utf-8"))["commit_lsn"])
        return 0
