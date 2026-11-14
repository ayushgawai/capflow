"""Replication lag / error tracking, alert thresholds, backfill & replay."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .capture import CDCCapture
from .landing import Warehouse


@dataclass
class Alert:
    level: str  # warn | critical
    kind: str
    message: str
    value: float
    threshold: float


@dataclass
class ReplicationMonitor:
    max_lag_lsn_warn: int = 10_000
    max_lag_lsn_critical: int = 100_000
    max_lag_seconds_warn: float = 30.0
    max_lag_seconds_critical: float = 300.0
    max_errors_warn: int = 1
    max_errors_critical: int = 10
    source_lsn: int = 0
    applied_lsn: int = 0
    last_commit_ts: float | None = None
    last_apply_ts: float | None = None
    errors: list[str] = field(default_factory=list)
    batches: int = 0

    def record_source(self, max_lsn: int, commit_ts: float | None = None) -> None:
        self.source_lsn = max(self.source_lsn, max_lsn)
        if commit_ts is not None:
            self.last_commit_ts = commit_ts

    def record_batch(self, applied_lsn: int, n: int = 0) -> None:
        self.applied_lsn = max(self.applied_lsn, applied_lsn)
        self.last_apply_ts = time.time()
        self.batches += 1

    def record_error(self, message: str) -> None:
        self.errors.append(message)

    @property
    def lag_lsn(self) -> int:
        return max(0, self.source_lsn - self.applied_lsn)

    @property
    def lag_seconds(self) -> float:
        if self.last_commit_ts is None:
            return 0.0
        end = self.last_apply_ts or time.time()
        return max(0.0, end - self.last_commit_ts) if self.lag_lsn else 0.0

    def alerts(self) -> list[Alert]:
        out: list[Alert] = []
        for kind, value, warn, crit in [
            ("lag_lsn", self.lag_lsn, self.max_lag_lsn_warn, self.max_lag_lsn_critical),
            ("lag_seconds", self.lag_seconds, self.max_lag_seconds_warn, self.max_lag_seconds_critical),
            ("errors", len(self.errors), self.max_errors_warn, self.max_errors_critical),
        ]:
            if value >= crit:
                out.append(Alert("critical", kind, f"{kind}={value} >= critical {crit}", float(value), float(crit)))
            elif value >= warn:
                out.append(Alert("warn", kind, f"{kind}={value} >= warn {warn}", float(value), float(warn)))
        return out

    def health(self) -> str:
        levels = {a.level for a in self.alerts()}
        return "critical" if "critical" in levels else "warn" if "warn" in levels else "ok"

    def status(self) -> dict[str, Any]:
        return {
            "health": self.health(), "lag_lsn": self.lag_lsn, "lag_seconds": round(self.lag_seconds, 3),
            "source_lsn": self.source_lsn, "applied_lsn": self.applied_lsn,
            "errors": len(self.errors), "batches": self.batches,
            "alerts": [a.__dict__ for a in self.alerts()],
        }


def save_checkpoint(path: str | Path, lsn: int) -> None:
    Path(path).write_text(json.dumps({"lsn": lsn}), encoding="utf-8")


def load_checkpoint(path: str | Path) -> int:
    p = Path(path)
    return int(json.loads(p.read_text(encoding="utf-8"))["lsn"]) if p.exists() else 0


def backfill(capture: CDCCapture, warehouse: Warehouse, from_lsn: int,
             batch_size: int = 5000, monitor: ReplicationMonitor | None = None) -> dict[str, int]:
    """Replay change log from a saved LSN. Idempotent: already-applied LSNs skip."""
    total = {"applied": 0, "skipped_duplicate": 0, "skipped_stale": 0, "deletes": 0, "batches": 0}
    offset_lsn = from_lsn
    while True:
        recs = capture.peek(offset_lsn, limit=batch_size)
        if not recs:
            break
        try:
            stats = warehouse.apply_batch(recs)
        except Exception as exc:  # recorded, then surfaced — caller decides retry
            if monitor:
                monitor.record_error(str(exc))
            raise
        for k in ("applied", "skipped_duplicate", "skipped_stale", "deletes"):
            total[k] += stats[k]
        total["batches"] += 1
        offset_lsn = recs[-1].lsn
        if monitor:
            monitor.record_batch(offset_lsn, len(recs))
        if len(recs) < batch_size:
            break
    return total
