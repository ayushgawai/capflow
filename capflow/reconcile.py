"""Row-level reconciliation against the source of record.

Gates a replication batch: a batch is only marked complete when source and
target agree on count and checksum (or any mismatch is reported).
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from .landing import Warehouse


def checksum(rows: dict[str, dict[str, Any]]) -> str:
    h = hashlib.sha256()
    for pk in sorted(rows):
        h.update(pk.encode())
        h.update(b"\x00")
        h.update(json.dumps(rows[pk], sort_keys=True, separators=(",", ":")).encode())
        h.update(b"\n")
    return h.hexdigest()


@dataclass
class ReconcileReport:
    table: str
    source_count: int
    target_count: int
    source_checksum: str
    target_checksum: str
    missing_in_target: list[str] = field(default_factory=list)
    extra_in_target: list[str] = field(default_factory=list)
    differing: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return (
            self.source_count == self.target_count
            and self.source_checksum == self.target_checksum
            and not self.missing_in_target
            and not self.extra_in_target
            and not self.differing
        )

    def to_dict(self) -> dict[str, Any]:
        return {**self.__dict__, "passed": self.passed}


def reconcile(source_rows: dict[str, dict[str, Any]], warehouse: Warehouse,
              table: str, sample: int = 20) -> ReconcileReport:
    target = warehouse.rows(table)
    missing = sorted(set(source_rows) - set(target))[:sample]
    extra = sorted(set(target) - set(source_rows))[:sample]
    differing = sorted(
        pk for pk in set(source_rows) & set(target) if source_rows[pk] != target[pk]
    )[:sample]
    return ReconcileReport(
        table=table,
        source_count=len(source_rows), target_count=len(target),
        source_checksum=checksum(source_rows), target_checksum=checksum(target),
        missing_in_target=missing, extra_in_target=extra, differing=differing,
    )


def gate(report: ReconcileReport) -> bool:
    """True only if the batch may be marked complete."""
    return report.passed
