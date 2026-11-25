#!/usr/bin/env python3
"""Convenience entry point and import surface.

CLI:
    python pipeline.py ingest data/exchanges.jsonl
    python pipeline.py status --json

Python:
    from pipeline import LakeEngine, LakeStore, replay, process_records
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from imaging_lake.cli import main
from imaging_lake.engine import LakeEngine, replay
from imaging_lake.store import LakeStore

__all__ = [
    "LakeEngine",
    "LakeStore",
    "Pipeline",
    "Lakehouse",
    "replay",
    "process_records",
    "build_status",
    "process_file",
    "main",
]

Pipeline = LakeEngine
Lakehouse = LakeStore


def load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    p = Path(path)
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


def process_records(rows: Iterable[dict[str, Any]], shift: str | None = None) -> dict[str, Any]:
    """Replay records and return the operator status dictionary."""
    return replay(rows, shift=shift).operator_view()


def build_status(rows: Iterable[dict[str, Any]], shift: str | None = None) -> dict[str, Any]:
    """Alias for :func:`process_records`."""
    return process_records(rows, shift=shift)


def process_file(
    path: str | Path,
    db_path: str | Path = ":memory:",
    shift: str | None = None,
) -> dict[str, Any]:
    """Ingest one JSONL exchange into a store and return operator status."""
    with LakeStore(db_path, shift=shift) as store:
        store.ingest_jsonl(path)
        return store.status()


if __name__ == "__main__":
    raise SystemExit(main())
