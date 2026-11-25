"""Durable local SQLite lakehouse and resumable change delivery."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any, Callable, Iterable

from .engine import LakeEngine, canonical_hash


SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_records (
    digest TEXT PRIMARY KEY,
    source TEXT,
    line_no INTEGER,
    step INTEGER,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL,
    ingested_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS source_files (
    path TEXT PRIMARY KEY,
    inode TEXT,
    byte_offset INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS consumer_offsets (
    consumer TEXT PRIMARY KEY,
    last_seq INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS processed_events (
    consumer TEXT NOT NULL,
    event_id TEXT NOT NULL,
    processed_at TEXT NOT NULL,
    PRIMARY KEY (consumer, event_id)
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox_events (
    seq INTEGER PRIMARY KEY,
    event_id TEXT UNIQUE NOT NULL,
    target TEXT,
    kind TEXT,
    event_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS trigger_facts (
    target TEXT PRIMARY KEY,
    boot TEXT,
    seq INTEGER,
    channel TEXT,
    state TEXT NOT NULL,
    fact_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS retake_intentions (
    target TEXT PRIMARY KEY,
    intention_id TEXT UNIQUE NOT NULL,
    status TEXT NOT NULL,
    claimable INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    intention_json TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS one_issued_intention_per_target
    ON outbox_events(target) WHERE kind = 'intention.issued';
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class LakeStore:
    def __init__(self, path: str | Path = ":memory:", shift: str | None = None):
        raw_path = str(path)
        if raw_path == ":memory:":
            self.path = ":memory:"
        else:
            p = Path(raw_path)
            if p.is_dir():
                p = p / "lakehouse.sqlite"
            self.path = str(p)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        self.shift = shift
        self.engine = self._load_engine()
        # Make the relational outbox complete even for databases created before
        # it existed or for state loaded from the JSON meta projection.
        self._save_engine()
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "LakeStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _load_engine(self) -> LakeEngine:
        row = self.conn.execute("SELECT value FROM meta WHERE key='engine_state'").fetchone()
        if row:
            return LakeEngine.from_state(json.loads(row["value"]))
        return LakeEngine(shift=self.shift)

    def _save_engine(self) -> None:
        state = json.dumps(self.engine.to_state(), ensure_ascii=False, sort_keys=True)
        self.conn.execute(
            "INSERT INTO meta(key,value) VALUES('engine_state',?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (state,),
        )
        for event in self.engine.events:
            self.conn.execute(
                "INSERT OR IGNORE INTO outbox_events(seq,event_id,target,kind,event_json) "
                "VALUES(?,?,?,?,?)",
                (
                    int(event["seq"]),
                    event["event_id"],
                    event.get("target"),
                    event.get("kind"),
                    json.dumps(event, ensure_ascii=False, sort_keys=True),
                ),
            )

        self.conn.execute("DELETE FROM trigger_facts")
        for target, fact in self.engine.facts.items():
            self.conn.execute(
                "INSERT INTO trigger_facts(target,boot,seq,channel,state,fact_json) VALUES(?,?,?,?,?,?)",
                (
                    target,
                    fact.get("boot"),
                    fact.get("seq"),
                    fact.get("channel"),
                    fact.get("state"),
                    json.dumps(fact, ensure_ascii=False, sort_keys=True),
                ),
            )

        self.conn.execute("DELETE FROM retake_intentions")
        for target, intention in self.engine.intentions.items():
            self.conn.execute(
                "INSERT INTO retake_intentions(target,intention_id,status,claimable,revision,intention_json) "
                "VALUES(?,?,?,?,?,?)",
                (
                    target,
                    intention.get("intention_id"),
                    intention.get("status"),
                    1 if intention.get("claimable") else 0,
                    int(intention.get("revision", 1)),
                    json.dumps(intention, ensure_ascii=False, sort_keys=True),
                ),
            )

    @contextmanager
    def transaction(self):
        try:
            yield self.conn
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            # Reload committed state so a failed tentative engine mutation does
            # not leak into the next operation.
            self.engine = self._load_engine()
            raise

    def _insert_raw(self, row: dict[str, Any], source: str | None = None, line_no: int | None = None) -> bool:
        step = int(row.get("step") or row.get("body", {}).get("step") or 0)
        digest = canonical_hash({"step": step, "kind": row["kind"], "body": row["body"]})
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO raw_records(digest,source,line_no,step,kind,payload,ingested_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (digest, source, line_no, step, row["kind"], json.dumps(row, ensure_ascii=False), _now()),
        )
        return cur.rowcount == 1

    def ingest_record(self, row: dict[str, Any], source: str | None = None, line_no: int | None = None) -> dict[str, Any]:
        with self.transaction():
            inserted = self._insert_raw(row, source, line_no)
            result = self.engine.ingest_record(row)
            self._save_engine()
        result["raw_inserted"] = inserted
        return result

    def ingest(self, row: dict[str, Any], source: str | None = None, line_no: int | None = None) -> dict[str, Any]:
        """Alias for :meth:`ingest_record`."""
        return self.ingest_record(row, source=source, line_no=line_no)

    def ingest_rows(self, rows: Iterable[dict[str, Any]], source: str | None = None) -> int:
        rows = list(rows)
        with self.transaction():
            inserted = 0
            for idx, row in enumerate(rows, 1):
                if self._insert_raw(row, source, idx):
                    inserted += 1
            self.engine.ingest_rows(rows)
            self._save_engine()
        return inserted

    def ingest_jsonl(self, path: str | Path) -> int:
        path = Path(path)
        rows: list[dict[str, Any]] = []
        for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            rows.append(row)
        source = str(path.resolve())
        with self.transaction():
            inserted = 0
            for line_no, row in enumerate(rows, 1):
                if self._insert_raw(row, source, line_no):
                    inserted += 1
            self.engine.ingest_rows(rows)
            stat = path.stat()
            self.conn.execute(
                "INSERT INTO source_files(path,inode,byte_offset,updated_at) VALUES(?,?,?,?) "
                "ON CONFLICT(path) DO UPDATE SET inode=excluded.inode, byte_offset=excluded.byte_offset, updated_at=excluded.updated_at",
                (source, f"{stat.st_dev}:{stat.st_ino}", stat.st_size, _now()),
            )
            self._save_engine()
        return inserted

    # ------------------------------------------------------------- field API
    def register_receipt(self, receipt: dict[str, Any], step: int | None = None) -> dict[str, Any]:
        self.refresh()
        row = {"step": int(step or self.engine.max_step or 0), "kind": "field", "body": receipt}
        with self.transaction():
            self._insert_raw(row, source="field-api")
            intention = self.engine.register_receipt(receipt, step=row["step"])
            self._save_engine()
        return intention

    def claim(self, target: str, claimer: str, step: int | None = None) -> dict[str, Any]:
        self.refresh()
        with self.transaction():
            intention = self.engine.claim_intention(target, claimer, step=step)
            self._save_engine()
        return intention

    # -------------------------------------------------------------- queries
    def status(self) -> dict[str, Any]:
        self.refresh()
        return self.engine.operator_view()

    def snapshot(self) -> dict[str, Any]:
        self.refresh()
        return self.engine.snapshot()

    def claimable(self) -> list[dict[str, Any]]:
        self.refresh()
        return self.engine.claimable_intentions()

    def intention(self, target: str) -> dict[str, Any] | None:
        self.refresh()
        value = self.engine.intentions.get(target)
        return json.loads(json.dumps(value, ensure_ascii=False, default=str)) if value else None

    def receipt(self, receipt_id: str) -> dict[str, Any] | None:
        self.refresh()
        return self.engine.receipts.get(receipt_id)

    # ------------------------------------------------------- change delivery
    def refresh(self) -> LakeEngine:
        """Reload committed engine state (useful for a long-lived reader)."""
        self.engine = self._load_engine()
        return self.engine

    def current_offset(self, consumer: str) -> int:
        row = self.conn.execute("SELECT last_seq FROM consumer_offsets WHERE consumer=?", (consumer,)).fetchone()
        return int(row["last_seq"]) if row else 0

    def poll_changes(self, consumer: str, limit: int = 100) -> list[dict[str, Any]]:
        last = self.current_offset(consumer)
        rows = self.conn.execute(
            "SELECT event_json FROM outbox_events WHERE seq > ? ORDER BY seq LIMIT ?",
            (last, int(limit)),
        ).fetchall()
        return [json.loads(row["event_json"]) for row in rows]

    def ack(self, consumer: str, seq: int) -> int:
        with self.transaction():
            current = self.current_offset(consumer)
            seq = max(current, int(seq))
            self.conn.execute(
                "INSERT INTO consumer_offsets(consumer,last_seq,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(consumer) DO UPDATE SET last_seq=MAX(consumer_offsets.last_seq, excluded.last_seq), updated_at=excluded.updated_at",
                (consumer, seq, _now()),
            )
        return seq

    def mark_processed(self, consumer: str, event_id: str) -> bool:
        with self.transaction():
            cur = self.conn.execute(
                "INSERT OR IGNORE INTO processed_events(consumer,event_id,processed_at) VALUES(?,?,?)",
                (consumer, event_id, _now()),
            )
            return cur.rowcount == 1

    def is_processed(self, consumer: str, event_id: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM processed_events WHERE consumer=? AND event_id=?", (consumer, event_id)
        ).fetchone()
        return row is not None

    def deliver(
        self,
        consumer: str,
        handler: Callable[[dict[str, Any]], None],
        limit: int = 100,
        acknowledge: bool = True,
    ) -> list[dict[str, Any]]:
        """Deliver events after the consumer's durable cursor.

        Events are at-least-once if the process exits after handler work but
        before acknowledgement.  Stable ``event_id`` and ``intention_id`` let
        consumers make handling idempotent.
        """
        delivered: list[dict[str, Any]] = []
        for event in self.poll_changes(consumer, limit=limit):
            if not self.is_processed(consumer, event["event_id"]):
                handler(event)
                self.mark_processed(consumer, event["event_id"])
            if acknowledge:
                self.ack(consumer, event["seq"])
            delivered.append(event)
        return delivered
