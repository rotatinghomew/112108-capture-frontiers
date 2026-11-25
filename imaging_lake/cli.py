"""Command line interface for the local imaging/trigger lakehouse."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time
from typing import Any

from .store import LakeStore


DEFAULT_DB = ".imaging-lake/lakehouse.sqlite"


def _print_json(value: Any, out=sys.stdout) -> None:
    json.dump(value, out, ensure_ascii=False, indent=2, sort_keys=False)
    out.write("\n")


def _store(args: argparse.Namespace) -> LakeStore:
    db = getattr(args, "db", None)
    shift = getattr(args, "shift", None)
    if not db:
        safe_shift = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in (shift or "default"))
        db = f".imaging-lake/{safe_shift}.sqlite"
    return LakeStore(db, shift=shift)


def cmd_ingest(args: argparse.Namespace) -> int:
    store = _store(args)
    try:
        inserted = store.ingest_jsonl(args.input)
        result = {"inserted_raw_records": inserted, "db": store.path, "status": store.status()}
        if args.json:
            _print_json(result)
        else:
            print(f"ingested {inserted} raw record(s) into {store.path}")
            print_status(store.status())
    finally:
        store.close()
    return 0


def cmd_watch(args: argparse.Namespace) -> int:
    """Poll one or more JSONL exchanges. Re-ingestion is idempotent."""
    store = _store(args)
    try:
        while True:
            for input_path in args.inputs:
                path = Path(input_path)
                if path.exists():
                    try:
                        store.ingest_jsonl(path)
                    except json.JSONDecodeError as exc:
                        print(f"skip incomplete/invalid JSONL {path}: {exc}", file=sys.stderr)
            if args.once:
                _print_json({"status": store.status()}) if args.json else print_status(store.status())
                return 0
            time.sleep(args.interval)
    finally:
        store.close()


def cmd_status(args: argparse.Namespace) -> int:
    store = _store(args)
    try:
        status = store.status()
        if args.json:
            _print_json(status)
        else:
            print_status(status)
    finally:
        store.close()
    return 0


def print_status(status: dict[str, Any]) -> None:
    print(f"班次: {status.get('shift') or '(未命名)'}；交付步: {status.get('max_step')}")
    print("\n当前能用的图:")
    if not status["usable_images"]:
        print("  - 无已唯一配对原图")
    for item in status["usable_images"]:
        frame = item.get("paired_frame") or {}
        print(
            f"  ✓ {item['target']}: {frame.get('id')} rev{frame.get('rev')} "
            f"统一时刻={frame.get('unified_time')} ({frame.get('conversion')})"
        )

    print("\n尚未确认的缺口:")
    if not status["unconfirmed_gaps"]:
        print("  - 无")
    for item in status["unconfirmed_gaps"]:
        suffix = f"；补拍单={item['retake_status']}" if item.get("retake_status") else ""
        print(f"  ? {item['target']}: {', '.join(item['reasons']) or 'pending'}{suffix}")

    print("\n已确认缺帧/待补拍:")
    if not status["confirmed_missing"]:
        print("  - 无")
    for item in status["confirmed_missing"]:
        cov = item["camera_coverage"]
        ids = ",".join(cov.get("supporting_commitment_ids", [])) or "无"
        retake = item.get("retake_status") or "未建单"
        print(f"  ! {item['target']}: 原图仍缺；补拍单={retake}；序号连续；相机覆盖承诺 {ids} 覆盖 {item['candidate_boundary']}")

    print("\n归属冲突:")
    if not status["conflicts"]:
        print("  - 无")
    for conflict in status["conflicts"]:
        print(f"  ⚠ {conflict['conflict_id']}: targets={conflict['targets']} frames={conflict['frames']}")

    print("\n已经交给补拍班/有回执的事项:")
    if not status["handed_to_retake_team"]:
        print("  - 无")
    for item in status["handed_to_retake_team"]:
        flags = []
        if item.get("original_recovered_after_acceptance"):
            flags.append("原帧后到但接手记录保留")
        if item.get("original_recovered_after_completion"):
            flags.append("原帧后到但完成记录保留")
        print(
            f"  • {item['target']}: {item['status']} receipts={item['receipt_ids']} "
            f"retake_frames={item['retake_frame_ids']} {'；'.join(flags)}"
        )

    print("\n可领取补拍意向:")
    claimable = status.get("claimable_retakes", [])
    if not claimable:
        print("  - 无")
    for item in claimable:
        print(f"  → {item['intention_id']} ({item['target']}) revision={item['revision']}")


def cmd_claimable(args: argparse.Namespace) -> int:
    store = _store(args)
    try:
        _print_json(store.claimable())
    finally:
        store.close()
    return 0


def cmd_intention(args: argparse.Namespace) -> int:
    store = _store(args)
    try:
        value = store.intention(args.target)
        if value is None:
            raise SystemExit(f"unknown target: {args.target}")
        _print_json(value)
    finally:
        store.close()
    return 0


def cmd_receipt(args: argparse.Namespace) -> int:
    store = _store(args)
    try:
        receipt = {"target": args.target, "receipt": args.receipt, "state": args.state}
        if args.extra_json:
            receipt.update(json.loads(args.extra_json))
        intention = store.register_receipt(receipt, step=args.step)
        _print_json(intention)
    finally:
        store.close()
    return 0


def cmd_claim(args: argparse.Namespace) -> int:
    store = _store(args)
    try:
        _print_json(store.claim(args.target, args.claimer, step=args.step))
    finally:
        store.close()
    return 0


def cmd_changes(args: argparse.Namespace) -> int:
    store = _store(args)
    try:
        _print_json(store.poll_changes(args.consumer, limit=args.limit))
    finally:
        store.close()
    return 0


def cmd_ack(args: argparse.Namespace) -> int:
    store = _store(args)
    try:
        seq = store.ack(args.consumer, args.seq)
        _print_json({"consumer": args.consumer, "last_seq": seq})
    finally:
        store.close()
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    store = _store(args)
    try:
        payload = store.snapshot() if args.full else store.status()
        Path(args.output).write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(args.output)
    finally:
        store.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="imaging-lake", description=__doc__)
    parser.add_argument("--db", default=None, help="SQLite lakehouse path; defaults to .imaging-lake/<shift|default>.sqlite")
    parser.add_argument("--shift", default=None, help="shift name; a new shift gets an independent SQLite file by default")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("ingest", help="ingest an exchange JSONL file")
    p.add_argument("input")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_ingest)

    p = sub.add_parser("watch", help="continuously poll and ingest JSONL exchange files")
    p.add_argument("inputs", nargs="+")
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--once", action="store_true", help="process current contents once and exit")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_watch)

    p = sub.add_parser("status", help="operator status")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("claimable", help="list claimable retake intentions as JSON")
    p.set_defaults(func=cmd_claimable)

    p = sub.add_parser("intention", help="show one retake intention/audit record")
    p.add_argument("target", help="e.g. g9/1/cam1")
    p.set_defaults(func=cmd_intention)

    p = sub.add_parser("receipt", help="register an accepted/completed field receipt")
    p.add_argument("target")
    p.add_argument("--receipt", required=True)
    p.add_argument("--state", choices=["accepted", "completed"], default="accepted")
    p.add_argument("--step", type=int)
    p.add_argument("--extra-json")
    p.set_defaults(func=cmd_receipt)

    p = sub.add_parser("claim", help="take a delivery lease; not a field acceptance receipt")
    p.add_argument("target")
    p.add_argument("--claimer", required=True)
    p.add_argument("--step", type=int)
    p.set_defaults(func=cmd_claim)

    p = sub.add_parser("changes", help="poll resumable intention changes")
    p.add_argument("--consumer", required=True)
    p.add_argument("--limit", type=int, default=100)
    p.set_defaults(func=cmd_changes)

    p = sub.add_parser("ack", help="acknowledge changes through seq")
    p.add_argument("--consumer", required=True)
    p.add_argument("--seq", type=int, required=True)
    p.set_defaults(func=cmd_ack)

    p = sub.add_parser("export", help="export status or full snapshot JSON")
    p.add_argument("output")
    p.add_argument("--full", action="store_true")
    p.set_defaults(func=cmd_export)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
