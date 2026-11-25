"""Order-independent reconciliation engine.

The engine is deliberately storage-free: feed it exchange records in delivery
order (or call :func:`replay`) and it returns JSON-serialisable facts,
one logical retake intention per ``boot/seq/channel``, and an append-only
change log.

Device arrival time is never used for pairing.  Pairing uses the trigger's
unified clock time/tolerance, the camera tick converted through its highest
known clock revision, pulse mark, channel and the global candidate graph.
"""

from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
import hashlib
import json
from typing import Any, Iterable

# Fact states.
PAIRED = "paired"
AMBIGUOUS = "ambiguous"
MISSING_CONFIRMED = "missing_confirmed"
PENDING = "pending"

# Intention handling states are intentionally conservative.  A receipt is an
# immutable fact; the combined status below is convenient for queries.
OFFERED = "offered"
CLAIMED = "claimed"
HELD = "held_pending_confirmation"
WITHDRAWN = "withdrawn"
ACCEPTED = "accepted"
COMPLETED = "completed"

KNOWN_KINDS = {
    "trigger",
    "clock",
    "frame",
    "close",
    "coverage",
    "field",
}

ACCEPTED_STATES = {"accepted", "accept", "claimed", "received", "taken", "handed", "acknowledged"}
COMPLETED_STATES = {"completed", "complete", "done", "finished"}


def canonical_hash(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


def short_id(prefix: str, value: str) -> str:
    return f"{prefix}-{hashlib.sha1(value.encode()).hexdigest()[:12]}"


def target_key(boot: str, seq: int, channel: str) -> str:
    return f"{boot}/{seq}/{channel}"


def parse_target(target: str) -> tuple[str, int, str] | None:
    if not isinstance(target, str):
        return None
    parts = target.split("/")
    if len(parts) != 3:
        return None
    boot, seq_text, channel = parts
    try:
        return boot, int(seq_text), channel
    except (TypeError, ValueError):
        return None


def _sorted_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


class ReceiptConflict(ValueError):
    """A receipt id was reused with different payload."""


class LakeEngine:
    def __init__(self, shift: str | None = None):
        self.shift = shift
        self.reset()

    def reset(self) -> None:
        self.max_step = 0
        self.processed_hashes: set[str] = set()
        self.triggers: dict[tuple[str, int], dict[str, Any]] = {}
        self.trigger_observations: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
        self.trigger_conflicts: dict[str, list[dict[str, Any]]] = {}
        self.clocks: dict[tuple[str, str, int], dict[str, Any]] = {}
        self.frames: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
        self.frame_uploads: dict[str, set[str]] = defaultdict(set)
        self.close_records: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.coverages: list[dict[str, Any]] = []
        self._coverage_hashes: set[str] = set()
        self.receipts: dict[str, dict[str, Any]] = {}
        self.intentions: dict[str, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.facts: dict[str, dict[str, Any]] = {}
        self.conflicts: list[dict[str, Any]] = []
        self.validation_issues: list[dict[str, Any]] = []

    # ------------------------------------------------------------------ ingest
    def ingest_record(self, row: dict[str, Any], reconcile: bool = True) -> dict[str, Any]:
        """Ingest one ``{"step", "kind", "body"}`` record.

        Exact duplicate deliveries are ignored.  Domain upserts such as a
        higher frame revision are handled by :meth:`_apply`.
        """
        if not isinstance(row, dict) or "kind" not in row or "body" not in row:
            raise ValueError("exchange record must contain kind and body")
        step = int(row.get("step") or row["body"].get("step") or 0)
        normalized = {"step": step, "kind": row["kind"], "body": row["body"]}
        digest = canonical_hash(normalized)
        if digest in self.processed_hashes:
            return {"duplicate": True, "hash": digest}
        self._apply(row["kind"], row["body"], step, digest)
        self.processed_hashes.add(digest)
        self.max_step = max(self.max_step, step)
        if reconcile:
            self.reconcile(step)
        return {"duplicate": False, "hash": digest}

    def ingest_rows(self, rows: Iterable[dict[str, Any]]) -> "LakeEngine":
        """Ingest records, reconciling once per delivery ``step`` batch.

        Records sharing a step are one delivery unit.  This prevents an
        intermediate line order from creating visible decisions or change-log
        events that the complete batch does not imply.
        """
        groups: dict[int, list[dict[str, Any]]] = {}
        order: list[int] = []
        for row in rows:
            step = int(row.get("step") or row.get("body", {}).get("step") or 0)
            if step not in groups:
                groups[step] = []
                order.append(step)
            groups[step].append(row)
        for step in order:
            changed = False
            for row in groups[step]:
                normalized = {"step": step, "kind": row["kind"], "body": row["body"]}
                digest = canonical_hash(normalized)
                if digest in self.processed_hashes:
                    continue
                self._apply(row["kind"], row["body"], step, digest)
                self.processed_hashes.add(digest)
                self.max_step = max(self.max_step, step)
                changed = True
            # Reconcile even if all rows were exact duplicates: harmless and
            # makes replay callers robust.
            self.reconcile(step)
        return self

    def ingest(
        self,
        row: dict[str, Any] | str | None = None,
        body: dict[str, Any] | None = None,
        step: int | None = None,
        kind: str | None = None,
    ) -> dict[str, Any]:
        """Convenience alias accepting either a row or ``kind, body``."""
        if isinstance(row, str):
            kind = row
            row = None
        if row is None:
            if kind is None or body is None:
                raise ValueError("provide a record row or kind plus body")
            row = {"step": step or self.max_step or 0, "kind": kind, "body": body}
        return self.ingest_record(row)

    def _issue(self, code: str, message: str, **context: Any) -> None:
        issue = {"code": code, "message": message, **context}
        key = canonical_hash(issue)
        if not any(canonical_hash(x) == key for x in self.validation_issues):
            self.validation_issues.append(issue)

    def _apply(self, kind: str, body: dict[str, Any], step: int, digest: str) -> None:
        if not isinstance(body, dict) or not body:
            self._issue("empty_body", "ignored empty record", kind=kind, step=step)
            return
        if kind == "trigger":
            key = (str(body["boot"]), int(body["seq"]))
            observations = self.trigger_observations[key]
            if not any(canonical_hash(o["body"]) == canonical_hash(body) for o in observations):
                observations.append({"body": deepcopy(body), "step": step})
            variants = {canonical_hash(o["body"]) for o in observations}
            if len(variants) == 1:
                self.triggers[key] = deepcopy(body)
            else:
                self.trigger_conflicts[target_key(key[0], key[1], "*")] = [
                    deepcopy(o["body"]) for o in observations
                ]
                self._issue(
                    "trigger_identity_conflict",
                    "same boot/seq has conflicting trigger payloads",
                    boot=key[0],
                    seq=key[1],
                    step=step,
                )
        elif kind == "clock":
            key = (str(body["camera"]), str(body["boot"]), int(body["rev"]))
            old = self.clocks.get(key)
            if old is not None and canonical_hash(old) != canonical_hash(body):
                self._issue("clock_revision_conflict", "same clock rev has different payload", key=key)
            else:
                self.clocks[key] = deepcopy(body)
        elif kind == "frame":
            fid = str(body["id"])
            rev = int(body.get("rev", 1))
            upload = body.get("upload")
            if upload is not None:
                self.frame_uploads[fid].add(str(upload))
            old = self.frames[fid].get(rev)
            comparable = {k: v for k, v in body.items() if k != "upload"}
            if old is not None:
                ignored = {"upload", "upload_numbers"}
                old_comparable = {k: v for k, v in old.items() if k not in ignored}
                new_comparable = {k: v for k, v in comparable.items() if k not in ignored}
                if canonical_hash(old_comparable) != canonical_hash(new_comparable):
                    self._issue(
                        "frame_revision_conflict",
                        "same frame id/rev has conflicting payload",
                        frame_id=fid,
                        rev=rev,
                    )
                    return
                # Preserve all upload aliases without creating another frame.
                old.setdefault("upload_numbers", [])
                if upload is not None and str(upload) not in old["upload_numbers"]:
                    old["upload_numbers"].append(str(upload))
            else:
                stored = deepcopy(body)
                stored["upload_numbers"] = sorted(self.frame_uploads[fid])
                self.frames[fid][rev] = stored
        elif kind == "close":
            boot = str(body["boot"])
            if not any(canonical_hash(r["body"]) == canonical_hash(body) for r in self.close_records[boot]):
                self.close_records[boot].append({"body": deepcopy(body), "step": step})
        elif kind == "coverage":
            cid = short_id("COV", canonical_hash(body))
            if cid not in self._coverage_hashes:
                self._coverage_hashes.add(cid)
                record = {"coverage_id": cid, "step": step, "body": deepcopy(body)}
                self.coverages.append(record)
        elif kind == "field":
            rid = str(body["receipt"])
            if rid in self.receipts:
                old = self.receipts[rid]
                old_body = {k: v for k, v in old.items() if not k.startswith("_")}
                if canonical_hash(old_body) != canonical_hash(body):
                    raise ReceiptConflict(f"receipt {rid} reused with different body")
                return
            record = deepcopy(body)
            record["_step"] = step
            record["_digest"] = digest
            self.receipts[rid] = record
            target = str(body.get("target", ""))
            if parse_target(target) is None:
                self._issue("bad_receipt_target", "field receipt target is not boot/seq/channel", receipt=rid)
            else:
                self._ensure_intention(target, step, origin="field_receipt")
        else:
            self._issue("unknown_kind", "unknown exchange record kind", kind=kind, step=step)

    # ------------------------------------------------------------ clock/frames
    def _current_clock_revs(self) -> dict[tuple[str, str], int]:
        current: dict[tuple[str, str], int] = {}
        for camera, boot, rev in self.clocks:
            key = (camera, boot)
            current[key] = max(rev, current.get(key, rev))
        return current

    def frame_views(self) -> list[dict[str, Any]]:
        current_revs = self._current_clock_revs()
        views: list[dict[str, Any]] = []
        for fid in sorted(self.frames):
            rev = max(self.frames[fid])
            body = self.frames[fid][rev]
            camera = str(body["camera"])
            camera_boot = str(body["boot"])
            clock_rev = current_revs.get((camera, camera_boot))
            clock = self.clocks.get((camera, camera_boot, clock_rev)) if clock_rev is not None else None
            tick = body.get("tick")
            reliable = False
            unified_time = None
            clock_summary = None
            reasons: list[str] = []
            if clock is None:
                reasons.append("no_approved_clock")
            else:
                clock_summary = {
                    "camera": camera,
                    "camera_boot": camera_boot,
                    "rev": clock_rev,
                    "offset": int(clock["offset"]),
                    "first_tick": int(clock["first_tick"]),
                    "last_tick": int(clock["last_tick"]),
                    "reliable_unified_interval": [
                        int(clock["first_tick"]) + int(clock["offset"]),
                        int(clock["last_tick"]) + int(clock["offset"]),
                    ],
                }
                if not (int(clock["first_tick"]) <= int(tick) <= int(clock["last_tick"])):
                    reasons.append("tick_outside_reliable_range")
                else:
                    reliable = True
                    unified_time = int(tick) + int(clock["offset"])
            if body.get("retake_of"):
                reasons.append("retake_frame_excluded_from_original_candidates")
            view = {
                "id": fid,
                "rev": rev,
                "camera": camera,
                "camera_boot": camera_boot,
                "tick": tick,
                "pulse": body.get("pulse"),
                "upload_numbers": body.get("upload_numbers", sorted(self.frame_uploads.get(fid, set()))),
                "retake_of": body.get("retake_of"),
                "clock_rev": clock_rev,
                "clock": clock_summary,
                "reliable_time": reliable,
                "unified_time": unified_time,
                "conversion": (
                    f"{tick}+{clock['offset']}={unified_time}" if clock is not None and reliable else None
                ),
                "reasons": reasons,
            }
            views.append(view)
        return views

    def _candidate_edges(self, frame_views: list[dict[str, Any]]) -> list[dict[str, Any]]:
        edges: list[dict[str, Any]] = []
        originals = [
            f
            for f in frame_views
            if not f.get("retake_of") and f["reliable_time"] and f["unified_time"] is not None
        ]
        for (boot, seq), trigger in sorted(self.triggers.items()):
            for channel in trigger.get("channels", []):
                target = target_key(boot, seq, str(channel))
                tol = int(trigger["tol"])
                trigger_time = int(trigger["time"])
                for frame in originals:
                    if frame["camera"] != channel:
                        continue
                    if frame.get("pulse") != trigger.get("pulse"):
                        continue
                    delta = abs(int(frame["unified_time"]) - trigger_time)
                    if delta > tol:
                        continue
                    edges.append(
                        {
                            "target": target,
                            "frame": frame["id"],
                            "frame_rev": frame["rev"],
                            "camera": channel,
                            "unified_time": frame["unified_time"],
                            "trigger_time": trigger_time,
                            "delta": delta,
                            "tolerance": tol,
                            "edge_key": f"{target}\u0000{frame['id']}",
                        }
                    )
        return edges

    # ---------------------------------------------------------- graph matching
    @staticmethod
    def _matching_size(
        targets: list[str],
        adjacency: dict[str, list[str]],
        allowed: set[tuple[str, str]] | None = None,
    ) -> int:
        match_frame_to_target: dict[str, str] = {}

        def augment(target: str, seen: set[str]) -> bool:
            for frame in adjacency.get(target, []):
                if allowed is not None and (target, frame) not in allowed:
                    continue
                if frame in seen:
                    continue
                seen.add(frame)
                incumbent = match_frame_to_target.get(frame)
                if incumbent is None or augment(incumbent, seen):
                    match_frame_to_target[frame] = target
                    return True
            return False

        for target in targets:
            augment(target, set())
        return len(match_frame_to_target)

    def _resolve_graph(self, edges: list[dict[str, Any]]) -> tuple[dict[str, bool], list[dict[str, Any]]]:
        """Return edge_key -> forced and conflict component descriptions."""
        adjacency: dict[str, list[str]] = defaultdict(list)
        targets_set: set[str] = set()
        frames_set: set[str] = set()
        edge_pairs: set[tuple[str, str]] = set()
        edge_by_key = {e["edge_key"]: e for e in edges}
        for edge in edges:
            t, f = edge["target"], edge["frame"]
            adjacency[t].append(f)
            targets_set.add(t)
            frames_set.add(f)
            edge_pairs.add((t, f))
        targets = sorted(targets_set)
        for t in adjacency:
            adjacency[t] = sorted(set(adjacency[t]))

        k = self._matching_size(targets, adjacency)
        forced: dict[str, bool] = {}
        for edge in edges:
            pair = (edge["target"], edge["frame"])
            allowed = edge_pairs - {pair}
            without = self._matching_size(targets, adjacency, allowed)
            forced[edge["edge_key"]] = without < k

        # Connected components over target/frame endpoint nodes.
        parent: dict[str, str] = {}

        def find(x: str) -> str:
            parent.setdefault(x, x)
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a: str, b: str) -> None:
            parent.setdefault(a, a)
            parent.setdefault(b, b)
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[rb] = ra

        for edge in edges:
            union("T:" + edge["target"], "F:" + edge["frame"])

        components: dict[str, dict[str, Any]] = defaultdict(lambda: {"targets": set(), "frames": set(), "edges": []})
        for edge in edges:
            root = find("T:" + edge["target"])
            comp = components[root]
            comp["targets"].add(edge["target"])
            comp["frames"].add(edge["frame"])
            comp["edges"].append(edge)

        conflicts: list[dict[str, Any]] = []
        for comp in components.values():
            comp_edges = comp["edges"]
            has_unresolved_target = any(
                not forced.get(e["edge_key"], False)
                and not any(forced.get(x["edge_key"], False) for x in comp_edges if x["target"] == e["target"])
                for e in comp_edges
            )
            # A component with a unique maximum pairing has no unresolved relation.
            all_forced_unique = sum(1 for e in comp_edges if forced.get(e["edge_key"])) == len(
                {e["target"] for e in comp_edges}
            ) and all(forced.get(e["edge_key"]) or True for e in comp_edges)
            unresolved_edges = [e for e in comp_edges if not forced.get(e["edge_key"])]
            if not unresolved_edges:
                continue
            # If every target already has a forced edge, remaining edges are
            # dominated alternatives rather than an undecidable ownership hole.
            targets_with_forced = {
                e["target"] for e in comp_edges if forced.get(e["edge_key"])
            }
            unresolved_targets = sorted({e["target"] for e in unresolved_edges} - targets_with_forced)
            if not unresolved_targets and all_forced_unique:
                continue
            ordered_edges = sorted(comp_edges, key=lambda e: (e["target"], e["frame"]))
            seed = "|".join(
                sorted(comp["targets"]) + sorted(comp["frames"]) + [e["edge_key"] for e in ordered_edges]
            )
            reasons = []
            if any(len(adjacency.get(t, [])) > 1 for t in comp["targets"]):
                reasons.append("multiple_valid_frames_for_channel")
            frame_degree: dict[str, int] = defaultdict(int)
            for e in comp_edges:
                frame_degree[e["frame"]] += 1
            if any(d > 1 for d in frame_degree.values()):
                reasons.append("frame_matches_multiple_triggers")
            if unresolved_targets:
                reasons.append("no_order_independent_unique_attribution")
            conflicts.append(
                {
                    "conflict_id": short_id("CFL", seed),
                    "targets": sorted(comp["targets"]),
                    "frames": sorted(comp["frames"]),
                    "unresolved_targets": unresolved_targets,
                    "reasons": reasons or ["nonunique_candidate_relation"],
                    "candidate_edges": [
                        {
                            "target": e["target"],
                            "frame": e["frame"],
                            "frame_rev": e["frame_rev"],
                            "unified_time": e["unified_time"],
                            "delta": e["delta"],
                            "tolerance": e["tolerance"],
                            "forced": forced.get(e["edge_key"], False),
                        }
                        for e in ordered_edges
                    ],
                    "rule": (
                        "Resolve the whole connected candidate set. Forced edges are "
                        "in every maximum unique pairing; unresolved edges are not "
                        "assigned by delivery order or timeout."
                    ),
                }
            )
        conflicts.sort(key=lambda c: c["conflict_id"])
        return forced, conflicts

    # -------------------------------------------------------------- coverage
    def _control_evidence(self, boot: str) -> dict[str, Any]:
        records = sorted(self.close_records.get(boot, []), key=lambda r: (int(r["body"].get("last", 0)), r["step"]))
        if not records:
            return {
                "boot": boot,
                "close_credential": None,
                "close_credentials": [],
                "expected_seq_interval": None,
                "observed_seqs": sorted(seq for b, seq in self.triggers if b == boot),
                "continuous": False,
                "missing_seqs": [],
                "unexpected_seqs": [],
                "supporting": False,
                "reasons": ["close_credential_missing"],
            }
        # A boot normally has one close credential. Exact retransmissions are
        # deduped; distinct last claims are conflicts, not arrival-order choices.
        selected = records[-1]
        last = int(selected["body"]["last"])
        distinct_lasts = sorted({int(r["body"]["last"]) for r in records})
        observed = sorted(seq for b, seq in self.triggers if b == boot)
        expected = list(range(1, last + 1))
        missing = sorted(set(expected) - set(observed))
        unexpected = sorted(set(observed) - set(expected))
        prefix_end = 0
        for seq in observed:
            if seq == prefix_end + 1:
                prefix_end = seq
            else:
                break
        prefix_continuous = prefix_end > 0 and prefix_end == len(observed) and observed == list(range(1, prefix_end + 1))
        continuous = (
            not missing
            and not unexpected
            and observed == expected
            and len(distinct_lasts) == 1
        )
        reasons: list[str] = []
        if len(distinct_lasts) > 1:
            reasons.append("conflicting_close_credentials")
        if missing or unexpected or observed != expected:
            reasons.append("business_sequence_not_continuous_through_close_last")
        return {
            "boot": boot,
            "close_credential": {
                "step": selected["step"],
                "last": last,
                "body": selected["body"],
            },
            "close_credentials": [
                {"step": r["step"], "last": int(r["body"]["last"]), "body": r["body"]} for r in records
            ],
            "distinct_last_values": distinct_lasts,
            "expected_seq_interval": [1, last],
            "observed_seqs": observed,
            "continuous": continuous,
            "continuous_through_close_last": continuous,
            "continuous_prefix": prefix_continuous,
            "continuous_prefix_end": prefix_end,
            "missing_seqs": missing,
            "unexpected_seqs": unexpected,
            "supporting": continuous,
            "reasons": reasons,
        }

    def _camera_evidence(self, channel: str, lo: int, hi: int) -> dict[str, Any]:
        current_revs = self._current_clock_revs()
        current_clocks: list[dict[str, Any]] = []
        for (camera, camera_boot), rev in sorted(current_revs.items()):
            if camera != channel:
                continue
            clock = self.clocks.get((camera, camera_boot, rev))
            if clock is not None:
                current_clocks.append(
                    {
                        "camera_boot": camera_boot,
                        "clock_rev": rev,
                        "offset": int(clock["offset"]),
                        "first_tick": int(clock["first_tick"]),
                        "last_tick": int(clock["last_tick"]),
                        "reliable_unified_interval": [
                            int(clock["first_tick"]) + int(clock["offset"]),
                            int(clock["last_tick"]) + int(clock["offset"]),
                        ],
                    }
                )
        evaluated: list[dict[str, Any]] = []
        supporting: list[dict[str, Any]] = []
        for record in sorted(self.coverages, key=lambda r: (r["step"], r["coverage_id"])):
            cov = record["body"]
            if str(cov.get("camera")) != channel:
                continue
            camera_boot = str(cov.get("boot"))
            clock_rev = int(cov.get("clock_rev"))
            current_rev = current_revs.get((channel, camera_boot))
            exact_clock = self.clocks.get((channel, camera_boot, clock_rev))
            covers = int(cov["from"]) <= lo and int(cov["until"]) >= hi
            revision_current = current_rev == clock_rev and exact_clock is not None
            reasons = []
            if not covers:
                reasons.append("interval_does_not_cover_full_candidate_boundary")
            if exact_clock is None:
                reasons.append("coverage_clock_revision_unknown")
            elif not revision_current:
                reasons.append("coverage_clock_revision_is_stale_after_clock_correction")
            item = {
                "coverage_id": record["coverage_id"],
                "step": record["step"],
                "camera": channel,
                "camera_boot": camera_boot,
                "clock_rev": clock_rev,
                "current_clock_rev": current_rev,
                "promised_unified_interval": [int(cov["from"]), int(cov["until"])],
                "covers_boundary": covers,
                "revision_current": revision_current,
                "supports": covers and revision_current,
                "reasons": reasons,
            }
            evaluated.append(item)
            if item["supports"]:
                supporting.append(item)
        reasons: list[str] = []
        if not evaluated:
            reasons.append("camera_coverage_commitment_missing")
        elif not supporting:
            if any(not x["revision_current"] for x in evaluated):
                reasons.append("coverage_commitment_not_for_current_clock_revision")
            if any(not x["covers_boundary"] for x in evaluated):
                reasons.append("camera_coverage_does_not_promise_full_boundary")
        return {
            "channel": channel,
            "required_unified_interval": [lo, hi],
            "current_clock_segments": current_clocks,
            "supporting_commitment_ids": [x["coverage_id"] for x in supporting],
            "supporting_commitments": supporting,
            "evaluated_commitments": evaluated,
            "supporting": bool(supporting),
            "reasons": reasons,
        }

    # ------------------------------------------------------------- intentions
    def _ensure_intention(self, target: str, step: int, origin: str = "reconcile") -> dict[str, Any]:
        intention = self.intentions.get(target)
        if intention is not None:
            return intention
        parsed = parse_target(target)
        boot, seq, channel = parsed if parsed else (None, None, None)
        intention = {
            "intention_id": "RT:" + target,
            "target": target,
            "boot": boot,
            "seq": seq,
            "channel": channel,
            "status": OFFERED,
            "origin": origin,
            "created_step": step,
            "updated_step": step,
            "revision": 1,
            "version": 0,
            "claimable": False,
            "receipts": [],
            "receipt_ids": [],
            "retake_frame_ids": [],
            "original_frame_ids": [],
            "withdrawal_reason": None,
            "hold_reason": None,
            "original_recovered_after_acceptance": False,
            "original_recovered_after_completion": False,
            "late_receipt_after_withdrawal": False,
            "lease": None,
            "history": [],
        }
        self.intentions[target] = intention
        return intention

    def _append_event(
        self,
        intention: dict[str, Any],
        kind: str,
        step: int,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        intention["version"] += 1
        intention["updated_step"] = step
        event = {
            "seq": len(self.events) + 1,
            "event_id": short_id("EVT", f"{intention['intention_id']}:{intention['version']}:{kind}"),
            "event_key": f"{intention['intention_id']}@v{intention['version']}:{kind}",
            "intention_id": intention["intention_id"],
            "target": intention["target"],
            "kind": kind,
            "revision": intention["revision"],
            "version": intention["version"],
            "step": step,
            "payload": payload or {},
        }
        self.events.append(event)
        intention["history"].append(
            {
                "event": kind,
                "step": step,
                "version": intention["version"],
                "revision": intention["revision"],
                **(payload or {}),
            }
        )
        return event

    def _sync_receipts(self, intention: dict[str, Any], step: int) -> tuple[bool, bool]:
        target = intention["target"]
        target_receipts = [r for r in self.receipts.values() if r.get("target") == target]
        target_receipts.sort(key=lambda r: (int(r.get("_step", 0)), str(r.get("receipt"))))
        known = set(intention.get("receipt_ids", []))
        for receipt in target_receipts:
            rid = str(receipt["receipt"])
            if rid in known:
                continue
            known.add(rid)
            summary = {
                "receipt": rid,
                "state": receipt.get("state"),
                "step": int(receipt.get("_step", step)),
            }
            intention["receipts"].append(summary)
            intention["receipt_ids"].append(rid)
            state = str(receipt.get("state", "")).lower()
            if state in COMPLETED_STATES:
                intention["status"] = COMPLETED
                self._append_event(intention, "intention.completed", summary["step"], {"receipt": rid})
            elif state in ACCEPTED_STATES:
                if intention["status"] != COMPLETED:
                    intention["status"] = ACCEPTED
                self._append_event(intention, "intention.accepted", summary["step"], {"receipt": rid})
            else:
                self._issue("unknown_receipt_state", "field receipt state is not accepted/completed", receipt=rid, state=state)
        has_completed = any(str(r["state"]).lower() in COMPLETED_STATES for r in intention["receipts"])
        has_accepted = bool(intention["receipts"])
        return has_accepted, has_completed

    def claim_intention(self, target: str, claimer: str, step: int | None = None) -> dict[str, Any]:
        """Record a delivery/claim lease.

        A lease is deliberately not a field acceptance receipt.  An intention
        with a lease but no accepted/completed receipt remains revocable.
        """
        intention = self.intentions.get(target)
        if intention is None:
            raise KeyError(f"no retake intention for {target}")
        fact = self.facts.get(target)
        if intention.get("status") == CLAIMED and intention.get("lease", {}).get("claimer") == claimer:
            # Polling/retry by the same field worker must not create a new task.
            return intention
        if intention["status"] != OFFERED or not intention.get("claimable"):
            raise PermissionError(f"intention {target} is not claimable: {intention['status']}")
        if fact is not None and fact["state"] != MISSING_CONFIRMED:
            raise PermissionError(f"intention {target} is not currently confirmed missing")
        step = int(step or self.max_step or 0)
        token = short_id("CLAIM", f"{target}|{claimer}|{step}|{canonical_hash(intention['version'])}")
        intention["status"] = CLAIMED
        intention["claimable"] = False
        intention["lease"] = {"claimer": claimer, "token": token, "step": step}
        self._append_event(intention, "intention.claimed", step, {"claimer": claimer, "token": token})
        return intention

    def register_receipt(self, receipt_body: dict[str, Any], step: int | None = None) -> dict[str, Any]:
        row = {"step": step or self.max_step or 0, "kind": "field", "body": receipt_body}
        self.ingest_record(row)
        return self.intentions[str(receipt_body["target"])]

    # ------------------------------------------------------------- reconcile
    def reconcile(self, step: int | None = None) -> None:
        step = int(step or self.max_step or 0)
        frame_views = self.frame_views()
        edges = self._candidate_edges(frame_views)
        forced, conflicts = self._resolve_graph(edges)
        self.conflicts = conflicts
        conflict_by_target: dict[str, str] = {}
        for conflict in conflicts:
            for target in conflict["unresolved_targets"] or conflict["targets"]:
                conflict_by_target[target] = conflict["conflict_id"]

        frame_by_id = {f["id"]: f for f in frame_views}
        edges_by_target: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for edge in edges:
            edge["forced"] = forced.get(edge["edge_key"], False)
            edges_by_target[edge["target"]].append(edge)

        # Ensure receipt-referenced targets exist before building facts.
        for receipt in self.receipts.values():
            target = str(receipt.get("target", ""))
            if parse_target(target):
                self._ensure_intention(target, int(receipt.get("_step", step)), origin="field_receipt")

        facts: dict[str, dict[str, Any]] = {}
        for (boot, seq), trigger in sorted(self.triggers.items()):
            control = self._control_evidence(boot)
            lo = int(trigger["time"]) - int(trigger["tol"])
            hi = int(trigger["time"]) + int(trigger["tol"])
            for raw_channel in trigger.get("channels", []):
                channel = str(raw_channel)
                target = target_key(boot, seq, channel)
                target_edges = sorted(edges_by_target.get(target, []), key=lambda e: (e["frame"], e["edge_key"]))
                forced_edges = [e for e in target_edges if e["forced"]]
                camera = self._camera_evidence(channel, lo, hi)
                fact: dict[str, Any] = {
                    "target": target,
                    "boot": boot,
                    "seq": seq,
                    "channel": channel,
                    "trigger": deepcopy(trigger),
                    "candidate_boundary": [lo, hi],
                    "state": PENDING,
                    "paired_frame": None,
                    "forced_frames": [],
                    "candidate_edges": target_edges,
                    "candidate_frame_ids": [e["frame"] for e in target_edges],
                    "conflict_id": conflict_by_target.get(target),
                    "control_coverage": control,
                    "camera_coverage": camera,
                    "coverage_used_for_decision": None,
                    "reasons": [],
                    "decided_step": step,
                }
                if forced_edges:
                    # Forced graph matching cannot put two forced edges on one target.
                    edge = forced_edges[0]
                    frame = frame_by_id[edge["frame"]]
                    fact["state"] = PAIRED
                    fact["paired_frame"] = frame
                    fact["forced_frames"] = [frame]
                    fact["coverage_used_for_decision"] = {
                        "trigger_box": control,
                        "camera": camera,
                    }
                    if len(forced_edges) > 1:
                        fact["reasons"].append("multiple_forced_edges_unexpected")
                elif target_edges:
                    fact["state"] = AMBIGUOUS
                    fact["conflict_id"] = conflict_by_target.get(target) or short_id(
                        "CFL", "single-target-multi-frame|" + target
                    )
                    fact["reasons"].append("candidate_attribution_not_unique")
                    fact["coverage_used_for_decision"] = None
                else:
                    reasons = control["reasons"] + camera["reasons"]
                    fact["reasons"] = reasons
                    if control["supporting"] and camera["supporting"]:
                        fact["state"] = MISSING_CONFIRMED
                        fact["coverage_used_for_decision"] = {
                            "trigger_box": control,
                            "camera": camera,
                        }
                    else:
                        fact["state"] = PENDING
                facts[target] = fact
        self.facts = facts

        retake_by_target: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for frame in frame_views:
            if frame.get("retake_of"):
                target = str(frame["retake_of"])
                retake_by_target[target].append(frame)
                if parse_target(target):
                    self._ensure_intention(target, step, origin="retake_frame")

        # Create confirmed-missing intentions and reconcile every known intention.
        fact_order = sorted(
            facts.items(),
            key=lambda kv: (str(kv[1].get("boot")), int(kv[1].get("seq", 0)), str(kv[1].get("channel"))),
        )
        for target, fact in fact_order:
            intention = self.intentions.get(target)
            if fact["state"] == MISSING_CONFIRMED:
                if intention is None:
                    intention = self._ensure_intention(target, step, origin="missing_confirmed")
                already_issued = any(h.get("event") == "intention.issued" for h in intention.get("history", []))
                if not already_issued and not intention.get("receipts"):
                    self._append_event(
                        intention,
                        "intention.issued",
                        step,
                        {
                            "target": target,
                            "reason": "original_frame_missing_and_coverage_confirms_boundary",
                            "control": fact["control_coverage"]["expected_seq_interval"],
                            "camera_coverage_ids": fact["camera_coverage"]["supporting_commitment_ids"],
                        },
                    )
            if intention is None:
                continue

            has_accepted, has_completed = self._sync_receipts(intention, step)
            intention["retake_frame_ids"] = sorted(
                f["id"] for f in retake_by_target.get(target, [])
            )
            if fact["state"] == PAIRED and fact.get("paired_frame"):
                intention["original_frame_ids"] = [fact["paired_frame"]["id"]]
            else:
                intention["original_frame_ids"] = []

            # Receipts dominate handling state, but never erase the lifecycle.
            if has_completed:
                if intention["status"] != COMPLETED:
                    intention["status"] = COMPLETED
                if fact["state"] == PAIRED and not intention["original_recovered_after_completion"]:
                    intention["original_recovered_after_completion"] = True
                    self._append_event(
                        intention,
                        "intention.original_recovered_after_completion",
                        step,
                        {"original_frame_ids": intention["original_frame_ids"], "distinction": "completed_record_preserved"},
                    )
            elif has_accepted:
                if intention["status"] != ACCEPTED:
                    intention["status"] = ACCEPTED
                if fact["state"] == PAIRED and not intention["original_recovered_after_acceptance"]:
                    intention["original_recovered_after_acceptance"] = True
                    self._append_event(
                        intention,
                        "intention.superseded_after_acceptance",
                        step,
                        {
                            "original_frame_ids": intention["original_frame_ids"],
                            "distinction": "acceptance_preserved_stop_open_retake_work",
                        },
                    )
            else:
                # No field acceptance/completion receipt: safe to withdraw,
                # hold, or reopen the single stable intention.
                if fact["state"] == PAIRED:
                    if intention["status"] != WITHDRAWN:
                        old_status = intention["status"]
                        intention["status"] = WITHDRAWN
                        intention["claimable"] = False
                        intention["withdrawal_reason"] = "original_frame_paired"
                        intention["hold_reason"] = None
                        intention["revision"] += 1
                        self._append_event(
                            intention,
                            "intention.withdrawn",
                            step,
                            {"from_status": old_status, "reason": "original_frame_paired", "revision": intention["revision"]},
                        )
                elif fact["state"] == AMBIGUOUS:
                    if intention["status"] != WITHDRAWN or intention["withdrawal_reason"] != "unresolved_candidate":
                        old_status = intention["status"]
                        intention["status"] = WITHDRAWN
                        intention["claimable"] = False
                        intention["withdrawal_reason"] = "unresolved_candidate"
                        intention["hold_reason"] = fact.get("conflict_id")
                        intention["revision"] += 1
                        self._append_event(
                            intention,
                            "intention.withdrawn",
                            step,
                            {"from_status": old_status, "reason": "unresolved_candidate", "conflict_id": fact.get("conflict_id")},
                        )
                elif fact["state"] == PENDING:
                    if intention["status"] != HELD:
                        old_status = intention["status"]
                        intention["status"] = HELD
                        intention["claimable"] = False
                        intention["hold_reason"] = ";".join(fact.get("reasons", [])) or "confirmation_pending"
                        self._append_event(
                            intention,
                            "intention.held",
                            step,
                            {"from_status": old_status, "reasons": fact.get("reasons", [])},
                        )
                elif fact["state"] == MISSING_CONFIRMED:
                    intention["withdrawal_reason"] = None
                    intention["hold_reason"] = None
                    if intention["status"] in {WITHDRAWN, HELD}:
                        old_status = intention["status"]
                        # A lease without a receipt is still only a checkout.
                        # Preserve it when a temporary hold is resumed.
                        if old_status == HELD and intention.get("lease"):
                            intention["status"] = CLAIMED
                        else:
                            intention["status"] = OFFERED
                        intention["revision"] += 1
                        event_kind = "intention.reopened" if old_status == WITHDRAWN else "intention.resumed"
                        self._append_event(
                            intention,
                            event_kind,
                            step,
                            {"from_status": old_status, "revision": intention["revision"]},
                        )
                    elif intention["status"] == CLAIMED:
                        pass
                    else:
                        intention["status"] = OFFERED

            # A late receipt can arrive after withdrawal.  Keep withdrawal
            # history but expose the immutable handling fact.
            if has_accepted and intention["status"] == WITHDRAWN:
                intention["late_receipt_after_withdrawal"] = True
                if has_completed:
                    intention["status"] = COMPLETED
                else:
                    intention["status"] = ACCEPTED

            intention["claimable"] = (
                intention["status"] == OFFERED
                and fact["state"] == MISSING_CONFIRMED
                and not has_accepted
                and not has_completed
            )

        # Receipt/retake-only intentions have no trigger fact yet.
        for target, intention in sorted(self.intentions.items()):
            if target not in facts:
                has_accepted = bool(intention["receipts"])
                has_completed = any(str(r["state"]).lower() in COMPLETED_STATES for r in intention["receipts"])
                intention["claimable"] = False
                if has_completed:
                    intention["status"] = COMPLETED
                elif has_accepted:
                    intention["status"] = ACCEPTED
                else:
                    intention["status"] = "recorded_waiting_for_trigger_fact"

    # ----------------------------------------------------------------- views
    def claimable_intentions(self) -> list[dict[str, Any]]:
        return [deepcopy(x) for x in sorted(self.intentions.values(), key=lambda x: x["target"]) if x.get("claimable")]

    def operator_view(self) -> dict[str, Any]:
        facts = [
            self.facts[k]
            for k in sorted(
                self.facts,
                key=lambda t: (
                    str(self.facts[t].get("boot")),
                    int(self.facts[t].get("seq", 0)),
                    str(self.facts[t].get("channel")),
                ),
            )
        ]
        usable = [
            {
                "target": f["target"],
                "state": f["state"],
                "paired_frame": f["paired_frame"],
                "candidate_frame_ids": f["candidate_frame_ids"],
                "conflict_id": f["conflict_id"],
                "retake_status": (self.intentions.get(f["target"]) or {}).get("status"),
            }
            for f in facts
            if f["state"] == PAIRED
        ]
        ambiguous = [self._fact_summary(f, self.intentions.get(f["target"])) for f in facts if f["state"] == AMBIGUOUS]
        pending = [self._fact_summary(f, self.intentions.get(f["target"])) for f in facts if f["state"] == PENDING]
        confirmed_gaps = [
            self._fact_summary(f, self.intentions.get(f["target"]))
            for f in facts
            if f["state"] == MISSING_CONFIRMED
        ]
        handed = [
            i
            for i in sorted(self.intentions.values(), key=lambda x: x["target"])
            if i["status"] != WITHDRAWN
        ]
        return {
            "shift": self.shift,
            "max_step": self.max_step,
            "original_frames": [deepcopy(f) for f in self.frame_views() if not f.get("retake_of")],
            "retake_frames": [deepcopy(f) for f in self.frame_views() if f.get("retake_of")],
            "usable_images": usable,
            "ambiguous_attributions": ambiguous,
            "unconfirmed_gaps": pending,
            "confirmed_missing": confirmed_gaps,
            "retake_intentions": [deepcopy(i) for i in sorted(self.intentions.values(), key=lambda x: x["target"])],
            "retake_items": [deepcopy(i) for i in sorted(self.intentions.values(), key=lambda x: x["target"])],
            "field_receipts": [deepcopy(r) for _, r in sorted(self.receipts.items())],
            "claimable_retakes": self.claimable_intentions(),
            "handed_to_retake_team": [deepcopy(i) for i in handed],
            "conflicts": deepcopy(self.conflicts),
            "coverage_for_each_decision": [
                {
                    "target": f["target"],
                    "state": f["state"],
                    "candidate_boundary": f["candidate_boundary"],
                    "control_coverage": f["control_coverage"],
                    "camera_coverage": f["camera_coverage"],
                }
                for f in facts
            ],
            "validation_issues": deepcopy(self.validation_issues),
        }

    @staticmethod
    def _fact_summary(f: dict[str, Any], intention: dict[str, Any] | None = None) -> dict[str, Any]:
        intention = intention or {}
        return {
            "target": f["target"],
            "state": f["state"],
            "reasons": f["reasons"],
            "conflict_id": f["conflict_id"],
            "candidate_boundary": f["candidate_boundary"],
            "candidate_frame_ids": f["candidate_frame_ids"],
            "retake_intention_id": intention.get("intention_id"),
            "retake_status": intention.get("status"),
            "retake_claimable": intention.get("claimable", False),
            "control_supporting": f["control_coverage"]["supporting"],
            "camera_supporting": f["camera_coverage"]["supporting"],
            "control_coverage": f["control_coverage"],
            "camera_coverage": f["camera_coverage"],
        }

    def snapshot(self) -> dict[str, Any]:
        return {
            "shift": self.shift,
            "max_step": self.max_step,
            "processed_hashes": sorted(self.processed_hashes),
            "triggers": [{"key": list(k), "body": v} for k, v in sorted(self.triggers.items())],
            "clocks": [{"key": list(k), "body": v} for k, v in sorted(self.clocks.items())],
            "frames": self.frame_views(),
            "coverages": deepcopy(self.coverages),
            "receipts": [v for _, v in sorted(self.receipts.items())],
            "facts": [
                self.facts[k]
                for k in sorted(
                    self.facts,
                    key=lambda t: (
                        str(self.facts[t].get("boot")),
                        int(self.facts[t].get("seq", 0)),
                        str(self.facts[t].get("channel")),
                    ),
                )
            ],
            "conflicts": deepcopy(self.conflicts),
            "intentions": [self.intentions[k] for k in sorted(self.intentions)],
            "events": deepcopy(self.events),
            "validation_issues": deepcopy(self.validation_issues),
            "operator": self.operator_view(),
        }

    def to_state(self) -> dict[str, Any]:
        """Compact durable state used by :class:`imaging_lake.LakeStore`."""
        return {
            "shift": self.shift,
            "max_step": self.max_step,
            "processed_hashes": sorted(self.processed_hashes),
            "triggers": {f"{k[0]}\u0000{k[1]}": v for k, v in self.triggers.items()},
            "trigger_observations": {
                f"{k[0]}\u0000{k[1]}": v for k, v in self.trigger_observations.items()
            },
            "trigger_conflicts": self.trigger_conflicts,
            "clocks": {f"{k[0]}\u0000{k[1]}\u0000{k[2]}": v for k, v in self.clocks.items()},
            "frames": self.frames,
            "frame_uploads": {k: sorted(v) for k, v in self.frame_uploads.items()},
            "close_records": dict(self.close_records),
            "coverages": self.coverages,
            "coverage_hashes": sorted(self._coverage_hashes),
            "receipts": self.receipts,
            "intentions": self.intentions,
            "events": self.events,
            "facts": self.facts,
            "conflicts": self.conflicts,
            "validation_issues": self.validation_issues,
        }

    @classmethod
    def from_state(cls, state: dict[str, Any]) -> "LakeEngine":
        engine = cls(shift=state.get("shift"))
        engine.max_step = state.get("max_step", 0)
        engine.processed_hashes = set(state.get("processed_hashes", []))
        engine.triggers = {
            (k.split("\u0000")[0], int(k.split("\u0000")[1])): v for k, v in state.get("triggers", {}).items()
        }
        engine.trigger_observations = defaultdict(
            list,
            {
                (k.split("\u0000")[0], int(k.split("\u0000")[1])): v
                for k, v in state.get("trigger_observations", {}).items()
            },
        )
        engine.trigger_conflicts = state.get("trigger_conflicts", {})
        engine.clocks = {
            (parts[0], parts[1], int(parts[2])): v
            for k, v in state.get("clocks", {}).items()
            for parts in [k.split("\u0000")]
        }
        # JSON object keys make rev strings; defaultdict keys are ints in memory.
        raw_frames = state.get("frames", {})
        engine.frames = defaultdict(dict)
        for fid, revs in raw_frames.items():
            for rev, body in revs.items():
                engine.frames[fid][int(rev)] = body
        engine.frame_uploads = defaultdict(set, {k: set(v) for k, v in state.get("frame_uploads", {}).items()})
        engine.close_records = defaultdict(list, state.get("close_records", {}))
        engine.coverages = state.get("coverages", [])
        engine._coverage_hashes = set(state.get("coverage_hashes", []))
        engine.receipts = state.get("receipts", {})
        engine.intentions = state.get("intentions", {})
        engine.events = state.get("events", [])
        engine.facts = state.get("facts", {})
        engine.conflicts = state.get("conflicts", [])
        engine.validation_issues = state.get("validation_issues", [])
        return engine


def replay(rows: Iterable[dict[str, Any]], shift: str | None = None) -> LakeEngine:
    """Replay all exchange rows and return the reconciled engine."""
    engine = LakeEngine(shift=shift)
    engine.ingest_rows(rows)
    return engine
