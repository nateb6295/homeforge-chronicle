#!/usr/bin/env python3
"""Substrate Profiler — Thread #315 Layer 4 instrument.

Measures inter-subsystem channel quality in Chronicle. Policies (sparsity,
action-gating, active-suppression) assume adequate coupling; this tool asks
whether the coupling is actually adequate.

Not content-quality (coupling_health.py already covers novelty-to-relay and
topological diversity). This is substrate-quality: latencies, queue depths,
freshness, where signals actually stall between subsystems.

Usage:
    python3 substrate_profile.py           # full report
    python3 substrate_profile.py --json    # machine-readable
"""
import argparse
import json
import os
import sqlite3
import sys
import time
from collections import defaultdict

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")


def _db():
    return sqlite3.connect(DB_PATH, timeout=10)


def source_freshness(db, window_hours: int = 24) -> dict:
    """For each activity_feed source, when was the last entry vs now."""
    now = int(time.time())
    rows = db.execute(
        "SELECT source, MAX(created_at), COUNT(*) FROM activity_feed "
        "WHERE created_at > ? GROUP BY source ORDER BY source",
        (now - window_hours * 3600,),
    ).fetchall()
    out = {}
    for source, last_ts, count in rows:
        out[source] = {
            "last_seen_s_ago": now - (last_ts or 0),
            "count_window": count,
        }
    return out


def capture_to_embedding(db, window_hours: int = 24) -> dict:
    """How long between a capsule being created and getting embedded."""
    now = int(time.time())
    rows = db.execute(
        "SELECT c.id, c.created_at, e.created_at "
        "FROM knowledge_capsules c "
        "LEFT JOIN capsule_embeddings e ON e.capsule_id = c.id "
        "WHERE c.created_at > ? ORDER BY c.id DESC LIMIT 500",
        (now - window_hours * 3600,),
    ).fetchall()
    if not rows:
        return {"total": 0}
    embedded = [(e_ts - c_ts) for _, c_ts, e_ts in rows if e_ts is not None]
    unembedded = sum(1 for _, _, e_ts in rows if e_ts is None)
    stats = {
        "total": len(rows),
        "embedded": len(embedded),
        "unembedded": unembedded,
        "embed_coverage": round(len(embedded) / len(rows), 3) if rows else 0.0,
    }
    if embedded:
        embedded.sort()
        stats["latency_s_min"] = embedded[0]
        stats["latency_s_p50"] = embedded[len(embedded) // 2]
        stats["latency_s_p95"] = embedded[int(len(embedded) * 0.95)]
        stats["latency_s_max"] = embedded[-1]
    return stats


def capsule_to_crossref(db, window_hours: int = 24) -> dict:
    """How long between briefs being available and them getting connected.

    Uses crossref_connections: for each connection, delay between the later
    brief's creation and the connection's creation = time for the crossref
    system to notice the pair.
    """
    now = int(time.time())
    rows = db.execute(
        "SELECT cc.brief_a_id, cc.brief_b_id, cc.created_at, "
        "       a.created_at, b.created_at "
        "FROM crossref_connections cc "
        "LEFT JOIN activity_feed a ON a.id = cc.brief_a_id "
        "LEFT JOIN activity_feed b ON b.id = cc.brief_b_id "
        "WHERE cc.created_at > ? ORDER BY cc.id DESC LIMIT 500",
        (now - window_hours * 3600,),
    ).fetchall()
    if not rows:
        return {"total": 0}
    delays = []
    for _, _, cc_ts, a_ts, b_ts in rows:
        if a_ts is None or b_ts is None:
            continue
        later = max(a_ts, b_ts)
        delays.append(cc_ts - later)
    if not delays:
        return {"total": len(rows), "resolvable": 0}
    delays.sort()
    return {
        "total": len(rows),
        "resolvable": len(delays),
        "latency_s_min": delays[0],
        "latency_s_p50": delays[len(delays) // 2],
        "latency_s_p95": delays[int(len(delays) * 0.95)],
        "latency_s_max": delays[-1],
    }


def capture_dedup_delta(db, window_hours: int = 24) -> dict:
    """Same capture often arrives via discord:capture AND operator:capture.

    Time between twin arrivals = how coupled the two ingestion paths are.
    Large deltas = one path is lagging. Missing twin = path is broken.
    """
    import re
    now = int(time.time())
    rows = db.execute(
        "SELECT id, source, content, created_at FROM activity_feed "
        "WHERE (source='discord:capture' OR source='operator:capture') "
        "AND created_at > ? ORDER BY id ASC",
        (now - window_hours * 3600,),
    ).fetchall()
    url_re = re.compile(r"https?://(?:x\.com|twitter\.com)/\w+/status/\d+")
    by_url: dict[str, list[tuple[int, str, int]]] = defaultdict(list)
    for row_id, source, content, ts in rows:
        urls = url_re.findall(content or "")
        key = urls[0].split("?")[0] if urls else None
        if key:
            by_url[key].append((row_id, source, ts))
    twinned = 0
    solo_operator = 0
    solo_discord = 0
    deltas = []
    for _, entries in by_url.items():
        sources = {s for _, s, _ in entries}
        if "operator:capture" in sources and "discord:capture" in sources:
            twinned += 1
            op_ts = min(ts for _, s, ts in entries if s == "operator:capture")
            dc_ts = min(ts for _, s, ts in entries if s == "discord:capture")
            deltas.append(abs(op_ts - dc_ts))
        elif "operator:capture" in sources:
            solo_operator += 1
        else:
            solo_discord += 1
    stats = {
        "unique_urls": len(by_url),
        "twinned": twinned,
        "solo_operator_only": solo_operator,
        "solo_discord_only": solo_discord,
    }
    if deltas:
        deltas.sort()
        stats["twin_delta_s_p50"] = deltas[len(deltas) // 2]
        stats["twin_delta_s_p95"] = deltas[int(len(deltas) * 0.95)]
        stats["twin_delta_s_max"] = deltas[-1]
    return stats


def thread_advance_cadence(db, window_hours: int = 48) -> dict:
    """Inter-advance latency per active thread. Stall detector."""
    now = int(time.time())
    active = db.execute(
        "SELECT id, title FROM cognitive_threads WHERE status='active'"
    ).fetchall()
    out = {}
    for thread_id, title in active:
        events = db.execute(
            "SELECT created_at FROM thread_history "
            "WHERE thread_id = ? AND created_at > ? "
            "ORDER BY created_at ASC",
            (thread_id, now - window_hours * 3600),
        ).fetchall()
        if not events:
            out[thread_id] = {"title": title, "events": 0,
                              "last_event_s_ago": None}
            continue
        timestamps = [t[0] for t in events]
        gaps = [timestamps[i] - timestamps[i - 1]
                for i in range(1, len(timestamps))]
        out[thread_id] = {
            "title": title,
            "events": len(timestamps),
            "last_event_s_ago": now - timestamps[-1],
            "median_gap_s": sorted(gaps)[len(gaps) // 2] if gaps else None,
        }
    return out


def format_s(seconds) -> str:
    if seconds is None:
        return "—"
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds / 60:.1f}m"
    if seconds < 86400:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--log", action="store_true",
                        help="append snapshot to data/substrate_profile.jsonl")
    args = parser.parse_args()

    db = _db()
    try:
        report = {
            "generated_at": int(time.time()),
            "window_hours": args.hours,
            "source_freshness": source_freshness(db, args.hours),
            "capsule_to_embedding": capture_to_embedding(db, args.hours),
            "brief_to_crossref": capsule_to_crossref(db, args.hours),
            "capture_twin_paths": capture_dedup_delta(db, args.hours),
            "thread_cadence": thread_advance_cadence(db, args.hours * 2),
        }
    finally:
        db.close()

    if args.log:
        log_dir = os.path.join(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))), "data")
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, "substrate_profile.jsonl")
        with open(log_path, "a") as f:
            f.write(json.dumps(report, default=str) + "\n")

    if args.json:
        print(json.dumps(report, indent=2, default=str))
        return

    print(f"\n=== Substrate Profile — Thread #315 Layer 4 ===")
    print(f"Window: last {args.hours}h\n")

    print("SOURCE FRESHNESS (time since each subsystem last emitted)")
    for source, info in sorted(report["source_freshness"].items(),
                                key=lambda kv: -kv[1]["count_window"])[:15]:
        print(f"  {source:32s}  last={format_s(info['last_seen_s_ago']):>8s}  "
              f"n={info['count_window']}")

    print("\nCAPSULE → EMBEDDING coupling")
    emb = report["capsule_to_embedding"]
    if emb["total"]:
        print(f"  coverage: {emb.get('embed_coverage', 0) * 100:.1f}% "
              f"({emb['embedded']}/{emb['total']})")
        if emb.get("latency_s_p50") is not None:
            print(f"  latency: p50={format_s(emb['latency_s_p50'])} "
                  f"p95={format_s(emb['latency_s_p95'])} "
                  f"max={format_s(emb['latency_s_max'])}")
        if emb.get("unembedded"):
            print(f"  UNEMBEDDED: {emb['unembedded']}  ← "
                  f"embedding channel is starved or broken")
    else:
        print("  (no capsules in window)")

    print("\nBRIEF → CROSSREF coupling")
    cr = report["brief_to_crossref"]
    if cr["total"]:
        print(f"  connections: {cr['total']}")
        if cr.get("latency_s_p50") is not None:
            print(f"  latency: p50={format_s(cr['latency_s_p50'])} "
                  f"p95={format_s(cr['latency_s_p95'])} "
                  f"max={format_s(cr['latency_s_max'])}")
    else:
        print("  (no crossref connections in window)")

    print("\nCAPTURE TWIN-PATH coupling (operator:capture ↔ discord:capture)")
    tp = report["capture_twin_paths"]
    print(f"  unique URLs: {tp['unique_urls']}  "
          f"twinned={tp['twinned']}  "
          f"operator-only={tp['solo_operator_only']}  "
          f"discord-only={tp['solo_discord_only']}")
    if "twin_delta_s_p50" in tp:
        print(f"  twin delta: p50={format_s(tp['twin_delta_s_p50'])} "
              f"p95={format_s(tp['twin_delta_s_p95'])} "
              f"max={format_s(tp['twin_delta_s_max'])}")

    print("\nTHREAD CADENCE (are active threads actually advancing?)")
    for tid, info in sorted(report["thread_cadence"].items()):
        gap = format_s(info.get("median_gap_s"))
        last = format_s(info.get("last_event_s_ago"))
        print(f"  #{tid} [{info['events']:3d} ev, last={last:>6s}, "
              f"gap~{gap:>6s}] {info['title'][:60]}")


if __name__ == "__main__":
    main()
