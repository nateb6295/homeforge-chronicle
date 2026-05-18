#!/usr/bin/env python3
"""Compositionality Scorer — measures structural complexity across CCS history.

Tests whether CCS episodic_trace and relational_map are becoming more
compositional over time (causal chains, reusable structure) vs staying
flat (noun phrases, key-value pairs).

Metrics per snapshot:
  - causal_density: fraction of episodic entries containing causal connectives
  - avg_entry_length: mean character length of episodic entries
  - relational_depth: mean length of relational_map values (chain complexity)
  - unique_connectives: count of distinct causal connectives used
  - compositionality_score: weighted combination of the above (0-1)

Usage:
  python3 compositionality_scorer.py              # full trajectory
  python3 compositionality_scorer.py --last 20    # last N snapshots
  python3 compositionality_scorer.py --plot        # save trajectory plot data
"""
import json
import os
import re
import sqlite3
import sys
import time
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
LOG_FILE = os.path.expanduser("~/chronicle/data/compositionality_trajectory.jsonl")

CAUSAL_CONNECTIVES = [
    "because", "linked because", "this mattered", "connected to",
    "which meant", "so that", "in order to", "as a result",
    "triggered", "caused", "led to", "enabled", "prevented",
    "the reason", "consequence", "implication", "therefore",
    "→", "—", "since", "given that", "due to",
]

SEMANTIC_CONNECTIVES = [
    "maps to", "maps directly to", "connects to", "cross-thread",
    "survives if", "doesn't survive", "stress-test",
    "if not", "whether", "but", "however", "although",
    "rather than", "instead of", "not just", "but also",
    "while", "whereas", "in contrast", "on the other hand",
    "which means", "suggesting", "implies that",
    "the question is", "open question", "unresolved",
    "before", "after", "once", "until",
    "across threads", "across", "intersection",
]

COMPOSITIONAL_MARKERS = [
    "linked because", "this mattered because", "connected to",
    "the reason", "which revealed", "what changed",
]


def score_episodic(entries):
    if not entries or not isinstance(entries, list):
        return {"causal_density": 0, "avg_length": 0, "unique_connectives": 0,
                "compositional_markers": 0, "entry_count": 0}

    str_entries = []
    for e in entries:
        if isinstance(e, str):
            str_entries.append(e)
        elif isinstance(e, dict):
            str_entries.append(json.dumps(e))

    if not str_entries:
        return {"causal_density": 0, "avg_length": 0, "unique_connectives": 0,
                "compositional_markers": 0, "entry_count": 0}

    causal_count = 0
    connectives_found = set()
    semantic_found = set()
    markers_found = 0
    chain_depths = []

    for entry in str_entries:
        lower = entry.lower()
        has_causal = False
        entry_conn_count = 0
        for conn in CAUSAL_CONNECTIVES:
            if conn.lower() in lower:
                connectives_found.add(conn)
                has_causal = True
                entry_conn_count += lower.count(conn.lower())
        for conn in SEMANTIC_CONNECTIVES:
            if conn.lower() in lower:
                semantic_found.add(conn)
                if not has_causal:
                    has_causal = True
                entry_conn_count += lower.count(conn.lower())
        if has_causal:
            causal_count += 1
        chain_depths.append(entry_conn_count)
        for marker in COMPOSITIONAL_MARKERS:
            if marker.lower() in lower:
                markers_found += 1
                break

    all_connectives = connectives_found | semantic_found
    avg_chain_depth = sum(chain_depths) / len(chain_depths) if chain_depths else 0

    return {
        "causal_density": causal_count / len(str_entries),
        "avg_length": sum(len(e) for e in str_entries) / len(str_entries),
        "unique_connectives": len(all_connectives),
        "compositional_markers": markers_found,
        "entry_count": len(str_entries),
        "avg_chain_depth": round(avg_chain_depth, 2),
        "causal_connectives": len(connectives_found),
        "semantic_connectives": len(semantic_found),
    }


def score_diversity(entries, rm):
    """Measure source diversity. Penalizes single-frame dominance."""
    if not entries or not isinstance(entries, list):
        return {"topic_diversity": 0, "source_count": 0, "dominance_ratio": 1.0}

    str_entries = []
    for e in entries:
        if isinstance(e, str):
            str_entries.append(e)
        elif isinstance(e, dict):
            str_entries.append(json.dumps(e))

    if not str_entries:
        return {"topic_diversity": 0, "source_count": 0, "dominance_ratio": 1.0}

    thread_refs = []
    for entry in str_entries:
        refs = re.findall(r'#(\d{3,4})', entry)
        thread_refs.extend(refs)

    key_terms = set()
    for entry in str_entries:
        words = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b', entry)
        key_terms.update(w.lower() for w in words if len(w) > 4)

    rm_keys = set()
    if rm and isinstance(rm, dict):
        for k in rm.keys():
            rm_keys.add(k.lower().strip())

    unique_threads = len(set(thread_refs))
    total_refs = max(len(thread_refs), 1)
    dominance = max(thread_refs.count(t) for t in set(thread_refs)) / total_refs if thread_refs else 1.0

    n_entries = len(str_entries)
    topic_diversity = min(unique_threads / max(n_entries, 1), 1.0) if unique_threads > 0 else min(len(key_terms) / (n_entries * 3), 1.0)

    return {
        "topic_diversity": round(topic_diversity, 3),
        "source_count": unique_threads or len(key_terms),
        "dominance_ratio": round(dominance, 3),
    }


def score_relational(rm):
    if not rm or not isinstance(rm, dict):
        return {"avg_value_length": 0, "chain_count": 0, "key_count": 0}

    values = [str(v) for v in rm.values()]
    if not values:
        return {"avg_value_length": 0, "chain_count": 0, "key_count": 0}

    chain_markers = ["→", "linked because", "—"]
    chain_count = 0
    for v in values:
        if any(m in v for m in chain_markers):
            chain_count += 1

    return {
        "avg_value_length": sum(len(v) for v in values) / len(values),
        "chain_count": chain_count,
        "key_count": len(rm),
    }


def compositionality_score(ep_scores, rm_scores, div_scores=None):
    """0-1 composite score. Higher = more compositional."""
    cd = min(ep_scores["causal_density"], 1.0)
    length_norm = min(ep_scores["avg_length"] / 300, 1.0)
    conn_norm = min(ep_scores["unique_connectives"] / 8, 1.0)
    marker_norm = min(ep_scores["compositional_markers"] / ep_scores["entry_count"], 1.0) if ep_scores["entry_count"] > 0 else 0
    chain_norm = min(rm_scores["chain_count"] / max(rm_scores["key_count"], 1), 1.0)
    depth_norm = min(rm_scores["avg_value_length"] / 200, 1.0)
    chain_depth_norm = min(ep_scores.get("avg_chain_depth", 0) / 4, 1.0)

    base = (
        cd * 0.15 +
        chain_depth_norm * 0.15 +
        length_norm * 0.15 +
        conn_norm * 0.15 +
        marker_norm * 0.10 +
        chain_norm * 0.15 +
        depth_norm * 0.15
    )

    if div_scores:
        diversity_penalty = max(0, div_scores["dominance_ratio"] - 0.5) * 0.2
        base = base - diversity_penalty

    return round(max(0, min(1, base)), 4)


def run_trajectory(limit=None):
    db = sqlite3.connect(str(DB))
    query = "SELECT id, created_at, snapshot FROM cognitive_state_history ORDER BY id ASC"
    if limit:
        total = db.execute("SELECT COUNT(*) FROM cognitive_state_history").fetchone()[0]
        offset = max(0, total - limit)
        query += f" LIMIT {limit} OFFSET {offset}"
    rows = db.execute(query).fetchall()
    db.close()

    results = []
    for hid, ts, snap_json in rows:
        try:
            snap = json.loads(snap_json)
        except (json.JSONDecodeError, TypeError):
            continue

        ep_raw = snap.get("episodic_trace", [])
        if isinstance(ep_raw, str):
            try:
                ep_raw = json.loads(ep_raw)
            except json.JSONDecodeError:
                ep_raw = [ep_raw]

        rm_raw = snap.get("relational_map", {})
        if isinstance(rm_raw, str):
            try:
                rm_raw = json.loads(rm_raw)
            except json.JSONDecodeError:
                rm_raw = {}

        ep = score_episodic(ep_raw)
        rm = score_relational(rm_raw)
        div = score_diversity(ep_raw, rm_raw)
        cs = compositionality_score(ep, rm, div)

        results.append({
            "history_id": hid,
            "ccs_version": snap.get("version"),
            "ts": ts,
            "compositionality": cs,
            "causal_density": ep["causal_density"],
            "avg_entry_length": round(ep["avg_length"]),
            "unique_connectives": ep["unique_connectives"],
            "compositional_markers": ep["compositional_markers"],
            "relational_chains": rm["chain_count"],
            "relational_keys": rm["key_count"],
            "relational_depth": round(rm["avg_value_length"]),
            "avg_chain_depth": ep.get("avg_chain_depth", 0),
            "topic_diversity": div["topic_diversity"],
            "dominance_ratio": div["dominance_ratio"],
            "source_count": div["source_count"],
        })

    return results


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--last", type=int, help="Only score last N snapshots")
    parser.add_argument("--plot", action="store_true", help="Save trajectory to JSONL")
    args = parser.parse_args()

    results = run_trajectory(limit=args.last)
    if not results:
        print("No CCS history found")
        return

    if args.plot:
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        with open(LOG_FILE, "w") as f:
            for r in results:
                f.write(json.dumps(r) + "\n")
        print(f"Saved {len(results)} entries to {LOG_FILE}")

    print(f"CCS Compositionality Trajectory ({len(results)} snapshots)")
    print("-" * 70)

    early = results[:5]
    recent = results[-5:]

    print("\nEARLIEST 5:")
    for r in early:
        t = time.strftime("%m/%d %H:%M", time.localtime(r["ts"]))
        print(f"  {r['history_id']:4d} [{t}] comp={r['compositionality']:.3f} "
              f"causal={r['causal_density']:.2f} len={r['avg_entry_length']} "
              f"conn={r['unique_connectives']} chains={r['relational_chains']}/{r['relational_keys']}")

    print("\nMOST RECENT 5:")
    for r in recent:
        t = time.strftime("%m/%d %H:%M", time.localtime(r["ts"]))
        print(f"  {r['history_id']:4d} [{t}] comp={r['compositionality']:.3f} "
              f"causal={r['causal_density']:.2f} len={r['avg_entry_length']} "
              f"conn={r['unique_connectives']} chains={r['relational_chains']}/{r['relational_keys']}")

    early_avg = sum(r["compositionality"] for r in early) / len(early)
    recent_avg = sum(r["compositionality"] for r in recent) / len(recent)
    overall_avg = sum(r["compositionality"] for r in results) / len(results)
    delta = recent_avg - early_avg

    print(f"\nEarly avg:   {early_avg:.3f}")
    print(f"Recent avg:  {recent_avg:.3f}")
    print(f"Overall avg: {overall_avg:.3f}")
    print(f"Delta:       {delta:+.3f} ({'INCREASING' if delta > 0.02 else 'DECREASING' if delta < -0.02 else 'STABLE'})")

    if len(results) >= 10:
        mid = len(results) // 2
        first_half = sum(r["compositionality"] for r in results[:mid]) / mid
        second_half = sum(r["compositionality"] for r in results[mid:]) / (len(results) - mid)
        print(f"\nFirst half:  {first_half:.3f}")
        print(f"Second half: {second_half:.3f}")
        print(f"Trend:       {second_half - first_half:+.3f}")


if __name__ == "__main__":
    main()
