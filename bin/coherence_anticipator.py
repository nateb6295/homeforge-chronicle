#!/usr/bin/env python3
"""Pintar Test 3: Anticipatory coherence maintenance.

Monitors gist-entity pressure — how much the semantic gist has diverged
since the last entity turnover. When pressure exceeds threshold, triggers
preemptive orphan repair before StrongSync actually degrades.

This makes CCS anticipatory (Pintar criterion 3) rather than purely reactive.

Usage: python3 coherence_anticipator.py [--repair]
  Without --repair: report pressure only
  With --repair: trigger orphan repair if pressure exceeds threshold
"""
import json
import sqlite3
import sys
import time
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
LOG = Path("/home/nate-agx/chronicle/data/entity_dynamics.jsonl")
PRESSURE_THRESHOLD = 0.3
CRITICAL_THRESHOLD = 0.5


def get_current_gist():
    db = sqlite3.connect(str(DB))
    row = db.execute(
        "SELECT version, semantic_gist FROM cognitive_state WHERE id = 1"
    ).fetchone()
    db.close()
    if not row:
        return 0, ""
    return row[0], row[1] or ""


def get_gist_at_last_turnover():
    if not LOG.exists():
        return None
    entries = []
    for line in LOG.read_text().strip().split("\n"):
        if line.strip():
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    for e in reversed(entries):
        if e.get("turnover", 0) > 0:
            return e.get("version", 0)
    return entries[0].get("version", 0) if entries else None


def get_gist_at_version(version):
    db = sqlite3.connect(str(DB))
    row = db.execute(
        "SELECT snapshot FROM cognitive_state_history WHERE json_extract(snapshot, '$.version') = ?",
        (version,)
    ).fetchone()
    if not row:
        row = db.execute(
            "SELECT snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT 1"
        ).fetchone()
    db.close()
    if not row:
        return ""
    try:
        snap = json.loads(row[0])
        return snap.get("semantic_gist", "")
    except (json.JSONDecodeError, TypeError):
        return ""


def word_overlap_similarity(text_a, text_b):
    if not text_a or not text_b:
        return 0.0
    words_a = set(text_a.lower().split())
    words_b = set(text_b.lower().split())
    if not words_a or not words_b:
        return 0.0
    intersection = words_a & words_b
    union = words_a | words_b
    return len(intersection) / len(union)


def compute_pressure():
    version, current_gist = get_current_gist()
    last_turnover_version = get_gist_at_last_turnover()

    if last_turnover_version is None:
        return {"pressure": 0.0, "reason": "no turnover history", "version": version}

    old_gist = get_gist_at_version(last_turnover_version)
    similarity = word_overlap_similarity(current_gist, old_gist)
    pressure = 1.0 - similarity

    return {
        "version": version,
        "last_turnover_version": last_turnover_version,
        "gist_similarity": round(similarity, 4),
        "pressure": round(pressure, 4),
        "advisory": (
            "anticipatory" if pressure > CRITICAL_THRESHOLD
            else "watch" if pressure > PRESSURE_THRESHOLD
            else "stable"
        ),
    }


def main():
    do_repair = "--repair" in sys.argv
    result = compute_pressure()

    print(f"v{result['version']}: gist-entity pressure = {result['pressure']:.3f} [{result['advisory']}]")
    if result.get("last_turnover_version"):
        print(f"  Last turnover at v{result['last_turnover_version']}, similarity = {result.get('gist_similarity', '?')}")

    if result["advisory"] == "anticipatory":
        print(f"  ⚠ ANTICIPATORY: pressure > {CRITICAL_THRESHOLD} — entity set likely stale")
        if do_repair:
            print("  Triggering preemptive orphan repair...")
            sys.path.insert(0, str(Path(__file__).parent))
            from entity_guard import proactive_decay, get_snapshots as guard_get_snapshots
            from entity_dynamics_log import get_current_state

            _, entities, _ = get_current_state()
            current_list = [e for e in entities if isinstance(e, dict)]
            history = guard_get_snapshots(30)
            if history and current_list:
                decayed, names = proactive_decay(current_list, history)
                if names:
                    db = sqlite3.connect(str(DB))
                    db.execute(
                        "UPDATE cognitive_state SET focal_entities = ? WHERE id = 1",
                        (json.dumps(decayed),)
                    )
                    db.commit()
                    db.close()
                    print(f"  Preemptive decay: removed {len(names)} entities")
                    for n in names:
                        print(f"    - {n}")
                else:
                    print("  No entities eligible for preemptive decay")
    elif result["advisory"] == "watch":
        print(f"  Pressure rising — monitoring")

    entry = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "type": "anticipatory_check",
        **result,
    }
    with open(LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")


if __name__ == "__main__":
    main()
