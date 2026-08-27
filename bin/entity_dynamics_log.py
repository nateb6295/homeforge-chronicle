#!/usr/bin/env python3
"""Log entity set changes across CCS compressions.

Compares current focal_entities against last logged state.
Appends a JSONL entry for each compression showing: added, removed, retained,
retention scores, and whether the operator moved toward or away from idempotency.

Usage: python3 entity_dynamics_log.py
  Run after each compression (e.g., from stabilized_compress.py or sentinel).
"""
import json
import sqlite3
import time
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
LOG = Path("/home/nate-agx/chronicle/data/entity_dynamics.jsonl")


def get_current_state():
    db = sqlite3.connect(str(DB))
    row = db.execute(
        "SELECT version, focal_entities, semantic_gist, goal_orientation, "
        "predictive_cue, episodic_trace, uncertainty_signals, constraints, "
        "relational_map FROM cognitive_state WHERE id = 1"
    ).fetchone()
    db.close()
    if not row:
        return 0, [], ""
    version = row[0]
    entities = json.loads(row[1] or "[]")
    text_parts = [row[2] or "", row[3] or "", row[4] or ""]
    for field in (row[5], row[6], row[7]):
        if field:
            try:
                items = json.loads(field)
                for item in items:
                    text_parts.append(str(item))
            except (json.JSONDecodeError, TypeError):
                text_parts.append(str(field))
    if row[8]:
        text_parts.append(str(row[8]))
    return version, entities, " ".join(text_parts).lower()


def get_last_logged():
    if not LOG.exists():
        return None
    with open(LOG) as f:
        lines = f.readlines()
    if not lines:
        return None
    return json.loads(lines[-1])


def main():
    version, entities, full_text = get_current_state()
    current_names = {}
    for e in entities:
        if isinstance(e, dict):
            current_names[e.get("name", "")] = e.get("salience", 0.5)

    last = get_last_logged()
    if last:
        prev_names = set(last.get("entity_names", []))
    else:
        prev_names = set()

    curr_set = set(current_names.keys())
    added = sorted(curr_set - prev_names)
    removed = sorted(prev_names - curr_set)
    retained = sorted(curr_set & prev_names)

    salience_dist = {
        "high_gt07": sum(1 for s in current_names.values() if s > 0.7),
        "med_045_07": sum(1 for s in current_names.values() if 0.45 <= s <= 0.7),
        "low_lt045": sum(1 for s in current_names.values() if s < 0.45),
    }

    grounded = sum(1 for name in current_names if name.lower() in full_text)
    strong_sync = grounded / len(current_names) if current_names else 0
    weak_sync = len(retained) / max(len(curr_set), len(prev_names)) if prev_names else None

    if strong_sync > 0.4:
        coherence_advisory = "healthy"
    elif strong_sync > 0.2:
        coherence_advisory = "degrading"
    else:
        coherence_advisory = "critical"

    entry = {
        "version": version,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "total": len(current_names),
        "added": added,
        "removed": removed,
        "retained_count": len(retained),
        "turnover": len(added) + len(removed),
        "salience_distribution": salience_dist,
        "strong_sync": round(strong_sync, 4),
        "weak_sync": round(weak_sync, 4) if weak_sync is not None else None,
        "grounded_count": grounded,
        "coherence_advisory": coherence_advisory,
        "entity_names": sorted(current_names.keys()),
    }

    with open(LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")

    if added or removed:
        print(f"v{version}: +{len(added)} -{len(removed)} (turnover={entry['turnover']})")
        for n in added:
            print(f"  + {n} (sal={current_names[n]:.2f})")
        for n in removed:
            print(f"  - {n}")
    else:
        print(f"v{version}: NO CHANGE ({len(current_names)} entities)")

    print(f"  Distribution: {salience_dist}")
    ws_str = f"{weak_sync:.3f}" if weak_sync is not None else "—"
    print(f"  StrongSync: {strong_sync:.3f} ({grounded}/{len(current_names)} grounded) | WeakSync: {ws_str}")
    if coherence_advisory != "healthy":
        print(f"  ⚠ Coherence: {coherence_advisory.upper()}")


if __name__ == "__main__":
    main()
