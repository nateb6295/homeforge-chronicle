#!/usr/bin/env python3
"""entity_connectivity.py — measure entity co-occurrence as a predictor of persistence.

Hypothesis: entities that co-occur with high-persistence "anchor" entities
(Nate, Hermes) are more likely to be load-bearing and should resist deletion.
This is the prospective tolerance signal Hermes suggested.
"""

import json
import sqlite3
from collections import Counter, defaultdict

DB = "/mnt/hdd/chronicle-data/processed.db"


def get_all_snapshots():
    conn = sqlite3.connect(DB)
    rows = conn.execute(
        "SELECT created_at, json_extract(snapshot, '$.focal_entities') "
        "FROM cognitive_state_history ORDER BY created_at ASC"
    ).fetchall()
    conn.close()
    results = []
    for ts, ent_json in rows:
        entities = set()
        if ent_json:
            try:
                for e in json.loads(ent_json):
                    if isinstance(e, dict):
                        entities.add(e.get("name", ""))
            except (json.JSONDecodeError, TypeError):
                pass
        results.append({"ts": ts, "entities": entities})
    return results


def entity_persistence(snapshots):
    counts = Counter()
    for s in snapshots:
        for e in s["entities"]:
            counts[e] += 1
    total = len(snapshots)
    return {name: count / total for name, count in counts.items()}


def co_occurrence_matrix(snapshots):
    cooccur = defaultdict(Counter)
    for s in snapshots:
        ents = list(s["entities"])
        for i, a in enumerate(ents):
            for b in ents[i + 1:]:
                cooccur[a][b] += 1
                cooccur[b][a] += 1
    return cooccur


def connectivity_score(entity, cooccur, persistence):
    """Score = sum of co-occurrence * persistence for all co-occurring entities."""
    if entity not in cooccur:
        return 0.0
    score = 0.0
    for partner, count in cooccur[entity].items():
        score += count * persistence.get(partner, 0)
    return score


def predict_persistence(snapshots):
    persistence = entity_persistence(snapshots)
    cooccur = co_occurrence_matrix(snapshots)

    scores = {}
    for entity in persistence:
        scores[entity] = connectivity_score(entity, cooccur, persistence)

    return scores, persistence


def main():
    snapshots = get_all_snapshots()
    scores, persistence = predict_persistence(snapshots)
    total = len(snapshots)

    print(f"=== Entity Connectivity Analysis ({total} snapshots) ===\n")
    print(f"{'Entity':<30} {'Persist%':>8} {'Connect':>8} {'Ratio':>8}")
    print("-" * 60)

    sorted_entities = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    for name, conn_score in sorted_entities:
        pers = persistence[name]
        ratio = conn_score / max(pers, 0.01)
        print(f"{name:<30} {pers*100:>7.0f}% {conn_score:>8.1f} {ratio:>8.1f}")

    # Validate: do high-connectivity entities persist longer?
    print("\n=== Predictive Validation ===")
    entities_with_data = [(name, scores[name], persistence[name])
                          for name in scores if 0.05 < persistence[name] < 0.95]
    if len(entities_with_data) >= 4:
        entities_with_data.sort(key=lambda x: x[1], reverse=True)
        half = len(entities_with_data) // 2
        high_conn = entities_with_data[:half]
        low_conn = entities_with_data[half:]
        avg_pers_high = sum(e[2] for e in high_conn) / len(high_conn)
        avg_pers_low = sum(e[2] for e in low_conn) / len(low_conn)
        print(f"High-connectivity entities avg persistence: {avg_pers_high:.1%}")
        print(f"Low-connectivity entities avg persistence:  {avg_pers_low:.1%}")
        print(f"Connectivity predicts persistence: {'YES' if avg_pers_high > avg_pers_low else 'NO'}")

    # Identify at-risk entities: currently present but low connectivity
    print("\n=== At-Risk Entities (present in last snapshot, low connectivity) ===")
    last_entities = snapshots[-1]["entities"]
    median_conn = sorted(scores.values())[len(scores) // 2] if scores else 0
    for name in last_entities:
        if scores.get(name, 0) < median_conn:
            print(f"  {name}: connectivity {scores.get(name, 0):.1f} "
                  f"(median: {median_conn:.1f})")


if __name__ == "__main__":
    main()
