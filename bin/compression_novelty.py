#!/usr/bin/env python3
"""Build #37: Compression novelty and persistence analysis.

Door 1 investigation: does compression CREATE genuine synthesis?

Tracks novel content (entities, relational edges, gist phrases) across
consecutive CCS states. Novel content that persists through subsequent
compressions is potential synthesis. Novel content that disappears is noise.

The key discriminant: noise is memoryless (like reflexive fields).
Synthesis sticks (like structural fields).
"""

import json
import sqlite3
import sys
import re
from collections import defaultdict

DB = "/mnt/hdd/chronicle-data/processed.db"


def get_ccs_history():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history ORDER BY id ASC"
    ).fetchall()
    db.close()
    return [(r[0], json.loads(r[1]), r[2]) for r in rows]


def extract_entities(ccs):
    return {e["name"] for e in ccs.get("focal_entities", [])}


def extract_edges(ccs):
    return set(ccs.get("relational_map", {}).keys())


def extract_gist_phrases(gist):
    gist = gist.lower()
    gist = re.sub(r'[^a-z0-9\s\-]', ' ', gist)
    words = gist.split()
    bigrams = set()
    for i in range(len(words) - 1):
        bg = f"{words[i]} {words[i+1]}"
        if len(words[i]) > 2 and len(words[i+1]) > 2:
            bigrams.add(bg)
    return bigrams


def track_persistence(history, extractor, label):
    all_states = []
    for sid, ccs, ts in history:
        items = extractor(ccs)
        all_states.append((sid, items))

    first_seen = {}
    last_seen = {}
    seen_at = defaultdict(list)

    for idx, (sid, items) in enumerate(all_states):
        for item in items:
            if item not in first_seen:
                first_seen[item] = idx
            last_seen[item] = idx
            seen_at[item].append(idx)

    novel_events = []
    for item, first_idx in first_seen.items():
        if first_idx == 0:
            continue
        lifespan = last_seen[item] - first_idx
        appearances = len(seen_at[item])
        continuous = True
        for i in range(first_idx, last_seen[item] + 1):
            if i not in seen_at[item]:
                continuous = False
                break
        novel_events.append({
            "item": item,
            "first_seen_idx": first_idx,
            "first_seen_id": all_states[first_idx][0],
            "lifespan": lifespan,
            "appearances": appearances,
            "continuous": continuous,
            "survived_1": (first_idx + 1 < len(all_states) and
                           item in all_states[first_idx + 1][1]),
            "survived_5": (first_idx + 5 < len(all_states) and
                           item in all_states[first_idx + 5][1]),
        })

    return novel_events


def analyze(events, label):
    if not events:
        print(f"\n  No novel {label} found")
        return {}

    lifespans = [e["lifespan"] for e in events]
    survived_1 = sum(1 for e in events if e["survived_1"])
    survived_5 = sum(1 for e in events if e["survived_5"])
    continuous = sum(1 for e in events if e["continuous"] and e["lifespan"] > 0)
    ephemeral = sum(1 for e in events if e["lifespan"] == 0)

    n = len(events)
    mean_life = sum(lifespans) / n if n else 0
    max_life = max(lifespans)

    print(f"\n{'='*60}")
    print(f"  {label.upper()} NOVELTY")
    print(f"{'='*60}")
    print(f"  Total novel items: {n}")
    print(f"  Ephemeral (lifespan 0): {ephemeral} ({ephemeral/n:.1%})")
    print(f"  Survived +1 step: {survived_1} ({survived_1/n:.1%})")
    print(f"  Survived +5 steps: {survived_5} ({survived_5/n:.1%})")
    print(f"  Continuously present: {continuous}")
    print(f"  Mean lifespan: {mean_life:.1f} steps")
    print(f"  Max lifespan: {max_life} steps")

    if n > 0 and survived_1 > 0:
        persistence_rate = survived_1 / n
        if persistence_rate > 0.5:
            print(f"  → SYNTHESIS DOMINANT: {persistence_rate:.0%} of novel content persists")
        elif persistence_rate > 0.2:
            print(f"  → MIXED: {persistence_rate:.0%} of novel content persists")
        else:
            print(f"  → NOISE DOMINANT: only {persistence_rate:.0%} of novel content persists")

    longest = sorted(events, key=lambda e: e["lifespan"], reverse=True)[:5]
    if longest and longest[0]["lifespan"] > 0:
        print(f"\n  Longest-lived novel {label}:")
        for e in longest:
            if e["lifespan"] > 0:
                cont = "continuous" if e["continuous"] else "intermittent"
                print(f"    '{e['item']}' — {e['lifespan']} steps, {e['appearances']} appearances, {cont}")

    return {
        "total_novel": n,
        "ephemeral": ephemeral,
        "survived_1": survived_1,
        "survived_5": survived_5,
        "continuous": continuous,
        "mean_lifespan": mean_life,
        "max_lifespan": max_life,
        "persistence_rate_1": survived_1 / n if n else 0,
        "persistence_rate_5": survived_5 / n if n else 0,
    }


def entity_churn_rate(history):
    """How many entities enter/exit per step?"""
    entries = []
    exits = []
    for i in range(1, len(history)):
        prev = extract_entities(history[i-1][1])
        curr = extract_entities(history[i][1])
        entered = curr - prev
        exited = prev - curr
        entries.append(len(entered))
        exits.append(len(exited))

    import numpy as np
    entries = np.array(entries)
    exits = np.array(exits)
    print(f"\n{'='*60}")
    print(f"  ENTITY CHURN PER STEP")
    print(f"{'='*60}")
    print(f"  Mean entries: {entries.mean():.2f}")
    print(f"  Mean exits: {exits.mean():.2f}")
    print(f"  Steps with zero churn: {((entries == 0) & (exits == 0)).sum()}")
    print(f"  Max entries in one step: {entries.max()}")
    print(f"  Max exits in one step: {exits.max()}")
    return {"mean_entries": float(entries.mean()), "mean_exits": float(exits.mean())}


def gist_evolution(history):
    """Track how the gist changes: wholesale replacement vs incremental edit."""
    from difflib import SequenceMatcher
    similarities = []
    for i in range(1, len(history)):
        g1 = history[i-1][1].get("semantic_gist", "")
        g2 = history[i][1].get("semantic_gist", "")
        sim = SequenceMatcher(None, g1, g2).ratio()
        similarities.append(sim)

    import numpy as np
    sims = np.array(similarities)
    print(f"\n{'='*60}")
    print(f"  GIST EVOLUTION (consecutive similarity)")
    print(f"{'='*60}")
    print(f"  Mean similarity: {sims.mean():.3f}")
    print(f"  Std: {sims.std():.3f}")
    print(f"  Min: {sims.min():.3f}")
    print(f"  Max: {sims.max():.3f}")
    print(f"  Steps with >90% similarity (preservative): {(sims > 0.9).sum()}")
    print(f"  Steps with <50% similarity (generative?): {(sims < 0.5).sum()}")

    big_changes = [(i, sims[i]) for i in range(len(sims)) if sims[i] < 0.5]
    if big_changes:
        print(f"\n  Big gist changes (potential generative events):")
        for idx, sim in big_changes[:5]:
            sid = history[idx+1][0]
            g_old = history[idx][1].get("semantic_gist", "")[:80]
            g_new = history[idx+1][1].get("semantic_gist", "")[:80]
            print(f"    CCS #{sid} (sim={sim:.3f}):")
            print(f"      FROM: {g_old}...")
            print(f"      TO:   {g_new}...")

    return {
        "mean_similarity": float(sims.mean()),
        "std": float(sims.std()),
        "preservative_steps": int((sims > 0.9).sum()),
        "generative_steps": int((sims < 0.5).sum()),
    }


def main():
    history = get_ccs_history()
    n = len(history)
    print(f"CCS history: {n} states (#{history[0][0]} to #{history[-1][0]})")

    entity_results = analyze(
        track_persistence(history, extract_entities, "entities"),
        "entities"
    )

    edge_results = analyze(
        track_persistence(history, extract_edges, "edges"),
        "relational edges"
    )

    bigram_results = analyze(
        track_persistence(history, lambda ccs: extract_gist_phrases(ccs.get("semantic_gist", "")),
                          "gist_bigrams"),
        "gist bigrams"
    )

    churn = entity_churn_rate(history)
    gist = gist_evolution(history)

    output = {
        "n_states": n,
        "entity_novelty": entity_results,
        "edge_novelty": edge_results,
        "gist_bigram_novelty": bigram_results,
        "entity_churn": churn,
        "gist_evolution": gist,
    }

    import os
    outpath = os.path.expanduser("~/chronicle/data/compression_novelty.json")
    with open(outpath, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved: {outpath}")


if __name__ == "__main__":
    main()
