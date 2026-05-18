#!/usr/bin/env python3
"""
Build #45c: Per-Field Causal Emergence Proxy

Tests whether the full CCS predicts its own future better than individual
fields predict theirs. The "excess" prediction from the whole vs. parts
= a proxy for integrated/emergent information.

Method:
  For each CCS field and for the full state:
    1. Embed the field/state at step t
    2. Embed the field/state at step t+1
    3. Measure cosine similarity (prediction = similarity, since high sim
       means the current state "predicts" the next one)
    4. Compare: does whole-state prediction exceed the best single-field
       prediction?

If whole > max(parts): the combination carries information that no single
field carries alone → emergence.

If whole ≈ max(parts): no integration, just parallel evolution.
"""

import json
import sqlite3
import urllib.request

import numpy as np

DB = "/mnt/hdd/chronicle-data/processed.db"
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"


def embed(text, timeout=60):
    if not text or not text.strip():
        return None
    payload = json.dumps({
        "model": "mxbai-embed-large",
        "prompt": text[:2000],
    }).encode()
    req = urllib.request.Request(
        EMBED_URL, data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return np.array(json.loads(r.read())["embedding"], dtype=np.float64)


def cosine(a, b):
    if a is None or b is None:
        return np.nan
    d = np.linalg.norm(a) * np.linalg.norm(b)
    if d == 0:
        return 0.0
    return float(np.dot(a, b) / d)


def field_text(state, field):
    """Extract text for a single CCS field."""
    if field == "gist":
        return state.get("semantic_gist", "")
    elif field == "goal":
        return state.get("goal_orientation", "")
    elif field == "entities":
        ents = state.get("focal_entities", [])
        return " ".join(
            f"{e.get('name', '')} {e.get('context', '')}"
            for e in ents if isinstance(e, dict)
        )
    elif field == "relational":
        rm = state.get("relational_map", {})
        return " ".join(f"{k}: {v}" for k, v in rm.items())
    elif field == "uncertainty":
        sigs = state.get("uncertainty_signals", [])
        return " ".join(
            s.get("description", "") for s in sigs if isinstance(s, dict)
        )
    elif field == "predictive":
        return state.get("predictive_cue", "")
    return ""


def full_text(state):
    parts = []
    for f in ["gist", "goal", "entities", "relational", "uncertainty", "predictive"]:
        t = field_text(state, f)
        if t:
            parts.append(t)
    return " ".join(parts)


def main():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot FROM cognitive_state_history ORDER BY id ASC"
    ).fetchall()
    db.close()

    states = []
    for rid, snap in rows:
        try:
            data = json.loads(snap)
            data["_id"] = rid
            states.append(data)
        except (json.JSONDecodeError, TypeError):
            continue

    n = len(states)
    print(f"Loaded {n} states")

    fields = ["gist", "goal", "entities", "relational", "uncertainty", "predictive"]

    # Embed all states for all fields + full
    print("Embedding all fields + full state...")
    field_embeds = {f: [] for f in fields}
    full_embeds = []

    for i, s in enumerate(states):
        for f in fields:
            text = field_text(s, f)
            field_embeds[f].append(embed(text) if text.strip() else None)
        full_embeds.append(embed(full_text(s)))
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{n}")

    # Step-to-step cosine similarity for each field and full state
    print("\nComputing step-to-step predictability...")
    field_sims = {f: [] for f in fields}
    full_sims = []

    for i in range(n - 1):
        full_sims.append(cosine(full_embeds[i], full_embeds[i + 1]))
        for f in fields:
            field_sims[f].append(cosine(field_embeds[f][i], field_embeds[f][i + 1]))

    # Summary statistics
    print(f"\n{'=' * 60}")
    print("STEP-TO-STEP PREDICTABILITY (cosine similarity t→t+1)")
    print(f"{'=' * 60}")
    print(f"{'Field':>15s}  {'Mean':>8s}  {'Std':>8s}  {'Min':>8s}  {'Max':>8s}")
    print("-" * 55)

    field_means = {}
    for f in fields:
        valid = [x for x in field_sims[f] if not np.isnan(x)]
        if valid:
            m = np.mean(valid)
            field_means[f] = m
            print(f"{f:>15s}  {m:8.4f}  {np.std(valid):8.4f}  {np.min(valid):8.4f}  {np.max(valid):8.4f}")
        else:
            field_means[f] = 0
            print(f"{f:>15s}  {'N/A':>8s}")

    valid_full = [x for x in full_sims if not np.isnan(x)]
    full_mean = np.mean(valid_full)
    print(f"{'FULL STATE':>15s}  {full_mean:8.4f}  {np.std(valid_full):8.4f}  {np.min(valid_full):8.4f}  {np.max(valid_full):8.4f}")

    # Emergence test
    best_field = max(field_means, key=field_means.get)
    best_field_mean = field_means[best_field]
    emergence_gap = full_mean - best_field_mean

    print(f"\n{'=' * 60}")
    print("EMERGENCE TEST")
    print(f"{'=' * 60}")
    print(f"  Full state predictability:  {full_mean:.4f}")
    print(f"  Best single field ({best_field}): {best_field_mean:.4f}")
    print(f"  Emergence gap:              {emergence_gap:+.4f}")

    if emergence_gap > 0.01:
        verdict = (f"INTEGRATED: Full state predicts its future {emergence_gap:.4f} "
                   f"better than the best single field. The combination carries "
                   f"information no individual field carries alone.")
    elif emergence_gap > -0.01:
        verdict = ("MARGINAL: Full state and best field are within 0.01. "
                   "Integration is weak or absent.")
    else:
        verdict = (f"NO EMERGENCE: Best single field ({best_field}) predicts "
                   f"better than the full state. No integration detected.")

    print(f"\n  Verdict: {verdict}")

    # Per-step emergence: at how many steps does whole > max(parts)?
    n_whole_wins = 0
    n_steps = 0
    for i in range(n - 1):
        f_val = full_sims[i]
        if np.isnan(f_val):
            continue
        part_vals = [field_sims[f][i] for f in fields if not np.isnan(field_sims[f][i])]
        if not part_vals:
            continue
        n_steps += 1
        if f_val > max(part_vals):
            n_whole_wins += 1

    pct = n_whole_wins / max(n_steps, 1) * 100
    print(f"\n  Whole > max(parts) at {n_whole_wins}/{n_steps} steps ({pct:.1f}%)")

    # Phase-specific analysis (from Build #43 changepoints at 53 and 94)
    print(f"\n{'=' * 60}")
    print("PHASE-SPECIFIC EMERGENCE (Build #43 phases)")
    print(f"{'=' * 60}")

    phases = [
        ("Phase 1 (1-52)", 0, 52),
        ("Phase 2 (53-93)", 52, 93),
        ("Phase 3 (94+)", 93, n),
    ]

    for name, start, end in phases:
        phase_full = [full_sims[i] for i in range(start, min(end, n-1))
                      if not np.isnan(full_sims[i])]
        if not phase_full:
            print(f"  {name}: no data")
            continue
        phase_full_mean = np.mean(phase_full)

        phase_field_means = {}
        for f in fields:
            pf = [field_sims[f][i] for i in range(start, min(end, n-1))
                  if not np.isnan(field_sims[f][i])]
            phase_field_means[f] = np.mean(pf) if pf else 0

        best_pf = max(phase_field_means, key=phase_field_means.get)
        gap = phase_full_mean - phase_field_means[best_pf]
        print(f"  {name}: full={phase_full_mean:.4f}, best_field={phase_field_means[best_pf]:.4f} ({best_pf}), gap={gap:+.4f}")

    # Save
    results = {
        "build": "45c",
        "n_states": n,
        "field_predictability": {f: float(field_means[f]) for f in fields},
        "full_predictability": float(full_mean),
        "best_field": best_field,
        "emergence_gap": float(emergence_gap),
        "whole_beats_parts_pct": float(pct),
        "verdict": verdict,
    }
    out = "/home/nate-agx/chronicle/data/build45c_emergence_proxy.json"
    with open(out, "w") as fp:
        json.dump(results, fp, indent=2)
    print(f"\n  Results saved to {out}")


if __name__ == "__main__":
    main()
