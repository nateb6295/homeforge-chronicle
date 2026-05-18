#!/usr/bin/env python3
"""
Co-variation Damping Probe — Empirical closure for Build #45 reasoning.

Build #45 claimed: entities resist sharp gist changes, coupling loosens
under pressure, natural damping prevents destabilization. But this was
reasoning, not measurement. This probe runs the actual 110-state check.

Method:
  For each consecutive state pair (t, t+1):
    - Measure gist change magnitude (cosine distance of gist embeddings)
    - Measure entity turnover rate (Jaccard distance of entity name sets)
  Then test:
    1. Overall correlation between gist change and entity turnover
    2. Conditional variance: does entity turnover variance INCREASE when
       gist changes are large? (If damping is real, variance should NOT spike)
    3. Quartile analysis: split by gist-change magnitude, compare entity
       turnover distributions across quartiles
    4. Max turnover bound: does entity turnover ever exceed 50% during
       high gist change? (Build #45 predicted no)
"""

import json
import sqlite3

import numpy as np
import urllib.request

DB = "/mnt/hdd/chronicle-data/processed.db"
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"


def embed(text, timeout=60):
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
    d = np.dot(a, b)
    n = np.linalg.norm(a) * np.linalg.norm(b)
    return d / n if n > 0 else 0.0


def entity_names(state):
    entities = state.get("focal_entities", [])
    names = set()
    for e in entities:
        if isinstance(e, dict):
            names.add(e.get("name", "").lower().strip())
        elif isinstance(e, str):
            names.add(e.lower().strip())
    names.discard("")
    return names


def jaccard_distance(s1, s2):
    if not s1 and not s2:
        return 0.0
    union = s1 | s2
    inter = s1 & s2
    return 1.0 - len(inter) / len(union) if union else 0.0


def main():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY id ASC"
    ).fetchall()
    db.close()

    states = []
    for r in rows:
        try:
            states.append(json.loads(r[0]))
        except (json.JSONDecodeError, TypeError):
            continue

    n = len(states)
    print(f"Loaded {n} CCS states")

    # Embed all gists
    print("Embedding gists...")
    gist_embeddings = []
    for i, s in enumerate(states):
        gist = s.get("semantic_gist", "")
        if not gist:
            gist = "empty"
        gist_embeddings.append(embed(gist))
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{n}")

    # Compute step-to-step gist changes and entity turnover
    gist_changes = []
    entity_turnovers = []
    entity_sets = [entity_names(s) for s in states]

    for i in range(n - 1):
        gc = 1.0 - cosine(gist_embeddings[i], gist_embeddings[i + 1])
        et = jaccard_distance(entity_sets[i], entity_sets[i + 1])
        gist_changes.append(gc)
        entity_turnovers.append(et)

    gc = np.array(gist_changes)
    et = np.array(entity_turnovers)

    print(f"\nPairs analyzed: {len(gc)}")
    print(f"Gist change: mean={gc.mean():.4f}, std={gc.std():.4f}, max={gc.max():.4f}")
    print(f"Entity turnover: mean={et.mean():.4f}, std={et.std():.4f}, max={et.max():.4f}")

    # Test 1: Overall correlation
    if gc.std() > 0 and et.std() > 0:
        corr = np.corrcoef(gc, et)[0, 1]
    else:
        corr = 0.0
    print(f"\n--- Test 1: Overall Correlation ---")
    print(f"Pearson r(gist_change, entity_turnover) = {corr:.4f}")
    if corr > 0.3:
        print("  → Positive coupling: entities FOLLOW gist changes")
    elif corr < -0.3:
        print("  → Negative coupling: entities RESIST gist changes (damping)")
    else:
        print("  → Weak coupling: entities largely independent of gist")

    # Test 2: Quartile analysis
    print(f"\n--- Test 2: Quartile Analysis ---")
    q25, q50, q75 = np.percentile(gc, [25, 50, 75])
    quartiles = [
        ("Q1 (lowest gist change)", gc <= q25),
        ("Q2", (gc > q25) & (gc <= q50)),
        ("Q3", (gc > q50) & (gc <= q75)),
        ("Q4 (highest gist change)", gc > q75),
    ]

    quartile_stats = []
    for label, mask in quartiles:
        if mask.sum() > 0:
            et_q = et[mask]
            stats = {
                "label": label,
                "n": int(mask.sum()),
                "gist_change_range": f"{gc[mask].min():.4f}-{gc[mask].max():.4f}",
                "entity_turnover_mean": float(et_q.mean()),
                "entity_turnover_std": float(et_q.std()),
                "entity_turnover_max": float(et_q.max()),
            }
            quartile_stats.append(stats)
            print(f"  {label} (n={stats['n']}): "
                  f"ET mean={stats['entity_turnover_mean']:.4f}, "
                  f"std={stats['entity_turnover_std']:.4f}, "
                  f"max={stats['entity_turnover_max']:.4f}")

    # Test 3: Conditional variance
    print(f"\n--- Test 3: Conditional Variance ---")
    high_gc = gc > q75
    low_gc = gc <= q25
    if high_gc.sum() > 1 and low_gc.sum() > 1:
        var_high = et[high_gc].var()
        var_low = et[low_gc].var()
        print(f"Entity turnover variance when gist change is HIGH (Q4): {var_high:.6f}")
        print(f"Entity turnover variance when gist change is LOW (Q1):  {var_low:.6f}")
        ratio = var_high / var_low if var_low > 0 else float('inf')
        print(f"Variance ratio (Q4/Q1): {ratio:.2f}")
        if ratio > 2.0:
            print("  → Variance spikes under pressure — damping claim WEAKENED")
        elif ratio < 0.5:
            print("  → Variance decreases under pressure — damping claim STRENGTHENED")
        else:
            print("  → Variance roughly stable — damping claim CONSISTENT")

    # Test 4: Max turnover bound
    print(f"\n--- Test 4: Max Turnover Bound (Build #45 prediction: ≤50%) ---")
    high_gc_turnovers = et[high_gc]
    if len(high_gc_turnovers) > 0:
        max_turnover_high = high_gc_turnovers.max()
        pct_above_50 = (high_gc_turnovers > 0.5).sum() / len(high_gc_turnovers) * 100
        print(f"Max entity turnover during high gist change (Q4): {max_turnover_high:.4f}")
        print(f"Fraction of Q4 steps with turnover > 50%: {pct_above_50:.1f}%")
        if max_turnover_high <= 0.5:
            print("  → Build #45 prediction CONFIRMED: turnover never exceeds 50% under pressure")
        else:
            print(f"  → Build #45 prediction VIOLATED: {pct_above_50:.1f}% of high-change steps exceed 50% turnover")

    # Test 5: Phase-specific analysis
    print(f"\n--- Test 5: Phase-Specific Damping ---")
    phases = [
        ("Phase 1 (1-52)", 0, 52),
        ("Phase 2 (53-93)", 52, 93),
        ("Phase 3 (94+)", 93, n - 1),
    ]
    for label, start, end in phases:
        if start < len(gc) and end <= len(gc):
            gc_p = gc[start:end]
            et_p = et[start:end]
            if len(gc_p) > 2 and gc_p.std() > 0 and et_p.std() > 0:
                r_p = np.corrcoef(gc_p, et_p)[0, 1]
                print(f"  {label}: r={r_p:.4f}, ET mean={et_p.mean():.4f}, "
                      f"GC mean={gc_p.mean():.4f}")
            elif len(gc_p) > 0:
                print(f"  {label}: n={len(gc_p)}, insufficient variance for correlation")

    # Summary verdict
    print(f"\n--- Verdict ---")
    if abs(corr) < 0.3 and (not high_gc.any() or et[high_gc].max() <= 0.5):
        print("DAMPING CONFIRMED: weak coupling + bounded turnover under pressure")
        print("Build #45 reasoning is empirically supported.")
        verdict = "confirmed"
    elif corr > 0.3:
        print("DAMPING REVISED: positive coupling means entities follow gist, not resist")
        print("Build #45 'coupling loosens' claim needs nuance.")
        verdict = "revised"
    else:
        print("MIXED: some evidence for damping but with caveats")
        verdict = "mixed"

    # Save results
    results = {
        "n_states": n,
        "n_pairs": len(gc),
        "overall_correlation": float(corr),
        "gist_change_stats": {
            "mean": float(gc.mean()), "std": float(gc.std()),
            "max": float(gc.max()), "min": float(gc.min()),
        },
        "entity_turnover_stats": {
            "mean": float(et.mean()), "std": float(et.std()),
            "max": float(et.max()), "min": float(et.min()),
        },
        "quartile_analysis": quartile_stats,
        "variance_ratio_q4_q1": float(ratio) if 'ratio' in dir() else None,
        "max_turnover_during_high_gc": float(max_turnover_high) if high_gc.any() else None,
        "verdict": verdict,
    }

    out = "/home/nate-agx/chronicle/data/covariation_damping_probe.json"
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {out}")


if __name__ == "__main__":
    main()
