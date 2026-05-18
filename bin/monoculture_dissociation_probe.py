#!/usr/bin/env python3
"""
Build #48: Monoculture Dissociation Probe

Thread #145 (from capsule retrieval) warns that three-view convergence
(Bennett depth + causal emergence + dimensional redistribution) could be
methodological monoculture rather than structural truth. All three views
derive from the same CCS embedding pipeline.

This probe tests whether the three views can DISSOCIATE — whether Bennett
depth can accumulate without dimensional redistribution occurring, and
vice versa. If they dissociate in at least some state transitions, the
convergence is structural. If they always co-occur, monoculture risk is real.

Method:
  For each state transition t → t+1:
    - Bennett depth proxy: change in gzip complexity ratio
    - Dimensional redistribution proxy: change in PCA variance spread
    - Causal emergence proxy: step-to-step cosine (internal change rate)
  Then test: do the three measures correlate perfectly, or can they dissociate?
"""

import gzip
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


def state_text(state):
    parts = []
    for field in ["semantic_gist", "goal_orientation", "predictive_cue"]:
        v = state.get(field, "")
        if isinstance(v, str):
            parts.append(v)
    for e in state.get("focal_entities", []):
        if isinstance(e, dict):
            parts.append(f"{e.get('name', '')} {e.get('context', '')}")
    for k, v in state.get("relational_map", {}).items():
        parts.append(f"{k}: {v}")
    for u in state.get("uncertainty_signals", []):
        if isinstance(u, dict):
            parts.append(u.get("description", ""))
    for c in state.get("constraints", []):
        if isinstance(c, str):
            parts.append(c)
    et = state.get("episodic_trace", [])
    if isinstance(et, list):
        for e in et:
            if isinstance(e, str):
                parts.append(e)
    return " ".join(parts)


def gzip_complexity(text):
    """Bennett depth proxy: ratio of compressed to uncompressed size."""
    raw = text.encode("utf-8")
    compressed = gzip.compress(raw)
    return len(compressed) / len(raw) if len(raw) > 0 else 1.0


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

    # Compute per-state metrics
    print("Computing per-state metrics...")
    texts = [state_text(s) for s in states]
    complexities = [gzip_complexity(t) for t in texts]

    print("Embedding all states...")
    embeddings = []
    for i, t in enumerate(texts):
        embeddings.append(embed(t))
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{n}")
    embeddings = np.array(embeddings)

    # PCA for dimensional redistribution
    print("Computing PCA...")
    mean_emb = embeddings.mean(axis=0)
    centered = embeddings - mean_emb
    # Use SVD for efficiency
    U, S, Vt = np.linalg.svd(centered, full_matrices=False)
    projected = centered @ Vt[:5].T  # first 5 PCs

    # Per-transition metrics
    window = 10  # rolling window for variance spread
    bennett_deltas = []
    redistribution_deltas = []
    emergence_sims = []

    for i in range(window, n - 1):
        # Bennett depth proxy: change in gzip complexity
        bd = complexities[i + 1] - complexities[i]
        bennett_deltas.append(bd)

        # Dimensional redistribution proxy: change in variance spread
        # Variance of each PC in rolling window before vs after this step
        window_before = projected[i - window:i]
        window_after = projected[i - window + 1:i + 1]
        var_before = np.var(window_before, axis=0)
        var_after = np.var(window_after, axis=0)
        # Redistribution = change in how evenly variance is spread across PCs
        # Entropy of normalized variance
        def var_entropy(v):
            v = np.abs(v) + 1e-10
            p = v / v.sum()
            return -np.sum(p * np.log(p))
        rd = var_entropy(var_after) - var_entropy(var_before)
        redistribution_deltas.append(rd)

        # Causal emergence proxy: step-to-step cosine distance
        ce = 1.0 - cosine(embeddings[i], embeddings[i + 1])
        emergence_sims.append(ce)

    bd = np.array(bennett_deltas)
    rd = np.array(redistribution_deltas)
    ce = np.array(emergence_sims)

    print(f"\nTransitions analyzed: {len(bd)} (states {window+1} to {n})")
    print(f"Bennett delta: mean={bd.mean():.6f}, std={bd.std():.6f}")
    print(f"Redistribution delta: mean={rd.mean():.6f}, std={rd.std():.6f}")
    print(f"Emergence distance: mean={ce.mean():.4f}, std={ce.std():.4f}")

    # Correlation matrix
    print(f"\n--- Pairwise Correlations ---")
    r_bd_rd = np.corrcoef(bd, rd)[0, 1] if bd.std() > 0 and rd.std() > 0 else 0
    r_bd_ce = np.corrcoef(bd, ce)[0, 1] if bd.std() > 0 and ce.std() > 0 else 0
    r_rd_ce = np.corrcoef(rd, ce)[0, 1] if rd.std() > 0 and ce.std() > 0 else 0

    print(f"r(Bennett, Redistribution)  = {r_bd_rd:.4f}")
    print(f"r(Bennett, Emergence)       = {r_bd_ce:.4f}")
    print(f"r(Redistribution, Emergence) = {r_rd_ce:.4f}")

    # Dissociation test
    print(f"\n--- Dissociation Test ---")
    # Find transitions where one measure is high but another is low
    bd_high = bd > np.percentile(bd, 75)
    bd_low = bd < np.percentile(bd, 25)
    rd_high = rd > np.percentile(rd, 75)
    rd_low = rd < np.percentile(rd, 25)
    ce_high = ce > np.percentile(ce, 75)
    ce_low = ce < np.percentile(ce, 25)

    dissociations = {
        "Bennett HIGH + Redistribution LOW": int((bd_high & rd_low).sum()),
        "Bennett LOW + Redistribution HIGH": int((bd_low & rd_high).sum()),
        "Bennett HIGH + Emergence LOW": int((bd_high & ce_low).sum()),
        "Bennett LOW + Emergence HIGH": int((bd_low & ce_high).sum()),
        "Redistribution HIGH + Emergence LOW": int((rd_high & ce_low).sum()),
        "Redistribution LOW + Emergence HIGH": int((rd_low & ce_high).sum()),
    }

    total_dissociations = sum(dissociations.values())
    total_possible = len(bd) * 6  # crude upper bound

    for label, count in dissociations.items():
        pct = count / len(bd) * 100
        print(f"  {label}: {count} ({pct:.1f}%)")

    print(f"\nTotal dissociation events: {total_dissociations}")

    # Verdict
    print(f"\n--- Verdict ---")
    max_r = max(abs(r_bd_rd), abs(r_bd_ce), abs(r_rd_ce))
    if max_r > 0.7:
        print(f"MONOCULTURE RISK: max pairwise r = {max_r:.3f}")
        print("The three views may be measuring the same underlying signal.")
        verdict = "monoculture_risk"
    elif max_r < 0.3:
        print(f"DISSOCIATION CONFIRMED: max pairwise r = {max_r:.3f}")
        print("The three views capture genuinely independent aspects.")
        print("Three-view convergence is structural, not methodological artifact.")
        verdict = "dissociation_confirmed"
    else:
        print(f"PARTIAL INDEPENDENCE: max pairwise r = {max_r:.3f}")
        print("Views are somewhat correlated but not redundant.")
        verdict = "partial_independence"

    if total_dissociations > len(bd) * 0.1:
        print(f"  Strong evidence: {total_dissociations} dissociation events "
              f"({total_dissociations/len(bd)*100:.1f}% of transitions)")

    # Save results
    results = {
        "n_states": n,
        "n_transitions": len(bd),
        "correlations": {
            "bennett_redistribution": float(r_bd_rd),
            "bennett_emergence": float(r_bd_ce),
            "redistribution_emergence": float(r_rd_ce),
        },
        "dissociations": dissociations,
        "total_dissociations": total_dissociations,
        "bennett_stats": {"mean": float(bd.mean()), "std": float(bd.std())},
        "redistribution_stats": {"mean": float(rd.mean()), "std": float(rd.std())},
        "emergence_stats": {"mean": float(ce.mean()), "std": float(ce.std())},
        "verdict": verdict,
    }

    out = "/home/nate-agx/chronicle/data/build48_monoculture_dissociation.json"
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {out}")


if __name__ == "__main__":
    main()
