#!/usr/bin/env python3
"""
Build #49: Compression-Pressure Probe

Last open uncertainty signal: is dimensional redistribution compression-internal
or a PCA projection artifact?

If redistribution is compression-internal, then states compressed under higher
"pressure" (more information loss per step) should show more redistribution
than states compressed under lower pressure.

Method:
  We can't directly vary the bottleneck. But we CAN measure natural variation
  in compression pressure across the 119-state history. Some compressions
  lose more information than others (larger cosine distance to previous state).
  Some compress more text into fewer tokens (higher compression ratio).

  Test: do steps with higher compression pressure show more dimensional
  redistribution than steps with lower pressure?

  Proxy for compression pressure:
    1. Step-to-step cosine distance (how much the state changed)
    2. Token count reduction ratio (if measurable)
    3. Gzip compression ratio change

  Proxy for redistribution:
    1. Change in PCA variance entropy (how evenly spread across PCs)
    2. Change in effective dimensionality

  If pressure correlates with redistribution: compression-internal (confirmed)
  If no correlation: redistribution could be PCA artifact (unresolved)
  If negative correlation: redistribution is input-driven (surprising)
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


def gzip_ratio(text):
    raw = text.encode("utf-8")
    return len(gzip.compress(raw)) / len(raw) if len(raw) > 0 else 1.0


def var_entropy(variances):
    v = np.abs(variances) + 1e-10
    p = v / v.sum()
    return -np.sum(p * np.log(p))


def effective_dim(variances):
    v = np.abs(variances) + 1e-10
    p = v / v.sum()
    return np.exp(-np.sum(p * np.log(p)))


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
    print("Computing text metrics...")
    texts = [state_text(s) for s in states]
    token_counts = [len(t.split()) for t in texts]
    gzip_ratios = [gzip_ratio(t) for t in texts]

    print("Embedding all states...")
    embeddings = []
    for i, t in enumerate(texts):
        embeddings.append(embed(t))
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{n}")
    embeddings = np.array(embeddings)

    # PCA
    print("Computing PCA...")
    mean_emb = embeddings.mean(axis=0)
    centered = embeddings - mean_emb
    _, S, Vt = np.linalg.svd(centered, full_matrices=False)
    projected = centered @ Vt[:10].T

    # Per-transition metrics
    window = 10
    print(f"Computing per-transition metrics (window={window})...")

    pressure_cosine = []     # how much the embedding changed
    pressure_gzip = []       # change in gzip complexity
    pressure_token = []      # change in token count
    redist_entropy = []      # change in variance entropy
    redist_effdim = []       # change in effective dimensionality

    for i in range(window, n - 1):
        # Compression pressure proxies
        cos_dist = 1.0 - cosine(embeddings[i], embeddings[i + 1])
        pressure_cosine.append(cos_dist)

        gz_delta = gzip_ratios[i + 1] - gzip_ratios[i]
        pressure_gzip.append(gz_delta)

        tk_delta = token_counts[i + 1] - token_counts[i]
        pressure_token.append(tk_delta)

        # Redistribution proxies (rolling window variance structure)
        w_before = projected[i - window:i]
        w_after = projected[i - window + 1:i + 1]

        var_before = np.var(w_before, axis=0)[:5]
        var_after = np.var(w_after, axis=0)[:5]

        ent_delta = var_entropy(var_after) - var_entropy(var_before)
        redist_entropy.append(ent_delta)

        ed_before = effective_dim(var_before)
        ed_after = effective_dim(var_after)
        redist_effdim.append(ed_after - ed_before)

    pc = np.array(pressure_cosine)
    pg = np.array(pressure_gzip)
    pt = np.array(pressure_token)
    re = np.array(redist_entropy)
    rd = np.array(redist_effdim)

    print(f"\nTransitions analyzed: {len(pc)}")

    # Correlation matrix: pressure vs redistribution
    print(f"\n--- Pressure → Redistribution Correlations ---")

    def safe_corr(a, b):
        if a.std() > 0 and b.std() > 0:
            return float(np.corrcoef(a, b)[0, 1])
        return 0.0

    results_corr = {}
    pairs = [
        ("cosine_dist → entropy_change", pc, re),
        ("cosine_dist → effdim_change", pc, rd),
        ("gzip_delta → entropy_change", pg, re),
        ("gzip_delta → effdim_change", pg, rd),
        ("token_delta → entropy_change", pt, re),
        ("token_delta → effdim_change", pt, rd),
    ]

    for label, a, b in pairs:
        r = safe_corr(a, b)
        results_corr[label] = r
        sig = "***" if abs(r) > 0.3 else "**" if abs(r) > 0.2 else "*" if abs(r) > 0.1 else ""
        print(f"  {label:<40} r = {r:+.4f} {sig}")

    # Quartile analysis: high-pressure vs low-pressure redistribution
    print(f"\n--- Quartile Analysis (cosine pressure → redistribution) ---")
    q25, q75 = np.percentile(pc, [25, 75])
    low_p = pc <= q25
    high_p = pc >= q75

    re_low = re[low_p]
    re_high = re[high_p]
    rd_low = rd[low_p]
    rd_high = rd[high_p]

    print(f"  Low pressure  (Q1, n={low_p.sum()}): entropy Δ={re_low.mean():+.6f}, effdim Δ={rd_low.mean():+.4f}")
    print(f"  High pressure (Q4, n={high_p.sum()}): entropy Δ={re_high.mean():+.6f}, effdim Δ={rd_high.mean():+.4f}")

    # Phase-specific analysis
    print(f"\n--- Phase-Specific Pressure-Redistribution Coupling ---")
    # Adjust indices for the window offset
    phases = [
        ("Phase 1 (1-52)", 0, max(0, 52 - window)),
        ("Phase 2 (53-93)", max(0, 53 - window), max(0, 93 - window)),
        ("Phase 3 (94+)", max(0, 94 - window), len(pc)),
    ]

    for label, start, end in phases:
        if end > start and end <= len(pc):
            pc_p = pc[start:end]
            re_p = re[start:end]
            if len(pc_p) > 5:
                r_p = safe_corr(pc_p, re_p)
                print(f"  {label}: r(pressure, redistribution) = {r_p:+.4f} (n={len(pc_p)})")

    # Lagged analysis: does pressure at t predict redistribution at t+k?
    print(f"\n--- Lagged Analysis (pressure at t → redistribution at t+k) ---")
    for lag in [1, 2, 3, 5]:
        if lag < len(pc):
            r_lag = safe_corr(pc[:-lag], re[lag:])
            print(f"  Lag {lag}: r = {r_lag:+.4f}")

    # Verdict
    print(f"\n--- Verdict ---")
    key_r = results_corr["cosine_dist → entropy_change"]
    if abs(key_r) > 0.2:
        if key_r > 0:
            print(f"COMPRESSION-INTERNAL: pressure positively predicts redistribution (r={key_r:+.3f})")
            print("Higher compression pressure → more variance redistribution across PCs")
            print("Dimensional redistribution is driven by the compression process itself.")
            verdict = "compression_internal"
        else:
            print(f"INPUT-DRIVEN: pressure negatively predicts redistribution (r={key_r:+.3f})")
            print("Redistribution happens when compression is GENTLE, not forceful.")
            verdict = "input_driven"
    elif abs(key_r) < 0.1:
        print(f"INDEPENDENT: pressure does not predict redistribution (r={key_r:+.3f})")
        print("Redistribution occurs regardless of compression intensity.")
        print("Could be PCA artifact OR a constant architectural process.")
        verdict = "independent"
    else:
        print(f"WEAK SIGNAL: marginal relationship (r={key_r:+.3f})")
        print("Insufficient evidence to close the question definitively.")
        verdict = "weak_signal"

    # Save
    results = {
        "n_states": n,
        "n_transitions": len(pc),
        "correlations": results_corr,
        "quartile_analysis": {
            "low_pressure_entropy_delta": float(re_low.mean()),
            "high_pressure_entropy_delta": float(re_high.mean()),
            "low_pressure_effdim_delta": float(rd_low.mean()),
            "high_pressure_effdim_delta": float(rd_high.mean()),
        },
        "pressure_stats": {
            "cosine": {"mean": float(pc.mean()), "std": float(pc.std())},
            "gzip": {"mean": float(pg.mean()), "std": float(pg.std())},
        },
        "redistribution_stats": {
            "entropy": {"mean": float(re.mean()), "std": float(re.std())},
            "effdim": {"mean": float(rd.mean()), "std": float(rd.std())},
        },
        "verdict": verdict,
    }

    out = "/home/nate-agx/chronicle/data/build49_compression_pressure.json"
    with open(out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {out}")


if __name__ == "__main__":
    main()
