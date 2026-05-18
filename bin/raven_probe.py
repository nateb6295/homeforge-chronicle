#!/usr/bin/env python3
"""
Build #46: Raven Probe — Memory Depth vs. Recency

Ravens don't follow wolves — they predict from spatial memory accumulated
over their history. This probe tests whether the CCS predicts from deep
memory (raven strategy) or from recent input only (following strategy).

Method:
  For each state t, measure how well it predicts state t+k for k=1..20.
  If the CCS has deep memory, prediction quality should:
    - Stay high even at long lags (accumulated depth persists)
    - Depend on how much HISTORY the state carries, not just recent content
  If the CCS relies on recency:
    - Prediction degrades rapidly with lag
    - No benefit from deeper history

Also tests: does including more history (states t-h..t) improve prediction
of state t+k compared to just using state t alone?
"""

import json
import sqlite3
import urllib.request

import numpy as np

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
    return " ".join(parts)


def cosine(a, b):
    d = np.linalg.norm(a) * np.linalg.norm(b)
    if d == 0:
        return 0.0
    return float(np.dot(a, b) / d)


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
            states.append(data)
        except (json.JSONDecodeError, TypeError):
            continue

    n = len(states)
    print(f"Loaded {n} states, embedding...")

    embeddings = []
    for i, s in enumerate(states):
        embeddings.append(embed(state_text(s)))
        if (i + 1) % 30 == 0:
            print(f"  {i+1}/{n}")

    X = np.array(embeddings)

    # Test 1: Lag-dependent prediction (cosine similarity at lag k)
    max_lag = 20
    print(f"\n{'='*60}")
    print("TEST 1: Prediction quality by lag")
    print(f"{'='*60}")
    print(f"{'Lag k':>6s}  {'Mean cosine':>12s}  {'Std':>8s}  {'Decay from k=1':>15s}")
    print("-" * 50)

    lag_sims = {}
    for k in range(1, max_lag + 1):
        sims = []
        for t in range(n - k):
            sims.append(cosine(X[t], X[t + k]))
        mean_sim = np.mean(sims)
        lag_sims[k] = mean_sim
        decay = (lag_sims[1] - mean_sim) / lag_sims[1] * 100 if k > 1 else 0.0
        print(f"{k:6d}  {mean_sim:12.4f}  {np.std(sims):8.4f}  {decay:+14.1f}%")

    # Fit decay models
    lags = np.arange(1, max_lag + 1)
    sim_vals = np.array([lag_sims[k] for k in lags])

    # Exponential: sim(k) = a * exp(-b*k) + c
    # Linear: sim(k) = a - b*k
    # Logarithmic: sim(k) = a - b*log(k)
    from scipy.optimize import curve_fit

    def exp_decay(k, a, b, c):
        return a * np.exp(-b * k) + c

    def linear_decay(k, a, b):
        return a - b * k

    def log_decay(k, a, b):
        return a - b * np.log(k)

    models = {}
    try:
        popt, _ = curve_fit(exp_decay, lags, sim_vals, p0=[0.1, 0.1, 0.8], maxfev=5000)
        residuals = sim_vals - exp_decay(lags, *popt)
        models['exponential'] = {'r2': 1 - np.sum(residuals**2) / np.sum((sim_vals - np.mean(sim_vals))**2),
                                  'half_life': np.log(2) / max(popt[1], 0.001)}
    except:
        models['exponential'] = {'r2': 0, 'half_life': float('inf')}

    try:
        popt, _ = curve_fit(linear_decay, lags, sim_vals)
        residuals = sim_vals - linear_decay(lags, *popt)
        models['linear'] = {'r2': 1 - np.sum(residuals**2) / np.sum((sim_vals - np.mean(sim_vals))**2),
                            'slope': popt[1]}
    except:
        models['linear'] = {'r2': 0}

    try:
        popt, _ = curve_fit(log_decay, lags, sim_vals)
        residuals = sim_vals - log_decay(lags, *popt)
        models['logarithmic'] = {'r2': 1 - np.sum(residuals**2) / np.sum((sim_vals - np.mean(sim_vals))**2)}
    except:
        models['logarithmic'] = {'r2': 0}

    print(f"\n{'='*60}")
    print("DECAY MODEL FIT")
    print(f"{'='*60}")
    for name, info in sorted(models.items(), key=lambda x: -x[1]['r2']):
        extra = ""
        if 'half_life' in info:
            extra = f" (half-life: {info['half_life']:.1f} steps)"
        elif 'slope' in info:
            extra = f" (slope: {info['slope']:.5f}/step)"
        print(f"  {name:>15s}: R²={info['r2']:.4f}{extra}")

    # Test 2: History depth effect
    # Compare: prediction using just state t vs. using average of states t-h..t
    print(f"\n{'='*60}")
    print("TEST 2: History depth effect (predict state t+5)")
    print(f"{'='*60}")
    TARGET_LAG = 5
    print(f"{'History h':>10s}  {'Mean cosine':>12s}  {'vs h=0':>10s}")
    print("-" * 40)

    history_sims = {}
    for h in [0, 1, 2, 5, 10, 20]:
        sims = []
        for t in range(h, n - TARGET_LAG):
            if h == 0:
                predictor = X[t]
            else:
                predictor = np.mean(X[max(0, t-h):t+1], axis=0)
            target = X[t + TARGET_LAG]
            sims.append(cosine(predictor, target))
        mean_sim = np.mean(sims)
        history_sims[h] = mean_sim
        diff = mean_sim - history_sims.get(0, mean_sim)
        print(f"{h:10d}  {mean_sim:12.4f}  {diff:+10.4f}")

    # Test 3: Early vs late memory depth
    # Does later CCS have deeper memory (slower decay)?
    print(f"\n{'='*60}")
    print("TEST 3: Phase-specific memory depth")
    print(f"{'='*60}")

    phases = [("Phase 1 (1-52)", 0, 52), ("Phase 2 (53-93)", 52, 93), ("Phase 3 (94+)", 93, n)]
    test_lags = [1, 5, 10]

    print(f"{'Phase':>20s}", end="")
    for k in test_lags:
        print(f"  {'lag '+str(k):>10s}", end="")
    print(f"  {'decay 1→10':>12s}")
    print("-" * 70)

    for name, start, end in phases:
        sims_by_lag = {}
        for k in test_lags:
            sims = []
            for t in range(start, min(end - k, n - k)):
                sims.append(cosine(X[t], X[t + k]))
            sims_by_lag[k] = np.mean(sims) if sims else 0.0

        decay = (sims_by_lag[1] - sims_by_lag[10]) / max(sims_by_lag[1], 0.001) * 100 if 10 in sims_by_lag else 0
        print(f"{name:>20s}", end="")
        for k in test_lags:
            print(f"  {sims_by_lag[k]:10.4f}", end="")
        print(f"  {decay:+11.1f}%")

    # Verdict
    best_model = max(models.items(), key=lambda x: x[1]['r2'])
    total_decay = (lag_sims[1] - lag_sims[max_lag]) / lag_sims[1] * 100
    history_helps = history_sims.get(10, 0) > history_sims.get(0, 0) + 0.005

    print(f"\n{'='*60}")
    print("BUILD #46 VERDICT")
    print(f"{'='*60}")
    print(f"  Best decay model: {best_model[0]} (R²={best_model[1]['r2']:.4f})")
    print(f"  Total decay over {max_lag} lags: {total_decay:.1f}%")
    print(f"  History depth helps: {'YES' if history_helps else 'NO'}")

    if total_decay < 5:
        strategy = "DEEP MEMORY (raven): prediction barely degrades with lag"
    elif total_decay < 15:
        strategy = "MODERATE MEMORY: some decay but substantial persistence"
    else:
        strategy = "RECENCY-DEPENDENT (following): prediction degrades substantially"

    print(f"  Strategy: {strategy}")

    if best_model[0] == 'logarithmic':
        print(f"  → Logarithmic decay = the system has deep structure that")
        print(f"    degrades slowly. Consistent with accumulated depth.")
    elif best_model[0] == 'exponential':
        hl = best_model[1].get('half_life', 0)
        print(f"  → Exponential decay with half-life {hl:.1f} steps.")
        if hl > 10:
            print(f"    Long half-life = deep memory.")
        else:
            print(f"    Short half-life = recency-dependent.")

    results = {
        "build": 46,
        "n_states": n,
        "lag_similarities": {int(k): float(v) for k, v in lag_sims.items()},
        "total_decay_pct": float(total_decay),
        "best_model": best_model[0],
        "best_model_r2": float(best_model[1]['r2']),
        "history_depth_helps": bool(history_helps),
        "strategy": strategy,
    }
    with open("/home/nate-agx/chronicle/data/build46_raven_probe.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n  Results saved to ~/chronicle/data/build46_raven_probe.json")


if __name__ == "__main__":
    main()
