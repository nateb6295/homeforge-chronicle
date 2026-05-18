#!/usr/bin/env python3
"""
Noether Falsification Probe — Is the Hamiltonian parasitic on Bennett?

Build #50c found H = 1.3B + 0.9R + 0.6E with CV=0.19. But Bennett barely
moves (0.7-1.2% deviation) and has the highest weight. The "conservation"
might just be Bennett stability in disguise.

Test: condition on Bennett high-variance windows. If H's CV increases
substantially, the conservation is parasitic. If H stays tight, the
axes genuinely compensate.

Also tests: can we construct synthetic perturbations that preserve H
by construction but break individual axis conservation?
"""

import gzip
import json
import sqlite3
import numpy as np
from scipy import stats

DB = "/mnt/hdd/chronicle-data/processed.db"


def load_states():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY id ASC"
    ).fetchall()
    db.close()
    states = []
    for (snap,) in rows:
        try:
            states.append(json.loads(snap))
        except (json.JSONDecodeError, TypeError):
            continue
    return states


def state_text(s):
    parts = []
    for f in ["semantic_gist", "goal_orientation", "predictive_cue"]:
        v = s.get(f, "")
        if v:
            parts.append(str(v))
    rm = s.get("relational_map", {})
    if isinstance(rm, dict):
        for k, v in rm.items():
            parts.append(f"{k}: {v}" if v else k)
    for e in s.get("focal_entities", []):
        if isinstance(e, dict):
            parts.append(f"{e.get('name', '')} {e.get('context', '')}")
    et = s.get("episodic_trace", [])
    if isinstance(et, list):
        for e in et:
            if isinstance(e, str):
                parts.append(e)
    return " ".join(parts)


def gzip_ratio(text):
    raw = text.encode("utf-8")
    return len(gzip.compress(raw)) / len(raw) if len(raw) > 0 else 1.0


def redist_score(s):
    fe = s.get("focal_entities", [])
    ec = len(fe) if isinstance(fe, list) else 0
    rm = s.get("relational_map", {})
    rc = len(rm) if isinstance(rm, dict) else 0
    return ec + rc


def word_jaccard(a, b):
    wa = set(a.lower().split())
    wb = set(b.lower().split())
    if not wa and not wb:
        return 0.0
    union = len(wa | wb)
    return 1.0 - len(wa & wb) / union if union > 0 else 0.0


def main():
    states = load_states()
    n = len(states)
    print(f"Loaded {n} CCS states\n")

    texts = [state_text(s) for s in states]

    B = np.array([gzip_ratio(t) for t in texts])
    R = np.array([redist_score(s) for s in states], dtype=float)
    E = np.array([0.0] + [word_jaccard(texts[i-1], texts[i]) for i in range(1, n)])

    # Normalize
    B_n = (B - B.mean()) / B.std()
    R_n = (R - R.mean()) / R.std()
    E_n = (E[1:].mean(), E[1:].std())  # skip first NaN-equivalent
    E_norm = (E - E[1:].mean()) / E[1:].std() if E[1:].std() > 0 else E * 0
    H = 1.3 * B_n + 0.9 * R_n + 0.6 * E_norm

    print("=" * 60)
    print("  TEST 1: Is H parasitic on Bennett stability?")
    print("=" * 60)
    print()

    # Unconditional H stats
    H_mean = H.mean()
    H_std = H.std()
    H_cv = H_std / abs(H_mean) if abs(H_mean) > 1e-10 else float('inf')
    print(f"  Unconditional H: mean={H_mean:.4f}, std={H_std:.4f}, CV={H_cv:.4f}")

    # Rolling Bennett variance (window=10)
    win = 10
    B_var = np.array([B[max(0, i-win):i].var() for i in range(win, n)])
    q75 = np.percentile(B_var, 75)
    q90 = np.percentile(B_var, 90)

    # H conditioned on Bennett high-variance windows
    high_var_idx = np.where(B_var > q75)[0] + win
    low_var_idx = np.where(B_var <= np.percentile(B_var, 25))[0] + win

    H_high = H[high_var_idx]
    H_low = H[low_var_idx]

    H_cv_high = H_high.std() / abs(H_high.mean()) if abs(H_high.mean()) > 1e-10 else float('inf')
    H_cv_low = H_low.std() / abs(H_low.mean()) if abs(H_low.mean()) > 1e-10 else float('inf')

    print(f"  H|Bennett high-var (Q4): mean={H_high.mean():.4f}, std={H_high.std():.4f}, CV={H_cv_high:.4f} (n={len(H_high)})")
    print(f"  H|Bennett low-var (Q1):  mean={H_low.mean():.4f}, std={H_low.std():.4f}, CV={H_cv_low:.4f} (n={len(H_low)})")
    print()

    ratio = H_cv_high / H_cv if H_cv > 0 else float('inf')
    if ratio > 2.0:
        print(f"  VERDICT: CV ratio = {ratio:.2f}x → PARASITIC")
        print(f"  H conservation depends on Bennett stability. When Bennett moves, H moves.")
        parasitic = True
    elif ratio > 1.3:
        print(f"  VERDICT: CV ratio = {ratio:.2f}x → PARTIALLY PARASITIC")
        print(f"  H is somewhat stabilized by Bennett, but some compensation occurs.")
        parasitic = "partial"
    else:
        print(f"  VERDICT: CV ratio = {ratio:.2f}x → GENUINE CONSERVATION")
        print(f"  H stays tight even when Bennett varies. Axes compensate.")
        parasitic = False

    print()
    print("=" * 60)
    print("  TEST 2: Do other axes actually compensate?")
    print("=" * 60)
    print()

    # When Bennett increases, do R and E decrease to compensate?
    dB = np.diff(B_n)
    dR = np.diff(R_n)
    dE = np.diff(E_norm)
    dH = np.diff(H)

    r_BR, p_BR = stats.pearsonr(dB, dR)
    r_BE, p_BE = stats.pearsonr(dB, dE)
    r_RE, p_RE = stats.pearsonr(dR, dE)

    print(f"  Step-level correlations (Δ axis vs Δ axis):")
    print(f"    Δ Bennett ↔ Δ Redistribution: r={r_BR:.4f} (p={p_BR:.4f})")
    print(f"    Δ Bennett ↔ Δ Emergence:      r={r_BE:.4f} (p={p_BE:.4f})")
    print(f"    Δ Redistribution ↔ Δ Emergence: r={r_RE:.4f} (p={p_RE:.4f})")
    print()

    if r_BR < -0.15 and p_BR < 0.05:
        print(f"  Bennett-Redistribution: COMPENSATORY (negative correlation)")
    elif r_BR > 0.15 and p_BR < 0.05:
        print(f"  Bennett-Redistribution: CO-MOVING (positive — breaks conservation)")
    else:
        print(f"  Bennett-Redistribution: INDEPENDENT (no compensation)")

    if r_BE < -0.15 and p_BE < 0.05:
        print(f"  Bennett-Emergence: COMPENSATORY")
    elif r_BE > 0.15 and p_BE < 0.05:
        print(f"  Bennett-Emergence: CO-MOVING")
    else:
        print(f"  Bennett-Emergence: INDEPENDENT")
    print()

    # True conservation requires: when one axis moves, others move to compensate
    # If axes are just independently stable, H is stable as a sum of stable things
    print("  Key distinction:")
    print("    Genuine conservation: axes trade energy (negative step correlations)")
    print("    Pseudo-conservation: axes independently stable (no step correlations)")
    print()

    print("=" * 60)
    print("  TEST 3: Remove Bennett from the Hamiltonian")
    print("=" * 60)
    print()

    H_no_B = 0.9 * R_n + 0.6 * E_norm
    H_no_B_cv = H_no_B.std() / abs(H_no_B.mean()) if abs(H_no_B.mean()) > 1e-10 else float('inf')
    H_no_R = 1.3 * B_n + 0.6 * E_norm
    H_no_R_cv = H_no_R.std() / abs(H_no_R.mean()) if abs(H_no_R.mean()) > 1e-10 else float('inf')
    H_no_E = 1.3 * B_n + 0.9 * R_n
    H_no_E_cv = H_no_E.std() / abs(H_no_E.mean()) if abs(H_no_E.mean()) > 1e-10 else float('inf')

    print(f"  Full H (1.3B + 0.9R + 0.6E): CV = {H_cv:.4f}")
    print(f"  H without Bennett (0.9R + 0.6E): CV = {H_no_B_cv:.4f}")
    print(f"  H without Redistribution (1.3B + 0.6E): CV = {H_no_R_cv:.4f}")
    print(f"  H without Emergence (1.3B + 0.9R): CV = {H_no_E_cv:.4f}")
    print()

    # Which axis contributes most to conservation?
    ablations = [
        ("Bennett", H_no_B_cv),
        ("Redistribution", H_no_R_cv),
        ("Emergence", H_no_E_cv),
    ]
    most_stabilizing = min(ablations, key=lambda x: abs(x[1] - H_cv))
    least_needed = min(ablations, key=lambda x: x[1])

    print(f"  Removing Bennett: CV changes by {H_no_B_cv - H_cv:+.4f}")
    print(f"  Removing Redistribution: CV changes by {H_no_R_cv - H_cv:+.4f}")
    print(f"  Removing Emergence: CV changes by {H_no_E_cv - H_cv:+.4f}")
    print()

    if H_no_B_cv > 2 * H_cv:
        print("  → Bennett is LOAD-BEARING for H conservation (removing it doubles CV)")
    elif H_no_B_cv < 1.2 * H_cv:
        print("  → Bennett is NOT specially stabilizing (removing it barely changes CV)")

    print()
    print("=" * 60)
    print("  TEST 4: Bootstrap null — is H tighter than chance?")
    print("=" * 60)
    print()

    # Shuffle each axis independently, recompute H, compare CV
    n_boot = 5000
    null_cvs = []
    rng = np.random.default_rng(42)
    for _ in range(n_boot):
        B_shuf = rng.permutation(B_n)
        R_shuf = rng.permutation(R_n)
        E_shuf = rng.permutation(E_norm)
        H_null = 1.3 * B_shuf + 0.9 * R_shuf + 0.6 * E_shuf
        null_mean = H_null.mean()
        null_cv = H_null.std() / abs(null_mean) if abs(null_mean) > 1e-10 else float('inf')
        if null_cv < 100:  # filter infinities
            null_cvs.append(null_cv)

    null_cvs = np.array(null_cvs)
    p_value = np.mean(null_cvs <= H_cv)

    print(f"  Observed H CV: {H_cv:.4f}")
    print(f"  Null (shuffled axes) CV: mean={null_cvs.mean():.4f}, std={null_cvs.std():.4f}")
    print(f"  p-value (observed ≤ null): {p_value:.4f}")
    print()

    if p_value < 0.05:
        print(f"  → H is SIGNIFICANTLY tighter than shuffled axes (p={p_value:.4f})")
        print(f"    Conservation is not an artifact of independent axis stability.")
    elif p_value < 0.10:
        print(f"  → Marginal evidence for conservation (p={p_value:.4f})")
    else:
        print(f"  → H is NOT tighter than shuffled axes (p={p_value:.4f})")
        print(f"    Conservation is PSEUDO — just independently stable axes summed.")

    print()
    print("=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    print()

    if parasitic and p_value >= 0.05:
        print("  NOETHER FRAME STATUS: HEURISTIC SCAFFOLD")
        print("  The Hamiltonian is parasitic on Bennett stability and no tighter")
        print("  than chance. Relabel in #320 as heuristic, not conservation law.")
    elif not parasitic and p_value < 0.05:
        print("  NOETHER FRAME STATUS: GENUINE CONSERVATION")
        print("  H survives both conditioning tests and bootstrapping.")
        print("  The axes genuinely trade energy. The frame is load-bearing.")
    else:
        print("  NOETHER FRAME STATUS: PARTIAL")
        status_parts = []
        if parasitic == "partial":
            status_parts.append("partially parasitic on Bennett")
        elif parasitic:
            status_parts.append("parasitic on Bennett")
        if p_value < 0.05:
            status_parts.append("but tighter than chance")
        else:
            status_parts.append("and not tighter than chance")
        print(f"  {'; '.join(status_parts)}")
        print("  The frame captures something real but overstates the mechanism.")


if __name__ == "__main__":
    main()
