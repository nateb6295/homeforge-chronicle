#!/usr/bin/env python3
"""Compression Spacing Test — Does timing beat repetition for CCS identity?

Inspired by Namboodiri & Burke (Nature Neuroscience, Feb 2026): learning rate
is proportional to inter-reward DURATION, not trial count. The nervous system
calibrates via temporal structure, not effort/volume.

Hypothesis: CCS compressions with longer intervals between them should show
BETTER identity preservation (stiff direction stability) than rapid-fire
compressions. The gap lets the sloppy directions relax, and the compressor
has more diverse episodic content to work with.

Measures:
1. Identity drift per compression (cosine distance of gist+goal between consecutive versions)
2. Episodic novelty (how different is the episodic content vs previous)
3. Correlation between inter-compression interval and identity stability

Thread 318 advance 183 connection: same principle as B83 pulsed dosing (9pp improvement)
and Namboodiri's dopamine timing result.

Usage:
  python3 compression_spacing_test.py           # Run full analysis
  python3 compression_spacing_test.py --plot     # ASCII scatter plot
"""

import json
import os
import sys
import sqlite3
import time
import requests
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
OLLAMA_URL = "http://localhost:11434"
MODEL = "snowflake-arctic-embed2"


def get_embedding(text: str) -> list:
    """Get embedding from Ollama."""
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/embed",
            json={"model": MODEL, "input": text},
            timeout=30
        )
        r.raise_for_status()
        return r.json().get("embeddings", [[]])[0]
    except Exception:
        return []


def cosine_sim(a: list, b: list) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(x * x for x in b) ** 0.5
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def load_ccs_history() -> list:
    """Load all CCS snapshots with timestamps and intervals."""
    db = sqlite3.connect(str(DB))
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history ORDER BY id"
    ).fetchall()
    db.close()

    entries = []
    prev_ts = None
    for cid, snap_text, created_at in rows:
        try:
            snap = json.loads(snap_text)
        except (json.JSONDecodeError, TypeError):
            continue

        interval = (created_at - prev_ts) if prev_ts is not None else None
        prev_ts = created_at

        entries.append({
            "id": cid,
            "snapshot": snap,
            "ts": created_at,
            "interval_sec": interval,
        })

    return entries


def extract_identity_text(snap: dict) -> str:
    """Extract stiff-direction fields (identity) from CCS snapshot."""
    gist = snap.get("semantic_gist", "")
    goal = snap.get("goal_orientation", "")
    return f"{gist}\n{goal}"


def extract_episodic_text(snap: dict) -> str:
    """Extract sloppy-direction fields (episodic) from CCS snapshot."""
    ep = snap.get("episodic_trace", [])
    if isinstance(ep, str):
        try:
            ep = json.loads(ep)
        except (json.JSONDecodeError, TypeError):
            ep = [ep]
    if isinstance(ep, list):
        return "\n".join(str(e) for e in ep)
    return str(ep)


def run_analysis():
    """Run the full spacing analysis."""
    entries = load_ccs_history()
    if len(entries) < 3:
        print(f"Need at least 3 CCS snapshots (have {len(entries)})")
        return None

    print(f"Loaded {len(entries)} CCS snapshots (v{entries[0]['id']}–v{entries[-1]['id']})")
    print(f"Time range: {time.strftime('%Y-%m-%d %H:%M', time.localtime(entries[0]['ts']))}"
          f" → {time.strftime('%Y-%m-%d %H:%M', time.localtime(entries[-1]['ts']))}")

    # Compute embeddings for identity fields
    print("\nEmbedding identity fields...")
    identity_embs = []
    episodic_embs = []
    for i, entry in enumerate(entries):
        id_text = extract_identity_text(entry["snapshot"])
        ep_text = extract_episodic_text(entry["snapshot"])

        id_emb = get_embedding(id_text)
        ep_emb = get_embedding(ep_text)

        identity_embs.append(id_emb)
        episodic_embs.append(ep_emb)

        if (i + 1) % 10 == 0:
            print(f"  {i + 1}/{len(entries)} embedded")

    print(f"  {len(entries)}/{len(entries)} done")

    # Compute pairwise metrics
    results = []
    for i in range(1, len(entries)):
        interval = entries[i]["interval_sec"]
        if interval is None:
            continue

        # Identity stability: high similarity = stable (good)
        id_sim = cosine_sim(identity_embs[i], identity_embs[i - 1])
        # Episodic novelty: low similarity = novel content (diversity)
        ep_sim = cosine_sim(episodic_embs[i], episodic_embs[i - 1])

        results.append({
            "from_v": entries[i - 1]["id"],
            "to_v": entries[i]["id"],
            "interval_sec": interval,
            "interval_min": round(interval / 60, 1),
            "identity_sim": round(id_sim, 4),
            "identity_drift": round(1 - id_sim, 4),
            "episodic_novelty": round(1 - ep_sim, 4),
        })

    if not results:
        print("No valid pairs.")
        return None

    # Sort into short vs long intervals
    intervals = sorted(r["interval_sec"] for r in results)
    median_interval = intervals[len(intervals) // 2]

    short = [r for r in results if r["interval_sec"] <= median_interval]
    long = [r for r in results if r["interval_sec"] > median_interval]

    def mean(vals):
        return sum(vals) / len(vals) if vals else 0

    def stdev(vals):
        if len(vals) < 2:
            return 0
        m = mean(vals)
        return (sum((v - m) ** 2 for v in vals) / (len(vals) - 1)) ** 0.5

    print(f"\n{'='*60}")
    print(f"COMPRESSION SPACING ANALYSIS")
    print(f"{'='*60}")
    print(f"Total pairs: {len(results)}")
    print(f"Median interval: {median_interval/60:.1f} min ({median_interval}s)")
    print(f"Range: {min(r['interval_sec'] for r in results)/60:.1f}–{max(r['interval_sec'] for r in results)/60:.1f} min")

    print(f"\n--- Short intervals (≤{median_interval/60:.0f} min, n={len(short)}) ---")
    short_drift = [r["identity_drift"] for r in short]
    short_novelty = [r["episodic_novelty"] for r in short]
    print(f"  Identity drift:    {mean(short_drift):.4f} ± {stdev(short_drift):.4f}")
    print(f"  Episodic novelty:  {mean(short_novelty):.4f} ± {stdev(short_novelty):.4f}")

    print(f"\n--- Long intervals (>{median_interval/60:.0f} min, n={len(long)}) ---")
    long_drift = [r["identity_drift"] for r in long]
    long_novelty = [r["episodic_novelty"] for r in long]
    print(f"  Identity drift:    {mean(long_drift):.4f} ± {stdev(long_drift):.4f}")
    print(f"  Episodic novelty:  {mean(long_novelty):.4f} ± {stdev(long_novelty):.4f}")

    # Namboodiri prediction: longer intervals → LESS identity drift (better stability)
    # AND more episodic novelty (more diverse content between compressions)
    drift_diff = mean(long_drift) - mean(short_drift)
    novelty_diff = mean(long_novelty) - mean(short_novelty)

    print(f"\n--- Namboodiri prediction test ---")
    print(f"  H1: Longer intervals → less identity drift")
    drift_direction = "CONFIRMED" if drift_diff < 0 else "FALSIFIED"
    print(f"  Result: long-short drift diff = {drift_diff:+.4f} ({drift_direction})")

    print(f"  H2: Longer intervals → more episodic novelty")
    novelty_direction = "CONFIRMED" if novelty_diff > 0 else "FALSIFIED"
    print(f"  Result: long-short novelty diff = {novelty_diff:+.4f} ({novelty_direction})")

    # Spearman rank correlation (manual, no scipy needed)
    def spearman(xs, ys):
        n = len(xs)
        if n < 3:
            return 0.0
        rx = rank(xs)
        ry = rank(ys)
        d2 = sum((a - b) ** 2 for a, b in zip(rx, ry))
        return 1 - 6 * d2 / (n * (n * n - 1))

    def rank(vals):
        n = len(vals)
        indexed = sorted(range(n), key=lambda i: vals[i])
        ranks = [0.0] * n
        for r, i in enumerate(indexed):
            ranks[i] = r + 1.0
        return ranks

    all_intervals = [r["interval_sec"] for r in results]
    all_drifts = [r["identity_drift"] for r in results]
    all_novelty = [r["episodic_novelty"] for r in results]

    rho_drift = spearman(all_intervals, all_drifts)
    rho_novelty = spearman(all_intervals, all_novelty)

    print(f"\n--- Rank correlations (Spearman) ---")
    print(f"  Interval × identity drift:   ρ = {rho_drift:+.3f}"
          f"  {'(longer→less drift, GOOD)' if rho_drift < 0 else '(longer→more drift)'}")
    print(f"  Interval × episodic novelty: ρ = {rho_novelty:+.3f}"
          f"  {'(longer→more novel, GOOD)' if rho_novelty > 0 else '(longer→less novel)'}")

    # Overall assessment
    print(f"\n{'='*60}")
    if drift_diff < 0 and novelty_diff > 0:
        print("TIMING BEATS REPETITION: Longer intervals preserve identity better")
        print("while accumulating more diverse episodic content.")
    elif drift_diff < 0:
        print("PARTIAL: Longer intervals stabilize identity but episodic diversity")
        print("doesn't increase — spacing may already be sufficient.")
    elif novelty_diff > 0:
        print("PARTIAL: Episodic diversity increases with spacing but identity drift")
        print("also increases — the stabilizer may not scale with gap length.")
    else:
        print("NEUTRAL: No clear timing effect on CCS identity. The compressor may")
        print("already be interval-agnostic (renormalization dominates timing).")
    print(f"{'='*60}")

    return {
        "n_pairs": len(results),
        "median_interval_sec": median_interval,
        "short_mean_drift": round(mean(short_drift), 4),
        "long_mean_drift": round(mean(long_drift), 4),
        "drift_diff": round(drift_diff, 4),
        "short_mean_novelty": round(mean(short_novelty), 4),
        "long_mean_novelty": round(mean(long_novelty), 4),
        "novelty_diff": round(novelty_diff, 4),
        "rho_drift": round(rho_drift, 3),
        "rho_novelty": round(rho_novelty, 3),
        "pairs": results,
    }


def ascii_scatter(results: dict):
    """ASCII scatter plot of interval vs identity drift."""
    if not results or not results.get("pairs"):
        print("No data.")
        return

    pairs = results["pairs"]
    width = 60
    height = 20

    intervals = [p["interval_min"] for p in pairs]
    drifts = [p["identity_drift"] for p in pairs]

    min_x, max_x = min(intervals), max(intervals)
    min_y, max_y = min(drifts), max(drifts)

    if max_x == min_x:
        max_x = min_x + 1
    if max_y == min_y:
        max_y = min_y + 0.01

    grid = [[" "] * width for _ in range(height)]

    for x, y in zip(intervals, drifts):
        col = int((x - min_x) / (max_x - min_x) * (width - 1))
        row = int((1 - (y - min_y) / (max_y - min_y)) * (height - 1))
        col = max(0, min(width - 1, col))
        row = max(0, min(height - 1, row))
        grid[row][col] = "●"

    print(f"\nInterval (min) vs Identity Drift")
    print(f"  {max_y:.3f} ┤{''.join(grid[0])}")
    for r in range(1, height - 1):
        print(f"        │{''.join(grid[r])}")
    print(f"  {min_y:.3f} ┤{''.join(grid[-1])}")
    print(f"        └{'─' * width}")
    print(f"         {min_x:.0f}{' ' * (width - len(f'{min_x:.0f}') - len(f'{max_x:.0f}'))}{max_x:.0f}")


if __name__ == "__main__":
    results = run_analysis()
    if results and "--plot" in sys.argv:
        ascii_scatter(results)

    if results:
        # Save results
        out = Path(os.path.expanduser("~/chronicle/data/compression_spacing.json"))
        out.write_text(json.dumps(results, indent=2))
        print(f"\nResults saved to {out}")
