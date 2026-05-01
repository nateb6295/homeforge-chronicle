#!/usr/bin/env python3
"""Input Correlation Spectrum — measures eigenvalue decay of CCS input correlation.

Chen et al. (2505.08915) proved that the geometric decay ratio of model eigenvalues
depends on the eigenvalue decay of the INPUT CORRELATION MATRIX. This tool:

1. Loads all CCS snapshots from cognitive_state_history
2. Extracts text from each field (gist, goal, episodic, entities, etc.)
3. Embeds each field using the local embedding model
4. Computes the correlation matrix across snapshots
5. Reports eigenvalue spectrum and decay ratio

If the correlation matrix eigenvalues decay geometrically with a similar ratio to
the Fisher profile (≈0.56), that's direct empirical confirmation that CCS is a
sloppy model governed by input correlation structure.

Usage:
  python3 input_correlation_spectrum.py
  python3 input_correlation_spectrum.py --plot
  python3 input_correlation_spectrum.py --field semantic_gist  # single field
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

import numpy as np
import requests

DB = Path("/mnt/hdd/chronicle-data/processed.db")
OLLAMA_URL = os.environ.get("CHRONICLE_OLLAMA_URL", "http://192.168.1.11:11434")
EMBED_MODEL = os.environ.get("CHRONICLE_EMBEDDING_MODEL", "mxbai-embed-large")
LOG_FILE = Path(os.path.expanduser("~/chronicle/data/input_correlation_spectrum.jsonl"))

CCS_FIELDS = [
    "semantic_gist",
    "goal_orientation",
    "episodic_trace",
    "focal_entities",
    "constraints",
    "predictive_cue",
    "uncertainty_signals",
]


def load_snapshots() -> list[dict]:
    """Load all CCS snapshots from history."""
    db = sqlite3.connect(str(DB))
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history ORDER BY id"
    ).fetchall()
    db.close()

    snapshots = []
    for sid, snap_json, ts in rows:
        try:
            snap = json.loads(snap_json)
            snap["_id"] = sid
            snap["_ts"] = ts
            snapshots.append(snap)
        except json.JSONDecodeError:
            continue
    return snapshots


def field_text(snapshot: dict, field: str) -> str:
    """Extract text from a CCS field."""
    val = snapshot.get(field, "")
    if isinstance(val, list):
        return " ".join(str(v) for v in val)
    if isinstance(val, dict):
        return json.dumps(val)
    return str(val)


def embed_text(text: str) -> list[float] | None:
    """Get embedding from Ollama."""
    if not text.strip():
        return None
    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/embed",
            json={"model": EMBED_MODEL, "input": text[:2000]},
            timeout=30,
        )
        if resp.status_code == 200:
            data = resp.json()
            embeddings = data.get("embeddings", [])
            if embeddings:
                return embeddings[0]
        return None
    except Exception:
        return None


def compute_spectrum(embeddings: list[list[float]]) -> dict:
    """Compute eigenvalue spectrum of the correlation matrix."""
    if len(embeddings) < 3:
        return {"status": "insufficient", "n": len(embeddings)}

    # Stack into matrix: rows = snapshots, cols = embedding dimensions
    X = np.array(embeddings)

    # Center
    X = X - X.mean(axis=0)

    # Correlation matrix (n_snapshots x n_snapshots for efficiency since n_dim >> n_snapshots)
    # Use SVD instead for numerical stability
    _, s, _ = np.linalg.svd(X, full_matrices=False)

    # Singular values squared / (n-1) = eigenvalues of sample covariance
    eigenvalues = (s ** 2) / (len(embeddings) - 1)

    # Normalize by largest
    if eigenvalues[0] > 0:
        normalized = eigenvalues / eigenvalues[0]
    else:
        normalized = eigenvalues

    # Compute consecutive ratios for non-negligible eigenvalues (> 1% of max)
    significant = [v for v in normalized if v > 0.01]
    ratios = []
    for i in range(1, len(significant)):
        if significant[i - 1] > 0:
            ratios.append(significant[i] / significant[i - 1])

    mean_ratio = sum(ratios) / len(ratios) if ratios else 0
    if ratios:
        var_r = sum((r - mean_ratio) ** 2 for r in ratios) / len(ratios)
        cv = (var_r ** 0.5) / mean_ratio if mean_ratio > 0 else float("inf")
    else:
        cv = float("inf")

    return {
        "status": "computed",
        "n_snapshots": len(embeddings),
        "n_significant": len(significant),
        "top_10_eigenvalues": [round(float(v), 6) for v in normalized[:10]],
        "ratios": [round(r, 4) for r in ratios[:10]],
        "mean_ratio": round(mean_ratio, 4),
        "cv": round(cv, 4),
        "is_geometric": cv < 0.3 if ratios else False,
        "total_variance_explained_top3": round(float(sum(normalized[:3]) / sum(normalized)), 4) if sum(normalized) > 0 else 0,
    }


def ascii_spectrum(eigenvalues: list[float], width: int = 40):
    """Print ASCII bar chart of eigenvalue spectrum."""
    print(f"\n  Eigenvalue spectrum (top {len(eigenvalues)}, normalized):")
    print("  " + "-" * (width + 15))
    for i, v in enumerate(eigenvalues):
        bar_len = int(v * width)
        bar = "█" * bar_len + "░" * (width - bar_len)
        print(f"  λ_{i:<3d} │{bar}│ {v:.4f}")
    print("  " + "-" * (width + 15))


def main():
    parser = argparse.ArgumentParser(description="CCS Input Correlation Spectrum")
    parser.add_argument("--plot", action="store_true")
    parser.add_argument("--field", help="Analyze single field only")
    args = parser.parse_args()

    snapshots = load_snapshots()
    print(f"Loaded {len(snapshots)} CCS snapshots")

    if len(snapshots) < 5:
        print("Need 5+ snapshots for meaningful spectrum analysis")
        return

    fields_to_analyze = [args.field] if args.field else CCS_FIELDS

    results = {}
    for field in fields_to_analyze:
        print(f"\n--- {field} ---")

        # Extract text for each snapshot
        texts = [field_text(s, field) for s in snapshots]
        non_empty = [(i, t) for i, t in enumerate(texts) if t.strip()]
        print(f"  Non-empty snapshots: {len(non_empty)}/{len(snapshots)}")

        if len(non_empty) < 5:
            print(f"  Skipping (need 5+)")
            continue

        # Embed
        print(f"  Embedding {len(non_empty)} texts...")
        embeddings = []
        for i, (idx, text) in enumerate(non_empty):
            emb = embed_text(text)
            if emb:
                embeddings.append(emb)
            if (i + 1) % 10 == 0:
                print(f"    {i + 1}/{len(non_empty)}...")

        print(f"  Got {len(embeddings)} embeddings")

        if len(embeddings) < 5:
            print(f"  Skipping (need 5+ embeddings)")
            continue

        # Compute spectrum
        spectrum = compute_spectrum(embeddings)
        results[field] = spectrum

        if spectrum["status"] == "computed":
            print(f"  Significant eigenvalues: {spectrum['n_significant']}")
            print(f"  Mean consecutive ratio: {spectrum['mean_ratio']}")
            print(f"  CV: {spectrum['cv']} ({'GEOMETRIC' if spectrum['is_geometric'] else 'not geometric'})")
            print(f"  Top-3 variance explained: {spectrum['total_variance_explained_top3']:.1%}")

            if args.plot and spectrum["top_10_eigenvalues"]:
                ascii_spectrum(spectrum["top_10_eigenvalues"])

    # Combined: embed ALL fields concatenated per snapshot
    if not args.field:
        print(f"\n--- COMBINED (all fields) ---")
        combined_texts = []
        for s in snapshots:
            parts = [field_text(s, f) for f in CCS_FIELDS]
            combined_texts.append(" ".join(parts))

        non_empty = [(i, t) for i, t in enumerate(combined_texts) if t.strip()]
        print(f"  Non-empty snapshots: {len(non_empty)}/{len(snapshots)}")

        print(f"  Embedding {len(non_empty)} combined texts...")
        embeddings = []
        for i, (idx, text) in enumerate(non_empty):
            emb = embed_text(text)
            if emb:
                embeddings.append(emb)
            if (i + 1) % 10 == 0:
                print(f"    {i + 1}/{len(non_empty)}...")

        print(f"  Got {len(embeddings)} embeddings")

        if len(embeddings) >= 5:
            spectrum = compute_spectrum(embeddings)
            results["_combined"] = spectrum

            if spectrum["status"] == "computed":
                print(f"  Significant eigenvalues: {spectrum['n_significant']}")
                print(f"  Mean consecutive ratio: {spectrum['mean_ratio']}")
                print(f"  CV: {spectrum['cv']} ({'GEOMETRIC' if spectrum['is_geometric'] else 'not geometric'})")
                print(f"  Top-3 variance explained: {spectrum['total_variance_explained_top3']:.1%}")

                if args.plot and spectrum["top_10_eigenvalues"]:
                    ascii_spectrum(spectrum["top_10_eigenvalues"])

    # Compare to Fisher profile
    fisher_file = Path(os.path.expanduser("~/chronicle/data/fisher_profiles.jsonl"))
    if fisher_file.exists() and results:
        print(f"\n=== COMPARISON: Input correlation vs Fisher profile ===")
        with open(fisher_file) as f:
            fisher = json.loads(f.readline())
        fisher_fields = fisher.get("profile", {})
        fisher_vals = sorted([d["drop_per_kt"] for d in fisher_fields.values()], reverse=True)
        fisher_nonzero = [v for v in fisher_vals if v > 0.001]
        fisher_ratios = [fisher_nonzero[i] / fisher_nonzero[i - 1] for i in range(1, len(fisher_nonzero))]
        fisher_mean = sum(fisher_ratios) / len(fisher_ratios) if fisher_ratios else 0

        print(f"  Fisher profile mean ratio: {fisher_mean:.4f}")
        if "_combined" in results and results["_combined"].get("mean_ratio"):
            corr_mean = results["_combined"]["mean_ratio"]
            print(f"  Input correlation mean ratio: {corr_mean:.4f}")
            match_pct = 1 - abs(fisher_mean - corr_mean) / fisher_mean if fisher_mean > 0 else 0
            print(f"  Match: {match_pct:.1%}")
            if match_pct > 0.7:
                print(f"  → CONSISTENT with Chen theorem: input correlation controls Fisher decay")
            else:
                print(f"  → DIVERGENT: Fisher and input correlation have different decay rates")

    # Log
    if results:
        event = {
            "ts": int(time.time()),
            "n_snapshots": len(snapshots),
            "results": results,
        }
        os.makedirs(LOG_FILE.parent, exist_ok=True)
        with open(LOG_FILE, "a") as f:
            f.write(json.dumps(event, default=str) + "\n")
        print(f"\nLogged to {LOG_FILE}")


if __name__ == "__main__":
    main()
