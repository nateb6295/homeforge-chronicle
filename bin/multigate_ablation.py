#!/usr/bin/env python3
"""Multi-gate ablation — tests CCS field importance across different evaluation levels.

Inspired by "Beyond Structure and Affinity" (biorxiv 2026-04-17): same feature
can help at one experimental gate and hurt at another. Single-objective ranking
selects candidates that pass one stage but fail later.

purpose_ablation.py measures Gate 1 (embedding identity). This script adds:
  Gate 2 — Inter-field coherence (P_strong): does removing this field break
           the relational structure between other fields?
  Gate 3 — Specificity: does removing this field make the CCS more generic?
  Gate 4 — Information density: does removing this field reduce falsifiable markers?

If field rankings differ across gates, that's the multi-gate effect: optimizing
for one gate alone (e.g., embedding identity) may miss what matters at another
level (e.g., coherence or specificity).

Usage:
  python3 multigate_ablation.py          # Run all gates on current CCS
  python3 multigate_ablation.py --gate 2 # Run only coherence gate
"""

import json
import math
import os
import re
import sqlite3
import sys
from collections import defaultdict
from itertools import combinations

DB = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")

# Same fields as purpose_ablation
ABLATION_FIELDS = [
    "goal_orientation",
    "semantic_gist",
    "constraints",
    "focal_entities",
    "episodic_trace",
    "predictive_cue",
    "uncertainty_signals",
    "relational_map",
]

# Coherence pairs (from persistence_probe.py)
COHERENCE_PAIRS = [
    ("semantic_gist", "focal_entities"),
    ("semantic_gist", "goal_orientation"),
    ("goal_orientation", "constraints"),
    ("goal_orientation", "focal_entities"),
    ("episodic_trace", "focal_entities"),
    ("episodic_trace", "goal_orientation"),
    ("predictive_cue", "goal_orientation"),
    ("predictive_cue", "episodic_trace"),
]

STOPWORDS = {"the", "and", "for", "with", "that", "this", "from", "have", "been",
             "will", "more", "also", "into", "when", "what", "which", "than",
             "each", "about", "should", "could", "would", "does", "their",
             "other", "some", "only", "most", "same", "very", "just", "over",
             "such", "like", "through", "between", "across", "within", "under",
             "after", "before", "during", "while", "where", "being", "both",
             "still", "well", "they", "them", "then", "these", "those", "here",
             "there", "were", "your", "name", "type", "true", "false", "null",
             "none", "context", "salience"}


def get_current_ccs() -> dict:
    """Get current CCS from database."""
    db = sqlite3.connect(DB)
    row = db.execute(
        "SELECT episodic_trace, semantic_gist, focal_entities, relational_map, "
        "goal_orientation, constraints, predictive_cue, uncertainty_signals "
        "FROM cognitive_state WHERE id = 1"
    ).fetchone()
    db.close()
    if not row:
        print("No CCS found")
        sys.exit(1)

    fields = ["episodic_trace", "semantic_gist", "focal_entities", "relational_map",
              "goal_orientation", "constraints", "predictive_cue", "uncertainty_signals"]
    ccs = {}
    for i, field in enumerate(fields):
        val = row[i]
        try:
            ccs[field] = json.loads(val) if val.startswith(("[", "{")) else val
        except (json.JSONDecodeError, AttributeError):
            ccs[field] = val or ""
    return ccs


def field_to_text(val) -> str:
    """Convert field value to searchable text."""
    if isinstance(val, list):
        parts = []
        for item in val:
            if isinstance(item, dict):
                parts.extend(str(v) for v in item.values())
            else:
                parts.append(str(item))
        return " ".join(parts)
    elif isinstance(val, dict):
        return " ".join(f"{k} {v}" for k, v in val.items())
    return str(val)


def extract_terms(text: str, min_len: int = 4) -> set[str]:
    """Extract meaningful terms from text."""
    words = re.findall(r"[a-zA-Z0-9_#.]+", text.lower())
    return {w for w in words if len(w) >= min_len and w not in STOPWORDS}


def ablate(ccs: dict, field: str) -> dict:
    """Remove a field from CCS."""
    ablated = dict(ccs)
    if field in ablated:
        if isinstance(ablated[field], list):
            ablated[field] = []
        elif isinstance(ablated[field], dict):
            ablated[field] = {}
        else:
            ablated[field] = ""
    return ablated


# === Gate 2: Inter-field Coherence ===

def compute_coherence(ccs: dict) -> tuple[float, dict]:
    """Compute inter-field coherence (P_strong).
    Returns (overall_score, per_pair_scores).
    """
    pair_scores = {}
    for field_a, field_b in COHERENCE_PAIRS:
        if field_a not in ccs or field_b not in ccs:
            continue
        terms_a = extract_terms(field_to_text(ccs[field_a]))
        terms_b = extract_terms(field_to_text(ccs[field_b]))
        if not terms_a or not terms_b:
            pair_scores[(field_a, field_b)] = 0.0
            continue
        overlap = terms_a & terms_b
        union = terms_a | terms_b
        pair_scores[(field_a, field_b)] = len(overlap) / len(union) if union else 0.0

    if not pair_scores:
        return 0.0, {}
    return sum(pair_scores.values()) / len(pair_scores), pair_scores


def gate2_coherence(ccs: dict) -> list[tuple]:
    """Gate 2: measure coherence impact of removing each field."""
    base_coherence, _ = compute_coherence(ccs)
    results = []

    for field in ABLATION_FIELDS:
        ablated = ablate(ccs, field)
        abl_coherence, _ = compute_coherence(ablated)
        drop = base_coherence - abl_coherence
        results.append((field, drop, base_coherence, abl_coherence))

    return sorted(results, key=lambda x: x[1], reverse=True)


# === Gate 3: Specificity ===

def compute_specificity(ccs: dict) -> tuple[int, dict]:
    """Count falsifiable markers across CCS.
    Returns (total_markers, per_field_counts).
    """
    patterns = {
        "dates": r"\d{4}[-/]\d{2}[-/]\d{2}",
        "thread_refs": r"(?:thread|advance)\s*#?\d+",
        "file_paths": r"[a-z_]+\.(?:py|rs|md|sh|toml|json)",
        "version_numbers": r"v?\d+\.\d+(?:\.\d+)?",
        "proper_names": r"\b[A-Z][a-z]+(?:\s[A-Z][a-z]+)*\b",
        "build_refs": r"build\s*#?\d+",
        "numbers_with_units": r"\d+(?:\.\d+)?(?:x|%|/kT|hrs?|min)",
    }

    per_field = {}
    total = 0

    for field in ABLATION_FIELDS:
        if field not in ccs:
            continue
        text = field_to_text(ccs[field])
        count = 0
        for pat_name, pattern in patterns.items():
            matches = re.findall(pattern, text, re.IGNORECASE)
            count += len(matches)
        per_field[field] = count
        total += count

    return total, per_field


def gate3_specificity(ccs: dict) -> list[tuple]:
    """Gate 3: measure specificity impact of removing each field."""
    base_spec, base_per_field = compute_specificity(ccs)
    results = []

    for field in ABLATION_FIELDS:
        ablated = ablate(ccs, field)
        abl_spec, _ = compute_specificity(ablated)
        drop = base_spec - abl_spec
        # Normalize by field's share of markers
        share = base_per_field.get(field, 0) / base_spec if base_spec else 0
        results.append((field, drop, share, base_spec, abl_spec))

    return sorted(results, key=lambda x: x[1], reverse=True)


# === Gate 4: Information Density ===

def compute_density(ccs: dict) -> tuple[float, dict]:
    """Compute information density: unique terms / total tokens per field."""
    per_field = {}
    for field in ABLATION_FIELDS:
        if field not in ccs:
            continue
        text = field_to_text(ccs[field])
        tokens = text.split()
        unique_terms = extract_terms(text, min_len=3)
        density = len(unique_terms) / len(tokens) if tokens else 0
        per_field[field] = {
            "tokens": len(tokens),
            "unique_terms": len(unique_terms),
            "density": round(density, 3),
        }
    return per_field


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Multi-gate CCS ablation")
    parser.add_argument("--gate", type=int, help="Run only specified gate (2, 3, or 4)")
    args = parser.parse_args()

    ccs = get_current_ccs()

    print("=" * 70)
    print("MULTI-GATE CCS ABLATION")
    print("=" * 70)

    # Gate 1 reference (from purpose_ablation — not re-run, just noted)
    print("\n--- Gate 1: Embedding Identity (reference from purpose_ablation) ---")
    print("  Ranking: gist (2.50/kT) > goal (1.06/kT) > constraints (0.53/kT)")
    print("           > entities (0.28/kT) > episodic (0.15/kT)")
    print("  Winner: semantic_gist")

    # Gate 2: Coherence
    if args.gate is None or args.gate == 2:
        print("\n--- Gate 2: Inter-field Coherence ---")
        print("  Which field, when removed, breaks coherence most?")
        base_coh, _ = compute_coherence(ccs)
        print(f"  Base coherence: {base_coh:.4f}")
        g2 = gate2_coherence(ccs)
        print(f"  {'Field':<25} {'Drop':>8} {'Post-ablation':>14}")
        print(f"  {'-'*25} {'-'*8} {'-'*14}")
        for field, drop, base, post in g2:
            marker = " ★" if drop == max(r[1] for r in g2) else ""
            print(f"  {field:<25} {drop:>8.4f} {post:>14.4f}{marker}")
        winner_2 = g2[0][0] if g2 else "?"
        print(f"  Winner: {winner_2}")

    # Gate 3: Specificity
    if args.gate is None or args.gate == 3:
        print("\n--- Gate 3: Specificity (falsifiable markers) ---")
        print("  Which field carries the most falsifiable content?")
        base_spec, base_per = compute_specificity(ccs)
        print(f"  Base specificity: {base_spec} markers")
        g3 = gate3_specificity(ccs)
        print(f"  {'Field':<25} {'Drop':>6} {'Share':>7} {'Markers'}")
        print(f"  {'-'*25} {'-'*6} {'-'*7} {'-'*8}")
        for field, drop, share, base, post in g3:
            marker = " ★" if drop == max(r[1] for r in g3) else ""
            print(f"  {field:<25} {drop:>6} {share:>7.1%} {base_per.get(field, 0):>8}{marker}")
        winner_3 = g3[0][0] if g3 else "?"
        print(f"  Winner: {winner_3}")

    # Gate 4: Information Density
    if args.gate is None or args.gate == 4:
        print("\n--- Gate 4: Information Density ---")
        print("  Unique terms / total tokens per field")
        density = compute_density(ccs)
        sorted_density = sorted(density.items(), key=lambda x: x[1]["density"], reverse=True)
        print(f"  {'Field':<25} {'Tokens':>7} {'Unique':>7} {'Density':>8}")
        print(f"  {'-'*25} {'-'*7} {'-'*7} {'-'*8}")
        for field, d in sorted_density:
            marker = " ★" if d["density"] == max(x[1]["density"] for x in sorted_density) else ""
            print(f"  {field:<25} {d['tokens']:>7} {d['unique_terms']:>7} {d['density']:>8.3f}{marker}")
        winner_4 = sorted_density[0][0] if sorted_density else "?"
        print(f"  Winner: {winner_4}")

    # Cross-gate comparison
    if args.gate is None:
        print("\n" + "=" * 70)
        print("CROSS-GATE COMPARISON")
        print("=" * 70)
        print(f"  Gate 1 (embedding identity):  semantic_gist")
        print(f"  Gate 2 (coherence):           {winner_2}")
        print(f"  Gate 3 (specificity):         {winner_3}")
        print(f"  Gate 4 (density):             {winner_4}")

        winners = ["semantic_gist", winner_2, winner_3, winner_4]
        if len(set(winners)) > 1:
            print(f"\n  MULTI-GATE EFFECT DETECTED: winners differ across gates.")
            print(f"  Optimizing for embedding identity alone would miss the importance")
            print(f"  of {', '.join(set(winners) - {'semantic_gist'})} at other evaluation levels.")
        else:
            print(f"\n  No multi-gate effect: {winners[0]} wins all gates.")


if __name__ == "__main__":
    main()
