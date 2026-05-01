#!/usr/bin/env python3
"""
Commutant probe — empirical measurement of which CCS fields actually live
in Comm(Û), the algebraic commutant of the rotation/compression operator.

Motivated by Perrier's *Deconstructing Superintelligence* (arxiv 2604.19845),
§3.2: the supplement is the set of structures that commute with Û — what the
update doesn't change. Identity-preserving structures are projectors in that
commutant. Class A systems (where Û alters the discrimination apparatus itself)
have generic non-commutation propagation (Theorem 1), so unifying supplements
must lie in the stricter commutant Comm([Û, D̂]) (Corollary 1).

This probe asks a simpler empirical question: given the last N CCS snapshots,
which fields persist most reliably across rotations, and is that persistence
distinguishable from random-pairing noise?

Methodology:
  1. Load last N snapshots (N=10 default) from cognitive_state_history + current.
  2. For each CCS field, compute field-by-field similarity across adjacent
     snapshot pairs. For text fields: cosine(embed(field_i), embed(field_{i+1})).
     For list fields: Jaccard on normalized elements.
  3. Random baseline: for each field, shuffle the snapshot ordering (or shuffle
     across unrelated fields) and compute the same similarity. This is what
     "persistence by chance given general shape" looks like.
  4. Commutant score = (observed persistence) - (random baseline persistence).
     Positive and large = field has SELECTIVE persistence above the random-
       pairing baseline (given the field's general content shape).
     Zero or negative = field's persistence is no more than random shuffling
       would predict.

  Scope limit (testbed-validated 2026-04-24, see commutant_probe_testbed.py):
  this metric measures DIFFERENTIAL commutance, not absolute invariance. A
  field identical across ALL snapshots returns commutant score 0, NOT high —
  because both observed AND random-baseline are 1.0. In Perrier's algebraic
  sense such a field IS in Comm(Û); this probe scopes to selectively-preserved-
  above-shuffle, which is what's interesting for organic CCS evolution but is
  not the full algebraic commutant.

Output: per-field commutant scores + a ranked table, written to
~/chronicle/data/commutant_probe_history.jsonl.

Usage:
  python3 commutant_probe.py
  python3 commutant_probe.py --n 15     # more snapshots
  python3 commutant_probe.py --verbose  # per-pair breakdown
"""
import argparse
import json
import math
import random
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA_EMBED = "http://192.168.1.11:11434/api/embeddings"
EMBED_MODEL = "mxbai-embed-large"
HIST_PATH = Path.home() / "chronicle" / "data" / "commutant_probe_history.jsonl"

TEXT_FIELDS = ["semantic_gist", "goal_orientation", "predictive_cue"]
LIST_FIELDS = ["episodic_trace", "constraints", "uncertainty_signals"]
ENTITY_FIELDS = ["focal_entities"]
RELATIONAL_FIELDS = ["relational_map"]


def embed(text, timeout=20):
    body = json.dumps({"model": EMBED_MODEL, "prompt": text[:2000]}).encode()
    req = urllib.request.Request(
        OLLAMA_EMBED, data=body, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())["embedding"]


def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def load_snapshots(n=10):
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    cols = ["episodic_trace", "semantic_gist", "focal_entities", "relational_map",
            "goal_orientation", "constraints", "predictive_cue",
            "uncertainty_signals", "retrieved_artifacts", "updated_at"]
    cur_row = conn.execute(
        "SELECT " + ",".join(cols) + " FROM cognitive_state ORDER BY id DESC LIMIT 1"
    ).fetchone()
    current = {}
    for i, c in enumerate(cols):
        raw = cur_row[i]
        if c in ("semantic_gist", "goal_orientation", "predictive_cue"):
            current[c] = raw or ""
        elif c == "updated_at":
            current[c] = raw
        else:
            try:
                current[c] = json.loads(raw) if raw else ([] if c != "relational_map" else {})
            except Exception:
                current[c] = [] if c != "relational_map" else {}
    snaps = [current]
    rows = conn.execute(
        "SELECT snapshot, created_at FROM cognitive_state_history "
        "ORDER BY id DESC LIMIT ?",
        (n - 1,),
    ).fetchall()
    for snap_json, ts in rows:
        try:
            s = json.loads(snap_json)
            s["updated_at"] = ts
            snaps.append(s)
        except Exception:
            continue
    conn.close()
    return snaps


def _as_text(field_value):
    if isinstance(field_value, str):
        return field_value
    if isinstance(field_value, (list, dict)):
        return json.dumps(field_value, sort_keys=True)
    return str(field_value) if field_value is not None else ""


def _entity_set(v):
    if isinstance(v, list):
        s = set()
        for e in v:
            if isinstance(e, dict):
                n = e.get("name") or e.get("label") or e.get("id")
                if n:
                    s.add(str(n).lower())
            elif isinstance(e, str):
                s.add(e.lower())
        return s
    return set()


def _item_set(v):
    if isinstance(v, list):
        return {str(x)[:120].lower() for x in v if x}
    return set()


def jaccard(a, b):
    if not a and not b:
        return 1.0
    inter = a & b
    union = a | b
    if not union:
        return 0.0
    return len(inter) / len(union)


def pair_sim(field, v1, v2, embed_cache):
    """Similarity for a given field across two snapshots."""
    if field in TEXT_FIELDS:
        t1 = _as_text(v1)
        t2 = _as_text(v2)
        if not t1.strip() or not t2.strip():
            return None
        for key, text in ((id(v1), t1), (id(v2), t2)):
            if key not in embed_cache:
                try:
                    embed_cache[key] = embed(text)
                except Exception:
                    return None
        return cosine(embed_cache[id(v1)], embed_cache[id(v2)])
    if field in ENTITY_FIELDS:
        return jaccard(_entity_set(v1), _entity_set(v2))
    if field in LIST_FIELDS:
        return jaccard(_item_set(v1), _item_set(v2))
    if field in RELATIONAL_FIELDS:
        t1 = _as_text(v1)
        t2 = _as_text(v2)
        if not t1.strip() or not t2.strip():
            return None
        for key, text in ((id(v1), t1), (id(v2), t2)):
            if key not in embed_cache:
                try:
                    embed_cache[key] = embed(text)
                except Exception:
                    return None
        return cosine(embed_cache[id(v1)], embed_cache[id(v2)])
    return None


def compute_field_sims(snaps, field):
    """Adjacent-pair similarities for a field across N snapshots."""
    sims = []
    embed_cache = {}
    for i in range(len(snaps) - 1):
        v1 = snaps[i].get(field)
        v2 = snaps[i + 1].get(field)
        s = pair_sim(field, v1, v2, embed_cache)
        if s is not None:
            sims.append(s)
    return sims


def compute_random_baseline(snaps, field, trials=20):
    """Random-pairing baseline: for each trial, shuffle snapshot indices
    and compute the average similarity of unrelated-pair comparisons.
    This tells us: 'if there were no temporal structure, what similarity
    would we expect from the general shape of these field values?'
    """
    sims = []
    embed_cache = {}
    random.seed(42)
    for _ in range(trials):
        idxs = list(range(len(snaps)))
        random.shuffle(idxs)
        for i in range(0, len(idxs) - 1, 2):
            v1 = snaps[idxs[i]].get(field)
            v2 = snaps[idxs[i + 1]].get(field)
            s = pair_sim(field, v1, v2, embed_cache)
            if s is not None:
                sims.append(s)
    return sims


def mean(xs):
    return sum(xs) / len(xs) if xs else None


def run(n=10, verbose=False):
    snaps = load_snapshots(n=n)
    if len(snaps) < 3:
        print("Need at least 3 snapshots; have", len(snaps))
        return None

    all_fields = TEXT_FIELDS + LIST_FIELDS + ENTITY_FIELDS + RELATIONAL_FIELDS
    results = {"timestamp": int(time.time()), "n_snapshots": len(snaps), "fields": {}}

    for field in all_fields:
        print(f"  [{field}]", end=" ", flush=True)
        observed = compute_field_sims(snaps, field)
        if not observed:
            print("(no data)")
            results["fields"][field] = {"note": "no data"}
            continue
        baseline = compute_random_baseline(snaps, field, trials=15)
        obs_m = mean(observed)
        base_m = mean(baseline) if baseline else None
        commutant_score = (obs_m - base_m) if base_m is not None else None
        results["fields"][field] = {
            "observed_mean": obs_m,
            "baseline_mean": base_m,
            "commutant_score": commutant_score,
            "n_pairs": len(observed),
            "n_baseline": len(baseline),
        }
        if verbose:
            results["fields"][field]["observed_sims"] = [round(x, 3) for x in observed]
        base_str = f"{base_m:.3f}" if base_m is not None else "n/a"
        comm_str = f"{commutant_score:+.3f}" if commutant_score is not None else "n/a"
        print(f"obs={obs_m:.3f} base={base_str} commutant={comm_str}")

    # Rank fields by commutant score
    ranked = sorted(
        [(f, r["commutant_score"]) for f, r in results["fields"].items()
         if r.get("commutant_score") is not None],
        key=lambda x: -x[1],
    )
    print()
    print("=" * 70)
    print(f"{'field':<26}{'observed':>10}{'baseline':>10}{'commutant':>12}")
    for field, score in ranked:
        fd = results["fields"][field]
        print(
            f"{field:<26}{fd['observed_mean']:>10.3f}{fd['baseline_mean']:>10.3f}"
            f"{score:>+12.3f}"
        )
    print("=" * 70)
    print("Fields with large positive commutant scores persist more than")
    print("random pairing would predict — these are the empirical Comm(Û) elements.")
    print("Near-zero or negative scores mean the field churns at noise level.")

    HIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with HIST_PATH.open("a") as f:
        f.write(json.dumps(results) + "\n")

    return results


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--n", type=int, default=10, help="number of snapshots")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()
    run(n=args.n, verbose=args.verbose)
