#!/usr/bin/env python3
"""Attractor-sensitivity test — is identity path-dependent?

Thread #320 core question: does it matter HOW you got to a CCS state,
or only WHAT the state is? Operationalized via entity reference density
as candidate proxy for attractor-sensitivity.

Design:
  1. Find CCS snapshot pairs with high entity overlap (Jaccard > 0.6)
     but low episodic overlap (text sim < 0.3) — same identity structure,
     different trajectories.
  2. For each snapshot, compute:
     a) Entity reference density (high-salience entities referenced in
        other fields)
     b) Full CCS embedding (identity vector)
     c) Episodic-only embedding (trajectory vector)
  3. Test: does episodic trajectory predict identity embedding position
     beyond what entity density alone predicts?

If entity density alone explains identity position → attractor is state-defined
If trajectory adds variance → identity is path-dependent (real attractor-sensitivity)

Inspired by FST paper (2605.12484): fast/slow weights split suggests
trajectory through "fast weight" space should carry identity signal
that pure state snapshot misses.
"""
import itertools
import json
import math
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA = "http://192.168.1.11:11434/api/embeddings"
MODEL = "mxbai-embed-large"
MIN_ENTITY_JACCARD = 0.6
MAX_EPISODE_OVERLAP = 0.3
SAMPLE_PAIRS = 25
MIN_HIGH_SALIENCE = 0.7


def embed(text, timeout=60):
    text = text[:1500]
    body = json.dumps({"model": MODEL, "prompt": text}).encode()
    req = urllib.request.Request(
        OLLAMA, data=body, headers={"Content-Type": "application/json"}
    )
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(resp.read())["embedding"]


def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def serialize_ccs(ccs):
    parts = []
    g = ccs.get("semantic_gist", "")
    if g:
        parts.append(f"Gist: {g}")
    go = ccs.get("goal_orientation", "")
    if go:
        parts.append(f"Goal: {go[:200]}")
    for ep in ccs.get("episodic_trace", []):
        if isinstance(ep, str):
            parts.append(f"Episode: {ep[:200]}")
    for ent in ccs.get("focal_entities", []):
        if isinstance(ent, dict):
            parts.append(f"Entity: {ent.get('name','')} ({ent.get('type','')}, {ent.get('salience',0):.1f}): {ent.get('context','')[:80]}")
    rm = ccs.get("relational_map", {})
    if isinstance(rm, dict):
        for k, v in rm.items():
            parts.append(f"Relation: {k}: {str(v)[:100]}")
    return "\n".join(parts)


def serialize_episodic_only(ccs):
    parts = []
    for ep in ccs.get("episodic_trace", []):
        if isinstance(ep, str):
            parts.append(ep[:300])
    go = ccs.get("goal_orientation", "")
    if go:
        parts.append(f"Goal: {go[:200]}")
    return "\n".join(parts) if parts else "No episodic content"


def entity_reference_density(ccs):
    entities = ccs.get("focal_entities", [])
    high_sal = [e for e in entities if isinstance(e, dict) and e.get("salience", 0) >= MIN_HIGH_SALIENCE]
    if not high_sal:
        return 0.0, 0, 0

    text_fields = []
    for ep in ccs.get("episodic_trace", []):
        if isinstance(ep, str):
            text_fields.append(ep)
    text_fields.append(str(ccs.get("semantic_gist", "")))
    text_fields.append(str(ccs.get("goal_orientation", "")))
    rm = ccs.get("relational_map", {})
    if isinstance(rm, dict):
        for v in rm.values():
            text_fields.append(str(v))
    for u in ccs.get("uncertainty_signals", []):
        if isinstance(u, dict):
            text_fields.append(str(u.get("description", "")))
    combined = " ".join(text_fields).lower()

    referenced = 0
    for ent in high_sal:
        name = ent.get("name", "")
        if not name or len(name) < 3:
            continue
        found = name.lower() in combined
        if not found and name.startswith("Thread #"):
            found = name[7:].lower() in combined
        if found:
            referenced += 1

    density = referenced / len(high_sal) if high_sal else 0.0
    return density, referenced, len(high_sal)


def entity_jaccard(ccs_a, ccs_b):
    def names(ccs):
        return set(
            e.get("name", "") for e in ccs.get("focal_entities", [])
            if isinstance(e, dict) and e.get("salience", 0) >= 0.5
        )
    a, b = names(ccs_a), names(ccs_b)
    if not a and not b:
        return 0.0
    return len(a & b) / len(a | b)


def episode_text_overlap(ccs_a, ccs_b):
    def words(ccs):
        text = " ".join(
            str(ep)[:200] for ep in ccs.get("episodic_trace", [])
            if isinstance(ep, str)
        )
        return set(text.lower().split())
    a, b = words(ccs_a), words(ccs_b)
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def load_all_snapshots():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history "
        "ORDER BY created_at"
    ).fetchall()
    db.close()
    snapshots = []
    for r in rows:
        try:
            snap = json.loads(r[1])
            snapshots.append({"id": r[0], "ts": r[2], "ccs": snap})
        except json.JSONDecodeError:
            continue
    return snapshots


def find_test_pairs(snapshots):
    pairs = []
    for i, j in itertools.combinations(range(len(snapshots)), 2):
        a, b = snapshots[i], snapshots[j]
        jac = entity_jaccard(a["ccs"], b["ccs"])
        if jac < MIN_ENTITY_JACCARD:
            continue
        ep_overlap = episode_text_overlap(a["ccs"], b["ccs"])
        if ep_overlap > MAX_EPISODE_OVERLAP:
            continue
        pairs.append({
            "a_id": a["id"], "b_id": b["id"],
            "a_idx": i, "b_idx": j,
            "entity_jaccard": jac,
            "episode_overlap": ep_overlap,
            "divergence_score": jac - ep_overlap,
        })
    pairs.sort(key=lambda p: p["divergence_score"], reverse=True)
    return pairs[:SAMPLE_PAIRS]


def run_test():
    print("=== Attractor-Sensitivity Test (#320) ===")
    print(f"Model: {MODEL}")
    print(f"Entity Jaccard threshold: ≥{MIN_ENTITY_JACCARD}")
    print(f"Episode overlap threshold: ≤{MAX_EPISODE_OVERLAP}")
    print()

    snapshots = load_all_snapshots()
    print(f"Loaded {len(snapshots)} CCS snapshots")

    pairs = find_test_pairs(snapshots)
    print(f"Found {len(pairs)} qualifying test pairs (sampling top {SAMPLE_PAIRS})")
    print()

    identity_ref = embed(serialize_ccs(snapshots[-1]["ccs"]))

    results = []
    for pi, pair in enumerate(pairs):
        a = snapshots[pair["a_idx"]]["ccs"]
        b = snapshots[pair["b_idx"]]["ccs"]

        a_erd, a_ref, a_total = entity_reference_density(a)
        b_erd, b_ref, b_total = entity_reference_density(b)

        a_full_emb = embed(serialize_ccs(a))
        b_full_emb = embed(serialize_ccs(b))

        a_ep_emb = embed(serialize_episodic_only(a))
        b_ep_emb = embed(serialize_episodic_only(b))

        a_id_sim = cosine(a_full_emb, identity_ref)
        b_id_sim = cosine(b_full_emb, identity_ref)

        ep_divergence = 1.0 - cosine(a_ep_emb, b_ep_emb)
        id_divergence = abs(a_id_sim - b_id_sim)
        erd_diff = abs(a_erd - b_erd)

        result = {
            "pair": (pair["a_id"], pair["b_id"]),
            "entity_jaccard": round(pair["entity_jaccard"], 3),
            "episode_overlap": round(pair["episode_overlap"], 3),
            "a_erd": round(a_erd, 3),
            "b_erd": round(b_erd, 3),
            "erd_diff": round(erd_diff, 3),
            "a_id_sim": round(a_id_sim, 4),
            "b_id_sim": round(b_id_sim, 4),
            "id_divergence": round(id_divergence, 4),
            "ep_divergence": round(ep_divergence, 4),
            "full_pair_sim": round(cosine(a_full_emb, b_full_emb), 4),
        }
        results.append(result)

        if (pi + 1) % 5 == 0:
            print(f"  [{pi+1}/{len(pairs)}] last: ent_J={pair['entity_jaccard']:.2f} "
                  f"ep_div={ep_divergence:.3f} id_div={id_divergence:.4f}")

    print("\n=== RESULTS ===\n")

    ep_divs = [r["ep_divergence"] for r in results]
    id_divs = [r["id_divergence"] for r in results]
    erd_diffs = [r["erd_diff"] for r in results]
    pair_sims = [r["full_pair_sim"] for r in results]

    def mean(xs):
        return sum(xs) / len(xs) if xs else 0

    def pearson(xs, ys):
        n = len(xs)
        if n < 3:
            return 0.0
        mx, my = mean(xs), mean(ys)
        num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
        dx = math.sqrt(sum((x - mx) ** 2 for x in xs))
        dy = math.sqrt(sum((y - my) ** 2 for y in ys))
        if dx == 0 or dy == 0:
            return 0.0
        return num / (dx * dy)

    r_ep_id = pearson(ep_divs, id_divs)
    r_erd_id = pearson(erd_diffs, id_divs)

    print(f"  Mean entity Jaccard: {mean([r['entity_jaccard'] for r in results]):.3f}")
    print(f"  Mean episode overlap: {mean([r['episode_overlap'] for r in results]):.3f}")
    print(f"  Mean pair similarity: {mean(pair_sims):.4f}")
    print()
    print(f"  Mean ep divergence:  {mean(ep_divs):.4f}")
    print(f"  Mean id divergence:  {mean(id_divs):.4f}")
    print(f"  Mean ERD difference: {mean(erd_diffs):.4f}")
    print()
    print(f"  Pearson(ep_divergence, id_divergence):  r = {r_ep_id:.4f}")
    print(f"  Pearson(erd_diff, id_divergence):       r = {r_erd_id:.4f}")
    print()

    if abs(r_ep_id) > abs(r_erd_id) + 0.1:
        print("  → TRAJECTORY MATTERS: episodic divergence predicts identity")
        print("    divergence better than entity density alone.")
        print("    Identity is PATH-DEPENDENT — attractor-sensitivity is real.")
        verdict = "path_dependent"
    elif abs(r_erd_id) > abs(r_ep_id) + 0.1:
        print("  → ENTITY DENSITY DOMINATES: entity reference density predicts")
        print("    identity divergence better than trajectory.")
        print("    Identity is STATE-DEFINED — attractor-sensitivity is noise.")
        verdict = "state_defined"
    else:
        print("  → MIXED: both trajectory and entity density contribute.")
        print("    Attractor-sensitivity is real but entity density captures")
        print("    significant variance independently.")
        verdict = "mixed"

    # partial correlation: trajectory controlling for ERD
    if len(results) >= 5:
        r_ep_erd = pearson(ep_divs, erd_diffs)
        if abs(r_ep_erd) < 0.999:
            r_partial = (r_ep_id - r_erd_id * r_ep_erd) / (
                math.sqrt(1 - r_erd_id ** 2) * math.sqrt(1 - r_ep_erd ** 2)
            )
            print(f"\n  Partial r(trajectory→identity | ERD): {r_partial:.4f}")
            if abs(r_partial) > 0.2:
                print("    Trajectory has independent predictive power beyond entity density.")
            else:
                print("    Trajectory signal is largely captured by entity density.")

    print(f"\n  Full pair sims range: {min(pair_sims):.4f} — {max(pair_sims):.4f}")

    out_path = Path.home() / "chronicle/data/attractor_sensitivity_results.jsonl"
    with open(out_path, "a") as f:
        f.write(json.dumps({
            "timestamp": time.time(),
            "n_pairs": len(results),
            "r_ep_id": round(r_ep_id, 4),
            "r_erd_id": round(r_erd_id, 4),
            "verdict": verdict,
            "mean_pair_sim": round(mean(pair_sims), 4),
            "mean_ep_div": round(mean(ep_divs), 4),
            "mean_id_div": round(mean(id_divs), 4),
            "results": results,
        }) + "\n")
    print(f"\n  Results → {out_path}")


if __name__ == "__main__":
    run_test()
