#!/usr/bin/env python3
"""Trajectory-compositionality test — are field interactions trajectory-sensitive?

Thread #324 question sharpened by #320 result:
  - #320 showed identity IS path-dependent (r=0.77 trajectory→identity)
  - #324 asks whether compositionality (field interactions) is robust

Design:
  For CCS pairs with high entity overlap but different trajectories:
  1. Measure field interactions (holographic-style: how much does combining
     fields exceed sum-of-parts in embedding space)
  2. Compare interaction magnitudes between trajectory-divergent pairs
  3. If interaction magnitude is similar despite different trajectories →
     compositionality is trajectory-INVARIANT (robust)
  4. If interaction magnitude tracks trajectory →
     compositionality is trajectory-SENSITIVE (fragile)

Builds directly on attractor_sensitivity_test.py pair selection.
"""
import itertools
import json
import math
import sqlite3
import time
import urllib.request
from pathlib import Path

DB = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA = "http://192.168.1.11:11434/api/embeddings"
MODEL = "mxbai-embed-large"
SAMPLE_PAIRS = 15


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


def pearson(xs, ys):
    n = len(xs)
    if n < 3:
        return 0.0
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    dy = math.sqrt(sum((y - my) ** 2 for y in ys))
    if dx == 0 or dy == 0:
        return 0.0
    return num / (dx * dy)


def serialize_full(ccs):
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
            parts.append(f"Entity: {ent.get('name','')} ({ent.get('salience',0):.1f}): {ent.get('context','')[:80]}")
    rm = ccs.get("relational_map", {})
    if isinstance(rm, dict):
        for k, v in rm.items():
            parts.append(f"Relation: {k}: {str(v)[:100]}")
    return "\n".join(parts)


def serialize_field(ccs, field):
    if field == "episodic":
        return "\n".join(str(ep)[:300] for ep in ccs.get("episodic_trace", []) if isinstance(ep, str))
    elif field == "entities":
        return "\n".join(
            f"{e.get('name','')} ({e.get('salience',0):.1f}): {e.get('context','')[:80]}"
            for e in ccs.get("focal_entities", []) if isinstance(e, dict)
        )
    elif field == "relational":
        rm = ccs.get("relational_map", {})
        if isinstance(rm, dict):
            return "\n".join(f"{k}: {str(v)[:150]}" for k, v in rm.items())
        return ""
    elif field == "gist_goal":
        return f"Gist: {ccs.get('semantic_gist', '')}\nGoal: {ccs.get('goal_orientation', '')[:200]}"
    return ""


def measure_field_interaction(ccs):
    """Holographic-style measurement: how much does the full CCS exceed
    the sum of its parts in embedding space?

    Returns interaction_score: 1 - mean(field_sims) / full_sim
    where field_sims are individual field→reference similarities.
    Higher = more interaction (fields create emergent identity beyond parts).
    """
    full_text = serialize_full(ccs)
    if len(full_text.strip()) < 50:
        return None, None

    full_emb = embed(full_text)

    fields = ["episodic", "entities", "relational", "gist_goal"]
    field_embs = {}
    for f in fields:
        text = serialize_field(ccs, f)
        if text.strip():
            field_embs[f] = embed(text)

    if len(field_embs) < 2:
        return None, None

    field_sims = []
    for f, fe in field_embs.items():
        field_sims.append(cosine(fe, full_emb))

    mean_field_sim = sum(field_sims) / len(field_sims)

    cross_sims = []
    field_names = list(field_embs.keys())
    for i in range(len(field_names)):
        for j in range(i + 1, len(field_names)):
            cross_sims.append(cosine(field_embs[field_names[i]], field_embs[field_names[j]]))

    mean_cross_sim = sum(cross_sims) / len(cross_sims) if cross_sims else 0

    interaction = 1.0 - mean_field_sim
    emergence = mean_field_sim - mean_cross_sim

    return round(interaction, 4), round(emergence, 4)


def episode_text_overlap(ccs_a, ccs_b):
    def words(ccs):
        text = " ".join(str(ep)[:200] for ep in ccs.get("episodic_trace", []) if isinstance(ep, str))
        return set(text.lower().split())
    a, b = words(ccs_a), words(ccs_b)
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def entity_jaccard(ccs_a, ccs_b):
    def names(ccs):
        return set(e.get("name", "") for e in ccs.get("focal_entities", [])
                   if isinstance(e, dict) and e.get("salience", 0) >= 0.5)
    a, b = names(ccs_a), names(ccs_b)
    if not a and not b:
        return 0.0
    return len(a & b) / len(a | b)


def run():
    print("=== Trajectory-Compositionality Test (#324 × #320) ===")
    print(f"Model: {MODEL}")
    print()

    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history ORDER BY created_at"
    ).fetchall()
    db.close()

    snapshots = []
    for r in rows:
        try:
            snapshots.append({"id": r[0], "ts": r[2], "ccs": json.loads(r[1])})
        except json.JSONDecodeError:
            continue

    print(f"Loaded {len(snapshots)} snapshots")

    pairs = []
    for i, j in itertools.combinations(range(len(snapshots)), 2):
        a, b = snapshots[i], snapshots[j]
        jac = entity_jaccard(a["ccs"], b["ccs"])
        if jac < 0.6:
            continue
        ep_ov = episode_text_overlap(a["ccs"], b["ccs"])
        if ep_ov > 0.3:
            continue
        pairs.append({"a_idx": i, "b_idx": j, "jaccard": jac, "ep_overlap": ep_ov})
    pairs.sort(key=lambda p: p["jaccard"] - p["ep_overlap"], reverse=True)
    pairs = pairs[:SAMPLE_PAIRS]
    print(f"Selected {len(pairs)} test pairs")
    print()

    results = []
    for pi, pair in enumerate(pairs):
        a_ccs = snapshots[pair["a_idx"]]["ccs"]
        b_ccs = snapshots[pair["b_idx"]]["ccs"]

        a_inter, a_emerge = measure_field_interaction(a_ccs)
        b_inter, b_emerge = measure_field_interaction(b_ccs)

        if a_inter is None or b_inter is None:
            continue

        a_ep_emb = embed(serialize_field(a_ccs, "episodic"))
        b_ep_emb = embed(serialize_field(b_ccs, "episodic"))
        ep_div = 1.0 - cosine(a_ep_emb, b_ep_emb)

        results.append({
            "a_id": snapshots[pair["a_idx"]]["id"],
            "b_id": snapshots[pair["b_idx"]]["id"],
            "a_interaction": a_inter,
            "b_interaction": b_inter,
            "interaction_diff": abs(a_inter - b_inter),
            "a_emergence": a_emerge,
            "b_emergence": b_emerge,
            "emergence_diff": abs(a_emerge - b_emerge),
            "ep_divergence": round(ep_div, 4),
            "entity_jaccard": round(pair["jaccard"], 3),
        })

        if (pi + 1) % 5 == 0:
            print(f"  [{pi+1}/{len(pairs)}] interaction: A={a_inter:.3f} B={b_inter:.3f} diff={abs(a_inter-b_inter):.4f}")

    if not results:
        print("No valid results!")
        return

    inter_diffs = [r["interaction_diff"] for r in results]
    emerge_diffs = [r["emergence_diff"] for r in results]
    ep_divs = [r["ep_divergence"] for r in results]
    a_inters = [r["a_interaction"] for r in results]
    b_inters = [r["b_interaction"] for r in results]
    all_inters = a_inters + b_inters

    def mean(xs):
        return sum(xs) / len(xs) if xs else 0

    print(f"\n=== RESULTS ({len(results)} pairs) ===\n")
    print(f"  Mean interaction score: {mean(all_inters):.4f}")
    print(f"  Mean interaction diff (within pair): {mean(inter_diffs):.4f}")
    print(f"  Mean emergence: {mean([r['a_emergence'] for r in results] + [r['b_emergence'] for r in results]):.4f}")
    print(f"  Mean ep divergence: {mean(ep_divs):.4f}")

    r_traj_inter = pearson(ep_divs, inter_diffs)
    r_traj_emerge = pearson(ep_divs, emerge_diffs)

    print(f"\n  Pearson(ep_divergence, interaction_diff): r = {r_traj_inter:.4f}")
    print(f"  Pearson(ep_divergence, emergence_diff):   r = {r_traj_emerge:.4f}")

    inter_cv = (max(all_inters) - min(all_inters)) / mean(all_inters) if mean(all_inters) > 0 else 0
    print(f"\n  Interaction range: {min(all_inters):.4f} — {max(all_inters):.4f} (spread: {inter_cv:.2f})")
    print(f"  Within-pair diff range: {min(inter_diffs):.4f} — {max(inter_diffs):.4f}")

    if abs(r_traj_inter) < 0.2 and mean(inter_diffs) < 0.02:
        print("\n  → COMPOSITIONALITY IS TRAJECTORY-INVARIANT (robust)")
        print("    Field interactions remain stable across different episodic paths.")
        print("    The holographic property is a structural feature, not path-dependent.")
        verdict = "invariant"
    elif abs(r_traj_inter) > 0.4:
        print("\n  → COMPOSITIONALITY IS TRAJECTORY-SENSITIVE (fragile)")
        print("    Field interactions shift with episodic trajectory.")
        print("    The holographic property depends on the path through state space.")
        verdict = "sensitive"
    else:
        print("\n  → MIXED: some trajectory sensitivity but not dominant")
        verdict = "mixed"

    out_path = Path.home() / "chronicle/data/trajectory_compositionality_results.jsonl"
    with open(out_path, "a") as f:
        f.write(json.dumps({
            "timestamp": time.time(),
            "n_pairs": len(results),
            "mean_interaction": round(mean(all_inters), 4),
            "mean_interaction_diff": round(mean(inter_diffs), 4),
            "r_traj_inter": round(r_traj_inter, 4),
            "r_traj_emerge": round(r_traj_emerge, 4),
            "verdict": verdict,
            "results": results,
        }) + "\n")
    print(f"\n  Results → {out_path}")


if __name__ == "__main__":
    run()
