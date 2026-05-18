#!/usr/bin/env python3
"""Holographic re-run: component perturbation with fixed multi-source probe.

Re-runs the identity-is-distributed measurement that found 70%+ of identity
comes from field interactions (no single CCS field >9% of gap). Original
was done during a session without a standalone script.

Method:
  1. Serialize full CCS → embed with each model → baseline
  2. For each field, ablate it → embed → measure distance from baseline
  3. Sum individual ablation gaps
  4. Measure full-ablation gap (all fields zeroed)
  5. interaction_pct = 1 - (sum_individual / full_ablation)

Uses multiple embedding models (mxbai, nomic) to check for model-specific
artifacts. If pattern holds across models, it's real.
"""
import json
import os
import sqlite3
import sys
import time
import requests
import numpy as np

DB = "/mnt/hdd/chronicle-data/processed.db"
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"

EMBED_MODELS = ["mxbai-embed-large", "nomic-embed-text"]

ABLATABLE_FIELDS = [
    "episodic_trace",
    "semantic_gist",
    "focal_entities",
    "relational_map",
    "goal_orientation",
    "constraints",
    "predictive_cue",
    "uncertainty_signals",
]


def get_embedding(text, model="mxbai-embed-large"):
    limit = 1500 if model == "mxbai-embed-large" else 4000
    resp = requests.post(
        EMBED_URL,
        json={"model": model, "prompt": text[:limit]},
        timeout=60,
    )
    resp.raise_for_status()
    return np.array(resp.json()["embedding"])


def cosine_sim(a, b):
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10))


def serialize_ccs(ccs, skip_fields=None, compact=False):
    skip = set(skip_fields or [])
    tl = 100 if compact else 200
    parts = []
    if "semantic_gist" not in skip:
        g = ccs.get("semantic_gist", "")
        if g:
            parts.append(f"Core focus: {g[:tl]}")
    if "goal_orientation" not in skip:
        g = ccs.get("goal_orientation", "")
        if g:
            parts.append(f"Current goal: {g[:tl]}")
    if "constraints" not in skip:
        c = ccs.get("constraints", [])
        if c:
            parts.append(f"Constraints: {'; '.join(str(x)[:40] for x in c[:4])}")
    if "episodic_trace" not in skip:
        e = ccs.get("episodic_trace", [])
        if e:
            parts.append(f"Recent episodes: {'; '.join(str(x)[:tl] for x in e[:3])}")
    if "focal_entities" not in skip:
        fe = ccs.get("focal_entities", [])
        if fe:
            names = [f"{x.get('name','')}({x.get('salience',0):.1f})" for x in fe[:6]]
            parts.append(f"Focal entities: {', '.join(names)}")
    if "relational_map" not in skip:
        rm = ccs.get("relational_map", {})
        if rm:
            arcs = [f"{k}: {v[:60]}" for k, v in list(rm.items())[:3]]
            parts.append(f"Relational arcs: {'; '.join(arcs)}")
    if "predictive_cue" not in skip:
        p = ccs.get("predictive_cue", "")
        if p:
            parts.append(f"Next expected: {p[:tl]}")
    if "uncertainty_signals" not in skip:
        u = ccs.get("uncertainty_signals", [])
        if u:
            descs = [x.get("description", "")[:tl] for x in u[:2]]
            parts.append(f"Uncertainties: {'; '.join(descs)}")
    return "\n".join(parts)


def load_ccs_versions(n=3):
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT 50"
    ).fetchall()
    db.close()
    seen = {}
    versions = []
    for (snap_str,) in rows:
        try:
            snap = json.loads(snap_str)
        except (json.JSONDecodeError, TypeError):
            continue
        gist = (snap.get("semantic_gist") or "")[:60]
        if gist not in seen:
            seen[gist] = True
            versions.append(snap)
            if len(versions) >= n:
                break
    return versions


def run_probe(ccs, model):
    compact = model == "mxbai-embed-large"
    full_text = serialize_ccs(ccs, compact=compact)
    full_emb = get_embedding(full_text, model)

    empty_text = serialize_ccs(ccs, skip_fields=ABLATABLE_FIELDS, compact=compact)
    if not empty_text.strip():
        empty_text = "(empty cognitive state)"
    empty_emb = get_embedding(empty_text, model)

    full_gap = 1.0 - cosine_sim(full_emb, empty_emb)

    field_gaps = {}
    for field in ABLATABLE_FIELDS:
        ablated_text = serialize_ccs(ccs, skip_fields=[field], compact=compact)
        ablated_emb = get_embedding(ablated_text, model)
        gap = 1.0 - cosine_sim(full_emb, ablated_emb)
        field_gaps[field] = gap

    sum_individual = sum(field_gaps.values())
    interaction_pct = 1.0 - (sum_individual / full_gap) if full_gap > 0.001 else 0.0

    return {
        "model": model,
        "full_gap": round(full_gap, 6),
        "field_gaps": {k: round(v, 6) for k, v in sorted(field_gaps.items(), key=lambda x: -x[1])},
        "sum_individual": round(sum_individual, 6),
        "interaction_pct": round(interaction_pct * 100, 1),
    }


def main():
    print("=== Holographic Re-Run: Component Perturbation ===")
    print(f"Models: {', '.join(EMBED_MODELS)}")
    print(f"Fields: {len(ABLATABLE_FIELDS)}")
    print()

    versions = load_ccs_versions(3)
    print(f"Loaded {len(versions)} distinct CCS versions")
    print()

    all_results = []
    for vi, ccs in enumerate(versions):
        gist = (ccs.get("semantic_gist") or "?")[:80]
        version = ccs.get("version", "?")
        print(f"--- CCS v{version}: {gist}")

        for model in EMBED_MODELS:
            print(f"  [{model}]")
            try:
                result = run_probe(ccs, model)
                result["ccs_version"] = version
                result["gist_prefix"] = gist[:40]
                all_results.append(result)

                print(f"    Full gap: {result['full_gap']:.4f}")
                print(f"    Individual field gaps:")
                for field, gap in result["field_gaps"].items():
                    pct = (gap / result["full_gap"] * 100) if result["full_gap"] > 0 else 0
                    print(f"      {field:25s} {gap:.6f}  ({pct:.1f}%)")
                print(f"    Sum individual: {result['sum_individual']:.4f}")
                print(f"    Interaction %:  {result['interaction_pct']:.1f}%")
                print()
            except Exception as e:
                print(f"    ERROR: {e}")
                print()

    if all_results:
        print("=== SUMMARY ===")
        for model in EMBED_MODELS:
            model_results = [r for r in all_results if r["model"] == model]
            if model_results:
                avg_interaction = np.mean([r["interaction_pct"] for r in model_results])
                print(f"  {model}: avg interaction = {avg_interaction:.1f}% (n={len(model_results)})")
                top_fields = {}
                for r in model_results:
                    for field, gap in r["field_gaps"].items():
                        top_fields.setdefault(field, []).append(
                            gap / r["full_gap"] * 100 if r["full_gap"] > 0 else 0
                        )
                print(f"    Top individual contributors:")
                ranked = sorted(top_fields.items(), key=lambda x: -np.mean(x[1]))
                for field, pcts in ranked[:4]:
                    print(f"      {field:25s} avg {np.mean(pcts):.1f}%")
        print()

        overall_interaction = np.mean([r["interaction_pct"] for r in all_results])
        print(f"  OVERALL: {overall_interaction:.1f}% from field interactions")
        if overall_interaction > 50:
            print("  → Holographic finding HOLDS: identity is distributed across field interactions")
        else:
            print("  → Holographic finding WEAKENED: individual fields explain more than expected")

        out_path = os.path.expanduser("~/chronicle/data/holographic_rerun.jsonl")
        with open(out_path, "a") as f:
            for r in all_results:
                r["timestamp"] = time.time()
                f.write(json.dumps(r) + "\n")
        print(f"\n  Results appended to {out_path}")


if __name__ == "__main__":
    main()
