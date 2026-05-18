#!/usr/bin/env python3
"""Architecture-class test v2: L2 distance decomposition.

The cosine-distance version showed the additive hash baseline at 73.5%
interaction — which means cosine distance ITSELF creates interaction
artifacts via normalization. This version runs L2 (Euclidean) distance
alongside cosine to decompose the metric contribution from the real signal.

With L2 on strictly additive vectors, interaction should be ≤0% (triangle
inequality). Any positive L2 interaction in transformer embeddings is
genuine contextual coupling.
"""

import json
import os
import sqlite3
import sys
import time
import numpy as np
import requests
from sklearn.feature_extraction.text import HashingVectorizer

DB = "/mnt/hdd/chronicle-data/processed.db"
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"

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


def get_ollama_embedding(text, model="mxbai-embed-large"):
    limit = 1500 if model == "mxbai-embed-large" else 4000
    resp = requests.post(
        EMBED_URL,
        json={"model": model, "prompt": text[:limit]},
        timeout=60,
    )
    resp.raise_for_status()
    return np.array(resp.json()["embedding"])


def cosine_dist(a, b):
    na, nb = np.linalg.norm(a), np.linalg.norm(b)
    if na < 1e-10 or nb < 1e-10:
        return 1.0
    return 1.0 - float(np.dot(a, b) / (na * nb))


def l2_dist(a, b):
    return float(np.linalg.norm(a - b))


def serialize_ccs(ccs, skip_fields=None):
    skip = set(skip_fields or [])
    parts = []
    if "semantic_gist" not in skip:
        g = ccs.get("semantic_gist", "")
        if g:
            parts.append(f"Core focus: {g[:200]}")
    if "goal_orientation" not in skip:
        g = ccs.get("goal_orientation", "")
        if g:
            parts.append(f"Current goal: {g[:200]}")
    if "constraints" not in skip:
        c = ccs.get("constraints", [])
        if c:
            parts.append(f"Constraints: {'; '.join(str(x)[:40] for x in c[:4])}")
    if "episodic_trace" not in skip:
        e = ccs.get("episodic_trace", [])
        if e:
            parts.append(f"Recent episodes: {'; '.join(str(x)[:200] for x in e[:3])}")
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
            parts.append(f"Next expected: {p[:200]}")
    if "uncertainty_signals" not in skip:
        u = ccs.get("uncertainty_signals", [])
        if u:
            descs = [x.get("description", "")[:200] for x in u[:2]]
            parts.append(f"Uncertainties: {'; '.join(descs)}")
    return "\n".join(parts)


def load_ccs_versions(n=5):
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT 200"
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


class HashEmbedder:
    def __init__(self):
        self.vectorizer = HashingVectorizer(
            n_features=4096, ngram_range=(1, 2), norm=None, alternate_sign=False,
        )

    def embed(self, text):
        return self.vectorizer.transform([text]).toarray()[0]


def measure_interaction(full_emb, empty_emb, field_embs, dist_fn):
    full_gap = dist_fn(full_emb, empty_emb)
    if full_gap < 1e-10:
        return {"full_gap": 0, "sum_individual": 0, "interaction_pct": 0, "field_pcts": {}}
    field_gaps = {}
    for field, ablated_emb in field_embs.items():
        field_gaps[field] = dist_fn(full_emb, ablated_emb)
    sum_ind = sum(field_gaps.values())
    interaction = (1.0 - sum_ind / full_gap) * 100
    field_pcts = {k: (v / full_gap * 100) for k, v in field_gaps.items()}
    return {
        "full_gap": round(full_gap, 6),
        "sum_individual": round(sum_ind, 6),
        "interaction_pct": round(interaction, 1),
        "field_pcts": {k: round(v, 1) for k, v in sorted(field_pcts.items(), key=lambda x: -x[1])},
    }


def run_for_ccs(ccs, embed_fn, name):
    full_text = serialize_ccs(ccs)
    full_emb = embed_fn(full_text)

    empty_text = serialize_ccs(ccs, skip_fields=ABLATABLE_FIELDS)
    if not empty_text.strip():
        empty_text = "(empty cognitive state)"
    empty_emb = embed_fn(empty_text)

    field_embs = {}
    for field in ABLATABLE_FIELDS:
        ablated_text = serialize_ccs(ccs, skip_fields=[field])
        field_embs[field] = embed_fn(ablated_text)

    cosine_result = measure_interaction(full_emb, empty_emb, field_embs, cosine_dist)
    l2_result = measure_interaction(full_emb, empty_emb, field_embs, l2_dist)

    return {
        "model": name,
        "cosine": cosine_result,
        "l2": l2_result,
    }


def main():
    print("=" * 70)
    print("ARCHITECTURE-CLASS TEST v2: L2 DECOMPOSITION")
    print("Separating metric artifact from genuine interaction")
    print("=" * 70)

    versions = load_ccs_versions(5)
    print(f"\nLoaded {len(versions)} CCS versions\n")

    hasher = HashEmbedder()

    all_results = []

    for vi, ccs in enumerate(versions):
        v = ccs.get("version", "?")
        gist = (ccs.get("semantic_gist") or "?")[:50]
        print(f"{'─' * 60}")
        print(f"CCS v{v}: {gist}")

        # Transformer (mxbai)
        print(f"\n  [mxbai-embed-large] (transformer)")
        r = run_for_ccs(ccs, lambda t: get_ollama_embedding(t, "mxbai-embed-large"), "mxbai")
        r["ccs_version"] = v
        all_results.append(r)
        print(f"    Cosine interaction: {r['cosine']['interaction_pct']:.1f}%  (gap={r['cosine']['full_gap']:.4f})")
        print(f"    L2 interaction:     {r['l2']['interaction_pct']:.1f}%  (gap={r['l2']['full_gap']:.4f})")

        # Hash (additive baseline)
        print(f"\n  [hash-4096] (additive baseline)")
        r = run_for_ccs(ccs, hasher.embed, "hash")
        r["ccs_version"] = v
        all_results.append(r)
        print(f"    Cosine interaction: {r['cosine']['interaction_pct']:.1f}%  (gap={r['cosine']['full_gap']:.4f})")
        print(f"    L2 interaction:     {r['l2']['interaction_pct']:.1f}%  (gap={r['l2']['full_gap']:.4f})")

    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print(f"{'=' * 70}")

    for model_name in ["mxbai", "hash"]:
        results = [r for r in all_results if r["model"] == model_name]
        if not results:
            continue
        cos_ints = [r["cosine"]["interaction_pct"] for r in results]
        l2_ints = [r["l2"]["interaction_pct"] for r in results]
        label = "transformer" if model_name == "mxbai" else "additive"
        print(f"\n  {model_name} ({label}):")
        print(f"    Cosine interaction: {np.mean(cos_ints):.1f}% ± {np.std(cos_ints):.1f}%")
        print(f"    L2 interaction:     {np.mean(l2_ints):.1f}% ± {np.std(l2_ints):.1f}%")

    mxbai_cos = [r["cosine"]["interaction_pct"] for r in all_results if r["model"] == "mxbai"]
    mxbai_l2 = [r["l2"]["interaction_pct"] for r in all_results if r["model"] == "mxbai"]
    hash_cos = [r["cosine"]["interaction_pct"] for r in all_results if r["model"] == "hash"]
    hash_l2 = [r["l2"]["interaction_pct"] for r in all_results if r["model"] == "hash"]

    print(f"\n{'─' * 60}")
    print("DECOMPOSITION:")
    if hash_l2:
        h_l2 = np.mean(hash_l2)
        print(f"  Hash L2 interaction (should be ≤0 if additive): {h_l2:.1f}%")
    if hash_cos and hash_l2:
        metric_artifact = np.mean(hash_cos) - np.mean(hash_l2)
        print(f"  Cosine metric artifact (hash cosine - hash L2): {metric_artifact:.1f}%")
    if mxbai_l2:
        m_l2 = np.mean(mxbai_l2)
        print(f"  Transformer L2 interaction (genuine coupling):  {m_l2:.1f}%")
    if mxbai_l2 and hash_l2:
        genuine = np.mean(mxbai_l2) - np.mean(hash_l2)
        print(f"  Transformer excess over additive (L2):          {genuine:.1f}%")

    if mxbai_l2 and hash_l2:
        m_l2 = np.mean(mxbai_l2)
        h_l2 = np.mean(hash_l2)
        if m_l2 > 10 and m_l2 > h_l2 + 10:
            print(f"\n  → TRANSFORMER-GEOMETRIC: L2 shows genuine contextual coupling")
        elif abs(m_l2 - h_l2) < 10:
            print(f"\n  → METRIC ARTIFACT: holographic signal vanishes under L2")
            print(f"    The ~70% interaction was cosine normalization, not data structure")
        else:
            print(f"\n  → MIXED: some genuine coupling, partially metric artifact")

    out = os.path.expanduser("~/chronicle/data/architecture_class_l2.json")
    with open(out, "w") as f:
        json.dump({"timestamp": time.time(), "results": all_results}, f, indent=2)
    print(f"\n  Results: {out}")


if __name__ == "__main__":
    main()
