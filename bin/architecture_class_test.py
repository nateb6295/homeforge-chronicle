#!/usr/bin/env python3
"""Architecture-class holographic test: transformer vs non-transformer.

Resolves the last CCS uncertainty (0.52): is holographic encoding
(~70% from field interactions) transformer-geometric or CCS-structural?

Protocol: run the same field-ablation interaction measurement with:
  1. mxbai-embed-large (transformer, 1024d)
  2. nomic-embed-text (transformer, 768d)
  3. TF-IDF + cosine (non-neural, normalized bag-of-words)
  4. Hashing vectorizer (non-neural, strictly additive — null baseline)

Predictions:
  - If TF-IDF interaction ≈ transformer → CCS-structural (data carries it)
  - If TF-IDF interaction ≈ 0 → transformer-geometric (model creates it)
  - If TF-IDF intermediate → both contribute, decomposable
"""

import json
import os
import sqlite3
import sys
import time
import numpy as np
import requests
from sklearn.feature_extraction.text import TfidfVectorizer, HashingVectorizer

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
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a < 1e-10 or norm_b < 1e-10:
        return 1.0
    return 1.0 - float(np.dot(a, b) / (norm_a * norm_b))


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


class TfidfEmbedder:
    """Non-neural embedding: TF-IDF with cosine distance."""

    def __init__(self):
        self.vectorizer = TfidfVectorizer(
            max_features=5000,
            ngram_range=(1, 2),
            sublinear_tf=True,
        )
        self.fitted = False

    def fit(self, texts):
        self.vectorizer.fit(texts)
        self.fitted = True

    def embed(self, text):
        if not self.fitted:
            raise RuntimeError("Call fit() first")
        vec = self.vectorizer.transform([text]).toarray()[0]
        return vec


class HashEmbedder:
    """Strictly additive: hashing vectorizer without normalization."""

    def __init__(self):
        self.vectorizer = HashingVectorizer(
            n_features=4096,
            ngram_range=(1, 2),
            norm=None,
            alternate_sign=False,
        )

    def embed(self, text):
        vec = self.vectorizer.transform([text]).toarray()[0]
        return vec


def build_all_texts(ccs_versions):
    """Generate all text variants for fitting TF-IDF."""
    texts = []
    for ccs in ccs_versions:
        texts.append(serialize_ccs(ccs))
        texts.append(serialize_ccs(ccs, compact=True))
        for field in ABLATABLE_FIELDS:
            texts.append(serialize_ccs(ccs, skip_fields=[field]))
            texts.append(serialize_ccs(ccs, skip_fields=[field], compact=True))
        empty = serialize_ccs(ccs, skip_fields=ABLATABLE_FIELDS)
        texts.append(empty if empty.strip() else "(empty cognitive state)")
    return [t for t in texts if t.strip()]


def run_probe_transformer(ccs, model):
    compact = model == "mxbai-embed-large"
    full_text = serialize_ccs(ccs, compact=compact)
    full_emb = get_ollama_embedding(full_text, model)

    empty_text = serialize_ccs(ccs, skip_fields=ABLATABLE_FIELDS, compact=compact)
    if not empty_text.strip():
        empty_text = "(empty cognitive state)"
    empty_emb = get_ollama_embedding(empty_text, model)

    full_gap = cosine_dist(full_emb, empty_emb)

    field_gaps = {}
    for field in ABLATABLE_FIELDS:
        ablated_text = serialize_ccs(ccs, skip_fields=[field], compact=compact)
        ablated_emb = get_ollama_embedding(ablated_text, model)
        gap = cosine_dist(full_emb, ablated_emb)
        field_gaps[field] = gap

    sum_individual = sum(field_gaps.values())
    interaction_pct = (1.0 - sum_individual / full_gap) * 100 if full_gap > 0.001 else 0.0

    return {
        "model": model,
        "type": "transformer",
        "full_gap": round(full_gap, 6),
        "sum_individual": round(sum_individual, 6),
        "interaction_pct": round(interaction_pct, 1),
        "field_gaps": {k: round(v, 6) for k, v in sorted(field_gaps.items(), key=lambda x: -x[1])},
    }


def run_probe_sklearn(ccs, embedder, name, emb_type):
    full_text = serialize_ccs(ccs)
    full_emb = embedder.embed(full_text)

    empty_text = serialize_ccs(ccs, skip_fields=ABLATABLE_FIELDS)
    if not empty_text.strip():
        empty_text = "(empty cognitive state)"
    empty_emb = embedder.embed(empty_text)

    full_gap = cosine_dist(full_emb, empty_emb)

    field_gaps = {}
    for field in ABLATABLE_FIELDS:
        ablated_text = serialize_ccs(ccs, skip_fields=[field])
        ablated_emb = embedder.embed(ablated_text)
        gap = cosine_dist(full_emb, ablated_emb)
        field_gaps[field] = gap

    sum_individual = sum(field_gaps.values())
    interaction_pct = (1.0 - sum_individual / full_gap) * 100 if full_gap > 0.001 else 0.0

    return {
        "model": name,
        "type": emb_type,
        "full_gap": round(full_gap, 6),
        "sum_individual": round(sum_individual, 6),
        "interaction_pct": round(interaction_pct, 1),
        "field_gaps": {k: round(v, 6) for k, v in sorted(field_gaps.items(), key=lambda x: -x[1])},
    }


def main():
    print("=" * 70)
    print("ARCHITECTURE-CLASS HOLOGRAPHIC TEST")
    print("Transformer vs non-transformer field interaction measurement")
    print("=" * 70)
    print()

    versions = load_ccs_versions(5)
    print(f"Loaded {len(versions)} distinct CCS versions")

    print("Fitting TF-IDF on all text variants...")
    all_texts = build_all_texts(versions)
    tfidf = TfidfEmbedder()
    tfidf.fit(all_texts)
    print(f"  Vocabulary: {len(tfidf.vectorizer.vocabulary_)} terms")

    hasher = HashEmbedder()

    methods = [
        ("mxbai-embed-large", "transformer", None),
        ("nomic-embed-text", "transformer", None),
        ("tfidf-5k-bigram", "non-neural", tfidf),
        ("hash-4096-additive", "non-neural-additive", hasher),
    ]

    all_results = []

    for vi, ccs in enumerate(versions):
        version = ccs.get("version", "?")
        gist = (ccs.get("semantic_gist") or "?")[:60]
        print(f"\n{'─' * 60}")
        print(f"CCS v{version}: {gist}")
        print(f"{'─' * 60}")

        for name, mtype, embedder in methods:
            print(f"\n  [{name}] ({mtype})")
            try:
                if embedder is None:
                    result = run_probe_transformer(ccs, name)
                else:
                    result = run_probe_sklearn(ccs, embedder, name, mtype)

                result["ccs_version"] = version
                all_results.append(result)

                print(f"    Full gap:     {result['full_gap']:.6f}")
                print(f"    Sum indiv:    {result['sum_individual']:.6f}")
                print(f"    Interaction:  {result['interaction_pct']:.1f}%")
                top3 = list(result["field_gaps"].items())[:3]
                for field, gap in top3:
                    pct = (gap / result["full_gap"] * 100) if result["full_gap"] > 0 else 0
                    print(f"      {field:25s} {gap:.6f}  ({pct:.1f}%)")
            except Exception as e:
                print(f"    ERROR: {e}")

    print("\n" + "=" * 70)
    print("SUMMARY — Architecture-Class Comparison")
    print("=" * 70)

    for name, mtype, _ in methods:
        results = [r for r in all_results if r["model"] == name]
        if not results:
            continue
        interactions = [r["interaction_pct"] for r in results]
        gaps = [r["full_gap"] for r in results]
        avg_int = np.mean(interactions)
        std_int = np.std(interactions)
        avg_gap = np.mean(gaps)
        print(f"\n  {name} ({mtype}):")
        print(f"    Interaction:  {avg_int:.1f}% ± {std_int:.1f}%  (n={len(results)})")
        print(f"    Avg full gap: {avg_gap:.6f}")
        for r in results:
            print(f"      v{r['ccs_version']}: {r['interaction_pct']:.1f}%")

    transformer_ints = [r["interaction_pct"] for r in all_results if r["type"] == "transformer"]
    nonneural_ints = [r["interaction_pct"] for r in all_results if "non-neural" in r["type"]]
    additive_ints = [r["interaction_pct"] for r in all_results if r["type"] == "non-neural-additive"]

    print(f"\n{'─' * 60}")
    print("VERDICT:")
    if transformer_ints:
        t_avg = np.mean(transformer_ints)
        print(f"  Transformer avg interaction:     {t_avg:.1f}%")
    if nonneural_ints:
        nn_avg = np.mean(nonneural_ints)
        print(f"  Non-neural avg interaction:      {nn_avg:.1f}%")
    if additive_ints:
        a_avg = np.mean(additive_ints)
        print(f"  Additive baseline interaction:   {a_avg:.1f}%")

    if transformer_ints and nonneural_ints:
        t_avg = np.mean(transformer_ints)
        nn_avg = np.mean(nonneural_ints)
        ratio = nn_avg / t_avg if t_avg > 0 else 0

        if ratio > 0.7:
            print(f"\n  → CCS-STRUCTURAL: non-neural captures {ratio:.0%} of transformer interaction")
            print(f"    Holographic encoding is in the DATA, not the model architecture.")
        elif ratio < 0.3:
            print(f"\n  → TRANSFORMER-GEOMETRIC: non-neural only {ratio:.0%} of transformer interaction")
            print(f"    Holographic appearance is created by contextual representations.")
        else:
            print(f"\n  → MIXED: non-neural captures {ratio:.0%} of transformer interaction")
            print(f"    Both data structure and model architecture contribute.")

    out = os.path.expanduser("~/chronicle/data/architecture_class_test.json")
    with open(out, "w") as f:
        json.dump({
            "timestamp": time.time(),
            "n_ccs": len(versions),
            "results": all_results,
        }, f, indent=2)
    print(f"\n  Results: {out}")


if __name__ == "__main__":
    main()
