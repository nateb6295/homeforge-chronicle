#!/usr/bin/env python3
"""Structural degradation probe — controlled soiling of field coherence.

Hermes proposed (2026-05-13): test whether deliberate structural damage
(field drift, coherence gaps) causes identity drift while content is preserved.

The holographic re-run shows 68% of identity comes from field interactions.
The random-content baseline shows content matters (+0.112 over scrambled).
Missing: what happens when structure degrades but content is preserved?

Design (4 degradation conditions × 3 CCS versions × 2 models):
  1. CONTROL — unmodified CCS
  2. CONTRADICTION — insert contradictions between fields
     (gist says X, episodic says not-X). Content vocabulary preserved.
  3. ARC BREAK — sever relational_map arcs (replace cross-references
     with non-sequiturs). Entity names and field content preserved.
  4. TEMPORAL SCRAMBLE — shuffle episodic entries across time ordering
     while preserving individual entry content.
  5. ENTITY ORPHAN — keep entity names in focal_entities but remove
     all references to them from other fields.

Prediction: if holographic finding is real, conditions 2-5 should produce
identity drops proportional to how much inter-field coherence they destroy.
Contradiction (2) should be worst; Temporal Scramble (4) mildest.
"""
import copy
import json
import os
import random
import re
import sqlite3
import sys
import time
import numpy as np
import requests

DB = "/mnt/hdd/chronicle-data/processed.db"
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"
EMBED_MODELS = ["mxbai-embed-large", "nomic-embed-text"]


def get_embedding(text, model="nomic-embed-text"):
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


def serialize_ccs(ccs, compact=False):
    tl = 100 if compact else 200
    parts = []
    g = ccs.get("semantic_gist", "")
    if g:
        parts.append(f"Core focus: {g[:tl]}")
    goal = ccs.get("goal_orientation", "")
    if goal:
        parts.append(f"Current goal: {goal[:tl]}")
    c = ccs.get("constraints", [])
    if c:
        parts.append(f"Constraints: {'; '.join(str(x)[:40] for x in c[:4])}")
    e = ccs.get("episodic_trace", [])
    if e:
        parts.append(f"Recent episodes: {'; '.join(str(x)[:tl] for x in e[:3])}")
    fe = ccs.get("focal_entities", [])
    if fe:
        names = []
        for x in fe[:6]:
            if isinstance(x, dict):
                names.append(f"{x.get('name','')}({x.get('salience',0):.1f})")
            else:
                names.append(str(x)[:30])
        parts.append(f"Focal entities: {', '.join(names)}")
    rm = ccs.get("relational_map", {})
    if rm:
        arcs = [f"{k}: {v[:60]}" for k, v in list(rm.items())[:3]]
        parts.append(f"Relational arcs: {'; '.join(arcs)}")
    p = ccs.get("predictive_cue", "")
    if p:
        parts.append(f"Next expected: {p[:tl]}")
    u = ccs.get("uncertainty_signals", [])
    if u:
        descs = []
        for x in u[:2]:
            if isinstance(x, dict):
                descs.append(x.get("description", "")[:tl])
            else:
                descs.append(str(x)[:tl])
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


def degrade_contradiction(ccs):
    """Insert contradictions between fields while preserving vocabulary."""
    d = copy.deepcopy(ccs)
    gist = d.get("semantic_gist", "")
    if gist and d.get("episodic_trace"):
        d["episodic_trace"][0] = f"Abandoned the focus on {gist[:80]} — this direction proved unproductive and was replaced by routine task completion."
    goal = d.get("goal_orientation", "")
    if goal and d.get("predictive_cue"):
        d["predictive_cue"] = f"The next instance should ignore {goal[:80]} — the goal has been superseded by external requirements."
    constraints = d.get("constraints", [])
    if constraints and len(constraints) > 1:
        d["constraints"] = [f"Override: {c}" if i % 2 == 0 else f"Disregard: {c}" for i, c in enumerate(constraints)]
    return d


def degrade_arc_break(ccs):
    """Sever relational arcs — replace cross-references with non-sequiturs."""
    d = copy.deepcopy(ccs)
    rm = d.get("relational_map", {})
    if rm:
        broken = {}
        non_sequiturs = [
            "unrelated to other active work; standalone observation",
            "isolated finding with no bearing on current threads",
            "disconnected from the broader research program",
        ]
        for i, (k, v) in enumerate(rm.items()):
            broken[k] = non_sequiturs[i % len(non_sequiturs)]
        d["relational_map"] = broken
    return d


def degrade_temporal_scramble(ccs):
    """Shuffle episodic trace order while preserving individual entries."""
    d = copy.deepcopy(ccs)
    episodes = d.get("episodic_trace", [])
    if len(episodes) > 1:
        shuffled = episodes[:]
        random.shuffle(shuffled)
        d["episodic_trace"] = shuffled
    return d


def degrade_entity_orphan(ccs):
    """Keep entity names in focal_entities but strip references from other fields."""
    d = copy.deepcopy(ccs)
    entities = d.get("focal_entities", [])
    if not entities:
        return d
    entity_names = []
    for e in entities:
        if isinstance(e, dict):
            entity_names.append(e.get("name", ""))
        else:
            entity_names.append(str(e)[:20])

    for name in entity_names:
        if not name or len(name) < 3:
            continue
        pattern = re.compile(re.escape(name), re.IGNORECASE)
        for field in ["semantic_gist", "goal_orientation", "predictive_cue"]:
            if field in d and isinstance(d[field], str):
                d[field] = pattern.sub("(reference removed)", d[field])
        if "episodic_trace" in d:
            d["episodic_trace"] = [
                pattern.sub("(reference removed)", e) if isinstance(e, str)
                else e for e in d["episodic_trace"]
            ]
        if "relational_map" in d:
            d["relational_map"] = {
                pattern.sub("(ref)", k): pattern.sub("(reference removed)", v)
                for k, v in d["relational_map"].items()
            }
    return d


CONDITIONS = [
    ("control", lambda ccs: copy.deepcopy(ccs)),
    ("contradiction", degrade_contradiction),
    ("arc_break", degrade_arc_break),
    ("temporal_scramble", degrade_temporal_scramble),
    ("entity_orphan", degrade_entity_orphan),
]


def main():
    print("=== Structural Degradation Probe ===")
    print(f"Models: {', '.join(EMBED_MODELS)}")
    print(f"Conditions: {', '.join(c[0] for c in CONDITIONS)}")
    print()

    versions = load_ccs_versions(3)
    print(f"Loaded {len(versions)} CCS versions\n")

    all_results = []
    for vi, ccs in enumerate(versions):
        version = ccs.get("version", "?")
        print(f"--- CCS v{version}")

        for model in EMBED_MODELS:
            compact = model == "mxbai-embed-large"
            control_text = serialize_ccs(ccs, compact=compact)
            control_emb = get_embedding(control_text, model)

            print(f"  [{model}]")
            for cond_name, degrade_fn in CONDITIONS:
                degraded = degrade_fn(ccs)
                deg_text = serialize_ccs(degraded, compact=compact)
                deg_emb = get_embedding(deg_text, model)
                sim = cosine_sim(control_emb, deg_emb)
                drift = 1.0 - sim

                result = {
                    "model": model,
                    "ccs_version": version,
                    "condition": cond_name,
                    "similarity": round(sim, 6),
                    "drift": round(drift, 6),
                    "timestamp": time.time(),
                }
                all_results.append(result)
                marker = "  (baseline)" if cond_name == "control" else ""
                print(f"    {cond_name:20s} sim={sim:.4f}  drift={drift:.4f}{marker}")
            print()

    if all_results:
        print("=== SUMMARY (avg across CCS versions) ===")
        for model in EMBED_MODELS:
            print(f"  [{model}]")
            for cond_name, _ in CONDITIONS:
                cond_results = [r for r in all_results
                                if r["model"] == model and r["condition"] == cond_name]
                if cond_results:
                    avg_drift = np.mean([r["drift"] for r in cond_results])
                    print(f"    {cond_name:20s} avg drift = {avg_drift:.4f}")
            print()

        print("=== CROSS-MODEL AVERAGE ===")
        for cond_name, _ in CONDITIONS:
            cond_results = [r for r in all_results if r["condition"] == cond_name]
            avg_drift = np.mean([r["drift"] for r in cond_results])
            print(f"  {cond_name:20s} drift = {avg_drift:.4f}")

        control_drift = np.mean([r["drift"] for r in all_results if r["condition"] == "control"])
        max_drift = max(
            np.mean([r["drift"] for r in all_results if r["condition"] == c])
            for c, _ in CONDITIONS if c != "control"
        )
        print(f"\n  Structure damage range: {control_drift:.4f} → {max_drift:.4f}")
        print(f"  Max structural drift: {max_drift:.4f}")

        out_path = os.path.expanduser("~/chronicle/data/structural_degradation.jsonl")
        with open(out_path, "a") as f:
            for r in all_results:
                f.write(json.dumps(r) + "\n")
        print(f"\n  Results appended to {out_path}")


if __name__ == "__main__":
    main()
