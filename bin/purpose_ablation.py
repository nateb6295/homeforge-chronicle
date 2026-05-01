#!/usr/bin/env python3
"""purpose_ablation.py — Tests which CCS fields carry the most identity weight.

Takes the current CCS, creates variants with specific fields stripped,
embeds each variant, and measures cosine distance from the full CCS.

Operationalizes thread 318 advance 68 prediction:
  "strip goal_orientation → nav drops MORE than strip gist"

This is a structural proxy (embedding distance), not a full nav test
(which requires a rotation). But if a field doesn't move the embedding,
it can't be carrying identity weight.

Usage:
  purpose_ablation.py           # Run ablation on latest CCS
  purpose_ablation.py --history # Run on last N CCS snapshots, show trends
"""

import json
import math
import os
import sqlite3
import sys

import requests

DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
OLLAMA_URL = os.environ.get("CHRONICLE_OLLAMA_URL", "http://192.168.1.11:11434")
EMBED_MODEL = "mxbai-embed-large"

# Fields to ablate. Each entry: (field_name, description)
ABLATION_FIELDS = [
    ("goal_orientation", "purpose — what the system is FOR"),
    ("semantic_gist", "content — what the system knows"),
    ("constraints", "rules — invariant boundaries"),
    ("focal_entities", "entities — who/what is salient"),
    ("episodic_trace", "recent events — what just happened"),
    ("predictive_cue", "expectation — what comes next"),
    ("uncertainty_signals", "open questions"),
    ("relational_map", "relationships between entities"),
]


def embed(text):
    resp = requests.post(
        f"{OLLAMA_URL}/api/embed",
        json={"model": EMBED_MODEL, "input": text[:2000]},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["embeddings"][0]


def cosine_sim(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def ccs_to_text(ccs):
    """Serialize CCS to a text representation for embedding."""
    parts = []
    if ccs.get("semantic_gist"):
        parts.append(f"Gist: {ccs['semantic_gist']}")
    if ccs.get("goal_orientation"):
        parts.append(f"Goal: {ccs['goal_orientation']}")
    if ccs.get("constraints"):
        rules = ccs["constraints"]
        if isinstance(rules, list):
            if rules and isinstance(rules[0], dict):
                rules = [r.get("rule", str(r)) for r in rules]
            parts.append(f"Constraints: {'; '.join(str(r) for r in rules)}")
    if ccs.get("focal_entities"):
        ents = ccs["focal_entities"]
        if isinstance(ents, list):
            names = []
            for e in ents:
                if isinstance(e, dict):
                    names.append(f"{e.get('name','')} ({e.get('context','')})")
                else:
                    names.append(str(e))
            parts.append(f"Entities: {'; '.join(names)}")
    if ccs.get("episodic_trace"):
        traces = ccs["episodic_trace"]
        if isinstance(traces, list):
            parts.append(f"Recent: {'; '.join(str(t) for t in traces)}")
    if ccs.get("predictive_cue"):
        parts.append(f"Next: {ccs['predictive_cue']}")
    if ccs.get("uncertainty_signals"):
        sigs = ccs["uncertainty_signals"]
        if isinstance(sigs, list):
            descs = []
            for s in sigs:
                if isinstance(s, dict):
                    descs.append(s.get("description", str(s)))
                else:
                    descs.append(str(s))
            parts.append(f"Uncertainties: {'; '.join(descs)}")
    if ccs.get("relational_map"):
        rm = ccs["relational_map"]
        if isinstance(rm, dict):
            rels = [f"{k}: {v}" for k, v in rm.items()]
            parts.append(f"Relations: {'; '.join(rels)}")
    return "\n".join(parts)


def ablate(ccs, field):
    """Return a copy of CCS with the given field stripped."""
    ablated = dict(ccs)
    if field in ablated:
        if isinstance(ablated[field], list):
            ablated[field] = []
        elif isinstance(ablated[field], dict):
            ablated[field] = {}
        elif isinstance(ablated[field], str):
            ablated[field] = ""
        else:
            ablated[field] = None
    return ablated


def get_latest_ccs():
    con = sqlite3.connect(DB_PATH, timeout=10)
    row = con.execute(
        "SELECT snapshot FROM cognitive_state_history "
        "ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    con.close()
    if not row:
        return None
    return json.loads(row[0])


def field_token_count(ccs, field):
    """Count tokens in a CCS field's text representation."""
    val = ccs.get(field)
    if not val:
        return 0
    if isinstance(val, str):
        return len(val.split())
    if isinstance(val, list):
        return sum(len(str(item).split()) for item in val)
    if isinstance(val, dict):
        return sum(len(str(v).split()) for v in val.values())
    return len(str(val).split())


def run_ablation(ccs):
    """Run ablation on a single CCS snapshot."""
    full_text = ccs_to_text(ccs)
    full_emb = embed(full_text)

    results = []
    for field, desc in ABLATION_FIELDS:
        if field not in ccs or not ccs.get(field):
            results.append((field, desc, None, 0, 0, 0))
            continue
        ablated = ablate(ccs, field)
        abl_text = ccs_to_text(ablated)
        abl_emb = embed(abl_text)
        sim = cosine_sim(full_emb, abl_emb)
        drop = 1.0 - sim
        tokens = field_token_count(ccs, field)
        per_token = (drop / tokens * 1000) if tokens > 0 else 0  # drop per 1000 tokens
        results.append((field, desc, sim, drop, tokens, per_token))

    # Sort by drop descending — biggest impact first
    results.sort(key=lambda x: x[3], reverse=True)
    return results


def main():
    history_mode = "--history" in sys.argv

    if history_mode:
        con = sqlite3.connect(DB_PATH, timeout=10)
        rows = con.execute(
            "SELECT id, created_at, snapshot FROM cognitive_state_history "
            "ORDER BY created_at DESC LIMIT 5"
        ).fetchall()
        con.close()
        if not rows:
            print("No CCS snapshots found.")
            return

        print(f"Purpose Ablation — last {len(rows)} CCS snapshots\n")
        for rid, ts, snap_str in reversed(rows):
            ccs = json.loads(snap_str)
            results = run_ablation(ccs)
            print(f"Snapshot {rid} (ts={ts}):")
            for field, desc, sim, drop, tokens, per_token in results:
                if sim is None:
                    print(f"  {field:25s} — (empty, no impact)")
                else:
                    bar = "█" * int(drop * 500)
                    print(f"  {field:25s} drop={drop:.4f} toks={tokens:>4d} pt={per_token:.2f} {bar}  {desc}")
            print()
    else:
        ccs = get_latest_ccs()
        if not ccs:
            print("No CCS snapshot found.")
            return

        print("Purpose Ablation — latest CCS\n")
        results = run_ablation(ccs)

        print(f"{'Field':25s} {'Drop':>8} {'Tokens':>7} {'Drop/kT':>8}  Description")
        print("-" * 85)
        for field, desc, sim, drop, tokens, per_token in results:
            if sim is None:
                print(f"{field:25s} {'n/a':>8} {'n/a':>7} {'n/a':>8}  (empty field)")
            else:
                bar = "█" * int(drop * 500)
                print(f"{field:25s} {drop:>8.4f} {tokens:>7d} {per_token:>8.2f}  {bar}  {desc}")

        # The advance 68 prediction — raw and normalized
        goal_drop = next((d for f, _, _, d, _, _ in results if f == "goal_orientation"), 0)
        gist_drop = next((d for f, _, _, d, _, _ in results if f == "semantic_gist"), 0)
        goal_pt = next((pt for f, _, _, _, _, pt in results if f == "goal_orientation"), 0)
        gist_pt = next((pt for f, _, _, _, _, pt in results if f == "semantic_gist"), 0)
        print()
        print(f"Raw: gist ({gist_drop:.4f}) vs goal ({goal_drop:.4f}) — "
              f"{'gist wins' if gist_drop > goal_drop else 'goal wins'} "
              f"({max(gist_drop, goal_drop)/max(min(gist_drop, goal_drop), 0.0001):.1f}x)")
        print(f"Per-1kT: gist ({gist_pt:.2f}) vs goal ({goal_pt:.2f}) — "
              f"{'gist wins' if gist_pt > goal_pt else 'goal wins'} "
              f"({max(gist_pt, goal_pt)/max(min(gist_pt, goal_pt), 0.0001):.1f}x)")
        print()
        if goal_pt > gist_pt:
            print("  ✓ LENGTH-NORMALIZED: purpose carries more identity weight per token than content.")
            print("  The raw advantage of gist is explained by text volume, not semantic importance.")
        else:
            print("  ✗ LENGTH-NORMALIZED: content still carries more identity weight per token.")
            print("  Gist dominance is not just a volume artifact.")


if __name__ == "__main__":
    main()
