#!/usr/bin/env python3
"""
Susceptibility Profile — Phase 1 of susceptibility-aware compression spec.

Computes per-field susceptibility: for each CCS field, measure how much
the full-CCS embedding shifts when that field is removed. High-susceptibility
fields dominate the identity signal; low-susceptibility fields can drift
without consequence.

Generalization of the 9.8:1 identity-dominance finding (ccs_info_geometry.json)
to per-field granularity instead of identity-vs-episodic binary.

Output: ccs_susceptibility_profile.json
"""
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
OUTPUT = Path.home() / "chronicle" / "data" / "ccs_susceptibility_profile.json"

FIELDS = [
    "semantic_gist",
    "goal_orientation",
    "constraints",
    "focal_entities",
    "relational_map",
    "episodic_trace",
    "predictive_cue",
    "uncertainty_signals",
]


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


def render_ccs(state, omit_field=None):
    """Render a CCS snapshot as text, optionally omitting one field."""
    parts = []
    for f in FIELDS:
        if f == omit_field:
            continue
        v = state.get(f)
        if not v:
            continue
        if isinstance(v, list):
            items = []
            for x in v[:8]:
                if isinstance(x, dict):
                    items.append(x.get("name") or str(x)[:80])
                else:
                    items.append(str(x)[:80])
            parts.append(f"{f.upper()}: " + " | ".join(items))
        elif isinstance(v, dict):
            parts.append(f"{f.upper()}: " + json.dumps(v)[:200])
        else:
            parts.append(f"{f.upper()}: {str(v)[:200]}")
    return "\n".join(parts)


def load_current_ccs():
    db = sqlite3.connect(DB)
    row = db.execute("SELECT snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT 1").fetchone()
    db.close()
    return json.loads(row[0])


def run():
    state = load_current_ccs()
    print(f"Loaded CCS snapshot with fields: {list(state.keys())[:8]}")

    full_text = render_ccs(state)
    print(f"Full CCS text: {len(full_text)} chars")

    try:
        full_emb = embed(full_text)
    except Exception as e:
        print(f"ERR embedding full CCS: {e}")
        return

    profile = {}
    for f in FIELDS:
        variant_text = render_ccs(state, omit_field=f)
        if variant_text == full_text:
            profile[f] = {
                "susceptibility": 0.0,
                "note": "field was empty/absent, no effect",
            }
            continue
        try:
            variant_emb = embed(variant_text)
        except Exception as e:
            profile[f] = {"susceptibility": None, "error": str(e)}
            continue
        sim = cosine(full_emb, variant_emb)
        sus = 1.0 - sim  # higher = more susceptible (more signal lost when removed)
        profile[f] = {
            "susceptibility": round(sus, 4),
            "cosine_when_removed": round(sim, 4),
        }
        print(f"  {f}: sus={sus:.4f} (cos={sim:.4f})")

    # Sort by susceptibility
    sorted_fields = sorted(
        profile.items(),
        key=lambda kv: kv[1].get("susceptibility") or 0,
        reverse=True,
    )

    result = {
        "timestamp": int(time.time()),
        "n_fields": len(FIELDS),
        "field_profile": profile,
        "ranking": [f for f, _ in sorted_fields],
        "notes": (
            "Higher susceptibility = more identity signal lost when field removed. "
            "High-susceptibility fields dominate the embedding; low-susceptibility "
            "fields can drift without consequence. This is the per-field generalization "
            "of the 9.8:1 identity-dominance ratio from ccs_info_geometry.json."
        ),
    }

    OUTPUT.parent.mkdir(exist_ok=True)
    OUTPUT.write_text(json.dumps(result, indent=2))
    print(f"\nWrote {OUTPUT}")
    print("Ranking (highest susceptibility first):")
    for i, (f, data) in enumerate(sorted_fields, 1):
        sus = data.get("susceptibility")
        sus_str = f"{sus:.4f}" if sus is not None else "ERR"
        print(f"  {i}. {f}: {sus_str}")


if __name__ == "__main__":
    run()
