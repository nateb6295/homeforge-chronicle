#!/usr/bin/env python3
"""Concept-cell retrieval probe — sparse entity-indexed layer.

SKETCH — parallels vector retrieval in memory.py. Not wired in.
Biology transfer from capture #137807 (Quanta on concept cells): nature uses
sparse entity-specific selectivity where we default to distributed vector search.
This probe takes a query cue and returns focal_entities that "fire" —
entities whose name/context overlaps the cue tokens, ranked by
salience * overlap-density.

Usage:
  python3 concept_cells.py "interoception and rest window"
  python3 concept_cells.py --compare "paper sprint"   # side-by-side vs vector
"""

import json
import re
import sqlite3
import sys
from pathlib import Path

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
TOKEN_RE = re.compile(r"[a-z0-9]+")
STOP = {"the","a","an","and","or","of","to","in","on","for","is","it","this","that"}


def tokenize(text: str) -> set:
    return {t for t in TOKEN_RE.findall(text.lower()) if t not in STOP and len(t) > 2}


def load_focal_entities() -> list:
    con = sqlite3.connect(DB_PATH)
    row = con.execute("SELECT focal_entities FROM cognitive_state WHERE id=1").fetchone()
    con.close()
    return json.loads(row[0]) if row else []


def fire(cue: str, entities: list) -> list:
    """Return [(entity, activation)] sorted desc. Activation = salience * overlap."""
    cue_tokens = tokenize(cue)
    if not cue_tokens:
        return []
    fired = []
    for e in entities:
        name_tokens = tokenize(e.get("name", ""))
        ctx_tokens = tokenize(e.get("context", ""))
        name_hits = len(cue_tokens & name_tokens)
        ctx_hits = len(cue_tokens & ctx_tokens)
        overlap = name_hits * 2.0 + ctx_hits * 1.0
        if overlap > 0:
            activation = e.get("salience", 0.5) * overlap
            fired.append((e, activation, name_hits, ctx_hits))
    fired.sort(key=lambda x: x[1], reverse=True)
    return fired


def format_firing(fired: list, limit: int = 5) -> str:
    if not fired:
        return "  (no cells fired)"
    lines = []
    for e, act, nh, ch in fired[:limit]:
        lines.append(f"  {e['name']:<30} act={act:.2f}  (name×{nh} ctx×{ch})  sal={e.get('salience',0):.2f}")
    return "\n".join(lines)


def main():
    args = sys.argv[1:]
    compare = "--compare" in args
    args = [a for a in args if not a.startswith("--")]
    cue = " ".join(args) if args else "concept cells biology memory"

    entities = load_focal_entities()
    print(f"CUE: {cue!r}")
    print(f"LOADED {len(entities)} focal_entities from CCS")
    print("\nCONCEPT-CELL FIRING:")
    fired = fire(cue, entities)
    print(format_firing(fired))

    if compare:
        print("\n[vector comparison would run memory.py assemble_working_memory here]")
        print("[not wired — sketch only]")


if __name__ == "__main__":
    main()
