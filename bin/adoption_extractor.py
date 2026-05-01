#!/usr/bin/env python3
"""Adoption Extractor v0.1 — entity/relation triples from adoption signals.

Reads items from the adoption collector (or from activity_feed rows tagged
by the watcher) and emits (subject, predicate, object) edges into
processed.db's `adoption_edges` table. Heuristic: seed-list entities +
capitalized-span candidates + relation templates.

v0.1 is rule-based on purpose: ship fast, let the data shape accumulate,
upgrade to a Gemma-based NER/RE pass once the patterns are visible.
Calibration beats effort: start with a 200-line extractor and measure
what it misses before scaling up.

Usage:
  python3 adoption_extractor.py < collector_output.txt
  python3 adoption_extractor.py --live   # pulls directly from collector
"""
import os
import re
import sys
import json
import sqlite3
import subprocess
import time
from datetime import datetime


DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
COLLECTOR = os.path.expanduser("~/.hermes/scripts/crosschain_collector.py")


# Seed entities with canonical names. Order matters — longer/more-specific
# matches are tried first so "Kyobo Life Insurance" wins over "Kyobo".
SEED_ENTITIES = [
    # XRP ecosystem
    ("Kyobo Life Insurance", "kyobo life"),
    ("Kyobo Life Insurance", "kyobo"),
    ("Ripple", "ripple"),
    ("XRP Ledger", "xrp ledger"),
    ("XRP Ledger", "xrpl"),
    ("XRP", "xrp"),
    ("RLUSD", "rlusd"),
    ("Rakuten Pay", "rakuten pay"),
    ("Rakuten", "rakuten"),
    # ICP ecosystem
    ("Internet Computer", "internet computer protocol"),
    ("Internet Computer", "internet computer"),
    ("DFINITY", "dfinity"),
    ("ICP", "icp"),
    # Cross-cutting infra
    ("Zero-Knowledge Proofs", "zero-knowledge proofs"),
    ("Zero-Knowledge Proofs", "zk-proofs"),
    ("Zero-Knowledge Proofs", "zk proofs"),
    # Product types
    ("Government Bonds", "government bonds"),
    ("Bond Settlement", "bond settlement"),
    ("Institutional Infrastructure", "institutional infrastructure"),
    ("Digital Asset Custody", "digital asset adoption"),
    ("Digital Asset Custody", "digital asset custody"),
]

# Countries / regions to tag as geographic context
GEOS = [
    ("Korea", "korea"),
    ("Korea", "korean"),
    ("Japan", "japan"),
    ("Japan", "japanese"),
    ("United States", "united states"),
    ("United States", "u.s."),
    ("China", "china"),
    ("Singapore", "singapore"),
    ("UAE", "uae"),
    ("Switzerland", "switzerland"),
]

# Relation templates — (regex, predicate). Subjects/objects are filled from
# matched entities inside the captured spans.
RELATION_PATTERNS = [
    (r"\bpartners?\s+with\b", "partners_with"),
    (r"\bpartnership\s+with\b", "partners_with"),
    (r"\bcollaborat\w+\s+with\b", "collaborates_with"),
    (r"\bselect(?:s|ed)\b", "selects"),
    (r"\badopt(?:s|ed|ing)\b", "adopts"),
    (r"\bintegrat(?:es|ed|ing)\b", "integrates"),
    (r"\bpilot(?:s|ed|ing)\b", "pilots"),
    (r"\blaunch(?:es|ed|ing)\b", "launches"),
    (r"\bdeploy(?:s|ed|ing)\b", "deploys"),
    (r"\bbring(?:s|ing)?\s+.+\s+on[- ]chain\b", "brings_onchain"),
    (r"\badd(?:s|ed|ing)\b", "adds"),
]

# Ecosystem groups — entities in the same group share canonical identity
# for the purposes of the adoption lattice. Edges within a group are
# suppressed (e.g. "Internet Computer adopts DFINITY" is noise, they're
# the same ecosystem). Cross-group edges stay.
ECOSYSTEMS = {
    "icp": {"Internet Computer", "DFINITY", "ICP"},
    "xrp": {"XRP Ledger", "XRP", "Ripple", "RLUSD"},
}


def _ecosystem(name):
    for eco, members in ECOSYSTEMS.items():
        if name in members:
            return eco
    return None


def _ensure_schema(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS adoption_edges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject TEXT NOT NULL,
            predicate TEXT NOT NULL,
            object TEXT NOT NULL,
            lane TEXT,
            geo TEXT,
            source_title TEXT,
            source_url TEXT,
            confidence REAL DEFAULT 0.7,
            created_at INTEGER NOT NULL
        )
    """)
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_adoption_subj ON adoption_edges(subject)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_adoption_obj ON adoption_edges(object)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_adoption_pred ON adoption_edges(predicate)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_adoption_lane ON adoption_edges(lane)""")
    conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_adoption_unique
                    ON adoption_edges(subject, predicate, object, source_url)""")
    conn.commit()


def _find_entities(text):
    """Return ordered list of (canonical_name, span_start, span_end) hits."""
    low = text.lower()
    hits = []
    taken = [False] * len(low)
    for canonical, alias in SEED_ENTITIES:
        start = 0
        while True:
            idx = low.find(alias, start)
            if idx < 0:
                break
            end = idx + len(alias)
            if not any(taken[idx:end]):
                hits.append((canonical, idx, end))
                for i in range(idx, end):
                    taken[i] = True
            start = end
    hits.sort(key=lambda x: x[1])
    return hits


def _find_geos(text):
    low = text.lower()
    out = []
    for canonical, alias in GEOS:
        if alias in low:
            if canonical not in out:
                out.append(canonical)
    return out


def _extract_edges(title, summary, lane, source_url):
    """Heuristic edge extraction. Returns list of dicts."""
    text = f"{title}. {summary}"
    entities = _find_entities(text)
    geos = _find_geos(text)
    edges = []

    if len(entities) < 2:
        if entities and geos:
            # Single-entity geo tag: (entity, operates_in, geo)
            for ent, _, _ in entities:
                for g in geos:
                    edges.append({
                        "subject": ent, "predicate": "operates_in", "object": g,
                        "confidence": 0.55,
                    })
        return edges

    # Pairwise within matched relation windows. For each pattern, if the
    # pattern regex fires, connect the nearest entity to the left with the
    # nearest entity to the right.
    low = text.lower()
    for pat, predicate in RELATION_PATTERNS:
        for m in re.finditer(pat, low):
            mid = (m.start() + m.end()) // 2
            left = [e for e in entities if e[2] <= m.start()]
            right = [e for e in entities if e[1] >= m.end()]
            if not left or not right:
                continue
            subj = max(left, key=lambda x: x[2])[0]
            obj = min(right, key=lambda x: x[1])[0]
            if subj == obj:
                continue
            subj_eco = _ecosystem(subj)
            if subj_eco and subj_eco == _ecosystem(obj):
                continue
            edges.append({
                "subject": subj, "predicate": predicate, "object": obj,
                "confidence": 0.8,
            })

    # Geo edges for all entities present
    for ent, _, _ in entities:
        for g in geos:
            edges.append({
                "subject": ent, "predicate": "operates_in", "object": g,
                "confidence": 0.6,
            })

    # Dedupe by (s, p, o)
    seen = set()
    out = []
    for e in edges:
        key = (e["subject"], e["predicate"], e["object"])
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


def _parse_collector_blocks(raw):
    """Parse collector output into list of dicts with title/summary/lane/url."""
    items = []
    current = None
    for line in raw.split("\n"):
        if line.startswith("--- [") and line.endswith("---"):
            if current:
                items.append(current)
            m = re.match(r"--- \[([^\]]+)\](?: \[CROSS: ([^\]]+)\])? ---", line)
            source = m.group(1) if m else "?"
            cross = m.group(2) if m and m.group(2) else ""
            lane = "XRP-ADOPT" if "XRP-ADOPT" in cross else ("ICP-ADOPT" if "ICP-ADOPT" in cross else "")
            current = {"source": source, "lane": lane, "title": "", "url": "", "summary": "", "_state": "title"}
        elif current is not None:
            if current["_state"] == "title" and line.strip():
                current["title"] = line.strip()
                current["_state"] = "url"
            elif current["_state"] == "url" and line.startswith("http"):
                current["url"] = line.strip()
                current["_state"] = "summary"
            elif line.strip() and not line.startswith("(posted:"):
                current["summary"] += " " + line.strip()
    if current:
        items.append(current)
    return [it for it in items if it.get("lane")]


def _store_edges(conn, edges, item):
    now = int(time.time())
    geos = _find_geos(f"{item['title']} {item['summary']}")
    geo = geos[0] if geos else None
    for e in edges:
        conn.execute(
            """INSERT OR IGNORE INTO adoption_edges
               (subject, predicate, object, lane, geo, source_title, source_url, confidence, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (e["subject"], e["predicate"], e["object"], item["lane"], geo,
             item["title"][:300], item["url"], e["confidence"], now),
        )
    conn.commit()


def main():
    live = "--live" in sys.argv
    if live:
        r = subprocess.run(["python3", COLLECTOR], capture_output=True, text=True, timeout=90)
        raw = r.stdout
    else:
        raw = sys.stdin.read() if not sys.stdin.isatty() else ""

    if not raw.strip():
        print("[extractor] no input", file=sys.stderr)
        return

    items = _parse_collector_blocks(raw)
    if not items:
        print("[extractor] no lane-tagged items", file=sys.stderr)
        return

    conn = sqlite3.connect(DB_PATH)
    _ensure_schema(conn)

    total_edges = 0
    summary_lines = []
    for it in items:
        edges = _extract_edges(it["title"], it["summary"], it["lane"], it["url"])
        if not edges:
            continue
        _store_edges(conn, edges, it)
        total_edges += len(edges)
        for e in edges:
            summary_lines.append(f"  ({e['subject']}) -[{e['predicate']}]-> ({e['object']})")

    conn.close()
    print(f"[extractor] stored {total_edges} edges from {len(items)} items")
    for line in summary_lines[:30]:
        print(line)


if __name__ == "__main__":
    main()
