#!/usr/bin/env python3
"""KG Predicate Normalizer — Collapse 1300+ free-form predicates into ~30 typed relations.

Chronicle's KG has 1,332 distinct predicates for 1,727 relationships. Most are
full sentences or synonyms of each other ("uses", "leverages", "utilizes", "relies on").

This normalizer maps free-form predicates to a small canonical set using:
1. Exact match against canonical predicates
2. Synonym expansion (leverages → uses, authored → created_by)
3. Verb stem matching (develops/developed/developing → creates)
4. Directional normalization (developed_by vs develops → same relation, flipped)
5. Fallback: extract the core verb and map to nearest canonical

Adapted from MemPalace's knowledge_graph.py approach of typed predicates.

Usage:
    python3 kg_normalize.py preview           # show what would change
    python3 kg_normalize.py apply             # normalize all predicates
    python3 kg_normalize.py stats             # predicate distribution after normalization
    python3 kg_normalize.py lookup "predicate" # show what a predicate maps to

Build #82. Inspired by MemPalace, adapted for Chronicle.
"""

import os
import sys
import re
import sqlite3
from collections import Counter, defaultdict

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    "/mnt/hdd/chronicle-data/processed.db"
)

# ═══════════════════════════════════════════════════════════════════
#  Canonical Predicate Set — ~30 typed relations
# ═══════════════════════════════════════════════════════════════════

# Format: canonical → [synonyms that map to it]
CANONICAL_PREDICATES = {
    # Creation / authorship
    "created_by": [
        "authored", "authored by", "developed by", "built by", "designed by",
        "proposed by", "introduced by", "invented by", "written by", "founded by",
        "established by", "launched by", "published by", "posted by",
        "was developed by", "was created by", "was built by", "was designed by",
        "was proposed by", "was introduced by", "was invented by",
        "was developed by researchers from",
    ],
    "creates": [
        "developed", "develops", "built", "builds", "designed", "designs",
        "introduced", "introduces", "invented", "authored", "published",
        "launched", "proposed", "proposes", "produced", "generates", "crafted",
        "combines", "enhances",
    ],
    # Usage / dependency
    "uses": [
        "leverages", "utilizes", "employs", "relies on", "depends on",
        "uses as structured knowledge base", "built on", "built with",
        "based on", "powered by", "runs on", "operates on",
        "built using", "implemented with", "implemented in",
        "uses as", "uses for",
    ],
    # Containment / membership
    "part_of": [
        "is part of", "belongs to", "is a component of", "is a member of",
        "contained in", "included in", "is in", "is within", "falls under",
        "is a subset of", "is a branch of",
    ],
    "contains": [
        "includes", "has", "comprises", "consists of", "encompasses",
        "incorporates",
    ],
    # Location
    "located_in": [
        "is located in", "are located in", "is based in", "is headquartered in",
        "operates in", "is in", "based in", "situated in", "originates from",
        "occurred in", "was arrested in", "crashed in",
    ],
    # Collaboration / association
    "collaborates_with": [
        "collaborates with", "works with", "partners with",
        "allied with", "cooperates with", "joined with",
    ],
    "affiliated_with": [
        "is affiliated with", "is associated with", "connected to",
        "related to", "linked to", "tied to", "affiliated with",
        "associated with", "are involved in", "involved in", "involves",
    ],
    # Comparison / similarity
    "similar_to": [
        "mirrors", "mirrors patterns seen in", "resembles",
        "is similar to", "comparable to", "analogous to",
        "shares strategic approach with", "shares approach with",
    ],
    "differs_from": [
        "differs from", "contrasts with", "is different from",
        "is distinct from", "diverges from", "compared to",
    ],
    # Hierarchy / leadership
    "leads": [
        "is chairman of", "is head of", "is ceo of", "is director of",
        "runs", "manages", "oversees", "heads",
    ],
    "reports_on": [
        "reports on", "reported on", "covers", "discusses", "examines",
        "analyzes", "investigates", "studies", "evaluates", "assesses",
        "reviews", "cited by", "references", "documents", "explores",
        "emphasizes", "frames", "exhibits", "indicates",
    ],
    # Impact / influence
    "enables": [
        "enables", "facilitates", "supports", "assists", "helps",
        "empowers", "allows", "permits", "promotes", "guides",
        "application of", "correlate with",
    ],
    "impacts": [
        "affects", "influences", "shapes", "drives", "determines",
        "modifies", "alters", "changes", "transforms",
    ],
    "threatens": [
        "threatens", "endangers", "undermines", "targets", "attacks",
        "criticizes", "opposes", "challenges", "disputes", "contests",
        "is accused of", "accused of",
    ],
    # Integration / interaction
    "integrates_with": [
        "integrates with", "integrates", "integrates into",
        "interoperates with", "connects to", "interfaces with",
        "communicates with", "interacts with",
    ],
    # Application / relevance
    "applies_to": [
        "applies to", "is relevant to", "pertains to",
        "concerns", "relates to", "focuses on",
    ],
    # Replacement / succession
    "replaces": [
        "replaces", "supersedes", "succeeds", "displaces",
        "takes over from", "upgrades",
    ],
    # Hosting / running
    "hosts": [
        "hosts", "serves", "provides", "offers", "delivers",
        "runs", "operates",
    ],
    # Ownership / possession
    "owns": [
        "owns", "holds", "possesses", "controls",
    ],
    # Alignment / agreement
    "aligns_with": [
        "aligns with", "agrees with", "supports", "endorses",
        "advocates for", "promotes",
    ],
    # Generic relation (explicit catch-all — not a normalization target)
    "related_to": [
        "related_to", "is related to", "relates to",
    ],
    # Identification / classification
    "identifies": [
        "identifies", "recognizes", "detects", "discovers",
        "finds", "reveals", "uncovers", "highlights",
    ],
    "implements": [
        "implements", "executes", "carries out", "performs",
        "conducts", "applies",
    ],
    # Contribution
    "contributes_to": [
        "contributes to", "adds to", "feeds into",
        "provides input to", "benefits",
    ],
    # Capture / observation
    "captures": [
        "captures", "observes", "records", "logs", "tracks",
        "monitors", "measures",
    ],
    # Reframing / interpretation
    "reframes": [
        "reframes", "reinterprets", "recasts", "reconceptualizes",
        "rethinks", "reimagines",
    ],
}

# Build reverse lookup: synonym → canonical
_SYNONYM_MAP = {}
for canonical, synonyms in CANONICAL_PREDICATES.items():
    _SYNONYM_MAP[canonical] = canonical
    for syn in synonyms:
        _SYNONYM_MAP[syn.lower()] = canonical


# ═══════════════════════════════════════════════════════════════════
#  Verb Stem Patterns — catch inflected forms
# ═══════════════════════════════════════════════════════════════════

# Maps verb stems to canonical predicates
_VERB_STEMS = {
    "creat": "creates",
    "develop": "creates",
    "build": "creates",
    "design": "creates",
    "invent": "creates",
    "author": "created_by",
    "publish": "creates",
    "launch": "creates",
    "us": "uses",
    "leverag": "uses",
    "utiliz": "uses",
    "employ": "uses",
    "reli": "uses",  # relies
    "depend": "uses",
    "integrat": "integrates_with",
    "collaborat": "collaborates_with",
    "partner": "collaborates_with",
    "enabl": "enables",
    "facilitat": "enables",
    "support": "enables",
    "impact": "impacts",
    "influenc": "impacts",
    "affect": "impacts",
    "driv": "impacts",
    "shap": "impacts",
    "threat": "threatens",
    "attack": "threatens",
    "target": "threatens",
    "criticiz": "threatens",
    "oppos": "threatens",
    "replac": "replaces",
    "supersed": "replaces",
    "implement": "implements",
    "execut": "implements",
    "conduct": "implements",
    "identif": "identifies",
    "detect": "identifies",
    "discover": "identifies",
    "reveal": "identifies",
    "report": "reports_on",
    "analyz": "reports_on",
    "examin": "reports_on",
    "evaluat": "reports_on",
    "stud": "reports_on",
    "investigat": "reports_on",
    "contribut": "contributes_to",
    "captur": "captures",
    "observ": "captures",
    "monitor": "captures",
    "track": "captures",
    "refram": "reframes",
    "reinterpret": "reframes",
    "host": "hosts",
    "serv": "hosts",
    "provid": "hosts",
    "own": "owns",
    "hold": "owns",
    "control": "owns",
    "lead": "leads",
    "manag": "leads",
    "head": "leads",
    "overse": "leads",
    "align": "aligns_with",
    "locat": "located_in",
    "situat": "located_in",
    "mirror": "similar_to",
    "resembl": "similar_to",
    "differ": "differs_from",
    "contrast": "differs_from",
    "compar": "differs_from",
}

# Passive voice patterns that flip direction
_PASSIVE_PATTERNS = [
    (re.compile(r"^(was |is |are |were |been )(developed|created|built|designed|proposed|invented|written|authored|founded|established|launched|published) (by )?", re.I), "created_by"),
    (re.compile(r"^(was |is |are |were )(used|leveraged|utilized|employed) (by )?", re.I), "uses"),  # flip: X was used by Y → Y uses X
]


def normalize_predicate(predicate: str) -> str:
    """
    Normalize a free-form predicate to a canonical form.
    Returns the canonical predicate string.
    """
    pred = predicate.strip().lower()

    # 1. Exact match
    if pred in _SYNONYM_MAP:
        return _SYNONYM_MAP[pred]

    # 2. Check passive patterns
    for pattern, canonical in _PASSIVE_PATTERNS:
        if pattern.match(pred):
            return canonical

    # 3. Strip common prefixes/suffixes
    cleaned = pred
    for prefix in ["is ", "are ", "was ", "were ", "has ", "have ", "had ", "been "]:
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix):]
    for suffix in [" of", " for", " with", " to", " in", " on", " by", " from", " at"]:
        if cleaned.endswith(suffix):
            cleaned = cleaned[:len(cleaned) - len(suffix)]

    # Check cleaned version
    if cleaned in _SYNONYM_MAP:
        return _SYNONYM_MAP[cleaned]

    # 4. Verb stem matching — extract first word and match stem
    words = pred.split()
    if words:
        first_word = words[0].rstrip("s").rstrip("ed").rstrip("ing").rstrip("e")
        for stem, canonical in _VERB_STEMS.items():
            if first_word.startswith(stem) or first_word == stem:
                return canonical

    # 5. Check if any canonical synonym appears as a substring
    for syn, canonical in sorted(_SYNONYM_MAP.items(), key=lambda x: -len(x[0])):
        if len(syn) > 3 and syn in pred:
            return canonical

    # 6. Fallback: "related_to" for anything we can't classify
    return "related_to"


def preview_normalization():
    """Show what normalization would do without changing anything."""
    db = sqlite3.connect(DB_PATH)
    rows = db.execute(
        "SELECT relation_type, COUNT(*) as cnt FROM kg_relationships "
        "GROUP BY relation_type ORDER BY cnt DESC"
    ).fetchall()
    db.close()

    mapping = defaultdict(list)
    for rel_type, count in rows:
        canonical = normalize_predicate(rel_type)
        mapping[canonical].append((rel_type, count))

    print(f"\nPredicate Normalization Preview")
    print(f"{'=' * 60}")
    print(f"  {len(rows)} unique predicates → {len(mapping)} canonical types\n")

    for canonical in sorted(mapping.keys(), key=lambda k: -sum(c for _, c in mapping[k])):
        total = sum(c for _, c in mapping[canonical])
        originals = mapping[canonical]
        if len(originals) == 1 and originals[0][0] == canonical:
            print(f"  {canonical:25} {total:4} (already canonical)")
        else:
            print(f"  {canonical:25} {total:4} ← {len(originals)} variants")
            for orig, cnt in sorted(originals, key=lambda x: -x[1])[:5]:
                print(f"    {'':3} {orig[:50]:50} ({cnt})")
            if len(originals) > 5:
                print(f"    {'':3} ... +{len(originals) - 5} more")


def apply_normalization(dry_run: bool = False):
    """Apply normalization to all KG predicates."""
    db = sqlite3.connect(DB_PATH)
    rows = db.execute(
        "SELECT id, relation_type FROM kg_relationships"
    ).fetchall()

    changes = 0
    unchanged = 0
    for row_id, rel_type in rows:
        canonical = normalize_predicate(rel_type)
        if canonical != rel_type.strip().lower():
            changes += 1
            if not dry_run:
                db.execute(
                    "UPDATE kg_relationships SET relation_type = ? WHERE id = ?",
                    (canonical, row_id)
                )
        else:
            unchanged += 1

    if not dry_run:
        db.commit()
    db.close()

    print(f"\nKG Predicate Normalization {'(DRY RUN)' if dry_run else 'APPLIED'}")
    print(f"  Total relationships: {len(rows)}")
    print(f"  Changed: {changes}")
    print(f"  Unchanged: {unchanged}")
    return changes


def show_stats():
    """Show predicate distribution after normalization."""
    db = sqlite3.connect(DB_PATH)
    rows = db.execute(
        "SELECT relation_type, COUNT(*) as cnt FROM kg_relationships "
        "GROUP BY relation_type ORDER BY cnt DESC"
    ).fetchall()
    total = db.execute("SELECT COUNT(*) FROM kg_relationships").fetchone()[0]
    entities = db.execute("SELECT COUNT(*) FROM kg_entities").fetchone()[0]
    db.close()

    print(f"\nKG Predicate Stats")
    print(f"  Entities: {entities:,}")
    print(f"  Relationships: {total:,}")
    print(f"  Unique predicates: {len(rows)}")
    print(f"\n  Distribution:")
    for rel_type, cnt in rows[:30]:
        pct = cnt / total * 100
        bar = "█" * int(pct / 2)
        print(f"    {rel_type:25} {cnt:4} ({pct:4.1f}%) {bar}")
    if len(rows) > 30:
        rest = sum(c for _, c in rows[30:])
        print(f"    {'... others':25} {rest:4}")


def lookup_predicate(predicate: str):
    """Show what a predicate normalizes to."""
    canonical = normalize_predicate(predicate)
    print(f'  "{predicate}" → {canonical}')


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  kg_normalize.py preview              # show what would change")
        print("  kg_normalize.py apply [--dry-run]     # normalize all predicates")
        print("  kg_normalize.py stats                 # predicate distribution")
        print('  kg_normalize.py lookup "predicate"    # test normalization')
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "preview":
        preview_normalization()
    elif cmd == "apply":
        dry_run = "--dry-run" in sys.argv
        apply_normalization(dry_run=dry_run)
    elif cmd == "stats":
        show_stats()
    elif cmd == "lookup":
        if len(sys.argv) < 3:
            print("Usage: kg_normalize.py lookup 'predicate text'")
            sys.exit(1)
        lookup_predicate(" ".join(sys.argv[2:]))
    else:
        print(f"Unknown command: {cmd}")
