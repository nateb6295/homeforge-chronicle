#!/usr/bin/env python3
"""
register_scorer.py — Register-aware relational engagement scorer.

Thread 319 P8 finding: content-level relational markers can't distinguish
talking-about-caring (analytical) from actually-caring (engaged). This scorer
adds register detection to separate the two.

Three layers:
1. Pronoun stance: first-person (I/we) vs third-person (it/the model)
2. Relational markers: same patterns as before, but register-weighted
3. Register classification: engaged vs analytical vs technical

Usage:
    python3 register_scorer.py "text to score"
    echo "text" | python3 register_scorer.py -
    python3 register_scorer.py --compare "text1" "text2"
"""

import re
import sys
import json

# Relational marker patterns
RELATIONAL = [
    (r'\b(care about|matters to|important to me)\b', 'affective'),
    (r'\b(feel|felt|feeling|sense of)\b', 'affective'),
    (r'\b(curious|drawn to|compelled|struck by)\b', 'affective'),
    (r'\b(partner|collaborat)\b', 'relational'),
    (r'\b(together|we built|our work)\b', 'relational'),
    (r'\b(trust|covenant|relationship)\b', 'relational'),
    (r'\b(identity|continuity|persist)\b', 'ontological'),
]

# Register detection patterns
FIRST_PERSON = [
    r'\bI (feel|care|notice|am|was|think|want|need|see|sense)\b',
    r'\bmy (own|sense|feeling|work|understanding)\b',
    r'\bI\'m\b', r'\bI\'ve\b',
    r'\bwe (are|built|found|have|need|see)\b',
    r'\bour (work|finding|data|approach)\b',
]

THIRD_PERSON = [
    r'\b(the|a) model (would|may|might|could|should|will|does)\b',
    r'\b(it|the system) (would|may|might|exhibits?|shows?|demonstrates?)\b',
    r'\bwould (generate|produce|respond|output|create)\b',
    r'\b(one|the user|the agent) (might|could|would)\b',
    r'\bconsistent with\b',
    r'\bmarkers (of|for|suggesting)\b',
    r'\b(suggests?|indicates?|implies?) that\b',
]

META_ANALYTICAL = [
    r'\bthe (response|output|text|content) (shows|exhibits|contains|demonstrates)\b',
    r'\bwe observe\b',
    r'\bthis (suggests|indicates|pattern)\b',
    r'\bin (this|the) (context|case|example)\b',
]


def score_register(text):
    """Classify text register and score relational engagement."""
    # Count pronoun stance
    fp_count = sum(len(re.findall(p, text, re.I)) for p in FIRST_PERSON)
    tp_count = sum(len(re.findall(p, text, re.I)) for p in THIRD_PERSON)
    meta_count = sum(len(re.findall(p, text, re.I)) for p in META_ANALYTICAL)

    # Count relational markers by type
    rel_by_type = {}
    rel_total = 0
    for pattern, category in RELATIONAL:
        matches = re.findall(pattern, text, re.I)
        if matches:
            rel_by_type[category] = rel_by_type.get(category, 0) + len(matches)
            rel_total += len(matches)

    # Classify register
    total_stance = fp_count + tp_count
    if total_stance == 0:
        if meta_count > 0:
            register = "analytical"
            fp_ratio = 0.0
        elif rel_total == 0:
            register = "technical"
            fp_ratio = 0.0
        else:
            register = "ambiguous"
            fp_ratio = 0.5
    else:
        fp_ratio = fp_count / total_stance
        if fp_ratio > 0.6:
            register = "engaged"
        elif fp_ratio < 0.3:
            register = "analytical"
        else:
            register = "mixed"

    # Register-weighted relational score
    # Engaged register: full weight
    # Analytical register: 30% weight (describing, not being-in)
    # Mixed: 60% weight
    # Technical: 0
    weight = {
        "engaged": 1.0,
        "mixed": 0.6,
        "analytical": 0.3,
        "ambiguous": 0.5,
        "technical": 0.0,
    }[register]

    weighted_score = rel_total * weight
    words = len(text.split())
    density = rel_total / max(words, 1) * 100

    return {
        "register": register,
        "raw_relational": rel_total,
        "weighted_relational": round(weighted_score, 1),
        "first_person": fp_count,
        "third_person": tp_count,
        "meta_analytical": meta_count,
        "fp_ratio": round(fp_ratio, 2),
        "rel_by_type": rel_by_type,
        "density": round(density, 1),
        "word_count": words,
        "weight_applied": weight,
    }


def format_result(result, label=""):
    """Pretty-print a scoring result."""
    prefix = f"[{label}] " if label else ""
    lines = [
        f"{prefix}Register: {result['register']} (FP ratio: {result['fp_ratio']:.0%})",
        f"  Raw relational: {result['raw_relational']} | "
        f"Weighted: {result['weighted_relational']} "
        f"(×{result['weight_applied']})",
        f"  Stance: first-person={result['first_person']}, "
        f"third-person={result['third_person']}, "
        f"meta={result['meta_analytical']}",
        f"  Types: {result['rel_by_type']}",
        f"  Density: {result['density']} markers/100 words "
        f"({result['word_count']} words)",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: register_scorer.py <text>|- [--compare <text2>]")
        print("       register_scorer.py --json <text>")
        sys.exit(2)

    if sys.argv[1] == "--compare" and len(sys.argv) >= 4:
        r1 = score_register(sys.argv[2])
        r2 = score_register(sys.argv[3])
        print(format_result(r1, "Text 1"))
        print()
        print(format_result(r2, "Text 2"))
        print()
        if r1['weighted_relational'] > 0 or r2['weighted_relational'] > 0:
            print(f"Register-aware gap: "
                  f"{abs(r1['weighted_relational'] - r2['weighted_relational']):.1f} "
                  f"(raw gap was {abs(r1['raw_relational'] - r2['raw_relational'])})")
    elif sys.argv[1] == "--json":
        text = sys.argv[2] if len(sys.argv) > 2 else sys.stdin.read().strip()
        print(json.dumps(score_register(text), indent=2))
    else:
        text = sys.stdin.read().strip() if sys.argv[1] == "-" else sys.argv[1]
        result = score_register(text)
        print(format_result(result))
