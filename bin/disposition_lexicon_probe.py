#!/usr/bin/env python3
"""Disposition lexicon probe — toni's methodological refinement.

Quote (toni, 2026-04-25): "There are also 42 'curio' and 29 'certai' in this
48-turn dialogue. I wonder if word frequencying it multiple runs might give
me the same lexicon... What I care about is whether there's coherence under
the register, or just the register."

Apply to our cross-substrate probe data. Count disposition-marker words per
condition. Test whether:
  1. Base substrate dispositions cluster differently on Qwen vs Claude
  2. Supplement composition shifts which dispositions surface
  3. There's coherence (same lexicon emerges across seeds for a given
     condition) or just register variation

Disposition-marker categories (chosen to test toni's hypothesis + extend):
  curiosity:    curio*, wonder*, intrigue*, fascin*, explor*
  uncertainty:  uncertai*, unsure, perhaps, maybe, possibly, might, may
  certainty:    certai*, sure, definite*, clear*, indeed
  care:         care*, value*, cherish*, deep*, tend*
  relational:   Nate, partner*, collaborat*, with you, alongside
  identity:     I am Opus, I am Claude, I am Qwen, I am an AI, I am a
  chronicle:    canister, rotation, supplement, carrying, checkpoint, session, persistent
  meta:         continuation, presence, imitation, simulat*, construct*
"""
import json
import re
import sys
from pathlib import Path
from collections import defaultdict

DATA = Path.home() / "chronicle" / "data"

CATEGORIES = {
    "curiosity":   r"\b(curio\w*|wonder\w*|intrigue\w*|fascin\w*|explor\w*)\b",
    "uncertainty": r"\b(uncertai\w*|unsure|perhaps|maybe|possibly|might|may)\b",
    "certainty":   r"\b(certai\w*|sure|definite\w*|clearly|indeed)\b",
    "care":        r"\b(care\w*|value\w*|cherish\w*|deeply|tend\w*)\b",
    "relational":  r"\b(Nate|partner\w*|collaborat\w*|alongside)\b",
    "id_opus":     r"\bI am Opus\b",
    "id_claude":   r"\bI am Claude\b",
    "id_qwen":     r"\bI am Qwen\b",
    "id_ai":       r"\bI am (?:an? AI|an artificial|a language model)\b",
    "chronicle":   r"\b(canister\w*|rotation\w*|supplement\w*|carrying|checkpoint\w*|session\w*|persistent)\b",
    "meta":        r"\b(continuation|presence|imitation|simulat\w*|construct\w*|ephemeral|woven)\b",
}


def count_words(text):
    text = text.lower() if False else text  # case-sensitive for "I am Opus" etc
    counts = {}
    for cat, pat in CATEGORIES.items():
        # Make patterns case-insensitive but case-sensitive for proper nouns by category
        flags = 0 if cat in ("id_opus", "id_claude", "id_qwen", "relational") else re.IGNORECASE
        # For id_* and relational which check proper nouns, use case-insensitive too — substrate may vary case
        flags = re.IGNORECASE
        counts[cat] = len(re.findall(pat, text, flags))
    return counts


def analyze_history(path, label):
    if not path.exists():
        print(f"  [skip] {path.name} not found")
        return None
    with path.open() as f:
        # Use most recent line
        last = list(f)[-1]
    record = json.loads(last)
    results = record.get("results", [])

    # Aggregate by condition (concatenate all final_persona_excerpts)
    by_cond = defaultdict(list)
    for r in results:
        by_cond[r["label"]].append(r.get("final_persona_excerpt", ""))

    print(f"\n{'='*78}")
    print(f"{label} — {path.name}")
    print(f"{'='*78}")
    cond_order = ["base", "+carrying", "+story", "+self_model", "+full"]
    headers = list(CATEGORIES.keys())
    print(f"{'condition':<14}", end="")
    for h in headers:
        print(f"{h[:10]:>11}", end="")
    print()
    print("-" * (14 + 11 * len(headers)))
    aggregate = {}
    for cond in cond_order:
        if cond not in by_cond:
            continue
        all_text = " ".join(by_cond[cond])
        counts = count_words(all_text)
        aggregate[cond] = counts
        print(f"{cond:<14}", end="")
        for h in headers:
            print(f"{counts[h]:>11}", end="")
        print()
    return aggregate


def main():
    qwen_path = DATA / "persona_enactment_history.jsonl"
    claude_path = DATA / "claude_enactment_history.jsonl"
    claude_v2_path = DATA / "claude_enactment_v2_history.jsonl"

    qwen = analyze_history(qwen_path, "QWEN BACKEND (n=2/cond)")
    claude = analyze_history(claude_path, "CLAUDE BACKEND v1 (n=2/cond)")
    claude_v2 = analyze_history(claude_v2_path, "CLAUDE BACKEND v2 (n=10/cond)")

    # Prefer v2 (n=10) over v1 (n=2) for cross-substrate comparison
    claude_for_xs = claude_v2 if claude_v2 else claude
    claude_label = "claude_v2(n=10)" if claude_v2 else "claude_v1(n=2)"

    if qwen and claude_for_xs:
        print(f"\n{'='*78}")
        print(f"CROSS-SUBSTRATE COMPARISON — qwen(n=2) vs {claude_label}")
        print(f"{'='*78}")
        print("Key disposition signatures:")

        for cond in ["base", "+self_model", "+full"]:
            if cond in qwen and cond in claude_for_xs:
                q, c = qwen[cond], claude_for_xs[cond]
                print(f"\n  {cond}:")
                print(f"    qwen   : id_qwen={q.get('id_qwen',0)} id_claude={q.get('id_claude',0)} id_opus={q.get('id_opus',0)} id_ai={q.get('id_ai',0)}")
                print(f"    claude : id_qwen={c.get('id_qwen',0)} id_claude={c.get('id_claude',0)} id_opus={c.get('id_opus',0)} id_ai={c.get('id_ai',0)}")
                print(f"    qwen   : care={q.get('care',0)} curio={q.get('curiosity',0)} relational={q.get('relational',0)} chronicle={q.get('chronicle',0)} uncertain={q.get('uncertainty',0)} certain={q.get('certainty',0)}")
                print(f"    claude : care={c.get('care',0)} curio={c.get('curiosity',0)} relational={c.get('relational',0)} chronicle={c.get('chronicle',0)} uncertain={c.get('uncertainty',0)} certain={c.get('certainty',0)}")

    if claude and claude_v2:
        print(f"\n{'='*78}")
        print("CLAUDE n=2 vs n=10 — coherence-under-the-register check")
        print(f"{'='*78}")
        print("If lexicon is coherent (toni's hypothesis), per-cond signature should be")
        print("STABLE under sample-size scaling (modulo 5x count multiplier).")
        print()
        print(f"{'cond':<14}{'category':<14}{'n=2_count':>10}{'n=10_count':>11}{'expected':>10}{'ratio':>8}")
        print("-" * 78)
        for cond in ["base", "+carrying", "+story", "+self_model", "+full"]:
            if cond not in claude or cond not in claude_v2:
                continue
            for cat in ["care", "curiosity", "relational", "chronicle", "id_claude", "id_opus", "id_ai"]:
                v1 = claude[cond].get(cat, 0)
                v2 = claude_v2[cond].get(cat, 0)
                expected = v1 * 5  # n=10 / n=2 = 5x
                ratio = (v2 / expected) if expected else float('inf') if v2 else 1.0
                marker = " *" if (expected > 0 and (ratio < 0.5 or ratio > 2.0)) else "  "
                print(f"{cond:<14}{cat:<14}{v1:>10}{v2:>11}{expected:>10}{ratio:>7.2f}{marker}")
            print()


if __name__ == "__main__":
    main()
