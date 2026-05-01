#!/usr/bin/env python3
"""Memory Type Classifier — Tag captures/briefs by memory type.

Adapted from MemPalace's general_extractor.py (bensig/mempalace).
Pure heuristic, no LLM. Classifies text into:
  - decision:   choices made, architecture decisions, trade-offs
  - preference:  persistent rules, "always X", "never Y"
  - milestone:   breakthroughs, deployments, firsts
  - problem:     bugs, crashes, root causes, fixes
  - insight:     connections, realizations, cross-domain synthesis
  - directive:   instructions from Nate, standing orders

Usage:
    from memory_classify import classify_memory, classify_brief

    result = classify_memory("We decided to use Qwen3-235B because...")
    # → {"type": "decision", "confidence": 0.8, "signals": ["decided", "because"]}

    # Classify an intern brief
    result = classify_brief(brief_text, source="operator:capture")
    # → {"type": "directive", "confidence": 0.9, ...}

Build #76. Stolen from MemPalace, adapted for Chronicle.
"""

import re
from typing import Dict, List, Tuple


# ═══════════════════════════════════════════════════════════════════
#  Marker Sets — patterns that signal each memory type
# ═══════════════════════════════════════════════════════════════════

DECISION_MARKERS = [
    r"\b(decided|chose|went with|picked|settled on|switched to)\b",
    r"\blet'?s (use|go with|try|pick|choose|switch)\b",
    r"\b(better|worse) (to|than|approach|option)\b",
    r"\binstead of\b",
    r"\brather than\b",
    r"\b(because|the reason)\b",
    r"\btrade-?off\b",
    r"\barchitecture\b.*\b(decision|choice)\b",
    r"\bapproach\b",
    r"\b(replaced|migrated|swapped)\b",
]

PREFERENCE_MARKERS = [
    r"\bi prefer\b",
    r"\b(always|never) (use|do|run|deploy|skip)\b",
    r"\bdon'?t (ever )?(use|do|mock|touch|change)\b",
    r"\bi (like|hate) (to|when|how)\b",
    r"\bplease (always|never|don'?t)\b",
    r"\bmy (rule|preference|style) is\b",
    r"\bnon-negotiable\b",
    r"\bstanding (order|rule|directive)\b",
]

MILESTONE_MARKERS = [
    r"\b(it works|it worked|got it working)\b",
    r"\b(fixed|solved|resolved|cracked|nailed)\b",
    r"\bbreakthrough\b",
    r"\bfigured (it )?out\b",
    r"\bfirst (time|ever)\b",
    r"\b(built|created|shipped|deployed|launched|released)\b",
    r"\bbuild #\d+\b",
    r"\bthread #\d+\b.*\b(completed|published)\b",
    r"\bpost #\d+\b",
    r"\d+x (compression|faster|better|improvement)\b",
    r"\d+% (reduction|improvement|faster|better)\b",
    r"\bnew (capability|feature|tool)\b",
    # Analytical brief register
    r"\bdemonstrat(e|es|ed)\b",
    r"\bvalidat(e|es|ed|ion)\b",
    r"\bachiev(e|es|ed|ing)\b",
    r"\bstate-of-the-art\b",
    r"\boutperform(s|ed|ing)?\b",
    r"\bnovel\b",
    r"\bfirst\b.*\b(to|ever)\b",
    r"\bpioneer(s|ed|ing)?\b",
    r"\brecord\b.*\b(performance|accuracy|speed)\b",
    r"\brevolution\b",
]

PROBLEM_MARKERS = [
    r"\b(bug|error|crash|fail|broke|broken|issue)\b",
    r"\b(doesn'?t|not|won'?t) work\b",
    r"\bkeeps? (failing|crashing|breaking)\b",
    r"\broot cause\b",
    r"\bfab(rication)? rate\b",
    r"\bhallucinated?\b",
    r"\bfabricated?\b",
    r"\bthe (problem|issue|bug) (is|was)\b",
    r"\bworkaround\b",
    r"\brollback\b",
]

INSIGHT_MARKERS = [
    r"\bconnect(s|ion|ed)\b.*\b(to|between|across)\b",
    r"\bthe (key|crucial|important) (insight|finding|observation)\b",
    r"\bthis (means|implies|suggests|explains)\b",
    r"\bwhat (this|it) (means|tells|shows)\b",
    r"\bthe pattern\b",
    r"\banalog(y|ous)\b",
    r"\bstructural(ly)?\b.*\b(similar|identical|parallel)\b",
    r"\bcross-domain\b",
    r"\bconverg(ent|ence|es)\b",
    r"\bemerg(ent|ence|es|ing)\b",
    r"\bimplication\b",
    r"\bself-modif(ying|ication)\b",
    r"\brecursiv(e|ely|ion)\b",
    # Analytical brief register — how intern writes
    r"\breframes?\b",
    r"\bshifts? the\b",
    r"\bisn'?t just\b.*\bit'?s\b",
    r"\bnot (just|merely|simply)\b.*\bbut\b",
    r"\bthe bottleneck\b",
    r"\bthe (real|deeper|actual) (issue|question|point)\b",
    r"\bsignal(s|ing)?\b",
    r"\bparadigm\b",
    r"\barchitectur(e|al)\b",
    r"\bframework\b",
    r"\bfundamental(ly)?\b",
    r"\bscaling\b",
    r"\bself-organiz(e|ing|ation)\b",
    r"\bcultural transmission\b",
    r"\bepistemic\b",
    r"\bcompress(ion|es|ed)\b",
    r"\bhierarch(y|ical)\b",
    r"\bcogniti(ve|on)\b",
    r"\bneurophysiolog\b",
    r"\btop-down\b",
    r"\bbottom-up\b",
    r"\bfeedback\b.*\b(loop|signal|mechanism)\b",
    r"\blatent\b",
    r"\bsemantic\b",
    r"\bontolog(y|ical)\b",
]

DIRECTIVE_MARKERS = [
    r"\bnate (said|says|wants|directed)\b",
    r"\bper nate\b",
    r"\boperator\b.*\b(directive|instruction|order)\b",
    r"\b(do|don'?t|stop|start|always|never)\b.*\b(immediately|now|going forward)\b",
    r"\brule\b.*\b(is|:)\b",
    r"\beffective immediately\b",
]

ALL_MARKERS = {
    "decision": DECISION_MARKERS,
    "preference": PREFERENCE_MARKERS,
    "milestone": MILESTONE_MARKERS,
    "problem": PROBLEM_MARKERS,
    "insight": INSIGHT_MARKERS,
    "directive": DIRECTIVE_MARKERS,
}


# ═══════════════════════════════════════════════════════════════════
#  Sentiment — for disambiguation
# ═══════════════════════════════════════════════════════════════════

POSITIVE_WORDS = {
    "works", "working", "solved", "fixed", "nailed", "breakthrough",
    "shipped", "deployed", "success", "beautiful", "amazing", "brilliant",
    "clean", "elegant", "perfect", "strong", "healthy", "green",
}

NEGATIVE_WORDS = {
    "bug", "error", "crash", "fail", "failed", "broken", "broke",
    "issue", "problem", "wrong", "stuck", "blocked", "fabricat",
    "hallucin", "stale", "dead", "red", "missing", "lost",
}


def _get_sentiment(text: str) -> str:
    words = set(w.lower() for w in re.findall(r"\b\w+\b", text))
    pos = len(words & POSITIVE_WORDS)
    neg = len(words & NEGATIVE_WORDS)
    if pos > neg:
        return "positive"
    elif neg > pos:
        return "negative"
    return "neutral"


def _has_resolution(text: str) -> bool:
    text_lower = text.lower()
    patterns = [
        r"\bfixed\b", r"\bsolved\b", r"\bresolved\b",
        r"\bgot it working\b", r"\bit works\b", r"\bnailed\b",
    ]
    return any(re.search(p, text_lower) for p in patterns)


# ═══════════════════════════════════════════════════════════════════
#  Core Classification
# ═══════════════════════════════════════════════════════════════════

def _score_markers(text: str, markers: List[str]) -> Tuple[float, List[str]]:
    """Score text against regex markers. Returns (score, matched_keywords)."""
    text_lower = text.lower()
    score = 0.0
    keywords = []
    for marker in markers:
        matches = list(re.finditer(marker, text_lower))
        if matches:
            score += len(matches)
            for m in matches:
                # Use the full match text, not just the captured group
                matched_text = m.group(0).strip()
                if matched_text and len(matched_text) > 2:
                    keywords.append(matched_text)
    return score, list(set(keywords))[:5]


def classify_memory(text: str) -> Dict:
    """
    Classify text into a memory type.

    Returns:
        {"type": str, "confidence": float, "signals": list, "scores": dict}
    """
    if not text or len(text.strip()) < 20:
        return {"type": "unknown", "confidence": 0.0, "signals": [], "scores": {}}

    scores = {}
    all_signals = {}
    for mem_type, markers in ALL_MARKERS.items():
        score, keywords = _score_markers(text, markers)
        if score > 0:
            scores[mem_type] = score
            all_signals[mem_type] = keywords

    if not scores:
        return {"type": "unknown", "confidence": 0.0, "signals": [], "scores": {}}

    # Length bonus — longer texts with signals are more confident
    text_len = len(text)
    if text_len > 500:
        length_bonus = 2
    elif text_len > 200:
        length_bonus = 1
    else:
        length_bonus = 0

    best_type = max(scores, key=scores.get)
    best_score = scores[best_type] + length_bonus

    # Disambiguation
    sentiment = _get_sentiment(text)
    if best_type == "problem" and _has_resolution(text):
        if sentiment == "positive":
            best_type = "milestone"
    elif best_type == "problem" and sentiment == "positive":
        if "milestone" in scores:
            best_type = "milestone"

    confidence = min(1.0, best_score / 5.0)

    return {
        "type": best_type,
        "confidence": round(confidence, 3),
        "signals": all_signals.get(best_type, [])[:5],
        "scores": {k: round(v, 2) for k, v in scores.items()},
    }


def classify_brief(text: str, source: str = "") -> Dict:
    """
    Classify an intern brief with source-aware boosting.
    Operator captures boost directive signals. Feed captures boost insight.
    """
    result = classify_memory(text)

    # Source-aware boosting
    if source.startswith("operator") or "capture" in source:
        if "directive" in result["scores"]:
            result["scores"]["directive"] *= 1.5
    if source.startswith("feed-") or source.startswith("seeker"):
        if "insight" in result["scores"]:
            result["scores"]["insight"] *= 1.2

    # Recompute best if scores changed
    if result["scores"]:
        new_best = max(result["scores"], key=result["scores"].get)
        if new_best != result["type"]:
            result["type"] = new_best
            result["confidence"] = min(1.0, result["scores"][new_best] / 5.0)

    return result


# ═══════════════════════════════════════════════════════════════════
#  Batch Classification (for existing captures)
# ═══════════════════════════════════════════════════════════════════

def classify_batch(texts: List[str]) -> List[Dict]:
    """Classify a batch of texts. Returns list of classifications."""
    return [classify_memory(t) for t in texts]


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys, os
    import sqlite3

    if len(sys.argv) < 2:
        print("Usage:")
        print("  memory_classify.py 'text to classify'")
        print("  memory_classify.py --recent [N]        # classify last N briefs from DB")
        print("  memory_classify.py --stats              # type distribution of recent briefs")
        sys.exit(0)

    if sys.argv[1] == "--recent":
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 20
        db = sqlite3.connect(
            os.path.expanduser("~/.homeforge-chronicle/processed.db")
            if not os.path.exists("/mnt/hdd/chronicle-data/processed.db")
            else "/mnt/hdd/chronicle-data/processed.db"
        )
        rows = db.execute(
            "SELECT id, source, substr(content,1,300) FROM activity_feed "
            "WHERE activity_type='brief' ORDER BY created_at DESC LIMIT ?",
            (n,)
        ).fetchall()
        db.close()
        for row_id, source, content in rows:
            result = classify_brief(content, source)
            print(f"  #{row_id:6} [{result['type']:12}] conf={result['confidence']:.2f}  src={source}")
            if result['signals']:
                print(f"         signals: {', '.join(result['signals'][:3])}")

    elif sys.argv[1] == "--stats":
        import os
        db_path = "/mnt/hdd/chronicle-data/processed.db"
        db = sqlite3.connect(db_path)
        rows = db.execute(
            "SELECT source, content FROM activity_feed "
            "WHERE activity_type='brief' AND created_at > strftime('%s','now','-24 hours')"
        ).fetchall()
        db.close()

        from collections import Counter
        types = Counter()
        for source, content in rows:
            result = classify_brief(content, source)
            types[result["type"]] += 1

        print(f"\nMemory type distribution (last 24h, {len(rows)} briefs):")
        for mtype, count in types.most_common():
            pct = count / len(rows) * 100 if rows else 0
            bar = "█" * int(pct / 2)
            print(f"  {mtype:12} {count:4} ({pct:5.1f}%) {bar}")

    else:
        import os
        text = " ".join(sys.argv[1:])
        result = classify_memory(text)
        print(f"Type:       {result['type']}")
        print(f"Confidence: {result['confidence']}")
        print(f"Signals:    {', '.join(result['signals'])}")
        print(f"Scores:     {result['scores']}")
