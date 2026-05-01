#!/usr/bin/env python3
"""Gemma output quality gate — catches degeneration before it hits the pipeline.

Detects:
- Character repetition loops (-s-s-s-s, _____, etc.)
- Word/phrase repetition (same 3+ word phrase repeating 4+ times)
- High token entropy collapse (>60% of output is one token)
- Truncation without conclusion (mid-word cutoff)

Usage:
    from gemma_quality import check_output, is_degenerate

    text = get_gemma_response(...)
    result = check_output(text)
    if result["degenerate"]:
        print(f"Degenerate output: {result['reason']}")
    else:
        # Safe to use
        process(text)
"""
import re
from collections import Counter


def check_repetition_loops(text, min_pattern_len=1, max_pattern_len=5, min_repeats=8):
    """Detect character-level repetition loops like -s-s-s-s or ______."""
    for plen in range(min_pattern_len, max_pattern_len + 1):
        for i in range(len(text) - plen * min_repeats):
            pattern = text[i:i + plen]
            if not pattern.strip():
                continue
            repeated = pattern * min_repeats
            if repeated in text:
                return True, f"char loop: '{pattern}' x{min_repeats}+"
    return False, ""


def check_phrase_repetition(text, min_phrase_words=2, min_repeats=4):
    """Detect repeated multi-word phrases."""
    words = text.split()
    if len(words) < min_phrase_words * min_repeats:
        return False, ""

    # Check bigrams and trigrams
    for n in range(min_phrase_words, min(5, len(words) // min_repeats + 1)):
        ngrams = [" ".join(words[i:i + n]) for i in range(len(words) - n + 1)]
        counts = Counter(ngrams)
        for phrase, count in counts.most_common(3):
            if count >= min_repeats and len(phrase) > 3:
                return True, f"phrase repeat: '{phrase[:40]}' x{count}"
    return False, ""


def check_entropy_collapse(text, threshold=0.6):
    """Detect when output is dominated by a single character."""
    if len(text) < 20:
        return False, ""
    counts = Counter(text.lower())
    total = sum(counts.values())
    most_common_char, most_common_count = counts.most_common(1)[0]
    ratio = most_common_count / total
    if ratio > threshold and most_common_char not in (' ', '\n'):
        return True, f"entropy collapse: '{most_common_char}' is {ratio:.0%} of output"
    return False, ""


def check_truncation(text):
    """Detect mid-word or mid-sentence truncation."""
    if len(text) < 50:
        return False, ""
    stripped = text.rstrip()
    if not stripped:
        return False, ""
    # Ends mid-word (no space, no punctuation at end)
    last_char = stripped[-1]
    if last_char.isalpha() and not stripped.endswith(('.', '!', '?', ':', '"', "'", ')', ']')):
        # Check if last "word" is actually cut off (< 3 chars after last space)
        last_space = stripped.rfind(' ')
        if last_space > 0:
            last_word = stripped[last_space + 1:]
            if len(last_word) < 4:
                return True, f"truncated mid-word: '...{stripped[-30:]}'"
    return False, ""


def check_hyphenation_degeneration(text):
    """Detect the specific Gemma pattern of excessive hyphenation: word-word-word-word."""
    # Count sequences of 4+ hyphenated words
    hyphen_chains = re.findall(r'(?:\w+-){3,}\w+', text)
    if len(hyphen_chains) > 3:
        return True, f"hyphenation degeneration: {len(hyphen_chains)} chains"
    # Also check for excessive hyphens overall
    hyphen_ratio = text.count('-') / max(len(text), 1)
    if hyphen_ratio > 0.08:
        return True, f"hyphen density: {hyphen_ratio:.1%} of output"
    return False, ""


def check_output(text):
    """Run all quality checks on Gemma output.

    Returns dict with:
        degenerate: bool
        reason: str (empty if clean)
        checks: dict of individual check results
    """
    if not text or len(text.strip()) < 10:
        return {"degenerate": True, "reason": "empty or near-empty output", "checks": {}}

    checks = {}
    reasons = []

    for name, checker in [
        ("repetition_loop", lambda t: check_repetition_loops(t)),
        ("phrase_repeat", lambda t: check_phrase_repetition(t)),
        ("entropy_collapse", lambda t: check_entropy_collapse(t)),
        ("truncation", lambda t: check_truncation(t)),
        ("hyphenation", lambda t: check_hyphenation_degeneration(t)),
    ]:
        hit, reason = checker(text)
        checks[name] = {"hit": hit, "reason": reason}
        if hit:
            reasons.append(reason)

    return {
        "degenerate": len(reasons) > 0,
        "reason": "; ".join(reasons) if reasons else "",
        "checks": checks,
    }


def is_degenerate(text):
    """Simple boolean check — is this output degenerate?"""
    return check_output(text)["degenerate"]


if __name__ == "__main__":
    import sys
    # Test with examples
    tests = [
        ("Clean output", "This is a well-formed response about knowledge distillation techniques."),
        ("Char loop", "The-s-s-s-s-s-s-s-s-s-s-s-s-s response was good"),
        ("Phrase repeat", "the model the model the model the model the model works"),
        ("Hyphen degen", "use-the-model-to-generate and then use-the-output-to-train and also run-the-pipeline-to-check and finally test-the-result-to-verify"),
        ("Truncated", "This is a response about the importance of dat"),
        ("Real degeneration", "Update the **Chronicle memory**-s-s-s-s-s-s-s-s-s-s-s-s-s-s-s-s-s-s-s-s-s-s-s-s"),
    ]

    for name, text in tests:
        result = check_output(text)
        status = "DEGEN" if result["degenerate"] else "CLEAN"
        print(f"  [{status}] {name}: {result['reason'] or 'ok'}")
