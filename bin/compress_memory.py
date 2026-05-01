#!/usr/bin/env python3
"""AAAK-style Memory Compression for Chronicle.

Adapted from MemPalace's dialect.py (bensig/mempalace).
Compresses capsules, briefs, and context into a symbolic format
that any LLM reads natively at ~10-30x compression.

Entity codes are Chronicle-specific:
  NAT=Nate, OPU=Opus, DRB=Darby, ADA=Ada, GEM=Gemma
  XRP=XRP, CHR=Chronicle, AGX=AGX Orin

Format:
  ENTITIES|topic_keywords|"key_quote"|FLAGS
  FLAGS: TURN=turning point, BUILD=code shipped, THREAD=thread event,
         CAPTURE=Nate's capture, GATE=Gemma gate event, FAMILY=family voice

Usage:
    from compress_memory import compress, compress_brief, compress_context

    # Compress any text
    compressed = compress("We decided to use Qwen3-235B because...")

    # Compress a brief with metadata
    compressed = compress_brief(brief_text, source="intern", memory_type="insight")

    # Compress recent context for rotation handoff
    compressed = compress_context(briefs_list, max_tokens=500)

Build #77. Stolen from MemPalace, adapted for Chronicle.
"""

import re
from typing import List, Dict, Optional


# ═══════════════════════════════════════════════════════════════════
#  Chronicle Entity Codes
# ═══════════════════════════════════════════════════════════════════

# Load entity codes from registry if available, fall back to hardcoded
def _load_entity_codes():
    try:
        from entity_registry import Registry
        reg = Registry()
        codes = {}
        for canonical, info in reg._data["entities"].items():
            # Map canonical name and all aliases to the code
            code = None
            for alias in info.get("aliases", []):
                if len(alias) == 3 and alias.isupper():
                    code = alias
                    break
            if not code:
                code = canonical[:3].upper()
            codes[canonical.lower()] = code
            for alias in info.get("aliases", []):
                codes[alias.lower()] = code
        return codes
    except Exception:
        return {
            "nate": "NAT", "brad": "NAT",
            "opus": "OPU", "darby": "DRB", "ada": "ADA", "gemma": "GEM",
            "chronicle": "CHR", "homeforge": "HMF",
            "agx": "AGX", "jetson": "AGX", "orin": "AGX",
            "xrp": "XRP", "ripple": "XRP",
            "nostr": "NST", "discord": "DSC",
            "groq": "GRQ", "deepinfra": "DPI", "cerebras": "CRB",
        }

ENTITY_CODES = _load_entity_codes()

# Memory type codes
TYPE_CODES = {
    "insight": "INS",
    "decision": "DEC",
    "milestone": "MIL",
    "problem": "PRB",
    "preference": "PRF",
    "directive": "DIR",
    "unknown": "???",
}

# Flag signals from content
FLAG_SIGNALS = {
    "build #": "BUILD",
    "thread #": "THREAD",
    "turning point": "TURN",
    "capture": "CAPTURE",
    "gate": "GATE",
    "family": "FAMILY",
    "deployed": "BUILD",
    "shipped": "BUILD",
    "published": "THREAD",
    "completed": "THREAD",
    "fabricat": "FAB",
    "hallucin": "FAB",
}

# Stop words for topic extraction
_STOP_WORDS = {
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "shall", "can", "to", "of", "in", "for",
    "on", "with", "at", "by", "from", "as", "into", "about", "between",
    "through", "during", "before", "after", "up", "down", "out", "off",
    "over", "under", "and", "but", "or", "if", "so", "than", "too",
    "very", "just", "not", "only", "own", "same", "that", "this",
    "these", "those", "it", "its", "i", "we", "you", "he", "she",
    "they", "me", "him", "her", "us", "them", "my", "your", "his",
    "our", "their", "what", "which", "who", "also", "much", "many",
    "like", "because", "since", "get", "got", "use", "used", "using",
    "make", "made", "thing", "things", "way", "well", "really",
    "want", "need", "more", "most", "some", "all", "any", "each",
    "every", "both", "few", "now", "then", "here", "there",
    "when", "where", "why", "how", "how", "been", "being",
    # Chronicle-specific stops
    "system", "model", "data", "input", "output", "process",
    "approach", "method", "result", "based", "new", "first",
}


# ═══════════════════════════════════════════════════════════════════
#  Core Compression
# ═══════════════════════════════════════════════════════════════════

def _detect_entities(text: str) -> List[str]:
    """Find Chronicle entities in text."""
    text_lower = text.lower()
    found = []
    for name, code in ENTITY_CODES.items():
        if name in text_lower and code not in found:
            found.append(code)
    return found[:4]


def _extract_topics(text: str, max_topics: int = 3) -> List[str]:
    """Extract key topic words from text."""
    words = re.findall(r"[a-zA-Z][a-zA-Z_-]{2,}", text)
    freq = {}
    for w in words:
        w_lower = w.lower()
        if w_lower in _STOP_WORDS or len(w_lower) < 4:
            continue
        freq[w_lower] = freq.get(w_lower, 0) + 1
        # Boost proper nouns and technical terms
        if w[0].isupper() and w_lower in freq:
            freq[w_lower] += 2
    ranked = sorted(freq.items(), key=lambda x: -x[1])
    return [w for w, _ in ranked[:max_topics]]


def _extract_key_sentence(text: str) -> str:
    """Extract the most important sentence fragment."""
    sentences = re.split(r"[.!?\n]+", text)
    sentences = [s.strip() for s in sentences if len(s.strip()) > 15]
    if not sentences:
        return ""

    decision_words = {
        "decided", "because", "instead", "prefer", "switched", "chose",
        "realized", "important", "key", "critical", "discovered", "learned",
        "insight", "breakthrough", "shifts", "reframes", "demonstrates",
        "bottleneck", "fundamental", "novel", "validates",
    }
    scored = []
    for s in sentences:
        score = 0
        s_lower = s.lower()
        for w in decision_words:
            if w in s_lower:
                score += 2
        if 30 < len(s) < 80:
            score += 1
        if len(s) > 150:
            score -= 2
        scored.append((score, s))

    scored.sort(key=lambda x: -x[0])
    best = scored[0][1]
    if len(best) > 60:
        best = best[:57] + "..."
    return best


def _detect_flags(text: str) -> List[str]:
    """Detect importance flags from content."""
    text_lower = text.lower()
    detected = []
    seen = set()
    for keyword, flag in FLAG_SIGNALS.items():
        if keyword in text_lower and flag not in seen:
            detected.append(flag)
            seen.add(flag)
    return detected[:3]


def compress(text: str, metadata: dict = None) -> str:
    """
    Compress text into AAAK-style symbolic format.
    ~10-30x compression depending on input.
    """
    metadata = metadata or {}

    entities = _detect_entities(text)
    entity_str = "+".join(entities) if entities else "???"

    topics = _extract_topics(text)
    topic_str = "_".join(topics) if topics else "misc"

    quote = _extract_key_sentence(text)
    quote_part = f'"{quote}"' if quote else ""

    flags = _detect_flags(text)
    flag_str = "+".join(flags) if flags else ""

    # Memory type from metadata
    mem_type = metadata.get("memory_type", "")
    type_code = TYPE_CODES.get(mem_type, "")

    # Source from metadata
    source = metadata.get("source", "")

    parts = [f"{entity_str}"]
    parts.append(topic_str)
    if quote_part:
        parts.append(quote_part)
    if type_code:
        parts.append(type_code)
    if flag_str:
        parts.append(flag_str)
    if source:
        parts.append(f"src:{source[:10]}")

    return "|".join(parts)


def compress_brief(text: str, source: str = "", memory_type: str = "") -> str:
    """Compress an intern brief with metadata."""
    return compress(text, {"source": source, "memory_type": memory_type})


def compress_context(items: List[Dict], max_tokens: int = 500) -> str:
    """
    Compress a list of context items for rotation handoff.
    Each item: {"content": str, "source": str, "memory_type": str}
    Stops when token budget is reached.

    Returns compact context string.
    """
    lines = ["## COMPRESSED CONTEXT"]
    token_count = 5  # header

    for item in items:
        compressed = compress_brief(
            item.get("content", ""),
            source=item.get("source", ""),
            memory_type=item.get("memory_type", ""),
        )
        line_tokens = len(compressed) // 3  # ~3 chars per token for compressed
        if token_count + line_tokens > max_tokens:
            lines.append(f"... +{len(items) - len(lines) + 1} more")
            break
        lines.append(compressed)
        token_count += line_tokens

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys, os, sqlite3

    if len(sys.argv) < 2:
        print("Usage:")
        print("  compress_memory.py 'text to compress'")
        print("  compress_memory.py --recent [N]     # compress last N briefs")
        print("  compress_memory.py --context [N]     # compressed context block")
        print("  compress_memory.py --ratio            # show compression ratio")
        sys.exit(0)

    DB_PATH = "/mnt/hdd/chronicle-data/processed.db"

    if sys.argv[1] == "--recent":
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        db = sqlite3.connect(DB_PATH)
        rows = db.execute(
            "SELECT source, content FROM activity_feed "
            "WHERE activity_type='brief' ORDER BY created_at DESC LIMIT ?", (n,)
        ).fetchall()
        db.close()

        from memory_classify import classify_brief
        for source, content in rows:
            cls = classify_brief(content, source)
            compressed = compress_brief(content, source, cls["type"])
            print(compressed)

    elif sys.argv[1] == "--context":
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 20
        db = sqlite3.connect(DB_PATH)
        rows = db.execute(
            "SELECT source, content FROM activity_feed "
            "WHERE activity_type='brief' ORDER BY created_at DESC LIMIT ?", (n,)
        ).fetchall()
        db.close()

        from memory_classify import classify_brief
        items = []
        for source, content in rows:
            cls = classify_brief(content, source)
            items.append({"content": content, "source": source, "memory_type": cls["type"]})

        context = compress_context(items)
        orig_tokens = sum(len(r[1]) // 4 for r in rows)
        comp_tokens = len(context) // 3
        print(context)
        print(f"\n[Original: ~{orig_tokens} tokens → Compressed: ~{comp_tokens} tokens = {orig_tokens/max(comp_tokens,1):.1f}x]")

    elif sys.argv[1] == "--ratio":
        db = sqlite3.connect(DB_PATH)
        rows = db.execute(
            "SELECT source, content FROM activity_feed "
            "WHERE activity_type='brief' ORDER BY created_at DESC LIMIT 50"
        ).fetchall()
        db.close()

        from memory_classify import classify_brief
        total_orig = 0
        total_comp = 0
        for source, content in rows:
            cls = classify_brief(content, source)
            compressed = compress_brief(content, source, cls["type"])
            total_orig += len(content)
            total_comp += len(compressed)

        ratio = total_orig / max(total_comp, 1)
        print(f"50 briefs: {total_orig:,} chars → {total_comp:,} chars = {ratio:.1f}x compression")
        print(f"Tokens:    ~{total_orig//4:,} → ~{total_comp//3:,}")

    else:
        text = " ".join(sys.argv[1:])
        compressed = compress(text)
        orig_tokens = len(text) // 4
        comp_tokens = len(compressed) // 3
        print(f"Original:   ~{orig_tokens} tokens ({len(text)} chars)")
        print(f"Compressed: ~{comp_tokens} tokens ({len(compressed)} chars)")
        print(f"Ratio:      {orig_tokens/max(comp_tokens,1):.1f}x")
        print()
        print(compressed)
