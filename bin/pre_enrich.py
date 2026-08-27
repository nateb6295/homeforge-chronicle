#!/usr/bin/env python3
"""Pre-compression enrichment: expand telegraphic episodic_trace entries.

Reverses sedimentation where entries compress from content → pointer → orphan
across CCS cycles. Identifies entries where pointers do more work than content,
retrieves source material, and builds an enrichment block for the compressor.

Architecture:
  1. Heuristic pre-filter — skip entirely if no entries look telegraphic
  2. Haiku landscape scan — one LLM call classifies entries + provides
     retrieval hints (what concept to search for, not just keywords)
  3. Programmatic retrieval — capsule search using Haiku's hints
  4. Block construction — (entry, source) pairs for compressor context

The enrichment block is ADDITIVE: raw entries stay in DB unchanged,
enriched source material goes into enhanced_context. The main compressor
decides what to keep.

Integration: called from stabilized_compress.py before injection assembly.
"""

import json
import os
import re
import subprocess
import time
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
MCP_BIN = Path.home() / ".local" / "bin" / "chronicle-mcp"
ENRICHMENT_LOG = Path.home() / "chronicle" / "data" / "enrichment_log.jsonl"
ENGINE_URL = "http://127.0.0.1:11436"
HAIKU_MODEL = "chronicle-compress-light"

TELEGRAPHIC_THRESHOLD = 0.4
MAX_ENRICHMENTS = 5
MIN_ENRICHMENT_INTERVAL_MIN = 20


# ---------------------------------------------------------------------------
# Phase 1: Heuristic pre-filter
# ---------------------------------------------------------------------------

def _heuristic_score(text: str) -> tuple[float, list[str]]:
    """Score an entry for telegraphic-ness. Returns (score, reasons).

    Intelligence: short ≠ telegraphic, long ≠ substantial.
    A 50-char confirmed finding is complete. A 200-char entry that's all
    pointers with no content is telegraphic.
    """
    score = 0.0
    reasons = []

    if re.match(r"^capture:\s*\[", text):
        score += 0.6
        reasons.append("bare_capture")

    stripped = text.rstrip()
    if stripped and stripped[-1] not in '.!?)\'"…:':
        if len(stripped) > 30:
            score += 0.3
            reasons.append("truncated")

    if len(text) < 60:
        score += 0.3
        reasons.append("short")
    elif len(text) < 100:
        score += 0.1
        reasons.append("brief")

    pointers = re.findall(
        r"F\d{2,3}|#\d{3}|Thread #\d+|§\d+\.\d+|exp_\w+\.py|data/\S+\.(?:md|py|json)",
        text,
    )
    words = text.split()
    if words and len(pointers) / len(words) > 0.12:
        score += 0.2
        reasons.append("pointer_dense")

    return min(score, 1.0), reasons


def heuristic_prefilter(episodic_trace: list[str]) -> tuple[bool, int]:
    """Quick check: are there enough telegraphic entries to warrant enrichment?"""
    telegraphic_count = 0
    for entry in episodic_trace:
        score, _ = _heuristic_score(str(entry))
        if score >= TELEGRAPHIC_THRESHOLD:
            telegraphic_count += 1
    return telegraphic_count > 0, telegraphic_count


# ---------------------------------------------------------------------------
# Phase 2: Haiku landscape scan
# ---------------------------------------------------------------------------

HAIKU_PROMPT = """You are a memory curator preparing episodic trace entries for compression.
A fresh AI instance will read these entries with NO prior context — it has never
seen the original conversations where these entries were created.

Review each entry and classify:
- SUBSTANTIAL: Self-contained, intelligible without external lookup
- TELEGRAPHIC: Contains pointers (finding numbers, thread refs, names) that
  assume context the reader doesn't have. The entry gestures at content
  without providing it.
- BARE: Raw URL or minimal-content reference with no analysis

For TELEGRAPHIC and BARE entries, provide a retrieval_hint: a natural-language
search query (15-40 words) that would find the source material this entry points
to. The hint should capture the CONCEPT, not just repeat the labels. Think:
what would I search for to find the actual finding, conversation, or analysis
this entry is compressed from?

Current episodic_trace:
{entries}

Return ONLY a JSON array. Include ONLY telegraphic/bare entries (omit substantial):
[{{"index": 0, "class": "telegraphic", "retrieval_hint": "...", "reason": "5 words"}}]

If all entries are substantial, return [].
"""


def _call_haiku(prompt: str) -> str | None:
    """Call Haiku via the chronicle engine (Ollama-format /api/chat). Returns response text or None."""
    import requests

    payload = {
        "model": HAIKU_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "options": {"temperature": 0.2, "num_predict": 2048},
        "stream": False,
    }
    try:
        r = requests.post(
            f"{ENGINE_URL}/api/chat",
            json=payload,
            timeout=45,
        )
        if r.status_code != 200:
            print(f"    Haiku error {r.status_code}: {r.text[:200]}")
            return None
        data = r.json()
        # Engine returns Ollama-format: {"message": {"content": "..."}}
        # or OpenAI-format: {"choices": [{"message": {"content": "..."}}]}
        msg = data.get("message", {})
        if msg:
            return msg.get("content", "")
        return data.get("choices", [{}])[0].get("message", {}).get("content", "")
    except Exception as e:
        print(f"    Haiku call failed: {e}")
        return None


def _parse_haiku_response(text: str) -> list[dict]:
    """Extract JSON array from Haiku's response, handling markdown fences."""
    if not text:
        return []
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        cleaned = "\n".join(lines).strip()
    try:
        result = json.loads(cleaned)
        if isinstance(result, list):
            return result
    except json.JSONDecodeError:
        match = re.search(r"\[.*\]", cleaned, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
    return []


def haiku_landscape_scan(episodic_trace: list[str]) -> list[dict]:
    """One Haiku call: classify all entries, return flagged ones with retrieval hints."""
    entries_text = "\n".join(
        f"[{i}] {str(e)}" for i, e in enumerate(episodic_trace)
    )
    prompt = HAIKU_PROMPT.format(entries=entries_text)
    raw = _call_haiku(prompt)
    if not raw:
        return []
    flagged = _parse_haiku_response(raw)
    valid = []
    for item in flagged:
        idx = item.get("index")
        if isinstance(idx, int) and 0 <= idx < len(episodic_trace):
            valid.append({
                "index": idx,
                "text": str(episodic_trace[idx]),
                "class": item.get("class", "telegraphic"),
                "retrieval_hint": item.get("retrieval_hint", ""),
                "reason": item.get("reason", ""),
            })
    return valid[:MAX_ENRICHMENTS]


# ---------------------------------------------------------------------------
# Phase 3: Programmatic retrieval
# ---------------------------------------------------------------------------

def _mcp_search(query: str, limit: int, env: dict) -> list[dict]:
    """Search capsules via MCP binary."""
    if not MCP_BIN.exists():
        return []

    init_msg = json.dumps({
        "jsonrpc": "2.0",
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "pre-enrich", "version": "1.0"},
        },
        "id": 1,
    })
    search_msg = json.dumps({
        "jsonrpc": "2.0",
        "method": "tools/call",
        "params": {
            "name": "search_memory",
            "arguments": {"query": query, "limit": limit},
        },
        "id": 2,
    })

    try:
        result = subprocess.run(
            [str(MCP_BIN)],
            input=f"{init_msg}\n{search_msg}\n",
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )
        for line in result.stdout.strip().split("\n"):
            try:
                d = json.loads(line)
                if d.get("id") == 2:
                    content = d.get("result", {}).get("content", [])
                    if content:
                        text = content[0].get("text", "")
                        try:
                            parsed = json.loads(text)
                            if isinstance(parsed, list):
                                return parsed
                            if isinstance(parsed, dict):
                                if "memories" in parsed:
                                    return parsed["memories"]
                                if "results" in parsed:
                                    return parsed["results"]
                        except json.JSONDecodeError:
                            pass
            except json.JSONDecodeError:
                continue
    except Exception:
        pass
    return []


def retrieve_sources(flagged: list[dict], env: dict) -> list[dict]:
    """For each flagged entry, retrieve source content using Haiku's hint."""
    enriched = []
    for item in flagged:
        hint = item.get("retrieval_hint", "")
        text = item.get("text", "")
        entry_class = item.get("class", "")

        # Bare captures are handled by capture_tracker — skip retrieval
        if entry_class == "bare" or "bare_capture" in item.get("reasons", []):
            continue

        source = None
        # Primary: use Haiku's retrieval hint
        if hint:
            results = _mcp_search(hint[:300], 2, env)
            if results:
                content = results[0].get("content", "") or results[0].get("restatement", "")
                if content and len(content) > 40:
                    source = content[:500]

        # Fallback: search using raw entry text
        if not source and text:
            results = _mcp_search(text[:200], 2, env)
            if results:
                content = results[0].get("content", "") or results[0].get("restatement", "")
                if content and len(content) > len(text) * 0.5:
                    source = content[:500]

        if source and _is_relevant(text, source):
            enriched.append({**item, "source": source})

    return enriched


_STOPWORDS = {
    "the", "and", "that", "this", "with", "for", "from", "are", "was",
    "not", "but", "has", "have", "had", "been", "will", "would", "should",
    "could", "into", "than", "then", "them", "they", "their", "there",
    "some", "more", "also", "each", "both", "does", "which", "about",
    "over", "under", "after", "before", "between", "through", "during",
    "without", "within", "being", "only",
}


def _is_relevant(entry_text: str, source_text: str) -> bool:
    """Check if retrieved source is actually relevant to the entry.

    Catches false hits like retrieving ice-layer physics for a Robert Frost entry.
    Uses keyword overlap: at least 2 significant words from the entry must appear
    in the source.
    """
    entry_words = {
        w.lower().strip(".,;:!?()[]\"'")
        for w in entry_text.split()
        if len(w) > 4 and w.lower().strip(".,;:!?()[]\"'") not in _STOPWORDS
    }
    source_lower = source_text.lower()
    matches = sum(1 for w in entry_words if w in source_lower)
    return matches >= 2


# ---------------------------------------------------------------------------
# Phase 4: Block construction
# ---------------------------------------------------------------------------

def build_enrichment_block(enriched: list[dict]) -> str:
    """Assemble enrichment context block for the main compressor."""
    if not enriched:
        return ""

    block = (
        "\n\n## Pre-Compression Enrichment (MANDATORY REWRITE)\n\n"
        "The following episodic_trace entries are TRUNCATED or TELEGRAPHIC — "
        "they end mid-word or contain pointers without content. Source material "
        "has been retrieved from the memory store.\n\n"
        "**INSTRUCTION**: You MUST REWRITE these entries in your compressed "
        "episodic_trace output. Do NOT carry them forward as-is. For each "
        "entry below, replace it with a self-contained 1-2 sentence version "
        "that incorporates the source material. A reader who has never seen "
        "the original conversation must understand what happened without "
        "looking anything up. Truncated entries carried forward verbatim is "
        "a compression failure.\n\n"
        "Do NOT copy source material verbatim — distill the substance into "
        "the rewritten entry.\n\n"
    )
    for e in enriched:
        label = e.get("class", "telegraphic")
        reason = e.get("reason", "")
        block += f"**Entry [{e['index']}]** ({label}: {reason}):\n"
        block += f"{e['text'][:300]}\n"
        block += f"**Source material**:\n{e['source'][:500]}\n\n"

    return block


# ---------------------------------------------------------------------------
# Skip conditions
# ---------------------------------------------------------------------------

def _check_interval() -> tuple[bool, str]:
    """Check if enough time has passed since last enrichment."""
    if not ENRICHMENT_LOG.exists():
        return True, "first enrichment"
    try:
        lines = ENRICHMENT_LOG.read_text().strip().split("\n")
        if lines:
            last = json.loads(lines[-1])
            gap_min = (time.time() - last.get("ts", 0)) / 60
            if gap_min < MIN_ENRICHMENT_INTERVAL_MIN:
                return False, f"too recent ({gap_min:.0f}min < {MIN_ENRICHMENT_INTERVAL_MIN}min)"
    except Exception:
        pass
    return True, "interval OK"


def _log_enrichment(trace_len: int, flagged_count: int, enriched_count: int, block_len: int, skipped: str = ""):
    """Log enrichment event for analysis."""
    try:
        ENRICHMENT_LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(ENRICHMENT_LOG, "a") as f:
            f.write(json.dumps({
                "ts": int(time.time()),
                "trace_entries": trace_len,
                "flagged": flagged_count,
                "enriched": enriched_count,
                "block_chars": block_len,
                "skipped": skipped,
            }) + "\n")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def enrich(episodic_trace: list) -> str:
    """Pre-compression enrichment. Returns context block or empty string.

    Call from stabilized_compress.py between pre-compression state capture
    and injection assembly. The returned block goes into enhanced_context
    alongside capsule_block, anchor_block, etc.
    """
    entries = [str(e) for e in episodic_trace]
    if not entries:
        print("  Pre-enrichment: empty episodic_trace, skipping")
        return ""

    # Skip condition: interval
    ok, reason = _check_interval()
    if not ok:
        print(f"  Pre-enrichment skipped: {reason}")
        return ""

    # Skip condition: heuristic pre-filter
    has_telegraphic, count = heuristic_prefilter(entries)
    if not has_telegraphic:
        print("  Pre-enrichment: no telegraphic entries (heuristic), skipping")
        _log_enrichment(len(entries), 0, 0, 0, skipped="no_telegraphic")
        return ""

    print(f"  Pre-enrichment: {count} heuristically telegraphic entries, calling Haiku...")

    # Haiku landscape scan
    flagged = haiku_landscape_scan(entries)
    if not flagged:
        print("  Pre-enrichment: Haiku found no telegraphic entries (or call failed), skipping")
        _log_enrichment(len(entries), 0, 0, 0, skipped="haiku_none")
        return ""

    print(f"  Pre-enrichment: Haiku flagged {len(flagged)} entries")
    for f in flagged:
        print(f"    [{f['index']}] {f['class']}: {f['text'][:60]}...")

    # Retrieve source material
    env = os.environ.copy()
    env["CHRONICLE_OLLAMA_URL"] = "http://localhost:11434"
    env["CHRONICLE_EMBEDDING_MODEL"] = "snowflake-arctic-embed2"

    enriched = retrieve_sources(flagged, env)
    if not enriched:
        print("  Pre-enrichment: no source material retrieved, skipping block")
        _log_enrichment(len(entries), len(flagged), 0, 0, skipped="no_sources")
        return ""

    for e in enriched:
        print(f"    ✓ [{e['index']}] → {len(e['source'])} chars retrieved")

    block = build_enrichment_block(enriched)
    _log_enrichment(len(entries), len(flagged), len(enriched), len(block))
    print(f"  Pre-enrichment: {len(enriched)}/{len(flagged)} entries enriched ({len(block)} chars)")

    return block


# ---------------------------------------------------------------------------
# Standalone test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sqlite3

    print("Pre-compression enrichment test\n")

    db = sqlite3.connect(str(DB))
    row = db.execute("SELECT episodic_trace FROM cognitive_state WHERE id = 1").fetchone()
    db.close()

    if not row or not row[0]:
        print("No episodic_trace in CCS")
        exit(1)

    try:
        trace = json.loads(row[0])
    except json.JSONDecodeError:
        trace = [row[0]]

    if not isinstance(trace, list):
        trace = [trace]

    print(f"Episodic trace: {len(trace)} entries\n")
    for i, e in enumerate(trace):
        score, reasons = _heuristic_score(str(e))
        flag = "→ TELEGRAPHIC" if score >= TELEGRAPHIC_THRESHOLD else ""
        print(f"  [{i}] score={score:.2f} {reasons} {flag}")
        print(f"      {str(e)[:100]}")
    print()

    block = enrich(trace)
    if block:
        print(f"\n--- Enrichment block ({len(block)} chars) ---")
        print(block)
    else:
        print("\nNo enrichment block produced.")
