#!/usr/bin/env python3
"""Depth Evaluator — Closes the pipeline's depth gap.

The seeker finds things. The digest summarizes them. Nobody evaluates.
This script fetches the actual content of recent seeker discoveries
and runs them through Gemma for an actionability assessment against
our current architecture.

Usage:
    python3 depth_eval.py              # Evaluate top 3 recent discoveries
    python3 depth_eval.py --hours 6    # Look back 6 hours
    python3 depth_eval.py --dry        # Fetch + show, don't log
    python3 depth_eval.py --all        # Show all recent, don't evaluate
"""
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime

import requests

DB_PATH = os.environ.get("CHRONICLE_DB",
    "/mnt/hdd/chronicle-data/processed.db")

# Gemma via llama-server
GEMMA_URL = "http://localhost:11435"
GEMMA_MODEL = "gemma-4-26B-A4B-it-Q4_K_M.gguf"

LOOKBACK_HOURS = 4
MAX_EVAL = 3
FETCH_TIMEOUT = 15

EVAL_PROMPT = """You are evaluating a research finding for a local AI infrastructure project.

The project runs on an NVIDIA Jetson AGX Orin (64GB RAM):
- Gemma 4 26B via llama-server for local inference
- Ollama for embeddings (nomic-embed-text, 768d)
- Chronicle memory system (ICP canisters + SQLite + FTS5 + FAISS)
- RSS/arxiv feeds → clustering → synthesis pipeline
- Hermes agent (70B via cloud API) for Discord interaction
- Control vectors for activation steering (critical_analysis at alpha 0.5)

{existing_knowledge}
Evaluate this finding:
---
{content}
---

Answer these three questions concisely:
1. ACTIONABLE? Can we use this in the next week? If yes, how specifically?
2. INTERESTING? What's the non-obvious insight? Does it connect to what we already know?
3. SKIP? If not useful, one sentence why.

Be direct. Under 200 words total."""


def get_discoveries(hours=LOOKBACK_HOURS, limit=10):
    """Get recent seeker discoveries."""
    db = sqlite3.connect(DB_PATH, timeout=10)
    rows = db.execute(
        "SELECT id, content, created_at FROM activity_feed "
        "WHERE source='seeker:algo' AND activity_type='discovery' "
        f"AND created_at > strftime('%s','now','-{hours} hours') "
        "ORDER BY created_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    db.close()
    return rows


def extract_url(content):
    """Extract URL from seeker discovery content."""
    urls = re.findall(r'https?://\S+', content)
    return urls[0] if urls else None


def fetch_content(url):
    """Fetch and extract text content from a URL."""
    try:
        # Try trafilatura first (lightweight, no browser)
        import trafilatura
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            text = trafilatura.extract(downloaded, include_links=False,
                                       include_tables=False)
            if text and len(text) > 100:
                return text[:3000]  # Cap for Gemma context
    except ImportError:
        pass

    # Fallback: raw requests + basic extraction
    try:
        resp = requests.get(url, timeout=FETCH_TIMEOUT,
                           headers={"User-Agent": "Chronicle/1.0"})
        resp.raise_for_status()
        text = resp.text
        # Strip HTML tags crudely
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text[:3000] if len(text) > 100 else None
    except Exception as e:
        return f"[fetch failed: {e}]"


def get_existing_knowledge(content):
    """Search capsule knowledge base for related existing knowledge."""
    try:
        from capsule_fts import search_bm25
        # Extract key terms from the first 200 chars of content
        query = content[:200].replace('\n', ' ')
        # Remove URLs and common stop words
        query = re.sub(r'https?://\S+', '', query)
        query = re.sub(r'\[.*?\]', '', query)
        query = query.strip()[:100]  # FTS5 query from first ~100 chars

        if not query:
            return ""

        results = search_bm25(query, limit=3)
        if not results:
            return ""

        context = "What we already know about this topic:\n"
        for cid, text, topic, ts, score in results[:3]:
            context += f"- [{topic}] {text[:200]}\n"
        return context + "\n"
    except Exception:
        return ""


def evaluate_with_gemma(content, retries=1):
    """Send content to Gemma for actionability evaluation with existing knowledge context."""
    existing = get_existing_knowledge(content)
    prompt = EVAL_PROMPT.format(content=content[:2500], existing_knowledge=existing)

    try:
        from gemma_quality import check_output
    except ImportError:
        check_output = None

    for attempt in range(retries + 1):
        try:
            resp = requests.post(
                f"{GEMMA_URL}/v1/chat/completions",
                json={
                    "model": GEMMA_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 300,
                    "temperature": 0.3 + (attempt * 0.1),
                },
                timeout=120,
            )
            resp.raise_for_status()
            text = resp.json()["choices"][0]["message"]["content"]

            if check_output:
                quality = check_output(text)
                if quality["degenerate"]:
                    print(f"  [quality gate: {quality['reason']}]", file=sys.stderr)
                    if attempt < retries:
                        print(f"  [retrying with temp {0.3 + ((attempt+1) * 0.1):.1f}]", file=sys.stderr)
                        continue
                    return f"[degenerate output filtered: {quality['reason']}]"

            return text
        except Exception as e:
            return f"[evaluation failed: {e}]"


def log_evaluation(discovery_id, url, evaluation):
    """Log depth evaluation to activity_feed."""
    db = sqlite3.connect(DB_PATH, timeout=10)
    db.execute(
        "INSERT INTO activity_feed (source, activity_type, content, metadata, created_at) "
        "VALUES (?, ?, ?, ?, strftime('%s','now'))",
        (
            "depth_eval",
            "evaluation",
            f"Evaluated seeker discovery #{discovery_id}:\n{evaluation}",
            json.dumps({"url": url, "discovery_id": discovery_id}),
        )
    )
    db.commit()
    db.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Depth evaluator for seeker discoveries")
    parser.add_argument("--hours", type=int, default=LOOKBACK_HOURS)
    parser.add_argument("--dry", action="store_true", help="Don't log to DB")
    parser.add_argument("--all", action="store_true", help="Show all, don't evaluate")
    parser.add_argument("--max", type=int, default=MAX_EVAL)
    args = parser.parse_args()

    discoveries = get_discoveries(args.hours, limit=20)

    if not discoveries:
        print(f"No seeker discoveries in the last {args.hours}h.")
        return

    if args.all:
        print(f"=== {len(discoveries)} discoveries in last {args.hours}h ===\n")
        for did, content, ts in discoveries:
            ts_str = datetime.fromtimestamp(ts).strftime("%H:%M")
            url = extract_url(content) or "no-url"
            # First line of content (the title/tag)
            title = content.split("\n")[0][:80]
            print(f"  [{ts_str}] #{did} {title}")
            print(f"          {url}")
        return

    print(f"Evaluating top {min(args.max, len(discoveries))} of {len(discoveries)} discoveries...\n")

    evaluated = 0
    for did, content, ts in discoveries[:args.max]:
        url = extract_url(content)
        title = content.split("\n")[0][:80]
        ts_str = datetime.fromtimestamp(ts).strftime("%H:%M")

        print(f"--- Discovery #{did} [{ts_str}] ---")
        print(f"  {title}")

        if url:
            print(f"  Fetching {url}...", file=sys.stderr)
            fetched = fetch_content(url)
            if fetched and not fetched.startswith("[fetch"):
                full_content = f"{content}\n\n--- Fetched Content ---\n{fetched}"
            else:
                full_content = content
                print(f"  {fetched}", file=sys.stderr)
        else:
            full_content = content

        print(f"  Evaluating...", file=sys.stderr)
        evaluation = evaluate_with_gemma(full_content)
        print(f"\n{evaluation}\n")

        if not args.dry and url:
            log_evaluation(did, url, evaluation)
            print(f"  [logged to activity_feed]", file=sys.stderr)

        evaluated += 1

    print(f"\n=== {evaluated} discoveries evaluated ===")


if __name__ == "__main__":
    main()
