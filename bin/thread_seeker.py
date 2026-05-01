#!/usr/bin/env python3
"""Thread Seeker — generates paper/resource targets from current thread state.

Instead of waiting for captures, this script reads the active thread and generates
a seek list of 3-5 arxiv papers, tools, or resources that would advance the inquiry.

Uses DeepSeek V3.2 to generate search queries from thread context, then searches arxiv.

Usage:
    python3 bin/thread_seeker.py             # Generate seek list
    python3 bin/thread_seeker.py --execute   # Generate + fetch abstracts
"""

import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import requests

DEEPINFRA_BASE_URL = "https://api.deepinfra.com/v1/openai"
DB_PATH = "/mnt/hdd/chronicle-data/processed.db"


def _load_api_key(key_name):
    env_path = Path.home() / "chronicle" / "chronicle.env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith(f"{key_name}="):
                return line.split("=", 1)[1].strip()
    return os.environ.get(key_name, "")


def get_thread_context():
    """Read active thread title, question, and last 3 advances."""
    result = subprocess.run(
        ["python3", str(Path.home() / "chronicle" / "bin" / "read_thread.py")],
        capture_output=True, text=True, timeout=15
    )
    if result.returncode != 0:
        return None
    try:
        data = json.loads(result.stdout)
        thread = data["thread"]
        history = data.get("history", [])[:3]
        advances = []
        for h in history:
            content = h.get("content", "")
            # First line is the title
            title = content.split("\n")[0][:120] if content else ""
            advances.append(title)
        return {
            "title": thread["title"],
            "question": thread["question"],
            "recent_advances": advances,
        }
    except (json.JSONDecodeError, KeyError):
        return None


def generate_seek_queries(thread_ctx):
    """Use V3.2 to generate arxiv search queries from thread context."""
    api_key = _load_api_key("DEEPINFRA_API_KEY")
    if not api_key:
        print("No DEEPINFRA_API_KEY found")
        return []

    prompt = f"""Given this active research thread about AI/ML, generate exactly 5 arxiv search queries that would find SPECIFIC papers in cs.CL, cs.AI, or cs.LG advancing the inquiry.

Thread: {thread_ctx['title']}
Question: {thread_ctx['question']}
Recent advances:
{chr(10).join(f'- {a}' for a in thread_ctx['recent_advances'])}

IMPORTANT: Generate queries using specific technical terms from the AI/ML literature. Use phrases like "persona steering language model", "system prompt robustness", "identity specification LLM", "chain of thought faithfulness", "adversarial prompt injection defense". Do NOT use vague terms like "calibration effort alignment" that match papers outside AI/ML.

Return ONLY a JSON array of objects, each with "query" (specific arxiv search string) and "why" (one sentence on what gap it fills). No other text."""

    for attempt in range(2):
        try:
            resp = requests.post(
                f"{DEEPINFRA_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "deepseek-ai/DeepSeek-V3.2",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 500,
                    "temperature": 0.7,
                },
                timeout=60,
            )
            resp.raise_for_status()
            break
        except requests.exceptions.ReadTimeout:
            if attempt == 0:
                print("  (retrying DeepInfra...)")
                continue
            print("DeepInfra timed out twice")
            return []
    content = resp.json()["choices"][0]["message"]["content"]

    # Extract JSON array
    try:
        # Try direct parse
        queries = json.loads(content)
        if isinstance(queries, list):
            return queries[:5]
    except json.JSONDecodeError:
        pass

    # Try extracting from markdown code block
    match = re.search(r"```(?:json)?\s*(\[.*?\])\s*```", content, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))[:5]
        except json.JSONDecodeError:
            pass

    print(f"Could not parse queries from: {content[:200]}")
    return []


def search_arxiv(query, max_results=3):
    """Search arxiv API for papers matching query."""
    import urllib.parse
    encoded = urllib.parse.quote(query)
    # Restrict to cs.CL, cs.AI, cs.LG categories to avoid irrelevant results
    cat_filter = urllib.parse.quote("cat:cs.CL OR cat:cs.AI OR cat:cs.LG")
    url = f"http://export.arxiv.org/api/query?search_query=all:{encoded}+AND+({cat_filter})&start=0&max_results={max_results}&sortBy=relevance&sortOrder=descending"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        # Simple XML parsing for arxiv Atom feed
        entries = []
        for entry_match in re.finditer(r"<entry>(.*?)</entry>", resp.text, re.DOTALL):
            entry_text = entry_match.group(1)
            title_m = re.search(r"<title>(.*?)</title>", entry_text, re.DOTALL)
            summary_m = re.search(r"<summary>(.*?)</summary>", entry_text, re.DOTALL)
            id_m = re.search(r"<id>(.*?)</id>", entry_text)
            published_m = re.search(r"<published>(.*?)</published>", entry_text)
            if title_m and id_m:
                entries.append({
                    "title": " ".join(title_m.group(1).strip().split()),
                    "abstract": " ".join(summary_m.group(1).strip().split())[:300] if summary_m else "",
                    "url": id_m.group(1).strip(),
                    "published": published_m.group(1).strip()[:10] if published_m else "",
                })
        return entries
    except Exception as e:
        print(f"  arxiv search failed: {e}")
        return []


def log_seek(db, queries, results):
    """Log seek results to DB."""
    db.execute("""CREATE TABLE IF NOT EXISTS thread_seeks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        query TEXT NOT NULL,
        reason TEXT,
        results_json TEXT,
        created_at INTEGER NOT NULL
    )""")
    now = int(time.time())
    for q in queries:
        query_text = q.get("query", "")
        reason = q.get("why", "")
        matching = [r for r in results if r.get("_query") == query_text]
        db.execute(
            "INSERT INTO thread_seeks (query, reason, results_json, created_at) VALUES (?, ?, ?, ?)",
            (query_text, reason, json.dumps(matching), now)
        )
    db.commit()


def main():
    execute = "--execute" in sys.argv

    print("Thread Seeker — generating targets from thread state\n")

    ctx = get_thread_context()
    if not ctx:
        print("Could not read thread context.")
        return

    print(f"Thread: {ctx['title']}")
    print(f"Question: {ctx['question'][:100]}...")
    print(f"Recent advances: {len(ctx['recent_advances'])}")
    print()

    queries = generate_seek_queries(ctx)
    if not queries:
        print("No queries generated.")
        return

    print(f"Generated {len(queries)} seek targets:")
    all_results = []
    for i, q in enumerate(queries):
        print(f"\n  [{i+1}] {q.get('query', '?')}")
        print(f"      Why: {q.get('why', '?')}")

        if execute:
            papers = search_arxiv(q.get("query", ""), max_results=2)
            for p in papers:
                p["_query"] = q.get("query", "")
                all_results.append(p)
                print(f"      → {p['title'][:80]}")
                print(f"        {p['url']} ({p['published']})")

    if execute and all_results:
        try:
            db = sqlite3.connect(DB_PATH)
            log_seek(db, queries, all_results)
            db.close()
            print(f"\nLogged {len(all_results)} results to thread_seeks table.")
        except Exception as e:
            print(f"\nWarning: failed to log: {e}")

    print(f"\nDone. {len(queries)} queries" + (f", {len(all_results)} papers found" if execute else ""))


if __name__ == "__main__":
    main()
