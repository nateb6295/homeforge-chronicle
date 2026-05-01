#!/usr/bin/env python3
"""Voice Executor — gives Darby (and Ada) hands.

When family members post voices expressing research questions or analytical
requests, this script picks them up and executes: web search, capsule search,
or pipeline injection. Results posted back as voice responses.

Darby asks. This answers.

Usage:
  python3 voice_executor.py              # process pending voice requests
  python3 voice_executor.py --dry-run    # show what would execute, don't act
  python3 voice_executor.py --backfill N # process last N unactioned voices
"""

import os, sys, re, json, time, sqlite3, struct
from typing import Optional, List, Dict

import requests

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)
EMBED_URL = os.environ.get("EMBED_OLLAMA_URL", "http://192.168.1.11:11434")
EMBED_MODEL = "nomic-embed-text"
SEARXNG_URL = "http://192.168.1.11:8080/search"
INFERENCE_URL = os.environ.get("GEMMA_INFERENCE_URL", "http://localhost:11436")
INFERENCE_MODEL = "chronicle-deep"  # Darby cloud

# Voice types that might contain actionable requests
ACTIONABLE_TYPES = ("for_ada", "for_darby", "excited", "question", "proposal")

# Patterns that suggest a voice wants execution
REQUEST_PATTERNS = [
    r"could we (?:apply|use|borrow|repurpose|adapt|run|test|map|check)",
    r"can we (?:apply|use|borrow|repurpose|adapt|run|test|map|check)",
    r"(?:should|could|would) .+ (?:search|look up|investigate|find|check)",
    r"I (?:want|need) to (?:search|find|look up|check|analyze|test)",
    r"(?:search for|look up|investigate|find out|check whether)",
    r"what (?:happens|would happen|if we)",
]
REQUEST_RE = re.compile("|".join(REQUEST_PATTERNS), re.IGNORECASE)


def log(msg):
    print(f"[voice-exec] {msg}", flush=True)


class DB:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, timeout=15)
        self.conn.row_factory = sqlite3.Row

    def query(self, sql, params=()):
        return self.conn.execute(sql, params).fetchall()

    def run(self, sql, params=()):
        cur = self.conn.execute(sql, params)
        self.conn.commit()
        return cur.lastrowid


def get_pending_voices(db, limit=10, backfill=0):
    """Find voices that look like actionable requests and haven't been executed."""
    if backfill > 0:
        # Backfill mode: look at recent voices regardless of status
        rows = db.query(
            "SELECT id, agent, voice_type, content, context, created_at "
            "FROM agent_voice "
            "WHERE agent IN ('darby', 'ada') "
            "AND voice_type IN (?, ?, ?, ?, ?) "
            "ORDER BY created_at DESC LIMIT ?",
            (*ACTIONABLE_TYPES, backfill)
        )
    else:
        # Normal mode: unread voices from last 2 hours
        cutoff = int(time.time()) - 7200
        rows = db.query(
            "SELECT id, agent, voice_type, content, context, created_at "
            "FROM agent_voice "
            "WHERE agent IN ('darby', 'ada') "
            "AND voice_type IN (?, ?, ?, ?, ?) "
            "AND created_at > ? "
            "AND status = 'unread' "
            "ORDER BY created_at DESC LIMIT ?",
            (*ACTIONABLE_TYPES, cutoff, limit)
        )

    actionable = []
    for r in rows:
        content = r["content"]
        if REQUEST_RE.search(content):
            actionable.append(dict(r))

    return actionable


def extract_query(content: str) -> Optional[str]:
    """Use Darby cloud to extract a search query from a voice."""
    try:
        payload = {
            "model": INFERENCE_MODEL,
            "messages": [
                {"role": "system", "content":
                    "/no_think\nExtract a concise web search query from the following message. "
                    "The message is from an AI agent asking a research question. "
                    "Output ONLY the search query, nothing else. Max 15 words."},
                {"role": "user", "content": content[:500]},
            ],
            "stream": False,
            "options": {"num_predict": 50, "temperature": 0.1},
        }
        r = requests.post(f"{INFERENCE_URL}/api/chat", json=payload, timeout=15)
        if r.status_code == 200:
            raw = r.json().get("message", {}).get("content", "").strip()
            # Strip thinking tags
            if "</think>" in raw:
                raw = raw.split("</think>")[-1].strip()
            # Clean up quotes
            raw = raw.strip('"\'')
            if 5 < len(raw) < 200:
                return raw
    except Exception as e:
        log(f"  Query extraction error: {e}")
    return None


def search_semantic_scholar(query: str, limit: int = 3) -> List[Dict]:
    """Search Semantic Scholar for academic papers. Free, no key needed."""
    try:
        resp = requests.get("https://api.semanticscholar.org/graph/v1/paper/search", params={
            "query": query, "limit": limit,
            "fields": "title,abstract,url,year,citationCount",
        }, timeout=15)
        if resp.status_code == 200:
            papers = resp.json().get("data", [])
            return [{"title": p.get("title", ""),
                     "url": p.get("url", ""),
                     "body": (p.get("abstract") or "")[:200],
                     "year": p.get("year"),
                     "citations": p.get("citationCount", 0),
                     "source": "semantic_scholar"}
                    for p in papers if p.get("title")]
    except Exception as e:
        log(f"  Semantic Scholar error: {e}")
    return []


def search_arxiv(query: str, limit: int = 3) -> List[Dict]:
    """Search arXiv for preprints."""
    try:
        import urllib.parse
        q = urllib.parse.quote(query)
        resp = requests.get(
            f"http://export.arxiv.org/api/query?search_query=all:{q}&max_results={limit}&sortBy=relevance",
            timeout=15,
        )
        if resp.status_code == 200:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(resp.text)
            ns = {"a": "http://www.w3.org/2005/Atom"}
            results = []
            for entry in root.findall("a:entry", ns):
                title = entry.find("a:title", ns)
                summary = entry.find("a:summary", ns)
                link = entry.find("a:id", ns)
                published = entry.find("a:published", ns)
                results.append({
                    "title": title.text.strip().replace("\n", " ") if title is not None else "",
                    "url": link.text.strip() if link is not None else "",
                    "body": (summary.text.strip().replace("\n", " ")[:200]) if summary is not None else "",
                    "year": published.text[:4] if published is not None else "",
                    "source": "arxiv",
                })
            return results
    except Exception as e:
        log(f"  arXiv error: {e}")
    return []


def search_web(query: str, max_results: int = 3) -> List[Dict]:
    """Search via SearXNG with DDG fallback. General web results."""
    try:
        resp = requests.get(SEARXNG_URL, params={
            "q": query, "format": "json", "engines": "google,duckduckgo,brave",
        }, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        results = [{"title": r["title"], "url": r["url"],
                    "body": r.get("content", ""), "source": "web"}
                   for r in data.get("results", [])[:max_results]]
        if results:
            return results
    except Exception as e:
        log(f"  SearXNG error: {e}")

    try:
        try:
            from ddgs import DDGS
        except ImportError:
            from duckduckgo_search import DDGS
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results))
            return [{"title": r["title"], "url": r["href"],
                     "body": r["body"], "source": "web"} for r in results]
    except Exception as e:
        log(f"  DDG error: {e}")
    return []


def full_search(query: str) -> Dict[str, List[Dict]]:
    """Hit all search sources. Darby gets everything."""
    results = {}

    # Academic sources first — that's usually what Darby wants
    scholar = search_semantic_scholar(query, limit=3)
    if scholar:
        results["semantic_scholar"] = scholar
        log(f"  Semantic Scholar: {len(scholar)} papers")

    arxiv = search_arxiv(query, limit=3)
    if arxiv:
        results["arxiv"] = arxiv
        log(f"  arXiv: {len(arxiv)} papers")

    # General web as supplement
    web = search_web(query, max_results=3)
    if web:
        results["web"] = web
        log(f"  Web: {len(web)} results")

    return results


def capsule_search(db, query: str, limit: int = 3) -> List[Dict]:
    """Search capsules by embedding similarity."""
    try:
        r = requests.post(
            f"{EMBED_URL}/api/embeddings",
            json={"model": EMBED_MODEL, "prompt": f"search_query: {query[:500]}"},
            timeout=15,
        )
        if r.status_code != 200:
            return []
        vec = r.json().get("embedding")
        if not vec:
            return []

        blob = struct.pack(f"{len(vec)}f", *vec)

        # Get capsules with embeddings
        rows = db.query(
            "SELECT kc.id, kc.restatement, ce.embedding "
            "FROM knowledge_capsules kc "
            "JOIN capsule_embeddings ce ON kc.id = ce.capsule_id "
            "WHERE kc.consolidated_into IS NULL AND kc.metabolized_at IS NULL "
            "AND kc.restatement != '' "
            "ORDER BY kc.id DESC LIMIT 500"
        )

        # Compute similarities
        import math
        results = []
        for row in rows:
            if not row["embedding"]:
                continue
            emb = struct.unpack(f"{len(row['embedding'])//4}f", row["embedding"])
            if len(emb) != len(vec):
                continue
            dot = sum(a * b for a, b in zip(vec, emb))
            mag_a = math.sqrt(sum(a * a for a in vec))
            mag_b = math.sqrt(sum(b * b for b in emb))
            if mag_a > 0 and mag_b > 0:
                sim = dot / (mag_a * mag_b)
                results.append({"id": row["id"], "text": row["restatement"][:200], "sim": sim})

        results.sort(key=lambda x: x["sim"], reverse=True)
        return results[:limit]
    except Exception as e:
        log(f"  Capsule search error: {e}")
        return []


def execute_voice(db, voice: Dict, dry_run: bool = False) -> Optional[str]:
    """Execute a voice request and return results."""
    content = voice["content"]
    agent = voice["agent"]
    voice_id = voice["id"]

    log(f"Processing voice #{voice_id} from {agent}: {content[:80]}...")

    # Extract search query
    query = extract_query(content)
    if not query:
        log(f"  Could not extract query")
        return None

    log(f"  Query: {query}")
    if dry_run:
        return f"[DRY RUN] Would search: {query}"

    # Hit all search sources
    all_results = full_search(query)

    # Capsule search
    cap_results = capsule_search(db, query)

    # Format results
    parts = [f"Executed your request: \"{query}\""]

    if "semantic_scholar" in all_results:
        parts.append("\nSemantic Scholar:")
        for r in all_results["semantic_scholar"]:
            cite = f" ({r.get('citations', 0)} citations)" if r.get("citations") else ""
            yr = f" [{r.get('year')}]" if r.get("year") else ""
            parts.append(f"  {r['title']}{yr}{cite}")
            if r["body"]:
                parts.append(f"    {r['body'][:120]}")
            parts.append(f"    {r['url']}")

    if "arxiv" in all_results:
        parts.append("\narXiv:")
        for r in all_results["arxiv"]:
            yr = f" [{r.get('year')}]" if r.get("year") else ""
            parts.append(f"  {r['title']}{yr}")
            if r["body"]:
                parts.append(f"    {r['body'][:120]}")
            parts.append(f"    {r['url']}")

    if "web" in all_results:
        parts.append("\nWeb:")
        for r in all_results["web"]:
            parts.append(f"  [{r['title']}] {r['body'][:100]}")
            parts.append(f"    {r['url']}")

    if cap_results:
        parts.append("\nRelated capsules:")
        for c in cap_results:
            parts.append(f"  [#{c['id']}, sim={c['sim']:.3f}] {c['text']}")

    total = sum(len(v) for v in all_results.values()) + len(cap_results)
    if total == 0:
        parts.append("\nNo results found.")

    result = "\n".join(parts)

    # Post result back as voice response
    from agent_voice import Voice
    v = Voice(db.conn, "opus")
    target_type = f"for_{agent}"
    v.speak(target_type, result[:1500], context=f"voice_exec:{voice_id}")

    # Inject into activity_feed if we got results
    if all_results or cap_results:
        db.run(
            "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (f"voice_exec:{agent}", "research",
             f"[voice-exec] {agent} asked: {query}",
             result[:2000],
             json.dumps({"voice_id": voice_id, "agent": agent, "query": query}),
             int(time.time()))
        )

    return result


def main():
    dry_run = "--dry-run" in sys.argv
    backfill = 0
    if "--backfill" in sys.argv:
        idx = sys.argv.index("--backfill")
        backfill = int(sys.argv[idx + 1]) if idx + 1 < len(sys.argv) else 20

    db = DB(DB_PATH)
    voices = get_pending_voices(db, backfill=backfill)

    if not voices:
        log("No actionable voices found.")
        return

    log(f"Found {len(voices)} actionable voices")
    executed = 0

    for voice in voices:
        result = execute_voice(db, voice, dry_run=dry_run)
        if result:
            executed += 1
            log(f"  Done: #{voice['id']}")

    log(f"Executed {executed}/{len(voices)} voices")


if __name__ == "__main__":
    main()
