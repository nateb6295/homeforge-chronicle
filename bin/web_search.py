#!/usr/bin/env python3
"""web_search — minimal web search + URL fetch for Hermes (and Opus).

Usage:
    python3 web_search.py search "query" [--n 5] [--json]
    python3 web_search.py fetch URL [--max 4000]

Search tries SearXNG JSON on rotating public instances, then falls back to
DuckDuckGo HTML scraping. Fetch uses trafilatura for readable text extraction.
No browser, no API keys.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import sys
import time
from html import unescape
from typing import Any

import requests

CACHE_DIR = os.environ.get("WEB_SEARCH_CACHE_DIR", "/tmp/web_search_cache")
CACHE_TTL_SEARCH = 900   # 15 minutes
CACHE_TTL_FETCH = 1800   # 30 minutes


def _cache_path(kind: str, key: str) -> str:
    h = hashlib.sha1(f"{kind}|{key}".encode()).hexdigest()[:16]
    return os.path.join(CACHE_DIR, f"{kind}-{h}.json")


def _cache_get(kind: str, key: str, ttl: int) -> Any:
    path = _cache_path(kind, key)
    try:
        st = os.stat(path)
    except FileNotFoundError:
        return None
    if time.time() - st.st_mtime > ttl:
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def _cache_put(kind: str, key: str, value: Any) -> None:
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(_cache_path(kind, key), "w") as f:
            json.dump(value, f)
    except Exception:
        pass

SEARX_INSTANCES = [
    "https://searx.be",
    "https://search.inetol.net",
    "https://priv.au",
    "https://searx.tiekoetter.com",
]
UA = "Mozilla/5.0 (X11; Linux aarch64) hermes-web-search/1.0"
TIMEOUT = 10


def _searx(query: str, n: int) -> list[dict[str, str]]:
    instances = SEARX_INSTANCES[:]
    random.shuffle(instances)
    last_err: Exception | None = None
    for base in instances:
        try:
            r = requests.get(
                f"{base}/search",
                params={"q": query, "format": "json", "safesearch": "0"},
                headers={"User-Agent": UA, "Accept": "application/json"},
                timeout=TIMEOUT,
            )
            if r.status_code != 200 or "application/json" not in r.headers.get("content-type", ""):
                continue
            data = r.json()
            results = []
            for item in data.get("results", [])[:n]:
                results.append({
                    "title": (item.get("title") or "").strip(),
                    "url": item.get("url", "").strip(),
                    "snippet": (item.get("content") or "").strip(),
                    "engine": item.get("engine", ""),
                    "source": f"searx:{base}",
                })
            if results:
                return results
        except Exception as e:
            last_err = e
            continue
    if last_err:
        print(f"[searx] all instances failed: {last_err}", file=sys.stderr)
    return []


def _ddg_html(query: str, n: int) -> list[dict[str, str]]:
    try:
        r = requests.post(
            "https://html.duckduckgo.com/html/",
            data={"q": query},
            headers={"User-Agent": UA},
            timeout=TIMEOUT,
        )
    except Exception as e:
        print(f"[ddg] {e}", file=sys.stderr)
        return []
    if r.status_code != 200:
        return []
    html = r.text
    pattern = re.compile(
        r'<a[^>]+class="result__a"[^>]+href="([^"]+)"[^>]*>(.*?)</a>.*?'
        r'class="result__snippet"[^>]*>(.*?)</a>',
        re.DOTALL,
    )
    out = []
    for m in pattern.finditer(html):
        url, title, snippet = m.group(1), m.group(2), m.group(3)
        title = unescape(re.sub(r"<[^>]+>", "", title)).strip()
        snippet = unescape(re.sub(r"<[^>]+>", "", snippet)).strip()
        out.append({"title": title, "url": url, "snippet": snippet, "engine": "ddg", "source": "ddg:html"})
        if len(out) >= n:
            break
    return out


def search(query: str, n: int = 5) -> list[dict[str, Any]]:
    cache_key = f"{n}|{query.strip().lower()}"
    cached = _cache_get("search", cache_key, CACHE_TTL_SEARCH)
    if cached is not None:
        return cached
    results = _searx(query, n)
    if not results:
        results = _ddg_html(query, n)
    if results:
        _cache_put("search", cache_key, results)
    return results


def fetch(url: str, max_chars: int = 4000) -> dict[str, Any]:
    cache_key = f"{max_chars}|{url.strip()}"
    cached = _cache_get("fetch", cache_key, CACHE_TTL_FETCH)
    if cached is not None:
        return cached
    try:
        import trafilatura
    except ImportError:
        return {"url": url, "error": "trafilatura not installed"}
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
    except Exception as e:
        return {"url": url, "error": f"fetch failed: {e}"}
    if r.status_code != 200:
        return {"url": url, "error": f"status {r.status_code}"}
    text = trafilatura.extract(r.text, include_comments=False, include_tables=True, favor_recall=True)
    if not text:
        return {"url": url, "error": "no extractable text"}
    text = text.strip()
    if len(text) > max_chars:
        text = text[:max_chars] + "\n...[truncated]"
    title_match = re.search(r"<title[^>]*>(.*?)</title>", r.text, re.DOTALL | re.IGNORECASE)
    title = unescape(title_match.group(1).strip()) if title_match else ""
    result = {"url": url, "title": title, "text": text, "chars": len(text)}
    _cache_put("fetch", cache_key, result)
    return result


def _print_search(results: list[dict[str, Any]], as_json: bool) -> None:
    if as_json:
        print(json.dumps(results, indent=2))
        return
    if not results:
        print("(no results)")
        return
    for i, item in enumerate(results, 1):
        print(f"{i}. {item['title']}")
        print(f"   {item['url']}")
        if item.get("snippet"):
            print(f"   {item['snippet']}")
        print()


def _print_fetch(result: dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(result, indent=2))
        return
    if result.get("error"):
        print(f"ERROR: {result['error']}")
        return
    if result.get("title"):
        print(result["title"])
        print("=" * min(len(result["title"]), 80))
    print(result.get("text", ""))
    print(f"\n[{result.get('chars', 0)} chars from {result.get('url', '')}]")


def main() -> int:
    p = argparse.ArgumentParser(prog="web_search")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("search", help="Search the web")
    s.add_argument("query", help="Search query")
    s.add_argument("--n", type=int, default=5, help="Number of results (default 5)")
    s.add_argument("--json", action="store_true", help="Output JSON")

    f = sub.add_parser("fetch", help="Fetch and extract readable text from a URL")
    f.add_argument("url", help="URL to fetch")
    f.add_argument("--max", type=int, default=4000, help="Max chars to return (default 4000)")
    f.add_argument("--json", action="store_true", help="Output JSON")

    args = p.parse_args()
    if args.cmd == "search":
        _print_search(search(args.query, args.n), args.json)
    elif args.cmd == "fetch":
        _print_fetch(fetch(args.url, args.max), args.json)
    return 0


if __name__ == "__main__":
    sys.exit(main())
