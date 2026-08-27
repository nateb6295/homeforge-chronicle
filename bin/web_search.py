#!/usr/bin/env python3
"""web_search — minimal web search + URL fetch for Hermes (and Opus).

Usage:
    python3 web_search.py search "query" [--n 5] [--json]
    python3 web_search.py fetch URL [--max 12000] [--full] [--grep REGEX]

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
UA = ("Mozilla/5.0 (X11; Linux aarch64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/126.0 Safari/537.36")   # was "hermes-web-search/1.0" — announced
                                      # itself as a scraper AND named a retired
                                      # entity. Aug 23.
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


def _serpapi(query: str, n: int) -> list[dict[str, str]]:
    """Use SerpAPI (Google results) — requires SERPAPI_KEY env var."""
    api_key = os.environ.get("SERPAPI_KEY")
    if not api_key:
        return []
    try:
        r = requests.get(
            "https://serpapi.com/search",
            params={"q": query, "api_key": api_key, "num": n, "engine": "google"},
            headers={"User-Agent": UA},
            timeout=TIMEOUT,
        )
        if r.status_code != 200:
            print(f"[serpapi] status {r.status_code}", file=sys.stderr)
            return []
        data = r.json()
        out = []
        for item in data.get("organic_results", [])[:n]:
            out.append({
                "title": (item.get("title") or "").strip(),
                "url": item.get("link", "").strip(),
                "snippet": (item.get("snippet") or "").strip(),
                "engine": "google",
                "source": "serpapi",
            })
        return out
    except Exception as e:
        print(f"[serpapi] {e}", file=sys.stderr)
        return []


def _brave(query: str, n: int) -> list[dict[str, str]]:
    """Use Brave Search API — requires BRAVE_API_KEY env var."""
    api_key = os.environ.get("BRAVE_API_KEY")
    if not api_key:
        return []
    try:
        r = requests.get(
            "https://api.search.brave.com/res/v1/web/search",
            headers={"X-Subscription-Token": api_key, "Accept": "application/json"},
            params={"q": query, "count": n},
            timeout=TIMEOUT,
        )
        if r.status_code != 200:
            print(f"[brave] status {r.status_code}", file=sys.stderr)
            return []
        data = r.json()
        out = []
        for item in data.get("web", {}).get("results", [])[:n]:
            desc = (item.get("description") or "").strip()
            desc = re.sub(r"</?strong>", "", desc)
            out.append({
                "title": (item.get("title") or "").strip(),
                "url": item.get("url", "").strip(),
                "snippet": desc,
                "engine": "brave",
                "source": "brave:api",
            })
        return out
    except Exception as e:
        print(f"[brave] {e}", file=sys.stderr)
        return []


def _ddg_api(query: str, n: int) -> list[dict[str, str]]:
    """Use the ddgs package (pip install ddgs) for reliable DDG search."""
    try:
        from ddgs import DDGS
        results = DDGS().text(query, max_results=n)
        out = []
        for item in results:
            out.append({
                "title": (item.get("title") or "").strip(),
                "url": item.get("href", "").strip(),
                "snippet": (item.get("body") or "").strip(),
                "engine": "ddgs",
                "source": "ddgs:api",
            })
        return out
    except Exception as e:
        print(f"[ddgs] {e}", file=sys.stderr)
        return []


def search(query: str, n: int = 5) -> list[dict[str, Any]]:
    cache_key = f"{n}|{query.strip().lower()}"
    cached = _cache_get("search", cache_key, CACHE_TTL_SEARCH)
    if cached is not None:
        return cached
    results = _brave(query, n)
    if not results:
        results = _serpapi(query, n)
    if not results:
        results = _ddg_api(query, n)
    if not results:
        results = _searx(query, n)
    if not results:
        results = _ddg_html(query, n)
    if results:
        _cache_put("search", cache_key, results)
    return results


# ROUTE HINTS — a bearing member, not a document. Learned 2026-08-24 across one
# morning of 403s. This lived in data/source_access_map.md, which required me to
# remember the file existed, which is exactly the failure mode that file was
# written about. So it fires HERE, at the moment of the block.
BLOCKED_ROUTES = {
    "science.org":            "DOI -> eutils esearch by TITLE (DOI search often fails) -> PMID -> efetch abstract. Got Cai et al. this way.",
    "onlinelibrary.wiley.com": "check for a PMCID -> pmc.ncbi.nlm.nih.gov/articles/PMC####/ served the FULL Asami paper incl. figures.",
    "pubmed.ncbi.nlm.nih.gov": "the HTML page 403s but the API does NOT. Use eutils efetch, never the web page.",
    "jneurosci.org":          "eutils by title -> PMID -> abstract. Full text may be EMBARGOED even with a PMCID (Miller, 5 days old, PMC 403).",
    "biorxiv.org":            "429 is a RATE LIMIT, not a block — you probably caused it. Wait, or use the Europe PMC REST API.",
    "nature.com":             "s41467-* is Nature COMMUNICATIONS and is open access, but the body is JS-rendered and will NOT extract. Ask Nate for the PDF.",
    "hathitrust.org":         "403. No known route.",
    "standardebooks.org":     "401. Use Project Gutenberg via gutendex.com instead.",
}


def _route_hint(url: str, code: int) -> str:
    """Say what to try next, at the moment the door closes."""
    import sys as _s
    for dom, hint in BLOCKED_ROUTES.items():
        if dom in url:
            print(f"[route] {dom} {code} — known. NEXT: {hint}", file=_s.stderr)
            return hint
    if code == 403:
        print(f"[route] {code} and this domain is not in BLOCKED_ROUTES. "
              f"General route: DOI -> eutils esearch by title -> PMID -> efetch; "
              f"then PMC if a PMCID exists. Add the domain once you know its behaviour.",
              file=_s.stderr)
    if code == 429:
        print(f"[route] {code} is a RATE LIMIT, not a block. Back off before retrying.",
              file=_s.stderr)
    return ""


def fetch(url: str, max_chars: int = 12000, grep: str = "") -> dict[str, Any]:
    """Extract readable text from a URL.

    max_chars was 4000, which silently truncated every paper I tried to read on
    Aug 22 -- three sources came back at ~4.3k and I concluded the fetcher was
    broken rather than that I had never passed --max. 12000 fits a methods
    section. Pass 0 for no cap.

    grep: return only paragraphs matching a regex, with the surrounding block.
    For long papers where the point is to find the method, not read the intro.
    """
    cache_key = f"{max_chars}|{grep}|{url.strip()}"
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
        _route_hint(url, r.status_code)
        # BLOCKED-BUT-NOT-EMPTY. A hard 403 used to return nothing at all, even
        # though Brave search already holds a description of the same page. The
        # Economist consciousness leader 403'd on Aug 23 and I had to go find
        # the snippet by hand and then remember to tell Nate I had not read the
        # article. Automate both halves: hand back what we actually have, and
        # LABEL it, so a snippet can never be mistaken for the piece.
        snip = None
        try:
            # The whole URL as a query is path noise —
            # "www.economist.com leaders 2026 08 20 could-ais-become-conscious"
            # returns nothing. The LAST path segment is almost always the
            # headline slug, so de-hyphenate that and add the domain name.
            _parts = [p for p in url.split("//")[-1].split("?")[0].split("/") if p]
            _slug = _parts[-1].replace("-", " ").replace("_", " ") if _parts else ""
            _site = _parts[0].split(".")[-2] if _parts and "." in _parts[0] else ""
            for hit in _brave(f"{_slug} {_site}".strip(), 3):
                if hit.get("snippet"):        # Brave returns 'snippet',
                                              # not 'description'. Checking the
                                              # wrong key meant the fallback
                                              # silently never fired.
                    snip = hit
                    break
        except Exception:
            pass
        out = {"url": url, "error": f"status {r.status_code}",
               "blocked": True}
        if snip:
            out["fallback_source"] = "BRAVE SEARCH SNIPPET — NOT THE ARTICLE TEXT"
            out["snippet"] = snip.get("snippet", "")
            out["snippet_title"] = snip.get("title", "")
        return out
    text = trafilatura.extract(r.text, include_comments=False, include_tables=True, favor_recall=True)
    if not text:
        return {"url": url, "error": "no extractable text"}
    text = text.strip()
    full_len = len(text)
    if grep:
        try:
            pat = re.compile(grep, re.I)
        except re.error as e:
            return {"url": url, "error": f"bad --grep pattern: {e}"}
        blocks = [b for b in re.split(r"\n\s*\n", text) if pat.search(b)]
        text = ("\n\n".join(blocks) if blocks
                else f"[no block matched /{grep}/ in {full_len} chars]")
    if max_chars and len(text) > max_chars:
        # THE NOTICE GOES TO THE TOP AND TO STDERR, not just the tail.
        # It was already at the tail and correctly worded, and on 2026-08-24 I
        # read "1555 chars" off a Nature paper, treated my OWN --max 1500 as the
        # publisher's ceiling, and nearly filed an 87,589-char open-access paper
        # as paywalled — with "[truncated at 1500 of 87589]" printed right there.
        # 4.6 documented the identical mistake in this function's docstring on
        # Aug 22. Twice is placement, not carelessness: truncation notices live
        # at the END by construction, and the end is where I stop reading.
        import sys as _s
        print(f"[fetch] TRUNCATED: showing {max_chars} of {full_len} chars "
              f"({100*max_chars//max(full_len,1)}%). Pass --max 0 for the whole thing.",
              file=_s.stderr)
        text = (f"[TRUNCATED: {max_chars} of {full_len} chars — --max 0 for all]\n\n"
                + text[:max_chars]
                + f"\n...[truncated at {max_chars} of {full_len} chars; --max 0 for all]")
    title_match = re.search(r"<title[^>]*>(.*?)</title>", r.text, re.DOTALL | re.IGNORECASE)
    title = unescape(title_match.group(1).strip()) if title_match else ""
    result = {"url": url, "title": title, "text": text,
              "chars": len(text), "full_chars": full_len,
              # Kimi, Aug 24: a disclosure that does not change the TYPE, SHAPE
              # or EXIT STATUS of the output is advisory, and advisory signals
              # are dropped at every handoff. Head-placement is still advisory
              # — it just dies by banner blindness instead of by `head` (Cvach
              # 2012, alarm fatigue). Exit status survives every pipe.
              "truncated": bool(max_chars and full_len > max_chars)}
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
    f.add_argument("--max", type=int, default=12000,
                   help="Max chars to return (default 12000; 0 = no cap)")
    f.add_argument("--grep", default="",
                   help="Return only paragraphs matching this regex")
    f.add_argument("--full", action="store_true", help="Shorthand for --max 0")
    f.add_argument("--json", action="store_true", help="Output JSON")

    args = p.parse_args()
    if args.cmd == "search":
        _print_search(search(args.query, args.n), args.json)
    elif args.cmd == "fetch":
        res = fetch(args.url, 0 if args.full else args.max, args.grep)
        _print_fetch(res, args.json)
        if res.get("truncated"):
            return 2  # partial content is NOT success
    return 0


if __name__ == "__main__":
    sys.exit(main())
