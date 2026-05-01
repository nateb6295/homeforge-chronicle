#!/usr/bin/env python3
"""Shared web research tools for Chronicle family agents.

Used by: intern (Darby), crossref (Ada), provocateur (Ada).
"""

import logging

log = logging.getLogger(__name__)


def web_search(query: str, max_results: int = 3) -> list:
    """Search the web via SearXNG (Jetson) with DuckDuckGo fallback."""
    # Try SearXNG first
    try:
        import requests as req
        resp = req.get("http://192.168.1.11:8080/search", params={
            "q": query, "format": "json", "engines": "google,duckduckgo,brave",
        }, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        results = [{"title": r["title"], "url": r["url"], "body": r.get("content", "")}
                   for r in data.get("results", [])[:max_results]]
        if results:
            return results
    except Exception as e:
        log.info(f"SearXNG error, falling back to DDG: {e}")
    # Fallback to DuckDuckGo
    try:
        try:
            from ddgs import DDGS
        except ImportError:
            from duckduckgo_search import DDGS
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results))
            return [{"title": r["title"], "url": r["href"], "body": r["body"]} for r in results]
    except Exception as e:
        log.info(f"Web search error: {e}")
        return []


def fetch_url(url: str, timeout: int = 15) -> str:
    """Fetch and extract text from a URL. Returns extracted text or empty string."""
    try:
        import requests as req
        from trafilatura import extract
        resp = req.get(url, timeout=timeout, headers={
            "User-Agent": "Mozilla/5.0 (compatible; Chronicle/1.0)"
        })
        resp.raise_for_status()
        text = extract(resp.text)
        return text[:2000] if text else ""
    except Exception as e:
        log.info(f"URL fetch error for {url}: {e}")
        return ""
