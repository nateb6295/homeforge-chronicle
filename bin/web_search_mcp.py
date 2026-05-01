#!/usr/bin/env python3
"""web_search_mcp — MCP server exposing web search + URL fetch.

Runs as a streamable-http MCP server on 127.0.0.1:8011. Wired into Hermes
via mcp_servers.web-search in ~/.hermes/config.yaml.

Tools:
    web_search(query, n=5)  -> list[result]
    fetch_url(url, max_chars=4000) -> {title, text, chars, ...}

Backend: web_search.py (SearXNG JSON primary, DDG HTML fallback, trafilatura).
"""
from __future__ import annotations

import os
import sys

BIN_DIR = os.path.dirname(os.path.abspath(__file__))
if BIN_DIR not in sys.path:
    sys.path.insert(0, BIN_DIR)

import web_search  # noqa: E402  (our backend)
from fastmcp import FastMCP  # noqa: E402

mcp = FastMCP("Chronicle Web Search")


@mcp.tool()
def web_search_query(query: str, n: int = 5) -> list[dict]:
    """Search the live web. Returns up to n {title, url, snippet, engine, source} results.

    Uses SearXNG JSON (rotating public instances) with DuckDuckGo HTML fallback.
    No API key needed. Prefer this over scraping HTML directly.
    """
    n = max(1, min(int(n or 5), 15))
    return web_search.search(query, n)


@mcp.tool()
def fetch_url(url: str, max_chars: int = 4000) -> dict:
    """Fetch a URL and extract readable text via trafilatura.

    Returns {url, title, text, chars} on success, {url, error} on failure.
    Use this after web_search_query to read the page body. Do NOT use for
    x.com / twitter.com — those require the x-api MCP tools instead.
    """
    max_chars = max(200, min(int(max_chars or 4000), 20000))
    return web_search.fetch(url, max_chars)


if __name__ == "__main__":
    mcp.run(transport="http", host="127.0.0.1", port=8011)
