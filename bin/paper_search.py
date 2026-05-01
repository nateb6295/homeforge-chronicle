#!/usr/bin/env python3
"""paper_search.py — arxiv-first paper lookup, routing around Nature/Science paywalls.

Most STEM papers published in Nature/Science/Cell have arxiv preprints. This
script tries arxiv first via the arxiv API, falls back to other sources if not
found.

Usage:
    python3 paper_search.py "Topology shapes dynamics of higher-order networks"
    python3 paper_search.py "Topology shapes dynamics" --author Millan
    python3 paper_search.py --doi 10.1038/s41567-024-02757-w

Returns: arxiv ID + abstract if found, fallback URLs otherwise.
"""
from __future__ import annotations

import argparse
import re
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


ARXIV_API = "http://export.arxiv.org/api/query"


def search_arxiv(title: str, author: str | None = None, max_results: int = 5) -> list[dict]:
    """Query the arxiv API for papers matching title (and optional author)."""
    parts = [f'ti:"{title}"']
    if author:
        parts.append(f'au:"{author}"')
    query = " AND ".join(parts)
    params = {
        "search_query": query,
        "max_results": str(max_results),
        "sortBy": "relevance",
    }
    url = f"{ARXIV_API}?{urllib.parse.urlencode(params)}"

    req = urllib.request.Request(url, headers={"User-Agent": "chronicle-paper-search/1.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        body = resp.read().decode()

    # arxiv API returns Atom XML
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    root = ET.fromstring(body)
    results = []
    for entry in root.findall("atom:entry", ns):
        title_el = entry.find("atom:title", ns)
        summary_el = entry.find("atom:summary", ns)
        id_el = entry.find("atom:id", ns)
        published_el = entry.find("atom:published", ns)
        authors = [a.find("atom:name", ns).text for a in entry.findall("atom:author", ns)]

        arxiv_url = id_el.text if id_el is not None else ""
        # Extract arxiv ID from URL like http://arxiv.org/abs/2403.12345v1
        arxiv_id_match = re.search(r"abs/([\d\.]+v?\d*)", arxiv_url)
        arxiv_id = arxiv_id_match.group(1) if arxiv_id_match else "?"

        results.append({
            "arxiv_id": arxiv_id,
            "url": arxiv_url,
            "title": (title_el.text or "").strip().replace("\n", " "),
            "authors": authors[:5],
            "published": (published_el.text or "")[:10],
            "summary": (summary_el.text or "").strip().replace("\n", " ")[:500],
        })
    return results


def main() -> int:
    p = argparse.ArgumentParser(description="arxiv-first paper lookup")
    p.add_argument("title", nargs="?", help="paper title (or partial)")
    p.add_argument("--author", help="optional author filter")
    p.add_argument("--doi", help="DOI lookup (currently passthrough — arxiv search by title still preferred)")
    p.add_argument("--max", type=int, default=5, help="max results")
    args = p.parse_args()

    if not args.title and not args.doi:
        p.error("must provide title or --doi")

    title = args.title
    if args.doi and not title:
        # DOI-only path: not implemented for arxiv search; print a hint
        print(f"DOI {args.doi} provided but title-search is more reliable. "
              "Use --title for arxiv-first lookup.", file=sys.stderr)
        return 1

    print(f"Searching arxiv for: {title!r}" + (f" by {args.author!r}" if args.author else ""))
    print()
    results = search_arxiv(title, args.author, args.max)

    if not results:
        print("No arxiv results found.")
        print("Fallback options:")
        print(f"  - Google Scholar: https://scholar.google.com/scholar?q={urllib.parse.quote(title)}")
        print(f"  - Semantic Scholar: https://www.semanticscholar.org/search?q={urllib.parse.quote(title)}")
        return 2

    for r in results:
        print(f"arxiv:{r['arxiv_id']}  {r['published']}")
        print(f"  Title: {r['title']}")
        print(f"  Authors: {', '.join(r['authors'])}")
        print(f"  URL: {r['url']}")
        print(f"  Abstract: {r['summary'][:300]}...")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
