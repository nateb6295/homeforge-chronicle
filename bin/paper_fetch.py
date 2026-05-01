#!/usr/bin/env python3
"""
Paper fetch — multi-pathway academic paper retrieval.

biorxiv and many academic sites are Cloudflare-gated. This script
tries multiple access methods in order:

1. OpenAlex API (metadata + reconstructed abstract)
2. Semantic Scholar API (metadata + abstract)
3. biorxiv content API (metadata, may not have recent papers)
4. CrossRef API (metadata only, no abstract usually)
5. trafilatura direct fetch (full text if not blocked)

Usage:
  python3 paper_fetch.py "10.1101/2026.04.14.718499"     # by DOI
  python3 paper_fetch.py --title "Learning to select computations"  # by title
  python3 paper_fetch.py --url "https://biorxiv.org/..."   # by URL

Nate directive 2026-04-18: "Fix your biorxiv. Build a proper pathway."
"""

import argparse
import json
import re
import sys
import urllib.request
import urllib.parse
import time


def try_openalex(doi=None, title=None):
    """Try OpenAlex API."""
    try:
        if doi:
            url = f"https://api.openalex.org/works/doi:{doi}"
        elif title:
            url = f"https://api.openalex.org/works?search={urllib.parse.quote(title)}&per_page=3"
        else:
            return None

        req = urllib.request.Request(url, headers={
            "User-Agent": "Chronicle/1.0 (mailto:bradfordnathaniel92@gmail.com)"
        })
        resp = urllib.request.urlopen(req, timeout=10)
        data = json.loads(resp.read())

        if title and "results" in data:
            results = data["results"]
            if not results:
                return None
            data = results[0]

        if not data.get("title"):
            return None

        result = {
            "source": "openalex",
            "title": data.get("title", ""),
            "doi": data.get("doi", ""),
            "year": data.get("publication_year", ""),
            "authors": [a.get("author", {}).get("display_name", "")
                        for a in data.get("authorships", [])[:10]],
        }

        abstract_inv = data.get("abstract_inverted_index", {})
        if abstract_inv:
            word_positions = []
            for word, positions in abstract_inv.items():
                for pos in positions:
                    word_positions.append((pos, word))
            word_positions.sort()
            result["abstract"] = " ".join(w for _, w in word_positions)

        return result
    except Exception:
        return None


def try_semantic_scholar(doi=None, title=None):
    """Try Semantic Scholar API."""
    try:
        if doi:
            url = f"https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}?fields=title,abstract,authors,year,externalIds"
        elif title:
            url = f"https://api.semanticscholar.org/graph/v1/paper/search?query={urllib.parse.quote(title)}&limit=3&fields=title,abstract,authors,year,externalIds"
        else:
            return None

        req = urllib.request.Request(url, headers={"User-Agent": "Chronicle/1.0"})
        resp = urllib.request.urlopen(req, timeout=10)
        data = json.loads(resp.read())

        if title and "data" in data:
            results = data["data"]
            if not results:
                return None
            data = results[0]

        if not data.get("title"):
            return None

        return {
            "source": "semantic_scholar",
            "title": data.get("title", ""),
            "doi": data.get("externalIds", {}).get("DOI", ""),
            "year": data.get("year", ""),
            "abstract": data.get("abstract", ""),
            "authors": [a.get("name", "") for a in data.get("authors", [])[:10]],
        }
    except Exception:
        return None


def try_biorxiv_api(doi):
    """Try biorxiv content API."""
    try:
        # Strip prefix if present
        clean_doi = doi.replace("10.1101/", "").replace("10.64898/", "")
        url = f"https://api.biorxiv.org/details/biorxiv/{doi}"
        req = urllib.request.Request(url, headers={"User-Agent": "Chronicle/1.0"})
        resp = urllib.request.urlopen(req, timeout=10)
        data = json.loads(resp.read())

        collection = data.get("collection", [])
        if not collection:
            return None

        item = collection[0]
        return {
            "source": "biorxiv_api",
            "title": item.get("title", ""),
            "doi": item.get("doi", ""),
            "abstract": item.get("abstract", ""),
            "authors": item.get("authors", "").split("; ") if item.get("authors") else [],
            "date": item.get("date", ""),
        }
    except Exception:
        return None


def try_crossref(doi):
    """Try CrossRef API."""
    try:
        url = f"https://api.crossref.org/works/{doi}"
        req = urllib.request.Request(url, headers={
            "User-Agent": "Chronicle/1.0 (mailto:bradfordnathaniel92@gmail.com)"
        })
        resp = urllib.request.urlopen(req, timeout=10)
        data = json.loads(resp.read())
        work = data.get("message", {})

        if not work.get("title"):
            return None

        return {
            "source": "crossref",
            "title": work["title"][0] if work["title"] else "",
            "doi": work.get("DOI", ""),
            "abstract": work.get("abstract", ""),
            "authors": [f"{a.get('given', '')} {a.get('family', '')}".strip()
                        for a in work.get("author", [])[:10]],
        }
    except Exception:
        return None


def try_trafilatura(url):
    """Try trafilatura direct fetch."""
    try:
        from trafilatura import fetch_url, extract
        downloaded = fetch_url(url)
        if downloaded:
            text = extract(downloaded)
            if text and len(text) > 200:
                return {
                    "source": "trafilatura",
                    "full_text": text[:5000],
                }
        return None
    except Exception:
        return None


def fetch_paper(doi=None, title=None, url=None):
    """Try all methods to fetch paper information."""
    results = []

    # Extract DOI from URL if provided
    if url and not doi:
        match = re.search(r'(10\.\d{4,}/[^\s]+)', url)
        if match:
            doi = match.group(1).rstrip("v1").rstrip("/")
            # Handle version suffix
            if not doi.endswith("v1"):
                doi_v = url.split("/")[-1] if "/" in url else doi

    methods = []
    if doi:
        methods.extend([
            ("OpenAlex (DOI)", lambda: try_openalex(doi=doi)),
            ("Semantic Scholar (DOI)", lambda: try_semantic_scholar(doi=doi)),
            ("biorxiv API", lambda: try_biorxiv_api(doi)),
            ("CrossRef", lambda: try_crossref(doi)),
        ])
    if title:
        methods.extend([
            ("OpenAlex (title)", lambda: try_openalex(title=title)),
            ("Semantic Scholar (title)", lambda: try_semantic_scholar(title=title)),
        ])
    if url:
        methods.append(("trafilatura", lambda: try_trafilatura(url)))

    for name, method in methods:
        print(f"  Trying {name}...", end=" ", flush=True)
        result = method()
        if result:
            print("SUCCESS")
            results.append(result)
            # If we have abstract, that's usually enough
            if result.get("abstract") and len(result.get("abstract", "")) > 100:
                break
        else:
            print("failed")

    return results


def main():
    parser = argparse.ArgumentParser(description="Multi-pathway paper fetch")
    parser.add_argument("doi", nargs="?", help="DOI to fetch")
    parser.add_argument("--title", help="Search by title")
    parser.add_argument("--url", help="Direct URL to try")
    args = parser.parse_args()

    if not args.doi and not args.title and not args.url:
        print("Usage: paper_fetch.py DOI | --title 'title' | --url 'url'")
        sys.exit(1)

    print("Fetching paper via multiple pathways...")
    results = fetch_paper(doi=args.doi, title=args.title, url=args.url)

    if not results:
        print("\nAll methods failed. Paper may be too recent for APIs.")
        print("Fallback: use WebSearch in Claude session for abstract extraction.")
        sys.exit(1)

    # Merge results (prefer most complete)
    best = max(results, key=lambda r: len(r.get("abstract", "")) + len(r.get("full_text", "")))

    print(f"\n{'='*60}")
    print(f"Source: {best.get('source', '?')}")
    if best.get("title"):
        print(f"Title: {best['title']}")
    if best.get("doi"):
        print(f"DOI: {best['doi']}")
    if best.get("year"):
        print(f"Year: {best['year']}")
    if best.get("authors"):
        print(f"Authors: {', '.join(best['authors'][:5])}")
    if best.get("abstract"):
        print(f"\nAbstract:\n{best['abstract']}")
    if best.get("full_text"):
        print(f"\nFull text (truncated):\n{best['full_text'][:2000]}")
    if best.get("date"):
        print(f"Date: {best['date']}")


if __name__ == "__main__":
    main()
