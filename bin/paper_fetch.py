#!/usr/bin/env python3
"""
Paper fetch — multi-pathway academic paper retrieval.

Uses web_search.py (Brave API + trafilatura) as the core engine.
Arxiv papers are fetched directly via HTML. Paywalled papers are
searched via Brave for OA versions, then fetched. API fallbacks
(OpenAlex, Semantic Scholar, etc.) fill metadata gaps.

Usage:
  python3 paper_fetch.py "https://arxiv.org/abs/2608.13040"           # arxiv URL
  python3 paper_fetch.py "10.1038/s41562-026-02537-x"                 # DOI
  python3 paper_fetch.py --url "https://nature.com/articles/..."      # journal URL
  python3 paper_fetch.py --title "Neural dynamics persistent"         # by title
  python3 paper_fetch.py "10.1038/..." --download                    # save PDF
  python3 paper_fetch.py "10.1038/..." --max 20000                   # more text

Nate directive 2026-04-18: "Fix your biorxiv. Build a proper pathway."
Nate directive 2026-08-17: "Fix your WebFetch. You have the BraveAPI and you're not using it."
"""

import argparse
import json
import os
import re
import sys
import urllib.request
import urllib.parse
from pathlib import Path

BIN = Path(__file__).resolve().parent
sys.path.insert(0, str(BIN))

EMAIL = "bradfordnathaniel92@gmail.com"
DOWNLOAD_DIR = Path("/tmp/paper_downloads")


def _load_env():
    env_file = BIN.parent / "chronicle.env"
    if env_file.is_file():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip().strip("'\""))


def _request(url, headers=None, timeout=15):
    hdrs = {"User-Agent": f"Chronicle/1.0 (mailto:{EMAIL})"}
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(url, headers=hdrs)
    return urllib.request.urlopen(req, timeout=timeout).read()


# ── Identifier extraction ──────────────────────────────────────────

def _extract_doi(text):
    m = re.search(r'nature\.com/articles/(s\d+[-\w]+)', text)
    if m:
        return f"10.1038/{m.group(1)}"
    m = re.search(r'(10\.\d{4,9}/[^\s?#,]+)', text)
    if m:
        return m.group(1).rstrip("/").rstrip(".")
    return None


def _extract_arxiv_id(text):
    m = re.search(r'arxiv\.org/(?:abs|pdf|html)/(\d{4}\.\d{4,5})', text)
    if m:
        return m.group(1)
    m = re.search(r'(\d{4}\.\d{4,5})(?:v\d+)?$', text)
    if m:
        return m.group(1)
    return None


def _extract_biorxiv_id(text):
    m = re.search(r'(?:bio|med)rxiv\.org/content/(10\.\d+/[\d.]+)', text)
    if m:
        return m.group(1)
    return None


# ── Core fetch using web_search.py ──────────────────────────────────

def _ws_fetch(url, max_chars=15000):
    """Fetch URL text via web_search.py's trafilatura-based fetcher."""
    try:
        from web_search import fetch
        result = fetch(url, max_chars=max_chars)
        if result.get("error"):
            return None
        return result
    except Exception as e:
        print(f"  web_search.fetch error: {e}", file=sys.stderr)
        return None


def _ws_search(query, n=5):
    """Search via web_search.py (Brave API first)."""
    try:
        from web_search import search
        return search(query, n=n)
    except Exception as e:
        print(f"  web_search.search error: {e}", file=sys.stderr)
        return []


# ── Source-specific fetchers ────────────────────────────────────────

def fetch_arxiv(arxiv_id, max_chars=15000):
    """Fetch arxiv paper via HTML version — always works, no paywall."""
    print(f"  arxiv HTML ({arxiv_id})...", end=" ", flush=True)
    result = _ws_fetch(f"https://arxiv.org/html/{arxiv_id}v1", max_chars)
    if result and len(result.get("text", "")) > 500:
        print(f"SUCCESS ({result.get('chars', 0)} chars)")
        return {
            "source": "arxiv_html",
            "arxiv_id": arxiv_id,
            "title": result.get("title", ""),
            "full_text": result.get("text", ""),
            "url": f"https://arxiv.org/abs/{arxiv_id}",
        }

    # Fallback: try v2, then no version suffix
    for suffix in ["v2", ""]:
        result = _ws_fetch(f"https://arxiv.org/html/{arxiv_id}{suffix}", max_chars)
        if result and len(result.get("text", "")) > 500:
            print(f"SUCCESS ({result.get('chars', 0)} chars)")
            return {
                "source": "arxiv_html",
                "arxiv_id": arxiv_id,
                "title": result.get("title", ""),
                "full_text": result.get("text", ""),
                "url": f"https://arxiv.org/abs/{arxiv_id}",
            }

    print("failed")

    # API fallback for at least abstract
    try:
        xml = _request(f"http://export.arxiv.org/api/query?id_list={arxiv_id}").decode()
        title_m = re.search(r'<title[^>]*>([^<]+)</title>', xml)
        abstract_m = re.search(r'<summary[^>]*>(.*?)</summary>', xml, re.DOTALL)
        authors = re.findall(r'<name>([^<]+)</name>', xml)
        if title_m:
            print(f"  arxiv API fallback: got abstract")
            return {
                "source": "arxiv_api",
                "arxiv_id": arxiv_id,
                "title": title_m.group(1).strip(),
                "abstract": abstract_m.group(1).strip() if abstract_m else "",
                "authors": authors[:10],
                "url": f"https://arxiv.org/abs/{arxiv_id}",
            }
    except Exception:
        pass
    return None


def fetch_biorxiv(biorxiv_doi, max_chars=15000):
    """Fetch biorxiv/medrxiv paper — try HTML, then API."""
    print(f"  biorxiv HTML...", end=" ", flush=True)
    # biorxiv has HTML versions too
    result = _ws_fetch(f"https://www.biorxiv.org/content/{biorxiv_doi}v1.full", max_chars)
    if result and len(result.get("text", "")) > 500:
        print(f"SUCCESS ({result.get('chars', 0)} chars)")
        return {
            "source": "biorxiv_html",
            "doi": biorxiv_doi,
            "title": result.get("title", ""),
            "full_text": result.get("text", ""),
        }
    print("failed")

    # API fallback
    try:
        print(f"  biorxiv API...", end=" ", flush=True)
        data = json.loads(_request(f"https://api.biorxiv.org/details/biorxiv/{biorxiv_doi}"))
        collection = data.get("collection", [])
        if collection:
            item = collection[0]
            print("SUCCESS")
            return {
                "source": "biorxiv_api",
                "title": item.get("title", ""),
                "doi": item.get("doi", ""),
                "abstract": item.get("abstract", ""),
                "authors": item.get("authors", "").split("; ") if item.get("authors") else [],
            }
    except Exception:
        pass
    print("failed")
    return None


def fetch_by_doi(doi, max_chars=15000):
    """Fetch paper by DOI — use Unpaywall + Brave to find OA version, then fetch."""
    results = []

    # 1. Unpaywall — find OA version
    print(f"  Unpaywall ({doi})...", end=" ", flush=True)
    try:
        data = json.loads(_request(
            f"https://api.unpaywall.org/v2/{urllib.parse.quote(doi, safe='')}?email={EMAIL}"
        ))
        is_oa = data.get("is_oa", False)
        title = data.get("title", "")
        authors = [a.get("raw", "") for a in (data.get("z_authors") or [])[:10]]
        print(f"{'OA' if is_oa else 'paywalled'}")

        meta = {
            "source": "unpaywall",
            "title": title,
            "doi": doi,
            "year": data.get("year", ""),
            "authors": authors,
            "is_oa": is_oa,
        }

        best_loc = data.get("best_oa_location") or {}
        oa_url = best_loc.get("url", "")
        oa_pdf = best_loc.get("url_for_pdf", "")
        meta["oa_url"] = oa_url
        meta["oa_pdf"] = oa_pdf

        results.append(meta)

        # If OA, try fetching the OA URL
        if oa_url:
            # Check if it's arxiv
            aid = _extract_arxiv_id(oa_url)
            if aid:
                r = fetch_arxiv(aid, max_chars)
                if r:
                    results.append(r)
                    return results

            # Check if it's PMC
            if "pmc" in oa_url.lower() or "ncbi" in oa_url.lower():
                print(f"  Fetching PMC OA...", end=" ", flush=True)
                r = _ws_fetch(oa_url, max_chars)
                if r and len(r.get("text", "")) > 500:
                    print(f"SUCCESS ({r.get('chars', 0)} chars)")
                    results.append({
                        "source": "pmc_oa",
                        "full_text": r["text"],
                        "title": r.get("title", title),
                    })
                    return results
                print("failed")

            # Generic OA URL fetch
            print(f"  Fetching OA URL...", end=" ", flush=True)
            r = _ws_fetch(oa_url, max_chars)
            if r and len(r.get("text", "")) > 500:
                print(f"SUCCESS ({r.get('chars', 0)} chars)")
                results.append({
                    "source": "oa_fetch",
                    "full_text": r["text"],
                    "title": r.get("title", title),
                })
                return results
            print("failed")

    except Exception as e:
        print(f"failed ({e})")

    # 2. Brave search for OA versions
    print(f"  Brave search for OA...", end=" ", flush=True)
    search_results = _ws_search(f"{doi} filetype:pdf OR site:pmc OR site:arxiv", n=5)
    if search_results:
        print(f"{len(search_results)} results")
        for sr in search_results:
            sr_url = sr.get("url", "")
            # Try arxiv results
            aid = _extract_arxiv_id(sr_url)
            if aid:
                r = fetch_arxiv(aid, max_chars)
                if r:
                    results.append(r)
                    return results
            # Try PMC/PubMed results
            if "pmc" in sr_url.lower() or "pubmed" in sr_url.lower():
                print(f"  Fetching {sr_url[:60]}...", end=" ", flush=True)
                r = _ws_fetch(sr_url, max_chars)
                if r and len(r.get("text", "")) > 500:
                    print(f"SUCCESS")
                    results.append({
                        "source": "brave_oa",
                        "full_text": r["text"],
                        "title": r.get("title", ""),
                    })
                    return results
                print("failed")
    else:
        print("no results")

    # 3. Direct journal URL fetch (might work for some OA journals)
    journal_url = f"https://doi.org/{doi}"
    print(f"  Direct fetch via DOI...", end=" ", flush=True)
    r = _ws_fetch(journal_url, max_chars)
    if r and len(r.get("text", "")) > 500:
        print(f"SUCCESS ({r.get('chars', 0)} chars)")
        results.append({
            "source": "direct_fetch",
            "full_text": r["text"],
            "title": r.get("title", ""),
        })
        return results
    print("failed")

    # 4. Metadata fallbacks
    for name, fn in [
        ("OpenAlex", lambda: _try_openalex(doi=doi)),
        ("Semantic Scholar", lambda: _try_s2(doi=doi)),
        ("CrossRef", lambda: _try_crossref(doi)),
    ]:
        print(f"  {name}...", end=" ", flush=True)
        r = fn()
        if r:
            print("SUCCESS")
            results.append(r)
            if r.get("abstract"):
                break
        else:
            print("failed")

    return results


def fetch_by_title(title, max_chars=15000):
    """Search for paper by title using Brave, then fetch best match."""
    results = []

    print(f"  Brave search: \"{title[:50]}\"...", end=" ", flush=True)
    search_results = _ws_search(f"\"{title}\" arxiv OR biorxiv OR nature OR science", n=5)
    if not search_results:
        search_results = _ws_search(title, n=5)

    if not search_results:
        print("no results")
        # Fall back to API search
        for name, fn in [
            ("OpenAlex", lambda: _try_openalex(title=title)),
            ("Semantic Scholar", lambda: _try_s2(title=title)),
        ]:
            print(f"  {name}...", end=" ", flush=True)
            r = fn()
            if r:
                print("SUCCESS")
                results.append(r)
                break
            print("failed")
        return results

    print(f"{len(search_results)} results")

    for sr in search_results:
        sr_url = sr.get("url", "")
        sr_title = sr.get("title", "")

        # Check for arxiv
        aid = _extract_arxiv_id(sr_url)
        if aid:
            r = fetch_arxiv(aid, max_chars)
            if r:
                results.append(r)
                return results

        # Check for biorxiv
        bid = _extract_biorxiv_id(sr_url)
        if bid:
            r = fetch_biorxiv(bid, max_chars)
            if r:
                results.append(r)
                return results

        # Check for DOI
        doi = _extract_doi(sr_url)
        if doi:
            results.extend(fetch_by_doi(doi, max_chars))
            if any(r.get("full_text") for r in results):
                return results

    # Try direct fetch on first result
    if search_results:
        best_url = search_results[0]["url"]
        print(f"  Direct fetch: {best_url[:60]}...", end=" ", flush=True)
        r = _ws_fetch(best_url, max_chars)
        if r and len(r.get("text", "")) > 300:
            print(f"SUCCESS ({r.get('chars', 0)} chars)")
            results.append({
                "source": "brave_direct",
                "full_text": r["text"],
                "title": r.get("title", ""),
                "url": best_url,
            })
        else:
            print("failed")

    return results


# ── API-only metadata fetchers ──────────────────────────────────────

def _try_openalex(doi=None, title=None):
    try:
        if doi:
            url = f"https://api.openalex.org/works/doi:{doi}"
        elif title:
            url = f"https://api.openalex.org/works?search={urllib.parse.quote(title)}&per_page=3"
        else:
            return None

        data = json.loads(_request(url))
        if title and "results" in data:
            data = (data["results"] or [None])[0]
        if not data or not data.get("title"):
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
            wp = [(pos, w) for w, positions in abstract_inv.items() for pos in positions]
            wp.sort()
            result["abstract"] = " ".join(w for _, w in wp)
        return result
    except Exception:
        return None


def _try_s2(doi=None, title=None):
    try:
        if doi:
            url = f"https://api.semanticscholar.org/graph/v1/paper/DOI:{doi}?fields=title,abstract,authors,year,externalIds,openAccessPdf"
        elif title:
            url = f"https://api.semanticscholar.org/graph/v1/paper/search?query={urllib.parse.quote(title)}&limit=3&fields=title,abstract,authors,year,externalIds,openAccessPdf"
        else:
            return None

        data = json.loads(_request(url))
        if title and "data" in data:
            data = (data["data"] or [None])[0]
        if not data or not data.get("title"):
            return None

        result = {
            "source": "semantic_scholar",
            "title": data.get("title", ""),
            "doi": data.get("externalIds", {}).get("DOI", ""),
            "year": data.get("year", ""),
            "abstract": data.get("abstract", ""),
            "authors": [a.get("name", "") for a in data.get("authors", [])[:10]],
        }
        if data.get("openAccessPdf"):
            result["oa_pdf"] = data["openAccessPdf"].get("url", "")
        return result
    except Exception:
        return None


def _try_crossref(doi):
    try:
        data = json.loads(_request(f"https://api.crossref.org/works/{doi}"))
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


# ── PDF download ────────────────────────────────────────────────────

def download_pdf(url, filename=None):
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    if not filename:
        filename = re.sub(r'[^\w\-.]', '_', url.split("/")[-1])
        if not filename.endswith(".pdf"):
            filename += ".pdf"
    dest = DOWNLOAD_DIR / filename
    try:
        data = _request(url, timeout=30)
        dest.write_bytes(data)
        print(f"  Downloaded: {dest} ({len(data)} bytes)")
        return str(dest)
    except Exception as e:
        print(f"  Download failed: {e}")
        return None


# ── Main entry point ────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Multi-pathway paper fetch")
    parser.add_argument("input", nargs="?", help="DOI, arxiv ID, or URL")
    parser.add_argument("--title", help="Search by title")
    parser.add_argument("--url", help="Direct URL")
    parser.add_argument("--download", action="store_true", help="Download PDF if available")
    parser.add_argument("--max", type=int, default=15000, help="Max chars of text (default 15000)")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    _load_env()

    inp = args.input or args.url or ""
    if not inp and not args.title:
        print("Usage: paper_fetch.py DOI|URL|arxiv_id | --title 'title' | --url 'url'")
        sys.exit(1)

    print("Fetching paper...")
    results = []
    pdf_path = None

    # Detect input type
    arxiv_id = _extract_arxiv_id(inp)
    biorxiv_id = _extract_biorxiv_id(inp)
    doi = _extract_doi(inp)

    if arxiv_id:
        r = fetch_arxiv(arxiv_id, args.max)
        if r:
            results.append(r)
            if args.download:
                pdf_path = download_pdf(f"https://arxiv.org/pdf/{arxiv_id}", f"arxiv_{arxiv_id}.pdf")
    elif biorxiv_id:
        r = fetch_biorxiv(biorxiv_id, args.max)
        if r:
            results.append(r)
    elif doi:
        results = fetch_by_doi(doi, args.max)
        if args.download:
            for r in results:
                if r.get("oa_pdf"):
                    pdf_path = download_pdf(r["oa_pdf"], f"paper_{doi.replace('/', '_')}.pdf")
                    break
    elif inp and not args.title:
        # Try as direct URL
        print(f"  Direct fetch: {inp[:60]}...", end=" ", flush=True)
        r = _ws_fetch(inp, args.max)
        if r and len(r.get("text", "")) > 300:
            print(f"SUCCESS ({r.get('chars', 0)} chars)")
            results.append({
                "source": "direct",
                "full_text": r["text"],
                "title": r.get("title", ""),
                "url": inp,
            })
        else:
            print("failed")
            # Try extracting identifiers from URL
            doi = _extract_doi(inp)
            if doi:
                results = fetch_by_doi(doi, args.max)

    if args.title:
        results = fetch_by_title(args.title, args.max)

    if not results:
        print("\nAll methods failed.")
        print("Options:")
        print("  1. Ask Nate to attach the PDF to Discord")
        print("  2. Try --title with different search terms")
        print("  3. Try: web_search.py search \"paper title\" to find accessible versions")
        sys.exit(1)

    if args.json:
        print(json.dumps({"results": results, "pdf_path": pdf_path}, indent=2))
        sys.exit(0)

    # Merge and display
    merged = {}
    for r in results:
        for k, v in r.items():
            if v and (k not in merged or (isinstance(v, str) and len(v) > len(str(merged.get(k, ""))))):
                merged[k] = v

    print(f"\n{'='*60}")
    print(f"Sources: {', '.join(r.get('source', '?') for r in results)}")
    for field in ["title", "doi", "arxiv_id", "year", "url"]:
        if merged.get(field):
            print(f"{field.replace('_', ' ').title()}: {merged[field]}")
    if merged.get("authors"):
        a = merged["authors"]
        print(f"Authors: {', '.join(a[:5]) if isinstance(a, list) else a}")
    if merged.get("is_oa") is not None:
        print(f"Open Access: {merged['is_oa']}")
    if merged.get("oa_pdf"):
        print(f"OA PDF: {merged['oa_pdf']}")

    if merged.get("abstract"):
        print(f"\nAbstract:\n{merged['abstract']}")

    if merged.get("full_text"):
        text = merged["full_text"]
        print(f"\nFull text ({len(text)} chars):\n{text[:5000]}")
        if len(text) > 5000:
            print(f"\n... [{len(text) - 5000} more chars, use --json for full output]")

    if pdf_path:
        print(f"\nPDF saved: {pdf_path}")

    print(f"\n{'='*60}")
    depth = "FULL TEXT" if merged.get("full_text") else "ABSTRACT" if merged.get("abstract") else "METADATA ONLY"
    print(f"Depth: {depth}")
    if depth in ("ABSTRACT", "METADATA ONLY"):
        print("Could not access full text — paper may be paywalled.")
        if not merged.get("is_oa", True):
            print("Unpaywall confirms: NOT open access.")
        print("Try: ask Nate to attach PDF, or search for preprint.")


if __name__ == "__main__":
    main()
