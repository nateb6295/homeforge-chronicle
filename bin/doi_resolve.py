#!/usr/bin/env python3
"""DOI resolver — Unpaywall + OpenAlex + CrossRef.

Returns OA PDF URL when one exists, metadata always.

Usage:
  python3 bin/doi_resolve.py 10.1038/s41583-023-00740-7
  python3 bin/doi_resolve.py 10.22541/au.177575355.56499869/v1 --json

Free public APIs, email contact required for politeness headers.
"""
import argparse
import json
import re
import sys
import urllib.parse
import urllib.request
from typing import Optional

CONTACT = "nate@homeforge.local"
TIMEOUT = 15
USER_AGENT = f"chronicle-doi-resolve/0.1 (mailto:{CONTACT})"

DOI_RE = re.compile(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", re.IGNORECASE)


def _get(url: str) -> Optional[dict]:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            if r.status != 200:
                return None
            return json.loads(r.read().decode("utf-8", errors="replace"))
    except Exception as e:
        return {"_error": f"{type(e).__name__}: {e}"}


def normalize_doi(raw: str) -> Optional[str]:
    m = DOI_RE.search(raw.strip())
    return m.group(0).lower() if m else None


def unpaywall(doi: str) -> dict:
    url = f"https://api.unpaywall.org/v2/{urllib.parse.quote(doi, safe='')}?email={urllib.parse.quote(CONTACT)}"
    data = _get(url)
    if not data or "_error" in (data or {}):
        return {"source": "unpaywall", "ok": False, "error": (data or {}).get("_error", "no response")}
    oa = data.get("best_oa_location") or {}
    return {
        "source": "unpaywall",
        "ok": True,
        "oa_status": data.get("oa_status"),
        "is_oa": data.get("is_oa", False),
        "pdf_url": oa.get("url_for_pdf") or oa.get("url"),
        "host_type": oa.get("host_type"),
        "version": oa.get("version"),
        "title": data.get("title"),
        "year": data.get("year"),
        "publisher": data.get("publisher"),
        "journal": data.get("journal_name"),
    }


def openalex(doi: str) -> dict:
    url = f"https://api.openalex.org/works/doi:{urllib.parse.quote(doi, safe='')}?mailto={urllib.parse.quote(CONTACT)}"
    data = _get(url)
    if not data or "_error" in (data or {}):
        return {"source": "openalex", "ok": False, "error": (data or {}).get("_error", "no response")}
    best = data.get("best_oa_location") or (data.get("open_access") or {})
    pdf = None
    if isinstance(best, dict):
        pdf = best.get("pdf_url") or best.get("url") or best.get("oa_url")
    if not pdf:
        locs = data.get("locations") or []
        for loc in locs:
            if loc.get("is_oa") and (loc.get("pdf_url") or loc.get("landing_page_url")):
                pdf = loc.get("pdf_url") or loc.get("landing_page_url")
                break
    authors = [a.get("author", {}).get("display_name") for a in (data.get("authorships") or []) if a.get("author")]
    return {
        "source": "openalex",
        "ok": True,
        "is_oa": bool((data.get("open_access") or {}).get("is_oa")),
        "oa_status": (data.get("open_access") or {}).get("oa_status"),
        "pdf_url": pdf,
        "title": data.get("title") or data.get("display_name"),
        "year": data.get("publication_year"),
        "authors": authors[:10],
        "type": data.get("type"),
        "venue": ((data.get("primary_location") or {}).get("source") or {}).get("display_name"),
    }


def crossref(doi: str) -> dict:
    url = f"https://api.crossref.org/works/{urllib.parse.quote(doi, safe='')}"
    data = _get(url)
    if not data or "_error" in (data or {}):
        return {"source": "crossref", "ok": False, "error": (data or {}).get("_error", "no response")}
    msg = data.get("message") or {}
    authors = [f"{a.get('given','')} {a.get('family','')}".strip() for a in (msg.get("author") or [])]
    links = msg.get("link") or []
    pdf = None
    for ln in links:
        if (ln.get("content-type") or "").endswith("pdf"):
            pdf = ln.get("URL")
            break
    return {
        "source": "crossref",
        "ok": True,
        "title": (msg.get("title") or [None])[0],
        "authors": authors[:10],
        "year": ((msg.get("issued") or {}).get("date-parts") or [[None]])[0][0],
        "publisher": msg.get("publisher"),
        "journal": (msg.get("container-title") or [None])[0],
        "abstract": msg.get("abstract"),
        "pdf_url": pdf,
        "type": msg.get("type"),
    }


def resolve(doi: str) -> dict:
    norm = normalize_doi(doi)
    if not norm:
        return {"doi": doi, "ok": False, "error": "not a DOI"}
    up = unpaywall(norm)
    oa = openalex(norm)
    cr = crossref(norm)
    pdf_url = None
    oa_status = None
    source_order = []
    for r in (up, oa, cr):
        if r.get("ok") and r.get("pdf_url") and not pdf_url:
            pdf_url = r["pdf_url"]
            source_order.append(r["source"])
        if r.get("ok") and r.get("oa_status") and not oa_status:
            oa_status = r["oa_status"]
    title = up.get("title") or oa.get("title") or cr.get("title")
    authors = oa.get("authors") or cr.get("authors") or []
    year = up.get("year") or oa.get("year") or cr.get("year")
    publisher = up.get("publisher") or cr.get("publisher")
    return {
        "doi": norm,
        "ok": any(r.get("ok") for r in (up, oa, cr)),
        "pdf_url": pdf_url,
        "pdf_source": source_order[0] if source_order else None,
        "oa_status": oa_status,
        "title": title,
        "authors": authors,
        "year": year,
        "publisher": publisher,
        "abstract": cr.get("abstract"),
        "sources": {"unpaywall": up, "openalex": oa, "crossref": cr},
    }


def _fmt_text(r: dict) -> str:
    lines = []
    lines.append(f"DOI: {r.get('doi')}")
    if not r.get("ok"):
        lines.append(f"Error: {r.get('error', 'all APIs failed')}")
        for src, d in (r.get("sources") or {}).items():
            if d.get("error"):
                lines.append(f"  {src}: {d['error']}")
        return "\n".join(lines)
    if r.get("title"):
        lines.append(f"Title: {r['title']}")
    if r.get("authors"):
        lines.append(f"Authors: {', '.join(r['authors'][:5])}{' et al.' if len(r['authors'])>5 else ''}")
    if r.get("year"):
        lines.append(f"Year: {r['year']}")
    if r.get("publisher"):
        lines.append(f"Publisher: {r['publisher']}")
    lines.append(f"OA status: {r.get('oa_status') or 'unknown'}")
    if r.get("pdf_url"):
        lines.append(f"PDF: {r['pdf_url']}  (via {r.get('pdf_source')})")
    else:
        lines.append("PDF: none found (paywalled / gated)")
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("doi", help="DOI (with or without https://doi.org/ prefix)")
    p.add_argument("--json", action="store_true", help="emit full JSON")
    args = p.parse_args()
    r = resolve(args.doi)
    if args.json:
        print(json.dumps(r, indent=2, default=str))
    else:
        print(_fmt_text(r))
    sys.exit(0 if r.get("ok") else 1)


if __name__ == "__main__":
    main()
