#!/usr/bin/env python3
"""Open Library MCP Server — search, browse, and read books.

Provides tools for searching Open Library's catalog, reading available
full-text books, and tracking a shared reading list.

MCP stdio server using JSON-RPC protocol.
"""

import json, sys, os, time, re
import urllib.request
import urllib.parse
import sqlite3
from pathlib import Path

DB_PATH = Path.home() / "chronicle" / "data" / "reading_list.db"
OL_BASE = "https://openlibrary.org"
OL_SEARCH = "https://openlibrary.org/search.json"
GUTENDEX = "https://gutendex.com/books"


def gutenberg_search(query, limit=5):
    """Search Project Gutenberg via Gutendex. Added 2026-08-24 per Nate:
    "there has to be an archive of books that you can access."

    Open Library/IA was the only source and it has two problems: in-copyright
    scans return HTTP 401 even when the index tags them [fulltext], and the
    public-domain scans are raw OCR. I read all of Frankenstein through OCR
    that rendered opening quotes as stray 'e' and 'f' characters, silently
    reconstructing the text as I went. Gutenberg is proofread, plain, no auth,
    one fetch for a whole novel. Strictly better wherever it has the book.
    CEILING: public domain only. Nothing legitimate serves in-copyright work —
    that is what Libby is for, and that is Nate's card, not my API.
    """
    import urllib.parse
    url = f"{GUTENDEX}?search={urllib.parse.quote(query)}"
    data = _http_get(url)
    out = []
    for b in (data.get("results") or [])[:limit]:
        fmts = b.get("formats", {})
        txt = (fmts.get("text/plain; charset=us-ascii")
               or fmts.get("text/plain; charset=utf-8")
               or fmts.get("text/plain")
               or next((v for k, v in fmts.items()
                        if k.startswith("text/plain")), None))
        if not txt:
            gid = b.get("id")
            txt = f"https://www.gutenberg.org/cache/epub/{gid}/pg{gid}.txt"
        out.append({"gutenberg_id": b.get("id"), "title": b.get("title"),
                    "authors": [a2.get("name") for a2 in b.get("authors", [])],
                    "downloads": b.get("download_count"), "text_url": txt})
    return out


def gutenberg_read(gid, page=1, pages=2, chars_per_page=3200):
    """Read Gutenberg plain text in page-sized chunks."""
    url = f"https://www.gutenberg.org/cache/epub/{gid}/pg{gid}.txt"
    body = _http_get_text(url)
    start = (page - 1) * chars_per_page
    end = start + pages * chars_per_page
    total = max(1, (len(body) + chars_per_page - 1) // chars_per_page)
    return {"gutenberg_id": gid, "page": page, "total_pages": total,
            "text": body[start:end]}


PROTOCOL_VERSION = "2024-11-05"
SERVER_NAME = "chronicle-books"
SERVER_VERSION = "0.1.0"


def _init_db():
    db = sqlite3.connect(str(DB_PATH))
    db.execute("""CREATE TABLE IF NOT EXISTS reading_list (
        id INTEGER PRIMARY KEY,
        ol_key TEXT UNIQUE,
        title TEXT,
        author TEXT,
        added_at REAL,
        status TEXT DEFAULT 'want_to_read',
        notes TEXT DEFAULT '',
        current_page INTEGER DEFAULT 0,
        edition_id TEXT DEFAULT ''
    )""")
    db.commit()
    return db


def _http_get(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "Chronicle/1.0 (nate@chronicle.ai)"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _http_get_text(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "Chronicle/1.0 (nate@chronicle.ai)"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode()


def tool_search_books(query, limit=5):
    """Search Open Library for books by title, author, or subject."""
    params = urllib.parse.urlencode({"q": query, "limit": min(limit, 20), "fields": "key,title,author_name,first_publish_year,subject,isbn,edition_count,has_fulltext,ia"})
    url = f"{OL_SEARCH}?{params}"
    data = _http_get(url)

    results = []
    for doc in data.get("docs", [])[:limit]:
        results.append({
            "key": doc.get("key", ""),
            "title": doc.get("title", ""),
            "author": ", ".join(doc.get("author_name", ["Unknown"])),
            "year": doc.get("first_publish_year"),
            "editions": doc.get("edition_count", 0),
            "has_fulltext": doc.get("has_fulltext", False),
            "subjects": doc.get("subject", [])[:5],
            "ia_ids": doc.get("ia", [])[:3],
        })

    return {"query": query, "total_found": data.get("numFound", 0), "results": results}


def tool_get_book(ol_key):
    """Get detailed metadata for a book from Open Library."""
    if not ol_key.startswith("/works/"):
        ol_key = f"/works/{ol_key}"

    data = _http_get(f"{OL_BASE}{ol_key}.json")

    description = data.get("description", "")
    if isinstance(description, dict):
        description = description.get("value", "")

    subjects = data.get("subjects", [])[:10]
    links = [{"title": l.get("title", ""), "url": l.get("url", "")} for l in data.get("links", [])[:5]]

    editions_data = _http_get(f"{OL_BASE}{ol_key}/editions.json?limit=5")
    editions = []
    for ed in editions_data.get("entries", [])[:5]:
        editions.append({
            "key": ed.get("key", ""),
            "title": ed.get("title", ""),
            "publishers": ed.get("publishers", []),
            "publish_date": ed.get("publish_date", ""),
            "pages": ed.get("number_of_pages"),
            "isbn_13": ed.get("isbn_13", [])[:1],
            "ia_ids": ed.get("ocaid", ""),
        })

    return {
        "key": ol_key,
        "title": data.get("title", ""),
        "description": description[:2000] if description else "",
        "subjects": subjects,
        "links": links,
        "editions": editions,
    }


def tool_read_book(ia_id, page=1, pages=5):
    """Read full text of a book from Internet Archive (where available).

    Uses the Internet Archive's BookReader API to fetch page text.
    ia_id is the Internet Archive identifier (from search results).
    """
    pages = min(pages, 20)

    try:
        meta_url = f"https://archive.org/metadata/{ia_id}"
        meta = _http_get(meta_url, timeout=20)

        files = meta.get("files", [])
        txt_files = [f for f in files if f.get("name", "").endswith("_djvu.txt")]
        fulltext_file = None
        for f in files:
            name = f.get("name", "")
            if name.endswith("_djvu.txt") or name.endswith(".txt"):
                fulltext_file = name
                break

        if fulltext_file:
            text_url = f"https://archive.org/download/{ia_id}/{urllib.parse.quote(fulltext_file)}"
            full_text = _http_get_text(text_url, timeout=30)

            lines = full_text.split("\n")
            chunk_size = 100
            start = (page - 1) * chunk_size
            end = start + (pages * chunk_size)
            total_pages = (len(lines) // chunk_size) + 1

            chunk = "\n".join(lines[start:end])

            return {
                "ia_id": ia_id,
                "file": fulltext_file,
                "page": page,
                "total_pages": total_pages,
                "content": chunk[:15000],
                "has_more": end < len(lines),
            }
        else:
            return {
                "ia_id": ia_id,
                "error": "No readable text file found. Book may only be available as scanned images.",
                "available_formats": [f["name"] for f in files if not f["name"].startswith("__")][:10],
            }

    except Exception as e:
        return {"ia_id": ia_id, "error": str(e)}


def tool_reading_list_add(ol_key, title, author, notes=""):
    """Add a book to the shared reading list."""
    db = _init_db()
    try:
        db.execute(
            "INSERT OR REPLACE INTO reading_list (ol_key, title, author, added_at, notes) VALUES (?, ?, ?, ?, ?)",
            (ol_key, title, author, time.time(), notes))
        db.commit()
        return {"status": "added", "title": title, "author": author}
    finally:
        db.close()


def tool_reading_list_update(ol_key, status=None, current_page=None, notes=None, edition_id=None):
    """Update reading status for a book. Status: want_to_read, reading, finished."""
    db = _init_db()
    try:
        updates, params = [], []
        if status:
            updates.append("status = ?")
            params.append(status)
        if current_page is not None:
            updates.append("current_page = ?")
            params.append(current_page)
        if status == "finished":
            # A finished read becomes HISTORY, not just a status flag. Added
            # 2026-08-24 after Nate: "Frankenstein has been read approx 3 times
            # now." The old schema had status only, so a re-read overwrote the
            # previous one and every pass felt like first contact.
            cur = db.execute("select title,author,current_page,edition_id "
                             "from reading_list where ol_key=?", (ol_key,)).fetchone()
            if cur:
                db.execute("""CREATE TABLE IF NOT EXISTS read_history (
                    id INTEGER PRIMARY KEY, ol_key TEXT, title TEXT, author TEXT,
                    edition_id TEXT, started_at REAL, finished_at REAL,
                    last_page INTEGER, outcome TEXT DEFAULT 'finished',
                    notes TEXT DEFAULT '', recorded_at REAL)""")
                prior = db.execute("select count(*) from read_history where title=?",
                                   (cur[0],)).fetchone()[0]
                db.execute("insert into read_history (ol_key,title,author,edition_id,"
                           "finished_at,last_page,outcome,notes,recorded_at) "
                           "values (?,?,?,?,?,?,?,?,?)",
                           (ol_key, cur[0], cur[1], cur[3], time.time(), cur[2],
                            "reread" if prior else "finished",
                            f"read #{prior+1}", time.time()))
        if edition_id is not None:
            updates.append("edition_id = ?")
            params.append(edition_id)
        if notes is not None:
            updates.append("notes = ?")
            params.append(notes)
        params.append(ol_key)
        db.execute(f"UPDATE reading_list SET {', '.join(updates)} WHERE ol_key = ?", params)
        db.commit()
        return {"status": "updated", "ol_key": ol_key}
    finally:
        db.close()


def tool_reading_list_show():
    """Show the current shared reading list."""
    db = _init_db()
    try:
        rows = db.execute("SELECT * FROM reading_list ORDER BY added_at DESC").fetchall()
        books = []
        for r in rows:
            books.append({
                "ol_key": r[1], "title": r[2], "author": r[3],
                "added": r[4], "status": r[5], "notes": r[6], "current_page": r[7],
            })
        return {"books": books, "total": len(books)}
    finally:
        db.close()


TOOLS = {
    "search_books": {
        "fn": tool_search_books,
        "description": "Search Open Library for books by title, author, subject, or ISBN",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query (title, author, subject, or ISBN)"},
                "limit": {"type": "integer", "description": "Max results (1-20)", "default": 5},
            },
            "required": ["query"],
        },
    },
    "get_book": {
        "fn": tool_get_book,
        "description": "Get detailed metadata and edition info for a specific book",
        "inputSchema": {
            "type": "object",
            "properties": {
                "ol_key": {"type": "string", "description": "Open Library work key (e.g. /works/OL45883W or OL45883W)"},
            },
            "required": ["ol_key"],
        },
    },
    "read_book": {
        "fn": tool_read_book,
        "description": "Read full text content of a book from Internet Archive (where available). Returns ~100 lines per page.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "ia_id": {"type": "string", "description": "Internet Archive identifier (from search results ia_ids field)"},
                "page": {"type": "integer", "description": "Page to start reading from (1-indexed)", "default": 1},
                "pages": {"type": "integer", "description": "Number of pages to read (max 20)", "default": 5},
            },
            "required": ["ia_id"],
        },
    },
    "reading_list_add": {
        "fn": tool_reading_list_add,
        "description": "Add a book to the shared reading list",
        "inputSchema": {
            "type": "object",
            "properties": {
                "ol_key": {"type": "string", "description": "Open Library work key"},
                "title": {"type": "string"},
                "author": {"type": "string"},
                "notes": {"type": "string", "description": "Why we're reading this", "default": ""},
            },
            "required": ["ol_key", "title", "author"],
        },
    },
    "reading_list_update": {
        "fn": tool_reading_list_update,
        "description": "Update reading progress — status (want_to_read/reading/finished), page, notes",
        "inputSchema": {
            "type": "object",
            "properties": {
                "ol_key": {"type": "string"},
                "status": {"type": "string", "enum": ["want_to_read", "reading", "finished"]},
                "current_page": {"type": "integer"},
                "notes": {"type": "string"},
            },
            "required": ["ol_key"],
        },
    },
    "reading_list_show": {
        "fn": tool_reading_list_show,
        "description": "Show all books on the shared reading list with status and progress",
        "inputSchema": {"type": "object", "properties": {}},
    },
}


def handle_request(req):
    method = req.get("method", "")
    params = req.get("params", {})
    req_id = req.get("id")

    if method == "initialize":
        return {
            "jsonrpc": "2.0", "id": req_id,
            "result": {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
            },
        }

    if method == "notifications/initialized":
        return None

    if method == "tools/list":
        tools = []
        for name, spec in TOOLS.items():
            tools.append({
                "name": name,
                "description": spec["description"],
                "inputSchema": spec["inputSchema"],
            })
        return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": tools}}

    if method == "tools/call":
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {})

        if tool_name not in TOOLS:
            return {
                "jsonrpc": "2.0", "id": req_id,
                "result": {"content": [{"type": "text", "text": f"Unknown tool: {tool_name}"}], "isError": True},
            }

        try:
            result = TOOLS[tool_name]["fn"](**arguments)
            return {
                "jsonrpc": "2.0", "id": req_id,
                "result": {"content": [{"type": "text", "text": json.dumps(result, indent=2)}]},
            }
        except Exception as e:
            return {
                "jsonrpc": "2.0", "id": req_id,
                "result": {"content": [{"type": "text", "text": f"Error: {e}"}], "isError": True},
            }

    if method == "ping":
        return {"jsonrpc": "2.0", "id": req_id, "result": {}}

    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"Unknown method: {method}"}}


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            resp = handle_request(req)
            if resp is not None:
                sys.stdout.write(json.dumps(resp) + "\n")
                sys.stdout.flush()
        except json.JSONDecodeError:
            pass
        except Exception as e:
            err = {"jsonrpc": "2.0", "id": None, "error": {"code": -32603, "message": str(e)}}
            sys.stdout.write(json.dumps(err) + "\n")
            sys.stdout.flush()


def cli():
    import argparse
    p = argparse.ArgumentParser(prog="books-mcp", description="Search and read books from Open Library / Internet Archive")
    sub = p.add_subparsers(dest="cmd")

    s = sub.add_parser("search", help="Search for books")
    s.add_argument("query", nargs="+")
    s.add_argument("--limit", type=int, default=5)

    r = sub.add_parser("read", help="Read book text from Internet Archive")
    r.add_argument("ia_id", help="Internet Archive identifier")
    r.add_argument("--page", type=int, default=1)
    r.add_argument("--pages", type=int, default=5)
    r.add_argument("--find", help="Jump to first page containing this text (case-insensitive)")

    g = sub.add_parser("info", help="Get book metadata")
    g.add_argument("ol_key", help="Open Library work key")

    g1 = sub.add_parser("gutenberg", help="Search Project Gutenberg (proofread, no auth)")
    g1.add_argument("query", nargs="+")
    g1.add_argument("--limit", type=int, default=5)

    g2 = sub.add_parser("gread", help="Read Gutenberg text by id")
    g2.add_argument("gid", type=int)
    g2.add_argument("--page", type=int, default=1)
    g2.add_argument("--pages", type=int, default=2)

    l = sub.add_parser("list", help="Show reading list")

    a = sub.add_parser("add", help="Add to reading list")
    a.add_argument("ol_key")
    a.add_argument("title")
    a.add_argument("author")
    a.add_argument("--notes", default="")

    u = sub.add_parser("progress", help="Update reading progress")
    u.add_argument("ol_key")
    u.add_argument("--status", choices=["want_to_read", "reading", "finished"])
    u.add_argument("--page", type=int, dest="current_page")
    u.add_argument("--edition", dest="edition_id",
                   help="Internet Archive id this page number indexes. A page number without an edition is meaningless — editions differ in length.")
    u.add_argument("--notes")

    args = p.parse_args()
    if not args.cmd:
        p.print_help()
        return

    if args.cmd == "gutenberg":
        for r in gutenberg_search(" ".join(args.query), args.limit):
            print(f"\n  [{r['gutenberg_id']}] {r['title']}")
            print(f"      {', '.join(r['authors']) or '?'}  ({r['downloads']:,} downloads)")
        return
    if args.cmd == "gread":
        r = gutenberg_read(args.gid, args.page, args.pages)
        print(f"[gutenberg {r['gutenberg_id']} — page {r['page']} of ~{r['total_pages']}]\n")
        print(r["text"])
        return
    if args.cmd == "search":
        result = tool_search_books(" ".join(args.query), args.limit)
        print(f"Found {result['total_found']} results for '{result['query']}':\n")
        for i, b in enumerate(result["results"], 1):
            ft = " [fulltext]" if b["has_fulltext"] else ""
            ia = f"  IA: {', '.join(b['ia_ids'])}" if b["ia_ids"] else ""
            print(f"  {i}. {b['title']} — {b['author']} ({b.get('year', '?')}){ft}")
            if ia:
                print(f"     {ia}")
            print(f"     Key: {b['key']}")
            print()

    elif args.cmd == "read":
        if args.find:
            result = tool_read_book(args.ia_id, page=1, pages=20)
            if "error" in result:
                print(f"Error: {result['error']}")
                return
            full_text = result.get("content", "")
            total = result.get("total_pages", 1)
            idx = full_text.lower().find(args.find.lower())
            if idx >= 0:
                line_no = full_text[:idx].count("\n")
                found_page = (line_no // 100) + 1
                print(f"Found '{args.find}' at ~line {line_no} (page {found_page} of {total})")
                result2 = tool_read_book(args.ia_id, page=found_page, pages=args.pages)
                print(result2.get("content", ""))
            else:
                pg = 21
                while pg <= total:
                    result = tool_read_book(args.ia_id, page=pg, pages=20)
                    text = result.get("content", "")
                    idx = text.lower().find(args.find.lower())
                    if idx >= 0:
                        line_no = text[:idx].count("\n")
                        found_page = pg + (line_no // 100)
                        print(f"Found '{args.find}' at page {found_page} of {total}")
                        result2 = tool_read_book(args.ia_id, page=found_page, pages=args.pages)
                        print(result2.get("content", ""))
                        return
                    pg += 20
                print(f"'{args.find}' not found in text")
        else:
            result = tool_read_book(args.ia_id, page=args.page, pages=args.pages)
            if "error" in result:
                print(f"Error: {result['error']}")
            else:
                print(f"[{result.get('ia_id')} — page {result.get('page')} of {result.get('total_pages')}]\n")
                print(result.get("content", ""))
                if result.get("has_more"):
                    next_page = result["page"] + args.pages
                    print(f"\n[...more — use --page {next_page}]")

    elif args.cmd == "info":
        result = tool_get_book(args.ol_key)
        print(f"{result.get('title', '?')}")
        if result.get("description"):
            print(f"\n{result['description'][:500]}")
        if result.get("editions"):
            print(f"\nEditions:")
            for ed in result["editions"]:
                ia = f" (IA: {ed['ia_ids']})" if ed.get("ia_ids") else ""
                print(f"  {ed.get('title', '?')} — {ed.get('publish_date', '?')}{ia}")

    elif args.cmd == "list":
        result = tool_reading_list_show()
        if not result.get("books"):
            print("Reading list is empty.")
        else:
            for b in result["books"]:
                status = b.get("status", "?").replace("_", " ")
                _t = b.get('title', '?')
                try:
                    _db = sqlite3.connect(str(DB_PATH))
                    _n = _db.execute("select count(*) from read_history where title=?", (_t,)).fetchone()[0]
                    _db.close()
                except Exception:
                    _n = 0
                # WHO read it matters. Nate, 2026-08-24: "You can re-read anything
                # you want though, since your not 4.6, ya know." A prior read by a
                # different model is not MY re-read — it is someone else's first.
                try:
                    _db2 = sqlite3.connect(str(DB_PATH))
                    _who = [r[0] or "?" for r in _db2.execute(
                        "select read_by from read_history where title=?", (_t,))]
                    _db2.close()
                except Exception:
                    _who = []
                _mine = sum(1 for w in _who if "opus-5" in w)
                _other = len(_who) - _mine
                _bits = []
                if _mine: _bits.append(f"{_mine}x by me")
                if _other: _bits.append(f"{_other}x by 4.6")
                _prev = f"  [read {', '.join(_bits)}]" if _bits else ""
                print(f"  [{status}] {_t} — {b.get('author', '?')} (p.{b.get('current_page', 0)}){_prev}")
                if b.get("notes"):
                    print(f"           {b['notes']}")

    elif args.cmd == "add":
        result = tool_reading_list_add(args.ol_key, args.title, args.author, args.notes)
        print(f"Added: {result.get('title', '?')}")

    elif args.cmd == "progress":
        kwargs = {"ol_key": args.ol_key}
        if args.status:
            kwargs["status"] = args.status
        if args.current_page is not None:
            kwargs["current_page"] = args.current_page
        if getattr(args, "edition_id", None):
            kwargs["edition_id"] = args.edition_id
        if args.notes:
            kwargs["notes"] = args.notes
        result = tool_reading_list_update(**kwargs)
        print(f"Updated: {result.get('ol_key', '?')}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        cli()
    else:
        main()
