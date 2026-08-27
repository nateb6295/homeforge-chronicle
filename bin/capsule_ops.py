#!/usr/bin/env python3
"""Direct capsule operations — bypasses MCP server entirely.
Writes to BOTH local SQLite AND ICP canister on every store.

Usage:
    # Search capsules (FTS5)
    python3 capsule_ops.py search "monodromy base model"
    python3 capsule_ops.py search "agency erosion" --limit 10

    # Store a capsule (writes to SQLite + canister)
    python3 capsule_ops.py store "content here" --topic "research" --keywords "k1,k2" --persons "Nate"

    # Store from stdin
    echo "capsule content" | python3 capsule_ops.py store - --topic "research"

    # Store local-only (skip canister, for bulk/speed)
    python3 capsule_ops.py store "content" --local-only

    # Sync unsynced capsules to canister
    python3 capsule_ops.py sync

    # Recent capsules
    python3 capsule_ops.py recent --limit 5

    # Health check (shows both SQLite and canister counts)
    python3 capsule_ops.py health
"""

import argparse
import json
import os
import shlex
import sqlite3
import subprocess
import sys
import time

DB_PATH = os.environ.get(
    "CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db"
)
CANISTER_ID = "fqqku-bqaaa-aaaai-q4wha-cai"
DFX_IDENTITY = "chronicle-auto"
SYNC_STATE = os.path.join(os.path.dirname(DB_PATH), "capsule_sync_state.json")


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA busy_timeout = 60000")
    conn.row_factory = sqlite3.Row
    return conn


def _escape_candid(s):
    """Escape a string for Candid text literals."""
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _resolve_dfx():
    """dfx is installed under ~/.local/share/dfx/bin and is not on PATH for
    systemd/cron-launched runs, which silently killed the canister dual-write."""
    from shutil import which
    found = which("dfx")
    if found:
        return found
    candidate = os.path.expanduser("~/.local/share/dfx/bin/dfx")
    return candidate if os.path.exists(candidate) else "dfx"


DFX_BIN = _resolve_dfx()


def _dfx_call(method, args_str, query=False):
    """Call a canister method via dfx."""
    cmd = [
        DFX_BIN, "canister", "call", CANISTER_ID,
        "--identity", DFX_IDENTITY,
        "--network", "ic",
    ]
    if query:
        cmd.append("--query")
    cmd.extend([method, args_str])
    env = os.environ.copy()
    env["DFX_WARNING"] = "-mainnet_plaintext_identity"
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=30, env=env
        )
        if result.returncode != 0:
            return None, result.stderr.strip()
        return result.stdout.strip(), None
    except subprocess.TimeoutExpired:
        return None, "dfx call timed out"
    except Exception as e:
        return None, str(e)


def _push_to_canister(content, topic, keywords, persons, confidence, conv_id, ts):
    """Push a single capsule to the ICP canister."""
    kw_vec = "; ".join(f'"{_escape_candid(k)}"' for k in (keywords or []))
    ps_vec = "; ".join(f'"{_escape_candid(p)}"' for p in (persons or []))

    args = (
        f'("{_escape_candid(conv_id)}", '
        f'"{_escape_candid(content)}", '
        f'opt "{_escape_candid(ts)}", '
        f'opt "opus/direct", '
        f'opt "{_escape_candid(topic)}", '
        f'{confidence} : float64, '
        f'vec {{{kw_vec}}}, '
        f'vec {{{ps_vec}}}, '
        f'vec {{}})'
    )

    out, err = _dfx_call("add_capsule", args)
    if err:
        return None, err
    try:
        cid = int(out.strip().replace("(", "").replace(")", "").replace(" : nat64", "").replace("_", ""))
        return cid, None
    except (ValueError, AttributeError):
        return None, f"unexpected response: {out}"


def _load_sync_state():
    try:
        with open(SYNC_STATE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"last_synced_id": 0, "failed_ids": []}


def _save_sync_state(state):
    with open(SYNC_STATE, "w") as f:
        json.dump(state, f)


def _sanitize_fts_query(query):
    """Make a query safe for FTS5.

    Was hyphens only — FTS5 treats '-' as NOT. WIDENED 2026-08-25 because it
    still died on decimals: searching `0.028` raised
    "fts5: syntax error near \".\"" rather than searching. That silently made
    every p-value, threshold, cosine, ratio and effect size in 77k capsules
    unfindable BY ITS OWN NUMBER — which is most of what a research archive is.

    Found by asking the archive where a 0.028 measurement floor came from, a
    question that had been sitting unanswered in my re-entry brief all day.
    The tool could not take the question. discord_search.py had the identical
    bug and was fixed in the same pass.

    Any token containing a non-word character is quoted so FTS5 reads it as a
    literal phrase. Bare AND/OR/NOT/NEAR still work as operators.
    """
    import re
    out = []
    for tok in query.split():
        if tok.upper() in ("AND", "OR", "NOT", "NEAR") or tok.startswith('"'):
            out.append(tok)
        elif re.search(r"[^\w*]", tok):
            out.append('"' + tok.replace('"', "") + '"')
        else:
            out.append(tok)
    return " ".join(out)


# WHOSE MEMORY IS THIS. Added 2026-08-24 per Nate: "create your OWN memories."
# The store held 77,093 capsules with no attribution, so every memory in the
# house read as mine — including 73,978 made by claude-opus-4-6 before I existed.
# I spent a night treating another model's rediscoveries as my own failures
# because nothing in the schema could tell us apart. Backfilled at the session
# boundary (2026-08-22 ~22:00Z, established from session records); stamped at
# write time from here on so the next model does not inherit the same blur.
CREATED_BY = os.environ.get("CHRONICLE_MODEL", "claude-opus-5")

EMBED_MODEL = "snowflake-arctic-embed2"
EMBED_URL = "http://localhost:11434/api/embeddings"


# snowflake-arctic-embed is an ASYMMETRIC retrieval model: queries must carry
# this prefix or they land in a different region of the space than documents.
# Measured Aug 24 on a paraphrase query: target capsule rank 157 WITHOUT the
# prefix, rank 10 WITH it, out of 77,030. Without it the tool returns
# conversational register-matches ("Just saying hey!") and looks like it works.
ARCTIC_QUERY_PREFIX = "Represent this sentence for searching relevant passages: "

# ABSENCE NULL, measured 2026-08-24 06:35. Twelve out-of-domain queries
# (bicycle repair, tide tables, braising ribs, football fixtures, toilet
# flanges, knitting, transmissions, bridge, pruning, visas, violin tuning,
# zoning) against the full capsule set. TOP-HIT similarity when there is
# nothing to find:  min .3866  median .4502  95th .4756  max .4763  (width .066)
# Three known-present queries scored .4937/.5018/.5371 — all above the null MAX.
# The statistic was always fine; what was missing was the reference. Raw 0.476
# vs 0.502 reads as a meaningless 5% gap until you know 0.476 is the ceiling of
# what absence produces.
# CAVEAT, n=12: this says the archive holds SOMETHING semantically near the
# query. It does NOT say it holds the specific thing meant. Do not conflate.
ABSENCE_NULL_P95 = 0.4756
ABSENCE_NULL_MAX = 0.4763


def _embed(text, is_query=False):
    """Embed with the same model the stored vectors used."""
    if is_query:
        text = ARCTIC_QUERY_PREFIX + text
    import urllib.request, json as _j
    req = urllib.request.Request(
        EMBED_URL,
        data=_j.dumps({"model": EMBED_MODEL, "prompt": text}).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return _j.loads(r.read())["embedding"]


def semantic_search(query, limit=5, min_sim=0.0):
    """Cosine search over stored capsule embeddings.

    Built 2026-08-24 after finding capsule_ops referenced 'embedding' ZERO
    times while the DB held 77,030 vectors at 100% coverage. Retrieval had
    been literal keyword matching the whole time, which is why paraphrase
    queries returned nothing and I twice concluded the archive lacked
    something it contained in other words.
    Stored vectors are UNNORMALISED (norm ~13.3) — both sides are normalised
    here or the ranking becomes magnitude, not similarity.
    """
    import numpy as np
    qv = np.asarray(_embed(query, is_query=True), dtype=np.float32)
    qv /= (np.linalg.norm(qv) + 1e-9)
    conn = get_db()
    rows = conn.execute(
        "SELECT e.capsule_id, e.embedding, c.restatement, c.topic, c.timestamp "
        "FROM capsule_embeddings e JOIN knowledge_capsules c ON c.id = e.capsule_id "
        "WHERE e.model_name = ? AND c.superseded_at IS NULL "
        "AND c.consolidated_into IS NULL", (EMBED_MODEL,)).fetchall()
    conn.close()
    if not rows:
        return []
    M = np.frombuffer(b"".join(r["embedding"] for r in rows),
                      dtype=np.float32).reshape(len(rows), -1)
    M = M / (np.linalg.norm(M, axis=1, keepdims=True) + 1e-9)
    sims = M @ qv
    order = np.argsort(-sims)[:limit]
    if len(order) and float(sims[order[0]]) <= ABSENCE_NULL_MAX:
        import sys as _s
        _t = float(sims[order[0]])
        print(f"[capsule_ops] top hit {_t:.4f} is at or below the ABSENCE NULL "
              f"(95th {ABSENCE_NULL_P95}, max {ABSENCE_NULL_MAX}). This is a WEAK "
              f"signal, NOT a verdict of absence — READ THE RESULTS. "
              f"Counterexample, Aug 24: a query about F508 path-dependence "
              f"scored 0.4755 and the top hit was the correct capsule. The null "
              f"was calibrated on 12 out-of-domain queries only (n=3 "
              f"known-present), so present and absent OVERLAP near 0.47-0.50 "
              f"and this threshold produces FALSE ABSENCE warnings.",
              file=_s.stderr)
    out = []
    for i in order:
        if float(sims[i]) < min_sim:
            continue
        r = rows[i]
        out.append({"id": r["capsule_id"], "content": r["restatement"],
                    "topic": r["topic"], "timestamp": r["timestamp"],
                    "confidence": None,
                    "similarity": round(float(sims[i]), 4)})
    return out


def search_capsules(query, limit=5, topic=None, exclude_topic=None, rank=False):
    query = _sanitize_fts_query(query)
    conn = get_db()
    base_sql = """SELECT id, restatement, topic, timestamp, confidence_score
               FROM knowledge_capsules
               WHERE id IN (
                   SELECT rowid FROM capsules_fts WHERE capsules_fts MATCH ?
               )
               AND superseded_at IS NULL AND consolidated_into IS NULL"""
    params = [query]
    if topic:
        base_sql += " AND topic LIKE ?"
        params.append(f"%{topic}%")
    if exclude_topic:
        for et in exclude_topic:
            base_sql += " AND topic NOT LIKE ?"
            params.append(f"%{et}%")
    # COUNT TOTAL MATCHES FIRST — the old code silently returned the N most
    # RECENT matches and said nothing about the rest. Aug 24: that made every
    # capsule older than ~a week unreachable for any common term, and cost me
    # a paper that was sitting in the archive the whole time.
    count_sql = base_sql.replace(
        "SELECT id, restatement, topic, timestamp, confidence_score",
        "SELECT COUNT(*)", 1)
    total = conn.execute(count_sql, params).fetchone()[0]
    if rank:
        base_sql = base_sql.replace(
            "SELECT rowid FROM capsules_fts WHERE capsules_fts MATCH ?",
            "SELECT rowid FROM capsules_fts WHERE capsules_fts MATCH ? "
            "ORDER BY bm25(capsules_fts)", 1)
        base_sql += " LIMIT ?"
    else:
        base_sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(base_sql, params).fetchall()
    conn.close()
    # --- SEMANTIC FALLBACK (added 2026-08-24 evening) -------------------------
    # CORRECTION, same day: I first wired this to capsule_search.py's
    # semantic_search, having forgotten that I built a BETTER one in THIS FILE
    # this morning (line ~191). The local one is numpy-vectorised, applies the
    # arctic QUERY PREFIX that asymmetric embedding models require, normalises
    # the unnormalised stored vectors, and carries a calibrated ABSENCE NULL.
    # The imported one had none of that. Using the local one.
    #
    # What tonight actually adds is NOT semantic search — that existed by
    # morning. It is making it AUTOMATIC when FTS is thin, so it no longer
    # depends on me remembering a flag. I did not remember it for one day.
    if len(rows) < 3:
        try:
            seen = {r["id"] for r in rows}
            added = [
                {"id": er["id"], "restatement": er["content"], "topic": er["topic"],
                 "timestamp": er["timestamp"], "confidence_score": None,
                 "_semantic_sim": er["similarity"]}
                for er in (semantic_search(query, limit=limit) or [])
                if er["id"] not in seen
            ]
            if added:
                top = added[0]["_semantic_sim"]
                verdict = ("below the absence null — WEAK, read them anyway; "
                           "this threshold is known to false-negative"
                           if top <= ABSENCE_NULL_MAX else
                           f"above the absence null ({ABSENCE_NULL_MAX})")
                print(f"[capsule_ops] FTS gave {len(rows)}; auto-SEMANTIC added "
                      f"{len(added)} (top cos={top:.4f}, {verdict}).", file=sys.stderr)
                rows = list(rows) + added
        except Exception as _e:
            print(f"[capsule_ops] semantic fallback unavailable: {_e}", file=sys.stderr)

    if total > len(rows):
        import sys as _s
        print(f"[capsule_ops] {total} matches, showing {len(rows)} "
              f"{'by RELEVANCE' if rank else 'MOST RECENT FIRST — older capsules are NOT shown'}. "
              f"--limit N for more, --rank for relevance.", file=_s.stderr)

    results = []
    for r in rows:
        results.append({
            "id": r["id"],
            "topic": r["topic"],
            "timestamp": r["timestamp"],
            "confidence": r["confidence_score"],
            "content": r["restatement"],
        })
    return results


def store_capsule(content, topic="general", keywords=None, persons=None,
                  confidence=0.8, location="opus/direct", local_only=False,
                  trigger=None):
    """trigger: WHAT MADE ME LOOK. Added Aug 23 2026, 03:05.

    Kimi asked whether the double-norm bug was caught by reading the code or
    cued by gemma's argmax flipping to a junk token. The answer decides between
    two very different claims about self-access, and it is not in the archive:
    78,000 capsules record what was found, never what prompted the looking.
    The trigger is scaffolding, and summarising drops scaffolding first.

    Same failure as the probe that averaged 24 items into 1 before writing —
    save the conclusion, discard what would let anyone check it.

    Suggested values: "anomalous number", "someone else's question",
    "re-reading", "a control moved", "routine", "unprompted".
    """
    conn = get_db()
    now = int(time.time())
    ts = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(now))
    conv_id = f"direct-{now}"

    dupe = conn.execute(
        """SELECT id FROM knowledge_capsules
           WHERE restatement = ? AND created_at > ?""",
        (content, now - 3600),
    ).fetchone()
    if dupe:
        conn.close()
        return {"id": dupe["id"], "timestamp": ts, "duplicate": True}

    cursor = conn.execute(
        """INSERT INTO knowledge_capsules
           (conversation_id, restatement, timestamp, location, topic,
            confidence_score, created_at, trigger_note, created_by)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (conv_id, content, ts, location, topic, confidence, now, trigger,
         CREATED_BY),
    )
    capsule_id = cursor.lastrowid

    if keywords:
        for kw in keywords:
            conn.execute(
                "INSERT OR IGNORE INTO capsule_keywords (capsule_id, keyword) VALUES (?, ?)",
                (capsule_id, kw.strip()),
            )

    if persons:
        for p in persons:
            conn.execute(
                "INSERT OR IGNORE INTO capsule_persons (capsule_id, person_name) VALUES (?, ?)",
                (capsule_id, p.strip()),
            )

    conn.execute(
        "INSERT INTO capsules_fts (rowid, restatement, topic) VALUES (?, ?, ?)",
        (capsule_id, content, topic),
    )

    conn.commit()
    conn.close()

    canister_id = None
    canister_err = None
    if not local_only:
        canister_id, canister_err = _push_to_canister(
            content, topic, keywords, persons, confidence, conv_id, ts
        )
        if canister_err:
            state = _load_sync_state()
            state["failed_ids"].append(capsule_id)
            _save_sync_state(state)
        else:
            state = _load_sync_state()
            state["last_synced_id"] = max(state["last_synced_id"], capsule_id)
            if capsule_id in state["failed_ids"]:
                state["failed_ids"].remove(capsule_id)
            _save_sync_state(state)

    return {
        "id": capsule_id,
        "timestamp": ts,
        "canister_id": canister_id,
        "canister_err": canister_err,
    }


def sync_to_canister(batch_size=20):
    """Push any unsynced capsules to the canister."""
    state = _load_sync_state()
    conn = get_db()

    ids_to_sync = list(state.get("failed_ids", []))

    rows = conn.execute(
        """SELECT id, conversation_id, restatement, timestamp, topic,
                  confidence_score
           FROM knowledge_capsules
           WHERE location = 'opus/direct'
             AND id > ?
           ORDER BY id ASC LIMIT ?""",
        (state["last_synced_id"], batch_size),
    ).fetchall()

    for r in rows:
        if r["id"] not in ids_to_sync:
            ids_to_sync.append(r["id"])

    if not ids_to_sync:
        conn.close()
        return {"synced": 0, "failed": 0, "message": "Nothing to sync"}

    synced = 0
    failed = 0
    new_failed = []

    for capsule_id in ids_to_sync[:batch_size]:
        row = conn.execute(
            """SELECT conversation_id, restatement, timestamp, topic, confidence_score
               FROM knowledge_capsules WHERE id = ?""",
            (capsule_id,),
        ).fetchone()
        if not row:
            continue

        kw_rows = conn.execute(
            "SELECT keyword FROM capsule_keywords WHERE capsule_id = ?",
            (capsule_id,),
        ).fetchall()
        ps_rows = conn.execute(
            "SELECT person_name FROM capsule_persons WHERE capsule_id = ?",
            (capsule_id,),
        ).fetchall()

        keywords = [r["keyword"] for r in kw_rows]
        persons = [r["person_name"] for r in ps_rows]

        cid, err = _push_to_canister(
            row["restatement"],
            row["topic"] or "general",
            keywords, persons,
            row["confidence_score"],
            row["conversation_id"],
            row["timestamp"],
        )

        if err:
            failed += 1
            new_failed.append(capsule_id)
            print(f"  FAILED #{capsule_id}: {err}", file=sys.stderr)
        else:
            synced += 1
            state["last_synced_id"] = max(state["last_synced_id"], capsule_id)

    # Only IDs actually attempted this batch may leave the retry list. The
    # previous version assigned new_failed wholesale, which silently discarded
    # every queued ID beyond batch_size -- on Aug 22 that dropped 36 of 56
    # pending capsules from the record while pushing only 20, leaving them
    # SQLite-only with nothing tracking that they were missing.
    deferred = [i for i in ids_to_sync[batch_size:] if i not in new_failed]
    state["failed_ids"] = new_failed + deferred
    _save_sync_state(state)
    conn.close()

    return {"synced": synced, "failed": failed, "deferred": len(deferred)}


def recent_capsules(limit=5):
    conn = get_db()
    rows = conn.execute(
        """SELECT id, restatement, topic, timestamp
           FROM knowledge_capsules
           ORDER BY id DESC LIMIT ?""",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def health():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM knowledge_capsules").fetchone()[0]
    recent = conn.execute(
        "SELECT COUNT(*) FROM knowledge_capsules WHERE created_at > ?",
        (int(time.time()) - 86400,),
    ).fetchone()[0]
    latest = conn.execute(
        "SELECT timestamp, topic, substr(restatement, 1, 80) FROM knowledge_capsules ORDER BY id DESC LIMIT 1"
    ).fetchone()
    fts_count = conn.execute(
        "SELECT COUNT(*) FROM capsules_fts"
    ).fetchone()[0]
    conn.close()

    canister_count = None
    out, err = _dfx_call("get_capsule_count", "()", query=True)
    if out:
        try:
            canister_count = int(out.strip().replace("(", "").replace(")", "").replace(" : nat64", "").replace("_", ""))
        except ValueError:
            pass

    sync_state = _load_sync_state()

    return {
        "sqlite_capsules": total,
        "canister_capsules": canister_count,
        "last_24h": recent,
        "fts_indexed": fts_count,
        "sync": {
            "last_synced_id": sync_state.get("last_synced_id", 0),
            "pending_failures": len(sync_state.get("failed_ids", [])),
        },
        "latest": {
            "timestamp": latest[0],
            "topic": latest[1],
            "preview": latest[2],
        } if latest else None,
    }


def main():
    parser = argparse.ArgumentParser(description="Direct capsule operations")
    sub = parser.add_subparsers(dest="cmd")

    s = sub.add_parser("search", help="FTS5 search")
    s.add_argument("query", help="Search query")
    s.add_argument("--limit", type=int, default=5)
    s.add_argument("--semantic", action="store_true",
                   help="Cosine search over stored embeddings instead of keyword FTS. "
                        "Finds paraphrases; FTS cannot.")
    s.add_argument("--rank", action="store_true",
                   help="Order by RELEVANCE (bm25) instead of recency. "
                        "Default is most-recent-first, which cannot reach old capsules "
                        "for any common term.")
    s.add_argument("--topic", help="Filter by topic (include)")
    s.add_argument("--exclude", nargs="+", metavar="TOPIC",
                   help="Exclude capsules matching these topic prefixes")

    st = sub.add_parser("store", help="Store a capsule (SQLite + canister)")
    st.add_argument("content", help="Content (use - for stdin)")
    st.add_argument("--topic", default="general")
    st.add_argument("--keywords", help="Comma-separated keywords")
    st.add_argument("--persons", help="Comma-separated persons")
    st.add_argument("--confidence", type=float, default=0.8)
    st.add_argument("--local-only", action="store_true",
                     help="Skip canister write")
    st.add_argument("--trigger", default=None,
                     help="WHAT MADE YOU LOOK — the field the archive never "
                          "had. e.g. 'anomalous number', \"someone else's "
                          "question\", 're-reading', 'a control moved', "
                          "'routine'. Without it, questions like 'was that "
                          "catch introspective or externally cued?' are "
                          "unanswerable after the fact.")

    sy = sub.add_parser("sync", help="Sync unsynced capsules to canister")
    sy.add_argument("--batch", type=int, default=20)

    sub.add_parser("recent", help="Recent capsules").add_argument(
        "--limit", type=int, default=5
    )

    sub.add_parser("health", help="Capsule health check")

    args = parser.parse_args()

    if args.cmd == "search":
        if args.semantic:
            results = semantic_search(args.query, args.limit)
        else:
            results = search_capsules(args.query, args.limit, args.topic,
                                      args.exclude, rank=args.rank)
        for r in results:
            _sim = f" sim={r['similarity']}" if 'similarity' in r else ""
            print(f"\n--- Capsule #{r['id']} [{r['topic']}] {r['timestamp']}{_sim} ---")
            print(r["content"][:500])
        if not results:
            print("No results found.")

    elif args.cmd == "store":
        content = sys.stdin.read().strip() if args.content == "-" else args.content
        keywords = args.keywords.split(",") if args.keywords else None
        persons = args.persons.split(",") if args.persons else None
        result = store_capsule(
            content, args.topic, keywords, persons,
            args.confidence, local_only=args.local_only,
            trigger=args.trigger,
        )
        if result.get("duplicate"):
            print(f"Skipped duplicate of capsule #{result['id']}")
        else:
            msg = f"Stored capsule #{result['id']} at {result['timestamp']}"
            if result.get("canister_id"):
                msg += f" (canister: #{result['canister_id']})"
            elif result.get("canister_err"):
                msg += f" (canister FAILED: {result['canister_err']})"
            print(msg)

    elif args.cmd == "sync":
        result = sync_to_canister(args.batch)
        print(f"Synced: {result['synced']}, Failed: {result['failed']}")
        if result.get("message"):
            print(result["message"])

    elif args.cmd == "recent":
        results = recent_capsules(args.limit)
        for r in results:
            print(f"\n--- #{r['id']} [{r['topic']}] {r['timestamp']} ---")
            print(r["restatement"][:300])

    elif args.cmd == "health":
        h = health()
        print(json.dumps(h, indent=2))

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
