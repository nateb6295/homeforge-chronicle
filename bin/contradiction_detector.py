#!/usr/bin/env python3
"""Contradiction Detector — experimental survival filter for Chronicle memory.

Frame (thread #315, Rosenblatt reframe): the harness is a filter, not a store.
Chronicle's only active selection pressure today is Nate. This tool is a first
attempt at an *internal* pressure: when two capsules say incompatible things,
the older one earns demotion unless it's the one that survives judgment.

Usage:
    contradiction_detector.py <capsule_id> [--top-k 8] [--apply]
    contradiction_detector.py --recent N [--apply]   # scan N most recent

Without --apply, runs in dry-run: judges and prints, but never demotes.
With --apply, records the judgment and decrements the loser's confidence.

This is a probe. Keep it cheap, keep it observable, keep it reversible.
"""

import argparse
import json
import os
import sqlite3
import struct
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from vector_index import VectorIndex

DB = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
FAISS_PATH = "/mnt/hdd/chronicle-data/capsules.faiss"
LLAMA_URL = "http://localhost:11435/v1/chat/completions"
LLAMA_MODEL = "gemma-4-26B-A4B-it-Q4_K_M.gguf"
ENGINE_URL = "http://localhost:11436/api/chat"
ENGINE_MODEL = "qwen3:32b"
# Topics excluded from the "claim pool" — observations, not claims we made.
NOISE_TOPIC_PREFIXES = ("feed/", "crossref/", "intern/")

SIMILARITY_FLOOR = 0.72   # below this, pairs are too different to be in tension
DEMOTION_STEP = 0.08      # confidence decrement on contradiction (loser side)
BOOST_STEP = 0.02         # small reinforcement on agreement (winner side)

JUDGE_SYSTEM = """You are judging two memory entries from the same journaling system.
Both are written in first person by the same agent over time. They are CLAIMS the
agent made about itself, people it works with, or how its system should behave.

Return strict JSON with fields:
  verdict: "contradicts" | "agrees" | "extends" | "orthogonal"
  loser:   "newer" | "older" | "neither"
  rationale: <=200 chars explaining why.

Definitions:
- "contradicts" = the two claims cannot both be true as stated. Pick a loser
  by this rule, IN ORDER:
    1. If the NEWER claim explicitly corrects, resolves, or supersedes the
       older (e.g. "CORRECTION", "updated view", "what I got wrong was…",
       or the newer is an architectural resolution the older was reaching
       toward) → loser = "older".
    2. Otherwise, if one claim is factual and the other is a question,
       speculation, or earlier framing later shown to be wrong → loser is
       the weaker factual standing, usually the older.
    3. Only use loser = "newer" if the older is the more stable, tested,
       or canonical claim and the newer is a drift or regression.
    4. Use loser = "neither" only when both are stated with equal force and
       neither reads as a resolution of the other.
  Do NOT use confidence, hedging, or rhetorical softness to pick the loser.
  A tentative voice does not mean a weaker claim.

- "agrees" = the claims are compatible and overlap. loser = "neither".

- "extends" = newer refines or adds nuance to older WITHOUT invalidating it.
  loser = "neither".

- "orthogonal" = they share vocabulary but make different claims, OR they are
  two separate installments / chapters / posts in the same series. Two thread
  openers on related themes are ALWAYS "orthogonal" unless the newer actually
  names and revises a stance from the older.

Hard rules:
- "different artifact, same topic" is "orthogonal", NOT "contradicts" or
  "extends". Example: Thread #248 and Thread #302 both exploring filters are
  orthogonal, even if the vocabulary rhymes.
- An objective/goal entry being replaced by a corrected objective/goal IS a
  contradiction with loser = "older".

Return ONLY the JSON object, no prose, no preamble."""

JUDGE_SCHEMA = {
    "type": "object",
    "properties": {
        "verdict": {"type": "string", "enum": ["contradicts", "agrees", "extends", "orthogonal"]},
        "loser": {"type": "string", "enum": ["newer", "older", "neither"]},
        "rationale": {"type": "string"},
    },
    "required": ["verdict", "loser", "rationale"],
}


def ensure_schema(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS capsule_contradictions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        newer_id INTEGER NOT NULL,
        older_id INTEGER NOT NULL,
        similarity REAL NOT NULL,
        verdict TEXT NOT NULL,
        loser TEXT NOT NULL,
        rationale TEXT,
        applied INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL,
        FOREIGN KEY(newer_id) REFERENCES knowledge_capsules(id),
        FOREIGN KEY(older_id) REFERENCES knowledge_capsules(id)
    );
    CREATE INDEX IF NOT EXISTS idx_contra_pair ON capsule_contradictions(newer_id, older_id);
    CREATE INDEX IF NOT EXISTS idx_contra_verdict ON capsule_contradictions(verdict);
    """)


def load_capsule(conn, cap_id):
    row = conn.execute(
        "SELECT id, restatement, topic, confidence_score, created_at, superseded_at "
        "FROM knowledge_capsules WHERE id = ?", (cap_id,)).fetchone()
    if not row:
        return None
    return dict(id=row[0], restatement=row[1], topic=row[2],
                confidence=row[3], created_at=row[4], superseded_at=row[5])


def load_embedding(conn, cap_id):
    row = conn.execute(
        "SELECT embedding FROM capsule_embeddings WHERE capsule_id = ?",
        (cap_id,)).fetchone()
    if not row:
        return None
    blob = row[0]
    n = len(blob) // 4
    return list(struct.unpack(f"{n}f", blob))


def _extract_json(content):
    content = content.strip()
    if not content:
        return None
    if content.startswith("```"):
        content = content.strip("`")
        if content.startswith("json"):
            content = content[4:]
        content = content.strip()
    # If the model wrapped JSON in prose, grab the first {...} block.
    if not content.startswith("{"):
        import re
        m = re.search(r"\{.*\}", content, re.DOTALL)
        if m:
            content = m.group(0)
    return content


def judge_pair(newer, older, backend="llama"):
    """Judge a capsule pair. backend: 'llama' (local Gemma) or 'engine' (cloud Qwen)."""
    prompt = (
        f"NEWER (id={newer['id']}):\n{newer['restatement'].strip()}\n\n"
        f"OLDER (id={older['id']}):\n{older['restatement'].strip()}"
    )
    messages = [
        {"role": "system", "content": JUDGE_SYSTEM},
        {"role": "user", "content": prompt},
    ]
    content = None
    try:
        if backend == "engine":
            body = {"model": ENGINE_MODEL, "messages": messages,
                    "stream": False, "options": {"temperature": 0.1}}
            req = urllib.request.Request(ENGINE_URL,
                data=json.dumps(body).encode(),
                headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=90) as resp:
                data = json.loads(resp.read())
            content = data.get("message", {}).get("content", "")
        else:
            body = {"model": LLAMA_MODEL, "messages": messages,
                    "temperature": 0.1, "max_tokens": 300,
                    "response_format": {"type": "json_schema",
                        "json_schema": {"name": "verdict", "schema": JUDGE_SCHEMA}}}
            req = urllib.request.Request(LLAMA_URL,
                data=json.dumps(body).encode(),
                headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=90) as resp:
                data = json.loads(resp.read())
            content = data["choices"][0]["message"]["content"]
        parsed = _extract_json(content)
        if not parsed:
            return None
        return json.loads(parsed)
    except Exception as e:
        print(f"  [judge error/{backend}] {e} | content={str(content)[:120]!r}",
              file=sys.stderr)
        return None


def is_claim_topic(topic):
    """A capsule is a 'claim' if it's not from the noise-heavy feed/crossref pipeline."""
    if topic is None:
        return False
    return not any(topic.startswith(p) for p in NOISE_TOPIC_PREFIXES)


def process_capsule(conn, idx, cap_id, top_k, apply_changes, claim_pool=False, backend="llama"):
    newer = load_capsule(conn, cap_id)
    if not newer:
        print(f"capsule {cap_id} not found")
        return []
    if newer["superseded_at"]:
        print(f"capsule {cap_id} already superseded, skipping")
        return []

    emb = load_embedding(conn, cap_id)
    if emb is None:
        print(f"capsule {cap_id} has no embedding")
        return []

    # If claim-pool mode, skip if the candidate itself isn't a claim.
    if claim_pool and not is_claim_topic(newer.get("topic")):
        return []

    # Search with expanded k so we can filter neighbors that aren't claims.
    search_k = top_k * 6 if claim_pool else top_k + 1
    hits = idx.search(emb, k=search_k)
    results = []
    accepted = 0
    for hit_id, sim in hits:
        if accepted >= top_k:
            break
        if hit_id == cap_id:
            continue
        if sim < SIMILARITY_FLOOR:
            continue
        older = load_capsule(conn, hit_id)
        if not older or older["superseded_at"]:
            continue
        if older["created_at"] >= newer["created_at"]:
            continue
        if claim_pool and not is_claim_topic(older.get("topic")):
            continue

        verdict = judge_pair(newer, older, backend=backend)
        if not verdict:
            continue
        results.append((older, sim, verdict))
        accepted += 1

    return newer, results


def _retry_write(conn, fn, attempts=6):
    for i in range(attempts):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            if "locked" not in str(e).lower() or i == attempts - 1:
                raise
            time.sleep(0.5 * (i + 1))


def apply_judgment(conn, newer, older, sim, verdict, do_apply):
    loser = verdict.get("loser", "neither")
    v = verdict.get("verdict", "orthogonal")
    rationale = verdict.get("rationale", "")[:400]
    now = int(time.time())

    _retry_write(conn, lambda: conn.execute(
        "INSERT INTO capsule_contradictions "
        "(newer_id, older_id, similarity, verdict, loser, rationale, applied, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (newer["id"], older["id"], sim, v, loser, rationale,
         1 if (do_apply and v in ("contradicts", "agrees")) else 0, now)))

    if not do_apply:
        return
    if v == "contradicts":
        loser_id = older["id"] if loser == "older" else (newer["id"] if loser == "newer" else None)
        if loser_id is None:
            return  # ambiguous contradiction, record only
        new_conf = max(0.0, (older["confidence"] if loser_id == older["id"]
                              else newer["confidence"]) - DEMOTION_STEP)
        _retry_write(conn, lambda: conn.execute(
            "UPDATE knowledge_capsules SET confidence_score = ? WHERE id = ?",
            (new_conf, loser_id)))
    elif v == "agrees":
        # small boost to older (it survived a re-encounter)
        new_conf = min(1.0, older["confidence"] + BOOST_STEP)
        conn.execute(
            "UPDATE knowledge_capsules SET confidence_score = ? WHERE id = ?",
            (new_conf, older["id"]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("capsule_id", nargs="?", type=int)
    ap.add_argument("--recent", type=int, help="scan N most recent capsules instead")
    ap.add_argument("--top-k", type=int, default=8)
    ap.add_argument("--apply", action="store_true",
                    help="write changes (default: dry-run)")
    ap.add_argument("--claim-pool", action="store_true",
                    help="restrict to non-feed/non-crossref/non-intern capsules on both sides")
    ap.add_argument("--backend", choices=["llama", "engine"], default="llama",
                    help="judge backend (engine = cloud Qwen via chronicle-engine)")
    args = ap.parse_args()

    conn = sqlite3.connect(DB, timeout=60.0)
    conn.execute("PRAGMA busy_timeout = 60000")
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    conn.commit()

    idx = VectorIndex(FAISS_PATH)
    print(f"[contradiction] index: {idx.count()} vectors, dim={idx.dim}")

    if args.recent:
        where = "WHERE superseded_at IS NULL"
        if args.claim_pool:
            where += " AND topic IS NOT NULL "
            for p in NOISE_TOPIC_PREFIXES:
                where += f"AND topic NOT LIKE '{p}%' "
        ids = [r[0] for r in conn.execute(
            f"SELECT id FROM knowledge_capsules {where} ORDER BY id DESC LIMIT ?",
            (args.recent,)).fetchall()]
    elif args.capsule_id:
        ids = [args.capsule_id]
    else:
        ap.error("need capsule_id or --recent N")

    totals = {"contradicts": 0, "agrees": 0, "extends": 0, "orthogonal": 0}

    for cid in ids:
        out = process_capsule(conn, idx, cid, args.top_k, args.apply,
                              claim_pool=args.claim_pool, backend=args.backend)
        if not out:
            continue
        newer, pairs = out
        if not pairs:
            continue
        print(f"\n=== capsule #{newer['id']} ({newer.get('topic') or '-'}) ===")
        print(f"  restatement: {newer['restatement'][:140]}...")
        for older, sim, verdict in pairs:
            v = verdict.get("verdict", "?")
            totals[v] = totals.get(v, 0) + 1
            print(f"  [{v}] vs #{older['id']} sim={sim:.3f} "
                  f"loser={verdict.get('loser')} — {verdict.get('rationale', '')[:120]}")
            apply_judgment(conn, newer, older, sim, verdict, args.apply)

    conn.commit()
    conn.close()
    print(f"\n[totals] {totals}")
    print("[mode]", "APPLY" if args.apply else "DRY-RUN")


if __name__ == "__main__":
    main()
