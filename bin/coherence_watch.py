#!/usr/bin/env python3
"""
coherence_watch.py — live probe for coherence-modulated gate events.

Theory (from selective_plasticity probe + #436 forensics): the gate widens
its update authority when recent signals converge on a single focus.
Empirical signature is (a) a large entity-set churn between consecutive CCS
snapshots and (b) an upstream burst of coherent activity in the preceding
window converging on a referent that survives the prune.

This script scans for the pattern in two modes:
  --backfill   look across all CCS history (default behavior, no logging)
  --watch      scan only since the last logged event, append new finds to
               the event log, and emit a one-line digest per find

Event log: ~/chronicle/data/coherence_events.jsonl
Each line: {ts, snapshot_id, drop_ratio, entities_dropped, entities_held,
            survivors, pre_window_signals, top_focus_terms, coherence_score}
"""
import json
import math
import re
import sqlite3
import sys
import unicodedata
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
LOG = Path("/home/nate-agx/chronicle/data/coherence_events.jsonl")
LOG.parent.mkdir(parents=True, exist_ok=True)

# A flush is at least this fraction of the prior entity set being dropped.
DROP_THRESHOLD = 0.40
# Look this many seconds back for converging signals.
PRE_WINDOW_SEC = 30 * 60

sys.path.insert(0, str(Path(__file__).parent))
from embed_config import EMBED_URL, EMBED_MODEL
# Cosine threshold for semantic coherence match. Calibrated 2026-04-13 across
# 10 known flush events: activity-feed text against survivor name+context
# focus tops out around 0.72, floors around 0.36. High-coherence events (#436)
# hit 40% of pre-window rows >= 0.55; low-coherence events (#438, #463) sit
# at ≤11%. 0.55 discriminates well; lower over-counts noise, higher misses
# #436 below its known-high signal level.
SEMANTIC_THRESHOLD = 0.55
# Skip embedding rows with less than this many chars — fall back to literal.
MIN_EMBED_CHARS = 20
# Stopwords for focus-term extraction.
STOP = set("the a an of for and or but to in on at by from with without is are was were "
           "be been being it its this that these those as if then so than into onto over "
           "under not no nor too very can could would should may might will shall you i we "
           "they he she them us our your their his her my me him about up down out".split())
TOKEN_RE = re.compile(r"[a-z][a-z0-9_-]{2,}")
DASH_CHARS = "\u2010\u2011\u2012\u2013\u2014\u2015\u2212"


def normalize(s: str) -> str:
    s = unicodedata.normalize("NFKC", s).lower().strip()
    for d in DASH_CHARS:
        s = s.replace(d, "-")
    return re.sub(r"\s+", " ", s)


def entity_map(ccs: dict) -> dict[str, dict]:
    out = {}
    for e in ccs.get("focal_entities") or []:
        if not isinstance(e, dict):
            continue
        n = normalize(e.get("name") or "")
        if n:
            out[n] = e
    return out


def entity_terms(entity: dict) -> set[str]:
    """all salient terms for an entity — name AND context field.
    The context field carries the topical content (e.g. 'Patient Elliot,
    Memento Problem'), which is what activity_feed text actually mentions.
    Pure name-tokens (chronicle-local.rs, hermes) miss the semantic link.
    """
    parts = []
    if entity.get("name"):
        parts.append(entity["name"])
    if entity.get("context"):
        parts.append(entity["context"])
    blob = " ".join(parts).lower()
    return {t for t in TOKEN_RE.findall(blob)
            if t not in STOP and len(t) > 3}


def topic_terms(items: list[str]) -> list[str]:
    """salient bigrams/unigrams from a list of strings"""
    bag = Counter()
    for s in items:
        toks = [t for t in TOKEN_RE.findall(s.lower()) if t not in STOP and len(t) > 3]
        for t in toks:
            bag[t] += 1
    return [w for w, _ in bag.most_common(8)]


_EMBED_CACHE: dict[str, list[float]] = {}


def embed(text: str) -> list[float] | None:
    """embed a single text via Ollama. Returns None on failure."""
    text = text.strip()
    if not text:
        return None
    if text in _EMBED_CACHE:
        return _EMBED_CACHE[text]
    payload = json.dumps({"model": EMBED_MODEL, "prompt": text[:4000]}).encode()
    req = urllib.request.Request(
        f"{EMBED_URL}/api/embeddings", data=payload, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            vec = data.get("embedding")
            if vec and isinstance(vec, list):
                _EMBED_CACHE[text] = vec
                return vec
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError):
        return None
    return None


def cosine(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def mean_vec(vecs: list[list[float]]) -> list[float] | None:
    """element-wise mean of a list of vectors."""
    vecs = [v for v in vecs if v]
    if not vecs:
        return None
    dim = len(vecs[0])
    out = [0.0] * dim
    for v in vecs:
        for i, x in enumerate(v):
            out[i] += x
    return [x / len(vecs) for x in out]


def compute_coherence(
    con,
    t_end: int,
    survivors: list[str],
    prev_em: dict[str, dict],
    survivor_terms: set[str],
):
    """count signals in [t_end - PRE_WINDOW_SEC, t_end]. Score them two ways:
      - literal_hits: rows containing any survivor term as substring
      - semantic_hits: rows whose embedding cosine-similarity to the focus
        embedding (mean of survivor name+context) exceeds SEMANTIC_THRESHOLD
    Returns (total, literal_hits, literal_score, semantic_hits, semantic_score).
    Semantic score is None if the embedding endpoint is unreachable.
    """
    t_start = t_end - PRE_WINDOW_SEC
    rows = con.execute(
        "SELECT source, activity_type, title, content "
        "FROM activity_feed WHERE created_at BETWEEN ? AND ?",
        (t_start, t_end),
    ).fetchall()
    total = len(rows)
    if total == 0:
        return total, 0, 0.0, 0, 0.0

    # Literal pass (unchanged behavior).
    literal_hits = 0
    if survivor_terms:
        for src, at, ti, c in rows:
            blob = f"{src or ''} {at or ''} {ti or ''} {c or ''}".lower()
            if any(term in blob for term in survivor_terms):
                literal_hits += 1
    literal_score = literal_hits / total if total else 0.0

    # Semantic pass. Build focus embedding from survivors' name+context.
    focus_texts = []
    for n in survivors:
        ent = prev_em.get(n, {})
        parts = [ent.get("name") or n]
        if ent.get("context"):
            parts.append(str(ent["context"]))
        focus_texts.append(" — ".join(parts))
    focus_vecs = [v for v in (embed(t) for t in focus_texts) if v]
    focus_vec = mean_vec(focus_vecs)
    if focus_vec is None:
        # embedding endpoint unreachable — return literal-only result
        return total, literal_hits, literal_score, 0, None

    semantic_hits = 0
    for src, at, ti, c in rows:
        blob = " ".join(str(x) for x in (src, at, ti, c) if x).strip()
        if len(blob) < MIN_EMBED_CHARS:
            continue
        vec = embed(blob)
        if vec is None:
            continue
        if cosine(vec, focus_vec) >= SEMANTIC_THRESHOLD:
            semantic_hits += 1
    semantic_score = semantic_hits / total if total else 0.0
    return total, literal_hits, literal_score, semantic_hits, semantic_score


def scan(since_ts: int = 0) -> list[dict]:
    """find flush-pattern events in CCS history newer than since_ts."""
    con = sqlite3.connect(DB)
    rows = con.execute(
        "SELECT id, created_at, snapshot FROM cognitive_state_history "
        "WHERE created_at > ? ORDER BY created_at ASC",
        (since_ts,),
    ).fetchall()
    if len(rows) < 2:
        con.close()
        return []

    events = []
    prev_em = None
    for rid, ts, snap_str in rows:
        try:
            ccs = json.loads(snap_str)
        except Exception:
            continue
        em = entity_map(ccs)
        if prev_em is not None and prev_em:
            held = set(prev_em) & set(em)
            dropped = set(prev_em) - set(em)
            drop_ratio = len(dropped) / len(prev_em)
            if drop_ratio >= DROP_THRESHOLD and len(dropped) >= 5:
                survivors = sorted(held)
                terms = set()
                for n in survivors:
                    terms |= entity_terms(prev_em[n])
                total, lit_hit, lit_coh, sem_hit, sem_coh = compute_coherence(
                    con, ts, survivors, prev_em, terms
                )
                events.append({
                    "ts": ts,
                    "snapshot_id": rid,
                    "drop_ratio": round(drop_ratio, 3),
                    "entities_dropped": len(dropped),
                    "entities_held": len(held),
                    "survivors": survivors[:10],
                    "pre_window_signals": total,
                    "pre_window_hits": lit_hit,
                    "coherence_score": round(lit_coh, 3),
                    "semantic_hits": sem_hit,
                    "semantic_score": round(sem_coh, 3) if sem_coh is not None else None,
                    "top_focus_terms": sorted(terms)[:10],
                })
        prev_em = em
    con.close()
    return events


def load_logged() -> set[int]:
    if not LOG.exists():
        return set()
    out = set()
    for line in LOG.read_text().splitlines():
        if not line.strip():
            continue
        try:
            out.add(json.loads(line)["snapshot_id"])
        except Exception:
            pass
    return out


def append_log(events: list[dict]):
    with LOG.open("a") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "backfill"
    if mode == "backfill":
        events = scan(since_ts=0)
        for e in events:
            print(json.dumps(e, indent=2))
        print(f"\n{len(events)} flush-pattern event(s) across all history.")
        return

    if mode == "watch":
        logged = load_logged()
        events = scan(since_ts=0)
        new = [e for e in events if e["snapshot_id"] not in logged]
        if new:
            append_log(new)
            for e in new:
                sem = e.get("semantic_score")
                sem_str = f" sem={sem}" if sem is not None else ""
                print(f"[FLUSH] snap={e['snapshot_id']} "
                      f"drop={e['drop_ratio']} "
                      f"signals={e['pre_window_hits']}/{e['pre_window_signals']} "
                      f"coherence={e['coherence_score']}{sem_str} "
                      f"survivors={e['survivors'][:3]}")
        else:
            print(f"no new flush-pattern events. {len(events)} total in log.")
        return

    print(f"unknown mode: {mode}. use backfill or watch.")
    sys.exit(2)


if __name__ == "__main__":
    main()
