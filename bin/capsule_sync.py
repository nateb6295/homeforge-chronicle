#!/usr/bin/env python3
"""Capsule Sync — Pull capsules from backend canister to local SQLite + FAISS.

Runs on a systemd timer (every 30 minutes). Pulls capsule metadata via HTTP API,
embeds restatements locally via Ollama, stores in knowledge_capsules +
capsule_embeddings tables, and adds vectors to the FAISS index.

Usage:
    python3 capsule_sync.py                  # normal sync cycle
    python3 capsule_sync.py --backfill 500   # backfill N capsules from oldest missing
    python3 capsule_sync.py --status         # show sync status
"""

import json
import os
import sqlite3
import struct
import sys
import time
from datetime import datetime

import requests

# ── Configuration ──
DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
CANISTER_URL = "https://fqqku-bqaaa-aaaai-q4wha-cai.raw.icp0.io"
TOKEN_PATH = os.path.expanduser("~/.homeforge-chronicle/.api_token")
sys.path.insert(0, os.path.dirname(__file__))
from embed_config import EMBED_URL as _EMBED_URL, EMBED_MODEL, EMBED_DIM
OLLAMA_URL = os.environ.get("CHRONICLE_OLLAMA_URL", "http://localhost:11434")
EMBED_URL_PRIMARY = _EMBED_URL
EMBED_URL_FALLBACK = "http://192.168.1.11:11434"
DATA_DIR = os.environ.get("CHRONICLE_DATA_DIR", "/mnt/hdd/chronicle-data")
FAISS_INDEX_PATH = os.path.join(DATA_DIR, "capsules.faiss")

# Sync parameters
BATCH_SIZE = 50        # capsules per HTTP request (API caps at 50)
EMBED_BATCH = 10       # embeddings per Ollama batch call
MAX_PER_CYCLE = 500    # max capsules to process per timer cycle

# Build #45: Topics that are auto-generated noise — supersede on arrival.
# chronicle/reflection: Gemma generates identical "my memory is growing" every cycle.
#   80.1% never accessed; accessed ones hit only by coincidental keyword overlap.
# chronicle/heartbeat: System pulse data, not useful as capsules.
# crossref/connection: Service stopped. Random connections (sim=0.000) that pollute search.
AUTOSUPERSEDE_TOPICS = {"chronicle/reflection", "chronicle/heartbeat", "crossref/connection"}

CENTROID_PATH = os.path.join(os.path.expanduser("~/chronicle/data"), "recognition_centroid.json")
_centroid_cache = None

def _load_centroid():
    global _centroid_cache
    if _centroid_cache is not None:
        return _centroid_cache
    try:
        import numpy as np
        with open(CENTROID_PATH) as f:
            data = json.load(f)
        _centroid_cache = (
            np.array(data["vector"], dtype=np.float32),
            data.get("built_at", 0),
        )
        return _centroid_cache
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        return None

def _score_alignment(vec_blob, confidence_score):
    """Score a capsule embedding against the recognition centroid."""
    import numpy as np
    result = _load_centroid()
    if result is None:
        return None
    centroid_vec, centroid_version = result
    emb = np.array(struct.unpack(f'{len(vec_blob)//4}f', vec_blob), dtype=np.float32)
    if len(emb) != len(centroid_vec):
        return None
    dot = np.dot(emb, centroid_vec)
    norm = np.linalg.norm(emb) * np.linalg.norm(centroid_vec)
    alignment = float(dot / norm) if norm > 0 else 0.0
    novelty = float(confidence_score) if confidence_score else 0.5
    if novelty >= 0.8 and alignment >= 0.69:
        quadrant = "INSIGHT"
    elif novelty >= 0.8 and alignment < 0.69:
        quadrant = "SEED"
    elif novelty < 0.8 and alignment >= 0.69:
        quadrant = "REDUNDANT"
    else:
        quadrant = "SEDIMENT"
    return (alignment, centroid_version, quadrant)


def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [capsule-sync] {msg}", flush=True)


def _load_token() -> str:
    try:
        with open(TOKEN_PATH) as f:
            return f.read().strip()
    except Exception:
        return ""


def _vec_to_blob(vec: list) -> bytes:
    return struct.pack(f"{len(vec)}f", *vec)


def _embed_single(text: str, url: str) -> list:
    """Embed a single text via Ollama. Returns embedding or empty list."""
    r = requests.post(
        f"{url}/api/embeddings",
        json={"model": EMBED_MODEL, "prompt": text},
        timeout=30,
    )
    if r.status_code == 200:
        emb = r.json().get("embedding", [])
        if emb and len(emb) == EMBED_DIM:
            return emb
        elif emb:
            log(f"  Dimension mismatch: got {len(emb)}, expected {EMBED_DIM} — rejecting")
    return []


def _pick_embed_url() -> str:
    """Return reachable Ollama URL, preferring Jetson."""
    for url in [EMBED_URL_PRIMARY, EMBED_URL_FALLBACK]:
        try:
            r = requests.get(f"{url}/api/tags", timeout=3)
            if r.status_code == 200:
                return url
        except Exception:
            continue
    return EMBED_URL_FALLBACK


def _embed_texts(texts: list, ollama_url: str = None) -> list:
    """Batch embed texts via nomic-embed-text.
    Tries Jetson first, falls back to AGX localhost.
    Adds 'search_document:' prefix for stored capsule embeddings."""
    url = ollama_url or _pick_embed_url()
    prefixed = texts
    try:
        results = []
        for text in prefixed:
            emb = _embed_single(text, url)
            results.append(emb)
        return results
    except Exception as e:
        log(f"  Embed error on {url}: {e}")
        if url == EMBED_URL_PRIMARY:
            log(f"  Retrying on fallback {EMBED_URL_FALLBACK}")
            return _embed_texts(texts, EMBED_URL_FALLBACK)
    return []


def _ensure_tables(db: sqlite3.Connection):
    """Ensure required tables exist."""
    db.execute("""CREATE TABLE IF NOT EXISTS knowledge_capsules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        conversation_id TEXT NOT NULL,
        restatement TEXT NOT NULL,
        timestamp TEXT,
        location TEXT,
        topic TEXT,
        confidence_score REAL NOT NULL DEFAULT 0.8,
        created_at INTEGER NOT NULL,
        consolidated_into INTEGER,
        metabolized_at INTEGER,
        memory_type TEXT DEFAULT NULL
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS capsule_embeddings (
        capsule_id INTEGER PRIMARY KEY,
        embedding BLOB NOT NULL,
        model_name TEXT NOT NULL,
        created_at INTEGER NOT NULL
    )""")
    db.commit()


def _get_state(db: sqlite3.Connection, key: str, default: str = "0") -> str:
    """Get state value from the state table."""
    try:
        row = db.execute("SELECT value FROM state WHERE key = ?", (key,)).fetchone()
        return row[0] if row else default
    except Exception:
        return default


def _set_state(db: sqlite3.Connection, key: str, value: str):
    """Set state value."""
    try:
        db.execute("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)", (key, value))
        db.commit()
    except Exception:
        pass


def pull_recent_capsules(token: str, limit: int = BATCH_SIZE) -> list:
    """Pull recent capsules from backend canister HTTP API."""
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        r = requests.get(
            f"{CANISTER_URL}/api/recent?limit={limit}",
            headers=headers,
            timeout=15,
        )
        if r.status_code == 200:
            data = r.json()
            return data.get("capsules", [])
    except Exception as e:
        log(f"  HTTP pull error: {e}")
    return []


def _fetch_capsule(token: str, cid: int) -> dict:
    """Fetch a single capsule by ID from the HTTP API."""
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        r = requests.get(
            f"{CANISTER_URL}/api/capsule?id={cid}",
            headers=headers,
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json()
            if data and data.get("id"):
                return data
    except Exception:
        pass
    return {}


def sync_cycle(db: sqlite3.Connection, token: str, max_capsules: int = MAX_PER_CYCLE):
    """Main sync: pull recent capsules, embed, store, index.

    Strategy: get remote max ID from /api/recent, compare to local max,
    fetch missing IDs individually (API caps at 50 for bulk).
    """
    _ensure_tables(db)

    # Get local max ID
    row = db.execute("SELECT MAX(id) FROM knowledge_capsules").fetchone()
    local_max_id = row[0] if row and row[0] else 0

    # Get remote max ID from recent endpoint
    capsules = pull_recent_capsules(token, limit=BATCH_SIZE)
    if not capsules:
        log("  No capsules returned from API")
        return 0

    remote_max_id = max(c.get("id", 0) for c in capsules)
    gap = remote_max_id - local_max_id

    if gap <= 0:
        log(f"  Up to date (local: {local_max_id}, remote: {remote_max_id})")
        return 0

    # Cap how many we fetch this cycle
    fetch_count = min(gap, max_capsules)
    start_id = remote_max_id - fetch_count + 1
    if local_max_id > 0:
        start_id = local_max_id + 1
        fetch_count = min(remote_max_id - local_max_id, max_capsules)

    log(f"  {fetch_count} new capsules to sync (local: {local_max_id}, remote: {remote_max_id})")

    # Build #77: Content-hash dedup — prevent duplicate feed capsules
    # Feeds sometimes post the same article to canister multiple times (different IDs).
    # Check first 80 chars of restatement against recent capsules to catch content dupes.
    recent_hashes = set()
    try:
        rows = db.execute(
            "SELECT substr(restatement, 1, 80) FROM knowledge_capsules "
            "WHERE created_at > ? AND topic LIKE 'feed/%'",
            (int(time.time()) - 7200,)  # last 2 hours
        ).fetchall()
        recent_hashes = {r[0] for r in rows if r[0]}
    except Exception:
        pass  # dedup is best-effort, don't block sync

    # Insert any capsules from the recent batch that we need
    inserted = 0
    deduped = 0
    content_deduped = 0
    for c in capsules:
        cid = c.get("id", 0)
        if cid <= local_max_id:
            continue
        topic = c.get("topic") or ""
        text = c.get("restatement") or ""
        now_ts_val = int(time.time())
        # Build #77: Content-hash dedup for feed capsules
        content_key = text[:80] if text else ""
        if topic.startswith("feed/") and content_key in recent_hashes:
            content_deduped += 1
            continue  # skip content duplicate
        # Build #45: Auto-supersede noise topics on arrival
        supersede = now_ts_val if topic in AUTOSUPERSEDE_TOPICS else None
        try:
            db.execute(
                "INSERT OR IGNORE INTO knowledge_capsules "
                "(id, conversation_id, restatement, timestamp, topic, confidence_score, created_at, superseded_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (cid, f"canister:{cid}", text,
                 c.get("timestamp", ""), topic,
                 c.get("confidence", 0.8), now_ts_val, supersede),
            )
            inserted += 1
            if supersede:
                deduped += 1
            # Track this content for intra-batch dedup
            if content_key:
                recent_hashes.add(content_key)
        except Exception as e:
            log(f"  Insert error for capsule {cid}: {e}")

    # Fetch remaining IDs individually if the recent batch didn't cover them all
    recent_ids = {c.get("id", 0) for c in capsules}
    remaining = [i for i in range(start_id, remote_max_id + 1)
                 if i not in recent_ids and i > local_max_id]

    for cid in remaining[:max_capsules - inserted - content_deduped]:
        c = _fetch_capsule(token, cid)
        if c:
            topic = c.get("topic") or ""
            text = c.get("restatement") or ""
            now_ts_val = int(time.time())
            # Build #77: Content-hash dedup for feed capsules
            content_key = text[:80] if text else ""
            if topic.startswith("feed/") and content_key in recent_hashes:
                content_deduped += 1
                continue
            # Build #45: Auto-supersede noise topics on arrival
            supersede = now_ts_val if topic in AUTOSUPERSEDE_TOPICS else None
            try:
                db.execute(
                    "INSERT OR IGNORE INTO knowledge_capsules "
                    "(id, conversation_id, restatement, timestamp, topic, confidence_score, created_at, superseded_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (c["id"], f"canister:{c['id']}", text,
                     c.get("timestamp", ""), topic,
                     c.get("confidence", 0.8), now_ts_val, supersede),
                )
                inserted += 1
                if supersede:
                    deduped += 1
                if content_key:
                    recent_hashes.add(content_key)
            except Exception as e:
                log(f"  Insert error for capsule {cid}: {e}")
        if inserted % 50 == 0 and inserted > 0:
            db.commit()
            log(f"  Progress: {inserted} capsules inserted...")

    db.commit()
    dedup_msg = f" ({deduped} noise-deduped)" if deduped else ""
    content_msg = f" ({content_deduped} content-deduped)" if content_deduped else ""
    log(f"  Inserted {inserted} capsule metadata records{dedup_msg}{content_msg}")

    # Embed in batches (skip superseded — they don't need embeddings)
    embedded = _embed_new_capsules(db)
    log(f"  Embedded {embedded} capsules")

    # Update FAISS index
    indexed = _update_faiss_index(db)
    log(f"  FAISS index updated ({indexed} total vectors)")

    # Build graph edges for newly embedded capsules
    try:
        from capsule_ingest_graph import process_unconnected
        edges, entities = process_unconnected(db, limit=embedded + 10)
        if edges > 0 or entities > 0:
            log(f"  Graph: +{edges} edges, +{entities} entities")
    except Exception as e:
        log(f"  Graph build skipped: {e}")

    return inserted


def backfill(db: sqlite3.Connection, token: str, count: int = 500):
    """Backfill capsules by fetching individual IDs we're missing.

    Uses the HTTP API /api/capsule?id=N endpoint.
    """
    _ensure_tables(db)

    # Find which IDs we're missing
    row = db.execute("SELECT MAX(id) FROM knowledge_capsules").fetchone()
    local_max = row[0] if row and row[0] else 0

    # Get total count from API
    capsules = pull_recent_capsules(token, limit=1)
    if not capsules:
        log("  Can't reach API for backfill")
        return
    canister_max = max(c.get("id", 0) for c in capsules)
    log(f"  Canister max ID: {canister_max}, local max: {local_max}")

    # Find gaps: IDs that exist on canister but not locally
    local_ids = set(
        r[0] for r in db.execute("SELECT id FROM knowledge_capsules").fetchall()
    )
    missing = [i for i in range(1, canister_max + 1) if i not in local_ids]
    log(f"  {len(missing)} missing capsules, backfilling {min(count, len(missing))}")

    headers = {"Authorization": f"Bearer {token}"} if token else {}
    filled = 0
    for cid in missing[:count]:
        try:
            r = requests.get(
                f"{CANISTER_URL}/api/capsule?id={cid}",
                headers=headers,
                timeout=10,
            )
            if r.status_code == 200:
                c = r.json()
                if c and c.get("id"):
                    db.execute(
                        "INSERT OR IGNORE INTO knowledge_capsules "
                        "(id, conversation_id, restatement, timestamp, topic, confidence_score, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (c["id"], f"canister:{c['id']}", c.get("restatement", ""),
                         c.get("timestamp", ""), c.get("topic", ""),
                         c.get("confidence", 0.8), int(time.time())),
                    )
                    filled += 1
            # Commit every 10 inserts and yield lock so pipeline isn't starved
            if filled % 10 == 0 and filled > 0:
                db.commit()
                time.sleep(0.1)  # yield DB lock for other writers
            if filled % 50 == 0 and filled > 0:
                log(f"  Backfilled {filled}/{min(count, len(missing))}...")
        except Exception as e:
            log(f"  Backfill error for {cid}: {e}")

    db.commit()
    log(f"  Backfilled {filled} capsules")

    # Embed and index
    if filled > 0:
        embedded = _embed_new_capsules(db)
        log(f"  Embedded {embedded} capsules")
        indexed = _update_faiss_index(db)
        log(f"  FAISS index updated ({indexed} total vectors)")


def _embed_new_capsules(db: sqlite3.Connection, capsules: list = None,
                        batch_size: int = EMBED_BATCH) -> int:
    """Embed capsules that don't have embeddings yet."""
    # Find unembedded capsules
    rows = db.execute(
        "SELECT kc.id, kc.restatement, kc.confidence_score FROM knowledge_capsules kc "
        "LEFT JOIN capsule_embeddings ce ON kc.id = ce.capsule_id "
        "WHERE ce.capsule_id IS NULL AND kc.restatement != '' "
        "ORDER BY kc.id DESC LIMIT ?",
        (MAX_PER_CYCLE,)
    ).fetchall()

    if not rows:
        return 0

    embedded = 0
    aligned = 0
    now = int(time.time())

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        texts = [r[1][:500] for r in batch]  # truncate to 500 chars for embedding
        ids = [r[0] for r in batch]
        conf_scores = [r[2] for r in batch]

        vecs = _embed_texts(texts)
        if not vecs or len(vecs) != len(batch):
            log(f"  Embedding batch failed (got {len(vecs) if vecs else 0}, expected {len(batch)})")
            continue

        for cid, vec, conf in zip(ids, vecs, conf_scores):
            if not vec or len(vec) < 256:  # nomic-embed-text = 768 dims
                continue
            blob = _vec_to_blob(vec)
            db.execute(
                "INSERT OR REPLACE INTO capsule_embeddings (capsule_id, embedding, model_name, created_at) "
                "VALUES (?, ?, ?, ?)",
                (cid, blob, EMBED_MODEL, now),
            )
            embedded += 1

            score = _score_alignment(blob, conf)
            if score:
                alignment, centroid_ver, quadrant = score
                db.execute(
                    "INSERT OR REPLACE INTO capsule_alignment "
                    "(capsule_id, alignment_score, centroid_version, quadrant, scored_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (cid, alignment, centroid_ver, quadrant, now),
                )
                aligned += 1

        db.commit()
        time.sleep(0.1)  # yield DB lock between embed batches

    if aligned:
        log(f"  Alignment scored: {aligned} capsules against centroid")
    return embedded


def _update_faiss_index(db: sqlite3.Connection) -> int:
    """Rebuild FAISS index from all capsule embeddings."""
    try:
        from vector_index import VectorIndex

        idx = VectorIndex(FAISS_INDEX_PATH)

        # Get all embeddings not yet in the FAISS index
        rows = db.execute(
            "SELECT capsule_id, embedding FROM capsule_embeddings"
        ).fetchall()

        new_ids = []
        new_vecs = []
        for r in rows:
            cid = r[0]
            if idx.contains(cid):
                continue
            vec = list(struct.unpack(f"{len(r[1])//4}f", r[1]))
            new_ids.append(cid)
            new_vecs.append(vec)

        if new_ids:
            idx.add(new_ids, new_vecs)
            idx.save()

        return idx.count()
    except Exception as e:
        log(f"  FAISS index error: {e}")
        return 0


def show_status(db: sqlite3.Connection):
    """Show sync status."""
    _ensure_tables(db)
    cap_count = db.execute("SELECT COUNT(*) FROM knowledge_capsules").fetchone()[0]
    emb_count = db.execute("SELECT COUNT(*) FROM capsule_embeddings").fetchone()[0]
    max_id = db.execute("SELECT MAX(id) FROM knowledge_capsules").fetchone()[0] or 0

    try:
        from vector_index import VectorIndex
        idx = VectorIndex(FAISS_INDEX_PATH)
        faiss_count = idx.count()
    except Exception:
        faiss_count = 0

    print(f"Capsule Sync Status:")
    print(f"  Local capsules:  {cap_count}")
    print(f"  Local embeddings: {emb_count}")
    print(f"  FAISS indexed:   {faiss_count}")
    print(f"  Max local ID:    {max_id}")


def main():
    args = sys.argv[1:]
    token = _load_token()
    db = sqlite3.connect(DB_PATH, timeout=10)

    if "--status" in args:
        show_status(db)
        db.close()
        return

    if "--backfill" in args:
        count = 500
        idx = args.index("--backfill")
        if idx + 1 < len(args):
            try:
                count = int(args[idx + 1])
            except ValueError:
                pass
        log(f"Starting backfill ({count} capsules)...")
        backfill(db, token, count)
        db.close()
        return

    # Mesh — join the nervous system
    try:
        from chronicle_mesh import Mesh
        mesh = Mesh("capsule_sync", db_path=DB_PATH)
        mesh.depends_on("feeds")  # syncs what feeds produces
    except Exception:
        pass

    # Normal sync cycle
    log("Starting capsule sync...")
    synced = sync_cycle(db, token)

    # Log to activity_feed
    try:
        db.execute(
            "INSERT INTO activity_feed (source, activity_type, content, created_at) "
            "VALUES (?, ?, ?, ?)",
            ("capsule-sync", "sync",
             f"Synced {synced} capsules from canister",
             int(time.time())),
        )
        db.commit()
    except Exception:
        pass

    db.close()
    log(f"Done. {synced} capsules synced.")


if __name__ == "__main__":
    main()
