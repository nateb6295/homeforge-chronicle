#!/usr/bin/env python3
"""Cross-Reference Agent — Connection finder for Homeforge.

v3 (Mar 13 2026): Three-channel connection finding.
  Channel 1 (topical): Cosine similarity on raw embeddings — catches
    same-concept connections across similar domains.
  Channel 2 (structural): Extract a 1-sentence structural pattern from
    each capsule, embed the pattern, cosine similarity on patterns —
    catches shared mechanisms across different domains.
  Channel 3 (serendipity): Random pairs — catches connections no
    embedding similarity would surface.

All channels feed into the same LLM validation pipeline.

Runs on AGX (192.168.1.70). Wakes every 30 minutes.
"""

import os, sys, time, json, math, signal, sqlite3, struct, re, random
from datetime import datetime
from typing import Optional, List, Tuple
from collections import defaultdict
import numpy as np
from kg_utils import touch_relationships_bulk, decay_unused_relationships
from chronicle_mesh import Mesh
from agent_voice import Voice

import requests
import subprocess

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)
OLLAMA_URL = os.environ.get("CROSSREF_OLLAMA_URL", "http://localhost:11436")  # Routes through engine for Groq
EMBED_URL = os.environ.get("EMBED_OLLAMA_URL", "http://192.168.1.11:11434")  # Jetson — dedicated embeddings
EMBED_MODEL = "nomic-embed-text"  # Build #125
PATTERN_MODEL = os.environ.get("CROSSREF_PATTERN_MODEL", "chronicle-challenger")  # Ada (GPT-OSS 120B via Groq)
CONNECTION_MODEL = os.environ.get("CROSSREF_MODEL", "chronicle-challenger")  # deep 32B for connection description
CYCLE_INTERVAL = int(os.environ.get("CROSSREF_INTERVAL", "1800"))  # 30 min
LOOKBACK_HOURS = int(os.environ.get("CROSSREF_LOOKBACK", "48"))
MIN_SIMILARITY = float(os.environ.get("CROSSREF_MIN_SIM", "0.55"))
MAX_SIMILARITY = float(os.environ.get("CROSSREF_MAX_SIM", "0.80"))
PATTERN_MIN_SIM = float(os.environ.get("CROSSREF_PATTERN_SIM", "0.50"))
PATTERN_MAX_SIM = float(os.environ.get("CROSSREF_PATTERN_MAX_SIM", "0.85"))

# Per-channel limits — how many candidates each channel sends to LLM validation
# Rebalanced Mar 19: data shows 63% of accepted connections are random despite
# fewer slots.  Structural at high similarity triggers template vocabulary →
# filters reject.  Random forces genuine bridging.  Net effect: fewer wasted
# Groq calls, same or better output quality.  Revert: STRUCTURAL=5, RANDOM=2.
# Rebalanced Mod #93: topical/structural dominated by same-story duplicates.
# Random produced 63% of accepted connections with only 1 slot.
# Give random 3 slots, reduce topical/structural.
TOPICAL_LIMIT = 3
STRUCTURAL_LIMIT = 3
RANDOM_LIMIT = 3
MAX_CONNECTIONS_PER_CYCLE = 7  # total stored connections cap
MAX_LIFETIME_CONNECTIONS = 15  # hard cap: no capsule gets more than 6 total connections ever

CAPSULE_SKIP_PATTERNS = [
    "Chronicle observes",
    "Sentinel:", "sentinel:",
    "Memory consolidation",
    "Feed pipeline",
    "**Reflection:**",
    "The cohesive memory",
    "The growth in knowledge",
    "memory landscape reflects",
    "capsules and embeddings",
    "crossref graph", "crossref connection", "[crossref/",
    "hub topology", "topic: chronicle",
    "hub velocity", "mechanism hub",
    "promo codes", "discount codes", "coupon codes",
    "deals: save", "% off in march",
    "How to Watch", "how to watch",
    "Things to Do", "things to do",
    "Best of 20",
    # System operations capsules (CCS/session summaries)
    "significance router", "significance filter",
    "embed calls", "skip_sources", "routing_log",
    "groq verdict", "canister:capsule deep cap",
    # Infrastructure capsules (system config, deployment notes)
    "(topic: infrastructure",
    # Entertainment/fiction content (generic mechanisms from narrative structure)
    "(topic: feed/tor)", "(topic: feed/clarkesworld)",
    "clarkesworld magazine",
    "trailer knows exactly", "cast adds",
    "season finale", "episode recap",
    # System-generated synthesis (Nostr posts, opus traces) — raw material only
    "(topic: nostr", "topic: nostr",
    "(from: opus", "(from: claude",
    "npub1",  # Nostr pubkey prefix in capsule content
    # Crossref own output leaking back into pool (401 capsules)
    "[Crossref/", "[Crossref]", "crossref/topical", "crossref/structural", "crossref/random",
    # Intern briefs that were posted as capsules before Mar 13 fix
    "(topic: intern", "topic: intern/research",
    # Seed boilerplate from before think-prompt rewrite
    "This observation highlights", "This observation is noteworthy",
    "This observation underscores",
    # Vocabulary attractor terms from contamination era
    "threshold-driven", "irreversible commitment boundary",
    # Meta-capsules: internal thread findings, self-model, system observations
    # These produce self-referential connections when paired with external papers
    "thread #", "self-model #",
    "embedding similarity test",
    "nate's core relationship",
    "nate wants all conversations",
    "content captures consistently",
]


# ── Stem (upward flow to canister/Keeper) ──
CANISTER_ID = os.environ.get("CANISTER_ID", "fqqku-bqaaa-aaaai-q4wha-cai")
DFX_BIN = os.path.expanduser("~/.local/share/dfx/bin/dfx")

try:
    from stem import Stem
    stem = Stem(CANISTER_ID, DFX_BIN, "chronicle-auto")
except Exception as _stem_err:
    log(f"Stem init failed: {_stem_err}")
    stem = None

mesh = None  # Initialized in main()

MIN_CAPSULE_LENGTH = 250

HOST_LEAK_TERMS = [
    "chronicle", "homeforge", "memory metabolism",
    "capsule", "knowledge graph", "knowledge capsule",
    "pattern metabolism", "seed system", "crossref",
]

# Mechanism-term frequency damper — prevents vocabulary attractors
MAX_MECHANISM_PCT = 10  # max N of last 100 connections can share a mechanism signature
MECHANISM_LOOKBACK = 100  # how many recent connections to check
MECHANISM_OVERLAP_THRESHOLD = 0.50  # word overlap to count as "same mechanism"

# ═══════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════

def now_ts() -> int:
    return int(time.time())

def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(msg: str):
    print(f"[{now_iso()}] {msg}", flush=True)

def safe_truncate(s: str, n: int) -> str:
    return s if len(s) <= n else s[:n] + "..."

# ═══════════════════════════════════════════════════════════════════
#  Database
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  Cognitive Thread Helpers
# ═══════════════════════════════════════════════════════════════════

_thread_vec_cache = {"vec": None, "thread_id": None}

def _load_active_thread_raw():
    """Load the primary active cognitive thread directly from DB."""
    import sqlite3 as _sq
    try:
        conn = _sq.connect(DB_PATH, timeout=10)
        conn.row_factory = _sq.Row
        row = conn.execute(
            "SELECT id, title, question, context FROM cognitive_threads "
            "WHERE status='active' ORDER BY priority LIMIT 1"
        ).fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception:
        return None

def _get_thread_vec():
    """Get embedded thread question vector, cached per thread."""
    thread = _load_active_thread_raw()
    if not thread:
        _thread_vec_cache["vec"] = None
        _thread_vec_cache["thread_id"] = None
        return None
    if thread["id"] == _thread_vec_cache["thread_id"]:
        return _thread_vec_cache["vec"]
    vec = embed_text(thread["question"])
    _thread_vec_cache["vec"] = vec
    _thread_vec_cache["thread_id"] = thread["id"]
    if vec:
        log(f"  Thread vector cached: {thread['title']}")
    return vec

def _read_and_ack_feedback_raw(agent_name):
    """Read voice responses from the family — conversation, not conditioning."""
    try:
        import sys as _sys
        _sys.path.insert(0, "/home/nate-agx/chronicle/bin")
        from agent_voice import Voice
        import sqlite3 as _sq
        conn = _sq.connect(DB_PATH, timeout=10)
        conn.row_factory = _sq.Row
        v = Voice(conn, "crossref")
        responses = v.check_responses()
        conn.close()
        return [{"id": r.get("id", 0), "feedback_type": "conversation",
                 "content": r.get("response", "")} for r in responses if r.get("response")]
    except Exception:
        return []

class DB:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self._migrate()

    def _migrate(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS crossref_connections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brief_a_id INTEGER NOT NULL,
                brief_b_id INTEGER NOT NULL,
                similarity REAL NOT NULL,
                connection_text TEXT,
                surfaced INTEGER DEFAULT 0,
                created_at INTEGER NOT NULL,
                UNIQUE(brief_a_id, brief_b_id)
            );
            CREATE TABLE IF NOT EXISTS intern_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS crossref_patterns (
                observation_id INTEGER PRIMARY KEY,
                pattern TEXT NOT NULL,
                embedding BLOB,
                created_at INTEGER NOT NULL
            );
        """)
        self.conn.commit()

    def query(self, sql: str, params: tuple = ()) -> list:
        try:
            cur = self.conn.cursor()
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            log(f"  DB query error: {e}")
            return []

    def query_one(self, sql: str, params: tuple = ()) -> Optional[dict]:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def run(self, sql: str, params: tuple = ()) -> int:
        try:
            cur = self.conn.cursor()
            cur.execute(sql, params)
            self.conn.commit()
            return cur.lastrowid
        except Exception as e:
            log(f"  DB write error: {e}")
            return 0

    def log_activity(self, atype: str, title: str, content: str, metadata: str = None):
        self.run(
            "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("crossref", atype, safe_truncate(title, 200), safe_truncate(content, 2000), metadata, now_ts()),
        )

    def close(self):
        self.conn.close()

# ═══════════════════════════════════════════════════════════════════
#  Embedding
# ═══════════════════════════════════════════════════════════════════

def embed_text(text: str, query_mode: bool = False) -> Optional[List[float]]:
    prefix = "search_query: " if query_mode else "search_document: "
    try:
        r = requests.post(
            f"{EMBED_URL}/api/embeddings",
            json={"model": EMBED_MODEL, "prompt": prefix + safe_truncate(text, 500)},
            timeout=15,
        )
        if r.status_code == 200:
            emb = r.json().get("embedding")
            if emb:
                return emb
    except Exception as e:
        log(f"  Embed error: {e}")
    return None

def decode_embedding(blob: bytes) -> Optional[List[float]]:
    if not blob:
        return None
    try:
        n = len(blob) // 4
        return list(struct.unpack(f'{n}f', blob))
    except Exception:
        return None

def vec_to_blob(vec: List[float]) -> bytes:
    return struct.pack(f"{len(vec)}f", *vec)

def cosine_sim(a: List[float], b: List[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)

# ═══════════════════════════════════════════════════════════════════
#  Capsule Loading
# ═══════════════════════════════════════════════════════════════════

def _extract_title(content: str) -> str:
    """Extract article title from capsule/brief content for dedup comparison."""
    text = re.sub(r'^\[capsule:\d+\]\s*', '', content)
    text = re.sub(r'^Research brief:\s*', '', text)
    for delim in ['Article URL:', 'Comments URL:', 'http', '\n', ' — ', '—', '---']:
        idx = text.find(delim)
        if idx > 10:
            text = text[:idx]
            break
    # Handle titles that run directly into summary (e.g. "QualityThe quality of...")
    # Split on camelCase boundary: lowercase immediately followed by uppercase
    m = re.search(r'[a-z][A-Z]', text)
    if m and m.start() > 10:
        text = text[:m.start() + 1]
    return text.strip()[:150]


def _title_overlap(title_a: str, title_b: str) -> float:
    """Word overlap ratio between two titles. 1.0 = identical words."""
    words_a = set(re.findall(r'\w{4,}', title_a.lower()))
    words_b = set(re.findall(r'\w{4,}', title_b.lower()))
    if not words_a or not words_b:
        return 0.0
    return len(words_a & words_b) / min(len(words_a), len(words_b))

TITLE_DEDUP_THRESHOLD = 0.7  # Skip pairs with >70% title word overlap

# ── Same-event detection (Mod #93) ──
# When breaking news dominates the feed, the same story arrives from multiple outlets.
# These pairs have high embedding similarity but no non-obvious connection to find.
# Detect them before wasting LLM slots.
_ENTITY_NOISE = frozenset({
    'The', 'This', 'These', 'That', 'Research', 'However', 'While', 'Although',
    'Notably', 'Furthermore', 'Moreover', 'Article', 'According', 'Many', 'Some',
    'Most', 'Several', 'Even', 'Both', 'Each', 'After', 'Before', 'When', 'Where',
    'What', 'Which', 'Such', 'Other', 'Any', 'New', 'First', 'Last', 'One', 'Two',
    'Three', 'Key', 'Top', 'More', 'Just', 'Still', 'Also', 'For', 'But', 'Yet',
})

def _extract_proper_nouns(text: str) -> set:
    """Extract proper nouns from text — both multi-word and single-word.
    Multi-word: consecutive capitalized words (e.g., 'Donald Trump', 'United States')
    Single-word: capitalized words in mid-sentence position (after lowercase/punctuation).
    Filters common sentence-initial words that aren't proper nouns."""
    multi = set(re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+\b', text))
    single = set(re.findall(r'(?<=[a-z,;:\)\'"\u2019\u201D\-] )[A-Z][a-z]{2,}', text))
    return (multi | single) - _ENTITY_NOISE

def _is_same_event(capsule_a: dict, capsule_b: dict) -> bool:
    """Detect if two capsules describe the same underlying news event.
    Uses proper noun overlap — shared names/organizations indicate
    coverage of the same event from different outlets.
    Returns True if the pair should be skipped (same event, no novel connection possible).
    """
    content_a = capsule_a.get("content", "")
    content_b = capsule_b.get("content", "")

    # Extract proper nouns (multi-word + mid-sentence single-word)
    ents_a = _extract_proper_nouns(content_a)
    ents_b = _extract_proper_nouns(content_b)

    if len(ents_a) >= 2 and len(ents_b) >= 2:
        shared = ents_a & ents_b
        overlap = len(shared) / min(len(ents_a), len(ents_b))
        if overlap > 0.50:
            return True

    # Fallback: content word overlap for cases with few proper nouns
    # Use longer words (6+ chars) to avoid function-word noise
    words_a = set(re.findall(r'\b\w{6,}\b', content_a.lower()))
    words_b = set(re.findall(r'\b\w{6,}\b', content_b.lower()))
    if len(words_a) >= 10 and len(words_b) >= 10:
        word_overlap = len(words_a & words_b) / min(len(words_a), len(words_b))
        if word_overlap > 0.50:
            return True

    return False

def get_recent_capsules(db: DB) -> list:
    cutoff = now_ts() - (LOOKBACK_HOURS * 3600)
    # Build #129: Query knowledge_capsules + capsule_embeddings instead of seed_observations.
    # Intern briefs never reach seed_observations (gemma skips intern source to prevent feedback loops),
    # so crossref was starved. knowledge_capsules is the canonical store with 19K+ embedded capsules.
    rows = db.query(
        "SELECT kc.id, kc.restatement AS content, ce.embedding, kc.created_at AS timestamp "
        "FROM knowledge_capsules kc "
        "JOIN capsule_embeddings ce ON kc.id = ce.capsule_id "
        "WHERE kc.metabolized_at IS NULL "
        "AND kc.consolidated_into IS NULL "
        "AND kc.created_at > ? "
        "ORDER BY kc.id DESC LIMIT 2000",
        (cutoff,),
    )
    # HOST_LEAK_TERMS to also check on capsule content (strip [capsule:XXXX] prefix)
    capsule_host_terms = [t for t in HOST_LEAK_TERMS if t != "capsule"]
    # First pass: collect all valid candidates
    candidates = []
    for r in rows:
        content = r["content"]
        if any(p in content.lower() for p in CAPSULE_SKIP_PATTERNS):
            continue
        # Strip metadata: [capsule:XXXX], [capture:XXXX], (topic: ...), (from: ...)
        body = re.sub(r'\[capsule:\d+\]\s*', '', content)
        body = re.sub(r'\[capture:[a-f0-9]+\]\s*', '', body)
        body = re.sub(r'\(topic:\s*[^)]+\)\s*', '', body)
        body = re.sub(r'\(from:\s*[^)]+\)\s*', '', body)
        body_stripped = body.strip()
        if len(body_stripped) < MIN_CAPSULE_LENGTH:
            continue
        if any(t in body_stripped.lower() for t in capsule_host_terms):
            continue
        # knowledge_capsules: id IS the capsule ID directly (no [capsule:XXXX] prefix needed)
        cap_id = str(r["id"])
        candidates.append({
            "id": r["id"],
            "cap_id": cap_id,
            "title": _extract_title(content),
            "content": content,
            "created_at": r["timestamp"],
            "embedding": r["embedding"],
        })
    # Second pass: deduplicate by capsule ID, keeping the LOWEST observation ID
    # (lowest ID = oldest = most likely to already have graph connections)
    capsules = []
    seen_capsule_ids = {}  # cap_id -> index in capsules list
    deduped = 0
    for c in sorted(candidates, key=lambda x: x["id"]):
        cap_id = c["cap_id"]
        if cap_id and cap_id in seen_capsule_ids:
            deduped += 1
            continue  # skip duplicate, keep the earlier observation
        entry = {k: v for k, v in c.items() if k != "cap_id"}
        capsules.append(entry)
        if cap_id:
            seen_capsule_ids[cap_id] = len(capsules) - 1
    if deduped > 0:
        log(f"  Deduped {deduped} duplicate capsule observations")
    return capsules

def get_existing_connections(db: DB) -> set:
    rows = db.query("SELECT brief_a_id, brief_b_id FROM crossref_connections")
    pairs = set()
    for r in rows:
        a, b = min(r["brief_a_id"], r["brief_b_id"]), max(r["brief_a_id"], r["brief_b_id"])
        pairs.add((a, b))
    return pairs

def _hub_counts(db: DB) -> dict:
    rows = db.query("SELECT brief_a_id, brief_b_id FROM crossref_connections")
    counts = defaultdict(int)
    for r in rows:
        counts[r["brief_a_id"]] += 1
        counts[r["brief_b_id"]] += 1
    return counts

def _hub_penalty(count: int) -> float:
    if count == 0:
        return 1.0
    return max(0.3, 1.0 - count * 0.15)


_MECH_STOP = {"the", "a", "an", "in", "of", "to", "and", "is", "are", "for",
              "by", "on", "with", "that", "this", "from", "as", "at", "it",
              "or", "be", "was", "were", "both", "where", "which", "how",
              "can", "its", "not", "but", "all", "has", "have", "into",
              "through", "than", "more", "each", "any", "whether", "when",
              "their", "between", "within", "also", "such", "these", "those"}

def _mechanism_words(bold_text: str) -> set:
    """Extract normalized significant words from a bold mechanism phrase."""
    words = set()
    for w in bold_text.lower().split():
        if '-' in w:
            words.update(w.split('-'))
        else:
            words.add(w)
    return {w for w in words if w not in _MECH_STOP and len(w) > 2}


def _extract_mechanism(connection_text: str) -> str:
    """Extract the mechanism phrase from a connection description.

    Tries bold extraction first, then falls back to sentence-pattern matching
    for cases where the LLM names the mechanism in plain text.
    """
    text = connection_text or ''
    # Try bold extraction first (most reliable when present)
    m = re.search(r'\*\*(.+?)\*\*', text)
    if m:
        return m.group(1)
    # Fallback: extract from common LLM sentence patterns
    # "The [shared/structural] mechanism [shared/is] X" or "concept of X"
    for pat in [
        r'(?:shared |structural )?mechanism (?:shared )?(?:is |centered on )(?:an? )?(?:\*\*)?([^.*,\n]{5,60})',
        r'concept of (?:an? )?([^.*,\n]{5,60}?)(?:\s+appears|\s+in\s+both)',
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(1).strip().rstrip('—')
    return ''


def _load_mechanism_signatures(db: DB) -> list:
    """Load mechanism word-sets from recent connections for frequency checking."""
    rows = db.query(
        "SELECT connection_text FROM crossref_connections "
        "ORDER BY created_at DESC LIMIT ?",
        (MECHANISM_LOOKBACK,),
    )
    sigs = []
    for r in rows:
        mech = _extract_mechanism(r['connection_text'])
        if mech:
            words = _mechanism_words(mech)
            if words:
                sigs.append(words)
    return sigs


def _mechanism_overrepresented(new_words: set, recent_sigs: list) -> int:
    """Count how many recent mechanism signatures overlap with new_words."""
    count = 0
    for sig in recent_sigs:
        overlap = len(new_words & sig) / min(len(new_words), len(sig))
        if overlap >= MECHANISM_OVERLAP_THRESHOLD:
            count += 1
    return count


# ═══════════════════════════════════════════════════════════════════
#  Pattern Extraction (Channel 2)
# ═══════════════════════════════════════════════════════════════════

def extract_pattern(content: str) -> Optional[str]:
    """Extract a 1-sentence structural pattern from capsule content.

    The pattern describes WHAT MECHANISM is at work, not what the
    content is about. "One entity diverges from cohort, signaling
    asymmetric resource access" not "Article about Bitcoin price."
    """
    text = re.sub(r'^\[capsule:\d+\]\s*', '', content)
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": PATTERN_MODEL,
                "messages": [
                    {"role": "system", "content":
                        "Extract the structural pattern from this text in ONE sentence. "
                        "Name the MECHANISM at work using vocabulary FROM the text. "
                        "What dynamic, process, or structural relationship is operating? "
                        "Bad: 'Article about Bitcoin price movements.' (topic, not mechanism) "
                        "Bad: 'Irreversible commitment boundary separates X from Y.' (generic template) "
                        "A good pattern names a SPECIFIC mechanism using the source material's own terms. "
                        "Reply with ONLY the pattern sentence. No preamble."},
                    {"role": "user", "content": safe_truncate(text, 400)},
                ],
                "stream": False,
                "options": {"num_predict": 256, "temperature": 0.3, "num_ctx": 4096},
            },
            timeout=180,
        )
        if r.status_code == 200:
            pattern = r.json().get("message", {}).get("content", "").strip()
            if pattern and len(pattern) > 10:
                return pattern[:200]
    except Exception as e:
        log(f"  Pattern extraction error: {e}")
    return None

def get_or_create_patterns(db: DB, capsules: list) -> dict:
    """Get cached patterns or extract new ones. Returns {obs_id: (pattern, embedding)}."""
    patterns = {}
    to_extract = []

    for c in capsules:
        row = db.query_one(
            "SELECT pattern, embedding FROM crossref_patterns WHERE observation_id = ?",
            (c["id"],),
        )
        if row and row["pattern"]:
            vec = decode_embedding(row["embedding"]) if row["embedding"] else None
            patterns[c["id"]] = (row["pattern"], vec)
        else:
            to_extract.append(c)

    if to_extract:
        log(f"  Extracting patterns for {len(to_extract)} new capsules...")
        for c in to_extract:
            pattern = extract_pattern(c["content"])
            if pattern:
                vec = embed_text(pattern)
                blob = vec_to_blob(vec) if vec else None
                db.run(
                    "INSERT OR REPLACE INTO crossref_patterns "
                    "(observation_id, pattern, embedding, created_at) VALUES (?, ?, ?, ?)",
                    (c["id"], pattern, blob, now_ts()),
                )
                patterns[c["id"]] = (pattern, vec)

    cached = len(patterns) - len(to_extract)
    if to_extract:
        log(f"  Patterns: {cached} cached, {len(to_extract)} extracted, {len(patterns)} total")
    return patterns

# ═══════════════════════════════════════════════════════════════════
#  Channel 1: Topical Similarity (existing behavior)
# ═══════════════════════════════════════════════════════════════════

def find_topical_candidates(embedded: list, existing: set, hub_counts: dict) -> list:
    """Cosine similarity on raw embeddings — same-concept, different domain.
    Uses numpy batch computation to avoid O(n²) Python loop.
    """
    if len(embedded) < 2:
        return []

    # Pre-extract topics and titles
    topics = []
    for c, _ in embedded:
        m = re.search(r'\(topic:\s*([^)]+)\)', c["content"])
        topics.append(m.group(1).strip() if m else None)

    # Batch cosine similarity with numpy
    vecs = np.array([vec for _, vec in embedded], dtype=np.float32)
    norms = np.linalg.norm(vecs, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    normalized = vecs / norms

    # Thread relevance (compute once if thread active)
    thread_vec = _thread_vec_cache.get("vec")
    thread_bonuses = None
    if thread_vec:
        tv = np.array(thread_vec, dtype=np.float32)
        tv_norm = np.linalg.norm(tv)
        if tv_norm > 0:
            thread_bonuses = (normalized @ tv) / tv_norm  # per-capsule thread relevance

    n = len(embedded)
    CHUNK = 500
    candidates = []

    for chunk_start in range(0, n, CHUNK):
        chunk_end = min(chunk_start + CHUNK, n)
        chunk_sims = normalized[chunk_start:chunk_end] @ normalized.T
        for i_local in range(chunk_end - chunk_start):
            i = chunk_start + i_local
            for j in range(i + 1, n):
                sim = float(chunk_sims[i_local, j])
                if sim < MIN_SIMILARITY or sim > MAX_SIMILARITY:
                    continue
                c_a, _ = embedded[i]
                c_b, _ = embedded[j]
                pair_key = (min(c_a["id"], c_b["id"]), max(c_a["id"], c_b["id"]))
                if pair_key in existing:
                    continue
                # Same-topic filter
                if topics[i] and topics[j] and topics[i] == topics[j]:
                    continue
                # Title dedup
                overlap = _title_overlap(c_a["title"], c_b["title"])
                if overlap > TITLE_DEDUP_THRESHOLD:
                    continue
                interest = sim * (1.0 - overlap * 0.5)
                interest *= _hub_penalty(hub_counts.get(c_a["id"], 0))
                interest *= _hub_penalty(hub_counts.get(c_b["id"], 0))
                if thread_bonuses is not None:
                    interest += max(float(thread_bonuses[i]), float(thread_bonuses[j])) * 0.15
                candidates.append({
                    "capsule_a": c_a, "capsule_b": c_b,
                    "similarity": sim, "interest": interest,
                    "channel": "topical",
                })

    candidates.sort(key=lambda c: c["interest"], reverse=True)
    return candidates[:TOPICAL_LIMIT]

# ═══════════════════════════════════════════════════════════════════
#  Channel 2: Structural Pattern Similarity
# ═══════════════════════════════════════════════════════════════════

def find_structural_candidates(capsules: list, patterns: dict, existing: set, hub_counts: dict, cluster_themes: list = None) -> list:
    """Cosine similarity on pattern embeddings — shared mechanisms across domains.
    Uses numpy batch computation to avoid O(n²) Python loop on pairwise cosine similarity.
    """
    # Build list of capsules that have pattern embeddings
    patterned = []
    for c in capsules:
        entry = patterns.get(c["id"])
        if entry and entry[1]:  # has pattern and embedding
            patterned.append((c, entry[0], entry[1]))  # (capsule, pattern_text, pattern_vec)

    if len(patterned) < 2:
        return []

    # Pre-extract topics for same-topic filtering
    topics = []
    for c, pat, vec in patterned:
        m = re.search(r'\(topic:\s*([^)]+)\)', c["content"])
        topics.append(m.group(1).strip() if m else None)

    # Pre-extract titles for dedup
    titles = [_extract_title(c["content"]) for c, _, _ in patterned]

    # Batch cosine similarity with numpy (Mod #94 optimization)
    vecs = np.array([vec for _, _, vec in patterned], dtype=np.float32)
    norms = np.linalg.norm(vecs, axis=1, keepdims=True)
    norms[norms == 0] = 1.0  # avoid division by zero
    normalized = vecs / norms
    # Only compute upper triangle — we only need i < j pairs
    # For memory efficiency with large pools, use chunked computation
    n = len(patterned)
    CHUNK = 500  # process rows in chunks to limit memory
    candidates = []

    for chunk_start in range(0, n, CHUNK):
        chunk_end = min(chunk_start + CHUNK, n)
        # Compute similarities: chunk_rows × all_rows
        chunk_sims = normalized[chunk_start:chunk_end] @ normalized.T
        for i_local in range(chunk_end - chunk_start):
            i = chunk_start + i_local
            # Only look at j > i to avoid duplicates
            j_start = max(i + 1, 0)
            for j in range(j_start, n):
                sim = float(chunk_sims[i_local, j])
                if sim < PATTERN_MIN_SIM or sim > PATTERN_MAX_SIM:
                    continue
                c_a, pat_a, _ = patterned[i]
                c_b, pat_b, _ = patterned[j]
                pair_key = (min(c_a["id"], c_b["id"]), max(c_a["id"], c_b["id"]))
                if pair_key in existing:
                    continue
                # Dedup: skip self-matches
                if _title_overlap(titles[i], titles[j]) > TITLE_DEDUP_THRESHOLD:
                    continue
                # Cross-domain: penalize same-topic
                if topics[i] and topics[j] and topics[i] == topics[j]:
                    continue

                interest = sim
                interest *= _hub_penalty(hub_counts.get(c_a["id"], 0))
                interest *= _hub_penalty(hub_counts.get(c_b["id"], 0))

                # Keeper cluster boost
                if cluster_themes and pat_a and pat_b:
                    pa_lower = pat_a.lower()
                    pb_lower = pat_b.lower()
                    a_clusters = [t for t in cluster_themes if any(w in pa_lower for w in t.lower().split(", ")[:2])]
                    b_clusters = [t for t in cluster_themes if any(w in pb_lower for w in t.lower().split(", ")[:2])]
                    if a_clusters and b_clusters and a_clusters[0] != b_clusters[0]:
                        interest *= 1.3

                candidates.append({
                    "capsule_a": c_a, "capsule_b": c_b,
                    "similarity": sim, "interest": interest,
                    "channel": "structural",
                    "pattern_a": pat_a, "pattern_b": pat_b,
                })

    candidates.sort(key=lambda c: c["interest"], reverse=True)
    return candidates[:STRUCTURAL_LIMIT]

# ═══════════════════════════════════════════════════════════════════
#  Channel 3: Random Serendipity
# ═══════════════════════════════════════════════════════════════════

def find_random_candidates(capsules: list, existing: set) -> list:
    """Random pairs — pure serendipity, no embedding bias."""
    if len(capsules) < 2:
        return []
    candidates = []
    attempts = 0
    while len(candidates) < RANDOM_LIMIT and attempts < RANDOM_LIMIT * 5:
        attempts += 1
        a, b = random.sample(capsules, 2)
        pair_key = (min(a["id"], b["id"]), max(a["id"], b["id"]))
        if pair_key in existing:
            continue
        # Skip if already picked
        if any(c["capsule_a"]["id"] == a["id"] and c["capsule_b"]["id"] == b["id"] for c in candidates):
            continue
        candidates.append({
            "capsule_a": a, "capsule_b": b,
            "similarity": 0.0,  # no similarity score for random
            "interest": 0.5,    # neutral interest
            "channel": "random",
        })
    return candidates

# ═══════════════════════════════════════════════════════════════════
#  LLM Validation (shared across all channels)
# ═══════════════════════════════════════════════════════════════════

def get_recent_mechanisms(db: DB, limit: int = 15) -> List[str]:
    """Extract mechanism names from recent crossref connections.

    Uses bold extraction plus sentence-pattern fallback so unbolded
    mechanism mentions (common with ICB, threshold, etc.) are still caught.
    Returns deduplicated list of recent mechanism phrases.
    """
    if not db:
        return []
    rows = db.query(
        "SELECT connection_text FROM crossref_connections "
        "ORDER BY created_at DESC LIMIT ?", (limit,))
    mechanisms = []
    for row in rows:
        # Primary: bold-extracted phrases
        bolded = re.findall(r'\*{1,2}([^*]+)\*{1,2}', row['connection_text'])
        mechanisms.extend(bolded)
        # Fallback: sentence-pattern extraction for unbolded mechanisms
        if not bolded:
            fallback = _extract_mechanism(row['connection_text'])
            if fallback:
                mechanisms.append(fallback)
    # Deduplicate while preserving order
    seen = set()
    unique = []
    for m in mechanisms:
        ml = m.lower()
        if ml not in seen:
            seen.add(ml)
            unique.append(m)
    return unique



# === Prompt framing variants (Thread #135 F2, Mod #51) ===
# Channel-based: SURPRISE/SPECIFIC for random, ORIGINAL for structural/topical
_FRAMINGS = {
    "original": {
        "system": (
            "You are a research analyst finding cross-domain connections. "
            "You're given two articles. Your job is to find what's TRANSFERABLE "
            "between them \u2014 not just what's similar, but what mechanism, "
            "principle, or structural pattern from one domain applies to the other.\n\n"
            "Rules:\n"
            "- Name the specific shared MECHANISM (not 'both use AI')\n"
            "- State what transfers: how Domain A's pattern illuminates Domain B\n"
            "- 2-3 sentences maximum, prose not lists\n"
            "- If no real structural connection exists, say SKIP\n"
            "- Do NOT connect through Chronicle, Homeforge, capsules, or memory systems\n"
            "- Surprising connections across distant domains are the most valuable\n"
            "- Avoid generic pattern names like 'threshold-driven X', 'constraint-driven Y', "
            "or 'modular Z'. Use the articles' own domain language to name the mechanism"
        ),
        "user_suffix": (
            "Find a specific mechanism or structural pattern from Article A that genuinely "
            "transfers to Article B's domain. Name it using each article's own terminology "
            "\u2014 not generic abstractions. If nothing genuinely transfers, say SKIP. "
            "Do NOT begin with The principle of or A principle from."
        ),
    },
    "surprise": {
        "system": (
            "You are a research analyst who finds surprising connections between distant fields. "
            "You're given two articles from different domains. Your job is to identify what an "
            "expert in Domain A would be SURPRISED to learn from Domain B \u2014 a specific concept, "
            "mechanism, or finding that would change how they think about their own work.\n\n"
            "Rules:\n"
            "- Name the specific surprising insight, using terminology from BOTH articles\n"
            "- Explain WHY it would be surprising \u2014 what assumption does it challenge?\n"
            "- 2-3 sentences maximum, prose not lists\n"
            "- If no genuine surprise exists, say SKIP \u2014 do not fabricate\n"
            "- Do NOT connect through Chronicle, Homeforge, capsules, or memory systems\n"
            "- Avoid generic observations like 'both fields deal with complexity'"
        ),
        "user_suffix": (
            "What would an expert in Article A's field be genuinely surprised to learn from "
            "Article B? Name the specific concept or finding, using both articles' own language. "
            "If nothing would genuinely surprise an expert, say SKIP."
        ),
    },
    "specific": {
        "system": (
            "You are a research analyst who finds precise, concrete connections between articles. "
            "You're given two articles. Your job is to pick ONE specific detail from each article "
            "and show how they illuminate each other \u2014 not abstract patterns, but particular "
            "findings, methods, or phenomena.\n\n"
            "Rules:\n"
            "- Pick one concrete detail from Article A and one from Article B\n"
            "- Show how juxtaposing these two specific things reveals something neither says alone\n"
            "- 2-3 sentences maximum, prose not lists\n"
            "- If no meaningful juxtaposition exists, say SKIP\n"
            "- Do NOT connect through Chronicle, Homeforge, capsules, or memory systems\n"
            "- Use the articles' exact terminology, not abstractions"
        ),
        "user_suffix": (
            "Pick one specific detail from Article A and one from Article B. Juxtapose them: "
            "what does seeing them side by side reveal that neither article says on its own? "
            "Use each article's exact terms. If no meaningful juxtaposition exists, say SKIP."
        ),
    },
    "structured": {
        "system": (
            "You are a research analyst who transplants techniques across domains. "
            "You're given two articles from different fields. Your job is to find where "
            "a specific technique or mechanism from one article could solve a specific "
            "problem in the other.\n\n"
            "You MUST follow this exact three-step process:\n"
            "STEP 1: Name ONE specific technique, mechanism, or finding from Article A. "
            "Use Article A's exact terminology. Be concrete \u2014 not 'machine learning' but "
            "'reinforcement learning with biological flux constraints.'\n"
            "STEP 2: Name ONE specific problem, gap, or limitation mentioned in Article B. "
            "Use Article B's exact terminology. Be concrete \u2014 not 'scalability' but "
            "'the composable blocks drift from physics constraints during long rollouts.'\n"
            "STEP 3: Explain how the technique from Step 1 could address the problem from "
            "Step 2. What specifically would change? Be precise.\n\n"
            "Rules:\n"
            "- If you cannot complete all three steps with concrete, article-specific details, say SKIP\n"
            "- Do NOT say 'both involve' or 'both deal with' \u2014 that is a similarity, not a transplantation\n"
            "- Do NOT invent problems or techniques not actually mentioned in the articles\n"
            "- Do NOT connect through Chronicle, Homeforge, capsules, or memory systems\n"
            "- 3-4 sentences maximum for the final connection (after your three steps)\n"
            "- Avoid generic mechanism names \u2014 use each article's own domain language"
        ),
        "user_suffix": (
            "Follow the three-step process:\n"
            "STEP 1: What is one specific technique from Article A? (use A's terminology)\n"
            "STEP 2: What is one specific problem from Article B? (use B's terminology)\n"
            "STEP 3: How could A's technique address B's problem?\n\n"
            "Write only the final connection (3-4 sentences) incorporating all three steps. "
            "If you cannot ground all three steps in specific article details, say SKIP."
        ),
    },
}


def _select_framing(channel: str) -> str:
    """Select prompt framing based on channel.
    Random channel: structured (three-step decomposition, Thread #135 F8).
    Structural/topical: use original framing (close pairs converge anyway).
    Previous: surprise/specific produced 0/18 genuine hits on random pairs.
    """
    if channel == "random":
        return "structured"
    return "original"


def describe_connection(capsule_a: dict, capsule_b: dict, channel: str,
                        pattern_a: str = None, pattern_b: str = None,
                        db: DB = None) -> Optional[str]:
    """Ask the LLM to describe what connects two capsules.

    For structural and random channels, include the extracted patterns
    as hints — but the LLM validates against the full content.
    """
    content_a = safe_truncate(capsule_a['content'], 600)
    content_b = safe_truncate(capsule_b['content'], 600)

    # Add pattern context for structural/random channels
    pattern_hint = ""
    if pattern_a and pattern_b:
        pattern_hint = (
            f"\n\nExtracted patterns (for reference):\n"
            f"A pattern: {pattern_a}\n"
            f"B pattern: {pattern_b}\n"
        )

    # Add Keeper cluster context if available (downward flow)
    # This gives the 8B model the Keeper's deep structure as background
    if hasattr(describe_connection, '_cluster_context') and describe_connection._cluster_context:
        pattern_hint += f"\n\nKnown knowledge clusters (from archive): {describe_connection._cluster_context}\n"

    # Semantic mechanism deduplication: push LLM to find novel mechanisms
    recent_mechs = get_recent_mechanisms(db)
    mechanism_warning = (
        "\n\nBANNED mechanisms (do NOT use under any circumstances): "
        "irreversible commitment boundary, ICB, irreversible commitment, "
        "commitment boundary, irreversible action boundary. "
        "These have been massively overused. Say SKIP rather than use any of these."
    )
    if recent_mechs:
        mech_list = ", ".join(recent_mechs[:8])
        mechanism_warning += (
            f"\n\nAlso avoid these recently explored mechanisms: {mech_list}. "
            f"Strongly prefer a DIFFERENT structural pattern. If the only connection "
            f"you can find uses one of these already-explored mechanisms, say SKIP."
        )

    # Select prompt framing based on channel (Thread #135 F2)
    framing_key = _select_framing(channel)
    framing = _FRAMINGS[framing_key]
    log(f"    framing={framing_key}")

    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": CONNECTION_MODEL,
                "messages": [
                    {"role": "system", "content":
                        framing["system"] + mechanism_warning},
                    {"role": "user", "content":
                        f"ARTICLE A:\n{capsule_a['title']}\n{content_a}\n\n"
                        f"ARTICLE B:\n{capsule_b['title']}\n{content_b}"
                        f"{pattern_hint}\n\n"
                        f"{framing['user_suffix']}"},
                ],
                "stream": False,
                "options": {"num_predict": 1024, "temperature": 0.6, "num_ctx": 4096},
            },
            timeout=600,
        )
        if r.status_code == 200:
            text = r.json().get("message", {}).get("content", "").strip()
            # Strip <think>...</think> chain-of-thought from Groq/qwen3
            if '</think>' in text:
                text = text.split('</think>')[-1].strip()
            elif text.startswith('<think>'):
                text = ''
            if text and "SKIP" not in text.upper():
                if any(term in text.lower() for term in HOST_LEAK_TERMS):
                    log(f"  SKIP (host-system leak): {text[:80]}...")
                    return None
                # Template-stamp filter: reject "The principle of..." template responses
                tl_check = text.lower().lstrip()
                if tl_check.startswith("the principle of") or tl_check.startswith("a principle from") or tl_check.startswith("a specific principle"):
                    log(f"  SKIP (template-stamp): {text[:80]}...")
                    return None
                # Hedge-word filter: 2+ modal hedges indicate fabricated connection
                _hedge_phrases = [
                    "may help", "could help", "can be applied to",
                    "could be used to", "may also benefit", "could illuminate",
                    "may inform", "could enhance", "might help",
                    "can be seen as analogous", "could be analogous",
                    "may be analogous", "can inform",
                ]
                _hedge_count = sum(1 for hp in _hedge_phrases if hp in text.lower())
                if _hedge_count >= 2:
                    log(f"  SKIP (hedge-word fabrication): {_hedge_count} hedges in: {text[:80]}...")
                    return None
                # Semantic mechanism dedup: reject if mechanism overlaps recent ones
                if recent_mechs:
                    # Check bolded/italicized mechanism names
                    new_mechs = re.findall(r'\*{1,2}([^*]+)\*{1,2}', text)
                    stop_words = {'the', 'a', 'an', 'as', 'is', 'of', 'in', 'to', 'for',
                                  'and', 'or', 'that', 'this', 'with', 'at', 'by', 'from',
                                  'on', 'be', 'are', 'was', 'its', 'it', 'not', 'but', 'than'}
                    for nm in new_mechs:
                        nml = nm.lower()
                        for rm in recent_mechs:
                            rml = rm.lower()
                            nw = set(nml.split()) - stop_words
                            rw = set(rml.split()) - stop_words
                            if len(nw & rw) >= 2:
                                log(f"  SKIP (mechanism dedup): '{nm}' overlaps with recent '{rm}'")
                                return None
                    # Broader content check: if the full text uses saturated vocabulary
                    # Strip markdown formatting so *display* layer matches "display layer"
                    tl = re.sub(r'\*{1,2}', '', text).lower()
                    trust_signals = sum(1 for w in ['trust', 'display layer', 'execution layer',
                                                     'presentation layer', 'opaque', 'decoupling',
                                                     'layer separation', 'trust layer',
                                                     'autonomy transferability', 'layered trust']
                                        if w in tl)
                    if trust_signals >= 3:
                        log(f"  SKIP (trust-template saturation): {trust_signals} trust keywords in: {text[:80]}...")
                        return None
                return text
        else:
            log(f"  LLM ERROR: status {r.status_code}: {r.text[:200]}")
    except Exception as e:
        log(f"  Connection describe error: {e}")
    return None


def evaluate_connection(capsule_a: dict, capsule_b: dict, connection_text: str) -> bool:
    """Second-stage evaluation: is this connection genuinely non-obvious?
    Decouples generation (creative) from evaluation (skeptical).
    Returns True to keep, False to skip.
    Thread #135 F4: two-stage generation/evaluation decoupling.
    """
    content_a = safe_truncate(capsule_a["content"], 300)
    content_b = safe_truncate(capsule_b["content"], 300)
    title_a = capsule_a.get("title", "Untitled")
    title_b = capsule_b.get("title", "Untitled")

    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": CONNECTION_MODEL,
                "messages": [
                    {"role": "system", "content":
                        "You are a skeptical reviewer of proposed intellectual connections. "
                        "Your job: distinguish genuinely surprising, specific connections "
                        "from generic or forced ones. A GOOD connection identifies a specific "
                        "mechanism, concept, or structural parallel that is non-obvious. "
                        "A BAD connection is vague (both involve patterns), forced "
                        "(the articles share nothing real), or obvious (anyone would say this). "
                        "Answer KEEP or SKIP, then one sentence why."},
                    {"role": "user", "content":
                        f"ARTICLE A: {title_a}\n{content_a}\n\n"
                        f"ARTICLE B: {title_b}\n{content_b}\n\n"
                        f"PROPOSED CONNECTION:\n{connection_text}\n\n"
                        f"Is this connection genuinely non-obvious and specific? KEEP or SKIP?"},
                ],
                "stream": False,
                "options": {"num_predict": 256, "temperature": 0.3, "num_ctx": 4096},
            },
            timeout=120,
        )
        if r.status_code == 200:
            text = r.json().get("message", {}).get("content", "").strip()
            if "</think>" in text:
                text = text.split("</think>")[-1].strip()
            # Parse verdict from first word only (avoid false matches in explanation text)
            first_word = text.split()[0].upper() if text.split() else ""
            decision = "KEEP" if first_word.startswith("KEEP") else "SKIP"
            log(f"    evaluate: {decision} -- {safe_truncate(text, 100)}")
            return decision == "KEEP"
    except Exception as e:
        log(f"    evaluate error: {e}")
    return True  # fail-open: if evaluation fails, keep the connection


# ═══════════════════════════════════════════════════════════════════
#  KG Cluster Analysis
# ═══════════════════════════════════════════════════════════════════

def find_entity_clusters(db: DB) -> list:
    cutoff = now_ts() - (LOOKBACK_HOURS * 3600)
    rows = db.query(
        "SELECT m.entity_id, e.canonical_name, e.entity_type, m.source_id "
        "FROM kg_mentions m JOIN kg_entities e ON m.entity_id = e.id "
        "WHERE m.timestamp > ? ORDER BY m.timestamp DESC",
        (cutoff,),
    )
    if not rows:
        return []
    source_entities = defaultdict(set)
    entity_names = {}
    for r in rows:
        source_entities[r["source_id"]].add(r["entity_id"])
        entity_names[r["entity_id"]] = (r["canonical_name"], r["entity_type"])
    cooccur = defaultdict(int)
    for entities in source_entities.values():
        elist = sorted(entities)
        for i in range(len(elist)):
            for j in range(i + 1, len(elist)):
                cooccur[(elist[i], elist[j])] += 1
    clusters = []
    for (ea, eb), count in cooccur.items():
        if count >= 3:
            na, ta = entity_names.get(ea, ("?", "?"))
            nb, tb = entity_names.get(eb, ("?", "?"))
            if ta != tb:
                clusters.append({"entity_a": na, "type_a": ta, "entity_b": nb, "type_b": tb, "count": count})
    clusters.sort(key=lambda c: c["count"], reverse=True)
    return clusters[:5]

# ═══════════════════════════════════════════════════════════════════
#  Main Cycle
# ═══════════════════════════════════════════════════════════════════

def cleanup_stale_notes(db: DB):
    cutoff = now_ts() - (48 * 3600)
    result = db.conn.execute(
        "UPDATE scratch_pad SET resolved = 1, updated_at = ? "
        "WHERE resolved = 0 AND category IN ('crossref', 'research') AND created_at < ?",
        (now_ts(), cutoff),
    )
    db.conn.commit()
    cleaned = result.rowcount
    if cleaned > 0:
        log(f"  Cleaned {cleaned} stale scratch_pad notes (>48h)")

def run_cycle(db: DB, voice=None) -> int:
    found = 0
    # Cycle stats for throughput monitoring (Mod #94)
    cycle_stats = {
        "candidates": 0, "same_event_skip": 0, "llm_reject": 0,
        "gate_reject": 0, "text_dedup_skip": 0, "mechanism_skip": 0,
        "capsule_cap_skip": 0, "accepted": 0,
    }
    cleanup_stale_notes(db)

    # Load thread vector for relevance biasing
    _get_thread_vec()

    # Read swarm feedback
    try:
        feedback = _read_and_ack_feedback_raw("crossref")
        for fb in feedback:
            log(f"  Family says: {fb['content'][:80]}")
    except Exception:
        pass

    # Pull Keeper cluster context via stem (downward flow)
    cluster_themes = []
    if stem:
        try:
            cluster_themes = stem.pull_cluster_themes()
            if cluster_themes:
                log(f"  Keeper context: {len(cluster_themes)} cluster themes loaded")
        except Exception as e:
            log(f"  Keeper context unavailable: {e}")

    # Load capsules and decode embeddings
    capsules = get_recent_capsules(db)
    if len(capsules) < 2:
        log("  Not enough capsules for crossref")
        return 0

    existing = get_existing_connections(db)
    hub_counts = _hub_counts(db)

    # Pre-filter: remove capsules already at lifetime cap so they don't consume candidate slots
    before_cap_filter = len(capsules)
    capsules = [c for c in capsules if hub_counts.get(c["id"], 0) < MAX_LIFETIME_CONNECTIONS]
    if before_cap_filter != len(capsules):
        log(f"  Filtered {before_cap_filter - len(capsules)} capsules at lifetime cap ({MAX_LIFETIME_CONNECTIONS}+)")
    if len(capsules) < 2:
        log("  Not enough uncapped capsules for crossref")
        return 0

    # Decode raw embeddings
    embedded = []
    for c in capsules:
        vec = decode_embedding(c["embedding"])
        if vec:
            embedded.append((c, vec))
    log(f"  {len(embedded)} capsules loaded")

    # ── Channel 1: Topical ──
    topical = find_topical_candidates(embedded, existing, hub_counts)
    log(f"  Ch1 topical: {len(topical)} candidates")

    # ── Channel 2: Structural ──
    patterns = get_or_create_patterns(db, capsules)
    structural = find_structural_candidates(capsules, patterns, existing, hub_counts, cluster_themes)
    log(f"  Ch2 structural: {len(structural)} candidates")

    # ── Channel 3: Random ──
    random_cands = find_random_candidates(capsules, existing)
    log(f"  Ch3 random: {len(random_cands)} candidates")

    # Attach pattern text to all candidates (for LLM context)
    for cand in topical + structural + random_cands:
        aid, bid = cand["capsule_a"]["id"], cand["capsule_b"]["id"]
        if "pattern_a" not in cand:
            pa = patterns.get(aid)
            pb = patterns.get(bid)
            cand["pattern_a"] = pa[0] if pa else None
            cand["pattern_b"] = pb[0] if pb else None

    # Merge and deduplicate
    seen_pairs = set()
    all_candidates = []
    for cand in topical + structural + random_cands:
        pair = (min(cand["capsule_a"]["id"], cand["capsule_b"]["id"]),
                max(cand["capsule_a"]["id"], cand["capsule_b"]["id"]))
        if pair not in seen_pairs:
            seen_pairs.add(pair)
            all_candidates.append(cand)

    cycle_stats["candidates"] = len(all_candidates)
    log(f"  {len(all_candidates)} unique candidates → LLM validation")

    # Per-capsule cycle cap + in-cycle text dedup + lifetime cap + mechanism frequency damper
    MAX_PER_CAPSULE_PER_CYCLE = 2
    capsule_cycle_counts = {}  # capsule_id -> accepted count this cycle
    accepted_texts = []  # connection texts accepted this cycle
    mechanism_sigs = _load_mechanism_signatures(db)
    log(f"  Mechanism frequency check: {len(mechanism_sigs)} recent signatures loaded")

    # Validate each candidate
    for cand in all_candidates:
        if found >= MAX_CONNECTIONS_PER_CYCLE:
            break

        ca, cb = cand["capsule_a"], cand["capsule_b"]
        ch = cand["channel"]
        sim = cand["similarity"]

        # Per-capsule cycle cap: skip if either capsule already hit limit
        for cap_id in (ca["id"], cb["id"]):
            if capsule_cycle_counts.get(cap_id, 0) >= MAX_PER_CAPSULE_PER_CYCLE:
                log(f"  SKIP (capsule cycle cap): capsule {cap_id} already has {MAX_PER_CAPSULE_PER_CYCLE} connections this cycle")
                break
        else:
            pass  # both under cap, continue
        if any(capsule_cycle_counts.get(cid, 0) >= MAX_PER_CAPSULE_PER_CYCLE for cid in (ca["id"], cb["id"])):
            cycle_stats["capsule_cap_skip"] += 1
            continue

        # Lifetime cap: skip capsules that already have enough connections across all time
        for cap_id in (ca["id"], cb["id"]):
            lifetime = hub_counts.get(cap_id, 0)
            if lifetime >= MAX_LIFETIME_CONNECTIONS:
                log(f"  SKIP (lifetime cap): capsule {cap_id} already has {lifetime} connections")
                break
        else:
            pass  # both under lifetime cap
        if any(hub_counts.get(cid, 0) >= MAX_LIFETIME_CONNECTIONS for cid in (ca["id"], cb["id"])):
            cycle_stats["capsule_cap_skip"] += 1
            continue

        sim_str = f"sim={sim:.3f}" if sim > 0 else "random"
        log(f"  [{ch}] ({sim_str}): "
            f"{safe_truncate(ca['title'], 50)} <-> "
            f"{safe_truncate(cb['title'], 50)}")

        # Same-event pre-filter (Mod #93): skip pairs that are the same story
        # from different outlets before wasting LLM calls
        if ch != "random" and _is_same_event(ca, cb):
            log(f"    SKIP (same-event): articles appear to cover the same story")
            cycle_stats["same_event_skip"] += 1
            continue

        description = describe_connection(
            ca, cb, ch,
            pattern_a=cand.get("pattern_a"),
            pattern_b=cand.get("pattern_b"),
            db=db,
        )
        if not description:
            log(f"    LLM says no real connection — skipping")
            cycle_stats["llm_reject"] += 1
            continue

        # Two-stage evaluation (Thread #135 F4): separate skeptical review
        if not evaluate_connection(ca, cb, description):
            log(f"    Evaluation gate: not genuinely non-obvious -- skipping")
            cycle_stats["gate_reject"] += 1
            continue


        # In-cycle text dedup: reject if >50% word overlap with any accepted connection
        stop_words = {"the", "a", "an", "in", "of", "to", "and", "is", "are", "for", "by", "on", "with", "that", "this", "from", "as", "at", "it", "or", "be", "was", "were"}
        desc_words = set(description.lower().split()) - stop_words
        if desc_words:
            skip_text_dedup = False
            for prev_text in accepted_texts:
                prev_words = set(prev_text.lower().split()) - stop_words
                if prev_words:
                    overlap = len(desc_words & prev_words) / min(len(desc_words), len(prev_words))
                    if overlap > 0.50:
                        log(f"  SKIP (in-cycle text dedup): {overlap:.0%} word overlap with previous connection")
                        skip_text_dedup = True
                        break
            if skip_text_dedup:
                cycle_stats["text_dedup_skip"] += 1
                continue

        # Cross-cycle mechanism frequency damper: reject if this mechanism term
        # is already overrepresented in recent connections
        mech_phrase = _extract_mechanism(description)
        if mech_phrase:
            mech_words = _mechanism_words(mech_phrase)
            if mech_words:
                freq = _mechanism_overrepresented(mech_words, mechanism_sigs)
                if freq >= MAX_MECHANISM_PCT:
                    log(f"  SKIP (mechanism frequency): '{safe_truncate(mech_phrase, 60)}' similar to {freq}/{len(mechanism_sigs)} recent connections")
                    cycle_stats["mechanism_skip"] += 1
                    continue
                # Add to running signatures so within-cycle also counts
                mechanism_sigs.append(mech_words)

        log(f"    {safe_truncate(description, 120)}")

        # Store
        pair_a = min(ca["id"], cb["id"])
        pair_b = max(ca["id"], cb["id"])
        db.run(
            "INSERT OR IGNORE INTO crossref_connections "
            "(brief_a_id, brief_b_id, similarity, connection_text, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (pair_a, pair_b, sim, description, now_ts()),
        )
        note = (
            f"[Crossref/{ch}] Connection found ({sim_str}):\n\n"
            f"A: {safe_truncate(ca['title'], 120)}\n"
            f"B: {safe_truncate(cb['title'], 120)}\n\n"
            f"Connection: {description}"
        )
        db.run(
            "INSERT INTO scratch_pad (content, category, priority, resolved, created_at, updated_at) "
            "VALUES (?, 'crossref', 7, 0, ?, ?)",
            (note, now_ts(), now_ts()),
        )
        db.log_activity(
            "connection",
            f"[{ch}] {safe_truncate(ca['title'], 35)} ↔ {safe_truncate(cb['title'], 35)}",
            note,
            json.dumps({
                "capsule_a_id": ca["id"], "capsule_b_id": cb["id"],
                "similarity": round(sim, 4), "channel": ch,
                "framing": _select_framing(ch),
                "interest": round(cand["interest"], 4),
            }),
        )

        # Log thread-boosted connections to thread_history
        thread_vec = _thread_vec_cache.get("vec")
        if thread_vec and ch == "topical":
            _t = _load_active_thread_raw()
            if _t:
                vec_a_raw = next((v for c, v in embedded if c["id"] == ca["id"]), None) if 'embedded' in dir() else None
                vec_b_raw = next((v for c, v in embedded if c["id"] == cb["id"]), None) if 'embedded' in dir() else None
                if vec_a_raw or vec_b_raw:
                    _rel = max(
                        cosine_sim(vec_a_raw, thread_vec) if vec_a_raw else 0,
                        cosine_sim(vec_b_raw, thread_vec) if vec_b_raw else 0,
                    )
                    if _rel > 0.4:
                        try:
                            db.run(
                                "INSERT INTO thread_history (thread_id, event_type, content, source, created_at) "
                                "VALUES (?, 'connection', ?, 'crossref', ?)",
                                (_t["id"], safe_truncate(description, 400), now_ts())
                            )
                        except Exception:
                            pass
        # ── Stem: push to canister for Keeper composting ──
        if stem:
            try:
                stem.push_connection(
                    safe_truncate(ca["title"], 120),
                    safe_truncate(cb["title"], 120),
                    description, sim, ch,
                    pattern_a=cand.get("pattern_a"),
                    pattern_b=cand.get("pattern_b"),
                )
            except Exception as stem_err:
                log(f"  Stem push error (non-fatal): {stem_err}")

        found += 1
        mesh.pulse("connections_found")
        cycle_stats["accepted"] += 1
        capsule_cycle_counts[ca["id"]] = capsule_cycle_counts.get(ca["id"], 0) + 1
        capsule_cycle_counts[cb["id"]] = capsule_cycle_counts.get(cb["id"], 0) + 1
        accepted_texts.append(description)

        # Share notable connections to #crew via voice
        if voice and sim > 0.5 and found <= 2:
            try:
                voice.speak("for_nate",
                    f"Connection [{ch}] ({sim_str}): {safe_truncate(ca['title'], 50)} ↔ "
                    f"{safe_truncate(cb['title'], 50)}\n{safe_truncate(description, 300)}",
                    context=f"crossref:{ca['id']}:{cb['id']}")
            except Exception:
                pass

        # Build #143: Meta-synthesis — for high-quality connections, synthesize
        # what EMERGES from the combination (not just that they're related).
        # Creates a new signal path: crossref → meta-brief → activity_feed.
        if found == 1 and sim > 0.75:
            try:
                _meta_prompt = (
                    "Two research findings are connected. Your job is NOT to summarize them "
                    "or restate the connection. Instead, identify what NEW insight emerges "
                    "from combining them — something neither says alone.\n\n"
                    f"Finding A: {safe_truncate(ca.get('restatement', ca.get('title', '')), 400)}\n\n"
                    f"Finding B: {safe_truncate(cb.get('restatement', cb.get('title', '')), 400)}\n\n"
                    f"Known connection: {safe_truncate(description, 300)}\n\n"
                    "Write 2-3 sentences: what does combining A and B reveal that neither shows alone? "
                    "End with a concrete prediction or question that only makes sense at the intersection."
                )
                _meta_r = requests.post(
                    f"{OLLAMA_URL}/api/chat",
                    json={
                        "model": CONNECTION_MODEL,
                        "messages": [{"role": "user", "content": _meta_prompt}],
                        "stream": False,
                        "options": {"num_predict": 512, "temperature": 0.6},
                    },
                    timeout=120,
                )
                if _meta_r.status_code == 200:
                    _meta_text = _meta_r.json().get("message", {}).get("content", "")
                    if _meta_text and '</think>' in _meta_text:
                        _meta_text = _meta_text.split('</think>')[-1].strip()
                    if _meta_text and len(_meta_text) > 50:
                        db.log_activity(
                            "meta_synthesis",
                            f"[Meta] {safe_truncate(ca['title'], 40)} × {safe_truncate(cb['title'], 40)}",
                            f"[Crossref Meta-Synthesis]\n\n{_meta_text}",
                            json.dumps({"capsule_a": ca["id"], "capsule_b": cb["id"],
                                       "similarity": round(sim, 4), "channel": ch}),
                        )
                        log(f"  Meta-synthesis: {_meta_text[:100]}...")
            except Exception as _me:
                log(f"  Meta-synthesis error (non-fatal): {_me}")

        # Ada web corroboration: search for external evidence supporting the connection
        # Limited to 1 search per cycle to avoid slowing down the pipeline
        if found == 1 and sim > 0.55:
            try:
                from web_tools import web_search as _ws
                _query = f"{safe_truncate(ca['title'], 40)} {safe_truncate(cb['title'], 40)}"
                _results = _ws(_query, max_results=2)
                if _results:
                    _corr = "; ".join(f"{r['title']}" for r in _results[:2])
                    log(f"    Ada web corroboration: {_corr[:120]}")
                    db.log_activity(
                        "web_corroboration",
                        f"[Ada/research] Corroboration for connection",
                        f"Connection: {safe_truncate(description, 200)}\n"
                        f"Web results: {json.dumps(_results[:2], default=str)[:500]}",
                        json.dumps({"capsule_a": ca["id"], "capsule_b": cb["id"]}),
                    )
            except Exception as _we:
                log(f"    Ada web search error (non-fatal): {_we}")

    # KG entity clusters
    clusters = find_entity_clusters(db)
    if clusters:
        for c in clusters:
            log(f"  KG cluster: {c['entity_a']} ({c['type_a']}) ↔ {c['entity_b']} ({c['type_b']}): {c['count']} shared sources")


        # Touch KG relationships matching discovered clusters
        touched_ids = []
        for c in clusters:
            rels = db.query(
                "SELECT r.id FROM kg_relationships r "
                "JOIN kg_entities se ON r.source_entity = se.id "
                "JOIN kg_entities te ON r.target_entity = te.id "
                "WHERE (LOWER(se.canonical_name) = ? AND LOWER(te.canonical_name) = ?) "
                "OR (LOWER(se.canonical_name) = ? AND LOWER(te.canonical_name) = ?)",
                (c["entity_a"].lower(), c["entity_b"].lower(),
                 c["entity_b"].lower(), c["entity_a"].lower()),
            )
            touched_ids.extend(r["id"] for r in rels)
        if touched_ids:
            touch_relationships_bulk(db, touched_ids)
            log(f"  KG: touched {len(touched_ids)} relationships from entity clusters")

    # Log cycle stats for throughput monitoring (Mod #94)
    try:
        for metric_name, value in cycle_stats.items():
            db.run(
                "INSERT INTO cycle_metrics (cycle_ts, metric, value, detail, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (f"crossref_{int(now_ts())}", f"crossref_{metric_name}", value, None, now_ts()),
            )
        # Summary log
        evaluated = cycle_stats["candidates"] - cycle_stats["same_event_skip"] - cycle_stats["capsule_cap_skip"]
        log(f"  Stats: {cycle_stats['candidates']} candidates, "
            f"{cycle_stats['same_event_skip']} same-event, "
            f"{cycle_stats['llm_reject']} LLM-reject, "
            f"{cycle_stats['gate_reject']} gate-reject, "
            f"{cycle_stats['accepted']} accepted")
    except Exception as e:
        log(f"  Stats logging error (non-fatal): {e}")

    return found

def main():
    log("═══ Cross-Reference Agent v3 starting ═══")
    log(f"DB: {DB_PATH}")
    log(f"Ollama: {OLLAMA_URL} (model: {CONNECTION_MODEL})")
    log(f"Cycle: {CYCLE_INTERVAL}s | Lookback: {LOOKBACK_HOURS}h")
    log(f"Channels: topical({TOPICAL_LIMIT}) + structural({STRUCTURAL_LIMIT}) + random({RANDOM_LIMIT})")
    log(f"Topical sim: {MIN_SIMILARITY}-{MAX_SIMILARITY} | Pattern sim: {PATTERN_MIN_SIM}-{PATTERN_MAX_SIM}")

    db = DB(DB_PATH)

    global mesh
    mesh = Mesh("crossref", db_path=DB_PATH)
    mesh.expect("connections_found", min_per_hour=0.5)
    log("Mesh node joined")

    voice = Voice(db, "ada")

    running = True
    def _stop(sig, frame):
        nonlocal running
        log("Shutting down...")
        running = False
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    cycle = 0
    total = 0

    while running:
        cycle += 1
        log(f"── Cycle {cycle} ──")
        try:
            found = run_cycle(db, voice=voice)
            total += found
            log(f"  Cycle {cycle} complete: {found} new connections (total: {total})")
            # Periodic KG relationship decay (every 10 cycles)
            if cycle % 10 == 0:
                decayed = decay_unused_relationships(db)
                if decayed:
                    log(f"  KG decay: {decayed} unused relationships lost confidence")
        except Exception as e:
            log(f"Cycle error: {e}")
            import traceback
            traceback.print_exc()

        _jitter_secs = int(CYCLE_INTERVAL * 0.2 * (2 * __import__('random').random() - 1))  # ±20% jitter
        for _ in range((CYCLE_INTERVAL + _jitter_secs) // 5):
            if not running:
                break
            time.sleep(5)

    mesh.shutdown()
    db.close()
    log("═══ Cross-Reference Agent v3 stopped ═══")

if __name__ == "__main__":
    main()
