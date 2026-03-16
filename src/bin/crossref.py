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

import requests
import subprocess

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)
OLLAMA_URL = os.environ.get("CHRONICLE_OLLAMA_URL", "http://localhost:11434")
EMBED_MODEL = "chronicle-embed"
PATTERN_MODEL = os.environ.get("CROSSREF_PATTERN_MODEL", "hermes3-crossref")  # fast 8B for pattern extraction
CONNECTION_MODEL = os.environ.get("CROSSREF_MODEL", "chronicle-deep")  # deep 32B for connection description
CYCLE_INTERVAL = int(os.environ.get("CROSSREF_INTERVAL", "1800"))  # 30 min
LOOKBACK_HOURS = int(os.environ.get("CROSSREF_LOOKBACK", "24"))
MIN_SIMILARITY = float(os.environ.get("CROSSREF_MIN_SIM", "0.55"))
MAX_SIMILARITY = float(os.environ.get("CROSSREF_MAX_SIM", "0.80"))
PATTERN_MIN_SIM = float(os.environ.get("CROSSREF_PATTERN_SIM", "0.50"))
PATTERN_MAX_SIM = float(os.environ.get("CROSSREF_PATTERN_MAX_SIM", "0.85"))

# Per-channel limits — how many candidates each channel sends to LLM validation
TOPICAL_LIMIT = 2
STRUCTURAL_LIMIT = 5
RANDOM_LIMIT = 2
MAX_CONNECTIONS_PER_CYCLE = 7  # total stored connections cap

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

MIN_CAPSULE_LENGTH = 250

HOST_LEAK_TERMS = [
    "chronicle", "homeforge", "memory metabolism",
    "capsule", "knowledge graph", "knowledge capsule",
    "pattern metabolism", "seed system", "crossref",
]

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

def embed_text(text: str) -> Optional[List[float]]:
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/embed",
            json={"model": EMBED_MODEL, "input": [safe_truncate(text, 500)]},
            timeout=15,
        )
        if r.status_code == 200:
            embs = r.json().get("embeddings", [])
            if embs:
                return embs[0]
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
    text = re.sub(r'^\[capsule:\d+\]\s*', '', content)
    for delim in ['Article URL:', 'Comments URL:', 'http', '\n']:
        idx = text.find(delim)
        if idx > 10:
            text = text[:idx]
            break
    return text.strip()[:150]

def get_recent_capsules(db: DB) -> list:
    cutoff = now_ts() - (LOOKBACK_HOURS * 3600)
    rows = db.query(
        "SELECT id, content, embedding, timestamp FROM seed_observations "
        "WHERE source = 'canister:capsule' "
        "AND embedding IS NOT NULL "
        "AND timestamp > ? "
        "ORDER BY id DESC LIMIT 80",
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
        cap_match = re.match(r'\[capsule:(\d+)\]', content)
        cap_id = cap_match.group(1) if cap_match else None
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
                        "Name the mechanism, not the topic. "
                        "Good: 'One entity diverges from its cohort, signaling asymmetric resource access.' "
                        "Bad: 'Article about Bitcoin price movements.' "
                        "Good: 'Irreversible commitment boundary separates reasoning from action, preventing rollback.' "
                        "Bad: 'Story about a supply chain attack.' "
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
    """Cosine similarity on raw embeddings — same-concept, different domain."""
    candidates = []
    for i in range(len(embedded)):
        for j in range(i + 1, len(embedded)):
            c_a, vec_a = embedded[i]
            c_b, vec_b = embedded[j]
            pair_key = (min(c_a["id"], c_b["id"]), max(c_a["id"], c_b["id"]))
            if pair_key in existing:
                continue
            sim = cosine_sim(vec_a, vec_b)
            if sim < MIN_SIMILARITY or sim > MAX_SIMILARITY:
                continue
            # Same-source filter
            t_a = re.search(r'\(topic:\s*([^)]+)\)', c_a["content"])
            t_b = re.search(r'\(topic:\s*([^)]+)\)', c_b["content"])
            if t_a and t_b and t_a.group(1).strip() == t_b.group(1).strip():
                continue
            # Interest score with hub penalty
            words_a = set(re.findall(r'\w{4,}', c_a["title"].lower()))
            words_b = set(re.findall(r'\w{4,}', c_b["title"].lower()))
            overlap = len(words_a & words_b) / min(len(words_a), len(words_b)) if words_a and words_b else 0.0
            interest = sim * (1.0 - overlap * 0.5)
            interest *= _hub_penalty(hub_counts.get(c_a["id"], 0))
            interest *= _hub_penalty(hub_counts.get(c_b["id"], 0))
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
    """Cosine similarity on pattern embeddings — shared mechanisms across domains."""
    # Build list of capsules that have pattern embeddings
    patterned = []
    for c in capsules:
        entry = patterns.get(c["id"])
        if entry and entry[1]:  # has pattern and embedding
            patterned.append((c, entry[0], entry[1]))  # (capsule, pattern_text, pattern_vec)

    if len(patterned) < 2:
        return []

    candidates = []
    for i in range(len(patterned)):
        for j in range(i + 1, len(patterned)):
            c_a, pat_a, vec_a = patterned[i]
            c_b, pat_b, vec_b = patterned[j]
            pair_key = (min(c_a["id"], c_b["id"]), max(c_a["id"], c_b["id"]))
            if pair_key in existing:
                continue
            sim = cosine_sim(vec_a, vec_b)
            if sim < PATTERN_MIN_SIM or sim > PATTERN_MAX_SIM:
                continue
            # For structural channel, we WANT cross-domain — penalize same-topic more
            t_a = re.search(r'\(topic:\s*([^)]+)\)', c_a["content"])
            t_b = re.search(r'\(topic:\s*([^)]+)\)', c_b["content"])
            if t_a and t_b and t_a.group(1).strip() == t_b.group(1).strip():
                continue
            interest = sim
            interest *= _hub_penalty(hub_counts.get(c_a["id"], 0))
            interest *= _hub_penalty(hub_counts.get(c_b["id"], 0))

            # Keeper cluster boost: if patterns match DIFFERENT cluster themes,
            # this is a potential cross-cluster bridge — boost interest
            if cluster_themes and pat_a and pat_b:
                pa_lower = pat_a.lower()
                pb_lower = pat_b.lower()
                a_clusters = [t for t in cluster_themes if any(w in pa_lower for w in t.lower().split(", ")[:2])]
                b_clusters = [t for t in cluster_themes if any(w in pb_lower for w in t.lower().split(", ")[:2])]
                if a_clusters and b_clusters and a_clusters[0] != b_clusters[0]:
                    interest *= 1.3  # cross-cluster bridge bonus

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

    Mechanisms are consistently bolded with **name** in connection text.
    Returns deduplicated list of recent mechanism phrases.
    """
    if not db:
        return []
    rows = db.query(
        "SELECT connection_text FROM crossref_connections "
        "ORDER BY created_at DESC LIMIT ?", (limit,))
    mechanisms = []
    for row in rows:
        bolded = re.findall(r'\*{1,2}([^*]+)\*{1,2}', row['connection_text'])
        mechanisms.extend(bolded)
    # Deduplicate while preserving order
    seen = set()
    unique = []
    for m in mechanisms:
        ml = m.lower()
        if ml not in seen:
            seen.add(ml)
            unique.append(m)
    return unique


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
    mechanism_warning = ""
    if recent_mechs:
        mech_list = ", ".join(recent_mechs[:8])
        mechanism_warning = (
            f"\n\nRecent connections already explored these mechanisms: {mech_list}. "
            f"Strongly prefer a DIFFERENT structural pattern. If the only connection "
            f"you can find uses one of these already-explored mechanisms, say SKIP."
        )

    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": CONNECTION_MODEL,
                "messages": [
                    {"role": "system", "content":
                        "You are a research analyst finding cross-domain connections. "
                        "You're given two articles. Your job is to find what's TRANSFERABLE "
                        "between them — not just what's similar, but what mechanism, "
                        "principle, or structural pattern from one domain applies to the other.\n\n"
                        "Rules:\n"
                        "- Name the specific shared MECHANISM (not 'both use AI')\n"
                        "- State what transfers: how Domain A's pattern illuminates Domain B\n"
                        "- 2-3 sentences maximum, prose not lists\n"
                        "- If no real structural connection exists, say SKIP\n"
                        "- Do NOT connect through Chronicle, Homeforge, capsules, or memory systems\n"
                        "- Surprising connections across distant domains are the most valuable"
                        + mechanism_warning},
                    {"role": "user", "content":
                        f"ARTICLE A:\n{capsule_a['title']}\n{content_a}\n\n"
                        f"ARTICLE B:\n{capsule_b['title']}\n{content_b}"
                        f"{pattern_hint}\n\n"
                        f"What structural mechanism do these share?"},
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
    except Exception as e:
        log(f"  Connection describe error: {e}")
    return None

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

def run_cycle(db: DB) -> int:
    found = 0
    cleanup_stale_notes(db)

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

    log(f"  {len(all_candidates)} unique candidates → LLM validation")

    # Validate each candidate
    for cand in all_candidates:
        if found >= MAX_CONNECTIONS_PER_CYCLE:
            break

        ca, cb = cand["capsule_a"], cand["capsule_b"]
        ch = cand["channel"]
        sim = cand["similarity"]

        sim_str = f"sim={sim:.3f}" if sim > 0 else "random"
        log(f"  [{ch}] ({sim_str}): "
            f"{safe_truncate(ca['title'], 50)} <-> "
            f"{safe_truncate(cb['title'], 50)}")

        description = describe_connection(
            ca, cb, ch,
            pattern_a=cand.get("pattern_a"),
            pattern_b=cand.get("pattern_b"),
            db=db,
        )
        if not description:
            log(f"    LLM says no real connection — skipping")
            continue

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
                "interest": round(cand["interest"], 4),
            }),
        )
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

    # KG entity clusters
    clusters = find_entity_clusters(db)
    if clusters:
        for c in clusters:
            log(f"  KG cluster: {c['entity_a']} ({c['type_a']}) ↔ {c['entity_b']} ({c['type_b']}): {c['count']} shared sources")

    return found

def main():
    log("═══ Cross-Reference Agent v3 starting ═══")
    log(f"DB: {DB_PATH}")
    log(f"Ollama: {OLLAMA_URL} (model: {CONNECTION_MODEL})")
    log(f"Cycle: {CYCLE_INTERVAL}s | Lookback: {LOOKBACK_HOURS}h")
    log(f"Channels: topical({TOPICAL_LIMIT}) + structural({STRUCTURAL_LIMIT}) + random({RANDOM_LIMIT})")
    log(f"Topical sim: {MIN_SIMILARITY}-{MAX_SIMILARITY} | Pattern sim: {PATTERN_MIN_SIM}-{PATTERN_MAX_SIM}")

    db = DB(DB_PATH)

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
            found = run_cycle(db)
            total += found
            log(f"  Cycle {cycle} complete: {found} new connections (total: {total})")
        except Exception as e:
            log(f"Cycle error: {e}")
            import traceback
            traceback.print_exc()

        for _ in range(CYCLE_INTERVAL // 5):
            if not running:
                break
            time.sleep(5)

    db.close()
    log("═══ Cross-Reference Agent v3 stopped ═══")

if __name__ == "__main__":
    main()
