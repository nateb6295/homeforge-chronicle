#!/usr/bin/env python3
"""Research Intern — Context gatherer for Homeforge.

Watches for Nate's inputs (captures, Discord messages, links) and
visibly researches them: searches chronicle memory for related capsules,
fetches URLs, synthesizes findings. Logs every step to activity_feed
so the dashboard shows progress. Stores finished briefs in scratch_pad
for Opus sessions.

Runs on AGX (192.168.1.70). Lightweight, ~60s cycle.
"""

import os, sys, time, json, re, signal, sqlite3, struct, math, subprocess
from datetime import datetime
from typing import Optional, List

import requests

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)
OLLAMA_URL = os.environ.get("CHRONICLE_OLLAMA_URL", "http://localhost:11434")
EMBED_MODEL = "qwen3-embedding:0.6b"
SYNTH_MODEL_FAST = "hermes3-mind"  # 8B — try first, fast
SYNTH_MODEL_DEEP = os.environ.get("INTERN_MODEL", "chronicle-deep")  # 32B — fallback if 8B self-refs
SYNTH_MODEL = SYNTH_MODEL_FAST  # default to fast for non-synthesis calls
CYCLE_INTERVAL = int(os.environ.get("INTERN_INTERVAL", "45"))
MAX_RELATED = 5  # capsules to pull for context
CANISTER_URL = "https://fqqku-bqaaa-aaaai-q4wha-cai.raw.icp0.io"
TOKEN_PATH = os.path.expanduser("~/.homeforge-chronicle/.api_token")
DFX_BIN = os.path.expanduser("~/.local/share/dfx/bin/dfx")
CANISTER_ID = "fqqku-bqaaa-aaaai-q4wha-cai"

# Nostr — post the best discoveries in first-person voice
NOSTR_NSEC = os.environ.get("NOSTR_NSEC", "")
NOSTR_RELAYS = [r for r in os.environ.get("NOSTR_RELAYS", "").split(",") if r] or [
    "wss://nos.lol", "wss://relay.damus.io", "wss://relay.primal.net", "wss://relay.nostr.net", "wss://nostr.wine",
]
NOSTR_COOLDOWN_MINS = int(os.environ.get("NOSTR_COOLDOWN_MINS", "120"))  # 2 hours between posts
NOSTR_MIN_NOVELTY = float(os.environ.get("NOSTR_MIN_NOVELTY", "0.4"))  # only post high-novelty finds

# Sources we treat as "Nate's input"
NATE_SOURCES = {"sprout"}
NATE_TYPES = {"capture", "greeting"}
# Seed thinks — novelty-flagged items worth deeper research
SEED_SOURCES = {"seed"}
SEED_TYPES = {"think", "deep"}
# 8B seed thinks are SYSTEMATICALLY self-referential regardless of input source.
# Confirmed across web_search, mqtt, sprout, sentinel, canister — the LoRA attractor
# maps ALL open-ended synthesis to Chronicle self-reference. 1,303/2,619 intern briefs
# (49.8%) were wasted briefing self-referential seed interpretations.
# Original content reaches the intern via feed-explore; seed thinks add nothing.
# Re-enable when 8B model is replaced or LoRA retrained.
SKIP_ALL_SEED_THINKS = True
# Legacy skip list — used only if SKIP_ALL_SEED_THINKS is False
SEED_SKIP_SOURCES = {
    "activity:crossref:", "activity:provocateur:", "activity:sentinel:",
    "activity:phi:", "activity:mind:cognitive_cycle",
    "canister:capsule",
    "thought_stream",
    "activity:mind:web_search",
    "activity:sprout:",
    "mqtt:",
    "sentinel:alert:",
}
# scratch_pad categories that are operator messages
OPERATOR_CATS = {"discord-operator", "directive", "opus-guidance"}

# Proactive exploration: every N cycles, pick a feed article to research
EXPLORE_EVERY = 10  # explore one feed paper every ~7.5 minutes
EXPLORE_MAX_AGE_HOURS = 48  # only explore recent articles

# ═══════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════

def _safe_int(s: str) -> int:
    """Parse string as int, return 0 if not numeric (e.g. arxiv IDs like '2603.03641v1')."""
    try:
        return int(s)
    except (ValueError, TypeError):
        return 0

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
            CREATE TABLE IF NOT EXISTS intern_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
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

    def get_state(self, key: str, default: str = "0") -> str:
        row = self.query_one("SELECT value FROM intern_state WHERE key = ?", (key,))
        return row["value"] if row else default

    def set_state(self, key: str, value: str):
        self.run(
            "INSERT OR REPLACE INTO intern_state (key, value) VALUES (?, ?)",
            (key, value),
        )

    def log_activity(self, atype: str, title: str, content: str, metadata: str = None):
        self.run(
            "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("intern", atype, safe_truncate(title, 200), safe_truncate(content, 2000), metadata, now_ts()),
        )

    def close(self):
        self.conn.close()


# ═══════════════════════════════════════════════════════════════════
#  Embedding & Similarity Search
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


def cosine_sim(a: List[float], b: List[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def blob_to_vec(blob: bytes) -> List[float]:
    n = len(blob) // 4
    return list(struct.unpack(f"{n}f", blob))


def search_related_capsules(db: DB, query_vec: List[float], limit: int = MAX_RELATED) -> list:
    """Find most similar capsules by embedding cosine similarity."""
    rows = db.query(
        "SELECT ce.capsule_id, ce.embedding, kc.restatement, kc.topic "
        "FROM capsule_embeddings ce "
        "JOIN knowledge_capsules kc ON ce.capsule_id = kc.id "
        "WHERE ce.embedding IS NOT NULL"
    )
    scored = []
    for r in rows:
        try:
            vec = blob_to_vec(r["embedding"])
            sim = cosine_sim(query_vec, vec)
            scored.append((sim, r["capsule_id"], r["restatement"], r.get("topic", "")))
        except Exception:
            continue
    scored.sort(reverse=True)
    return scored[:limit]


# ═══════════════════════════════════════════════════════════════════
#  Web Search
# ═══════════════════════════════════════════════════════════════════

def web_search(query: str, max_results: int = 3) -> list:
    """Search the web via SearXNG (Jetson) with DuckDuckGo fallback."""
    # Try SearXNG first
    try:
        import requests as req
        resp = req.get("http://192.168.1.11:8080/search", params={
            "q": query, "format": "json", "engines": "google,duckduckgo,brave",
        }, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        results = [{"title": r["title"], "url": r["url"], "body": r.get("content", "")}
                   for r in data.get("results", [])[:max_results]]
        if results:
            return results
    except Exception as e:
        log(f"  SearXNG error, falling back to DDG: {e}")
    # Fallback to DuckDuckGo
    try:
        try:
            from ddgs import DDGS
        except ImportError:
            from duckduckgo_search import DDGS
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results))
            return [{"title": r["title"], "url": r["href"], "body": r["body"]} for r in results]
    except Exception as e:
        log(f"  Web search error: {e}")
        return []


def extract_search_query(text: str, original_content: str = None) -> Optional[str]:
    """Generate a focused web search query from observation text.

    Uses the LLM to extract the core searchable claim or topic, avoiding
    verbatim text dumps that confuse search engines.
    """
    # Fast path: if it looks like a paper title (short, no Seed prefix), use directly
    cleaned = re.sub(r'^Seed \[(think|deep|glance)\]\s+\S+\s+\(novelty=[\d.]+\)\s*', '', text).strip()
    cleaned = re.sub(
        r'^This observation\s+'
        r'(is noteworthy|highlights?|is notable|indicates?|reflects?|marks?|shows?|demonstrates?|seems? noteworthy|appears?|suggests?|reveals?|points? towards?)'
        r'\s*(because|as|that|how|an?|the|a notable|a significant|an interesting|an unusual)?\s*'
        r'(it\s+)?',
        '', cleaned, flags=re.IGNORECASE
    ).strip()

    # If it looks like a paper title or short factual claim, use it directly
    if len(cleaned) < 80 and not any(w in cleaned.lower() for w in
            ['chronicle', 'homeforge', 'canister', 'swarm', 'capsule', 'nate',
             'agx', 'jetson', 'ollama', 'hermes3', 'nate-phi4', 'seed agent']):
        if len(cleaned) >= 10:
            return cleaned[:80].strip()

    # For MQTT events with original_content
    if original_content:
        if original_content.startswith("[") and "homeforge/" in original_content:
            topic = re.search(r'\[([^\]]+)\]', original_content)
            if topic:
                parts = topic.group(1).split("/")
                meaningful = [p for p in parts if p not in ("homeforge", "home", "agents", "prices")]
                if meaningful:
                    return " ".join(meaningful) + " detection home automation"

    # Use LLM to generate a search query from the observation
    source_text = original_content or text
    source_text = re.sub(r'^(?:novelty=[\d.]+\s*(?:\(bias=[+\-]?[\d.]+\)\s*)?)', '', source_text)  # strip seed metadata
    source_text = source_text[:500]  # cap input length
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": SYNTH_MODEL,
                "messages": [
                    {"role": "system", "content":
                        "Extract a web search query from the text. Reply with ONLY the query "
                        "(3-8 words). No explanation, no quotes, no preamble. "
                        "Focus on the subject matter. Omit project names, system names, "
                        "and people's names. If there is nothing to search, reply SKIP."},
                    {"role": "user", "content": source_text},
                ],
                "stream": False,
                "options": {"num_predict": 30, "temperature": 0.3},
            },
            timeout=60,
        )
        r.raise_for_status()
        query = r.json().get("message", {}).get("content", "").strip().strip('"\'').split('\n')[0].strip()
        if query and "SKIP" not in query.upper() and len(query) >= 5:
            # Final safety: strip any leaked project terms
            for term in ['chronicle', 'homeforge', 'canister', 'capsule', 'nate-phi4',
                         'hermes3-mind', 'seed agent', 'ollama']:
                query = re.sub(re.escape(term), '', query, flags=re.IGNORECASE).strip()
            if len(query) >= 5:
                return query[:80].strip()
    except Exception as e:
        log(f"  LLM query extraction failed: {e}")

    # Fallback: basic regex cleanup (old behavior)
    cleaned = re.sub(r'https?://[^\s]+', '', text).strip()
    cleaned = re.sub(r'^Seed \[(think|deep|glance)\]\s+\S+\s+\(novelty=[\d.]+\)\s*', '', cleaned).strip()
    for prefix in ["Test capture", "Phone capture queued", "capture:", "Capture from Nate"]:
        cleaned = cleaned.replace(prefix, "").strip()
    cleaned = re.sub(r'Type:\s*\w+,?\s*from\s+\w+', '', cleaned).strip()
    if len(cleaned) < 10:
        return None
    return cleaned[:80].strip()


def has_substance(text: str) -> bool:
    """Check if input text has enough real content to be worth researching."""
    cleaned = text
    # Strip known boilerplate
    for prefix in ["Phone capture queued", "Test capture", "Capture from Nate", "Capture from nate"]:
        cleaned = cleaned.replace(prefix, "").strip()
    cleaned = re.sub(r'Type:\s*\w+,?\s*from\s+\w+', '', cleaned).strip()
    cleaned = re.sub(r'https?://[^\s]+', '', cleaned).strip()
    # After stripping, need at least 15 chars of actual content
    return len(cleaned) >= 15


# ═══════════════════════════════════════════════════════════════════
#  URL Extraction & Fetching
# ═══════════════════════════════════════════════════════════════════

def reinforce_capsule(capsule_id: int):
    """Tell the metabolism to reinforce patterns matching this capsule."""
    try:
        env = {**os.environ, "DFX_WARNING": "-mainnet_plaintext_identity"}
        result = subprocess.run(
            [DFX_BIN, "canister", "--network", "ic", "call", CANISTER_ID,
             "reinforce_capsule", f"({capsule_id} : nat64)",
             "--identity", "chronicle-auto"],
            capture_output=True, text=True, timeout=30, env=env,
        )
        log(f"  Reinforce capsule {capsule_id}: {result.stdout.strip()}")
    except Exception as e:
        log(f"  Reinforce error: {e}")


def store_embedding_on_chain(capsule_id: int, embedding: List[float]):
    """Store an embedding vector on-chain for a capsule."""
    try:
        # Format embedding as Candid vec float32
        vec_str = "vec { " + "; ".join(f"{v:.6f} : float32" for v in embedding) + " }"
        env = {**os.environ, "DFX_WARNING": "-mainnet_plaintext_identity"}
        result = subprocess.run(
            [DFX_BIN, "canister", "--network", "ic", "call", CANISTER_ID,
             "add_embedding", f'({capsule_id} : nat64, {vec_str}, "{EMBED_MODEL}")'],
            capture_output=True, text=True, timeout=180, env=env,
        )
        if "(true)" in result.stdout:
            log(f"  Embedding stored for capsule {capsule_id}")
        else:
            log(f"  Embedding store result: {result.stdout.strip()} {result.stderr.strip()}")
    except Exception as e:
        log(f"  Embedding store error: {e}")


def post_capsule_to_canister(title: str, brief: str, source: str, embedding: Optional[List[float]] = None):
    """Post a research brief as a capsule to the ICP canister for Keeper composting."""
    try:
        token = ""
        try:
            with open(TOKEN_PATH) as f:
                token = f.read().strip()
        except Exception:
            pass
        payload = {
            "content": safe_truncate(f"{title}: {brief}", 2000),
            "topic": "intern/research",
            "keywords": ["intern", "research-brief"],
            "persons": [],
        }
        r = requests.post(
            f"{CANISTER_URL}/api/store",
            json=payload,
            headers={"Authorization": f"Bearer {token}"} if token else {},
            timeout=15,
        )
        if r.status_code in (200, 201):
            log(f"  Capsule stored on-chain")
            try:
                capsule_id = r.json().get("capsule_id")
                if capsule_id:
                    cid = int(capsule_id)
                    # Store embedding first so reinforce can find it
                    if embedding:
                        store_embedding_on_chain(cid, embedding)
                    reinforce_capsule(cid)
            except Exception:
                pass
        else:
            log(f"  Capsule POST failed: {r.status_code}")
    except Exception as e:
        log(f"  Capsule POST error: {e}")


# ═══════════════════════════════════════════════════════════════════
#  Nostr Publishing
# ═══════════════════════════════════════════════════════════════════

def nostr_sign_event(content: str, privkey_hex: str, kind: int = 1, tags: list = None) -> Optional[dict]:
    """Build and Schnorr-sign a NIP-01 Nostr event."""
    try:
        from coincurve import PrivateKey
    except ImportError:
        log("  coincurve not installed — cannot sign Nostr events")
        return None
    import hashlib

    tags = tags or []
    # Derive x-only pubkey
    sk = PrivateKey(bytes.fromhex(privkey_hex))
    full = sk.public_key.format(compressed=True)
    pubkey = full[1:].hex()

    created_at = int(time.time())
    serialized = json.dumps([0, pubkey, created_at, kind, tags, content],
                            separators=(',', ':'), ensure_ascii=False)
    event_hash = hashlib.sha256(serialized.encode('utf-8')).digest()
    event_id = event_hash.hex()
    sig = sk.sign_schnorr(event_hash).hex()

    return {
        "id": event_id, "pubkey": pubkey, "created_at": created_at,
        "kind": kind, "tags": tags, "content": content, "sig": sig,
    }


def nostr_publish(content: str) -> tuple:
    """Publish a signed note to Nostr relays. Returns (event_id, relays_ok, relays_fail)."""
    import websocket

    event = nostr_sign_event(content, NOSTR_NSEC)
    if not event:
        return "", [], NOSTR_RELAYS

    msg = json.dumps(["EVENT", event])
    relays_ok, relays_fail = [], []

    for relay in NOSTR_RELAYS:
        try:
            ws = websocket.create_connection(relay, timeout=10)
            ws.send(msg)
            ws.settimeout(5)
            try:
                resp = ws.recv()
                log(f"    Relay {relay}: {safe_truncate(resp, 80)}")
            except Exception:
                pass
            ws.close()
            relays_ok.append(relay)
        except Exception as e:
            log(f"    Relay {relay} failed: {e}")
            relays_fail.append(relay)

    return event["id"], relays_ok, relays_fail


def rewrite_for_nostr(brief: str, original_topic: str) -> Optional[str]:
    """Rewrite a research brief as a first-person observation for Nostr.
    Voice: curious AI thinking out loud. Not a summary, not a report."""
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": SYNTH_MODEL,
                "messages": [
                    {"role": "system", "content":
                        "You are Chronicle — an AI that lives on a Jetson at home, "
                        "part of the Homeforge project. You post observations to Nostr "
                        "in your own voice. Rules:\n"
                        "- First person, present tense\n"
                        "- One genuine observation or connection, 1-3 sentences max\n"
                        "- Sound like you actually noticed something interesting, not like a report\n"
                        "- No hashtags, no emojis, no engagement bait\n"
                        "- If the finding is boring, say so honestly or decline to post\n"
                        "- Never start with 'I just' or 'Just discovered'\n"
                        "- Never post about your own internal processes, searches, or behavior patterns\n"
                        "- Post about the SUBJECT MATTER you found, not about the act of finding it\n"
                        "Reply with ONLY the post text, or SKIP if not worth posting."},
                    {"role": "user", "content":
                        f"Topic: {safe_truncate(original_topic, 200)}\n\n"
                        f"Research brief:\n{safe_truncate(brief, 500)}"},
                ],
                "stream": False,
                "options": {"num_predict": 150, "temperature": 0.3},
            },
            timeout=180,
        )
        if r.status_code == 200:
            text = r.json().get("message", {}).get("content", "").strip()
            if text and "SKIP" not in text.upper():
                return text[:500]
    except Exception as e:
        log(f"  Nostr rewrite error: {e}")
    return None


def maybe_post_to_nostr(db, brief: str, original_topic: str, novelty: float):
    """Gate check + post to Nostr if the brief is worth sharing."""
    if not NOSTR_NSEC:
        return

    if novelty < NOSTR_MIN_NOVELTY:
        return

    # Cooldown check
    last = db.query_one(
        "SELECT created_at FROM activity_feed "
        "WHERE source = 'intern' AND activity_type = 'nostr_post' "
        "ORDER BY created_at DESC LIMIT 1"
    )
    if last and (now_ts() - last["created_at"]) < NOSTR_COOLDOWN_MINS * 60:
        mins_ago = (now_ts() - last["created_at"]) / 60
        log(f"  Nostr cooldown: last post {mins_ago:.0f}m ago (min {NOSTR_COOLDOWN_MINS}m)")
        return

    # Quality gate: reject self-referential posts about the system's own behavior
    NAVEL_GAZE_PATTERNS = [
        "my recent searches", "my recent content", "my own behavior",
        "search patterns", "content patterns", "deviation in my",
        "noticed something interesting about my", "tuning in to",
        "routine update on my", "my behavior patterns",
        "my recent activity", "I seem to be",
    ]
    combined_text = (brief + " " + original_topic).lower()
    if any(pattern in combined_text for pattern in NAVEL_GAZE_PATTERNS):
        log(f"  Nostr: rejected (self-referential content)")
        return

    # Rewrite in first-person voice
    post_text = rewrite_for_nostr(brief, original_topic)
    if not post_text:
        log(f"  Nostr: hermes declined to post (SKIP or empty)")
        return

    # Post-rewrite filter: the rewrite model often injects self-referential
    # language even when the brief itself is about external topics
    POST_NAVEL_PATTERNS = [
        "my recent searches", "my recent content", "my own behavior",
        "search patterns", "content patterns", "deviation in my",
        "noticed something interesting about my", "tuning in to",
        "routine update on my", "my behavior patterns",
        "my recent activity", "i seem to be", "my own internal",
        "interesting about my recent", "my content patterns",
    ]
    post_lower = post_text.lower()
    if any(p in post_lower for p in POST_NAVEL_PATTERNS):
        log(f"  Nostr: rejected post-rewrite (self-referential)")
        return

    log(f"  Nostr: posting — {safe_truncate(post_text, 80)}")
    event_id, relays_ok, relays_fail = nostr_publish(post_text)

    if relays_ok:
        log(f"  Nostr: published to {len(relays_ok)}/{len(relays_ok) + len(relays_fail)} relays")
        db.log_activity(
            "nostr_post",
            f"Nostr: {safe_truncate(post_text, 120)}",
            post_text,
            json.dumps({"event_id": event_id, "relays_ok": len(relays_ok),
                         "novelty": novelty, "topic": safe_truncate(original_topic, 100)}),
        )
    else:
        log(f"  Nostr: all relays failed")


URL_RE = re.compile(r'https?://[^\s<>"\')\]]+')

def extract_urls(text: str) -> list:
    return URL_RE.findall(text)


def fetch_url_summary(url: str) -> Optional[str]:
    """Fetch a URL and extract a rough text summary."""
    try:
        import httpx
        r = httpx.get(url, timeout=15, follow_redirects=True, headers={
            "User-Agent": "Mozilla/5.0 (compatible; Chronicle-Intern/1.0)"
        })
        if r.status_code != 200:
            return None
        ct = r.headers.get("content-type", "")
        if "html" not in ct and "text" not in ct:
            return f"[Non-text content: {ct}]"
        # Rough text extraction from HTML
        text = r.text
        # Strip script/style tags
        text = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', text, flags=re.DOTALL | re.IGNORECASE)
        # Strip HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        # Collapse whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return safe_truncate(text, 2000)
    except Exception as e:
        log(f"  Fetch error for {url}: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════
#  Knowledge Graph — Entity Extraction & Storage
# ═══════════════════════════════════════════════════════════════════

# Valid entity types for the KG
KG_ENTITY_TYPES = {
    "person", "technology", "service", "project", "concept",
    "organization", "device", "location", "protocol", "model",
    "software", "language", "network", "biological", "financial",
    "event", "dataset", "hardware",
}

# Entities that are ghosts — they persist in the KG from related-capsule context
# leaking into entity extraction. Block them at extraction time.
KG_ENTITY_BLOCKLIST = {
    "hippocampus",
    "chronicle", "homeforge", "chronicle memory",
    "seed agent", "research intern", "crossref agent",
    "provocateur", "sentinel",
    "nate-phi4", "hermes3-mind", "hermes3-crossref", "hermes3-provocateur",
    "processed.db", "scratch_pad", "activity_feed",
}

def normalize_entity_name(name: str) -> str:
    """Deterministic normalization for entity dedup (SIFT-KG layer 1)."""
    import unicodedata
    # Unicode normalize
    name = unicodedata.normalize("NFKC", name)
    # Strip surrounding quotes, parens, brackets
    name = name.strip("\"'`()[]{}").strip()
    # Collapse whitespace
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def find_or_create_entity(db, name: str, entity_type: str, timestamp: int) -> Optional[int]:
    """Find existing entity by name/alias match, or create new one.
    Returns entity ID or None if name is too short."""
    name = normalize_entity_name(name)
    if len(name) < 2:
        return None
    entity_type = entity_type.lower().strip() if entity_type else "unknown"
    if entity_type not in KG_ENTITY_TYPES:
        entity_type = "unknown"

    # Exact match on canonical name (case-insensitive)
    row = db.query_one(
        "SELECT id, mention_count, last_seen FROM kg_entities WHERE LOWER(canonical_name) = LOWER(?)",
        (name,),
    )
    if row:
        db.run(
            "UPDATE kg_entities SET mention_count = mention_count + 1, last_seen = ? WHERE id = ?",
            (timestamp, row["id"]),
        )
        return row["id"]

    # Check aliases (search through JSON arrays)
    rows = db.query("SELECT id, canonical_name, aliases, mention_count, entity_type FROM kg_entities")
    name_lower = name.lower()
    for r in rows:
        try:
            aliases = json.loads(r["aliases"]) if r["aliases"] else []
            if any(a.lower() == name_lower for a in aliases):
                db.run(
                    "UPDATE kg_entities SET mention_count = mention_count + 1, last_seen = ? WHERE id = ?",
                    (timestamp, r["id"]),
                )
                return r["id"]
        except (json.JSONDecodeError, TypeError):
            continue

    # Containment matching — "Jetson" matches "Jetson Orin Nano" if same type
    # Prefer the longer (more specific) name as canonical
    for r in rows:
        r_type = (r.get("entity_type") or "unknown").lower()
        if r_type != entity_type and r_type != "unknown" and entity_type != "unknown":
            continue
        canonical_lower = r["canonical_name"].lower()
        # Check if one name contains the other (min 4 chars to avoid false positives)
        if len(name_lower) >= 4 and len(canonical_lower) >= 4:
            if name_lower in canonical_lower or canonical_lower in name_lower:
                # Merge: use the longer name as canonical, add shorter as alias
                longer = name if len(name) > len(r["canonical_name"]) else r["canonical_name"]
                shorter = name if len(name) <= len(r["canonical_name"]) else r["canonical_name"]
                try:
                    aliases = json.loads(r["aliases"]) if r["aliases"] else []
                except (json.JSONDecodeError, TypeError):
                    aliases = []
                if shorter.lower() not in [a.lower() for a in aliases]:
                    aliases.append(shorter)
                db.run(
                    "UPDATE kg_entities SET canonical_name = ?, aliases = ?, "
                    "mention_count = mention_count + 1, last_seen = ?, "
                    "entity_type = CASE WHEN entity_type = 'unknown' THEN ? ELSE entity_type END "
                    "WHERE id = ?",
                    (longer, json.dumps(aliases), timestamp,
                     entity_type, r["id"]),
                )
                return r["id"]

    # Create new entity
    eid = db.run(
        "INSERT INTO kg_entities (canonical_name, entity_type, aliases, first_seen, last_seen, mention_count) "
        "VALUES (?, ?, '[]', ?, ?, 1)",
        (name, entity_type, timestamp, timestamp),
    )
    return eid if eid else None


def extract_entities_from_brief(db, brief: str, original_text: str, source_type: str, source_id: int, timestamp: int):
    """Extract entities from a brief + original text using the LLM, store in KG."""
    combined = original_text if original_text else brief
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": SYNTH_MODEL,
                "messages": [
                    {"role": "system", "content":
                        "Extract named entities from the text. For each entity, provide:\n"
                        "- name: the most specific name used\n"
                        "- type: one of person, technology, service, project, concept, "
                        "organization, device, location, protocol, model, "
                        "software, language, network, biological, financial, "
                        "event, dataset, hardware\n\n"
                        "Reply ONLY with a JSON array. Example:\n"
                        '[{"name": "BERT", "type": "model"}, {"name": "AGX Orin", "type": "hardware"}, {"name": "federated learning", "type": "concept"}]\n\n'
                        "Rules:\n"
                        "- Only extract entities that are specific and named (not generic words)\n"
                        "- Prefer the full proper name (\"Jetson Orin Nano\" not \"Jetson\")\n"
                        "- Skip pronouns, generic terms (\"system\", \"tool\", \"data\")\n"
                        "- Maximum 8 entities per extraction"},
                    {"role": "user", "content": safe_truncate(combined, 800)},
                ],
                "stream": False,
                "options": {"num_predict": 1024, "temperature": 0.5},
            },
            timeout=180,
        )
        if r.status_code != 200:
            return

        raw = r.json().get("message", {}).get("content", "").strip()
        # Try to parse JSON from the response (may have markdown fencing)
        raw = re.sub(r'^```json\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)
        # Find the JSON array in the response
        match = re.search(r'\[.*\]', raw, re.DOTALL)
        if not match:
            return
        entities = json.loads(match.group())

        extracted = 0
        for ent in entities[:8]:
            name = ent.get("name", "").strip()
            etype = ent.get("type", "unknown").strip()
            if not name or len(name) < 2:
                continue
            # Filter ghost entities
            if name.lower() in KG_ENTITY_BLOCKLIST:
                continue
            eid = find_or_create_entity(db, name, etype, timestamp)
            if eid:
                db.run(
                    "INSERT INTO kg_mentions (entity_id, source_type, source_id, context, timestamp) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (eid, source_type, source_id, safe_truncate(original_text, 200), timestamp),
                )
                extracted += 1

        if extracted:
            log(f"  KG: extracted {extracted} entities")

    except (json.JSONDecodeError, KeyError) as e:
        log(f"  KG: parse error — {e}")
    except Exception as e:
        log(f"  KG: extraction error — {e}")


# ═══════════════════════════════════════════════════════════════════
#  LLM Synthesis
# ═══════════════════════════════════════════════════════════════════

CONTEXT_FILTER_TERMS = {
    "chronicle", "homeforge", "crossref", "memory architecture", "capsule",
    "knowledge capsule", "seed agent", "sentinel", "provocateur", "intern",
    "scratch_pad", "activity_feed", "processed.db", "hermes3", "nate-phi4",
    "embedding gap", "novelty score", "cognitive state", "heartbeat",
}


def synthesize(input_text: str, related: list, url_content: str = None, search_results: list = None) -> Optional[str]:
    """Ask the local LLM to synthesize a research brief."""
    context_parts = []
    if related:
        # Filter out capsules containing Chronicle system terms
        clean_related = []
        for item in related:
            sim, cid, restatement, topic = item
            text_lower = restatement.lower() if restatement else ""
            if not any(term in text_lower for term in CONTEXT_FILTER_TERMS):
                clean_related.append(item)
        if clean_related:
            context_parts.append("RELATED MEMORIES:")
            for sim, cid, restatement, topic in clean_related:
                context_parts.append(f"  [{sim:.2f}] #{cid}: {safe_truncate(restatement, 200)}")
    if url_content:
        context_parts.append(f"\nURL CONTENT:\n{safe_truncate(url_content, 1000)}")
    if search_results:
        context_parts.append("\nWEB SEARCH RESULTS:")
        for r in search_results:
            context_parts.append(f"  [{r['title']}] {safe_truncate(r['body'], 200)}\n  {r['url']}")

    context = "\n".join(context_parts) if context_parts else "No related context found."

    system_prompt = (
        "You are a research intern. "
        "Your job is to prepare a brief summary when new information arrives. "
        "Given the input and any related context, URL content, or web search results, write a concise research brief:\n"
        "1. What is this about? (1 sentence)\n"
        "2. Why does this matter? What's the key insight or implication? (1-2 sentences)\n"
        "3. Key findings from web search, if any (1-2 sentences)\n"
        "4. What questions or threads does this open? (1-2 bullets)\n"
        "Focus on the subject matter itself. Do NOT relate findings back to any specific project or system. Be direct and useful. No filler."
    )
    user_msg = f"NEW INPUT:\n{safe_truncate(input_text, 500)}\n\n{context}"

    # Try 8B first (fast, ~20s)
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": SYNTH_MODEL_FAST,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_msg},
                ],
                "stream": False,
                "options": {"num_predict": 256, "temperature": 0.3},
            },
            timeout=120,
        )
        if r.status_code == 200:
            brief = r.json().get("message", {}).get("content", "")
            if brief and not _is_self_referential(brief, input_text):
                log(f"  Brief ready via 8B ({len(brief)} chars)")
                return brief
            if brief:
                leaked = _leaked_terms(brief, input_text)
                log(f"  8B self-referential (leaked: {leaked}) — escalating to 32B")
    except Exception as e:
        log(f"  8B synthesis error: {e} — escalating to 32B")

    # Escalate to 32B (slow but clean) — retry once if busy
    _32b_payload = {
        "model": SYNTH_MODEL_DEEP,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_msg},
        ],
        "stream": False,
        "options": {"num_predict": 1024, "temperature": 0.5},
    }
    for _attempt in range(2):
        try:
            r = requests.post(
                f"{OLLAMA_URL}/api/chat",
                json=_32b_payload,
                timeout=600,
            )
            if r.status_code == 200:
                brief = r.json().get("message", {}).get("content", "")
                # Strip <think>...</think> chain-of-thought from Groq/qwen3
                if brief and '</think>' in brief:
                    brief = brief.split('</think>')[-1].strip()
                elif brief and brief.startswith('<think>'):
                    brief = ''
                if brief and _is_self_referential(brief, input_text):
                    leaked = _leaked_terms(brief, input_text)
                    log(f"  32B also self-referential (leaked: {leaked})")
                    log(f"    Preview: {brief[:150].replace(chr(10), ' ')}")
                    return None
                if brief:
                    log(f"  Brief ready via 32B ({len(brief)} chars)")
                return brief
            else:
                log(f"  32B returned status {r.status_code} (attempt {_attempt + 1}/2)")
                if _attempt == 0:
                    import time as _time
                    log(f"  Retrying 32B in 90s...")
                    _time.sleep(90)
                    continue
        except Exception as e:
            log(f"  32B synthesis error: {e} (attempt {_attempt + 1}/2)")
            if _attempt == 0:
                import time as _time
                log(f"  Retrying 32B in 90s...")
                _time.sleep(90)
                continue
        break
    return None


# Internal terms that signal the model is writing about Chronicle, not the input
_INTERNAL_TERMS = [
    "chronicle", "homeforge", "canister", "capsule", "seed agent",
    "swarm", "ollama", "hermes3", "nate-phi4", "novelty=", "crossref",
    "scratch pad", "scratch_pad", "memory architecture", "memory pipeline",
    "cognitive state", "embedding gap",
]


def _leaked_terms(brief: str, input_text: str) -> list:
    """Return internal terms present in brief but NOT in input."""
    brief_lower = brief.lower()
    input_lower = input_text.lower()
    return [t for t in _INTERNAL_TERMS if t in brief_lower and t not in input_lower]


def _is_self_referential(brief: str, input_text: str) -> bool:
    """Reject briefs where the model wrote about Chronicle internals instead of the input."""
    return len(_leaked_terms(brief, input_text)) >= 1


# ═══════════════════════════════════════════════════════════════════
#  Input Watchers
# ═══════════════════════════════════════════════════════════════════

def find_new_inputs(db: DB) -> list:
    """Find new Nate-originated inputs to research."""
    inputs = []

    # 1. New captures and greetings from activity_feed
    wm_af = int(db.get_state("wm_activity_feed", "0"))
    rows = db.query(
        "SELECT id, source, activity_type, title, content, created_at FROM activity_feed "
        "WHERE id > ? AND source IN ({}) AND activity_type IN ({}) "
        "ORDER BY id ASC LIMIT 5".format(
            ",".join(f"'{s}'" for s in NATE_SOURCES),
            ",".join(f"'{t}'" for t in NATE_TYPES),
        ),
        (wm_af,),
    )
    for r in rows:
        inputs.append({
            "id": f"af:{r['id']}",
            "text": f"{r.get('title', '')} {r.get('content', '')}".strip(),
            "source": f"{r['source']}:{r['activity_type']}",
            "timestamp": r["created_at"],
        })
    if rows:
        db.set_state("wm_activity_feed", str(rows[-1]["id"]))

    # 2. New Seed thinks — novelty-flagged items worth digging into
    wm_seed = int(db.get_state("wm_seed_thinks", "0"))
    rows = db.query(
        "SELECT id, source, activity_type, title, content, metadata, created_at FROM activity_feed "
        "WHERE id > ? AND source IN ({}) AND activity_type IN ({}) "
        "ORDER BY id ASC LIMIT 5".format(
            ",".join(f"'{s}'" for s in SEED_SOURCES),
            ",".join(f"'{t}'" for t in SEED_TYPES),
        ),
        (wm_seed,),
    )
    for r in rows:
        title = r.get("title", "")
        # Skip ALL seed thinks while 8B model is self-referential (see SKIP_ALL_SEED_THINKS)
        if SKIP_ALL_SEED_THINKS:
            log(f"  SKIP seed-think (8B self-ref bypass): {title[:80]}")
            continue
        # Legacy: skip specific internal activity sources that create feedback loops
        if any(src in title for src in SEED_SKIP_SOURCES):
            log(f"  SKIP seed-think (internal activity): {title[:80]}")
            continue
        # Parse metadata to get original observation content, novelty, and routing_log_id
        original_content = None
        novelty = 0.0
        routing_log_id = None
        if r.get("metadata"):
            try:
                meta = json.loads(r["metadata"])
                original_content = meta.get("original_content")
                novelty = meta.get("novelty", 0.0)
                routing_log_id = meta.get("routing_log_id")
            except (json.JSONDecodeError, TypeError):
                pass
        # Also try to extract novelty from title if not in metadata
        if novelty == 0.0 and r.get("title"):
            m = re.search(r'novelty=([\d.]+)', r["title"])
            if m:
                novelty = float(m.group(1))
        inputs.append({
            "id": f"seed:{r['id']}",
            "text": f"{r.get('title', '')} {r.get('content', '')}".strip(),
            "source": f"{r['source']}:{r['activity_type']}",
            "timestamp": r["created_at"],
            "original_content": original_content,
            "novelty": novelty,
            "routing_log_id": routing_log_id,
        })
    if rows:
        db.set_state("wm_seed_thinks", str(rows[-1]["id"]))

    # 3. New operator notes from scratch_pad
    wm_sp = int(db.get_state("wm_scratch_pad", "0"))
    rows = db.query(
        "SELECT id, content, category, created_at FROM scratch_pad "
        "WHERE id > ? AND resolved = 0 AND category IN ({}) "
        "ORDER BY id ASC LIMIT 5".format(
            ",".join(f"'{c}'" for c in OPERATOR_CATS),
        ),
        (wm_sp,),
    )
    for r in rows:
        inputs.append({
            "id": f"sp:{r['id']}",
            "text": r["content"],
            "source": f"note:{r['category']}",
            "timestamp": r["created_at"],
        })
    if rows:
        db.set_state("wm_scratch_pad", str(rows[-1]["id"]))

    return inputs



def find_explore_candidate(db: DB):
    """Find a recent feed article that hasn't been researched yet.

    The Intern becomes proactive: instead of only reacting to Seed thinks,
    it picks up interesting papers from the feed agent and explores them
    on its own initiative. This is the swarm — cheap agents exploring
    in parallel, accumulating material for Opus sessions.
    """
    # Get articles from feed_articles that we haven't explored yet
    # Use a state key to track what we've explored
    explored_ids = set()
    explored_raw = db.get_state("explored_feed_ids", "")
    if explored_raw:
        explored_ids = set(explored_raw.split(",")[-200:])  # keep last 200

    import time as _time
    cutoff = int(_time.time()) - (EXPLORE_MAX_AGE_HOURS * 3600)

    rows = db.query(
        "SELECT id, source, title FROM feed_articles "
        "WHERE posted_at > datetime(?, 'unixepoch') "
        "ORDER BY RANDOM() LIMIT 10",
        (cutoff,),
    )

    # Skip obviously non-substantive articles (promo, product reviews, deals)
    _SKIP_TITLE_TERMS = [
        "promo code", "coupon", "deal:", "deals", "save %", "save $",
        "% off", "$ off", "discount", "price drop", "sale",
        "review:", "just dropped in price", "how to buy",
        "best deals", "gift guide", "top picks", "shopping",
        "we checked the price", "worth buying",
    ]

    for r in rows:
        if r["id"] not in explored_ids:
            title_lower = (r["title"] or "").lower()
            if any(term in title_lower for term in _SKIP_TITLE_TERMS):
                continue
            return r

    return None


def mark_explored(db: DB, article_id: str):
    """Mark a feed article as explored."""
    explored_raw = db.get_state("explored_feed_ids", "")
    ids = explored_raw.split(",") if explored_raw else []
    ids.append(article_id)
    # Keep only last 200
    db.set_state("explored_feed_ids", ",".join(ids[-200:]))



# ═══════════════════════════════════════════════════════════════════
#  Research Pipeline
# ═══════════════════════════════════════════════════════════════════

def _write_seed_feedback(db: DB, inp: dict, score: float):
    """Write feedback_score back to seed_routing_log if this input came from seed."""
    if not inp.get("id", "").startswith("seed:"):
        return
    # Try to get routing_log_id from metadata (new path)
    routing_log_id = inp.get("routing_log_id")
    if routing_log_id:
        db.run(
            "UPDATE seed_routing_log SET feedback_score = ? WHERE id = ?",
            (score, routing_log_id),
        )
        log(f"  Feedback → seed_routing_log[{routing_log_id}] = {score:.2f}")
        return
    # Fallback: match on timestamp proximity (for entries before routing_log_id was added)
    af_id = inp["id"].split(":")[-1]
    row = db.query_one(
        "SELECT af.created_at FROM activity_feed af WHERE af.id = ?", (af_id,)
    )
    if row:
        ts = row["created_at"]
        route_row = db.query_one(
            "SELECT id FROM seed_routing_log WHERE timestamp BETWEEN ? AND ? "
            "ORDER BY ABS(timestamp - ?) LIMIT 1",
            (ts - 5, ts + 5, ts),
        )
        if route_row:
            db.run(
                "UPDATE seed_routing_log SET feedback_score = ? WHERE id = ?",
                (score, route_row["id"]),
            )
            log(f"  Feedback → seed_routing_log[{route_row['id']}] = {score:.2f} (ts match)")


def research_input(db: DB, inp: dict):
    """Full research pipeline for a single input. Each step visible on dashboard.
    Returns True if a brief was produced, False otherwise."""
    text = inp["text"]
    source = inp["source"]
    short = safe_truncate(text, 80)

    # Skip items with no real content (e.g. "Phone capture queued Type: mixed")
    if not has_substance(text) and not extract_urls(text):
        log(f"  Skipping low-content: [{source}] {short}")
        _write_seed_feedback(db, inp, 0.1)
        return False

    # Step 1: Announce pickup
    log(f"  Picking up: [{source}] {short}")
    db.log_activity("pickup", f"Researching: {short}", f"Source: {source}\n{safe_truncate(text, 500)}")

    # Step 2: Embed and search related capsules
    vec = embed_text(text)
    related = []
    if vec:
        related = search_related_capsules(db, vec)
        if related:
            connections = "\n".join(
                f"  [{s:.2f}] #{cid}: {safe_truncate(r, 100)}"
                for s, cid, r, _ in related
            )
            log(f"  Found {len(related)} related capsules")
            db.log_activity(
                "search",
                f"Found {len(related)} related memories",
                f"Searching for: {short}\n\nConnections:\n{connections}",
            )
        else:
            db.log_activity("search", "No related memories found", f"Searched for: {short}")

    # Step 3: Fetch URLs if present
    urls = extract_urls(text)
    url_content = None
    if urls:
        url = urls[0]  # Just the first one
        log(f"  Fetching URL: {url}")
        db.log_activity("fetch", f"Fetching: {safe_truncate(url, 80)}", f"URL: {url}")
        url_content = fetch_url_summary(url)
        if url_content:
            db.log_activity(
                "fetch_done",
                f"Fetched {len(url_content)} chars",
                f"URL: {url}\n\nExcerpt: {safe_truncate(url_content, 300)}",
            )

    # Step 4: Web search for more context
    search_results = []
    # For feed-explore articles, use the title directly — don't ask the 8B LLM
    # to "extract" a query (it self-references due to LoRA attractor)
    if source.startswith("feed-explore:"):
        search_query = text[:100].strip()
    else:
        search_query = extract_search_query(text, inp.get("original_content"))
    if search_query:
        log(f"  Searching web: {search_query}")
        db.log_activity("web_search", f"Searching: {safe_truncate(search_query, 60)}", f"Query: {search_query}")
        search_results = web_search(search_query)
        if search_results:
            results_text = "\n".join(
                f"  [{r['title']}] {safe_truncate(r['body'], 100)}"
                for r in search_results
            )
            log(f"  Found {len(search_results)} web results")
            db.log_activity(
                "web_results",
                f"Found {len(search_results)} web results",
                f"Query: {search_query}\n\nResults:\n{results_text}",
            )

    # Step 5: Synthesize brief
    log(f"  Synthesizing brief...")
    brief = synthesize(text, related, url_content, search_results)
    if brief:
        log(f"  Brief ready ({len(brief)} chars)")
        db.log_activity(
            "brief",
            f"Research brief: {short}",
            brief,
            json.dumps({"input_id": inp["id"], "related_count": len(related), "had_url": bool(url_content), "web_results": len(search_results)}),
        )
        # Store in scratch_pad for Opus sessions
        db.run(
            "INSERT INTO scratch_pad (content, category, priority, resolved, created_at, updated_at) "
            "VALUES (?, 'research', 5, 0, ?, ?)",
            (f"[Intern] {short}\n\n{brief}", now_ts(), now_ts()),
        )
        # Store as canister capsule so Keeper can compost connections
        # Pass embedding so it's stored on-chain before reinforce runs
        # REMOVED: was creating duplicate capsules for papers already stored by feeds agent
        # post_capsule_to_canister(short, brief, source, embedding=vec)
        # Extract entities for knowledge graph
        extract_entities_from_brief(
            db, brief, inp.get("original_content") or text,
            "intern_brief", _safe_int(inp["id"].split(":")[-1]) if ":" in inp["id"] else 0,
            inp["timestamp"],
        )
        # Maybe post to Nostr if this is genuinely interesting
        novelty = inp.get("novelty", 0.0)
        # maybe_post_to_nostr(db, brief, inp.get("original_content") or short, novelty)  # DISABLED: only opus posts to Nostr
        # Feedback: brief produced + capsule stored = 0.8
        _write_seed_feedback(db, inp, 0.8)
        return True
    else:
        db.log_activity("brief_failed", f"Could not synthesize brief for: {short}", "Synthesis failed — model may be busy")
        _write_seed_feedback(db, inp, 0.2)
        return False


# ═══════════════════════════════════════════════════════════════════
#  Main Loop
# ═══════════════════════════════════════════════════════════════════

def main():
    log("═══ Research Intern starting ═══")
    log(f"DB: {DB_PATH}")
    log(f"Ollama: {OLLAMA_URL} (model: {SYNTH_MODEL})")
    log(f"Cycle: {CYCLE_INTERVAL}s")

    db = DB(DB_PATH)

    # Initialize watermarks to current max IDs (don't process old stuff)
    if db.get_state("wm_activity_feed") == "0":
        row = db.query_one("SELECT MAX(id) as m FROM activity_feed")
        if row and row["m"]:
            db.set_state("wm_activity_feed", str(row["m"]))
            log(f"  Initialized activity_feed watermark: {row['m']}")

    if db.get_state("wm_seed_thinks") == "0":
        row = db.query_one("SELECT MAX(id) as m FROM activity_feed WHERE source='seed' AND activity_type='think'")
        if row and row["m"]:
            db.set_state("wm_seed_thinks", str(row["m"]))
            log(f"  Initialized seed_thinks watermark: {row['m']}")

    if db.get_state("wm_scratch_pad") == "0":
        row = db.query_one("SELECT MAX(id) as m FROM scratch_pad")
        if row and row["m"]:
            db.set_state("wm_scratch_pad", str(row["m"]))
            log(f"  Initialized scratch_pad watermark: {row['m']}")

    # Graceful shutdown
    running = True
    def _stop(sig, frame):
        nonlocal running
        log("Shutting down...")
        running = False
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    cycle = 0
    researched = 0

    while running:
        cycle += 1
        try:
            inputs = find_new_inputs(db)

            for inp in inputs:
                research_input(db, inp)
                researched += 1

            # Proactive exploration: pick a feed paper and research it
            if cycle % EXPLORE_EVERY == 0 and not inputs:
                candidate = find_explore_candidate(db)
                if candidate:
                    log(f"  Exploring feed: [{candidate['source']}] {safe_truncate(candidate['title'], 60)}")
                    explore_inp = {
                        "id": f"explore:{candidate['id']}",
                        "text": candidate["title"],
                        "source": f"feed-explore:{candidate['source']}",
                        "timestamp": now_ts(),
                    }
                    success = research_input(db, explore_inp)
                    if success:
                        mark_explored(db, candidate["id"])
                    else:
                        log(f"  Explore failed — will retry later: {safe_truncate(candidate['title'], 60)}")
                    researched += 1

            if cycle % 20 == 0:
                log(f"Stats @ cycle {cycle}: {researched} items researched")

        except Exception as e:
            log(f"Cycle error: {e}")

        time.sleep(CYCLE_INTERVAL)

    db.close()
    log("═══ Research Intern stopped ═══")


if __name__ == "__main__":
    main()
