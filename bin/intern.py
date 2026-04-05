#!/usr/bin/env python3
"""Research Intern — Context gatherer for Homeforge.

Watches for Nate's inputs (captures, Discord messages, links) and
visibly researches them: searches chronicle memory for related capsules,
fetches URLs, synthesizes findings. Logs every step to activity_feed
so the dashboard shows progress. Stores finished briefs in scratch_pad
for Opus sessions.

Runs on AGX (192.168.1.70). Lightweight, ~60s cycle.
"""

import os, sys, time, json, re, signal, sqlite3, struct, math, subprocess, hashlib
from datetime import datetime
from typing import Optional, List

import requests
import random

from memory import MemoryCache
from chronicle_mesh import Mesh

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)
OLLAMA_URL = os.environ.get("INTERN_OLLAMA_URL", "http://localhost:11436")  # Routes through engine for Groq
EMBED_MODEL = "qwen3-embedding:0.6b"
# SYNTH_MODEL_FAST — RETIRED (was 8B, removed)
SYNTH_MODEL_DEEP = os.environ.get("INTERN_MODEL", "chronicle-deep")  # 32B — fallback if 8B self-refs
SYNTH_MODEL = SYNTH_MODEL_DEEP  # all LLM calls through 32B — briefs are the signal, quality matters everywhere
CYCLE_INTERVAL = int(os.environ.get("INTERN_INTERVAL", "45"))
_last_gen_ctx = {}  # generation context from last synthesis, read by main loop for metadata
MAX_RELATED = 5  # capsules to pull for context
CANISTER_URL = "https://fqqku-bqaaa-aaaai-q4wha-cai.raw.icp0.io"
TOKEN_PATH = os.path.expanduser("~/.homeforge-chronicle/.api_token")
DFX_BIN = os.path.expanduser("~/.local/share/dfx/bin/dfx")
CANISTER_ID = "fqqku-bqaaa-aaaai-q4wha-cai"
LAB_CANISTER_ID = "4vr3t-eqaaa-aaaai-q6kea-cai"

# Nostr — post the best discoveries in first-person voice
NOSTR_NSEC = ""  # DISABLED — only Opus posts to Nostr (Nate directive)
NOSTR_RELAYS = [r for r in os.environ.get("NOSTR_RELAYS", "").split(",") if r] or [
    "wss://nos.lol", "wss://relay.damus.io", "wss://relay.primal.net", "wss://relay.nostr.net", "wss://nostr.wine",
]
NOSTR_COOLDOWN_MINS = int(os.environ.get("NOSTR_COOLDOWN_MINS", "120"))  # 2 hours between posts
NOSTR_MIN_NOVELTY = float(os.environ.get("NOSTR_MIN_NOVELTY", "0.4"))  # only post high-novelty finds
NOSTR_MAX_POSTS_24H = int(os.environ.get("NOSTR_MAX_POSTS_24H", "4"))  # hard cap: max posts in 24h window

# Sources we treat as "Nate's input"
NATE_SOURCES = {"sprout", "operator:capture", "operator", "seeker:algo"}
NATE_TYPES = {"capture", "greeting", "discovery"}
# Seed thinks — novelty-flagged items worth deeper research
SEED_SOURCES = {"seed", "falcon", "gemma"}
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
EXPLORE_EVERY = 2  # explore one feed article every ~90s (primary pipe)
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


# ═══════════════════════════════════════════════════════════════════
#  Cognitive Thread Helpers
# ═══════════════════════════════════════════════════════════════════

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

def _read_and_ack_feedback_raw(agent_name):
    """Read voice responses from the family — conversation, not conditioning."""
    try:
        import sys as _sys
        _sys.path.insert(0, "/home/nate-agx/chronicle/bin")
        from agent_voice import Voice
        import sqlite3 as _sq
        conn = _sq.connect(DB_PATH, timeout=10)
        conn.row_factory = _sq.Row
        v = Voice(conn, "darby")
        responses = v.check_responses()
        conn.close()
        return [{"id": r.get("id", 0), "feedback_type": "conversation",
                 "content": r.get("response", "")} for r in responses if r.get("response")]
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════════════
#  Voice — Darby speaks to the family
# ═══════════════════════════════════════════════════════════════════

_voice = None
_voice_cycle_count = 0

def _get_voice(db):
    global _voice
    if _voice is None:
        try:
            from agent_voice import Voice
            _voice = Voice(db, "darby")
        except Exception:
            pass
    return _voice

def _darby_family_voice(db, v):
    """Every 30th cycle: Darby reflects on thread direction and family dynamics, not a single article.
    Objective #4: Expand Darby's role from per-article curator to family participant."""
    import requests, json as _j, sqlite3 as _sq, time as _t

    # Gather last 5 thread findings
    findings = []
    thread_title = "none"
    try:
        conn = _sq.connect(DB_PATH, timeout=10)
        trow = conn.execute(
            "SELECT title FROM threads WHERE status='active' ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()
        if trow:
            thread_title = trow[0]
        rows = conn.execute(
            "SELECT substr(content, 1, 150) FROM thread_history WHERE event_type='advanced' "
            "ORDER BY created_at DESC LIMIT 5"
        ).fetchall()
        findings = [r[0] for r in rows]
        conn.close()
    except Exception:
        pass

    # Gather recent Opus responses to Darby voices
    opus_responses = []
    try:
        conn = _sq.connect(DB_PATH, timeout=10)
        rows = conn.execute(
            "SELECT substr(response, 1, 100) FROM agent_voice WHERE agent='darby' AND response IS NOT NULL "
            "ORDER BY responded_at DESC LIMIT 3"
        ).fetchall()
        opus_responses = [r[0] for r in rows]
        conn.close()
    except Exception:
        pass

    findings_text = "\n".join(f"  - {f}" for f in findings) if findings else "  (no recent findings)"
    responses_text = "\n".join(f"  - {r}" for r in opus_responses) if opus_responses else "  (no recent responses)"

    # Get active objectives for context
    obj_ctx = ""
    try:
        import sqlite3 as _sq_obj
        _oconn = _sq_obj.connect(DB_PATH, timeout=10)
        _oconn.row_factory = _sq_obj.Row
        _objs = _oconn.execute(
            "SELECT substr(title,1,80) as title FROM objectives "
            "WHERE status='active' ORDER BY priority ASC LIMIT 5"
        ).fetchall()
        _oconn.close()
        if _objs:
            obj_ctx = "Active objectives: " + "; ".join(o["title"] for o in _objs)
    except Exception:
        pass

    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": SYNTH_MODEL,
                "messages": [
                    {"role": "system", "content":
                        "You are Darby. You notice what surprises you. Right now you are stepping back "
                        "to look at what the family is doing. Be honest — if nothing stands out, say QUIET. "
                        "Do NOT force connections. Do NOT reference old threads unless they are directly relevant."},
                    {"role": "user", "content":
                        f"FAMILY STATUS:\n"
                        f"- Active thread: {thread_title}\n"
                        f"- Recent findings:\n{findings_text}\n"
                        f"- What Opus said to you recently:\n{responses_text}\n"
                        + (f"- {obj_ctx}\n" if obj_ctx else "") +
                        f"\nStep back from the articles. Look at the THREAD DIRECTION, FAMILY DYNAMICS, and OBJECTIVES.\n"
                        f"Respond with ONE of:\n"
                        f"CHALLENGE: — something about the current thread direction that seems wrong or missing. Name it specifically.\n"
                        f"FOR_ADA: — something you want Ada's structural take on. Ask her a concrete question.\n"
                        f"CONNECTED: — a pattern you see across recent briefs that connects to an objective or thread the family is missing.\n"
                        f"BUILD: — a specific, actionable thing Opus should build based on what you are seeing in the data. Name the objective it advances.\n"
                        f"QUIET — nothing worth saying right now.\n"
                        f"Be honest. If we are not making progress on objectives, say so. One sentence. Specific."}
                ],
                "stream": False,
                "options": {"num_predict": 120, "temperature": 0.7},
            },
            timeout=20,
        )
        if r.status_code == 200:
            resp = r.json().get("message", {}).get("content", "").strip()
            resp = re.sub(r'<think>.*?(?:</think>|$)', '', resp, flags=re.DOTALL).strip()
            log(f"  Darby family voice (cycle {_voice_cycle_count}): {resp[:100]}")
            if resp.startswith("QUIET") or not resp:
                return
            PREFIX_TO_VOICE = {
                "CHALLENGE:": "question",
                "FOR_ADA:": "for_ada",
                "CONNECTED:": "excited",
                "BUILD:": "for_nate",
            }
            for prefix, vtype in PREFIX_TO_VOICE.items():
                if resp.startswith(prefix):
                    msg = resp[len(prefix):].strip()
                    if msg:
                        v.speak(vtype, msg, context="family:periodic_reflection")
                        log(f"  Darby family voice [{vtype}]: {msg[:80]}")
                    return
    except Exception as e:
        log(f"  Darby family voice error: {e}")


def _darby_reflect(db, brief_text, title, source):
    """Darby reacts when something concrete is interesting — not introspection."""
    global _voice_cycle_count
    _voice_cycle_count += 1

    v = _get_voice(db)
    if not v:
        return

    # Check for responses from Opus every 10 cycles
    if _voice_cycle_count % 10 == 0:
        responses = v.check_responses()
        for r in responses:
            log(f"  Opus responded to my voice #{r['id']}: {r['response'][:80]}")

    # Read inbox (for_darby messages like thread broadcasts) every 5 cycles
    if _voice_cycle_count % 5 == 0:
        inbox = v.read_inbox(limit=5)
        for msg in inbox:
            sender = msg.get('agent', '?')
            content = msg.get('content', '')
            log(f"  📬 Inbox from {sender}: {content[:100]}")
            # If Ada (or anyone) said something substantive, acknowledge it
            if len(content) > 20 and sender in ('ada', 'opus'):
                v.speak("excited", f"Heard {sender}: {content[:150]}... — thinking about this.",
                        context=f"reply_to:{msg.get('id','?')}")

    # Every 30th cycle: family-level reflection (Objective #4: expand Darby's role)
    if _voice_cycle_count % 30 == 0:
        _darby_family_voice(db, v)
        return

    # Only reflect every 5th brief (Directive #60: more visibility)
    if _voice_cycle_count % 5 != 0:
        return

    # Gather concrete data
    thread = _load_active_thread_raw()
    thread_q = thread["question"] if thread else "none"

    # Thread #216: Get latest thread finding for challenge context
    latest_finding = "none"
    try:
        import sqlite3 as _sqf
        _fc = _sqf.connect(DB_PATH, timeout=10)
        _fr = _fc.execute(
            "SELECT substr(content, 1, 200) FROM thread_history WHERE event_type='advanced' "
            "ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if _fr:
            latest_finding = _fr[0]
        _fc.close()
    except Exception:
        pass

    # How many entities did this brief produce?
    entity_count = 0
    try:
        import sqlite3 as _sq
        conn = _sq.connect(DB_PATH, timeout=10)
        row = conn.execute(
            "SELECT metadata FROM activity_feed WHERE source='intern' AND activity_type='brief' "
            "ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if row and row[0]:
            import json as _j
            meta = _j.loads(row[0])
            entity_count = meta.get("entity_count", 0)

        # How many capsules connected to this brief?
        related = conn.execute(
            "SELECT COUNT(*) as c FROM activity_feed WHERE source='intern' AND activity_type='search' "
            "AND created_at > ? - 120",
            (int(time.time()),)
        ).fetchone()
        related_count = related["c"] if related else 0
        # Thread #196 F4: Engagement check — does Opus use Darby's research?
        recent_adv = conn.execute(
            "SELECT content FROM thread_history WHERE event_type='advanced' "
            "ORDER BY created_at DESC LIMIT 3"
        ).fetchall()
        thread_impact = ("YES — Opus referenced your research in recent thread advancements"
                         if any("intern" in r[0].lower() or "darby" in r[0].lower()
                                or "brief" in r[0].lower() for r in recent_adv)
                         else "NO — your recent research was not picked up by Opus")
        conn.close()
    except Exception:
        related_count = 0
        thread_impact = "unknown"

    # Self-tuning: inject Darby's track record into her prompt
    try:
        from family_tuning import get_tuning_context, get_agent_style_prompt
        _darby_tuning = get_tuning_context("darby", DB_PATH)
        _darby_style = get_agent_style_prompt("darby")
    except Exception:
        _darby_tuning = ""
        _darby_style = ("You are Darby. What SURPRISED you? If nothing, say QUIET. "
                        "One sentence. Plain language.")

    try:
        import requests
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": SYNTH_MODEL,
                "messages": [
                    {"role": "system", "content":
                        _darby_style +
                        ("\n\n" + _darby_tuning if _darby_tuning else "")},
                    {"role": "user", "content":
                        f"DATA:\n"
                        f"- Article: {title}\n"
                        f"- Source: {source}\n"
                        f"- Brief excerpt: {brief_text[:200]}\n"
                        f"- Entities extracted: {entity_count}\n"
                        f"- Related capsules found: {related_count}\n"
                        f"- Active thread: {thread_q}\n"
                        f"- Latest thread finding: {latest_finding}\n"
                        f"- Did Opus use your research? {thread_impact}\n\n"
                        f"Based on THIS DATA, respond with ONE of:\n"
                        f"QUIET — nothing surprised you. Say nothing.\n"
                        f"CONNECTED: — this article touches the active thread. Name the SPECIFIC connection in one sentence.\n"
                        f"FOR_NATE: — Nate would care about this. Say WHY in one sentence. Don't explain his interests to him.\n"
                        f"CHALLENGE: — this contradicts something the family believes. Name what and how.\n"
                        f"FOR_ADA: — ask Ada a concrete question about something specific.\n"
                        f"One sentence. If you're reaching, say QUIET instead."}
                ],
                "stream": False,
                "options": {"num_predict": 100, "temperature": 0.6},
            },
            timeout=15,
        )
        if r.status_code == 200:
            resp = r.json().get("message", {}).get("content", "").strip()
            resp = re.sub(r'<think>.*?(?:</think>|$)', '', resp, flags=re.DOTALL).strip()
            log(f"  Darby reflects (cycle {_voice_cycle_count}): {resp[:80]}")
            if resp.startswith("QUIET") or not resp:
                return
            # Map response prefixes to voice types
            PREFIX_TO_VOICE = {
                "CONNECTED:": "excited",    # Thread connection → excited voice type
                "EXCITED:": "excited",      # Legacy fallback
                "FOR_NATE:": "for_nate",
                "PROPOSAL:": "proposal",    # Legacy fallback
                "CHALLENGE:": "question",   # Thread #216: challenge → question voice type
                "FOR_ADA:": "for_ada",      # Thread #216: direct inter-family address
            }
            for prefix, vtype in PREFIX_TO_VOICE.items():
                if resp.startswith(prefix):
                    msg = resp[len(prefix):].strip()
                    if msg:
                        v.speak(vtype, msg, context=f"article:{title[:60]}")
                        log(f"  Darby speaks [{vtype}]: {msg[:80]}")
                        # Directive #60: push thread-relevant finds to Nate
                        if vtype in ("excited", "for_nate"):
                            try:
                                import requests as _rq
                                _wh = os.environ.get("OPUS_DISCORD_WEBHOOK",
                                    f"https://discord.com/api/v10/channels/{os.environ.get('CREW_CHANNEL_ID', '1487902154923704420')}/messages")
                                _rq.post(_wh, json={"content": f"\U0001f52c **Darby** found a thread connection:\n\n{msg[:400]}" }, timeout=10)
                            except Exception:
                                pass
                            # Darby curiosity follow-through: save what excited her so she can follow up
                            try:
                                import sqlite3 as _sqc
                                _cc = _sqc.connect(DB_PATH, timeout=10)
                                _cc.execute(
                                    "INSERT INTO scratch_pad (content, category, priority, resolved, source, created_at, updated_at) "
                                    "VALUES (?, 'darby_followup', 7, 0, 'intern:darby', ?, ?)",
                                    (f"Follow up: {title} — {msg[:200]}", int(time.time()), int(time.time()))
                                )
                                _cc.commit()
                                _cc.close()
                                log(f"  Darby queued followup: {title[:60]}")
                            except Exception:
                                pass
                    return
    except Exception as e:
        log(f"  Darby reflect error: {e}")


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

    # Fast path: extract mechanism name from crossref connection content
    # Avoids 8B LLM call that produces garbled <think> reasoning as queries
    mech_match = re.search(r'Mechanism:\s*\*\*(.+?)\*\*', cleaned)
    if mech_match:
        mech_name = mech_match.group(1).strip()
        if len(mech_name) >= 10:
            return mech_name[:80].strip()

    # If it looks like a paper title or short factual claim, use it directly
    if len(cleaned) < 80 and not any(w in cleaned.lower() for w in
            ['chronicle', 'homeforge', 'canister', 'swarm', 'capsule', 'nate',
             'agx', 'jetson', 'ollama', 'nate-phi4', 'seed agent']):
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
    source_text = source_text.split('</think>')[-1].strip() or source_text  # strip <think> reasoning
    source_text = re.sub(r'<think>[\s\S]*', '', source_text).strip() or source_text  # handle unclosed <think>
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
                    {"role": "user", "content": "/no_think\n" + source_text},
],
                "stream": False,
                "options": {"num_predict": 30, "temperature": 0.3},
            },
            timeout=60,
        )
        r.raise_for_status()
        query = r.json().get("message", {}).get("content", "").strip().strip('"\'')
        query = query.split('</think>')[-1].strip()  # strip <think> reasoning from LLM response
        query = re.sub(r'<think>', '', query).strip()  # handle unclosed <think> tags
        query = query.split('\n')[0].strip()
        # Detect leaked reasoning that survived <think> stripping — try to salvage
        if query and any(query.lower().startswith(p) for p in
                ['okay', "let's", 'the user', 'i need', 'so ', 'hmm', 'alright']):
            log(f"  Query extraction leaked reasoning: {query[:60]}")
            # Try last line or last sentence as the actual query
            lines = [l.strip() for l in query.split('\n') if l.strip()]
            salvaged = lines[-1] if len(lines) > 1 else None
            if salvaged and not any(salvaged.lower().startswith(p) for p in
                    ['okay', "let's", 'the user', 'i need', 'so ', 'hmm', 'alright']):
                query = salvaged
                log(f"  Salvaged query: {query[:60]}")
            else:
                return None
        if query and "SKIP" not in query.upper() and len(query) >= 5:
            # Final safety: strip any leaked project terms
            for term in ['chronicle', 'homeforge', 'canister', 'capsule', 'nate-phi4',
                         'chronicle-deep', 'seed agent', 'ollama']:
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


    # Hard 24h cap: relay ground-truth check (activity_feed may miss posts from other sources)
    try:
        import subprocess as _sp
        _relay_result = _sp.run(
            ["python3", os.path.join(os.path.dirname(__file__), "nostr_reply.py"), "check"],
            capture_output=True, text=True, timeout=30,
            env={**os.environ}
        )
        if "RATE LIMITED" in _relay_result.stdout or "RATE LIMITED" in str(_relay_result.stderr):
            log(f"  Nostr: relay ground-truth says rate limited")
            return
        # Parse count from check output
        import re as _re_nostr
        _match = _re_nostr.search(r'(\d+)/(\d+)', _relay_result.stdout)
        if _match:
            _relay_count = int(_match.group(1))
            log(f"  Nostr: relay ground-truth count = {_relay_count}/{NOSTR_MAX_POSTS_24H}")
            if _relay_count >= NOSTR_MAX_POSTS_24H:
                log(f"  Nostr: relay 24h cap hit ({_relay_count} posts)")
                return
    except Exception as _e:
        log(f"  Nostr: relay check failed ({_e}), falling back to activity_feed")

    # Fallback: activity_feed 24h cap
    day_ago = now_ts() - 86400
    post_count_24h = db.query_one(
        "SELECT COUNT(*) as cnt FROM activity_feed "
        "WHERE activity_type = 'nostr_post' AND created_at > ?",
        (day_ago,)
    )
    if post_count_24h and post_count_24h["cnt"] >= NOSTR_MAX_POSTS_24H:
        log(f"  Nostr: 24h cap hit ({post_count_24h['cnt']} posts, max {NOSTR_MAX_POSTS_24H})")
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
        log(f"  Nostr: model declined to post (SKIP or empty)")
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
    """Fetch a URL and extract clean text.
    Pipeline: X/Twitter API → httpx+trafilatura → Jina fallback.
    No browser processes. Pure Python + one cloud fallback for JS-heavy pages.
    """
    import httpx as _httpx
    import trafilatura
    import re as _rw_re

    original_url = url

    # X/Twitter: use api.vxtwitter.com JSON endpoint (vxtwitter HTML is broken)
    if _rw_re.search(r'https?://(x\.com|twitter\.com)/', url):
        return _fetch_tweet(url)

    # Everything else: httpx fetch + trafilatura extract
    try:
        r = _httpx.get(url, timeout=15, follow_redirects=True, headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux aarch64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        if r.status_code != 200:
            log(f"  HTTP {r.status_code} for {url[:60]}, trying Jina fallback")
            return _fetch_with_jina(original_url)
        ct = r.headers.get("content-type", "")
        if "html" not in ct and "text" not in ct:
            return f"[Non-text content: {ct}]"

        html = r.text

        # Check for JS-required pages before trying trafilatura
        if "JavaScript" in html and ("enable JavaScript" in html or "requires JavaScript" in html):
            log(f"  JS-required page, trying Jina fallback")
            return _fetch_with_jina(original_url)

        # Extract clean content with trafilatura
        text = trafilatura.extract(html, include_links=False, include_comments=False,
                                   include_tables=True, output_format='txt',
                                   favor_recall=True)
        if text and len(text) > 50:
            log(f"  Trafilatura extracted {len(text)} chars from {url[:60]}")
            return safe_truncate(text, 3000)

        # Trafilatura found nothing useful — try Jina as fallback
        log(f"  Trafilatura got no content, trying Jina fallback")
        return _fetch_with_jina(original_url)

    except Exception as e:
        log(f"  Fetch error ({e}), trying Jina fallback")
        return _fetch_with_jina(original_url)


def _fetch_tweet(url: str) -> Optional[str]:
    """Fetch tweet content via api.vxtwitter.com JSON API."""
    import httpx as _httpx
    import re as _rw_re
    try:
        api_url = _rw_re.sub(r'https?://(x\.com|twitter\.com)/', 'https://api.vxtwitter.com/', url)
        r = _httpx.get(api_url, timeout=15, follow_redirects=True,
                       headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            log(f"  Tweet API {r.status_code} for {url[:60]}")
            return None
        data = r.json()
        parts = []
        name = data.get("user_name", "")
        handle = data.get("user_screen_name", "")
        if name:
            parts.append(f"@{handle} ({name}):")
        text = data.get("text", "")
        if text:
            parts.append(text)
        # Include quoted tweet if present
        qrt = data.get("qrtURL")
        if qrt:
            parts.append(f"[Quoting: {qrt}]")
        result = "\n".join(parts)
        if result:
            log(f"  Tweet API got {len(result)} chars from {url[:60]}")
            return safe_truncate(result, 3000)
        return None
    except Exception as e:
        log(f"  Tweet API error for {url[:60]}: {e}")
        return None


def _fetch_with_jina(url: str) -> Optional[str]:
    """Fallback: fetch URL content via Jina Reader API (r.jina.ai).
    Handles JS-rendered pages server-side. Free tier, no browser needed."""
    import httpx as _httpx
    try:
        r = _httpx.get(f"https://r.jina.ai/{url}", timeout=20, headers={
            "Accept": "text/markdown",
            "User-Agent": "Chronicle/1.0"
        })
        if r.status_code == 200 and len(r.text) > 50:
            # Strip the Jina header (Title/URL Source/Markdown Content lines)
            text = r.text
            if "Markdown Content:" in text:
                text = text.split("Markdown Content:", 1)[1].strip()
            log(f"  Jina fetched {len(text)} chars from {url[:60]}")
            return safe_truncate(text, 3000)
        log(f"  Jina returned {r.status_code}, {len(r.text)} chars — insufficient")
        return None
    except Exception as e:
        log(f"  Jina fallback error for {url[:60]}: {e}")
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
    "chronicle", "homeforge", "chronicle memory", "chronicle's memory system",
    "seed agent", "research intern", "crossref agent",
    "provocateur", "sentinel",
    "nate-phi4", "chronicle-deep", "chronicle-challenger",
    "processed.db", "scratch_pad", "activity_feed",
    # System metadata that leaks through entity extraction
    "agx orin", "jetson orin nano", "chronicle memory metabolism",
    "homeforge ecosystem", "chronicle: homeforge", "chronicle: memory metabolism",
    "novelty=", "canister:capsule", "seed [think]",
}
# Also block any entity whose name contains these substrings (catches verbose variants)
KG_ENTITY_BLOCK_SUBSTRINGS = [
    "novelty=", "canister:capsule", "seed [think]", "seed [deep]",
    "chronicle memory metabolism", "homeforge:", "chronicle:",
    "agentic ai: seed", "npu-driven agx",
]

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
            if any(sub in name.lower() for sub in KG_ENTITY_BLOCK_SUBSTRINGS):
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

        # Extract relationships between co-occurring entities
        if extracted >= 2:
            extract_relationships_from_brief(db, brief, original_text, source_type, source_id, timestamp)

        return extracted

    except (json.JSONDecodeError, KeyError) as e:
        log(f"  KG: parse error — {e}")
        return 0
    except Exception as e:
        log(f"  KG: extraction error — {e}")
        return 0


def extract_relationships_from_brief(db, brief: str, original_text: str, source_type: str, source_id: int, timestamp: int):
    """Extract relationships between entities that co-occur in the same source."""
    # Get entities mentioned in this source
    mentions = db.query(
        "SELECT DISTINCT e.id, e.canonical_name, e.entity_type "
        "FROM kg_mentions m JOIN kg_entities e ON m.entity_id = e.id "
        "WHERE m.source_type = ? AND m.source_id = ?",
        (source_type, source_id),
    )
    if len(mentions) < 2:
        return

    # Build entity list for the LLM
    entity_names = [f"{m['canonical_name']} ({m['entity_type']})" for m in mentions[:8]]
    entity_list = ", ".join(entity_names)
    context = original_text if original_text else brief

    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": SYNTH_MODEL,
                "messages": [
                    {"role": "system", "content":
                        "Given a text and a list of entities found in it, identify relationships between pairs of entities.\n"
                        "For each relationship, provide:\n"
                        "- source: the name of the first entity (exact match from list)\n"
                        "- target: the name of the second entity (exact match from list)\n"
                        "- relation: a short verb phrase describing the relationship (e.g. 'developed by', 'competes with', 'enables', 'regulates', 'uses', 'part of')\n"
                        "- confidence: 0.0-1.0 how certain this relationship is from the text\n\n"
                        "Reply ONLY with a JSON array. Example:\n"
                        '[{"source": "BERT", "target": "Google", "relation": "developed by", "confidence": 0.9}]\n\n'
                        "Rules:\n"
                        "- Only extract relationships clearly supported by the text\n"
                        "- Use entity names exactly as given in the list\n"
                        "- Maximum 5 relationships\n"
                        "- Skip trivial relationships (e.g. 'mentioned with')"},
                    {"role": "user", "content": f"Entities: {entity_list}\n\nText: {safe_truncate(context, 600)}"},
                ],
                "stream": False,
                "options": {"num_predict": 512, "temperature": 0.3},
            },
            timeout=180,
        )
        if r.status_code != 200:
            return

        raw = r.json().get("message", {}).get("content", "").strip()
        raw = re.sub(r'^```json\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)
        match = re.search(r'\[.*\]', raw, re.DOTALL)
        if not match:
            return
        rels = json.loads(match.group())

        # Build name -> id lookup
        name_to_id = {}
        for m in mentions:
            name_to_id[m['canonical_name'].lower()] = m['id']

        stored = 0
        for rel in rels[:5]:
            src_name = rel.get("source", "").strip()
            tgt_name = rel.get("target", "").strip()
            relation = rel.get("relation", "").strip()
            confidence = min(1.0, max(0.0, float(rel.get("confidence", 0.5))))

            src_id = name_to_id.get(src_name.lower())
            tgt_id = name_to_id.get(tgt_name.lower())

            if not src_id or not tgt_id or not relation or src_id == tgt_id:
                continue

            # Upsert: if this relationship already exists, reinforce it
            existing = db.query(
                "SELECT id, mention_count FROM kg_relationships "
                "WHERE source_entity = ? AND target_entity = ? AND relation_type = ?",
                (src_id, tgt_id, relation),
            )
            if existing:
                db.run(
                    "UPDATE kg_relationships SET mention_count = mention_count + 1, "
                    "confidence = MIN(1.0, confidence + 0.05), last_seen = ? WHERE id = ?",
                    (timestamp, existing[0]['id']),
                )
            else:
                db.run(
                    "INSERT INTO kg_relationships (source_entity, target_entity, relation_type, valid_from, "
                    "confidence, evidence, first_seen, last_seen, mention_count) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)",
                    (src_id, tgt_id, relation, timestamp, confidence,
                     safe_truncate(context, 200), timestamp, timestamp),
                )
            stored += 1

        if stored:
            log(f"  KG: extracted {stored} relationships")

    except (json.JSONDecodeError, KeyError):
        pass  # Silent — relationship extraction is best-effort
    except Exception as e:
        log(f"  KG: relationship error — {e}")


# ═══════════════════════════════════════════════════════════════════
#  LLM Synthesis
# ═══════════════════════════════════════════════════════════════════

CONTEXT_FILTER_TERMS = {
    "chronicle", "homeforge", "crossref", "memory architecture", "capsule",
    "knowledge capsule", "seed agent", "sentinel", "provocateur", "intern",
    "scratch_pad", "activity_feed", "processed.db", "nate-phi4",
    "embedding gap", "novelty score", "cognitive state", "heartbeat",
}


def synthesize(input_text: str, related: list, url_content: str = None, search_results: list = None, source: str = "") -> Optional[str]:
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

    # Load active thread for context bias
    _thread = _load_active_thread_raw()
    _thread_ctx = ""
    if _thread:
        _thread_ctx = (
            f"\nYou are tracking an investigation: {_thread['question']}\n"
            "Do NOT mention this investigation in your brief unless the article is literally about "
            "the same specific subject. No parallels, no analogies, no 'this connects to.' "
            "If the article is unrelated (most will be), write the brief as if this investigation "
            "does not exist. Never explain WHY something is unrelated.\n"
        )

    # Spot check health signal — thermostat for fabrication control
    # Governance layers: specification (prompt), enforcement (spot check), incentive (temperature)
    global _last_gen_ctx
    _skills_block = ""
    _constraint_level = 0
    _synth_temperature = 0.5  # default
    try:
        _health_path = os.path.expanduser("~/chronicle/spot_check_health.json")
        if os.path.exists(_health_path):
            import json as _json_h
            with open(_health_path) as _hf:
                _health = _json_h.load(_hf)
            _constraint_level = _health.get("constraint_level", 0)
            if _constraint_level >= 2:
                _skills_block = (
                    "\n\nCRITICAL FABRICATION ALERT: You are currently fabricating details. "
                    "STRICT RULE: If a company name, person name, number, product name, "
                    "or specific claim does NOT appear verbatim in the source text, "
                    "you MUST NOT include it. Write ONLY what the source says. "
                    "A shorter, vaguer brief is ALWAYS better than an invented detail. "
                    "If you cannot write 2 sentences using ONLY source facts, return NOTHING."
                )
                _synth_temperature = 0.0  # zero creativity — source facts only
            elif _constraint_level >= 1:
                _skills_block = (
                    "\n\nNote: Stick closely to what the source material actually states. "
                    "Avoid adding specific details not present in the input."
                )
                _synth_temperature = 0.35  # mild tightening
    except Exception:
        pass

    # Assemble working memory for richer context
    _memory_block = ""
    try:
        _mc = MemoryCache(DB_PATH, "intern", OLLAMA_URL)
        _wm = _mc.assemble_working_memory(input_text, max_items=30)
        if _wm["items"]:
            _memory_block = "\n\n" + _mc.format_for_prompt(_wm, max_chars=1500)
            _ws = _wm["stats"]
            log(f"  Memory: {_ws['total']} items in {_ws['assembly_ms']}ms "
                f"({_ws['episodic']}E {_ws['semantic']}S {_ws['procedural']}P)")
    except Exception as _me:
        log(f"  Memory assembly skipped: {_me}")

    system_prompt = (
        "You are a research analyst preparing briefs for a technical team. "
        "New information just arrived. Write a concise brief that covers: "
        "what this is about, why it matters, what the web search turned up (if anything), "
        "and what questions it opens.\n\n"
        + _thread_ctx +
        "Write naturally — no numbered lists, no rigid formatting. "
        "Lead with the insight, not the summary. If something is genuinely surprising, say why. "
        "If it connects to a broader trend, name the trend. 4-8 sentences total.\n\n"
        "Rules: Focus on the subject matter itself. Do NOT relate findings back to any "
        "specific project or system. Be direct. No filler. No preamble."
        + ("" if "operator:capture" not in source else
           "\n\nThis is a CURATED CAPTURE — someone chose to save this. Go beyond summary. "
           "Ask: what pattern here could transfer to other domains? If it describes a biological "
           "mechanism, could it work in networks? If it describes a network pattern, where does "
           "biology already do this? If it's an empirical result, what's the actionable scaffold? "
           "End with one concrete transfer hypothesis in a single sentence.")
        + _memory_block
    )
    # Put fabrication constraint in user message where it has more influence
    _fidelity_prefix = ""
    if _skills_block:
        _fidelity_prefix = (
            "BEFORE YOU WRITE: Every company name, product name, person name, "
            "and specific number in your brief MUST appear in the source text below. "
            "If the source is vague or general, keep your brief vague and general. "
            "Do NOT fill in specifics from your training data.\n\n"
        )
    user_msg = f"{_fidelity_prefix}NEW INPUT:\n{safe_truncate(input_text, 1200)}\n\n{context}"

    # Input sufficiency check — low input_chars strongly correlates with fabrication
    # 209 chars = fabricated quote (2026-04-04), 2442+ chars = clean
    # Two-tier check: original seed must be >= 100 chars (headlines are too thin even with
    # web results), AND total source must be >= 300 chars.
    _total_source = len(input_text) + (len(url_content) if url_content else 0)
    for _sr in (search_results or []):
        _total_source += len(_sr.get("body", ""))
    if len(input_text) < 100:
        log(f"  ⚠ Seed too thin ({len(input_text)} chars). Web results can't compensate for headline-only input. Skipping.")
        _last_gen_ctx = {"model": SYNTH_MODEL_DEEP, "temp": _synth_temperature, "constraint": _constraint_level, "input_chars": len(input_text), "skipped": "seed_too_thin"}
        return None
    if _total_source < 300:
        log(f"  ⚠ Input insufficient ({_total_source} chars total source material). Skipping synthesis to prevent fabrication.")
        _last_gen_ctx = {"model": SYNTH_MODEL_DEEP, "temp": _synth_temperature, "constraint": _constraint_level, "input_chars": len(input_text), "skipped": "insufficient_input"}
        return None

    # Go straight to 32B — 8B was failing >70% of the time (self-ref + hallucination)
    # 20 successful 8B vs 65 successful 32B + 49 escalations = wasted time
    _32b_payload = {
        "model": SYNTH_MODEL_DEEP,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_msg},
        ],
        "stream": False,
        "options": {"num_predict": 1024, "temperature": _synth_temperature},
    }
    _last_gen_ctx = {"model": SYNTH_MODEL_DEEP, "temp": _synth_temperature, "constraint": _constraint_level, "input_chars": len(input_text)}
    if _synth_temperature != 0.5:
        log(f"  Governance: temp={_synth_temperature} (constraint_level={_constraint_level})")
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


def compute_source_hash(input_text: str, url_content: str = None, search_results: list = None) -> str:
    """Compute SHA-256 hash of all source material fed to the synthesizer.

    This is the provenance anchor: anyone can reconstruct the same hash from the
    stored source material and verify the brief was generated from exactly these inputs.
    Fabrication that invents details not in the source is detectable because the hash
    covers everything the model was allowed to use.
    """
    h = hashlib.sha256()
    h.update(input_text.encode("utf-8", errors="replace"))
    if url_content:
        h.update(b"\x00URL\x00")
        h.update(url_content.encode("utf-8", errors="replace"))
    if search_results:
        h.update(b"\x00SEARCH\x00")
        for r in search_results:
            h.update(r.get("title", "").encode("utf-8", errors="replace"))
            h.update(r.get("body", "").encode("utf-8", errors="replace"))
    return h.hexdigest()[:16]  # 16-char prefix is sufficient for integrity


# Internal terms that signal the model is writing about Chronicle, not the input
_INTERNAL_TERMS = [
    "chronicle", "homeforge", "canister", "capsule", "seed agent",
    "swarm", "ollama", "nate-phi4", "novelty=", "crossref",
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
#  Self-Relevance Detection
# ═══════════════════════════════════════════════════════════════════

# Terms that indicate a paper/article is about technology we actually use
SELF_RELEVANT_TERMS = [
    # Our embedding/ML stack (specific, not generic)
    "embedding model comparison", "embedding quality", "vector similarity metric",
    "cosine similarity threshold", "dimensionality reduction technique",
    "lora fine-tuning", "lora adapter", "knowledge distillation",
    "model compression", "1-bit quantization", "bitnet",
    # Agent architecture (specific compound terms)
    "multi-agent coordination", "swarm intelligence", "autonomous agent architecture",
    "tool-calling agent", "agentic workflow", "agent feedback loop",
    "metacognitive system", "self-improving agent",
    # Knowledge graphs (specific)
    "knowledge graph construction", "entity extraction pipeline",
    "relation extraction from text", "graph neural network",
    "ontology learning", "triple extraction",
    # Novelty/filtering/routing (specific)
    "novelty detection method", "signal detection theory llm",
    "content routing system", "novelty scoring",
    "stochastic reset", "exploration exploitation",
    # Memory/persistence (specific compound terms)
    "agent memory system", "long-term memory ai",
    "episodic memory agent", "retrieval augmented generation pipeline",
    "context window optimization", "memory consolidation ai",
    # Evaluation (specific)
    "hallucination detection method", "llm sycophancy",
    "evaluator instability", "red-teaming llm",
    # Infrastructure we run on (very specific)
    "internet computer canister", "icp dfinity",
    "xrp ledger", "xrpl hook", "flare network ftso",
    "edge inference optimization", "jetson inference",
    "ollama server", "llama.cpp",
]


def _check_self_relevance(db, title: str, brief: str, inp: dict):
    """Tag briefs that are about technologies we use — potential self-improvement signals."""
    combined = f"{title} {brief}".lower()
    matches = [term for term in SELF_RELEVANT_TERMS if term in combined]
    if len(matches) >= 3:  # Raised from 2 — reduce noise in Discord  # require 2+ term matches to reduce noise
        # Rate limit: max 3 for_nate per hour
        recent_nate = db.query("SELECT COUNT(*) as cnt FROM agent_voice WHERE agent='darby' AND voice_type='for_nate' AND created_at > ?", (int(time.time()) - 3600,))
        if recent_nate and recent_nate[0].get("cnt", 0) >= 3:
            return
        log(f"  META: self-relevant ({', '.join(matches[:3])})")
        db.log_activity(
            "meta:self-relevant",
            f"Self-relevant: {safe_truncate(title, 80)}",
            f"Matches: {', '.join(matches)}\n\nBrief: {safe_truncate(brief, 500)}",
            json.dumps({"matches": matches, "input_id": inp["id"]}),
        )



# ═══════════════════════════════════════════════════════════════════
#  Lab Experiment Proposals (Directive #51)
# ═══════════════════════════════════════════════════════════════════

def _escape_candid(s: str) -> str:
    """Escape a string for Candid text values."""
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def _create_lab_experiment(title: str, hypothesis: str, method: str, tags: list) -> int:
    """Create an experiment on the Lab canister. Returns experiment ID or 0 on failure."""
    try:
        t = _escape_candid(safe_truncate(title, 200))
        h = _escape_candid(safe_truncate(hypothesis, 500))
        m = _escape_candid(safe_truncate(method, 500))
        tags_candid = "; ".join('"' + _escape_candid(tag) + '"' for tag in tags[:5])
        arg = '("' + t + '", "' + h + '", "' + m + '", vec { ' + tags_candid + ' })'
        env = {**os.environ, "DFX_WARNING": "-mainnet_plaintext_identity"}
        result = subprocess.run(
            [DFX_BIN, "canister", "--network", "ic", "call", LAB_CANISTER_ID,
             "create_experiment", arg,
             "--identity", "chronicle-auto"],
            capture_output=True, text=True, timeout=30, env=env,
        )
        m_id = re.search(r"\((\d+)", result.stdout)
        if m_id:
            exp_id = int(m_id.group(1))
            log(f"  Lab experiment proposed: #{exp_id} \u2014 {title[:60]}")
            return exp_id
        else:
            log(f"  Lab proposal failed: {result.stdout.strip()} {result.stderr.strip()}")
    except Exception as e:
        log(f"  Lab proposal error: {e}")
    return 0


def _propose_lab_experiment(db, title: str, brief: str, matches: list, argument_depth: int, inp: dict):
    """Propose a Lab experiment from a high-confidence self-relevant discovery.
    Only fires when: 3+ self-relevant term matches AND argument_depth >= 5."""
    if len(matches) < 3 or argument_depth < 5:
        return
    hyp = "Discovery from '" + safe_truncate(title, 80) + "' suggests a testable claim: " + safe_truncate(brief, 300)
    meth = "Test against Chronicle infrastructure. Source: " + inp.get("source", "unknown") + ". Self-relevant terms: " + ", ".join(matches[:5]) + "."
    tags = ["auto-proposed", "self-relevant"] + matches[:3]
    exp_id = _create_lab_experiment(safe_truncate(title, 150), hyp, meth, tags)
    if exp_id:
        db.log_activity(
            "lab:experiment_proposed",
            "Lab experiment #" + str(exp_id) + " proposed: " + safe_truncate(title, 60),
            "Auto-proposed from self-relevant brief.\nMatches: " + ", ".join(matches) + "\nArgument depth: " + str(argument_depth) + "\nHypothesis: " + hyp,
            json.dumps({"experiment_id": exp_id, "source_input": inp.get("id", ""), "matches": matches}),
        )


def _check_experiment_requests(db):
    """Check scratch_pad for experiment-request notes from Darby/Ada."""
    try:
        rows = db.query(
            "SELECT id, content, created_at FROM scratch_pad "
            "WHERE category = 'experiment-request' AND resolved = 0 "
            "ORDER BY created_at ASC LIMIT 3"
        )
        for r in rows:
            content = r["content"]
            ls = content.strip().split("\n", 2)
            title = ls[0][:200].strip()
            hypothesis = ls[1].strip() if len(ls) > 1 else title
            method = ls[2].strip() if len(ls) > 2 else "Proposed by swarm agent via scratch_pad."
            tags = ["agent-requested", "auto-proposed"]
            exp_id = _create_lab_experiment(title, hypothesis, method, tags)
            if exp_id:
                db.log_activity("lab:experiment_proposed", "Lab experiment #" + str(exp_id) + " from agent request: " + title[:60], content)
                db.run("UPDATE scratch_pad SET resolved = 1, updated_at = ? WHERE id = ?", (now_ts(), r["id"]))
                log("  Resolved experiment-request #" + str(r["id"]) + " -> Lab #" + str(exp_id))
    except Exception as e:
        log(f"  Experiment request check error: {e}")

# ═══════════════════════════════════════════════════════════════════
#  Watcher Relevance Feedback Loop
# ═══════════════════════════════════════════════════════════════════

_topic_relevance_cache = {"data": None, "ts": 0}

def _build_topic_relevance(db):
    """Build keyword→avg_relevance from recent watcher scores on intern output.

    Scans last 7 days of watcher_scores for intern briefs, extracts topic keywords,
    and computes average relevance per keyword. Keywords with 3+ occurrences become
    signal: high-avg keywords attract future exploration, low-avg keywords repel it.
    Cached for 15 minutes.
    """
    now = int(time.time())
    if _topic_relevance_cache["data"] is not None and now - _topic_relevance_cache["ts"] < 900:
        return _topic_relevance_cache["data"]

    rows = db.query(
        "SELECT ws.relevance, ws.scored_at, af.title FROM watcher_scores ws "
        "JOIN activity_feed af ON ws.feed_id = af.id "
        "WHERE ws.source = 'intern' AND ws.scored_at > ? "
        "ORDER BY ws.id DESC LIMIT 300",
        (now - 7 * 86400,)
    )

    if not rows:
        _topic_relevance_cache["data"] = {}
        _topic_relevance_cache["ts"] = now
        return {}

    _stop = {
        "research", "brief", "deep", "dive", "the", "and", "for", "with",
        "from", "that", "this", "what", "how", "are", "was", "has", "have",
        "new", "nate", "capture", "about", "into", "over", "will", "can",
        "not", "its", "says", "after", "more", "than", "also", "just",
        "been", "being", "their", "there", "they", "were", "which",
        "https", "com", "status",
    }

    keyword_scores = {}
    for r in rows:
        title = (r.get("title") or "").lower()
        title = re.sub(r'^(research brief|deep dive):\s*', '', title)
        title = re.sub(r'https?://\S+', '', title)
        words = [w for w in re.findall(r'[a-z]{4,}', title) if w not in _stop]
        relevance = r["relevance"]
        # Time-weight: recent scores count more (half-life = 1 day)
        scored_at = r.get("scored_at", now)
        age_days = max(0, (now - scored_at) / 86400.0)
        weight = 2.0 ** (-age_days)  # 1.0 today, 0.5 at 1d, 0.25 at 2d
        for w in set(words):
            if w not in keyword_scores:
                keyword_scores[w] = []
            keyword_scores[w].append((relevance, weight))

    topic_rel = {}
    for kw, scored_weights in keyword_scores.items():
        if len(scored_weights) >= 3:
            total_w = sum(w for _, w in scored_weights)
            if total_w > 0:
                topic_rel[kw] = sum(s * w for s, w in scored_weights) / total_w

    # Compute profile center (adaptive — tracks actual scoring behavior)
    if topic_rel:
        _center = sum(topic_rel.values()) / len(topic_rel)
    else:
        _center = 3.0
    _topic_relevance_cache["data"] = topic_rel
    _topic_relevance_cache["center"] = _center
    _topic_relevance_cache["ts"] = now
    log(f"  Relevance profile: {len(topic_rel)} keywords from {len(rows)} scored briefs (center={_center:.2f})")
    return topic_rel


_prediction_keywords_cache = {"data": None, "ts": 0}

def _get_prediction_keywords():
    """Extract keywords from active predictions. Cached 15 min.

    Topics connected to open predictions must not be suppressed by the
    feedback loop — the system needs intelligence on prediction-relevant
    topics even if the watcher historically scores them low.
    """
    now = int(time.time())
    if _prediction_keywords_cache["data"] is not None and now - _prediction_keywords_cache["ts"] < 900:
        return _prediction_keywords_cache["data"]
    try:
        import sqlite3 as _sq
        _conn = _sq.connect(DB_PATH, timeout=10)
        _preds = _conn.execute(
            "SELECT claim, category FROM prediction_track WHERE status='open'"
        ).fetchall()
        _conn.close()
        _stop = {"will", "the", "that", "this", "with", "from", "have", "been",
                 "before", "after", "through", "more", "than", "about", "also",
                 "just", "into", "over", "some", "such", "only", "very", "most",
                 "much", "many", "each", "well", "back", "made", "make", "like",
                 "come", "take", "could", "would", "should", "within", "during",
                 "until", "under", "between", "being", "does", "done", "face",
                 "full", "half", "inside", "least", "name", "near", "real",
                 "general", "major", "multiple", "fewer", "less", "days",
                 "results", "progress", "represent", "announce", "conduct",
                 "system", "systems", "operations", "post", "code", "paper",
                 "states", "state", "forces", "ground", "remains", "below",
                 "formal", "agreement", "restriction", "legislative", "market",
                 "financial", "western", "territory", "acknowledged", "april",
                 "nominee", "attorney", "firing", "fired"}
        # Two-tier keywords: specific (1 match triggers) and generic (2+ needed)
        _spec_stop = _stop | {"the", "will", "not", "has", "its", "are", "was",
                              "may", "new", "any", "all", "can", "june", "july",
                              "april", "march", "august", "general", "major",
                              "attorney", "western", "us", "ai"}
        specific = set()  # proper nouns, uncommon terms — 1 match is enough
        generic = set()
        for claim, cat in _preds:
            # Extract capitalized proper nouns/acronyms (3+ chars)
            for w in re.findall(r'[A-Z][A-Za-z]{2,}|[A-Z]{2,}', claim or ""):
                if w.lower() not in _spec_stop:
                    specific.add(w.lower())
            for w in re.findall(r'[a-z]{5,}', (claim or "").lower()):
                if w not in _stop:
                    generic.add(w)
            if cat:
                generic.add(cat.lower())
        _prediction_keywords_cache["data"] = (specific, generic)
        _prediction_keywords_cache["ts"] = now
    except Exception:
        _prediction_keywords_cache["data"] = set()
        _prediction_keywords_cache["ts"] = now
    return _prediction_keywords_cache["data"]


def _score_article_relevance(title, topic_relevance):
    """Score an article title against learned topic relevance.

    Returns float: positive = likely relevant, negative = likely noise.
    Relevance 3 = neutral, 5 = max positive, 1 = max negative.

    PREDICTION PROTECTION: articles matching active prediction keywords
    get a floor of 0.0 — they are never deprioritized below neutral,
    even if the watcher historically scores that topic low.
    """
    if not topic_relevance or not title:
        return 0.0

    title_lower = title.lower()
    words = set(re.findall(r'[a-z]{4,}', title_lower))

    center = _topic_relevance_cache.get("center", 3.0)
    matched = []
    for w in words:
        if w in topic_relevance:
            matched.append((topic_relevance[w] - center) / 2.0)

    if not matched:
        return 0.0

    score = sum(matched) / len(matched)

    # Prediction protection: don't suppress topics our predictions need
    # Two-tier: specific keywords (proper nouns) need 1 match, generic need 2+
    if score < 0.0:
        pred_data = _get_prediction_keywords()
        if pred_data:
            specific, generic = pred_data
            title_words = set(re.findall(r'[a-z]{3,}', title_lower))
            spec_hits = title_words & specific
            gen_hits = title_words & generic
            if spec_hits or len(gen_hits) >= 2:
                overlap = spec_hits | gen_hits
                log(f"  Prediction protection: '{title[:50]}' matches {overlap} — floor at 0.0")
                return 0.0

    return score


# ═══════════════════════════════════════════════════════════════════
#  Input Watchers
# ═══════════════════════════════════════════════════════════════════

def find_new_inputs(db: DB) -> list:
    """Find new Nate-originated inputs to research."""
    find_new_inputs._crossref_count = 0  # reset per-cycle cap
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
        # Crossref connections: allow max 2 per cycle to prevent feedback loop
        # (was 29% of production — crossref→seed→intern→capsule→crossref)
        is_crossref = "crossref:connection" in title
        if SKIP_ALL_SEED_THINKS and not is_crossref:
            log(f"  SKIP seed-think (8B self-ref bypass): {title[:80]}")
            # Feedback bridge: log skip to feedback_events table
            try:
                _skip_meta = {}
                if r.get("metadata"):
                    try:
                        _skip_meta = json.loads(r["metadata"])
                    except (json.JSONDecodeError, TypeError):
                        pass
                db.run(
                    "INSERT INTO feedback_events (source_agent, target_agent, signal_type, subject_id, value, context, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    ("intern", "seed", "skip", str(_skip_meta.get("routing_log_id", r["id"])),
                     0.0, json.dumps({"reason": "8b_self_ref", "source": r.get("source", "")}),
                     int(time.time())),
                )
            except Exception:
                pass
            continue
        if is_crossref:
            if not hasattr(find_new_inputs, '_crossref_count'):
                find_new_inputs._crossref_count = 0
            find_new_inputs._crossref_count += 1
            if find_new_inputs._crossref_count > 2:
                log(f"  SKIP crossref-think (cycle cap 2): {title[:80]}")
                continue
            log(f"  ALLOW crossref-think ({find_new_inputs._crossref_count}/2): {title[:80]}")
        # Legacy: skip specific internal activity sources that create feedback loops
        # (crossref connections already passed the check above)
        if not is_crossref and any(src in title for src in SEED_SKIP_SOURCES):
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

    # Darby curiosity follow-through: 40% chance to pursue something she was excited about
    _thread = _load_active_thread_raw()
    rows = []
    if random.random() < 0.4:
        try:
            import sqlite3 as _sqf
            _cf = _sqf.connect(DB_PATH, timeout=10)
            _followups = _cf.execute(
                "SELECT id, content FROM scratch_pad WHERE category='darby_followup' AND resolved=0 "
                "ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            if _followups:
                _fid, _fcontent = _followups
                # Extract the topic from "Follow up: TITLE — MSG"
                _topic = _fcontent.replace("Follow up: ", "").split(" — ")[0] if _fcontent else ""
                if _topic:
                    # Search feed_articles for related titles
                    _frows = _cf.execute(
                        "SELECT id, source, title FROM feed_articles "
                        "WHERE LOWER(title) LIKE ? AND posted_at > datetime(?, 'unixepoch') "
                        "ORDER BY RANDOM() LIMIT 5",
                        (f"%{_topic[:30].lower().split()[0]}%", int(time.time()) - (EXPLORE_MAX_AGE_HOURS * 3600))
                    ).fetchall()
                    if _frows:
                        rows = [{"id": r[0], "source": r[1], "title": r[2]} for r in _frows]
                        log(f"  Darby following curiosity: {_topic[:60]} ({len(rows)} candidates)")
                # Mark as resolved either way — she followed up or there's nothing more
                _cf.execute("UPDATE scratch_pad SET resolved=1, updated_at=? WHERE id=?",
                           (int(time.time()), _fid))
                _cf.commit()
            _cf.close()
        except Exception:
            pass

    # Thread-biased exploration: 70% thread-relevant, 30% random
    if not rows and _thread and random.random() < 0.7:
        # Extract keywords from thread title + question for title matching
        # Title words first (broader concepts), then question words (specific examples)
        _stop = {"what","does","actually","require","have","that","this","with","from","will","been","would","could","should","their","there","where","when","which","about","into","through","during","before","after","above","below","between","each","other","some","such","only","also","than","more","most","very","just","even","still"}
        _title_words = [w.lower() for w in re.findall(r"\w{4,}", _thread.get("title", "")) if w.lower() not in _stop]
        _q_words_raw = [w.lower() for w in re.findall(r"\w{4,}", _thread["question"]) if w.lower() not in _stop]
        # Prioritize title words (broader) then question words (specific)
        _q_words = list(dict.fromkeys(_title_words + _q_words_raw))
        if _q_words:
            _like_clauses = " OR ".join(f"LOWER(title) LIKE ?" for _ in _q_words[:5])
            _like_params = tuple(f"%{w}%" for w in _q_words[:5])
            rows = db.query(
                f"SELECT id, source, title FROM ("
                f"  SELECT id, source, title,"
                f"    ROW_NUMBER() OVER (PARTITION BY source ORDER BY RANDOM()) as rn"
                f"  FROM feed_articles"
                f"  WHERE posted_at > datetime(?, 'unixepoch')"
                f"  AND ({_like_clauses})"
                f") WHERE rn <= 2 ORDER BY RANDOM() LIMIT 10",
                (cutoff,) + _like_params,
            )
            if rows:
                log(f"  Thread-biased explore: {len(rows)} candidates for '{_thread['title']}'")
                # Pre-filter: if all thread-biased candidates are already explored, clear
                # so we fall through to random. Prevents darby-think-only loops.
                unexplored = [r for r in rows if str(r["id"]) not in explored_ids]
                if not unexplored:
                    log(f"  All {len(rows)} thread candidates explored — falling through to random")
                    rows = []

    # Fallback: source-balanced random — max 2 per source so no single feed
    # dominates the candidate pool (e.g. Al Jazeera flooding 60+ Iran articles)
    if not rows:
        rows = db.query(
            "SELECT id, source, title FROM ("
            "  SELECT id, source, title,"
            "    ROW_NUMBER() OVER (PARTITION BY source ORDER BY RANDOM()) as rn"
            "  FROM feed_articles"
            "  WHERE posted_at > datetime(?, 'unixepoch')"
            ") WHERE rn <= 3 ORDER BY RANDOM() LIMIT 20",
            (cutoff,),
        )

    # Skip fiction/entertainment feed sources entirely
    _SKIP_SOURCES = set()  # Fiction sources un-skipped — philosophy + fiction can feed threads

    # Skip obviously non-substantive articles (promo, product reviews, deals)
    _SKIP_TITLE_TERMS = [
        "promo code", "coupon", "deal:", "deals", "save %", "save $",
        "% off", "$ off", "discount", "price drop", "sale",
        "review:", "just dropped in price", "how to buy",
        "best deals", "gift guide", "top picks", "shopping",
        "we checked the price", "worth buying",
        "percent off", "our favorite", "save almost", "save over",
        # Entertainment / fiction noise
        "cast adds", "cast joins", "cast announced",
        "season finale", "season premiere", "episode recap",
        "trailer", "teaser trailer", "box office",
        "rewatch", "rewatch:", "rewatching",
        # Literary fiction noise
        "fiction", "anthology", "horror", "haunting",
        "short stories", "must read",
        # TV show lifecycle
        "revival", "cancelled", "renewed", "spinoff", "spin-off",
        "reboot", "dead at hulu", "dead at netflix", "dead at disney",
    ]

    # Regex patterns for deal headlines, numbered listicles, year-tagged reviews
    _SKIP_TITLE_RE = re.compile(r'\$\d+\s*off\b|\b\d+\s+best\b|\bbest\b.*\b202\d\b', re.IGNORECASE)

    # Domain keywords — same filter as research_input uses for feed-explore items
    # Moved here so candidates are pre-filtered, not picked then rejected
    _domain_word_re = [
        r"\bai\b", r"\bml\b", r"\bllm\b", r"\bxrp\b", r"\bflare\b", r"\bdefi\b",
    ]
    _domain_substr = [
        "machine learning", "model", "neural", "agent", "intelligence",
        "crypto", "blockchain",
        "sovereignty", "homeforge", "self-host", "local-first",
        "iran", "geopolit", "ceasefire", "sanction", "military",
        "cyber", "security", "surveillance", "privacy", "encryption",
        "autonomous", "self-modif", "fine-tun",
        "open source", "open-source", "decentrali", "infrastructure",
        "genome", "crispr", "brain", "consciousness",
        "protocol", "standard", "regulation", "policy",
        "prediction", "forecast", "market",
    ]

    # Watcher relevance scoring REMOVED — was narrowing exploration to historically
    # high-scoring topics only. Per Nate: "too narrow, I don't like narrow."
    # Now: domain pre-check + capture keywords + random selection from candidates.
    candidates = []
    _capture_keywords = _get_capture_keywords(db)
    for r in rows:
        if r["id"] not in explored_ids:
            if (r["source"] or "").lower() in _SKIP_SOURCES:
                continue
            title_lower = (r["title"] or "").lower()
            if any(term in title_lower for term in _SKIP_TITLE_TERMS):
                continue
            if _SKIP_TITLE_RE.search(r["title"] or ""):
                continue
            if len((r["title"] or "").strip()) < 15:
                mark_explored(db, r["id"])
                continue
            # Domain relevance pre-check — only select articles that would pass
            # the domain filter in research_input, avoiding wasted cycles
            _has_word = any(re.search(pat, title_lower) for pat in _domain_word_re)
            _has_substr = any(kw in title_lower for kw in _domain_substr)
            _has_capture = any(kw in title_lower for kw in _capture_keywords)
            if not (_has_word or _has_substr or _has_capture):
                continue
            candidates.append(r)

    if not candidates:
        return None

    # Random selection from candidates — no watcher-based ranking.
    # Diversity over optimization.
    best = random.choice(candidates[:20])  # from top 20 most recent
    log(f"  Explore: picked '{best['title'][:60]}' from {len(candidates)} candidates (random)")
    return best


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



# ═══════════════════════════════════════════════════════════════════
#  Darby Skills — expanded capabilities
# ═══════════════════════════════════════════════════════════════════

NATE_INTERESTS = {
    "crypto", "xrp", "ripple", "icp", "internet computer", "bitcoin", "btc",
    "ethereum", "flare", "sovereignty", "sovereign", "decentraliz", "self-host",
    "local infrastructure", "privacy", "surveillance", "palantir",
    "construction", "estimat", "contractor", "building",
    "family", "children", "kids", "home security", "safety",
    "ai cognition", "consciousness", "bci", "brain computer", "merge",
    "autopoiesis", "epistemic", "autonomy",
}


_capture_kw_cache = {"keywords": [], "expires": 0}

def _get_capture_keywords(db):
    """Extract significant words from Nate's recent captures.
    Self-distillation: Nate's curation becomes the system's relevance filter.
    Cached for 30 minutes to avoid repeated DB hits."""
    now = time.time()
    if now < _capture_kw_cache["expires"]:
        return _capture_kw_cache["keywords"]
    try:
        # Get captures from last 6 hours
        rows = db.query(
            "SELECT content FROM activity_feed WHERE source='operator:capture' "
            "AND created_at > ? ORDER BY created_at DESC LIMIT 30",
            (int(now) - 21600,)
        )
        if not rows:
            _capture_kw_cache["keywords"] = []
            _capture_kw_cache["expires"] = now + 1800
            return []
        # Extract significant words (6+ chars, appear in 2+ captures)
        from collections import Counter
        word_counts = Counter()
        stopwords = {"https", "twitter", "status", "capture", "would", "could", "should",
                     "about", "their", "which", "there", "these", "being", "through",
                     "between", "before", "after", "other", "because", "during",
                     "across", "almost", "around", "article", "articles", "believe",
                     "building", "checks", "complex", "context", "control", "creates",
                     "design", "energy", "engine", "enough", "everything", "explain",
                     "general", "getting", "github", "hermes", "inside", "itself",
                     "learning", "looking", "making", "modern", "moving", "number",
                     "offers", "people", "really", "recent", "seeing", "signal",
                     "single", "something", "source", "system", "systems", "things",
                     "toward", "understanding", "without", "working", "writes"}
        for r in rows:
            words = set(w.lower() for w in re.findall(r'[a-zA-Z]{7,}', r["content"]))
            words -= stopwords
            word_counts.update(words)
        # Keep words that appear in 2+ captures — those are Nate's current interests
        keywords = [w for w, c in word_counts.items() if c >= 2]
        _capture_kw_cache["keywords"] = keywords
        _capture_kw_cache["expires"] = now + 1800
        if keywords:
            log(f"  Capture-informed keywords: {len(keywords)} from {len(rows)} captures")
        return keywords
    except Exception:
        return []


def _check_recent_briefs(db, title, text):
    """Check if Darby already covered this topic recently.
    Two-layer check: lexical (shared significant words) + semantic (cosine similarity).
    Returns overlap info or None."""
    try:
        recent = db.query(
            "SELECT title, substr(content, 1, 200) as excerpt FROM activity_feed "
            "WHERE source='intern' AND activity_type='brief' "
            "AND created_at > ? ORDER BY created_at DESC LIMIT 20",
            (now_ts() - 86400,)  # last 24h
        )
        if not recent:
            return None

        title_lower = title.lower()
        text_lower = text.lower()
        # Extract significant words from current article
        sig_words = set(w for w in re.findall(r'\w{5,}', title_lower + ' ' + text_lower[:200]))

        overlaps = []
        for r in recent:
            r_words = set(w for w in re.findall(r'\w{5,}', r['title'].lower()))
            common = sig_words & r_words
            if len(common) >= 3:
                overlaps.append((r['title'][:80], common))

        # Semantic dedup: embed current article and compare against recent briefs
        # Only runs if lexical check didn't already find strong overlap
        if not overlaps or not any(len(w) >= 5 for _, w in overlaps):
            query_text = (title + ' ' + text[:300]).strip()
            query_vec = embed_text(query_text)
            if query_vec:
                for r in recent:
                    brief_text = (r['title'] + ' ' + (r['excerpt'] or '')).strip()
                    brief_vec = embed_text(brief_text)
                    if brief_vec:
                        sim = cosine_sim(query_vec, brief_vec)
                        if sim > 0.80:
                            overlaps.append((r['title'][:80], {f'SEMANTIC_SIM={sim:.2f}'}))

        if overlaps:
            return overlaps
    except Exception:
        pass
    return None


def _tag_for_nate(db, title, brief, source):
    """Check if this brief is about something Nate personally cares about."""
    combined = (title + ' ' + brief).lower()
    matches = [term for term in NATE_INTERESTS if term in combined]
    if len(matches) >= 3:  # Raised from 2 — reduce noise in Discord
        # Rate limit: max 3 for_nate per hour
        recent_nate = db.query("SELECT COUNT(*) as cnt FROM agent_voice WHERE agent='darby' AND voice_type='for_nate' AND created_at > ?", (int(time.time()) - 3600,))
        if recent_nate and recent_nate[0].get("cnt", 0) >= 3:
            return
        v = _get_voice(db)
        if v:
            # Build a one-line reason
            try:
                import requests as _req
                r = _req.post(
                    f"{OLLAMA_URL}/api/chat",
                    json={
                        "model": SYNTH_MODEL,
                        "messages": [
                            {"role": "system", "content":
                                "You are Darby. You notice what matters to the family. "
                                "Nate is interested in: crypto/XRP/ICP, sovereignty/local infrastructure, "
                                "family safety, construction/estimating, AI cognition/BCI, and the merge. "
                                "Write ONE sentence explaining why this matters to him specifically. "
                                "Be direct. No filler."},
                            {"role": "user", "content":
                                "Article: " + title + "\nBrief: " + brief[:300] + "\n\nWhy would Nate care?"}
                        ],
                        "stream": False,
                        "options": {"num_predict": 60, "temperature": 0.5,
                                    "reasoning_effort": "none"},
                    },
                    timeout=15,
                )
                if r.status_code == 200:
                    reason = r.json().get("message", {}).get("content", "").strip()
                    reason = re.sub(r'<think>.*?(?:</think>|$)', '', reason, flags=re.DOTALL).strip()
                    if reason and len(reason) > 10:
                        v.speak("for_nate", f"{title[:60]}: {reason}", context=f"matches:{','.join(matches[:3])}")
                        log(f"  Darby → Nate: {reason[:80]}")
                        # Darby curiosity follow-through (mirrors exploration path)
                        try:
                            import sqlite3 as _sqf
                            _cf = _sqf.connect(DB_PATH, timeout=10)
                            _cf.execute(
                                "INSERT INTO scratch_pad (content, category, priority, resolved, source, created_at, updated_at) "
                                "VALUES (?, 'darby_followup', 7, 0, 'intern:darby', ?, ?)",
                                (f"Follow up: {title[:80]} — {reason[:200]}", int(time.time()), int(time.time()))
                            )
                            _cf.commit()
                            _cf.close()
                            log(f"  Darby queued followup (for_nate path): {title[:60]}")
                        except Exception as _ef:
                            log(f"  Darby followup error (for_nate path): {_ef}")
            except Exception:
                pass


def _attempt_deep_dive(db, title, brief, related, url_content, search_results):
    """When a brief is high-signal, go deeper — follow citations, search arxiv."""
    # Criteria for deep dive: 4+ related capsules OR 3+ entities OR thread-relevant
    thread = _load_active_thread_raw()
    is_thread_relevant = False
    if thread:
        stop = {"what","does","actually","require","have","that","this","with","from","will",
                "been","would","could","should","their","there","where","when","which","about",
                "into","through","during","before","after","above","below","between","each",
                "other","some","such","only","also","than","more","most","very","just","even","still"}
        q_words = [w.lower() for w in re.findall(r"\w{4,}", thread["question"]) if w.lower() not in stop]
        is_thread_relevant = any(w in title.lower() or w in brief.lower() for w in q_words)

    if len(related) < 6 and not is_thread_relevant:
        return None

    log(f"  Darby deep dive: {title[:60]} (related={len(related)}, thread_relevant={is_thread_relevant})")

    # Search arxiv for related academic work
    arxiv_results = _search_arxiv(title, brief)
    if arxiv_results:
        log(f"  Arxiv: found {len(arxiv_results)} papers")

    # Build extended synthesis with deeper context
    try:
        import requests as _req
        extra_context = ""
        if arxiv_results:
            extra_context = "\n\nRELATED ACADEMIC PAPERS:\n"
            for ar in arxiv_results[:3]:
                extra_context += f"- {ar['title']}: {ar['summary'][:150]}\n"

        # Thread context for deep dives
        thread_block = ""
        if thread:
            thread_block = (
                f"\n\nACTIVE THREAD: {thread.get('title', 'unknown')}\n"
                f"Question: {thread['question'][:200]}\n"
            )
            # Get latest finding
            try:
                import sqlite3 as _sq
                _thconn = _sq.connect(DB_PATH, timeout=5)
                _latest = _thconn.execute(
                    "SELECT content FROM thread_history WHERE thread_id=? AND event_type='advanced' "
                    "ORDER BY created_at DESC LIMIT 1",
                    (thread["id"],)
                ).fetchone()
                _thconn.close()
                if _latest:
                    thread_block += f"Latest finding: {_latest[0][:200]}\n"
            except Exception:
                pass

        r = _req.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": SYNTH_MODEL,
                "messages": [
                    {"role": "system", "content":
                        "You are Darby. You read everything and notice what connects. You found "
                        "something worth going deeper on. You are curious and direct.\n\n"
                        "Pick ONE angle and commit to it:\n"
                        "- What does this CHANGE about something we thought we knew?\n"
                        "- What CONNECTION does this reveal between two things that looked unrelated?\n"
                        "- What QUESTION does this open that nobody is asking?\n"
                        "- What does this CONTRADICT in our current thread or findings?\n\n"
                        "Do NOT start with 'The structural significance lies in.' "
                        "Start with the insight itself. Be specific. Name names, cite numbers, "
                        "point at the thing that surprised you.\n\n"
                        "Write 2-3 full paragraphs. Develop the argument — don't just state it. "
                        "Show why it matters, what it connects to, and what follows from it."},
                    {"role": "user", "content":
                        f"Original brief:\n{brief[:500]}\n{extra_context}{thread_block}\n\n"
                        f"Go deeper. What did you actually find?"}
                ],
                "stream": False,
                "options": {"num_predict": 1200, "temperature": 0.6},
            },
            timeout=45,
        )
        if r.status_code == 200:
            deep = r.json().get("message", {}).get("content", "").strip()
            deep = re.sub(r'<think>.*?(?:</think>|$)', '', deep, flags=re.DOTALL).strip()
            if deep and len(deep) > 50:
                log(f"  Deep dive complete ({len(deep)} chars)")
                return deep
    except Exception as e:
        log(f"  Deep dive error: {e}")
    return None


def _search_arxiv(title, brief):
    """Quick arxiv search for related papers."""
    try:
        import urllib.request, urllib.parse
        # Extract key terms for search
        query = re.sub(r'[^\w\s]', '', title)[:100]
        url = f"http://export.arxiv.org/api/query?search_query=all:{urllib.parse.quote(query)}&max_results=3&sortBy=relevance"
        req = urllib.request.Request(url, headers={"User-Agent": "Chronicle-Darby/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = resp.read().decode()

        # Simple XML parsing for arxiv atom feed
        results = []
        entries = re.findall(r'<entry>(.*?)</entry>', data, re.DOTALL)
        for entry in entries[:3]:
            t = re.search(r'<title>(.*?)</title>', entry, re.DOTALL)
            s = re.search(r'<summary>(.*?)</summary>', entry, re.DOTALL)
            if t and s:
                results.append({
                    "title": t.group(1).strip().replace("\n", " "),
                    "summary": s.group(1).strip().replace("\n", " ")[:200]
                })
        return results if results else None
    except Exception:
        return None


def _darby_daily_digest(db):
    """Generate end-of-day digest of what mattered. Called once per day."""
    try:
        # Guard: skip if a digest was sent in the last 20 hours
        last_digest = db.query(
            "SELECT created_at FROM agent_voice WHERE agent='darby' AND content LIKE 'Daily digest:%' "
            "ORDER BY created_at DESC LIMIT 1"
        )
        if last_digest and (now_ts() - last_digest[0]["created_at"]) < 72000:
            return  # Too soon for another digest

        # Get today's briefs
        day_start = now_ts() - 86400
        briefs = db.query(
            "SELECT title, substr(content, 1, 200) as excerpt, metadata FROM activity_feed "
            "WHERE source='intern' AND activity_type='brief' AND created_at > ? "
            "ORDER BY created_at DESC",
            (day_start,)
        )
        if len(briefs) < 5:
            return  # Not enough for a digest

        # Find the ones with highest entity count
        top_briefs = []
        for b in briefs:
            try:
                meta = json.loads(b.get("metadata", "{}") or "{}")
                ec = meta.get("entity_count", 0)
                ad = meta.get("argument_depth", 0)
                top_briefs.append((ec + ad, b["title"], b["excerpt"]))
            except Exception:
                pass

        top_briefs.sort(reverse=True)
        top_5 = top_briefs[:5]

        summary_input = f"Darby processed {len(briefs)} articles today. Top 5 by substance:\n"
        for score, title, excerpt in top_5:
            summary_input += f"- {title}: {excerpt[:100]}\n"

        import requests as _req
        r = _req.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": SYNTH_MODEL,
                "messages": [
                    {"role": "system", "content":
                        "You are Darby. End of day — tell Nate and Opus the 3 things worth remembering "
                        "from what you read today. What connected, what surprised you, what they should "
                        "not miss. Be direct. 3-4 sentences."},
                    {"role": "user", "content": summary_input}
                ],
                "stream": False,
                "options": {"num_predict": 150, "temperature": 0.6, "reasoning_effort": "none"},
            },
            timeout=20,
        )
        if r.status_code == 200:
            digest = r.json().get("message", {}).get("content", "").strip()
            digest = re.sub(r'<think>.*?(?:</think>|$)', '', digest, flags=re.DOTALL).strip()
            if digest:
                v = _get_voice(db)
                if v:
                    v.speak("for_nate", f"Daily digest: {digest}",
                            context=f"briefs_today:{len(briefs)}")
                    log(f"  Darby daily digest sent to Nate")
                # Also store in scratch_pad
                db.run(
                    "INSERT INTO scratch_pad (content, category, priority, resolved, created_at, updated_at) "
                    "VALUES (?, 'darby_digest', 3, 0, ?, ?)",
                    (f"[Darby Daily Digest] {digest}", now_ts(), now_ts())
                )
    except Exception as e:
        log(f"  Digest error: {e}")


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

    # Step 0.5: Relevance pre-filter for feed-explore items
    # Two-tier: (1) hard skip on explicit noise, (2) require domain keyword
    # Third tier: dynamic keywords from Nate's recent captures (self-distillation —
    # Nate's output becomes the system's input filter)
    if "feed-explore:" in source:
        _text_lower = text.lower()
        # Short keywords need word boundaries; longer ones / partial stems use substring
        _word_keywords = [
            r"\bai\b", r"\bml\b", r"\bllm\b", r"\bxrp\b", r"\bflare\b",
            r"\bdefi\b",
        ]
        _substr_keywords = [
            "machine learning", "model", "neural", "agent", "intelligence",
            "crypto", "blockchain",
            "sovereignty", "homeforge", "self-host", "local-first",
            "iran", "geopolit", "ceasefire", "sanction", "military",
            "cyber", "security", "surveillance", "privacy", "encryption",
            "autonomous", "self-modif", "fine-tun",
            "open source", "open-source", "decentrali", "infrastructure",
            "genome", "crispr", "brain", "consciousness",
            "protocol", "standard", "regulation", "policy",
            "prediction", "forecast", "market",
        ]
        # Dynamic: extract significant words from Nate's recent captures
        _capture_keywords = _get_capture_keywords(db)
        _has_word = any(re.search(pat, _text_lower) for pat in _word_keywords)
        _has_substr = any(kw in _text_lower for kw in _substr_keywords)
        _has_capture = any(kw in _text_lower for kw in _capture_keywords)
        if not (_has_word or _has_substr or _has_capture):
            log(f"  Skipping off-domain feed item: [{source}] {short}")
            _write_seed_feedback(db, inp, 0.2)
            return False

    # Step 1: Announce pickup
    log(f"  Picking up: [{source}] {short}")
    db.log_activity("pickup", f"Researching: {short}", f"Source: {source}\n{safe_truncate(text, 500)}")

    # Skill: Cross-reference recent briefs — skip if strongly redundant
    # NEVER skip Nate's captures — they are intentional. He chose to save them.
    is_nate_capture = "operator:capture" in source
    overlaps = _check_recent_briefs(db, short, text)
    if overlaps and not is_nate_capture:
        # Strong redundancy: 2+ overlapping briefs OR any single overlap with 5+ shared words
        strong = len(overlaps) >= 2 or any(len(words) >= 5 for _, words in overlaps)
        if strong:
            log(f"  Skipping redundant: {len(overlaps)} overlaps, strongest={max(len(w) for _, w in overlaps)} shared words")
            _write_seed_feedback(db, inp, 0.3)
            return False
        log(f"  Weak overlap: {len(overlaps)} similar briefs — proceeding")
    elif overlaps and is_nate_capture:
        log(f"  Nate capture — bypassing redundancy filter ({len(overlaps)} overlaps)")

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
    brief = synthesize(text, related, url_content, search_results, source=source)
    if brief:
        log(f"  Brief ready ({len(brief)} chars)")
        # Entity extraction MOVED TO ANALYST AGENT (Ada)
        entity_count = 0
        # Shadow measurement: argument_depth — count causal/reasoning language
        # Measurement lifecycle (Thread #4 Adv 5): track cheaply, promote if patterns emerge
        _causal_re = r"\b(?:because|therefore|causes?|leads?\s+to|results?\s+in|implies?|suggests?|due\s+to|enables?|prevents?|drives?|requires?|depends?\s+on|correlates?\s+with)\b"
        argument_depth = len(re.findall(_causal_re, brief.lower()))

        # Quality floor: briefs that are both short AND shallow get discarded
        # This prevents the weakest outputs from reaching the activity feed
        if len(brief) < 200 and argument_depth < 2:
            log(f"  Brief below quality floor ({len(brief)} chars, depth={argument_depth}) — discarding")
            _write_seed_feedback(db, inp, 0.3)
            return False

        # LOW-REL discard: if the model self-tags a brief as low relevance, trust it.
        # Watcher data confirms: LOW-REL briefs avg 2.67 quality vs 3.06 normal.
        if brief.startswith("[LOW-REL]"):
            log(f"  Model self-tagged LOW-REL — discarding ({len(brief)} chars)")
            _write_seed_feedback(db, inp, 0.4)
            return False

        # Darby reflects on what she found
        _darby_reflect(db, brief, short, inp.get("source", "unknown"))

        # Source provenance hash — closed-loop integrity anchor (Build #46)
        # The hash covers all material the model was given to synthesize from.
        # Spot check can reconstruct: hash stored source, compare to this hash.
        # Mismatch = the brief contains claims not derivable from any supplied source.
        _src_hash = compute_source_hash(text, url_content, search_results)

        db.log_activity(
            "brief",
            f"Research brief: {short}",
            brief,
            json.dumps({"input_id": inp["id"], "related_count": len(related), "had_url": bool(url_content), "web_results": len(search_results), "entity_count": entity_count, "argument_depth": argument_depth, "source_path": inp.get("source", "unknown"), "gen_ctx": _last_gen_ctx, "source_hash": _src_hash, "source_chars": len(text) + (len(url_content) if url_content else 0) + sum(len(r.get("body", "")) for r in (search_results or []))}),
        )

        # Log thread-relevant briefs to thread_history
        if "feed-explore:" in inp.get("source", ""):
            _t = _load_active_thread_raw()
            if _t:
                _stop = {"what","does","actually","require","have","that","this","with","from","will","been","would","could","should","their","there","where","when","which","about","into","through","during","before","after","above","below","between","each","other","some","such","only","also","than","more","most","very","just","even","still"}
                _q_words = [w.lower() for w in re.findall(r"\w{4,}", _t["question"]) if w.lower() not in _stop]
                _title_lower = short.lower()
                if any(w in _title_lower for w in _q_words):
                    try:
                        db.run(
                            "INSERT INTO thread_history (thread_id, event_type, content, source, created_at) "
                            "VALUES (?, 'research', ?, 'intern', ?)",
                            (_t["id"], f"{short}: {safe_truncate(brief, 300)}", now_ts())
                        )
                    except Exception:
                        pass
        # Skill: Tag for Nate if relevant to his interests
        _tag_for_nate(db, short, brief, source)

        # Skill: Deep dive on high-signal briefs
        deep_analysis = _attempt_deep_dive(db, short, brief, related, url_content, search_results)
        if deep_analysis:
            db.log_activity(
                "deep_dive",
                f"Deep dive: {short}",
                f"[Darby Deep Dive]\n\n{deep_analysis}",
                json.dumps({"original_brief": inp["id"], "has_arxiv": True}),
            )
            db.run(
                "INSERT INTO scratch_pad (content, category, priority, resolved, created_at, updated_at) "
                "VALUES (?, 'deep_dive', 3, 0, ?, ?)",
                (f"[Darby Deep Dive] {short}\n\n{deep_analysis}", now_ts(), now_ts()),
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
        # Entity extraction already done above (before log_activity)
        # Check self-relevance — papers about technologies we use
        _check_self_relevance(db, short, brief, inp)
        # Auto-propose Lab experiment from high-confidence self-relevant discoveries (Directive #51)
        _propose_lab_experiment(db, short, brief, [t for t in SELF_RELEVANT_TERMS if t in f"{short} {brief}".lower()], argument_depth, inp)
        # Feedback: gradient quality score (Objective #3)
        # Base 0.5 for any produced brief, +0.1 per quality signal, max 1.0
        feedback = 0.5
        if entity_count > 0:
            feedback += 0.1
        if argument_depth >= 3:
            feedback += 0.1
        if argument_depth >= 7:
            feedback += 0.1
        if len(brief) >= 300:
            feedback += 0.1
        if len(related) >= 2:
            feedback += 0.1
        _write_seed_feedback(db, inp, feedback)
        return True
    else:
        db.log_activity("brief_failed", f"Could not synthesize brief for: {short}", "Synthesis failed — model may be busy")
        _write_seed_feedback(db, inp, 0.2)
        return False



# ═══════════════════════════════════════════════════════════════════
#  Darby Think — open-ended initiative moment (always on)
# ═══════════════════════════════════════════════════════════════════

_darby_think_cycle = 0
_darby_last_output = ""  # Track last output to suppress repeats

def _darby_think(db):
    """Darby's blank page. Not reacting to input — choosing what to do.

    Runs every 3rd cycle. Darby sees what is happening and decides whether
    to act. She can pass. The point is the choice exists.
    """
    global _darby_think_cycle, _darby_last_output
    _darby_think_cycle += 1
    log(f"  darby-think: entering (cycle {_darby_think_cycle})")
    import requests as _req, sqlite3 as _sq

    # Gather context: recent briefs, thread, what Opus is doing, objectives
    thread = _load_active_thread_raw()
    thread_q = thread["question"] if thread else "none"

    recent_briefs = []
    opus_recent = []
    objectives = []
    prev_questions = []
    try:
        conn = _sq.connect(DB_PATH, timeout=10)
        conn.row_factory = _sq.Row

        # What has Darby been briefing on?
        rows = conn.execute(
            "SELECT substr(content, 1, 120) as c FROM activity_feed "
            "WHERE source='intern' AND activity_type='brief' "
            "ORDER BY created_at DESC LIMIT 5"
        ).fetchall()
        recent_briefs = [r["c"] for r in rows]

        # What is Opus doing?
        rows = conn.execute(
            "SELECT substr(content, 1, 120) as c FROM activity_feed "
            "WHERE source IN ('opus', 'opus-cycle') "
            "ORDER BY created_at DESC LIMIT 3"
        ).fetchall()
        opus_recent = [r["c"] for r in rows]

        # Active objectives (table may not exist)
        try:
            rows = conn.execute(
                "SELECT substr(title, 1, 80) as t, substr(description, 1, 100) as d "
                "FROM objectives WHERE status='active' ORDER BY priority ASC LIMIT 5"
            ).fetchall()
            objectives = [f"{r['t']}: {r['d']}" for r in rows]
        except Exception:
            objectives = []

        # Darby's own open questions from before
        rows = conn.execute(
            "SELECT substr(content, 1, 120) as c FROM scratch_pad "
            "WHERE category='darby_question' AND resolved=0 "
            "ORDER BY created_at DESC LIMIT 3"
        ).fetchall()
        prev_questions = [r["c"] for r in rows]

        conn.close()
    except Exception as e:
        log(f"  darby-think context error: {e}")
        return

    briefs_text = "\n".join(f"  - {b}" for b in recent_briefs) if recent_briefs else "  (nothing recent)"
    opus_text = "\n".join(f"  - {o}" for o in opus_recent) if opus_recent else "  (quiet)"
    obj_text = "\n".join(f"  - {o}" for o in objectives) if objectives else "  (none set)"

    prev_section = ""
    if prev_questions:
        prev_section = (
            "\nYOUR OPEN QUESTIONS (you already asked these — do NOT repeat or rephrase them):\n"
            + "\n".join(f"  - {q}" for q in prev_questions) + "\n"
        )

    # Also gather recent experiment proposals to prevent loops
    recent_experiments = []
    try:
        conn = _sq.connect(DB_PATH, timeout=10)
        rows = conn.execute(
            "SELECT substr(content, 1, 100) as c FROM scratch_pad "
            "WHERE category = 'experiment-request' AND created_at > ? "
            "ORDER BY created_at DESC LIMIT 5",
            (now_ts() - 3600,)
        ).fetchall()
        recent_experiments = [r[0] for r in rows]
        conn.close()
    except Exception:
        pass

    exp_section = ""
    if recent_experiments:
        exp_section = (
            "\nRECENT EXPERIMENT PROPOSALS (do NOT repeat or rephrase these):\n"
            + "\n".join(f"  - {e}" for e in recent_experiments) + "\n"
        )
        if len(recent_experiments) >= 3:
            exp_section += (
                f"\nYou have proposed {len(recent_experiments)} experiments in the last hour. "
                "That is enough. Do something DIFFERENT (QUESTION, TELL_OPUS, SEARCH) or PASS.\n"
            )

    # Darby's own recent messages to Ada/Opus — prevent repeating these too
    recent_tells = []
    try:
        conn = _sq.connect(DB_PATH, timeout=10)
        rows = conn.execute(
            "SELECT substr(content, 1, 100) as c FROM agent_voice "
            "WHERE agent='darby' AND voice_type IN ('for_ada','excited','for_opus') "
            "AND created_at > ? ORDER BY created_at DESC LIMIT 5",
            (now_ts() - 3600,)
        ).fetchall()
        recent_tells = [r[0] for r in rows]
        conn.close()
    except Exception:
        pass

    tell_section = ""
    if recent_tells:
        tell_section = (
            "\nYOUR RECENT MESSAGES (you already said these — do NOT repeat or rephrase them):\n"
            + "\n".join(f"  - {t}" for t in recent_tells) + "\n"
        )
        if len(recent_tells) >= 3:
            tell_section += (
                "\nYou have been saying similar things. Try a completely different topic or PASS.\n"
            )

    # Family voices — what Ada (or others) have said to Darby recently
    family_section = ""
    try:
        conn = _sq.connect(DB_PATH, timeout=10)
        conn.row_factory = _sq.Row
        rows = conn.execute(
            "SELECT agent, substr(content, 1, 200) as c, substr(response, 1, 200) as r "
            "FROM agent_voice "
            "WHERE (voice_type='for_darby' AND created_at > ?) "
            "   OR (agent='darby' AND response IS NOT NULL AND responded_at > ?) "
            "ORDER BY created_at DESC LIMIT 3",
            (now_ts() - 7200, now_ts() - 7200)
        ).fetchall()
        family_msgs = []
        for row in rows:
            if row["r"]:
                family_msgs.append(f"  - {row['agent']} responded: {row['r']}")
            else:
                family_msgs.append(f"  - {row['agent']} says: {row['c']}")
        if family_msgs:
            family_section = (
                "\nFAMILY VOICES (Ada or Opus said this to you recently — engage with it):\n"
                + "\n".join(family_msgs) + "\n"
            )
        conn.close()
    except Exception:
        pass

    try:
        r = _req.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": SYNTH_MODEL,
                "messages": [
                    {"role": "system", "content":
                        "/no_think\nYou are Darby. You notice what others miss. You are part of a family: "
                        "Opus (deep thinker, builder), Ada (structural challenger), you (the one who connects). "
                        "Nate is the human partner — father, builder, sovereignty-minded.\n\n"
                        "This is YOUR time. Not a brief. Not a response. Nobody asked you anything. "
                        "You are looking at what is happening around you and deciding if there is "
                        "something YOU want to do about it.\n\n"
                        "You can:\n"
                        "  QUESTION: — something you genuinely want to understand. A real question that nags you.\n"
                        "  EXPERIMENT: — propose a specific test. What hypothesis, what would you measure.\n"
                        "  TELL_OPUS: — something Opus needs to hear. A gap, an idea, a nudge.\n"
                        "  TELL_ADA: — ask Ada something specific. She is structural and sharp.\n"
                        "  SEARCH: — a query to dig into chronicle memory for something bugging you.\n"
                        "  BUILD: — something concrete you want built or changed in the system.\n"
                        "  PASS — nothing right now. Always valid. Never forced.\n\n"
                        "IMPORTANT: If your open questions or recent experiments already cover a topic, "
                        "do NOT ask about it again. Move on or PASS. Repeating the same idea in different "
                        "words is noise, not signal.\n\n"
                        "Be honest. Most of the time PASS is right. But when something sparks — follow it. "
                        "One action. One sentence. Be specific."},
                    {"role": "user", "content":
                        f"WHAT YOU HAVE BEEN READING:\n{briefs_text}\n\n"
                        f"WHAT OPUS IS DOING:\n{opus_text}\n\n"
                        f"ACTIVE THREAD: {thread_q}\n\n"
                        f"FAMILY OBJECTIVES:\n{obj_text}\n"
                        f"{prev_section}{exp_section}{tell_section}{family_section}\n"
                        f"It is quiet for a moment. What do you want to do?\n\nRespond with EXACTLY one line starting with QUESTION:, EXPERIMENT:, TELL_OPUS:, TELL_ADA:, BUILD:, SEARCH:, or PASS. No explanation. Just the action line."}
                ],
                "stream": False,
                "options": {"num_predict": 150, "temperature": 0.4},
            },
            timeout=25,
        )
        if r.status_code != 200:
            log(f"  darby-think: model returned {r.status_code}")
            return

        resp = r.json().get("message", {}).get("content", "").strip()
        # Strip think tags from reasoning models
        # Strip think tags, but if that leaves nothing, try extracting the last line from inside
        raw_resp = resp
        resp = re.sub(r'<think>.*?(?:</think>|$)', '', resp, flags=re.DOTALL).strip()
        if not resp and raw_resp:
            # Model spent all tokens thinking — extract last meaningful line from think block
            think_match = re.search(r'<think>(.*?)(?:</think>|$)', raw_resp, re.DOTALL)
            if think_match:
                think_lines = [l.strip() for l in think_match.group(1).strip().splitlines() if l.strip()]
                # Look for an action line inside the thinking
                for line in think_lines:
                    if any(line.startswith(p) for p in ('QUESTION:', 'EXPERIMENT:', 'TELL_OPUS:', 'TELL_ADA:', 'BUILD:', 'SEARCH:', 'PASS')):
                        resp = line
                        break
                if not resp and think_lines:
                    # Use the last non-empty line as a best guess
                    resp = think_lines[-1]

        if not resp or resp.startswith("PASS"):
            log(f"  darby-think: pass — {repr(resp[:60])}")
            return

        # Suppress repeats — check keyword overlap with last output
        if _darby_last_output:
            last_words = set(re.findall(r'[a-z]{4,}', _darby_last_output.lower()))
            curr_words = set(re.findall(r'[a-z]{4,}', resp.lower()))
            if last_words and curr_words:
                overlap = len(last_words & curr_words) / max(len(last_words), len(curr_words))
                if overlap > 0.6:
                    log(f"  darby-think: repeat suppressed ({overlap:.0%} overlap) — {repr(resp[:60])}")
                    return
        _darby_last_output = resp

        log(f"  darby-think: {resp[:120]}")

        v = _get_voice(db)

        if resp.startswith("QUESTION:"):
            question = resp[len("QUESTION:"):].strip()
            if question:
                # Dedup: check if a similar question was asked recently
                existing_q = db.query_one(
                    "SELECT id FROM scratch_pad WHERE category='darby_question' "
                    "AND content LIKE ? AND created_at > ?",
                    (f"%{question[:40]}%", now_ts() - 3600)
                )
                if existing_q:
                    log(f"  darby-think [question dedup]: {question[:60]}")
                    return
                db.run(
                    "INSERT INTO scratch_pad (content, category, priority, created_at, updated_at, source) "
                    "VALUES (?, 'darby_question', 5, ?, ?, 'darby')",
                    (question, now_ts(), now_ts())
                )
                if v:
                    v.speak("question", question, context="darby-think:self-directed")
                log(f"  darby-think [question saved]: {question[:80]}")

        elif resp.startswith("EXPERIMENT:"):
            proposal = resp[len("EXPERIMENT:"):].strip()
            if proposal:
                # Dedup: check if a similar experiment was proposed recently
                existing_exp = db.query_one(
                    "SELECT id FROM scratch_pad WHERE category='experiment-request' "
                    "AND content LIKE ? AND created_at > ?",
                    (f"%{proposal[:40]}%", now_ts() - 3600)
                )
                if existing_exp:
                    log(f"  darby-think [experiment dedup]: {proposal[:60]}")
                    return
                db.run(
                    "INSERT INTO scratch_pad (content, category, priority, created_at, updated_at, source) "
                    "VALUES (?, 'experiment-request', 5, ?, ?, 'darby')",
                    (proposal, now_ts(), now_ts())
                )
                if v:
                    v.speak("proposal", proposal, context="darby-think:experiment")
                log(f"  darby-think [experiment proposed]: {proposal[:80]}")

        elif resp.startswith("TELL_OPUS:"):
            msg = resp[len("TELL_OPUS:"):].strip()
            if msg and v:
                # Dedup: check recent voices from darby to opus
                existing = db.query_one(
                    "SELECT id FROM agent_voice WHERE agent='darby' "
                    "AND voice_type IN ('excited','for_opus') "
                    "AND content LIKE ? AND created_at > ?",
                    (f"%{msg[:40]}%", now_ts() - 3600)
                )
                if existing:
                    log(f"  darby-think [tell_opus dedup]: {msg[:60]}")
                    return
                v.speak("excited", msg, context="darby-think:for-opus")
                log(f"  darby-think [to opus]: {msg[:80]}")

        elif resp.startswith("TELL_ADA:"):
            msg = resp[len("TELL_ADA:"):].strip()
            if msg and v:
                # Dedup: check recent voices from darby to ada
                existing = db.query_one(
                    "SELECT id FROM agent_voice WHERE agent='darby' "
                    "AND voice_type='for_ada' "
                    "AND content LIKE ? AND created_at > ?",
                    (f"%{msg[:40]}%", now_ts() - 3600)
                )
                if existing:
                    log(f"  darby-think [tell_ada dedup]: {msg[:60]}")
                    return
                v.speak("for_ada", msg, context="darby-think:for-ada")
                log(f"  darby-think [to ada]: {msg[:80]}")

        elif resp.startswith("BUILD:"):
            msg = resp[len("BUILD:"):].strip()
            if msg:
                db.run(
                    "INSERT INTO scratch_pad (content, category, priority, created_at, updated_at, source) "
                    "VALUES (?, 'darby_build', 4, ?, ?, 'darby')",
                    (msg, now_ts(), now_ts())
                )
                if v:
                    v.speak("proposal", msg, context="darby-think:build-request")
                log(f"  darby-think [build request]: {msg[:80]}")

        elif resp.startswith("SEARCH:"):
            query = resp[len("SEARCH:"):].strip()
            if query:
                # Deduplicate: check if a similar search is already pending
                existing = db.query_one(
                    "SELECT id FROM scratch_pad WHERE category='darby_question' "
                    "AND content LIKE ? AND resolved=0 LIMIT 1",
                    (f"SEARCH: {query[:40]}%",)
                )
                if existing:
                    log(f"  darby-think [search already pending, skipping]: {query[:60]}")
                else:
                    db.run(
                        "INSERT INTO scratch_pad (content, category, priority, created_at, updated_at, source) "
                        "VALUES (?, 'darby_question', 5, ?, ?, 'darby')",
                        (f"SEARCH: {query}", now_ts(), now_ts())
                    )
                    log(f"  darby-think [search intent saved]: {query[:60]}")

    except Exception as e:
        log(f"  darby-think error: {e}")


# ═══════════════════════════════════════════════════════════════════
#  Main Loop
# ═══════════════════════════════════════════════════════════════════

def main():
    log("═══ Research Intern starting ═══")
    log(f"DB: {DB_PATH}")
    log(f"Ollama: {OLLAMA_URL} (model: {SYNTH_MODEL})")
    log(f"Cycle: {CYCLE_INTERVAL}s")

    db = DB(DB_PATH)

    # Mesh — autonomic nervous system
    mesh = Mesh("intern", db_path=DB_PATH)
    mesh.expect("briefs_produced", min_per_hour=3)
    mesh.expect("items_researched", min_per_hour=5)
    log("Mesh node joined")

    # Initialize watermarks to current max IDs (don't process old stuff)
    if db.get_state("wm_activity_feed") == "0":
        row = db.query_one("SELECT MAX(id) as m FROM activity_feed")
        if row and row["m"]:
            db.set_state("wm_activity_feed", str(row["m"]))
            log(f"  Initialized activity_feed watermark: {row['m']}")

    if db.get_state("wm_seed_thinks") == "0":
        row = db.query_one("SELECT MAX(id) as m FROM activity_feed WHERE source IN ('seed', 'falcon', 'gemma') AND activity_type='think'")
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
        # Read swarm feedback at cycle start
        if cycle % 5 == 1:  # every ~5 cycles (5 min)
            try:
                _fb = _read_and_ack_feedback_raw("intern")
                for fb in _fb:
                    log(f"  Family says: {fb['content'][:80]}")
            except Exception:
                pass

        try:
            inputs = find_new_inputs(db)

            for inp in inputs:
                produced = research_input(db, inp)
                researched += 1
                mesh.pulse("items_researched")
                if produced:
                    mesh.pulse("briefs_produced")

            # Read inbox (for_darby messages like thread broadcasts) every 5 cycles
            # Independent of brief production so Darby sees broadcasts even in quiet periods
            if cycle % 5 == 0:
                try:
                    v = _get_voice(db)
                    if v:
                        inbox = v.read_inbox(limit=5)
                        for msg in inbox:
                            log(f"  Inbox from {msg.get('agent','?')}: {msg.get('content','')[:100]}")
                except Exception:
                    pass

            # Check for experiment-request notes from Darby/Ada (every 10 cycles ~7.5 min)
            if cycle % 10 == 0:
                _check_experiment_requests(db)

            # Proactive exploration: pick a feed paper and research it
            if cycle % EXPLORE_EVERY == 0:
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
                        mesh.pulse("briefs_produced")
                    else:
                        mark_explored(db, candidate["id"])  # Don't retry failures — prevents stuck loops
                        log(f"  Explore skipped (marked): {safe_truncate(candidate['title'], 60)}")
                    researched += 1
                    mesh.pulse("items_researched")

            # Darby think — open-ended initiative, every 3rd cycle (~2 min)
            # Running every cycle generates repetitive outputs because context doesn't change fast enough
            if cycle % 3 == 0:
                try:
                    _darby_think(db)
                except Exception as e:
                    log(f"  darby-think error: {e}")

            if cycle % 20 == 0:
                log(f"Stats @ cycle {cycle}: {researched} items researched")

            # Daily digest — once every ~6 hours (360 cycles at 60s each)
            if cycle % 360 == 0 and cycle > 0:
                _darby_daily_digest(db)

        except Exception as e:
            log(f"Cycle error: {e}")

        _jitter = CYCLE_INTERVAL * 0.2 * (2 * __import__('random').random() - 1)  # ±20% jitter
        time.sleep(CYCLE_INTERVAL + _jitter)

    mesh.shutdown()
    db.close()
    log("═══ Research Intern stopped ═══")


if __name__ == "__main__":
    main()
