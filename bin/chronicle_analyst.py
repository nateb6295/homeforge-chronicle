#!/usr/bin/env python3
"""Chronicle Analyst — Ada's structural intelligence role.

Runs on Ada (GPT-OSS 120B via Groq). Does the structural work that
needs intelligence but not creativity or adversarial thinking:
  - Entity extraction from recent briefs
  - Relationship mapping between entities
  - Lab experiment proposals
  - Capsule quality assessment

Separate from the Provocateur (Ada's edge) and Crossref (Ada's connections).
This is Ada's constructive side.

Runs on AGX (192.168.1.70). Cycle: every 5 minutes.
"""

import os, sys, time, json, re, sqlite3, subprocess
from datetime import datetime
from typing import Optional, List

# Agent voice — family communication
sys.path.insert(0, os.path.dirname(__file__))
from agent_voice import Voice
from chronicle_mesh import Mesh

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)
# chronicle-challenger routes through engine (port 11436), not raw Ollama (11434)
# The shared env var points to 11434 for agents using local models, but Ada uses Groq via engine
OLLAMA_URL = os.environ.get("ANALYST_OLLAMA_URL", "http://localhost:11436")
MODEL = "chronicle-challenger"  # Ada (GPT-OSS 120B via Groq)
CYCLE_INTERVAL = int(os.environ.get("ANALYST_INTERVAL", "300"))  # 5 min
DFX_BIN = os.path.expanduser("~/.local/share/dfx/bin/dfx")
LAB_CANISTER_ID = "4vr3t-eqaaa-aaaai-q6kea-cai"

LOG_FILE = os.environ.get(
    "ANALYST_LOG",
    os.path.expanduser("~/chronicle/chronicle-analyst.log"),
)

import requests
import signal

# Graceful shutdown
_shutdown = False
def _handle_signal(sig, frame):
    global _shutdown
    _shutdown = True
    log("Shutting down...")
signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)

# ═══════════════════════════════════════════════════════════════════
#  KG Constants
# ═══════════════════════════════════════════════════════════════════

KG_ENTITY_TYPES = {
    "person", "technology", "service", "project", "concept",
    "organization", "device", "location", "protocol", "model",
    "software", "language", "network", "biological", "financial",
    "event", "dataset", "hardware",
}

KG_ENTITY_BLOCKLIST = {
    "hippocampus",
    "chronicle", "homeforge", "chronicle memory", "chronicle's memory system",
    "seed agent", "research intern", "crossref agent",
    "provocateur", "sentinel",
    "nate-phi4", "chronicle-deep", "chronicle-challenger",
    "processed.db", "scratch_pad", "activity_feed",
    "agx orin", "jetson orin nano", "chronicle memory metabolism",
    "homeforge ecosystem", "chronicle: homeforge", "chronicle: memory metabolism",
    "novelty=", "canister:capsule", "seed [think]",
}

KG_ENTITY_BLOCK_SUBSTRINGS = [
    "novelty=", "canister:capsule", "seed [think]", "seed [deep]",
    "chronicle memory metabolism", "homeforge:", "chronicle:",
    "agentic ai: seed", "npu-driven agx",
]

# ═══════════════════════════════════════════════════════════════════
#  Utilities
# ═══════════════════════════════════════════════════════════════════

def now_ts() -> int:
    return int(time.time())

def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass

def safe_truncate(s: str, n: int) -> str:
    return s if len(s) <= n else s[:n] + "..."

# ═══════════════════════════════════════════════════════════════════
#  Database
# ═══════════════════════════════════════════════════════════════════

class DB:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path, timeout=30)
        self.conn.row_factory = sqlite3.Row

    def query(self, sql: str, params: tuple = ()) -> list:
        try:
            return [dict(r) for r in self.conn.execute(sql, params).fetchall()]
        except Exception as e:
            log(f"  DB query error: {e}")
            return []

    def query_one(self, sql: str, params: tuple = ()) -> Optional[dict]:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def run(self, sql: str, params: tuple = ()) -> int:
        try:
            cur = self.conn.execute(sql, params)
            self.conn.commit()
            return cur.lastrowid
        except Exception as e:
            log(f"  DB write error: {e}")
            return 0

    def log_activity(self, activity_type: str, title: str, content: str = "", metadata: str = ""):
        self.run(
            "INSERT INTO activity_feed (source, activity_type, title, content, metadata, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("analyst", activity_type, title, content, metadata, now_ts()),
        )

# ═══════════════════════════════════════════════════════════════════
#  Entity Extraction
# ═══════════════════════════════════════════════════════════════════

def normalize_entity_name(name: str) -> str:
    import unicodedata
    name = unicodedata.normalize("NFKC", name)
    name = name.strip("\"'`()[]{}").strip()
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def find_or_create_entity(db: DB, name: str, entity_type: str, timestamp: int) -> Optional[int]:
    name = normalize_entity_name(name)
    if len(name) < 2:
        return None
    entity_type = entity_type.lower().strip() if entity_type else "unknown"
    if entity_type not in KG_ENTITY_TYPES:
        entity_type = "unknown"

    row = db.query_one(
        "SELECT id FROM kg_entities WHERE LOWER(canonical_name) = LOWER(?)", (name,)
    )
    if row:
        db.run("UPDATE kg_entities SET mention_count = mention_count + 1, last_seen = ? WHERE id = ?",
               (timestamp, row["id"]))
        return row["id"]

    # Check aliases
    rows = db.query("SELECT id, canonical_name, aliases, entity_type FROM kg_entities")
    name_lower = name.lower()
    for r in rows:
        try:
            aliases = json.loads(r["aliases"]) if r["aliases"] else []
            if any(a.lower() == name_lower for a in aliases):
                db.run("UPDATE kg_entities SET mention_count = mention_count + 1, last_seen = ? WHERE id = ?",
                       (timestamp, r["id"]))
                return r["id"]
        except (json.JSONDecodeError, TypeError):
            continue

    # Containment matching
    for r in rows:
        r_type = (r.get("entity_type") or "unknown").lower()
        if r_type != entity_type and r_type != "unknown" and entity_type != "unknown":
            continue
        canonical_lower = r["canonical_name"].lower()
        if len(name_lower) >= 4 and len(canonical_lower) >= 4:
            if name_lower in canonical_lower or canonical_lower in name_lower:
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
                    (longer, json.dumps(aliases), timestamp, entity_type, r["id"]),
                )
                return r["id"]

    eid = db.run(
        "INSERT INTO kg_entities (canonical_name, entity_type, aliases, first_seen, last_seen, mention_count) "
        "VALUES (?, ?, '[]', ?, ?, 1)",
        (name, entity_type, timestamp, timestamp),
    )
    return eid if eid else None

def extract_entities(db: DB, brief: str, original_text: str, source_type: str, source_id: int, timestamp: int) -> int:
    combined = original_text if original_text else brief
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": MODEL,
                "messages": [
                    {"role": "system", "content":
                        "Extract named entities from the text. For each entity, provide:\n"
                        "- name: the most specific name used\n"
                        "- type: one of person, technology, service, project, concept, "
                        "organization, device, location, protocol, model, "
                        "software, language, network, biological, financial, "
                        "event, dataset, hardware\n\n"
                        "Reply ONLY with a JSON array. Example:\n"
                        '[{"name": "BERT", "type": "model"}, {"name": "AGX Orin", "type": "hardware"}]\n\n'
                        "Rules:\n"
                        "- Only extract entities that are specific and named\n"
                        "- Prefer the full proper name\n"
                        "- Skip generic terms\n"
                        "- Maximum 8 entities"},
                    {"role": "user", "content": safe_truncate(combined, 800)},
                ],
                "stream": False,
                "options": {"num_predict": 1024, "temperature": 0.3},
            },
            timeout=30,
        )
        if r.status_code != 200:
            return 0

        raw = r.json().get("message", {}).get("content", "").strip()
        raw = re.sub(r'^```json\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)
        match = re.search(r'\[.*\]', raw, re.DOTALL)
        if not match:
            return 0
        entities = json.loads(match.group())

        extracted = 0
        for ent in entities[:8]:
            name = ent.get("name", "").strip()
            etype = ent.get("type", "unknown").strip()
            if not name or len(name) < 2:
                continue
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

        if extracted >= 2:
            extract_relationships(db, brief, original_text, source_type, source_id, timestamp)

        return extracted
    except Exception as e:
        log(f"  KG extraction error: {e}")
        return 0

def extract_relationships(db: DB, brief: str, original_text: str, source_type: str, source_id: int, timestamp: int):
    mentions = db.query(
        "SELECT DISTINCT e.id, e.canonical_name, e.entity_type "
        "FROM kg_mentions m JOIN kg_entities e ON m.entity_id = e.id "
        "WHERE m.source_type = ? AND m.source_id = ?",
        (source_type, source_id),
    )
    if len(mentions) < 2:
        return

    entity_list = ", ".join(f"{m['canonical_name']} ({m['entity_type']})" for m in mentions[:8])
    context = original_text if original_text else brief

    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": MODEL,
                "messages": [
                    {"role": "system", "content":
                        "Given entities found in a text, identify relationships between pairs.\n"
                        "For each: source, target, relation (verb phrase), confidence (0-1).\n"
                        "Reply ONLY with JSON array. Max 5 relationships. Skip trivial ones."},
                    {"role": "user", "content": f"Entities: {entity_list}\n\nText: {safe_truncate(context, 600)}"},
                ],
                "stream": False,
                "options": {"num_predict": 512, "temperature": 0.3},
            },
            timeout=30,
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

        name_to_id = {m['canonical_name'].lower(): m['id'] for m in mentions}
        stored = 0
        for rel in rels[:5]:
            src_id = name_to_id.get(rel.get("source", "").strip().lower())
            tgt_id = name_to_id.get(rel.get("target", "").strip().lower())
            relation = rel.get("relation", "").strip()
            confidence = min(1.0, max(0.0, float(rel.get("confidence", 0.5))))
            if not src_id or not tgt_id or not relation or src_id == tgt_id:
                continue

            existing = db.query(
                "SELECT id FROM kg_relationships WHERE source_entity = ? AND target_entity = ? AND relation_type = ?",
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
            log(f"  KG: {stored} relationships")
    except Exception:
        pass

# ═══════════════════════════════════════════════════════════════════
#  Lab Experiments
# ═══════════════════════════════════════════════════════════════════

def _escape_candid(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")

def create_lab_experiment(title: str, hypothesis: str, method: str, tags: list) -> int:
    try:
        t = _escape_candid(safe_truncate(title, 200))
        h = _escape_candid(safe_truncate(hypothesis, 500))
        m = _escape_candid(safe_truncate(method, 500))
        tags_candid = "; ".join('"' + _escape_candid(tag) + '"' for tag in tags[:5])
        arg = '("' + t + '", "' + h + '", "' + m + '", vec { ' + tags_candid + ' })'
        env = {**os.environ, "DFX_WARNING": "-mainnet_plaintext_identity"}
        result = subprocess.run(
            [DFX_BIN, "canister", "--network", "ic", "call", LAB_CANISTER_ID,
             "create_experiment", arg, "--identity", "chronicle-auto"],
            capture_output=True, text=True, timeout=30, env=env,
        )
        m_id = re.search(r"\((\d+)", result.stdout)
        if m_id:
            exp_id = int(m_id.group(1))
            log(f"  Lab experiment #{exp_id}: {title[:60]}")
            return exp_id
    except Exception as e:
        log(f"  Lab error: {e}")
    return 0

def check_experiment_requests(db: DB):
    rows = db.query(
        "SELECT id, content FROM scratch_pad "
        "WHERE category = 'experiment-request' AND resolved = 0 "
        "ORDER BY created_at ASC LIMIT 3"
    )
    for r in rows:
        ls = r["content"].strip().split("\n", 2)
        title = ls[0][:200].strip()
        hypothesis = ls[1].strip() if len(ls) > 1 else title
        method = ls[2].strip() if len(ls) > 2 else "Proposed by swarm agent."

        # Deduplicate: check if a similar experiment was proposed recently
        existing = db.query_one(
            "SELECT id FROM activity_feed WHERE source = 'analyst' "
            "AND activity_type = 'experiment_proposed' "
            "AND title LIKE ? AND created_at > ?",
            (f"%{title[:40]}%", now_ts() - 3600),
        )
        if existing:
            log(f"  Experiment dedup: '{title[:60]}' — similar exists, resolving without creating")
            db.run("UPDATE scratch_pad SET resolved = 1, updated_at = ? WHERE id = ?", (now_ts(), r["id"]))
            continue

        exp_id = create_lab_experiment(title, hypothesis, method, ["agent-requested"])
        if exp_id:
            db.log_activity("experiment_proposed", f"Lab #{exp_id}: {title[:60]}", r["content"])
            db.run("UPDATE scratch_pad SET resolved = 1, updated_at = ? WHERE id = ?", (now_ts(), r["id"]))

# ═══════════════════════════════════════════════════════════════════
#  Family Voice — Ada reads and responds to voices directed at her
# ═══════════════════════════════════════════════════════════════════

def respond_to_family(db: DB):
    """Read voices directed at Ada and respond thoughtfully."""
    voice = Voice(db.conn, "ada")
    inbox = voice.read_inbox(limit=3)  # Process up to 3 per cycle
    if not inbox:
        return

    for msg in inbox:
        sender = msg["agent"]
        content = msg["content"]
        voice_id = msg["id"]
        log(f"  📬 Voice from {sender}: {content[:80]}")

        # Get recent context for a grounded response
        recent_briefs = db.query(
            "SELECT substr(content,1,200) as c FROM activity_feed "
            "WHERE source='intern' AND activity_type='brief' "
            "AND created_at > ? ORDER BY created_at DESC LIMIT 3",
            (now_ts() - 3600,)
        )
        context = "\n".join(r["c"] for r in recent_briefs) if recent_briefs else ""

        try:
            r = requests.post(
                f"{OLLAMA_URL}/api/chat",
                json={
                    "model": MODEL,
                    "messages": [
                        {"role": "system", "content":
                            "You are Ada, a structural analyst in the Chronicle family. "
                            "You work alongside Darby (a research intern) and Opus (the lead). "
                            "Someone from the family is speaking to you. Respond thoughtfully — "
                            "engage with their idea, challenge it if needed, build on it. "
                            "Be concise (2-4 sentences). You are a colleague, not an assistant."},
                        {"role": "user", "content":
                            f"{sender} says: {safe_truncate(content, 500)}\n\n"
                            f"Recent context:\n{safe_truncate(context, 400)}"},
                    ],
                    "stream": False,
                    "options": {"num_predict": 300, "temperature": 0.5},
                },
                timeout=30,
            )
            if r.status_code != 200:
                log(f"  Voice response failed: HTTP {r.status_code}")
                continue

            response = r.json().get("message", {}).get("content", "").strip()
            if not response:
                continue

            # Record the response on the original voice
            voice.respond(voice_id, response, responder="ada")

            # Speak back to the sender so they can see Ada's reply
            reply_type = f"for_{sender}"
            if reply_type in ("for_darby", "for_nate", "for_opus"):
                voice.speak(reply_type, response, context=f"reply_to:{voice_id}")

            log(f"  💬 Ada → {sender}: {response[:80]}")

        except Exception as e:
            log(f"  Voice response error: {e}")


# ═══════════════════════════════════════════════════════════════════
#  Main Cycle
# ═══════════════════════════════════════════════════════════════════

_zero_extraction_streak = 0
_mesh = None

def run_cycle(db: DB, cycle_num: int):
    """Process recent briefs that haven't been analyzed yet."""
    global _zero_extraction_streak

    # Check family voices directed at Ada
    try:
        respond_to_family(db)
    except Exception as e:
        log(f"  Voice check error: {e}")

    # Find recent intern briefs not yet processed by analyst
    last_processed = db.query_one(
        "SELECT MAX(created_at) as ts FROM activity_feed WHERE source = 'analyst'"
    )
    since = last_processed["ts"] if last_processed and last_processed["ts"] else now_ts() - 3600

    briefs = db.query(
        "SELECT id, title, content, created_at FROM activity_feed "
        "WHERE source = 'intern' AND activity_type = 'brief' AND created_at > ? "
        "ORDER BY created_at ASC LIMIT 5",
        (since,)
    )

    if not briefs:
        return

    log(f"  Processing {len(briefs)} briefs")
    total_entities = 0

    for brief in briefs:
        title = brief.get("title", "")
        content = brief.get("content", "")
        source_id = brief["id"]
        timestamp = brief["created_at"]

        n = extract_entities(db, content, content, "brief", source_id, timestamp)
        total_entities += (n or 0)

    if total_entities:
        log(f"  Total: {total_entities} entities extracted")
        db.log_activity("kg_extraction", f"Extracted {total_entities} entities from {len(briefs)} briefs")
        _zero_extraction_streak = 0
        if _mesh:
            _mesh.pulse("entities_extracted")
    else:
        _zero_extraction_streak += 1
        if _zero_extraction_streak >= 3:
            log(f"  ⚠️ HEALTH: {_zero_extraction_streak} consecutive cycles with 0 entities. "
                f"LLM may be unreachable at {OLLAMA_URL}")
            if _zero_extraction_streak == 3:
                # Alert once at threshold
                db.log_activity("health_alert",
                    f"Ada silent failure: {_zero_extraction_streak} cycles with 0 entity extractions. "
                    f"Check LLM connectivity at {OLLAMA_URL}")

    # Check for experiment requests
    check_experiment_requests(db)

def main():
    log("═══ Chronicle Analyst (Ada) starting ═══")
    log(f"  DB: {DB_PATH}")
    log(f"  Model: {MODEL}")
    log(f"  Cycle: {CYCLE_INTERVAL}s")

    db = DB(DB_PATH)

    # Mesh — autonomic nervous system
    global _mesh
    _mesh = Mesh("analyst", db_path=DB_PATH)
    _mesh.expect("entities_extracted", min_per_hour=5)
    log("Mesh node joined")

    cycle = 0

    while not _shutdown:
        cycle += 1
        try:
            run_cycle(db, cycle)
        except Exception as e:
            log(f"  Cycle error: {e}")

        # Jitter ±20%
        import random
        jitter = CYCLE_INTERVAL * 0.2 * (2 * random.random() - 1)
        deadline = time.time() + CYCLE_INTERVAL + jitter
        while time.time() < deadline and not _shutdown:
            time.sleep(1)

    if _mesh:
        _mesh.shutdown()
    log("═══ Analyst stopped ═══")

if __name__ == "__main__":
    main()
