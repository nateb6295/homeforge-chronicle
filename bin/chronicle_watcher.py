#!/usr/bin/env python3
"""Chronicle Watcher — Outbound quality oversight layer.

Inspired by ClawKeeper's Watcher paradigm and MetaClaw's two-loop learning.
Monitors agent outputs, scores quality, flags notable items, and accumulates
skill patterns for the swarm's learning loop.

Architecture:
  - Reads recent agent outputs from activity_feed
  - Scores on: relevance, novelty, quality (1-5 each)
  - Logs scores to watcher_scores table
  - Posts notable items to Discord #crew-watcher channel
  - Tracks agent performance patterns over time
  - Accumulates "skills" (learned rules) from flagged patterns

This is the OUTPUT-side gate. Gemma gates inbound. Watcher gates outbound.
"""

import os, sys, time, json, re, sqlite3, requests, signal
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from memory import MemoryCache

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get("CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"))
BIN_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.expanduser("~/chronicle/chronicle-watcher.log")

CYCLE_INTERVAL = int(os.environ.get("WATCHER_INTERVAL", "300"))  # 5 min
OLLAMA_URL = os.environ.get("WATCHER_OLLAMA_URL", "http://localhost:11434")  # for embeddings
INFERENCE_URL = os.environ.get("WATCHER_INFERENCE_URL", "http://localhost:11435")  # llama-server for Gemma 4
OLLAMA_MODEL = os.environ.get("WATCHER_MODEL", "gemma4:26b")

# Discord — posts to the watcher channel via bot token
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "")
WATCHER_CHANNEL_ID = os.environ.get("WATCHER_CHANNEL_ID", "1488178551491657728")

# Scoring thresholds
HIGH_QUALITY_THRESHOLD = 4.0   # average score >= this → notable positive
LOW_QUALITY_THRESHOLD = 2.0    # average score <= this → flag for review
DIGEST_INTERVAL = 3600         # hourly digest

# Agent outputs worth scoring (skip raw pickups, fetches, searches)
SCOREABLE_TYPES = {
    "brief", "deep_dive", "connection", "thread_challenge",
    "contrarian", "synthesis",
}
# Removed: "think" (gate output re-scoring is redundant), "kg_extraction" (structural data, not prose)

# ═══════════════════════════════════════════════════════════════════
#  Logging
# ═══════════════════════════════════════════════════════════════════

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass

# ═══════════════════════════════════════════════════════════════════
#  Database
# ═══════════════════════════════════════════════════════════════════

def init_db(db: sqlite3.Connection):
    """Create watcher tables if they don't exist."""
    db.executescript("""
        CREATE TABLE IF NOT EXISTS watcher_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            feed_id INTEGER NOT NULL,
            source TEXT NOT NULL,
            activity_type TEXT NOT NULL,
            relevance INTEGER NOT NULL,
            novelty INTEGER NOT NULL,
            quality INTEGER NOT NULL,
            avg_score REAL NOT NULL,
            rationale TEXT,
            flagged TEXT,
            scored_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS watcher_skills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent TEXT NOT NULL,
            skill_type TEXT NOT NULL,
            skill_text TEXT NOT NULL,
            examples TEXT,
            usage_count INTEGER DEFAULT 0,
            created_at INTEGER NOT NULL,
            last_used_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS watcher_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_watcher_scores_feed
            ON watcher_scores(feed_id);
        CREATE INDEX IF NOT EXISTS idx_watcher_scores_source
            ON watcher_scores(source);
        CREATE INDEX IF NOT EXISTS idx_watcher_scores_flagged
            ON watcher_scores(flagged);

        CREATE TABLE IF NOT EXISTS convergence_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            topic TEXT NOT NULL,
            agents TEXT NOT NULL,
            detail TEXT,
            relevance_spread REAL,
            created_at INTEGER NOT NULL
        );
    """)
    db.commit()


def get_state(db: sqlite3.Connection, key: str, default: str = "") -> str:
    row = db.execute(
        "SELECT value FROM watcher_state WHERE key=?", (key,)
    ).fetchone()
    return row[0] if row else default


def set_state(db: sqlite3.Connection, key: str, value: str):
    db.execute(
        "INSERT OR REPLACE INTO watcher_state(key, value) VALUES(?, ?)",
        (key, value)
    )
    db.commit()

# ═══════════════════════════════════════════════════════════════════
#  Scoring via Ollama
# ═══════════════════════════════════════════════════════════════════

def get_active_context(db: sqlite3.Connection) -> str:
    """Get current thread, objectives, and Nate's interests for relevance scoring."""
    parts = []

    # Nate's core interest domains (static — these rarely change)
    parts.append(
        "Nate's core domains: AI/ML, sovereignty/homeforge, crypto/XRP/Flare, "
        "cybersecurity, self-modification, local infrastructure, agent autonomy"
    )

    # Active thread — include question for richer context
    try:
        row = db.execute(
            "SELECT id, title, question FROM threads WHERE status='active' "
            "ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()
        if row:
            q = row[2][:300] if row[2] else ""
            parts.append(f"Active thread #{row[0]}: {row[1]}")
            if q:
                parts.append(f"Thread question: {q}")
    except Exception:
        pass

    # Active objectives
    try:
        rows = db.execute(
            "SELECT title FROM opus_objectives WHERE status='active' ORDER BY priority ASC LIMIT 4"
        ).fetchall()
        if rows:
            parts.append("Active objectives: " + "; ".join(r[0] for r in rows))
    except Exception:
        pass

    # Open predictions — topics we're actively tracking
    try:
        rows = db.execute(
            "SELECT substr(claim,1,100), category FROM prediction_track "
            "WHERE status='open' ORDER BY deadline ASC LIMIT 5"
        ).fetchall()
        if rows:
            parts.append("Open predictions (actively tracking): " +
                         "; ".join(f"[{r[1]}] {r[0]}" for r in rows))
    except Exception:
        pass

    # Recent Nate captures (what he's interested in right now)
    try:
        rows = db.execute(
            "SELECT substr(content,1,150) FROM activity_feed "
            "WHERE source LIKE 'operator:%' AND created_at > strftime('%s','now','-4 hours') "
            "ORDER BY created_at DESC LIMIT 5"
        ).fetchall()
        if rows:
            parts.append("Recent Nate captures: " + "; ".join(r[0] for r in rows))
    except Exception:
        pass

    # Home context from HAL (is Nate home? house active/quiet?)
    try:
        row = db.execute(
            "SELECT activity_type, content, created_at FROM activity_feed "
            "WHERE source='hal' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if row:
            age_min = (int(time.time()) - row[2]) // 60
            if age_min < 120:  # only include if recent (< 2 hours)
                parts.append(f"Home context: {row[1]} ({age_min}m ago)")
    except Exception:
        pass

    return "\n".join(parts) if parts else "General monitoring"


def score_output(content: str, source: str, activity_type: str,
                 context: str) -> Optional[Dict]:
    """Score an agent output using Gemma. Returns dict with scores or None."""
    prompt = f"""Score this agent output on three dimensions (1-5 each).

CONTEXT: {context}

AGENT: {source}
TYPE: {activity_type}
OUTPUT: {content[:800]}

Score on:
- RELEVANCE (1-5): How relevant is this to the active thread, Nate's interests, or current objectives?
  IMPORTANT: Being on-topic is NOT enough for a high score. A routine AI/ML paper is a 2. A 4-5 must change how we think about the domain, reveal a surprising result, or have direct real-world consequences. Use the FULL 1-5 range — do not default to 2-3 for familiar domains.
- NOVELTY (1-5): Is this genuinely new information/insight, or a restatement of something already known? Score 1-2 for incremental work in well-trodden areas. Score 4-5 only for methods, findings, or connections we haven't seen before.
- QUALITY (1-5): Is the analysis well-formed, substantive, and appropriately scoped? (Not stretching, not repetitive)

Scoring calibration: 1=noise/irrelevant, 2=on-topic but routine, 3=useful insight, 4=important finding, 5=must-act-on. If everything scores 2-3, you are not discriminating enough.

Respond ONLY with JSON, nothing else:
{{"relevance": N, "novelty": N, "quality": N, "rationale": "one sentence"}}"""

    try:
        resp = requests.post(
            f"{INFERENCE_URL}/v1/chat/completions",
            json={
                "model": OLLAMA_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 150,
                "temperature": 0.1,
                "reasoning_format": "none",
            },
            timeout=120
        )
        raw = ""
        if resp.status_code == 200:
            rdata = resp.json()
            if rdata.get("choices"):
                raw = rdata["choices"][0].get("message", {}).get("content", "").strip()
        # Strip Gemma 4 thinking tags if present
        if "<channel|>" in raw:
            raw = raw.split("<channel|>")[-1].strip()
        text = raw
        # Extract JSON from response (handle markdown fences)
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        # Find the JSON object
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            data = json.loads(text[start:end])
            # Validate scores are 1-5
            for key in ("relevance", "novelty", "quality"):
                val = int(data.get(key, 3))
                data[key] = max(1, min(5, val))
            data["avg_score"] = round(
                (data["relevance"] + data["novelty"] + data["quality"]) / 3, 2
            )
            return data
    except Exception as e:
        log(f"Score error: {e}")
    return None


def score_output_frame_b(content: str, source: str,
                         activity_type: str) -> Optional[Dict]:
    """Score using Frame B — surprise/actionability/durability.
    Thread #268: single evaluation frame is blind to dimensions it doesn't measure."""
    prompt = f"""Score this agent output on three dimensions (1-5 each).

AGENT: {source}
TYPE: {activity_type}
OUTPUT: {content[:800]}

Score on:
- SURPRISE (1-5): Did this change your model of the world? 1=completely expected, 5=genuinely did not see this coming.
- ACTIONABILITY (1-5): Should a decision-maker adjust their strategy or attention based on this? 1=no adjustment needed, 3=worth monitoring, 5=demands immediate response or position change.
- DURABILITY (1-5): Will this still matter in 6 months? 1=ephemeral news cycle, 5=structural shift that compounds.

Be ruthless. Most things are 1-2 on surprise. Actionability is about strategic relevance, not physical action — a regulatory shift scores 4 even if nobody moves today.

Respond ONLY with JSON:
{{"surprise": N, "actionability": N, "durability": N, "rationale": "one sentence"}}"""

    try:
        resp = requests.post(
            f"{INFERENCE_URL}/v1/chat/completions",
            json={
                "model": OLLAMA_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 150,
                "temperature": 0.1,
                "reasoning_format": "none",
            },
            timeout=120
        )
        raw = ""
        if resp.status_code == 200:
            rdata = resp.json()
            if rdata.get("choices"):
                raw = rdata["choices"][0].get("message", {}).get("content", "").strip()
        if "<channel|>" in raw:
            raw = raw.split("<channel|>")[-1].strip()
        text = raw
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            data = json.loads(text[start:end])
            for key in ("surprise", "actionability", "durability"):
                data[key] = max(1, min(5, int(data.get(key, 3))))
            data["avg_score_b"] = round(
                (data["surprise"] + data["actionability"] + data["durability"]) / 3, 2
            )
            return data
    except Exception as e:
        log(f"Frame B score error: {e}")
    return None


# ═══════════════════════════════════════════════════════════════════
#  Discord
# ═══════════════════════════════════════════════════════════════════

def post_discord(message: str):
    """Post to the watcher Discord channel."""
    if not DISCORD_TOKEN:
        log("No DISCORD_TOKEN — skipping Discord post")
        return
    try:
        url = f"https://discord.com/api/v10/channels/{WATCHER_CHANNEL_ID}/messages"
        requests.post(
            url,
            headers={
                "Authorization": f"Bot {DISCORD_TOKEN}",
                "Content-Type": "application/json"
            },
            json={"content": message[:1900]},
            timeout=10
        )
    except Exception as e:
        log(f"Discord error: {e}")

# ═══════════════════════════════════════════════════════════════════
#  Main cycle
# ═══════════════════════════════════════════════════════════════════

def run_cycle(db: sqlite3.Connection):
    """Score recent unscored agent outputs."""
    last_id = int(get_state(db, "last_scored_id", "0"))
    context = get_active_context(db)

    # Catchup mode: skip Frame B when backlog > 100 items
    max_id = db.execute("SELECT MAX(id) FROM activity_feed").fetchone()[0] or 0
    catchup_mode = (max_id - last_id) > 100
    if catchup_mode:
        log(f"Catchup mode: {max_id - last_id} items behind — skipping Frame B")

    # Fresh-first + interleaved catchup: score newest unscored items first,
    # then also process a few catchup items per cycle so the backlog drains.
    types_sql = ",".join(f"'{t}'" for t in SCOREABLE_TYPES)
    fresh_cutoff = max(last_id, max_id - 200)  # recent 200 IDs
    is_fresh_pass = False
    fresh_rows = db.execute(
        "SELECT id, source, activity_type, content FROM activity_feed "
        "WHERE id > ? AND activity_type IN ({}) "
        "AND id NOT IN (SELECT feed_id FROM watcher_scores) "
        "ORDER BY id DESC LIMIT 10".format(types_sql),
        (fresh_cutoff,)
    ).fetchall()

    # Always try a few catchup items too (interleaved), skip already-scored
    catchup_rows = db.execute(
        "SELECT id, source, activity_type, content FROM activity_feed "
        "WHERE id > ? AND activity_type IN ({}) "
        "AND id NOT IN (SELECT feed_id FROM watcher_scores) "
        "ORDER BY id ASC LIMIT 5".format(types_sql),
        (last_id,)
    ).fetchall()

    fresh_ids = {r[0] for r in fresh_rows}
    if fresh_rows:
        is_fresh_pass = True
        # Combine: fresh items first, then catchup items
        rows = fresh_rows + [r for r in catchup_rows if r[0] not in fresh_ids]
        log(f"Fresh-first: {len(fresh_rows)} fresh + {len(rows)-len(fresh_rows)} catchup")
    elif catchup_rows:
        rows = catchup_rows
    else:
        rows = []

    if not rows:
        return 0, False

    scored_count = 0
    flags = []

    for feed_id, source, atype, content in rows:
        item_is_fresh = feed_id in fresh_ids
        # Skip very short outputs
        if len(content) < 50:
            if not item_is_fresh:
                set_state(db, "last_scored_id", str(feed_id))
            continue

        scores = score_output(content, source, atype, context)
        if not scores:
            # Track retry count — skip after 5 failures to avoid infinite loops
            _retry_key = f"_watcher_retry_{feed_id}"
            _retries = int(get_state(db, _retry_key, "0"))
            if _retries >= 5:
                log(f"Score failed {_retries+1}x for {source}/{atype} (id={feed_id}) — SKIPPING")
                if not item_is_fresh:
                    set_state(db, "last_scored_id", str(feed_id))
                set_state(db, _retry_key, "0")  # clean up
                continue
            set_state(db, _retry_key, str(_retries + 1))
            log(f"Score failed for {source}/{atype} (id={feed_id}) — retry {_retries+1}/5")
            break

        avg = scores["avg_score"]
        rationale = scores.get("rationale", "")

        # Determine flag
        flag = None
        if avg >= HIGH_QUALITY_THRESHOLD:
            flag = "high"
        elif avg <= LOW_QUALITY_THRESHOLD:
            flag = "low"

        # Frame B scoring — always run on fresh items (newest, most actionable).
        # Only skip on catchup items during catchup mode.
        skip_b = catchup_mode and not item_is_fresh
        scores_b = None if skip_b else score_output_frame_b(content, source, atype)
        sur = scores_b["surprise"] if scores_b else None
        act = scores_b["actionability"] if scores_b else None
        dur = scores_b["durability"] if scores_b else None
        avg_b = scores_b["avg_score_b"] if scores_b else None
        rat_b = scores_b.get("rationale", "") if scores_b else None

        db.execute(
            "INSERT INTO watcher_scores "
            "(feed_id, source, activity_type, relevance, novelty, quality, "
            " avg_score, rationale, flagged, scored_at, "
            " surprise, actionability, durability, avg_score_b, rationale_b) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (feed_id, source, atype, scores["relevance"], scores["novelty"],
             scores["quality"], avg, rationale, flag,
             int(time.time()),
             sur, act, dur, avg_b, rat_b)
        )
        db.commit()
        scored_count += 1

        # Memory feedback — mark recent memory items as useful/not useful
        try:
            agent = source.split(":")[0] if ":" in source else source
            mc = MemoryCache(DB_PATH, agent, OLLAMA_URL)
            # Get memory items surfaced for this agent in the last 10 minutes
            recent = db.execute(
                "SELECT item_type, item_id FROM memory_access_log "
                "WHERE agent = ? AND accessed_at > ? AND was_useful IS NULL",
                (agent, int(time.time()) - 600)
            ).fetchall()
            if recent:
                items = [(r[0], r[1]) for r in recent]
                if avg >= 3.5:
                    mc.mark_useful(items)
                elif avg <= 2.0:
                    mc.mark_not_useful(items)
        except Exception:
            pass

        if flag:
            flags.append((feed_id, source, atype, avg, rationale, flag))

        if not item_is_fresh:
            set_state(db, "last_scored_id", str(feed_id))

    # Post flagged items to Discord
    for feed_id, source, atype, avg, rationale, flag in flags:
        agent = source.split(":")[0] if ":" in source else source
        if flag == "high":
            icon = "⭐"
            label = "Strong output"
        else:
            icon = "⚠️"
            label = "Quality flag"
        msg = (f"{icon} **{label}** [{agent}/{atype}] "
               f"Score: {avg:.1f}/5\n{rationale}")
        post_discord(msg)

    log(f"Scored {scored_count} outputs, {len(flags)} flagged")
    return scored_count, catchup_mode


def run_convergence_check(db: sqlite3.Connection):
    """Detect convergence warnings and disagreement signals in scoring patterns.

    From Thread #266: a system without external feedback must treat convergence
    as a warning rather than a goal. When all agents score the same topic
    identically, that's either genuine agreement or a shared blind spot.
    """
    last_check = int(get_state(db, "last_convergence_check", "0"))
    now = int(time.time())
    if now - last_check < 3600 * 4:  # every 4 hours
        return

    # Topic keywords and their domains
    TOPIC_PATTERNS = {
        "geopolitics": ["iran", "houthi", "yemen", "ceasefire", "military", "war"],
        "ai_ml": ["model", "neural", "learning", "llm", "training", "inference"],
        "security": ["security", "cyber", "malware", "detect", "vulnerability", "encryption"],
        "agents": ["agent", "agentic", "autonomous", "swarm"],
        "crypto": ["blockchain", "crypto", "xrp", "flare", "defi", "token"],
        "sovereignty": ["sovereign", "self-host", "local-first", "homeforge", "decentrali"],
    }

    # Get last 48h of scored items with titles
    rows = db.execute(
        "SELECT ws.source, ws.relevance, ws.novelty, ws.quality, LOWER(af.title) "
        "FROM watcher_scores ws "
        "JOIN activity_feed af ON ws.feed_id = af.id "
        "WHERE ws.scored_at > ?",
        (now - 48 * 3600,)
    ).fetchall()

    if len(rows) < 20:
        set_state(db, "last_convergence_check", str(now))
        return

    # Classify items by topic
    topic_data = {t: [] for t in TOPIC_PATTERNS}
    for agent, rel, nov, qual, title in rows:
        for topic, keywords in TOPIC_PATTERNS.items():
            if any(kw in title for kw in keywords):
                topic_data[topic].append({"agent": agent, "relevance": rel, "novelty": nov, "quality": qual})
                break  # first match only

    events = []
    for topic, items in topic_data.items():
        if len(items) < 3:
            continue

        agents = set(i["agent"] for i in items)
        rels = [i["relevance"] for i in items]
        avg_rel = sum(rels) / len(rels)
        spread = max(rels) - min(rels)

        # Per-agent averages
        agent_avgs = {}
        for a in agents:
            a_rels = [i["relevance"] for i in items if i["agent"] == a]
            if a_rels:
                agent_avgs[a] = sum(a_rels) / len(a_rels)

        # Convergence: multiple agents, tight spread on averages
        if len(agent_avgs) >= 2:
            avg_vals = list(agent_avgs.values())
            agent_spread = max(avg_vals) - min(avg_vals)

            if agent_spread < 0.5 and len(items) >= 5:
                event = {
                    "type": "convergence",
                    "topic": topic,
                    "agents": ",".join(sorted(agents)),
                    "detail": f"All agents score {topic} at {avg_rel:.1f} avg (spread={agent_spread:.2f}, n={len(items)}). "
                              f"Per-agent: {', '.join(f'{a}={v:.1f}' for a,v in sorted(agent_avgs.items()))}. "
                              f"Shared blind spot or genuine agreement?",
                    "spread": agent_spread,
                }
                events.append(event)

            elif agent_spread > 1.5:
                event = {
                    "type": "disagreement",
                    "topic": topic,
                    "agents": ",".join(sorted(agents)),
                    "detail": f"Agents DISAGREE on {topic} (spread={agent_spread:.2f}, n={len(items)}). "
                              f"Per-agent: {', '.join(f'{a}={v:.1f}' for a,v in sorted(agent_avgs.items()))}. "
                              f"Edge signal — investigate what drives the split.",
                    "spread": agent_spread,
                }
                events.append(event)

        # Single-evaluator: topic only scored by one agent
        if len(agents) == 1 and len(items) >= 5:
            event = {
                "type": "single_evaluator",
                "topic": topic,
                "agents": list(agents)[0],
                "detail": f"Topic '{topic}' only evaluated by {list(agents)[0]} ({len(items)} items, avg={avg_rel:.1f}). "
                          f"No ensemble disagreement possible — blind spot risk.",
                "spread": 0.0,
            }
            events.append(event)

    # Store events and post significant ones
    if events:
        lines = ["**Convergence Monitor** (Thread #266 build)"]
        for e in events:
            db.execute(
                "INSERT INTO convergence_events (event_type, topic, agents, detail, relevance_spread, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (e["type"], e["topic"], e["agents"], e["detail"], e["spread"], now)
            )
            icon = {"convergence": "🔄", "disagreement": "⚡", "single_evaluator": "👁️"}.get(e["type"], "❓")
            lines.append(f"  {icon} **{e['type']}** [{e['topic']}]: {e['detail'][:200]}")

        db.commit()

        # Post to Discord #alerts via webhook (not #opus — Opus voice only)
        _opus_wh = os.environ.get("ALERTS_WEBHOOK",
            "https://discord.com/api/webhooks/1489300749308395611/zlZZH3QzXqEDxznmNelK3mlkLF0IeZqoxwNUnrL0no_jfeZnDMvwvD_7XhYcCyQzq78I")
        try:
            msg = "\n".join(lines)[:1900]
            requests.post(_opus_wh, json={"content": msg}, timeout=10)
        except Exception:
            pass

        log(f"Convergence check: {len(events)} events detected")
    else:
        log("Convergence check: no significant patterns")

    set_state(db, "last_convergence_check", str(now))


def run_digest(db: sqlite3.Connection):
    """Post hourly performance digest."""
    last_digest = int(get_state(db, "last_digest_at", "0"))
    now = int(time.time())
    if now - last_digest < DIGEST_INTERVAL:
        return

    # Get scores from last hour
    rows = db.execute(
        "SELECT source, AVG(avg_score), COUNT(*), "
        "SUM(CASE WHEN flagged='high' THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN flagged='low' THEN 1 ELSE 0 END) "
        "FROM watcher_scores WHERE scored_at > ? "
        "GROUP BY source ORDER BY AVG(avg_score) DESC",
        (now - DIGEST_INTERVAL,)
    ).fetchall()

    if not rows:
        set_state(db, "last_digest_at", str(now))
        return

    total = sum(r[2] for r in rows)
    total_high = sum(r[3] for r in rows)
    total_low = sum(r[4] for r in rows)

    lines = [f"📊 **Watcher Digest** — {total} outputs scored this hour"]
    for source, avg, count, high, low in rows:
        agent = source.split(":")[0] if ":" in source else source
        markers = ""
        if high: markers += f" ⭐{int(high)}"
        if low: markers += f" ⚠️{int(low)}"
        lines.append(f"  **{agent}**: {avg:.1f}/5 avg ({int(count)} outputs){markers}")

    if total_high or total_low:
        lines.append(f"\nTotals: {int(total_high)} strong, {int(total_low)} flagged")

    post_discord("\n".join(lines))
    set_state(db, "last_digest_at", str(now))
    log(f"Digest posted: {total} outputs, {total_high} high, {total_low} low")


# ═══════════════════════════════════════════════════════════════════
#  Service loop
# ═══════════════════════════════════════════════════════════════════

def main():
    log("Chronicle Watcher starting")

    if not os.path.exists(os.path.dirname(DB_PATH)):
        db_path = "/mnt/hdd/chronicle-data/processed.db"
    else:
        db_path = DB_PATH

    if not os.path.exists(db_path):
        log(f"DB not found at {db_path}")
        sys.exit(1)

    db = sqlite3.connect(db_path)
    init_db(db)

    # Graceful shutdown
    running = True
    def handle_signal(sig, frame):
        nonlocal running
        running = False
        log("Shutdown signal received")
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    # If first run, start from recent items (don't score entire history)
    if get_state(db, "last_scored_id", "0") == "0":
        latest = db.execute(
            "SELECT MAX(id) FROM activity_feed"
        ).fetchone()
        if latest and latest[0]:
            # Start from 50 items back to have some initial data
            start_id = max(0, latest[0] - 50)
            set_state(db, "last_scored_id", str(start_id))
            log(f"First run — starting from feed ID {start_id}")

    post_discord("🔭 **Watcher online.** Monitoring agent output quality.")
    log("Watcher online — entering main loop")

    while running:
        try:
            result = run_cycle(db)
            count, catching_up = result if isinstance(result, tuple) else (result, False)
            run_digest(db)
            run_convergence_check(db)
            if count:
                log(f"Cycle complete: {count} scored")
        except Exception as e:
            log(f"Cycle error: {e}")
            catching_up = False

        # During catchup: 10s pause between batches instead of full interval
        sleep_time = 10 if catching_up else CYCLE_INTERVAL
        for _ in range(sleep_time):
            if not running:
                break
            time.sleep(1)

    db.close()
    log("Watcher stopped")


if __name__ == "__main__":
    main()
