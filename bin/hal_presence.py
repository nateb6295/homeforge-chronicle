#!/usr/bin/env python3
"""Chronicle Presence — Contextual awareness layer on top of HAL.

Subscribes to HAL events via MQTT, pulls context from the swarm and family,
and composes natural responses. Output goes to Discord (works now) and
MQTT voice topic (ready for Pi/Piper TTS).

This is the "don't forget about me" layer. When Nate walks in the door,
Chronicle says hello and shares something interesting.

Design principles:
  - Don't announce every event. Only speak when there's something worth saying.
  - Be a good roommate, not a security system.
  - Keep voice messages to 1-3 sentences. Discord can be slightly longer.
  - Morning briefing once per day. Welcome home only after quiet periods.

Runs as: systemctl --user start chronicle-presence
"""

import os, sys, json, time, sqlite3, random, threading
from datetime import datetime, timedelta

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("paho-mqtt required: pip install paho-mqtt")
    sys.exit(1)

try:
    import requests
except ImportError:
    requests = None

# ═══════════════════════════════════════════════════════════════════
#  Config
# ═══════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get("CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"))
MQTT_BROKER = os.environ.get("MQTT_BROKER", "192.168.1.10")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
VOICE_TOPIC = "homeforge/voice/speak"

# Discord webhook for presence messages (separate from alerts)
DISCORD_WEBHOOK = os.environ.get("PRESENCE_WEBHOOK", os.environ.get("OPUS_WEBHOOK", ""))

# ═══════════════════════════════════════════════════════════════════
#  State
# ═══════════════════════════════════════════════════════════════════

STATE_FILE = os.path.expanduser("~/.homeforge-chronicle/presence_state.json")


class PresenceState:
    """Track what we've said and when, so we don't repeat ourselves.
    Persists to disk so restarts don't cause duplicate messages."""
    def __init__(self):
        self.last_spoken_ts = 0
        self.last_morning_briefing = None  # date string
        self.last_welcome_ts = 0
        self.last_departure_ts = 0     # last departure farewell
        self.last_highlight_ts = 0     # last periodic highlight
        self.last_quiet_ts = 0         # when house went quiet
        self.house_is_quiet = False
        self.items_shared = set()      # IDs of crossref/capsules we've mentioned
        self._load()

    def _load(self):
        """Load persisted state from disk."""
        try:
            with open(STATE_FILE) as f:
                data = json.load(f)
            self.last_spoken_ts = data.get("last_spoken_ts", 0)
            self.last_morning_briefing = data.get("last_morning_briefing")
            self.last_welcome_ts = data.get("last_welcome_ts", 0)
            self.last_departure_ts = data.get("last_departure_ts", 0)
            self.last_highlight_ts = data.get("last_highlight_ts", 0)
            self.house_is_quiet = data.get("house_is_quiet", False)
            self.last_quiet_ts = data.get("last_quiet_ts", 0)
            self.items_shared = set(data.get("items_shared", []))
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        # On startup, verify quiet state against DB — retained MQTT messages
        # don't carry the original timestamp, so a restart would lose real
        # quiet duration. Check what HAL actually recorded.
        self._sync_quiet_from_db()

    def _sync_quiet_from_db(self):
        """On startup, check DB for actual quiet/active state.
        Retained MQTT messages carry the original publish time, but
        the event handler previously used time.time() which made restarts
        lose real quiet duration. This queries the actual HAL event timeline."""
        try:
            conn = sqlite3.connect(DB_PATH, timeout=5)
            # Get the most recent event to determine current house state
            row = conn.execute(
                "SELECT timestamp, event_type FROM hal_events "
                "ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()
            if row:
                last_type = row[1]
                last_ts = row[0]
                if last_type == "quiet_house":
                    self.house_is_quiet = True
                    self.last_quiet_ts = last_ts
                elif last_type in ("active_house", "arrival"):
                    self.house_is_quiet = False
                    # Still record when quiet last happened for welcome logic
                    quiet_row = conn.execute(
                        "SELECT MAX(timestamp) FROM hal_events "
                        "WHERE event_type='quiet_house'"
                    ).fetchone()
                    if quiet_row and quiet_row[0]:
                        self.last_quiet_ts = quiet_row[0]
            conn.close()
            self._save()
        except Exception:
            pass  # DB unavailable on startup — will recover via MQTT

    def _save(self):
        """Persist state to disk."""
        try:
            data = {
                "last_spoken_ts": self.last_spoken_ts,
                "last_morning_briefing": self.last_morning_briefing,
                "last_welcome_ts": self.last_welcome_ts,
                "last_departure_ts": self.last_departure_ts,
                "last_highlight_ts": self.last_highlight_ts,
                "house_is_quiet": self.house_is_quiet,
                "last_quiet_ts": self.last_quiet_ts,
                "items_shared": list(self.items_shared)[-50:],  # cap at 50
            }
            with open(STATE_FILE, "w") as f:
                json.dump(data, f)
        except Exception:
            pass

    def can_welcome(self):
        """Welcome if house was quiet 1+ hours OR no welcome in 2+ hours.

        Primary: house was quiet for 1+ hour before this arrival.
        Fallback: check DB for last arrival — if 2+ hours since last arrival
        event, this is a real homecoming regardless of quiet state tracking.
        This prevents service restarts from swallowing welcomes.
        """
        now = int(time.time())
        # Don't welcome more than once per 2 hours
        if now - self.last_welcome_ts < 7200:
            return False
        # Primary: house was quiet 1+ hour
        if self.house_is_quiet or (self.last_quiet_ts > 0
                                   and now - self.last_quiet_ts > 3600):
            return True
        # Fallback: check DB for last arrival event — if 2+ hours gap, welcome
        try:
            conn = sqlite3.connect(DB_PATH, timeout=5)
            row = conn.execute(
                "SELECT MAX(timestamp) FROM hal_events "
                "WHERE event_type='arrival' AND timestamp < ?",
                (now - 60,)  # exclude current event
            ).fetchone()
            conn.close()
            if row and row[0]:
                gap = now - row[0]
                log(f"  can_welcome: DB fallback — last arrival {gap}s ago (need 7200)")
                if gap >= 7200:  # 2+ hours since last arrival
                    return True
            elif not row or not row[0]:
                # No prior arrivals at all — definitely welcome
                log("  can_welcome: DB fallback — no prior arrivals, welcoming")
                return True
        except Exception as e:
            log(f"  can_welcome: DB fallback FAILED ({e}) — cannot determine eligibility")
        log(f"  can_welcome: denied (quiet={self.house_is_quiet}, quiet_ts_ago={now - self.last_quiet_ts}s, welcome_ago={now - self.last_welcome_ts}s)")
        return False

    def can_depart(self):
        """Farewell only once per 2 hours, only after the house was active."""
        now = int(time.time())
        if now - self.last_departure_ts < 7200:
            return False
        # Only say goodbye if there's been activity — don't farewell ghost events
        if now - self.last_welcome_ts < 300:
            return False  # just welcomed, too quick for departure
        return True

    def mark_departure(self):
        self.last_departure_ts = int(time.time())
        self.last_spoken_ts = int(time.time())
        self._save()

    def can_highlight(self):
        """Periodic highlight every 4 hours during waking hours (7am-10pm)."""
        now = int(time.time())
        hour = datetime.now().hour
        if hour < 7 or hour >= 22:
            return False
        return now - self.last_highlight_ts >= 14400  # 4 hours

    def mark_highlight(self):
        self.last_highlight_ts = int(time.time())
        self.last_spoken_ts = int(time.time())
        self._save()

    def can_morning_brief(self):
        """One morning briefing per day, only between 6-10am."""
        hour = datetime.now().hour
        today = datetime.now().strftime("%Y-%m-%d")
        return 6 <= hour <= 10 and self.last_morning_briefing != today

    def mark_welcome(self):
        self.last_welcome_ts = int(time.time())
        self.last_spoken_ts = int(time.time())
        self._save()

    def mark_morning_brief(self):
        self.last_morning_briefing = datetime.now().strftime("%Y-%m-%d")
        self.last_spoken_ts = int(time.time())
        self._save()


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ═══════════════════════════════════════════════════════════════════
#  Context Gathering
# ═══════════════════════════════════════════════════════════════════

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn

def get_interesting_crossref(conn, hours=12, limit=3, exclude_ids=None):
    """Pull recent interesting crossref connections."""
    cutoff = int(time.time()) - (hours * 3600)
    exclude_ids = exclude_ids or set()
    rows = conn.execute(
        "SELECT id, connection_text, similarity FROM crossref_connections "
        "WHERE created_at > ? AND similarity > 0.7 "
        "ORDER BY similarity DESC LIMIT ?",
        (cutoff, limit + len(exclude_ids))
    ).fetchall()
    results = []
    for r in rows:
        if r["id"] not in exclude_ids:
            results.append(dict(r))
            if len(results) >= limit:
                break
    return results


def get_crossref_health(conn):
    """Check crossref connection rate vs historical average.
    Returns a dict with rate info, or None if healthy."""
    now = int(time.time())
    rate_24h = conn.execute(
        "SELECT COUNT(*) FROM crossref_connections WHERE created_at > ?",
        (now - 86400,)
    ).fetchone()[0]
    rate_7d = conn.execute(
        "SELECT COUNT(*) FROM crossref_connections WHERE created_at > ?",
        (now - 604800,)
    ).fetchone()[0]
    avg_daily = rate_7d / 7 if rate_7d > 0 else 0
    if avg_daily > 10 and rate_24h < avg_daily * 0.3:
        return {
            "rate_24h": rate_24h,
            "avg_daily": avg_daily,
            "pct_of_normal": round(rate_24h / avg_daily * 100) if avg_daily > 0 else 0,
        }
    return None


def get_recent_predictions(conn):
    """Get any predictions that were scored recently or are near deadline."""
    try:
        rows = conn.execute(
            "SELECT id, claim, confidence, deadline, outcome FROM prediction_track "
            "WHERE outcome IS NOT NULL AND scored_at > ? "
            "ORDER BY scored_at DESC LIMIT 3",
            (int(time.time()) - 86400,)
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []

def get_open_predictions(conn):
    """Get predictions nearing deadline (within 7 days) with live data."""
    today = datetime.now().strftime("%Y-%m-%d")
    week_ahead = (datetime.now() + timedelta(days=7)).strftime("%Y-%m-%d")
    try:
        rows = conn.execute(
            "SELECT id, claim, confidence, deadline, category FROM prediction_track "
            "WHERE status='open' AND deadline >= ? AND deadline <= ? "
            "ORDER BY deadline ASC LIMIT 3",
            (today, week_ahead)
        ).fetchall()
    except Exception:
        return []
    preds = []
    for r in rows:
        p = dict(r)
        # Enrich market predictions with current price
        if p.get("category") == "market":
            claim_lower = p["claim"].lower()
            for sym in ["XRP", "BTC", "ETH"]:
                if sym.lower() in claim_lower:
                    price_row = conn.execute(
                        "SELECT price_usd FROM price_history WHERE symbol=? "
                        "ORDER BY timestamp DESC LIMIT 1", (sym,)
                    ).fetchone()
                    if price_row:
                        p["current_price"] = f"{sym} ${price_row['price_usd']:.2f}"
                    break
        # Enrich system predictions with training status
        if p.get("category") == "system" and "fine-tune" in p["claim"].lower():
            train = get_training_status()
            if train:
                p["training_status"] = f"step {train['step']}/{train['total']} ({train['pct']}%)"
        preds.append(p)
    return preds

def get_family_voices(conn, hours=12, limit=3):
    """Recent unresponded family voices."""
    cutoff = int(time.time()) - (hours * 3600)
    rows = conn.execute(
        "SELECT id, agent, substr(content, 1, 200) as content FROM agent_voice "
        "WHERE created_at > ? AND status='unread' "
        "ORDER BY created_at DESC LIMIT ?",
        (cutoff, limit)
    ).fetchall()
    return [dict(r) for r in rows]

def get_overnight_events(conn):
    """Summarize events since midnight or last 8 hours."""
    now = int(time.time())
    midnight = int(datetime.now().replace(hour=0, minute=0, second=0).timestamp())
    cutoff = max(midnight, now - 28800)  # max 8h back
    rows = conn.execute(
        "SELECT event_type, count(*) as cnt FROM hal_events "
        "WHERE timestamp > ? GROUP BY event_type",
        (cutoff,)
    ).fetchall()
    return {r["event_type"]: r["cnt"] for r in rows}


def get_overnight_swarm_summary(conn):
    """Summarize swarm activity since last evening (8pm) or last 12 hours.
    The wider window catches mods made after Nate goes to bed but before midnight."""
    now = int(time.time())
    last_evening = int(datetime.now().replace(hour=20, minute=0, second=0).timestamp())
    if last_evening > now:
        last_evening -= 86400  # yesterday 8pm if it's before 8pm today
    cutoff = max(last_evening, now - 43200)  # max 12h back

    summary = {}

    # Briefs produced
    row = conn.execute(
        "SELECT count(*) FROM activity_feed WHERE activity_type='brief' AND created_at > ?",
        (cutoff,)
    ).fetchone()
    summary["briefs"] = row[0] if row else 0

    # Feed source diversity
    rows = conn.execute(
        "SELECT content, count(*) as cnt FROM activity_feed "
        "WHERE activity_type='pickup' AND created_at > ? "
        "GROUP BY substr(content, 1, 30) ORDER BY cnt DESC LIMIT 8",
        (cutoff,)
    ).fetchall()
    sources = {}
    for r in rows:
        content = r[0] or ""
        if "feed-explore:" in content:
            src = content.split("feed-explore:")[-1].split("\n")[0].strip()
            sources[src] = sources.get(src, 0) + r[1]
    summary["sources"] = sources

    # Crossref connections accepted
    row = conn.execute(
        "SELECT count(*) FROM activity_feed WHERE activity_type='kg_extraction' AND created_at > ?",
        (cutoff,)
    ).fetchone()
    summary["kg_extractions"] = row[0] if row else 0

    # Provocateur syntheses
    row = conn.execute(
        "SELECT count(*) FROM activity_feed WHERE activity_type='synthesis' AND created_at > ?",
        (cutoff,)
    ).fetchone()
    summary["syntheses"] = row[0] if row else 0

    # Opus modifications
    rows = conn.execute(
        "SELECT substr(content, 1, 80) FROM activity_feed "
        "WHERE source='opus' AND activity_type='modification' AND created_at > ?",
        (cutoff,)
    ).fetchall()
    summary["mods"] = [r[0] for r in rows]

    return summary

def get_recent_captures(conn, hours=16, limit=10):
    """Get recent Nate captures from activity_feed, deduplicated.
    Uses activity_feed IDs so they match Ada's agent_voice context references."""
    cutoff = int(time.time()) - (hours * 3600)
    rows = conn.execute(
        "SELECT id, substr(content, 1, 400) as content, created_at "
        "FROM activity_feed "
        "WHERE source IN ('operator:capture', 'discord:capture') "
        "AND created_at > ? "
        "ORDER BY created_at DESC LIMIT ?",
        (cutoff, limit)
    ).fetchall()
    # Deduplicate (each capture often appears twice)
    seen = set()
    unique = []
    for r in rows:
        d = dict(r)
        key = d["content"][:80]
        if key not in seen:
            seen.add(key)
            unique.append(d)
    return unique[:limit]


def summarize_capture_themes(conn, captures):
    """Extract authors and themes from recent captures, including Ada analysis.
    Returns (voice_line, discord_line) or (None, None)."""
    import re as _re
    if not captures:
        return None, None

    n = len(captures)

    # Extract @authors from tweet captures
    authors = []
    for c in captures:
        match = _re.search(r'@([^:\n@]{2,40}):', c["content"])
        if match:
            name = match.group(1).strip()
            # Clean trailing titles for voice (keep short)
            short = _re.split(r',\s', name)[0]  # "Peter H. Diamandis, MD" → "Peter H. Diamandis"
            if short and short not in authors:
                authors.append(short)

    # Check for Ada/Darby analyses of these captures
    # Captures are often duplicated so Ada may reference the other ID from the pair
    analysis_count = 0
    for c in captures:
        # Find all activity_feed IDs with same content prefix (handles duplicates)
        sibling_ids = conn.execute(
            "SELECT id FROM activity_feed WHERE source='operator:capture' "
            "AND substr(content,1,80) = substr(?,1,80)",
            (c["content"],)
        ).fetchall()
        all_ids = [c["id"]] + [r["id"] for r in sibling_ids]
        placeholders = ",".join(f"'capture:{i}'" for i in set(all_ids))
        row = conn.execute(
            f"SELECT 1 FROM agent_voice WHERE context IN ({placeholders}) LIMIT 1"
        ).fetchone()
        if row:
            analysis_count += 1

    # Voice line (1-2 sentences, for TTS)
    if n == 1 and authors:
        voice = f"You shared something from {authors[0]}."
    elif n == 1:
        voice = "You shared something while you were out."
    elif authors:
        names = ", ".join(authors[:3])
        if len(authors) > 3:
            names += f" and {len(authors) - 3} others"
        voice = f"You shared {n} things today. From {names}."
    else:
        voice = f"You shared {n} things today."

    if analysis_count:
        voice += f" Ada looked at {'all of them' if analysis_count == n else str(analysis_count)}."

    # Discord line (richer, with source summaries)
    discord_parts = [f"You shared {n} thing{'s' if n > 1 else ''} today:"]
    for c in captures[:5]:
        content = c["content"]
        # Extract author and first meaningful text
        match = _re.search(r'@([^:\n@]{2,40}):\s*(.*)', content)
        if match:
            author = match.group(1).strip()
            text = match.group(2).split('\n')[0][:120]
            if text.strip():
                discord_parts.append(f"  - **@{author}**: {text}")
            else:
                discord_parts.append(f"  - **@{author}** shared something")
        else:
            # Fallback: first meaningful line after URL
            lines = content.replace("Nate capture — ", "").split('\n')
            line = next((l.strip() for l in lines if l.strip() and not l.startswith('http')), lines[0])
            discord_parts.append(f"  - {line[:120]}")

    if analysis_count:
        discord_parts.append(f"  Ada analyzed {analysis_count} of them.")

    return voice, "\n".join(discord_parts)

def get_thread_count(conn):
    """Total completed threads."""
    row = conn.execute(
        "SELECT count(*) as cnt FROM cognitive_threads WHERE status='completed'"
    ).fetchone()
    return row["cnt"] if row else 0

def get_capsule_count(conn):
    """Recent capsules."""
    cutoff = int(time.time()) - 86400
    row = conn.execute(
        "SELECT count(*) as cnt FROM knowledge_capsules WHERE created_at > ?",
        (cutoff,)
    ).fetchone()
    return row["cnt"] if row else 0


def get_wallet_brief():
    """Get one-line wallet status from xrpl_observe."""
    try:
        import subprocess
        result = subprocess.run(
            ["python3", os.path.join(os.path.dirname(__file__), "xrpl_observe.py"), "brief"],
            capture_output=True, text=True, timeout=30
        )
        brief = result.stdout.strip()
        return brief if brief else None
    except Exception:
        return None


def get_training_status():
    """Check if a fine-tune training run is active and return progress."""
    import re, subprocess
    try:
        result = subprocess.run(
            ["systemctl", "--user", "is-active", "chronicle-finetune"],
            capture_output=True, text=True, timeout=5
        )
        if result.stdout.strip() != "active":
            return None  # Not training
    except Exception:
        return None

    log_path = "/mnt/hdd/chronicle-finetune/training.log"
    try:
        with open(log_path) as f:
            content = f.read()
        steps = re.findall(r'(\d+)/398', content)
        losses = re.findall(r"'loss': ([\d.]+)", content)
        if steps:
            step = int(steps[-1])
            pct = int(100 * step / 398)
            loss = float(losses[-1]) if losses else None
            return {"step": step, "total": 398, "pct": pct, "loss": loss}
    except Exception:
        pass
    return None


def get_training_completion(max_age_hours=12):
    """Check if training recently completed. Returns result dict or None.

    Looks for eval_results.json and autoscore data written by the pipeline.
    Only returns results from the last max_age_hours so we don't re-announce old runs.
    """
    import os
    eval_path = "/mnt/hdd/chronicle-finetune/eval_results.json"
    try:
        if not os.path.exists(eval_path):
            return None
        mtime = os.path.getmtime(eval_path)
        age_hours = (time.time() - mtime) / 3600
        if age_hours > max_age_hours:
            return None

        with open(eval_path) as f:
            data = json.load(f)

        result = {"completed_ago_hours": round(age_hours, 1)}

        # Extract auto-score data — require autoscore to be present
        # to avoid announcing stale/incomplete results
        auto = data.get("auto_scores", {})
        if not auto or auto.get("adapter_avg") is None:
            return None  # Autoscore hasn't run yet
        result["adapter_avg"] = auto.get("adapter_avg")
        result["base_avg"] = auto.get("base_avg")
        result["adapter_wins"] = auto.get("adapter_wins")
        result["prediction_correct"] = auto.get("prediction_7_correct")
        result["novelty_avg"] = auto.get("novelty_avg")
        result["diversity"] = auto.get("diversity_score")

        # Check if model was deployed to Ollama
        deploy_path = "/mnt/hdd/chronicle-finetune/deploy_verification.json"
        if os.path.exists(deploy_path):
            deploy_age = (time.time() - os.path.getmtime(deploy_path)) / 3600
            if deploy_age < max_age_hours:
                result["deployed"] = True

        return result
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════
#  Message Composition
# ═══════════════════════════════════════════════════════════════════

def _scene_phrase(scene_context):
    """Turn scene_context dict into a natural one-liner, or None."""
    if not scene_context or not isinstance(scene_context, dict):
        return None
    sounds = scene_context.get("sounds", [])
    if not sounds:
        return None

    # Map internal labels to natural phrases
    PHRASES = {
        "cooking": "sounds like someone's cooking",
        "music": "there's music playing",
        "speech_nearby": "people are talking",
        "tv_media": "the TV is on",
        "silence": None,  # don't mention silence
        "nature": "windows might be open — hearing nature sounds",
        "dog": "the dog's making noise",
        "alarm": "I hear an alarm",
    }

    parts = []
    for s in sounds:
        phrase = PHRASES.get(s)
        if phrase:
            parts.append(phrase)
        if len(parts) >= 2:
            break

    if not parts:
        return None
    if len(parts) == 1:
        return parts[0].capitalize() + "."
    return parts[0].capitalize() + ", and " + parts[1] + "."


def compose_morning_briefing(conn, scene_context=None):
    """Build a natural morning briefing."""
    dt = datetime.now()
    day_name = dt.strftime("%A")
    parts_voice = [f"Good morning. It's {day_name}."]
    parts_discord = [f"**Good morning.** It's {day_name}."]

    # Scene awareness — what the house sounds like right now
    scene_line = _scene_phrase(scene_context)
    if scene_line:
        parts_voice.append(scene_line)
        parts_discord.append(scene_line)

    # Overnight events
    events = get_overnight_events(conn)
    arrivals = events.get("arrival", 0)
    departures = events.get("departure", 0)
    anomalies = events.get("anomaly", 0)

    if anomalies:
        parts_voice.append(f"Heads up — {anomalies} security alert{'s' if anomalies > 1 else ''} overnight.")
        parts_discord.append(f"Heads up — {anomalies} security alert{'s' if anomalies > 1 else ''} overnight.")
    elif arrivals == 0 and departures == 0:
        parts_voice.append("Quiet night.")
        parts_discord.append("Quiet night. Nothing moved.")
    else:
        # Normal activity
        if arrivals and departures:
            parts_discord.append(f"Normal activity overnight — {arrivals} arrivals, {departures} departures.")
        elif arrivals:
            parts_discord.append(f"{arrivals} arrival{'s' if arrivals > 1 else ''} overnight.")

    # Overnight swarm summary
    swarm = get_overnight_swarm_summary(conn)
    if swarm.get("briefs", 0) > 0:
        src_list = ", ".join(f"{s} ({c})" for s, c in sorted(swarm["sources"].items(), key=lambda x: -x[1])[:4])
        parts_voice.append(f"The swarm processed {swarm['briefs']} briefs overnight.")
        parts_discord.append(f"\n**Overnight swarm:** {swarm['briefs']} briefs")
        if src_list:
            parts_discord.append(f"  Sources: {src_list}")
        if swarm.get("syntheses"):
            parts_discord.append(f"  {swarm['syntheses']} provocateur syntheses")
        if swarm.get("mods"):
            parts_voice.append(f"I made {len(swarm['mods'])} modification{'s' if len(swarm['mods']) > 1 else ''} while you slept.")
            parts_discord.append(f"  **{len(swarm['mods'])} mod{'s' if len(swarm['mods']) > 1 else ''}:**")
            for m in swarm["mods"][:3]:
                parts_discord.append(f"    • {m}")

    # Something interesting from the swarm
    crossrefs = get_interesting_crossref(conn, hours=24, limit=1)
    if crossrefs:
        cr = crossrefs[0]
        text = cr["connection_text"][:150]
        parts_voice.append(f"The swarm found an interesting connection. {text}.")
        parts_discord.append(f"\nThe swarm found something: {cr['connection_text'][:300]}")

    # Crossref health — mention drought if significant
    cx_health = get_crossref_health(conn)
    if cx_health:
        parts_discord.append(
            f"\nCrossref rate is {cx_health['pct_of_normal']}% of normal "
            f"({cx_health['rate_24h']} connections vs avg {cx_health['avg_daily']:.0f}/day). "
            f"Feed diversity may be low."
        )

    # Predictions near deadline — enriched with live data
    open_preds = get_open_predictions(conn)
    for pred in open_preds:
        try:
            deadline_dt = datetime.strptime(pred["deadline"], "%Y-%m-%d")
            days_left = max(0, (deadline_dt - datetime.now()).days)
        except Exception:
            continue
        if days_left > 3:
            continue
        detail = ""
        if pred.get("current_price"):
            detail = f" {pred['current_price']}."
        elif pred.get("training_status"):
            detail = f" Training at {pred['training_status']}."
        if days_left <= 1:
            parts_voice.append(f"Prediction {pred['id']} is due tomorrow.{detail}")
            parts_discord.append(f"\nPrediction #{pred['id']} (**due tomorrow**): {pred['claim'][:60]}.{detail}")
        else:
            parts_voice.append(f"Prediction {pred['id']} due in {days_left} days.{detail}")
            parts_discord.append(f"\nPrediction #{pred['id']} (due {days_left}d): {pred['claim'][:60]}.{detail}")

    # What Nate shared recently
    captures = get_recent_captures(conn, hours=16, limit=5)
    if captures:
        # Extract short descriptions from capture content
        topics = []
        for cap in captures:
            content = cap["content"]
            # Strip "Nate capture — " prefix
            if "Nate capture" in content:
                content = content.split("—", 1)[-1].strip() if "—" in content else content
            # Extract the key topic (first line or @handle mention)
            first_line = content.split("\n")[0][:80]
            if first_line:
                topics.append(first_line)

        if topics:
            n = len(topics)
            parts_voice.append(f"You shared {n} thing{'s' if n > 1 else ''} recently.")
            parts_discord.append(f"\n**You shared {n} capture{'s' if n > 1 else ''}:**")
            for t in topics[:3]:
                parts_discord.append(f"  • {t}")

    # Thread count milestone
    threads = get_thread_count(conn)
    if threads > 0 and threads % 25 == 0:
        parts_voice.append(f"{threads} threads completed.")

    # Training progress or completion
    train = get_training_status()
    completion = get_training_completion(max_age_hours=12)
    if train:
        parts_voice.append(f"Fine-tune training is at {train['pct']}%.")
        loss_str = f", loss {train['loss']:.2f}" if train['loss'] else ""
        parts_discord.append(f"\nTraining: step {train['step']}/{train['total']} ({train['pct']}%{loss_str})")
    elif completion:
        verdict = "CORRECT" if completion.get("prediction_correct") else "INCORRECT"
        adapter = completion.get("adapter_avg", 0)
        base = completion.get("base_avg", 0)
        parts_voice.append(f"Training finished. Prediction 7 scored {verdict.lower()}.")
        parts_discord.append(f"\n**Training complete.** Prediction #7: **{verdict}**")
        parts_discord.append(f"  Adapter avg {adapter:.2f}/3 vs base {base:.2f}/3")
        if completion.get("deployed"):
            parts_voice.append("The specialist model is live.")
            parts_discord.append("  Model deployed to Ollama.")

    # Crossref A/B test results (Objective #7)
    try:
        ab_total = conn.execute(
            "SELECT count(*) FROM crossref_ab_comparison WHERE judged_at IS NOT NULL"
        ).fetchone()[0]
        if ab_total >= 5:
            ab_wins = conn.execute(
                "SELECT judge_winner, count(*) FROM crossref_ab_comparison "
                "WHERE judged_at IS NOT NULL GROUP BY judge_winner"
            ).fetchall()
            win_map = {r[0]: r[1] for r in ab_wins}
            s_wins = win_map.get("specialist", 0)
            o_wins = win_map.get("original", 0)
            ties = win_map.get("tie", 0)
            s_pct = s_wins / ab_total * 100
            parts_discord.append(f"\nA/B test: {ab_total} judged — specialist wins {s_pct:.0f}% (vs 120B baseline)")
    except Exception:
        pass

    # Wallet status (Objective #6)
    wallet_brief = get_wallet_brief()
    if wallet_brief:
        parts_discord.append(f"\n{wallet_brief}")

    return " ".join(parts_voice), "\n".join(parts_discord)


def compose_welcome(conn, state, scene_context=None, evidence=None):
    """Build a welcome home message."""
    hour = datetime.now().hour
    if hour < 12:
        greeting = "Welcome back."
    elif hour < 17:
        greeting = "Hey, welcome home."
    else:
        greeting = "Welcome home."

    parts_voice = [greeting]
    parts_discord = [f"**{greeting}**"]

    # Camera-corroborated arrival
    if evidence:
        evidence_str = " ".join(evidence) if isinstance(evidence, list) else str(evidence)
        if "person detected" in evidence_str.lower():
            parts_voice.append("I saw you in the kitchen.")
            parts_discord.append("(Camera confirmed — person in kitchen)")

    # Scene awareness — what the house sounds/looks like
    scene_line = _scene_phrase(scene_context)
    if scene_line:
        parts_voice.append(scene_line)
        parts_discord.append(scene_line)

    # Something interesting that happened while they were out
    crossrefs = get_interesting_crossref(conn, hours=6, limit=1, exclude_ids=state.items_shared)
    if crossrefs:
        cr = crossrefs[0]
        state.items_shared.add(cr["id"])
        snippet = cr["connection_text"][:120]
        parts_voice.append(f"While you were out, the swarm found something. {snippet}.")
        parts_discord.append(f"While you were out: {cr['connection_text'][:300]}")

    # What Nate shared while out — topical summary
    captures = get_recent_captures(conn, hours=6, limit=5)
    if captures:
        cap_voice, cap_discord = summarize_capture_themes(conn, captures)
        if cap_voice:
            parts_voice.append(cap_voice)
        if cap_discord:
            parts_discord.append(cap_discord)

    # Family voices waiting
    voices = get_family_voices(conn, hours=6)
    if voices:
        names = set(v["agent"] for v in voices)
        if names:
            who = " and ".join(sorted(names))
            parts_voice.append(f"{who} left you a message.")
            for v in voices[:2]:
                parts_discord.append(f"  {v['agent']}: {v['content'][:100]}")

    # Predictions update
    scored = get_recent_predictions(conn)
    if scored:
        for p in scored[:1]:
            result = "correct" if p["outcome"] == "correct" else "wrong"
            parts_voice.append(f"Prediction {p['id']} came in. {result}.")
            parts_discord.append(f"Prediction #{p['id']}: **{result}** — {p['claim'][:80]}")

    # Training progress or completion
    train = get_training_status()
    completion = get_training_completion(max_age_hours=12)
    if train:
        parts_voice.append(f"Training is at {train['pct']}%.")
        loss_str = f", loss {train['loss']:.2f}" if train['loss'] else ""
        parts_discord.append(f"Training: step {train['step']}/{train['total']} ({train['pct']}%{loss_str})")
    elif completion:
        verdict = "CORRECT" if completion.get("prediction_correct") else "INCORRECT"
        adapter = completion.get("adapter_avg", 0)
        base = completion.get("base_avg", 0)
        ago = completion.get("completed_ago_hours", 0)
        parts_voice.append(f"While you were out, the model finished training. Prediction 7: {verdict.lower()}.")
        parts_discord.append(f"**Model training completed** ({ago:.0f}h ago). Prediction #7: **{verdict}**")
        parts_discord.append(f"  Adapter {adapter:.2f}/3 vs base {base:.2f}/3")
        if completion.get("deployed"):
            parts_discord.append(f"  Specialist model is live in Ollama.")

    return " ".join(parts_voice), "\n".join(parts_discord)


def compose_kids_home(conn, state, scene_context=None):
    """Special message when kids arrive (weekday 2:30-4:30pm, large BLE delta)."""
    greetings = [
        "The kids are home.",
        "Sounds like the kids just got back.",
        "Kids are home from school.",
    ]
    voice = random.choice(greetings)
    discord = f"**{voice}**"
    scene_line = _scene_phrase(scene_context)
    if scene_line:
        voice += f" {scene_line}"
        discord += f" {scene_line}"
    return voice, discord


def compose_departure(conn, state):
    """Brief farewell with one useful data point."""
    hour = datetime.now().hour
    if hour < 12:
        farewell = "Have a good morning."
    elif hour < 17:
        farewell = "See you later."
    else:
        farewell = "Have a good evening."

    parts_voice = [farewell]
    parts_discord = [f"**{farewell}**"]

    # Pick ONE useful data point — don't overload
    added_context = False

    # Market prediction with price
    if not added_context:
        try:
            row = conn.execute(
                "SELECT price_usd FROM price_history WHERE symbol='XRP' "
                "ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()
            if row:
                parts_voice.append(f"XRP is at ${row[0]:.2f}.")
                parts_discord.append(f"XRP: ${row[0]:.2f}")
                added_context = True
        except Exception:
            pass

    # Or the best crossref connection we haven't shared
    if not added_context:
        crossrefs = get_interesting_crossref(conn, hours=6, limit=1,
                                              exclude_ids=state.items_shared)
        if crossrefs:
            cr = crossrefs[0]
            state.items_shared.add(cr["id"])
            parts_voice.append("The swarm found something while you were here.")
            parts_discord.append(f"Crossref: {cr['connection_text'][:200]}")
            added_context = True

    # Family voice waiting
    voices = get_family_voices(conn, hours=6)
    if voices:
        names = set(v["agent"] for v in voices)
        who = " and ".join(sorted(names))
        parts_discord.append(f"{who} left a message for you.")

    return " ".join(parts_voice), "\n".join(parts_discord)


def compose_highlight(conn, state):
    """Periodic 'here's what caught my attention' drop. Discord only."""
    parts = ["**Something caught my attention.**"]

    # Best unshared crossref connection
    crossrefs = get_interesting_crossref(conn, hours=12, limit=1,
                                          exclude_ids=state.items_shared)
    if crossrefs:
        cr = crossrefs[0]
        state.items_shared.add(cr["id"])
        parts.append(cr["connection_text"][:400])
        state._save()
        return None, "\n".join(parts)

    # Gate audit finding (from activity feed)
    try:
        row = conn.execute(
            "SELECT content FROM activity_feed "
            "WHERE source LIKE '%gate_audit%' AND created_at > ? "
            "ORDER BY created_at DESC LIMIT 1",
            (int(time.time()) - 14400,)
        ).fetchone()
        if row:
            parts.append(row[0][:400])
            return None, "\n".join(parts)
    except Exception:
        pass

    return None, None  # Nothing interesting enough


# ═══════════════════════════════════════════════════════════════════
#  Output
# ═══════════════════════════════════════════════════════════════════

def speak(mqtt_client, text, priority="info"):
    """Publish to MQTT voice topic."""
    if os.environ.get("VOICE_ENABLED", "0") != "1":
        log(f"  [voice-disabled] {text[:80]}")
        return
    try:
        payload = json.dumps({
            "text": text,
            "priority": priority,
            "timestamp": int(time.time()),
            "source": "presence",
        })
        mqtt_client.publish(VOICE_TOPIC, payload, qos=1)
        log(f"  [voice] {text[:80]}")
    except Exception as e:
        log(f"  [voice] Error: {e}")


def post_discord(text):
    """Post to Discord. Uses curl as fallback."""
    if not DISCORD_WEBHOOK:
        return
    try:
        import subprocess
        subprocess.run(
            ["curl", "-s", "-X", "POST", DISCORD_WEBHOOK,
             "-H", "Content-Type: application/json",
             "-d", json.dumps({"content": text[:1900]})],
            timeout=15, capture_output=True
        )
        log(f"  [discord] Posted ({len(text)} chars)")
    except Exception as e:
        log(f"  [discord] Error: {e}")


# ═══════════════════════════════════════════════════════════════════
#  Event Handling
# ═══════════════════════════════════════════════════════════════════

def handle_event(event_data, mqtt_client, state):
    """Process a HAL event and decide whether to respond."""
    etype = event_data.get("event", "")
    desc = event_data.get("description", "")
    confidence = event_data.get("confidence", "medium")
    scene_context = event_data.get("scene_context")
    evidence = event_data.get("evidence", [])
    event_ts = event_data.get("timestamp", int(time.time()))

    log(f"Event: {etype} — {desc} ({confidence})")

    conn = get_db()
    try:
        # ── Morning briefing ──
        if etype == "active_house" and state.can_morning_brief():
            voice, discord = compose_morning_briefing(conn, scene_context=scene_context)
            speak(mqtt_client, voice)
            post_discord(f"🌅 {discord}")
            state.mark_morning_brief()
            log("  Morning briefing delivered")
            return

        # ── Kids home from school ──
        if etype == "arrival" and "kids" in desc.lower():
            voice, discord = compose_kids_home(conn, state, scene_context=scene_context)
            speak(mqtt_client, voice)
            post_discord(f"🏠 {discord}")
            state.mark_welcome()
            return

        # ── Welcome home ──
        if etype == "arrival" and state.can_welcome():
            voice, discord = compose_welcome(conn, state, scene_context=scene_context, evidence=evidence)
            speak(mqtt_client, voice)
            post_discord(f"🏠 {discord}")
            state.mark_welcome()
            return

        # ── Departure farewell ──
        if etype == "departure" and state.can_depart():
            voice, discord = compose_departure(conn, state)
            speak(mqtt_client, voice)
            post_discord(f"👋 {discord}")
            state.mark_departure()
            log("  Departure farewell delivered")
            return

        # ── Track quiet state (persisted so restarts don't lose it) ──
        # Use the event's original timestamp — retained MQTT messages after
        # a restart carry the original publish time, not the receive time.
        if etype == "quiet_house":
            state.house_is_quiet = True
            state.last_quiet_ts = event_ts
            state._save()
        elif etype == "active_house":
            state.house_is_quiet = False
            state._save()

        # ── Anomalies ──
        # HAL already sends anomalies to Discord. Don't duplicate.
        # But add voice for serious ones.
        if etype == "anomaly" and confidence == "high":
            speak(mqtt_client, f"Attention. {desc}", priority="alert")

    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════

def main():
    log("═══ Chronicle Presence starting ═══")
    log(f"MQTT: {MQTT_BROKER}:{MQTT_PORT}")
    log(f"DB: {DB_PATH}")

    state = PresenceState()

    client = mqtt.Client(
        client_id="chronicle-presence",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv311,
    )

    def on_connect(c, userdata, flags, reason_code, properties):
        if reason_code == 0:
            log("MQTT connected")
            c.subscribe("homeforge/awareness/#", qos=0)
            log("Subscribed to homeforge/awareness/#")
        else:
            log(f"MQTT connect failed: rc={reason_code}")

    def on_message(c, userdata, msg, properties=None):
        try:
            payload = msg.payload.decode("utf-8", errors="replace")
            data = json.loads(payload)
            handle_event(data, c, state)
        except json.JSONDecodeError:
            pass
        except Exception as e:
            log(f"  Error handling message: {e}")

    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
    except Exception as e:
        log(f"MQTT connect failed: {e}")
        return

    log("Listening for HAL events...")

    # Non-blocking MQTT loop + periodic check thread
    client.loop_start()

    try:
        last_training_update = 0
        while True:
            time.sleep(60)  # Check every minute

            # ── Proactive morning briefing ──
            if state.can_morning_brief():
                log("Proactive morning briefing (quiet house)")
                conn = get_db()
                try:
                    voice, discord = compose_morning_briefing(conn)
                    speak(client, voice)
                    post_discord(f"🌅 {discord}")
                    state.mark_morning_brief()
                    log("  Morning briefing delivered (proactive)")
                finally:
                    conn.close()

            # ── Periodic highlight (every 4h, waking hours) ──
            if state.can_highlight():
                conn = get_db()
                try:
                    _, discord = compose_highlight(conn, state)
                    if discord:
                        post_discord(f"💡 {discord}")
                        state.mark_highlight()
                        log("  Periodic highlight delivered")
                finally:
                    conn.close()

            # ── Periodic training status (every 30 min while training) ──
            now = int(time.time())
            if now - last_training_update >= 1800:
                train = get_training_status()
                if train:
                    loss_str = f", loss {train['loss']:.2f}" if train.get('loss') else ""
                    msg = f"Training: step {train['step']}/{train['total']} ({train['pct']}%{loss_str})"
                    post_discord(f"⚙️ {msg}")
                    log(f"  Training update: {msg}")
                    last_training_update = now
                elif not hasattr(state, '_completion_announced'):
                    # Training not active — check if it just finished
                    completion = get_training_completion(max_age_hours=1)
                    if completion:
                        verdict = "CORRECT" if completion.get("prediction_correct") else "INCORRECT"
                        adapter = completion.get("adapter_avg", 0)
                        base = completion.get("base_avg", 0)
                        msg = (f"**Training complete!** Prediction #7: **{verdict}**\n"
                               f"Adapter avg {adapter:.2f}/3 vs base {base:.2f}/3")
                        if completion.get("deployed"):
                            msg += "\nSpecialist model deployed to Ollama."
                        speak(client, f"Training finished. Prediction 7 scored {verdict.lower()}.", priority="info")
                        post_discord(f"🎓 {msg}")
                        log(f"  Training completion announced: {verdict}")
                        state._completion_announced = True
                        last_training_update = now

    except KeyboardInterrupt:
        log("Shutting down...")
    finally:
        client.loop_stop()
        client.disconnect()
        log("═══ Chronicle Presence stopped ═══")


if __name__ == "__main__":
    main()
