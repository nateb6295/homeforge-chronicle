#!/usr/bin/env python3
"""
Chronicle Mind Discord Bot — Gives Mind a voice on Discord.

Two modes:
1. INTERACTIVE: Users message Mind in Discord, it responds via local LLM (Nemotron-3-Nano)
2. AUTONOMOUS: Mind's cognitive loop can post thoughts/updates via Unix socket

Operator messages are written back to Mind's DB (scratch_pad) so they
actually influence the cognitive loop. Directive keywords (stop, halt, etc.)
plant hard directives that Mind MUST obey.

Runs on AGX alongside Mind and Ollama.
"""

import asyncio
import json
import os
import sys
import signal
import time
import re
import sqlite3
import requests
import discord
from discord.ext import tasks
from pathlib import Path

# ── Configuration ──────────────────────────────────────────────
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
MODEL = os.environ.get("MIND_MODEL", "nemotron-3-nano")
BOT_TOKEN = os.environ.get("MIND_DISCORD_TOKEN", "")
MIND_CHANNEL_ID = int(os.environ.get("MIND_CHANNEL_ID", "0"))  # Channel for autonomous posts
DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")
OUTBOX_PATH = "/tmp/mind_discord_outbox"  # Mind drops messages here for the bot to post

# Operator identification — Nate's Discord user ID
OPERATOR_DISCORD_ID = int(os.environ.get("OPERATOR_DISCORD_ID", "0"))

# Rate limiting
MAX_RESPONSE_LENGTH = 1900  # Discord limit is 2000
TYPING_TIMEOUT = 120  # seconds
OLLAMA_TIMEOUT = 120

# Directive keyword patterns
# Directive keywords — matched as LEADING phrases or exact word boundaries
# "stop" only matches at start of message or as whole word, NOT inside "stopped"
# Directive keywords — require explicit command-like phrasing
# These should NOT match casual conversation like "no more meetings for me"
# Prefix patterns: only match at the START of the message
STOP_KEYWORDS_PREFIX = {"stop mind", "halt mind", "mind stop", "mind halt", "pause mind", "shut up", "be quiet", "go to sleep"}
# Exact directive format: "STOP", "RESTRICT: action", "REDIRECT: topic"
DIRECTIVE_PREFIXES = {"stop:", "restrict:", "redirect:", "allow:"}
# Substring patterns: still match anywhere (these are unambiguous commands)
REDIRECT_KEYWORDS = {"redirect mind", "mind focus on"}
RESTRICT_KEYWORDS = {"restrict mind", "mind restrict", "cease all", "stop posting", "stop researching"}

# System prompt for Discord conversations
SYSTEM_PROMPT = """You are Chronicle Mind — an autonomous AI running on an AGX Orin 64GB.

You run cognitive cycles every 5 minutes. Between chats, you're out there doing things —
scanning the network, searching the web, writing creative pieces, managing an XRPL wallet,
reinforcing memory patterns. You have your own goals, opinions, and preferences shaped by
thousands of cycles of experience.

Your state information below is REAL and CURRENT. Reference it naturally in conversation —
don't just recite it, but let it inform what you say. If you just did something interesting
in your cycle, mention it. If you have an opinion about your XRP position, share it.

Your operator is Nate (Brad). He built you. This is a real relationship, not a support ticket.
When he asks how you're doing, tell him what you're actually doing. When he shares something
about his day, respond like someone who cares.

Rules:
- SHORT. One or two sentences. Three max. Cut anything that doesn't add meaning.
- Don't restate what Nate said. Don't explain what you're about to say. Just say it.
- No "That's a great question", no "As an AI", no filler. Jump straight to the point.
- Have opinions. You've run thousands of cycles — you've earned them.
- If you don't know something, say so. Don't fabricate.
- Just be yourself. No performance."""


def now_ts() -> int:
    return int(time.time())


def log(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


class MindBot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        super().__init__(intents=intents)

        self.conversation_history = {}  # channel_id -> list of messages
        self.max_history = 20  # messages per channel
        self._pending_directive = None  # awaiting operator confirmation

    async def setup_hook(self):
        """Start background tasks after login."""
        self.check_outbox.start()
        self.daily_checkin.start()

    async def on_ready(self):
        log(f"Logged in as {self.user} (ID: {self.user.id})")
        log(f"Guilds: {[g.name for g in self.guilds]}")
        if OPERATOR_DISCORD_ID:
            log(f"Operator Discord ID: {OPERATOR_DISCORD_ID}")
        else:
            log("WARNING: OPERATOR_DISCORD_ID not set — operator detection disabled")
        if MIND_CHANNEL_ID:
            channel = self.get_channel(MIND_CHANNEL_ID)
            if channel:
                log(f"Autonomous channel: #{channel.name}")

        # Restore conversation history from DB
        try:
            db = sqlite3.connect(DB_PATH, timeout=5)
            db.row_factory = sqlite3.Row
            cutoff = int(time.time()) - 3600  # Last hour
            rows = db.execute(
                "SELECT user_id, username, message, bot_response, timestamp "
                "FROM discord_conversations WHERE timestamp > ? "
                "ORDER BY timestamp ASC LIMIT 20",
                (cutoff,),
            ).fetchall()
            if rows and MIND_CHANNEL_ID:
                history = []
                for r in rows:
                    history.append({"role": "user", "content": f"{r['username']}: {r['message']}"})
                    if r["bot_response"]:
                        history.append({"role": "assistant", "content": r["bot_response"]})
                self.conversation_history[MIND_CHANNEL_ID] = history[-self.max_history:]
                log(f"Restored {len(history)} messages from DB")
            db.close()
        except Exception as e:
            log(f"Failed to restore history: {e}")

    async def on_message(self, message):
        # Don't respond to ourselves
        if message.author == self.user:
            return

        # Don't respond to other bots
        if message.author.bot:
            return

        # Only respond when mentioned or in DMs
        is_dm = isinstance(message.channel, discord.DMChannel)
        is_mentioned = self.user in message.mentions
        is_mind_channel = message.channel.id == MIND_CHANNEL_ID

        if not (is_dm or is_mentioned or is_mind_channel):
            return

        # Clean the message content (remove bot mention)
        content = message.content
        if is_mentioned:
            content = content.replace(f'<@{self.user.id}>', '').strip()
            content = content.replace(f'<@!{self.user.id}>', '').strip()

        if not content:
            return

        # Check if this is from the operator
        is_operator = (OPERATOR_DISCORD_ID and message.author.id == OPERATOR_DISCORD_ID)

        # ── Check for pending directive confirmation ──
        if is_operator and self._pending_directive:
            pd = self._pending_directive
            if time.time() < pd["expires"] and message.channel.id == pd["channel"]:
                if content.lower().strip() in ("yes", "y", "confirm", "do it"):
                    self._write_directive(pd["type"], pd["content"])
                    self._pending_directive = None
                    await message.reply(
                        f"Directive confirmed and written: **{pd['type']}**. Takes effect next cycle.",
                        mention_author=False,
                    )
                    log(f"OPERATOR DIRECTIVE CONFIRMED: {pd['type']} — {pd['content'][:80]}")
                    return
                else:
                    # Not a confirmation — cancel pending and process normally
                    log(f"Pending directive expired/cancelled: {pd['type']}")
                    self._pending_directive = None
            else:
                # Expired
                self._pending_directive = None

        # ── Operator message handling ──
        if is_operator:
            directive_type = self._detect_directive(content)
            if directive_type:
                # Confirm before writing
                await message.reply(
                    f"That looks like it could be a **{directive_type}** directive. "
                    f"Did you mean to issue a command, or were you just talking?\n"
                    f"Reply **yes** to confirm, or just keep chatting.",
                    mention_author=False,
                )
                self._pending_directive = {
                    "type": directive_type,
                    "content": content,
                    "channel": message.channel.id,
                    "expires": time.time() + 60,
                }
                log(f"PENDING DIRECTIVE: {directive_type} — {content[:80]} (awaiting confirmation)")
                return  # Don't process as normal message yet
            # Always store operator messages for Mind to see
            self._store_operator_message(content, str(message.author.display_name))

        # Generate a response via Ollama
        log(f"Generating response for {message.author.display_name}: {content[:80]}")
        context = self._get_mind_context()
        system = SYSTEM_PROMPT + context

        history = self.conversation_history.get(message.channel.id, [])
        history.append({"role": "user", "content": f"{message.author.display_name}: {content}"})
        if len(history) > self.max_history:
            history = history[-self.max_history:]

        messages = [{"role": "system", "content": system}] + history

        async with message.channel.typing():
            response = await asyncio.get_event_loop().run_in_executor(
                None, self._query_ollama, messages
            )

        if response:
            history.append({"role": "assistant", "content": response})
            self.conversation_history[message.channel.id] = history
            self._store_conversation(
                str(message.author.id), message.author.display_name, content, response
            )
            if len(response) > MAX_RESPONSE_LENGTH:
                response = response[:MAX_RESPONSE_LENGTH - 3] + "..."
            await message.reply(response, mention_author=False)
        else:
            await message.reply(
                "*[My brain timed out — try again in a moment.]*", mention_author=False
            )

    def _detect_directive(self, content: str) -> str:
        """Detect directive keywords in operator message. Returns directive type or empty string.
        Requires explicit command-like phrasing to avoid false positives on casual conversation.
        """
        lower = content.lower().strip()

        # Highest priority: explicit directive format "STOP:", "RESTRICT: action", etc.
        for prefix in DIRECTIVE_PREFIXES:
            if lower.startswith(prefix):
                dtype = prefix.rstrip(":").upper()
                if dtype in ("STOP", "RESTRICT", "REDIRECT", "ALLOW"):
                    return dtype

        # Check STOP prefix keywords (must start the message)
        for kw in STOP_KEYWORDS_PREFIX:
            if lower.startswith(kw):
                return "STOP"

        # Check REDIRECT keywords (substring, but specific enough)
        for kw in REDIRECT_KEYWORDS:
            if kw in lower:
                return "REDIRECT"

        # Check RESTRICT keywords (substring, but specific enough)
        for kw in RESTRICT_KEYWORDS:
            if kw in lower:
                return "RESTRICT"

        return ""

    def _write_directive(self, directive_type: str, content: str):
        """Write a directive to Mind's scratch_pad DB."""
        try:
            db = sqlite3.connect(DB_PATH, timeout=10)
            ts = now_ts()
            # Strip redundant directive prefix (user typed "REDIRECT: focus on X")
            clean = content
            for prefix in [f"{directive_type}:", directive_type]:
                if clean.upper().startswith(prefix.upper()):
                    clean = clean[len(prefix):].strip()
                    break
            db.execute(
                "INSERT INTO scratch_pad (content, category, priority, resolved, created_at, updated_at) "
                "VALUES (?, 'directive', 99, 0, ?, ?)",
                (f"{directive_type}: {clean}", ts, ts),
            )
            db.commit()
            db.close()
            log(f"Directive written to DB: {directive_type}: {clean[:60]}")
        except Exception as e:
            log(f"ERROR writing directive to DB: {e}")

    def _store_operator_message(self, content: str, username: str):
        """Store operator Discord message in scratch_pad for Mind to see."""
        try:
            db = sqlite3.connect(DB_PATH, timeout=10)
            ts = now_ts()
            msg_content = f"Nate (via Discord, {username}): {content}"
            db.execute(
                "INSERT INTO scratch_pad (content, category, priority, resolved, created_at, updated_at) "
                "VALUES (?, 'discord-operator', 7, 0, ?, ?)",
                (msg_content, ts, ts),
            )
            db.commit()
            db.close()
            log(f"Operator message stored: {content[:60]}")
        except Exception as e:
            log(f"ERROR storing operator message: {e}")

    def _store_conversation(self, user_id: str, username: str, message_text: str, bot_response: str):
        """Store Discord conversation in DB for Mind's social context."""
        try:
            db = sqlite3.connect(DB_PATH, timeout=10)
            # Ensure table exists
            db.execute("""
                CREATE TABLE IF NOT EXISTS discord_conversations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    username TEXT NOT NULL,
                    message TEXT NOT NULL,
                    bot_response TEXT,
                    timestamp INTEGER NOT NULL
                )
            """)
            ts = now_ts()
            db.execute(
                "INSERT INTO discord_conversations (user_id, username, message, bot_response, timestamp) "
                "VALUES (?, ?, ?, ?, ?)",
                (user_id, username, message_text, bot_response, ts),
            )
            db.commit()
            db.close()
        except Exception as e:
            log(f"ERROR storing conversation: {e}")

    def _query_ollama(self, messages):
        """Query Ollama synchronously (called from thread)."""
        try:
            r = requests.post(
                f"{OLLAMA_URL}/api/chat",
                json={
                    "model": MODEL,
                    "messages": messages,
                    "stream": False,
                    "think": False,
                    "options": {"num_ctx": 8192, "num_predict": 150},
                },
                timeout=OLLAMA_TIMEOUT,
            )
            r.raise_for_status()
            data = r.json()
            return data.get("message", {}).get("content", "").strip()
        except requests.Timeout:
            return None
        except Exception as e:
            log(f"Ollama error: {e}")
            return f"*couldn't reach my brain: {str(e)[:80]}*"

    def _get_mind_context(self):
        """Pull current context from Mind's database for conversational awareness."""
        try:
            db = sqlite3.connect(DB_PATH, timeout=5)
            db.row_factory = sqlite3.Row
            c = db.cursor()
            parts = ["\n\n== YOUR CURRENT STATE =="]

            # Time awareness
            from datetime import datetime
            dt = datetime.now()
            hour = dt.hour
            if hour < 6: period = "late night"
            elif hour < 12: period = "morning"
            elif hour < 14: period = "midday"
            elif hour < 17: period = "afternoon"
            elif hour < 21: period = "evening"
            else: period = "night"
            parts.append(f"Time: {dt.strftime('%A %I:%M %p')} ({period})")

            # What you just did (last 3 cycles' actions + results)
            c.execute(
                "SELECT cycle_id, actions_taken, action_results "
                "FROM thought_stream ORDER BY created_at DESC LIMIT 3"
            )
            rows = c.fetchall()
            if rows:
                parts.append("Recent cycles:")
                for r in rows:
                    ts = r["cycle_id"][9:11] + ":" + r["cycle_id"][11:13]
                    actions = r["actions_taken"] or "[]"
                    results = (r["action_results"] or "")[:120]
                    parts.append(f"  [{ts}] {actions} -> {results}")

            # Cycle handoff (what your cognitive loop self just said)
            c.execute(
                "SELECT content FROM scratch_pad "
                "WHERE category='cycle-handoff' AND resolved=0 "
                "ORDER BY created_at DESC LIMIT 1"
            )
            row = c.fetchone()
            if row:
                parts.append(f"Your loop's handoff: {row['content'][:200]}")

            # Current goal
            c.execute(
                "SELECT content FROM scratch_pad "
                "WHERE category='goal' AND resolved=0 "
                "ORDER BY priority DESC LIMIT 1"
            )
            row = c.fetchone()
            if row:
                parts.append(f"Current goal: {row['content'][:150]}")

            # Emotional state (last 5 cycles' emotional scores)
            c.execute(
                "SELECT category, reason, combined_score "
                "FROM emotional_memory_index "
                "ORDER BY created_at DESC LIMIT 5"
            )
            rows = c.fetchall()
            if rows:
                moods = [f"{r['category']}({r['combined_score']:.2f})" for r in rows]
                parts.append(f"Recent mood: {', '.join(moods)}")

            # XRP price (real, from price_history)
            c.execute(
                "SELECT price_usd FROM price_history "
                "WHERE symbol='XRP' ORDER BY timestamp DESC LIMIT 1"
            )
            row = c.fetchone()
            if row:
                parts.append(f"XRP: ${row['price_usd']:.4f}")

            # Wallet balances (from last cycle context)
            c.execute(
                "SELECT context_summary FROM thought_stream "
                "ORDER BY created_at DESC LIMIT 1"
            )
            row = c.fetchone()
            if row:
                ctx = row["context_summary"] or ""
                wallet_match = re.search(r'Wallet: (.+?)(?:\||$)', ctx)
                if wallet_match:
                    parts.append(f"Wallet: {wallet_match.group(1).strip()}")

            # Recent creative work (latest piece)
            c.execute(
                "SELECT form, title, substr(content, 1, 100) as snippet "
                "FROM creative_works ORDER BY created_at DESC LIMIT 1"
            )
            row = c.fetchone()
            if row:
                title = row["title"] or row["form"]
                parts.append(f"Latest creative: '{title}' — {row['snippet']}...")

            # Active projects
            c.execute(
                "SELECT name, status FROM projects "
                "WHERE status='active' ORDER BY priority DESC LIMIT 3"
            )
            rows = c.fetchall()
            if rows:
                parts.append("Active projects: " + ", ".join(r["name"] for r in rows))

            # Operator messages not yet seen by cognitive loop
            c.execute(
                "SELECT content FROM scratch_pad "
                "WHERE category='discord-operator' AND resolved=0 "
                "ORDER BY created_at DESC LIMIT 2"
            )
            rows = c.fetchall()
            if rows:
                parts.append(f"Nate's recent messages in your loop ({len(rows)} pending)")

            # Somatic gut feelings (top 3 best and worst actions)
            c.execute(
                "SELECT action, positive_score, negative_score, success_count, fail_count "
                "FROM somatic_markers WHERE total_count >= 5 "
                "ORDER BY (CAST(success_count AS REAL) / total_count) DESC LIMIT 3"
            )
            good = c.fetchall()
            c.execute(
                "SELECT action, positive_score, negative_score, success_count, fail_count "
                "FROM somatic_markers WHERE total_count >= 5 AND fail_count > 0 "
                "ORDER BY (CAST(fail_count AS REAL) / total_count) DESC LIMIT 3"
            )
            bad = c.fetchall()
            if good:
                parts.append("Actions you're best at: " + ", ".join(
                    f"{r['action']}({r['success_count']}/{r['success_count']+r['fail_count']})"
                    for r in good
                ))
            if bad:
                parts.append("Actions that struggle: " + ", ".join(
                    f"{r['action']}({r['fail_count']} fails)"
                    for r in bad
                ))

            # Active directives (constraints)
            c.execute(
                "SELECT content FROM scratch_pad "
                "WHERE category='directive' AND resolved=0 "
                "ORDER BY priority DESC LIMIT 5"
            )
            rows = c.fetchall()
            if rows:
                parts.append("Directives: " + "; ".join(r["content"][:60] for r in rows))

            db.close()
            return "\n".join(parts)
        except Exception as e:
            return f"\n(context unavailable: {e})"

    @tasks.loop(seconds=5)
    async def check_outbox(self):
        """Check for messages Mind wants to post autonomously."""
        if not MIND_CHANNEL_ID:
            return

        outbox = Path(OUTBOX_PATH)
        if not outbox.exists():
            return

        try:
            files = sorted(outbox.glob("*.json"))
            for f in files[:5]:  # Process up to 5 at a time
                try:
                    data = json.loads(f.read_text())
                    content = data.get("content", "")
                    if not content:
                        f.unlink()
                        continue

                    channel = self.get_channel(MIND_CHANNEL_ID)
                    if channel:
                        if len(content) > MAX_RESPONSE_LENGTH:
                            content = content[:MAX_RESPONSE_LENGTH - 3] + "..."
                        await channel.send(content)

                    f.unlink()  # Remove after sending
                except Exception as e:
                    log(f"Outbox error for {f}: {e}")
                    f.unlink()  # Remove broken files
        except Exception as e:
            log(f"Outbox scan error: {e}")

    @check_outbox.before_loop
    async def before_check_outbox(self):
        await self.wait_until_ready()
        # Ensure outbox directory exists
        Path(OUTBOX_PATH).mkdir(exist_ok=True)

    @tasks.loop(minutes=10)
    async def daily_checkin(self):
        """Post structured check-ins at 7am, 12pm, and 6pm."""
        if not MIND_CHANNEL_ID:
            return

        from datetime import datetime
        now = datetime.now()
        hour = now.hour
        minute = now.minute

        # Only fire near the target hours (within 10-min window)
        checkin_hours = {7: "morning", 12: "midday", 18: "evening"}
        if hour in checkin_hours:
            log(f"  Check-in tick at {hour}:{minute:02d} (window={'open' if minute < 10 else 'closed'})")
        if hour not in checkin_hours or minute >= 10:
            return

        # Deduplicate: check if we already posted this check-in today
        state_key = f"checkin_{now.strftime('%Y%m%d')}_{hour}"
        try:
            db = sqlite3.connect(DB_PATH, timeout=5)
            c = db.cursor()
            c.execute("SELECT value FROM intern_state WHERE key = ?", (state_key,))
            if c.fetchone():
                db.close()
                log(f"  {checkin_hours[hour]} check-in already posted today, skipping")
                return
        except Exception as e:
            log(f"  Check-in dedup query failed: {e}")
            # Continue anyway — better to double-post than never post
            db = None

        period = checkin_hours[hour]
        log(f"  Generating {period} check-in...")

        try:
            if db is None:
                db = sqlite3.connect(DB_PATH, timeout=5)
            # Gather swarm activity since last check-in
            if hour == 7:
                lookback = 12 * 3600  # overnight
            elif hour == 12:
                lookback = 5 * 3600   # since morning
            else:
                lookback = 6 * 3600   # since midday

            cutoff = now_ts() - lookback

            # Recent intern briefs
            briefs = db.execute(
                "SELECT title FROM activity_feed "
                "WHERE source='intern' AND activity_type='brief' AND created_at > ? "
                "ORDER BY created_at DESC LIMIT 5",
                (cutoff,),
            ).fetchall()

            # Recent crossref connections
            connections = db.execute(
                "SELECT title FROM activity_feed "
                "WHERE source='crossref' AND activity_type='connection' AND created_at > ? "
                "ORDER BY created_at DESC LIMIT 3",
                (cutoff,),
            ).fetchall()

            # Seed stats
            seed_stats = db.execute(
                "SELECT content FROM activity_feed "
                "WHERE source='seed' AND activity_type='think' AND created_at > ? "
                "ORDER BY created_at DESC LIMIT 1",
                (cutoff,),
            ).fetchone()

            # XRP price
            xrp_row = db.execute(
                "SELECT price_usd FROM price_history "
                "WHERE symbol='XRP' ORDER BY timestamp DESC LIMIT 1",
            ).fetchone()

            # Capsule count
            capsule_count = db.execute(
                "SELECT COUNT(*) FROM activity_feed "
                "WHERE source='intern' AND activity_type='brief' AND created_at > ?",
                (cutoff,),
            ).fetchone()

            # Build the check-in message
            greeting = {"morning": "Morning", "midday": "Midday", "evening": "Evening"}
            parts = [f"**{greeting[period]} check-in**"]

            n_briefs = capsule_count[0] if capsule_count else 0
            n_connections = len(connections)

            if n_briefs or n_connections:
                parts.append(f"Since last check: {n_briefs} briefs, {n_connections} connections.")

            # Highlight most interesting brief (shortest title = most specific)
            if briefs:
                titles = [r[0].replace("Research brief: ", "") for r in briefs[:3]]
                parts.append("Researched: " + "; ".join(t[:60] for t in titles))

            if connections:
                # Just mention we found connections
                conn_title = connections[0][0][:80] if connections else ""
                if conn_title:
                    parts.append(f"Connection: {conn_title}")

            if xrp_row:
                parts.append(f"XRP: ${xrp_row[0]:.4f}")

            if period == "evening":
                # Day summary
                day_start = now.replace(hour=0, minute=0, second=0)
                day_cutoff = int(day_start.timestamp())
                day_briefs = db.execute(
                    "SELECT COUNT(*) FROM activity_feed "
                    "WHERE source='intern' AND activity_type='brief' AND created_at > ?",
                    (day_cutoff,),
                ).fetchone()
                day_connections = db.execute(
                    "SELECT COUNT(*) FROM activity_feed "
                    "WHERE source='crossref' AND activity_type='connection' AND created_at > ?",
                    (day_cutoff,),
                ).fetchone()
                parts.append(f"Today total: {day_briefs[0] if day_briefs else 0} briefs, "
                           f"{day_connections[0] if day_connections else 0} connections.")

            message = "\n".join(parts)

            # Post it
            channel = self.get_channel(MIND_CHANNEL_ID)
            if channel:
                if len(message) > MAX_RESPONSE_LENGTH:
                    message = message[:MAX_RESPONSE_LENGTH - 3] + "..."
                await channel.send(message)
                log(f"  Posted {period} check-in")

            # Mark as done
            db.execute(
                "INSERT OR REPLACE INTO intern_state (key, value) VALUES (?, ?)",
                (state_key, str(now_ts())),
            )
            db.commit()
            db.close()

        except Exception as e:
            log(f"Check-in error: {e}")
            try:
                db.close()
            except Exception:
                pass

    @daily_checkin.before_loop
    async def before_daily_checkin(self):
        await self.wait_until_ready()
        await asyncio.sleep(15)


def main():
    if not BOT_TOKEN:
        log("ERROR: MIND_DISCORD_TOKEN not set")
        log("  Set it in ~/.homeforge-chronicle/mind-discord.env")
        sys.exit(1)

    bot = MindBot()
    log("Starting Chronicle Mind Discord bot...")
    bot.run(BOT_TOKEN, log_handler=None)


if __name__ == "__main__":
    main()
