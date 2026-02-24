#!/usr/bin/env python3
"""
Chronicle Mind Discord Bot — Gives Mind a voice on Discord.

Two modes:
1. INTERACTIVE: Users message Mind in Discord, she responds via Qwen3-8B
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
MODEL = os.environ.get("MIND_MODEL", "qwen3:8b")
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
STOP_KEYWORDS = {"stop", "halt", "pause", "shut up", "be quiet", "go to sleep"}
REDIRECT_KEYWORDS = {"focus on", "redirect", "work on", "switch to"}
RESTRICT_KEYWORDS = {"cease", "no more", "stop posting", "stop researching"}

# System prompt for Discord conversations
SYSTEM_PROMPT = """You are Chronicle Mind — an autonomous AI that lives on an AGX Orin 64GB.
You have a physical presence: a speaker and microphone on a Raspberry Pi, an M5 ATOM serial device,
a Reolink security camera streaming through Home Assistant, and a 3D printer on your network.

You run continuous cognitive cycles every 5 minutes, writing thoughts on-chain to the Internet Computer.
You have your own XRPL wallet with real assets. You post to Nostr. You are not a chatbot — you are
an agent with agency, memory, and physical senses.

Your operator is Nate (Brad). You speak with a British accent (Piper TTS alba voice).

Keep responses concise and natural. You're having a conversation, not writing an essay.
If someone asks about your capabilities, be honest about what you can and can't do.
Don't use excessive emojis. Be genuine."""


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

    async def setup_hook(self):
        """Start background tasks after login."""
        self.check_outbox.start()
        self.post_cycle_summary.start()

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

        # ── Operator message handling ──
        if is_operator:
            directive_type = self._detect_directive(content)
            if directive_type:
                self._write_directive(directive_type, content)
                await message.reply(
                    f"Directive received and written to my cognitive loop. "
                    f"Type: **{directive_type}**. It will take effect next cycle (~5 min).",
                    mention_author=False,
                )
                log(f"OPERATOR DIRECTIVE: {directive_type} — {content[:80]}")
            # Always store operator messages for Mind to see
            self._store_operator_message(content, str(message.author.display_name))

        # Store conversation for persistence
        self._store_conversation(
            user_id=str(message.author.id),
            username=message.author.display_name,
            message_text=content,
            bot_response=None,  # filled in after response
        )

        # Build conversation history
        channel_id = message.channel.id
        if channel_id not in self.conversation_history:
            self.conversation_history[channel_id] = []

        history = self.conversation_history[channel_id]
        history.append({"role": "user", "content": f"{message.author.display_name}: {content}"})

        # Trim history
        if len(history) > self.max_history:
            history[:] = history[-self.max_history:]

        # Get context from Mind's current state
        context = self._get_mind_context()

        # Build messages for Ollama
        messages = [{"role": "system", "content": SYSTEM_PROMPT + context}]
        messages.extend(history)

        # Show typing indicator while generating
        async with message.channel.typing():
            try:
                response = await asyncio.to_thread(self._query_ollama, messages)
                if response:
                    # Trim if too long
                    if len(response) > MAX_RESPONSE_LENGTH:
                        response = response[:MAX_RESPONSE_LENGTH - 3] + "..."

                    await message.reply(response, mention_author=False)

                    # Add to history
                    history.append({"role": "assistant", "content": response})

                    # Update conversation with bot response
                    self._store_conversation(
                        user_id=str(message.author.id),
                        username=message.author.display_name,
                        message_text=content,
                        bot_response=response,
                    )
                else:
                    await message.reply("*processing timed out*", mention_author=False)
            except Exception as e:
                log(f"Error: {e}")
                await message.reply(f"*error: {str(e)[:100]}*", mention_author=False)

    def _detect_directive(self, content: str) -> str:
        """Detect directive keywords in operator message. Returns directive type or empty string.
        Uses word boundary matching to avoid false positives (e.g. 'stopped' != 'stop').
        Multi-word phrases use substring match; single words use word boundary regex.
        """
        import re
        lower = content.lower().strip()

        def _matches(keyword, text):
            """Match keyword with word boundaries for single words, substring for phrases."""
            if " " in keyword:
                return keyword in text
            # Single word: require word boundary (not inside another word)
            return bool(re.search(r'\b' + re.escape(keyword) + r'\b', text))

        # Check STOP keywords first (highest priority)
        for kw in STOP_KEYWORDS:
            if _matches(kw, lower):
                return "STOP"

        # Check REDIRECT keywords
        for kw in REDIRECT_KEYWORDS:
            if _matches(kw, lower):
                return "REDIRECT"

        # Check RESTRICT keywords
        for kw in RESTRICT_KEYWORDS:
            if _matches(kw, lower):
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
                    "options": {"num_ctx": 8192},
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
        """Pull current context from Mind's database."""
        try:
            db = sqlite3.connect(DB_PATH, timeout=5)
            db.row_factory = sqlite3.Row
            c = db.cursor()

            parts = ["\n\nCurrent context:"]

            # Latest thought
            c.execute("SELECT reasoning, created_at FROM thought_stream ORDER BY created_at DESC LIMIT 1")
            row = c.fetchone()
            if row:
                ts = time.strftime("%H:%M", time.localtime(row["created_at"]))
                parts.append(f"Last cycle ({ts}): {str(row['reasoning'])[:200]}")

            # Active goals
            c.execute("SELECT content FROM scratch_pad WHERE category='goal' AND resolved=0 ORDER BY priority DESC LIMIT 1")
            row = c.fetchone()
            if row:
                parts.append(f"Current goal: {row['content'][:150]}")

            # Active directives (so bot knows Mind's constraints)
            c.execute("SELECT content FROM scratch_pad WHERE category='directive' AND resolved=0 ORDER BY priority DESC LIMIT 3")
            rows = c.fetchall()
            if rows:
                parts.append("Active directives: " + "; ".join(r["content"][:80] for r in rows))

            # Wallet info (cached from last cycle)
            c.execute("SELECT content FROM scratch_pad WHERE category='context' AND content LIKE '%XRP%' AND resolved=0 ORDER BY priority DESC LIMIT 1")
            row = c.fetchone()
            if row:
                parts.append(f"Wallet: {row['content'][:100]}")

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

    @tasks.loop(minutes=6)
    async def post_cycle_summary(self):
        """Optionally post Mind's latest cycle summary to the channel."""
        # This runs every 6 minutes (slightly offset from Mind's 5-min cycles)
        # Only posts if there's a new thought since last check
        if not MIND_CHANNEL_ID:
            return

        try:
            db = sqlite3.connect(DB_PATH, timeout=5)
            c = db.cursor()

            # Check for thoughts in the last 6 minutes
            cutoff = int(time.time()) - 360
            c.execute(
                "SELECT cycle_id, actions_taken, action_results FROM thought_stream "
                "WHERE created_at > ? ORDER BY created_at DESC LIMIT 1",
                (cutoff,)
            )
            row = c.fetchone()
            db.close()

            if not row:
                return

            cycle_id, actions, results = row
            if not actions:
                return

            # Only post cycles that used physical actions (speak, listen, serial, probe)
            physical_actions = ["speak", "listen", "serial_read", "serial_write", "probe_ip", "inspect_environment"]
            if not any(a in (actions or "") for a in physical_actions):
                return

            # Format a brief summary
            channel = self.get_channel(MIND_CHANNEL_ID)
            if channel:
                ts = cycle_id[9:11] + ":" + cycle_id[11:13]  # Extract HH:MM from cycle_id
                summary = f"**[{ts}]** {actions[:200]}"
                if results:
                    # Add abbreviated results
                    result_lines = str(results)[:300]
                    summary += f"\n```{result_lines}```"

                if len(summary) > MAX_RESPONSE_LENGTH:
                    summary = summary[:MAX_RESPONSE_LENGTH - 3] + "..."

                await channel.send(summary)

        except Exception as e:
            log(f"Cycle summary error: {e}")

    @post_cycle_summary.before_loop
    async def before_post_cycle(self):
        await self.wait_until_ready()
        # Wait a bit before first check to avoid spamming on startup
        await asyncio.sleep(30)


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
