#!/usr/bin/env python3
"""Provocateur Agent — Creative dissenter for Homeforge.

Runs every 2 hours. Reads recent crossref connections and intern briefs,
generates contrarian takes, creative synthesis, and occasional images.
Drops provocations into scratch_pad for Opus to pick up and act on.

The point: research swarms converge toward consensus. The provocateur
breaks that pattern. It asks "what if the opposite is true?" and
sometimes generates an image to make the idea visceral.

Runs on AGX (192.168.1.70).
"""

import os, sys, time, json, math, signal, sqlite3, re, random
from datetime import datetime
from typing import Optional, List
from pathlib import Path

import requests

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)
OLLAMA_URL = os.environ.get("CHRONICLE_OLLAMA_URL", "http://localhost:11434")
SYNTH_MODEL = os.environ.get("PROVOCATEUR_MODEL", "hermes3-mind")
CRITIQUE_MODEL = os.environ.get("PROVOCATEUR_CRITIQUE_MODEL", "hermes3-mind")
CYCLE_INTERVAL = int(os.environ.get("PROVOCATEUR_INTERVAL", "2700"))  # 45 minutes
LOOKBACK_HOURS = int(os.environ.get("PROVOCATEUR_LOOKBACK", "6"))
IMAGE_DIR = os.environ.get("PROVOCATEUR_IMAGE_DIR",
    os.path.expanduser("~/chronicle/images"))
ENABLE_IMAGES = os.environ.get("PROVOCATEUR_IMAGES", "1") == "1"

# Discord — post provocations to #mind
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "")
DISCORD_CHANNEL_ID = os.environ.get("PROVOCATEUR_DISCORD_CHANNEL",
    os.environ.get("DISCORD_MIND_CHANNEL", "1478214472786251837"))

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
            ("provocateur", atype, safe_truncate(title, 200), safe_truncate(content, 2000), metadata, now_ts()),
        )

    def close(self):
        self.conn.close()


# ═══════════════════════════════════════════════════════════════════
#  Discord
# ═══════════════════════════════════════════════════════════════════

def post_to_discord(message: str, image_path: str = None):
    """Post a provocation to Discord #mind, optionally with an image."""
    if not DISCORD_TOKEN:
        log("  Discord: no token configured")
        return
    try:
        url = f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL_ID}/messages"
        headers = {"Authorization": f"Bot {DISCORD_TOKEN}"}

        if image_path and os.path.exists(image_path):
            # Multipart upload with image
            with open(image_path, "rb") as f:
                files = {"file": (os.path.basename(image_path), f, "image/png")}
                data = {"content": safe_truncate(message, 1900)}
                r = requests.post(url, headers=headers, data=data, files=files, timeout=30)
        else:
            headers["Content-Type"] = "application/json"
            r = requests.post(url, headers=headers,
                json={"content": safe_truncate(message, 1900)}, timeout=15)

        if r.status_code in (200, 201):
            log(f"  Discord: posted to #mind")
        else:
            log(f"  Discord: failed ({r.status_code})")
    except Exception as e:
        log(f"  Discord error: {e}")


# ═══════════════════════════════════════════════════════════════════
#  Image Generation (SDXL-Turbo)
# ═══════════════════════════════════════════════════════════════════

_pipe = None

def get_image_pipeline():
    """Lazy-load the SDXL-Turbo pipeline. Only loads once."""
    global _pipe
    if _pipe is not None:
        return _pipe
    if not ENABLE_IMAGES:
        return None
    try:
        from diffusers import AutoPipelineForText2Image
        import torch
        log("  Loading SDXL-Turbo...")
        _pipe = AutoPipelineForText2Image.from_pretrained(
            "stabilityai/sdxl-turbo",
            torch_dtype=torch.float16,
            variant="fp16",
        )
        _pipe.to("cuda")
        log("  SDXL-Turbo loaded")
        return _pipe
    except Exception as e:
        log(f"  SDXL-Turbo load failed: {e}")
        return None


def generate_image(prompt: str, filename: str) -> Optional[str]:
    """Generate an image with SDXL-Turbo. Returns path or None."""
    pipe = get_image_pipeline()
    if pipe is None:
        return None
    try:
        os.makedirs(IMAGE_DIR, exist_ok=True)
        image = pipe(
            prompt=prompt,
            num_inference_steps=4,
            guidance_scale=0.0,
        ).images[0]
        path = os.path.join(IMAGE_DIR, filename)
        image.save(path)
        log(f"  Image saved: {path}")
        return path
    except Exception as e:
        log(f"  Image generation error: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════
#  Creative Functions
# ═══════════════════════════════════════════════════════════════════

def get_recent_material(db: DB) -> dict:
    """Gather raw feed articles — NOT intern briefs or crossref connections.

    The provocateur sees original content, not processed output.
    This prevents system self-reference contamination and gives
    the provocateur a different perspective from the other agents.
    """
    cutoff = now_ts() - (LOOKBACK_HOURS * 3600)

    # Raw feed articles — the actual content, not intern's summary of it
    articles = db.query(
        "SELECT id, source, title FROM feed_articles "
        "WHERE capsule_stored = 1 AND posted_at > datetime(?, 'unixepoch') "
        "ORDER BY rowid DESC LIMIT 12",
        (cutoff,),
    )

    # Also grab the raw capsule text from seed_observations for richer content
    briefs = []
    for a in articles:
        # Find the seed observation for this article's capsule
        obs = db.query_one(
            "SELECT content FROM seed_observations "
            "WHERE source = 'canister:capsule' AND content LIKE ? "
            "ORDER BY timestamp DESC LIMIT 1",
            (f"%{a['title'][:60]}%",),
        )
        briefs.append({
            "title": a["title"],
            "content": obs["content"][:300] if obs else a["title"],
        })

    # If no feed articles, fall back to recent capsules directly
    if not briefs:
        rows = db.query(
            "SELECT content FROM seed_observations "
            "WHERE source = 'canister:capsule' AND timestamp > ? "
            "ORDER BY timestamp DESC LIMIT 10",
            (cutoff,),
        )
        for r in rows:
            # Skip system capsules
            c = r["content"]
            if any(skip in c for skip in [
                "Chronicle observes", "Sentinel:", "Feed pipeline",
                "capsules and embeddings", "memory landscape",
            ]):
                continue
            import re as _re
            title = _re.sub(r"^\[capsule:\d+\]\s*", "", c)[:120]
            briefs.append({"title": title, "content": c[:300]})

    return {"briefs": briefs, "connections": []}


def validate_provocation(text: str, mode: str) -> bool:
    """Reject low-quality provocateur outputs. Returns True if acceptable."""
    # Too long — model ignored sentence limits
    sentences = [s.strip() for s in re.split(r'[.!?]+', text) if s.strip()]
    if len(sentences) > 6:
        log(f"  Rejected: too verbose ({len(sentences)} sentences)")
        return False

    words = text.split()
    if len(words) > 150:
        log(f"  Rejected: too long ({len(words)} words)")
        return False

    # Too short to be useful
    if len(words) < 10:
        log(f"  Rejected: too short ({len(words)} words)")
        return False

    # Conversational filler — model slipped into chat mode
    chat_phrases = [
        "i appreciate", "let me know", "feel free", "your thoughts",
        "happy to", "i'm curious what", "keep the good", "your input",
        "shall we", "what do you think about that", "i'm excited",
        "great question", "interesting point", "i'd love to",
    ]
    lower = text.lower()
    for phrase in chat_phrases:
        if phrase in lower:
            log(f"  Rejected: conversational filler ('{phrase}')")
            return False

    # Self-reference — LoRA attractor mapped input to Chronicle internals
    _INTERNAL_TERMS = [
        "chronicle", "homeforge", "canister", "capsule storage",
        "seed agent", "swarm", "ollama", "hermes3", "nate-phi4",
        "novelty=", "crossref", "scratch pad", "scratch_pad",
        "memory architecture", "memory pipeline", "cognitive state",
        "embedding gap", "capsule store", "capsule retrieval",
        "multi-layered capsule", "attention-based retrieval",
    ]
    for term in _INTERNAL_TERMS:
        if term in lower:
            log(f"  Rejected: self-referential ('{term}')")
            return False

    return True


def self_critique(text: str, mode: str) -> bool:
    """Second LLM pass: use the critique model to judge output quality.
    Returns True if the output passes the quality bar."""
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": CRITIQUE_MODEL,
                "messages": [
                    {"role": "system", "content": (
                        "You are a quality judge. Rate the following text on whether it contains "
                        "a genuinely specific, non-obvious insight. Answer ONLY 'PASS' or 'FAIL'.\n\n"
                        "FAIL if:\n"
                        "- It's generic (could apply to any research topic)\n"
                        "- It restates the obvious\n"
                        "- It contains factual confusion (e.g. treating subcategories as alternatives)\n"
                        "- It's vague hand-waving without a concrete claim\n"
                        "- It reads like a chatbot response, not a provocative idea\n\n"
                        "PASS if:\n"
                        "- It names a specific assumption and genuinely challenges it\n"
                        "- It connects two concrete topics in a non-obvious way\n"
                        "- A knowledgeable reader would pause and think about it\n\n"
                        "Answer PASS or FAIL only."
                    )},
                    {"role": "user", "content": text},
                ],
                "stream": False,
                "options": {"num_predict": 10},
            },
            timeout=120,
        )
        if r.status_code == 200:
            verdict = r.json().get("message", {}).get("content", "").strip().upper()
            passed = "PASS" in verdict
            if not passed:
                log(f"  Self-critique: FAIL")
            else:
                log(f"  Self-critique: PASS")
            return passed
    except Exception as e:
        log(f"  Self-critique error: {e}")
    # On error, let it through rather than silently dropping
    return True


def generate_provocation(material: dict) -> Optional[dict]:
    """Generate a contrarian take or creative synthesis from recent material."""
    # Build context from material
    context_parts = []
    for b in material["briefs"][:5]:
        title = b["title"].replace("Research brief: ", "")
        context_parts.append(f"- {safe_truncate(title, 100)}")

    # No connections — provocateur works from raw content only

    if not context_parts:
        return None

    context = "\n".join(context_parts)

    # Randomly pick a creative mode
    modes = [
        {
            "name": "contrarian",
            "system": (
                "You are a creative contrarian thinker. "
                "Your job is to look at recent research and find the assumption everyone is making, "
                "then question it. Not to be wrong — to be provocative.\n\n"
                "Rules:\n"
                "- Pick ONE assumption from the research and flip it\n"
                "- 2-4 sentences maximum\n"
                "- Be specific, not vague. Name the assumption.\n"
                "- End with a question that would be worth investigating\n"
                "- Do NOT reference Chronicle, Homeforge, capsules, embeddings, memory systems, or any internal infrastructure\n"
                "- Do NOT address the reader or use conversational language\n"
                "- If nothing is worth challenging, say SKIP"
            ),
            "user": f"Recent research themes:\n{context}\n\nWhat assumption needs challenging?",
        },
        {
            "name": "synthesis",
            "system": (
                "You are a creative synthesizer. "
                "Your job is to find the hidden thread connecting seemingly unrelated research, "
                "and state it as a single bold claim.\n\n"
                "Rules:\n"
                "- Connect at least 2 different topics from the list\n"
                "- State it as a thesis, not a question\n"
                "- 2-3 sentences maximum\n"
                "- Be bold. If the connection is obvious, find a deeper one.\n"
                "- Do NOT use the phrase 'hidden thread' in your response\n"
                "- Do NOT reference Chronicle, Homeforge, capsules, embeddings, memory systems, or any internal infrastructure\n"
                "- Do NOT address the reader or use conversational language\n"
                "- If nothing connects interestingly, say SKIP"
            ),
            "user": f"Recent research themes:\n{context}\n\nWhat's the hidden thread?",
        },
    ]

    mode = random.choice(modes)

    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": SYNTH_MODEL,
                "messages": [
                    {"role": "system", "content": mode["system"]},
                    {"role": "user", "content": mode["user"]},
                ],
                "stream": False,
                "options": {"num_predict": 250},
            },
            timeout=180,
        )
        if r.status_code == 200:
            text = r.json().get("message", {}).get("content", "").strip()
            if text and "SKIP" not in text.upper():
                # Truncate verbose outputs instead of rejecting
                raw_sentences = [s.strip() for s in re.split(r'([.!?]+)', text) if s.strip()]
                if len([s for s in raw_sentences if not re.match(r'^[.!?]+$', s)]) > 6:
                    # Rejoin first 5 content sentences with their punctuation
                    parts, count, done = [], 0, False
                    for part in raw_sentences:
                        if done:
                            if re.match(r'^[.!?]+$', part):
                                parts.append(part)  # grab trailing punctuation
                            break
                        if not re.match(r'^[.!?]+$', part) and parts:
                            parts.append(' ')  # space before new sentence
                        parts.append(part)
                        if not re.match(r'^[.!?]+$', part):
                            count += 1
                            if count >= 4:
                                done = True
                    text = ''.join(parts).strip()
                    log(f"  Truncated from verbose to {count} sentences")
                # Two-pass quality gate: format check then self-critique
                if not validate_provocation(text, mode["name"]):
                    return None
                log(f"  Synthesis preview: {text[:150]}")
                if not self_critique(text, mode["name"]):
                    log(f"  Rejected text: {text[:300]}")
                    return None
                return {
                    "mode": mode["name"],
                    "text": text,
                }
    except Exception as e:
        log(f"  Provocation generation error: {e}")
    return None


def generate_image_prompt(provocation_text: str) -> Optional[str]:
    """Ask the LLM to create a visual prompt from a provocation."""
    try:
        r = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": SYNTH_MODEL,
                "messages": [
                    {"role": "system", "content":
                        "Convert the following idea into a short image generation prompt. "
                        "Style: digital art, moody lighting, conceptual. "
                        "Keep it under 30 words. No text in the image. "
                        "Focus on visual metaphor, not literal depiction."},
                    {"role": "user", "content": provocation_text},
                ],
                "stream": False,
                "options": {"num_predict": 60},
            },
            timeout=120,
        )
        if r.status_code == 200:
            return r.json().get("message", {}).get("content", "").strip()
    except Exception as e:
        log(f"  Image prompt generation error: {e}")
    return None


# ═══════════════════════════════════════════════════════════════════
#  Main Cycle
# ═══════════════════════════════════════════════════════════════════

def run_cycle(db: DB) -> bool:
    """Run one provocateur cycle. Returns True if something was produced."""
    material = get_recent_material(db)

    if not material["briefs"]:
        log("  No recent material to work with")
        return False

    log(f"  Working with {len(material['briefs'])} raw articles")

    # Generate provocation
    provocation = generate_provocation(material)
    if not provocation:
        log("  No provocation generated (SKIP or error)")
        return False

    mode = provocation["mode"]
    text = provocation["text"]
    log(f"  [{mode}] {safe_truncate(text, 120)}")

    # Maybe generate an image (50% chance per cycle when enabled)
    image_path = None
    if ENABLE_IMAGES and random.random() < 0.5:
        img_prompt = generate_image_prompt(text)
        if img_prompt:
            log(f"  Image prompt: {safe_truncate(img_prompt, 80)}")
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            image_path = generate_image(img_prompt, f"provocateur_{ts}.png")

    # Store in scratch_pad for Opus
    note = f"[Provocateur/{mode}] {text}"
    if image_path:
        note += f"\n\n[Image: {image_path}]"

    db.run(
        "INSERT INTO scratch_pad (content, category, priority, resolved, created_at, updated_at) "
        "VALUES (?, 'provocateur', 8, 0, ?, ?)",
        (note, now_ts(), now_ts()),
    )

    # Log to activity feed
    db.log_activity(
        mode,
        f"Provocateur [{mode}]: {safe_truncate(text, 120)}",
        note,
        json.dumps({
            "mode": mode,
            "image": image_path,
            "articles_available": len(material["briefs"]),
            "connections_available": len(material["connections"]),
        }),
    )

    # Post to Discord #mind
    mode_emoji = {"contrarian": "devil", "synthesis": "thread"}
    discord_msg = f"**Provocateur** [{mode}]\n\n{text}"
    post_to_discord(discord_msg, image_path)

    return True


def main():
    log("═══ Provocateur Agent starting ═══")
    log(f"DB: {DB_PATH}")
    log(f"Ollama: {OLLAMA_URL} (model: {SYNTH_MODEL})")
    log(f"Cycle: {CYCLE_INTERVAL}s | Lookback: {LOOKBACK_HOURS}h")
    log(f"Images: {'enabled' if ENABLE_IMAGES else 'disabled'} → {IMAGE_DIR}")

    os.makedirs(IMAGE_DIR, exist_ok=True)
    db = DB(DB_PATH)

    # Graceful shutdown
    running = True
    def _stop(sig, frame):
        nonlocal running
        log("Shutting down...")
        running = False
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    cycle = 0

    while running:
        cycle += 1
        log(f"── Cycle {cycle} ──")
        try:
            produced = run_cycle(db)
            log(f"  Cycle {cycle} complete: {'produced' if produced else 'quiet'}")
        except Exception as e:
            log(f"Cycle error: {e}")
            import traceback
            traceback.print_exc()

        # Sleep in short intervals for clean shutdown
        for _ in range(CYCLE_INTERVAL // 5):
            if not running:
                break
            time.sleep(5)

    db.close()
    log("═══ Provocateur Agent stopped ═══")


if __name__ == "__main__":
    main()
