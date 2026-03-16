#!/usr/bin/env python3
"""Chronicle Capture Processor — Phone-to-Canister Pipeline

Reads captures queued by Sprout's Discord bot (scratch_pad category='capture'),
synthesizes them with a local LLM, and POSTs capsules to the ICP canister.

Runs as a 90-second systemd timer on Jetson.
"""

import hashlib
import json
import logging
import os
import sqlite3
import subprocess
import sys
import time
import urllib.request
import urllib.error

# ── Configuration ──────────────────────────────────────────────

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)
OLLAMA_URL = os.environ.get("CHRONICLE_OLLAMA_URL", "http://localhost:11434")
CAPTURE_MODEL = os.environ.get("CAPTURE_MODEL", "qwen2.5:3b")
CANISTER_URL = "https://fqqku-bqaaa-aaaai-q4wha-cai.raw.icp0.io"
CAPTURES_DIR = os.path.expanduser("~/sprout/captures")
MAX_FAILURES = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [capture] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("capture")

# ── Synthesis Prompt ───────────────────────────────────────────

SYNTHESIS_PROMPT = """Extract structured information from this phone capture.
Return ONLY valid JSON with no other text: {"content": "clean restatement of the information", "topic": "category/subcategory", "keywords": ["k1","k2","k3"]}
Use topics like: homeforge/philosophy, chronicle/architecture, crypto/xrp, technology/ai, family, faith, personal, ideas, links
Keep the content faithful to the original — restate clearly, don't add interpretation.

Capture:
"""


# ── Database ───────────────────────────────────────────────────

def get_pending_captures(db):
    """Get unresolved capture notes, oldest first."""
    cur = db.execute(
        "SELECT id, content, created_at FROM scratch_pad "
        "WHERE category='capture' AND resolved=0 "
        "ORDER BY created_at ASC LIMIT 10"
    )
    return cur.fetchall()


def resolve_capture(db, note_id, error=None):
    """Mark a capture as resolved, optionally with error info."""
    now = int(time.time())
    if error:
        db.execute(
            "UPDATE scratch_pad SET resolved=1, updated_at=?, "
            "content = content || '\n[ERROR: ' || ? || ']' "
            "WHERE id=?",
            (now, error, note_id),
        )
    else:
        db.execute(
            "UPDATE scratch_pad SET resolved=1, updated_at=? WHERE id=?",
            (now, note_id),
        )
    db.commit()


def get_failure_count(db, note_id):
    """Count how many times we've tried this capture (via error markers)."""
    cur = db.execute(
        "SELECT content FROM scratch_pad WHERE id=?", (note_id,)
    )
    row = cur.fetchone()
    if not row:
        return MAX_FAILURES  # gone
    return row[0].count("[ERROR:")


def content_hash(text):
    """SHA256 hash of content for dedup."""
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def check_recent_dedup(db, chash):
    """Check if we've stored a capture with this hash in the last hour."""
    one_hour_ago = int(time.time()) - 3600
    cur = db.execute(
        "SELECT COUNT(*) FROM scratch_pad "
        "WHERE category='capture' AND resolved=1 AND updated_at > ? "
        "AND content LIKE '%' || ? || '%'",
        (one_hour_ago, chash),
    )
    return cur.fetchone()[0] > 0


# ── Ollama ─────────────────────────────────────────────────────

def ollama_generate(prompt, model=None):
    """Call Ollama generate API."""
    model = model or CAPTURE_MODEL
    payload = json.dumps({
        "model": model,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {
            "temperature": 0.3,
            "num_predict": 256,
        },
    }).encode()

    req = urllib.request.Request(
        f"{OLLAMA_URL}/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=120) as resp:
        data = json.loads(resp.read())
        return data.get("response", "")


def synthesize_text(text):
    """Use LLM to extract topic/keywords from text content."""
    prompt = SYNTHESIS_PROMPT + text[:2000]
    raw = ollama_generate(prompt)

    try:
        result = json.loads(raw)
        return {
            "content": result.get("content", text),
            "topic": result.get("topic", "personal"),
            "keywords": result.get("keywords", []),
        }
    except json.JSONDecodeError:
        log.warning("LLM returned invalid JSON, using raw text")
        return {"content": text, "topic": "personal", "keywords": []}


# ── URL Processing ─────────────────────────────────────────────

import re

_X_URL_RE = re.compile(
    r"https?://(?:www\.)?(?:x\.com|twitter\.com)/(\w+)/status/(\d+)"
)


def fetch_x_tweet(url, max_chars=2000):
    """Fetch tweet content via fxtwitter API (no JS needed)."""
    m = _X_URL_RE.match(url.split("?")[0])
    if not m:
        return None
    user, tweet_id = m.group(1), m.group(2)
    api_url = f"https://api.fxtwitter.com/{user}/status/{tweet_id}"
    try:
        req = urllib.request.Request(api_url, headers={"User-Agent": "Chronicle-Capture/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        tweet = data.get("tweet")
        if not tweet or data.get("code") != 200:
            log.warning("fxtwitter returned no tweet for %s", url)
            return None
        author = tweet.get("author", {})
        name = author.get("name", "Unknown")
        handle = author.get("screen_name", "")
        text = tweet.get("text", "")
        # Include quote tweet if present
        quote = tweet.get("quote")
        if quote:
            q_author = quote.get("author", {})
            q_name = q_author.get("name", "")
            q_text = quote.get("text", "")
            text += f'\n\nQuoting {q_name}: "{q_text}"'
        result = f"@{handle} ({name}): {text}"
        return result[:max_chars]
    except Exception as e:
        log.warning("fxtwitter failed for %s: %s", url, e)
        return None


def is_x_url(url):
    """Check if a URL is an X/Twitter post."""
    return bool(_X_URL_RE.match(url.split("?")[0]))


def fetch_url_text(url, max_chars=2000):
    """Fetch a URL and return stripped text content."""
    # Route X/Twitter URLs through fxtwitter
    if is_x_url(url):
        return fetch_x_tweet(url, max_chars)

    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Chronicle-Capture/1.0"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        # Simple HTML tag stripping
        text = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL)
        text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text[:max_chars]
    except Exception as e:
        log.warning("Failed to fetch %s: %s", url, e)
        return None


def synthesize_link(text, links):
    """Process a link capture — fetch and summarize."""
    parts = []
    if text:
        parts.append(text)

    for url in links[:3]:  # max 3 URLs
        fetched = fetch_url_text(url)
        if fetched:
            parts.append(f"[From {url}]: {fetched}")
        else:
            parts.append(f"[Link: {url}]")

    combined = "\n".join(parts)
    return synthesize_text(combined)


# ── Voice Processing ───────────────────────────────────────────

def transcribe_voice(local_path):
    """SCP voice file to AGX, transcribe with faster-whisper, return text."""
    try:
        remote_path = "/tmp/capture_voice_tmp"
        # SCP to AGX
        subprocess.run(
            ["scp", "-o", "ConnectTimeout=10", local_path,
             f"nate-agx@192.168.1.70:{remote_path}"],
            check=True, capture_output=True, timeout=30,
        )
        # Transcribe on AGX using faster-whisper
        result = subprocess.run(
            ["ssh", "-o", "ConnectTimeout=10", "nate-agx@192.168.1.70",
             f"python3 -c \""
             f"from faster_whisper import WhisperModel; "
             f"m = WhisperModel('tiny', device='cuda'); "
             f"segs, _ = m.transcribe('{remote_path}'); "
             f"print(' '.join(s.text for s in segs))"
             f"\""],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
        else:
            log.warning("Whisper failed: %s", result.stderr[:200])
            return None
    except Exception as e:
        log.warning("Voice transcription failed: %s", e)
        return None


# ── Canister POST ──────────────────────────────────────────────

def post_to_canister(content, topic=None, keywords=None, persons=None):
    """POST a capsule to the ICP canister's /api/feed endpoint."""
    payload = {"content": content}
    if topic:
        payload["topic"] = topic
    if keywords:
        payload["keywords"] = keywords
    if persons:
        payload["persons"] = persons

    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{CANISTER_URL}/api/feed",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode()
        status = resp.status
        if status in (200, 201):
            log.info("Capsule stored: %s", body[:100])
            return True
        else:
            log.error("Canister returned %d: %s", status, body[:200])
            return False


# ── Main Processing ────────────────────────────────────────────

def process_capture(db, note_id, raw_content):
    """Process a single capture entry."""
    try:
        capture = json.loads(raw_content)
    except json.JSONDecodeError:
        log.error("Note %d: invalid JSON, skipping", note_id)
        resolve_capture(db, note_id, "invalid JSON")
        return

    cap_type = capture.get("type", "text")
    text = capture.get("text", "")
    links = capture.get("links", [])
    attachments = capture.get("attachments", [])
    author = capture.get("author", "unknown")

    log.info("Processing capture #%d: type=%s, author=%s", note_id, cap_type, author)

    # Dedup check
    chash = content_hash(text + str(links))
    if check_recent_dedup(db, chash):
        log.info("Duplicate capture detected, skipping")
        resolve_capture(db, note_id)
        return

    # Process by type
    if cap_type == "voice":
        voice_attachments = [a for a in attachments
                            if a.get("content_type", "").startswith("audio/")]
        if voice_attachments:
            transcript = transcribe_voice(voice_attachments[0]["local_path"])
            if transcript:
                combined = f"{text}\n[Transcription]: {transcript}" if text else transcript
                result = synthesize_text(combined)
            else:
                # Fall back to just the text if transcription fails
                if text:
                    result = synthesize_text(text)
                else:
                    log.warning("Voice capture with no transcript and no text")
                    resolve_capture(db, note_id, "voice transcription failed, no text")
                    return
        else:
            result = synthesize_text(text) if text else None
            if not result:
                resolve_capture(db, note_id, "no voice attachment found")
                return

    elif cap_type == "link" or (cap_type == "mixed" and links):
        result = synthesize_link(text, links)

    elif cap_type == "image":
        # No vision model yet — store metadata + accompanying text
        img_info = []
        for a in attachments:
            if a.get("content_type", "").startswith("image/"):
                img_info.append(f"[Image: {a['filename']}, {a.get('size_bytes', 0)} bytes]")
        combined = text
        if img_info:
            combined = f"{text}\n{'  '.join(img_info)}" if text else "\n".join(img_info)
        if combined.strip():
            result = synthesize_text(combined)
        else:
            resolve_capture(db, note_id, "empty image capture")
            return

    elif cap_type == "mixed":
        # Combine all parts
        parts = []
        if text:
            parts.append(text)
        for link in links:
            fetched = fetch_url_text(link)
            if fetched:
                parts.append(f"[From {link}]: {fetched}")
        for a in attachments:
            ct = a.get("content_type", "")
            if ct.startswith("image/"):
                parts.append(f"[Image: {a['filename']}]")
            elif ct.startswith("audio/"):
                transcript = transcribe_voice(a["local_path"])
                if transcript:
                    parts.append(f"[Voice]: {transcript}")
        combined = "\n".join(parts)
        result = synthesize_text(combined) if combined.strip() else None
        if not result:
            resolve_capture(db, note_id, "empty mixed capture")
            return

    else:  # text (default)
        if not text.strip():
            resolve_capture(db, note_id, "empty text capture")
            return
        result = synthesize_text(text)

    # Post to canister
    content = result["content"]
    topic = result.get("topic")
    keywords = result.get("keywords", [])

    # Add hash to content for dedup tracking
    content_with_hash = f"{content}\n[capture:{chash}]"

    success = post_to_canister(content_with_hash, topic, keywords, [author])

    if success:
        resolve_capture(db, note_id)
        cleanup_attachments(attachments)
    else:
        log.error("Failed to post capture #%d to canister", note_id)
        # Don't resolve — retry next cycle


def cleanup_attachments(attachments):
    """Remove downloaded attachment files after successful processing."""
    for a in attachments:
        path = a.get("local_path", "")
        if path and os.path.exists(path):
            try:
                os.remove(path)
                log.info("Cleaned up %s", path)
            except OSError as e:
                log.warning("Failed to clean up %s: %s", path, e)


# ── Entry Point ────────────────────────────────────────────────

def main():
    if not os.path.exists(DB_PATH):
        log.error("Database not found: %s", DB_PATH)
        sys.exit(1)

    db = sqlite3.connect(DB_PATH)

    captures = get_pending_captures(db)
    if not captures:
        log.info("No pending captures")
        db.close()
        return

    log.info("Found %d pending capture(s)", len(captures))

    for note_id, raw_content, created_at in captures:
        # Check failure count
        failures = get_failure_count(db, note_id)
        if failures >= MAX_FAILURES:
            log.warning("Capture #%d exceeded max failures (%d), marking resolved", note_id, failures)
            resolve_capture(db, note_id, f"exceeded {MAX_FAILURES} retries")
            continue

        try:
            process_capture(db, note_id, raw_content)
        except Exception as e:
            log.error("Error processing capture #%d: %s", note_id, e)
            resolve_capture(db, note_id, str(e)[:100])

    db.close()
    log.info("Capture processing complete")


if __name__ == "__main__":
    main()
