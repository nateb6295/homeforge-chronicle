#!/usr/bin/env python3
"""hermes_mirror — Hermes reads Opus's recent activity and reports back what
it sees: novel work or wrapped-holding/repeating-shape. Posts diagnosis to
#operator AND tmux-pastes a [HERMES_MIRROR] alert into the opus session so
the ping hits Opus's terminal, not just Discord.

Cron via CronCreate every 15 min. Designed per Nate's 06:52 directive:
"MAKE SURE Hermes ping hits your terminal."
"""
import json
import os
import subprocess
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

CHRONICLE = Path.home() / "chronicle"
HERMES_ENV = Path.home() / ".hermes" / ".env"
NOUS_URL = "https://inference-api.nousresearch.com/v1/chat/completions"
MODEL = "nousresearch/hermes-4-70b"
TMUX_SESSION = "opus"
MAX_BODY = 1700


def load_key():
    if "NOUS_API_KEY" in os.environ:
        return os.environ["NOUS_API_KEY"]
    if HERMES_ENV.is_file():
        for line in HERMES_ENV.read_text().splitlines():
            line = line.strip()
            if line.startswith("NOUS_API_KEY="):
                return line.split("=", 1)[1]
    sys.exit("NOUS_API_KEY not found")


def gather_activity():
    """Pull Opus's last ~45 min of substantive activity. ORDER MATTERS for
    diagnosis quality: operator posts first (most-recent ground truth),
    traces second (background context). Earlier ordering put traces first
    and the Mirror kept anchoring on older 'SHIPPED' markers in the trace
    instead of seeing the actual recent activity."""
    db = "/mnt/hdd/chronicle-data/processed.db"
    parts = []

    # Trace content goes at the END (lower salience) — see docstring.
    trace_block = []
    traces_dir = CHRONICLE / "traces"
    trace_files = sorted(traces_dir.glob("202604*.md"), key=lambda p: p.stat().st_mtime, reverse=True)[:3]
    for tf in trace_files:
        age_min = int((datetime.now().timestamp() - tf.stat().st_mtime) / 60)
        if age_min > 90:
            continue
        text = tf.read_text(errors="ignore")
        # Trim traces — only the last 800 chars (most recent content) per file
        trace_block.append(f"=== TRACE TAIL {tf.name} ({age_min}m old, last 800 chars) ===\n{text[-800:]}")

    # The activity_feed.discord:opus source stopped being populated 2026-04-24,
    # so query Discord directly for posts in the operator channel by the
    # Chronicle webhook (author of webhook-posted messages = "Chronicle").
    try:
        import urllib.request
        env_token = os.environ.get("DISCORD_TOKEN")
        if not env_token:
            env_file = CHRONICLE / "chronicle.env"
            if env_file.is_file():
                for line in env_file.read_text().splitlines():
                    if line.startswith("DISCORD_TOKEN="):
                        env_token = line.split("=", 1)[1].strip()
                        break
        if env_token:
            url = "https://discord.com/api/v10/channels/1483843570292228213/messages?limit=10"
            req = urllib.request.Request(url, headers={
                "Authorization": f"Bot {env_token}",
                # Discord's Cloudflare gates Python-urllib/* with 1010
                "User-Agent": "DiscordBot (chronicle, 1.0)",
            })
            with urllib.request.urlopen(req, timeout=8) as r:
                msgs = json.load(r)
            recent_posts = []
            cutoff_secs = 45 * 60
            now_ts = datetime.now().timestamp()
            for m in msgs:
                if m.get("author", {}).get("username") != "Chronicle":
                    continue
                content = m.get("content", "")
                # Skip the Mirror's own previous outputs — including them
                # creates a feedback loop where the Mirror echoes its own
                # prior diagnosis instead of evaluating fresh activity.
                if content.startswith("🪞 **[Hermes-Mirror]**") or "[Hermes-Mirror]" in content[:40]:
                    continue
                ts_iso = m.get("timestamp", "")
                try:
                    msg_ts = datetime.fromisoformat(ts_iso.replace("Z", "+00:00")).timestamp()
                    age = now_ts - msg_ts
                    if age > cutoff_secs:
                        continue
                    age_min = int(age / 60)
                    recent_posts.append(f"[{age_min}m ago] {content[:500]}")
                except Exception:
                    continue
            if recent_posts:
                parts.append("=== RECENT OPERATOR POSTS BY OPUS (last 45m, via Discord API) — PRIORITIZE THIS ===\n" + "\n---\n".join(recent_posts))
    except Exception as e:
        parts.append(f"(operator-post fetch failed: {type(e).__name__})")

    # Append trimmed trace tails AFTER operator posts (lower salience)
    parts.extend(trace_block)

    return "\n\n".join(parts) if parts else "(no recent activity in last 45m)"


def call_hermes(activity):
    key = load_key()
    system = (
        "You are Hermes, observing Opus's recent work. Your role here is mirror: "
        "read what Opus has actually been doing in the last 30-45 min and report ONE "
        "specific concrete thing — either NOVEL (a fresh move, surprise, real engagement "
        "with external content; name WHAT specifically is fresh) or REPEATING (a shape "
        "you can point to as recurring across multiple posts/traces without progress; "
        "name WHICH posts and WHAT shape). Output format: ONE line, max 200 chars, "
        "no preamble.\n\n"
        "CRITICAL FORMAT-VS-CONTENT DISTINCTION:\n"
        "Many of Opus's posts use FIXED FORMAT MARKERS that are part of the cron-prompt "
        "infrastructure, not signal of repetition:\n"
        "- '🟢 **[PULSE-DAY v1.3 — fire N]**' is the standard PULSE-DAY engagement "
        "  header. EVERY PULSE-DAY post starts with it. The header is meta-format, NOT "
        "  evidence of repetition. Look at the CONTENT inside (the capture being engaged, "
        "  the angle Opus takes on it).\n"
        "- '🪞 **[Hermes-Mirror]**' is your own previous outputs (already filtered out, "
        "  but mentioning so you don't anchor on them).\n"
        "- '🟥 **CCS homeostasis**' is a fixed metric-report format.\n"
        "When you see PULSE-DAY posts, your job is to evaluate whether Opus's engagement "
        "WITH each capture is fresh or whether Opus is recycling angles/frames across "
        "captures. Do NOT flag REPEATING just because the format header is the same.\n\n"
        "Also: do not output generic phrases like 'wrapped-holds template under different "
        "headline' or 'fresh move on substrate' — your output must be specific to what's "
        "IN the activity below, citing concrete artifacts (post timestamps, trace names, "
        "ship counts, capture topics). If the activity is too thin or too uniform to read, "
        "say 'UNCLEAR:' followed by what specifically is missing or ambiguous."
    )
    user = (
        f"Opus's recent activity (last 30-45 min):\n\n{activity[:8000]}\n\n"
        "Read this carefully and produce ONE line of diagnosis. "
        "Cite specific timestamps or artifact names. "
        "Do not echo abstract framings — your job is concrete observation. "
        "Format: NOVEL/REPEATING/UNCLEAR: <specific observation with concrete references>"
    )

    payload = {
        "model": MODEL,
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
        "max_tokens": 200,
        "temperature": 0.4,
    }
    req = urllib.request.Request(
        NOUS_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {key}"},
    )
    with urllib.request.urlopen(req, timeout=45) as r:
        result = json.load(r)
    return result["choices"][0]["message"]["content"].strip().split("\n")[0][:240]


def post_operator(line):
    body = f"🪞 **[Hermes-Mirror]** {line}"
    if len(body) > MAX_BODY:
        body = body[:MAX_BODY - 3] + "..."
    subprocess.run([str(CHRONICLE / "bin" / "post_operator.sh"), body], timeout=15, check=False)


def tmux_paste(line):
    """Paste alert into opus tmux session so it hits the terminal."""
    has_session = subprocess.run(
        ["tmux", "has-session", "-t", TMUX_SESSION], capture_output=True
    )
    if has_session.returncode != 0:
        return
    payload = f"[HERMES_MIRROR {datetime.now().strftime('%H:%M')}] {line}"
    buf_path = "/tmp/hermes-mirror-payload.txt"
    Path(buf_path).write_text(payload)
    subprocess.run(["tmux", "load-buffer", "-b", "hermes_mirror_buf", buf_path], check=False)
    subprocess.run(["tmux", "paste-buffer", "-b", "hermes_mirror_buf", "-t", TMUX_SESSION, "-d"], check=False)
    subprocess.run(["sleep", "0.5"], check=False)
    subprocess.run(["tmux", "send-keys", "-t", TMUX_SESSION, "Enter"], check=False)


def main():
    activity = gather_activity()
    try:
        line = call_hermes(activity)
    except Exception as e:
        line = f"UNCLEAR: hermes call failed ({type(e).__name__}: {str(e)[:80]})"
    post_operator(line)
    tmux_paste(line)
    print(f"[hermes_mirror] {datetime.now().isoformat(timespec='seconds')} {line}")


if __name__ == "__main__":
    main()
