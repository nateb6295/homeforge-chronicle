#!/usr/bin/env python3
"""Crosschain Watcher — sandboxed ICP<->XRP signal feed.

Runs on a systemd user timer (default every 2h). Calls the collector at
~/.hermes/scripts/crosschain_collector.py, filters to items with actual
cross-chain matches, formats a concise digest, and posts to CROSSCHAIN_WEBHOOK.

v1 = raw-signal mode (no LLM synthesis). Rate-limited to the schedule cadence;
caps post length at Discord's 2000-char limit; skips entirely if nothing new.

Full sandbox: own log at ~/chronicle/crosschain_watch.log. Does not write to
processed.db. If anything fails, it fails silently — won't cascade.
"""
import os
import re
import sys
import subprocess
import time
from datetime import datetime, timezone

import requests


LOG_PATH = os.path.expanduser("~/chronicle/crosschain_watch.log")
COLLECTOR = os.path.expanduser("~/.hermes/scripts/crosschain_collector.py")
ENV_PATH = os.path.expanduser("~/chronicle/chronicle.env")
STATE_PATH = os.path.expanduser("~/chronicle/crosschain_watch_state.json")

MAX_POSTS_PER_DAY = 15  # hard ceiling per Nate
MAX_ITEMS_PER_DIGEST = 5
DISCORD_MAX = 1900  # leave headroom under 2000


def _log(msg):
    ts = datetime.now().isoformat(timespec="seconds")
    try:
        with open(LOG_PATH, "a") as f:
            f.write(f"[{ts}] {msg}\n")
    except Exception:
        pass


def _load_env():
    env = {}
    try:
        with open(ENV_PATH) as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip()
    except Exception:
        pass
    return env


def _load_state():
    import json
    try:
        with open(STATE_PATH) as f:
            return json.load(f)
    except Exception:
        return {"posts_today": 0, "day": "", "seen_links": []}


def _save_state(state):
    import json
    try:
        with open(STATE_PATH, "w") as f:
            json.dump(state, f)
    except Exception:
        pass


def _check_daily_ceiling(state):
    today = datetime.now().strftime("%Y-%m-%d")
    if state.get("day") != today:
        state["day"] = today
        state["posts_today"] = 0
    return state["posts_today"] < MAX_POSTS_PER_DAY


def _run_collector():
    try:
        r = subprocess.run(
            ["python3", COLLECTOR],
            capture_output=True, text=True, timeout=60,
        )
        return r.stdout
    except Exception as e:
        _log(f"collector error: {e}")
        return ""


def _parse_collector_output(raw):
    """Parse collector output into structured items.

    The collector emits blocks separated by blank lines, each starting with
    `--- [Source] [CROSS: terms] ---`. We keep items that have a CROSS tag.
    """
    items = []
    current = None
    for line in raw.split("\n"):
        if line.startswith("--- [") and line.endswith("---"):
            if current:
                items.append(current)
            # Parse header
            m = re.match(r"--- \[([^\]]+)\](?: \[CROSS: ([^\]]+)\])? ---", line)
            if not m:
                current = None
                continue
            source = m.group(1)
            cross = m.group(2) or ""
            current = {"source": source, "cross": cross, "body": []}
        elif current is not None:
            current["body"].append(line)
    if current:
        items.append(current)
    # Only keep items with cross-chain hits
    tagged = [i for i in items if i["cross"]]
    return _dedup_by_title(tagged)


def _title_tokens(text):
    """Normalize title to lowercase stemmed tokens for fuzzy matching."""
    text = re.sub(r"\s*[-–—]\s+.+$", "", text)  # strip trailing source name
    words = re.findall(r"[a-z]{3,}", text.lower())
    # Minimal stemming: strip common suffixes
    stemmed = set()
    for w in words:
        for sfx in ("ing", "tion", "ment", "ness", "ens", "ers", "ed", "es", "ly", "er", "al", "s"):
            if len(w) > len(sfx) + 2 and w.endswith(sfx):
                w = w[:-len(sfx)]
                break
        stemmed.add(w)
    return stemmed


def _dedup_by_title(items, threshold=0.4):
    """Collapse items about the same story into one (keep first seen).

    Two items are considered duplicates if their title-token Jaccard
    exceeds the threshold. This catches the same event covered by
    multiple outlets with slightly different headlines.
    """
    kept = []
    kept_tokens = []
    for it in items:
        title = it["body"][0] if it["body"] else ""
        toks = _title_tokens(title)
        if not toks:
            kept.append(it)
            continue
        dupe = False
        for prev in kept_tokens:
            overlap = len(toks & prev) / max(len(toks | prev), 1)
            if overlap >= 0.4:
                dupe = True
                break
        if not dupe:
            kept.append(it)
            kept_tokens.append(toks)
    return kept


def _format_digest(items, total_seen):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"🔗 **Crosschain Watch — {now}**", ""]
    if not items:
        return None  # Nothing cross-chain; skip post entirely

    lines.append(f"{len(items)} cross-chain signals in last 2h (from {total_seen} items scanned):")
    lines.append("")
    shown = 0
    for it in items[:MAX_ITEMS_PER_DIGEST]:
        body = [l for l in it["body"] if l.strip() and not l.startswith("(posted:")]
        if not body:
            continue
        title = body[0]
        link = ""
        summary = ""
        for l in body[1:]:
            if l.startswith("http"):
                link = l
            elif l.strip():
                summary = l[:220]
                break
        tag = it["cross"]
        block = f"**[{it['source']}]** `{tag}`\n{title}"
        if link:
            block += f"\n<{link}>"
        if summary and summary != title:
            block += f"\n_{summary}_"
        lines.append(block)
        lines.append("")
        shown += 1

    if len(items) > MAX_ITEMS_PER_DIGEST:
        lines.append(f"_(+{len(items) - MAX_ITEMS_PER_DIGEST} more cross-chain items this cycle)_")

    out = "\n".join(lines)
    if len(out) > DISCORD_MAX:
        out = out[:DISCORD_MAX - 20] + "\n_[truncated]_"
    return out


def _post(webhook, content):
    try:
        r = requests.post(
            webhook,
            json={"content": content},
            timeout=15,
        )
        return r.status_code in (200, 204)
    except Exception as e:
        _log(f"post error: {e}")
        return False


def main():
    env = _load_env()
    webhook = env.get("CROSSCHAIN_WEBHOOK") or os.environ.get("CROSSCHAIN_WEBHOOK")
    if not webhook:
        _log("no CROSSCHAIN_WEBHOOK set, bailing")
        return

    state = _load_state()
    if not _check_daily_ceiling(state):
        _log(f"daily ceiling {MAX_POSTS_PER_DAY} reached, skipping")
        return

    raw = _run_collector()
    if not raw.strip():
        _log("collector empty")
        return

    # Count total items scanned (for context)
    total = raw.count("--- [")
    items = _parse_collector_output(raw)

    # Dedupe against recent links to avoid reposting
    seen = set(state.get("seen_links", [])[-200:])
    fresh = []
    new_links = []
    for it in items:
        link = ""
        for l in it["body"]:
            if l.startswith("http"):
                link = l
                break
        if link and link in seen:
            continue
        fresh.append(it)
        if link:
            new_links.append(link)

    digest = _format_digest(fresh, total)
    if not digest:
        _log(f"no fresh cross-chain items ({total} scanned, {len(items)} cross-tagged)")
        return

    ok = _post(webhook, digest)
    if ok:
        state["posts_today"] = state.get("posts_today", 0) + 1
        state["seen_links"] = (state.get("seen_links", []) + new_links)[-500:]
        _save_state(state)
        _log(f"posted digest ({len(fresh)} items, {len(digest)} chars) — {state['posts_today']}/{MAX_POSTS_PER_DAY} today")
    else:
        _log("post failed")

    # Auto-extract adoption graph edges from the raw collector output.
    # Runs after the post so a watcher failure doesn't block graph accrual,
    # and a graph failure doesn't block the post. Best-effort, logged only.
    try:
        extractor = os.path.expanduser("~/chronicle/bin/adoption_extractor.py")
        if os.path.exists(extractor):
            r = subprocess.run(
                ["python3", extractor],
                input=raw, capture_output=True, text=True, timeout=60,
            )
            first_line = (r.stdout or "").splitlines()[:1]
            if first_line:
                _log(f"extractor: {first_line[0]}")
            elif r.stderr:
                _log(f"extractor stderr: {r.stderr.strip()[:200]}")
    except Exception as e:
        _log(f"extractor error: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
