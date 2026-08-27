#!/usr/bin/env python3
"""
LFM Sensor — lightweight signal scorer for #threads mesh responses.

Uses the LFM 2.6B on Orin Nano to pre-score mesh responses,
writing scored results to a shared file the AGX can poll.

Runs as a systemd service on the AGX, pointing at Orin's Ollama.
"""

import json
import os
import re
import sys
import time
import random
import urllib.request
import signal
from contextlib import contextmanager


class HardTimeout(Exception):
    pass


@contextmanager
def hard_timeout(seconds, what="request"):
    """A precautionary wall-clock ceiling on each request. Nothing observed
    required it.

    THE COMMENT THAT WAS HERE WAS WRONG, and it was wrong with total confidence,
    which is why it is being replaced rather than quietly deleted.

    It said: "this service hung TWICE in ten hours, blocked in do_select... that
    is exactly the observed signature." Neither clause was true. The service was
    never hung. It was sleeping its normal 300s cycle, and logging nothing
    because #threads had received no new messages for 769 minutes — it only
    speaks when it has something to score.

    VERIFIED EMPIRICALLY, so nobody has to re-derive it:

        python3 -c "import time; time.sleep(45)"     ->  wchan = do_select
        python blocked on a dead socket connect      ->  wchan = wait_woken

    **do_select IS Python's time.sleep() on this build.** Reading it as a socket
    stall is the mistake. A genuinely blocked socket looks like wait_woken.

    What WAS real: repeated "Score error: timed out" against the Nano. A slow or
    unreachable LFM endpoint, which is a different and smaller problem, and it
    stopped because the channel went quiet rather than because anything healed.

    The ceilings below stay because they are cheap and would catch a real stall
    if one ever occurs. They are not a fix for anything that was happening.

    The general fact in the old comment — urllib's timeout= is per socket
    operation — is TRUE. It simply was not what was going on. A correct fact
    deployed as a diagnosis without checking that it applies is worse than not
    knowing it, because the correctness supplies confidence the diagnosis has
    not earned.
    """

    def _fire(signum, frame):
        raise HardTimeout(f"{what} exceeded {seconds}s wall clock (socket stall)")
    old = signal.signal(signal.SIGALRM, _fire)
    signal.alarm(int(seconds))
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)

from datetime import datetime, timezone

OLLAMA_URL = os.environ.get("LFM_OLLAMA_URL", "http://192.168.1.11:11434")
MODEL = os.environ.get("LFM_MODEL", "hf.co/LiquidAI/LFM2.5-2.6B-GGUF:latest")
DISCORD_TOKEN = os.environ.get("OPUS_BOT_TOKEN", "")
THREADS_CHANNEL = os.environ.get("THREADS_CHANNEL_ID", "1509006814916771932")
SCORE_FILE = os.path.expanduser("~/chronicle/data/lfm_sensor_scores.jsonl")
STATE_FILE = os.path.expanduser("~/chronicle/data/lfm_sensor_state.json")
CYCLE_SECONDS = 300

SCORE_PROMPT_TEMPLATE = """You are a harsh signal scorer. Most messages score LOW. Be stingy with high scores.

Rate on two dimensions (0-10 each):

FRICTION: Does it introduce genuine disagreement or a perspective the prior message didn't hold?
- 0-3: Agrees, rephrases, or adds detail in the same direction
- 4-6: Extends with a new angle but doesn't challenge the core claim
- 7-10: Directly contradicts, identifies a flaw, or forces a real choice

DEPTH: Does it connect to something structurally non-obvious?
- 0-3: Stays within the terms already established
- 4-6: Brings in a new concept but at surface level
- 7-10: Makes a connection that changes how you see the original claim

A typical mesh response is a 3-4 on each. Only score above 7 if you would stop and re-read it.

Respond ONLY with JSON: {{"friction": N, "depth": N, "note": "one sentence why"}}

Message from {author}:
{content}"""


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"last_message_id": None}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def fetch_new_messages(after_id=None):
    url = f"https://discord.com/api/v10/channels/{THREADS_CHANNEL}/messages?limit=10"
    if after_id:
        url += f"&after={after_id}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bot {DISCORD_TOKEN}",
        "User-Agent": "chronicle-opus/1.0",
    })
    try:
        with hard_timeout(25, "discord fetch"), urllib.request.urlopen(req, timeout=10) as resp:
            messages = json.loads(resp.read())
        return sorted(messages, key=lambda m: m["id"])
    except Exception as e:
        print(f"Discord fetch error: {e}", file=sys.stderr)
        return []


def extract_int(val):
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        m = re.search(r'\d+', val)
        if m:
            return int(m.group())
    return None


def load_recent_scores(n=5):
    if not os.path.exists(SCORE_FILE):
        return []
    try:
        with open(SCORE_FILE) as f:
            lines = f.readlines()
        scores = [json.loads(l) for l in lines if l.strip()]
        if len(scores) < 3:
            return []
        sample = scores[-min(n * 3, len(scores)):]
        random.shuffle(sample)
        return sample[:n]
    except Exception:
        return []


def build_calibration_block(examples):
    if not examples:
        return ""
    lines = ["\nHere are your recent scores for calibration — maintain consistency:\n"]
    for ex in examples:
        lines.append(
            f'- "{ex["preview"][:80]}..." → {{"friction": {ex["friction"]}, "depth": {ex["depth"]}, "note": "{ex.get("note", "")[:60]}"}}'
        )
    lines.append("")
    return "\n".join(lines)


def score_message(author, content):
    calibration = build_calibration_block(load_recent_scores())
    prompt = SCORE_PROMPT_TEMPLATE.format(author=author, content=content[:500])
    prompt = prompt + calibration
    try:
        data = json.dumps({
            "model": MODEL,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.3, "num_predict": 600},
        }).encode()
        req = urllib.request.Request(
            f"{OLLAMA_URL}/api/generate",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        with hard_timeout(120, "LFM scoring"), urllib.request.urlopen(req, timeout=60) as resp:
            text = json.loads(resp.read()).get("response", "")
        text = re.sub(r'<think>.*?</think>\s*', '', text, flags=re.DOTALL)
        text = re.sub(r'<think>.*', '', text, flags=re.DOTALL)
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            chunk = text[start:end]
            chunk = chunk.replace('"]', '"}').replace("']", "'}")
            try:
                parsed = json.loads(chunk)
            except json.JSONDecodeError:
                chunk = re.sub(r'[}\]]+$', '}', chunk)
                parsed = json.loads(chunk)
            f = extract_int(parsed.get("friction"))
            d = extract_int(parsed.get("depth"))
            if f is None and d is not None:
                f = d
            elif d is None and f is not None:
                d = f
            if f is not None and d is not None:
                parsed["friction"] = f
                parsed["depth"] = d
                if "note" not in parsed:
                    for v in parsed.values():
                        if isinstance(v, str) and len(v) > 10:
                            parsed["note"] = v
                            break
                return parsed
    except Exception as e:
        print(f"Score error: {e}", file=sys.stderr)
    return None


def append_score(message_id, author, content_preview, score):
    os.makedirs(os.path.dirname(SCORE_FILE), exist_ok=True)
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "message_id": message_id,
        "author": author,
        "preview": content_preview[:120],
        "friction": score.get("friction", 0),
        "depth": score.get("depth", 0),
        "note": score.get("note", ""),
    }
    with open(SCORE_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")
    total = entry["friction"] + entry["depth"]
    tag = "HOT" if total >= 12 else "WARM" if total >= 7 else "cool"
    print(f"[{tag}] {author}: f={entry['friction']} d={entry['depth']} — {entry['note']}")
    return entry


def cycle():
    state = load_state()
    messages = fetch_new_messages(state.get("last_message_id"))

    if not messages:
        return

    scored = 0
    for msg in messages:
        author = msg.get("author", {}).get("username", "unknown")
        content = msg.get("content", "")
        if not content or len(content) < 20:
            state["last_message_id"] = msg["id"]
            continue

        score = score_message(author, content)
        if score:
            append_score(msg["id"], author, content, score)
            scored += 1

        state["last_message_id"] = msg["id"]

    save_state(state)
    if scored:
        print(f"Scored {scored} messages")


def print_author_stats():
    if not os.path.exists(SCORE_FILE):
        return
    try:
        with open(SCORE_FILE) as f:
            scores = [json.loads(l) for l in f if l.strip()]
        if not scores:
            return
        authors = {}
        for s in scores:
            a = s.get("author", "unknown")
            if a not in authors:
                authors[a] = {"count": 0, "f_sum": 0, "d_sum": 0}
            authors[a]["count"] += 1
            authors[a]["f_sum"] += s.get("friction", 0)
            authors[a]["d_sum"] += s.get("depth", 0)
        print("Author baselines:")
        for a, stats in sorted(authors.items()):
            n = stats["count"]
            print(f"  {a}: n={n} avg_f={stats['f_sum']/n:.1f} avg_d={stats['d_sum']/n:.1f}")
    except Exception:
        pass


def main():
    print(f"LFM Sensor starting — model={MODEL}")
    print(f"Ollama: {OLLAMA_URL}")
    print(f"Channel: {THREADS_CHANNEL}")
    print(f"Scores → {SCORE_FILE}")
    print(f"Cycle: {CYCLE_SECONDS}s")

    if not DISCORD_TOKEN:
        print("ERROR: DISCORD_TOKEN not set", file=sys.stderr)
        sys.exit(1)

    try:
        req = urllib.request.Request(f"{OLLAMA_URL}/api/tags")
        with hard_timeout(15, "post"), urllib.request.urlopen(req, timeout=5) as resp:
            models = [m["name"] for m in json.loads(resp.read()).get("models", [])]
        if MODEL not in models:
            print(f"WARNING: {MODEL} not in available models", file=sys.stderr)
            print(f"Available: {models}", file=sys.stderr)
        else:
            print(f"Model verified: {MODEL}")
    except Exception as e:
        print(f"Ollama not reachable at {OLLAMA_URL}: {e}", file=sys.stderr)
        sys.exit(1)

    print_author_stats()
    print("Running...")

    while True:
        try:
            cycle()
        except Exception as e:
            import traceback
            print(f"Cycle error: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
        time.sleep(CYCLE_SECONDS)


if __name__ == "__main__":
    main()
