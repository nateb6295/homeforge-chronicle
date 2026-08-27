#!/usr/bin/env python3
"""LFM Capture Scorer — signal density reading before analysis.

Runs capture content through LFM on the Orin Nano to give a temperature
hint. Not triage (we engage all), but a pre-read signal.

Usage:
    python3 lfm_capture_score.py "tweet text here"
    echo "tweet text" | python3 lfm_capture_score.py
    python3 lfm_capture_score.py --tweet-id 2074596637893181868
    python3 lfm_capture_score.py --batch  # scores all pending captures
"""

import json
import os
import re
import subprocess
import sys
import time

ORIN_OLLAMA = "http://192.168.1.11:11434"
MODEL = "hf.co/LiquidAI/LFM2.5-2.6B-GGUF:latest"
SCORE_LOG = os.path.expanduser("~/chronicle/data/capture_scores.jsonl")

CAPTURE_SCORE_PROMPT = """You are a signal density scorer for research captures. Score HARSHLY — most content is average.

Rate on two dimensions (0-10 each):

DENSITY: How conceptually packed is this content?
- 0-3: Surface opinion, reaction, or hot take with no supporting structure
- 4-6: Makes a real claim with some reasoning but stays in familiar territory
- 7-10: Multiple connected ideas, empirical claims, or novel framing packed tight

NOVELTY: How much new ground does this break?
- 0-3: Restates what's already well-known in the space
- 4-6: Familiar topic but with a new angle or unexpected connection
- 7-10: Something you haven't seen framed this way before — genuinely surprising

Most tweets are 3-4 on each. Only score above 7 if it would make you stop scrolling.

Respond ONLY with JSON: {{"density": N, "novelty": N, "note": "one sentence why"}}

Content:
{content}"""


def score_content(text):
    """Send text to LFM on Orin, return score dict or None."""
    import urllib.request
    prompt = CAPTURE_SCORE_PROMPT.format(content=text[:800])
    try:
        data = json.dumps({"model": MODEL, "prompt": prompt, "stream": False, "options": {"temperature": 0.3, "num_predict": 100}}).encode()
        req = urllib.request.Request(
            f"{ORIN_OLLAMA}/api/generate",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = json.loads(resp.read()).get("response", "")
        if os.environ.get("LFM_DEBUG"):
            print(f"LFM raw: {repr(raw[:200])}", file=sys.stderr)
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start >= 0 and end > start:
            chunk = raw[start:end]
            chunk = chunk.replace('"]', '"}').replace("']", "'}")
            try:
                parsed = json.loads(chunk)
            except json.JSONDecodeError:
                chunk = re.sub(r'[}\]]+$', '}', chunk)
                parsed = json.loads(chunk)
            def extract_int(val):
                if isinstance(val, (int, float)):
                    return int(val)
                if isinstance(val, str):
                    m = re.search(r'\d+', val)
                    if m:
                        return int(m.group())
                return None
            d = extract_int(parsed.get("density"))
            n = extract_int(parsed.get("novelty"))
            if d is None and n is not None:
                d = n
            elif n is None and d is not None:
                n = d
            if d is not None and n is not None:
                parsed["density"] = d
                parsed["novelty"] = n
                if "note" not in parsed:
                    for v in parsed.values():
                        if isinstance(v, str) and len(v) > 10:
                            parsed["note"] = v
                            break
                return parsed
    except Exception as e:
        import traceback
        print(f"LFM score error: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
    return None


def tag_temperature(score):
    """Return temperature tag from score."""
    total = int(score.get("density", 0)) + int(score.get("novelty", 0))
    if total >= 14:
        return "FIRE"
    elif total >= 10:
        return "HOT"
    elif total >= 7:
        return "WARM"
    else:
        return "cool"


def format_score(score, content_preview=""):
    """Format score for display."""
    tag = tag_temperature(score)
    d = int(score.get("density", 0))
    n = int(score.get("novelty", 0))
    note = score.get("note", "")
    return f"[{tag}] d={d} n={n} — {note}"


def log_score(tweet_id, author, score, content_preview=""):
    """Append score to persistent log."""
    os.makedirs(os.path.dirname(SCORE_LOG), exist_ok=True)
    entry = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "tweet_id": tweet_id or "stdin",
        "author": author or "unknown",
        "preview": content_preview[:120],
        "density": score.get("density", 0),
        "novelty": score.get("novelty", 0),
        "note": score.get("note", ""),
        "tag": tag_temperature(score),
    }
    with open(SCORE_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")


def fetch_tweet(tweet_id):
    """Fetch tweet content via tweet_fetch.py."""
    env_file = os.path.expanduser("~/chronicle/chronicle.env")
    extra_env = {}
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    extra_env[k] = v.strip().strip('"').strip("'")
    try:
        r = subprocess.run(
            ["python3", os.path.join(os.path.dirname(__file__), "tweet_fetch.py"), tweet_id],
            capture_output=True, text=True, timeout=30,
            env={**os.environ, **extra_env}
        )
        if r.returncode == 0 and r.stdout.strip():
            tweets = json.loads(r.stdout.strip())
            if tweets and isinstance(tweets, list):
                parts = []
                for t in tweets:
                    parts.append(f"@{t.get('author','?')}: {t.get('text','')}")
                    if t.get("quoted"):
                        parts.append(f"(quoting @{t['quoted'].get('author','?')}: {t['quoted'].get('text','')})")
                return "\n".join(parts)
            return r.stdout.strip()
    except Exception:
        pass
    return None


def score_pending_batch():
    """Score all pending captures and print results."""
    tracker = os.path.join(os.path.dirname(__file__), "capture_tracker.py")
    env_file = os.path.expanduser("~/chronicle/chronicle.env")
    extra_env = {}
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    extra_env[k] = v.strip().strip('"').strip("'")

    r = subprocess.run(
        ["python3", tracker, "pending"],
        capture_output=True, text=True, timeout=15,
        env={**os.environ, **extra_env}
    )
    if "No unprocessed" in r.stdout:
        print("No pending captures to score.")
        return

    lines = r.stdout.strip().split("\n")
    for line in lines[1:]:
        match = re.search(r'tweet:(\d+)', line)
        if not match:
            match = re.search(r'(?:substack|url|arxiv):(\S+)', line)
        if not match:
            continue
        cap_id = match.group(1) if match.group(0).startswith("tweet") else match.group(0)
        author_match = re.search(r'@(\w+)', line)
        author = author_match.group(1) if author_match else "unknown"

        if cap_id.isdigit():
            content = fetch_tweet(cap_id)
        else:
            content = None

        if not content:
            print(f"  @{author} {cap_id}: [skip — no content fetched]")
            continue

        score = score_content(content)
        if score:
            log_score(cap_id, author, score, content[:120])
            print(f"  @{author} {cap_id}: {format_score(score, content[:80])}")
        else:
            print(f"  @{author} {cap_id}: [LFM unavailable]")


def main():
    if "--batch" in sys.argv:
        score_pending_batch()
        return

    if "--tweet-id" in sys.argv:
        idx = sys.argv.index("--tweet-id")
        if idx + 1 >= len(sys.argv):
            print("Usage: lfm_capture_score.py --tweet-id <id>")
            return
        tweet_id = sys.argv[idx + 1]
        content = fetch_tweet(tweet_id)
        if not content:
            print(f"Could not fetch tweet {tweet_id}")
            return
        score = score_content(content)
        if score:
            log_score(tweet_id, "", score, content[:120])
            print(format_score(score, content[:80]))
            print(json.dumps(score))
        else:
            print("LFM scoring failed")
        return

    if "--json" in sys.argv:
        json_mode = True
        sys.argv.remove("--json")
    else:
        json_mode = False

    if len(sys.argv) > 1 and not sys.argv[1].startswith("-"):
        content = " ".join(sys.argv[1:])
    else:
        content = sys.stdin.read().strip()

    if not content:
        print("No content to score. Pass as argument or pipe to stdin.")
        return

    score = score_content(content)
    if score:
        log_score(None, "", score, content[:120])
        if json_mode:
            score["tag"] = tag_temperature(score)
            print(json.dumps(score))
        else:
            print(format_score(score, content[:80]))
    else:
        print("LFM scoring failed — is Orin reachable?")


if __name__ == "__main__":
    main()
