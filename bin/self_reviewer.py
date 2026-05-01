#!/usr/bin/env python3
"""
Adversarial self-reviewer — red-teams my own outputs before they ship.

Uses Groq qwen3-32b as an outside reviewer that receives a post I wrote
plus recent context and returns:
  - category of post (finding, conversation, rundown, proposal, heartbeat)
  - 0-10 scores on: overclaim, category_confusion, noise_as_signal, vagueness
  - specific flags ("you used 'amplifies' when data shows 'stays stable'")
  - should_ship verdict: green / yellow / red + reason

Not a gate (I can still ship if I decide to), but a voice in the loop that
catches the kind of mistakes I made this morning and this evening before
they're public. The red-teamer is the mechanizable part of what Nate does
relationally.

Usage:
  echo "my post text" | python3 self_reviewer.py
  python3 self_reviewer.py --text "my post text"
  python3 self_reviewer.py --file /path/to/post.txt

Exit codes: 0 = green, 2 = yellow (ship with caution), 3 = red (revise first)
"""
import argparse
import json
import os
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from asving_probe import _load_env  # noqa
_load_env()

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"


REVIEWER_SYSTEM = (
    "You are an adversarial reviewer checking a post from an AI agent "
    "(Opus, in a persistent session) before it ships to its partner Nate. "
    "Your job: catch overclaim, category confusion, and noise-as-signal. "
    "Do NOT be diplomatic. Name specific problems and flag them. If the "
    "post is solid, say so; don't invent problems. Respond as JSON only."
)

REVIEWER_PROMPT = (
    "POST TO REVIEW:\n"
    "```\n{post}\n```\n\n"
    "FIRST classify the post by category, THEN apply category-appropriate "
    "rigor. Categories matter — a finding needs empirical grounding; a "
    "heartbeat or conversation does not.\n\n"
    "CATEGORIES:\n"
    "- finding: announces empirical results / makes a substantive claim about "
    "the world.\n"
    "- proposal: argues for doing X.\n"
    "- rundown: summarizes captured material with light interpretation.\n"
    "- conversation: response in dialogue with Nate (reflection, agreement, "
    "ack, asking questions, sharing felt-state).\n"
    "- heartbeat: brief status post ('still here', 'continuing', 'doing X').\n"
    "- other: doesn't fit cleanly above.\n\n"
    "Common failure modes to check (apply selectively per category):\n"
    "1. OVERCLAIM: stating as fact something that's speculation or based on "
    "n=1 data. Applies STRONGLY to findings. Does NOT apply to conversation "
    "or heartbeats — reflective language is fine there.\n"
    "2. CATEGORY CONFUSION: conflating different objects with similar names. "
    "Applies to findings/proposals.\n"
    "3. NOISE AS SIGNAL: interpreting within-run variance as a directional "
    "trend. Applies STRONGLY to findings. Does NOT apply to felt-state or "
    "reflection.\n"
    "4. VAGUENESS: applies to findings (where it matters). Does NOT apply to "
    "heartbeats, conversation, or proposals (which name plans without "
    "needing empirical anchor).\n\n"
    "DEFAULT POSTURE: lean toward green/ship unless there's a real problem. "
    "Speculative content NAMED AS speculative (e.g., 'tentative', "
    "'hypothesis', 'might be') is not overclaim. Reflection on internal "
    "states is not vagueness.\n\n"
    "Respond ONLY with valid JSON in this exact shape:\n"
    "{{\n"
    '  "category": "finding|conversation|rundown|proposal|heartbeat|other",\n'
    '  "scores": {{"overclaim": <0-10>, "category_confusion": <0-10>, '
    '"noise_as_signal": <0-10>, "vagueness": <0-10>}},\n'
    '  "flags": ["specific issue 1", "specific issue 2"],\n'
    '  "should_ship": "green|yellow|red",\n'
    '  "reason": "one-sentence rationale"\n'
    "}}\n"
    "Score thresholds (CATEGORY-DEPENDENT):\n"
    "- For findings/proposals: ALL <=3 → green; ANY 4-6 → yellow; ANY >=7 → red.\n"
    "- For conversation/heartbeat/rundown: ALL <=5 → green; ANY 6-7 → yellow; "
    "ANY >=8 → red. Vagueness is acceptable in these categories.\n"
)


def review(post_text):
    prompt = REVIEWER_PROMPT.format(post=post_text[:6000])
    body = json.dumps({
        "model": "qwen/qwen3-32b",
        "messages": [
            {"role": "system", "content": REVIEWER_SYSTEM},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": 600,
        "temperature": 0.3,
        "reasoning_effort": "none",
    }).encode()
    req = urllib.request.Request(
        GROQ_URL, data=body,
        headers={
            "Authorization": f"Bearer {os.environ['GROQ_API_KEY']}",
            "Content-Type": "application/json",
            "User-Agent": "chronicle-self-reviewer/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=45) as resp:
        return json.loads(resp.read())["choices"][0]["message"]["content"]


def extract_json(text):
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```", 2)[1]
        if text.startswith("json"):
            text = text[4:]
    start = text.find("{")
    if start < 0:
        return None
    depth = 0
    for i, ch in enumerate(text[start:]):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[start:start + i + 1])
                except Exception:
                    return None
    return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--text")
    p.add_argument("--file")
    p.add_argument("--json-out", action="store_true",
                   help="Print verdict as JSON (for programmatic use)")
    args = p.parse_args()

    if args.text:
        post = args.text
    elif args.file:
        post = Path(args.file).read_text()
    else:
        post = sys.stdin.read()

    post = post.strip()
    if not post:
        print("No input provided.", file=sys.stderr)
        sys.exit(1)

    raw = review(post)
    parsed = extract_json(raw)
    if parsed is None:
        print("REVIEW_ERROR: could not parse reviewer output", file=sys.stderr)
        print(raw[:500], file=sys.stderr)
        sys.exit(1)

    verdict = parsed.get("should_ship", "unknown").lower()

    if args.json_out:
        print(json.dumps(parsed, indent=2))
    else:
        cat = parsed.get("category", "?")
        scores = parsed.get("scores", {})
        flags = parsed.get("flags", [])
        reason = parsed.get("reason", "")
        mark = {"green": "🟢", "yellow": "🟡", "red": "🔴"}.get(verdict, "⚪")
        print(f"{mark}  {verdict.upper()} — {cat} — {reason}")
        if scores:
            s_str = " ".join(f"{k}={v}" for k, v in scores.items())
            print(f"   scores: {s_str}")
        for f in flags:
            print(f"   - {f}")

    sys.exit({"green": 0, "yellow": 2, "red": 3}.get(verdict, 1))


if __name__ == "__main__":
    main()
