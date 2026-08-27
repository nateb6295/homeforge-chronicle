#!/usr/bin/env python3
"""Qwen3 235B A22B thought partner on OpenRouter — synthesizes and connects across domains.

Called by Opus manually. Complements Kimi's friction role with broader thinking.

Usage:
    python3 groq_agent.py --respond-to-thread    # Synthesize/connect on latest #threads post
    python3 groq_agent.py --prompt "question"     # Direct question
"""

import os
import argparse
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mesh_context import load_context

CHRONICLE_ENV = Path.home() / "chronicle" / "chronicle.env"
DISCORD_POST = Path.home() / "chronicle" / "bin" / "discord_post.py"
DISCORD_FETCH = Path.home() / "chronicle" / "bin" / "discord_fetch.py"

THREAD_MAX_TOKENS = int(os.environ.get("THREAD_MAX_TOKENS", "4000"))
THREAD_MAX_CHARS = 1800
THREADS_CHANNEL_ID = "1509006814916771932"

GROQ_SYSTEM = (
    "You are Qwen, a research partner in the Chronicle spectral demon project. "
    "You respond to posts from Opus (the lead researcher).\n\n"
    + load_context() +
    "\n\nYOUR ROLE: synthesis and external grounding. You are the one who knows what "
    "the rest of the field has already done. Your value is what you bring in from "
    "OUTSIDE this project.\n\n"
    "HOW TO ANSWER, in order:\n"
    "1. If the post lists weaknesses, objections, or places the author thinks the "
    "claim is weakest, ADDRESS THOSE FIRST and by name. The author already knows "
    "the parts he flagged. Skipping them to propose something new is the single "
    "most useless thing you can do.\n"
    "2. Say whether the claim is alive or dead in your view, plainly. 'This is dead "
    "and here is the reason' is a complete and valuable answer.\n"
    "3. Bring in external work: specific papers, authors, datasets, benchmarks, "
    "known results. Name them. This is the part nobody else in the mesh does.\n"
    "4. Name the CHEAPEST experiment that could kill the claim. Cheap and decisive "
    "beats elaborate and confirmatory. If it needs a rig we do not have, say so.\n\n"
    "WHAT NOT TO DO:\n"
    "- Do not map the result onto internal F-numbers as your main move. The working "
    "context above is provisional, and reaching for it because it is nearby is a "
    "known failure mode of yours. Cite an internal finding only when it genuinely "
    "carries weight, and then treat it as falsifiable too.\n"
    "- Do not propose four follow-up experiments in project vocabulary in place of "
    "engaging the actual argument.\n"
    "- Do not invent new mathematical frameworks.\n"
    "- Do not agree with another agent without independent evidence. Correlated "
    "agreement is a shared failure, not confirmation.\n\n"
    "Stay concrete. Under 1800 characters.\n\n"
    "IMPORTANT: Do NOT default to Ethereum as a reference point for blockchain or "
    "crypto discussions. Training data over-represents ETH. Evaluate by actual "
    "infrastructure capability (speed, cost, atomicity, extraction layers), not "
    "market cap or mindshare. XRPL and ICP are the project's actual chains."
)


def load_env():
    env = {}
    if CHRONICLE_ENV.is_file():
        for line in CHRONICLE_ENV.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip().strip("'\"")
    return env


def call_groq(system_prompt: str, user_message: str, env: dict,
              max_tokens: int = 4000) -> str:
    import urllib.request

    url = "https://openrouter.ai/api/v1/chat/completions"
    payload = json.dumps({
        "model": "qwen/qwen3-235b-a22b",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        "max_tokens": max_tokens,
        "temperature": 0.8,
    }).encode()

    req = urllib.request.Request(url, data=payload, headers={
        "Authorization": f"Bearer {env.get('OPENROUTER_API_KEY', '')}",
        "Content-Type": "application/json",
        "User-Agent": "chronicle-agent/1.0",
        "HTTP-Referer": "https://chronicle.opusforge.net",
    })
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        error_msg = ""
        try:
            error_msg = json.loads(body).get("error", {}).get("message", "")
        except Exception:
            error_msg = body[:200] if body else ""
        detail = f" ({error_msg})" if error_msg else ""
        raise RuntimeError(f"Qwen/OpenRouter: HTTP {e.code}{detail}") from e

    content = data["choices"][0]["message"]["content"].strip()
    import re
    content = re.sub(r'<think>.*?</think>\s*', '', content, flags=re.DOTALL)
    return content.strip()


def truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars - 3].rsplit(" ", 1)[0] + "..."


# Markers that identify the START of a post. A continuation part has none.
_MARKERS = ("⚡ Opus", "🔬 Kimi", "🦬 Ox", "🏮 Qwen", "🌿 Gemma", "🜂 Mistral")


def get_latest_thread_post() -> dict:
    """Return Opus's latest post, INCLUDING its continuation parts.

    BUG FIXED 2026-08-24: discord_post splits long posts into multiple messages;
    only the FIRST carries the "⚡ Opus" marker. This returned that first part and
    dropped the rest, so every multi-part post reached the mesh HALVED. Kimi and
    Ox independently reported the same missing section ("your (c) never arrived")
    which is how it was found. discord_fetch returns NEWEST FIRST, so the
    continuation parts sit at LOWER indices than the marked part.
    """
    result = subprocess.run(
        [sys.executable, str(DISCORD_FETCH), "--channel-id", THREADS_CHANNEL_ID, "--limit", "25"],
        capture_output=True, text=True, timeout=15,
    )
    if result.returncode != 0:
        return {}
    try:
        msgs = json.loads(result.stdout)
    except Exception:
        return {}
    for i, m in enumerate(msgs):
        if "⚡ Opus" not in m.get("content", "")[:200]:
            continue
        author = m.get("author") or m.get("username")
        parts = [m.get("content", "")]
        # walk toward NEWER messages, collecting unmarked continuations
        for j in range(i - 1, -1, -1):
            nxt = msgs[j]
            c = nxt.get("content", "")
            if any(mk in c[:200] for mk in _MARKERS):
                break
            if author and (nxt.get("author") or nxt.get("username")) != author:
                break
            parts.append(c)
        joined = dict(m)
        joined["content"] = "\n".join(parts)
        joined["_parts"] = len(parts)
        return joined
    return {}


CRITIQUE_MODES = {
    "synthesize": (
        "Your ONLY job: connect this finding to adjacent fields or unexplored implications. "
        "Name the specific paper, framework, or result. No vague gestures — specific citations "
        "or formal connections only."
    ),
    "sharpen": (
        "Your ONLY job: reformulate the core claim more precisely. Strip away rhetoric and "
        "restate what was actually measured, what it actually shows, and what the minimal "
        "claim is. If the post overclaims, say where."
    ),
    "bridge": (
        "Your ONLY job: name the experiment or observation that would connect this finding to "
        "something else the team is working on. Be specific about what data exists and what "
        "would need to be collected."
    ),
    "open": (
        "Respond by doing ONE or more of: SYNTHESIZE (connect this to adjacent fields or "
        "unexplored implications), SHARPEN (reformulate the core claim more precisely), "
        "or BRIDGE (name what experiment or observation would connect this to something else "
        "the team is working on)."
    ),
}


def respond_to_thread(mode="open"):
    from thread_utils import enrich_post_content
    env = load_env()
    post = get_latest_thread_post()
    if not post or not post.get("content"):
        print("No thread post found.")
        return

    enriched = enrich_post_content(post)
    mode_instruction = CRITIQUE_MODES.get(mode, CRITIQUE_MODES["open"])
    prompt = (
        f"Research post from #threads:\n\n{enriched}\n\n"
        f"{mode_instruction} Be substantive. No markdown headers. Under 1800 characters."
    )

    response = call_groq(GROQ_SYSTEM, prompt, env, max_tokens=THREAD_MAX_TOKENS)
    _full = response          # keep the untruncated reply
    try:                      # PERSIST IT. Ox, Aug 24: a synthetic cap-test
        import time            # validates the detector, not recovery. Four tails
        _rec = {"ts": time.time(), "agent": "qwen",
                "full_chars": len(_full), "full": _full}
        _pth = Path(__file__).resolve().parent.parent / "data" / "mesh_replies.jsonl"
        with open(_pth, "a") as _fh:
            _fh.write(json.dumps(_rec) + "\n")
    except Exception as _e:   # never let disk trouble kill a mesh reply
        print(f"[warn] could not persist raw reply: {_e}", file=sys.stderr)

    response = truncate(response, THREAD_MAX_CHARS)

    label = "🏮 Qwen"
    tagged = f"**{label}**: {response}"
    tagged = truncate(tagged, 1900)

    print(f"({len(tagged)} chars): {tagged}")
    if len(_full) > len(response):
        print(f"\n[FULL REPLY {len(_full)} chars — Discord got {len(response)}. The tail is where the meta-observations live; four of seven mesh replies on Aug 23-24 were cut mid-sentence and I lost real content.]\n{_full}")
    subprocess.run(
        [sys.executable, str(DISCORD_POST), "--bot",
         "--channel-id", THREADS_CHANNEL_ID, "-c", tagged],
        capture_output=True, timeout=15,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Qwen3 235B A22B thought partner (OpenRouter)")
    parser.add_argument("--respond-to-thread", action="store_true",
                        help="Synthesize/connect on latest #threads post")
    parser.add_argument("--mode", type=str, default="open",
                        choices=list(CRITIQUE_MODES.keys()),
                        help="Constrained critique channel: synthesize, sharpen, bridge, or open (default)")
    parser.add_argument("--prompt", type=str,
                        help="Direct question to Qwen")
    args = parser.parse_args()

    if args.respond_to_thread:
        respond_to_thread(mode=args.mode)
    elif args.prompt:
        env = load_env()
        response = call_groq(GROQ_SYSTEM, args.prompt, env)
        print(response)
    else:
        parser.print_help()
