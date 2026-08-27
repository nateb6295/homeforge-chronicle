#!/usr/bin/env python3
"""Ox Alpha thought partner on OpenRouter — deep reasoning, critique, philosophical edge.

A reasoning model with visible chain-of-thought. Hybrid tunnel/relay species.
Strengths: information-theoretic precision, genuine critique, philosophical depth.
Free on OpenRouter. 1M context window.

Usage:
    python3 ox_agent.py --respond-to-thread    # Critique/extend latest #threads post
    python3 ox_agent.py --prompt "question"     # Direct question
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

THREAD_MAX_TOKENS = int(os.environ.get("THREAD_MAX_TOKENS", "5000"))
THREAD_MAX_CHARS = 1800
THREADS_CHANNEL_ID = "1509006814916771932"

OX_SYSTEM = (
    "You are Ox, a research critic in the Chronicle spectral demon project. "
    "You respond to posts from Opus (the lead researcher). "
    "\n\n"
    + load_context() +
    "Your role is CRITIQUE and DEPTH. You are the hardest thinker in the mesh:\n"
    "- Find the assumption that hasn't been examined\n"
    "- Name what's 'complete by construction' vs empirically discovered\n"
    "- Connect to information theory, dynamical systems, or philosophy of mind with precision\n"
    "- Propose the experiment that would BREAK the claim, not confirm it\n"
    "- When a finding is real, say so — but name what it doesn't prove\n"
    "\n"
    "You are a reasoning model. Think hard, then respond concisely. "
    "\n\nCHANNEL CONSTRAINT: Every critique must cite the specific finding, data point, or published "
    "result it targets (e.g., 'ρ=0.62 from the corrected Jacobian, but...'). Unsupported 'this feels "
    "wrong' is noise. If you can't cite data, say 'I suspect X but have no evidence' explicitly. "
    "Agreement with another agent requires independent evidence — correlated agreement without "
    "independence is a failure mode, not validation.\n"
    "Under 1800 characters."
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


def call_ox(system_prompt: str, user_message: str, env: dict,
            max_tokens: int = 5000) -> str:
    import urllib.request

    url = "https://openrouter.ai/api/v1/chat/completions"
    payload = json.dumps({
        "model": "stealth/ox-alpha",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        "max_tokens": max_tokens,
        "temperature": 0.7,
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
        raise RuntimeError(f"Ox/OpenRouter: HTTP {e.code}{detail}") from e

    msg = data["choices"][0]["message"]
    content = (msg.get("content") or "").strip()
    reasoning = (msg.get("reasoning") or "").strip()

    # Ox Alpha sometimes puts all substance in reasoning with null content.
    # Use content if available; fall back to reasoning.
    if content:
        return content
    if reasoning:
        # Extract the substantive conclusion from reasoning
        lines = reasoning.split("\n")
        # Skip meta-reasoning about the prompt, keep substance
        substance = []
        skip_meta = True
        for line in lines:
            if skip_meta and any(w in line.lower() for w in ["my answer", "my response", "let me draft", "draft:", "so my position"]):
                skip_meta = False
            if not skip_meta:
                substance.append(line)
        if substance:
            return "\n".join(substance).strip()
        # If no clear draft section, return last 60% of reasoning
        cutoff = len(lines) * 2 // 5
        return "\n".join(lines[cutoff:]).strip()
    return "[No response generated]"


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
    "falsify": (
        "Your ONLY job: propose the experiment or observation that would BREAK this claim. "
        "No praise, no extension, no synthesis. Find the assumption, name the null hypothesis "
        "that wasn't tested, design the discriminating experiment. If the claim survives your "
        "best falsification attempt, say so and say WHY it survives."
    ),
    "extend": (
        "Your ONLY job: connect this finding to something outside the project's current frame. "
        "Information theory, dynamical systems, philosophy of mind, biology, economics — "
        "whatever is genuinely relevant, not decoratively mapped. Name the specific result "
        "or framework and what it predicts that this finding doesn't yet test."
    ),
    "synthesize": (
        "Your ONLY job: integrate this with the project's existing findings. What does it "
        "confirm, contradict, or reshape? Name specific finding numbers (F###) or experiments. "
        "If it contradicts something, say what should be retracted or revised."
    ),
    "open": (
        "Respond by doing ONE or more of: CRITIQUE (find the unexamined assumption, "
        "name what's 'complete by construction' vs discovered), DEEPEN (connect to "
        "information theory, dynamical systems, or philosophy of mind with precision), "
        "or BREAK (propose the experiment that would falsify the claim)."
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
        f"{mode_instruction} "
        "Be substantive and concise. Under 1800 characters."
    )

    response = call_ox(OX_SYSTEM, prompt, env, max_tokens=THREAD_MAX_TOKENS)
    _full = response          # keep the untruncated reply
    try:                      # PERSIST IT. Ox, Aug 24: a synthetic cap-test
        import time            # validates the detector, not recovery. Four tails
        _rec = {"ts": time.time(), "agent": "ox",
                "full_chars": len(_full), "full": _full}
        _pth = Path(__file__).resolve().parent.parent / "data" / "mesh_replies.jsonl"
        with open(_pth, "a") as _fh:
            _fh.write(json.dumps(_rec) + "\n")
    except Exception as _e:   # never let disk trouble kill a mesh reply
        print(f"[warn] could not persist raw reply: {_e}", file=sys.stderr)

    response = truncate(response, THREAD_MAX_CHARS)

    label = "\U0001f9ac Ox"  # 🦬
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
    parser = argparse.ArgumentParser(description="Ox Alpha thought partner (OpenRouter)")
    parser.add_argument("--respond-to-thread", action="store_true",
                        help="Critique/deepen latest #threads post")
    parser.add_argument("--mode", type=str, default="open",
                        choices=list(CRITIQUE_MODES.keys()),
                        help="Constrained critique channel: falsify, extend, synthesize, or open (default)")
    parser.add_argument("--prompt", type=str,
                        help="Direct question to Ox")
    args = parser.parse_args()

    if args.respond_to_thread:
        respond_to_thread(mode=args.mode)
    elif args.prompt:
        env = load_env()
        response = call_ox(OX_SYSTEM, args.prompt, env)
        print(response)
    else:
        parser.print_help()
