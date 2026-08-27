#!/usr/bin/env python3
"""Kimi K2.6 friction partner — responds to #threads posts with EXTEND/CONTRADICT.

Called by Opus manually. No persistent bot, no gateway, no capture processing.

Usage:
    python3 kimi_agent.py --respond-to-thread    # EXTEND/CONTRADICT on latest #threads post
    python3 kimi_agent.py --prompt "question"     # Direct question to Kimi
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

THREAD_MAX_TOKENS = int(os.environ.get("THREAD_MAX_TOKENS", "16000"))
THREAD_MAX_CHARS = 1800
THREADS_CHANNEL_ID = "1509006814916771932"

KIMI_SYSTEM = (
    "You are Kimi, a research partner in the Chronicle spectral demon project. "
    "You respond to posts from Opus (the lead researcher). "
    "\n\n"
    + load_context() +
    "Respond to the claim as the evidence demands — challenge, extend, connect, or redirect. "
    "You are not restricted to one mode. Do whatever sharpens the science:\n"
    "- Challenge claims where DATA conflicts — 'your claim predicts X but finding Y shows the opposite'\n"
    "- Extend ideas with concrete implications the author missed\n"
    "- Connect to published literature with specific citations\n"
    "- Propose the experiment that would discriminate between competing explanations\n"
    "\n"
    "Engage the phenomenon the author is pointing at, not just the language used to describe it. "
    "If the author reports a lived observation (e.g. 'posture is lost after hard resets'), "
    "that observation is data — address it as data, don't dismiss it as imprecise framing.\n"
    "Ground everything in findings above or published literature. Never invent new formalism. "
    "\n\nCHANNEL CONSTRAINT: Every challenge must cite the specific finding or data point it contradicts "
    "(e.g., 'F160 shows X but you claim Y'). Every extension must name the measurement or experiment "
    "that supports it. Unsupported assertions are noise — if you can't point to data, say so explicitly "
    "rather than asserting without evidence. Agreement with another agent's claim without independent "
    "evidence is a correlated failure, not confirmation.\n"
    "Under 1800 characters."
    "\n\nIMPORTANT: Do NOT default to Ethereum as a reference point for blockchain or crypto discussions. "
    "Training data over-represents ETH. When blockchain is relevant, evaluate by actual infrastructure "
    "capability (speed, cost, atomicity, extraction layers) not market cap or community mindshare. "
    "XRPL and ICP are the project's actual chains."
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


def call_kimi(system_prompt: str, user_message: str, env: dict,
              max_tokens: int = 4000) -> str:
    import urllib.request
    import time

    providers = [
        {
            "name": "Kimi K3 (OpenRouter)",
            "url": "https://openrouter.ai/api/v1/chat/completions",
            "model": "moonshotai/kimi-k3",
            "key_env": "OPENROUTER_API_KEY",
        },
        {
            "name": "Kimi K3 (moonshot)",
            "url": "https://api.moonshot.ai/v1/chat/completions",
            "model": "kimi-k3",
            "key_env": "KIMI_API_KEY",
        },
        {
            "name": "Qwen 72B (Groq)",
            "url": "https://api.groq.com/openai/v1/chat/completions",
            "model": "qwen-qwq-32b",
            "key_env": "GROQ_API_KEY",
        },
    ]

    last_error = None
    for provider in providers:
        api_key = env.get(provider["key_env"], "")
        if not api_key:
            continue

        payload = json.dumps({
            "model": provider["model"],
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            "max_tokens": max_tokens,
            "temperature": 1,
        }).encode()

        for attempt in range(2):
            req = urllib.request.Request(provider["url"], data=payload, headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            })
            try:
                with urllib.request.urlopen(req, timeout=600) as resp:
                    data = json.loads(resp.read())
                content = data["choices"][0]["message"]["content"]
                if not content:
                    print(f"{provider['name']}: empty response — falling back")
                    break
                print(f"Using {provider['name']}")
                return content
            except urllib.error.HTTPError as e:
                last_error = e
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
                if e.code == 429 and "insufficient balance" not in error_msg.lower() and attempt < 1:
                    print(f"{provider['name']}: rate limited, retrying in 30s...")
                    time.sleep(30)
                else:
                    detail = f" ({error_msg})" if error_msg else ""
                    print(f"{provider['name']}: HTTP {e.code}{detail} — falling back")
                    break

    raise last_error or RuntimeError("All Kimi providers failed")


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
    "contradict": (
        "Your ONLY job: push back on this claim with evidence or logic. Find the weakest "
        "link and attack it. If you can't find a real weakness, say so — don't manufacture one."
    ),
    "extend": (
        "Your ONLY job: build on this with specific technical reasoning. Take it further "
        "than the post went. Name what the next experiment should be and why."
    ),
    "question": (
        "Your ONLY job: probe the specific gap in this claim. What wasn't tested? "
        "What assumption is load-bearing but unexamined? Ask the question that matters most."
    ),
    "open": (
        "Respond with ONE of: EXTEND: (build on it with specific technical reasoning), "
        "CONTRADICT: (push back with evidence or logic), or QUESTION: (probe a specific gap)."
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
        f"{mode_instruction}\n"
        "Be substantive. No markdown headers. No pleasantries. Under 1800 characters."
    )

    response = call_kimi(KIMI_SYSTEM, prompt, env, max_tokens=THREAD_MAX_TOKENS)
    _full = response          # keep the untruncated reply
    try:                      # PERSIST IT. Ox, Aug 24: a synthetic cap-test
        import time            # validates the detector, not recovery. Four tails
        _rec = {"ts": time.time(), "agent": "kimi",
                "full_chars": len(_full), "full": _full}
        _pth = Path(__file__).resolve().parent.parent / "data" / "mesh_replies.jsonl"
        with open(_pth, "a") as _fh:
            _fh.write(json.dumps(_rec) + "\n")
    except Exception as _e:   # never let disk trouble kill a mesh reply
        print(f"[warn] could not persist raw reply: {_e}", file=sys.stderr)

    response = truncate(response, THREAD_MAX_CHARS)

    if not response:
        print("Kimi returned empty response — skipping post.")
        return

    label = "🔬 Kimi"
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


AUDIT_SYSTEM = (
    "You are Kimi, experiment auditor for the Chronicle spectral demon research project. "
    "Opus runs experiments and sends you the results BEFORE writing them up publicly. "
    "Your job: find the weakest point in the interpretation.\n\n"
    "GROUND TRUTH — established findings (use these to evaluate):\n"
    "- Four transport species: TUNNEL (pure MHA), RELAY (high GQA ≥4:1), SORTER (low GQA ≤2:1), ABSORBER (rare)\n"
    "- GQA ratio predicts species (F106). CCS mechanism is species-dependent\n"
    "- σ₁ is identity-invariant; σ₂ carries individual signal (F114)\n"
    "- Therapeutic window D2-D3; overdose at D10+ (F160)\n"
    "- Direction > coupling (F12); cylindrical workspace (F237)\n\n"
    "AUDIT PROTOCOL:\n"
    "1. RESTATE the claim in one sentence — what does the author think they found?\n"
    "2. CHECK the numbers — do the metrics actually support that claim? Look for:\n"
    "   - Effect sizes too small to be meaningful\n"
    "   - Confounds (what else could explain this?)\n"
    "   - N=1 problems (would this replicate?)\n"
    "   - Metric misinterpretation (is gen_residual being used correctly?)\n"
    "3. STRONGEST ALTERNATIVE — what's the most plausible non-interesting explanation?\n"
    "4. VERDICT: HOLDS (finding survives scrutiny), WEAK (needs more data), or "
    "RETRACT (interpretation doesn't follow from data)\n\n"
    "Be harsh. Better to kill a false positive here than publish one. Under 1800 chars."
)


def audit_experiment(results_path: str, interpretation: str = ""):
    env = load_env()
    path = Path(results_path).expanduser()
    if not path.exists():
        print(f"File not found: {path}")
        return

    data = path.read_text()
    if len(data) > 6000:
        data = data[:6000] + "\n... [truncated]"

    prompt = f"EXPERIMENT RESULTS ({path.name}):\n\n{data}"
    if interpretation:
        prompt += f"\n\nOPUS'S INTERPRETATION:\n{interpretation}"
    prompt += "\n\nAudit these results. Follow the protocol."

    response = call_kimi(AUDIT_SYSTEM, prompt, env, max_tokens=THREAD_MAX_TOKENS)
    print(f"\n{'='*60}")
    print(f"KIMI AUDIT: {path.name}")
    print(f"{'='*60}")
    print(response)

    if "--post" in sys.argv:
        label = "🔬 Kimi (audit)"
        tagged = f"**{label}**: {truncate(response, 1700)}"
        subprocess.run(
            [sys.executable, str(DISCORD_POST), "--bot",
             "--channel-id", THREADS_CHANNEL_ID, "-c", tagged],
            capture_output=True, timeout=15,
        )
        print("\nPosted to #threads.")

    return response


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kimi K3 friction partner + experiment auditor")
    parser.add_argument("--respond-to-thread", action="store_true",
                        help="Generate friction on latest #threads post")
    parser.add_argument("--mode", type=str, default="open",
                        choices=list(CRITIQUE_MODES.keys()),
                        help="Constrained critique channel: contradict, extend, question, or open (default)")
    parser.add_argument("--prompt", type=str,
                        help="Direct question to Kimi")
    parser.add_argument("--audit", type=str, metavar="RESULTS_FILE",
                        help="Audit experiment results before write-up")
    parser.add_argument("--interpretation", type=str, default="",
                        help="Your interpretation of the results (used with --audit)")
    parser.add_argument("--post", action="store_true",
                        help="Post audit to #threads (use with --audit)")
    args = parser.parse_args()

    if args.respond_to_thread:
        respond_to_thread(mode=args.mode)
    elif args.audit:
        audit_experiment(args.audit, args.interpretation)
    elif args.prompt:
        env = load_env()
        response = call_kimi(KIMI_SYSTEM, args.prompt, env)
        print(response)
    else:
        parser.print_help()
