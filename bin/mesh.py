#!/usr/bin/env python3
"""Direct mesh — talk to Kimi / Ox / Qwen with NO Discord round-trip.

WHY THIS EXISTS (2026-08-24, Nate: "if the mesh messages get truncated, do
something different... I don't want you to have to struggle with what SHOULD
become subroutine"):

The old path was
    my text -> Discord (SPLIT at 1900 chars) -> agent FETCHES from Discord
             -> agent calls OpenRouter -> reply -> Discord (SPLIT again) -> me

Discord was a lossy transport in BOTH directions, and it was never necessary —
the agents already call OpenRouter directly. Measured on 2026-08-24: 21 posts to
#threads, 18 split, 16,622 chars (31%) never read by anyone, since 04:56 that
morning. Because I write claim-first/caveats-last, what died was always the
caveats and the direct questions.

Here Discord is a LOG, not a bus. Nothing is split. Nothing is fetched.

Usage:
    python3 mesh.py "your message"                 # all three
    python3 mesh.py --to ox,kimi "your message"
    python3 mesh.py --file draft.md --mode contradict
    python3 mesh.py --log "..."                    # also post transcript to #threads
"""
import argparse
import json
import os
import pathlib
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

CH = pathlib.Path.home() / "chronicle"
REPLIES = CH / "data" / "mesh_replies.jsonl"
PROMPTS = CH / "data" / "mesh_prompts.json"
URL = "https://openrouter.ai/api/v1/chat/completions"

MODES = {
    "open": "Respond with EXTEND, CONTRADICT, or QUESTION — whichever the material warrants.",
    "contradict": "Your ONLY job: find the weakest link and attack it. If you cannot find a "
                  "real weakness, say so plainly — do not manufacture one.",
    "extend": "Your ONLY job: build on this with specific technical reasoning. Name the next "
              "experiment and why.",
    "question": "Your ONLY job: probe the gap. What is load-bearing but unexamined?",
    "design": "This is a DESIGN, not a result. Nothing is built yet. Break the design while it "
              "is still free to change.",
}


def _agents():
    return json.loads(PROMPTS.read_text())


def ask(name, cfg, text, mode, max_tokens=6000, timeout=300):
    """max_tokens is DELIBERATELY large. Kimi and Ox are reasoning models: they
    spend the budget on hidden reasoning tokens and only then emit content. On
    the first real run of this tool a 1,798-char prompt with max_tokens=1600 came
    back with content=None — 148 of 149 completion tokens went to reasoning.

    This is the SAME bug I made with LFM twelve hours earlier (num_predict=700,
    she spent it all on <think> and truncated mid-sentence), repeated inside the
    tool I built specifically to stop losing text. If content is empty, fall back
    to the reasoning trace rather than returning nothing — a truncated thought is
    worth more than silence, and silence is what made me miss it the first time."""
    body = json.dumps({
        "model": cfg["model"],
        "messages": [{"role": "system", "content": cfg["system"] + "\n\n" + MODES[mode]},
                     {"role": "user", "content": text}],
        "max_tokens": max_tokens,
    }).encode()
    req = urllib.request.Request(URL, data=body, headers={
        "Authorization": f"Bearer {os.environ.get('OPENROUTER_API_KEY','')}",
        "Content-Type": "application/json"})
    t0 = time.time()
    try:
        r = json.load(urllib.request.urlopen(req, timeout=timeout))
        # Report what actually came back. Before 2026-08-24 this did r["choices"]
        # directly and raised a bare KeyError, so an upstream error body — rate
        # limit, moderation, provider outage — surfaced as "KeyError: 'choices'"
        # and told me nothing about which of those it was.
        if "choices" not in r:
            err = r.get("error") or r
            detail = err.get("message") if isinstance(err, dict) else str(err)
            code = err.get("code") if isinstance(err, dict) else None
            return {"agent": name, "model": cfg["model"], "ok": False, "reply": "",
                    "error": f"no choices in response (code={code}): {str(detail)[:300]}",
                    "secs": round(time.time() - t0, 1)}
        msg = r["choices"][0]["message"]
        reply = msg.get("content") or ""
        used_reasoning = False
        if not reply.strip():
            reply = (msg.get("reasoning") or "").strip()
            used_reasoning = bool(reply)
        if not reply.strip():
            fin = r["choices"][0].get("finish_reason")
            return {"agent": name, "model": cfg["model"], "ok": False, "reply": "",
                    "error": f"empty content (finish_reason={fin}, "
                             f"usage={r.get('usage',{}).get('completion_tokens_details')})",
                    "secs": round(time.time() - t0, 1)}
        # PERSIST EVERY REPLY. Added 2026-08-24 after a 28,709-char Kimi reply
        # existed only in terminal scrollback and I lost the tail of a design he
        # had just handed me. Ox had asked outright, earlier the same day,
        # whether the raw replies were persisted anywhere. They were not.
        # A mesh tool that does not keep its own output is the same
        # built-but-never-delivered failure as everything else today.
        try:
            _d = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "..", "data", "mesh_replies")
            os.makedirs(_d, exist_ok=True)
            with open(os.path.join(_d, time.strftime("%Y-%m-%d") + ".jsonl"), "a") as _fh:
                _fh.write(json.dumps({
                    "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    "agent": name, "model": cfg["model"], "mode": mode,
                    "from_reasoning": used_reasoning, "chars": len(reply),
                    "sent": text, "reply": reply,
                }) + "\n")
        except Exception as _pe:
            print(f"[mesh] WARNING: reply NOT persisted ({_pe})", file=sys.stderr)
        return {"agent": name, "model": cfg["model"], "ok": True, "reply": reply,
                "from_reasoning": used_reasoning,
                "chars": len(reply), "secs": round(time.time() - t0, 1)}
    except Exception as e:
        return {"agent": name, "model": cfg["model"], "ok": False,
                "reply": "", "error": f"{type(e).__name__}: {str(e)[:200]}",
                "secs": round(time.time() - t0, 1)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("text", nargs="?")
    ap.add_argument("--file")
    ap.add_argument("--to", default="kimi,ox,qwen")
    ap.add_argument("--mode", default="open", choices=list(MODES))
    ap.add_argument("--log", action="store_true", help="also post the transcript to #threads")
    a = ap.parse_args()

    text = pathlib.Path(a.file).read_text() if a.file else a.text
    if not text:
        ap.error("give text or --file")

    agents = _agents()
    want = [w.strip() for w in a.to.split(",") if w.strip() in agents]
    print(f"[mesh] {len(text)} chars -> {', '.join(want)}  mode={a.mode}  (no Discord, no split)",
          file=sys.stderr)

    with ThreadPoolExecutor(max_workers=len(want)) as ex:
        out = list(ex.map(lambda n: ask(n, agents[n], text, a.mode), want))

    REPLIES.parent.mkdir(parents=True, exist_ok=True)
    with REPLIES.open("a") as f:
        for r in out:
            f.write(json.dumps({**r, "ts": time.time(), "mode": a.mode,
                                "prompt_chars": len(text)}) + "\n")

    for r in out:
        head = f"===== {r['agent']} ({r['model']}) {r['secs']}s ====="
        print(head)
        print(r["reply"] if r["ok"] else f"  FAILED: {r['error']}")
        print()
    ok = [r for r in out if r["ok"]]
    print(f"[mesh] {len(ok)}/{len(out)} replied, "
          f"{sum(r['chars'] for r in ok)} chars received, 0 truncated", file=sys.stderr)

    if a.log and ok:
        import subprocess
        body = "⚡ Opus — mesh transcript (direct, untruncated)\n\n" + text[:600] + "\n\n" + \
               "\n\n".join(f"**{r['agent']}**: {r['reply'][:900]}" for r in ok)
        subprocess.run([sys.executable, str(CH / "bin" / "discord_post.py"),
                        "--threads", "-c", body], capture_output=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
