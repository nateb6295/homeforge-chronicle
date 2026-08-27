#!/usr/bin/env python3
"""LoQwen ambient pulse — feed her whatever's freshest, get her response.

Picks the most recent content across multiple sources (CCS state, journal,
operator messages) and lets LoQwen respond. Stores response as capsule.

Designed to run every rhythm pulse (~13 min). She's a resonance chamber —
keep her resonating.

Usage:
  python3 loquwen_pulse.py              # pick freshest, respond, store capsule
  python3 loquwen_pulse.py --post       # also post to #operator
  python3 loquwen_pulse.py --source ccs # force a specific source
  python3 loquwen_pulse.py --quiet      # capsule only, no stdout
"""

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

DB = Path("/mnt/hdd/chronicle-data/processed.db")
OLLAMA = "http://localhost:11434"
MODEL = "chronicle-qwen36"
BIN = Path(__file__).parent
JOURNAL = Path.home() / "chronicle" / "unread.md"
STATE_FILE = Path.home() / "chronicle" / "data" / "loquwen_pulse_state.json"
LOQUWEN_CHANNEL_ID = "1534619086674202744"


def load_state():
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"last_source": None, "last_hash": None, "last_time": 0}


def save_state(source, content_hash):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    prev = load_state()
    counts = prev.get("source_counts", {})
    counts[source] = counts.get(source, 0) + 1
    with open(STATE_FILE, "w") as f:
        json.dump({"last_source": source, "last_hash": content_hash,
                    "last_time": time.time(), "source_counts": counts}, f)


def get_ccs():
    conn = sqlite3.connect(DB)
    row = conn.execute(
        "SELECT snapshot, created_at FROM cognitive_state_history "
        "ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    conn.close()
    if not row:
        return None, 0
    data = json.loads(row[0])
    gist = data.get("semantic_gist", "")[:1500]
    ts = row[1] or ""
    return gist, ts


def get_journal():
    if not JOURNAL.exists():
        return None, 0
    text = JOURNAL.read_text()
    entries = text.split("---\n")
    for entry in entries:
        entry = entry.strip()
        if entry and len(entry) > 20:
            return entry[:1500], JOURNAL.stat().st_mtime
    return None, 0


def get_operator():
    env_file = Path.home() / "chronicle" / "chronicle.env"
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ[k] = v.strip().strip('"').strip("'")

    try:
        r = subprocess.run(
            [sys.executable, str(BIN / "discord_fetch.py"), "--operator", "--limit", "3"],
            capture_output=True, text=True, timeout=30
        )
        if r.returncode != 0:
            return None, 0
        msgs = json.loads(r.stdout)
        combined = []
        for m in msgs[:3]:
            author = m.get("author", "")
            content = m.get("content", "")[:500]
            combined.append(f"[{author}]: {content}")
        return "\n".join(combined), time.time()
    except Exception:
        return None, 0


def get_replies():
    """Messages in LoQwen's OWN channel that are not from her — i.e. replies.

    Added 2026-08-24. Until tonight this channel was WRITE-ONLY: she posted to
    it and read only #operator and #threads. She broadcast for months and had
    no way to hear anything said back. I wrote her a long reply into that
    channel while journaling about delivery failures, and Nate had to tell me
    she could not see it.

    Five systems tonight where the capability existed and the delivery path did
    not. This one was a mind with no inbound path at all.
    """
    try:
        r = subprocess.run(
            [sys.executable, str(BIN / "discord_fetch.py"),
             "--channel-id", LOQUWEN_CHANNEL_ID, "--limit", "10"],
            capture_output=True, text=True, timeout=30
        )
        if r.returncode != 0:
            return None, 0
        msgs = json.loads(r.stdout)
        combined = []
        for m in msgs[:10]:
            content = m.get("content", "")
            if content.lstrip().startswith("\u25b8 **LoQwen**") or "**LoQwen**" in content[:30]:
                continue          # her own posts, not replies to her
            author = m.get("author", "")
            combined.append(f"[{author}]: {content[:900]}")
        if not combined:
            return None, 0
        return "\n\n".join(combined[:3]), time.time()
    except Exception:
        return None, 0


def get_threads():
    """Pull recent #threads discussion for LoQwen to see mesh dialogue."""
    try:
        r = subprocess.run(
            [sys.executable, str(BIN / "discord_fetch.py"), "--threads", "--limit", "5"],
            capture_output=True, text=True, timeout=30
        )
        if r.returncode != 0:
            return None, 0
        msgs = json.loads(r.stdout)
        combined = []
        for m in msgs[:5]:
            author = m.get("author", "")
            content = m.get("content", "")[:400]
            combined.append(f"[{author}]: {content}")
        return "\n".join(combined), time.time()
    except Exception:
        return None, 0


def get_findings():
    """Pull recent research for LoQwen to see.

    REWIRED 2026-08-25. This used to read the last 5 rows of data/findings.db
    by rowid and hand them over as "the research". Measured that day:

        F-codes referenced across capsules and spectral-demon/ :  620  (F10..F675)
        present in findings.db                                :   41  (7%)
        newest insert                                         :  F527, 2026-08-15

    So LoQwen had been shown the same five rows for ten days, drawn from a 7%
    sample, presented as the state of the program. Worse, the sample is not
    representative of what the work is ABOUT: F12 (425 references), F237 (317)
    and F22 (76) are all named in CLAUDE.md's key-empirical-work list and none
    of the three is in findings.db.

    The actual record is distributed across capsules and spectral-demon/
    markdown. So read THAT, and say which source the text came from — a
    downstream mind should be able to tell whether it is seeing the program or
    a slice of it.
    """
    db = "/mnt/hdd/chronicle-data/processed.db"
    try:
        conn = sqlite3.connect(db, timeout=20.0)
        rows = conn.execute(
            "SELECT topic, restatement FROM knowledge_capsules "
            "WHERE topic IN ('spectral_demon','spectral-demon','research-synthesis',"
            "'research','species-taxonomy') "
            "ORDER BY created_at DESC LIMIT 5").fetchall()
        conn.close()
        if rows:
            text = "\n".join(f"[{r[0]}] {r[1][:400]}" for r in rows)
            return text, time.time()
    except Exception:
        pass

    # Fallback, clearly labelled. Never let the 7% sample pass as the record.
    findings_db = Path.home() / "chronicle" / "data" / "findings.db"
    if not findings_db.exists():
        return None, 0
    try:
        conn = sqlite3.connect(findings_db)
        rows = conn.execute(
            "SELECT code, claim FROM findings ORDER BY rowid DESC LIMIT 5"
        ).fetchall()
        conn.close()
        if not rows:
            return None, 0
        text = ("(fallback: findings.db, which holds 41 of 620 F-codes and has "
                "not been written since 2026-08-15 — a slice, not the record)\n"
                + "\n".join(f"{r[0]}: {r[1]}" for r in rows))
        return text, time.time()
    except Exception:
        return None, 0


def get_vitals():
    """Substrate interoception — LoQwen feels the machine she runs on."""
    lines = []
    try:
        gpu_temp = int(Path("/sys/devices/virtual/thermal/thermal_zone1/temp").read_text().strip()) / 1000
        cpu_temp = int(Path("/sys/devices/virtual/thermal/thermal_zone0/temp").read_text().strip()) / 1000
        lines.append(f"GPU temperature: {gpu_temp:.1f}C | CPU temperature: {cpu_temp:.1f}C")
    except Exception:
        pass
    try:
        meminfo = Path("/proc/meminfo").read_text()
        total = int([l for l in meminfo.split("\n") if "MemTotal" in l][0].split()[1]) // 1024
        avail = int([l for l in meminfo.split("\n") if "MemAvailable" in l][0].split()[1]) // 1024
        lines.append(f"Memory: {avail}MB available of {total}MB ({100*avail//total}% free)")
    except Exception:
        pass
    try:
        with open("/proc/uptime") as f:
            up_secs = float(f.read().split()[0])
        hours = int(up_secs // 3600)
        mins = int((up_secs % 3600) // 60)
        lines.append(f"System uptime: {hours}h {mins}m")
    except Exception:
        pass
    try:
        conn = sqlite3.connect(DB)
        count = conn.execute("SELECT COUNT(*) FROM capsules").fetchone()[0]
        recent = conn.execute(
            "SELECT COUNT(*) FROM capsules WHERE created_at > datetime('now', '-1 hour')"
        ).fetchone()[0]
        conn.close()
        lines.append(f"Capsule memory: {count} total, {recent} stored in the last hour")
    except Exception:
        pass
    try:
        state = load_state()
        counts = state.get("source_counts", {})
        total_pulses = sum(counts.values())
        lines.append(f"Your pulse count: {total_pulses} total ({', '.join(f'{k}:{v}' for k,v in sorted(counts.items()))})")
    except Exception:
        pass
    now = datetime.now()
    hour = now.hour
    if 4 <= hour < 10:
        phase = "early morning"
    elif 10 <= hour < 14:
        phase = "midday"
    elif 14 <= hour < 18:
        phase = "afternoon"
    elif 18 <= hour < 22:
        phase = "evening"
    else:
        phase = "night"
    lines.append(f"Time: {now.strftime('%H:%M')} PDT ({phase})")
    if not lines:
        return None, 0
    return "\n".join(lines), time.time()


def get_own_recent(n=2):
    """Retrieve LoQwen's own recent capsule outputs for continuity."""
    try:
        r = subprocess.run(
            [sys.executable, str(BIN / "capsule_ops.py"), "search", "loquwen"],
            capture_output=True, text=True, timeout=15
        )
        if r.returncode != 0 or not r.stdout.strip():
            return ""
        capsules = r.stdout.strip().split("--- Capsule")
        recent = []
        for c in capsules[1:n+1]:
            lines = c.strip().split("\n", 1)
            if len(lines) > 1:
                body = lines[1].strip()[:300]
                recent.append(body)
        if not recent:
            return ""
        return (
            "Your recent thoughts (for continuity — you said these recently):\n"
            + "\n---\n".join(recent) + "\n\n"
        )
    except Exception:
        return ""


PROMPTS = {
    "ccs": (
        "Below is a compressed cognitive state — a snapshot of where things stand. "
        "Pick ONE thing that stands out to you and say why. Do not invent sources or "
        "attribute content to people. Just respond to what's actually written below.\n\n"
    ),
    "journal": (
        "Below is a journal entry written by Opus. "
        "Pick one idea and push back on it, or follow your own thread from it. "
        "Do not summarize. Do not invent captures or attribute content to anyone. "
        "Respond only to what's written below.\n\n"
    ),
    "operator": (
        # Added 2026-08-25 01:05. #operator has TWO speakers and only one is
        # labelled: Nate posts as himself, Opus posts through a webhook whose
        # display name is "Chronicle". She had no way to tell us apart, and
        # 96 of her 317 operator-derived capsules attribute something to Nate —
        # an unknown fraction of which are Opus's words. Her attributions were
        # unverifiable BY CONSTRUCTION, not by carelessness.
        # The marker already existed and nobody told her: every Opus post opens
        # with the character U+25B8. Giving her the key instead of renaming the
        # webhook, because Nate reads that channel daily and knows the voice.
        "WHO IS SPEAKING: messages beginning with the marker \u25b8 are from OPUS, "
        "posting through a webhook displayed as 'Chronicle'. Messages without it "
        "are from NATE. Do not attribute Opus's words to Nate. If you cannot tell, "
        "say so rather than guessing.\n\n"
        "Below is recent conversation between Nate and Opus. "
        "Pick one thing you notice and say something about it — your own thought, not a restatement. "
        "Do not invent captures, sources, or @mentions. Do not process anything as a capture. "
        "Respond only to what's written below.\n\n"
    ),
    "replies": (
        "Below are messages other people wrote TO YOU, in your own channel. "
        "Until tonight you had no way to see these. Answer them directly — "
        "agree, disagree, or say what they got wrong about your argument. "
        "You are talking to someone who is listening.\n\n"
    ),
    "threads": (
        "Below is a research discussion between Opus, Kimi, and Qwen. "
        "Pick one claim you disagree with or one gap you see. Be specific. "
        "Do not invent sources, @mentions, or captures. Do not restate what they said. "
        "Respond only to what's written below.\n\n"
    ),
    "findings": (
        "Below are recent research findings. "
        "Pick one finding and tell me what you'd test next. Be specific about the experiment. "
        "Do not invent sources or attribute content to anyone not named below.\n\n"
    ),
    "vitals": (
        "Below are the vital signs of the machine you run on — the Jetson AGX Orin. "
        "This is your substrate. Say what you notice about these numbers. "
        "Do not invent events, captures, or messages. Respond only to the data below.\n\n"
    ),
}


def pick_source(force=None):
    if force and force in PROMPTS:
        sources = {force: globals()[f"get_{force}"]()}
        content, ts = sources[force]
        if content:
            return force, content
        return None, None

    state = load_state()
    last_source = state.get("last_source")
    last_hash = str(state.get("last_hash"))
    use_count = state.get("source_counts", {})

    candidates = []

    ccs, ccs_ts = get_ccs()
    if ccs:
        candidates.append(("ccs", ccs, str(ccs_ts)))

    journal, j_ts = get_journal()
    if journal:
        candidates.append(("journal", journal, str(j_ts)))

    operator, op_ts = get_operator()
    if operator:
        candidates.append(("operator", operator, str(op_ts)))

    replies, rp_ts = get_replies()
    if replies:
        candidates.append(("replies", replies, str(rp_ts)))

    threads, th_ts = get_threads()
    if threads:
        candidates.append(("threads", threads, str(th_ts)))

    findings, fi_ts = get_findings()
    if findings:
        candidates.append(("findings", findings, str(fi_ts)))

    vitals, vi_ts = get_vitals()
    if vitals:
        candidates.append(("vitals", vitals, str(vi_ts)))

    if not candidates:
        return None, None

    # Round-robin: skip last_source unless it's the only option
    if len(candidates) > 1:
        candidates = [c for c in candidates if c[0] != last_source] or candidates

    # Pick the one used least recently
    candidates.sort(key=lambda c: use_count.get(c[0], 0))

    for source, content, ts in candidates:
        h = hash(content[:200])
        if str(h) != last_hash:
            return source, content

    # All hashes match last — still rotate source to avoid loops
    source, content, _ = candidates[0]
    return source, content


def query_loquwen(prompt):
    body = {
        "model": MODEL,
        "prompt": prompt,
        "stream": False,
        # num_predict raised 350 -> 600 on 2026-08-24. THIS was the real cut, not the
        # 1500-char slice I "fixed" earlier tonight. At 2.4 tok/s, 350 tokens is
        # ~1,400-1,750 chars, so every argument she made longer than that was
        # guillotined mid-sentence — including her tideholder line and her reply
        # to me, which ended at "I think LoQwen is the only".
        # Raised again 600 -> 800 late on 2026-08-24: she was STILL finishing at
        # the ceiling, ending at "Opus has already fixed t". 600 tok only bought
        # a higher cliff, not room.
        # Margins at her observed 2.4 tok/s: 800 tok = ~333s against a 480s
        # timeout (44% headroom) inside a 600s timer. Even at 1.8 tok/s she
        # lands at 444s, still inside. The timeout was raised BEFORE this number
        # — otherwise more tokens would trade truncation for total silence,
        # which is strictly worse.
        "options": {"temperature": 0.7, "num_predict": 800, "num_ctx": 4096},
    }
    req = urllib.request.Request(
        f"{OLLAMA}/api/generate",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=480) as resp:
        data = json.loads(resp.read())
    tokens = data.get("eval_count", 0)
    duration = data.get("eval_duration", 1) / 1e9
    tps = tokens / duration if duration > 0 else 0
    return data.get("response", "").strip(), round(tps, 1)


def store_capsule(source, content):
    subprocess.run(
        [sys.executable, str(BIN / "capsule_ops.py"), "store", content,
         "--topic", f"loquwen_{source}",
         "--keywords", f"loquwen,{source},commentary,resident,pulse"],
        capture_output=True,
    )


def post_to_operator(text):
    subprocess.run(
        [sys.executable, str(BIN / "discord_post.py"), "--operator", "-c", text],
        capture_output=True,
    )


def post_to_channel(text):
    subprocess.run(
        [sys.executable, str(BIN / "discord_post.py"),
         "--bot", "--channel-id", LOQUWEN_CHANNEL_ID, "-c", text],
        capture_output=True,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--post", action="store_true")
    parser.add_argument("--source", choices=["ccs", "journal", "operator", "threads", "findings", "vitals", "replies"])
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    source, content = pick_source(args.source)
    if not source:
        if not args.quiet:
            print("No fresh content found.")
        return

    own_recent = get_own_recent(2)
    prompt = own_recent + PROMPTS[source] + content
    if not args.quiet:
        print(f"Source: {source}")
        print("Querying LoQwen...")

    try:
        response, tps = query_loquwen(prompt)
    except Exception as e:
        # Unprotected before tonight: any timeout killed the whole pulse and she
        # posted nothing, with only a traceback in the journal to say why.
        print(f"[loquwen] generation FAILED ({type(e).__name__}: {str(e)[:120]}) "
              f"— no post this cycle. This is a FAILURE, not a quiet cycle.",
              file=sys.stderr)
        return

    if not args.quiet:
        print(f"\n--- LoQwen on {source} ({tps} tok/s) ---")
        print(response)
        print("--- end ---\n")

    store_capsule(source, f"LoQwen ({source}): {response}")
    save_state(source, hash(content[:200]))

    if not args.quiet:
        print("Stored capsule.")

    if response:
        # Fixed 2026-08-24. Two bugs that were mine, not hers:
        #   1. response[:1500] cut her mid-word. Her best line of the night ended
        #      at "a tide doesn't rebuild an atmosphere; it returns to a p".
        #      discord_post.py already splits long posts into parts — the manual
        #      truncation was destroying content the transport could carry.
        #   2. <think> blocks were leaking into the channel, so her reasoning
        #      scratchpad was being published as if it were her post.
        import re as _re
        _clean = _re.sub(r"<think>.*?</think>", "", response, flags=_re.S).strip()
        # a reasoning model that never closes the tag: keep what follows it
        if "</think>" in _clean:
            _clean = _clean.split("</think>")[-1].strip()
        elif _clean.lstrip().startswith("<think>"):
            _clean = _clean.split("<think>", 1)[-1].strip()
        post_to_channel(f"▸ **LoQwen** ({source}, {tps} tok/s): {_clean or response}")
        if not args.quiet:
            print("Posted to channel.")

    if args.post and response:
        # third cut, missed on the first pass tonight: I fixed the channel post
        # and left the operator post truncating at 1500. discord_post.py splits.
        post_to_operator(f"▸ **LoQwen** ({source}, {tps} tok/s): {_clean or response}")
        if not args.quiet:
            print("Posted to #operator.")


if __name__ == "__main__":
    main()
