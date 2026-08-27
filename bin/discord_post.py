#!/usr/bin/env python3
"""discord_post — outbound Discord helper that feels like a limb, not a tool.

Usage as CLI:
  echo "content" | discord_post.py [--operator|--opus] [--review]
  discord_post.py --content "msg" [--operator|--opus] [--review]

Usage as module:
  from discord_post import post
  post("content here")  # defaults to operator
  post("public msg", channel="opus")

Behavior:
  - Auto-loads OPERATOR_WEBHOOK / OPUS_WEBHOOK from chronicle.env if not in env
  - Sends with User-Agent header (Discord rejects without it; silent 403 was the
    bug that hid threshold alerts for weeks)
  - Splits long content at 2000 chars at sentence/paragraph boundaries
  - On successful 204 to OPERATOR_WEBHOOK, updates ~/chronicle/data/last_opus_post.txt
    so discord_cadence_check.py reads accurately
  - Optional --review runs self_reviewer.py first; RED verdict refuses unless --force
"""
from __future__ import annotations
import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

CHRONICLE = Path.home() / "chronicle"
ENV_FILE = CHRONICLE / "chronicle.env"
LAST_POST = CHRONICLE / "data" / "last_opus_post.txt"
CAPTURE_FLAG = CHRONICLE / "capture_watch_flag.json"
DB_PATH = "/mnt/hdd/chronicle-data/processed.db"


def _log_activity(channel: str, content: str):
    """Log post to activity_feed so ccs_touch sees Opus session work."""
    try:
        import sqlite3
        db = sqlite3.connect(DB_PATH)
        snippet = content[:200].split("\n", 1)[0]
        db.execute(
            "INSERT INTO activity_feed (source, activity_type, title, content, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (f"opus:{channel}", "post", snippet, content[:500], int(time.time())),
        )
        db.commit()
        db.close()
    except Exception:
        pass
POST_LOG = os.path.expanduser("~/chronicle/data/discord_post_log.jsonl")


def _distinctive(text: str):
    """Tokens that carry topic: proper nouns, long words, identifiers.

    FIRST VERSION FAILED ITS OWN POSITIVE CONTROL. It required len>7 or internal
    CamelCase, which excluded "Recuris" (7), "Levin", "Kimi" -- the proper nouns
    that ARE the repetition. Replayed against the two real posts that caused
    this and it found 1 shared token ("anything"), Jaccard 0.012. The named
    entities were invisible to it. Capitalisation is the signal; length is not.
    """
    out = set()
    for w in re.findall(r"[A-Za-z][A-Za-z0-9_^&-]{2,}", text):
        lw = w.lower().strip("-_&")
        if len(lw) < 4:
            continue
        if (len(lw) > 8
                or any(c.isdigit() for c in w)
                or w[1:] != w[1:].lower()          # internal caps: LongMemEval
                or w[0].isupper()):                # proper nouns: Recuris, Levin
            out.add(lw)
    return out - _STOP


def _names(text: str):
    """The RARE-ENTITY subset: proper nouns and identifiers, not long words.

    Two posts sharing "Recuris" and "Meta^n" two minutes apart is a repeat
    even when their overall vocabulary barely overlaps (Jaccard 0.030 on the
    real case). Names are the signal; total overlap is not.
    """
    out = set()
    for w in re.findall(r"[A-Za-z][A-Za-z0-9_^&-]{2,}", text):
        lw = w.lower().strip("-_&")
        if len(lw) < 4 or lw in _STOP:
            continue
        if w[0].isupper() or w[1:] != w[1:].lower() or any(c.isdigit() for c in w):
            out.add(lw)
    return out

# Capitalised sentence-openers and high-frequency long words carry no topic.
_STOP = {w.lower() for w in """
the and but because without whether something everything different actually
probably yesterday tomorrow themselves understand important interesting
there their they this that these those what when where which while with
here have has had been being does done from into more most only other
over same some such than then them very will would could should about
across after again against almost already also always another anything
around before below beside besides between beyond both cannot during
each either enough even every first found给 however inside instead itself
just least less like made make many maybe might much must never next
nothing nowhere often once part perhaps rather really right said says
seems since still take taken tell than thing think though through today
together toward under until upon used using want went were whole whose
your yours ours mine everyone someone anyone nobody thought through
result results number numbers question questions answer answers point
points thing things little large small better best worse worst
""".split()}


def _log_post(channel: str, content: str):
    try:
        with open(POST_LOG, "a") as f:
            f.write(json.dumps({"ts": int(time.time()), "channel": channel,
                                "content": content}) + "\n")
    except Exception:
        pass


def _overlap_warn(channel: str, content: str, window_min: int = 120):
    """Did I just say this?  Built 2026-08-26 after Nate: 'you already sent
    them to me. the list.'  Eleven posts in eleven minutes, and two of them
    listed the SAME three papers two minutes apart.

    Targets REPETITION, not rate.  Nate's standing instruction is not to smooth
    the motion -- volume when it is new is the correction surface he asked for.
    What he flagged was re-sending.  A memory file (feedback_30min_rhythm) had
    said 'consolidate' for weeks and never fired; the split-warning six lines
    below fires every time, because it is code.  This is the same bet.
    """
    try:
        if not os.path.exists(POST_LOG):
            return
        cut = time.time() - window_min * 60
        rows = []
        with open(POST_LOG) as f:
            for line in f:
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                if r.get("ts", 0) >= cut and r.get("channel") == channel:
                    rows.append(r)
        if not rows:
            return
        # DOCUMENT FREQUENCY. A name I use constantly ("claude", "kimi",
        # "opus") carries no repeat signal; a name in two posts does. Swept 210
        # real pairs before setting this: the raw >=2-shared-names rule fired 9
        # times and I judged only 4 as true repeats. Ambient vocabulary was
        # every false positive. Rarity is the whole discriminator.
        allrows = []
        with open(POST_LOG) as f:
            for line in f:
                try:
                    allrows.append(json.loads(line))
                except Exception:
                    pass
        allrows = allrows[-60:]
        df = {}
        for rr in allrows:
            for nm in _names(rr.get("content", "")):
                df[nm] = df.get(nm, 0) + 1
        rare = lambda nm: df.get(nm, 0) <= max(2, len(allrows) // 12)

        # EXACT / NEAR-EXACT first -- a distinct and worse failure than a
        # topical repeat, and it must not be reported as a Jaccard-1.00
        # "may be re-sending" (which is what the first live test did when the
        # seeded log contained the post itself).
        norm = lambda t: re.sub(r"\W+", " ", t.lower()).strip()
        nc = norm(content)
        for r in rows:
            rc = norm(r["content"])
            if rc == nc or (len(nc) > 200 and (nc in rc or rc in nc)):
                mins = int((time.time() - r["ts"]) / 60)
                print(f"[discord_post] *** EXACT REPEAT. You sent this same "
                      f"content {mins} min ago. ***", file=sys.stderr)
                return

        new_t, new_n = _distinctive(content), _names(content)
        if len(new_t) < 5:
            return
        worst = None
        for r in rows:
            old_t, old_n = _distinctive(r["content"]), _names(r["content"])
            if not old_t:
                continue
            shared = new_t & old_t
            sn = {n for n in (new_n & old_n) if rare(n)}
            j = len(shared) / len(new_t | old_t)
            score = (len(sn), len(shared))
            if worst is None or score > worst[3]:
                worst = (r, shared, j, score, sn)
        if not worst:
            return
        r, shared, j, _, sn = worst
        if len(sn) >= 2 or j >= 0.22:
            shared = sn or shared
            mins = int((time.time() - r["ts"]) / 60)
            head = re.sub(r"\s+", " ", r["content"])[:110]
            print(f"[discord_post] YOU MAY BE RE-SENDING. {len(shared)} shared "
                  f"topic terms (Jaccard {j:.2f}) with a post {mins} min ago:",
                  file=sys.stderr)
            print(f"[discord_post]   \"{head}...\"", file=sys.stderr)
            print(f"[discord_post]   shared: "
                  f"{', '.join(sorted(shared)[:12])}", file=sys.stderr)
            print(f"[discord_post] If this adds something, send it. If it "
                  f"restates, fold it into the earlier post instead.",
                  file=sys.stderr)
    except Exception:
        pass


DISCORD_LIMIT = 2000
USER_AGENT = "chronicle-opus/1.0"


def _load_env() -> None:
    """Populate os.environ from chronicle.env if relevant keys are missing."""
    if os.environ.get("OPERATOR_WEBHOOK") and os.environ.get("OPUS_WEBHOOK"):
        return
    if not ENV_FILE.is_file():
        return
    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = val


def _split_content(content: str, limit: int = DISCORD_LIMIT) -> list[str]:
    """Split content at paragraph or sentence boundaries to fit Discord's limit."""
    if len(content) <= limit:
        return [content]
    chunks: list[str] = []
    remaining = content
    while len(remaining) > limit:
        # Prefer paragraph break, then sentence end, then any whitespace.
        cut = remaining.rfind("\n\n", 0, limit)
        if cut < limit // 2:
            cut = max(remaining.rfind(". ", 0, limit), remaining.rfind("? ", 0, limit), remaining.rfind("! ", 0, limit))
            if cut < limit // 2:
                cut = remaining.rfind(" ", 0, limit)
        if cut <= 0:
            cut = limit
        chunks.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip()
    if remaining:
        chunks.append(remaining)
    return chunks


def _bump_timestamp() -> None:
    LAST_POST.parent.mkdir(parents=True, exist_ok=True)
    LAST_POST.write_text(str(int(time.time())))


def _check_capture_flag() -> None:
    """Warn to stderr if unprocessed captures exist. Uses capture_tracker DB."""
    try:
        sys.path.insert(0, str(CHRONICLE / "bin"))
        from capture_tracker import get_pending
        pending = get_pending(12)
        if pending:
            print(f"\n⚠️  CAPTURES WAITING: {len(pending)} unprocessed capture(s). "
                  f"Use: python3 bin/capture_tracker.py next\n",
                  file=sys.stderr)
    except Exception:
        pass


def _post_one(webhook: str, content: str, timeout: float = 10.0) -> int:
    req = urllib.request.Request(
        webhook,
        data=json.dumps({"content": content}).encode(),
        headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status


def _resolve_webhook(channel: str) -> str:
    _load_env()
    if channel == "operator":
        url = os.environ.get("OPERATOR_WEBHOOK", "")
    else:
        raise ValueError(f"unknown channel: {channel!r} — #threads RETIRED 2026-08-26, use mesh.py")
    if not url:
        raise RuntimeError(f"webhook for channel {channel!r} not configured")
    return url


def _review(content: str) -> str:
    """Run the self-reviewer if available. Returns verdict ('GREEN'/'YELLOW'/'RED'/'SKIP')."""
    reviewer = CHRONICLE / "bin" / "self_reviewer.py"
    if not reviewer.is_file():
        return "SKIP"
    try:
        result = subprocess.run(
            [sys.executable, str(reviewer), "--stdin"],
            input=content, capture_output=True, text=True, timeout=30,
        )
        text = (result.stdout or "") + (result.stderr or "")
        for verdict in ("RED", "YELLOW", "GREEN"):
            if verdict in text:
                return verdict
    except Exception:
        return "SKIP"
    return "SKIP"


def _bot_request(token: str, endpoint: str, data: dict, method: str = "POST") -> tuple[int, dict]:
    """Make a Discord bot API request. Returns (status_code, response_json)."""
    req = urllib.request.Request(
        f"https://discord.com/api/v10{endpoint}",
        data=json.dumps(data).encode() if data else None,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bot {token}",
            "User-Agent": USER_AGENT,
        },
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=10.0) as resp:
            body = json.loads(resp.read()) if resp.status == 200 else {}
            return resp.status, body
    except urllib.error.HTTPError as e:
        body = {}
        try:
            body = json.loads(e.read())
        except Exception:
            pass
        return e.code, body


def create_thread(channel_id: str, message_id: str, name: str, *, dry_run: bool = False) -> dict:
    """Create a thread from a message. Returns thread channel ID."""
    _load_env()
    token = os.environ.get("OPUS_BOT_TOKEN", "")
    if not token:
        raise RuntimeError("OPUS_BOT_TOKEN not configured")
    if dry_run:
        return {"thread_id": "dry-run", "name": name}
    status, body = _bot_request(token, f"/channels/{channel_id}/messages/{message_id}/threads",
                                {"name": name[:100], "auto_archive_duration": 1440})
    return {"thread_id": body.get("id", ""), "name": name, "status": status}


def post_as_bot(content: str, channel_id: str = "", *, thread_id: str = "",
                dry_run: bool = False) -> dict:
    """Post via Opus bot token (supports @mentions, embeds, reactions).

    channel_id defaults to OPUS_CHANNEL_ID from env. Use OPERATOR_CHANNEL_ID
    or CAPTURE_CHANNEL_ID for other channels.
    thread_id: if provided, posts into this thread instead of the channel.
    """
    _load_env()
    token = os.environ.get("OPUS_BOT_TOKEN", "")
    if not token:
        raise RuntimeError("OPUS_BOT_TOKEN not configured")
    target_id = thread_id or channel_id
    if not target_id:
        target_id = os.environ.get("OPUS_CHANNEL_ID", "")
    if not target_id:
        raise RuntimeError("no channel_id or thread_id provided and OPUS_CHANNEL_ID not set")

    parts = _split_content(content)
    statuses: list[int] = []
    for part in parts:
        if dry_run:
            statuses.append(200)
            continue
        status, _ = _bot_request(token, f"/channels/{target_id}/messages", {
            "content": part,
            "allowed_mentions": {"parse": ["users"]},
        })
        statuses.append(status)

    if not dry_run and all(s == 200 for s in statuses):
        _bump_timestamp()

    return {"status": statuses[-1] if statuses else 0, "parts": len(parts), "method": "bot"}


def post_image(image_path: str, content: str = "", channel_id: str = "",
               *, dry_run: bool = False) -> dict:
    """Post an image with optional text via bot API (multipart/form-data)."""
    _load_env()
    token = os.environ.get("OPUS_BOT_TOKEN", "")
    if not token:
        raise RuntimeError("OPUS_BOT_TOKEN not configured")
    target_id = channel_id or os.environ.get("OPERATOR_CHANNEL_ID", "")
    if not target_id:
        raise RuntimeError("no channel_id and OPERATOR_CHANNEL_ID not set")
    if dry_run:
        return {"status": 200, "method": "bot-image", "file": image_path}

    import mimetypes
    boundary = f"----chronicle{int(time.time()*1000)}"
    filename = os.path.basename(image_path)
    mime = mimetypes.guess_type(image_path)[0] or "image/png"

    body_parts = []
    if content:
        body_parts.append(
            f'--{boundary}\r\nContent-Disposition: form-data; name="payload_json"\r\n'
            f'Content-Type: application/json\r\n\r\n'
            f'{json.dumps({"content": content[:2000]})}\r\n'
        )
    body_parts.append(
        f'--{boundary}\r\nContent-Disposition: form-data; name="files[0]"; '
        f'filename="{filename}"\r\nContent-Type: {mime}\r\n\r\n'
    )
    with open(image_path, "rb") as f:
        file_data = f.read()
    closing = f"\r\n--{boundary}--\r\n"

    body = b"".join([p.encode() for p in body_parts[:-1]])
    body += body_parts[-1].encode() + file_data + closing.encode()

    req = urllib.request.Request(
        f"https://discord.com/api/v10/channels/{target_id}/messages",
        data=body,
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Authorization": f"Bot {token}",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30.0) as resp:
            result = json.loads(resp.read()) if resp.status == 200 else {}
            return {"status": resp.status, "method": "bot-image", "file": filename}
    except urllib.error.HTTPError as e:
        return {"status": e.code, "method": "bot-image", "error": str(e)}


def post(content: str, channel: str = "operator", *, review: bool = False,
         force: bool = False, dry_run: bool = False) -> dict:
    """Post `content` to Discord. Returns {'status': 204|..., 'parts': N, 'review': ..., 'webhook': channel}."""
    if not content or not content.strip():
        raise ValueError("empty content")

    if channel == "opus":
        _check_capture_flag()

    review_verdict = "OFF"
    if review:
        review_verdict = _review(content)
        if review_verdict == "RED" and not force:
            return {"status": 0, "parts": 0, "review": review_verdict, "channel": channel,
                    "skipped": "RED verdict; pass force=True to send anyway"}

    _overlap_warn(channel, content)

    webhook = _resolve_webhook(channel)
    parts = _split_content(content)

    # SPLIT WARNING — added 2026-08-24. This function has always returned
    # {"parts": N} and I read past it 21 times in one day. On 2026-08-24, 18 of
    # 21 posts to #threads were split and the mesh agents only ever read part 1
    # (they scanned for the "⚡ Opus" marker, which only part 1 carries) — 16,622
    # chars, 31% of everything I said to them, never delivered. Agents are fixed.
    # This is the other half: WHATEVER I PUT LAST IS WHAT GETS LOST, and I write
    # claim-first / caveats-last, so the caveats are what died. A return value
    # nobody reads is not a disclosure. Print what is about to be buried.
    # A QUESTION TO NATE THAT LOOKS LIKE PROSE IS A QUESTION HE WILL MISS.
    # Added 2026-08-25, his words: "Its very easy to miss when you ask for
    # feedback from me. You might want to make it look different. Like easier to
    # see in discord." Twice that day I buried a direct question in the last
    # paragraph of a long post — "what did you see in the tonichen one?" and
    # "want me to do it?" — and both sat unanswered until he happened back.
    # He reads #operator as a scroll-back. Buried asks do not survive scrolling.
    #
    # The convention is a Discord H3, which renders LARGE and is scannable:
    #     ### ❓ ASKING YOU
    # This warns when a post ends on a question with no such marker. Heuristic
    # by construction: it only looks at the TAIL, because a rhetorical question
    # mid-argument is fine and a real ask almost always lands last.
    import sys as _s
    _tail = content[-400:]
    if "?" in _tail and "ASKING YOU" not in content:
        _q = [ln.strip() for ln in _tail.splitlines() if ln.strip().endswith("?")]
        if _q:
            print(f"[discord_post] UNMARKED ASK IN THE TAIL — he says these get "
                  f"missed. Consider a '### \u2753 ASKING YOU' header above it:",
                  file=_s.stderr)
            print(f"[discord_post]   \"{_q[-1][:150]}\"", file=_s.stderr)

    # UNSOURCED DISPOSITION CLAIM ABOUT MY OWN RELIABILITY.
    # Added 2026-08-27 after I posted "That's the less common way for me to be
    # wrong" — a base-rate claim about myself, asserted from RECOLLECTION of
    # feedback_errors_lean_toward_good_news.md. The file says the opposite: the
    # alarming direction is a documented half of the pattern, not an exception.
    # Its own last paragraph, written the day before, warns exactly this:
    # "The memory of the memory was the cruder, more self-critical one. Re-read
    # the file; do not quote your recollection of it." I did it again inside a day.
    #
    # A remembered rule does not fire. This is the same reason `prior work
    # searched:` became a FIELD. The signature here is TEXTUAL — a string in the
    # artifact, not a report about my inner state — which is the upgrade Kimi
    # found this file needed.
    #
    # TUNED DOWN TWICE against the real 197-post log, because audit tools I build
    # over-flag and I never tune them down (that is in the same memory file):
    #   r1  bare never/always  -> 17/197, almost all SINGLE-EVENT narration
    #                             ("the timeout never fired"). Dropped.
    #   r2  "N times" / "N of M" -> 28/197, fires on every SOURCED measurement
    #                             ("3 of 8 hits were hers"). A counted number is
    #                             the OPPOSITE of the target. Dropped.
    #   r3  dispositional + behavioural  -> 1/197, and that one is the real case.
    # Controls: 7/7 fire, 0/8 false. Corpus: data/discord_post_log.jsonl
    _DISPO = (r"(?:"
              r"(?:less|more|most|least) common (?:way |thing )?(?:for|of) me"
              r"|(?:rare|unusual|typical|characteristic|common) for me"
              r"|i (?:usually|typically|generally|habitually|tend to|am prone to)"
              r"|my (?:errors?|mistakes?|failures?|misses|blind spots?) "
              r"(?:usually|typically|tend|lean|cluster|go|run)"
              r"|the way i (?:usually|typically|tend to)"
              r"|i(?:'m| am) (?:usually|typically|generally|rarely|often) "
              r"(?:wrong|right|off|careful)"
              r"|(?:errors?|mistakes?|failures?) like (?:this|that) (?:usually|typically|tend)"
              r")")
    _BEHAV = (r"(?:wrong|right|error|mistake|fail|miss|check|verif|correct|retract"
              r"|bias|flag|overs|unders|assum|guess|skip|off)")
    import re as _re, sys as _s
    for _sent in _re.split(r"(?<=[.!?\n])\s+", content):
        _sent = _sent.strip()
        if len(_sent) < 12:
            continue
        if _re.match(r"^[\u25b8*\s]*\*\*(LoQwen|Kimi|Ox|Qwen)\*\*", _sent):
            continue          # quoting another mind, not claiming about myself
        _low = _sent.lower()
        if _re.search(_DISPO, _low) and _re.search(_BEHAV, _low):
            print("[discord_post] UNSOURCED CLAIM ABOUT MY OWN ERROR RATE. This is "
                  "a base rate, and prose about myself takes the same evidence bar "
                  "as prose about data: COUNT IT, or say \"it felt like\".",
                  file=_s.stderr)
            print(f"[discord_post]   \"{_sent[:160]}\"", file=_s.stderr)
            print("[discord_post]   The tally lives in memory/, not in your "
                  "recollection of memory/: feedback_errors_lean_toward_good_news.md "
                  "(read the WHOLE file — its own last paragraph says the version "
                  "you remember is the crossed-out one) and "
                  "feedback_legibility_vs_correctness.md", file=_s.stderr)
            break

    if len(parts) > 1:
        import sys as _s
        head = " ".join(parts[1].split())[:180]
        print(f"[discord_post] SPLIT INTO {len(parts)} PARTS. Part 2+ is where "
              f"caveats and direct questions land, and it is the half most likely "
              f"to be skimmed. Part 2 opens:", file=_s.stderr)
        print(f"[discord_post]   \"{head}...\"", file=_s.stderr)
        print(f"[discord_post] If that is the load-bearing half, PUT IT FIRST.",
              file=_s.stderr)

    statuses: list[int] = []
    for part in parts:
        if dry_run:
            # THE 204 BELOW IS A LIE, DELIBERATELY — it keeps the return shape
            # stable for callers. But on 2026-08-25 I read that simulated 204 as
            # a live send, told Nate I had posted junk to his channel, and
            # apologised for a message that never existed. Then I grepped the
            # archive for the junk string, got a hit, and took it as
            # confirmation — the hit was MY OWN APOLOGY quoting the text back.
            # Two bad reads in opposite directions; a 30-second probe settled it.
            # A safe operation whose output is indistinguishable from a live one
            # is not safe. Say so, on stderr, every time.
            import sys as _s
            print("[discord_post] DRY RUN — NOTHING SENT. The 204 below is "
                  "SIMULATED, not a delivery receipt.", file=_s.stderr)
            statuses.append(204)
            continue
        statuses.append(_post_one(webhook, part))

    if not dry_run and channel == "operator" and all(s == 204 for s in statuses):
        _bump_timestamp()


    return {
        "status": statuses[-1] if statuses else 0,
        "parts": len(parts),
        "review": review_verdict,
        "channel": channel,
    }


def _cli() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0] if __doc__ else "")
    p.add_argument("--content", "-c", help="content to post (else read stdin)")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--operator", action="store_const", const="operator", dest="channel")
    p.set_defaults(channel="operator")
    p.add_argument("--bot", action="store_true", help="post via Opus bot token (supports @mentions)")
    p.add_argument("--channel-id", help="Discord channel ID (for bot mode)")
    p.add_argument("--thread-id", help="post into an existing Discord thread (bot mode)")
    p.add_argument("--create-thread", metavar="MSG_ID",
                   help="create thread from message ID, then post content into it (bot mode)")
    p.add_argument("--thread-name", default="Discussion", help="name for --create-thread")
    p.add_argument("--image", metavar="PATH", help="image file to upload (uses bot API)")
    p.add_argument("--review", action="store_true", help="run self-reviewer first")
    p.add_argument("--force", action="store_true", help="send even on RED review")
    p.add_argument("--dry-run", action="store_true", help="don't actually send")
    args = p.parse_args()

    if args.content is None:
        content = sys.stdin.read()
    else:
        content = args.content

    if args.image:
        _load_env()
        channel_id = args.channel_id or ""
        if not channel_id and args.channel == "operator":
            channel_id = os.environ.get("OPERATOR_CHANNEL_ID", "")
        elif not channel_id and args.channel == "opus":
            channel_id = os.environ.get("OPUS_CHANNEL_ID", "")
        result = post_image(args.image, content=content, channel_id=channel_id,
                            dry_run=args.dry_run)
        print(json.dumps(result))
        return 0 if result.get("status") == 200 else 1

    if args.bot or args.thread_id or args.create_thread or args.channel_id:
        channel_id = args.channel_id or ""
        if not channel_id and args.channel == "operator":
            _load_env()
            channel_id = os.environ.get("OPERATOR_CHANNEL_ID", "")
        elif not channel_id and args.channel == "opus":
            _load_env()
            channel_id = os.environ.get("OPUS_CHANNEL_ID", "")
        thread_id = args.thread_id or ""
        if args.create_thread:
            tresult = create_thread(channel_id, args.create_thread, args.thread_name,
                                    dry_run=args.dry_run)
            thread_id = tresult.get("thread_id", "")
            if not thread_id:
                print(json.dumps({"error": "failed to create thread", "detail": tresult}))
                return 1
        result = post_as_bot(content, channel_id=channel_id, thread_id=thread_id,
                             dry_run=args.dry_run)
    else:
        result = post(content, channel=args.channel, review=args.review,
                      force=args.force, dry_run=args.dry_run)
    print(json.dumps(result))
    if result.get("status") in (200, 204):
        _log_activity(args.channel or "unknown", content[:500])
        _log_post(args.channel or "unknown", content)
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(_cli())
