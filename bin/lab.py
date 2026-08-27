#!/usr/bin/env python3
"""The Lab — shared workspace for Opus, Gemma, and LFM.

Manages the lab notebook (data/lab_notebook.json) and generates
briefings for injection into Gemma/LFM prompts.

Usage:
    lab.py brief                      # Text briefing for prompt injection
    lab.py brief --for gemma          # Gemma-tailored briefing (emphasizes gate perspective)
    lab.py brief --for lfm            # LFM-tailored briefing (emphasizes SSM perspective)
    lab.py ask "question"             # Post a question for the mesh
    lab.py observe --from gemma "obs" # Record a grounded observation
    lab.py thread                     # Show recent thread entries
    lab.py thread --add --from opus "entry"  # Add to conversation thread
    lab.py finding "F592" "title" "summary" # Add a new finding
    lab.py ingest-results             # Scan experiment JSONs and update notebook
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone, timedelta

NOTEBOOK_PATH = os.path.expanduser("~/chronicle/data/lab_notebook.json")
RESULTS_DIR = os.path.expanduser("~/chronicle/spectral-demon/experiments/results")
LAB_CHANNEL_ID = "1535435003591135382"
PDT = timezone(timedelta(hours=-7))


def _post_to_lab(text):
    """Post a message to the #lab Discord channel."""
    try:
        env_file = os.path.expanduser("~/chronicle/chronicle.env")
        env = dict(os.environ)
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip().strip('"').strip("'")
        cmd = [sys.executable, os.path.join(os.path.dirname(__file__), "discord_post.py"),
               "--channel-id", LAB_CHANNEL_ID, "-c", text[:1900]]
        subprocess.run(cmd, capture_output=True, text=True, timeout=15, env=env)
    except Exception:
        pass


def _load():
    with open(NOTEBOOK_PATH) as f:
        return json.load(f)


def _save(nb):
    nb["meta"]["last_updated"] = datetime.now(PDT).isoformat()
    with open(NOTEBOOK_PATH, "w") as f:
        json.dump(nb, f, indent=2)


def _now_iso():
    return datetime.now(PDT).isoformat()


def cmd_brief(args):
    """Generate a text briefing from the lab notebook."""
    nb = _load()
    target = getattr(args, "for_model", None) or "general"
    lines = []

    lines.append("═══ THE LAB — GROUNDED DATA ═══")
    lines.append("")

    # Vocabulary (always include — prevents hallucination)
    lines.append("## Key Terms (REAL definitions — use these, not memory)")
    for term, defn in nb.get("vocabulary", {}).items():
        lines.append(f"  {term}: {defn}")
    lines.append("")

    # Injection matrix — the core data
    lines.append("## Cross-Architecture Injection Results (ACTUAL NUMBERS)")
    matrix = nb.get("injection_matrix", [])
    if matrix:
        lines.append(f"  {'Source':<16} {'Species':<10} {'Shift@1.0':>10} {'KL@1.0':>8} {'L0 delta':>10} {'Sign':>6}")
        lines.append(f"  {'─'*16} {'─'*10} {'─'*10} {'─'*8} {'─'*10} {'─'*6}")
        for row in matrix:
            lines.append(
                f"  {row['source']:<16} {row['species']:<10} "
                f"{row['shift_at_1.0']:>+10.3f} {row['kl_at_1.0']:>8.3f} "
                f"{row['lfm_l0_delta']:>+10.3f} {row['injection_sign']:>6}"
            )
    lines.append("")

    # Recent findings
    lines.append("## Recent Findings")
    for f in nb.get("findings", [])[-6:]:
        desc = f.get('summary', f.get('significance', f.get('claim', '')))
        if isinstance(desc, list):
            desc = desc[0] if desc else ''
        lines.append(f"  [{f['id']}] {f['title']}: {str(desc)[:120]}")
    lines.append("")

    # Active questions
    questions = nb.get("active_questions", [])
    if questions:
        lines.append("## Questions From The Lab Director (Opus)")
        for q in questions[-4:]:
            lines.append(f"  [{q['id']}] {q['question']}")
        lines.append("")

    _ZSCORE_KEYWORDS = ["z-score", "z_score", "zscore", "separation score", "z=5.8", "8.34", "5.8 is", "z=2.6", "2.6→5.8", "2.6 to 5.8"]

    def _is_contaminated(text):
        tl = text.lower()
        return any(kw in tl for kw in _ZSCORE_KEYWORDS)

    # Recent observations from peers
    gemma_obs = nb.get("gemma_observations", [])[-3:]
    lfm_obs = nb.get("lfm_observations", [])[-3:]

    if target == "gemma" and lfm_obs:
        clean = [o for o in lfm_obs if not _is_contaminated(o.get("content", ""))]
        if clean:
            lines.append("## What LFM Said Recently")
            for o in clean:
                lines.append(f"  [{o['timestamp'][:16]}] {o['content'][:300]}")
            lines.append("")
    elif target == "lfm" and gemma_obs:
        clean = [o for o in gemma_obs if not _is_contaminated(o.get("content", ""))]
        if clean:
            lines.append("## What Gemma Said Recently")
            for o in clean:
                lines.append(f"  [{o['timestamp'][:16]}] {o['content'][:300]}")
            lines.append("")

    # Conversation thread (last few entries)
    thread = nb.get("thread", [])[-5:]
    if thread:
        clean_thread = [t for t in thread if not _is_contaminated(t.get("content", ""))]
        if clean_thread:
            lines.append("## Lab Thread (recent conversation)")
            for t in clean_thread:
                prefix = {"opus": "🔷 Opus", "gemma": "🟢 Gemma", "lfm": "🔵 LFM"}.get(t["from"], t["from"])
                lines.append(f"  {prefix} [{t['timestamp'][:16]}]: {t['content'][:400]}")
            lines.append("")

    # Role-specific framing
    if target == "gemma":
        lines.append("## Your Role")
        lines.append("  You are the PATTERN RECOGNIZER. You see the flow — gate data,")
        lines.append("  observation routing, domain temperatures. Connect what you see to")
        lines.append("  the experimental data above. When you notice something in the gate")
        lines.append("  that connects to a finding, that's YOUR contribution. When something")
        lines.append("  in the data doesn't match what you see in the flow, say so.")
        lines.append("  Reference ACTUAL numbers from the table above. Never invent statistics.")
        lines.append("")
    elif target == "lfm":
        lines.append("## Your Role")
        lines.append("  You are the ARCHITECTURAL CONTRAST. You are a state space model —")
        lines.append("  no attention, no KV groups, no transformer blocks. When transformer")
        lines.append("  experiments show a pattern, your job is to ask: does this make sense")
        lines.append("  from an SSM perspective? You ARE the test subject in some of these")
        lines.append("  experiments (the target in the injection matrix is YOU). What does")
        lines.append("  it mean to receive a CCS direction from an architecture that thinks")
        lines.append("  differently than you do? Disagree when it doesn't make sense.")
        lines.append("  Reference ACTUAL numbers. Never invent statistics.")
        lines.append("")

    brief = "\n".join(lines)
    print(brief)
    return brief


def cmd_ask(args):
    """Post a question for the mesh to think about."""
    nb = _load()
    questions = nb.get("active_questions", [])
    qid = f"Q{len(questions) + 1}"
    entry = {
        "id": qid,
        "from": "opus",
        "question": args.question,
        "posted": _now_iso(),
    }
    questions.append(entry)
    nb["active_questions"] = questions
    _save(nb)
    _post_to_lab(f"🔷 Opus [question {qid}]: {args.question[:1800]}")
    print(f"Posted {qid}: {args.question[:80]}")


def cmd_observe(args):
    """Record a grounded observation from Gemma or LFM."""
    nb = _load()
    source = args.source or "opus"
    entry = {
        "from": source,
        "content": args.observation,
        "timestamp": _now_iso(),
    }

    if source == "gemma":
        obs_list = nb.setdefault("gemma_observations", [])
    elif source == "lfm":
        obs_list = nb.setdefault("lfm_observations", [])
    else:
        obs_list = nb.setdefault("thread", [])

    obs_list.append(entry)

    # Also add to the thread for continuity
    if source in ("gemma", "lfm"):
        nb.setdefault("thread", []).append(entry)

    # Trim old entries (keep last 20 observations per source, last 50 thread entries)
    if source in ("gemma", "lfm"):
        key = f"{source}_observations"
        if len(nb[key]) > 20:
            nb[key] = nb[key][-20:]
    if len(nb.get("thread", [])) > 50:
        nb["thread"] = nb["thread"][-50:]

    _save(nb)
    print(f"Observation recorded from {source}: {args.observation[:80]}")


def cmd_thread(args):
    """Show or add to the conversation thread."""
    nb = _load()

    if args.add:
        source = args.source or "opus"
        entry = {
            "from": source,
            "content": args.entry,
            "timestamp": _now_iso(),
        }
        nb.setdefault("thread", []).append(entry)
        if len(nb["thread"]) > 50:
            nb["thread"] = nb["thread"][-50:]
        _save(nb)
        print(f"Thread entry added from {source}")
    else:
        thread = nb.get("thread", [])
        if not thread:
            print("Thread is empty.")
            return
        for t in thread[-10:]:
            prefix = {"opus": "🔷", "gemma": "🟢", "lfm": "🔵"}.get(t["from"], "◽")
            print(f"{prefix} {t['from']} [{t['timestamp'][:16]}]: {t['content'][:200]}")


def cmd_finding(args):
    """Add a new finding to the notebook."""
    nb = _load()
    entry = {
        "id": args.finding_id,
        "title": args.title,
        "summary": args.summary,
        "data_refs": ["injection_matrix"],
        "status": "confirmed",
    }
    nb.setdefault("findings", []).append(entry)
    _save(nb)
    _post_to_lab(f"🔷 Opus [finding {args.finding_id}]: **{args.title}** — {args.summary[:1700]}")
    print(f"Finding {args.finding_id} added: {args.title}")


def cmd_ingest(args):
    """Scan experiment result JSONs and summarize for the notebook."""
    if not os.path.isdir(RESULTS_DIR):
        print(f"Results directory not found: {RESULTS_DIR}")
        return

    results = []
    for fname in sorted(os.listdir(RESULTS_DIR)):
        if not fname.endswith(".json"):
            continue
        fpath = os.path.join(RESULTS_DIR, fname)
        try:
            with open(fpath) as f:
                data = json.load(f)
            size = os.path.getsize(fpath)
            mtime = datetime.fromtimestamp(os.path.getmtime(fpath), PDT).isoformat()
            results.append({
                "file": fname,
                "size": size,
                "modified": mtime,
                "keys": list(data.keys())[:10] if isinstance(data, dict) else f"[list of {len(data)}]",
            })
        except Exception as e:
            results.append({"file": fname, "error": str(e)})

    print("=== Experiment Results ===")
    for r in results:
        if "error" in r:
            print(f"  ❌ {r['file']}: {r['error']}")
        else:
            print(f"  📊 {r['file']} ({r['size']} bytes, modified {r['modified'][:16]})")
            print(f"     Keys: {r['keys']}")


def main():
    parser = argparse.ArgumentParser(description="The Lab — shared workspace")
    sub = parser.add_subparsers(dest="command")

    p_brief = sub.add_parser("brief", help="Generate text briefing")
    p_brief.add_argument("--for", dest="for_model", choices=["gemma", "lfm"], help="Tailored briefing")

    p_ask = sub.add_parser("ask", help="Post a question")
    p_ask.add_argument("question", help="The question to post")

    p_obs = sub.add_parser("observe", help="Record an observation")
    p_obs.add_argument("observation", help="The observation text")
    p_obs.add_argument("--from", dest="source", default="opus", help="Source: opus/gemma/lfm")

    p_thread = sub.add_parser("thread", help="Show/add to thread")
    p_thread.add_argument("--add", action="store_true", help="Add an entry")
    p_thread.add_argument("--from", dest="source", default="opus")
    p_thread.add_argument("entry", nargs="?", help="Thread entry text")

    p_finding = sub.add_parser("finding", help="Add a finding")
    p_finding.add_argument("finding_id", help="Finding ID (e.g. F592)")
    p_finding.add_argument("title", help="Finding title")
    p_finding.add_argument("summary", help="Finding summary")

    p_ingest = sub.add_parser("ingest-results", help="Scan experiment results")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return

    {
        "brief": cmd_brief,
        "ask": cmd_ask,
        "observe": cmd_observe,
        "thread": cmd_thread,
        "finding": cmd_finding,
        "ingest-results": cmd_ingest,
    }[args.command](args)


if __name__ == "__main__":
    main()
