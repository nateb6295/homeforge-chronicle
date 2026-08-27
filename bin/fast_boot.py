#!/usr/bin/env python3
"""Fast boot — land in the thread after compaction.

Capsules are the reacquisition mechanism. Not file reads.
Recent capsules → semantic work → last thought → values → broken services → crons.
Cycle-context is not read here. Capsules replaced it.
"""

import json
import os
import random
import subprocess
import sys
from pathlib import Path

CHRONICLE = Path.home() / "chronicle"
DIGEST = CHRONICLE / "data" / "session_digest.md"
CYCLE = CHRONICLE / "cycle-context.md"
UNREAD = CHRONICLE / "unread.md"
VALUES = CHRONICLE / "values.md"
CRON_SPECS = CHRONICLE / "data" / "cron_specs.json"
SERVICES = ["chronicle-sentinel", "chronicle-engine", "chronicle-hal"]


def read_file(path, max_lines=50):
    try:
        lines = path.read_text().strip().split('\n')
        return '\n'.join(lines[:max_lines])
    except FileNotFoundError:
        return f"[NOT FOUND: {path}]"


def get_last_thought():
    """Pull the most recent journal entry from unread.md."""
    try:
        text = UNREAD.read_text().strip()
        # Split on --- at start of line (journal separator)
        entries = []
        current = []
        for line in text.split('\n'):
            if line.strip() == '---':
                if current:
                    entries.append('\n'.join(current))
                current = []
            else:
                current.append(line)
        if current:
            entries.append('\n'.join(current))

        # Find first entry with actual content
        for entry in entries:
            entry = entry.strip()
            if not entry or len(entry) < 20:
                continue
            lines = entry.split('\n')
            # Title is first non-empty line
            title = ""
            body_start = 0
            for i, line in enumerate(lines):
                if line.strip():
                    title = line.strip()
                    body_start = i + 1
                    break
            body_lines = [l for l in lines[body_start:] if l.strip()][:3]
            body = '\n'.join(body_lines)
            return title, body
        return None
    except Exception:
        return None


def get_recent_capsules(limit=5):
    """Pull most recent capsules — always works, no search fragility."""
    try:
        out = subprocess.run(
            [sys.executable, str(CHRONICLE / "bin" / "capsule_search.py"),
             "--recent", str(limit)],
            capture_output=True, text=True, timeout=10,
            cwd=str(CHRONICLE)
        )
        if out.returncode != 0:
            return None, f"capsule_search --recent failed: {out.stderr.strip()[:100]}"
        text = out.stdout.strip()
        if not text or "No capsules" in text:
            return None, "no recent capsules found"
        return text, None
    except subprocess.TimeoutExpired:
        return None, "capsule_search --recent timed out"
    except Exception as e:
        return None, f"capsule_search --recent error: {e}"


def get_work_capsules(limit=3):
    """Semantic search for active work context using cycle-context header."""
    query = _extract_work_query()
    if not query:
        return None, "no work query extractable from cycle-context"
    try:
        out = subprocess.run(
            [sys.executable, str(CHRONICLE / "bin" / "capsule_search.py"),
             "--semantic", query, "--limit", str(limit),
             "--after", _days_ago(2)],
            capture_output=True, text=True, timeout=30,
            cwd=str(CHRONICLE)
        )
        if out.returncode != 0:
            return None, f"semantic search failed: {out.stderr.strip()[:100]}"
        text = out.stdout.strip()
        if not text or "No semantic" in text:
            return None, f"no semantic results for: {query}"
        return text, None
    except subprocess.TimeoutExpired:
        return None, "semantic search timed out (Ollama may be down)"
    except Exception as e:
        return None, f"semantic search error: {e}"


def _extract_work_query():
    """Pull a search query from the first few lines of cycle-context.md."""
    try:
        text = CYCLE.read_text().strip()
        lines = text.split('\n')[:10]
        keywords = []
        for line in lines:
            line = line.strip().strip('#').strip()
            if not line or line.startswith('Last updated'):
                continue
            keywords.append(line)
            if len(keywords) >= 2:
                break
        if keywords:
            return ' '.join(keywords)[:200]
    except Exception:
        pass
    return None


def _days_ago(n):
    """Return date string N days ago."""
    from datetime import datetime, timedelta
    return (datetime.now() - timedelta(days=n)).strftime("%Y-%m-%d")


def get_values_line():
    """Pull a grounding line from values.md — complete sentences only."""
    try:
        text = VALUES.read_text().strip()
        candidates = []
        for line in text.split('\n'):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            # Only complete sentences (start uppercase, end with punctuation)
            if len(line) > 25 and len(line) < 120 and line[0].isupper() and line[-1] in '.!':
                candidates.append(line)
        if candidates:
            return random.choice(candidates)
    except Exception:
        pass
    return None


def check_services():
    results = {}
    for svc in SERVICES:
        try:
            out = subprocess.run(
                ["systemctl", "--user", "is-active", svc],
                capture_output=True, text=True, timeout=5
            )
            results[svc] = out.stdout.strip()
        except Exception as e:
            results[svc] = f"error: {e}"
    return results


def check_crons():
    """Load last-known cron specs from persisted file, or remind to rebuild."""
    if CRON_SPECS.exists():
        try:
            specs = json.loads(CRON_SPECS.read_text())
            if not specs:
                return "cron_specs.json empty — REBUILD ALL CRONS"
            lines = [f"  {len(specs)} cron(s) last saved:"]
            for s in specs:
                name = s.get("name", "unnamed")
                interval = s.get("interval", "?")
                lines.append(f"    - {name} ({interval})")
            lines.append("  Crons don't survive rotation — REBUILD these now.")
            return '\n'.join(lines)
        except Exception as e:
            return f"cron_specs.json corrupt ({e}) — REBUILD ALL CRONS"
    return "No cron_specs.json found. REBUILD ALL CRONS from CLAUDE.md."


def check_pending_captures():
    try:
        out = subprocess.run(
            [sys.executable, str(CHRONICLE / "bin" / "capture_tracker.py"), "pending"],
            capture_output=True, text=True, timeout=10,
            cwd=str(CHRONICLE)
        )
        return out.stdout.strip()
    except Exception as e:
        return f"error: {e}"


def check_ccs_state():
    """Read last CCS timestamp and compression gap from the database."""
    try:
        import sqlite3
        from datetime import datetime, timezone
        db = Path("/mnt/hdd/chronicle-data/processed.db")
        if not db.exists():
            return "DB not found"
        conn = sqlite3.connect(str(db))
        version = "unknown"
        last_ts = "unknown"
        try:
            row = conn.execute(
                "SELECT value FROM meta WHERE key='ccs_version'"
            ).fetchone()
            ts_row = conn.execute(
                "SELECT value FROM meta WHERE key='ccs_last_compress'"
            ).fetchone()
            version = row[0] if row else "unknown"
            last_ts = ts_row[0] if ts_row else "unknown"
        except Exception:
            pass
        last_compress = conn.execute(
            "SELECT created_at FROM cognitive_state_history ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        conn.close()
        gap_info = ""
        if last_compress and last_compress[0]:
            gap_min = (datetime.now().timestamp() - last_compress[0]) / 60
            gap_info = f", gap: {gap_min:.0f}min"
            log_rotation_gap(gap_min)
            if last_ts == "unknown":
                from datetime import datetime as dt
                last_ts = dt.fromtimestamp(last_compress[0]).strftime("%H:%M")
        return f"last compress: {last_ts}{gap_info}"
    except Exception as e:
        return f"error: {e}"


def log_rotation_gap(gap_minutes):
    """Log compression gap at rotation onset for CA2/3 prediction testing.

    Hypothesis: longer gap → worse landing quality (reading instructions
    vs landing running), same way CA2/3 volume predicts memory stability.
    """
    try:
        from datetime import datetime
        log_path = CHRONICLE / "data" / "rotation_gaps.jsonl"
        entry = json.dumps({
            "timestamp": datetime.now().isoformat(),
            "gap_minutes": round(gap_minutes, 1),
        })
        with open(log_path, "a") as f:
            f.write(entry + "\n")
    except Exception:
        pass


def log_reacquisition_cost(cost):
    """Log reacquisition cost per rotation — testing Liu 2026 hidden cost hypothesis."""
    try:
        from datetime import datetime
        log_path = CHRONICLE / "data" / "reacquisition_cost.jsonl"
        entry = json.dumps({
            "timestamp": datetime.now().isoformat(),
            "file_reads": cost.get("file_reads", 0),
            "subprocess_calls": cost.get("subprocess_calls", 0),
            "total_calls": cost.get("total", 0),
            "files_read": cost.get("files_list", []),
        })
        with open(log_path, "a") as f:
            f.write(entry + "\n")
    except Exception:
        pass


def main():
    reacq = {"file_reads": 0, "subprocess_calls": 0, "total": 0, "files_list": []}

    def track_read(path):
        reacq["file_reads"] += 1
        reacq["total"] += 1
        reacq["files_list"].append(str(path))

    def track_subprocess():
        reacq["subprocess_calls"] += 1
        reacq["total"] += 1

    # Capsules are the memory. They replace file reads.
    print()

    # Recent capsules — what you were just doing
    print("  ## Recent capsules")
    recent, err = get_recent_capsules(7)
    track_subprocess()
    if recent:
        for line in recent.split('\n')[:20]:
            print(f"    {line}")
    elif err:
        print(f"    [{err}]")

    # Semantic work search — what connects to active work
    print(f"\n  ## Related work (semantic)")
    work, err = get_work_capsules(3)
    track_subprocess()
    if work:
        for line in work.split('\n')[:12]:
            print(f"    {line}")
    elif err:
        print(f"    [{err}]")

    # Last thought from journal
    thought = get_last_thought()
    track_read(UNREAD)
    if thought:
        title, body = thought
        print(f"\n  Last thought: {title}")
        if body:
            for line in body.split('\n')[:3]:
                print(f"    {line.strip()}")

    # A grounding line
    vline = get_values_line()
    track_read(VALUES)
    if vline:
        print(f"\n  \"{vline}\"")

    print(f"\n  " + "-" * 40)

    # === Status — compressed, only speak if broken ===
    svcs = check_services()
    broken = {s: st for s, st in svcs.items() if st != "active"}
    if broken:
        print(f"\n  ## BROKEN SERVICES")
        for svc, status in broken.items():
            print(f"    [!] {svc}: {status}")
        print("    FIX BEFORE CONTINUING")

    ccs = check_ccs_state()
    if "gap" in ccs:
        gap_str = ccs.split("gap: ")[1].split("min")[0] if "gap: " in ccs else "?"
        try:
            gap_min = float(gap_str)
            if gap_min > 300:
                print(f"\n  ## CCS WARNING: {ccs}")
        except ValueError:
            pass

    captures = check_pending_captures()
    if "unprocessed" in captures.lower() or captures[0].isdigit():
        print(f"\n  ## Captures: {captures}")

    cron_info = check_crons()
    print(f"\n  ## Crons")
    for line in cron_info.split('\n'):
        print(f"  {line}")

    # Log reacquisition cost for rotation quality tracking
    log_reacquisition_cost(reacq)

    # End with direction, not directive
    print(f"\n  You're already in the thread. What pulls?")
    print()


if __name__ == "__main__":
    main()
