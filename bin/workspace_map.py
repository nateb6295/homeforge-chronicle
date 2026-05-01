#!/usr/bin/env python3
"""Workspace Map — compact textual index of Chronicle's file-as-bus state.

Emits a one-screen map of what exists, how big, how fresh. Meant to be read
by an incoming Opus instance BEFORE any targeted file reads, giving it a
navigational index rather than requiring it to re-derive the layout from
CLAUDE.md prose every rotation.

Inspired by the AiScientist (arxiv 2604.13018) workspace-map pattern:
`m_t = M(W_t)` — a lightweight textual index, not a lossy replacement for
the workspace itself. The map points at files; targeted reads fetch content.

Runs locally, no canister calls, no pipeline writes. Read-only.
"""
from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

HOME = Path.home()
CHRON = HOME / "chronicle"
TRACES = CHRON / "traces"

# (path, one-line role). Order = logical reading order on rotation.
FILES = [
    (CHRON / "session-state.md", "handoff from previous instance — READ FIRST"),
    (HOME / ".claude/projects/-home-nate-agx-chronicle/memory/MEMORY.md", "auto-memory index (cross-session)"),
    (CHRON / "opus-story.md", "continuing narrative — identity, not log"),
    (CHRON / "opus-board.md", "self-set directives + who-I-am section"),
    (CHRON / "nate-board.md", "Nate's persistent directives"),
    (CHRON / "cycle-context.md", "last-cycle learning snapshot"),
    (CHRON / "CLAUDE.md", "ritual / startup-sequence / tool discipline"),
]


def _human_age(mtime: float) -> str:
    secs = int(time.time() - mtime)
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m"
    if secs < 86400:
        return f"{secs // 3600}h"
    return f"{secs // 86400}d"


def _human_size(n: int) -> str:
    if n < 1024:
        return f"{n}B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f}KB"
    return f"{n / (1024 * 1024):.1f}MB"


def file_row(path: Path, role: str) -> str:
    if not path.exists():
        return f"  [MISSING]  {path.name:<22}  — {role}"
    st = path.stat()
    return f"  {_human_size(st.st_size):>8}  {_human_age(st.st_mtime):>5}  {path.name:<22}  — {role}"


def recent_traces(n: int = 5) -> list[str]:
    if not TRACES.is_dir():
        return ["  (traces/ missing)"]
    files = sorted(TRACES.glob("*.md"), key=lambda p: p.stat().st_mtime, reverse=True)[:n]
    rows = []
    for f in files:
        rows.append(f"  {_human_age(f.stat().st_mtime):>5} ago  {f.name}")
    return rows


def active_thread_summary() -> str:
    try:
        out = subprocess.run(
            ["python3", str(CHRON / "bin/read_thread.py")],
            capture_output=True, text=True, timeout=10,
        )
        if out.returncode != 0:
            return "  (read_thread.py failed)"
        # Pull just the title + id line from the JSON
        import json
        try:
            data = json.loads(out.stdout)
            t = data.get("thread", {})
            adv = data.get("advancements", [])
            last_adv_age = ""
            if adv:
                last = adv[-1]
                ts = last.get("created_at") or last.get("timestamp")
                if ts:
                    try:
                        last_adv_age = f" (last advance {_human_age(int(ts))} ago)"
                    except Exception:
                        pass
            return f"  #{t.get('id','?')}  {t.get('title','?')}{last_adv_age}"
        except Exception:
            return "  (thread JSON parse failed)"
    except Exception as e:
        return f"  (thread query error: {e})"


def git_recent() -> list[str]:
    try:
        out = subprocess.run(
            ["git", "-C", str(CHRON), "log", "--oneline", "-5"],
            capture_output=True, text=True, timeout=5,
        )
        if out.returncode == 0:
            return [f"  {l}" for l in out.stdout.strip().split("\n")[:5]]
    except Exception:
        pass
    return ["  (git log unavailable)"]


def main():
    print("# Chronicle Workspace Map")
    print(f"# generated {time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print()
    print("## Core state files (size, age, role)")
    for path, role in FILES:
        print(file_row(path, role))
    print()
    print("## Active thread")
    print(active_thread_summary())
    print()
    print("## Recent traces (most recent first)")
    for r in recent_traces():
        print(r)
    print()
    print("## Recent git commits")
    for r in git_recent():
        print(r)
    print()
    print("# Targeted reads: use Read tool on any of the above paths.")
    print("# This map is the index, not the workspace.")


if __name__ == "__main__":
    main()
