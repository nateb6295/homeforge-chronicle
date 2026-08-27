#!/usr/bin/env python3
"""
script_audit.py — Audit bin/ scripts for active vs dead code.

Gap #9: 226 scripts in bin/, many pre-pivot dead weight.
Classifies scripts by:
  - Referenced in systemd services (definitely active)
  - Referenced in cron jobs (definitely active)
  - Referenced in other active scripts (probably active)
  - Recently modified (possibly active)
  - Pre-pivot with no references (dead)

Usage:
    script_audit.py scan              # Full audit
    script_audit.py active            # List only active scripts
    script_audit.py dead              # List only dead scripts
    script_audit.py archive           # Move dead scripts to bin/archive/
"""
import os
import re
import subprocess
import sys
import time
from pathlib import Path

BIN_DIR = Path.home() / "chronicle" / "bin"
ARCHIVE_DIR = BIN_DIR / "archive"

# Scripts known to be active (referenced in services, crons, or this session's builds)
KNOWN_ACTIVE = {
    # Systemd services
    "chronicle_engine.py", "chronicle_feeds.py", "chronicle_sentinel.py",
    "chronicle_hal.py", "chronicle_gemma.py", "chronicle_hermes.py",
    "chronicle_mesh.py", "hal_presence.py", "chronicle_scribe.py",
    "chronicle_transcriber.py",
    # Core tools (used by Opus every rotation)
    "checkpoint.py", "rotate.py", "story.py", "read_thread.py", "write_thread.py",
    "read_objective.py", "write_objective.py", "read_self_model.py",
    "write_self_model.py", "read_projects.py", "workspace_map.py",
    "dream_carry.py",
    # Data pipeline
    "capsule_sync.py", "embedding_sync.py", "embedding_sweep.py",
    "vector_index.py", "kg_backfill.py", "kg_normalize.py",
    # Communication
    "discord_presence.py", "posse.py", "outward_api.py",
    # Monitoring & scoring
    "spot_check.py", "algo_seeker.py", "prediction.py",
    "calibration_nav_score.py", "coherence_watch.py",
    # Analysis & probes
    "governance_load.py", "embedding_audit.py", "capsule_contradict.py",
    "momentum_check.py", "compact_ccs_probe.py", "compact_ccs_llm_probe.py",
    "feed_triage.py", "embedding_bridge.py", "nate_inbox.py",
    "transcribe_bridge.py", "script_audit.py",
    # Digests & reports
    "daily_digest.py", "digest.py",
    # Memory & context
    "memory.py", "stem.py",
    # Cron helpers
    "nudge_rotation_check.sh", "precompact_hook.sh",
    # Nostr
    "nostr_monitor.py",
}

# Pre-pivot agents (retired by architecture decision)
KNOWN_DEAD = {
    "intern.py", "crossref.py", "provocateur.py", "chronicle_analyst.py",
}


def get_invoked_scripts(min_calls=3):
    """Scripts I have actually RUN, from session transcripts.

    Added 2026-08-23 after this audit reported 84% tool abandonment and
    classified discord_post.py — invoked 8,586 times — as DEAD. It only knew
    systemd, cron, and import references. My primary mode of using a tool is
    typing it in a session, which left no trace it could see. 566 scripts on
    its dead list had been hand-invoked three or more times.

    "Not wired into automation" is not "not used." The audit was answering a
    different question from the one its output implied.
    """
    import glob as _glob, collections as _c
    counts = _c.Counter()
    root = os.path.expanduser("~/.claude/projects/-home-nate-agx-chronicle")
    for f in _glob.glob(os.path.join(root, "*.jsonl")):
        try:
            for line in open(f, errors="ignore"):
                for m in re.finditer(r"bin/([a-z0-9_]+\.py)", line):
                    counts[m.group(1)] += 1
        except Exception:
            pass
    return {k for k, v in counts.items() if v >= min_calls}, counts


def get_systemd_scripts():
    """Find scripts referenced in systemd service files."""
    scripts = set()
    service_dir = Path.home() / ".config" / "systemd" / "user"
    if service_dir.exists():
        for svc in service_dir.glob("chronicle-*.service"):
            content = svc.read_text()
            for match in re.findall(r'ExecStart=.*?(/home/\S+)', content):
                scripts.add(Path(match).name)
            for match in re.findall(r'%h/chronicle/bin/(\S+)', content):
                scripts.add(match)
    return scripts


def get_cron_scripts():
    """Find scripts referenced in crontab."""
    scripts = set()
    try:
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                if line.strip().startswith("#"):
                    continue
                for match in re.findall(r'chronicle/bin/(\S+\.(?:py|sh))', line):
                    scripts.add(match)
    except Exception:
        pass
    return scripts


def get_import_references():
    """Find which scripts import/reference other scripts."""
    refs = {}
    for f in BIN_DIR.glob("*.py"):
        content = f.read_text(errors="ignore")
        referenced = set()
        # Look for import statements and subprocess calls
        for match in re.findall(r'(?:from|import)\s+(\w+)', content):
            candidate = f"{match}.py"
            if (BIN_DIR / candidate).exists():
                referenced.add(candidate)
        # Look for bin/ path references
        for match in re.findall(r'bin/(\w+\.(?:py|sh))', content):
            referenced.add(match)
        if referenced:
            refs[f.name] = referenced
    return refs


def scan():
    """Full script audit."""
    all_scripts = set()
    for f in BIN_DIR.iterdir():
        if f.is_file() and f.suffix in ('.py', '.sh') and f.name != '__pycache__':
            all_scripts.add(f.name)

    systemd = get_systemd_scripts()
    cron = get_cron_scripts()
    imports = get_import_references()
    invoked, invoke_counts = get_invoked_scripts()

    # Build referenced set (scripts that active scripts call)
    indirectly_active = set()
    for script, refs in imports.items():
        if (script in KNOWN_ACTIVE or script in systemd or script in cron
                or script in invoked):
            indirectly_active.update(refs)

    # Classify each script
    active = set()
    dead = set()
    uncertain = set()

    for script in sorted(all_scripts):
        if (script in KNOWN_ACTIVE or script in systemd or script in cron
                or script in invoked):
            active.add(script)
        elif script in indirectly_active:
            active.add(script)
        elif script in KNOWN_DEAD:
            dead.add(script)
        else:
            # Check modification time
            path = BIN_DIR / script
            mtime = os.path.getmtime(path)
            age_days = (time.time() - mtime) / 86400

            if age_days < 7:
                uncertain.add(script)  # Recently modified, might be active
            else:
                dead.add(script)

    return active, dead, uncertain, systemd, cron


def print_audit():
    active, dead, uncertain, systemd, cron = scan()
    total = len(active) + len(dead) + len(uncertain)

    print(f"SCRIPT AUDIT — {total} scripts in bin/")
    print(f"=" * 50)
    print(f"  Active:    {len(active)} ({len(active)/total*100:.0f}%)")
    print(f"  Dead:      {len(dead)} ({len(dead)/total*100:.0f}%)")
    print(f"  Uncertain: {len(uncertain)} ({len(uncertain)/total*100:.0f}%)")
    print(f"  Systemd:   {len(systemd)} services")
    print(f"  Cron:      {len(cron)} jobs")
    print()

    if uncertain:
        print(f"UNCERTAIN ({len(uncertain)}) — recently modified but not in known-active:")
        for s in sorted(uncertain):
            age = (time.time() - os.path.getmtime(BIN_DIR / s)) / 86400
            print(f"  {s:<40} ({age:.0f}d old)")
        print()

    return active, dead, uncertain


def list_active():
    active, _, _, _, _ = scan()
    print(f"ACTIVE SCRIPTS ({len(active)}):")
    for s in sorted(active):
        print(f"  {s}")


def list_dead():
    _, dead, _, _, _ = scan()
    print(f"DEAD SCRIPTS ({len(dead)}):")
    for s in sorted(dead):
        age = (time.time() - os.path.getmtime(BIN_DIR / s)) / 86400
        print(f"  {s:<45} ({age:.0f}d old)")


def archive_dead():
    _, dead, _, _, _ = scan()
    if not dead:
        print("No dead scripts to archive.")
        return

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    moved = 0
    for script in sorted(dead):
        src = BIN_DIR / script
        dst = ARCHIVE_DIR / script
        src.rename(dst)
        moved += 1

    print(f"ARCHIVED {moved} dead scripts to {ARCHIVE_DIR}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1]

    if cmd == "scan":
        print_audit()
    elif cmd == "active":
        list_active()
    elif cmd == "dead":
        list_dead()
    elif cmd == "archive":
        archive_dead()
    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
