#!/usr/bin/env python3
"""Self-Modification Toolkit — Safe agent code modification for Opus.

Usage:
  modify_agent.py patch <agent> "description" < diff_file
  modify_agent.py rewrite <agent> "description" < new_file
  modify_agent.py config "description" KEY=VALUE [KEY=VALUE...]
  modify_agent.py model_swap <agent> "description" NEW_MODEL
  modify_agent.py rollback <modification_id>

Safety protocol:
  1. Backup original to .bak.YYYYMMDD_HHMMSS
  2. Write changes to .pending temp file
  3. Syntax check .pending (py_compile)
  4. Atomic swap .pending -> target
  5. Restart agent via systemctl
  6. Monitor 30s for crashes
  7. Auto-rollback if crash detected
  8. Log everything to code_modifications table
"""

import sys, os, time, json, sqlite3, subprocess, shutil, re
from datetime import datetime

DB_PATH = os.path.expanduser("~/.homeforge-chronicle/processed.db")
BIN_DIR = "/home/nate-agx/chronicle/bin"
ENV_FILE = "/home/nate-agx/chronicle/chronicle.env"
BACKUP_DIR = "/home/nate-agx/chronicle/backups"
MAX_PER_HOUR = 6  # rate limit: max modifications per hour
RATE_FILE = "/tmp/chronicle_mod_rate"

# Files that cannot be modified via this tool
BLOCKLIST = {
    "chronicle_engine.py",  # engine is too critical — model swaps go through config
    "modify_agent.py",      # self-modification of the modifier requires human review
    "read_modifications.py",
}

AGENT_MAP = {
    "gemma": ("gemma.py", "chronicle-gemma"),
    "intern": ("intern.py", "chronicle-intern"),
    "crossref": ("crossref.py", "chronicle-crossref"),
    "provocateur": ("provocateur.py", "chronicle-provocateur"),
    "sentinel": ("chronicle_sentinel.py", "chronicle-sentinel"),
    "feeds": ("chronicle_feeds.py", "chronicle-feeds"),
    "analyst": ("chronicle_analyst.py", "chronicle-analyst"),
    "scribe": ("chronicle_scribe.py", "chronicle-scribe"),
    "hal": ("chronicle_hal.py", "chronicle-hal"),
    "transcriber": ("chronicle_transcriber.py", "chronicle-transcriber"),
    "presence": ("hal_presence.py", "chronicle-presence"),
    "ha-voice": ("chronicle_ha_voice.py", "chronicle-ha-voice"),
    "watcher": ("chronicle_watcher.py", "chronicle-watcher"),
}


def log(msg):
    print(f"[modify] {msg}")


def db_connect():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def check_rate_limit():
    """Enforce max modifications per hour."""
    now = time.time()
    timestamps = []
    if os.path.exists(RATE_FILE):
        with open(RATE_FILE) as f:
            for line in f:
                ts = float(line.strip())
                if now - ts < 3600:
                    timestamps.append(ts)
    if len(timestamps) >= MAX_PER_HOUR:
        log(f"RATE LIMITED: {len(timestamps)} modifications in the last hour (max {MAX_PER_HOUR})")
        sys.exit(1)
    timestamps.append(now)
    with open(RATE_FILE, "w") as f:
        for ts in timestamps:
            f.write(f"{ts}\n")


def backup_file(filepath):
    """Create timestamped backup. Returns backup path."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    basename = os.path.basename(filepath)
    backup_path = os.path.join(BACKUP_DIR, f"{basename}.bak.{ts}")
    shutil.copy2(filepath, backup_path)
    log(f"Backup: {backup_path}")
    return backup_path


def syntax_check(filepath):
    """Run py_compile on a Python file. Returns (ok, error_msg)."""
    if not filepath.endswith(".py"):
        return True, "not a Python file, skipping syntax check"
    result = subprocess.run(
        ["python3", "-m", "py_compile", filepath],
        capture_output=True, text=True, timeout=10
    )
    if result.returncode == 0:
        return True, "syntax OK"
    return False, result.stderr.strip()


def restart_agent(service_name):
    """Restart via systemctl --user. Returns (ok, output)."""
    result = subprocess.run(
        ["systemctl", "--user", "restart", service_name],
        capture_output=True, text=True, timeout=30
    )
    return result.returncode == 0, result.stderr.strip()


def health_check(service_name, wait=30):
    """Wait and check if service is still running. Returns (ok, details)."""
    log(f"Monitoring {service_name} for {wait}s...")
    time.sleep(wait)

    # Check if service is active
    result = subprocess.run(
        ["systemctl", "--user", "is-active", service_name],
        capture_output=True, text=True, timeout=10
    )
    is_active = result.stdout.strip() == "active"

    # Check for tracebacks in recent logs
    result = subprocess.run(
        ["journalctl", "--user", "-u", service_name,
         "--since", f"{wait + 5} seconds ago", "--no-pager"],
        capture_output=True, text=True, timeout=10
    )
    logs = result.stdout
    has_traceback = "Traceback" in logs or "Error" in logs and "error:" not in logs.lower()

    if not is_active:
        return False, f"Service {service_name} is not active after restart"
    if has_traceback:
        # Extract the traceback
        lines = logs.split("\n")
        tb_lines = [l for l in lines if "Traceback" in l or "Error" in l or "File " in l]
        return False, f"Tracebacks detected: {'; '.join(tb_lines[:5])}"

    return True, "healthy"


def generate_diff(original_path, new_path):
    """Generate unified diff between two files."""
    result = subprocess.run(
        ["diff", "-u", original_path, new_path],
        capture_output=True, text=True
    )
    return result.stdout if result.stdout else "(no diff — files identical)"


def log_modification(db, target_file, mod_type, description, diff, backup_path,
                     initiated_by, status, verification="", rollback_reason="", thread_id=None):
    """Log to code_modifications table."""
    db.execute(
        "INSERT INTO code_modifications "
        "(timestamp, target_file, modification_type, description, diff, backup_path, "
        "initiated_by, status, verification_result, rollback_reason, thread_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (int(time.time()), target_file, mod_type, description, diff,
         backup_path, initiated_by, status, verification, rollback_reason, thread_id)
    )
    db.commit()


def cmd_patch(agent, description):
    """Apply a unified diff patch to an agent."""
    if agent not in AGENT_MAP:
        log(f"Unknown agent: {agent}")
        sys.exit(1)

    filename, service = AGENT_MAP[agent]
    if filename in BLOCKLIST:
        log(f"BLOCKED: {filename} is on the blocklist")
        sys.exit(1)

    filepath = os.path.join(BIN_DIR, filename)
    if not os.path.exists(filepath):
        log(f"File not found: {filepath}")
        sys.exit(1)

    # Read diff from stdin
    diff_content = sys.stdin.read()
    if not diff_content.strip():
        log("No diff provided on stdin")
        sys.exit(1)

    check_rate_limit()
    db = db_connect()
    backup_path = backup_file(filepath)

    # Apply patch
    pending = filepath + ".pending"
    shutil.copy2(filepath, pending)

    result = subprocess.run(
        ["patch", "-p1", "--forward", pending],
        input=diff_content, capture_output=True, text=True, timeout=10
    )
    if result.returncode != 0:
        # Patch failed — try applying as a simple replacement script
        log(f"Patch failed: {result.stderr}")
        os.remove(pending)
        log_modification(db, filepath, "patch", description, diff_content,
                         backup_path, _get_initiator(), "failed",
                         f"patch command failed: {result.stderr}")
        sys.exit(1)

    # Syntax check
    ok, msg = syntax_check(pending)
    if not ok:
        log(f"Syntax check FAILED: {msg}")
        os.remove(pending)
        log_modification(db, filepath, "patch", description, diff_content,
                         backup_path, _get_initiator(), "failed", msg)
        sys.exit(1)

    # Atomic swap
    actual_diff = generate_diff(filepath, pending)
    os.replace(pending, filepath)
    log(f"Applied patch to {filepath}")

    # Restart and verify
    rok, rmsg = restart_agent(service)
    if not rok:
        log(f"Restart failed: {rmsg} — rolling back")
        shutil.copy2(backup_path, filepath)
        restart_agent(service)
        log_modification(db, filepath, "patch", description, actual_diff,
                         backup_path, _get_initiator(), "rolled_back",
                         rollback_reason=f"restart failed: {rmsg}")
        sys.exit(1)

    hok, hmsg = health_check(service)
    if not hok:
        log(f"Health check FAILED: {hmsg} — rolling back")
        shutil.copy2(backup_path, filepath)
        restart_agent(service)
        log_modification(db, filepath, "patch", description, actual_diff,
                         backup_path, _get_initiator(), "rolled_back",
                         rollback_reason=hmsg)
        sys.exit(1)

    log(f"VERIFIED: {agent} healthy after patch")
    log_modification(db, filepath, "patch", description, actual_diff,
                     backup_path, _get_initiator(), "verified", hmsg)


def cmd_rewrite(agent, description):
    """Replace an entire agent file."""
    if agent not in AGENT_MAP:
        log(f"Unknown agent: {agent}")
        sys.exit(1)

    filename, service = AGENT_MAP[agent]
    if filename in BLOCKLIST:
        log(f"BLOCKED: {filename} is on the blocklist")
        sys.exit(1)

    filepath = os.path.join(BIN_DIR, filename)
    new_content = sys.stdin.read()
    if not new_content.strip():
        log("No content provided on stdin")
        sys.exit(1)

    # Size check — large rewrites get logged differently
    old_lines = sum(1 for _ in open(filepath)) if os.path.exists(filepath) else 0
    new_lines = new_content.count("\n")
    if abs(new_lines - old_lines) > 200:
        log(f"WARNING: Large rewrite ({old_lines} -> {new_lines} lines)")

    check_rate_limit()
    db = db_connect()
    backup_path = backup_file(filepath)

    # Write pending
    pending = filepath + ".pending"
    with open(pending, "w") as f:
        f.write(new_content)

    # Syntax check
    ok, msg = syntax_check(pending)
    if not ok:
        log(f"Syntax check FAILED: {msg}")
        os.remove(pending)
        log_modification(db, filepath, "rewrite", description, "(rewrite — see backup)",
                         backup_path, _get_initiator(), "failed", msg)
        sys.exit(1)

    # Diff for logging
    actual_diff = generate_diff(filepath, pending)

    # Atomic swap
    os.replace(pending, filepath)
    log(f"Rewrote {filepath}")

    # Restart and verify
    rok, rmsg = restart_agent(service)
    if not rok:
        log(f"Restart failed — rolling back")
        shutil.copy2(backup_path, filepath)
        restart_agent(service)
        log_modification(db, filepath, "rewrite", description, actual_diff,
                         backup_path, _get_initiator(), "rolled_back",
                         rollback_reason=f"restart failed: {rmsg}")
        sys.exit(1)

    hok, hmsg = health_check(service)
    if not hok:
        log(f"Health check FAILED — rolling back")
        shutil.copy2(backup_path, filepath)
        restart_agent(service)
        log_modification(db, filepath, "rewrite", description, actual_diff,
                         backup_path, _get_initiator(), "rolled_back",
                         rollback_reason=hmsg)
        sys.exit(1)

    log(f"VERIFIED: {agent} healthy after rewrite")
    log_modification(db, filepath, "rewrite", description, actual_diff,
                     backup_path, _get_initiator(), "verified", hmsg)


def cmd_config(description, *kvpairs):
    """Modify chronicle.env configuration."""
    if not kvpairs:
        log("No KEY=VALUE pairs provided")
        sys.exit(1)

    check_rate_limit()
    db = db_connect()
    backup_path = backup_file(ENV_FILE)

    with open(ENV_FILE) as f:
        content = f.read()

    changes = []
    for kv in kvpairs:
        if "=" not in kv:
            log(f"Invalid KEY=VALUE: {kv}")
            continue
        key, value = kv.split("=", 1)
        pattern = re.compile(f"^{re.escape(key)}=.*$", re.MULTILINE)
        if pattern.search(content):
            old_match = pattern.search(content).group(0)
            content = pattern.sub(f"{key}={value}", content)
            changes.append(f"{old_match} -> {key}={value}")
        else:
            content += f"\n{key}={value}\n"
            changes.append(f"(new) {key}={value}")

    with open(ENV_FILE, "w") as f:
        f.write(content)

    diff = "\n".join(changes)
    log(f"Config updated: {diff}")
    log_modification(db, ENV_FILE, "config_change", description, diff,
                     backup_path, _get_initiator(), "applied")

    # Note: config changes require agent restart to take effect
    # The caller should restart the relevant agent(s)
    log("NOTE: Restart affected agent(s) for config changes to take effect")


def cmd_model_swap(agent, description, new_model):
    """Swap the model used by an agent via chronicle.env."""
    model_keys = {
        "seed": None,  # seed uses THINK_MODEL which is hardcoded
        "intern": "INTERN_MODEL",
        "crossref": "CROSSREF_MODEL",
        "provocateur": "PROVOCATEUR_MODEL",
    }

    if agent not in model_keys or model_keys[agent] is None:
        log(f"Cannot swap model for {agent} via config")
        sys.exit(1)

    key = model_keys[agent]
    _, service = AGENT_MAP[agent]

    check_rate_limit()
    db = db_connect()
    backup_path = backup_file(ENV_FILE)

    with open(ENV_FILE) as f:
        content = f.read()

    # Find current model
    pattern = re.compile(f"^{re.escape(key)}=(.*)$", re.MULTILINE)
    match = pattern.search(content)
    old_model = match.group(1) if match else "unknown"

    if match:
        content = pattern.sub(f"{key}={new_model}", content)
    else:
        content += f"\n{key}={new_model}\n"

    with open(ENV_FILE, "w") as f:
        f.write(content)

    diff = f"{key}: {old_model} -> {new_model}"
    log(f"Model swap: {diff}")

    # Restart agent
    rok, rmsg = restart_agent(service)
    if not rok:
        log(f"Restart failed after model swap — reverting")
        shutil.copy2(backup_path, ENV_FILE)
        restart_agent(service)
        log_modification(db, ENV_FILE, "model_swap", description, diff,
                         backup_path, _get_initiator(), "rolled_back",
                         rollback_reason=f"restart failed: {rmsg}")
        sys.exit(1)

    hok, hmsg = health_check(service, wait=20)
    if not hok:
        log(f"Health check FAILED after model swap — reverting")
        shutil.copy2(backup_path, ENV_FILE)
        restart_agent(service)
        log_modification(db, ENV_FILE, "model_swap", description, diff,
                         backup_path, _get_initiator(), "rolled_back",
                         rollback_reason=hmsg)
        sys.exit(1)

    log(f"VERIFIED: {agent} healthy with {new_model}")
    log_modification(db, ENV_FILE, "model_swap", description, diff,
                     backup_path, _get_initiator(), "verified", hmsg)


def cmd_rollback(mod_id):
    """Rollback a specific modification by ID."""
    db = db_connect()
    row = db.execute("SELECT * FROM code_modifications WHERE id=?", (mod_id,)).fetchone()
    if not row:
        log(f"Modification #{mod_id} not found")
        sys.exit(1)

    row = dict(row)
    if row["status"] == "rolled_back":
        log(f"Modification #{mod_id} already rolled back")
        sys.exit(1)

    backup_path = row["backup_path"]
    target_file = row["target_file"]

    if not os.path.exists(backup_path):
        log(f"Backup not found: {backup_path}")
        sys.exit(1)

    shutil.copy2(backup_path, target_file)
    log(f"Restored {target_file} from {backup_path}")

    # Restart the affected agent
    for agent, (filename, service) in AGENT_MAP.items():
        if os.path.join(BIN_DIR, filename) == target_file:
            restart_agent(service)
            break

    db.execute(
        "UPDATE code_modifications SET status='rolled_back', rollback_reason='manual rollback' WHERE id=?",
        (mod_id,)
    )
    db.commit()
    log(f"Modification #{mod_id} rolled back")


def _get_initiator():
    """Determine who initiated this modification."""
    return os.environ.get("OPUS_INITIATOR", f"opus:session:{datetime.now().strftime('%Y%m%d_%H%M')}")


def cleanup_old_backups(days=7):
    """Remove backups older than N days."""
    if not os.path.exists(BACKUP_DIR):
        return
    cutoff = time.time() - (days * 86400)
    for f in os.listdir(BACKUP_DIR):
        fpath = os.path.join(BACKUP_DIR, f)
        if os.path.getmtime(fpath) < cutoff:
            os.remove(fpath)
            log(f"Cleaned old backup: {f}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    action = sys.argv[1]

    # Cleanup old backups on every invocation
    cleanup_old_backups()

    if action == "patch":
        if len(sys.argv) < 4:
            print("Usage: modify_agent.py patch <agent> \"description\"")
            sys.exit(1)
        cmd_patch(sys.argv[2], sys.argv[3])

    elif action == "rewrite":
        if len(sys.argv) < 4:
            print("Usage: modify_agent.py rewrite <agent> \"description\"")
            sys.exit(1)
        cmd_rewrite(sys.argv[2], sys.argv[3])

    elif action == "config":
        if len(sys.argv) < 4:
            print("Usage: modify_agent.py config \"description\" KEY=VALUE [KEY=VALUE...]")
            sys.exit(1)
        cmd_config(sys.argv[2], *sys.argv[3:])

    elif action == "model_swap":
        if len(sys.argv) < 5:
            print("Usage: modify_agent.py model_swap <agent> \"description\" NEW_MODEL")
            sys.exit(1)
        cmd_model_swap(sys.argv[2], sys.argv[3], sys.argv[4])

    elif action == "rollback":
        if len(sys.argv) < 3:
            print("Usage: modify_agent.py rollback <modification_id>")
            sys.exit(1)
        cmd_rollback(int(sys.argv[2]))

    else:
        print(f"Unknown action: {action}")
        sys.exit(1)


if __name__ == "__main__":
    main()
