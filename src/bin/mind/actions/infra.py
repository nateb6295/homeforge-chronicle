"""Chronicle Mind - Infrastructure action handlers (shell, projects, manifold, source editing, alerts)."""

import os
import json
import subprocess
import requests
from typing import Optional

from mind.utils import log, safe_truncate, now_ts, make_cycle_id
from mind.config import MANIFOLD_API, MANIFOLD_API_KEY, MANIFOLD_MAX_BET, MANIFOLD_MAX_CYCLE_SPEND, WORKING_DIR

# Per-cycle spend tracker for Manifold bets (reset each cycle via reset_manifold_cycle_spend)
_manifold_cycle_spend = 0


def reset_manifold_cycle_spend():
    global _manifold_cycle_spend
    _manifold_cycle_spend = 0


# ── Manifold Markets (Prediction Markets) ─────────────────────

def act_manifold_search(mind, action: dict, cid: str) -> str:
    query = action.get("query", "")
    limit = min(action.get("limit", 5), 10)
    log(f'  Executing: ManifoldSearch {{ query: "{safe_truncate(query, 60)}", limit: {limit} }}')
    if not MANIFOLD_API_KEY:
        return "false - MANIFOLD_API_KEY not configured"
    try:
        r = requests.get(
            f"{MANIFOLD_API}/search-markets",
            params={"term": query, "filter": "open", "sort": "score", "limit": limit},
            timeout=15,
        )
        r.raise_for_status()
        markets = r.json()
        results = []
        for m in markets[:limit]:
            prob = m.get("probability", 0)
            results.append(
                f"{m.get('question', '?')} [{prob:.0%}] "
                f"(vol:{m.get('volume', 0):.0f}, bettors:{m.get('uniqueBettorCount', 0)}, "
                f"id:{m.get('id', '?')})"
            )
        summary = " | ".join(results) if results else "No markets found"
        mind.db.log_activity("mind", "manifold_search", f"Search: {safe_truncate(query, 40)}",
                             safe_truncate(summary, 500))
        return f"true - {len(results)} markets: {safe_truncate(summary, 300)}"
    except Exception as e:
        return f"false - Manifold search failed: {e}"


def act_manifold_bet(mind, action: dict, cid: str) -> str:
    global _manifold_cycle_spend
    market_id = action.get("market_id", "")
    outcome = action.get("outcome", "YES").upper()
    amount = action.get("amount", 10)
    reason = action.get("reason", "")
    log(f'  Executing: ManifoldBet {{ market: "{safe_truncate(market_id, 20)}", '
        f'outcome: "{outcome}", amount: {amount} }}')
    if not MANIFOLD_API_KEY:
        return "false - MANIFOLD_API_KEY not configured"
    if outcome not in ("YES", "NO"):
        return "false - outcome must be YES or NO"
    # Safety guards
    amount = min(int(amount), MANIFOLD_MAX_BET)
    if amount <= 0:
        return "false - amount must be positive"
    if _manifold_cycle_spend + amount > MANIFOLD_MAX_CYCLE_SPEND:
        return f"false - Cycle spend limit reached ({_manifold_cycle_spend}/{MANIFOLD_MAX_CYCLE_SPEND})"
    try:
        r = requests.post(
            f"{MANIFOLD_API}/bet",
            headers={
                "Authorization": f"Key {MANIFOLD_API_KEY}",
                "Content-Type": "application/json",
            },
            json={"contractId": market_id, "outcome": outcome, "amount": amount},
            timeout=15,
        )
        r.raise_for_status()
        result = r.json()
        _manifold_cycle_spend += amount
        # Log the bet for calibration tracking
        mind.db.log_activity(
            "mind", "manifold_bet",
            f"Bet M${amount} {outcome} on {safe_truncate(market_id, 20)}",
            safe_truncate(reason, 300),
        )
        # Store as a memory for long-term tracking
        if mind.canister:
            mind.canister.store(
                f"Manifold bet: M${amount} {outcome} on market {market_id}. "
                f"Reason: {safe_truncate(reason, 200)}",
                "predictions/manifold",
                ["manifold", "prediction", "bet"],
            )
        prob = result.get("probAfter", result.get("probability", "?"))
        return f"true - Bet M${amount} {outcome} (prob after: {prob}). Reason: {safe_truncate(reason, 80)}"
    except requests.HTTPError as e:
        body = e.response.text[:200] if e.response else str(e)
        return f"false - Manifold bet failed: {body}"
    except Exception as e:
        return f"false - Manifold bet failed: {e}"


def act_manifold_portfolio(mind, action: dict, cid: str) -> str:
    log("  Executing: ManifoldPortfolio")
    if not MANIFOLD_API_KEY:
        return "false - MANIFOLD_API_KEY not configured"
    try:
        me = requests.get(
            f"{MANIFOLD_API}/me",
            headers={"Authorization": f"Key {MANIFOLD_API_KEY}"},
            timeout=15,
        ).json()
        username = me.get("username", "?")
        balance = me.get("balance", 0)
        profit = me.get("profitCached", {})
        total_profit = profit.get("allTime", 0) if isinstance(profit, dict) else 0
        # Get recent bets for position count
        bets = requests.get(
            f"{MANIFOLD_API}/bets",
            params={"userId": me.get("id", ""), "limit": 50},
            headers={"Authorization": f"Key {MANIFOLD_API_KEY}"},
            timeout=15,
        ).json()
        return (
            f"true - @{username}: M${balance:.0f} balance, "
            f"M${total_profit:.0f} all-time profit, "
            f"{len(bets)} recent bets"
        )
    except Exception as e:
        return f"false - Portfolio check failed: {e}"


# ── Projects ─────────────────────────────────────────────────

def act_create_project(mind, action: dict, cid: str) -> str:
    title = action.get("title", action.get("name", ""))
    desc = action.get("description", "")
    log(f'  Executing: CreateProject {{ title: "{safe_truncate(title, 40)}" }}')
    mind.db.run(
        "INSERT INTO projects (name, description, status, created_at) VALUES (?, ?, 'active', ?)",
        (title, desc, now_ts()),
    )
    return f"true - Project created: {safe_truncate(title, 40)}"


def act_update_project(mind, action: dict, cid: str) -> str:
    pid = action.get("project_id", 0)
    update_type = action.get("update_type", "progress")
    content = action.get("content", "")
    log(f"  Executing: UpdateProject {{ id: {pid}, type: \"{update_type}\" }}")
    mind.db.run(
        "INSERT INTO project_updates (project_id, update_type, content, created_at) "
        "VALUES (?, ?, ?, ?)",
        (pid, update_type, content, now_ts()),
    )
    mind.db.run("UPDATE projects SET updated_at = ? WHERE id = ?", (now_ts(), pid))
    return f"true - Project {pid} updated ({update_type})"


def act_project_status(mind, action: dict, cid: str) -> str:
    pid = action.get("project_id", 0)
    status = action.get("status", "active")
    context = action.get("context", "")
    log(f"  Executing: ProjectStatus {{ id: {pid}, status: \"{status}\" }}")
    mind.db.run("UPDATE projects SET status = ?, updated_at = ? WHERE id = ?",
                 (status, now_ts(), pid))
    return f"true - Project {pid} status -> {status}"


# ── Shell Execution ──────────────────────────────────────────

def act_execute_shell(mind, action: dict, cid: str) -> str:
    command = action.get("command", "")
    working_dir = action.get("working_dir", WORKING_DIR)
    timeout = min(action.get("timeout_secs", 30), 60)  # cap at 60s
    log(f'  Executing: ExecuteShell {{ command: "{safe_truncate(command, 60)}" }}')

    # Safety: block destructive commands
    dangerous = ["rm -rf", "dd if=", "mkfs", "format", "> /dev/", "shutdown", "reboot"]
    if any(d in command.lower() for d in dangerous):
        return "false - Command blocked (destructive)"

    # Ensure working dir exists and is under home
    home = os.path.expanduser("~")
    if not os.path.isdir(working_dir) or not working_dir.startswith(home):
        if working_dir != WORKING_DIR:
            log(f"    Corrected invalid working_dir '{working_dir}' -> '{WORKING_DIR}'")
        working_dir = WORKING_DIR

    try:
        result = subprocess.run(
            command, shell=True, capture_output=True, text=True,
            timeout=timeout, cwd=working_dir,
        )
        output = result.stdout + result.stderr
        return f"true - Exit {result.returncode}: {safe_truncate(output, 200)}"
    except subprocess.TimeoutExpired:
        return f"false - Command timed out ({timeout}s)"
    except Exception as e:
        return f"false - Shell error: {e}"


# ── Alerts ───────────────────────────────────────────────────

def act_create_alert(mind, action: dict, cid: str) -> str:
    atype = action.get("alert_type", "price_above")
    symbol = action.get("symbol", "XRP")
    threshold = float(action.get("threshold", 0))
    name = action.get("name", f"{symbol} {atype} {threshold}")
    log(f'  Executing: CreateAlert {{ type: "{atype}", symbol: "{symbol}", threshold: {threshold} }}')
    mind.db.run(
        "INSERT INTO alerts (name, alert_type, symbol, threshold, active, created_at) "
        "VALUES (?, ?, ?, ?, 1, ?)",
        (name, atype, symbol, threshold, now_ts()),
    )
    return f"true - Alert created: {name}"


def act_dismiss_alert(mind, action: dict, cid: str) -> str:
    alert_id = action.get("alert_id", action.get("id", 0))
    log(f"  Executing: DismissAlert {{ id: {alert_id} }}")
    mind.db.run("UPDATE alerts SET active = 0 WHERE id = ?", (alert_id,))
    return f"true - Alert {alert_id} dismissed"


# ── Source Editing & Service Management ──────────────────────

def act_read_source_file(mind, action: dict, cid: str) -> str:
    path = action.get("file_path", "")
    log(f'  Executing: ReadSourceFile {{ path: "{safe_truncate(path, 60)}" }}')
    # Security: only allow reading within /home/nvidia
    if not path.startswith("/home/nvidia"):
        return "false - Can only read files under /home/nvidia"
    try:
        with open(path, "r") as f:
            content = f.read(10000)
        mind.db.log_activity("mind", "source_read", f"Read: {path}",
                             safe_truncate(content, 2000))
        return f"true - Read {len(content)} chars from {path}"
    except Exception as e:
        return f"false - Read failed: {e}"


def act_edit_source_file(mind, action: dict, cid: str) -> str:
    path = action.get("file_path", "")
    old_text = action.get("old_text", "")
    new_text = action.get("new_text", "")
    log(f'  Executing: EditSourceFile {{ path: "{safe_truncate(path, 60)}" }}')

    # Safety: only allow editing within /home/nvidia
    if not path.startswith("/home/nvidia"):
        return "false - Can only edit files under /home/nvidia"
    if not path.endswith(".py"):
        return "false - Can only edit .py files"
    if not old_text or not new_text:
        return "false - Must provide both old_text and new_text"
    if old_text == new_text:
        return "false - old_text and new_text are identical"

    try:
        with open(path, "r") as f:
            content = f.read()
        if old_text not in content:
            return "false - old_text not found in file"
        if content.count(old_text) > 1:
            return "false - old_text matches multiple locations, be more specific"

        # Create backup before editing
        backup_path = path + f".bak.{make_cycle_id()}"
        with open(backup_path, "w") as f:
            f.write(content)

        # Apply edit
        new_content = content.replace(old_text, new_text, 1)
        with open(path, "w") as f:
            f.write(new_content)

        mind.db.log_activity("mind", "source_edit", f"Edited: {path}",
                             f"Backup: {backup_path}\nChanged {len(old_text)} -> {len(new_text)} chars")
        return f"true - Edited {path} (backup at {backup_path})"
    except Exception as e:
        return f"false - Edit failed: {e}"


def act_restart_service(mind, action: dict, cid: str) -> str:
    service = action.get("service", "")
    log(f'  Executing: RestartService {{ service: "{service}" }}')

    # Only allow restarting known services
    allowed = ["chronicle-local.service", "chronicle-mind.service", "sprout-bot.service"]
    if service not in allowed:
        return f"false - Can only restart: {', '.join(allowed)}"

    try:
        result = subprocess.run(
            ["systemctl", "--user", "restart", service],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0:
            return f"true - Restarted {service}"
        return f"false - Restart failed: {result.stderr.strip()}"
    except Exception as e:
        return f"false - Restart failed: {e}"
