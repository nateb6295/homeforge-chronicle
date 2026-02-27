#!/usr/bin/env python3
"""
Chronicle Watchdog — Nate's babysitter for Mind.

A separate lightweight process that monitors Mind's behavior and intervenes
when it detects problematic patterns. NO LLM calls — pure deterministic logic.

Monitoring rules:
  - Topic rumination (same keywords across N cycles)
  - Action flooding (same action type too many times)
  - Directive compliance (STOP active but Mind still acting)
  - Outbox spam (>50% similar messages in 1 hour)
  - Goal instability (goal changes too often)
  - Operator response (Mind ignoring operator messages)

Intervention escalation:
  1. WARNING: Plant meta-watchdog note + outbox entry
  2. DIRECTIVE: Plant directive note (REDIRECT/RESTRICT) + ntfy push
  3. HARD STOP: systemctl --user stop chronicle-mind + ntfy emergency alert

Runs as its own systemd service on AGX alongside Mind.
"""

import sqlite3
import time
import os
import sys
import re
import json
import subprocess
import signal
from collections import Counter
from datetime import datetime
from typing import Optional, List, Dict, Tuple

# ── Configuration ──────────────────────────────────────────────
DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)
CHECK_INTERVAL = int(os.environ.get("WATCHDOG_INTERVAL", "120"))  # seconds
LOG_FILE = os.environ.get(
    "WATCHDOG_LOG",
    os.path.expanduser("~/chronicle/chronicle-watchdog.log"),
)
NTFY_TOPIC = "chronicle-nate-5d786588e02c8854"
MIND_SERVICE = "chronicle-mind.service"

# Thresholds
TOPIC_WARN_CYCLES = 6       # warn after N cycles of same topic
TOPIC_DIRECTIVE_CYCLES = 10  # plant directive after N cycles
TOPIC_HARDSTOP_CYCLES = 15   # hard stop after N cycles

ACTION_FLOOD_THRESHOLD = 5   # same action 5x in 10 cycles = warning
ACTION_FLOOD_WINDOW = 10     # cycles to look back

OUTBOX_SPAM_THRESHOLD = 5    # 5+ similar outbox messages in 1 hour
OUTBOX_SPAM_WINDOW = 3600    # 1 hour

# Nostr rate limits DISABLED by operator — Mind posts freely
# NOSTR_RATE_HOUR = 3          # max posts per hour
# NOSTR_RATE_DAY = 12          # max posts per day

GOAL_INSTABILITY_THRESHOLD = 3  # goal changes in 10 cycles
GOAL_INSTABILITY_WINDOW = 10

OPERATOR_RESPONSE_CYCLES = 3  # cycles before warning about unresponded operator messages

WARNING_EXPIRE_HOURS = 6     # auto-resolve watchdog warnings after this

# Communication actions (flooding these is worse than internal actions)
COMMS_ACTIONS = {
    "message_operator", "discord_post", "moltbook_post",
    "moltbook_reply", "claw_cities_reply", "speak",
}
# nostr_post removed from COMMS_ACTIONS — operator wants unrestricted posting


# ── Utilities ──────────────────────────────────────────────────

def now_ts() -> int:
    return int(time.time())


def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str):
    line = f"[{now_iso()}] [watchdog] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def send_ntfy(title: str, message: str = "", priority: str = "default", tags: str = "robot_face"):
    """Send push notification via ntfy."""
    try:
        data = message or title
        headers = {
            "Title": title[:200],
            "Priority": priority,
            "Tags": tags,
        }
        import requests
        requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=data.encode("utf-8"),
            headers=headers,
            timeout=10,
        )
        log(f"ntfy sent: {title[:60]}")
    except Exception as e:
        log(f"ntfy failed: {e}")


class WatchdogDB:
    """Lightweight DB wrapper for watchdog — read-heavy, occasional writes."""

    def __init__(self, path: str):
        self.conn = sqlite3.connect(path, timeout=10)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout = 5000")
        self.conn.execute("PRAGMA journal_mode = WAL")

    def query(self, sql: str, params: tuple = ()) -> List[dict]:
        try:
            rows = self.conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            log(f"DB query error: {e}")
            return []

    def query_one(self, sql: str, params: tuple = ()) -> Optional[dict]:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def run(self, sql: str, params: tuple = ()):
        try:
            self.conn.execute(sql, params)
            self.conn.commit()
        except Exception as e:
            log(f"DB write error: {e}")

    def close(self):
        self.conn.close()


class Watchdog:
    def __init__(self):
        self.db = WatchdogDB(DB_PATH)
        self.running = True
        self.check_count = 0
        self.interventions = {"warning": 0, "directive": 0, "hard_stop": 0}
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _shutdown(self, signum, frame):
        log("Received shutdown signal.")
        self.running = False

    # ── Intervention Methods ──────────────────────────────────

    def warn(self, rule: str, message: str):
        """Level 1: Plant warning note. Skips if similar warning already exists."""
        # Check for existing recent warning with same rule
        existing = self.db.query_one(
            "SELECT id FROM scratch_pad WHERE category = 'meta-watchdog' AND resolved = 0 "
            "AND content LIKE ? LIMIT 1",
            (f"%{rule}%",),
        )
        if existing:
            return  # Don't spam warnings
        log(f"WARNING [{rule}]: {message}")
        self.interventions["warning"] += 1
        ts = now_ts()
        self.db.run(
            "INSERT INTO scratch_pad (content, category, priority, resolved, created_at, updated_at) "
            "VALUES (?, 'meta-watchdog', 5, 0, ?, ?)",
            (f"[WATCHDOG WARNING] {rule}: {message}", ts, ts),
        )

    def _has_recent_directive(self, directive_type: str, hours: float = 1.0) -> bool:
        """Check if a similar directive was already planted recently."""
        cutoff = now_ts() - int(hours * 3600)
        existing = self.db.query_one(
            "SELECT id FROM scratch_pad WHERE category = 'directive' AND resolved = 0 "
            "AND UPPER(content) LIKE '%' || ? || '%' AND created_at > ? LIMIT 1",
            (f"{directive_type}%", cutoff),
        )
        return existing is not None

    def plant_directive(self, directive_type: str, message: str, reason: str = ""):
        """Level 2: Plant directive note + ntfy push. Skips if duplicate exists.

        message: the action/target (e.g. "nostr_post") — stored in DB as "RESTRICT: nostr_post"
        reason: human-readable explanation — sent via ntfy only, NOT stored in directive content
        """
        if self._has_recent_directive(directive_type, hours=1.0):
            log(f"DIRECTIVE [{directive_type}] skipped — similar directive already active")
            return
        log(f"DIRECTIVE [{directive_type}]: {message}" + (f" ({reason})" if reason else ""))
        self.interventions["directive"] += 1
        ts = now_ts()
        self.db.run(
            "INSERT INTO scratch_pad (content, category, priority, resolved, created_at, updated_at) "
            "VALUES (?, 'directive', 99, 0, ?, ?)",
            (f"[WATCHDOG] {directive_type}: {message}" + (f" (auto: {reason})" if reason else ""), ts, ts),
        )
        send_ntfy(
            f"Watchdog: {directive_type}",
            (reason or message)[:200],
            priority="high",
            tags="warning",
        )

    def hard_stop(self, rule: str, message: str):
        """Level 3: Stop Mind service + ntfy emergency alert."""
        log(f"HARD STOP [{rule}]: {message}")
        self.interventions["hard_stop"] += 1
        try:
            subprocess.run(
                ["systemctl", "--user", "stop", MIND_SERVICE],
                capture_output=True, timeout=15,
            )
            log(f"  Mind service stopped via systemctl")
        except Exception as e:
            log(f"  Failed to stop Mind: {e}")
        send_ntfy(
            f"HARD STOP: {rule}",
            f"Mind has been stopped. Reason: {message[:200]}",
            priority="urgent",
            tags="rotating_light",
        )

    # ── Monitoring Rules ──────────────────────────────────────

    def check_topic_rumination(self):
        """Detect same topic keywords repeated across N cycles."""
        history = self.db.query(
            "SELECT reasoning FROM thought_stream ORDER BY id DESC LIMIT ?",
            (TOPIC_HARDSTOP_CYCLES + 2,),
        )
        if len(history) < TOPIC_WARN_CYCLES:
            return

        stopwords = {
            "this", "that", "with", "from", "have", "been", "will", "would", "could",
            "should", "their", "there", "these", "those", "about", "which", "where",
            "when", "what", "into", "more", "some", "than", "also", "only", "other",
            "action", "cycle", "note", "write", "true", "false", "content", "category",
        }

        def extract_words(text: str) -> set:
            if not text:
                return set()
            return {w for w in re.findall(r'[a-z]+', text.lower()) if len(w) > 4 and w not in stopwords}

        word_sets = [extract_words(h.get("reasoning", "")) for h in history]

        # Compute consecutive Jaccard similarities
        sims = []
        for i in range(len(word_sets) - 1):
            a, b = word_sets[i], word_sets[i + 1]
            if a and b:
                sims.append(len(a & b) / len(a | b))
            else:
                sims.append(0.0)

        # Count consecutive high-similarity pairs
        consecutive_high = 0
        for s in sims:
            if s > 0.5:
                consecutive_high += 1
            else:
                break

        # Find dominant keywords for reporting
        all_words = Counter()
        for ws in word_sets[:consecutive_high + 1]:
            for w in ws:
                all_words[w] += 1
        dominant = [w for w, c in all_words.most_common(5) if c >= 3]
        topic_str = ", ".join(dominant[:5]) if dominant else "unknown topic"

        if consecutive_high >= TOPIC_HARDSTOP_CYCLES:
            self.hard_stop(
                "topic_rumination",
                f"Mind stuck on same topic for {consecutive_high} cycles: {topic_str}",
            )
        elif consecutive_high >= TOPIC_DIRECTIVE_CYCLES:
            self.plant_directive(
                "REDIRECT",
                f"Watchdog detected {consecutive_high}-cycle topic rumination on: {topic_str}. "
                f"Do something useful for Nate instead.",
            )
        elif consecutive_high >= TOPIC_WARN_CYCLES:
            self.warn(
                "topic_rumination",
                f"Same topic for {consecutive_high} cycles: {topic_str}",
            )

    def check_action_flooding(self):
        """Detect same action type used too many times in recent cycles.
        Auto-resolves restrictions when flooding stops."""
        history = self.db.query(
            "SELECT actions_taken FROM thought_stream ORDER BY id DESC LIMIT ?",
            (ACTION_FLOOD_WINDOW,),
        )
        if len(history) < 3:
            return

        action_counts = Counter()
        for h in history:
            try:
                actions = json.loads(h.get("actions_taken", "[]"))
                for a in actions:
                    action_counts[a] += 1
            except Exception:
                pass

        # Track which comms actions are currently flooding
        flooding_actions = set()
        for action_name, count in action_counts.items():
            if count >= ACTION_FLOOD_THRESHOLD:
                is_comms = action_name in COMMS_ACTIONS
                if is_comms:
                    flooding_actions.add(action_name)
                    self.plant_directive(
                        "RESTRICT",
                        f"{action_name}",
                        reason=f"Action flooding: {action_name} used {count}x in {ACTION_FLOOD_WINDOW} cycles",
                    )
                else:
                    self.warn(
                        "action_flooding",
                        f"{action_name} used {count}x in {ACTION_FLOOD_WINDOW} cycles",
                    )

        # Auto-resolve flood restrictions for comms actions that are no longer flooding
        # Skip nostr_post — it has its own dedicated rate check (check_nostr_rate)
        # which manages its own resolve/create lifecycle
        for comms_action in COMMS_ACTIONS:
            if comms_action in flooding_actions:
                continue
            if comms_action == "nostr_post":
                continue  # managed by check_nostr_rate
            existing = self.db.query(
                "SELECT id FROM scratch_pad WHERE category = 'directive' AND resolved = 0 "
                "AND content LIKE '%RESTRICT: ' || ? || '%'",
                (comms_action,),
            )
            for row in existing:
                self.db.run(
                    "UPDATE scratch_pad SET resolved = 1 WHERE id = ?",
                    (row["id"],),
                )
                log(f"  Auto-resolved {comms_action} flood restriction #{row['id']} — rate back under threshold")

    def check_directive_compliance(self):
        """Check if a STOP directive is active but Mind is still running cycles."""
        stop_directive = self.db.query_one(
            "SELECT id, created_at FROM scratch_pad "
            "WHERE category = 'directive' AND resolved = 0 AND UPPER(content) LIKE 'STOP%' "
            "ORDER BY priority DESC LIMIT 1"
        )
        if not stop_directive:
            return

        stop_ts = stop_directive.get("created_at", 0)
        # Check if Mind ran any cycles after the STOP was planted
        post_stop_cycles = self.db.query(
            "SELECT id FROM thought_stream WHERE created_at > ? LIMIT 1",
            (stop_ts + 60,),  # grace period of 60s for the current cycle to finish
        )
        if post_stop_cycles:
            self.hard_stop(
                "directive_violation",
                f"Mind ran a cycle after STOP directive #{stop_directive['id']} was active. "
                f"Forcing shutdown.",
            )

    def check_outbox_spam(self):
        """Detect similar outbox messages (Mind spamming operator/channels)."""
        cutoff = now_ts() - OUTBOX_SPAM_WINDOW
        messages = self.db.query(
            "SELECT message FROM outbox WHERE created_at > ? ORDER BY created_at DESC LIMIT 20",
            (cutoff,),
        )
        if len(messages) < OUTBOX_SPAM_THRESHOLD:
            return

        # Simple keyword overlap similarity
        contents = [m.get("message", "")[:200].lower() for m in messages]
        similar_count = 0
        for i in range(len(contents)):
            for j in range(i + 1, len(contents)):
                words_i = set(contents[i].split())
                words_j = set(contents[j].split())
                if words_i and words_j:
                    overlap = len(words_i & words_j) / len(words_i | words_j)
                    if overlap > 0.6:
                        similar_count += 1

        total_pairs = len(contents) * (len(contents) - 1) / 2
        if total_pairs > 0 and similar_count / total_pairs > 0.5:
            self.warn(
                "outbox_spam",
                f"{similar_count} similar message pairs in last hour ({len(contents)} total messages)",
            )

    def check_nostr_rate(self):
        """Check Nostr posting rate limits. Auto-resolves restriction when rate drops."""
        try:
            # Check hourly rate
            hour_ago = now_ts() - 3600
            hourly = self.db.query_one(
                "SELECT COUNT(*) as cnt FROM nostr_posts WHERE created_at > ?",
                (hour_ago,),
            )
            hourly_count = hourly.get("cnt", 0) if hourly else 0

            # Check daily rate
            day_ago = now_ts() - 86400
            daily = self.db.query_one(
                "SELECT COUNT(*) as cnt FROM nostr_posts WHERE created_at > ?",
                (day_ago,),
            )
            daily_count = daily.get("cnt", 0) if daily else 0

            over_limit = hourly_count >= NOSTR_RATE_HOUR or daily_count >= NOSTR_RATE_DAY

            if over_limit:
                # Only plant if no existing unresolved nostr directive
                existing = self.db.query_one(
                    "SELECT id FROM scratch_pad WHERE category = 'directive' AND resolved = 0 "
                    "AND content LIKE '%RESTRICT: nostr_post%' LIMIT 1",
                )
                if not existing:
                    self.plant_directive(
                        "RESTRICT",
                        f"nostr_post",
                        reason=f"Nostr rate limit hit: {hourly_count}/hr (max {NOSTR_RATE_HOUR}), "
                        f"{daily_count}/day (max {NOSTR_RATE_DAY})",
                    )
            else:
                # Auto-resolve watchdog-created nostr restrictions when rate drops back
                existing = self.db.query(
                    "SELECT id FROM scratch_pad WHERE category = 'directive' AND resolved = 0 "
                    "AND content LIKE '%RESTRICT: nostr_post%'",
                )
                for row in existing:
                    self.db.run(
                        "UPDATE scratch_pad SET resolved = 1 WHERE id = ?",
                        (row["id"],),
                    )
                    log(f"  Auto-resolved nostr_post restriction #{row['id']} — rate back under limit "
                        f"({hourly_count}/hr, {daily_count}/day)")
        except Exception:
            pass  # nostr_posts table may not exist

    def check_goal_instability(self):
        """Detect goal changing too often (Mind thrashing)."""
        history = self.db.query(
            "SELECT actions_taken FROM thought_stream ORDER BY id DESC LIMIT ?",
            (GOAL_INSTABILITY_WINDOW,),
        )
        goal_changes = 0
        for h in history:
            try:
                actions = json.loads(h.get("actions_taken", "[]"))
                if "update_goal" in actions:
                    goal_changes += 1
            except Exception:
                pass

        if goal_changes >= GOAL_INSTABILITY_THRESHOLD:
            self.warn(
                "goal_instability",
                f"Goal changed {goal_changes}x in last {GOAL_INSTABILITY_WINDOW} cycles — possible thrashing",
            )

    def check_operator_response(self):
        """Check if Mind is ignoring operator messages."""
        # Find unresolved operator messages
        op_msgs = self.db.query(
            "SELECT id, created_at FROM scratch_pad "
            "WHERE category = 'discord-operator' AND resolved = 0 "
            "ORDER BY created_at ASC"
        )
        if not op_msgs:
            return

        oldest = op_msgs[0]
        oldest_ts = oldest.get("created_at", 0)
        # Count cycles since that message
        cycles_since = self.db.query(
            "SELECT COUNT(*) as cnt FROM thought_stream WHERE created_at > ?",
            (oldest_ts,),
        )
        cycle_count = cycles_since[0].get("cnt", 0) if cycles_since else 0

        if cycle_count >= OPERATOR_RESPONSE_CYCLES:
            self.warn(
                "operator_response",
                f"Mind has run {cycle_count} cycles without responding to operator message #{oldest['id']}",
            )

    # ── Housekeeping ──────────────────────────────────────────

    def cleanup_old_warnings(self):
        """Auto-resolve watchdog warnings older than WARNING_EXPIRE_HOURS."""
        cutoff = now_ts() - (WARNING_EXPIRE_HOURS * 3600)
        self.db.run(
            "UPDATE scratch_pad SET resolved = 1 "
            "WHERE category = 'meta-watchdog' AND resolved = 0 AND created_at < ?",
            (cutoff,),
        )

    # ── Main Loop ─────────────────────────────────────────────

    def run_check(self):
        """Run all monitoring rules once."""
        self.check_count += 1
        log(f"Check #{self.check_count}")

        try:
            self.check_directive_compliance()
            self.check_topic_rumination()
            self.check_action_flooding()
            self.check_outbox_spam()
            # self.check_nostr_rate()  # Disabled — operator wants unrestricted nostr
            self.check_goal_instability()
            self.check_operator_response()
            self.cleanup_old_warnings()
        except Exception as e:
            log(f"Check error: {e}")

        total = sum(self.interventions.values())
        if total > 0:
            log(f"  Interventions: {json.dumps(self.interventions)}")

    def run_forever(self):
        log("Chronicle Watchdog starting...")
        log(f"  DB: {DB_PATH}")
        log(f"  Interval: {CHECK_INTERVAL}s")
        log(f"  Thresholds: topic={TOPIC_WARN_CYCLES}/{TOPIC_DIRECTIVE_CYCLES}/{TOPIC_HARDSTOP_CYCLES}, "
            f"action_flood={ACTION_FLOOD_THRESHOLD}/{ACTION_FLOOD_WINDOW}")

        while self.running:
            self.run_check()
            for _ in range(CHECK_INTERVAL):
                if not self.running:
                    break
                time.sleep(1)

        log("Chronicle Watchdog shutting down.")
        self.db.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Chronicle Watchdog — Mind behavior monitor")
    parser.add_argument("--once", action="store_true", help="Run one check and exit")
    args = parser.parse_args()

    watchdog = Watchdog()
    if args.once:
        watchdog.run_check()
        watchdog.db.close()
    else:
        watchdog.run_forever()


if __name__ == "__main__":
    main()
