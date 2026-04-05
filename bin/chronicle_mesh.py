#!/usr/bin/env python3
"""Chronicle Mesh — Autonomic nervous system for the swarm.

Every agent imports this. On creation, it silently starts:
  - Heartbeat pulse (writes to mesh_heartbeats every 30s)
  - Expectation monitoring (checks input/output rates against declared norms)
  - Pain routing (surfaces anomalies to mesh_pain table + Discord)

The mesh IS the stable abstraction boundary. Agents don't implement
communication health — they stand on the mesh and it handles itself.

Usage:
    from chronicle_mesh import Mesh

    mesh = Mesh("intern")

    # Declare what normal looks like
    mesh.expect("briefs_produced", min_per_hour=2)
    mesh.expect("activity_feed_inputs", min_per_hour=10)

    # In your main loop, after doing work:
    mesh.pulse("briefs_produced")   # "I just made a brief"

    # That's it. Heartbeat, monitoring, and pain are automatic.

    # Optional: check who's alive
    mesh.family()          # → [{"agent": "intern", "alive": True, "last_seen": 1234}, ...]
    mesh.alive("gemma")    # → True/False
    mesh.pain_signals()    # → current unresolved pain across all agents
"""

import os
import sys
import time
import json
import sqlite3
import threading
import traceback

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)

HEARTBEAT_INTERVAL = 30     # seconds between heartbeat writes
EXPECT_CHECK_INTERVAL = 300 # seconds between expectation checks (5 min)
PAIN_COOLDOWN = 600         # don't re-fire same pain signal within 10 min
ALIVE_THRESHOLD = 120       # agent is "dead" if no heartbeat in 2 min

# Learned baselines
BASELINE_WARMUP = 3600      # 1 hour before baselines are trusted
BASELINE_WINDOW = 14400     # 4 hour rolling window for learning normal
DEGRADATION_RATIO = 0.3     # current < 30% of baseline = degradation pain

# Discord webhook for pain alerts (Alerts channel, not Opus)
DISCORD_WEBHOOK = os.environ.get(
    "MESH_DISCORD_WEBHOOK",
    "https://discord.com/api/webhooks/1489300749308395611/"
    "zlZZH3QzXqEDxznmNelK3mlkLF0IeZqoxwNUnrL0no_jfeZnDMvwvD_7XhYcCyQzq78I"
)

# ═══════════════════════════════════════════════════════════════════
#  Schema
# ═══════════════════════════════════════════════════════════════════

SCHEMA = """
CREATE TABLE IF NOT EXISTS mesh_heartbeats (
    agent       TEXT PRIMARY KEY,
    started_at  INTEGER NOT NULL,
    last_pulse  INTEGER NOT NULL,
    pid         INTEGER,
    meta        TEXT
);

CREATE TABLE IF NOT EXISTS mesh_pulses (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    agent       TEXT NOT NULL,
    metric      TEXT NOT NULL,
    ts          INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mesh_pulses_agent_metric
    ON mesh_pulses(agent, metric, ts);

CREATE TABLE IF NOT EXISTS mesh_expectations (
    agent       TEXT NOT NULL,
    metric      TEXT NOT NULL,
    min_per_hour REAL NOT NULL,
    PRIMARY KEY (agent, metric)
);

CREATE TABLE IF NOT EXISTS mesh_pain (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    agent       TEXT NOT NULL,
    pain_type   TEXT NOT NULL,
    message     TEXT NOT NULL,
    severity    TEXT NOT NULL DEFAULT 'warn',
    ts          INTEGER NOT NULL,
    resolved_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_mesh_pain_unresolved
    ON mesh_pain(resolved_at) WHERE resolved_at IS NULL;

CREATE TABLE IF NOT EXISTS mesh_baselines (
    agent       TEXT NOT NULL,
    metric      TEXT NOT NULL,
    avg_per_hour REAL NOT NULL,
    sample_hours REAL NOT NULL,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY (agent, metric)
);

CREATE TABLE IF NOT EXISTS mesh_context (
    context_key TEXT PRIMARY KEY,
    agent       TEXT NOT NULL,
    value       TEXT NOT NULL,
    updated_at  INTEGER NOT NULL
);
"""


# ═══════════════════════════════════════════════════════════════════
#  Mesh
# ═══════════════════════════════════════════════════════════════════

class Mesh:
    """Autonomic mesh node. One per agent process."""

    def __init__(self, agent_name, db_path=None):
        self.agent = agent_name
        self.db_path = db_path or DB_PATH
        self._stop = threading.Event()
        self._expectations = {}  # metric -> min_per_hour
        self._last_pain = {}     # pain_key -> timestamp (cooldown tracking)
        self._started_at = int(time.time())

        # Ensure tables exist (main thread connection, brief use)
        self._ensure_schema()

        # Start autonomic thread
        self._thread = threading.Thread(
            target=self._autonomic_loop,
            name=f"mesh-{agent_name}",
            daemon=True,
        )
        self._thread.start()

    # ─── Public API ───────────────────────────────────────────────

    def expect(self, metric, min_per_hour):
        """Declare what normal looks like for a metric.

        Example: mesh.expect("briefs_produced", min_per_hour=2)
        Means: if fewer than 2 briefs per hour, something is wrong.
        """
        self._expectations[metric] = min_per_hour
        try:
            conn = self._connect()
            conn.execute(
                "INSERT OR REPLACE INTO mesh_expectations (agent, metric, min_per_hour) "
                "VALUES (?, ?, ?)",
                (self.agent, metric, min_per_hour),
            )
            conn.commit()
            conn.close()
        except Exception:
            pass  # non-fatal — expectations still tracked in memory

    def pulse(self, metric):
        """Record that you just produced/consumed something.

        Example: mesh.pulse("briefs_produced")
        Call this every time you complete a unit of work.
        """
        try:
            conn = self._connect()
            conn.execute(
                "INSERT INTO mesh_pulses (agent, metric, ts) VALUES (?, ?, ?)",
                (self.agent, metric, int(time.time())),
            )
            conn.commit()
            conn.close()
        except Exception:
            pass  # non-fatal

    def alive(self, agent_name):
        """Is a given agent alive? Based on heartbeat recency."""
        try:
            conn = self._connect()
            row = conn.execute(
                "SELECT last_pulse FROM mesh_heartbeats WHERE agent = ?",
                (agent_name,),
            ).fetchone()
            conn.close()
            if not row:
                return False
            return (int(time.time()) - row[0]) < ALIVE_THRESHOLD
        except Exception:
            return False

    def family(self):
        """List all known agents and their status."""
        try:
            conn = self._connect()
            now = int(time.time())
            rows = conn.execute(
                "SELECT agent, last_pulse, started_at, pid FROM mesh_heartbeats "
                "ORDER BY agent"
            ).fetchall()
            conn.close()
            return [
                {
                    "agent": r[0],
                    "alive": (now - r[1]) < ALIVE_THRESHOLD,
                    "last_seen": r[1],
                    "uptime": r[1] - r[2],
                    "pid": r[3],
                }
                for r in rows
            ]
        except Exception:
            return []

    def pain_signals(self, include_resolved=False):
        """Get current pain signals across all agents."""
        try:
            conn = self._connect()
            if include_resolved:
                rows = conn.execute(
                    "SELECT agent, pain_type, message, severity, ts, resolved_at "
                    "FROM mesh_pain ORDER BY ts DESC LIMIT 50"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT agent, pain_type, message, severity, ts, resolved_at "
                    "FROM mesh_pain WHERE resolved_at IS NULL ORDER BY ts DESC LIMIT 50"
                ).fetchall()
            conn.close()
            return [
                {
                    "agent": r[0], "pain_type": r[1], "message": r[2],
                    "severity": r[3], "ts": r[4], "resolved": r[5],
                }
                for r in rows
            ]
        except Exception:
            return []

    def set_context(self, key, value):
        """Share context with the swarm. Value is JSON-serializable.

        Example: mesh.set_context("home_state", {"quiet": True, "motion": False})
        Any agent can read this with mesh.get_context("home_state").
        """
        try:
            conn = self._connect()
            conn.execute(
                "INSERT OR REPLACE INTO mesh_context "
                "(context_key, agent, value, updated_at) VALUES (?, ?, ?, ?)",
                (key, self.agent, json.dumps(value) if not isinstance(value, str) else value,
                 int(time.time())),
            )
            conn.commit()
            conn.close()
        except Exception:
            pass

    def get_context(self, key, max_age=None):
        """Read shared context from any agent. Returns parsed value or None.

        Args:
            key: Context key (e.g., "home_state")
            max_age: Max seconds old (None = any age)
        """
        try:
            conn = self._connect()
            row = conn.execute(
                "SELECT value, updated_at FROM mesh_context WHERE context_key = ?",
                (key,),
            ).fetchone()
            conn.close()
            if not row:
                return None
            if max_age and (int(time.time()) - row[1]) > max_age:
                return None
            try:
                return json.loads(row[0])
            except (json.JSONDecodeError, TypeError):
                return row[0]
        except Exception:
            return None

    def all_context(self, max_age=600):
        """Get all shared context updated within max_age seconds."""
        try:
            conn = self._connect()
            cutoff = int(time.time()) - max_age
            rows = conn.execute(
                "SELECT context_key, agent, value, updated_at FROM mesh_context "
                "WHERE updated_at > ? ORDER BY context_key",
                (cutoff,),
            ).fetchall()
            conn.close()
            result = {}
            for key, agent, val, ts in rows:
                try:
                    result[key] = {"agent": agent, "value": json.loads(val), "age": int(time.time()) - ts}
                except (json.JSONDecodeError, TypeError):
                    result[key] = {"agent": agent, "value": val, "age": int(time.time()) - ts}
            return result
        except Exception:
            return {}

    def resolve_pain(self, agent=None, pain_type=None):
        """Mark pain signals as resolved."""
        try:
            conn = self._connect()
            now = int(time.time())
            if agent and pain_type:
                conn.execute(
                    "UPDATE mesh_pain SET resolved_at = ? "
                    "WHERE agent = ? AND pain_type = ? AND resolved_at IS NULL",
                    (now, agent, pain_type),
                )
            elif agent:
                conn.execute(
                    "UPDATE mesh_pain SET resolved_at = ? "
                    "WHERE agent = ? AND resolved_at IS NULL",
                    (now, agent),
                )
            conn.commit()
            conn.close()
        except Exception:
            pass

    def swarm_health(self):
        """One-line-per-agent health summary. Inject into prompts for swarm awareness."""
        try:
            conn = self._connect()
            now = int(time.time())
            agents = conn.execute(
                "SELECT agent, last_pulse FROM mesh_heartbeats ORDER BY agent"
            ).fetchall()
            lines = []
            for agent, lp in agents:
                alive = (now - lp) < ALIVE_THRESHOLD
                # Get baselines for this agent
                baselines = conn.execute(
                    "SELECT metric, avg_per_hour FROM mesh_baselines WHERE agent = ?",
                    (agent,),
                ).fetchall()
                # Get current rates
                rates = []
                for metric, avg in baselines:
                    count = conn.execute(
                        "SELECT COUNT(*) FROM mesh_pulses "
                        "WHERE agent = ? AND metric = ? AND ts > ?",
                        (agent, metric, now - 3600),
                    ).fetchone()[0]
                    status = "ok" if count >= avg * DEGRADATION_RATIO else "low"
                    rates.append(f"{metric}={count}/hr[{status}]")
                # Active pain count
                pain_count = conn.execute(
                    "SELECT COUNT(*) FROM mesh_pain "
                    "WHERE agent = ? AND resolved_at IS NULL",
                    (agent,),
                ).fetchone()[0]
                state = "ALIVE" if alive else "DEAD"
                rate_str = " ".join(rates) if rates else "warming-up"
                pain_str = f" PAIN={pain_count}" if pain_count > 0 else ""
                lines.append(f"{agent}: {state} {rate_str}{pain_str}")
            conn.close()
            return "\n".join(lines)
        except Exception:
            return "(mesh unavailable)"

    def shutdown(self):
        """Clean shutdown of the autonomic thread."""
        self._stop.set()
        self._thread.join(timeout=5)

    # ─── Internal ─────────────────────────────────────────────────

    def _connect(self):
        """Fresh connection (safe for cross-thread use)."""
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _ensure_schema(self):
        """Create mesh tables if they don't exist."""
        conn = self._connect()
        conn.executescript(SCHEMA)
        conn.commit()
        conn.close()

    def _autonomic_loop(self):
        """Background loop: heartbeat + expectation checks."""
        conn = self._connect()
        last_expect_check = 0

        while not self._stop.is_set():
            try:
                now = int(time.time())

                # ── Heartbeat ──
                conn.execute(
                    "INSERT OR REPLACE INTO mesh_heartbeats "
                    "(agent, started_at, last_pulse, pid) VALUES (?, ?, ?, ?)",
                    (self.agent, self._started_at, now, os.getpid()),
                )
                conn.commit()

                # ── Expectation check (every 5 min) ──
                if now - last_expect_check >= EXPECT_CHECK_INTERVAL:
                    self._check_expectations(conn, now)
                    self._learn_baselines(conn, now)
                    self._check_degradation(conn, now)
                    last_expect_check = now

                    # ── Dead agent detection ──
                    self._check_dead_agents(conn, now)

                    # ── Pulse table cleanup (keep 24h) ──
                    conn.execute(
                        "DELETE FROM mesh_pulses WHERE ts < ?",
                        (now - 86400,),
                    )
                    # ── Pain auto-resolve (48h old pain is stale) ──
                    conn.execute(
                        "UPDATE mesh_pain SET resolved_at = ? "
                        "WHERE resolved_at IS NULL AND ts < ?",
                        (now, now - 172800),
                    )
                    conn.commit()

            except Exception as e:
                # Never crash the host agent
                try:
                    self._log(f"mesh error: {e}")
                except Exception:
                    pass

            # Wait 30s or until stopped
            self._stop.wait(HEARTBEAT_INTERVAL)

        conn.close()

    def _check_expectations(self, conn, now):
        """Check all declared expectations against actual pulse rates.
        Auto-resolves pain when metrics recover."""
        for metric, min_per_hour in self._expectations.items():
            try:
                lookback = max(3600, int(3600 / max(min_per_hour, 0.1)))
                cutoff = now - lookback
                row = conn.execute(
                    "SELECT COUNT(*) FROM mesh_pulses "
                    "WHERE agent = ? AND metric = ? AND ts > ?",
                    (self.agent, metric, cutoff),
                ).fetchone()
                count = row[0] if row else 0

                hours = lookback / 3600.0
                rate = count / hours if hours > 0 else 0

                pain_key = f"{self.agent}:{metric}"
                if rate < min_per_hour:
                    last = self._last_pain.get(pain_key, 0)
                    if now - last >= PAIN_COOLDOWN:
                        self._raise_pain(
                            conn, "expectation_miss",
                            f"{metric}: {rate:.1f}/hr (expected >={min_per_hour}/hr, "
                            f"got {count} in {hours:.1f}h)",
                            severity="warn",
                        )
                        self._last_pain[pain_key] = now
                else:
                    # Auto-resolve: metric is healthy, clear old pain
                    conn.execute(
                        "UPDATE mesh_pain SET resolved_at = ? "
                        "WHERE agent = ? AND pain_type = 'expectation_miss' "
                        "AND message LIKE ? AND resolved_at IS NULL",
                        (now, self.agent, f"{metric}:%"),
                    )
                    conn.commit()
            except Exception:
                pass

    def _check_dead_agents(self, conn, now):
        """Check if any previously-alive agents have gone silent."""
        try:
            rows = conn.execute(
                "SELECT agent, last_pulse FROM mesh_heartbeats"
            ).fetchall()
            for agent, last_pulse in rows:
                if agent == self.agent:
                    continue
                gap = now - last_pulse
                # Agent is dead if no heartbeat in 2 minutes
                # But only alert if they were recently alive (not ancient entries)
                if ALIVE_THRESHOLD < gap < 600:
                    pain_key = f"dead:{agent}"
                    last = self._last_pain.get(pain_key, 0)
                    if now - last >= PAIN_COOLDOWN:
                        self._raise_pain(
                            conn, "agent_down",
                            f"{agent} has not pulsed in {gap}s",
                            severity="alert",
                        )
                        self._last_pain[pain_key] = now
        except Exception:
            pass

    def _learn_baselines(self, conn, now):
        """Update rolling averages for each metric. The mesh learns its own normal."""
        for metric in self._expectations:
            try:
                # How long has this agent been pulsing this metric?
                first_pulse = conn.execute(
                    "SELECT MIN(ts) FROM mesh_pulses WHERE agent = ? AND metric = ?",
                    (self.agent, metric),
                ).fetchone()
                if not first_pulse or not first_pulse[0]:
                    continue

                age = now - first_pulse[0]
                if age < BASELINE_WARMUP:
                    continue  # not enough data yet

                # Compute rate over the baseline window (or total age, whichever is shorter)
                window = min(age, BASELINE_WINDOW)
                cutoff = now - window
                count = conn.execute(
                    "SELECT COUNT(*) FROM mesh_pulses "
                    "WHERE agent = ? AND metric = ? AND ts > ?",
                    (self.agent, metric, cutoff),
                ).fetchone()[0]

                hours = window / 3600.0
                avg = count / hours if hours > 0 else 0

                conn.execute(
                    "INSERT OR REPLACE INTO mesh_baselines "
                    "(agent, metric, avg_per_hour, sample_hours, updated_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (self.agent, metric, round(avg, 2), round(hours, 1), now),
                )
                conn.commit()
            except Exception:
                pass

    def _check_degradation(self, conn, now):
        """Detect degradation: current rate far below learned baseline.
        Catches: 'intern was doing 12/hr and dropped to 3/hr' even if 3 > static floor."""
        for metric in self._expectations:
            try:
                # Get baseline
                row = conn.execute(
                    "SELECT avg_per_hour, sample_hours FROM mesh_baselines "
                    "WHERE agent = ? AND metric = ?",
                    (self.agent, metric),
                ).fetchone()
                if not row or row[1] < (BASELINE_WARMUP / 3600.0):
                    continue  # no baseline yet

                baseline_avg = row[0]
                if baseline_avg < 1.0:
                    continue  # baseline too low to meaningfully degrade

                # Current rate (last hour)
                count = conn.execute(
                    "SELECT COUNT(*) FROM mesh_pulses "
                    "WHERE agent = ? AND metric = ? AND ts > ?",
                    (self.agent, metric, now - 3600),
                ).fetchone()[0]
                current_rate = count  # per hour

                ratio = current_rate / baseline_avg if baseline_avg > 0 else 1.0

                if ratio < DEGRADATION_RATIO:
                    pain_key = f"degrade:{self.agent}:{metric}"
                    last = self._last_pain.get(pain_key, 0)
                    if now - last >= PAIN_COOLDOWN:
                        self._raise_pain(
                            conn, "degradation",
                            f"{metric}: {current_rate:.0f}/hr vs baseline {baseline_avg:.1f}/hr "
                            f"({ratio:.0%} of normal)",
                            severity="alert",
                        )
                        self._last_pain[pain_key] = now
                else:
                    # Auto-resolve degradation pain if recovered
                    conn.execute(
                        "UPDATE mesh_pain SET resolved_at = ? "
                        "WHERE agent = ? AND pain_type = 'degradation' "
                        "AND message LIKE ? AND resolved_at IS NULL",
                        (now, self.agent, f"{metric}:%"),
                    )
                    conn.commit()
            except Exception:
                pass

    def _raise_pain(self, conn, pain_type, message, severity="warn"):
        """Record a pain signal and optionally alert Discord."""
        try:
            conn.execute(
                "INSERT INTO mesh_pain (agent, pain_type, message, severity, ts) "
                "VALUES (?, ?, ?, ?, ?)",
                (self.agent, pain_type, message, severity, int(time.time())),
            )
            conn.commit()
        except Exception:
            pass

        # Alert-level pain goes to Discord
        if severity == "alert" and DISCORD_WEBHOOK:
            self._discord_alert(pain_type, message)

        self._log(f"PAIN [{severity}] {pain_type}: {message}")

    def _discord_alert(self, pain_type, message):
        """Post pain signal to Discord."""
        try:
            import requests
            text = f"**MESH PAIN** [{self.agent}] {pain_type}: {message}"
            requests.post(
                DISCORD_WEBHOOK,
                json={"content": text[:1900]},
                timeout=5,
            )
        except Exception:
            pass  # never crash over a notification

    def _log(self, msg):
        """Simple stderr log."""
        ts = time.strftime("%H:%M:%S")
        print(f"[mesh:{self.agent} {ts}] {msg}", file=sys.stderr, flush=True)


# ═══════════════════════════════════════════════════════════════════
#  Convenience: standalone health check
# ═══════════════════════════════════════════════════════════════════

def mesh_status(db_path=None):
    """Print current mesh status. Callable from CLI."""
    db = db_path or DB_PATH
    conn = sqlite3.connect(db, timeout=10)
    now = int(time.time())

    print("=== MESH HEARTBEATS ===")
    rows = conn.execute(
        "SELECT agent, last_pulse, started_at, pid FROM mesh_heartbeats ORDER BY agent"
    ).fetchall()
    if not rows:
        print("  (no agents registered)")
    for agent, last, started, pid in rows:
        gap = now - last
        status = "ALIVE" if gap < ALIVE_THRESHOLD else f"DEAD ({gap}s ago)"
        uptime_h = (last - started) / 3600
        print(f"  {agent:20s}  {status:15s}  pid={pid}  uptime={uptime_h:.1f}h")

    print("\n=== EXPECTATIONS ===")
    rows = conn.execute(
        "SELECT agent, metric, min_per_hour FROM mesh_expectations ORDER BY agent, metric"
    ).fetchall()
    if not rows:
        print("  (no expectations declared)")
    for agent, metric, mph in rows:
        # Check actual rate
        count = conn.execute(
            "SELECT COUNT(*) FROM mesh_pulses WHERE agent = ? AND metric = ? AND ts > ?",
            (agent, metric, now - 3600),
        ).fetchone()[0]
        status = "OK" if count >= mph else "LOW"
        print(f"  {agent:15s}  {metric:25s}  {count}/hr (need {mph:.0f})  [{status}]")

    print("\n=== LEARNED BASELINES ===")
    rows = conn.execute(
        "SELECT agent, metric, avg_per_hour, sample_hours FROM mesh_baselines "
        "ORDER BY agent, metric"
    ).fetchall()
    if not rows:
        print("  (warming up — baselines learned after 1h)")
    for agent, metric, avg, hours in rows:
        # Current rate for comparison
        count = conn.execute(
            "SELECT COUNT(*) FROM mesh_pulses WHERE agent = ? AND metric = ? AND ts > ?",
            (agent, metric, now - 3600),
        ).fetchone()[0]
        ratio = count / avg if avg > 0 else 1.0
        indicator = "OK" if ratio >= 0.3 else "DEGRADED"
        print(f"  {agent:15s}  {metric:25s}  baseline={avg:.1f}/hr  now={count}/hr  [{indicator}]")

    print("\n=== ACTIVE PAIN ===")
    rows = conn.execute(
        "SELECT agent, pain_type, message, severity, ts FROM mesh_pain "
        "WHERE resolved_at IS NULL ORDER BY ts DESC LIMIT 10"
    ).fetchall()
    if not rows:
        print("  (no active pain)")
    for agent, pt, msg, sev, ts in rows:
        age = (now - ts) // 60
        print(f"  [{sev}] {agent}: {pt} — {msg} ({age}m ago)")

    print("\n=== SHARED CONTEXT ===")
    try:
        rows = conn.execute(
            "SELECT context_key, agent, value, updated_at FROM mesh_context "
            "ORDER BY context_key"
        ).fetchall()
        if not rows:
            print("  (no shared context)")
        for key, agent, val, ts in rows:
            age = (now - ts) // 60
            # Compact display: truncate long values
            display = val[:120] + "..." if len(val) > 120 else val
            print(f"  {key:20s}  [{agent}]  {display}  ({age}m ago)")
    except Exception:
        print("  (table not yet created)")

    conn.close()


if __name__ == "__main__":
    mesh_status()
