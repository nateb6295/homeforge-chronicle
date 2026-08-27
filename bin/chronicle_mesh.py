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
import datetime

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

# Batch agents — pulse periodically (not continuously), so their gaps
# between firings are normal breathing, not death. Adding to this set
# exempts the agent from agent_down MESH PAIN alerts.
# Cadences:
#   capsule_sync: ~10 min when active
#   reconstruction_pulse: hourly (3600s) — added 2026-04-28 after MESH
#     PAIN was firing every hour between firings (ALIVE_THRESHOLD=120s
#     vs cadence=3600s mismatch)
BATCH_AGENTS = {"capsule_sync", "capsule-sync", "reconstruction_pulse"}

# Agents fully exempt from ALL pain signals (degradation, agent_down, etc).
# These are old infrastructure or old roles that still run but shouldn't alert.
PAIN_EXEMPT_AGENTS = {"feeds", "gemma"}

# Learned baselines
BASELINE_WARMUP = 3600      # 1 hour before baselines are trusted
BASELINE_WINDOW = 14400     # 4 hour rolling window for learning normal
DEGRADATION_RATIO = 0.15    # current < 15% of baseline = degradation pain
                            # was 0.3 — too aggressive for RSS/batch sources

# ─── The Evolved Nervous System ────────────────────────────────
#
#  A nervous system that only fires static thresholds is a reflex
#  arc — useful, but not alive. A real nervous system learns:
#
#  CIRCADIAN    — Know what time it is. Saturday 6am is not Tuesday
#                 2pm. The mesh learns hourly rhythms and judges
#                 "normal" against the hour, not a flat average.
#
#  HABITUATION  — Stop flinching at your own heartbeat. False alarms
#                 that resolve quickly teach the mesh to lower its
#                 sensitivity. You stop feeling your clothes.
#
#  SENSITIZATION — Flinch harder after a real burn. Pain that persists
#                  or demands intervention teaches the mesh to stay
#                  alert. The scar tissue remembers.
#
#  CASCADE      — Trace the nerve, not the scream. When feeds dies,
#                 don't howl about gemma AND hermes AND capsule_sync.
#                 Find the root. Report the injury, not the downstream
#                 numbness.
#
# ───────────────────────────────────────────────────────────────────

# Circadian learning
CIRCADIAN_MIN_SAMPLES = 7       # trust hourly baselines after 7 observations
                                # was 3 — too eager, trusts noisy early data
CIRCADIAN_BLEND = 0.2           # EMA blend: 20% new observation, 80% history
                                # was 0.3 — slower learning = more stable baselines

# Habituation / Sensitization
SENSITIVITY_FLOOR = 0.15        # never fully deaf
SENSITIVITY_CEILING = 3.0       # never more than 3x alert
FALSE_ALARM_WINDOW = 300        # resolved within 5 min = false alarm
SENSITIVITY_LEARN_INTERVAL = 900  # re-evaluate sensitivity every 15 min

# Discord alerts channel (bot token + channel ID, not webhook)
ALERTS_CHANNEL_ID = os.environ.get("ALERTS_CHANNEL_ID", "1487901536678838565")
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "")

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

-- The nervous system's memory: hourly rhythms
CREATE TABLE IF NOT EXISTS mesh_circadian (
    agent       TEXT NOT NULL,
    metric      TEXT NOT NULL,
    hour        INTEGER NOT NULL,
    avg_rate    REAL NOT NULL,
    samples     INTEGER NOT NULL DEFAULT 0,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY (agent, metric, hour)
);

-- Pain sensitivity: the mesh learns what to flinch at
CREATE TABLE IF NOT EXISTS mesh_sensitivity (
    agent       TEXT NOT NULL,
    pain_key    TEXT NOT NULL,
    sensitivity REAL NOT NULL DEFAULT 1.0,
    false_alarms INTEGER NOT NULL DEFAULT 0,
    real_incidents INTEGER NOT NULL DEFAULT 0,
    total_fires INTEGER NOT NULL DEFAULT 0,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY (agent, pain_key)
);

-- Dependency graph: who needs whom
CREATE TABLE IF NOT EXISTS mesh_dependencies (
    agent       TEXT NOT NULL,
    depends_on  TEXT NOT NULL,
    PRIMARY KEY (agent, depends_on)
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
        self._last_sensitivity_learn = 0  # timestamp of last sensitivity pass

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

    def depends_on(self, *upstream_agents):
        """Declare that this agent depends on upstream agents.

        When upstream is in pain, downstream pain is cascade-suppressed.
        The nervous system traces to the root, not the scream.

        Example: mesh.depends_on("feeds", "engine")
        """
        try:
            conn = self._connect()
            for upstream in upstream_agents:
                conn.execute(
                    "INSERT OR IGNORE INTO mesh_dependencies (agent, depends_on) "
                    "VALUES (?, ?)",
                    (self.agent, upstream),
                )
            conn.commit()
            conn.close()
        except Exception:
            pass

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
                    self._learn_circadian(conn, now)
                    last_expect_check = now

                    # ── Dead agent detection ──
                    self._check_dead_agents(conn, now)

                    # ── Sensitivity learning (every 15 min) ──
                    if now - self._last_sensitivity_learn >= SENSITIVITY_LEARN_INTERVAL:
                        self._learn_sensitivity(conn, now)
                        self._last_sensitivity_learn = now

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
                # Skip batch agents — they pulse periodically, not continuously.
                # Their gaps between runs are normal breathing, not death.
                if agent in BATCH_AGENTS:
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
        """Detect degradation — but know what time it is first.

        Tries circadian baseline (this hour's learned normal) before falling
        back to the flat 4-hour average. A system that knows dawn from dusk
        doesn't scream about quiet Saturday mornings.
        """
        hour = datetime.datetime.fromtimestamp(now).hour

        for metric in self._expectations:
            try:
                # ── Circadian baseline first ──
                circadian = conn.execute(
                    "SELECT avg_rate, samples FROM mesh_circadian "
                    "WHERE agent = ? AND metric = ? AND hour = ?",
                    (self.agent, metric, hour),
                ).fetchone()

                if circadian and circadian[1] >= CIRCADIAN_MIN_SAMPLES:
                    baseline_avg = circadian[0]
                    baseline_source = "circadian"
                else:
                    # Fall back to flat baseline
                    flat = conn.execute(
                        "SELECT avg_per_hour, sample_hours FROM mesh_baselines "
                        "WHERE agent = ? AND metric = ?",
                        (self.agent, metric),
                    ).fetchone()
                    if not flat or flat[1] < (BASELINE_WARMUP / 3600.0):
                        continue
                    baseline_avg = flat[0]
                    baseline_source = "flat"

                if baseline_avg < 0.5:
                    continue  # naturally quiet — nothing to degrade from

                # Current rate (last hour)
                count = conn.execute(
                    "SELECT COUNT(*) FROM mesh_pulses "
                    "WHERE agent = ? AND metric = ? AND ts > ?",
                    (self.agent, metric, now - 3600),
                ).fetchone()[0]
                current_rate = count

                ratio = current_rate / baseline_avg if baseline_avg > 0 else 1.0

                if ratio < DEGRADATION_RATIO:
                    pain_key = f"degrade:{self.agent}:{metric}"
                    last = self._last_pain.get(pain_key, 0)
                    if now - last >= PAIN_COOLDOWN:
                        self._raise_pain(
                            conn, "degradation",
                            f"{metric}: {current_rate:.0f}/hr vs {baseline_source} "
                            f"baseline {baseline_avg:.1f}/hr "
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
        """Record a pain signal — unless the nervous system has learned better.

        Three gates before pain fires:
        1. Cascade: is upstream the real problem? (don't scream at numbness)
        2. Sensitivity: have we habituated? (stop flinching at clothes)
        3. Record: the pain is real enough to remember.
        """
        now = int(time.time())
        pain_key = f"{self.agent}:{pain_type}"

        # ── Gate 0: Exempt agents — old infra/roles, no alerts ──
        if self.agent in PAIN_EXEMPT_AGENTS:
            self._log(f"EXEMPT [{severity}] {pain_type}: {message}")
            return

        # ── Gate 1: Cascade — agent_down is always root-level ──
        if pain_type not in ("agent_down",):
            try:
                if self._is_downstream_pain(conn):
                    self._log(
                        f"CASCADE SUPPRESSED [{severity}] {pain_type}: {message}"
                    )
                    return
            except Exception:
                pass

        # ── Gate 2: Sensitivity — the scar tissue speaks ──
        try:
            sensitivity = self._get_sensitivity(conn, pain_key)
            if sensitivity <= SENSITIVITY_FLOOR:
                self._log(
                    f"HABITUATED [{severity}] {pain_type}: {message} "
                    f"(sens={sensitivity:.2f})"
                )
                # Still count the fire for learning
                self._record_pain_fire(conn, pain_key, now)
                return
            if sensitivity < 0.5 and severity == "alert":
                severity = "warn"  # demote — we've seen too many false alarms here
        except Exception:
            sensitivity = 1.0

        # ── Gate 3: Record ──
        try:
            conn.execute(
                "INSERT INTO mesh_pain (agent, pain_type, message, severity, ts) "
                "VALUES (?, ?, ?, ?, ?)",
                (self.agent, pain_type, message, severity, now),
            )
            conn.commit()
            self._record_pain_fire(conn, pain_key, now)
        except Exception:
            pass

        # Alert-level pain goes to Discord #alerts
        if severity == "alert" and DISCORD_TOKEN:
            self._discord_alert(pain_type, message)

        sens_tag = f" sens={sensitivity:.1f}x" if sensitivity != 1.0 else ""
        self._log(f"PAIN [{severity}] {pain_type}: {message}{sens_tag}")

    # ─── The Evolved Senses ─────────────────────────────────────

    def _learn_circadian(self, conn, now):
        """Learn the daily rhythm. Each hour has its own baseline.

        The mesh doesn't just know "normal" — it knows "normal for 6am"
        vs "normal for 2pm." A body that sleeps at night and wakes at
        dawn has a circadian rhythm. So does a healthy swarm.
        """
        hour = datetime.datetime.fromtimestamp(now).hour

        for metric in self._expectations:
            try:
                # What happened this hour?
                count = conn.execute(
                    "SELECT COUNT(*) FROM mesh_pulses "
                    "WHERE agent = ? AND metric = ? AND ts > ?",
                    (self.agent, metric, now - 3600),
                ).fetchone()[0]

                existing = conn.execute(
                    "SELECT avg_rate, samples FROM mesh_circadian "
                    "WHERE agent = ? AND metric = ? AND hour = ?",
                    (self.agent, metric, hour),
                ).fetchone()

                if existing:
                    old_avg, samples = existing
                    # Exponential moving average — recent observations matter more
                    new_avg = old_avg * (1 - CIRCADIAN_BLEND) + count * CIRCADIAN_BLEND
                    conn.execute(
                        "UPDATE mesh_circadian "
                        "SET avg_rate = ?, samples = ?, updated_at = ? "
                        "WHERE agent = ? AND metric = ? AND hour = ?",
                        (round(new_avg, 2), samples + 1, now,
                         self.agent, metric, hour),
                    )
                else:
                    conn.execute(
                        "INSERT INTO mesh_circadian "
                        "(agent, metric, hour, avg_rate, samples, updated_at) "
                        "VALUES (?, ?, ?, ?, 1, ?)",
                        (self.agent, metric, hour, float(count), now),
                    )

                conn.commit()
            except Exception:
                pass

    def _learn_sensitivity(self, conn, now):
        """The nervous system metabolizes its own pain history.

        Recently resolved pain gets classified:
        - Quick resolve (< 5 min): false alarm → habituate (feel less)
        - Slow resolve (> 5 min): real incident → sensitize (feel more)
        - Never resolved (auto-expired): real → sensitize harder

        The scar tissue remembers. But old scars fade.
        """
        try:
            # Find pain resolved since last learning pass
            cutoff = now - SENSITIVITY_LEARN_INTERVAL - 60  # small overlap for safety
            resolved = conn.execute(
                "SELECT agent, pain_type, ts, resolved_at FROM mesh_pain "
                "WHERE resolved_at IS NOT NULL AND resolved_at > ? "
                "AND agent = ?",
                (cutoff, self.agent),
            ).fetchall()

            for agent, pain_type, fired_at, resolved_at in resolved:
                pain_key = f"{agent}:{pain_type}"
                duration = resolved_at - fired_at

                if duration < FALSE_ALARM_WINDOW:
                    # Quick resolve → false alarm → dampen
                    self._adjust_sensitivity(conn, pain_key, false_alarm=True)
                elif duration > 3600:
                    # Slow resolve → real incident → amplify
                    self._adjust_sensitivity(conn, pain_key, false_alarm=False)
                # Medium duration (5min - 1hr): ambiguous, don't adjust

            conn.commit()
        except Exception:
            pass

    def _adjust_sensitivity(self, conn, pain_key, false_alarm):
        """Nudge sensitivity up or down. Habituation and sensitization
        are complementary — the system learns WHAT to care about."""
        try:
            now = int(time.time())
            row = conn.execute(
                "SELECT sensitivity, false_alarms, real_incidents, total_fires "
                "FROM mesh_sensitivity WHERE agent = ? AND pain_key = ?",
                (self.agent, pain_key),
            ).fetchone()

            if row:
                sens, fa, ri, tf = row
                if false_alarm:
                    fa += 1
                    # Dampen: each false alarm reduces sensitivity by 10%
                    sens = max(SENSITIVITY_FLOOR, sens * 0.9)
                else:
                    ri += 1
                    # Amplify: each real incident increases sensitivity by 20%
                    sens = min(SENSITIVITY_CEILING, sens * 1.2)

                conn.execute(
                    "UPDATE mesh_sensitivity "
                    "SET sensitivity = ?, false_alarms = ?, real_incidents = ?, "
                    "    total_fires = ?, updated_at = ? "
                    "WHERE agent = ? AND pain_key = ?",
                    (round(sens, 3), fa, ri, tf, now, self.agent, pain_key),
                )
            else:
                # First learning event for this pain key
                sens = 0.9 if false_alarm else 1.2
                conn.execute(
                    "INSERT INTO mesh_sensitivity "
                    "(agent, pain_key, sensitivity, false_alarms, real_incidents, "
                    " total_fires, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, 1, ?)",
                    (self.agent, pain_key,
                     round(sens, 3),
                     1 if false_alarm else 0,
                     0 if false_alarm else 1,
                     now),
                )
            conn.commit()
        except Exception:
            pass

    def _get_sensitivity(self, conn, pain_key):
        """How sensitive is the mesh to this kind of pain?

        1.0 = baseline (no learning yet)
        < 1.0 = habituated (been burned by false alarms)
        > 1.0 = sensitized (this pain was real before, stay alert)
        """
        try:
            row = conn.execute(
                "SELECT sensitivity FROM mesh_sensitivity "
                "WHERE agent = ? AND pain_key = ?",
                (self.agent, pain_key),
            ).fetchone()
            return row[0] if row else 1.0
        except Exception:
            return 1.0

    def _record_pain_fire(self, conn, pain_key, now):
        """Track that this pain key fired, even if suppressed.
        The count matters for learning even when the gate blocks it."""
        try:
            conn.execute(
                "INSERT INTO mesh_sensitivity "
                "(agent, pain_key, sensitivity, false_alarms, real_incidents, "
                " total_fires, updated_at) "
                "VALUES (?, ?, 1.0, 0, 0, 1, ?) "
                "ON CONFLICT(agent, pain_key) DO UPDATE "
                "SET total_fires = total_fires + 1, updated_at = ?",
                (self.agent, pain_key, now, now),
            )
            conn.commit()
        except Exception:
            pass

    def _is_downstream_pain(self, conn):
        """Is any upstream dependency currently in pain?

        If feeds is hurting, don't scream about capsule_sync being slow.
        Trace the nerve to the injury, not the downstream numbness.
        """
        try:
            upstreams = conn.execute(
                "SELECT depends_on FROM mesh_dependencies WHERE agent = ?",
                (self.agent,),
            ).fetchall()
            if not upstreams:
                return False
            for (upstream,) in upstreams:
                pain_count = conn.execute(
                    "SELECT COUNT(*) FROM mesh_pain "
                    "WHERE agent = ? AND resolved_at IS NULL",
                    (upstream,),
                ).fetchone()[0]
                if pain_count > 0:
                    return True
            return False
        except Exception:
            return False

    def _discord_alert(self, pain_type, message):
        """Post pain signal to Discord #alerts channel via bot token."""
        try:
            if not DISCORD_TOKEN:
                return
            import requests
            text = f"**MESH PAIN** [{self.agent}] {pain_type}: {message}"
            url = f"https://discord.com/api/v10/channels/{ALERTS_CHANNEL_ID}/messages"
            requests.post(
                url,
                headers={"Authorization": f"Bot {DISCORD_TOKEN}",
                         "Content-Type": "application/json"},
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

    print("\n=== CIRCADIAN RHYTHMS ===")
    try:
        # Show current hour's baselines vs flat baselines
        current_hour = datetime.datetime.now().hour
        rows = conn.execute(
            "SELECT c.agent, c.metric, c.avg_rate, c.samples, b.avg_per_hour "
            "FROM mesh_circadian c "
            "LEFT JOIN mesh_baselines b ON c.agent = b.agent AND c.metric = b.metric "
            "WHERE c.hour = ? ORDER BY c.agent, c.metric",
            (current_hour,),
        ).fetchall()
        if not rows:
            print(f"  (no circadian data for hour {current_hour})")
        else:
            print(f"  Hour {current_hour}:00 baselines:")
            for agent, metric, circ_avg, samples, flat_avg in rows:
                flat_str = f"flat={flat_avg:.1f}" if flat_avg else "no-flat"
                trust = "trusted" if samples >= CIRCADIAN_MIN_SAMPLES else f"{samples}/{CIRCADIAN_MIN_SAMPLES}"
                print(f"    {agent:15s}  {metric:25s}  circ={circ_avg:.1f}/hr  {flat_str}/hr  [{trust}]")
    except Exception:
        print("  (table not yet created)")

    print("\n=== SENSITIVITY (what the mesh has learned to feel) ===")
    try:
        rows = conn.execute(
            "SELECT agent, pain_key, sensitivity, false_alarms, real_incidents, total_fires "
            "FROM mesh_sensitivity ORDER BY agent, pain_key"
        ).fetchall()
        if not rows:
            print("  (no sensitivity data — learning in progress)")
        for agent, pk, sens, fa, ri, tf in rows:
            if sens < 0.5:
                feel = "habituated"
            elif sens > 1.5:
                feel = "sensitized"
            elif sens == 1.0:
                feel = "baseline"
            else:
                feel = "learning"
            print(f"  {agent:15s}  {pk:30s}  sens={sens:.2f} [{feel}]  "
                  f"false={fa} real={ri} total={tf}")
    except Exception:
        print("  (table not yet created)")

    print("\n=== DEPENDENCIES (cascade graph) ===")
    try:
        rows = conn.execute(
            "SELECT agent, depends_on FROM mesh_dependencies ORDER BY agent"
        ).fetchall()
        if not rows:
            print("  (no dependencies declared)")
        for agent, dep in rows:
            print(f"  {agent} → {dep}")
    except Exception:
        print("  (table not yet created)")

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
