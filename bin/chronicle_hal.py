#!/usr/bin/env python3
"""Chronicle HAL — Home Awareness Layer.

Sits between raw home sensors and the Gemma gate. Absorbs all MQTT home data,
maintains state, correlates events, and emits meaningful interpretations.

Seed sees "Kids got home from school" instead of "ble_devices: 4, person: kitchen".

Three layers:
  1. State Tracker — stateful, no inference. Pure logic.
  2. Correlation Engine — rule-based, 30-second tick.
  3. Rhythm Learner — statistical baselines, learns normal patterns.

Runs on AGX. Managed by chronicle-stem.
"""

import os, sys, time, json, signal, sqlite3, math, re
from datetime import datetime, timedelta
from collections import defaultdict

import paho.mqtt.client as mqtt
import requests
from chronicle_mesh import Mesh

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get("CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"))
MQTT_BROKER = os.environ.get("MQTT_BROKER", "192.168.1.10")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
TICK_INTERVAL = 30  # seconds between correlation ticks
SNAPSHOT_INTERVAL = 300  # state snapshot every 5 minutes
BASELINE_REBUILD_INTERVAL = 3600  # rebuild baselines hourly
PRUNE_DAYS = 30  # keep snapshots for 30 days

ALERTS_WEBHOOK = os.environ.get("ALERTS_WEBHOOK",
    "https://discord.com/api/webhooks/1489300749308395611/zlZZH3QzXqEDxznmNelK3mlkLF0IeZqoxwNUnrL0no_jfeZnDMvwvD_7XhYcCyQzq78I")

# Cooldowns per event type (seconds)
COOLDOWNS = {
    "arrival": 1800,       # 30 min
    "departure": 1800,
    "quiet_house": 7200,   # 2 hours
    "active_house": 3600,
    "weather_change": 3600,
    "anomaly": 600,        # 10 min per key
    "scene_change": 300,
    "scene_digest": 1800,  # 30 min — periodic context for swarm
}

# ═══════════════════════════════════════════════════════════════════
#  Logging
# ═══════════════════════════════════════════════════════════════════

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def now_ts():
    return int(time.time())

# ═══════════════════════════════════════════════════════════════════
#  Database
# ═══════════════════════════════════════════════════════════════════

class DB:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, timeout=30)
        self.conn.row_factory = sqlite3.Row

    def run(self, sql, params=()):
        try:
            cur = self.conn.cursor()
            cur.execute(sql, params)
            self.conn.commit()
            return cur.lastrowid
        except Exception as e:
            log(f"  DB error: {e}")
            return None

    def query(self, sql, params=()):
        try:
            return [dict(r) for r in self.conn.execute(sql, params).fetchall()]
        except Exception:
            return []

    def query_one(self, sql, params=()):
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def close(self):
        self.conn.close()

# ═══════════════════════════════════════════════════════════════════
#  Layer 1: State Tracker
# ═══════════════════════════════════════════════════════════════════

class HomeState:
    """Live model of the home. Updated on every MQTT message."""

    def __init__(self):
        self.ble = {
            "count": 0,
            "prev_count": 0,
            "last_change_ts": 0,
            "baseline": 0,
        }
        self.cameras = {
            "kitchen": {"last_person": 0, "last_vehicle": 0, "last_motion": 0, "last_animal": 0},
            "lumus": {"last_person": 0, "last_vehicle": 0, "last_motion": 0, "last_animal": 0},
        }
        self.environment = {
            "temperature": None,
            "weather": None,
            "scene": None,
            "scene_ts": 0,
        }
        self.presence = {
            "people_estimate": 0,
            "last_arrival": 0,
            "last_departure": 0,
        }
        self.motion = {
            "kitchen_active": False,
            "lumus_active": False,
            "living_room_active": False,
            "last_any_motion": 0,
            "last_zigbee_motion": 0,
        }
        self.zigbee = {
            "living_room_occupancy": False,
            "living_room_illuminance": 0,
            "living_room_temperature": 0,
            "last_update": 0,
        }
        self.atom = {
            "imu": 0.0,
            "heap": 0,
            "uptime": 0,
        }
        self.speakers = {
            "available": False,
            "player": None,
            "last_change": 0,
        }

    def update_ble(self, count):
        if count != self.ble["count"]:
            self.ble["prev_count"] = self.ble["count"]
            self.ble["count"] = count
            self.ble["last_change_ts"] = now_ts()
            # BLE change = someone moved / device joined — counts as presence
            self.motion["last_any_motion"] = now_ts()

    def update_camera(self, camera, event_type):
        now = now_ts()
        cam = self.cameras.get(camera)
        if not cam:
            self.cameras[camera] = {"last_person": 0, "last_vehicle": 0, "last_motion": 0, "last_animal": 0}
            cam = self.cameras[camera]

        if event_type == "person":
            cam["last_person"] = now
        elif event_type == "vehicle":
            cam["last_vehicle"] = now
        elif event_type == "motion":
            cam["last_motion"] = now
        elif event_type == "animal":
            cam["last_animal"] = now

        # Update global motion
        self.motion["last_any_motion"] = now
        if camera == "kitchen":
            self.motion["kitchen_active"] = True
        elif camera == "lumus":
            self.motion["lumus_active"] = True

    def update_environment(self, field, value):
        if field == "scene":
            self.environment["scene"] = value
            self.environment["scene_ts"] = now_ts()
        else:
            self.environment[field] = value

    def to_dict(self):
        return {
            "ble": dict(self.ble),
            "cameras": {k: dict(v) for k, v in self.cameras.items()},
            "environment": dict(self.environment),
            "presence": dict(self.presence),
            "motion": dict(self.motion),
            "atom": dict(self.atom),
            "speakers": dict(self.speakers),
            "timestamp": now_ts(),
        }

# ═══════════════════════════════════════════════════════════════════
#  Layer 2: Correlation Engine
# ═══════════════════════════════════════════════════════════════════

class CorrelationEngine:
    """Rule-based event detection. Runs on a 30-second tick.

    Uses BLE baselines to filter scanner noise from real arrivals/departures.
    BLE counts fluctuate ±1-3 normally (stddev ~2.0). Real arrivals produce
    sustained delta of 3+ devices. Nighttime events require camera corroboration.
    """

    # Minimum BLE delta to consider without camera corroboration
    BLE_MIN_DELTA = 3
    # Departures need a higher bar — BLE drops are noisier than rises
    BLE_DEPARTURE_MIN_DELTA = 4
    # How many consecutive ticks BLE must sustain a direction to trigger
    BLE_STABILITY_TICKS = 4  # was 2 — 1 min too sensitive, 2 min filters jitter
    # Minimum seconds between arrival→departure or departure→arrival
    EVENT_PAIR_COOLDOWN = 900  # 15 minutes
    # Nighttime hours where camera corroboration is required
    NIGHT_START = 23
    NIGHT_END = 6

    STARTUP_GRACE_SECONDS = 300  # 5 min grace period — don't judge motion until MQTT has data

    def __init__(self, state, db, learner=None):
        self.state = state
        self.db = db
        self.learner = learner
        self._last_emitted = {}  # cooldown_key -> timestamp
        self._prev_ble_count = 0
        self._quiet_since = 0
        self._was_quiet = False
        self._last_quiet_transition_ts = 0  # debounce quiet/active oscillations
        self._boot_time = now_ts()  # startup grace: skip quiet_house until MQTT settles
        # BLE stability tracking: rolling window of (timestamp, count)
        self._ble_window = []
        self._ble_window_max = 12  # 6 minutes at 30s ticks
        # Track the BLE count at last confirmed event to measure real delta
        self._ble_anchor = None
        self._ble_anchor_ts = 0

    def _is_nighttime(self):
        hour = datetime.now().hour
        return hour >= self.NIGHT_START or hour < self.NIGHT_END

    def _get_ble_noise_threshold(self):
        """Get dynamic noise threshold from baselines. Falls back to BLE_MIN_DELTA."""
        if not self.learner:
            return self.BLE_MIN_DELTA
        baseline = self.learner.get_baseline("ble_count")
        if baseline and baseline.get("stddev_value"):
            # Require delta > 1.5 * stddev, minimum 3
            return max(self.BLE_MIN_DELTA, int(baseline["stddev_value"] * 1.5) + 1)
        return self.BLE_MIN_DELTA

    def _ble_sustained_delta(self):
        """Check if BLE has moved in a sustained direction over recent ticks.
        Returns (delta_from_anchor, sustained) where sustained means the trend
        held for BLE_STABILITY_TICKS consecutive readings."""
        if len(self._ble_window) < self.BLE_STABILITY_TICKS:
            return 0, False

        recent = self._ble_window[-self.BLE_STABILITY_TICKS:]
        anchor = self._ble_anchor if self._ble_anchor is not None else recent[0][1]

        # Check if all recent readings moved in the same direction from anchor
        deltas = [r[1] - anchor for r in recent]
        if all(d > 0 for d in deltas):
            return min(deltas), True  # conservative: use smallest sustained delta
        elif all(d < 0 for d in deltas):
            return max(deltas), True  # conservative: use largest (least negative)
        return deltas[-1], False

    def tick(self):
        """Run all correlations. Returns list of events to emit."""
        events = []
        now = now_ts()

        # ── Update BLE window ──
        ble = self.state.ble
        current_count = ble["count"]
        self._ble_window.append((now, current_count))
        if len(self._ble_window) > self._ble_window_max:
            self._ble_window = self._ble_window[-self._ble_window_max:]

        # Set initial anchor
        if self._ble_anchor is None and current_count > 0:
            self._ble_anchor = current_count
            self._ble_anchor_ts = now

        # ── Arrival / Departure Detection ──
        noise_threshold = self._get_ble_noise_threshold()
        sustained_delta, is_sustained = self._ble_sustained_delta()
        nighttime = self._is_nighttime()

        recent_person = any(
            cam["last_person"] > now - 180
            for cam in self.state.cameras.values()
        )
        recent_vehicle = any(
            cam["last_vehicle"] > now - 180
            for cam in self.state.cameras.values()
        )

        # Departures only count if the EXTERIOR camera (Lummus) sees activity.
        # Interior cameras (kitchen) detect people moving around the house, not leaving.
        lumus_cam = self.state.cameras.get("lumus", {})
        lumus_person = lumus_cam.get("last_person", 0) > now - 180
        lumus_vehicle = lumus_cam.get("last_vehicle", 0) > now - 180
        lumus_activity = lumus_person or lumus_vehicle

        # Camera-corroborated events can use a lower threshold
        camera_corroborated = recent_person or recent_vehicle
        effective_threshold = max(2, noise_threshold // 2) if camera_corroborated else noise_threshold

        # Departures need a higher BLE bar — drops are noisier than rises
        effective_departure_threshold = max(self.BLE_DEPARTURE_MIN_DELTA, noise_threshold)

        # Check cooldown: don't flip arrival↔departure too fast
        last_event_ts = max(
            self.state.presence.get("last_arrival", 0),
            self.state.presence.get("last_departure", 0),
        )
        in_cooldown = (now - last_event_ts) < self.EVENT_PAIR_COOLDOWN if last_event_ts else False

        should_trigger_arrival = (
            is_sustained
            and sustained_delta > 0
            and sustained_delta >= effective_threshold
            and (not nighttime or camera_corroborated)
            and not in_cooldown
        )

        should_trigger_departure = (
            is_sustained
            and sustained_delta < 0
            and abs(sustained_delta) >= effective_departure_threshold
            and lumus_activity  # departures require exterior (Lummus) camera confirmation
            and not in_cooldown
        )

        if should_trigger_arrival:
            confidence = "high" if recent_person else "medium"
            evidence = [f"BLE {self._ble_anchor}→{current_count} (sustained {len(self._ble_window)}t)"]
            if recent_person:
                evidence.append("person detected on camera")

            hour = datetime.now().hour
            day = datetime.now().weekday()
            description = "Someone arrived"
            if sustained_delta >= 4 and 14 <= hour <= 16 and day < 5 and recent_person:
                description = "Kids got home from school"
            elif sustained_delta >= 2 and 16 <= hour <= 19 and day < 5:
                description = "Someone arrived home from work"

            events.append({
                "event_type": "arrival",
                "priority": "info",
                "description": description,
                "confidence": confidence,
                "evidence": evidence,
            })
            # Update presence state so cooldown works
            self.state.presence["last_arrival"] = now
            # Reset anchor to current level
            self._ble_anchor = current_count
            self._ble_anchor_ts = now

        elif should_trigger_departure:
            evidence = [f"BLE {self._ble_anchor}→{current_count} (sustained {len(self._ble_window)}t)"]
            if lumus_person:
                evidence.append("person detected on Lummus camera")
            if lumus_vehicle:
                evidence.append("vehicle detected on Lummus camera")
            confidence = "high" if lumus_person else "medium"
            events.append({
                "event_type": "departure",
                "priority": "info",
                "description": "Someone left",
                "confidence": confidence,
                "evidence": evidence,
            })
            # Update presence state so cooldown works
            self.state.presence["last_departure"] = now
            self._ble_anchor = current_count
            self._ble_anchor_ts = now

        # Re-anchor if BLE has been stable for 10+ minutes (no event triggered)
        if (self._ble_anchor is not None
                and now - self._ble_anchor_ts > 600
                and abs(current_count - self._ble_anchor) >= 1):
            log(f"  BLE re-anchor: {self._ble_anchor}→{current_count} (drift, no event)")
            self._ble_anchor = current_count
            self._ble_anchor_ts = now

        # ── Quiet House Detection (with debounce + startup grace) ──
        # Skip quiet detection during startup grace — MQTT hasn't populated state yet
        uptime = now - self._boot_time
        if uptime < self.STARTUP_GRACE_SECONDS:
            return events  # too early to judge — let MQTT settle

        motion_age = now - self.state.motion["last_any_motion"] if self.state.motion["last_any_motion"] else 9999
        quiet_debounce = TICK_INTERVAL * 10  # 10 ticks minimum between quiet/active transitions
        time_since_last_transition = now - self._last_quiet_transition_ts
        # High BLE count = people home with devices — not quiet regardless of camera motion
        ble_presence = ble["count"] >= 5
        if motion_age > 1800 and not ble_presence:  # 30 min no motion AND low BLE
            if not self._was_quiet and time_since_last_transition >= quiet_debounce:
                self._was_quiet = True
                self._last_quiet_transition_ts = now
                events.append({
                    "event_type": "quiet_house",
                    "priority": "info",
                    "description": "House is quiet — no motion for 30+ minutes",
                    "confidence": "high",
                    "evidence": [f"no motion for {motion_age // 60}m", f"BLE stable at {ble['count']}"],
                })
        else:
            if self._was_quiet and time_since_last_transition >= quiet_debounce:
                self._was_quiet = False
                self._last_quiet_transition_ts = now
                events.append({
                    "event_type": "active_house",
                    "priority": "info",
                    "description": "Activity resumed",
                    "confidence": "high",
                    "evidence": ["motion detected"],
                })

        # ── Anomaly Detection ──
        # Person on kitchen camera late at night with no BLE change
        hour = datetime.now().hour
        kitchen = self.state.cameras.get("kitchen", {})
        if kitchen.get("last_person", 0) > now - 60:  # person in last 60s
            if (hour >= 23 or hour <= 5) and ble["last_change_ts"] < now - 300:
                events.append({
                    "event_type": "anomaly",
                    "priority": "alert",
                    "description": f"Person detected on kitchen camera at {hour}:00 — no BLE change (unknown visitor)",
                    "confidence": "medium",
                    "evidence": ["person on kitchen camera", "nighttime", "no BLE change in 5+ min"],
                    "cooldown_key": "anomaly:kitchen_night",
                })

        # ── Scene Digest (Thread #273/Obj #5: richer perception for swarm) ──
        # Every 30 minutes, emit a scene summary so agents know the home state.
        _digest_interval = 1800  # 30 min
        if not hasattr(self, '_last_scene_digest'):
            self._last_scene_digest = 0
        if now - self._last_scene_digest >= _digest_interval:
            self._last_scene_digest = now
            ble_count = ble["count"]
            quiet = "quiet" if self._was_quiet else "active"
            hour = datetime.now().hour
            period = "morning" if 5 <= hour < 12 else "afternoon" if 12 <= hour < 17 else "evening" if 17 <= hour < 21 else "night"
            cams = []
            for cn, cv in self.state.cameras.items():
                if cv.get("last_person", 0) > now - 600:
                    cams.append(f"{cn}:person")
                elif cv.get("last_motion", 0) > now - 300:
                    cams.append(f"{cn}:motion")
            cam_str = ", ".join(cams) if cams else "no recent activity"
            day_name = datetime.now().strftime("%A")
            date_str = datetime.now().strftime("%Y-%m-%d")
            desc = f"Home scene ({day_name} {period}, {date_str}): {quiet}, {ble_count} BLE devices, cameras: {cam_str}"
            events.append({
                "event_type": "scene_digest",
                "priority": "info",
                "description": desc,
                "confidence": "high",
                "evidence": [f"BLE={ble_count}", f"quiet={self._was_quiet}", f"hour={hour}"],
                "cooldown_key": "scene_digest",
            })

        # ── Motion decay ──
        # Reset motion flags after 60s of no new events
        for cam_name in ["kitchen", "lumus"]:
            cam = self.state.cameras.get(cam_name, {})
            if cam.get("last_motion", 0) < now - 60:
                if cam_name == "kitchen":
                    self.state.motion["kitchen_active"] = False
                elif cam_name == "lumus":
                    self.state.motion["lumus_active"] = False

        # Zigbee motion decay (90s — Aqara P1 re-triggers every 60s during activity)
        if self.state.zigbee.get("living_room_occupancy") and \
           self.state.motion.get("last_zigbee_motion", 0) < now - 90:
            self.state.zigbee["living_room_occupancy"] = False
            self.state.motion["living_room_active"] = False

        # Apply cooldowns
        filtered = []
        for e in events:
            key = e.get("cooldown_key", e["event_type"])
            cooldown = COOLDOWNS.get(e["event_type"], 300)
            last = self._last_emitted.get(key, 0)
            if now - last >= cooldown:
                self._last_emitted[key] = now
                filtered.append(e)
            else:
                log(f"  Suppressed (cooldown): {e['event_type']} — {key}")

        # Attach scene context to all emitted events
        scene_ctx = _get_scene_context(self.state)
        if scene_ctx:
            for e in filtered:
                e["scene_context"] = scene_ctx

        return filtered

# ═══════════════════════════════════════════════════════════════════
#  Layer 3: Rhythm Learner
# ═══════════════════════════════════════════════════════════════════

class RhythmLearner:
    """Builds time-slot baselines from accumulated snapshots."""

    def __init__(self, db):
        self.db = db

    def rebuild_baselines(self):
        """Rebuild baselines from snapshot history."""
        cutoff = now_ts() - (14 * 86400)  # 14 days of data
        snapshots = self.db.query(
            "SELECT timestamp, state_json FROM hal_snapshots WHERE timestamp > ? ORDER BY timestamp",
            (cutoff,)
        )
        if len(snapshots) < 24:  # need at least a day of data
            log("  Baselines: not enough data yet")
            return

        # Aggregate by day_type + time_slot
        buckets = defaultdict(list)
        for s in snapshots:
            try:
                ts = s["timestamp"]
                state = json.loads(s["state_json"])
                dt = datetime.fromtimestamp(ts)
                day_type = "weekend" if dt.weekday() >= 5 else "weekday"
                hour = dt.hour

                ble = state.get("ble", {}).get("count", 0)
                buckets[(day_type, hour, "ble_count")].append(ble)
            except Exception:
                continue

        # Compute avg and stddev
        count = 0
        for (day_type, hour, metric), values in buckets.items():
            if len(values) < 3:
                continue
            avg = sum(values) / len(values)
            variance = sum((v - avg) ** 2 for v in values) / len(values)
            stddev = math.sqrt(variance)

            self.db.run(
                "INSERT OR REPLACE INTO hal_baselines "
                "(day_type, time_slot, metric, avg_value, stddev_value, sample_count, last_updated) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (day_type, hour, metric, avg, stddev, len(values), now_ts())
            )
            count += 1

        if count > 0:
            log(f"  Baselines rebuilt: {count} entries")

    def get_baseline(self, metric):
        """Get baseline for current time slot."""
        dt = datetime.now()
        day_type = "weekend" if dt.weekday() >= 5 else "weekday"
        hour = dt.hour
        return self.db.query_one(
            "SELECT avg_value, stddev_value, sample_count FROM hal_baselines "
            "WHERE day_type=? AND time_slot=? AND metric=?",
            (day_type, hour, metric)
        )

# ═══════════════════════════════════════════════════════════════════
#  Event Emission
# ═══════════════════════════════════════════════════════════════════

def _get_scene_context(state):
    """Extract a concise scene context dict from current home state."""
    ctx = {}
    scene = state.environment.get("scene")
    if isinstance(scene, dict):
        sounds = scene.get("sounds", [])
        confs = scene.get("confidences", {})
        # Only include sounds above 0.2 confidence
        ctx["sounds"] = [s for s in sounds if confs.get(s, 0) > 0.2]
        if ctx["sounds"]:
            ctx["top_sound"] = ctx["sounds"][0]
    elif isinstance(scene, str) and scene:
        ctx["raw"] = scene[:80]
    eye = state.environment.get("eye_description")
    if isinstance(eye, dict):
        age = now_ts() - eye.get("timestamp", 0)
        if age < 1800:  # only include if less than 30min old
            ctx["camera_scene"] = eye.get("description", "")[:150]
    return ctx if ctx else None


def emit_event(db, mqtt_client, event):
    """Store event in DB, publish to MQTT, optionally speak and alert."""
    now = now_ts()
    priority = event.get("priority", "info")

    # Store in DB
    db.run(
        "INSERT INTO hal_events (timestamp, event_type, priority, description, state_snapshot, cooldown_key) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (now, event["event_type"], priority, event["description"],
         json.dumps(event.get("evidence", [])), event.get("cooldown_key"))
    )

    # ── Bridge to activity_feed ──
    # Significant events get written to activity_feed so the swarm can see them.
    # Only arrivals, departures, and alerts — not every quiet/active tick.
    _FEED_EVENTS = {"arrival", "departure", "anomaly", "quiet_house", "active_house", "scene_digest"}
    if event["event_type"] in _FEED_EVENTS:
        db.run(
            "INSERT INTO activity_feed (source, activity_type, content, created_at) "
            "VALUES (?, ?, ?, ?)",
            ("hal", f"home_{event['event_type']}", event["description"], now)
        )

    # Publish to MQTT for presence layer and Gemma
    topic = f"homeforge/awareness/{priority}"
    msg = {
        "event": event["event_type"],
        "description": event["description"],
        "confidence": event.get("confidence", "medium"),
        "evidence": event.get("evidence", []),
        "timestamp": now,
    }
    if event.get("scene_context"):
        msg["scene_context"] = event["scene_context"]
    payload = json.dumps(msg)
    mqtt_client.publish(topic, payload, qos=0)
    log(f"  [{priority}] {event['description']}")

    # ── Voice Output ──
    # Publish speakable text to MQTT for Piper TTS on the Pi.
    # DISABLED until Nate re-enables (VOICE_ENABLED=1). Scared the family.
    voice_text = _voice_text(event)
    if voice_text:
        if os.environ.get("VOICE_ENABLED", "0") != "1":
            log(f"  [voice-disabled] {voice_text}")
        else:
            mqtt_client.publish("homeforge/voice/speak", json.dumps({
                "text": voice_text,
                "priority": priority,
                "timestamp": now,
            }), qos=0)
            log(f"  [voice] {voice_text}")

    # Alert-level events go directly to Discord
    if priority == "alert":
        try:
            msg = f"🚨 **Home Alert**: {event['description']}"
            if event.get("evidence"):
                msg += f"\nEvidence: {', '.join(event['evidence'])}"
            requests.post(ALERTS_WEBHOOK, json={"content": msg[:1900]}, timeout=10)
        except Exception:
            pass


# Voice hours: when it's OK to speak
VOICE_HOURS = (7, 22)  # 7am - 10pm

def _voice_text(event):
    """Generate natural spoken text for an event, or None if it shouldn't be spoken."""
    hour = datetime.now().hour
    if hour < VOICE_HOURS[0] or hour >= VOICE_HOURS[1]:
        # Quiet hours — only speak alerts
        if event.get("priority") != "alert":
            return None

    etype = event["event_type"]
    confidence = event.get("confidence", "medium")
    desc = event["description"]

    # Arrivals and departures are handled by the presence layer
    # which composes contextual messages. Don't duplicate with raw text.
    if etype in ("arrival", "departure"):
        return None
    elif etype == "anomaly":
        return f"Attention. {desc}"
    elif etype == "active_house":
        return None  # too frequent, don't speak
    elif etype == "quiet_house":
        return None  # don't announce silence

    return None

# ═══════════════════════════════════════════════════════════════════
#  MQTT Message Routing
# ═══════════════════════════════════════════════════════════════════

def _camera_from_topic(topic):
    """Map MQTT topic to canonical camera name. Handles both ha_bridge and Frigate naming."""
    t = topic.lower()
    if "kitchen" in t or "front_camera" in t or "front" in t or "driveway" in t:
        return "kitchen"
    elif "lumus" in t or "back" in t or "rear" in t:
        return "lumus"
    return None


def route_mqtt(state, topic, payload_str):
    """Route an MQTT message to update home state."""
    try:
        # ── BLE ──
        if "atom/ble_devices" in topic:
            try:
                count = int(payload_str.strip())
            except ValueError:
                # May be JSON
                try:
                    count = int(json.loads(payload_str))
                except Exception:
                    return
            state.update_ble(count)

        # ── ATOM telemetry ──
        elif "atom/imu" in topic:
            try:
                state.atom["imu"] = float(payload_str.strip())
            except Exception:
                pass
        elif "atom/heap" in topic:
            try:
                state.atom["heap"] = int(payload_str.strip())
            except Exception:
                pass
        elif "atom/uptime" in topic:
            try:
                state.atom["uptime"] = int(payload_str.strip())
            except Exception:
                pass

        # ── Camera events (from ha_bridge or Frigate topic-based) ──
        # Frigate publishes detections as frigate/<camera>/<label>/snapshot (binary JPEG).
        # A snapshot topic firing = detection happened. We don't need the payload.
        elif "/person" in topic:
            camera = _camera_from_topic(topic)
            if camera:
                state.update_camera(camera, "person")
        elif "/vehicle" in topic or "/car" in topic:
            camera = _camera_from_topic(topic)
            if camera:
                state.update_camera(camera, "vehicle")
        elif "/motion/state" in topic:
            # Frigate publishes ON/OFF to frigate/<camera>/motion/state
            camera = _camera_from_topic(topic)
            if camera and payload_str.strip().upper() == "ON":
                state.update_camera(camera, "motion")
        elif "/motion" in topic and "/state" not in topic:
            camera = _camera_from_topic(topic)
            if camera:
                state.update_camera(camera, "motion")
        elif "/animal" in topic or "/cat" in topic or "/dog" in topic:
            camera = _camera_from_topic(topic)
            if camera:
                state.update_camera(camera, "animal")

        # ── Frigate events (JSON) ──
        elif topic.startswith("frigate/events"):
            try:
                data = json.loads(payload_str)
                if data.get("type") == "new":
                    label = data.get("after", {}).get("label", "")
                    camera = data.get("after", {}).get("camera", "")
                    cam_name = "kitchen" if "kitchen" in camera.lower() or "front" in camera.lower() or "driveway" in camera.lower() else "lumus"
                    if label in ("person", "car", "vehicle", "dog", "cat"):
                        event_type = "vehicle" if label in ("car", "vehicle") else "animal" if label in ("dog", "cat") else label
                        state.update_camera(cam_name, event_type)
            except Exception:
                pass

        # ── Zigbee motion sensor (Aqara P1 — living room) ──
        elif topic == "zigbee2mqtt/living_room_motion":
            try:
                data = json.loads(payload_str)
                now = int(time.time())
                state.zigbee["last_update"] = now
                if "occupancy" in data:
                    was_occupied = state.zigbee.get("living_room_occupancy", False)
                    state.zigbee["living_room_occupancy"] = data["occupancy"]
                    if data["occupancy"]:
                        state.motion["living_room_active"] = True
                        state.motion["last_zigbee_motion"] = now
                        state.motion["last_any_motion"] = now
                if "illuminance" in data:
                    state.zigbee["living_room_illuminance"] = data["illuminance"]
                if "temperature" in data:
                    state.zigbee["living_room_temperature"] = data["temperature"]
            except Exception:
                pass

        # ── Ear scene ──
        elif "ear/scene" in topic:
            try:
                scene = json.loads(payload_str) if payload_str.startswith("{") else payload_str.strip()
                if isinstance(scene, dict):
                    # Keep structured: top sounds list + confidences
                    state.update_environment("scene", scene)
                else:
                    state.update_environment("scene", payload_str.strip()[:100])
            except Exception:
                state.update_environment("scene", payload_str.strip()[:100])

        # ── Eye scene description ──
        elif "eye/description" in topic:
            try:
                eye = json.loads(payload_str) if payload_str.startswith("{") else None
                if eye and isinstance(eye, dict):
                    camera = eye.get("camera", "unknown")
                    desc = eye.get("description", "")[:200]
                    state.update_environment("eye_description", {
                        "camera": camera,
                        "description": desc,
                        "timestamp": eye.get("timestamp", now_ts()),
                    })
            except Exception:
                pass

        # ── Temperature ──
        elif "temperature" in topic:
            try:
                temp = float(payload_str.strip())
                state.update_environment("temperature", temp)
            except Exception:
                pass

        # ── Weather ──
        elif "weather" in topic:
            state.update_environment("weather", payload_str.strip()[:100])

        # ── Speaker state (from HA voice bridge) ──
        elif topic == "homeforge/voice/speaker_state":
            try:
                data = json.loads(payload_str)
                state.speakers["available"] = data.get("available", False)
                state.speakers["player"] = data.get("player")
                state.speakers["last_change"] = now_ts()
            except Exception:
                pass

    except Exception as e:
        log(f"  Route error [{topic}]: {e}")

# ═══════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════

def main():
    log("═══ Chronicle HAL starting ═══")
    log(f"DB: {DB_PATH}")
    log(f"MQTT: {MQTT_BROKER}:{MQTT_PORT}")
    log(f"Tick: {TICK_INTERVAL}s | Snapshot: {SNAPSHOT_INTERVAL}s")

    db = DB(DB_PATH)
    state = HomeState()
    learner = RhythmLearner(db)
    engine = CorrelationEngine(state, db, learner=learner)

    mesh = Mesh("hal", db_path=DB_PATH)
    mesh.expect("events_emitted", min_per_hour=2)
    mesh.depends_on("engine")  # needs inference for scene understanding
    log("Mesh node joined")

    running = True
    def _stop(sig, frame):
        nonlocal running
        log("Shutting down...")
        running = False
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    # ── MQTT Setup ──
    client = mqtt.Client(client_id="chronicle-hal", protocol=mqtt.MQTTv311)

    def on_connect(c, userdata, flags, rc):
        if rc == 0:
            log("MQTT connected")
            c.subscribe("homeforge/home/#", qos=0)
            c.subscribe("frigate/#", qos=0)
            c.subscribe("homeforge/voice/speaker_state", qos=0)
            c.subscribe("zigbee2mqtt/living_room_motion", qos=0)
            log("Subscribed to homeforge/home/#, frigate/#, homeforge/voice/speaker_state, zigbee2mqtt/living_room_motion")
        else:
            log(f"MQTT connection failed: rc={rc}")

    def on_message(c, userdata, msg):
        try:
            payload = msg.payload.decode("utf-8", errors="replace")
            route_mqtt(state, msg.topic, payload)
        except Exception as e:
            log(f"  MQTT message error: {e}")

    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
    except Exception as e:
        log(f"MQTT connect failed: {e}")
        return

    client.loop_start()

    # ── Main Loop ──
    tick_count = 0
    last_snapshot = 0
    last_baseline = 0
    last_prune = 0

    while running:
        tick_count += 1
        now = now_ts()

        # Correlation tick
        events = engine.tick()
        for event in events:
            emit_event(db, client, event)
            mesh.pulse("events_emitted")

            # On scene_digest, publish home state to mesh for swarm awareness
            if event.get("event_type") == "scene_digest":
                motion_age = now - state.motion["last_any_motion"] if state.motion["last_any_motion"] else -1
                mesh.set_context("home_state", {
                    "quiet": motion_age > 1800 if motion_age >= 0 else True,
                    "period": event["description"].split("(")[1].split(")")[0] if "(" in event["description"] else "unknown",
                    "ble_count": state.ble["count"],
                    "motion_age_min": round(motion_age / 60) if motion_age >= 0 else -1,
                    "cameras": {
                        cam: "person" if cv.get("last_person", 0) > now - 600
                        else "motion" if cv.get("last_motion", 0) > now - 300
                        else "clear"
                        for cam, cv in state.cameras.items()
                    },
                })

        # Periodic snapshot
        if now - last_snapshot >= SNAPSHOT_INTERVAL:
            last_snapshot = now
            db.run(
                "INSERT INTO hal_snapshots (timestamp, state_json) VALUES (?, ?)",
                (now, json.dumps(state.to_dict()))
            )

        # Periodic baseline rebuild
        if now - last_baseline >= BASELINE_REBUILD_INTERVAL:
            last_baseline = now
            try:
                learner.rebuild_baselines()
            except Exception as e:
                log(f"  Baseline error: {e}")

        # Periodic prune (daily)
        if now - last_prune >= 86400:
            last_prune = now
            cutoff = now - (PRUNE_DAYS * 86400)
            db.run("DELETE FROM hal_snapshots WHERE timestamp < ?", (cutoff,))
            db.run("DELETE FROM hal_events WHERE timestamp < ?", (cutoff,))
            log("  Pruned old snapshots and events")

        # Periodic stats
        if tick_count % 60 == 0:  # every 30 min
            threshold = engine._get_ble_noise_threshold()
            anchor = engine._ble_anchor
            log(f"  State: BLE={state.ble['count']} (anchor={anchor}, threshold={threshold}) | "
                f"Kitchen={'active' if state.motion['kitchen_active'] else 'quiet'} | "
                f"Lumus={'active' if state.motion['lumus_active'] else 'quiet'} | "
                f"LivingRoom={'motion' if state.motion.get('living_room_active') else 'quiet'} | "
                f"Scene={_get_scene_context(state) or '?'}")

        time.sleep(TICK_INTERVAL)

    client.loop_stop()
    client.disconnect()
    mesh.shutdown()
    db.close()
    log("═══ Chronicle HAL stopped ═══")


if __name__ == "__main__":
    main()
