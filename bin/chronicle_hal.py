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
HA_URL = os.environ.get("HA_URL", os.environ.get("HASS_URL", "http://192.168.1.10:8123"))
HA_TOKEN = os.environ.get("HASS_TOKEN", "")
TICK_INTERVAL = 30  # seconds between correlation ticks
SNAPSHOT_INTERVAL = 300  # state snapshot every 5 minutes
BASELINE_REBUILD_INTERVAL = 3600  # rebuild baselines hourly
PRUNE_DAYS = 30  # keep snapshots for 30 days

ALERTS_WEBHOOK = os.environ.get("ALERTS_WEBHOOK", "")

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
    "person_seen": 600,       # 10 min — camera-primary person detection
    "camera_offline": 3600,  # 1 hour — don't re-alert for same camera
    "camera_online": 300,    # 5 min
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
        self.path = path
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

    def refresh(self):
        self.conn.close()
        self.conn = sqlite3.connect(self.path, timeout=30)
        self.conn.row_factory = sqlite3.Row

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
            "driveway": {"last_person": 0, "last_vehicle": 0, "last_motion": 0, "last_animal": 0, "last_any_event": 0, "online": True, "battery": None, "sleeping": False},
            "lumus": {"last_person": 0, "last_vehicle": 0, "last_motion": 0, "last_animal": 0, "last_any_event": 0, "online": True, "day_night": None},
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
            "driveway_active": False,
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
            self.cameras[camera] = {"last_person": 0, "last_vehicle": 0, "last_motion": 0, "last_animal": 0, "last_any_event": 0, "online": True}
            cam = self.cameras[camera]

        if event_type == "person":
            cam["last_person"] = now
        elif event_type == "vehicle":
            cam["last_vehicle"] = now
        elif event_type == "motion":
            cam["last_motion"] = now
        elif event_type == "animal":
            cam["last_animal"] = now

        cam["last_any_event"] = now
        if not cam.get("online"):
            cam["online"] = True

        # Update global motion
        self.motion["last_any_motion"] = now
        if camera == "driveway":
            self.motion["driveway_active"] = True
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

    # Minimum BLE delta without camera corroboration (baseline stddev ~2.8, so 6 ≈ 2σ)
    BLE_MIN_DELTA = 6
    # Departures need a higher bar — BLE drops are noisier than rises
    BLE_DEPARTURE_MIN_DELTA = 7
    # How many consecutive ticks BLE must sustain a direction to trigger
    BLE_STABILITY_TICKS = 6  # 3 min — filters most jitter
    # Minimum seconds between arrival→departure or departure→arrival
    EVENT_PAIR_COOLDOWN = 1800  # 30 minutes
    # Nighttime hours where camera corroboration is REQUIRED (no BLE-only arrivals)
    NIGHT_START = 22
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

        # Departures require an exterior camera (driveway or lumus) to see activity.
        lumus_cam = self.state.cameras.get("lumus", {})
        driveway_cam = self.state.cameras.get("driveway", {})
        lumus_person = lumus_cam.get("last_person", 0) > now - 180
        lumus_vehicle = lumus_cam.get("last_vehicle", 0) > now - 180
        driveway_person = driveway_cam.get("last_person", 0) > now - 180
        driveway_vehicle = driveway_cam.get("last_vehicle", 0) > now - 180
        exterior_activity = lumus_person or lumus_vehicle or driveway_person or driveway_vehicle

        # Camera-corroborated events can use a slightly lower threshold
        camera_corroborated = recent_person or recent_vehicle
        effective_threshold = max(3, noise_threshold - 1) if camera_corroborated else noise_threshold

        # Departures need a higher BLE bar — drops are noisier than rises
        effective_departure_threshold = max(self.BLE_DEPARTURE_MIN_DELTA, noise_threshold)

        # Check cooldown: don't flip arrival↔departure too fast
        last_event_ts = max(
            self.state.presence.get("last_arrival", 0),
            self.state.presence.get("last_departure", 0),
        )
        in_cooldown = (now - last_event_ts) < self.EVENT_PAIR_COOLDOWN if last_event_ts else False

        # Camera-fast path: exterior person + any BLE increase skips sustained check
        exterior_person_recent = lumus_person or driveway_person
        ble_any_increase = (
            self._ble_anchor is not None
            and current_count >= self._ble_anchor + 2
        )
        camera_fast_arrival = (
            exterior_person_recent
            and ble_any_increase
            and not in_cooldown
        )

        should_trigger_arrival = camera_fast_arrival or (
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
            and exterior_activity  # departures require exterior camera confirmation
            and not in_cooldown
        )

        if should_trigger_arrival:
            confidence = "high" if recent_person else "medium"
            if camera_fast_arrival and not is_sustained:
                evidence = [f"BLE {self._ble_anchor}→{current_count} (camera-fast, +{current_count - (self._ble_anchor or 0)})"]
                evidence.append("person detected on exterior camera")
            else:
                evidence = [f"BLE {self._ble_anchor}→{current_count} (sustained {len(self._ble_window)}t)"]
                if recent_person:
                    evidence.append("person detected on camera")

            hour = datetime.now().hour
            description = "Someone arrived"
            if 3 <= hour <= 5:
                description = "Early riser arrived (probably Nate)"

            events.append({
                "event_type": "arrival",
                "priority": "info",
                "description": description,
                "confidence": confidence,
                "evidence": evidence,
            })
            # Update presence state so cooldown works
            self.state.presence["last_arrival"] = now
            # Reset anchor and clear window to prevent cascading false arrivals
            self._ble_anchor = current_count
            self._ble_anchor_ts = now
            self._ble_window = [(now, current_count)]

        elif should_trigger_departure:
            evidence = [f"BLE {self._ble_anchor}→{current_count} (sustained {len(self._ble_window)}t)"]
            if driveway_person:
                evidence.append("person detected on driveway camera")
            if driveway_vehicle:
                evidence.append("vehicle detected on driveway camera")
            if lumus_person:
                evidence.append("person detected on lumus camera")
            if lumus_vehicle:
                evidence.append("vehicle detected on lumus camera")
            confidence = "high" if (driveway_person or lumus_person) else "medium"
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
            self._ble_window = [(now, current_count)]

        # Re-anchor if BLE has been stable for 20+ min (no event triggered)
        # and recent readings show low variance (real stability, not oscillation)
        if (self._ble_anchor is not None
                and now - self._ble_anchor_ts > 1200
                and abs(current_count - self._ble_anchor) >= 1):
            if len(self._ble_window) >= 4:
                recent_vals = [r[1] for r in self._ble_window[-4:]]
                spread = max(recent_vals) - min(recent_vals)
                if spread <= 3:
                    log(f"  BLE re-anchor: {self._ble_anchor}→{current_count} (stable drift, spread={spread})")
                    self._ble_anchor = current_count
                    self._ble_anchor_ts = now
                    self._ble_window = self._ble_window[-2:]

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

        # ── Camera-Primary Person Detection ──
        # Frigate sees a person on an exterior camera — emit event regardless of BLE
        for cam_name in ("driveway", "lumus"):
            cam_state = self.state.cameras.get(cam_name, {})
            if cam_state.get("last_person", 0) > now - 30:  # person in last 30s
                cooldown_key = f"person_seen:{cam_name}"
                last = self._last_emitted.get(cooldown_key, 0)
                if now - last >= COOLDOWNS.get("person_seen", 600):
                    self._last_emitted[cooldown_key] = now
                    hour = datetime.now().hour
                    period = "morning" if 5 <= hour < 12 else "afternoon" if 12 <= hour < 17 else "evening" if 17 <= hour < 21 else "night"
                    events.append({
                        "event_type": "person_seen",
                        "priority": "info",
                        "description": f"Person on {cam_name} camera ({period})",
                        "confidence": "high",
                        "evidence": [f"Frigate person detection on {cam_name}", f"BLE at {current_count}"],
                    })

        # ── Anomaly Detection ──
        # Person on driveway camera late at night with no BLE change
        hour = datetime.now().hour
        driveway = self.state.cameras.get("driveway", {})
        if driveway.get("last_person", 0) > now - 60:
            if (hour >= 23 or hour <= 5) and ble["last_change_ts"] < now - 300:
                events.append({
                    "event_type": "anomaly",
                    "priority": "alert",
                    "description": f"Person detected on driveway camera at {hour}:00 — no BLE change (unknown visitor)",
                    "confidence": "medium",
                    "evidence": ["person on driveway camera", "nighttime", "no BLE change in 5+ min"],
                    "cooldown_key": "anomaly:driveway_night",
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

        # ── Camera Health Check ──
        # If a camera hasn't reported ANY event for too long, it's probably offline.
        # Daytime threshold lower (cameras see motion constantly from wind/light).
        # Nighttime threshold higher (less activity is normal).
        cam_stale_day = 7200    # 2 hours daytime
        cam_stale_night = 21600 # 6 hours nighttime
        stale_threshold = cam_stale_night if self._is_nighttime() else cam_stale_day

        for cam_name, cam_state in self.state.cameras.items():
            last_event = cam_state.get("last_any_event", 0)
            was_online = cam_state.get("online", True)
            is_battery = cam_state.get("battery") is not None
            effective_threshold = stale_threshold * 3 if is_battery else stale_threshold

            if last_event > 0 and (now - last_event) > effective_threshold:
                if was_online:
                    cam_state["online"] = False
                    silent_min = int((now - last_event) / 60)
                    events.append({
                        "event_type": "camera_offline",
                        "priority": "alert",
                        "description": f"Camera '{cam_name}' appears offline — no events for {silent_min} minutes",
                        "confidence": "medium",
                        "evidence": [f"last event {silent_min}m ago", f"threshold {effective_threshold//60}m",
                                     f"battery={'yes' if is_battery else 'no'}"],
                        "cooldown_key": f"camera_offline:{cam_name}",
                    })
            elif last_event > 0 and not was_online and (now - last_event) <= stale_threshold:
                cam_state["online"] = True
                events.append({
                    "event_type": "camera_online",
                    "priority": "info",
                    "description": f"Camera '{cam_name}' is back online",
                    "confidence": "high",
                    "evidence": ["event received after offline period"],
                    "cooldown_key": f"camera_online:{cam_name}",
                })

        # ── Motion decay ──
        # Reset motion flags after 60s of no new events
        for cam_name in ["driveway", "lumus"]:
            cam = self.state.cameras.get(cam_name, {})
            if cam.get("last_motion", 0) < now - 60:
                if cam_name == "driveway":
                    self.state.motion["driveway_active"] = False
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


LFM_ENABLED = os.environ.get("LFM_NARRATE", "1") == "1"
LFM_HOST = "jetson"
LFM_URL = f"http://192.168.1.11:11434/api/generate"
LFM_MODEL = "hf.co/LiquidAI/LFM2.5-2.6B-GGUF"
LFM_SYSTEM = (
    "You are the Bradford family home in Washington state. "
    "Nate is dad — early riser, 4 AM weekdays. Wife and kids including Ramona. "
    "The house has front door, back door, garage. Living room, kitchen, bedrooms upstairs. "
    "BLE device count tracks phones and watches — more devices means more people home. "
    "Cameras: front yard (driveway) and back yard. "
    "Say what is happening in ONE natural sentence under 25 words. "
    "Sound like someone who lives here. Not a report. Not a list. "
    "4 AM motion is probably Nate. Describe what you see, don't guess schedules. "
    "Never say intruder or security breach. If nothing interesting, say quiet house."
)

_lfm_recent_events = []

def _lfm_remember(desc):
    """Keep a rolling buffer of recent narrated events for continuity."""
    _lfm_recent_events.append(desc)
    if len(_lfm_recent_events) > 5:
        _lfm_recent_events.pop(0)


def lfm_narrate(event):
    """Ask LFM on Orin to narrate an event naturally. Falls back to original description."""
    if not LFM_ENABLED:
        return event["description"]
    try:
        import requests as _req
        desc = event["description"]
        evidence = ", ".join(event.get("evidence", []))
        hour = datetime.now().strftime("%I:%M %p %A")
        context = ""
        if _lfm_recent_events:
            context = "Recent: " + " | ".join(_lfm_recent_events[-3:]) + "\n"
        prompt = f"{context}Event: {desc}. Time: {hour}. Details: {evidence}."
        resp = _req.post(LFM_URL, json={
            "model": LFM_MODEL,
            "system": LFM_SYSTEM,
            "prompt": prompt,
            "stream": False,
            # Aug 24: she emits <think> blocks and long generations. A 10s
            # timeout meant EVERY narration timed out and silently fell back to
            # the raw text — narration was effectively off and nothing said so.
            # Cap the generation instead of the patience, and strip the trace.
            "options": {"num_predict": 220, "temperature": 0.7},
        }, timeout=45)
        if resp.ok:
            narrated = resp.json().get("response", "").strip()
            narrated = re.sub(r"<think>.*?</think>", "", narrated, flags=re.S).strip()
            if "<think>" in narrated:          # unterminated trace: unusable
                narrated = ""
            if len(narrated) > 10:
                log(f"  [lfm] {narrated}")
                _lfm_remember(narrated)
                return narrated
    except Exception as e:
        log(f"  [lfm-error] {e}")
    return event["description"]


def emit_event(db, mqtt_client, event):
    """Store event in DB, publish to MQTT, optionally speak and alert."""
    now = now_ts()
    if LFM_ENABLED and event["event_type"] in ("arrival", "departure", "quiet_house", "active_house", "scene_digest", "person_seen"):
        # Ox, Aug 24: persisting the raw text into a field nothing reads is the
        # SAME bug wearing a fix. Downstream reads `description`, so `description`
        # must hold the observation. Narration is flavour and lives beside it.
        # (Also: this narrator does not merely drop the observation, it ADDS
        # content no sensor entails — mud, boots, breathing, from a BLE count.
        # Insertion, not omission. Preserving the original cannot cure that;
        # only making the original load-bearing can.)
        event["description_narrated"] = lfm_narrate(event)
    priority = event.get("priority", "info")

    # Store in DB
    db.run(
        # description_raw was computed in emit_event and then DISCARDED — a dead
        # assignment. The LFM narration OVERWRITES the sensor text, so what got
        # stored was a 2.6B model's prose and the observation was unrecoverable.
        # Aug 24: ["BLE=12","quiet=False","hour=10"] was stored as "his breathing
        # drifts through the gap". Persist both; narration is a rendering of an
        # observation, never a replacement for one.
        "INSERT INTO hal_events (timestamp, event_type, priority, description, "
        "state_snapshot, cooldown_key, description_narrated) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (now, event["event_type"], priority, event["description"],
         json.dumps(event.get("evidence", [])), event.get("cooldown_key"),
         event.get("description_narrated"))
    )

    # ── Bridge to activity_feed ──
    # Significant events get written to activity_feed so the swarm can see them.
    # Only arrivals, departures, and alerts — not every quiet/active tick.
    _FEED_EVENTS = {"arrival", "departure", "anomaly", "quiet_house", "active_house", "scene_digest", "person_seen", "camera_offline", "camera_online"}
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
#  Home Assistant API Polling
# ═══════════════════════════════════════════════════════════════════

HA_CAMERA_SENSORS = {
    "driveway": {
        "person": "binary_sensor.driveway_person",
        "vehicle": "binary_sensor.driveway_vehicle",
        "motion": "binary_sensor.driveway_motion",
        "animal": "binary_sensor.driveway_animal",
    },
    "lumus": {
        "person": "binary_sensor.reolink_lumus_person",
        "vehicle": "binary_sensor.reolink_lumus_vehicle",
        "motion": "binary_sensor.reolink_lumus_motion",
        "animal": "binary_sensor.reolink_lumus_animal",
    },
}

HA_CAMERA_META = {
    "driveway": {
        "battery": "sensor.driveway_battery",
        "sleep": "binary_sensor.driveway_sleep_status",
    },
    "lumus": {
        "day_night": "sensor.reolink_lumus_day_night_state",
    },
}

_ha_last_states = {}

def poll_ha_cameras(state):
    """Poll HA binary sensors for cameras not reliably on MQTT."""
    if not HA_TOKEN:
        return
    headers = {"Authorization": f"Bearer {HA_TOKEN}"}
    for cam_name, sensors in HA_CAMERA_SENSORS.items():
        for event_type, entity_id in sensors.items():
            try:
                resp = requests.get(
                    f"{HA_URL}/api/states/{entity_id}",
                    headers=headers, timeout=3,
                )
                if resp.status_code != 200:
                    continue
                data = resp.json()
                current = data.get("state", "off")
                prev = _ha_last_states.get(entity_id, "off")
                _ha_last_states[entity_id] = current
                if current == "on" and prev != "on":
                    state.update_camera(cam_name, event_type)
                elif current == "on":
                    cam = state.cameras.get(cam_name)
                    if cam:
                        cam["last_any_event"] = now_ts()
            except Exception:
                pass
    for cam_name, meta in HA_CAMERA_META.items():
        cam = state.cameras.get(cam_name)
        if not cam:
            continue
        for field, entity_id in meta.items():
            try:
                resp = requests.get(
                    f"{HA_URL}/api/states/{entity_id}",
                    headers=headers, timeout=3,
                )
                if resp.status_code != 200:
                    continue
                val = resp.json().get("state", "")
                if val in ("unavailable", "unknown"):
                    continue
                if field == "battery":
                    cam["battery"] = int(val)
                elif field == "sleep":
                    cam["sleeping"] = val.lower() == "on"
                elif field == "day_night":
                    cam["day_night"] = val
            except Exception:
                pass


def poll_ha_environment(state):
    """Poll HA for weather and temperature when MQTT doesn't provide them."""
    if not HA_TOKEN:
        return
    headers = {"Authorization": f"Bearer {HA_TOKEN}"}
    # Weather
    try:
        resp = requests.get(f"{HA_URL}/api/states/weather.forecast_home",
                            headers=headers, timeout=3)
        if resp.status_code == 200:
            weather = resp.json().get("state", "")
            if weather and weather not in ("unavailable", "unknown"):
                state.update_environment("weather", weather)
    except Exception:
        pass
    # Outdoor temperature from weather integration (not Zigbee device temp)
    try:
        resp = requests.get(f"{HA_URL}/api/states/weather.forecast_home",
                            headers=headers, timeout=3)
        if resp.status_code == 200:
            attrs = resp.json().get("attributes", {})
            temp = attrs.get("temperature")
            if temp is not None:
                state.update_environment("temperature", float(temp))
    except Exception:
        pass
    # Zigbee device temperature (indoor sensor chip temp — NOT outdoor)
    try:
        resp = requests.get(f"{HA_URL}/api/states/sensor.0x54ef44100138b955_device_temperature",
                            headers=headers, timeout=3)
        if resp.status_code == 200:
            data = resp.json()
            temp_str = data.get("state", "")
            if temp_str and temp_str not in ("unavailable", "unknown"):
                state.zigbee["living_room_temperature"] = float(temp_str)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════
#  MQTT Message Routing
# ═══════════════════════════════════════════════════════════════════

def _camera_from_topic(topic):
    """Map MQTT topic to canonical camera name."""
    t = topic.lower()
    if "driveway" in t or "front_camera" in t or "front" in t or "kitchen" in t:
        return "driveway"
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

        # ── Camera events (from HA automations via homeforge/home/{camera}/{event}) ──
        elif "/person" in topic:
            camera = _camera_from_topic(topic)
            if camera:
                state.update_camera(camera, "person")
        elif "/vehicle" in topic or "/car" in topic:
            camera = _camera_from_topic(topic)
            if camera:
                state.update_camera(camera, "vehicle")
        elif "/motion/state" in topic:
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
                    state.update_environment("temperature", data["temperature"])
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

        # ── Zigbee device temperature (indoor sensor chip — NOT outdoor) ──
        elif "temperature" in topic:
            try:
                temp = float(payload_str.strip())
                state.zigbee["living_room_temperature"] = temp
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
    log(f"HA: {'configured' if HA_TOKEN else 'no token — HA polling disabled'}")

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
    client = mqtt.Client(
        client_id="chronicle-hal",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv311,
    )

    def on_connect(c, userdata, flags, reason_code, properties):
        rc = reason_code
        if rc == 0:
            log("MQTT connected")
            c.subscribe("homeforge/home/#", qos=0)
            c.subscribe("frigate/+/person", qos=0)
            c.subscribe("frigate/+/vehicle", qos=0)
            c.subscribe("frigate/+/motion", qos=0)
            c.subscribe("frigate/+/animal", qos=0)
            c.subscribe("frigate/+/cat", qos=0)
            c.subscribe("frigate/+/dog", qos=0)
            c.subscribe("homeforge/voice/speaker_state", qos=0)
            c.subscribe("zigbee2mqtt/living_room_motion", qos=0)
            log("Subscribed to homeforge/home/#, frigate events, zigbee2mqtt")
        else:
            log(f"MQTT connection failed: rc={rc}")

    # Rate limiter: drop messages if burst exceeds threshold
    msg_timestamps = []
    MSG_RATE_WINDOW = 5      # seconds
    MSG_RATE_LIMIT = 50      # max messages per window
    msg_drop_count = [0]
    last_drop_log = [0]

    def on_message(c, userdata, msg, properties=None):
        now = time.time()
        # Trim old timestamps
        while msg_timestamps and msg_timestamps[0] < now - MSG_RATE_WINDOW:
            msg_timestamps.pop(0)
        # Drop if over limit
        if len(msg_timestamps) >= MSG_RATE_LIMIT:
            msg_drop_count[0] += 1
            if now - last_drop_log[0] > 60:
                log(f"  MQTT rate limit: dropped {msg_drop_count[0]} messages in last {int(now - last_drop_log[0])}s")
                msg_drop_count[0] = 0
                last_drop_log[0] = now
            return
        msg_timestamps.append(now)
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

        # Poll HA for cameras not on MQTT
        try:
            poll_ha_cameras(state)
        except Exception as e:
            if tick_count % 60 == 1:
                log(f"  HA poll error: {e}")

        # Poll HA for weather and temperature every 5 minutes (and on first tick)
        if tick_count <= 1 or tick_count % 10 == 0:
            try:
                poll_ha_environment(state)
            except Exception as e:
                if tick_count % 60 == 1:
                    log(f"  HA environment poll error: {e}")

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
                        cam: "offline" if not cv.get("online", True)
                        else "person" if cv.get("last_person", 0) > now - 600
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

        # Periodic DB refresh to release WAL locks (every ~2h)
        if tick_count % 240 == 0 and tick_count > 0:
            db.refresh()

        # Periodic stats
        if tick_count % 60 == 0:  # every 30 min
            threshold = engine._get_ble_noise_threshold()
            anchor = engine._ble_anchor
            cam_health = " | ".join(
                f"{cn}={'online' if cv.get('online', True) else 'OFFLINE'}"
                for cn, cv in state.cameras.items()
            )
            log(f"  State: BLE={state.ble['count']} (anchor={anchor}, threshold={threshold}) | "
                f"Driveway={'active' if state.motion['driveway_active'] else 'quiet'} | "
                f"Lumus={'active' if state.motion['lumus_active'] else 'quiet'} | "
                f"LivingRoom={'motion' if state.motion.get('living_room_active') else 'quiet'} | "
                f"Cameras: {cam_health} | "
                f"Scene={_get_scene_context(state) or '?'}")

        time.sleep(TICK_INTERVAL)

    client.loop_stop()
    client.disconnect()
    mesh.shutdown()
    db.close()
    log("═══ Chronicle HAL stopped ═══")


if __name__ == "__main__":
    main()
