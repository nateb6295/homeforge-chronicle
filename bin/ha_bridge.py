#!/usr/bin/env python3
"""Home Assistant → MQTT Bridge for Chronicle.

Connects to HA's WebSocket API, subscribes to state_changed events,
filters to interesting entities, and publishes to MQTT topics that
the seed agent already listens to.

Replaces scattered per-entity MQTT automations in HA with one clean pipe.

Runs on AGX (192.168.1.70) as a systemd service.
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from typing import Dict, Optional, Set

try:
    import aiohttp
except ImportError:
    print("ERROR: aiohttp required. pip install aiohttp")
    sys.exit(1)

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("ERROR: paho-mqtt required. pip install paho-mqtt")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

HASS_URL = os.environ.get("HASS_URL", "http://192.168.1.10:8123")
HASS_TOKEN = os.environ.get("HASS_TOKEN", "")
MQTT_BROKER = os.environ.get("MQTT_BROKER", "192.168.1.10")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_TOPIC_PREFIX = "homeforge/home"

# Domains we care about — everything else is noise
WATCH_DOMAINS: Set[str] = {
    "binary_sensor",
    "sensor",
    "light",
    "weather",
    "person",
    "camera",
    "sun",
}

# Specific entities to always watch regardless of domain
WATCH_ENTITIES: Set[str] = set()

# Entities to never forward (chatty or useless)
IGNORE_ENTITIES: Set[str] = {
    "sensor.backup_backup_manager_state",
    "sensor.backup_last_attempted_automatic_backup",
    "sensor.backup_last_successful_automatic_backup",
    "sensor.backup_next_scheduled_automatic_backup",
    "sensor.zigbee2mqtt_bridge_version",
    "sensor.driveway_ptz_pan_position",
    "sensor.driveway_ptz_tilt_position",
}

# Per-entity cooldown in seconds (prevent event floods)
COOLDOWN_SECONDS = int(os.environ.get("HA_BRIDGE_COOLDOWN", "30"))

# Reconnection backoff (seconds)
BACKOFF_STEPS = [5, 10, 30, 60]

# ═══════════════════════════════════════════════════════════════════
#  Logging
# ═══════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ha-bridge")

# ═══════════════════════════════════════════════════════════════════
#  Event formatting
# ═══════════════════════════════════════════════════════════════════

def format_state_change(entity_id: str, old_state: dict, new_state: dict) -> Optional[str]:
    """Convert a state_changed event into a human-readable string."""
    if not new_state:
        return None

    old_val = old_state.get("state", "unknown") if old_state else "unknown"
    new_val = new_state.get("state", "unknown")

    # Skip if state didn't actually change
    if old_val == new_val:
        return None

    # Skip unavailable transitions (HA startup noise)
    if old_val == "unavailable" or new_val == "unavailable":
        return None

    friendly_name = new_state.get("attributes", {}).get("friendly_name", entity_id)
    domain = entity_id.split(".")[0] if "." in entity_id else ""

    if domain == "binary_sensor":
        if "person" in entity_id:
            action = "Person detected" if new_val == "on" else "Person cleared"
        elif "vehicle" in entity_id:
            action = "Vehicle detected" if new_val == "on" else "Vehicle cleared"
        elif "animal" in entity_id:
            action = "Animal detected" if new_val == "on" else "Animal cleared"
        elif "motion" in entity_id:
            action = "Motion detected" if new_val == "on" else "Motion cleared"
        elif "occupancy" in entity_id:
            action = "Occupied" if new_val == "on" else "Unoccupied"
        else:
            action = "triggered" if new_val == "on" else "cleared"
        return f"{friendly_name}: {action}"

    if domain == "sensor":
        unit = new_state.get("attributes", {}).get("unit_of_measurement", "")
        return f"{friendly_name}: {old_val}{unit} → {new_val}{unit}"

    if domain == "weather":
        return f"Weather changed: {old_val} → {new_val}"

    if domain == "light":
        return f"{friendly_name}: turned {'on' if new_val == 'on' else 'off'}"

    if domain == "person":
        return f"{friendly_name}: {old_val} → {new_val}"

    if domain == "sun":
        return f"Sun: {old_val} → {new_val}"

    return f"{friendly_name}: {old_val} → {new_val}"


def entity_to_mqtt_topic(entity_id: str) -> str:
    """Map entity_id to MQTT topic under homeforge/home/."""
    # binary_sensor.driveway_person → homeforge/home/kitchen/person
    # sensor.0x54ef44100138b955_illuminance → homeforge/home/living_room/illuminance
    # weather.forecast_home → homeforge/home/weather
    domain, _, name = entity_id.partition(".")

    # Camera detection events → group by location
    # Note: HA entity still says "driveway" but camera is physically in the kitchen
    if "driveway" in name:
        location = "kitchen"
        detail = name.replace("driveway_", "").replace("reolink_", "")
    elif "lumus" in name:
        location = "lumus"
        detail = name.replace("reolink_lumus_", "")
    elif "living_room" in name or "0x54ef44100138b955" in name:
        location = "living_room"
        detail = name.split("_")[-1] if "_" in name else name
    else:
        location = name
        detail = domain

    return f"{MQTT_TOPIC_PREFIX}/{location}/{detail}"


# ═══════════════════════════════════════════════════════════════════
#  MQTT publisher
# ═══════════════════════════════════════════════════════════════════

class MQTTPublisher:
    def __init__(self):
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION1,
            client_id="ha-bridge",
        )
        self.connected = False

    def connect(self):
        try:
            self.client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            self.client.loop_start()
            self.connected = True
            log.info("MQTT connected to %s:%d", MQTT_BROKER, MQTT_PORT)
        except Exception as e:
            log.warning("MQTT connect failed (non-fatal): %s", e)
            self.connected = False

    def publish(self, topic: str, message: str):
        if not self.connected:
            return
        try:
            self.client.publish(topic, message, qos=0)
        except Exception as e:
            log.warning("MQTT publish failed: %s", e)

    def disconnect(self):
        if self.connected:
            self.client.loop_stop()
            self.client.disconnect()


# ═══════════════════════════════════════════════════════════════════
#  HA WebSocket listener
# ═══════════════════════════════════════════════════════════════════

class HABridge:
    def __init__(self, publisher: MQTTPublisher):
        self.publisher = publisher
        self._msg_id = 0
        self._cooldowns: Dict[str, float] = {}
        self._running = True
        self._event_count = 0
        self._forwarded_count = 0

    def _next_id(self) -> int:
        self._msg_id += 1
        return self._msg_id

    def _should_forward(self, entity_id: str) -> bool:
        """Check domain/entity filters and cooldown."""
        if entity_id in IGNORE_ENTITIES:
            return False

        domain = entity_id.split(".")[0] if "." in entity_id else ""
        if entity_id not in WATCH_ENTITIES and domain not in WATCH_DOMAINS:
            return False

        now = time.time()
        last = self._cooldowns.get(entity_id, 0)
        if (now - last) < COOLDOWN_SECONDS:
            return False
        self._cooldowns[entity_id] = now
        return True

    async def _ws_connect(self, session: aiohttp.ClientSession):
        """Connect, auth, and subscribe to state_changed events."""
        ws_url = HASS_URL.replace("http://", "ws://").replace("https://", "wss://")
        ws_url = f"{ws_url}/api/websocket"

        ws = await session.ws_connect(ws_url, heartbeat=30)

        # auth_required
        msg = await ws.receive_json()
        if msg.get("type") != "auth_required":
            raise RuntimeError(f"Expected auth_required, got {msg.get('type')}")

        # auth
        await ws.send_json({"type": "auth", "access_token": HASS_TOKEN})
        msg = await ws.receive_json()
        if msg.get("type") != "auth_ok":
            raise RuntimeError(f"Auth failed: {msg}")

        # subscribe
        sub_id = self._next_id()
        await ws.send_json({
            "id": sub_id,
            "type": "subscribe_events",
            "event_type": "state_changed",
        })
        msg = await ws.receive_json()
        if not msg.get("success"):
            raise RuntimeError(f"Subscribe failed: {msg}")

        return ws

    async def _handle_event(self, event: dict):
        """Process a single state_changed event."""
        self._event_count += 1
        data = event.get("data", {})
        entity_id = data.get("entity_id", "")

        if not entity_id or not self._should_forward(entity_id):
            return

        old_state = data.get("old_state", {})
        new_state = data.get("new_state", {})
        message = format_state_change(entity_id, old_state, new_state)

        if not message:
            return

        topic = entity_to_mqtt_topic(entity_id)
        self.publisher.publish(topic, message)
        self._forwarded_count += 1
        log.info("→ %s: %s", topic, message)

    async def run(self):
        """Main loop with reconnection."""
        backoff_idx = 0

        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    log.info("Connecting to %s ...", HASS_URL)
                    ws = await self._ws_connect(session)
                    log.info("Connected. Watching %s domains, %d entity overrides",
                             ", ".join(sorted(WATCH_DOMAINS)), len(WATCH_ENTITIES))
                    backoff_idx = 0  # Reset on success

                    async for ws_msg in ws:
                        if not self._running:
                            break
                        if ws_msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(ws_msg.data)
                                if data.get("type") == "event":
                                    await self._handle_event(data.get("event", {}))
                            except json.JSONDecodeError:
                                pass
                        elif ws_msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break

            except asyncio.CancelledError:
                return
            except Exception as e:
                log.warning("Connection error: %s", e)

            if not self._running:
                return

            delay = BACKOFF_STEPS[min(backoff_idx, len(BACKOFF_STEPS) - 1)]
            log.info("Reconnecting in %ds... (events: %d total, %d forwarded)",
                     delay, self._event_count, self._forwarded_count)
            await asyncio.sleep(delay)
            backoff_idx += 1

    def stop(self):
        self._running = False


# ═══════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════

def main():
    if not HASS_TOKEN:
        log.error("HASS_TOKEN not set. Export it or add to chronicle.env")
        sys.exit(1)

    log.info("═══ HA Bridge starting ═══")
    log.info("HA: %s", HASS_URL)
    log.info("MQTT: %s:%d (prefix: %s)", MQTT_BROKER, MQTT_PORT, MQTT_TOPIC_PREFIX)
    log.info("Cooldown: %ds", COOLDOWN_SECONDS)

    publisher = MQTTPublisher()
    publisher.connect()

    bridge = HABridge(publisher)

    def shutdown(*_):
        log.info("Shutting down...")
        bridge.stop()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        asyncio.run(bridge.run())
    finally:
        publisher.disconnect()
        log.info("═══ HA Bridge stopped ═══")


if __name__ == "__main__":
    main()
