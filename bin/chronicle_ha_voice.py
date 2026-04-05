#!/usr/bin/env python3
"""Chronicle HA Voice Bridge — MQTT voice messages to Home Assistant TTS.

Listens on homeforge/voice/speak for voice messages from HAL and the presence
layer, then plays them through any available Home Assistant media player using
the tts.speak service.

Features:
  - Message queuing: when no speaker is available, important messages are queued
    and delivered when a speaker comes online (within 2 hours).
  - Speaker polling: checks media player availability every 30s.
  - Speaker state published to MQTT for other services.
  - Queue consolidation: multiple queued messages become one summary.

Runs as: systemctl --user start chronicle-ha-voice
"""

import os
import sys
import json
import time
import logging
import threading
from collections import deque
from datetime import datetime

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("paho-mqtt required: pip install paho-mqtt")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("requests required: pip install requests")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════
#  Config
# ═══════════════════════════════════════════════════════════════

MQTT_BROKER = os.environ.get("MQTT_BROKER", "192.168.1.10")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
VOICE_TOPIC = "homeforge/voice/speak"
SPEAKER_STATE_TOPIC = "homeforge/voice/speaker_state"

HASS_URL = os.environ.get("HASS_URL", "http://192.168.1.10:8123")
HASS_TOKEN = os.environ.get("HASS_TOKEN", "")

# Media players to try, in priority order
MEDIA_PLAYERS = [
    "media_player.family_room_tv",
    "media_player.gym",
]

# TTS entity to use
TTS_ENTITY = os.environ.get("TTS_ENTITY", "tts.google_translate_en_com")

# Rate limiting
MIN_SPEAK_INTERVAL = 10  # seconds between TTS calls
QUIET_HOURS_START = 22   # 10pm
QUIET_HOURS_END = 7      # 7am

# Queue config
QUEUE_MAX_SIZE = 15
QUEUE_MAX_AGE = 7200       # 2 hours — don't deliver stale messages
SPEAKER_POLL_INTERVAL = 30  # seconds between speaker availability checks
MAX_CATCHUP_MESSAGES = 3    # max messages to speak on catchup

# Discord fallback — post voice messages here when no speaker available
ALERTS_WEBHOOK = os.environ.get("ALERTS_WEBHOOK",
    "https://discord.com/api/webhooks/1489300749308395611/zlZZH3QzXqEDxznmNelK3mlkLF0IeZqoxwNUnrL0no_jfeZnDMvwvD_7XhYcCyQzq78I")

# Wake config
WAKE_PLAYER = "media_player.family_room_tv"  # player to try waking
WAKE_SETTLE_SECS = 5  # seconds to wait after wake command
WAKE_COOLDOWN = 300    # don't try waking more than once per 5 min
_last_wake_attempt = 0

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [HA-VOICE] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  Home Assistant API
# ═══════════════════════════════════════════════════════════════

_headers = None

def ha_headers():
    global _headers
    if _headers is None:
        _headers = {
            "Authorization": f"Bearer {HASS_TOKEN}",
            "Content-Type": "application/json",
        }
    return _headers


def get_available_player():
    """Find the first media player that's not unavailable."""
    for player in MEDIA_PLAYERS:
        try:
            r = requests.get(
                f"{HASS_URL}/api/states/{player}",
                headers=ha_headers(),
                timeout=5,
            )
            if r.status_code == 200:
                state = r.json().get("state", "unavailable")
                if state not in ("unavailable", "unknown"):
                    log.info(f"  Player available: {player} [{state}]")
                    return player
        except Exception:
            continue
    return None


def wake_player():
    """Try to turn on the primary media player via HA. Returns player entity if successful."""
    global _last_wake_attempt
    now = time.time()
    if now - _last_wake_attempt < WAKE_COOLDOWN:
        return None
    _last_wake_attempt = now

    log.info(f"  Attempting to wake {WAKE_PLAYER}...")
    try:
        r = requests.post(
            f"{HASS_URL}/api/services/media_player/turn_on",
            headers=ha_headers(),
            json={"entity_id": WAKE_PLAYER},
            timeout=10,
        )
        if r.status_code != 200:
            log.warning(f"  Wake call returned {r.status_code}")
            return None
    except Exception as e:
        log.warning(f"  Wake call failed: {e}")
        return None

    # Give the device time to come up
    time.sleep(WAKE_SETTLE_SECS)

    # Check if it's now available
    try:
        r = requests.get(
            f"{HASS_URL}/api/states/{WAKE_PLAYER}",
            headers=ha_headers(),
            timeout=5,
        )
        if r.status_code == 200:
            state = r.json().get("state", "unavailable")
            if state not in ("unavailable", "unknown"):
                log.info(f"  Woke {WAKE_PLAYER} [{state}]")
                return WAKE_PLAYER
            else:
                log.info(f"  Wake sent but player still {state}")
    except Exception:
        pass
    return None


def discord_fallback(text, priority="info"):
    """Post voice message to Discord when no speaker is available."""
    try:
        prefix = "🔇" if priority == "info" else "🔊"
        msg = f"{prefix} **[voice]** {text}"
        requests.post(ALERTS_WEBHOOK, json={"content": msg[:1900]}, timeout=10)
        log.info(f"  Discord fallback: {text[:60]}")
    except Exception as e:
        log.error(f"  Discord fallback failed: {e}")


def speak_ha(text, player_entity):
    """Call HA tts.speak service to play text through a media player."""
    try:
        payload = {
            "entity_id": TTS_ENTITY,
            "media_player_entity_id": player_entity,
            "message": text,
            "language": "en",
        }
        r = requests.post(
            f"{HASS_URL}/api/services/tts/speak",
            headers=ha_headers(),
            json=payload,
            timeout=10,
        )
        if r.status_code == 200:
            log.info(f"  Spoke via {player_entity}: {text[:60]}")
            return True
        else:
            log.warning(f"  TTS failed ({r.status_code}): {r.text[:100]}")
            return False
    except Exception as e:
        log.error(f"  TTS error: {e}")
        return False


def speak_ha_cloud(text, player_entity):
    """Fallback: try tts.cloud_say if regular TTS fails."""
    try:
        payload = {
            "entity_id": player_entity,
            "message": text,
            "language": "en",
        }
        r = requests.post(
            f"{HASS_URL}/api/services/tts/cloud_say",
            headers=ha_headers(),
            json=payload,
            timeout=10,
        )
        return r.status_code == 200
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════
#  Message Queue
# ═══════════════════════════════════════════════════════════════

# Priority levels for queue ordering (higher = more important)
PRIORITY_WEIGHT = {"alert": 3, "important": 2, "info": 1, "debug": 0}

_message_queue = deque(maxlen=QUEUE_MAX_SIZE)
_queue_lock = threading.Lock()
_speaker_available = False
_last_speaker_state = False
_mqtt_client_ref = None


def queue_message(text, priority="info"):
    """Add a message to the delivery queue."""
    with _queue_lock:
        _message_queue.append({
            "text": text,
            "priority": priority,
            "timestamp": time.time(),
        })
    log.info(f"  Queued ({len(_message_queue)} in queue): {text[:60]}")


def flush_queue(player):
    """Deliver queued messages through available speaker."""
    global _last_speak_ts
    now = time.time()

    with _queue_lock:
        if not _message_queue:
            return

        # Filter out expired messages
        valid = [m for m in _message_queue if now - m["timestamp"] < QUEUE_MAX_AGE]
        _message_queue.clear()

    if not valid:
        log.info("  Queue flushed — all messages expired")
        return

    # Sort by priority (highest first), then by time (newest first)
    valid.sort(key=lambda m: (PRIORITY_WEIGHT.get(m["priority"], 0), m["timestamp"]), reverse=True)

    # Take top messages
    to_deliver = valid[:MAX_CATCHUP_MESSAGES]
    dropped = len(valid) - len(to_deliver)

    if len(to_deliver) == 1:
        catchup_text = to_deliver[0]["text"]
    else:
        # Consolidate into one spoken summary
        parts = [m["text"] for m in to_deliver]
        catchup_text = "While you were away. " + " ... ".join(parts)
        if dropped:
            catchup_text += f" ... and {dropped} more."

    log.info(f"  Delivering {len(to_deliver)} queued messages ({dropped} dropped)")

    if speak_ha(catchup_text, player):
        _last_speak_ts = time.time()
    elif speak_ha_cloud(catchup_text, player):
        _last_speak_ts = time.time()
    else:
        log.warning("  Catchup delivery failed — re-queuing")
        with _queue_lock:
            for m in to_deliver:
                _message_queue.append(m)


# ═══════════════════════════════════════════════════════════════
#  Speaker Polling
# ═══════════════════════════════════════════════════════════════

def speaker_poll_loop():
    """Background thread: poll HA for speaker availability every 30s."""
    global _speaker_available, _last_speaker_state
    _poll_count = 0

    while True:
        try:
            time.sleep(SPEAKER_POLL_INTERVAL)
            _poll_count += 1
            player = get_available_player()
            _speaker_available = player is not None

            # Log every 20 polls (~10 min) so we know the thread is alive
            if _poll_count % 20 == 0:
                status = f"available ({player})" if _speaker_available else "no speakers"
                log.info(f"  Speaker poll #{_poll_count}: {status}")

            # Detect speaker coming online (was off, now on)
            if _speaker_available and not _last_speaker_state:
                log.info(f"Speaker came online: {player}")
                # Publish state change
                if _mqtt_client_ref:
                    _mqtt_client_ref.publish(
                        SPEAKER_STATE_TOPIC,
                        json.dumps({"available": True, "player": player}),
                        qos=0,
                    )
                # Flush queued messages
                flush_queue(player)

            elif not _speaker_available and _last_speaker_state:
                log.info("All speakers went offline")
                if _mqtt_client_ref:
                    _mqtt_client_ref.publish(
                        SPEAKER_STATE_TOPIC,
                        json.dumps({"available": False, "player": None}),
                        qos=0,
                    )

            _last_speaker_state = _speaker_available

        except Exception as e:
            log.error(f"Speaker poll error: {e}")


# ═══════════════════════════════════════════════════════════════
#  Voice Message Handler
# ═══════════════════════════════════════════════════════════════

_last_speak_ts = 0


def handle_voice_message(payload_str):
    """Process a voice message from MQTT and speak it via HA."""
    global _last_speak_ts

    # Parse message first
    try:
        data = json.loads(payload_str)
        text = data.get("text", "")
        priority = data.get("priority", "info")
    except json.JSONDecodeError:
        text = payload_str.strip()
        priority = "info"

    if not text:
        return

    log.info(f"Voice message [{priority}]: {text[:80]}")

    # Quiet hours check (alerts bypass)
    hour = datetime.now().hour
    if hour >= QUIET_HOURS_START or hour < QUIET_HOURS_END:
        if priority != "alert":
            log.info("  Quiet hours, skipping non-alert")
            return

    # Rate limit
    now = time.time()
    if now - _last_speak_ts < MIN_SPEAK_INTERVAL:
        log.info("  Rate limited — queuing")
        queue_message(text, priority)
        return

    # Find a speaker
    player = get_available_player()

    # No speaker? Try waking one
    if not player:
        player = wake_player()

    # Still no speaker — discord fallback, don't silently queue
    if not player:
        discord_fallback(text, priority)
        return

    # Try speaking
    if speak_ha(text, player):
        _last_speak_ts = now
    elif speak_ha_cloud(text, player):
        _last_speak_ts = now
        log.info("  Spoke via cloud_say fallback")
    else:
        log.warning("  All TTS methods failed — discord fallback")
        discord_fallback(text, priority)


# ═══════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════

def main():
    if not HASS_TOKEN:
        log.error("HASS_TOKEN not set. Cannot connect to Home Assistant.")
        sys.exit(1)

    log.info("═══ Chronicle HA Voice Bridge starting ═══")
    log.info(f"MQTT: {MQTT_BROKER}:{MQTT_PORT}")
    log.info(f"HA: {HASS_URL}")
    log.info(f"TTS: {TTS_ENTITY}")
    log.info(f"Players: {', '.join(MEDIA_PLAYERS)}")

    # Test HA connection
    try:
        r = requests.get(f"{HASS_URL}/api/", headers=ha_headers(), timeout=5)
        if r.status_code == 200:
            log.info("HA API connected")
        else:
            log.warning(f"HA API returned {r.status_code}")
    except Exception as e:
        log.warning(f"HA API unreachable: {e}")

    # Check initial player availability
    global _speaker_available, _last_speaker_state, _mqtt_client_ref
    player = get_available_player()
    _speaker_available = player is not None
    _last_speaker_state = _speaker_available
    if player:
        log.info(f"Speaker ready: {player}")
    else:
        log.info("No speakers currently available — will queue messages until one comes online")

    client = mqtt.Client(
        client_id="chronicle-ha-voice",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv311,
    )
    _mqtt_client_ref = client

    def on_connect(c, userdata, flags, reason_code, properties):
        if reason_code == 0:
            log.info("MQTT connected")
            c.subscribe(VOICE_TOPIC, qos=1)
            log.info(f"Subscribed to {VOICE_TOPIC}")
            # Publish initial speaker state
            c.publish(
                SPEAKER_STATE_TOPIC,
                json.dumps({"available": _speaker_available, "player": player}),
                qos=0,
            )
        else:
            log.error(f"MQTT connect failed: rc={reason_code}")

    def on_message(c, userdata, msg, properties=None):
        try:
            payload = msg.payload.decode("utf-8", errors="replace")
            handle_voice_message(payload)
        except Exception as e:
            log.error(f"Error handling message: {e}")

    client.on_connect = on_connect
    client.on_message = on_message

    # Start speaker polling thread
    poll_thread = threading.Thread(target=speaker_poll_loop, daemon=True)
    poll_thread.start()
    log.info(f"Speaker polling started (every {SPEAKER_POLL_INTERVAL}s)")

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
    except Exception as e:
        log.error(f"MQTT connect failed: {e}")
        sys.exit(1)

    log.info("Listening for voice messages...")
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        client.disconnect()
        log.info("═══ Chronicle HA Voice Bridge stopped ═══")


if __name__ == "__main__":
    main()
