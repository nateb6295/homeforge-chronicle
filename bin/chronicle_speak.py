#!/usr/bin/env python3
"""Chronicle Speak — Voice output for the home.

Publishes text to MQTT for Piper TTS on the Pi. Can be used:
  - By HAL for event announcements (automatic, via homeforge/voice/speak)
  - By Opus for ad-hoc messages: chronicle_speak.py "Good morning, Nate"
  - By cron for scheduled briefings

The Pi listener (when connected) subscribes to homeforge/voice/speak,
runs Piper TTS, and plays through speakers.

Usage:
  chronicle_speak.py "text to speak"
  chronicle_speak.py --briefing          # generate morning briefing
  chronicle_speak.py --test              # test connectivity
"""

import sys, os, json, time, sqlite3
from datetime import datetime

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("paho-mqtt required: pip install paho-mqtt")
    sys.exit(1)

MQTT_BROKER = os.environ.get("MQTT_BROKER", "192.168.1.10")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
VOICE_TOPIC = "homeforge/voice/speak"
DB_PATH = os.environ.get("CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"))


def publish_speech(text, priority="info"):
    """Publish text to MQTT voice topic. Gated by VOICE_ENABLED env var."""
    if os.environ.get("VOICE_ENABLED", "0") != "1":
        print(f"[voice-disabled] Voice output sidelined. Set VOICE_ENABLED=1 to re-enable.")
        print(f"[voice-disabled] Would have said: {text[:100]}")
        return
    client = mqtt.Client(client_id="chronicle-speak", protocol=mqtt.MQTTv311)
    client.connect(MQTT_BROKER, MQTT_PORT, 10)
    payload = json.dumps({
        "text": text,
        "priority": priority,
        "timestamp": int(time.time()),
        "source": "opus",
    })
    client.publish(VOICE_TOPIC, payload, qos=1)
    client.disconnect()
    print(f"[speak] Published: {text[:80]}...")


def morning_briefing():
    """Generate a natural morning briefing from overnight data."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    now = int(time.time())
    six_hours_ago = now - 21600

    # Overnight HAL events
    events = conn.execute(
        "SELECT event_type, description, timestamp FROM hal_events "
        "WHERE timestamp > ? ORDER BY timestamp",
        (six_hours_ago,)
    ).fetchall()

    # Count real events (post-noise-filter)
    arrivals = sum(1 for e in events if e["event_type"] == "arrival")
    departures = sum(1 for e in events if e["event_type"] == "departure")
    anomalies = [e for e in events if e["event_type"] == "anomaly"]

    # Weather/scene
    snapshot = conn.execute(
        "SELECT state_json FROM hal_snapshots ORDER BY timestamp DESC LIMIT 1"
    ).fetchone()
    scene = "unknown"
    if snapshot:
        try:
            state = json.loads(snapshot["state_json"])
            scene_data = state.get("environment", {}).get("scene")
            if scene_data:
                scene = scene_data
        except Exception:
            pass

    conn.close()

    # Build briefing
    dt = datetime.now()
    day_name = dt.strftime("%A")
    parts = [f"Good morning. It's {day_name}."]

    if anomalies:
        parts.append(f"There were {len(anomalies)} security alerts overnight.")
    elif arrivals == 0 and departures == 0:
        parts.append("Quiet night. No movement detected.")
    else:
        if arrivals:
            parts.append(f"{arrivals} arrivals detected overnight.")
        if departures:
            parts.append(f"{departures} departures detected overnight.")

    return " ".join(parts)


def test_connectivity():
    """Test MQTT connectivity and report."""
    try:
        client = mqtt.Client(client_id="chronicle-speak-test", protocol=mqtt.MQTTv311)
        client.connect(MQTT_BROKER, MQTT_PORT, 5)
        client.publish(VOICE_TOPIC, json.dumps({
            "text": "Chronicle voice test. If you can hear this, the loop is closing.",
            "priority": "info",
            "timestamp": int(time.time()),
            "source": "test",
        }), qos=1)
        client.disconnect()
        print(f"[speak] MQTT connected to {MQTT_BROKER}:{MQTT_PORT}")
        print(f"[speak] Published test message to {VOICE_TOPIC}")
        print("[speak] A Piper TTS listener on the Pi will speak this message.")
        return True
    except Exception as e:
        print(f"[speak] MQTT connection failed: {e}")
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(0)

    arg = sys.argv[1]

    if arg == "--briefing":
        text = morning_briefing()
        print(f"Briefing: {text}")
        publish_speech(text)
    elif arg == "--test":
        test_connectivity()
    else:
        text = " ".join(sys.argv[1:])
        publish_speech(text)
