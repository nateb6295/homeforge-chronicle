#!/usr/bin/env python3
"""Chronicle Eye — Periodic camera snapshot analysis via Gemma 4 vision.

Fetches snapshots from Home Assistant cameras, describes them using Gemma 4 26B's
vision capabilities, and publishes descriptions to MQTT for HAL to consume.

This closes the "eye" part of the perception loop: cameras see → Gemma describes →
HAL knows what the house looks like → presence layer can reference it.

Runs as: systemctl --user start chronicle-eye
"""

import base64
import json
import os
import signal
import sqlite3
import sys
import time
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

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ══════════════════════════════════════════════════��════════════════

HASS_URL = os.environ.get("HASS_URL", "http://192.168.1.10:8123")
HASS_TOKEN = os.environ.get("HASS_TOKEN", "")
MQTT_BROKER = os.environ.get("MQTT_BROKER", "192.168.1.10")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")  # for embeddings
INFERENCE_URL = os.environ.get("EYE_INFERENCE_URL", "http://localhost:11435")  # llama-server for vision
VISION_MODEL = os.environ.get("VISION_MODEL", "gemma4:26b")
DB_PATH = os.environ.get("CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"))

# Camera entity IDs to check (HA entity_id → friendly name)
CAMERAS = {
    "camera.driveway_fluent": "kitchen",
    "camera.reolink_lumus_fluent": "lumus",
}

CYCLE_INTERVAL = 1800  # 30 minutes between full scans
SNAPSHOT_TIMEOUT = 15   # seconds to fetch a snapshot
VISION_TIMEOUT = 180    # seconds for Gemma vision inference
MQTT_TOPIC = "homeforge/home/eye/description"

# ═══════════════════════════════════════════════════════════════════
#  Logging
# ═══════════════════════════════════════════════════════════════════

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

# ═══════════════════════════════════════════════════════════════════
#  Snapshot Fetching
# ═══════════════════════════════════════════════════════════════════

def fetch_snapshot(entity_id):
    """Fetch a camera snapshot from Home Assistant. Returns JPEG bytes or None."""
    if not HASS_TOKEN:
        log(f"  No HASS_TOKEN — cannot fetch {entity_id}")
        return None
    try:
        resp = requests.get(
            f"{HASS_URL}/api/camera_proxy/{entity_id}",
            headers={"Authorization": f"Bearer {HASS_TOKEN}"},
            timeout=SNAPSHOT_TIMEOUT,
        )
        if resp.status_code == 200 and len(resp.content) > 1000:
            return resp.content
        log(f"  {entity_id}: HTTP {resp.status_code}, {len(resp.content)} bytes (skipping)")
        return None
    except requests.RequestException as e:
        log(f"  {entity_id}: fetch error — {e}")
        return None


def is_camera_available(entity_id):
    """Check if a camera entity is available in HA."""
    if not HASS_TOKEN:
        return False
    try:
        resp = requests.get(
            f"{HASS_URL}/api/states/{entity_id}",
            headers={"Authorization": f"Bearer {HASS_TOKEN}"},
            timeout=10,
        )
        if resp.status_code == 200:
            state = resp.json().get("state", "")
            return state not in ("unavailable", "unknown")
        return False
    except Exception:
        return False

# ═══════════════════════════════════════════════════════════════════
#  Vision Analysis
# ═══════════════════════════════════════════════════════════════════

def describe_image(image_bytes, camera_name):
    """Send image to Gemma 4 for description. Returns description string or None."""
    img_b64 = base64.b64encode(image_bytes).decode()

    hour = datetime.now().hour
    time_context = "at night" if hour < 6 or hour >= 21 else (
        "in the morning" if hour < 12 else (
        "in the afternoon" if hour < 17 else "in the evening"))

    prompt = (
        f"This is a security camera image from the {camera_name} camera {time_context}. "
        "Describe what you see in 2-3 sentences. Focus on: the general scene, "
        "any people present, vehicles, animals, weather/lighting conditions, "
        "and anything unusual. Be factual and concise."
    )

    try:
        resp = requests.post(
            f"{INFERENCE_URL}/v1/chat/completions",
            json={
                "model": VISION_MODEL,
                "messages": [{
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}},
                    ],
                }],
                "max_tokens": 300,
                "temperature": 0.3,
                "reasoning_format": "none",
            },
            timeout=VISION_TIMEOUT,
        )
        if resp.status_code == 200:
            data = resp.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "") if data.get("choices") else ""
            # Strip Gemma 4 thinking tags if present
            if "<channel|>" in content:
                content = content.split("<channel|>")[-1].strip()
            eval_count = data.get("usage", {}).get("completion_tokens", 0)
            duration = 0  # llama-server doesn't report total_duration
            # Strip common LLM preambles
            for prefix in [
                "Here's a description of the security camera image:\n\n",
                "Here's a description of the camera image:\n\n",
                "Here is a description of the security camera image:\n\n",
            ]:
                if content.startswith(prefix):
                    content = content[len(prefix):]
                    break
            log(f"  {camera_name}: \"{content[:80]}...\" ({eval_count} tokens, {duration:.1f}s)")
            return content.strip()
        log(f"  {camera_name}: Ollama HTTP {resp.status_code}")
        return None
    except requests.RequestException as e:
        log(f"  {camera_name}: vision error — {e}")
        return None

# ═══════════════════════════════════════════════════════════════════
#  Publishing
# ═══════════════════════════════════════════════════════════════════

def publish_description(mqtt_client, camera_name, description):
    """Publish eye description to MQTT for HAL."""
    payload = json.dumps({
        "camera": camera_name,
        "description": description,
        "timestamp": int(time.time()),
    })
    mqtt_client.publish(MQTT_TOPIC, payload, qos=0)


def store_description(camera_name, description):
    """Store in activity_feed for pipeline visibility."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.execute(
            "INSERT INTO activity_feed (source, activity_type, content, created_at) "
            "VALUES (?, ?, ?, ?)",
            ("eye", "camera_description",
             f"[{camera_name}] {description}", int(time.time()))
        )
        conn.commit()
        conn.close()
    except Exception as e:
        log(f"  DB store error: {e}")

# ═══════════════════════════════════════════════════════════════════
#  Main Loop
# ═══════════════════════════════════════════════════════════════════

def scan_cameras(mqtt_client):
    """Scan all cameras, describe what's visible, publish results."""
    described = 0
    for entity_id, camera_name in CAMERAS.items():
        if not is_camera_available(entity_id):
            log(f"  {camera_name}: unavailable, skipping")
            continue

        snapshot = fetch_snapshot(entity_id)
        if not snapshot:
            continue

        description = describe_image(snapshot, camera_name)
        if not description:
            continue

        publish_description(mqtt_client, camera_name, description)
        store_description(camera_name, description)
        described += 1

    return described


def main():
    log("═══ Chronicle Eye starting ═══")
    log(f"HA: {HASS_URL}")
    log(f"MQTT: {MQTT_BROKER}:{MQTT_PORT}")
    log(f"Ollama: {OLLAMA_URL} ({VISION_MODEL})")
    log(f"Cameras: {list(CAMERAS.values())}")
    log(f"Cycle: {CYCLE_INTERVAL}s")

    global HASS_TOKEN
    if not HASS_TOKEN:
        log("WARNING: No HASS_TOKEN set. Loading from chronicle.env...")
        env_path = os.path.expanduser("~/chronicle/chronicle.env")
        if os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    if line.startswith("HASS_TOKEN="):
                        HASS_TOKEN = line.strip().split("=", 1)[1]
                        log("  Loaded HASS_TOKEN from chronicle.env")
                        break
    if not HASS_TOKEN:
        log("ERROR: No HASS_TOKEN available. Cannot fetch camera snapshots.")
        return

    # MQTT setup
    client = mqtt.Client(
        client_id="chronicle-eye",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_start()
        log("MQTT connected")
    except Exception as e:
        log(f"MQTT connect failed: {e}")
        return

    # Graceful shutdown
    running = [True]
    def handle_signal(sig, frame):
        log("Shutting down...")
        running[0] = False
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    # Initial scan
    log("── Initial scan ──")
    n = scan_cameras(client)
    log(f"Described {n} camera(s)")

    # Main loop
    while running[0]:
        for _ in range(CYCLE_INTERVAL):
            if not running[0]:
                break
            time.sleep(1)

        if running[0]:
            log(f"── Scan cycle ──")
            n = scan_cameras(client)
            log(f"Described {n} camera(s)")

    client.disconnect()
    log("═══ Chronicle Eye stopped ═══")


if __name__ == "__main__":
    main()
