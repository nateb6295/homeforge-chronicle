#!/usr/bin/env python3
"""Frame Watch — MJPEG frame extractor with change detection for Chronicle Eye.

Pulls frames from HA's camera_proxy_stream (MJPEG), compares consecutive frames
for scene changes, and only sends changed frames to Gemma 4 vision for description.
Publishes descriptions to MQTT for HAL consumption.

This upgrades Eye from 30-min snapshot polling to near-realtime scene awareness
with intelligent filtering (only describe when something changes).

Usage:
    python3 bin/frame_watch.py                # Run once (extract + compare + describe if changed)
    python3 bin/frame_watch.py --loop         # Continuous loop at POLL_INTERVAL
    python3 bin/frame_watch.py --test         # Test frame extraction only (no vision)
"""

import argparse
import base64
import hashlib
import io
import json
import os
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print("requests required: pip install requests")
    sys.exit(1)

try:
    from PIL import Image
    import numpy as np
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

# ═══════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════

HASS_URL = "http://192.168.1.10:8123"
MQTT_BROKER = "192.168.1.10"
MQTT_PORT = 1883
INFERENCE_URL = "http://localhost:11435"  # llama-server for Gemma vision
VISION_MODEL = "gemma4:26b"
DB_PATH = "/mnt/hdd/chronicle-data/processed.db"

# Camera config — entity_id → friendly name
CAMERAS = {
    "camera.driveway_fluent": "kitchen",
}

POLL_INTERVAL = 30        # seconds between frame pulls
CHANGE_THRESHOLD = 0.03   # minimum pixel difference ratio to trigger description
FRAME_TIMEOUT = 8         # seconds to wait for a frame from stream
VISION_TIMEOUT = 120      # seconds for Gemma vision
MQTT_TOPIC = "homeforge/home/eye/description"

# State directory for last-frame storage
STATE_DIR = Path.home() / "chronicle" / ".frame_watch"


def _load_hass_token():
    env_path = Path.home() / "chronicle" / "chronicle.env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("HASS_TOKEN="):
                return line.split("=", 1)[1].strip()
    return os.environ.get("HASS_TOKEN", "")


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ═══════════════════════════════════════════════════════════════════
#  Frame Extraction
# ═══════════════════════════════════════════════════════════════════

def extract_frame(entity_id):
    """Extract a single JPEG frame from the MJPEG stream.

    Reads just enough data to get one complete frame, then closes the stream.
    More efficient than camera_proxy snapshot for frequent polling because
    the stream is already running on the Reolink hub.
    """
    token = _load_hass_token()
    if not token:
        log("No HASS_TOKEN found")
        return None

    try:
        resp = requests.get(
            f"{HASS_URL}/api/camera_proxy_stream/{entity_id}",
            headers={"Authorization": f"Bearer {token}"},
            stream=True,
            timeout=FRAME_TIMEOUT,
        )
        if resp.status_code != 200:
            log(f"Stream HTTP {resp.status_code} for {entity_id}")
            return None

        # Read MJPEG multipart stream until we have one complete frame
        buffer = b""
        jpeg_start = None
        content_length = None

        for chunk in resp.iter_content(chunk_size=4096):
            buffer += chunk

            # Parse multipart headers to find content-length
            if content_length is None:
                header_end = buffer.find(b"\r\n\r\n")
                if header_end >= 0:
                    headers_text = buffer[:header_end].decode("ascii", errors="replace")
                    for line in headers_text.split("\r\n"):
                        if line.lower().startswith("content-length:"):
                            content_length = int(line.split(":")[1].strip())
                    jpeg_start = header_end + 4

            # If we know content_length, check if we have the full frame
            if content_length is not None and jpeg_start is not None:
                jpeg_data = buffer[jpeg_start:]
                if len(jpeg_data) >= content_length:
                    resp.close()
                    return jpeg_data[:content_length]

            # Safety: don't read more than 500KB for a single frame
            if len(buffer) > 500_000:
                resp.close()
                log(f"Frame too large ({len(buffer)} bytes), aborting")
                return None

        resp.close()
        return None

    except requests.RequestException as e:
        log(f"Stream error for {entity_id}: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════
#  Change Detection
# ═══════════════════════════════════════════════════════════════════

def frame_hash(jpeg_bytes):
    """Quick hash for exact duplicate detection."""
    return hashlib.md5(jpeg_bytes).hexdigest()


def compute_difference(frame_a, frame_b):
    """Compute normalized pixel difference between two JPEG frames.

    Returns a float 0.0 (identical) to 1.0 (completely different).
    Falls back to hash comparison if PIL isn't available.
    """
    if not HAS_PIL:
        # Fallback: binary hash comparison (only detects exact dupes)
        return 0.0 if frame_hash(frame_a) == frame_hash(frame_b) else 1.0

    try:
        img_a = Image.open(io.BytesIO(frame_a)).convert("L").resize((160, 120))
        img_b = Image.open(io.BytesIO(frame_b)).convert("L").resize((160, 120))

        arr_a = np.array(img_a, dtype=np.float32)
        arr_b = np.array(img_b, dtype=np.float32)

        # Mean absolute difference normalized to 0-1
        diff = np.mean(np.abs(arr_a - arr_b)) / 255.0
        return float(diff)
    except Exception as e:
        log(f"Diff error: {e}")
        return 1.0  # assume changed on error


def load_last_frame(camera_name):
    """Load the last-seen frame for comparison."""
    path = STATE_DIR / f"{camera_name}_last.jpg"
    if path.exists():
        return path.read_bytes()
    return None


def save_last_frame(camera_name, jpeg_bytes):
    """Save the current frame as the reference for next comparison."""
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    path = STATE_DIR / f"{camera_name}_last.jpg"
    path.write_bytes(jpeg_bytes)


# ═══════════════════════════════════════════════════════════════════
#  Vision Description
# ═══════════════════════════════════════════════════════════════════

def describe_frame(jpeg_bytes, camera_name):
    """Send frame to Gemma 4 vision for description."""
    img_b64 = base64.b64encode(jpeg_bytes).decode()

    hour = datetime.now().hour
    time_context = "at night" if hour < 6 or hour >= 21 else (
        "in the morning" if hour < 12 else (
        "in the afternoon" if hour < 17 else "in the evening"))

    prompt = (
        f"This is a camera image from the {camera_name} {time_context}. "
        "Describe what you see in 2-3 sentences. Focus on: people present, "
        "activity, animals, lighting conditions, and anything that changed "
        "or is unusual. Be factual and concise."
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
                "max_tokens": 200,
                "temperature": 0.3,
                "reasoning_format": "none",
            },
            timeout=VISION_TIMEOUT,
        )
        if resp.status_code == 200:
            data = resp.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            if "<channel|>" in content:
                content = content.split("<channel|>")[-1].strip()
            for prefix in [
                "Here's a description of the security camera image:\n\n",
                "Here's a description of the camera image:\n\n",
                "Here is a description of the security camera image:\n\n",
            ]:
                if content.startswith(prefix):
                    content = content[len(prefix):]
                    break
            return content.strip()
        log(f"Vision HTTP {resp.status_code}")
        return None
    except requests.RequestException as e:
        log(f"Vision error: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════
#  Publishing
# ═══════════════════════════════════════════════════════════════════

def publish_mqtt(camera_name, description, diff_score):
    """Publish description to MQTT."""
    try:
        import paho.mqtt.client as mqtt
        client = mqtt.Client()
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        payload = json.dumps({
            "camera": camera_name,
            "description": description,
            "diff_score": round(diff_score, 4),
            "timestamp": datetime.now().isoformat(),
            "source": "frame_watch",
        })
        client.publish(MQTT_TOPIC, payload)
        client.disconnect()
        return True
    except Exception as e:
        log(f"MQTT publish error: {e}")
        return False


def log_to_db(camera_name, description, diff_score):
    """Log frame watch event to activity_feed."""
    try:
        db = sqlite3.connect(DB_PATH)
        db.execute(
            "INSERT INTO activity_feed (activity_type, content, source, created_at) VALUES (?, ?, ?, ?)",
            ("camera_description",
             json.dumps({"camera": camera_name, "description": description, "diff": round(diff_score, 4)}),
             "frame_watch",
             int(time.time())),
        )
        db.commit()
        db.close()
    except Exception as e:
        log(f"DB log error: {e}")


# ═══════════════════════════════════════════════════════════════════
#  Main Loop
# ═══════════════════════════════════════════════════════════════════

def process_cameras(describe=True):
    """Process all cameras once. Returns dict of results."""
    results = {}
    for entity_id, camera_name in CAMERAS.items():
        frame = extract_frame(entity_id)
        if frame is None:
            log(f"{camera_name}: no frame extracted")
            results[camera_name] = {"status": "no_frame"}
            continue

        log(f"{camera_name}: frame extracted ({len(frame)} bytes)")

        # Compare with last frame
        last_frame = load_last_frame(camera_name)
        if last_frame is None:
            diff = 1.0  # first frame, treat as changed
            log(f"{camera_name}: first frame (no reference)")
        else:
            diff = compute_difference(last_frame, frame)
            log(f"{camera_name}: diff={diff:.4f} (threshold={CHANGE_THRESHOLD})")

        save_last_frame(camera_name, frame)

        if diff < CHANGE_THRESHOLD:
            log(f"{camera_name}: no significant change, skipping description")
            results[camera_name] = {"status": "unchanged", "diff": diff}
            continue

        if not describe:
            results[camera_name] = {"status": "changed", "diff": diff}
            continue

        # Describe the changed frame
        log(f"{camera_name}: scene changed, describing...")
        description = describe_frame(frame, camera_name)
        if description:
            log(f"{camera_name}: \"{description[:80]}...\"")
            publish_mqtt(camera_name, description, diff)
            log_to_db(camera_name, description, diff)
            results[camera_name] = {"status": "described", "diff": diff, "description": description}
        else:
            log(f"{camera_name}: vision failed")
            results[camera_name] = {"status": "vision_failed", "diff": diff}

    return results


def main():
    parser = argparse.ArgumentParser(description="Frame Watch — MJPEG change detection for Chronicle Eye")
    parser.add_argument("--loop", action="store_true", help="Continuous polling loop")
    parser.add_argument("--test", action="store_true", help="Test frame extraction only (no vision)")
    parser.add_argument("--interval", type=int, default=POLL_INTERVAL, help=f"Poll interval in seconds (default: {POLL_INTERVAL})")
    args = parser.parse_args()

    log(f"Frame Watch starting (cameras: {list(CAMERAS.values())}, interval: {args.interval}s)")
    if not HAS_PIL:
        log("WARNING: PIL not available. Change detection using hash comparison only (exact dupes).")

    if args.loop:
        while True:
            process_cameras(describe=not args.test)
            time.sleep(args.interval)
    else:
        results = process_cameras(describe=not args.test)
        for cam, r in results.items():
            print(f"  {cam}: {r.get('status', 'unknown')}", end="")
            if "diff" in r:
                print(f" (diff={r['diff']:.4f})", end="")
            if "description" in r:
                print(f" — {r['description'][:60]}...", end="")
            print()


if __name__ == "__main__":
    main()
