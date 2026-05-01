#!/usr/bin/env python3
"""Piper TTS Listener — runs on the Pi, speaks what Chronicle sends.

Subscribes to homeforge/voice/speak on MQTT, converts text to speech
via Piper, and plays through the Pi's audio output.

Deploy to Pi:
  scp piper_listener.py pi@192.168.1.10:~/
  ssh pi@192.168.1.10 "pip3 install paho-mqtt && nohup python3 ~/piper_listener.py &"

Requirements on Pi:
  - piper-tts: pip3 install piper-tts  (or piper binary)
  - paho-mqtt: pip3 install paho-mqtt
  - Audio output configured (aplay/paplay working)
  - MQTT broker accessible (localhost if on same Pi)

If Piper isn't installed, falls back to espeak.
"""

import sys, os, json, subprocess, tempfile, time
from datetime import datetime

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("Install paho-mqtt: pip3 install paho-mqtt")
    sys.exit(1)

MQTT_BROKER = os.environ.get("MQTT_BROKER", "127.0.0.1")  # localhost on Pi
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
VOICE_TOPIC = "homeforge/voice/speak"

# Piper model path — adjust for your Pi
PIPER_MODEL = os.environ.get("PIPER_MODEL",
    os.path.expanduser("~/piper-models/en_US-lessac-medium.onnx"))

# Audio device — USB PnP Sound Device on Pi
AUDIO_DEVICE = os.environ.get("AUDIO_DEVICE", "plughw:2,0")

# Rate limit: minimum seconds between utterances
MIN_INTERVAL = 5
_last_spoke = 0


def speak_piper(text):
    """Speak text using Piper TTS."""
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as f:
        wav_path = f.name

    try:
        # Use python3 -m piper (pip-installed piper-tts)
        result = subprocess.run(
            ["python3", "-m", "piper", "--model", PIPER_MODEL, "--output_file", wav_path],
            input=text.encode("utf-8"),
            capture_output=True, timeout=30
        )
        if result.returncode == 0:
            subprocess.run(
                ["aplay", "-D", AUDIO_DEVICE, wav_path],
                capture_output=True, timeout=30,
            )
            return True
        else:
            log(f"Piper failed: {result.stderr.decode()[:100]}")
    except FileNotFoundError:
        pass
    except Exception as e:
        log(f"Piper error: {e}")
    finally:
        try:
            os.unlink(wav_path)
        except Exception:
            pass

    return False


def speak_espeak(text):
    """Fallback: speak using espeak."""
    try:
        subprocess.run(["espeak", text], capture_output=True, timeout=15)
        return True
    except Exception:
        return False


def speak(text):
    """Try Piper, fall back to espeak."""
    global _last_spoke
    now = time.time()
    if now - _last_spoke < MIN_INTERVAL:
        log(f"Rate limited, skipping: {text[:50]}")
        return

    _last_spoke = now
    if not speak_piper(text):
        if not speak_espeak(text):
            log(f"No TTS available. Would say: {text}")


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        log("Connected to MQTT")
        client.subscribe(VOICE_TOPIC, qos=1)
        log(f"Subscribed to {VOICE_TOPIC}")
    else:
        log(f"MQTT connect failed: rc={reason_code}")


def on_message(client, userdata, msg, properties=None):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        text = payload.get("text", "")
        priority = payload.get("priority", "info")
        source = payload.get("source", "unknown")

        if not text:
            return

        log(f"[{source}/{priority}] Speaking: {text[:80]}")
        speak(text)

    except Exception as e:
        log(f"Message error: {e}")


def main():
    log("=== Chronicle Piper TTS Listener starting ===")
    log(f"MQTT: {MQTT_BROKER}:{MQTT_PORT}")
    log(f"Topic: {VOICE_TOPIC}")
    log(f"Piper model: {PIPER_MODEL}")
    log(f"Audio device: {AUDIO_DEVICE}")

    client = mqtt.Client(
        client_id="piper-listener",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv311,
    )
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    log("Listening for voice requests...")

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        log("Shutting down")
        client.disconnect()


if __name__ == "__main__":
    main()
