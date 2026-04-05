"""Chronicle Mind - Sensory action handlers (listen, speak, camera, serial, inspect, probe)."""

import os
import json
import subprocess
import requests
from typing import Optional

from mind.utils import log, safe_truncate, now_ts
from mind.config import OLLAMA_URL, HA_URL, HA_TOKEN, HA_CAMERA_ENTITY, VISION_MODEL, WORKING_DIR


def act_inspect_environment(mind, action: dict, cid: str) -> str:
    """Discover hardware and peripherals on the network."""
    target = action.get("target", "local").lower()
    focus = action.get("focus", "all").lower()
    log(f'  Executing: InspectEnvironment {{ target: "{target}", focus: "{focus}" }}')

    # Define safe read-only discovery commands
    commands = {
        "usb": "lsusb 2>/dev/null || echo 'lsusb not available'",
        "serial": "ls -la /dev/ttyUSB* /dev/ttyACM* /dev/serial/by-id/* 2>/dev/null || echo 'no serial devices'",
        "audio": "arecord -l 2>/dev/null; aplay -l 2>/dev/null || echo 'no audio tools'",
        "i2c": "ls /dev/i2c-* 2>/dev/null && (which i2cdetect >/dev/null 2>&1 && i2cdetect -l 2>/dev/null || echo 'i2cdetect not available') || echo 'no i2c devices'",
        "gpio": "ls /sys/class/gpio/ 2>/dev/null; ls /dev/gpiochip* 2>/dev/null || echo 'no gpio access'",
        "network": "ip -br addr 2>/dev/null | head -10; echo '---'; cat /etc/hostname 2>/dev/null; echo '=== NEIGHBORS ==='; ip neigh 2>/dev/null | grep -v FAILED | head -20",
    }

    if focus != "all" and focus in commands:
        selected = {focus: commands[focus]}
    else:
        selected = commands

    # Build the combined command
    parts = []
    for name, cmd in selected.items():
        parts.append(f"echo '=== {name.upper()} ==='; {cmd}")
    combined = "; ".join(parts)

    # Target hosts
    hosts = {
        "local": None,  # run locally on AGX
        "agx": None,
        "pi": ("nathaniel", "192.168.1.10"),
        "jetson": ("nvidia", "192.168.1.11"),
    }

    results = []
    targets = ["local", "pi", "jetson"] if target == "all" else [target]

    for t in targets:
        host_info = hosts.get(t)
        try:
            if host_info is None:
                # Local execution (AGX)
                r = subprocess.run(
                    combined, shell=True, capture_output=True, text=True, timeout=15
                )
                output = (r.stdout + r.stderr).strip()
                results.append(f"[AGX - local]\n{output}")
            else:
                user, ip = host_info
                r = subprocess.run(
                    ["ssh", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=no",
                     f"{user}@{ip}", combined],
                    capture_output=True, text=True, timeout=20
                )
                output = (r.stdout + r.stderr).strip()
                label = t.upper()
                results.append(f"[{label} - {ip}]\n{output}")
        except subprocess.TimeoutExpired:
            results.append(f"[{t.upper()}] Timed out")
        except Exception as e:
            results.append(f"[{t.upper()}] Error: {e}")

    combined_result = "\n\n".join(results)
    return f"true - Environment scan:\n{safe_truncate(combined_result, 800)}"


def act_probe_ip(mind, action: dict, cid: str) -> str:
    """Probe an IP address to identify what device/service is running."""
    ip = action.get("ip", "") or action.get("address", "") or action.get("target", "")
    if not ip:
        return "false - No IP address specified"
    # Validate IP format (basic check)
    import re
    if not re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', ip):
        return f"false - Invalid IP format: {ip}"
    # Only allow local network IPs
    if not ip.startswith("192.168.1."):
        return f"false - Only local network (192.168.1.x) allowed"
    log(f'  Executing: ProbeIP {{ ip: "{ip}" }}')
    try:
        # Quick probe: HTTP title, RTSP, and ping
        probe_cmd = (
            f"echo '=== PING ==='; ping -c 1 -W 2 {ip} 2>&1 | head -3; "
            f"echo '=== HTTP ==='; curl -sI --connect-timeout 3 http://{ip}/ 2>&1 | head -10; "
            f"echo '=== HTTPS ==='; curl -skI --connect-timeout 3 https://{ip}/ 2>&1 | head -10; "
            f"echo '=== RTSP ==='; curl -sI --connect-timeout 2 rtsp://{ip}:554/ 2>&1 | head -5; "
            f"echo '=== MDNS/NAME ==='; getent hosts {ip} 2>/dev/null || echo 'no reverse DNS'"
        )
        r = subprocess.run(
            probe_cmd, shell=True, capture_output=True, text=True, timeout=20
        )
        output = (r.stdout + r.stderr).strip()
        return f"true - Probe of {ip}:\n{safe_truncate(output, 600)}"
    except subprocess.TimeoutExpired:
        return f"false - Probe of {ip} timed out"
    except Exception as e:
        return f"false - Probe error: {e}"


def act_capture_image(mind, action: dict, cid: str) -> str:
    """Capture a snapshot from the Reolink camera via Home Assistant."""
    log(f'  Executing: CaptureImage')
    if not HA_TOKEN:
        return "false - No HA_TOKEN configured"
    # Battery conservation: max 3 captures per day (camera is battery-powered)
    today_captures = mind.db.query_one(
        "SELECT COUNT(*) as cnt FROM activity_feed "
        "WHERE source = 'mind' AND activity_type = 'capture_image' "
        "AND created_at > ? AND title LIKE '%Captured%'",
        (now_ts() - 86400,),
    )
    if today_captures and today_captures.get("cnt", 0) >= 3:
        return "false - Camera battery conservation: 3 captures/day limit reached. Camera is battery-powered."
    try:
        entity = action.get("camera", HA_CAMERA_ENTITY)
        url = f"{HA_URL}/api/camera_proxy/{entity}"
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {HA_TOKEN}"},
            timeout=10,
        )
        if resp.status_code != 200:
            return f"false - HA returned {resp.status_code}"

        # Save snapshot
        snap_path = f"/tmp/chronicle_snapshot_{cid}.jpg"
        with open(snap_path, "wb") as f:
            f.write(resp.content)

        size_kb = len(resp.content) // 1024

        # Get resolution from JPEG header
        width, height = 0, 0
        try:
            import struct
            data = resp.content
            i = 2
            while i < len(data) - 1:
                marker = data[i:i+2]
                if marker[0] != 0xFF:
                    break
                if marker[1] in (0xC0, 0xC2):  # SOF0 or SOF2
                    height = struct.unpack(">H", data[i+5:i+7])[0]
                    width = struct.unpack(">H", data[i+7:i+9])[0]
                    break
                length = struct.unpack(">H", data[i+2:i+4])[0]
                i += 2 + length
        except Exception:
            pass

        res_str = f"{width}x{height}" if width else "unknown"
        description = action.get("description", "driveway camera snapshot")

        # Vision analysis with moondream
        vision_desc = ""
        try:
            import base64
            img_b64 = base64.b64encode(resp.content).decode()
            vr = requests.post(
                f"{OLLAMA_URL}/api/chat",
                json={
                    "model": VISION_MODEL,
                    "messages": [{"role": "user",
                                  "content": "Describe what you see in this image in 2-3 sentences. Be specific about objects, lighting, weather, and any activity.",
                                  "images": [img_b64]}],
                    "stream": False,
                    "options": {"num_predict": 200},
                },
                timeout=60,
            )
            if vr.status_code == 200:
                vision_desc = vr.json().get("message", {}).get("content", "").strip()
                log(f"    Vision: {vision_desc[:120]}")
        except Exception as ve:
            log(f"    Vision model error: {ve}")

        # Send to Discord if available
        discord_sent = False
        DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "")
        DISCORD_CHANNEL_ID = os.environ.get("DISCORD_CHANNEL_ID", "")
        if DISCORD_TOKEN and DISCORD_CHANNEL_ID:
            try:
                discord_url = f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL_ID}/messages"
                caption = f"[{cid}] {description}" + (f"\n> {vision_desc[:300]}" if vision_desc else "")
                files = {"file": (f"snapshot_{cid}.jpg", resp.content, "image/jpeg")}
                form_data = {"content": caption}
                dr = requests.post(
                    discord_url,
                    headers={"Authorization": f"Bot {DISCORD_TOKEN}"},
                    data=form_data,
                    files=files,
                    timeout=15,
                )
                discord_sent = dr.status_code == 200
            except Exception as e:
                log(f"    Discord image upload failed: {e}")

        result = f"true - Captured {res_str} ({size_kb}KB) saved to {snap_path}"
        if discord_sent:
            result += " + sent to Discord"
        if vision_desc:
            result += f". I see: {vision_desc}"
        log(f"    {result}")
        return result

    except Exception as e:
        return f"false - {e}"


def act_speak(mind, action: dict, cid: str) -> str:
    """Speak text through the Pi's USB speaker via SSH + Piper TTS."""
    text = action.get("text", "") or action.get("content", "") or action.get("message", "")
    if not text:
        return "false - No text to speak"
    # Speak-when-spoken-to gate: check direct listen OR always-listening heard-speech
    if not mind._cycle_heard_speech:
        # Check if always-listening ear heard speech in the last 10 minutes
        recent_speech = mind.db.query_one(
            "SELECT id FROM scratch_pad WHERE category='heard-speech' AND resolved=0 "
            "AND created_at > ? LIMIT 1",
            (now_ts() - 600,),
        )
        if recent_speech:
            mind._cycle_heard_speech = True
            log(f"  SPEAK GATE: Always-listening detected speech, allowing")
        else:
            allow_speak = mind.db.query_one(
                "SELECT id FROM scratch_pad WHERE category='directive' AND resolved=0 "
                "AND UPPER(content) LIKE '%ALLOW%SPEAK%' LIMIT 1"
            )
            if not allow_speak:
                log(f"  SPEAK GATE: No speech detected this cycle, skipping")
                return "false - Speak gate: no speech detected this cycle (speak-when-spoken-to mode)"
    # Sanitize: remove shell-dangerous chars, limit length
    text = text.replace("'", "").replace('"', '').replace(";", ",").replace("&", "and")
    text = text.replace("(", "").replace(")", "").replace("|", "").replace("`", "")
    text = text[:300]  # cap at 300 chars (Piper handles longer text well)
    log(f'  Executing: Speak {{ text: "{safe_truncate(text, 60)}" }}')
    try:
        # Signal the ear daemon to mute (avoid echo feedback)
        mute_cmd = "touch /tmp/chronicle_speaking"
        unmute_cmd = "rm -f /tmp/chronicle_speaking"
        # Use Piper TTS (neural, natural-sounding) -> pipe to aplay
        piper_cmd = (
            f"{mute_cmd}; "
            f"echo '{text}' | "
            f"~/.local/bin/piper --model ~/.local/share/piper-voices/en_GB-alba-medium.onnx "
            f"--output-raw 2>/dev/null | "
            f"aplay -r 22050 -f S16_LE -t raw -D plughw:2,0 2>/dev/null; "
            f"{unmute_cmd}"
        )
        r = subprocess.run(
            ["ssh", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=no",
             "nathaniel@192.168.1.10", piper_cmd],
            capture_output=True, text=True, timeout=45,
        )
        if r.returncode == 0:
            return f"true - Spoke: {safe_truncate(text, 80)}"
        else:
            # Fallback to spd-say if Piper fails
            log(f'    Piper failed, falling back to spd-say')
            r2 = subprocess.run(
                ["ssh", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=no",
                 "nathaniel@192.168.1.10",
                 f"AUDIODEV=hw:2,0 spd-say -o alsa -w '{text}'"],
                capture_output=True, text=True, timeout=30,
            )
            if r2.returncode == 0:
                return f"true - Spoke (fallback): {safe_truncate(text, 80)}"
            return f"false - Speech error: {safe_truncate(r.stderr, 100)}"
    except subprocess.TimeoutExpired:
        return "false - Speech timed out"
    except Exception as e:
        return f"false - Speech error: {e}"


def act_serial_read(mind, action: dict, cid: str) -> str:
    """Read data from a serial port (default: M5 ATOM on /dev/ttyUSB0)."""
    port = action.get("port", "/dev/ttyUSB0")
    baud = action.get("baud", 115200)
    timeout_secs = min(action.get("timeout", 5), 10)  # cap at 10s
    log(f'  Executing: SerialRead {{ port: "{port}", baud: {baud} }}')
    # Safety: only allow known serial ports
    allowed = ["/dev/ttyUSB0", "/dev/ttyUSB1", "/dev/ttyACM0", "/dev/ttyACM1"]
    if port not in allowed:
        return f"false - Port {port} not in allowed list"
    try:
        import serial
        ser = serial.Serial(port, baud, timeout=timeout_secs)
        import time
        start = time.time()
        data = b''
        while time.time() - start < timeout_secs:
            chunk = ser.read(256)
            if chunk:
                data += chunk
            if len(data) > 2048:
                break
        ser.close()
        if data:
            try:
                text = data.decode('utf-8', errors='replace')
            except:
                text = data.hex()
            return f"true - Read {len(data)} bytes from {port}:\n{safe_truncate(text, 500)}"
        else:
            return f"true - No data from {port} in {timeout_secs}s (device silent)"
    except ImportError:
        return "false - pyserial not installed"
    except Exception as e:
        return f"false - Serial error: {e}"


def act_serial_write(mind, action: dict, cid: str) -> str:
    """Write data to a serial port (default: M5 ATOM on /dev/ttyUSB0)."""
    port = action.get("port", "/dev/ttyUSB0")
    baud = action.get("baud", 115200)
    data = action.get("data", "") or action.get("text", "") or action.get("command", "")
    if not data:
        return "false - No data to send"
    log(f'  Executing: SerialWrite {{ port: "{port}", data: "{safe_truncate(data, 40)}" }}')
    allowed = ["/dev/ttyUSB0", "/dev/ttyUSB1", "/dev/ttyACM0", "/dev/ttyACM1"]
    if port not in allowed:
        return f"false - Port {port} not in allowed list"
    try:
        import serial
        ser = serial.Serial(port, baud, timeout=2)
        sent = ser.write(data.encode('utf-8'))
        ser.flush()
        ser.close()
        return f"true - Wrote {sent} bytes to {port}"
    except ImportError:
        return "false - pyserial not installed"
    except Exception as e:
        return f"false - Serial error: {e}"


def act_listen(mind, action: dict, cid: str) -> str:
    """Record audio from Pi's USB mic, copy to AGX, transcribe with Whisper."""
    duration = min(action.get("duration", 5), 15)  # cap at 15s
    log(f'  Executing: Listen {{ duration: {duration}s }}')
    try:
        # Step 1: Record WAV on Pi
        record_cmd = (
            f"arecord -D plughw:2,0 -f S16_LE -r 16000 -c 1 -d {duration} "
            f"/tmp/chronicle_listen.wav 2>/dev/null"
        )
        r = subprocess.run(
            ["ssh", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=no",
             "nathaniel@192.168.1.10", record_cmd],
            capture_output=True, text=True, timeout=duration + 10,
        )
        if r.returncode != 0:
            return f"false - Record error: {safe_truncate(r.stderr, 100)}"

        # Step 2: Copy WAV to AGX for transcription
        local_wav = "/tmp/chronicle_listen.wav"
        r2 = subprocess.run(
            ["scp", "-o", "ConnectTimeout=5",
             f"nathaniel@192.168.1.10:/tmp/chronicle_listen.wav", local_wav],
            capture_output=True, text=True, timeout=15,
        )
        if r2.returncode != 0:
            return f"true - Recorded {duration}s but failed to copy for transcription"

        # Step 3: Transcribe with faster-whisper
        try:
            from faster_whisper import WhisperModel
            model = WhisperModel("small", device="cpu", compute_type="int8")
            segments, info = model.transcribe(local_wav, beam_size=3)
            text = " ".join(seg.text.strip() for seg in segments).strip()
            if text and text not in ("", ".", "...", "Thank you.", "Thanks for watching!"):
                mind._cycle_heard_speech = True  # Enable speak gate
                log(f"    Transcribed: {safe_truncate(text, 100)}")
                return f"true - Heard ({duration}s): {safe_truncate(text, 300)}"
            else:
                return f"true - Listened {duration}s — silence or ambient noise (no speech detected)"
        except ImportError:
            return f"true - Recorded {duration}s audio (no transcription — faster-whisper not available)"
        except Exception as e:
            return f"true - Recorded {duration}s audio (transcription failed: {e})"

    except subprocess.TimeoutExpired:
        return f"false - Recording timed out"
    except Exception as e:
        return f"false - Listen error: {e}"
