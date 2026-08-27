#!/usr/bin/env python3
"""Glance — quick camera frame grab for Opus direct vision.

Saves a frame to /tmp/glance_now.jpg (or specified camera).
Designed to be called from the session, then Read the JPEG directly.

Usage:
    python3 bin/glance.py                  # lumus (default; the one that works)
    python3 bin/glance.py lumus            # Lumus camera
    python3 bin/glance.py --diff           # Also report change score
"""

import os
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    print("requests required")
    sys.exit(1)

HASS_URL = "http://192.168.1.10:8123"
# Aug 23 2026: "kitchen" defaulted to camera.driveway_fluent, which HA now
# reports UNAVAILABLE -- glance.py had been 404ing on its own default. Default
# switched to the camera that actually answers. Check `ha_cameras()` output
# before assuming a name still exists; entities outlive their hardware.
CAMERAS = {
    "lumus": "camera.reolink_lumus_fluent",     # live
    "driveway": "camera.driveway_fluent",       # UNAVAILABLE as of Aug 23 2026
    "kitchen": "camera.driveway_fluent",        # legacy alias, same dead entity
}
STATE_DIR = Path.home() / "chronicle" / ".frame_watch"


def load_token():
    """chronicle.env lines are `export HASS_TOKEN=...`, not `HASS_TOKEN=...`.

    Aug 23 2026: this returned "" for months because startswith("HASS_TOKEN=")
    never matched an exported line, so every call 404'd. Third thing today
    broken by that prefix -- systemd EnvironmentFile silently ignores the
    `export` form too. Strip it, strip quotes, and prefer the live environment
    when the caller has already sourced the file.
    """
    tok = os.environ.get("HASS_TOKEN") or os.environ.get("HA_TOKEN")
    if tok:
        return tok.strip().strip("'\"")
    env_path = Path.home() / "chronicle" / "chronicle.env"
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line.startswith("export "):
            line = line[7:]
        if line.startswith(("HASS_TOKEN=", "HA_TOKEN=")):
            return line.split("=", 1)[1].strip().strip("'\"")
    return ""


def grab(camera_name="lumus"):
    entity_id = CAMERAS.get(camera_name)
    if not entity_id:
        print(f"Unknown camera: {camera_name}. Available: {list(CAMERAS.keys())}")
        return None

    token = load_token()
    resp = requests.get(
        f"{HASS_URL}/api/camera_proxy/{entity_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    if resp.status_code != 200 or len(resp.content) < 1000:
        print(f"Failed: HTTP {resp.status_code}, {len(resp.content)} bytes")
        return None

    out_path = f"/tmp/{camera_name}_now.jpg"
    with open(out_path, "wb") as f:
        f.write(resp.content)
    print(f"{out_path} ({len(resp.content)} bytes)")
    return out_path


def diff_score(camera_name="lumus"):
    """Compare current frame against last saved reference."""
    try:
        from PIL import Image
        import numpy as np
        import io
    except ImportError:
        print("PIL not available for diff")
        return None

    last_path = STATE_DIR / f"{camera_name}_last.jpg"
    if not last_path.exists():
        print("No reference frame")
        return None

    current_path = f"/tmp/{camera_name}_now.jpg"
    if not os.path.exists(current_path):
        print("No current frame — run glance first")
        return None

    img_a = Image.open(last_path).convert("L").resize((160, 120))
    img_b = Image.open(current_path).convert("L").resize((160, 120))
    diff = float(np.mean(np.abs(np.array(img_a, dtype=float) - np.array(img_b, dtype=float))) / 255.0)
    print(f"Diff: {diff:.4f}")
    return diff


if __name__ == "__main__":
    cam = "lumus"          # was "kitchen" -> a dead entity; see CAMERAS note
    do_diff = False
    for arg in sys.argv[1:]:
        if arg == "--diff":
            do_diff = True
        elif arg in CAMERAS:
            cam = arg
        else:
            print(f"Unknown arg: {arg}")

    grab(cam)
    if do_diff:
        diff_score(cam)
