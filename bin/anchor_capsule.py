#!/usr/bin/env python3
"""Anchor a curated capsule — mark it as relationally significant.

When a capsule is stored during a live session because the moment landed,
this appends it to the relational anchors file. On next CCS compression,
these anchors get pulled into retrieved_artifacts so the gist knows
where to point for texture.

Usage:
  python3 anchor_capsule.py 40738 "Direction set — relational memory as first-class"
  python3 anchor_capsule.py 41221 "evalladen: depth is when nuance is load-bearing"
  python3 anchor_capsule.py --list          # show current anchors
  python3 anchor_capsule.py --clear         # clear after compression consumed them
"""
import json
import os
import sys
import time
from pathlib import Path

ANCHOR_FILE = Path(os.path.expanduser("~/chronicle/data/relational_anchors.jsonl"))


def anchor(capsule_id: int, tag: str):
    ANCHOR_FILE.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "capsule_id": capsule_id,
        "tag": tag,
        "anchored_at": int(time.time()),
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    with open(ANCHOR_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")
    print(f"Anchored capsule {capsule_id}: {tag}")


def list_anchors():
    if not ANCHOR_FILE.exists():
        print("No anchors yet.")
        return []
    anchors = []
    for line in ANCHOR_FILE.read_text().strip().split("\n"):
        if line:
            anchors.append(json.loads(line))
    if not anchors:
        print("No anchors yet.")
        return []
    for a in anchors:
        print(f"  #{a['capsule_id']} [{a['ts']}] {a['tag']}")
    return anchors


def clear_anchors():
    if ANCHOR_FILE.exists():
        ANCHOR_FILE.unlink()
        print("Anchors cleared.")
    else:
        print("No anchors to clear.")


def load_anchors():
    if not ANCHOR_FILE.exists():
        return []
    anchors = []
    for line in ANCHOR_FILE.read_text().strip().split("\n"):
        if line:
            anchors.append(json.loads(line))
    return anchors


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: anchor_capsule.py <capsule_id> <tag>")
        print("       anchor_capsule.py --list")
        print("       anchor_capsule.py --clear")
        sys.exit(1)

    if sys.argv[1] == "--list":
        list_anchors()
    elif sys.argv[1] == "--clear":
        clear_anchors()
    else:
        cid = int(sys.argv[1])
        tag = " ".join(sys.argv[2:]) if len(sys.argv) > 2 else ""
        anchor(cid, tag)
