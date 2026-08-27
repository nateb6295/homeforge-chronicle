#!/usr/bin/env python3
"""
ccs_gist_anchor.py — Stabilize semantic_gist after CCS compression.

Per-field drift analysis (2026-04-17): semantic_gist is the most volatile
identity field (0.270 mean distance) — more volatile than episodic_trace.
The compressor rewrites gist from scratch each time, causing identity drift.

This script runs AFTER compress_cognitive_state. It compares the new gist
to the previous snapshot's gist. If the change is cosmetic (below threshold),
it reverts to the previous gist. Only allows gist changes when the semantic
shift is substantial enough to represent a real focus change.

Usage:
  python3 ccs_gist_anchor.py           # check and anchor gist
  python3 ccs_gist_anchor.py --dry     # show what would happen
  python3 ccs_gist_anchor.py --force   # allow the change
"""
import json
import math
import sqlite3
import sys
import urllib.request

DB = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA = "http://localhost:11434"
EMBED_MODEL = "snowflake-arctic-embed2"

# Gist changes below this threshold are reverted as cosmetic
GIST_CHANGE_THRESHOLD = 0.15


def embed(text, timeout=60):
    text = text[:400]
    body = json.dumps({"model": EMBED_MODEL, "prompt": text}).encode()
    req = urllib.request.Request(
        f"{OLLAMA}/api/embeddings", data=body,
        headers={"Content-Type": "application/json"}
    )
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(resp.read())["embedding"]


def cosine_dist(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 1.0
    return 1.0 - (dot / (na * nb))


def get_current_and_previous_gist():
    """Get the current gist and the most recent snapshot's gist."""
    db = sqlite3.connect(DB)

    # Current CCS
    row = db.execute(
        "SELECT id, semantic_gist FROM cognitive_state ORDER BY updated_at DESC LIMIT 1"
    ).fetchone()
    if not row:
        db.close()
        return None, None, None

    ccs_id, current_gist = row

    # Most recent snapshot (previous state)
    snap_row = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    db.close()

    if not snap_row:
        return ccs_id, current_gist, None

    try:
        snap = json.loads(snap_row[0])
        prev_gist = snap.get("semantic_gist", "")
    except (json.JSONDecodeError, TypeError):
        prev_gist = None

    return ccs_id, current_gist, prev_gist


def was_staleness_override():
    """Check if the most recent compression used a staleness override.

    Reads the stabilized compression log to see if the latest compression
    had a staleness override active. If so, the gist change should NOT be
    reverted — it was forced precisely because the gist was frozen.
    """
    import os
    log_file = os.path.expanduser("~/chronicle/data/stabilized_compression.jsonl")
    try:
        with open(log_file) as f:
            lines = f.readlines()
        if not lines:
            return False
        last = json.loads(lines[-1])
        # The stabilizer logs the injection_used flag. If injection was used
        # and the compression is recent (within 5 min), check staleness.
        import time
        if last.get("injection_used") and (time.time() - last.get("ts", 0)) < 300:
            return True
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        pass
    # Also check the snapshots directly for staleness
    try:
        db = sqlite3.connect(DB)
        rows = db.execute(
            "SELECT snapshot FROM cognitive_state_history ORDER BY created_at DESC LIMIT 6"
        ).fetchall()
        db.close()
        if len(rows) >= 5:
            gists = []
            for r in rows:
                try:
                    s = json.loads(r[0])
                    gists.append(s.get("semantic_gist", ""))
                except (json.JSONDecodeError, TypeError):
                    gists.append("")
            # If the 5 previous snapshots all had the same gist, staleness was active
            if len(set(gists[1:])) == 1 and gists[1]:
                return True
    except Exception:
        pass
    return False


def anchor_gist(dry=False, force=False):
    ccs_id, current_gist, prev_gist = get_current_and_previous_gist()

    if ccs_id is None:
        print("No CCS found.")
        return 1

    if not prev_gist:
        print("No previous snapshot to compare against. Keeping current gist.")
        return 0

    if not current_gist:
        print("Current gist is empty. Keeping as-is.")
        return 0

    print(f"Previous gist ({len(prev_gist)} chars): {prev_gist[:100]}...")
    print(f"Current gist  ({len(current_gist)} chars): {current_gist[:100]}...")

    # Check for staleness override — if the gist was frozen and the stabilizer
    # forced a REBUILD, do NOT revert the change. The whole point of the
    # staleness override is to break the freeze.
    staleness_active = was_staleness_override()
    if staleness_active:
        print("STALENESS OVERRIDE active — gist was frozen. Allowing change regardless of distance.")
        return 0

    # Measure semantic distance
    try:
        prev_emb = embed(prev_gist)
        curr_emb = embed(current_gist)
        dist = cosine_dist(prev_emb, curr_emb)
    except Exception as e:
        print(f"Embedding failed: {e}")
        return 1

    print(f"Semantic distance: {dist:.6f} (threshold: {GIST_CHANGE_THRESHOLD})")

    if force:
        print("--force: allowing change regardless of distance.")
        return 0

    if dist < GIST_CHANGE_THRESHOLD:
        # Cosmetic change — revert
        print(f"ANCHORING: change is cosmetic (below {GIST_CHANGE_THRESHOLD}). Reverting to previous gist.")

        if dry:
            print("(dry run — no changes made)")
            return 0

        db = sqlite3.connect(DB)
        db.execute(
            "UPDATE cognitive_state SET semantic_gist = ? WHERE id = ?",
            (prev_gist, ccs_id),
        )
        db.commit()
        db.close()
        print("Gist reverted to previous version.")
    else:
        print(f"ALLOWING: change is substantial (above {GIST_CHANGE_THRESHOLD}). Gist updated.")

    return 0


def show_gist_history():
    """Show gist across all snapshots."""
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT snapshot, created_at FROM cognitive_state_history ORDER BY created_at ASC"
    ).fetchall()
    db.close()

    print(f"Gist history ({len(rows)} snapshots):")
    print()
    for snap_json, ts in rows:
        try:
            snap = json.loads(snap_json)
            gist = snap.get("semantic_gist", "(empty)")
            from datetime import datetime, timezone, timedelta
            pdt = timezone(timedelta(hours=-7))
            time_str = datetime.fromtimestamp(ts, pdt).strftime("%m-%d %H:%M")
            print(f"[{time_str}] {gist[:120]}")
        except (json.JSONDecodeError, TypeError):
            pass


if __name__ == "__main__":
    if "--history" in sys.argv:
        show_gist_history()
    else:
        dry = "--dry" in sys.argv
        force = "--force" in sys.argv
        sys.exit(anchor_gist(dry=dry, force=force))
