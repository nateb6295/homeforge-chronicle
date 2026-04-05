#!/usr/bin/env python3
"""Nostr Engagement Monitor — checks for replies and reactions to our posts,
feeds them into activity_feed so the swarm can see external engagement.

Usage:
    python3 nostr_monitor.py           # Check and ingest new engagement
    python3 nostr_monitor.py --dry-run # Show what would be ingested

Run via cron every 30 minutes or as needed.
"""

import json
import os
import sqlite3
import sys
import time

import websocket

# --- Config ---
DB_PATH = os.environ.get("CHRONICLE_DB",
    "/mnt/hdd/chronicle-data/processed.db")

def _load_nsec():
    """Load NOSTR_NSEC from env or chronicle.env."""
    nsec = os.environ.get("NOSTR_NSEC")
    if not nsec:
        env_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "chronicle.env")
        if os.path.exists(env_file):
            with open(env_file) as f:
                for line in f:
                    if line.startswith("NOSTR_NSEC="):
                        nsec = line.strip().split("=", 1)[1]
    return nsec

NOSTR_NSEC = _load_nsec() or ""

RELAYS = [
    "wss://nos.lol",
    "wss://relay.damus.io",
    "wss://relay.primal.net",
    "wss://offchain.pub",
]

# How far back to look (hours) — first run uses 168h (7 days), subsequent uses 6h
LOOKBACK_HOURS = 6


def get_pubkey(privkey_hex: str) -> str:
    """Derive x-only pubkey from privkey."""
    from coincurve import PrivateKey
    sk = PrivateKey(bytes.fromhex(privkey_hex))
    full = sk.public_key.format(compressed=True)
    return full[1:].hex()


def fetch_engagement(privkey_hex: str, since_hours: int = LOOKBACK_HOURS) -> dict:
    """Fetch replies and reactions from relays."""
    my_pubkey = get_pubkey(privkey_hex)
    since = int(time.time()) - (since_hours * 3600)

    events = {}

    mention_filter = {"#p": [my_pubkey], "kinds": [1], "since": since}
    reaction_filter = {"#p": [my_pubkey], "kinds": [7], "since": since}

    for relay in RELAYS[:2]:  # Check 2 relays for speed
        try:
            ws = websocket.create_connection(relay, timeout=10)
            ws.send(json.dumps(["REQ", "mentions", mention_filter]))
            ws.send(json.dumps(["REQ", "reactions", reaction_filter]))

            ws.settimeout(5)
            eose_count = 0
            while eose_count < 2:
                try:
                    raw = ws.recv()
                    msg = json.loads(raw)
                    if msg[0] == "EVENT":
                        evt = msg[2]
                        # Skip our own posts
                        if evt["pubkey"] != my_pubkey:
                            events[evt["id"]] = evt
                    elif msg[0] == "EOSE":
                        eose_count += 1
                except websocket.WebSocketTimeoutException:
                    break
                except Exception:
                    break

            ws.send(json.dumps(["CLOSE", "mentions"]))
            ws.send(json.dumps(["CLOSE", "reactions"]))
            ws.close()
        except Exception:
            continue

    return my_pubkey, events


def ingest_engagement(events: dict, dry_run: bool = False) -> int:
    """Insert new engagement events into activity_feed."""
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    ingested = 0

    for eid, evt in events.items():
        # Check if we already ingested this event
        existing = db.execute(
            "SELECT 1 FROM activity_feed WHERE content LIKE ? LIMIT 1",
            (f"%{eid[:16]}%",)
        ).fetchone()
        if existing:
            continue

        pubkey_short = evt["pubkey"][:12]
        ts = evt["created_at"]
        ts_str = time.strftime("%Y-%m-%d %H:%M", time.localtime(ts))

        if evt["kind"] == 7:
            # Reaction
            reaction_content = evt.get("content", "+")
            # Find which post they reacted to
            parent_id = None
            for tag in evt.get("tags", []):
                if tag[0] == "e":
                    parent_id = tag[1][:16]
                    break
            content = f"[Nostr reaction] {reaction_content} from {pubkey_short}... on post {parent_id or 'unknown'} at {ts_str} (event:{eid[:16]})"
            activity_type = "reaction"
        elif evt["kind"] == 1:
            # Reply
            parent_id = None
            for tag in evt.get("tags", []):
                if tag[0] == "e":
                    parent_id = tag[1][:16]
                    break
            reply_text = evt.get("content", "")[:500]
            content = f"[Nostr reply] from {pubkey_short}... on post {parent_id or 'unknown'} at {ts_str}: {reply_text} (event:{eid[:16]})"
            activity_type = "reply"
        else:
            continue

        if dry_run:
            print(f"  Would ingest: {content[:120]}...")
            ingested += 1
            continue

        db.execute(
            "INSERT INTO activity_feed (source, activity_type, content, created_at) VALUES (?, ?, ?, ?)",
            ("nostr:engagement", activity_type, content, ts)
        )
        ingested += 1

    if not dry_run:
        db.commit()
    db.close()
    return ingested


def main():
    dry_run = "--dry-run" in sys.argv

    if not NOSTR_NSEC:
        print("ERROR: NOSTR_NSEC not set")
        sys.exit(1)

    # On first run (no prior ingestion), look back further
    db = sqlite3.connect(DB_PATH)
    has_prior = db.execute(
        "SELECT 1 FROM activity_feed WHERE source='nostr:engagement' LIMIT 1"
    ).fetchone()
    db.close()
    hours = LOOKBACK_HOURS if has_prior else 168  # 7 days on first run

    print(f"Checking Nostr engagement (last {hours}h)...")
    my_pubkey, events = fetch_engagement(NOSTR_NSEC, since_hours=hours)
    print(f"Found {len(events)} engagement events from others")

    if not events:
        print("No new engagement.")
        return

    ingested = ingest_engagement(events, dry_run=dry_run)
    prefix = "Would ingest" if dry_run else "Ingested"
    print(f"{prefix} {ingested} new engagement events into activity_feed")


if __name__ == "__main__":
    main()
