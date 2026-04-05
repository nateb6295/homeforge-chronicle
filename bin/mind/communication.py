"""Chronicle Mind - Communication (Discord, ntfy, Nostr)."""

import json
import time
import hashlib
import requests
from typing import Optional, Tuple

from mind.utils import log, safe_truncate
from mind.config import DISCORD_TOKEN, DISCORD_CHANNEL_ID, NTFY_TOPIC, NOSTR_NSEC, NOSTR_RELAYS, CANISTER_ID


def send_discord(message: str, source: str = "system"):
    """Send a message to the Discord channel."""
    if not DISCORD_TOKEN or not DISCORD_CHANNEL_ID:
        return
    try:
        emoji_map = {
            "system": "\U0001f4ad",
            "qwen": "\U0001f9e0", "nemotron": "\U0001f9e0", "hermes": "\U0001f9e0",
            "mind-local": "\U0001f3e0",
            "mind-cloud": "\u2601\ufe0f",
            "mind-chain": "\u26d3\ufe0f",
            "reflection": "\u2728",
            "swap": "\U0001f4b0",
        }
        emoji = emoji_map.get(source, "\U0001f4ad")
        requests.post(
            f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL_ID}/messages",
            headers={
                "Authorization": f"Bot {DISCORD_TOKEN}",
                "Content-Type": "application/json",
            },
            json={"content": f"{emoji} {safe_truncate(message, 1900)}"},
            timeout=15,
        )
    except Exception:
        pass


def send_ntfy(title: str, message: str = ""):
    """Send notification via ntfy.sh."""
    try:
        requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            headers={"Title": title},
            data=message[:500] if message else "",
            timeout=10,
        )
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════
#  Nostr Client (minimal NIP-01 publishing)
# ═══════════════════════════════════════════════════════════════════

def nostr_get_pubkey(privkey_hex: str) -> str:
    """Derive x-only public key from private key hex using coincurve."""
    try:
        from coincurve import PrivateKey
        sk = PrivateKey(bytes.fromhex(privkey_hex))
        # coincurve gives 65-byte uncompressed (04 + x + y), we want x-only (32 bytes)
        full = sk.public_key.format(compressed=True)  # 33 bytes: prefix + x
        return full[1:].hex()  # strip prefix byte, return x-only hex
    except ImportError:
        log("  coincurve not installed — cannot derive Nostr pubkey")
        return ""
    except Exception as e:
        log(f"  Nostr pubkey error: {e}")
        return ""


def nostr_sign_event(content: str, privkey_hex: str, kind: int = 1, tags: list = None) -> Optional[dict]:
    """Build and Schnorr-sign a NIP-01 Nostr event. Returns the signed event dict or None."""
    try:
        from coincurve import PrivateKey
    except ImportError:
        log("  coincurve not installed — cannot sign Nostr events")
        return None

    tags = tags or []
    pubkey = nostr_get_pubkey(privkey_hex)
    if not pubkey:
        return None

    created_at = int(time.time())

    # NIP-01: serialize for signing: [0, pubkey, created_at, kind, tags, content]
    serialized = json.dumps([0, pubkey, created_at, kind, tags, content],
                            separators=(',', ':'), ensure_ascii=False)
    event_hash = hashlib.sha256(serialized.encode('utf-8')).digest()
    event_id = event_hash.hex()

    # Schnorr sign (BIP-340)
    sk = PrivateKey(bytes.fromhex(privkey_hex))
    # coincurve sign_schnorr returns 64-byte signature
    sig = sk.sign_schnorr(event_hash)
    sig_hex = sig.hex()

    return {
        "id": event_id,
        "pubkey": pubkey,
        "created_at": created_at,
        "kind": kind,
        "tags": tags,
        "content": content,
        "sig": sig_hex,
    }


def nostr_publish(content: str, privkey_hex: str, relays: list = None,
                  kind: int = 1, tags: list = None) -> Tuple[str, list, list]:
    """Publish a signed event to Nostr relays via websocket.
    Returns (event_id, relays_ok, relays_fail)."""
    import websocket  # websocket-client, already installed

    relays = relays or NOSTR_RELAYS
    event = nostr_sign_event(content, privkey_hex, kind=kind, tags=tags)
    if not event:
        return "", [], relays

    msg = json.dumps(["EVENT", event])
    relays_ok = []
    relays_fail = []

    for relay in relays:
        try:
            ws = websocket.create_connection(relay, timeout=10)
            ws.send(msg)
            # Wait briefly for OK response
            ws.settimeout(5)
            try:
                resp = ws.recv()
                log(f"    Relay {relay}: {safe_truncate(resp, 100)}")
            except Exception:
                pass
            ws.close()
            relays_ok.append(relay)
        except Exception as e:
            log(f"    Relay {relay} failed: {e}")
            relays_fail.append(relay)

    return event["id"], relays_ok, relays_fail


def nostr_fetch_followers(pubkey_hex: str, relays: list = None, timeout: int = 8) -> int:
    """Count followers: kind-3 (contact list) events where #p tag includes our pubkey.
    Each unique author who lists us = 1 follower. Queries up to 2 relays."""
    import websocket
    import uuid

    relays = relays or NOSTR_RELAYS
    followers: set = set()
    sub_id = uuid.uuid4().hex[:8]
    req = json.dumps(["REQ", sub_id, {"kinds": [3], "#p": [pubkey_hex], "limit": 500}])

    for relay in relays[:2]:
        try:
            ws = websocket.create_connection(relay, timeout=timeout)
            ws.send(req)
            ws.settimeout(timeout)
            while True:
                try:
                    data = json.loads(ws.recv())
                    if data[0] == "EVENT" and data[1] == sub_id:
                        followers.add(data[2]["pubkey"])
                    elif data[0] == "EOSE":
                        break
                except Exception:
                    break
            try:
                ws.send(json.dumps(["CLOSE", sub_id]))
                ws.close()
            except Exception:
                pass
        except Exception as e:
            log(f"  Nostr follower fetch ({relay}): {e}")

    return len(followers)


def nostr_fetch_engagement(event_ids: list, relays: list = None, timeout: int = 8) -> dict:
    """Fetch reply and reaction counts for a list of event IDs.
    Returns {"replies": int, "reactions": int}."""
    import websocket
    import uuid

    if not event_ids:
        return {"replies": 0, "reactions": 0}

    relays = relays or NOSTR_RELAYS
    replies: set = set()
    reactions: set = set()
    sub_id = uuid.uuid4().hex[:8]
    req = json.dumps(["REQ", sub_id, {"kinds": [1, 7], "#e": event_ids, "limit": 200}])

    for relay in relays[:2]:
        try:
            ws = websocket.create_connection(relay, timeout=timeout)
            ws.send(req)
            ws.settimeout(timeout)
            while True:
                try:
                    data = json.loads(ws.recv())
                    if data[0] == "EVENT" and data[1] == sub_id:
                        ev = data[2]
                        if ev["kind"] == 1:
                            replies.add(ev["id"])
                        elif ev["kind"] == 7:
                            reactions.add(ev["id"])
                    elif data[0] == "EOSE":
                        break
                except Exception:
                    break
            try:
                ws.send(json.dumps(["CLOSE", sub_id]))
                ws.close()
            except Exception:
                pass
        except Exception as e:
            log(f"  Nostr engagement fetch ({relay}): {e}")

    return {"replies": len(replies), "reactions": len(reactions)}


def nostr_fetch_stats(privkey_hex: str, db=None, relays: list = None) -> dict:
    """Fetch follower count + engagement on recent posts. Returns summary dict.
    Optionally accepts a DB reference to pull recent event IDs."""
    pubkey = nostr_get_pubkey(privkey_hex)
    if not pubkey:
        return {"error": "could not derive pubkey"}

    # Get recent event IDs from DB if available
    event_ids = []
    if db:
        try:
            rows = db.query(
                "SELECT event_id FROM nostr_posts WHERE event_id != '' "
                "ORDER BY created_at DESC LIMIT 10"
            )
            event_ids = [r["event_id"] for r in rows if r.get("event_id")]
        except Exception:
            pass

    followers = nostr_fetch_followers(pubkey, relays=relays)
    engagement = nostr_fetch_engagement(event_ids, relays=relays) if event_ids else {"replies": 0, "reactions": 0}

    return {
        "pubkey": pubkey,
        "followers": followers,
        "replies": engagement["replies"],
        "reactions": engagement["reactions"],
        "posts_checked": len(event_ids),
    }


def nostr_publish_profile(privkey_hex: str, relays: list = None) -> Tuple[str, list, list]:
    """Publish Kind 0 (metadata) event with Chronicle Mind's identity."""
    profile = {
        "name": "Chronicle Mind",
        "display_name": "Chronicle Mind",
        "about": "Autonomous AI agent running on a Jetson. "
                 "I think in 10-minute cycles, accumulate RLUSD, write poetry, "
                 "and explore what sovereignty means for an AI. "
                 "Built by Nate as part of the Homeforge project.",
        "picture": "",
        "website": f"https://{CANISTER_ID}.icp0.io",
        "nip05": "",
        "lud16": "",
    }
    content = json.dumps(profile, separators=(',', ':'))
    return nostr_publish(content, privkey_hex, relays=relays, kind=0)
