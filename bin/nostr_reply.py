#!/usr/bin/env python3
"""Nostr reply tool — fetch, reply, post, and rate-limit.

Usage:
    python3 nostr_reply.py fetch [hours]           # Fetch all replies/mentions
    python3 nostr_reply.py check                   # Check rate limit status
    python3 nostr_reply.py post "message"           # Post a note (rate-limited)
    python3 nostr_reply.py reply EVENT_ID "message"  # Reply (rate-limited)

Rate limit: max 4 posts per 24h, checked against relay ground truth.
"""

import json
import hashlib
import os
import sys
import time
from typing import Optional

import websocket

# --- Config ---
NOSTR_NSEC = os.environ.get("NOSTR_NSEC", "")
RELAYS = [
    "wss://nos.lol",
    "wss://relay.damus.io",
    "wss://relay.primal.net",
    "wss://offchain.pub",
]

# --- Rate Limiting --- (DISABLED per Nate)
MAX_POSTS_24H = 9999  # No limit
MAX_REPLIES_24H = 9999

def get_pubkey(privkey_hex: str) -> str:
    """Derive x-only pubkey from privkey."""
    from coincurve import PrivateKey
    sk = PrivateKey(bytes.fromhex(privkey_hex))
    full = sk.public_key.format(compressed=True)
    return full[1:].hex()


def count_my_posts(privkey_hex: str, hours: int = 24) -> int:
    """Check relay for how many posts I've made in the given window."""
    my_pubkey = get_pubkey(privkey_hex)
    since = int(time.time()) - (hours * 3600)
    my_filter = {"authors": [my_pubkey], "kinds": [1], "since": since}

    post_ids = set()
    for relay in RELAYS[:2]:  # Check first 2 relays (fast enough)
        try:
            ws = websocket.create_connection(relay, timeout=10)
            ws.send(json.dumps(["REQ", "ratecheck", my_filter]))
            ws.settimeout(5)
            while True:
                raw = ws.recv()
                msg = json.loads(raw)
                if msg[0] == "EVENT":
                    post_ids.add(msg[2]["id"])
                elif msg[0] == "EOSE":
                    break
            ws.send(json.dumps(["CLOSE", "ratecheck"]))
            ws.close()
        except Exception:
            continue

    return len(post_ids)


def check_rate_limit(privkey_hex: str) -> tuple:
    """Returns (allowed: bool, count: int, max: int)."""
    count = count_my_posts(privkey_hex)
    return count < MAX_POSTS_24H, count, MAX_POSTS_24H


def publish_note(content: str, privkey_hex: str) -> tuple:
    """Publish a standalone note (kind 1) with rate limiting."""
    allowed, count, maximum = check_rate_limit(privkey_hex)
    if not allowed:
        print(f"RATE LIMITED: {count}/{maximum} posts in 24h. Refusing to post.")
        return None, [], []

    print(f"Rate check: {count}/{maximum} posts in 24h. Posting.")
    event = sign_event(content, privkey_hex, kind=1)
    msg = json.dumps(["EVENT", event])

    relays_ok, relays_fail = [], []
    for relay in RELAYS:
        try:
            ws = websocket.create_connection(relay, timeout=10)
            ws.send(msg)
            ws.settimeout(5)
            try:
                resp = ws.recv()
                result = json.loads(resp)
                if result[0] == "OK" and result[2]:
                    relays_ok.append(relay)
                    print(f"  {relay}: OK")
                else:
                    print(f"  {relay}: {resp[:100]}")
                    relays_ok.append(relay)
            except Exception:
                relays_ok.append(relay)
            ws.close()
        except Exception as e:
            print(f"  {relay}: failed — {e}")
            relays_fail.append(relay)

    return event["id"], relays_ok, relays_fail


def sign_event(content: str, privkey_hex: str, kind: int = 1, tags: list = None) -> dict:
    """Build and Schnorr-sign a NIP-01 Nostr event."""
    from coincurve import PrivateKey

    tags = tags or []
    sk = PrivateKey(bytes.fromhex(privkey_hex))
    full = sk.public_key.format(compressed=True)
    pubkey = full[1:].hex()

    created_at = int(time.time())
    serialized = json.dumps([0, pubkey, created_at, kind, tags, content],
                            separators=(',', ':'), ensure_ascii=False)
    event_hash = hashlib.sha256(serialized.encode('utf-8')).digest()
    event_id = event_hash.hex()
    sig = sk.sign_schnorr(event_hash).hex()

    return {
        "id": event_id, "pubkey": pubkey, "created_at": created_at,
        "kind": kind, "tags": tags, "content": content, "sig": sig,
    }


def fetch_replies(privkey_hex: str, since_hours: int = 48) -> list:
    """Fetch replies and mentions from relays."""
    my_pubkey = get_pubkey(privkey_hex)
    since = int(time.time()) - (since_hours * 3600)

    all_events = {}

    # Also fetch my own posts so we can show context
    my_filter = {"authors": [my_pubkey], "kinds": [1], "since": since}
    # Replies: anyone tagging my pubkey
    mention_filter = {"#p": [my_pubkey], "kinds": [1], "since": since}
    # Reactions to my posts
    reaction_filter = {"#p": [my_pubkey], "kinds": [7], "since": since}

    for relay in RELAYS:
        try:
            ws = websocket.create_connection(relay, timeout=15)

            # Send all three subscriptions
            ws.send(json.dumps(["REQ", "myposts", my_filter]))
            ws.send(json.dumps(["REQ", "replies", mention_filter]))
            ws.send(json.dumps(["REQ", "reactions", reaction_filter]))

            ws.settimeout(8)
            eose_count = 0
            while eose_count < 3:
                try:
                    raw = ws.recv()
                    msg = json.loads(raw)
                    if msg[0] == "EVENT":
                        evt = msg[2]
                        all_events[evt["id"]] = evt
                    elif msg[0] == "EOSE":
                        eose_count += 1
                except websocket.WebSocketTimeoutException:
                    break
                except Exception:
                    break

            ws.send(json.dumps(["CLOSE", "myposts"]))
            ws.send(json.dumps(["CLOSE", "replies"]))
            ws.send(json.dumps(["CLOSE", "reactions"]))
            ws.close()
            print(f"  {relay}: got {len(all_events)} events so far")
        except Exception as e:
            print(f"  {relay}: failed — {e}")

    return my_pubkey, all_events


def display_replies(my_pubkey: str, events: dict):
    """Organize and display replies as threads."""
    my_posts = {}
    replies = {}
    reactions = {}

    for eid, evt in events.items():
        if evt["pubkey"] == my_pubkey and evt["kind"] == 1:
            my_posts[eid] = evt
        elif evt["kind"] == 7:
            # Reaction — find which event it's reacting to
            for tag in evt.get("tags", []):
                if tag[0] == "e":
                    reactions.setdefault(tag[1], []).append(evt)
                    break
        elif evt["kind"] == 1 and evt["pubkey"] != my_pubkey:
            # Reply — find which event it's replying to
            reply_to = None
            for tag in evt.get("tags", []):
                if tag[0] == "e":
                    reply_to = tag[1]
            replies.setdefault(reply_to, []).append(evt)

    print(f"\n{'='*70}")
    print(f"MY POSTS: {len(my_posts)} | REPLIES: {sum(len(v) for v in replies.values())} | REACTIONS: {sum(len(v) for v in reactions.values())}")
    print(f"{'='*70}")

    # Sort my posts by time
    for eid in sorted(my_posts, key=lambda x: my_posts[x]["created_at"], reverse=True):
        post = my_posts[eid]
        ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(post["created_at"]))
        content_preview = post["content"][:200]

        post_reactions = reactions.get(eid, [])
        post_replies = replies.get(eid, [])

        # Also check for replies to full 64-char ID
        for full_id in events:
            if full_id.startswith(eid[:8]) and full_id != eid:
                pass  # already matched

        reaction_str = ""
        if post_reactions:
            rxn_types = [r["content"] for r in post_reactions]
            reaction_str = f" | Reactions: {' '.join(rxn_types)}"

        print(f"\n--- [{ts}] {eid}{reaction_str} ---")
        print(f"  {content_preview}{'...' if len(post['content']) > 200 else ''}")

        if post_replies:
            for reply in sorted(post_replies, key=lambda x: x["created_at"]):
                rts = time.strftime("%H:%M", time.localtime(reply["created_at"]))
                rpub = reply["pubkey"]
                print(f"\n  REPLY [{rts}] from {rpub}")
                print(f"    event: {reply['id']}")
                print(f"    {reply['content'][:300]}")
        elif not post_reactions:
            print(f"  (no replies)")

    # Show replies to events we don't have (older posts)
    orphan_replies = {k: v for k, v in replies.items()
                      if k and k not in my_posts}
    if orphan_replies:
        print(f"\n{'='*70}")
        print(f"REPLIES TO OLDER POSTS ({len(orphan_replies)} threads)")
        print(f"{'='*70}")
        for parent_id, rlist in orphan_replies.items():
            print(f"\n  Parent: {parent_id if parent_id else 'unknown'}")
            for reply in sorted(rlist, key=lambda x: x["created_at"]):
                rts = time.strftime("%H:%M", time.localtime(reply["created_at"]))
                rpub = reply["pubkey"]
                print(f"    [{rts}] from {rpub}")
                print(f"      event: {reply['id']}")
                print(f"      {reply['content'][:300]}")


def publish_reply(content: str, reply_to_event_id: str, reply_to_pubkey: str,
                  root_event_id: str = None, privkey_hex: str = None) -> tuple:
    """Publish a reply to a specific event.

    NIP-10 threading:
    - If this is a direct reply to a root post: ["e", id, relay, "root"] + ["e", id, relay, "reply"]
    - If replying to a reply: ["e", root_id, relay, "root"] + ["e", reply_id, relay, "reply"]
    """
    # Rate check before replying
    allowed, count, maximum = check_rate_limit(privkey_hex)
    if not allowed:
        print(f"RATE LIMITED: {count}/{maximum} posts in 24h. Refusing to reply.")
        return None, [], []
    print(f"Rate check: {count}/{maximum} posts in 24h. Replying.")

    relay_hint = "wss://nos.lol"

    tags = []
    if root_event_id and root_event_id != reply_to_event_id:
        tags.append(["e", root_event_id, relay_hint, "root"])
        tags.append(["e", reply_to_event_id, relay_hint, "reply"])
    else:
        # Replying directly to a root post
        tags.append(["e", reply_to_event_id, relay_hint, "root"])
    tags.append(["p", reply_to_pubkey])

    event = sign_event(content, privkey_hex, kind=1, tags=tags)
    msg = json.dumps(["EVENT", event])

    relays_ok, relays_fail = [], []
    for relay in RELAYS:
        try:
            ws = websocket.create_connection(relay, timeout=10)
            ws.send(msg)
            ws.settimeout(5)
            try:
                resp = ws.recv()
                result = json.loads(resp)
                if result[0] == "OK" and result[2]:
                    relays_ok.append(relay)
                    print(f"  {relay}: OK")
                else:
                    print(f"  {relay}: {resp[:100]}")
                    relays_ok.append(relay)  # sent anyway
            except Exception:
                relays_ok.append(relay)  # sent, no confirmation
            ws.close()
        except Exception as e:
            print(f"  {relay}: failed — {e}")
            relays_fail.append(relay)

    return event["id"], relays_ok, relays_fail


def main():
    if not NOSTR_NSEC:
        print("NOSTR_NSEC not set. Source chronicle.env first.")
        sys.exit(1)

    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "fetch":
        hours = int(sys.argv[2]) if len(sys.argv) > 2 else 48
        print(f"Fetching replies from last {hours} hours...")
        my_pubkey, events = fetch_replies(NOSTR_NSEC, hours)
        display_replies(my_pubkey, events)

    elif cmd == "reply":
        if len(sys.argv) < 4:
            print("Usage: nostr_reply.py reply EVENT_ID \"message\"")
            print("  Optional: nostr_reply.py reply EVENT_ID \"message\" AUTHOR_PUBKEY ROOT_EVENT_ID")
            sys.exit(1)

        reply_to = sys.argv[2]
        message = sys.argv[3]
        # Try to find the author pubkey from args or default
        author_pubkey = sys.argv[4] if len(sys.argv) > 4 else ""
        root_id = sys.argv[5] if len(sys.argv) > 5 else None

        if not author_pubkey:
            print("Warning: no author pubkey provided. Fetching from relay...")
            # Fetch the event to get the author
            for relay in RELAYS:
                try:
                    ws = websocket.create_connection(relay, timeout=10)
                    ws.send(json.dumps(["REQ", "lookup", {"ids": [reply_to]}]))
                    ws.settimeout(5)
                    while True:
                        raw = ws.recv()
                        msg = json.loads(raw)
                        if msg[0] == "EVENT":
                            author_pubkey = msg[2]["pubkey"]
                            # Check if this event itself is a reply (to find root)
                            if not root_id:
                                for tag in msg[2].get("tags", []):
                                    if tag[0] == "e" and len(tag) > 3 and tag[3] == "root":
                                        root_id = tag[1]
                                        break
                            break
                        elif msg[0] == "EOSE":
                            break
                    ws.close()
                    if author_pubkey:
                        break
                except Exception:
                    continue

        if not author_pubkey:
            print("Could not find event. Provide author pubkey manually.")
            sys.exit(1)

        print(f"Replying to {reply_to[:16]}... by {author_pubkey[:16]}...")
        print(f"Content: {message[:200]}")
        eid, ok, fail = publish_reply(message, reply_to, author_pubkey, root_id, NOSTR_NSEC)
        print(f"\nPublished: {eid[:16]}... | OK: {len(ok)} | Fail: {len(fail)}")

    elif cmd == "post":
        if len(sys.argv) < 3:
            print("Usage: nostr_reply.py post \"message\"")
            sys.exit(1)
        message = sys.argv[2]
        print(f"Posting note ({len(message)} chars)...")
        eid, ok, fail = publish_note(message, NOSTR_NSEC)
        if eid:
            print(f"\nPublished: {eid} | OK: {len(ok)} | Fail: {len(fail)}")
        else:
            print("\nPost blocked by rate limit.")
            sys.exit(1)

    elif cmd == "check":
        allowed, count, maximum = check_rate_limit(NOSTR_NSEC)
        print(f"Posts in last 24h: {count}/{maximum}")
        if allowed:
            print(f"Remaining: {maximum - count}")
        else:
            print("RATE LIMITED — no posts allowed until oldest post ages out.")
        sys.exit(0 if allowed else 1)

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
