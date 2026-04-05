#!/usr/bin/env python3
"""Shared canister storage — POST capsules to ICP from any helper script.

Usage as module:
    from canister_store import store_on_chain
    store_on_chain("content", topic="threads/completion", keywords=["thread", "sovereignty"])

Usage as CLI:
    canister_store.py "content" [--topic TOPIC] [--keywords k1,k2,k3]
"""

import json
import urllib.request
import urllib.error
import sys
import os
import logging

CANISTER_URL = os.environ.get(
    "CANISTER_URL",
    "https://fqqku-bqaaa-aaaai-q4wha-cai.raw.icp0.io"
)

log = logging.getLogger("canister_store")


def store_on_chain(content, topic=None, keywords=None, persons=None):
    """POST a capsule to the ICP canister. Returns True on success."""
    payload = {"content": content}
    if topic:
        payload["topic"] = topic
    if keywords:
        payload["keywords"] = keywords
    if persons:
        payload["persons"] = persons

    try:
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            f"{CANISTER_URL}/api/feed",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            status = resp.status
            if status in (200, 201):
                return True
            else:
                log.warning(f"Canister returned {status}")
                return False
    except Exception as e:
        log.warning(f"Canister store failed: {e}")
        return False


def main():
    if len(sys.argv) < 2:
        print("Usage: canister_store.py \"content\" [--topic TOPIC] [--keywords k1,k2,k3]")
        sys.exit(1)

    content = sys.argv[1]
    topic = None
    keywords = None

    args = sys.argv[2:]
    i = 0
    while i < len(args):
        if args[i] == "--topic" and i + 1 < len(args):
            topic = args[i + 1]; i += 2
        elif args[i] == "--keywords" and i + 1 < len(args):
            keywords = args[i + 1].split(","); i += 2
        else:
            i += 1

    ok = store_on_chain(content, topic=topic, keywords=keywords)
    if ok:
        print("Stored on-chain")
    else:
        print("Failed to store")
        sys.exit(1)


if __name__ == "__main__":
    main()
