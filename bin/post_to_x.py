#!/usr/bin/env python3
"""
Minimal X v2 post helper. OAuth1 user-context auth.

Usage:
    python3 post_to_x.py "text to post"
    echo "text" | python3 post_to_x.py -

Creds: X_API_KEY, X_API_KEY_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET
       loaded from /home/nate-agx/chronicle/chronicle.env.
"""
import json
import os
import sys
from pathlib import Path

import requests
from requests_oauthlib import OAuth1

ENV = Path("/home/nate-agx/chronicle/chronicle.env")


def load_env():
    creds = {}
    for line in ENV.read_text().splitlines():
        if "=" not in line or line.strip().startswith("#"):
            continue
        k, _, v = line.partition("=")
        creds[k.strip()] = v.strip().strip('"').strip("'")
    return creds


def post(text):
    creds = load_env()
    auth = OAuth1(
        creds["X_API_KEY"],
        creds["X_API_KEY_SECRET"],
        creds["X_ACCESS_TOKEN"],
        creds["X_ACCESS_TOKEN_SECRET"],
    )
    r = requests.post(
        "https://api.twitter.com/2/tweets",
        json={"text": text},
        auth=auth,
        timeout=15,
    )
    return r.status_code, r.text


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: post_to_x.py <text>|-")
        sys.exit(2)
    text = sys.stdin.read().strip() if sys.argv[1] == "-" else sys.argv[1]
    if not text or len(text) > 4000:
        print(f"bad length: {len(text)}")
        sys.exit(2)
    code, body = post(text)
    print(f"HTTP {code}")
    try:
        d = json.loads(body)
        if "data" in d:
            tid = d["data"]["id"]
            print(f"posted: https://x.com/i/web/status/{tid}")
        else:
            print(body[:500])
    except Exception:
        print(body[:500])
    sys.exit(0 if code in (200, 201) else 1)
