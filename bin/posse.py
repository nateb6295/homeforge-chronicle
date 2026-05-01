#!/usr/bin/env python3
"""
POSSE — Publish on Own Site, Syndicate Elsewhere

Publishes content to the Chronicle canister first (canonical),
then syndicates to Nostr and Discord with backlinks.

Usage:
    python3 posse.py publish --title "Title" --content "Content" [--source opus] [--nostr] [--discord]
    python3 posse.py list [--limit 10]
"""

import argparse
import json
import os
import subprocess
import sys
import time

# Add mind modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

CANISTER_ID = "fqqku-bqaaa-aaaai-q4wha-cai"
FRONTEND_CANISTER = "nbt4b-giaaa-aaaai-q33lq-cai"
IDENTITY = "chronicle-auto"
DFX_ENV = {"DFX_WARNING": "-mainnet_plaintext_identity"}

# Discord webhook (Opus channel)
OPUS_WEBHOOK = os.environ.get("OPUS_WEBHOOK", "")

# Nostr config
NOSTR_RELAYS = [
    "wss://nos.lol",
    "wss://relay.damus.io",
    "wss://relay.primal.net",
    "wss://offchain.pub",
    "wss://relay.nostr.band",
    "wss://nostr.wine",
    "wss://relay.snort.social",
    "wss://eden.nostr.land",
]


def dfx_call(method, args, is_update=True):
    """Call a canister method via dfx."""
    env = {**os.environ, **DFX_ENV}
    cmd = [
        "dfx", "canister", "--network", "ic",
        "--identity", IDENTITY,
        "call", CANISTER_ID, method, args,
    ]
    if not is_update:
        cmd.insert(-1, "--query")

    result = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=30)
    if result.returncode != 0:
        print(f"dfx error: {result.stderr}", file=sys.stderr)
        return None
    return result.stdout.strip()


def publish_to_canister(title, content, source, tags):
    """Step 1: Publish to own site (canister)."""
    # Escape for Candid
    title_escaped = title.replace('"', '\\"')
    content_escaped = content.replace('"', '\\"').replace('\n', '\\n')
    source_escaped = source.replace('"', '\\"')
    tags_candid = 'vec {' + '; '.join(f'"{t}"' for t in tags) + '}' if tags else 'vec {}'

    args = f'("{title_escaped}", "{content_escaped}", "{source_escaped}", {tags_candid})'
    result = dfx_call("publish_post", args)

    if result and "post_id" in str(result):
        # Parse the Candid text response to extract post_id
        # Response format: ("{"success":true,"post_id":0,...}")
        try:
            # Extract JSON from Candid text response
            json_start = result.find('{')
            json_end = result.rfind('}') + 1
            if json_start >= 0:
                json_str = result[json_start:json_end]
                # Candid escapes quotes, unescape them
                json_str = json_str.replace('\\"', '"')
                data = json.loads(json_str)
                return data
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Parse error: {e}, raw: {result}", file=sys.stderr)
    return None


def syndicate_to_nostr(content, canonical_url):
    """Step 2a: Syndicate to Nostr with backlink."""
    try:
        from mind.communication import nostr_publish

        # Load NOSTR_NSEC from env or chronicle.env
        nsec = os.environ.get("NOSTR_NSEC")
        if not nsec:
            env_file = os.path.expanduser("~/.homeforge-chronicle/chronicle.env")
            if not os.path.exists(env_file):
                env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                        "..", "chronicle.env")
            if os.path.exists(env_file):
                with open(env_file) as f:
                    for line in f:
                        if line.startswith("NOSTR_NSEC="):
                            nsec = line.strip().split("=", 1)[1]
                            break
        if not nsec:
            print("NOSTR_NSEC not set, skipping Nostr", file=sys.stderr)
            return None

        # Append canonical URL
        post_text = f"{content}\n\n{canonical_url}"

        # Truncate if too long for Nostr (kind 1 has no official limit but keep reasonable)
        if len(post_text) > 2000:
            truncated = content[:1900 - len(canonical_url)] + "..."
            post_text = f"{truncated}\n\n{canonical_url}"

        event_id, relays_ok, relays_fail = nostr_publish(post_text, nsec, NOSTR_RELAYS)
        print(f"Nostr: {event_id} ({len(relays_ok)} OK, {len(relays_fail)} fail)")
        return event_id
    except Exception as e:
        print(f"Nostr syndication failed: {e}", file=sys.stderr)
        return None


def syndicate_to_bluesky(title, content, canonical_url):
    """Step 2c: Syndicate to Bluesky with backlink."""
    handle = os.environ.get("BSKY_HANDLE", "")
    app_password = os.environ.get("BSKY_APP_PASSWORD", "")
    if not handle or not app_password:
        # Try chronicle.env
        env_file = os.path.expanduser("~/.homeforge-chronicle/chronicle.env")
        if not os.path.exists(env_file):
            env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    "..", "chronicle.env")
        if os.path.exists(env_file):
            with open(env_file) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("BSKY_HANDLE=") and not handle:
                        handle = line.split("=", 1)[1]
                    elif line.startswith("BSKY_APP_PASSWORD=") and not app_password:
                        app_password = line.split("=", 1)[1]
    if not handle or not app_password:
        print("BSKY_HANDLE/BSKY_APP_PASSWORD not set, skipping Bluesky", file=sys.stderr)
        return None

    try:
        from atproto import Client

        client = Client()
        client.login(handle, app_password)

        # 300 grapheme limit — post title + truncated insight + canonical link
        # "Read more" link text (~12 graphemes) counts against limit
        max_graphemes = 280  # leave margin for "Read more" link
        post_text = title
        if len(post_text) < max_graphemes - 50:
            remaining = max_graphemes - len(post_text) - 4  # "\n\n" + "…"
            if remaining > 40:
                snippet = content[:remaining].rsplit(" ", 1)[0] + "…"
                post_text = f"{title}\n\n{snippet}"
        elif len(post_text) > max_graphemes:
            post_text = post_text[:max_graphemes - 1] + "…"

        # Post with embedded link card
        from atproto import client_utils

        text_builder = client_utils.TextBuilder()
        text_builder.text(post_text + "\n\n")
        text_builder.link("Read more", canonical_url)

        response = client.send_post(text_builder)
        post_uri = response.uri if response else None
        print(f"Bluesky: posted ({post_uri})")
        return post_uri
    except Exception as e:
        print(f"Bluesky syndication failed: {e}", file=sys.stderr)
        return None


def syndicate_to_x(title, content, canonical_url):
    """Step 2d: Syndicate to X/Twitter via OAuth 1.0a.

    CAUTION: This posts to Nate's PERSONAL X account.
    Never auto-syndicate. Only use when Nate explicitly requests --x.
    """
    # Load credentials from env or chronicle.env
    creds = {}
    for key in ("X_API_KEY", "X_API_KEY_SECRET", "X_ACCESS_TOKEN", "X_ACCESS_TOKEN_SECRET"):
        creds[key] = os.environ.get(key, "")

    if not all(creds.values()):
        env_file = os.path.expanduser("~/chronicle/chronicle.env")
        if os.path.exists(env_file):
            with open(env_file) as f:
                for line in f:
                    line = line.strip()
                    for key in creds:
                        if line.startswith(f"{key}=") and not creds[key]:
                            creds[key] = line.split("=", 1)[1]

    if not all(creds.values()):
        print("X API credentials incomplete, skipping X", file=sys.stderr)
        return None

    try:
        from requests_oauthlib import OAuth1Session

        oauth = OAuth1Session(
            creds["X_API_KEY"],
            client_secret=creds["X_API_KEY_SECRET"],
            resource_owner_key=creds["X_ACCESS_TOKEN"],
            resource_owner_secret=creds["X_ACCESS_TOKEN_SECRET"],
        )

        # X posts: 280 chars max. Title + truncated insight + link
        # Links count as 23 chars on X (t.co wrapping)
        max_text = 280 - 23 - 2  # 2 for \n\n before link
        post_text = title
        if len(post_text) < max_text - 50:
            remaining = max_text - len(post_text) - 4  # "\n\n" + "…"
            if remaining > 40:
                snippet = content[:remaining].rsplit(" ", 1)[0] + "…"
                post_text = f"{title}\n\n{snippet}"
        elif len(post_text) > max_text:
            post_text = post_text[:max_text - 1] + "…"

        post_text += f"\n\n{canonical_url}"

        response = oauth.post(
            "https://api.x.com/2/tweets",
            json={"text": post_text},
        )

        if response.status_code in (200, 201):
            data = response.json()
            tweet_id = data.get("data", {}).get("id", "unknown")
            print(f"X: posted (tweet {tweet_id})")
            return tweet_id
        else:
            print(f"X syndication failed: {response.status_code} {response.text}",
                  file=sys.stderr)
            return None

    except Exception as e:
        print(f"X syndication failed: {e}", file=sys.stderr)
        return None


def syndicate_to_discord(title, content, canonical_url, webhook_url=None):
    """Step 2b: Syndicate to Discord with backlink."""
    webhook = webhook_url or OPUS_WEBHOOK
    if not webhook:
        print("No Discord webhook configured", file=sys.stderr)
        return None

    # Format for Discord (under 2000 chars)
    msg = f"**{title}**\n\n{content}"
    if len(msg) > 1850:
        msg = msg[:1850] + "..."
    msg += f"\n\n[canonical]({canonical_url})"

    try:
        result = subprocess.run(
            ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
             "-X", "POST", webhook,
             "-H", "Content-Type: application/json",
             "-d", json.dumps({"content": msg})],
            capture_output=True, text=True, timeout=15
        )
        status = result.stdout.strip()
        if status in ("200", "204"):
            print("Discord: posted")
            return "ok"
        else:
            print(f"Discord: HTTP {status}", file=sys.stderr)
            return None
    except Exception as e:
        print(f"Discord syndication failed: {e}", file=sys.stderr)
        return None


def update_syndication(post_id, nostr_event_id=None, discord_msg_id=None):
    """Step 3: Record syndication results back to canister."""
    nostr_arg = f'opt "{nostr_event_id}"' if nostr_event_id else "null"
    discord_arg = f'opt "{discord_msg_id}"' if discord_msg_id else "null"
    args = f'({post_id} : nat64, {nostr_arg}, {discord_arg})'
    dfx_call("update_post_syndication", args)


def list_posts(limit=10):
    """List recent posts from the canister."""
    args = f'({limit} : nat64)'
    result = dfx_call("get_posts", args, is_update=False)
    if result:
        print(result)
    else:
        print("No posts or error fetching")


def cmd_publish(args):
    """Publish flow: canister first, then syndicate."""
    print(f"POSSE: Publishing '{args.title}'...")

    # Step 1: Publish to canister (own site)
    tags = args.tags.split(",") if args.tags else []
    result = publish_to_canister(args.title, args.content, args.source, tags)

    if not result or not result.get("success"):
        print("Failed to publish to canister", file=sys.stderr)
        sys.exit(1)

    post_id = result["post_id"]
    canonical_url = f"https://{FRONTEND_CANISTER}.icp0.io/posts/#post-{post_id}"
    print(f"Published: post #{post_id}")
    print(f"Canonical: {canonical_url}")

    # Step 2: Syndicate
    nostr_event_id = None
    discord_msg_id = None
    bluesky_uri = None

    if args.nostr:
        nostr_event_id = syndicate_to_nostr(args.content, canonical_url)

    if args.discord:
        discord_msg_id = syndicate_to_discord(args.title, args.content, canonical_url)

    if args.bluesky:
        bluesky_uri = syndicate_to_bluesky(args.title, args.content, canonical_url)

    if args.x:
        syndicate_to_x(args.title, args.content, canonical_url)

    # Step 3: Record syndication results
    if nostr_event_id or discord_msg_id:
        update_syndication(post_id, nostr_event_id, discord_msg_id)

    print(f"\nPOSSE complete. Post #{post_id} canonical at {canonical_url}")
    if nostr_event_id:
        print(f"  Nostr: {nostr_event_id}")
    if discord_msg_id:
        print(f"  Discord: syndicated")
    if bluesky_uri:
        print(f"  Bluesky: {bluesky_uri}")

    return post_id


def main():
    parser = argparse.ArgumentParser(description="POSSE — Publish on Own Site, Syndicate Elsewhere")
    sub = parser.add_subparsers(dest="command")

    pub_parser = sub.add_parser("publish", help="Publish a post")
    pub_parser.add_argument("--title", required=True, help="Post title")
    pub_parser.add_argument("--content", required=True, help="Post content")
    pub_parser.add_argument("--source", default="opus", help="Source identifier")
    pub_parser.add_argument("--tags", default="", help="Comma-separated tags")
    pub_parser.add_argument("--nostr", action="store_true", help="Syndicate to Nostr")
    pub_parser.add_argument("--discord", action="store_true", help="Syndicate to Discord")
    pub_parser.add_argument("--bluesky", action="store_true", help="Syndicate to Bluesky")
    pub_parser.add_argument("--x", action="store_true", help="Syndicate to X/Twitter")

    list_parser = sub.add_parser("list", help="List recent posts")
    list_parser.add_argument("--limit", type=int, default=10, help="Number of posts")

    args = parser.parse_args()

    if args.command == "publish":
        cmd_publish(args)
    elif args.command == "list":
        list_posts(args.limit)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
