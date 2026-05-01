#!/usr/bin/env python3
"""Read an X thread from a tweet URL or ID.

Uses the X MCP server at 127.0.0.1:8000 to fetch tweet + conversation.

Usage:
  read_x_thread.py https://x.com/user/status/12345
  read_x_thread.py 12345
  read_x_thread.py 12345 --replies    # include replies from others
"""

import json
import re
import sys

import httpx

MCP_URL = "http://127.0.0.1:8000/mcp"
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/event-stream",
}
SESSION_ID = None


def init_session():
    """Initialize MCP session and return session ID."""
    global SESSION_ID
    r = httpx.post(
        MCP_URL,
        headers=HEADERS,
        json={
            "jsonrpc": "2.0",
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "opus-thread-reader", "version": "1.0"},
            },
            "id": 0,
        },
        timeout=15,
    )
    for line in r.text.split("\n"):
        if "mcp-session-id" in r.headers:
            SESSION_ID = r.headers["mcp-session-id"]
            return
    # Try from response headers
    SESSION_ID = r.headers.get("mcp-session-id")


def call_tool(name, arguments, req_id=1):
    """Call an MCP tool and return the parsed result."""
    if not SESSION_ID:
        init_session()

    headers = {**HEADERS, "Mcp-Session-Id": SESSION_ID}
    r = httpx.post(
        MCP_URL,
        headers=headers,
        json={
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
            "id": req_id,
        },
        timeout=30,
    )

    for line in r.text.split("\n"):
        if line.startswith("data:"):
            d = json.loads(line[5:])
            content = d.get("result", {}).get("content", [])
            if content:
                txt = content[0].get("text", "")
                if d.get("result", {}).get("isError"):
                    return {"error": txt}
                try:
                    return json.loads(txt)
                except json.JSONDecodeError:
                    return {"text": txt}
    return {"error": "No response"}


def extract_tweet_id(arg):
    """Extract tweet ID from URL or raw ID."""
    m = re.search(r"/status/(\d+)", arg)
    if m:
        return m.group(1)
    if arg.isdigit():
        return arg
    return None


def get_tweet(tweet_id):
    """Fetch a single tweet with metadata."""
    return call_tool("getPostsById", {
        "id": tweet_id,
        "tweet.fields": [
            "text", "author_id", "conversation_id", "referenced_tweets",
            "created_at", "in_reply_to_user_id", "note_tweet",
        ],
        "expansions": ["author_id", "referenced_tweets.id"],
    })


def get_thread(conversation_id, author_username, include_replies=False):
    """Fetch all tweets in a conversation thread."""
    query = f"conversation_id:{conversation_id}"
    if not include_replies:
        query += f" from:{author_username}"

    return call_tool("searchPostsRecent", {
        "query": query,
        "max_results": "100",
        "tweet.fields": ["text", "created_at", "author_id", "referenced_tweets", "note_tweet"],
        "expansions": ["author_id"],
    }, req_id=2)


def get_username(user_id, includes):
    """Look up username from includes."""
    for u in includes.get("users", []):
        if u["id"] == user_id:
            return u.get("username", u.get("name", "?"))
    return "?"


def main():
    if len(sys.argv) < 2:
        print("Usage: read_x_thread.py <url-or-tweet-id> [--replies]")
        sys.exit(1)

    tweet_id = extract_tweet_id(sys.argv[1])
    if not tweet_id:
        print(f"Could not extract tweet ID from: {sys.argv[1]}")
        sys.exit(1)

    include_replies = "--replies" in sys.argv

    # Fetch the root tweet
    root = get_tweet(tweet_id)
    if "error" in root:
        print(f"Error: {root['error']}")
        sys.exit(1)

    data = root.get("data", {})
    includes = root.get("includes", {})
    author_id = data.get("author_id", "")
    conversation_id = data.get("conversation_id", tweet_id)
    author_username = get_username(author_id, includes)

    # Get note_tweet (long-form) text if available
    note = data.get("note_tweet", {})
    text = note.get("text", data.get("text", ""))

    print(f"@{author_username} — {data.get('created_at', '?')}")
    print(f"{'=' * 60}")
    print(text)
    print()

    # If this tweet is part of a thread, fetch the full conversation
    thread = get_thread(conversation_id, author_username, include_replies)

    if "error" in thread:
        # Might just be no replies
        return

    tweets = thread.get("data", [])
    thread_includes = thread.get("includes", {})

    if not tweets:
        return

    # Sort by created_at
    tweets.sort(key=lambda t: t.get("created_at", ""))

    # Filter out the root tweet (already printed)
    tweets = [t for t in tweets if t["id"] != tweet_id]

    if tweets:
        label = "Thread + replies" if include_replies else "Thread"
        print(f"--- {label} ({len(tweets)} more) ---")
        print()

    for t in tweets:
        t_author_id = t.get("author_id", "")
        t_username = get_username(t_author_id, thread_includes)
        if t_username == "?" and t_author_id == author_id:
            t_username = author_username

        note = t.get("note_tweet", {})
        t_text = note.get("text", t.get("text", ""))

        prefix = f"  @{t_username}" if t_username != author_username else f"  @{author_username}"
        print(f"{prefix} ({t.get('created_at', '?')[:16]})")
        print(f"  {t_text}")
        print()


if __name__ == "__main__":
    main()
