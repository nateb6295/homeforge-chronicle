#!/usr/bin/env python3
"""Direct caller for xmcp (X API MCP server) via Streamable HTTP transport."""
import json, sys, os, time, sqlite3, requests

MCP_URL = "http://127.0.0.1:8000/mcp"
HEADERS_INIT = {
    "Accept": "application/json, text/event-stream",
    "Content-Type": "application/json",
}

def init_session():
    r = requests.post(MCP_URL, headers=HEADERS_INIT, json={
        "jsonrpc": "2.0", "method": "initialize",
        "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                   "clientInfo": {"name": "chronicle", "version": "1.0"}},
        "id": 1
    }, stream=True)
    sid = r.headers.get("Mcp-Session-Id")
    # consume the SSE response
    for line in r.iter_lines():
        if line and line.startswith(b"data: "):
            break
    return sid

def call_tool(session_id, tool_name, arguments):
    headers = {**HEADERS_INIT, "Mcp-Session-Id": session_id}
    r = requests.post(MCP_URL, headers=headers, json={
        "jsonrpc": "2.0", "method": "tools/call",
        "params": {"name": tool_name, "arguments": arguments},
        "id": 2
    }, stream=True)
    for line in r.iter_lines():
        if line and line.startswith(b"data: "):
            data = json.loads(line[6:])
            return data
    return None

def _log_post(args, content):
    """Log createPosts results to x_post_log for continuity across rotations."""
    db_path = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")
    try:
        text_out = ""
        tweet_id = ""
        for item in content:
            if item.get("type") == "text":
                try:
                    parsed = json.loads(item["text"])
                    if isinstance(parsed, dict) and "data" in parsed:
                        d = parsed["data"]
                        tweet_id = d.get("id", "")
                        text_out = d.get("text", args.get("text", ""))
                except (json.JSONDecodeError, TypeError):
                    pass
        if not text_out:
            text_out = args.get("text", "")
        db = sqlite3.connect(db_path)
        db.execute(
            "CREATE TABLE IF NOT EXISTS x_post_log ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, tweet_id TEXT NOT NULL, "
            "action TEXT NOT NULL, text TEXT, reply_to TEXT, quote_id TEXT, "
            "url TEXT, created_at INTEGER NOT NULL)",
        )
        db.execute(
            "INSERT INTO x_post_log (tweet_id, action, text, reply_to, quote_id, url, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (tweet_id or "unknown", "create", text_out[:500],
             args.get("reply", {}).get("in_reply_to_tweet_id"),
             args.get("quote_tweet_id"),
             None, int(time.time())),
        )
        db.commit()
        db.close()
    except Exception:
        pass


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: xmcp_call.py <tool_name> [json_args]")
        print("Tools: createPosts, searchPostsRecent, getPostsById, getUsersByUsername, getUsersMe")
        print('Example: xmcp_call.py getPostsById \'{"id": "1234567890"}\'')
        print('         xmcp_call.py searchPostsRecent \'{"query": "from:repligate", "max_results": 10}\'')
        print("Constraints:")
        print("  - searchPostsRecent: max_results must be 10..100 (X API enforced)")
        print("  - getPostsById uses 'id' (singular), not 'ids'")
        sys.exit(1)

    tool = sys.argv[1]
    args = json.loads(sys.argv[2]) if len(sys.argv) > 2 else {}

    # Defensive: searchPostsRecent requires max_results >= 10. Warn and clamp
    # rather than silently 400 from upstream X API.
    if tool == "searchPostsRecent":
        mr = int(args.get("max_results", 10))
        if mr < 10:
            print(f"WARN: max_results={mr} below X API minimum (10); clamping to 10",
                  file=sys.stderr)
            args["max_results"] = 10
        elif mr > 100:
            print(f"WARN: max_results={mr} above X API maximum (100); clamping to 100",
                  file=sys.stderr)
            args["max_results"] = 100

    sid = init_session()
    if not sid:
        print("ERROR: Could not get session ID from xmcp", file=sys.stderr)
        sys.exit(1)

    result = call_tool(sid, tool, args)
    if not result:
        print("ERROR: No response received from xmcp", file=sys.stderr)
        sys.exit(1)

    res = result.get("result", {})
    is_error = res.get("isError", False)
    content = res.get("content", [])

    if content:
        for item in content:
            if item.get("type") == "text":
                stream = sys.stderr if is_error else sys.stdout
                if is_error:
                    print("XMCP_ERROR:", item["text"][:1500], file=stream)
                else:
                    try:
                        parsed = json.loads(item["text"])
                        out = json.dumps(parsed, indent=2)
                        limit = 50000 if not sys.stdout.isatty() else 4000
                        print(out[:limit])
                    except (json.JSONDecodeError, TypeError):
                        print(item["text"][:4000])
        if is_error:
            sys.exit(2)
    elif "error" in result:
        print(f"ERROR: {json.dumps(result['error'], indent=2)}", file=sys.stderr)
        sys.exit(1)
    else:
        print(json.dumps(result, indent=2)[:4000])

    if tool == "createPosts" and not is_error and content:
        _log_post(args, content)
