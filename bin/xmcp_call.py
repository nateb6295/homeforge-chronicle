#!/usr/bin/env python3
"""Direct caller for xmcp (X API MCP server) via Streamable HTTP transport."""
import json, sys, requests

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
        mr = args.get("max_results", 10)
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
                        print(json.dumps(parsed, indent=2)[:4000])
                    except (json.JSONDecodeError, TypeError):
                        print(item["text"][:4000])
        if is_error:
            sys.exit(2)
    elif "error" in result:
        print(f"ERROR: {json.dumps(result['error'], indent=2)}", file=sys.stderr)
        sys.exit(1)
    else:
        print(json.dumps(result, indent=2)[:4000])
