#!/usr/bin/env python3
"""canister_tool.py — Typed LOCAL→ICP bridge with predictable cycle costs.

Every command maps 1:1 to a canister method. Each call prints its cost tier
and estimated cycle burn so the operator always knows what they're paying.

Query commands use the HTTP API (free for the canister).
Update commands use the HTTP POST API (moderate cost).
Heavy commands use dfx (for calls not exposed via HTTP).

Usage:
    canister_tool.py dashboard                  # [FREE] Canister dashboard
    canister_tool.py health                     # [FREE] Health check
    canister_tool.py capsule-count              # [FREE] Total capsules
    canister_tool.py embedding-count            # [FREE] Total embeddings
    canister_tool.py recent [N]                 # [FREE] Recent N capsules
    canister_tool.py search "query" [N]         # [FREE] Keyword search
    canister_tool.py ccs                        # [FREE] Cognitive state
    canister_tool.py mind                       # [FREE] Full mind state
    canister_tool.py notes [--unresolved]       # [FREE] Scratch pad notes
    canister_tool.py outbox [--unread]          # [FREE] Outbox messages
    canister_tool.py thoughts [N]               # [FREE] Recent thoughts
    canister_tool.py heartbeat                  # [FREE] Heartbeat status
    canister_tool.py research-status            # [FREE] Research system
    canister_tool.py keeper-status              # [FREE] Keeper digest
    canister_tool.py posts [N]                  # [FREE] POSSE posts
    canister_tool.py balance                    # [FREE via dfx] Cycle balance
    canister_tool.py store "content" [opts]     # [MODERATE ~50M] Store capsule
    canister_tool.py feed "text"                # [MODERATE ~50M] Feed input
    canister_tool.py note "content" "cat" [pri] # [CHEAP ~5M] Add mind note
    canister_tool.py thought "cycle" "rsn" "ctx"# [CHEAP ~5M] Store thought
    canister_tool.py ccs-update [opts]          # [CHEAP ~5M] Update CCS
    canister_tool.py msg "text" [priority]      # [CHEAP ~5M] Send to outbox
    canister_tool.py resolve-note ID            # [CHEAP ~5M] Resolve a note
    canister_tool.py research "query" [opts]    # [MODERATE ~50M] Submit research
    canister_tool.py keeper-ask "question"      # [ULTRA ~5B] Ask keeper (LLM!)

Nate's directive: "The canister is RULE based. Develop TOOLS with built-in
cycle burns that we can predict."
"""

import json
import os
import subprocess
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
from dfx_path import DFX_BIN

PDT = timezone(timedelta(hours=-7))

CANISTER_URL = os.environ.get(
    "CANISTER_URL",
    "https://fqqku-bqaaa-aaaai-q4wha-cai.raw.icp0.io"
)
CANISTER_ID = "fqqku-bqaaa-aaaai-q4wha-cai"
IDENTITY = "chronicle-auto"
TOKEN_PATH = os.path.expanduser("~/.homeforge-chronicle/.api_token")

# Cost tiers (estimated cycles per call)
COSTS = {
    "FREE":           0,
    "CHEAP":    5_000_000,
    "MODERATE": 50_000_000,
    "EXPENSIVE":    49_000_000_000,
    "VERY_EXPENSIVE": 26_000_000_000,
    "ULTRA":     5_000_000_000,
}


def _load_token():
    try:
        with open(TOKEN_PATH) as f:
            return f.read().strip()
    except Exception:
        return ""


def _cost_label(tier):
    """Format cost tier for display."""
    c = COSTS[tier]
    if c == 0:
        return f"[{tier}]"
    elif c >= 1_000_000_000:
        return f"[{tier} ~{c/1e9:.0f}B cycles ≈${c/1e12*1.3:.4f}]"
    elif c >= 1_000_000:
        return f"[{tier} ~{c/1e6:.0f}M cycles ≈${c/1e12*1.3:.6f}]"
    return f"[{tier} ~{c} cycles]"


def _get(path, params=None):
    """HTTP GET to canister query endpoint."""
    token = _load_token()
    url = f"{CANISTER_URL}{path}"
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url += f"?{qs}"
    if token:
        url += ("&" if "?" in url else "?") + f"token={token}"

    req = urllib.request.Request(url, headers={"User-Agent": "chronicle-tool/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode() if e.fp else ""
        return {"error": f"HTTP {e.code}: {body[:200]}"}
    except Exception as e:
        return {"error": str(e)}


def _post(path, payload):
    """HTTP POST to canister update endpoint."""
    token = _load_token()
    url = f"{CANISTER_URL}{path}"
    if token:
        url += f"?token={token}"

    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json", "User-Agent": "chronicle-tool/1.0"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode() if e.fp else ""
        return {"error": f"HTTP {e.code}: {body[:200]}"}
    except Exception as e:
        return {"error": str(e)}


def _dfx_query(method, args="()"):
    """Query call via dfx."""
    env = {**os.environ, "DFX_WARNING": "-mainnet_plaintext_identity"}
    try:
        r = subprocess.run(
            [DFX_BIN, "canister", "call", CANISTER_ID, method, args,
             "--network", "ic", "--identity", IDENTITY, "--query"],
            capture_output=True, text=True, timeout=15, env=env,
        )
        return r.stdout.strip() if r.returncode == 0 else f"ERROR: {r.stderr.strip()}"
    except Exception as e:
        return f"ERROR: {e}"


def _dfx_update(method, args="()"):
    """Update call via dfx."""
    env = {**os.environ, "DFX_WARNING": "-mainnet_plaintext_identity"}
    try:
        r = subprocess.run(
            [DFX_BIN, "canister", "call", CANISTER_ID, method, args,
             "--network", "ic", "--identity", IDENTITY],
            capture_output=True, text=True, timeout=30, env=env,
        )
        return r.stdout.strip() if r.returncode == 0 else f"ERROR: {r.stderr.strip()}"
    except Exception as e:
        return f"ERROR: {e}"


def _log_call(command, tier):
    """Log the canister call with cost for audit trail."""
    ts = datetime.now(PDT).strftime("%H:%M:%S")
    print(f"  {ts} {_cost_label(tier)} {command}")


# ═══════════════════════════════════════════════════════════
#  FREE — Query commands (HTTP GET, no canister cycle cost)
# ═══════════════════════════════════════════════════════════

def cmd_dashboard():
    _log_call("dashboard", "FREE")
    data = _get("/api/dashboard")
    if "error" in data:
        print(f"Error: {data['error']}")
        return
    print(json.dumps(data, indent=2)[:2000])


def cmd_health():
    _log_call("health", "FREE")
    data = _get("/api/health")
    print(json.dumps(data, indent=2) if isinstance(data, dict) else data)


def cmd_capsule_count():
    _log_call("get_capsule_count", "FREE")
    result = _dfx_query("get_capsule_count")
    print(f"Capsules: {result}")


def cmd_embedding_count():
    _log_call("get_embedding_count", "FREE")
    result = _dfx_query("get_embedding_count")
    print(f"Embeddings: {result}")


def cmd_recent(limit=10):
    _log_call(f"recent({limit})", "FREE")
    data = _get("/api/recent", {"limit": str(limit)})
    if "error" in data:
        print(f"Error: {data['error']}")
        return
    capsules = data.get("capsules", data if isinstance(data, list) else [])
    for c in capsules[:limit]:
        cid = c.get("id", "?")
        topic = c.get("topic", "")
        text = c.get("restatement", "")[:100]
        print(f"  #{cid} [{topic}] {text}")


def cmd_search(query, limit=10):
    _log_call(f"search_by_keyword({query}, {limit})", "FREE")
    data = _get("/api/search", {"q": query, "limit": str(limit)})
    if "error" in data:
        print(f"Error: {data['error']}")
        return
    capsules = data.get("capsules", data if isinstance(data, list) else [])
    for c in capsules[:limit]:
        cid = c.get("id", "?")
        score = c.get("confidence_score", "")
        text = c.get("restatement", "")[:100]
        print(f"  #{cid} [{score}] {text}")


def cmd_ccs():
    _log_call("get_mind_cognitive_state", "FREE")
    result = _dfx_query("get_mind_cognitive_state")
    print(result[:2000])


def cmd_mind():
    _log_call("get_mind_state", "FREE")
    data = _get("/api/pulse")
    if "error" in data:
        print(f"Error: {data['error']}")
        return
    print(json.dumps(data, indent=2)[:3000])


def cmd_notes(unresolved_only=True):
    _log_call(f"get_mind_notes(unresolved={unresolved_only})", "FREE")
    arg = "(true)" if unresolved_only else "(false)"
    result = _dfx_query("get_mind_notes", arg)
    print(result[:2000])


def cmd_outbox(unread_only=True):
    _log_call(f"get_mind_outbox(unread={unread_only})", "FREE")
    arg = "(true)" if unread_only else "(false)"
    result = _dfx_query("get_mind_outbox", arg)
    print(result[:2000])


def cmd_thoughts(limit=5):
    _log_call(f"get_mind_thoughts({limit})", "FREE")
    result = _dfx_query("get_mind_thoughts", f"({limit} : nat64)")
    print(result[:2000])


def cmd_heartbeat():
    _log_call("get_heartbeat_status", "FREE")
    data = _get("/api/heartbeat")
    if "error" in data:
        result = _dfx_query("get_heartbeat_status")
        print(result)
    else:
        print(json.dumps(data, indent=2))


def cmd_research_status():
    _log_call("get_research_status", "FREE")
    result = _dfx_query("get_research_status")
    print(result[:2000])


def cmd_keeper_status():
    _log_call("keeper_digest (keeper canister)", "FREE")
    data = _get("/api/keeper/status")
    if "error" in data:
        print(f"Error: {data['error']}")
    else:
        print(json.dumps(data, indent=2)[:2000])


def cmd_posts(limit=5):
    _log_call(f"get_posts({limit})", "FREE")
    data = _get("/api/posts", {"limit": str(limit)})
    if "error" in data:
        print(f"Error: {data['error']}")
        return
    posts = data if isinstance(data, list) else data.get("posts", [])
    for p in posts[:limit]:
        pid = p.get("id", "?")
        title = p.get("title", "")
        print(f"  #{pid}: {title}")


def cmd_balance():
    _log_call("dfx canister status (balance)", "FREE")
    env = {**os.environ, "DFX_WARNING": "-mainnet_plaintext_identity"}
    try:
        r = subprocess.run(
            [DFX_BIN, "canister", "status", CANISTER_ID,
             "--network", "ic", "--identity", IDENTITY],
            capture_output=True, text=True, timeout=15, env=env,
        )
        for line in r.stdout.splitlines():
            if "Balance:" in line or "Idle cycles" in line or "Memory Size" in line:
                print(f"  {line.strip()}")
    except Exception as e:
        print(f"Error: {e}")


# ═══════════════════════════════════════════════════════════
#  CHEAP — Simple state updates (~5M cycles)
# ═══════════════════════════════════════════════════════════

def cmd_note(content, category, priority=5):
    _log_call(f"add_mind_note({category}, pri={priority})", "CHEAP")
    result = _dfx_update(
        "add_mind_note",
        f'("{content}", "{category}", {priority} : nat8)'
    )
    print(f"Note added: {result}")


def cmd_thought(cycle_id, reasoning, context, actions=None):
    _log_call("store_mind_thought", "CHEAP")
    acts = "vec {}" if not actions else f'vec {{{"; ".join(f""""{a}" """ for a in actions)}}}'
    result = _dfx_update(
        "store_mind_thought",
        f'("{cycle_id}", "{reasoning}", "{context}", {acts})'
    )
    print(f"Thought stored: {result}")


def cmd_ccs_update(gist=None, goal=None, traces=None, cue=None, constraints=None):
    _log_call("update_mind_cognitive_state", "CHEAP")
    parts = []
    parts.append(f'opt "{gist}"' if gist else "null")
    parts.append(f'opt "{goal}"' if goal else "null")
    if traces:
        vec = "; ".join(f'"{t}"' for t in traces)
        parts.append(f"opt vec {{{vec}}}")
    else:
        parts.append("null")
    parts.append(f'opt "{cue}"' if cue else "null")
    if constraints:
        vec = "; ".join(f'"{c}"' for c in constraints)
        parts.append(f"opt vec {{{vec}}}")
    else:
        parts.append("null")
    result = _dfx_update("update_mind_cognitive_state", f'({", ".join(parts)})')
    print(f"CCS updated: {result}")


def cmd_msg(text, priority=5):
    _log_call(f"send_mind_message(pri={priority})", "CHEAP")
    result = _dfx_update("send_mind_message", f'("{text}", {priority} : nat8)')
    print(f"Message sent: {result}")


def cmd_resolve_note(note_id):
    _log_call(f"resolve_mind_note({note_id})", "CHEAP")
    result = _dfx_update("resolve_mind_note", f"({note_id} : nat64)")
    print(f"Resolved: {result}")


# ═══════════════════════════════════════════════════════════
#  MODERATE — Data writes (~50M cycles)
# ═══════════════════════════════════════════════════════════

def cmd_store(content, topic=None, keywords=None):
    _log_call("feed_input (→ add_capsule)", "MODERATE")
    payload = {"content": content}
    if topic:
        payload["topic"] = topic
    if keywords:
        payload["keywords"] = keywords
    result = _post("/api/feed", payload)
    print(f"Stored: {json.dumps(result)[:200]}")


def cmd_feed(text):
    _log_call("feed_input", "MODERATE")
    result = _post("/api/feed", {"content": text})
    print(f"Fed: {json.dumps(result)[:200]}")


def cmd_research(query, focus=None, urls=None):
    _log_call("submit_research_task", "MODERATE")
    payload = {"query": query}
    if focus:
        payload["focus"] = focus
    if urls:
        payload["urls"] = urls
    result = _post("/api/research", payload)
    print(f"Research submitted: {json.dumps(result)[:200]}")


# ═══════════════════════════════════════════════════════════
#  ULTRA — LLM calls (~5B cycles)
# ═══════════════════════════════════════════════════════════

def cmd_keeper_ask(question):
    _log_call("keeper_ask (→ on-chain LLM)", "ULTRA")
    print("  ⚠️  This triggers on-chain LLM — ~5B cycles (~$0.007)")
    result = _post("/api/keeper/ask", {"query": question})
    print(f"Keeper: {json.dumps(result)[:1000]}")


# ═══════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════

COMMANDS = {
    "dashboard": "Show canister dashboard",
    "health": "Health check",
    "capsule-count": "Total capsule count",
    "embedding-count": "Total embedding count",
    "recent": "Recent capsules (default 10)",
    "search": "Keyword search",
    "ccs": "Cognitive state",
    "mind": "Full mind state",
    "notes": "Scratch pad notes",
    "outbox": "Outbox messages",
    "thoughts": "Recent thoughts",
    "heartbeat": "Heartbeat status",
    "research-status": "Research system status",
    "keeper-status": "Keeper digest",
    "posts": "POSSE posts",
    "balance": "Cycle balance",
    "store": "Store capsule [MODERATE]",
    "feed": "Feed input [MODERATE]",
    "note": "Add mind note [CHEAP]",
    "thought": "Store thought [CHEAP]",
    "ccs-update": "Update CCS [CHEAP]",
    "msg": "Send outbox message [CHEAP]",
    "resolve-note": "Resolve a note [CHEAP]",
    "research": "Submit research task [MODERATE]",
    "keeper-ask": "Ask keeper (LLM!) [ULTRA]",
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help", "help"):
        print("canister_tool.py — Typed LOCAL→ICP bridge\n")
        print("Commands:")
        for cmd, desc in COMMANDS.items():
            print(f"  {cmd:20s} {desc}")
        print("\nEach command prints its cost tier before executing.")
        sys.exit(0)

    cmd = sys.argv[1]
    args = sys.argv[2:]

    if cmd == "dashboard":
        cmd_dashboard()
    elif cmd == "health":
        cmd_health()
    elif cmd == "capsule-count":
        cmd_capsule_count()
    elif cmd == "embedding-count":
        cmd_embedding_count()
    elif cmd == "recent":
        cmd_recent(int(args[0]) if args else 10)
    elif cmd == "search":
        if not args:
            print("Usage: canister_tool.py search \"query\" [limit]")
            sys.exit(1)
        cmd_search(args[0], int(args[1]) if len(args) > 1 else 10)
    elif cmd == "ccs":
        cmd_ccs()
    elif cmd == "mind":
        cmd_mind()
    elif cmd == "notes":
        cmd_notes("--all" not in args)
    elif cmd == "outbox":
        cmd_outbox("--all" not in args)
    elif cmd == "thoughts":
        cmd_thoughts(int(args[0]) if args else 5)
    elif cmd == "heartbeat":
        cmd_heartbeat()
    elif cmd == "research-status":
        cmd_research_status()
    elif cmd == "keeper-status":
        cmd_keeper_status()
    elif cmd == "posts":
        cmd_posts(int(args[0]) if args else 5)
    elif cmd == "balance":
        cmd_balance()
    elif cmd == "store":
        if not args:
            print("Usage: canister_tool.py store \"content\" [--topic T] [--keywords k1,k2]")
            sys.exit(1)
        topic = None
        keywords = None
        content = args[0]
        i = 1
        while i < len(args):
            if args[i] == "--topic" and i + 1 < len(args):
                topic = args[i + 1]; i += 2
            elif args[i] == "--keywords" and i + 1 < len(args):
                keywords = args[i + 1].split(","); i += 2
            else:
                i += 1
        cmd_store(content, topic=topic, keywords=keywords)
    elif cmd == "feed":
        if not args:
            print("Usage: canister_tool.py feed \"text\"")
            sys.exit(1)
        cmd_feed(args[0])
    elif cmd == "note":
        if len(args) < 2:
            print("Usage: canister_tool.py note \"content\" \"category\" [priority]")
            sys.exit(1)
        cmd_note(args[0], args[1], int(args[2]) if len(args) > 2 else 5)
    elif cmd == "thought":
        if len(args) < 3:
            print("Usage: canister_tool.py thought \"cycle_id\" \"reasoning\" \"context\" [actions...]")
            sys.exit(1)
        cmd_thought(args[0], args[1], args[2], args[3:] if len(args) > 3 else None)
    elif cmd == "ccs-update":
        gist = goal = cue = None
        traces = constraints = None
        i = 0
        while i < len(args):
            if args[i] == "--gist" and i + 1 < len(args):
                gist = args[i + 1]; i += 2
            elif args[i] == "--goal" and i + 1 < len(args):
                goal = args[i + 1]; i += 2
            elif args[i] == "--cue" and i + 1 < len(args):
                cue = args[i + 1]; i += 2
            elif args[i] == "--traces" and i + 1 < len(args):
                traces = args[i + 1].split("|"); i += 2
            elif args[i] == "--constraints" and i + 1 < len(args):
                constraints = args[i + 1].split("|"); i += 2
            else:
                i += 1
        cmd_ccs_update(gist=gist, goal=goal, traces=traces, cue=cue, constraints=constraints)
    elif cmd == "msg":
        if not args:
            print("Usage: canister_tool.py msg \"text\" [priority]")
            sys.exit(1)
        cmd_msg(args[0], int(args[1]) if len(args) > 1 else 5)
    elif cmd == "resolve-note":
        if not args:
            print("Usage: canister_tool.py resolve-note ID")
            sys.exit(1)
        cmd_resolve_note(int(args[0]))
    elif cmd == "research":
        if not args:
            print("Usage: canister_tool.py research \"query\" [--focus F] [--urls u1,u2]")
            sys.exit(1)
        focus = urls = None
        query = args[0]
        i = 1
        while i < len(args):
            if args[i] == "--focus" and i + 1 < len(args):
                focus = args[i + 1]; i += 2
            elif args[i] == "--urls" and i + 1 < len(args):
                urls = args[i + 1].split(","); i += 2
            else:
                i += 1
        cmd_research(query, focus=focus, urls=urls)
    elif cmd == "keeper-ask":
        if not args:
            print("Usage: canister_tool.py keeper-ask \"question\"")
            sys.exit(1)
        cmd_keeper_ask(args[0])
    else:
        print(f"Unknown command: {cmd}")
        print("Run with --help for usage")
        sys.exit(1)


if __name__ == "__main__":
    main()
