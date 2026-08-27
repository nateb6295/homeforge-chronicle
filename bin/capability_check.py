#!/usr/bin/env python3
"""Medium-independent capability checker.

Stores expected capabilities on DISK (not in context), checks actual
availability each run, and flags dimensional loss. Addresses Kimi's
correlated-thinning vulnerability: context compaction can't erase
both the capability and its mirror if the mirror is on a different medium.
"""
import json
import os
import sys
import subprocess
import time
import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
from dfx_path import DFX_BIN

CAPABILITY_FILE = "/mnt/hdd/chronicle-data/expected_capabilities.json"

EXPECTED_CAPABILITIES = {
    "mcp_chronicle": {
        "description": "Chronicle MCP memory tools",
        "check": "mcp_tool_count",
        "expected_count": 28,
        "critical": True,
    },
    "bridge_model": {
        "description": "Gemma bridge model on llama-server",
        "check": "http_health",
        "url": "http://localhost:11435/health",
        "critical": True,
    },
    "engine": {
        "description": "Chronicle engine (Anthropic API proxy)",
        "check": "http_health",
        "url": "http://localhost:11436/health",
        "critical": True,
    },
    "ollama_embeddings": {
        "description": "Ollama embedding service",
        "check": "http_health",
        "url": "http://192.168.1.11:11434/api/tags",
        "critical": False,
    },
    "canisters": {
        "description": "ICP canister backend",
        "check": "canister_ping",
        "critical": True,
    },
    "database": {
        "description": "SQLite processed.db",
        "check": "file_exists",
        "path": "/mnt/hdd/chronicle-data/processed.db",
        "critical": True,
    },
}

def check_http_health(url, timeout=5):
    """Check if an HTTP endpoint responds."""
    import requests
    try:
        r = requests.get(url, timeout=timeout)
        return r.status_code < 500
    except Exception:
        return False

def check_mcp_tools():
    """Test MCP binary handshake and count tools."""
    try:
        proc = subprocess.run(
            ["bash", "-c", """
(echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"capcheck","version":"1.0"}}}'
echo '{"jsonrpc":"2.0","method":"notifications/initialized"}'
echo '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}') | timeout 10 python3 /home/nate-agx/chronicle/bin/chronicle-mcp-wrapper.py 2>/dev/null
"""],
            capture_output=True, text=True, timeout=15
        )
        for line in proc.stdout.strip().split('\n'):
            try:
                msg = json.loads(line)
                tools = msg.get('result', {}).get('tools', [])
                if tools:
                    return len(tools)
            except json.JSONDecodeError:
                continue
        return 0
    except Exception:
        return 0

def check_canister():
    """Quick canister reachability check."""
    try:
        r = subprocess.run(
            [DFX_BIN, "canister", "--network", "ic", "status", "fqqku-bqaaa-aaaai-q4wha-cai"],
            capture_output=True, text=True, timeout=15,
            env={**os.environ, "DFX_WARNING": "-mainnet_plaintext_identity"}
        )
        return r.returncode == 0
    except Exception:
        return False

def run_checks():
    results = {}
    for name, cap in EXPECTED_CAPABILITIES.items():
        check_type = cap["check"]
        if check_type == "http_health":
            ok = check_http_health(cap["url"])
            results[name] = {"ok": ok, "detail": f"HTTP {'up' if ok else 'down'}"}
        elif check_type == "mcp_tool_count":
            count = check_mcp_tools()
            ok = count >= cap["expected_count"]
            results[name] = {"ok": ok, "detail": f"{count}/{cap['expected_count']} tools"}
        elif check_type == "canister_ping":
            ok = check_canister()
            results[name] = {"ok": ok, "detail": f"canister {'reachable' if ok else 'unreachable'}"}
        elif check_type == "file_exists":
            ok = os.path.exists(cap["path"])
            results[name] = {"ok": ok, "detail": f"{'exists' if ok else 'MISSING'}"}
        results[name]["critical"] = cap["critical"]
        results[name]["description"] = cap["description"]
    return results

def save_state(results):
    state = {
        "timestamp": int(time.time()),
        "capabilities": EXPECTED_CAPABILITIES,
        "last_check": results,
    }
    with open(CAPABILITY_FILE, 'w') as f:
        json.dump(state, f, indent=2)

def main():
    json_mode = "--json" in sys.argv

    results = run_checks()

    total = len(results)
    ok_count = sum(1 for r in results.values() if r["ok"])
    failed = [name for name, r in results.items() if not r["ok"]]
    critical_failures = [name for name in failed
                        if results[name]["critical"]]

    if json_mode:
        print(json.dumps({
            "ok": ok_count,
            "total": total,
            "failed": failed,
            "critical_failed": critical_failures,
        }))
    else:
        print(f"Capability check: {ok_count}/{total} operational")
        for name, r in results.items():
            crit = " [CRITICAL]" if not r["ok"] and r["critical"] else ""
            print(f"  {'✓' if r['ok'] else '✗'} {name}: {r['detail']}{crit}")
        if critical_failures:
            print(f"\n⚠ DIMENSIONAL LOSS: {len(critical_failures)} critical capabilities missing:")
            for name in critical_failures:
                print(f"  - {results[name]['description']}")

    save_state(results)
    return 1 if critical_failures else 0

if __name__ == "__main__":
    sys.exit(main())
