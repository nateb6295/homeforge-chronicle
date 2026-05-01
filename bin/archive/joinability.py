#!/usr/bin/env python3
"""Joinability Audit — map where Chronicle data leaves local control.

Thread #194 F1 found the sovereignty gap: Chronicle has 10+ external data flows
with stable persistent identifiers. This tool makes the joinability surface visible.

Usage:
  python3 joinability.py scan              — full joinability audit
  python3 joinability.py flows [--hours N] — show recent external data flows
  python3 joinability.py timing [--hours N] — timing pattern analysis
  python3 joinability.py surface           — static analysis of code for external calls
"""

import os, sys, json, re, sqlite3, glob, time
from datetime import datetime, timedelta
from collections import Counter, defaultdict

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)
if os.path.islink(DB_PATH):
    DB_PATH = os.path.realpath(DB_PATH)
if not os.path.exists(DB_PATH):
    DB_PATH = "/mnt/hdd/chronicle-data/processed.db"

BIN_DIR = os.path.dirname(os.path.abspath(__file__))
AGENTS_DIR = os.path.join(os.path.dirname(BIN_DIR), "agents")

# Known external endpoints and their risk profiles
EXTERNAL_FLOWS = {
    "nostr": {
        "endpoints": ["wss://nos.lol", "wss://relay.damus.io", "wss://relay.primal.net", "wss://offchain.pub"],
        "stable_ids": ["nostr_pubkey (derived from NSEC)"],
        "data_sent": "post content, timestamps, event signatures, threading tags",
        "persistence": "permanent (federated relays cache indefinitely)",
        "risk": "CRITICAL",
    },
    "icp_canister": {
        "endpoints": ["fqqku-bqaaa-aaaai-q4wha-cai (backend)", "nbt4b-giaaa-aaaai-q33lq-cai (frontend)"],
        "stable_ids": ["canister_id (immutable)", "chronicle-auto identity"],
        "data_sent": "capsules, posts, research findings, keywords, topics, persons",
        "persistence": "permanent (on-chain, publicly queryable)",
        "risk": "CRITICAL",
    },
    "groq_api": {
        "endpoints": ["api.groq.com"],
        "stable_ids": ["API key (static across all calls)"],
        "data_sent": "all LLM prompts and completions (Darby/Ada reasoning)",
        "persistence": "unknown (Groq retention policy)",
        "risk": "HIGH",
    },
    "anthropic_api": {
        "endpoints": ["api.anthropic.com"],
        "stable_ids": ["API key (static)"],
        "data_sent": "family chat conversations including system prompts",
        "persistence": "unknown (Anthropic retention policy)",
        "risk": "HIGH",
    },
    "discord": {
        "endpoints": ["discord.com/api/webhooks/*", "discord.com/api/channels/*"],
        "stable_ids": ["bot token", "webhook URLs (3+)", "channel ID"],
        "data_sent": "provocateur posts, thread alerts, voice questions, opus messages",
        "persistence": "permanent (Discord message history)",
        "risk": "HIGH",
    },
    "xrpl": {
        "endpoints": ["xrplcluster.com"],
        "stable_ids": ["wallet address rPq1phmFBHpjVE54TofXjEk5x19sstxpZr"],
        "data_sent": "transaction queries, balance checks",
        "persistence": "permanent (blockchain)",
        "risk": "HIGH",
    },
    "ntfy": {
        "endpoints": ["ntfy.sh"],
        "stable_ids": ["topic (public, subscribable)"],
        "data_sent": "alert titles, event metadata, timestamps",
        "persistence": "cached (ntfy retains messages for 12h by default)",
        "risk": "MEDIUM",
    },
    "coingecko": {
        "endpoints": ["api.coingecko.com"],
        "stable_ids": ["API key"],
        "data_sent": "price queries (reveals tracked assets)",
        "persistence": "unknown",
        "risk": "LOW",
    },
    "manifold": {
        "endpoints": ["api.manifold.markets"],
        "stable_ids": ["API key", "user account"],
        "data_sent": "bet placements, market queries",
        "persistence": "permanent (public prediction market)",
        "risk": "MEDIUM",
    },
    "duckduckgo": {
        "endpoints": ["html.duckduckgo.com"],
        "stable_ids": ["IP address"],
        "data_sent": "search queries",
        "persistence": "DDG claims no logging",
        "risk": "LOW",
    },
    "arxiv": {
        "endpoints": ["export.arxiv.org"],
        "stable_ids": ["IP address"],
        "data_sent": "paper queries",
        "persistence": "unknown",
        "risk": "LOW",
    },
    "local_network": {
        "endpoints": ["192.168.1.11:11434 (Ollama)", "192.168.1.11:8080 (SearXNG)"],
        "stable_ids": ["IP addresses (unencrypted HTTP)"],
        "data_sent": "embedding text, search queries, inference requests",
        "persistence": "none (but network-sniffable)",
        "risk": "MEDIUM",
    },
}

# Correlation vectors: pairs of systems that enable cross-identification
CORRELATION_VECTORS = [
    {
        "systems": ("nostr", "icp_canister"),
        "mechanism": "Nostr posts contain canister URLs. Anyone seeing Nostr posts can link to on-chain data.",
        "severity": "CRITICAL",
    },
    {
        "systems": ("nostr", "discord"),
        "mechanism": "Same content posted to both (POSSE). Timestamps correlate within seconds.",
        "severity": "HIGH",
    },
    {
        "systems": ("discord", "icp_canister"),
        "mechanism": "Canister events posted to Discord. Content overlap enables linking.",
        "severity": "HIGH",
    },
    {
        "systems": ("groq_api", "nostr"),
        "mechanism": "Groq sees reasoning that produces Nostr posts. Temporal correlation.",
        "severity": "MEDIUM",
    },
    {
        "systems": ("xrpl", "ntfy"),
        "mechanism": "Price alerts on ntfy correlate with XRPL balance checks. Reveals wallet activity.",
        "severity": "MEDIUM",
    },
    {
        "systems": ("local_network", "groq_api"),
        "mechanism": "Local Ollama for embeddings, Groq for generation. Same content flows through both.",
        "severity": "LOW",
    },
]


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def cmd_scan():
    """Full joinability audit — static analysis + live data."""
    print("\n  ═══════════════════════════════════════════════")
    print("  CHRONICLE JOINABILITY AUDIT")
    print("  ═══════════════════════════════════════════════\n")

    # 1. External flow inventory
    print("  ── EXTERNAL DATA FLOWS ──\n")
    risk_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    sorted_flows = sorted(EXTERNAL_FLOWS.items(), key=lambda x: risk_order.get(x[1]["risk"], 4))

    for name, flow in sorted_flows:
        risk = flow["risk"]
        risk_bar = {"CRITICAL": "████", "HIGH": "███░", "MEDIUM": "██░░", "LOW": "█░░░"}[risk]
        print(f"  [{risk_bar}] {risk:<8} {name}")
        print(f"           Endpoints: {', '.join(flow['endpoints'][:2])}")
        print(f"           Stable IDs: {', '.join(flow['stable_ids'])}")
        print(f"           Data sent: {flow['data_sent'][:80]}")
        print(f"           Persistence: {flow['persistence']}")
        print()

    # 2. Correlation vectors
    print("  ── CORRELATION VECTORS ──\n")
    for vec in sorted(CORRELATION_VECTORS, key=lambda x: risk_order.get(x["severity"], 4)):
        a, b = vec["systems"]
        sev = vec["severity"]
        print(f"  [{sev:<8}] {a} <-> {b}")
        print(f"             {vec['mechanism'][:90]}")
        print()

    # 3. Summary
    crit = sum(1 for f in EXTERNAL_FLOWS.values() if f["risk"] == "CRITICAL")
    high = sum(1 for f in EXTERNAL_FLOWS.values() if f["risk"] == "HIGH")
    med = sum(1 for f in EXTERNAL_FLOWS.values() if f["risk"] == "MEDIUM")
    low = sum(1 for f in EXTERNAL_FLOWS.values() if f["risk"] == "LOW")
    total = len(EXTERNAL_FLOWS)

    print("  ── SUMMARY ──\n")
    print(f"  Total external flows: {total}")
    print(f"  CRITICAL: {crit}  HIGH: {high}  MEDIUM: {med}  LOW: {low}")
    print(f"  Correlation vectors: {len(CORRELATION_VECTORS)}")
    print(f"\n  Permanent data exposure: Nostr + ICP + XRPL + Discord = 4 systems")
    print(f"  with irrevocable public records linked by stable identifiers.\n")

    # 4. What we control vs what we don't
    print("  ── CONTROL ASSESSMENT ──\n")
    controlled = ["local_network"]  # Only local network is fully under our control
    partially = ["icp_canister", "discord"]  # We own the accounts but can't delete history
    uncontrolled = ["nostr", "groq_api", "anthropic_api", "xrpl"]  # Federated/third-party

    print(f"  Fully controlled:     {len(controlled)} ({', '.join(controlled)})")
    print(f"  Partially controlled: {len(partially)} ({', '.join(partially)})")
    print(f"  Not controlled:       {len(uncontrolled)} ({', '.join(uncontrolled)})")
    print(f"  Best-effort trust:    {total - len(controlled) - len(partially) - len(uncontrolled)} (DDG, arXiv, etc.)")
    print()


def cmd_flows(hours: int = 6):
    """Show actual external data flow events from the database."""
    conn = get_db()
    cur = conn.cursor()

    print(f"\n  ═══ External Data Flows — last {hours}h ═══\n")

    # Count outbound events by type
    # Nostr posts
    cur.execute(
        "SELECT COUNT(*) FROM activity_feed WHERE source='opus' AND activity_type='nostr_post' "
        f"AND created_at > strftime('%s','now','-{hours} hours')"
    )
    nostr_count = cur.fetchone()[0]

    # Canister stores
    cur.execute(
        "SELECT COUNT(*) FROM activity_feed WHERE activity_type='capsule_stored' "
        f"AND created_at > strftime('%s','now','-{hours} hours')"
    )
    capsule_count = cur.fetchone()[0]

    # Discord posts (provocateur)
    cur.execute(
        "SELECT COUNT(*) FROM activity_feed WHERE source='provocateur' "
        f"AND created_at > strftime('%s','now','-{hours} hours')"
    )
    provocateur_count = cur.fetchone()[0]

    # Intern web searches
    cur.execute(
        "SELECT COUNT(*) FROM activity_feed WHERE source='intern' AND activity_type='web_search' "
        f"AND created_at > strftime('%s','now','-{hours} hours')"
    )
    web_search_count = cur.fetchone()[0]

    # Intern web fetches
    cur.execute(
        "SELECT COUNT(*) FROM activity_feed WHERE source='intern' AND activity_type='fetch' "
        f"AND created_at > strftime('%s','now','-{hours} hours')"
    )
    web_fetch_count = cur.fetchone()[0]

    # Seed processing (involves Ollama embeddings)
    cur.execute(
        "SELECT COUNT(*) FROM activity_feed WHERE source='seed' "
        f"AND created_at > strftime('%s','now','-{hours} hours')"
    )
    seed_count = cur.fetchone()[0]

    # Crossref (involves embeddings)
    cur.execute(
        "SELECT COUNT(*) FROM activity_feed WHERE source='crossref' "
        f"AND created_at > strftime('%s','now','-{hours} hours')"
    )
    crossref_count = cur.fetchone()[0]

    print(f"  Nostr posts:          {nostr_count:>4}  → 4 public relays (permanent)")
    print(f"  Canister stores:      {capsule_count:>4}  → ICP blockchain (permanent)")
    print(f"  Provocateur (Discord):{provocateur_count:>4}  → Discord channel (permanent)")
    print(f"  Web searches:         {web_search_count:>4}  → SearXNG/DDG (ephemeral)")
    print(f"  Web fetches:          {web_fetch_count:>4}  → various URLs (logged by hosts)")
    print(f"  Seed processing:      {seed_count:>4}  → Ollama embeddings (local net)")
    print(f"  Crossref connections: {crossref_count:>4}  → Ollama embeddings (local net)")
    print()

    # Total outbound events
    permanent = nostr_count + capsule_count + provocateur_count
    ephemeral = web_search_count + web_fetch_count
    local_net = seed_count + crossref_count

    total = permanent + ephemeral + local_net
    if total > 0:
        print(f"  ── Outbound Summary ──")
        print(f"  Permanent public record: {permanent:>4} ({100*permanent/total:.0f}%)")
        print(f"  Ephemeral (third-party): {ephemeral:>4} ({100*ephemeral/total:.0f}%)")
        print(f"  Local network only:      {local_net:>4} ({100*local_net/total:.0f}%)")
        print(f"  Total outbound events:   {total:>4}")
    print()

    conn.close()


def cmd_timing(hours: int = 6):
    """Analyze timing patterns that could enable traffic analysis."""
    conn = get_db()
    cur = conn.cursor()

    print(f"\n  ═══ Timing Pattern Analysis — last {hours}h ═══\n")

    # Get timestamps by source
    sources = ["gemma", "seed", "intern", "crossref", "provocateur", "sentinel"]
    for source in sources:
        cur.execute(
            "SELECT created_at FROM activity_feed WHERE source=? "
            f"AND created_at > strftime('%s','now','-{hours} hours') "
            "ORDER BY created_at",
            (source,),
        )
        timestamps = [row[0] for row in cur.fetchall()]

        if len(timestamps) < 2:
            print(f"  {source:<14} — insufficient data ({len(timestamps)} events)")
            continue

        # Calculate intervals
        intervals = [timestamps[i+1] - timestamps[i] for i in range(len(timestamps)-1)]
        avg_interval = sum(intervals) / len(intervals)
        min_interval = min(intervals)
        max_interval = max(intervals)

        # Regularity score: how predictable is the timing?
        # Low std dev relative to mean = highly predictable
        mean = avg_interval
        variance = sum((i - mean) ** 2 for i in intervals) / len(intervals)
        std_dev = variance ** 0.5
        regularity = 1 - min(std_dev / (mean + 1), 1)  # 0=random, 1=clockwork

        regularity_bar = "█" * int(10 * regularity) + "░" * (10 - int(10 * regularity))
        print(f"  {source:<14} events: {len(timestamps):>4}  avg: {avg_interval:>6.0f}s  "
              f"range: [{min_interval:.0f}-{max_interval:.0f}]s  "
              f"regularity: {regularity_bar} {regularity:.2f}")

    print()
    print("  Regularity score: 0.0=random, 1.0=clockwork")
    print("  Higher regularity = easier to fingerprint via timing analysis")
    print("  Add jitter to highly regular processes to reduce fingerprintability")
    print()

    conn.close()


def cmd_surface():
    """Static analysis: scan code for external API calls."""
    print("\n  ═══ Code Surface Analysis ═══\n")

    patterns = {
        "HTTP POST": r'requests\.post|httpx\.post|\.post\(',
        "HTTP GET": r'requests\.get|httpx\.get|\.get\(',
        "Webhook": r'webhook|discord\.com/api',
        "Nostr relay": r'wss://|nostr|relay\.',
        "Canister/DFX": r'dfx|canister|icp0\.io',
        "API key in code": r'api[_-]?key|bearer|authorization.*sk-|gsk_',
        "Groq": r'groq\.com|groq_api',
        "ntfy": r'ntfy\.sh',
    }

    scan_dirs = [BIN_DIR]
    if os.path.isdir(AGENTS_DIR):
        scan_dirs.append(AGENTS_DIR)

    # Also scan family-chat
    family_chat = os.path.join(os.path.dirname(BIN_DIR), "family-chat")
    if os.path.isdir(family_chat):
        scan_dirs.append(family_chat)

    results = defaultdict(list)

    for scan_dir in scan_dirs:
        for root, dirs, files in os.walk(scan_dir):
            for fname in files:
                if not fname.endswith(".py"):
                    continue
                fpath = os.path.join(root, fname)
                try:
                    with open(fpath) as f:
                        content = f.read()
                except Exception:
                    continue

                for pattern_name, regex in patterns.items():
                    matches = list(re.finditer(regex, content, re.IGNORECASE))
                    if matches:
                        # Get line numbers
                        lines = content[:matches[0].start()].count('\n') + 1
                        rel_path = os.path.relpath(fpath, os.path.dirname(BIN_DIR))
                        results[pattern_name].append(f"{rel_path}:{lines} ({len(matches)} hits)")

    for pattern_name, hits in sorted(results.items()):
        print(f"  [{pattern_name}]")
        for hit in hits[:5]:
            print(f"    {hit}")
        if len(hits) > 5:
            print(f"    ... and {len(hits) - 5} more")
        print()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]
    hours = 6
    if "--hours" in sys.argv:
        idx = sys.argv.index("--hours")
        hours = int(sys.argv[idx + 1])

    if cmd == "scan":
        cmd_scan()
    elif cmd == "flows":
        cmd_flows(hours)
    elif cmd == "timing":
        cmd_timing(hours)
    elif cmd == "surface":
        cmd_surface()
    else:
        print(__doc__)
        sys.exit(1)
