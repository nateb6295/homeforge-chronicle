#!/usr/bin/env python3
"""Capsule explorer — browse and search canister memories.

Usage:
    capsule_explorer.py ID                  # Read a single capsule
    capsule_explorer.py --range 400 410     # Read capsules in a range
    capsule_explorer.py --conversation ID   # Find capsules by conversation_id prefix
    capsule_explorer.py --map               # Map all conversation IDs (samples the range)
    capsule_explorer.py --map --full        # Map with denser sampling
    capsule_explorer.py --search QUERY      # Search capsule restatements (local grep)
    capsule_explorer.py --stats             # Capsule count and range info
"""

import subprocess
import sqlite3
import sys
import os
import re
import json
from collections import defaultdict

DFX = os.path.expanduser("~/.local/share/dfx/bin/dfx")
NETWORK = "ic"
BACKEND = "fqqku-bqaaa-aaaai-q4wha-cai"
DB_PATH = os.environ.get("CHRONICLE_DB", "/mnt/hdd/chronicle-data/processed.db")

# Known total capsules (update periodically)
ESTIMATED_MAX = 26500


def dfx_env():
    env = os.environ.copy()
    env["DFX_WARNING"] = "-mainnet_plaintext_identity"
    return env


def get_capsule(capsule_id):
    """Fetch a single capsule from the backend canister."""
    try:
        result = subprocess.run(
            [DFX, "canister", "--network", NETWORK, "call", BACKEND,
             "get_capsule", f"({capsule_id} : nat64)"],
            capture_output=True, text=True, timeout=15, env=dfx_env()
        )
        return parse_capsule(result.stdout, capsule_id)
    except Exception as e:
        return None


def parse_capsule(output, expected_id=None):
    """Parse dfx candid output into a dict."""
    if "(null)" in output or not output.strip():
        return None

    capsule = {"id": expected_id}

    # Extract fields using regex
    patterns = {
        "conversation_id": r'conversation_id = "([^"]+)"',
        "restatement": r'restatement = "(.+?)";\s*$',
        "topic": r'topic = opt "([^"]+)"',
        "timestamp": r'timestamp = opt "([^"]+)"',
        "confidence_score": r'confidence_score = ([\d.]+)',
    }

    for field, pattern in patterns.items():
        m = re.search(pattern, output, re.MULTILINE)
        if m:
            capsule[field] = m.group(1)

    # Extract keywords
    kw_match = re.search(r'keywords = vec \{([^}]*)\}', output, re.DOTALL)
    if kw_match:
        capsule["keywords"] = re.findall(r'"([^"]+)"', kw_match.group(1))

    # Extract persons
    p_match = re.search(r'persons = vec \{([^}]*)\}', output, re.DOTALL)
    if p_match:
        capsule["persons"] = re.findall(r'"([^"]+)"', p_match.group(1))

    # Extract entities
    e_match = re.search(r'entities = vec \{([^}]*)\}', output, re.DOTALL)
    if e_match:
        capsule["entities"] = re.findall(r'"([^"]+)"', e_match.group(1))

    return capsule if "restatement" in capsule else None


def print_capsule(c, compact=False):
    """Pretty-print a capsule."""
    if not c:
        return

    if compact:
        ts = c.get("timestamp", "")
        ts_str = f" [{ts}]" if ts else ""
        topic = c.get("topic", "")
        topic_str = f" ({topic})" if topic else ""
        conv = c.get("conversation_id", "")[:8]
        print(f"  #{c['id']:>5d}  {conv}  {c.get('restatement', '')[:100]}{ts_str}{topic_str}")
    else:
        print(f"\n  ╔═ Capsule #{c['id']}")
        if c.get("topic"):
            print(f"  ║ Topic:    {c['topic']}")
        if c.get("conversation_id"):
            print(f"  ║ Conv:     {c['conversation_id']}")
        if c.get("timestamp"):
            print(f"  ║ Time:     {c['timestamp']}")
        print(f"  ║")
        # Word wrap the restatement at ~80 chars
        text = c.get("restatement", "")
        while text:
            if len(text) <= 78:
                print(f"  ║ {text}")
                break
            # Find last space before 78 chars
            idx = text.rfind(" ", 0, 78)
            if idx == -1:
                idx = 78
            print(f"  ║ {text[:idx]}")
            text = text[idx:].lstrip()
        if c.get("keywords"):
            print(f"  ║")
            print(f"  ║ Keywords: {', '.join(c['keywords'])}")
        if c.get("persons"):
            print(f"  ║ Persons:  {', '.join(c['persons'])}")
        if c.get("entities"):
            print(f"  ║ Entities: {', '.join(c['entities'])}")
        print(f"  ╚{'═' * 60}")


def cmd_read(capsule_id):
    """Read a single capsule."""
    c = get_capsule(capsule_id)
    if c:
        print_capsule(c)
    else:
        print(f"  Capsule #{capsule_id} not found (null or empty)")


def cmd_range(start, end):
    """Read capsules in a range."""
    found = 0
    for i in range(start, end + 1):
        c = get_capsule(i)
        if c:
            print_capsule(c, compact=True)
            found += 1
    print(f"\n  {found} capsules found in range {start}-{end}")


def cmd_conversation(conv_prefix):
    """Find capsules matching a conversation_id prefix by sampling."""
    print(f"  Searching for conversation_id starting with '{conv_prefix}'...")
    print(f"  Sampling capsule range 1-{ESTIMATED_MAX}...\n")

    # Sample at different densities across the range
    matches = []
    sample_points = set()

    # Coarse sweep first
    for i in range(1, ESTIMATED_MAX, 50):
        sample_points.add(i)

    for i in sorted(sample_points):
        c = get_capsule(i)
        if c and c.get("conversation_id", "").startswith(conv_prefix):
            matches.append(c)
            print_capsule(c, compact=True)

            # Found one — do a dense sweep around it
            for j in range(max(1, i - 10), min(ESTIMATED_MAX, i + 10)):
                if j != i and j not in sample_points:
                    sample_points.add(j)
                    c2 = get_capsule(j)
                    if c2 and c2.get("conversation_id", "").startswith(conv_prefix):
                        matches.append(c2)
                        print_capsule(c2, compact=True)

    print(f"\n  Found {len(matches)} capsules with conversation prefix '{conv_prefix}'")
    print(f"  (Sampled {len(sample_points)} points — use --range for exhaustive search)")


def cmd_map(full=False):
    """Map all conversation IDs by sampling the capsule range."""
    step = 5 if full else 25
    print(f"  Mapping conversation IDs (step={step})...")
    print(f"  Range: 1 to {ESTIMATED_MAX}\n")

    conversations = defaultdict(lambda: {"count": 0, "first_id": 999999, "last_id": 0,
                                          "timestamps": [], "topics": set()})

    sampled = 0
    for i in range(1, ESTIMATED_MAX, step):
        c = get_capsule(i)
        sampled += 1
        if sampled % 100 == 0:
            sys.stderr.write(f"  ... sampled {sampled} points\r")

        if c and c.get("conversation_id"):
            conv = c["conversation_id"]
            info = conversations[conv]
            info["count"] += 1
            info["first_id"] = min(info["first_id"], i)
            info["last_id"] = max(info["last_id"], i)
            if c.get("timestamp"):
                info["timestamps"].append(c["timestamp"])
            if c.get("topic"):
                info["topics"].add(c["topic"])

    sys.stderr.write(f"  Sampled {sampled} points.        \n")

    # Sort by first capsule ID
    sorted_convs = sorted(conversations.items(), key=lambda x: x[1]["first_id"])

    # Classify
    imports = []
    live = []
    for conv_id, info in sorted_convs:
        if conv_id.startswith("mcp-") or conv_id.startswith("http-") or conv_id.startswith("chronicle-") or conv_id.startswith("arxiv-"):
            live.append((conv_id, info))
        else:
            imports.append((conv_id, info))

    if imports:
        print("  ── IMPORTED CONVERSATIONS ──")
        for conv_id, info in imports:
            ts = sorted(info["timestamps"])
            date_str = ts[0][:10] if ts else "unknown"
            topics = ", ".join(list(info["topics"])[:3])
            print(f"    {conv_id[:8]}  capsules ~{info['first_id']}-{info['last_id']}  "
                  f"({info['count']} sampled)  {date_str}  {topics}")

    if live:
        print(f"\n  ── LIVE CHRONICLE ({len(live)} sources) ──")
        source_types = defaultdict(int)
        for conv_id, info in live:
            prefix = conv_id.split("-")[0]
            source_types[prefix] += info["count"]
        for src, count in sorted(source_types.items(), key=lambda x: -x[1]):
            print(f"    {src}: {count} sampled capsules")

    print(f"\n  Total: {len(imports)} imported conversations, {len(live)} live sources")


def cmd_search(query, limit=20):
    """Search local capsule restatements, deduplicate, and link to conversation threads."""
    db = sqlite3.connect(DB_PATH, timeout=10)

    # Search by keyword matching in restatement and topic
    terms = query.split()
    where_clauses = []
    params = []
    for term in terms:
        where_clauses.append("(restatement LIKE ? OR topic LIKE ?)")
        params.extend([f"%{term}%", f"%{term}%"])

    where = " AND ".join(where_clauses)
    rows = db.execute(
        f"SELECT id, conversation_id, restatement, topic, timestamp "
        f"FROM knowledge_capsules WHERE {where} ORDER BY id",
        params
    ).fetchall()

    if not rows:
        print(f"  No capsules matching '{query}'")
        db.close()
        return

    # Deduplicate by restatement text (import artifact: same content at multiple IDs)
    seen_texts = {}
    unique = []
    for row in rows:
        cid, conv_id, restatement, topic, ts = row
        # Normalize: first 100 chars of restatement as dedup key
        key = restatement[:100].strip()
        if key not in seen_texts:
            seen_texts[key] = cid
            unique.append(row)

    # Try to resolve real conversation_ids from conversation_threads index
    thread_map = {}
    try:
        for row in unique:
            cid = row[0]
            conv_id = row[1]
            # For canister-imported capsules, extract the canister ID
            canister_id = None
            if conv_id.startswith("canister:"):
                canister_id = int(conv_id.split(":")[1])
            elif conv_id.startswith("mcp-") or conv_id.startswith("http-"):
                thread_map[cid] = conv_id[:8]
                continue

            if canister_id:
                real = db.execute(
                    "SELECT conversation_id FROM conversation_threads WHERE capsule_id = ?",
                    (canister_id,)
                ).fetchone()
                if real:
                    thread_map[cid] = real[0][:8]
                else:
                    # Find nearest indexed capsule in same range
                    near = db.execute(
                        "SELECT conversation_id FROM conversation_threads "
                        "WHERE capsule_id BETWEEN ? AND ? LIMIT 1",
                        (canister_id - 5, canister_id + 5)
                    ).fetchone()
                    thread_map[cid] = near[0][:8] if near else "?"
    except Exception:
        pass  # conversation_threads table may not exist

    # Group by conversation
    by_conv = defaultdict(list)
    for row in unique:
        cid = row[0]
        conv_key = thread_map.get(cid, row[1][:8])
        by_conv[conv_key].append(row)

    print(f"  ── Search: '{query}' — {len(unique)} unique results ({len(rows)} total, {len(rows) - len(unique)} duplicates filtered) ──\n")

    shown = 0
    for conv_key, capsules in sorted(by_conv.items(), key=lambda x: x[1][0][0]):
        if shown >= limit:
            remaining = len(unique) - shown
            if remaining > 0:
                print(f"\n  ... {remaining} more results. Use --limit N to see more.")
            break
        print(f"  [{conv_key}]")
        for cid, conv_id, restatement, topic, ts in capsules:
            if shown >= limit:
                break
            ts_str = f"  [{ts[:10]}]" if ts else ""
            topic_str = f"  ({topic})" if topic else ""
            print(f"    #{cid:>5d}  {restatement[:90]}{ts_str}{topic_str}")
            shown += 1
        print()

    db.close()


def cmd_stats():
    """Get capsule count and range info."""
    # Binary search for the max capsule ID
    lo, hi = 1, 30000
    while lo < hi:
        mid = (lo + hi + 1) // 2
        c = get_capsule(mid)
        if c:
            lo = mid
        else:
            hi = mid - 1

    # Find first capsule
    first = None
    for i in range(1, 20):
        c = get_capsule(i)
        if c:
            first = i
            break

    print(f"  First capsule: #{first}")
    print(f"  Last capsule:  ~#{lo}")
    print(f"  Estimated total: ~{lo - (first or 1) + 1}")


if __name__ == "__main__":
    args = sys.argv[1:]

    if not args:
        print(__doc__)
        sys.exit(0)

    if args[0] == "--range" and len(args) >= 3:
        cmd_range(int(args[1]), int(args[2]))
    elif args[0] == "--conversation" and len(args) >= 2:
        cmd_conversation(args[1])
    elif args[0] == "--map":
        cmd_map(full="--full" in args)
    elif args[0] == "--search" and len(args) >= 2:
        query = " ".join(args[1:])
        lim = 20
        if "--limit" in args:
            li = args.index("--limit")
            if li + 1 < len(args):
                lim = int(args[li + 1])
                query = " ".join(a for a in args[1:] if a != "--limit" and a != args[li + 1])
        cmd_search(query, limit=lim)
    elif args[0] == "--stats":
        cmd_stats()
    elif args[0].isdigit():
        cmd_read(int(args[0]))
    else:
        print(__doc__)
