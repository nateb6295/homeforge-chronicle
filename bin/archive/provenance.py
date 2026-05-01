#!/usr/bin/env python3
"""Provenance Tracer — follow any observation back to its origin.

Thread #191 F4 found provenance erasure: content passes through the seed pipeline
and loses source identity. This tool follows the existing metadata chains to
reconstruct where ideas actually came from.

Usage:
  python3 provenance.py trace <activity_feed_id>    — trace one entry
  python3 provenance.py diet [--hours N]             — input composition breakdown
  python3 provenance.py chain [--hours N]            — show full provenance chains
  python3 provenance.py dashboard [--hours N]        — combined view for Nate
"""

import os, sys, json, re, sqlite3
from datetime import datetime
from collections import Counter, defaultdict

DB_PATH = os.environ.get(
    "CHRONICLE_DB",
    os.path.expanduser("~/.homeforge-chronicle/processed.db"),
)

# Resolve symlinks
if os.path.islink(DB_PATH):
    DB_PATH = os.path.realpath(DB_PATH)
if not os.path.exists(DB_PATH):
    # Try HDD path directly
    DB_PATH = "/mnt/hdd/chronicle-data/processed.db"


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def trace_entry(conn, entry_id: int, depth: int = 0, max_depth: int = 10) -> list:
    """Trace an activity_feed entry back to its origin. Returns chain of dicts."""
    if depth >= max_depth:
        return [{"id": entry_id, "note": "max depth reached"}]

    cur = conn.cursor()
    cur.execute(
        "SELECT id, source, activity_type, title, substr(content, 1, 300) as content, "
        "metadata, created_at FROM activity_feed WHERE id = ?",
        (entry_id,),
    )
    row = cur.fetchone()
    if not row:
        return [{"id": entry_id, "note": "not found"}]

    entry = dict(row)
    chain = [entry]

    meta = {}
    if entry.get("metadata"):
        try:
            meta = json.loads(entry["metadata"])
        except (json.JSONDecodeError, TypeError):
            pass

    # Follow provenance based on source type
    if entry["source"] in ("seed", "falcon", "gemma") and entry["activity_type"] in ("think", "deep"):
        # Seed entries have original_source and original_content in metadata
        original_source = meta.get("original_source", "")
        original_content = meta.get("original_content", "")

        if original_source.startswith("activity:intern:") or original_source.startswith("activity:provocateur:") or original_source.startswith("activity:"):
            # Fast path: use upstream_id if available (added by provenance patch)
            upstream_id = meta.get("upstream_id")
            if upstream_id:
                chain.extend(trace_entry(conn, int(upstream_id), depth + 1, max_depth))
                return chain

            # Slow path: content match for intern briefs
            snippet = original_content[:80] if original_content else ""
            if snippet:
                # Escape SQL LIKE wildcards and use a safe substring
                safe_snip = snippet[:50].replace("%", "").replace("_", "").replace("'", "")
                if len(safe_snip) > 20:
                    # Try content match first, then title match
                    cur.execute(
                        "SELECT id FROM activity_feed WHERE source='intern' "
                        "AND activity_type='brief' AND (content LIKE ? OR title LIKE ?) "
                        "AND created_at <= ? ORDER BY created_at DESC LIMIT 1",
                        (f"%{safe_snip}%", f"%{safe_snip}%", entry["created_at"]),
                    )
                    match = cur.fetchone()
                    if match:
                        chain.extend(trace_entry(conn, match["id"], depth + 1, max_depth))
                        return chain
            # Fallback: unlinked
            chain.append({
                "source": original_source,
                "content": original_content[:200] if original_content else "?",
                "note": "intern brief (unlinked)",
            })
        elif original_source:
            # Direct source (capture, mqtt, etc.)
            chain.append({
                "source": original_source,
                "content": original_content[:200] if original_content else "?",
                "note": "terminal source",
            })

    elif entry["source"] == "intern" and entry["activity_type"] == "brief":
        # Intern briefs have input_id and source_path in metadata
        input_id = meta.get("input_id", "")
        source_path = meta.get("source_path", "")

        if input_id.startswith("seed:"):
            # Came from seed think/deep — follow back
            seed_af_id = input_id.split(":")[-1]
            try:
                chain.extend(trace_entry(conn, int(seed_af_id), depth + 1, max_depth))
            except (ValueError, TypeError):
                chain.append({"source": f"seed:{seed_af_id}", "note": "seed entry (parse error)"})
        elif input_id.startswith("explore:"):
            # Came from feed-explore — terminal source
            chain.append({
                "source": source_path or input_id,
                "input_id": input_id,
                "note": "feed article (terminal)",
            })
        elif input_id.startswith("crossref:"):
            # Came from crossref
            crossref_af_id = input_id.split(":")[-1]
            try:
                chain.extend(trace_entry(conn, int(crossref_af_id), depth + 1, max_depth))
            except (ValueError, TypeError):
                chain.append({"source": f"crossref:{crossref_af_id}", "note": "crossref entry"})
        else:
            chain.append({
                "source": source_path or input_id or "unknown",
                "note": "terminal",
            })

    return chain


def classify_origin(chain: list) -> str:
    """Classify a provenance chain by its terminal origin."""
    if len(chain) <= 1:
        entry = chain[0]
        source = entry.get("source", "")
        if "nate" in source or "capture" in source:
            return "nate"
        if "feed" in source:
            return "external"
        if "intern" in source:
            return "intern"
        if any(s in source for s in ("seed", "falcon", "gemma")):
            return "self-generated"
        return source

    terminal = chain[-1]
    source = terminal.get("source", "")
    note = terminal.get("note", "")

    if "feed" in source or "feed" in note:
        return "external:feed"
    if "nate" in source or "capture" in source:
        return "nate"
    if "mqtt" in source:
        return "external:mqtt"
    if "provocateur" in source:
        return "self:provocateur"
    if "canister" in source:
        return "self:canister"
    if "intern" in source:
        return "self:intern"
    if any(s in source for s in ("seed", "falcon", "gemma")):
        return "self:seed"
    if "crossref" in source:
        return "self:crossref"
    if "discord" in source:
        return "nate"
    if "family" in source or "dinner" in source:
        return "self:family"
    return source or "unknown"


def cmd_trace(entry_id: int):
    """Trace a single entry."""
    conn = get_db()
    chain = trace_entry(conn, entry_id)
    conn.close()

    print(f"\n  Provenance chain for activity_feed #{entry_id}:")
    print(f"  {'─' * 60}")
    for i, link in enumerate(chain):
        prefix = "  → " if i > 0 else "  ◉ "
        source = link.get("source", "?")
        atype = link.get("activity_type", "")
        title = link.get("title", "")[:80]
        content = link.get("content", "")[:100]
        note = link.get("note", "")
        aid = link.get("id", "")

        label = f"{source}"
        if atype:
            label += f":{atype}"
        if aid:
            label = f"[#{aid}] {label}"

        print(f"{prefix}{label}")
        if title:
            print(f"      {title}")
        elif content:
            print(f"      {content[:80]}")
        if note:
            print(f"      ({note})")
    print()


def cmd_diet(hours: int = 6):
    """Show input composition breakdown."""
    conn = get_db()
    cur = conn.cursor()

    # Get all seed think/deep entries in window
    cur.execute(
        "SELECT id, source, activity_type, metadata, created_at FROM activity_feed "
        "WHERE source='seed' AND activity_type IN ('think', 'deep') "
        f"AND created_at > strftime('%s','now','-{hours} hours') "
        "ORDER BY created_at DESC",
    )
    rows = cur.fetchall()

    origins = Counter()
    chains_by_origin = defaultdict(list)

    for row in rows:
        chain = trace_entry(conn, row["id"], max_depth=5)
        origin = classify_origin(chain)
        origins[origin] += 1
        chains_by_origin[origin].append(chain)

    conn.close()

    total = sum(origins.values())
    if total == 0:
        print(f"\n  No seed think/deep entries in last {hours}h.")
        return

    # Categorize into self vs external vs nate
    self_count = sum(v for k, v in origins.items() if k.startswith("self"))
    external_count = sum(v for k, v in origins.items() if k.startswith("external"))
    nate_count = origins.get("nate", 0)
    other_count = total - self_count - external_count - nate_count

    print(f"\n  ═══ Input Diet — last {hours}h ═══")
    print(f"  Total seed think/deep entries: {total}")
    print(f"  {'─' * 40}")
    print(f"  Self-generated:  {self_count:>4} ({100*self_count/total:.1f}%)")
    print(f"  External:        {external_count:>4} ({100*external_count/total:.1f}%)")
    print(f"  Nate:            {nate_count:>4} ({100*nate_count/total:.1f}%)")
    if other_count:
        print(f"  Other:           {other_count:>4} ({100*other_count/total:.1f}%)")
    print(f"  {'─' * 40}")
    print(f"\n  Breakdown:")
    for origin, count in origins.most_common():
        bar = "█" * int(30 * count / total)
        print(f"    {origin:<25} {count:>4} ({100*count/total:.1f}%) {bar}")
    print()


def cmd_chain(hours: int = 2):
    """Show recent provenance chains."""
    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        "SELECT id FROM activity_feed "
        "WHERE source='seed' AND activity_type IN ('think', 'deep') "
        f"AND created_at > strftime('%s','now','-{hours} hours') "
        "ORDER BY created_at DESC LIMIT 15",
    )
    rows = cur.fetchall()

    for row in rows:
        chain = trace_entry(conn, row["id"], max_depth=5)
        origin = classify_origin(chain)

        # Compact one-line format
        parts = []
        for link in chain:
            source = link.get("source", "?")
            aid = link.get("id", "")
            label = f"{source}" + (f"#{aid}" if aid else "")
            parts.append(label)

        chain_str = " → ".join(parts)
        print(f"  [{origin:<20}] {chain_str}")

    conn.close()


def cmd_dashboard(hours: int = 6):
    """Combined view: diet + recent chains + self-reference score."""
    cmd_diet(hours)
    print(f"\n  ═══ Recent Chains (last 2h) ═══")
    cmd_chain(min(hours, 2))

    # Self-reference trend
    conn = get_db()
    cur = conn.cursor()

    # Compare current period vs previous period
    for label, window in [("Current", f"-{hours} hours"), ("Previous", f"-{hours*2} hours")]:
        if label == "Previous":
            cur.execute(
                "SELECT COUNT(*) as total FROM activity_feed "
                "WHERE source='seed' AND activity_type IN ('think', 'deep') "
                f"AND created_at > strftime('%s','now','{window}') "
                f"AND created_at <= strftime('%s','now','-{hours} hours')",
            )
        else:
            cur.execute(
                "SELECT COUNT(*) as total FROM activity_feed "
                "WHERE source='seed' AND activity_type IN ('think', 'deep') "
                f"AND created_at > strftime('%s','now','{window}')",
            )
    conn.close()
    print()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "trace" and len(sys.argv) >= 3:
        cmd_trace(int(sys.argv[2]))
    elif cmd == "diet":
        hours = 6
        if "--hours" in sys.argv:
            idx = sys.argv.index("--hours")
            hours = int(sys.argv[idx + 1])
        cmd_diet(hours)
    elif cmd == "chain":
        hours = 2
        if "--hours" in sys.argv:
            idx = sys.argv.index("--hours")
            hours = int(sys.argv[idx + 1])
        cmd_chain(hours)
    elif cmd == "dashboard":
        hours = 6
        if "--hours" in sys.argv:
            idx = sys.argv.index("--hours")
            hours = int(sys.argv[idx + 1])
        cmd_dashboard(hours)
    else:
        print(__doc__)
        sys.exit(1)
