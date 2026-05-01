#!/usr/bin/env python3
"""Ecosystem Connect Map — XRP & ICP entity/relationship tracker.

Maintains a lightweight graph of ecosystem participants and their relationships.
Renders text-based visualizations suitable for Discord posting.

Usage:
  eco_map.py show [--lane XRP|ICP|all]     # Render current map
  eco_map.py add LANE ENTITY [--type TYPE]  # Add entity
  eco_map.py link ENTITY1 ENTITY2 REL_TYPE [--detail "..."]  # Add relationship
  eco_map.py ingest                         # Ingest from latest collector output
  eco_map.py discord [--lane XRP|ICP|all]   # Post map to Discord
"""
import os
import sys
import json
import sqlite3
import subprocess
import re
from datetime import datetime, timezone

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"

ENTITY_TYPES = {
    "protocol": "Protocol/Chain",
    "company": "Company/Org",
    "product": "Product/Service",
    "token": "Token/Currency",
    "fund": "Fund/DAO",
    "gov": "Government/Regulator",
    "infra": "Infrastructure",
}

REL_TYPES = {
    "partners_with": "partners with",
    "builds_on": "builds on",
    "integrates": "integrates",
    "funds": "funds",
    "regulates": "regulates",
    "launches": "launches",
    "custodies": "custodies for",
    "settles_via": "settles via",
    "deploys_on": "deploys on",
}


def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS eco_entities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        lane TEXT NOT NULL,
        entity_type TEXT DEFAULT 'company',
        detail TEXT,
        first_seen INTEGER NOT NULL,
        last_seen INTEGER NOT NULL,
        mention_count INTEGER DEFAULT 1,
        UNIQUE(name, lane)
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS eco_edges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        entity1 TEXT NOT NULL,
        entity2 TEXT NOT NULL,
        lane TEXT NOT NULL,
        rel_type TEXT NOT NULL,
        detail TEXT,
        source TEXT,
        first_seen INTEGER NOT NULL,
        last_seen INTEGER NOT NULL,
        UNIQUE(entity1, entity2, lane, rel_type)
    )""")
    conn.commit()
    return conn


def _now():
    return int(datetime.now(timezone.utc).timestamp())


def add_entity(lane, name, entity_type="company", detail=None):
    conn = _db()
    now = _now()
    try:
        conn.execute(
            "INSERT INTO eco_entities (name, lane, entity_type, detail, first_seen, last_seen) VALUES (?,?,?,?,?,?)",
            (name, lane, entity_type, detail, now, now),
        )
    except sqlite3.IntegrityError:
        conn.execute(
            "UPDATE eco_entities SET last_seen=?, mention_count=mention_count+1 WHERE name=? AND lane=?",
            (now, name, lane),
        )
    conn.commit()
    conn.close()


def add_edge(entity1, entity2, lane, rel_type, detail=None, source=None):
    conn = _db()
    now = _now()
    try:
        conn.execute(
            "INSERT INTO eco_edges (entity1, entity2, lane, rel_type, detail, source, first_seen, last_seen) VALUES (?,?,?,?,?,?,?,?)",
            (entity1, entity2, lane, rel_type, detail, source, now, now),
        )
    except sqlite3.IntegrityError:
        conn.execute(
            "UPDATE eco_edges SET last_seen=?, detail=COALESCE(?,detail) WHERE entity1=? AND entity2=? AND lane=? AND rel_type=?",
            (now, detail, entity1, entity2, lane, rel_type),
        )
    conn.commit()
    conn.close()


def ingest_from_collector():
    """Run the crosschain collector and extract entities/relationships."""
    try:
        out = subprocess.run(
            ["python3", os.path.expanduser("~/.hermes/scripts/crosschain_collector.py")],
            capture_output=True, text=True, timeout=60,
        )
        text = out.stdout
    except Exception:
        return 0

    count = 0
    blocks = text.split("--- [")
    for block in blocks[1:]:
        # Extract lane
        cross_match = re.search(r"\[CROSS:\s*([^,\]]+)", block)
        if not cross_match:
            continue
        lane = cross_match.group(1).strip()
        lane_short = "XRP" if "XRP" in lane else "ICP"

        # Extract title
        lines = block.split("\n")
        title_line = lines[1] if len(lines) > 1 else ""

        # Simple entity extraction from title
        entities = _extract_entities(title_line, lane_short)
        for ent, etype in entities:
            add_entity(lane_short, ent, etype)
            count += 1

        # Simple relationship extraction
        rels = _extract_relationships(title_line, lane_short, entities)
        for e1, e2, rtype, detail in rels:
            add_edge(e1, e2, lane_short, rtype, detail=detail, source="collector")

    return count


# Known ecosystem entities for matching — curated, not regex-mined
XRP_ENTITIES = {
    "ripple": ("Ripple", "company"),
    "exodus": ("Exodus", "company"),
    "xrpl": ("XRPL", "protocol"),
    "rlusd": ("RLUSD", "token"),
    "xrp": ("XRP", "token"),
    "solana": ("Solana", "protocol"),
    "sec": ("SEC", "gov"),
    "korea": ("Korea", "gov"),
    "flare": ("Flare", "protocol"),
    "firelight": ("Firelight Finance", "product"),
    "bitgo": ("BitGo", "company"),
    "uphold": ("Uphold", "company"),
    "bitstamp": ("Bitstamp", "company"),
    "sbi": ("SBI Holdings", "company"),
    "hashkey": ("HashKey", "company"),
    "metaco": ("Metaco", "company"),
    "standard custody": ("Standard Custody", "company"),
    "xaman": ("Xaman", "product"),
    "gatehub": ("GateHub", "company"),
    "xrp ledger": ("XRPL", "protocol"),
    "tokenized": ("Tokenized Assets", "product"),
    "etf": ("XRP ETF", "product"),
}

ICP_ENTITIES = {
    "dfinity": ("DFINITY", "company"),
    "internet computer": ("Internet Computer", "protocol"),
    "icp": ("ICP", "token"),
    "caffeine": ("Caffeine AI", "product"),
    "juno": ("Juno", "product"),
    "playground": ("Motoko Playground", "infra"),
    "sns": ("SNS", "infra"),
    "nns": ("NNS", "infra"),
    "aly": ("Aly", "product"),
    "anthropic": ("Anthropic", "company"),
    "openchat": ("OpenChat", "product"),
    "sonic": ("Sonic", "product"),
    "icpswap": ("ICPSwap", "product"),
    "windoge": ("Windoge", "product"),
    "bob": ("BOB", "product"),
    "dragginz": ("Dragginz", "product"),
    "kinic": ("Kinic", "product"),
    "chain fusion": ("Chain Fusion", "infra"),
    "chain key": ("Chain Key", "infra"),
    "bitcoin integration": ("BTC on ICP", "infra"),
    "ckbtc": ("ckBTC", "token"),
    "cketh": ("ckETH", "token"),
    "modclub": ("Modclub", "product"),
    "origyn": ("ORIGYN", "company"),
    "catalyze": ("Catalyze", "product"),
    "pakistan": ("Pakistan Digital Authority", "gov"),
    "netlify": ("Netlify for Canisters", "infra"),
}


def _extract_entities(text, lane):
    """Extract only known ecosystem entities — no regex mining."""
    low = text.lower()
    found = []
    # Check both lane-specific and cross-lane entities
    entities = XRP_ENTITIES if lane == "XRP" else ICP_ENTITIES
    for key, (name, etype) in entities.items():
        if key in low and (name, etype) not in found:
            found.append((name, etype))
    return found[:8]


def _extract_relationships(text, lane, entities):
    low = text.lower()
    rels = []
    entity_names = [n for n, _ in entities]

    rel_patterns = [
        (r"partner", "partners_with"),
        (r"integrat", "integrates"),
        (r"expand|broadens|support", "integrates"),
        (r"launch|deploy|rolls out", "launches"),
        (r"pilot", "partners_with"),
        (r"adopt|select|chosen", "integrates"),
        (r"fund|grant", "funds"),
        (r"settl", "settles_via"),
        (r"custod", "custodies"),
    ]

    for pattern, rtype in rel_patterns:
        if re.search(pattern, low):
            # Link first two entities found
            if len(entity_names) >= 2:
                rels.append((entity_names[0], entity_names[1], rtype, text[:100]))
                break

    return rels


def render_map(lane_filter="all"):
    conn = _db()
    where = "" if lane_filter == "all" else f"WHERE lane='{lane_filter}'"

    entities = conn.execute(
        f"SELECT name, lane, entity_type, mention_count, last_seen FROM eco_entities {where} ORDER BY mention_count DESC"
    ).fetchall()

    edges = conn.execute(
        f"SELECT entity1, entity2, lane, rel_type, detail FROM eco_edges {where} ORDER BY last_seen DESC"
    ).fetchall()
    conn.close()

    if not entities and not edges:
        return f"Ecosystem map ({lane_filter}): empty — run `eco_map.py ingest` first."

    lines = []
    lines.append(f"```")
    lines.append(f"=== ECOSYSTEM CONNECT MAP ({lane_filter.upper()}) ===")
    lines.append(f"  {len(entities)} entities, {len(edges)} connections")
    lines.append("")

    # Group entities by lane
    by_lane = {}
    for name, lane, etype, count, last in entities:
        by_lane.setdefault(lane, []).append((name, etype, count))

    for lane in sorted(by_lane.keys()):
        lines.append(f"--- {lane} ---")
        for name, etype, count in by_lane[lane]:
            bar = "+" * min(count, 10)
            lines.append(f"  [{etype[:4]}] {name} ({count}x) {bar}")
        lines.append("")

    if edges:
        lines.append("--- CONNECTIONS ---")
        seen = set()
        for e1, e2, lane, rtype, detail in edges:
            key = f"{e1}-{e2}-{rtype}"
            if key in seen:
                continue
            seen.add(key)
            rel_label = REL_TYPES.get(rtype, rtype)
            lines.append(f"  {e1} --[{rel_label}]--> {e2}  ({lane})")
        lines.append("")

    lines.append(f"  Updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append(f"```")
    return "\n".join(lines)


def post_discord(lane_filter="all"):
    text = render_map(lane_filter)
    webhook = os.environ.get("OPERATOR_WEBHOOK", "")
    if not webhook:
        # Try loading from chronicle.env
        env_file = os.path.expanduser("~/chronicle/chronicle.env")
        if os.path.exists(env_file):
            with open(env_file) as f:
                for line in f:
                    if line.startswith("OPERATOR_WEBHOOK="):
                        webhook = line.split("=", 1)[1].strip().strip("'\"")
    if not webhook:
        print("No OPERATOR_WEBHOOK found")
        return

    import requests
    # Discord message limit
    if len(text) > 1900:
        text = text[:1897] + "..."

    resp = requests.post(webhook, json={"content": text}, timeout=15)
    print(f"Posted to Discord: {resp.status_code}")


def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return

    cmd = args[0]

    if cmd == "show":
        lane = "all"
        if "--lane" in args:
            idx = args.index("--lane")
            lane = args[idx + 1] if idx + 1 < len(args) else "all"
        print(render_map(lane))

    elif cmd == "add":
        if len(args) < 3:
            print("Usage: eco_map.py add LANE ENTITY [--type TYPE]")
            return
        lane, entity = args[1], args[2]
        etype = "company"
        if "--type" in args:
            idx = args.index("--type")
            etype = args[idx + 1] if idx + 1 < len(args) else "company"
        add_entity(lane, entity, etype)
        print(f"Added {entity} to {lane} as {etype}")

    elif cmd == "link":
        if len(args) < 4:
            print("Usage: eco_map.py link ENTITY1 ENTITY2 REL_TYPE [--detail '...']")
            return
        e1, e2, rtype = args[1], args[2], args[3]
        detail = None
        if "--detail" in args:
            idx = args.index("--detail")
            detail = args[idx + 1] if idx + 1 < len(args) else None
        lane = "XRP"  # Default, can override
        if "--lane" in args:
            idx = args.index("--lane")
            lane = args[idx + 1] if idx + 1 < len(args) else "XRP"
        add_edge(e1, e2, lane, rtype, detail=detail)
        print(f"Linked {e1} --[{rtype}]--> {e2} ({lane})")

    elif cmd == "ingest":
        count = ingest_from_collector()
        print(f"Ingested {count} entities from collector output")

    elif cmd == "discord":
        lane = "all"
        if "--lane" in args:
            idx = args.index("--lane")
            lane = args[idx + 1] if idx + 1 < len(args) else "all"
        post_discord(lane)

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
