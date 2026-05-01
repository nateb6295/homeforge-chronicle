#!/usr/bin/env python3
"""adoption_ingest.py — Auto-extract adoption edges from new captures.

Scans recent activity_feed entries for blockchain partnership/adoption signals
and populates the adoption_edges table. Designed to run as a cron or on-demand.

Usage:
    python3 adoption_ingest.py              # Scan last 2h, insert edges
    python3 adoption_ingest.py --since 24h  # Scan last 24h
    python3 adoption_ingest.py --dry        # Show candidates without inserting
    python3 adoption_ingest.py --all        # Scan all unprocessed captures
"""

import json
import os
import re
import sqlite3
import sys
import time
import urllib.request

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA = "http://192.168.1.11:11434"
MODEL = "qwen2.5:3b"
STATE_FILE = os.path.expanduser("~/chronicle/adoption_ingest_state.json")

# Keywords that signal an adoption/partnership item
SIGNAL_KEYWORDS = [
    "ripple", "xrp", "xrpl", "rlusd", "icp", "dfinity", "internet computer",
    "partner", "acqui", "integrat", "launch", "deploy", "adopt", "custod",
    "tokeniz", "blockchain bond", "stablecoin", "cross-border", "remittance",
    "subnet", "canister", "chain fusion",
]

# Keywords that signal a security incident / risk event
RISK_KEYWORDS = [
    "exploit", "hack", "breach", "stolen", "drained", "attack",
    "vulnerability", "rug pull", "rugpull", "flash loan",
    "compromised", "malicious", "unauthorized", "loss",
]

# Chains to track for risk signals
RISK_CHAINS = {
    "sui": "SUI", "solana": "Solana", "ethereum": "Ethereum",
    "bsc": "BSC", "binance smart chain": "BSC", "bnb chain": "BSC",
    "polygon": "Polygon", "avalanche": "Avalanche", "arbitrum": "Arbitrum",
    "optimism": "Optimism", "base chain": "Base", "fantom": "Fantom",
    "near": "NEAR", "aptos": "Aptos", "ton": "TON",
}

# Known DeFi protocols → their chain (for risk detection when headline omits chain)
RISK_PROTOCOLS = {
    "kelp dao": "Ethereum", "kelp": "Ethereum",
    "drift": "Solana", "drift protocol": "Solana",
    "aave": "Ethereum", "uniswap": "Ethereum", "compound": "Ethereum",
    "maker": "Ethereum", "makerdao": "Ethereum", "lido": "Ethereum",
    "curve": "Ethereum", "convex": "Ethereum", "yearn": "Ethereum",
    "cetus": "SUI", "nemo": "SUI", "volo": "SUI",
    "raydium": "Solana", "marinade": "Solana", "jito": "Solana",
    "orca": "Solana", "jupiter": "Solana",
    "pancakeswap": "BSC", "venus": "BSC",
    "trader joe": "Avalanche", "benqi": "Avalanche",
    "gmx": "Arbitrum", "camelot": "Arbitrum",
    "velodrome": "Optimism",
    "aerodrome": "Base",
}

LANE_KEYWORDS = {
    "XRP-ADOPT": ["ripple", "xrp", "xrpl", "rlusd", "sbi", "hidden road",
                   "metaco", "palisade", "gtreasu", "rail ", "fortress trust",
                   "standard custody"],
    "ICP-ADOPT": ["icp", "dfinity", "internet computer", "canister",
                   "chain fusion", "subnet", "sns"],
}

EXTRACT_PROMPT_TEMPLATE = (
    'You are extracting blockchain adoption/partnership data from a text.\n'
    'Extract entities involved and their relationship. Return ONLY valid JSON, nothing else.\n\n'
    'Format:\n'
    '{{"edges": [\n'
    '  {{"subject": "Entity A", "predicate": "partners_with|integrates|launches|deploys|operates_in", "object": "Entity B", "geo": "country or empty", "summary": "one line"}}\n'
    ']}}\n\n'
    'Rules:\n'
    '- subject and object should be proper entity names (companies, protocols, countries)\n'
    '- Use "partners_with" for partnerships/collaborations\n'
    '- Use "integrates" for acquisitions or technology integration\n'
    '- Use "launches" for new products/services\n'
    '- Use "operates_in" for geographic expansion\n'
    '- Use "deploys" for infrastructure deployment\n'
    '- If no clear adoption/partnership signal, return {{"edges": []}}\n'
    '- Maximum 3 edges per text\n\n'
    'Text to analyze:\n'
    '{text}'
)


def _db():
    return sqlite3.connect(DB_PATH, timeout=10)


def _load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"last_processed_id": 0}


def _save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def _has_signal(text):
    """Check if text contains adoption/partnership keywords."""
    lower = text.lower()
    return sum(1 for kw in SIGNAL_KEYWORDS if kw in lower) >= 2


def _has_risk_signal(text):
    """Check if text contains blockchain security incident keywords."""
    lower = text.lower()
    risk_hits = sum(1 for kw in RISK_KEYWORDS if kw in lower)
    chain_hits = sum(1 for kw in RISK_CHAINS if kw in lower)
    proto_hits = sum(1 for kw in RISK_PROTOCOLS if kw in lower)
    return risk_hits >= 1 and (chain_hits >= 1 or proto_hits >= 1)


def _detect_risk_chain(text):
    """Detect which blockchain is involved in a risk event."""
    lower = text.lower()
    # Direct chain mention takes priority
    for kw, name in RISK_CHAINS.items():
        if kw in lower:
            return name
    # Fall back to protocol→chain mapping
    for kw, chain in RISK_PROTOCOLS.items():
        if kw in lower:
            return chain
    return None


def _detect_lane(text):
    """Detect which adoption lane this belongs to."""
    lower = text.lower()
    # Check for risk signal first
    if _has_risk_signal(text):
        return "risk-signal"
    xrp_score = sum(1 for kw in LANE_KEYWORDS["XRP-ADOPT"] if kw in lower)
    icp_score = sum(1 for kw in LANE_KEYWORDS["ICP-ADOPT"] if kw in lower)
    if xrp_score > icp_score:
        return "XRP-ADOPT"
    elif icp_score > xrp_score:
        return "ICP-ADOPT"
    elif xrp_score > 0:
        return "XRP-ADOPT"
    return None


def _extract_risk_edges(text):
    """Extract security incident edges from text."""
    lower = text.lower()
    edges = []

    chain = _detect_risk_chain(text)
    if not chain:
        return []

    # Try to extract protocol name — look for capitalized words before
    # exploit/hack keywords, or "X Protocol", "X Finance", etc.
    protocol = None
    protocol_patterns = [
        r'(\b[A-Z][a-zA-Z]+(?:\s+(?:Protocol|Finance|DEX|Swap|Network|Labs|DAO))?)\s+(?:confirm|suffer|exploit|hack|breach|drain|attack)',
        r'(?:exploit|hack|breach|drain|attack)\s+(?:on|at|of)\s+(\b[A-Z][a-zA-Z]+(?:\s+(?:Protocol|Finance|DEX|Swap|Network|Labs|DAO))?)',
    ]
    for pat in protocol_patterns:
        m = re.search(pat, text)
        if m:
            candidate = m.group(1).strip()
            # Skip if it's just the chain name
            if candidate.lower() not in RISK_CHAINS:
                protocol = candidate
                break

    # Extract dollar amounts
    amount_match = re.search(r'\$([0-9,.]+)\s*([MBK])', text)
    amount_str = ""
    if amount_match:
        amount_str = f"${amount_match.group(1)}{amount_match.group(2)}"

    summary = text[:120].strip()
    if amount_str:
        summary = f"{amount_str} — {summary}"

    subject = protocol or "Unknown Protocol"
    edges.append({
        "subject": subject,
        "predicate": "exploited_on",
        "object": chain,
        "geo": "",
        "summary": summary,
    })
    return edges[:1]


def _extract_edges(text):
    """Pattern-based edge extraction — no LLM needed.

    Looks for known entity names + action verbs to construct edges.
    More reliable and faster than small-model extraction.
    """
    lower = text.lower()
    edges = []

    # Known entities to look for
    ENTITIES = {
        "ripple": "Ripple", "xrp": "XRP", "xrpl": "XRP Ledger",
        "xrp ledger": "XRP Ledger", "rlusd": "RLUSD",
        "sbi": "SBI Holdings", "kyobo": "Kyobo Life Insurance",
        "hidden road": "Hidden Road", "metaco": "Metaco",
        "palisade": "Palisade", "gtreasu": "GTreasury (Ripple Prime)",
        "fortress trust": "Fortress Trust",
        "standard custody": "Standard Custody",
        "santander": "Santander", "swift": "SWIFT",
        "circle": "Circle (USDC)", "usdc": "Circle (USDC)",
        "icp": "Internet Computer", "dfinity": "DFINITY",
        "internet computer": "Internet Computer",
        "bitnomial": "Bitnomial", "kraken": "Kraken",
        "immunefi": "Immunefi", "sherlock": "Sherlock (Audit)",
        "coinbase": "Coinbase", "binance": "Binance",
        "mastercard": "Mastercard", "visa": "Visa",
        "paypal": "PayPal", "stripe": "Stripe",
        # Banking partners
        "bny mellon": "BNY Mellon", "standard chartered": "Standard Chartered",
        "mufg": "MUFG", "westpac": "Westpac", "dbs": "DBS",
        "societe generale": "Societe Generale", "ubs": "UBS",
        "deutsche bank": "Deutsche Bank", "bbva": "BBVA",
        "moneygram": "MoneyGram", "american express": "American Express",
        "amex": "American Express",
        # Exchanges / infra
        "bitstamp": "Bitstamp", "uphold": "Uphold", "moonpay": "MoonPay",
        "ledger": "Ledger",
        # ICP ecosystem
        "openchat": "OpenChat", "icpswap": "ICPSwap",
        "bitfinity": "Bitfinity EVM", "caffeine ai": "Caffeine AI",
        "chain fusion": "Internet Computer",
        "zcloaknetwork": "zCloakNetwork", "zcloak": "zCloakNetwork",
        "origyn": "ORIGYN", "dscvr": "DSCVR",
        "modclub": "Modclub", "kinic": "Kinic",
        "gold dao": "Gold DAO",
        # Regulatory
        "fednow": "FedNow", "nydfs": "NYDFS", "occ": "OCC",
        "genius act": "GENIUS Act",
        # Geographic (only match with "in" prefix handled by geo detection)
        "archax": "Archax", "meld gold": "Meld Gold",
    }

    # Action patterns
    PARTNER_WORDS = ["partner", "collaborat", "alliance", "agreement", "deal"]
    INTEGRATE_WORDS = ["acquir", "integrat", "bought", "purchase", "merger"]
    LAUNCH_WORDS = ["launch", "introduc", "announc", "release", "unveil"]
    DEPLOY_WORDS = ["deploy", "expand", "roll out", "pilot"]
    GEO_WORDS = ["in japan", "in korea", "in india", "in us ", "in europe",
                  "in singapore", "in switzerland", "in uae", "in china",
                  "in brazil", "in australia", "in uk ", "in pakistan",
                  "japan", "korea", "india", "singapore", "switzerland"]

    GEO_MAP = {
        "japan": "Japan", "korea": "Korea", "india": "India",
        "us ": "United States", "united states": "United States",
        "europe": "Europe", "singapore": "Singapore",
        "switzerland": "Switzerland", "uae": "UAE", "china": "China",
        "brazil": "Brazil", "australia": "Australia", "uk ": "United Kingdom",
        "pakistan": "Pakistan",
    }

    # Find all entities mentioned in text
    found = []
    for key, name in ENTITIES.items():
        if key in lower:
            found.append(name)

    if len(found) < 2:
        return []

    # Detect relationship type
    pred = "partners_with"  # default
    for w in INTEGRATE_WORDS:
        if w in lower:
            pred = "integrates"
            break
    for w in LAUNCH_WORDS:
        if w in lower:
            pred = "launches"
            break
    for w in PARTNER_WORDS:
        if w in lower:
            pred = "partners_with"
            break

    # Detect geo
    geo = ""
    for key, name in GEO_MAP.items():
        if key in lower:
            geo = name
            break

    # Self-referencing pairs — these are the same entity or co-referent names
    # (NOT acquisitions, which are legitimate edges)
    SELF_REFS = {
        frozenset({"Ripple", "XRP"}),
        frozenset({"Ripple", "XRP Ledger"}),
        frozenset({"XRP", "XRP Ledger"}),
        frozenset({"XRP Ledger", "Ledger"}),  # Ledger (hw wallet) ≠ XRP Ledger
        frozenset({"DFINITY", "Internet Computer"}),
    }

    def _same_family(a, b):
        return frozenset({a, b}) in SELF_REFS

    # Generate edges between found entities (first entity as subject)
    # Prefer Ripple/DFINITY as subject if present
    subject = None
    for pref in ["Ripple", "DFINITY", "Internet Computer", "XRP Ledger"]:
        if pref in found:
            subject = pref
            break
    if not subject:
        subject = found[0]

    summary = text[:120].strip()
    for entity in found[1:4]:  # max 3 edges
        if entity != subject and not _same_family(subject, entity):
            edges.append({
                "subject": subject, "predicate": pred,
                "object": entity, "geo": geo,
                "summary": summary
            })

    return edges[:3]


def _edge_exists(db, subject, predicate, obj):
    """Check if this edge already exists."""
    row = db.execute(
        "SELECT COUNT(*) AS n FROM adoption_edges WHERE subject=? AND predicate=? AND object=?",
        (subject, predicate, obj)
    ).fetchone()
    return row[0] > 0


def scan_and_ingest(since_seconds=7200, dry_run=False, use_all=False):
    """Scan activity_feed for adoption signals and extract edges."""
    db = _db()
    state = _load_state()

    if use_all:
        cutoff = state.get("last_processed_id", 0)
        rows = db.execute(
            "SELECT id, source, title, content FROM activity_feed WHERE id > ? ORDER BY id ASC",
            (cutoff,)
        ).fetchall()
    else:
        cutoff_ts = int(time.time()) - since_seconds
        rows = db.execute(
            "SELECT id, source, title, content FROM activity_feed WHERE created_at > ? ORDER BY id ASC",
            (cutoff_ts,)
        ).fetchall()

    # Sources to skip — these mention XRP/ICP in monitoring/summary data, not original captures
    SKIP_SOURCES = {"sentinel", "gemma", "hal", "eye", "discord:opus", "opus"}

    candidates = []
    for row in rows:
        rid, source, title, content = row
        # Skip monitoring/system sources
        if any(source.startswith(s) for s in SKIP_SOURCES):
            continue
        text = f"{title or ''} {content}"
        if _has_signal(text) or _has_risk_signal(text):
            candidates.append((rid, source, title, content))

    print(f"Scanned {len(rows)} items, {len(candidates)} have adoption signals")

    new_edges = 0
    skipped = 0
    max_id = state.get("last_processed_id", 0)

    for rid, source, title, content in candidates:
        max_id = max(max_id, rid)
        text = f"{title or ''}\n{content[:1000]}"
        lane = _detect_lane(text)
        if not lane:
            continue

        print(f"\n  [{source}] {(title or content[:60])[:60]}...")
        if lane == "risk-signal":
            edges = _extract_risk_edges(text)
        else:
            edges = _extract_edges(text)

        for edge in edges:
            subj = edge.get("subject", "").strip()
            pred = edge.get("predicate", "").strip()
            obj = edge.get("object", "").strip()
            geo = edge.get("geo", "").strip()
            summary = edge.get("summary", "").strip()

            if not subj or not pred or not obj:
                continue
            if pred not in ("partners_with", "integrates", "launches", "deploys", "operates_in", "exploited_on"):
                pred = "partners_with"

            if _edge_exists(db, subj, pred, obj):
                skipped += 1
                print(f"    SKIP (exists): ({subj}) -[{pred}]-> ({obj})")
                continue

            if dry_run:
                print(f"    DRY: ({subj}) -[{pred}]-> ({obj}) [{lane}] {geo}")
            else:
                db.execute(
                    "INSERT INTO adoption_edges (subject, predicate, object, lane, geo, source_title, confidence, created_at) VALUES (?,?,?,?,?,?,?,?)",
                    (subj, pred, obj, lane, geo, summary, 0.7, int(time.time()))
                )
                db.commit()
                print(f"    ADD: ({subj}) -[{pred}]-> ({obj}) [{lane}] {geo}")
                new_edges += 1

    if not dry_run:
        state["last_processed_id"] = max_id
        state["last_run"] = int(time.time())
        _save_state(state)

    db.close()
    print(f"\nDone: {new_edges} new edges, {skipped} duplicates skipped")
    return new_edges


if __name__ == "__main__":
    dry = "--dry" in sys.argv
    use_all = "--all" in sys.argv
    since = 7200  # default 2h

    for arg in sys.argv[1:]:
        if arg.startswith("--since"):
            idx = sys.argv.index(arg)
            if idx + 1 < len(sys.argv):
                val = sys.argv[idx + 1]
                units = {"h": 3600, "d": 86400, "w": 604800}
                try:
                    since = int(val[:-1]) * units.get(val[-1], 3600)
                except:
                    pass

    scan_and_ingest(since_seconds=since, dry_run=dry, use_all=use_all)
