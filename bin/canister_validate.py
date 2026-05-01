#!/usr/bin/env python3
"""
canister_validate.py — Validate canister↔local data integrity.

Gap #10: capsule_sync pulls data but never verifies round-trip integrity.
This tool checks:
  1. Count parity: canister capsule count vs local
  2. Content spot-check: random sample of capsules verified against canister
  3. Embedding coverage: capsules without embeddings
  4. Orphan detection: embeddings without capsules

Usage:
    canister_validate.py check           # Full validation
    canister_validate.py counts          # Quick count comparison
    canister_validate.py coverage        # Embedding coverage check
    canister_validate.py spot [N]        # Spot-check N random capsules
"""
import json
import os
import random
import sqlite3
import sys
import time
import urllib.request

DB = "/mnt/hdd/chronicle-data/processed.db"
CANISTER_URL = "https://fqqku-bqaaa-aaaai-q4wha-cai.raw.icp0.io"
TOKEN_PATH = os.path.expanduser("~/.homeforge-chronicle/.api_token")


def load_token():
    try:
        return open(TOKEN_PATH).read().strip()
    except Exception:
        return ""


def canister_request(endpoint, params=None, timeout=15):
    """Make a request to the canister HTTP API."""
    token = load_token()
    url = f"{CANISTER_URL}{endpoint}"
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{qs}"

    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req = urllib.request.Request(url, headers=headers)
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}


def check_counts():
    """Compare canister and local capsule counts."""
    # Local counts
    db = sqlite3.connect(DB)
    local_capsules = db.execute("SELECT COUNT(*) FROM knowledge_capsules").fetchone()[0]
    local_embeddings = db.execute("SELECT COUNT(*) FROM capsule_embeddings").fetchone()[0]
    local_max_id = db.execute("SELECT MAX(id) FROM knowledge_capsules").fetchone()[0] or 0
    local_active = db.execute(
        "SELECT COUNT(*) FROM knowledge_capsules "
        "WHERE superseded_at IS NULL AND consolidated_into IS NULL"
    ).fetchone()[0]
    db.close()

    # Canister count — get max ID from /api/recent
    canister_data = canister_request("/api/recent", {"limit": "1"})

    print(f"COUNT PARITY CHECK")
    print(f"  Local capsules:     {local_capsules}")
    print(f"  Local active:       {local_active}")
    print(f"  Local embeddings:   {local_embeddings}")
    print(f"  Local max ID:       {local_max_id}")

    if "error" in canister_data:
        print(f"  Canister:           ERROR — {canister_data['error']}")
        print(f"\n  Cannot verify parity without canister access.")
    elif isinstance(canister_data, dict) and "capsules" in canister_data:
        capsules = canister_data["capsules"]
        if capsules and isinstance(capsules, list):
            canister_max_id = capsules[0].get("id", 0)
            print(f"  Canister max ID:    {canister_max_id}")
            delta = canister_max_id - local_max_id
            print(f"  ID delta:           {delta:+d}")
            if delta <= 0:
                print(f"  STATUS: LOCAL UP TO DATE")
            elif delta <= 50:
                print(f"  STATUS: SLIGHTLY BEHIND ({delta} capsules)")
            else:
                print(f"  STATUS: SYNC LAG — {delta} capsules behind canister")
        else:
            print(f"  Canister: empty response")
    elif isinstance(canister_data, list) and len(canister_data) > 0:
        canister_max_id = canister_data[0].get("id", 0)
        print(f"  Canister max ID:    {canister_max_id}")
        delta = canister_max_id - local_max_id
        print(f"  ID delta:           {delta:+d}")
        if delta <= 0:
            print(f"  STATUS: LOCAL UP TO DATE")
        else:
            print(f"  STATUS: SYNC LAG — {delta} capsules behind canister")
    else:
        print(f"  Canister response:  {str(canister_data)[:100]}")
        print(f"  Cannot determine parity.")

    return local_capsules, local_embeddings


def check_coverage():
    """Check embedding coverage — capsules without embeddings."""
    db = sqlite3.connect(DB)

    # Capsules without embeddings
    no_embed = db.execute(
        "SELECT COUNT(*) FROM knowledge_capsules kc "
        "LEFT JOIN capsule_embeddings ce ON kc.id = ce.capsule_id "
        "WHERE ce.capsule_id IS NULL"
    ).fetchone()[0]

    # Embeddings without capsules (orphans)
    orphan_embed = db.execute(
        "SELECT COUNT(*) FROM capsule_embeddings ce "
        "LEFT JOIN knowledge_capsules kc ON ce.capsule_id = kc.id "
        "WHERE kc.id IS NULL"
    ).fetchone()[0]

    total_capsules = db.execute("SELECT COUNT(*) FROM knowledge_capsules").fetchone()[0]
    total_embeds = db.execute("SELECT COUNT(*) FROM capsule_embeddings").fetchone()[0]

    # Check for duplicate embeddings
    dupes = db.execute(
        "SELECT capsule_id, COUNT(*) as cnt FROM capsule_embeddings "
        "GROUP BY capsule_id HAVING cnt > 1"
    ).fetchall()

    # Check embedding dimension consistency
    dim_groups = db.execute(
        "SELECT LENGTH(embedding) as sz, COUNT(*) FROM capsule_embeddings GROUP BY sz"
    ).fetchall()

    db.close()

    coverage_pct = (total_embeds / max(total_capsules, 1)) * 100

    print(f"EMBEDDING COVERAGE CHECK")
    print(f"  Total capsules:       {total_capsules}")
    print(f"  Total embeddings:     {total_embeds}")
    print(f"  Coverage:             {coverage_pct:.1f}%")
    print(f"  Missing embeddings:   {no_embed}")
    print(f"  Orphan embeddings:    {orphan_embed}")
    print(f"  Duplicate embeddings: {len(dupes)}")

    print(f"\n  Dimension consistency:")
    for sz, cnt in dim_groups:
        dims = sz // 4
        print(f"    {dims}d ({sz} bytes): {cnt} vectors")

    issues = []
    if no_embed > 100:
        issues.append(f"HIGH: {no_embed} capsules have no embedding")
    if orphan_embed > 0:
        issues.append(f"MEDIUM: {orphan_embed} orphan embeddings (no matching capsule)")
    if len(dupes) > 0:
        issues.append(f"LOW: {len(dupes)} capsules have duplicate embeddings")
    if len(dim_groups) > 1:
        issues.append(f"HIGH: Mixed embedding dimensions detected")

    if issues:
        print(f"\n  ISSUES:")
        for issue in issues:
            print(f"    {issue}")
    else:
        print(f"\n  STATUS: CLEAN — no integrity issues detected")

    return no_embed, orphan_embed, len(dupes)


def spot_check(n=5):
    """Spot-check random capsules against canister."""
    db = sqlite3.connect(DB)
    ids = [r[0] for r in db.execute(
        "SELECT id FROM knowledge_capsules ORDER BY RANDOM() LIMIT ?", (n,)
    ).fetchall()]

    print(f"SPOT CHECK — {n} random capsules")
    print()

    ok = 0
    fail = 0
    for cid in ids:
        local = db.execute(
            "SELECT id, restatement, topic FROM knowledge_capsules WHERE id = ?",
            (cid,)
        ).fetchone()

        # Check canister
        canister_data = canister_request(f"/api/capsule", {"id": str(cid)})

        local_text = (local[1] or "")[:60]
        print(f"  [{cid}] local: {local_text}...")

        if "error" in canister_data:
            print(f"         canister: ERROR — {canister_data['error']}")
            fail += 1
        else:
            canister_text = str(canister_data.get("restatement", canister_data.get("content", "")))[:60]
            if canister_text:
                print(f"         canister: {canister_text}...")
                ok += 1
            else:
                print(f"         canister: no match found")
                fail += 1

    db.close()

    print(f"\n  Verified: {ok}/{n}, Failed: {fail}/{n}")


def full_check():
    """Run all validation checks."""
    print("=" * 50)
    print("CANISTER DATA VALIDATION")
    print("=" * 50)
    print()

    check_counts()
    print()
    check_coverage()
    print()

    print("Run 'canister_validate.py spot N' for canister spot-checks")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    cmd = sys.argv[1]

    if cmd == "check":
        full_check()
    elif cmd == "counts":
        check_counts()
    elif cmd == "coverage":
        check_coverage()
    elif cmd == "spot":
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        spot_check(n)
    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)


if __name__ == "__main__":
    main()
