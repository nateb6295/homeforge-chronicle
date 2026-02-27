#!/usr/bin/env python3
"""
Capsule Distillation — Phase 1: Export all capsules from ICP canister.

Runs on AGX Orin where ic-py and dfx identity are configured.
Exports all ~5,103 knowledge capsules to ~/capsule_export.json.

Usage:
    python3 /tmp/export_capsules.py                 # ic-py (fast)
    python3 /tmp/export_capsules.py --http           # HTTP API fallback
    python3 /tmp/export_capsules.py --workers 20     # adjust concurrency
"""

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

CANISTER_ID = "fqqku-bqaaa-aaaai-q4wha-cai"
HTTP_BASE = f"https://{CANISTER_ID}.raw.icp0.io"
OUTPUT_PATH = os.path.expanduser("~/capsule_export.json")


def export_via_icpy():
    """Export using ic-py native library (fastest, no HTTP overhead)."""
    print("[ic-py] Initializing ICP agent...")
    try:
        from ic.canister import Canister
        from ic.client import Client
        from ic.identity import Identity
        from ic.agent import Agent
    except ImportError:
        print("[ic-py] ic-py not installed. Use --http for HTTP API fallback.")
        return None

    pem_path = os.path.expanduser("~/.config/dfx/identity/chronicle-auto/identity.pem")
    did_path = os.path.expanduser("~/chronicle/chronicle_backend.did")
    # Fallback DID paths
    if not os.path.isfile(did_path):
        did_path = os.path.expanduser("~/.homeforge-chronicle/chronicle_backend.did")
    if not os.path.isfile(did_path):
        print(f"[ic-py] DID file not found. Tried ~/chronicle/ and ~/.homeforge-chronicle/")
        return None

    if not os.path.isfile(pem_path):
        print(f"[ic-py] PEM file not found at {pem_path}")
        return None

    with open(pem_path, 'r') as f:
        pem_data = f.read()
    ident = Identity.from_pem(pem_data)

    with open(did_path, 'r') as f:
        did = f.read()

    client = Client(url='https://ic0.app')
    agent = Agent(ident, client)
    canister = Canister(agent=agent, canister_id=CANISTER_ID, candid=did)

    # Get total count
    result = canister.get_capsule_count()
    total = result[0] if result else 0
    print(f"[ic-py] Canister reports {total} capsules")

    if total == 0:
        print("[ic-py] No capsules found!")
        return []

    # Fetch capsules by ID
    capsules = []
    errors = 0
    start = time.time()

    for i in range(1, total + 1):
        try:
            result = canister.get_capsule(i)
            if result and result[0]:
                cap = result[0]
                # ic-py returns opt as [value] or []
                if isinstance(cap, list):
                    cap = cap[0] if cap else None
                if cap:
                    capsules.append(normalize_capsule(cap))
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  [WARN] Capsule {i}: {e}")

        if i % 100 == 0:
            elapsed = time.time() - start
            rate = i / elapsed
            eta = (total - i) / rate if rate > 0 else 0
            print(f"  [{i}/{total}] {len(capsules)} exported, {errors} errors, "
                  f"{rate:.1f}/s, ETA {eta:.0f}s")

    elapsed = time.time() - start
    print(f"[ic-py] Done: {len(capsules)} capsules in {elapsed:.1f}s ({errors} errors)")
    return capsules


def export_via_http(workers=10):
    """Export using HTTP API (no dependencies, concurrent)."""
    import urllib.request
    import urllib.error

    print(f"[HTTP] Fetching capsule count...")

    # Get count from dashboard
    try:
        req = urllib.request.Request(f"{HTTP_BASE}/api/dashboard")
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
            total = data.get("capsule_count", 0)
    except Exception:
        # Fallback: try recent capsules and guess
        try:
            req = urllib.request.Request(f"{HTTP_BASE}/api/recent?limit=1")
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
                total = data.get("count", 5200)
        except Exception as e:
            print(f"[HTTP] Cannot reach canister: {e}")
            return None
        # If we got a count of capsules but not the total, estimate high
        total = max(total, 5200)

    print(f"[HTTP] Canister reports ~{total} capsules, fetching with {workers} workers...")

    capsules = {}
    errors = 0
    lock = __import__('threading').Lock()
    start = time.time()

    def fetch_one(capsule_id):
        nonlocal errors
        try:
            url = f"{HTTP_BASE}/api/capsule?id={capsule_id}"
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
                if "error" not in data and data.get("restatement"):
                    return capsule_id, data
        except Exception:
            with lock:
                errors += 1
        return capsule_id, None

    done = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_one, i): i for i in range(1, total + 1)}
        for future in as_completed(futures):
            capsule_id, data = future.result()
            if data:
                capsules[capsule_id] = normalize_capsule(data)
            done += 1
            if done % 200 == 0:
                elapsed = time.time() - start
                rate = done / elapsed
                eta = (total - done) / rate if rate > 0 else 0
                print(f"  [{done}/{total}] {len(capsules)} exported, {errors} errors, "
                      f"{rate:.1f}/s, ETA {eta:.0f}s")

    # Sort by ID
    result = [capsules[k] for k in sorted(capsules.keys())]
    elapsed = time.time() - start
    print(f"[HTTP] Done: {len(result)} capsules in {elapsed:.1f}s ({errors} errors)")
    return result


def normalize_capsule(cap):
    """Normalize a capsule dict from either ic-py or HTTP response."""
    if isinstance(cap, dict):
        return {
            "id": cap.get("id", 0),
            "restatement": cap.get("restatement", ""),
            "topic": unwrap_opt(cap.get("topic")),
            "timestamp": unwrap_opt(cap.get("timestamp")),
            "location": unwrap_opt(cap.get("location")),
            "confidence_score": cap.get("confidence_score", cap.get("confidence", 0.0)),
            "persons": cap.get("persons", []),
            "entities": cap.get("entities", []),
            "keywords": cap.get("keywords", []),
            "conversation_id": cap.get("conversation_id", ""),
            "created_at": cap.get("created_at", 0),
        }
    # ic-py might return a record-like object with attribute access
    try:
        return {
            "id": getattr(cap, "id", 0),
            "restatement": getattr(cap, "restatement", ""),
            "topic": unwrap_opt(getattr(cap, "topic", None)),
            "timestamp": unwrap_opt(getattr(cap, "timestamp", None)),
            "location": unwrap_opt(getattr(cap, "location", None)),
            "confidence_score": getattr(cap, "confidence_score", 0.0),
            "persons": list(getattr(cap, "persons", [])),
            "entities": list(getattr(cap, "entities", [])),
            "keywords": list(getattr(cap, "keywords", [])),
            "conversation_id": getattr(cap, "conversation_id", ""),
            "created_at": getattr(cap, "created_at", 0),
        }
    except Exception:
        return {"id": 0, "restatement": str(cap), "topic": None, "confidence_score": 0.5,
                "persons": [], "entities": [], "keywords": [], "conversation_id": "",
                "created_at": 0, "timestamp": None, "location": None}


def unwrap_opt(val):
    """Unwrap ic-py opt type: [] = None, [val] = val, else passthrough."""
    if isinstance(val, list):
        return val[0] if val else None
    return val


def main():
    parser = argparse.ArgumentParser(description="Export Chronicle capsules from ICP canister")
    parser.add_argument("--http", action="store_true", help="Use HTTP API instead of ic-py")
    parser.add_argument("--workers", type=int, default=10, help="HTTP concurrent workers (default: 10)")
    parser.add_argument("--output", type=str, default=OUTPUT_PATH, help="Output file path")
    args = parser.parse_args()

    print(f"=== Capsule Export ===")
    print(f"Canister: {CANISTER_ID}")
    print(f"Output:   {args.output}")
    print()

    if args.http:
        capsules = export_via_http(workers=args.workers)
    else:
        capsules = export_via_icpy()
        if capsules is None:
            print("\n[Fallback] Trying HTTP API...")
            capsules = export_via_http(workers=args.workers)

    if capsules is None:
        print("\nFATAL: Could not export capsules via any method.")
        sys.exit(1)

    # Write output
    print(f"\nWriting {len(capsules)} capsules to {args.output}...")
    with open(args.output, 'w') as f:
        json.dump(capsules, f, indent=2, default=str)

    size_mb = os.path.getsize(args.output) / (1024 * 1024)
    print(f"Done! {size_mb:.1f} MB written.")

    # Quick stats
    topics = set()
    persons = set()
    for cap in capsules:
        if cap.get("topic"):
            topics.add(cap["topic"])
        for p in cap.get("persons", []):
            persons.add(p)

    print(f"\nStats:")
    print(f"  Capsules:  {len(capsules)}")
    print(f"  Topics:    {len(topics)} unique")
    print(f"  Persons:   {len(persons)} unique")
    print(f"  ID range:  {capsules[0]['id']} - {capsules[-1]['id']}" if capsules else "  (empty)")


if __name__ == "__main__":
    main()
