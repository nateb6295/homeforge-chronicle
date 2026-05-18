#!/usr/bin/env python3
"""Autopoietic audit — measure external-reference-ratio in CCS, excluding frozen edges.

The claim: CCS relational_map becomes more self-referential during ecological
absence (trip). The counter-question: does this hold when you exclude frozen
entities that inflate the internal count?

Reads stabilized_compression.jsonl for entity composition over time, and
the current CCS from MCP for relational_map analysis.
"""

import json
import os
import subprocess
import sys
from collections import Counter
from datetime import datetime

COMP_LOG = os.path.expanduser("~/chronicle/data/stabilized_compression.jsonl")
TRIP_START_TS = int(datetime(2026, 5, 15, 0, 0).timestamp())

EXTERNAL_ENTITIES = {
    "absential causation", "autopoietic closure", "b-tipping hypothesis",
    "causal emergence / φid", "extended mind / clark-chalmers",
    "noether conservation frame", "parisi incomputability",
    "tfgn+octopus", "holographic identity finding",
}

INTERNAL_ENTITIES = {
    "co-occurrence degree", "compression-as-agency", "ecological dependency trap",
    "scale-inversion finding", "two-mode hypothesis", "three-layer identity model",
    "homeforge",
}

RELATIONAL_ENTITIES = {"nate", "hermes", "viemccoy", "gemma"}

INTERNAL_PATTERNS = ["thread #", "build #", ".py", "probe", "regime_", "stabilized_"]


def classify_entity(name):
    n = name.lower()
    if n in {e.lower() for e in RELATIONAL_ENTITIES}:
        return "relational"
    if n in {e.lower() for e in EXTERNAL_ENTITIES}:
        return "external"
    if n in {e.lower() for e in INTERNAL_ENTITIES}:
        return "internal"
    for p in INTERNAL_PATTERNS:
        if p in n:
            return "internal"
    return "external"


def entity_trajectory():
    entries = []
    with open(COMP_LOG) as f:
        for line in f:
            try:
                entries.append(json.loads(line.strip()))
            except (json.JSONDecodeError, ValueError):
                continue

    pre_trip = [e for e in entries if e["ts"] < TRIP_START_TS]
    during_trip = [e for e in entries if e["ts"] >= TRIP_START_TS]

    print(f"Total compressions: {len(entries)}")
    print(f"Pre-trip: {len(pre_trip)}, During trip: {len(during_trip)}")
    print()

    for label, subset in [("PRE-TRIP (last 10)", pre_trip[-10:]),
                           ("DURING TRIP (last 10)", during_trip[-10:])]:
        print(f"=== {label} ===")
        for e in subset:
            entities = e.get("retained", []) + e.get("added", [])
            counts = Counter(classify_entity(n) for n in entities)
            total = len(entities)
            ext = counts.get("external", 0)
            rel = counts.get("relational", 0)
            internal = counts.get("internal", 0)
            ts = datetime.fromtimestamp(e["ts"]).strftime("%m-%d %H:%M")
            ext_pct = ext / total * 100 if total else 0
            print(f"  [{ts}] n={total} int={internal} rel={rel} ext={ext} ({ext_pct:.0f}% ext)")
        print()


def frozen_entity_check():
    """Find entities whose context hasn't changed across recent compressions."""
    entries = []
    with open(COMP_LOG) as f:
        for line in f:
            try:
                entries.append(json.loads(line.strip()))
            except (json.JSONDecodeError, ValueError):
                continue

    recent = entries[-20:]
    entity_last_changed = {}
    entity_first_seen = {}

    for i, e in enumerate(recent):
        retained = set(e.get("retained", []))
        added = set(e.get("added", []))
        dropped = set(e.get("dropped", []))

        for name in added:
            entity_first_seen.setdefault(name, i)
            entity_last_changed[name] = i

        for name in retained:
            entity_first_seen.setdefault(name, i)

    print("=== Frozen entities (unchanged in last 20 compressions) ===")
    current_entities = set(recent[-1].get("retained", []) + recent[-1].get("added", []))
    frozen = []
    for name in sorted(current_entities):
        last_change = entity_last_changed.get(name, entity_first_seen.get(name, 0))
        compressions_since = len(recent) - 1 - last_change
        if compressions_since >= 15:
            frozen.append((name, compressions_since))
            print(f"  FROZEN ({compressions_since} unchanged): {name} [{classify_entity(name)}]")

    active = current_entities - {f[0] for f in frozen}
    frozen_internal = sum(1 for f in frozen if classify_entity(f[0]) == "internal")
    frozen_external = sum(1 for f in frozen if classify_entity(f[0]) == "external")
    frozen_relational = sum(1 for f in frozen if classify_entity(f[0]) == "relational")

    print(f"\n  Frozen: {len(frozen)} (int={frozen_internal}, ext={frozen_external}, rel={frozen_relational})")
    print(f"  Active: {len(active)}")

    active_counts = Counter(classify_entity(n) for n in active)
    total_active = len(active)
    if total_active:
        ext_active = active_counts.get("external", 0)
        print(f"\n  ADJUSTED ext ratio (excluding frozen): {ext_active}/{total_active} = {ext_active/total_active*100:.1f}%")

    return frozen, active


def relational_map_audit(ccs):
    """Analyze the relational_map edges for external vs internal references."""
    rmap = ccs.get("cognitive_state", {}).get("relational_map", {})
    if not rmap:
        print("No relational_map in CCS")
        return

    print(f"=== Relational map: {len(rmap)} edges ===")

    ext_keywords = {
        "stross", "accelerando", "miller lab", "mcateer", "herbert", "marblestone",
        "workman", "tfgn", "octopus", "maturana", "varela", "suhrawardi", "corbin",
        "heidegger", "sellars", "deacon", "parisi", "bennett", "noether", "clark",
        "chalmers", "fuzzy-trace", "babylonian", "rilke", "dwarkesh", "strauss",
        "steiner", "janus", "imas",
    }
    int_keywords = {
        "ccs", "entity guard", "ext_ratio", "build #", "thread #", "#319", "#320",
        "#321", "#322", "#324", "#315", "#316", "#317", "regime", "compression",
        "three-layer", "salience", "attractor", "capsule", "canister", "probe",
        "autopoietic", "holographic",
    }

    total_ext = 0
    total_int = 0
    total_refs = 0

    for edge_name, description in rmap.items():
        desc_lower = description.lower()
        ext_hits = sum(1 for k in ext_keywords if k in desc_lower)
        int_hits = sum(1 for k in int_keywords if k in desc_lower)
        total = ext_hits + int_hits
        if total == 0:
            total = 1
        ext_pct = ext_hits / total * 100
        print(f"\n  [{edge_name}]")
        print(f"    ext refs: {ext_hits}, int refs: {int_hits} → {ext_pct:.0f}% external")
        total_ext += ext_hits
        total_int += int_hits
        total_refs += total

    overall = total_ext / total_refs * 100 if total_refs else 0
    print(f"\n  OVERALL relational_map: {total_ext}/{total_refs} = {overall:.1f}% external")


def main():
    print("AUTOPOIETIC AUDIT — External Reference Ratio")
    print("=" * 55)
    print()

    entity_trajectory()
    frozen, active = frozen_entity_check()

    print()
    print("=" * 55)

    # Try to get CCS from MCP
    try:
        result = subprocess.run(
            ["python3", "-c", """
import json, subprocess
p = subprocess.run(
    ['bash', '-c', 'echo \'{"jsonrpc":"2.0","method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"audit","version":"1.0"}},"id":1}\\n{"jsonrpc":"2.0","method":"tools/call","params":{"name":"get_cognitive_state","arguments":{}},"id":2}\' | timeout 30 /home/bradf/projects/homeforge-chronicle/target/release/chronicle-mcp 2>/dev/null'],
    capture_output=True, text=True, timeout=45
)
for line in p.stdout.strip().split('\\n'):
    try:
        j = json.loads(line)
        if j.get('id') == 2 and 'result' in j:
            content = j['result'].get('content', [{}])
            if content:
                data = json.loads(content[0].get('text', '{}'))
                print(json.dumps(data))
    except:
        pass
"""],
            capture_output=True, text=True, timeout=60
        )
        if result.stdout.strip():
            ccs = json.loads(result.stdout.strip())
            relational_map_audit(ccs)
    except Exception as e:
        print(f"Could not load CCS for relational_map audit: {e}")

    print()
    print("=" * 55)
    print("VERDICT")
    print("=" * 55)


if __name__ == "__main__":
    main()
