#!/usr/bin/env python3
"""Compression delta viewer — shows how CCS fields evolve across compressions.

Usage:
  python3 delta_viewer.py           # show all deltas
  python3 delta_viewer.py --last N  # show last N deltas
  python3 delta_viewer.py --field semantic_gist  # track one field
  python3 delta_viewer.py --summary  # one-line-per-compression overview
"""
import argparse
import json
import os
import sys
from datetime import datetime

DELTA_LOG = os.path.expanduser("~/chronicle/data/compression_deltas.jsonl")

TEXT_FIELDS = {"semantic_gist", "goal_orientation", "predictive_cue"}
ARRAY_FIELDS = {"episodic_trace", "constraints", "uncertainty_signals"}
ENTITY_FIELD = "focal_entities"
MAP_FIELD = "relational_map"


def load_deltas():
    if not os.path.exists(DELTA_LOG):
        return []
    deltas = []
    with open(DELTA_LOG) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    deltas.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return deltas


def format_ts(ts):
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def show_summary(deltas):
    print(f"{'Time':>16}  {'Version':>10}  {'Changed':>7}  {'Entities':>20}  {'Gist drift'}")
    print("-" * 85)
    for d in deltas:
        ts = format_ts(d.get("ts", 0))
        ver = f"v{d.get('version_before', '?')}→v{d.get('version_after', '?')}"
        changed = f"{d.get('fields_changed', '?')}/{d.get('total_fields', '?')}"

        ent = d.get("focal_entities", {})
        ent_str = ""
        if ent.get("dropped"):
            ent_str += f"-{','.join(ent['dropped'][:2])}"
        if ent.get("added"):
            if ent_str:
                ent_str += " "
            ent_str += f"+{','.join(ent['added'][:2])}"
        if not ent_str:
            ent_str = "stable"

        gist = d.get("semantic_gist", {})
        if gist.get("changed"):
            gist_str = "changed"
        else:
            gist_str = "stable"

        print(f"{ts:>16}  {ver:>10}  {changed:>7}  {ent_str:>20}  {gist_str}")


def show_field(deltas, field):
    print(f"\n  Tracking: {field}")
    print("=" * 70)
    for d in deltas:
        ts = format_ts(d.get("ts", 0))
        ver = f"v{d.get('version_before', '?')}→v{d.get('version_after', '?')}"
        fd = d.get(field, {})

        if not fd.get("changed", False):
            print(f"\n  [{ts}] {ver} — {field}: no change")
            continue

        print(f"\n  [{ts}] {ver} — {field}:")
        if field in TEXT_FIELDS:
            before = fd.get("before", "")[:120]
            after = fd.get("after", "")[:120]
            print(f"    BEFORE: {before}")
            print(f"    AFTER:  {after}")
        elif field == ENTITY_FIELD:
            if fd.get("dropped"):
                print(f"    DROPPED: {fd['dropped']}")
            if fd.get("added"):
                print(f"    ADDED:   {fd['added']}")
            print(f"    RETAINED: {fd.get('retained', [])}")
        elif field == MAP_FIELD:
            if fd.get("dropped_keys"):
                print(f"    DROPPED: {fd['dropped_keys']}")
            if fd.get("added_keys"):
                print(f"    ADDED:   {fd['added_keys']}")
            if fd.get("changed_keys"):
                print(f"    CHANGED: {fd['changed_keys']}")
        else:
            print(f"    {fd.get('before_count', '?')}→{fd.get('after_count', '?')} "
                  f"(+{fd.get('added', 0)} -{fd.get('dropped', 0)} "
                  f"retained={fd.get('retained', 0)})")


def show_full(deltas):
    all_fields = list(TEXT_FIELDS) + list(ARRAY_FIELDS) + [ENTITY_FIELD, MAP_FIELD]
    for d in deltas:
        ts = format_ts(d.get("ts", 0))
        ver = f"v{d.get('version_before', '?')}→v{d.get('version_after', '?')}"
        changed = d.get("fields_changed", 0)
        total = d.get("total_fields", 0)
        print(f"\n{'='*70}")
        print(f"  [{ts}] {ver} — {changed}/{total} fields changed")
        print(f"{'='*70}")

        for field in all_fields:
            fd = d.get(field, {})
            if not fd.get("changed", False):
                print(f"  {field}: stable")
                continue
            if field in TEXT_FIELDS:
                print(f"  {field}: CHANGED")
                print(f"    before: {fd.get('before', '')[:80]}...")
                print(f"    after:  {fd.get('after', '')[:80]}...")
            elif field == ENTITY_FIELD:
                print(f"  {field}: {fd.get('before_count', '?')}→{fd.get('after_count', '?')}")
                if fd.get("dropped"):
                    print(f"    dropped: {fd['dropped']}")
                if fd.get("added"):
                    print(f"    added:   {fd['added']}")
            elif field == MAP_FIELD:
                print(f"  {field}: {fd.get('before_keys', '?')}→{fd.get('after_keys', '?')} keys")
            else:
                print(f"  {field}: {fd.get('before_count', '?')}→{fd.get('after_count', '?')} "
                      f"(+{fd.get('added', 0)} -{fd.get('dropped', 0)})")


def main():
    parser = argparse.ArgumentParser(description="Compression delta viewer")
    parser.add_argument("--last", type=int, help="Show last N deltas")
    parser.add_argument("--field", help="Track a specific field across all deltas")
    parser.add_argument("--summary", action="store_true", help="One-line-per-compression overview")
    args = parser.parse_args()

    deltas = load_deltas()
    if not deltas:
        print("No compression deltas found yet.")
        return

    if args.last:
        deltas = deltas[-args.last:]

    print(f"\n  Compression Lineage — {len(deltas)} delta(s)\n")

    if args.summary:
        show_summary(deltas)
    elif args.field:
        show_field(deltas, args.field)
    else:
        show_full(deltas)


if __name__ == "__main__":
    main()
