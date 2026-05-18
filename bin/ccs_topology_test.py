#!/usr/bin/env python3
"""CCS entity-count vs continuity test.

Hypothesis: entity count predicts identity probe scores better than token count.
Tests Thread #315's claim that grounding is relational topology, not information volume.
"""
import json
import os
import sqlite3
import sys

DB = "/mnt/hdd/chronicle-data/processed.db"
PROBE_LOG = "/home/nate-agx/chronicle/data/ccs_identity_probe_log.jsonl"
PAIRED_LOG = "/home/nate-agx/chronicle/data/ccs_paired_metrics.jsonl"


def load_probes():
    probes = {}
    with open(PROBE_LOG) as f:
        for line in f:
            entry = json.loads(line)
            v = entry["version"]
            if v not in probes or entry["timestamp"] > probes[v]["timestamp"]:
                probes[v] = entry
    return probes


def load_ccs_snapshots(db):
    rows = db.execute(
        "SELECT snapshot, created_at FROM cognitive_state_history ORDER BY created_at"
    ).fetchall()
    snapshots = []
    for raw, ts in rows:
        try:
            data = json.loads(raw)
            entities = data.get("focal_entities", [])
            entity_count = len(entities)
            token_count = len(raw)
            version = data.get("metadata", {}).get("version") or data.get("version")
            avg_salience = 0
            if entities:
                sals = [e.get("salience", 0) for e in entities]
                avg_salience = sum(sals) / len(sals)
            has_relational_map = bool(data.get("relational_map"))
            uncertainty_count = len(data.get("uncertainty_signals", []))
            snapshots.append({
                "version": version,
                "entity_count": entity_count,
                "token_count": token_count,
                "avg_salience": round(avg_salience, 3),
                "has_relational_map": has_relational_map,
                "uncertainty_count": uncertainty_count,
                "ts": ts,
            })
        except (json.JSONDecodeError, KeyError):
            continue
    return snapshots


def pearson(xs, ys):
    n = len(xs)
    if n < 3:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx = sum((x - mx) ** 2 for x in xs) ** 0.5
    dy = sum((y - my) ** 2 for y in ys) ** 0.5
    if dx == 0 or dy == 0:
        return None
    return round(num / (dx * dy), 4)


def main():
    db = sqlite3.connect(DB, timeout=10)
    snapshots = load_ccs_snapshots(db)
    db.close()
    probes = load_probes()

    print(f"CCS snapshots: {len(snapshots)}")
    print(f"Probe entries: {len(probes)} (unique versions)")
    print()

    # Distribution of entity counts
    ecounts = [s["entity_count"] for s in snapshots]
    print(f"Entity count range: {min(ecounts)}-{max(ecounts)}, mean: {sum(ecounts)/len(ecounts):.1f}")
    tcounts = [s["token_count"] for s in snapshots]
    print(f"Token count range: {min(tcounts)}-{max(tcounts)}, mean: {sum(tcounts)/len(tcounts):.0f}")
    print()

    # Match snapshots to probes by version
    matched = []
    for s in snapshots:
        v = s["version"]
        if v and v in probes:
            p = probes[v]
            matched.append({
                **s,
                "probe_overall": p["overall"],
                "probe_relational": p["categories"].get("relational", None),
                "probe_factual": p["categories"].get("factual", None),
                "probe_identity": p["categories"].get("identity", None),
                "probe_predictive": p["categories"].get("predictive", None),
            })

    print(f"Matched (snapshot+probe on same version): {len(matched)}")

    if len(matched) >= 3:
        ec = [m["entity_count"] for m in matched]
        tc = [m["token_count"] for m in matched]
        po = [m["probe_overall"] for m in matched]
        pr = [m["probe_relational"] for m in matched if m["probe_relational"] is not None]

        r_entity_overall = pearson(ec, po)
        r_token_overall = pearson(tc, po)

        print(f"\n--- Correlation: predictor vs probe_overall ---")
        print(f"  entity_count  r = {r_entity_overall}")
        print(f"  token_count   r = {r_token_overall}")

        if pr and len(pr) == len(matched):
            ec_r = [m["entity_count"] for m in matched if m["probe_relational"] is not None]
            r_entity_relational = pearson(ec_r, pr)
            tc_r = [m["token_count"] for m in matched if m["probe_relational"] is not None]
            r_token_relational = pearson(tc_r, pr)
            print(f"\n--- Correlation: predictor vs probe_relational ---")
            print(f"  entity_count  r = {r_entity_relational}")
            print(f"  token_count   r = {r_token_relational}")

        # Bucket analysis: low (<5) vs high (>=5) entity count
        low = [m for m in matched if m["entity_count"] < 5]
        high = [m for m in matched if m["entity_count"] >= 5]
        print(f"\n--- Bucket: entity_count < 5 ({len(low)}) vs >= 5 ({len(high)}) ---")
        if low:
            print(f"  Low:  mean probe = {sum(m['probe_overall'] for m in low)/len(low):.3f}")
        if high:
            print(f"  High: mean probe = {sum(m['probe_overall'] for m in high)/len(high):.3f}")
    else:
        print("\nNot enough matched data for correlation.")
        print("Falling back to snapshot-only analysis...\n")

    # Even without probe matches, analyze the structural patterns
    print(f"\n--- CCS structural evolution (all {len(snapshots)} snapshots) ---")
    has_rm = sum(1 for s in snapshots if s["has_relational_map"])
    print(f"  With relational_map: {has_rm}/{len(snapshots)}")
    print(f"  Avg uncertainty signals: {sum(s['uncertainty_count'] for s in snapshots)/len(snapshots):.1f}")

    # Entity count over time
    print(f"\n--- Entity count trajectory (last 10) ---")
    for s in snapshots[-10:]:
        rm = "+" if s["has_relational_map"] else "-"
        print(f"  v{s['version']:>4}  entities={s['entity_count']:>2}  tokens={s['token_count']:>5}  sal={s['avg_salience']:.2f}  rm={rm}  unc={s['uncertainty_count']}")

    # Check paired metrics log (new, aligned data)
    paired = []
    if os.path.exists(PAIRED_LOG):
        with open(PAIRED_LOG) as f:
            for line in f:
                try:
                    paired.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    if paired:
        print(f"\n--- Paired metrics log ({len(paired)} entries) ---")
        has_probes = [p for p in paired if p.get("probe_overall") is not None]
        if len(has_probes) >= 3:
            ec = [p["entity_count"] for p in has_probes]
            tc = [p["token_count"] for p in has_probes]
            po = [p["probe_overall"] for p in has_probes]
            pr = [p["probe_relational"] for p in has_probes if p.get("probe_relational") is not None]

            r_ec = pearson(ec, po)
            r_tc = pearson(tc, po)
            print(f"  entity_count vs probe_overall: r = {r_ec}")
            print(f"  token_count  vs probe_overall: r = {r_tc}")

            if pr and len(pr) >= 3:
                ec_r = [p["entity_count"] for p in has_probes if p.get("probe_relational") is not None]
                tc_r = [p["token_count"] for p in has_probes if p.get("probe_relational") is not None]
                r_ec_rel = pearson(ec_r, pr)
                r_tc_rel = pearson(tc_r, pr)
                print(f"  entity_count vs probe_relational: r = {r_ec_rel}")
                print(f"  token_count  vs probe_relational: r = {r_tc_rel}")
        else:
            print(f"  {len(has_probes)} entries with probes (need ≥3 for correlation)")

        for p in paired[-5:]:
            probe_s = f"probe={p['probe_overall']:.2f}" if p.get('probe_overall') is not None else "probe=N/A"
            print(f"  v{p['version']:>4}  ent={p['entity_count']:>2}  tok={p['token_count']:>5}  "
                  f"sal={p['avg_salience']:.2f}  rm={p['relational_map_keys']}  "
                  f"ret={p['retention_rate']:.1%}  {probe_s}")

    print(f"\n--- Verdict ---")
    # Try paired data first, fall back to matched snapshot+probe
    best_ec_r = None
    best_tc_r = None
    data_source = None

    if paired and len([p for p in paired if p.get("probe_overall") is not None]) >= 3:
        has_probes = [p for p in paired if p.get("probe_overall") is not None]
        best_ec_r = pearson([p["entity_count"] for p in has_probes],
                           [p["probe_overall"] for p in has_probes])
        best_tc_r = pearson([p["token_count"] for p in has_probes],
                           [p["probe_overall"] for p in has_probes])
        data_source = f"paired log ({len(has_probes)} entries)"
    elif len(matched) >= 3:
        best_ec_r = r_entity_overall
        best_tc_r = r_token_overall
        data_source = f"snapshot-probe match ({len(matched)} entries)"

    if best_ec_r is not None and best_tc_r is not None and data_source:
        print(f"  Data source: {data_source}")
        if abs(best_ec_r) > abs(best_tc_r):
            print(f"  Entity count is stronger predictor (r={best_ec_r} vs r={best_tc_r})")
            print(f"  → Supports Thread #315: grounding is relational topology")
        else:
            print(f"  Token count is stronger predictor (r={best_tc_r} vs r={best_ec_r})")
            print(f"  → Challenges Thread #315: information volume may matter more")
    else:
        print("  Insufficient matched data for definitive verdict.")
        print("  Paired metrics will accumulate with each compression.")
        print("  Run this test again after ~10 compressions for initial signal.")


if __name__ == "__main__":
    main()
