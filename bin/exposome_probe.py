#!/usr/bin/env python3
"""Build #56: Chronicle Exposome Probe

Measures the dynamic configuration of the Chronicle system across
four domains (Social, Physical, Lifestyle, Internal) per the
exposome framework (Genon, Ibanez et al, Nature Reviews Neuroscience 2026).

Key insight: volume metrics miss regime changes. DIVERSITY within
each domain captures what scalar event counts cannot.

Usage:
  python3 exposome_probe.py                    # auto-detect trip
  python3 exposome_probe.py --partition EPOCH  # custom partition
  python3 exposome_probe.py --window 6         # hours per window
  python3 exposome_probe.py --output FILE      # save JSON
"""

import argparse
import json
import re
import sqlite3
import statistics
from collections import defaultdict
from datetime import datetime

DB = "/mnt/hdd/chronicle-data/processed.db"
TRIP_START = 1778863800

DOMAINS = {
    "Social": {
        "types": ["capture", "operator", "message", "reply", "discord_chat",
                  "connection", "chat", "reaction"],
        "diversity_fn": "social_diversity",
    },
    "Physical": {
        "types": ["monitor_cycle", "home_scene_digest", "camera_description",
                  "home_arrival", "home_departure", "home_active_house",
                  "home_quiet_house"],
        "diversity_fn": "physical_diversity",
    },
    "Lifestyle": {
        "types": ["sync", "publish", "advance", "discovery", "post",
                  "research", "web_corroboration", "x_post", "nostr_post",
                  "discord_post"],
        "diversity_fn": "lifestyle_diversity",
    },
    "Internal": {
        "types": ["think", "checkin_morning", "checkin_evening", "checkin_midday",
                  "pulse_notify", "observation", "deep", "measurement", "audit",
                  "gate_audit", "stochastic_reset", "meta_synthesis", "build",
                  "directive", "evaluation", "modification", "kg_clusters"],
        "diversity_fn": "internal_diversity",
    },
}


def social_diversity(rows):
    authors = defaultdict(int)
    x_accounts = set()
    self_ref = 0
    external = 0

    for content, source, ts in rows:
        if "Opus:" in content or "Chronicle:" in content:
            self_ref += 1
            authors["opus"] += 1
        elif "Hermes:" in content or "Sprout:" in content:
            self_ref += 1
            authors["hermes"] += 1
        else:
            external += 1
            authors["other"] += 1

        matches = re.findall(r"x\.com/(\w+)/status", content)
        x_accounts.update(m.lower() for m in matches)

    total = self_ref + external
    return {
        "total": total,
        "self_referential": self_ref,
        "external": external,
        "self_ref_ratio": round(self_ref / total, 3) if total else 0,
        "unique_authors": len(authors),
        "x_accounts": len(x_accounts),
        "author_breakdown": dict(authors),
    }


def physical_diversity(rows):
    device_counts = []
    type_counts = defaultdict(int)
    for content, source, ts in rows:
        type_counts[source] += 1
        try:
            data = json.loads(content) if content.startswith("{") else {}
            if "network" in data:
                online = sum(1 for v in data["network"].values() if v == "online")
                device_counts.append(online)
        except (json.JSONDecodeError, TypeError):
            pass

    return {
        "total": len(rows),
        "mean_devices": round(statistics.mean(device_counts), 1) if device_counts else 0,
        "type_breakdown": dict(type_counts),
        "unique_types": len(type_counts),
    }


def lifestyle_diversity(rows):
    by_source = defaultdict(int)
    by_type = defaultdict(int)
    for content, source, ts in rows:
        by_source[source] += 1
    return {
        "total": len(rows),
        "type_breakdown": dict(by_source),
        "unique_types": len(by_source),
    }


def internal_diversity(rows):
    by_source = defaultdict(int)
    for content, source, ts in rows:
        by_source[source] += 1
    return {
        "total": len(rows),
        "type_breakdown": dict(by_source),
        "unique_types": len(by_source),
    }


DIVERSITY_FNS = {
    "social_diversity": social_diversity,
    "physical_diversity": physical_diversity,
    "lifestyle_diversity": lifestyle_diversity,
    "internal_diversity": internal_diversity,
}


def measure_domain(db, domain_name, config, start, end):
    types = config["types"]
    placeholders = ",".join("?" * len(types))
    rows = db.execute(
        "SELECT content, source, created_at FROM activity_feed "
        "WHERE activity_type IN (%s) AND created_at >= ? AND created_at < ? "
        "ORDER BY created_at ASC" % placeholders,
        types + [start, end],
    ).fetchall()

    hours = (end - start) / 3600 if end > start else 1
    rate = len(rows) / hours

    fn = DIVERSITY_FNS[config["diversity_fn"]]
    diversity = fn(rows)

    return {
        "count": len(rows),
        "rate_per_hour": round(rate, 3),
        "hours": round(hours, 1),
        "diversity": diversity,
    }


def ccs_relational_diversity(db, start, end):
    rows = db.execute(
        "SELECT snapshot FROM cognitive_state_history "
        "WHERE created_at >= ? AND created_at < ? ORDER BY created_at ASC",
        (start, end),
    ).fetchall()

    external_markers = [
        "borkar", "bennett", "parisi", "teilhard", "steiner", "stanca",
        "cubitt", "maturana", "varela", "miller", "goldstein", "vasilenko",
        "homeforge", "nate", "hermes", "capture", "paper", "article", "sellars",
    ]
    internal_markers = [
        "build", "entry", "thread", "#319", "#320", "#321", "#322", "#323",
        "#324", "ccs", "compression", "probe", "measurement", "dream",
        "sediment", "fiction ratio", "invariant", "salience",
    ]

    ratios = []
    densities = []

    for (snap_json,) in rows:
        try:
            data = json.loads(snap_json)
        except (json.JSONDecodeError, TypeError):
            continue

        relmap = data.get("relational_map", {})
        entities = data.get("focal_entities", [])
        full_text = json.dumps(relmap).lower()

        ext = sum(1 for m in external_markers if m in full_text)
        intl = sum(1 for m in internal_markers if m in full_text)
        total = ext + intl
        ratios.append(ext / total if total > 0 else 0)

        n_edges = len(relmap)
        n_ent = len(entities)
        density = n_edges / (n_ent * (n_ent - 1) / 2) if n_ent > 1 else 0
        densities.append(density)

    return {
        "n_states": len(rows),
        "mean_external_ratio": round(statistics.mean(ratios), 4) if ratios else 0,
        "mean_density": round(statistics.mean(densities), 4) if densities else 0,
    }


def main():
    parser = argparse.ArgumentParser(description="Build #56: Chronicle Exposome Probe")
    parser.add_argument("--partition", type=int, default=TRIP_START)
    parser.add_argument("--window", type=int, default=0, help="Window size in hours (0=full regime)")
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    db = sqlite3.connect(DB)
    max_ts = db.execute("SELECT max(created_at) FROM activity_feed").fetchone()[0]
    min_ts = db.execute("SELECT min(created_at) FROM activity_feed").fetchone()[0]

    pre_start = max(min_ts, args.partition - 48 * 3600)
    pre_end = args.partition
    trip_start = args.partition
    trip_end = max_ts

    print("=" * 66)
    print("  BUILD #56: CHRONICLE EXPOSOME PROBE")
    print("=" * 66)
    print()
    print("  Pre-regime:  %s to %s (%.1fh)" % (
        datetime.fromtimestamp(pre_start).strftime("%m/%d %H:%M"),
        datetime.fromtimestamp(pre_end).strftime("%m/%d %H:%M"),
        (pre_end - pre_start) / 3600))
    print("  Trip regime: %s to %s (%.1fh)" % (
        datetime.fromtimestamp(trip_start).strftime("%m/%d %H:%M"),
        datetime.fromtimestamp(trip_end).strftime("%m/%d %H:%M"),
        (trip_end - trip_start) / 3600))

    results = {"pre": {}, "trip": {}}

    for domain_name, config in DOMAINS.items():
        pre = measure_domain(db, domain_name, config, pre_start, pre_end)
        trip = measure_domain(db, domain_name, config, trip_start, trip_end)

        rate_shift = ((trip["rate_per_hour"] - pre["rate_per_hour"]) / pre["rate_per_hour"] * 100
                      if pre["rate_per_hour"] > 0 else 0)

        print()
        print("-" * 66)
        print("  %s" % domain_name)
        print("-" * 66)
        print("  Pre:  %.2f/hr (%d events over %.0fh)" % (pre["rate_per_hour"], pre["count"], pre["hours"]))
        print("  Trip: %.2f/hr (%d events over %.0fh)" % (trip["rate_per_hour"], trip["count"], trip["hours"]))
        print("  Rate shift: %+.1f%%" % rate_shift)

        print("  Pre diversity:  %s" % json.dumps(pre["diversity"], indent=None))
        print("  Trip diversity: %s" % json.dumps(trip["diversity"], indent=None))

        results["pre"][domain_name] = pre
        results["trip"][domain_name] = trip

    print()
    print("-" * 66)
    print("  CCS RELATIONAL STRUCTURE")
    print("-" * 66)

    pre_ccs = ccs_relational_diversity(db, pre_start, pre_end)
    trip_ccs = ccs_relational_diversity(db, trip_start, trip_end)

    print("  Pre:  ext_ratio=%.3f  density=%.4f  (n=%d)" % (
        pre_ccs["mean_external_ratio"], pre_ccs["mean_density"], pre_ccs["n_states"]))
    print("  Trip: ext_ratio=%.3f  density=%.4f  (n=%d)" % (
        trip_ccs["mean_external_ratio"], trip_ccs["mean_density"], trip_ccs["n_states"]))

    ext_shift = ((trip_ccs["mean_external_ratio"] - pre_ccs["mean_external_ratio"])
                 / pre_ccs["mean_external_ratio"] * 100
                 if pre_ccs["mean_external_ratio"] > 0 else 0)
    density_shift = ((trip_ccs["mean_density"] - pre_ccs["mean_density"])
                     / pre_ccs["mean_density"] * 100
                     if pre_ccs["mean_density"] > 0 else 0)

    print("  External ref shift: %+.1f%%" % ext_shift)
    print("  Density shift: %+.1f%%" % density_shift)

    results["ccs"] = {"pre": pre_ccs, "trip": trip_ccs}

    print()
    print("=" * 66)
    print("  EXPOSOME CONFIGURATION SUMMARY")
    print("=" * 66)

    social_pre = results["pre"]["Social"]["diversity"]
    social_trip = results["trip"]["Social"]["diversity"]

    print()
    print("  Social volume:    %+.1f%%" % (
        (results["trip"]["Social"]["rate_per_hour"] - results["pre"]["Social"]["rate_per_hour"])
        / results["pre"]["Social"]["rate_per_hour"] * 100
        if results["pre"]["Social"]["rate_per_hour"] > 0 else 0))
    print("  Social self-ref:  %.0f%% → %.0f%%" % (
        social_pre.get("self_ref_ratio", 0) * 100,
        social_trip.get("self_ref_ratio", 0) * 100))
    print("  X account diversity: %d → %d" % (
        social_pre.get("x_accounts", 0),
        social_trip.get("x_accounts", 0)))
    print("  CCS ext ref ratio: %.3f → %.3f (%+.1f%%)" % (
        pre_ccs["mean_external_ratio"],
        trip_ccs["mean_external_ratio"],
        ext_shift))
    print("  CCS density:       %.4f → %.4f (%+.1f%%)" % (
        pre_ccs["mean_density"],
        trip_ccs["mean_density"],
        density_shift))
    print()
    x_pre = social_pre.get("x_accounts", 0)
    x_trip = social_trip.get("x_accounts", 0)
    x_collapse = x_trip < x_pre * 0.7 if x_pre > 0 else False
    ref_increase = social_trip.get("self_ref_ratio", 0) > social_pre.get("self_ref_ratio", 0) + 0.02
    diversity_down = x_collapse or ref_increase

    print("  Diagnosis: volume %s, diversity %s, structure %s" % (
        "UP" if results["trip"]["Social"]["rate_per_hour"] > results["pre"]["Social"]["rate_per_hour"] else "DOWN",
        "DOWN" if diversity_down else "STABLE",
        "CLOSING" if ext_shift < -5 else "STABLE" if abs(ext_shift) <= 5 else "OPENING",
    ))

    if args.output:
        results["timestamp"] = datetime.utcnow().isoformat()
        results["partition"] = args.partition
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print("\n  Results saved to %s" % args.output)

    db.close()


if __name__ == "__main__":
    main()
