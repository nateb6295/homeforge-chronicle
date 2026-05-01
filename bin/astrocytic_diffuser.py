#!/usr/bin/env python3
"""astrocytic_diffuser.py — diffusive counter-process for Chronicle CCS.

v0.3 — word-boundary match for short names:
  - Names <5 chars (Ada, ICP, Nate) now require word-boundary match,
    case-preserving. Fixes false-positive where 'Ada' matched
    adapt/adaptive/readable/Self-Adapting.
  - Longer names (MetastabilityMechanism etc.) still use case-insensitive
    substring — those are distinctive enough.

v0.2 — source-weighted scoring:
  - NOISE_SOURCES (eye, gemma) zeroed in weighted count (high-volume chatter)
  - Entity requires >=2 distinct sources AND weighted >= threshold
  - Reports raw + weighted + source_diversity for each candidate


rotation_audit.py catches focal-entity drift after the fact. This closes the
other half: re-promotes recurring-but-low-salience entities BEFORE they fall
below the floor.

Inspired by the astrocyte-neural-field paper (arxiv 2604.10036). Biology uses
two-stage stabilization: astrocytic diffusion smooths resource asymmetries
continuously + synaptic replenishment transfers the smoothing back into
active state. Chronicle's compressor has neither; recurring-but-momentarily-
low-salience entities silently drop out.

v0.1 — dry-run report only. No state changes. Produces the candidate list so
we can validate the signal before wiring it to compress_cognitive_state.

Mechanism:
  1. Build the historical entity catalog from the last N CCS snapshots
     (default 50). Each entity's peak_salience is its max across snapshots.
  2. Scan activity_feed over the last M hours (default 6). Count case-
     insensitive substring mentions of each cataloged entity name in
     title+content.
  3. Load current CCS focal_entities. For each cataloged entity:
       - ABSENT + mentions >= threshold → PROMOTE candidate
       - PRESENT with salience < floor + mentions >= threshold → BOOST candidate
  4. Print a ranked report.

Usage:
  python3 astrocytic_diffuser.py
  python3 astrocytic_diffuser.py --hours 12 --threshold 3 --floor 0.6
  python3 astrocytic_diffuser.py --snapshots 100
"""
import argparse
import json
import re
import sqlite3
import sys
import time
from pathlib import Path

# Names <5 chars match as whole words (case-preserving). Longer names
# still use case-insensitive substring (distinctive enough).
SHORT_NAME_MAX_LEN = 4

DB = Path("/mnt/hdd/chronicle-data/processed.db")


def load_entity_catalog(con, n_snapshots):
    rows = con.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT ?",
        (n_snapshots,),
    ).fetchall()
    catalog = {}
    for (snap_json,) in rows:
        try:
            snap = json.loads(snap_json)
        except Exception:
            continue
        for ent in snap.get("focal_entities", []):
            name = (ent.get("name") or "").strip()
            if not name:
                continue
            sal = float(ent.get("salience") or 0.0)
            entry = catalog.setdefault(name, {"peak": 0.0, "type": ent.get("type"), "appearances": 0})
            entry["peak"] = max(entry["peak"], sal)
            entry["appearances"] += 1
    return catalog


def load_current_focal(con):
    row = con.execute(
        "SELECT focal_entities FROM cognitive_state WHERE id=1"
    ).fetchone()
    if not row:
        return {}
    try:
        ents = json.loads(row[0])
    except Exception:
        return {}
    return {
        (e.get("name") or "").strip(): float(e.get("salience") or 0.0)
        for e in ents
        if (e.get("name") or "").strip()
    }


# Sources dominated by scene/vision/heartbeat noise — low signal for
# focal-entity promotion. Substring hits here are treated as zero-weight.
NOISE_SOURCES = {"eye", "gemma"}


def count_mentions(con, name, since_ts, exclude_noise=True):
    """Return (total_mentions, source_diversity, weighted_score).

    For short names (<=SHORT_NAME_MAX_LEN), SQL prefilters with case-
    insensitive substring and Python post-filters with word-boundary +
    case-preserving regex. Longer names rely on SQL LIKE alone.

    total_mentions — hits after filtering.
    source_diversity — distinct activity sources with at least one hit.
    weighted_score — sum of hits per source, NOISE_SOURCES zeroed.
    """
    short = len(name) <= SHORT_NAME_MAX_LEN
    like = f"%{name}%"
    rows = con.execute(
        """
        SELECT source, title, content FROM activity_feed
        WHERE created_at >= ?
          AND (title LIKE ? COLLATE NOCASE OR content LIKE ? COLLATE NOCASE)
        """,
        (since_ts, like, like),
    ).fetchall()

    if short:
        # Word-boundary, case-preserving. The agent is "Ada", not "ada"
        # inside "readable" or "adapt".
        pat = re.compile(rf"\b{re.escape(name)}\b")
        def matches(text):
            return bool(text and pat.search(text))
    else:
        pat_i = re.compile(re.escape(name), re.IGNORECASE)
        def matches(text):
            return bool(text and pat_i.search(text))

    from collections import Counter
    per_source = Counter()
    for src, title, content in rows:
        if matches(title) or matches(content):
            per_source[src or ""] += 1

    total = sum(per_source.values())
    sources = len(per_source)
    weighted = 0
    for src, n in per_source.items():
        base_src = src.split(":")[0]
        if exclude_noise and base_src in NOISE_SOURCES:
            continue
        weighted += n
    return total, sources, weighted


def diffuse(hours, threshold, floor, n_snapshots):
    con = sqlite3.connect(DB)
    catalog = load_entity_catalog(con, n_snapshots)
    current = load_current_focal(con)
    since = int(time.time()) - hours * 3600

    promote = []
    boost = []
    stable = []

    for name, meta in catalog.items():
        total, sources, weighted = count_mentions(con, name, since)
        # Signal requires both: enough weighted hits AND at least 2 distinct
        # sources. One chatty source with 100 hits stays filtered.
        if weighted < threshold or sources < 2:
            continue
        cur_sal = current.get(name)
        rec = {
            "name": name,
            "type": meta["type"],
            "peak": meta["peak"],
            "appearances": meta["appearances"],
            "mentions_raw": total,
            "mentions_weighted": weighted,
            "source_diversity": sources,
        }
        if cur_sal is None:
            promote.append(rec)
        elif cur_sal < floor:
            rec["current_salience"] = cur_sal
            boost.append(rec)
        else:
            rec["current_salience"] = cur_sal
            stable.append(rec)

    promote.sort(key=lambda x: (-x["source_diversity"], -x["mentions_weighted"], -x["peak"]))
    boost.sort(key=lambda x: (-x["source_diversity"], -x["mentions_weighted"]))
    stable.sort(key=lambda x: -x["mentions_weighted"])

    print(f"# astrocytic_diffuser v0.3 — dry run")
    print(f"# window: {hours}h   threshold: {threshold}   floor: {floor}   snapshots: {n_snapshots}")
    print(f"# catalog size: {len(catalog)}   current focal: {len(current)}")
    print()

    def fmt(r):
        return (
            f"  {r['name']:<32}  "
            f"weighted={r['mentions_weighted']:<3} raw={r['mentions_raw']:<3} "
            f"sources={r['source_diversity']}  peak={r['peak']:.2f}"
        )

    print(f"## PROMOTE candidates ({len(promote)}) — absent from current focal, recurring in feed")
    if not promote:
        print("  (none)")
    for p in promote:
        print(fmt(p) + f"  type={p['type']}")
    print()

    print(f"## BOOST candidates ({len(boost)}) — present but below salience floor, recurring in feed")
    if not boost:
        print("  (none)")
    for b in boost:
        print(fmt(b) + f"  current={b['current_salience']:.2f}")
    print()

    print(f"## STABLE ({len(stable)}) — already in focal above floor, feed-active")
    for s in stable[:10]:
        print(fmt(s) + f"  current={s['current_salience']:.2f}")
    if len(stable) > 10:
        print(f"  (+ {len(stable) - 10} more)")

    return 0


def diffuse_and_post(hours, threshold, floor, n_snapshots):
    """Silent-unless-findings mode for cron. Computes candidates, posts to
    OPERATOR_WEBHOOK only if PROMOTE or BOOST non-empty. Always exits 0 so
    cron doesn't mail on empty runs."""
    import os
    import urllib.request
    con = sqlite3.connect(DB)
    catalog = load_entity_catalog(con, n_snapshots)
    current = load_current_focal(con)
    since = int(time.time()) - hours * 3600

    promote, boost = [], []
    for name, meta in catalog.items():
        total, sources, weighted = count_mentions(con, name, since)
        if weighted < threshold or sources < 2:
            continue
        cur_sal = current.get(name)
        rec = {
            "name": name,
            "peak": meta["peak"],
            "mentions_raw": total,
            "mentions_weighted": weighted,
            "source_diversity": sources,
        }
        if cur_sal is None:
            promote.append(rec)
        elif cur_sal < floor:
            rec["current_salience"] = cur_sal
            boost.append(rec)
    con.close()

    if not promote and not boost:
        return 0

    promote.sort(key=lambda x: (-x["source_diversity"], -x["mentions_weighted"]))
    boost.sort(key=lambda x: (-x["source_diversity"], -x["mentions_weighted"]))

    def fmt(r, extra=""):
        return (f"- **{r['name']}** weighted={r['mentions_weighted']} "
                f"sources={r['source_diversity']} peak={r['peak']:.2f}{extra}")

    lines = [f"**Diffuser findings** (window={hours}h threshold={threshold})"]
    if promote:
        lines.append(f"__PROMOTE__ ({len(promote)})")
        for p in promote:
            lines.append(fmt(p))
    if boost:
        lines.append(f"__BOOST__ ({len(boost)})")
        for b in boost:
            lines.append(fmt(b, f" current={b['current_salience']:.2f}"))

    webhook = os.environ.get("OPERATOR_WEBHOOK")
    if not webhook:
        env_path = Path.home() / "chronicle" / "chronicle.env"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("OPERATOR_WEBHOOK="):
                    webhook = line.split("=", 1)[1].strip()
                    break
    if not webhook:
        print("[diffuser] OPERATOR_WEBHOOK not set; findings not posted", file=sys.stderr)
        print("\n".join(lines))
        return 0

    payload = json.dumps({"content": "\n".join(lines)[:1800]})
    try:
        req = urllib.request.Request(
            webhook,
            data=payload.encode(),
            headers={
                "Content-Type": "application/json",
                "User-Agent": "chronicle-diffuser/1.0",
            },
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as e:
        print(f"[diffuser] webhook post failed: {e}", file=sys.stderr)
        print("\n".join(lines))
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=6)
    ap.add_argument("--threshold", type=int, default=2)
    ap.add_argument("--floor", type=float, default=0.5)
    ap.add_argument("--snapshots", type=int, default=50)
    ap.add_argument("--post-if-findings", action="store_true",
                    help="Post PROMOTE/BOOST findings to OPERATOR_WEBHOOK. Silent when empty.")
    args = ap.parse_args()
    if args.post_if_findings:
        sys.exit(diffuse_and_post(args.hours, args.threshold, args.floor, args.snapshots))
    sys.exit(diffuse(args.hours, args.threshold, args.floor, args.snapshots))


if __name__ == "__main__":
    main()
