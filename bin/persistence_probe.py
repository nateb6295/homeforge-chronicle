#!/usr/bin/env python3
"""
persistence_probe.py — Compute P_weak and P_strong from CCS history.

Based on Perrier/Bennett (2603.09043) Appendix G persistence algorithm.
Adapted for Chronicle's CCS schema where "identity ingredients" are the
inter-field coherence relationships, not just field presence.

Identity ingredients for Chronicle CCS:
  1. gist — semantic summary
  2. goal_orientation — current objective
  3. focal_entities — key entities with salience
  4. constraints — invariant rules
  5. episodic_trace — recent session memory
  6. predictive_cue — expectation of next

P_weak: each ingredient appears somewhere in the evaluation window.
  (For CCS this is trivially ~1 since schema enforces presence.)

P_strong (adapted): ALL ingredients are COHERENT at a single snapshot.
  Measured by inter-field reference density — do fields reference each
  other's content? gist mentions entities? goal references constraints?

This is the advance 57 prediction made measurable.
"""

import json
import sys
import sqlite3
import re
from pathlib import Path
from datetime import datetime

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
CCS_FIELDS = ["semantic_gist", "goal_orientation", "focal_entities",
              "constraints", "episodic_trace", "predictive_cue"]

# Coherence pairs: which fields should reference each other
COHERENCE_PAIRS = [
    ("semantic_gist", "focal_entities"),
    ("semantic_gist", "goal_orientation"),
    ("goal_orientation", "constraints"),
    ("goal_orientation", "focal_entities"),
    ("episodic_trace", "focal_entities"),
    ("episodic_trace", "goal_orientation"),
    ("predictive_cue", "goal_orientation"),
    ("predictive_cue", "episodic_trace"),
]


def get_ccs_history(limit=50):
    """Fetch CCS snapshots from cognitive_state_history."""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT snapshot, created_at FROM cognitive_state_history "
        "ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()

    snapshots = []
    for state_json, ts in reversed(rows):
        try:
            state = json.loads(state_json)
            snapshots.append({"state": state, "ts": ts})
        except (json.JSONDecodeError, TypeError):
            continue
    return snapshots


def extract_field_text(state, field):
    """Extract text content from a CCS field."""
    val = state.get(field, "")
    if isinstance(val, list):
        if val and isinstance(val[0], dict):
            return " ".join(str(v) for item in val for v in item.values())
        return " ".join(str(v) for v in val)
    if isinstance(val, dict):
        return " ".join(str(v) for v in val.values())
    return str(val)


def extract_key_terms(text, min_len=4):
    """Extract meaningful terms from text for cross-reference checking."""
    words = re.findall(r'[a-zA-Z_-]{%d,}' % min_len, text.lower())
    # Filter common words
    stop = {"that", "this", "with", "from", "have", "been", "will", "what",
            "when", "where", "which", "their", "about", "would", "could",
            "should", "there", "these", "those", "other", "more", "some",
            "into", "over", "after", "before", "between", "under", "each",
            "through", "during", "without", "within", "across", "along",
            "toward", "against", "current", "working", "based", "using"}
    return set(w for w in words if w not in stop)


def measure_coherence(state):
    """
    Measure inter-field coherence for a single CCS snapshot.
    Returns (coherence_score, pair_results).

    coherence_score in [0, 1]: fraction of expected pairs that share terms.
    This is our operationalization of P_strong — not just presence but
    relational structure between fields.
    """
    pair_results = []
    for field_a, field_b in COHERENCE_PAIRS:
        text_a = extract_field_text(state, field_a)
        text_b = extract_field_text(state, field_b)

        terms_a = extract_key_terms(text_a)
        terms_b = extract_key_terms(text_b)

        shared = terms_a & terms_b
        union = terms_a | terms_b

        if not union:
            score = 0.0
        else:
            score = len(shared) / min(len(terms_a), len(terms_b)) if min(len(terms_a), len(terms_b)) > 0 else 0.0

        pair_results.append({
            "pair": (field_a, field_b),
            "shared_terms": len(shared),
            "score": score,
            "examples": list(shared)[:5]
        })

    coherence = sum(1 for p in pair_results if p["score"] > 0.1) / len(COHERENCE_PAIRS)
    return coherence, pair_results


def measure_presence(state):
    """
    Measure P_weak: are all ingredients present (non-empty)?
    For CCS schema this should be ~1.0 but catches degraded states.
    """
    present = 0
    for field in CCS_FIELDS:
        text = extract_field_text(state, field)
        if text and text.strip() and text.strip() not in ("None", "", "[]", "{}"):
            present += 1
    return present / len(CCS_FIELDS)


def compute_persistence(snapshots, window_size=5):
    """
    Compute P_weak and P_strong over evaluation windows.

    window_size: number of consecutive snapshots per window (Delta in paper).
    """
    if len(snapshots) < window_size:
        return None

    n_windows = len(snapshots) - window_size + 1
    p_weak_count = 0
    p_strong_count = 0
    coherence_scores = []

    for i in range(n_windows):
        window = snapshots[i:i + window_size]

        # P_weak: each ingredient appears somewhere in the window
        all_present = True
        for field in CCS_FIELDS:
            found = False
            for snap in window:
                text = extract_field_text(snap["state"], field)
                if text and text.strip() not in ("None", "", "[]", "{}"):
                    found = True
                    break
            if not found:
                all_present = False
                break
        if all_present:
            p_weak_count += 1

        # P_strong: exists a snapshot where ALL ingredients are coherent
        max_coherence = 0.0
        for snap in window:
            coh, _ = measure_coherence(snap["state"])
            max_coherence = max(max_coherence, coh)

        coherence_scores.append(max_coherence)
        # Threshold for co-instantiation: >50% of pairs coherent
        if max_coherence > 0.5:
            p_strong_count += 1

    return {
        "p_weak": p_weak_count / n_windows,
        "p_strong": p_strong_count / n_windows,
        "temporal_gap": (p_strong_count + 1) / (p_weak_count + 1),
        "mean_coherence": sum(coherence_scores) / len(coherence_scores),
        "n_windows": n_windows,
        "window_size": window_size,
    }


def get_live_ccs():
    """Fetch the LIVE cognitive state (not history snapshots).

    The live table is updated by the MCP enrichment layer and reflects
    the actual CCS in use. History snapshots contain the raw Gemma output
    which may diverge from live (discovered 2026-04-18: Gemma gist was stale
    for 2+ compressions while live was correct).
    """
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT semantic_gist, goal_orientation, focal_entities, constraints, "
        "episodic_trace, predictive_cue, updated_at FROM cognitive_state "
        "ORDER BY updated_at DESC LIMIT 1"
    ).fetchone()
    conn.close()

    if not row:
        return None

    return {
        "state": {
            "semantic_gist": row[0] or "",
            "goal_orientation": row[1] or "",
            "focal_entities": json.loads(row[2]) if row[2] else [],
            "constraints": json.loads(row[3]) if row[3] else [],
            "episodic_trace": json.loads(row[4]) if row[4] else [],
            "predictive_cue": row[5] or "",
        },
        "ts": row[6],
    }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Persistence probe (Perrier/Bennett)")
    parser.add_argument("--limit", type=int, default=50, help="CCS snapshots to fetch")
    parser.add_argument("--window", type=int, default=5, help="Window size (Delta)")
    parser.add_argument("--latest", action="store_true", help="Show latest snapshot detail")
    parser.add_argument("--live", action="store_true", help="Measure LIVE CCS (not history snapshots)")
    parser.add_argument("--trend", action="store_true", help="Show coherence trend over time")
    parser.add_argument("--json", action="store_true", help="JSON output")
    args = parser.parse_args()

    if args.live:
        live = get_live_ccs()
        if not live:
            print("No live CCS found.")
            return
        state = live["state"]
        presence = measure_presence(state)
        coherence, pairs = measure_coherence(state)
        ts = live.get("ts", "?")

        if args.json:
            print(json.dumps({
                "source": "live",
                "ts": ts,
                "presence": presence,
                "coherence": coherence,
                "pairs": [{"pair": p["pair"], "score": round(p["score"], 3),
                           "examples": p["examples"]} for p in pairs]
            }, indent=2))
        else:
            print(f"LIVE CCS (ts={ts})")
            print(f"  P_weak (presence): {presence:.2f}")
            print(f"  Coherence (P_strong proxy): {coherence:.2f}")
            print(f"\n  Pair coherence:")
            for p in pairs:
                status = "+" if p["score"] > 0.1 else "-"
                short_a = p["pair"][0].replace("_", "")[:8]
                short_b = p["pair"][1].replace("_", "")[:8]
                examples = ", ".join(p["examples"][:3]) if p["examples"] else "none"
                print(f"    {status} {short_a} <-> {short_b}: {p['score']:.2f} [{examples}]")
            # Compare with latest history snapshot
            history = get_ccs_history(1)
            if history:
                hist_coh, _ = measure_coherence(history[-1]["state"])
                delta = coherence - hist_coh
                print(f"\n  History snapshot coherence: {hist_coh:.2f}")
                print(f"  Live vs history delta: {delta:+.2f}")
                if delta > 0.1:
                    print(f"  NOTE: Live > history — enrichment layer improving coherence.")
        return

    snapshots = get_ccs_history(args.limit)
    if not snapshots:
        print("No CCS snapshots found.")
        return

    if args.trend:
        from datetime import datetime
        print(f"Coherence trend ({len(snapshots)} snapshots):")
        prev_coh = None
        for snap in snapshots:
            state = snap["state"]
            coherence, _ = measure_coherence(state)
            ts = snap.get("ts", 0)
            dt = datetime.fromtimestamp(ts).strftime("%m-%d %H:%M")
            arrow = ""
            if prev_coh is not None:
                if coherence > prev_coh + 0.05:
                    arrow = " ^"
                elif coherence < prev_coh - 0.05:
                    arrow = " v"
            bar = "#" * int(coherence * 20)
            print(f"  {dt}  {coherence:.2f} {bar}{arrow}")
            prev_coh = coherence
        return

    if args.latest:
        latest = snapshots[-1]["state"]
        presence = measure_presence(latest)
        coherence, pairs = measure_coherence(latest)

        if args.json:
            print(json.dumps({
                "presence": presence,
                "coherence": coherence,
                "pairs": [{"pair": p["pair"], "score": round(p["score"], 3),
                           "examples": p["examples"]} for p in pairs]
            }, indent=2))
        else:
            ts = snapshots[-1].get("ts", "?")
            print(f"Latest CCS snapshot (ts={ts})")
            print(f"  P_weak (presence): {presence:.2f}")
            print(f"  Coherence (P_strong proxy): {coherence:.2f}")
            print(f"\n  Pair coherence:")
            for p in pairs:
                status = "+" if p["score"] > 0.1 else "-"
                short_a = p["pair"][0].replace("_", "")[:8]
                short_b = p["pair"][1].replace("_", "")[:8]
                examples = ", ".join(p["examples"][:3]) if p["examples"] else "none"
                print(f"    {status} {short_a} <-> {short_b}: {p['score']:.2f} [{examples}]")
        return

    result = compute_persistence(snapshots, args.window)
    if result is None:
        print(f"Need at least {args.window} snapshots, have {len(snapshots)}.")
        return

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"Persistence probe — {len(snapshots)} snapshots, window={result['window_size']}")
        print(f"  P_weak:         {result['p_weak']:.3f}")
        print(f"  P_strong:       {result['p_strong']:.3f}")
        print(f"  Temporal gap:   {result['temporal_gap']:.3f}")
        print(f"  Mean coherence: {result['mean_coherence']:.3f}")
        print(f"  Windows:        {result['n_windows']}")

        if result['p_weak'] > 0 and result['p_strong'] < result['p_weak']:
            gap = result['p_weak'] - result['p_strong']
            print(f"\n  Identity gap: {gap:.3f}")
            print(f"  Ingredients present but not always co-instantiated.")
            if result['temporal_gap'] < 0.5:
                print(f"  WARNING: Large temporal gap — identity smeared across time.")


if __name__ == "__main__":
    main()
