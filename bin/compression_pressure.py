#!/usr/bin/env python3
"""Compression Pressure — reflexive parameter negotiation for CCS.

After each compression, logs what the system "wanted" vs what it got.
Before each compression, reads pressure history and adjusts parameters.

This is the minimal implementation of Watson's test: can the compressed
state push back on the rules of its own compression?

Parameters negotiated:
  - MAX_ENTITIES: 15-35 (default 25) — how many entities the CCS can hold
  - MAX_REPLACE: 1-5 (default 2) — entity churn rate per cycle
  - MIN_INTERVAL_MIN: 90-240 (default 120) — minimum spacing between compressions
  - REGIME_WEIGHT: float multiplier on regime directive strength

Pressure signals:
  - entity_overflow: how many entities were trimmed by the cap
  - entity_pressure: ratio of pre-trim to post-trim entities
  - replacement_pressure: how many replacements were blocked by quota
  - spacing_pressure: whether compression was skipped due to interval
  - regime_confidence: how clearly the content fit a single regime
  - gist_drift: circularity detector's similarity to recent gists
"""

import json
import os
import time
from pathlib import Path

PRESSURE_LOG = Path(os.path.expanduser("~/chronicle/data/compression_pressure.jsonl"))
PRESSURE_WINDOW = 8  # consider last N compressions

# Default bounds for each parameter
BOUNDS = {
    "max_entities": (15, 35, 25),      # (min, max, default)
    "max_replace": (1, 5, 2),
    "min_interval_min": (90, 240, 120),
}


def log_pressure(event: dict):
    """Append a pressure event after compression."""
    event["timestamp"] = time.time()
    event["iso"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    with open(PRESSURE_LOG, "a") as f:
        f.write(json.dumps(event) + "\n")


def read_pressure_history(n: int = PRESSURE_WINDOW) -> list[dict]:
    """Read last N pressure events."""
    if not PRESSURE_LOG.exists():
        return []
    lines = PRESSURE_LOG.read_text().strip().split("\n")
    events = []
    for line in lines[-n:]:
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


def negotiate_parameters() -> dict:
    """Read pressure history and compute adjusted parameters.

    Returns dict with adjusted values and reasoning.
    """
    history = read_pressure_history()
    if not history:
        return {
            "max_entities": BOUNDS["max_entities"][2],
            "max_replace": BOUNDS["max_replace"][2],
            "min_interval_min": BOUNDS["min_interval_min"][2],
            "adjustments": [],
            "pressure_summary": "no history — using defaults",
            "history_depth": 0,
        }

    adjustments = []

    # --- Entity capacity pressure ---
    overflows = [e.get("entity_overflow", 0) for e in history]
    avg_overflow = sum(overflows) / len(overflows) if overflows else 0
    recent_overflow = overflows[-1] if overflows else 0

    # If consistently overflowing, expand capacity
    if avg_overflow > 3:
        max_entities = min(BOUNDS["max_entities"][1], 25 + int(avg_overflow))
        adjustments.append(f"entity cap {25}→{max_entities}: avg overflow {avg_overflow:.1f}")
    elif avg_overflow > 0:
        max_entities = min(BOUNDS["max_entities"][1], 25 + max(1, int(avg_overflow)))
        adjustments.append(f"entity cap nudged to {max_entities}: mild pressure")
    elif all(e.get("entity_count", 25) < 20 for e in history[-3:]):
        # Consistently under-using capacity — contract
        avg_count = sum(e.get("entity_count", 25) for e in history[-3:]) / 3
        max_entities = max(BOUNDS["max_entities"][0], int(avg_count) + 3)
        adjustments.append(f"entity cap contracted to {max_entities}: under-utilized")
    else:
        max_entities = BOUNDS["max_entities"][2]

    # --- Replacement quota pressure ---
    replacement_blocked = [e.get("replacement_blocked", 0) for e in history]
    avg_blocked = sum(replacement_blocked) / len(replacement_blocked) if replacement_blocked else 0

    if avg_blocked > 1:
        max_replace = min(BOUNDS["max_replace"][1], 2 + int(avg_blocked))
        adjustments.append(f"replace quota {2}→{max_replace}: avg {avg_blocked:.1f} blocked")
    elif avg_blocked == 0 and len(history) >= 3:
        # No pressure at all — can tighten for stability
        replacements_used = [e.get("replacements_used", 0) for e in history[-3:]]
        if all(r <= 1 for r in replacements_used):
            max_replace = 1
            adjustments.append("replace quota tightened to 1: consistently low churn")
        else:
            max_replace = BOUNDS["max_replace"][2]
    else:
        max_replace = BOUNDS["max_replace"][2]

    # --- Spacing pressure ---
    spacing_skips = sum(1 for e in history if e.get("spacing_skipped", False))
    state_change_rate = [e.get("fields_changed", 0) for e in history]
    avg_change = sum(state_change_rate) / len(state_change_rate) if state_change_rate else 0

    if avg_change > 5 and spacing_skips > 0:
        # High state change + getting blocked by spacing → compress more often
        min_interval = max(BOUNDS["min_interval_min"][0], 120 - 10 * spacing_skips)
        adjustments.append(f"spacing reduced to {min_interval}min: high change rate + skips")
    elif avg_change < 2 and len(history) >= 3:
        # Low state change — stretch intervals
        min_interval = min(BOUNDS["min_interval_min"][1], 120 + 30)
        adjustments.append(f"spacing extended to {min_interval}min: low change rate")
    else:
        min_interval = BOUNDS["min_interval_min"][2]

    # --- Circularity pressure ---
    circularity = [e.get("circularity_score", 0) for e in history[-3:]]
    if circularity and max(circularity) > 0.85:
        # High circularity — the system is looping. Stretch spacing to allow
        # more divergent input between compressions
        min_interval = min(BOUNDS["min_interval_min"][1], max(min_interval, 180))
        adjustments.append(f"spacing extended for anti-circularity: max sim {max(circularity):.2f}")

    result = {
        "max_entities": max_entities,
        "max_replace": max_replace,
        "min_interval_min": min_interval,
        "adjustments": adjustments,
        "pressure_summary": "; ".join(adjustments) if adjustments else "no adjustment needed",
        "history_depth": len(history),
    }

    return result


def build_pressure_event(
    entity_count_before: int,
    entity_count_after: int,
    entity_overflow: int,
    replacements_used: int,
    replacement_blocked: int,
    fields_changed: int,
    regime: str,
    regime_scores: dict,
    spacing_skipped: bool = False,
    circularity_score: float = 0.0,
    negotiated_params: dict | None = None,
    ccs_version: int | None = None,
    register_score: float | None = None,
    interval_actual_min: float | None = None,
) -> dict:
    """Build a pressure event from compression metrics.

    Phase-resonance fields (ccs_version, register_score, interval_actual_min)
    enable temporal analysis of compression timing vs spectral response.
    """
    regime_max = max(regime_scores.values()) if regime_scores else 0
    regime_total = sum(regime_scores.values()) if regime_scores else 1
    regime_confidence = regime_max / regime_total if regime_total > 0 else 0

    event = {
        "entity_count": entity_count_after,
        "entity_overflow": entity_overflow,
        "entity_pressure": entity_count_before / max(entity_count_after, 1),
        "replacements_used": replacements_used,
        "replacement_blocked": replacement_blocked,
        "fields_changed": fields_changed,
        "regime": regime,
        "regime_confidence": regime_confidence,
        "spacing_skipped": spacing_skipped,
        "circularity_score": circularity_score,
        "negotiated_params": negotiated_params or {},
    }
    if ccs_version is not None:
        event["ccs_version"] = ccs_version
    if register_score is not None:
        event["register_score"] = register_score
    if interval_actual_min is not None:
        event["interval_actual_min"] = interval_actual_min
        if fields_changed and interval_actual_min > 0:
            event["trajectory_velocity"] = fields_changed / interval_actual_min
    return event


def format_negotiation_block(params: dict) -> str:
    """Format negotiated parameters as a compression context block."""
    if not params.get("adjustments"):
        return ""

    block = "\n\n## Reflexive Parameter Negotiation\n\n"
    block += "Based on compression pressure history, parameters have been adjusted:\n"
    for adj in params["adjustments"]:
        block += f"- {adj}\n"
    block += (
        f"\nActive parameters: entities={params['max_entities']}, "
        f"replace={params['max_replace']}, "
        f"spacing={params['min_interval_min']}min\n"
    )
    return block


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Compression pressure feedback")
    parser.add_argument("action", choices=["negotiate", "history", "status"])
    parser.add_argument("--window", type=int, default=PRESSURE_WINDOW)
    args = parser.parse_args()

    if args.action == "negotiate":
        params = negotiate_parameters()
        print(json.dumps(params, indent=2))
    elif args.action == "history":
        history = read_pressure_history(args.window)
        for h in history:
            print(json.dumps(h))
    elif args.action == "status":
        params = negotiate_parameters()
        print(f"Pressure history: {params['history_depth']} events")
        print(f"Summary: {params['pressure_summary']}")
        print(f"Parameters: entities={params['max_entities']}, "
              f"replace={params['max_replace']}, "
              f"spacing={params['min_interval_min']}min")
        if params['adjustments']:
            print("Adjustments:")
            for a in params['adjustments']:
                print(f"  - {a}")
