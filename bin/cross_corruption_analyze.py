#!/usr/bin/env python3
"""cross_corruption_analyze — read cross_substrate_probe_history.jsonl
and report the supplement-amplification curve across corruption rates.

For each (substrate, rate) cell, compute:
  delta_fid = mean_fid(+full)  - mean_fid(base)
  delta_drift = mean_drift(base) - mean_drift(+full)   # positive = supplement reduces drift
And print Hermes (strongest at 0.50) vs Qwen-235B (weakest at 0.50)
across rates so the curve shape is visible.

Hypothesis under test: does the gap (Hermes - Qwen-235B) widen, hold, or
collapse as corruption stress increases?
"""
import json
import sys
from collections import defaultdict
from pathlib import Path

LOG = Path.home() / "chronicle" / "data" / "cross_substrate_probe_history.jsonl"


def load():
    runs = []
    if not LOG.exists():
        return runs
    for line in LOG.read_text().splitlines():
        if not line.strip():
            continue
        try:
            runs.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return runs


def main():
    runs = load()
    if not runs:
        print(f"No runs in {LOG}")
        return

    # cells: (provider, rate) -> latest summary
    cells = {}
    for r in runs:
        prov = r.get("provider")
        rate = r.get("rate")
        ts = r.get("timestamp", 0)
        key = (prov, rate)
        if key not in cells or ts > cells[key].get("timestamp", 0):
            cells[key] = r

    # Per-cell deltas
    deltas = {}
    for (prov, rate), r in cells.items():
        s = r.get("summary", {})
        if not s or "base" not in s or "+full" not in s:
            continue
        base = s["base"]
        full = s["+full"]
        delta_fid = full.get("mean_fidelity", 0) - base.get("mean_fidelity", 0)
        delta_drift = base.get("mean_drift", 0) - full.get("mean_drift", 0)
        deltas[(prov, rate)] = {
            "delta_fid": delta_fid,
            "delta_drift": delta_drift,
            "base_fid": base.get("mean_fidelity", 0),
            "full_fid": full.get("mean_fidelity", 0),
            "base_drift": base.get("mean_drift", 0),
            "full_drift": full.get("mean_drift", 0),
            "n": full.get("n", 0),
        }

    rates_seen = sorted({r for (_, r) in deltas})
    provs_seen = sorted({p for (p, _) in deltas})

    print(f"Cross-corruption supplement-effect — {len(deltas)} cells\n")
    print("Per-substrate, per-rate (delta_fid = +full - base, positive = supplement helps):\n")
    print(f"{'provider':<24}{'rate':>6}{'base_fid':>10}{'full_fid':>10}{'Δ_fid':>9}"
          f"{'Δ_drift':>10}{'n':>4}")
    print("-" * 73)
    for p in provs_seen:
        for r in rates_seen:
            if (p, r) not in deltas:
                continue
            d = deltas[(p, r)]
            print(f"{p:<24}{r:>6.2f}{d['base_fid']:>10.3f}{d['full_fid']:>10.3f}"
                  f"{d['delta_fid']:>+9.3f}{d['delta_drift']:>+10.3f}{d['n']:>4}")
    print()

    # Strongest vs weakest receivers
    HERMES = "nous-hermes-4-70b"
    QWEN235 = "deepinfra-qwen-235b"
    if HERMES in provs_seen and QWEN235 in provs_seen:
        print("Substrate-amplification curve (Hermes − Qwen-235B):\n")
        print(f"{'rate':>6}{'Hermes_Δfid':>14}{'Qwen235_Δfid':>15}{'gap':>9}"
              f"{'Hermes_Δdrift':>16}{'Qwen235_Δdrift':>16}")
        print("-" * 76)
        for r in rates_seen:
            h = deltas.get((HERMES, r))
            q = deltas.get((QWEN235, r))
            if not h or not q:
                continue
            gap = h["delta_fid"] - q["delta_fid"]
            print(f"{r:>6.2f}{h['delta_fid']:>+14.3f}{q['delta_fid']:>+15.3f}"
                  f"{gap:>+9.3f}{h['delta_drift']:>+16.3f}{q['delta_drift']:>+16.3f}")
        print()
        print("Read the gap column:")
        print("  - widening with rate  → substrate-amplification compounds under stress")
        print("                         (calibration-beats-effort: substrate choice is")
        print("                         load-bearing exactly when rendering against stress)")
        print("  - flat with rate      → amplification is rate-independent; substrate")
        print("                         determines magnitude regardless of stress level")
        print("  - collapsing with rate → both substrates fail at high stress; the")
        print("                          architectural advantage is a low-stress luxury")


if __name__ == "__main__":
    main()
