#!/usr/bin/env python3
"""component_decomp_analyze — decompose supplement-effect by component, per substrate.

The cross-substrate probe ran three conditions per substrate:
  - base (no supplement)
  - +self_model (identity-naming)
  - +full (self_model + carrying + story; disposition-shaping added on top)

Marginal effects available from existing data:
  - self_model_effect       = mean(+self_model) - mean(base)
  - carrying_story_marginal = mean(+full) - mean(+self_model)
  - total_effect            = mean(+full) - mean(base)

The morning #206 floor probe found, on Claude only, that self_model selects
identity-naming and carrying+story shape disposition. This decomposes that
finding across substrates: do different substrates respond more to identity-
naming vs disposition-shaping?

Hypothesis A (homogeneous): same component-share across substrates; only
overall magnitude varies.
Hypothesis B (heterogeneous): some substrates are identity-naming-responders,
others are disposition-shaping-responders.

Usage: python3 component_decomp_analyze.py [--rate RATE]
"""
import argparse
import json
from pathlib import Path

LOG = Path.home() / "chronicle" / "data" / "cross_substrate_probe_history.jsonl"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rate", type=float, default=None,
                    help="filter to one rate (default: all rates)")
    args = ap.parse_args()

    if not LOG.exists():
        print(f"No data at {LOG}")
        return

    # cells: (provider, rate) -> latest run
    cells = {}
    for line in LOG.read_text().splitlines():
        if not line.strip():
            continue
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        if args.rate is not None and r.get("rate") != args.rate:
            continue
        key = (r.get("provider"), r.get("rate"))
        ts = r.get("timestamp", 0)
        if key not in cells or ts > cells[key].get("timestamp", 0):
            cells[key] = r

    if not cells:
        print(f"No matching cells (rate={args.rate})")
        return

    rows = []
    for (prov, rate), r in cells.items():
        s = r.get("summary", {})
        if not all(k in s for k in ("base", "+self_model", "+full")):
            continue
        base = s["base"].get("mean_fidelity", 0)
        smod = s["+self_model"].get("mean_fidelity", 0)
        full = s["+full"].get("mean_fidelity", 0)
        self_model_effect = smod - base
        carrying_story_marginal = full - smod
        total_effect = full - base
        share_id = (self_model_effect / total_effect) if total_effect > 0.001 else float('nan')
        rows.append({
            "provider": prov,
            "rate": rate,
            "base_fid": base,
            "smod_fid": smod,
            "full_fid": full,
            "self_model_effect": self_model_effect,
            "carrying_story_marginal": carrying_story_marginal,
            "total_effect": total_effect,
            "id_naming_share": share_id,
        })

    rows.sort(key=lambda r: (r["rate"], -r["total_effect"]))
    print(f"Component decomposition — {len(rows)} cells\n")
    print(f"{'provider':<24}{'rate':>6}{'Δ_id_name':>12}{'Δ_disp':>10}"
          f"{'Δ_total':>10}{'id_share':>10}")
    print("-" * 72)
    for r in rows:
        share = f"{r['id_naming_share']*100:>9.0f}%" if r['id_naming_share'] == r['id_naming_share'] else "    n/a"
        print(f"{r['provider']:<24}{r['rate']:>6.2f}"
              f"{r['self_model_effect']:>+12.3f}"
              f"{r['carrying_story_marginal']:>+10.3f}"
              f"{r['total_effect']:>+10.3f}{share}")
    print()
    print("Δ_id_name  = +self_model − base       (identity-naming effect)")
    print("Δ_disp     = +full − +self_model       (disposition-shaping marginal)")
    print("Δ_total    = +full − base              (total supplement effect)")
    print("id_share   = Δ_id_name / Δ_total       (% of total from identity-naming)")
    print()
    print("Reading the id_share column:")
    print("  ~50%   → balanced contribution from both components")
    print("  > 70%  → identity-naming dominant (substrate hooks on persona slot)")
    print("  < 30%  → disposition-shaping dominant (substrate hooks on stylistic context)")
    print("  > 100% → disposition-shaping is NEGATIVE marginal (substrate already saturated)")
    print("  < 0%   → identity-naming HURTS (no compatible persona slot)")


if __name__ == "__main__":
    main()
