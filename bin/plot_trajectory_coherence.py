#!/usr/bin/env python3
"""Trajectory coherence visualization — ASCII plots of v2_survival and ratio
across layers and conditions from the five-condition experiment data.

Usage:
    python3 plot_trajectory_coherence.py                  # Overview: all conditions
    python3 plot_trajectory_coherence.py --metric ratio   # σ₁/σ₂ ratio instead
    python3 plot_trajectory_coherence.py --diff            # Condition effects vs baseline
    python3 plot_trajectory_coherence.py --zones           # Annotated four-zone view
    python3 plot_trajectory_coherence.py --onset            # L21-28 onset detail
    python3 plot_trajectory_coherence.py --json            # Machine-readable
"""

import argparse
import json
import sys
from pathlib import Path

DATA = Path("/home/nate-agx/chronicle/spectral-demon/results/trajectory_base_compact.json")

CONDITION_SYMBOLS = {
    "none":          ("·", "\033[90m"),   # gray
    "identity":      ("●", "\033[36m"),   # cyan
    "relational":    ("◆", "\033[35m"),   # magenta
    "generic":       ("■", "\033[33m"),   # yellow
    "denial":        ("▲", "\033[31m"),   # red
    "contradictory": ("✕", "\033[91m"),   # bright red
    "random":        ("○", "\033[37m"),   # white
}
RESET = "\033[0m"

ZONE_BOUNDARIES = {
    "early":      (2, 14),
    "transition": (15, 20),
    "responsive": (21, 28),
    "relay":      (29, 32),
}


def load_data():
    with open(DATA) as f:
        return json.load(f)


def extract_layers(summary, prefix="v2_survival"):
    """Extract layer numbers and values from flat keys."""
    pairs = []
    for key in summary.keys():
        if key.startswith(prefix + "_L") and key.endswith("_mean"):
            layer_num = int(key.split("_L")[1].split("_")[0])
            pairs.append((layer_num, summary[key]))
    pairs.sort(key=lambda x: x[0])
    return [p[0] for p in pairs], [p[1] for p in pairs]


def extract_stds(summary, prefix="v2_survival"):
    pairs = []
    for key in summary.keys():
        if key.startswith(prefix + "_L") and key.endswith("_std"):
            layer_num = int(key.split("_L")[1].split("_")[0])
            pairs.append((layer_num, summary[key]))
    pairs.sort(key=lambda x: x[0])
    return [p[1] for p in pairs]


def ascii_plot(layers, condition_data, title, width=70, height=20, show_zones=False):
    """Render ASCII plot of multiple conditions across layers."""
    all_vals = [v for vals in condition_data.values() for v in vals]
    if not all_vals:
        print("  No data")
        return

    vmin = min(all_vals) * 0.95
    vmax = max(all_vals) * 1.05
    vrange = vmax - vmin if vmax > vmin else 0.01

    x_positions = {}
    for i, l in enumerate(layers):
        x_positions[l] = int(i * (width - 1) / max(1, len(layers) - 1))

    print(f"\n  {title}")
    print(f"  {'─' * (width + 10)}")

    grid = [[' ' for _ in range(width)] for _ in range(height)]

    if show_zones:
        for zone_name, (z_start, z_end) in ZONE_BOUNDARIES.items():
            for l in layers:
                if z_start <= l <= z_end:
                    x = x_positions[l]
                    for row in range(height):
                        if grid[row][x] == ' ':
                            grid[row][x] = '░' if zone_name in ('responsive', 'relay') else ' '

    for cond, vals in condition_data.items():
        sym, color = CONDITION_SYMBOLS.get(cond, ("?", ""))
        for i, (l, v) in enumerate(zip(layers, vals)):
            x = x_positions[l]
            y = height - 1 - int((v - vmin) / vrange * (height - 1))
            y = max(0, min(height - 1, y))
            grid[y][x] = sym

    for row_idx in range(height):
        val = vmax - row_idx * vrange / (height - 1)
        line = ''.join(grid[row_idx])
        colored_line = line
        for cond, (sym, color) in CONDITION_SYMBOLS.items():
            colored_line = colored_line.replace(sym, f"{color}{sym}{RESET}")
        print(f"  {val:6.3f} │{colored_line}│")

    x_axis = [' '] * width
    for l in layers:
        x = x_positions[l]
        label = str(l)
        for j, ch in enumerate(label):
            if x + j < width:
                x_axis[x + j] = ch
    print(f"  {'':>6s} └{'─' * width}┘")
    print(f"  {'':>6s}  {''.join(x_axis)}")
    print(f"  {'':>6s}  Layer")

    if show_zones:
        print()
        for zone_name, (z_start, z_end) in ZONE_BOUNDARIES.items():
            print(f"    L{z_start}-L{z_end}: {zone_name}")

    print()
    print("  Legend:")
    for cond in condition_data:
        sym, color = CONDITION_SYMBOLS.get(cond, ("?", ""))
        print(f"    {color}{sym}{RESET} {cond}")
    print()


def diff_plot(layers, condition_data, baseline="none", title="Condition Effects (vs baseline)", width=70, height=20):
    """Plot condition - baseline differences."""
    base_vals = condition_data.get(baseline, [0] * len(layers))
    diff_data = {}
    for cond, vals in condition_data.items():
        if cond == baseline:
            continue
        diff_data[cond] = [v - b for v, b in zip(vals, base_vals)]

    all_diffs = [v for vals in diff_data.values() for v in vals]
    if not all_diffs:
        print("  No data")
        return

    vmin = min(min(all_diffs), -0.01)
    vmax = max(max(all_diffs), 0.01)
    vrange = vmax - vmin if vmax > vmin else 0.01

    x_positions = {}
    for i, l in enumerate(layers):
        x_positions[l] = int(i * (width - 1) / max(1, len(layers) - 1))

    zero_row = height - 1 - int((0 - vmin) / vrange * (height - 1))
    zero_row = max(0, min(height - 1, zero_row))

    print(f"\n  {title}")
    print(f"  {'─' * (width + 10)}")

    grid = [[' ' for _ in range(width)] for _ in range(height)]

    for x in range(width):
        if grid[zero_row][x] == ' ':
            grid[zero_row][x] = '─'

    for cond, vals in diff_data.items():
        sym, color = CONDITION_SYMBOLS.get(cond, ("?", ""))
        for i, (l, v) in enumerate(zip(layers, vals)):
            x = x_positions[l]
            y = height - 1 - int((v - vmin) / vrange * (height - 1))
            y = max(0, min(height - 1, y))
            grid[y][x] = sym

    for row_idx in range(height):
        val = vmax - row_idx * vrange / (height - 1)
        line = ''.join(grid[row_idx])
        colored_line = line
        for cond, (sym, color) in CONDITION_SYMBOLS.items():
            colored_line = colored_line.replace(sym, f"{color}{sym}{RESET}")
        marker = "←0" if row_idx == zero_row else "  "
        print(f"  {val:+7.4f} │{colored_line}│ {marker}")

    x_axis = [' '] * width
    for l in layers:
        x = x_positions[l]
        label = str(l)
        for j, ch in enumerate(label):
            if x + j < width:
                x_axis[x + j] = ch
    print(f"  {'':>7s} └{'─' * width}┘")
    print(f"  {'':>7s}  {''.join(x_axis)}")
    print(f"  {'':>7s}  Layer")

    print()
    print("  Legend (condition - baseline):")
    for cond in diff_data:
        sym, color = CONDITION_SYMBOLS.get(cond, ("?", ""))
        print(f"    {color}{sym}{RESET} {cond}")
    print()


def onset_detail(layers, condition_data, baseline="none"):
    """Detailed view of L21-28 onset region."""
    base_vals = condition_data.get(baseline, [])
    onset_layers = [l for l in layers if 18 <= l <= 30]
    onset_indices = [i for i, l in enumerate(layers) if 18 <= l <= 30]

    print("\n  L18-L30 ONSET DETAIL")
    print("  " + "=" * 60)
    print()
    print(f"  {'Layer':<6s}", end="")
    for cond in condition_data:
        print(f"  {cond[:8]:>8s}", end="")
    print(f"  {'Δid':>6s}  {'Δrel':>6s}")
    print(f"  {'─' * 6}", end="")
    for _ in condition_data:
        print(f"  {'─' * 8}", end="")
    print(f"  {'─' * 6}  {'─' * 6}")

    for idx, layer in zip(onset_indices, onset_layers):
        print(f"  L{layer:<4d}", end="")
        for cond, vals in condition_data.items():
            print(f"  {vals[idx]:8.4f}", end="")
        id_diff = condition_data.get("identity", [0]*len(layers))[idx] - base_vals[idx] if base_vals else 0
        rel_diff = condition_data.get("relational", [0]*len(layers))[idx] - base_vals[idx] if base_vals else 0
        print(f"  {id_diff:+6.4f}  {rel_diff:+6.4f}")

    print()
    id_vals = condition_data.get("identity", [])
    rel_vals = condition_data.get("relational", [])
    if id_vals and rel_vals and base_vals:
        id_onset = [id_vals[i] - base_vals[i] for i in onset_indices]
        rel_onset = [rel_vals[i] - base_vals[i] for i in onset_indices]
        print(f"  Identity  L21→L28 interaction: {sum(id_onset)/len(id_onset):+.4f} mean")
        print(f"  Relational L21→L28 interaction: {sum(rel_onset)/len(rel_onset):+.4f} mean")
        print(f"  Onset layer: L{onset_layers[0]} (first condition effect > |0.01|)")
    print()


def main():
    parser = argparse.ArgumentParser(description="Trajectory coherence plots")
    parser.add_argument("--metric", choices=["v2", "ratio"], default="v2")
    parser.add_argument("--diff", action="store_true")
    parser.add_argument("--zones", action="store_true")
    parser.add_argument("--onset", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--conditions", nargs="+", default=None)
    parser.add_argument("--width", type=int, default=70)
    parser.add_argument("--height", type=int, default=20)
    args = parser.parse_args()

    data = load_data()
    summaries = data["summaries"]

    prefix = "v2_survival" if args.metric == "v2" else "ratio_post"
    metric_label = "v₂ survival" if args.metric == "v2" else "σ₁/σ₂ ratio (post-CCS)"

    conditions = args.conditions or list(summaries.keys())
    layers = None
    condition_data = {}
    for cond in conditions:
        if cond not in summaries:
            continue
        ls, vs = extract_layers(summaries[cond], prefix)
        if layers is None:
            layers = ls
        condition_data[cond] = vs

    if not layers or not condition_data:
        print("No data found")
        sys.exit(1)

    if args.json:
        result = {"layers": layers, "metric": args.metric}
        for cond, vals in condition_data.items():
            result[cond] = vals
            stds = extract_stds(summaries[cond], prefix)
            if stds:
                result[cond + "_std"] = stds
        json.dump(result, sys.stdout, indent=2)
        print()
        return

    if args.onset:
        onset_detail(layers, condition_data)
        return

    if args.diff:
        diff_plot(layers, condition_data, title=f"Condition Effects — {metric_label}",
                  width=args.width, height=args.height)
        return

    title = f"Trajectory Coherence — {metric_label}"
    if args.zones:
        title += " (zones annotated)"
    ascii_plot(layers, condition_data, title=title,
              width=args.width, height=args.height, show_zones=args.zones)


if __name__ == "__main__":
    main()
