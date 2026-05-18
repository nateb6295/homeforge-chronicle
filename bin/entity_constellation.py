#!/usr/bin/env python3
"""Entity constellation — the attractor core as a star map.

Renders entity persistence tiers as a constellation diagram.
Core entities are bright stars connected by arcs weighted by co-occurrence.
Stable entities are dimmer. Coupled entities are faint dust.
"""
import json
import math
import os
import sqlite3
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
import numpy as np

DB = "/mnt/hdd/chronicle-data/processed.db"
OUT = Path.home() / "chronicle" / "data" / "visualizations" / "entity_constellation.png"


def load_entity_data():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY created_at"
    ).fetchall()
    db.close()

    total = len(rows)
    counts = defaultdict(int)
    cooccur = defaultdict(lambda: defaultdict(int))
    salience_sum = defaultdict(float)
    salience_count = defaultdict(int)

    for r in rows:
        snap = json.loads(r[0])
        ents = []
        for e in snap.get("focal_entities", []):
            if isinstance(e, dict) and e.get("name"):
                name = e["name"]
                ents.append(name)
                counts[name] += 1
                salience_sum[name] += e.get("salience", 0.5)
                salience_count[name] += 1

        for i, a in enumerate(ents):
            for b in ents[i + 1:]:
                cooccur[a][b] += 1
                cooccur[b][a] += 1

    entities = {}
    for name, count in counts.items():
        pct = count / total
        mean_sal = salience_sum[name] / salience_count[name] if salience_count[name] > 0 else 0.5
        if pct >= 0.9:
            tier = "core"
        elif pct >= 0.5:
            tier = "stable"
        else:
            tier = "coupled"
        entities[name] = {
            "persistence": pct,
            "tier": tier,
            "mean_salience": mean_sal,
            "connections": dict(cooccur.get(name, {})),
        }

    return entities, total


def layout_constellation(entities):
    core = [(n, e) for n, e in entities.items() if e["tier"] == "core"]
    stable = [(n, e) for n, e in entities.items() if e["tier"] == "stable"]
    coupled = [(n, e) for n, e in entities.items() if e["tier"] == "coupled"]

    positions = {}
    np.random.seed(42)

    # Core entities in a rough circle at center
    n_core = len(core)
    for i, (name, _) in enumerate(sorted(core, key=lambda x: -x[1]["mean_salience"])):
        angle = 2 * math.pi * i / n_core - math.pi / 2
        r = 0.25 + 0.08 * np.random.randn()
        positions[name] = (0.5 + r * math.cos(angle), 0.5 + r * math.sin(angle))

    # Stable entities in a wider ring
    for i, (name, _) in enumerate(stable):
        angle = 2 * math.pi * i / max(len(stable), 1) + 0.5
        r = 0.42 + 0.05 * np.random.randn()
        positions[name] = (0.5 + r * math.cos(angle), 0.5 + r * math.sin(angle))

    # Coupled entities scattered in outer region
    for i, (name, _) in enumerate(coupled):
        angle = 2 * math.pi * (i + 0.5 * np.random.rand()) / max(len(coupled), 1)
        r = 0.55 + 0.15 * np.random.rand()
        x = 0.5 + r * math.cos(angle)
        y = 0.5 + r * math.sin(angle)
        x = max(0.05, min(0.95, x))
        y = max(0.05, min(0.95, y))
        positions[name] = (x, y)

    return positions


def render(entities, total, positions):
    fig, ax = plt.subplots(1, 1, figsize=(18, 18), facecolor='#050510')
    ax.set_facecolor('#050510')
    ax.set_xlim(-0.05, 1.05)
    ax.set_ylim(-0.05, 1.05)

    # Draw connection arcs (only between entities that co-occur frequently)
    max_cooccur = max(
        (v for e in entities.values() for v in e["connections"].values()),
        default=1
    )
    for name, ent in entities.items():
        if name not in positions:
            continue
        x1, y1 = positions[name]
        for other, count in ent["connections"].items():
            if other not in positions or other <= name:
                continue
            strength = count / max_cooccur
            if strength < 0.3:
                continue
            x2, y2 = positions[other]

            tier1 = ent["tier"]
            tier2 = entities[other]["tier"]

            if tier1 == "core" and tier2 == "core":
                color = '#ffd700'
                alpha = 0.15 + 0.25 * strength
                lw = 0.5 + 1.5 * strength
            elif "core" in (tier1, tier2):
                color = '#8888cc'
                alpha = 0.08 + 0.12 * strength
                lw = 0.3 + 0.7 * strength
            else:
                color = '#444466'
                alpha = 0.05 + 0.05 * strength
                lw = 0.2

            ax.plot([x1, x2], [y1, y2], color=color, alpha=alpha, linewidth=lw)

    # Draw entities as stars
    tier_config = {
        "core": {"size_base": 300, "size_sal": 400, "color": '#ffd700', "glow": '#ffaa00',
                 "label_size": 10, "label_color": '#ffffff'},
        "stable": {"size_base": 80, "size_sal": 120, "color": '#7799dd', "glow": '#5577bb',
                   "label_size": 8, "label_color": '#aabbcc'},
        "coupled": {"size_base": 8, "size_sal": 20, "color": '#445566', "glow": None,
                    "label_size": 0, "label_color": '#556677'},
    }

    for name, ent in entities.items():
        if name not in positions:
            continue
        x, y = positions[name]
        cfg = tier_config[ent["tier"]]
        size = cfg["size_base"] + cfg["size_sal"] * ent["mean_salience"]

        # Glow effect for core/stable
        if cfg["glow"]:
            ax.scatter(x, y, s=size * 3, c=cfg["glow"], alpha=0.08, zorder=2)
            ax.scatter(x, y, s=size * 1.5, c=cfg["glow"], alpha=0.15, zorder=3)

        ax.scatter(x, y, s=size, c=cfg["color"], alpha=0.9, zorder=5,
                  edgecolors='white', linewidths=0.3 if ent["tier"] == "core" else 0)

        if cfg["label_size"] > 0:
            label = name
            if len(label) > 25:
                label = label[:22] + "..."

            txt = ax.text(x, y - 0.025, label,
                         fontsize=cfg["label_size"], color=cfg["label_color"],
                         ha='center', va='top', fontfamily='monospace',
                         fontweight='bold' if ent["tier"] == "core" else 'normal')
            txt.set_path_effects([
                pe.withStroke(linewidth=2, foreground='#050510')
            ])

            # Persistence percentage for core
            if ent["tier"] == "core":
                ax.text(x, y - 0.042, f'{ent["persistence"]:.0%}',
                       fontsize=7, color='#888888', ha='center', va='top',
                       fontfamily='monospace')

    # Title
    ax.text(0.5, 0.97, "Entity Constellation",
           transform=ax.transAxes, fontsize=20, fontweight='bold',
           color='#ffffff', ha='center', va='top', fontfamily='monospace')
    ax.text(0.5, 0.945, "The Identity Attractor — Who Persists",
           transform=ax.transAxes, fontsize=12, color='#888888',
           ha='center', va='top', fontfamily='monospace')

    # Legend
    legend_y = 0.08
    for tier, label, color in [("core", "Core (>90%)", '#ffd700'),
                                ("stable", "Stable (50-90%)", '#7799dd'),
                                ("coupled", "Coupled (<50%)", '#445566')]:
        count = len([e for e in entities.values() if e["tier"] == tier])
        ax.scatter(0.03, legend_y, s=50, c=color, transform=ax.transAxes, zorder=10)
        ax.text(0.05, legend_y, f"{label} — {count} entities",
               transform=ax.transAxes, fontsize=9, color='#aaaaaa',
               va='center', fontfamily='monospace')
        legend_y -= 0.025

    ax.text(0.97, 0.03, f"{total} CCS snapshots  •  {len(entities)} unique entities",
           transform=ax.transAxes, fontsize=8, color='#555555',
           ha='right', va='bottom', fontfamily='monospace')

    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.set_xticks([])
    ax.set_yticks([])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT, dpi=150, facecolor='#050510', bbox_inches='tight', pad_inches=0.3)
    plt.close()
    print(f"  Constellation → {OUT}")


def main():
    print("=== Entity Constellation ===\n")
    entities, total = load_entity_data()
    core = [n for n, e in entities.items() if e["tier"] == "core"]
    stable = [n for n, e in entities.items() if e["tier"] == "stable"]
    coupled = [n for n, e in entities.items() if e["tier"] == "coupled"]
    print(f"Entities: {len(core)} core, {len(stable)} stable, {len(coupled)} coupled")
    print(f"Core: {', '.join(sorted(core))}")

    positions = layout_constellation(entities)
    render(entities, total, positions)


if __name__ == "__main__":
    main()
