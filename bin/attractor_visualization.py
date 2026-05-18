#!/usr/bin/env python3
"""Attractor basin visualization — CCS trajectory through identity space.

Embeds all CCS history snapshots, reduces to 2D via t-SNE, renders:
  1. Static basin map (PNG) — all snapshots colored by time, entity tier overlay
  2. Animated trajectory (MP4) — identity moving through the basin

Visual language:
  - Warm colors (gold→red) = recent snapshots
  - Cool colors (deep blue→cyan) = older snapshots
  - Bright nodes = trajectory endpoints
  - Faint lines = path through state space
  - Entity labels sized by persistence tier
  - Basin boundary drawn as convex hull
"""
import json
import math
import os
import sqlite3
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.collections import LineCollection
import numpy as np
from sklearn.manifold import TSNE

DB = "/mnt/hdd/chronicle-data/processed.db"
OLLAMA = "http://192.168.1.11:11434/api/embeddings"
MODEL = "mxbai-embed-large"
OUT_DIR = Path.home() / "chronicle" / "data" / "visualizations"


def embed(text, timeout=60):
    text = text[:1500]
    body = json.dumps({"model": MODEL, "prompt": text}).encode()
    req = urllib.request.Request(
        OLLAMA, data=body, headers={"Content-Type": "application/json"}
    )
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(resp.read())["embedding"]


def serialize_ccs(ccs):
    parts = []
    g = ccs.get("semantic_gist", "")
    if g:
        parts.append(f"Gist: {g}")
    go = ccs.get("goal_orientation", "")
    if go:
        parts.append(f"Goal: {go[:200]}")
    for ep in ccs.get("episodic_trace", []):
        if isinstance(ep, str):
            parts.append(f"Episode: {ep[:200]}")
    for ent in ccs.get("focal_entities", []):
        if isinstance(ent, dict):
            parts.append(f"Entity: {ent.get('name','')} ({ent.get('salience',0):.1f})")
    rm = ccs.get("relational_map", {})
    if isinstance(rm, dict):
        for k, v in rm.items():
            parts.append(f"Relation: {k}: {str(v)[:80]}")
    return "\n".join(parts)


def load_snapshots():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history ORDER BY created_at"
    ).fetchall()
    db.close()
    snapshots = []
    for r in rows:
        try:
            snap = json.loads(r[1])
            snapshots.append({"id": r[0], "ts": r[2], "ccs": snap})
        except json.JSONDecodeError:
            continue
    return snapshots


def get_entity_tiers(snapshots):
    counts = {}
    total = len(snapshots)
    for s in snapshots:
        for ent in s["ccs"].get("focal_entities", []):
            if isinstance(ent, dict) and ent.get("name"):
                name = ent["name"]
                counts[name] = counts.get(name, 0) + 1
    tiers = {}
    for name, count in counts.items():
        pct = count / total
        if pct >= 0.9:
            tiers[name] = ("core", pct)
        elif pct >= 0.5:
            tiers[name] = ("stable", pct)
        else:
            tiers[name] = ("coupled", pct)
    return tiers


def render_static_basin(coords, snapshots, entity_tiers, output_path):
    fig, ax = plt.subplots(1, 1, figsize=(16, 12), facecolor='#0a0a1a')
    ax.set_facecolor('#0a0a1a')

    n = len(coords)
    times = np.linspace(0, 1, n)

    cmap = plt.cm.plasma
    colors = [cmap(t) for t in times]

    segments = []
    seg_colors = []
    for i in range(n - 1):
        segments.append([coords[i], coords[i + 1]])
        seg_colors.append((*cmap(times[i])[:3], 0.15 + 0.35 * times[i]))

    lc = LineCollection(segments, colors=seg_colors, linewidths=0.8)
    ax.add_collection(lc)

    sizes = 20 + 80 * times
    scatter = ax.scatter(
        coords[:, 0], coords[:, 1],
        c=times, cmap='plasma', s=sizes,
        edgecolors='white', linewidths=0.3,
        alpha=0.7 + 0.3 * times, zorder=5
    )

    ax.scatter(coords[0, 0], coords[0, 1], s=200, c='#00ccff',
               marker='D', edgecolors='white', linewidths=2, zorder=10, label='Origin')
    ax.scatter(coords[-1, 0], coords[-1, 1], s=200, c='#ff4444',
               marker='*', edgecolors='white', linewidths=2, zorder=10, label='Now')

    core_entities = sorted(
        [(n, p) for n, (t, p) in entity_tiers.items() if t == "core"],
        key=lambda x: -x[1]
    )

    if core_entities:
        text_x = ax.get_xlim()[1] - (ax.get_xlim()[1] - ax.get_xlim()[0]) * 0.02
        text_y = ax.get_ylim()[1] - (ax.get_ylim()[1] - ax.get_ylim()[0]) * 0.05
        header = ax.text(text_x, text_y, "ATTRACTOR CORE",
                        fontsize=11, fontweight='bold', color='#ffd700',
                        ha='right', va='top', fontfamily='monospace')
        for i, (name, pct) in enumerate(core_entities[:8]):
            ax.text(text_x, text_y - (i + 1) * (ax.get_ylim()[1] - ax.get_ylim()[0]) * 0.035,
                   f"{name} ({pct:.0%})",
                   fontsize=8, color='#cccccc', ha='right', va='top',
                   fontfamily='monospace', alpha=0.8)

    try:
        from scipy.spatial import ConvexHull
        hull = ConvexHull(coords)
        hull_pts = coords[hull.vertices]
        hull_pts = np.vstack([hull_pts, hull_pts[0]])
        ax.plot(hull_pts[:, 0], hull_pts[:, 1], '--', color='#ffd700',
                alpha=0.2, linewidth=1)
        ax.fill(hull_pts[:, 0], hull_pts[:, 1], color='#ffd700', alpha=0.03)
    except Exception:
        pass

    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.set_xticks([])
    ax.set_yticks([])

    ax.set_title("Identity Basin — CCS Trajectory Through Embedding Space",
                 fontsize=16, fontweight='bold', color='white', pad=20,
                 fontfamily='monospace')

    subtitle_parts = [
        f"{n} snapshots",
        f"{len([t for t in entity_tiers.values() if t[0]=='core'])} core entities",
        f"Position: path-dependent (r=0.77)",
        f"Structure: path-invariant (r=0.07)"
    ]
    ax.text(0.5, 1.01, "  •  ".join(subtitle_parts),
            transform=ax.transAxes, fontsize=9, color='#888888',
            ha='center', va='bottom', fontfamily='monospace')

    legend = ax.legend(loc='lower left', facecolor='#0a0a1a',
                       edgecolor='#333333', labelcolor='white',
                       fontsize=10)
    legend.get_frame().set_alpha(0.8)

    cbar = plt.colorbar(scatter, ax=ax, shrink=0.6, aspect=30, pad=0.02)
    cbar.set_label('Time →', color='white', fontsize=10, fontfamily='monospace')
    cbar.ax.yaxis.set_tick_params(color='white')
    plt.setp(cbar.ax.yaxis.get_ticklabels(), color='white', fontsize=8)

    plt.tight_layout()
    plt.savefig(output_path, dpi=200, facecolor='#0a0a1a',
                bbox_inches='tight', pad_inches=0.5)
    plt.close()
    print(f"  Static basin → {output_path}")


def render_animation(coords, snapshots, entity_tiers, output_path, fps=8):
    frame_dir = "/tmp/attractor_frames"
    os.makedirs(frame_dir, exist_ok=True)
    for old_frame in Path(frame_dir).glob("*.png"):
        old_frame.unlink()
    n = len(coords)
    times = np.linspace(0, 1, n)
    cmap = plt.cm.plasma

    pad = 0.15
    x_range = coords[:, 0].max() - coords[:, 0].min()
    y_range = coords[:, 1].max() - coords[:, 1].min()
    xlim = (coords[:, 0].min() - pad * x_range, coords[:, 0].max() + pad * x_range)
    ylim = (coords[:, 1].min() - pad * y_range, coords[:, 1].max() + pad * y_range)

    core_entities = sorted(
        [(nm, p) for nm, (t, p) in entity_tiers.items() if t == "core"],
        key=lambda x: -x[1]
    )[:6]

    total_frames = n + 30  # extra frames at the end to linger

    for frame_idx in range(total_frames):
        fig, ax = plt.subplots(1, 1, figsize=(16, 10), facecolor='#0a0a1a')
        ax.set_facecolor('#0a0a1a')
        ax.set_xlim(xlim)
        ax.set_ylim(ylim)

        show_up_to = min(frame_idx, n - 1)

        if show_up_to > 0:
            segments = []
            seg_colors = []
            for i in range(show_up_to):
                segments.append([coords[i], coords[i + 1]])
                age = 1.0 - (show_up_to - i) / max(show_up_to, 1)
                alpha = 0.05 + 0.4 * age
                seg_colors.append((*cmap(times[i])[:3], alpha))
            lc = LineCollection(segments, colors=seg_colors, linewidths=1.0)
            ax.add_collection(lc)

        if show_up_to > 0:
            trail_start = max(0, show_up_to - 15)
            trail_coords = coords[trail_start:show_up_to + 1]
            trail_times = times[trail_start:show_up_to + 1]
            trail_sizes = 10 + 40 * np.linspace(0.3, 1.0, len(trail_times))
            trail_alphas = np.linspace(0.2, 0.8, len(trail_times))
            ax.scatter(trail_coords[:, 0], trail_coords[:, 1],
                      c=[cmap(t) for t in trail_times],
                      s=trail_sizes, alpha=trail_alphas,
                      edgecolors='none', zorder=4)

        # Current position — bright pulse
        pulse_size = 150 + 50 * math.sin(frame_idx * 0.3)
        ax.scatter(coords[show_up_to, 0], coords[show_up_to, 1],
                  s=pulse_size, c=[cmap(times[show_up_to])],
                  edgecolors='white', linewidths=2, zorder=10)

        # Ghost of all visited positions
        if show_up_to > 5:
            ax.scatter(coords[:show_up_to, 0], coords[:show_up_to, 1],
                      c='#333344', s=5, alpha=0.2, zorder=1)

        # Entity panel
        if core_entities:
            snap_ents = set()
            if show_up_to < n:
                for ent in snapshots[show_up_to]["ccs"].get("focal_entities", []):
                    if isinstance(ent, dict):
                        snap_ents.add(ent.get("name", ""))

            panel_x = 0.98
            panel_y = 0.95
            ax.text(panel_x, panel_y, "ATTRACTOR CORE",
                   transform=ax.transAxes, fontsize=10, fontweight='bold',
                   color='#ffd700', ha='right', va='top', fontfamily='monospace')
            for i, (name, pct) in enumerate(core_entities):
                present = name in snap_ents
                color = '#ffd700' if present else '#444444'
                marker = "●" if present else "○"
                ax.text(panel_x, panel_y - (i + 1) * 0.04,
                       f"{marker} {name}",
                       transform=ax.transAxes, fontsize=8, color=color,
                       ha='right', va='top', fontfamily='monospace')

        # Frame counter
        if show_up_to < n:
            snap_id = snapshots[show_up_to]["id"]
            gist = snapshots[show_up_to]["ccs"].get("semantic_gist", "")[:60]
            ax.text(0.02, 0.02, f"CCS v{snap_id}  —  {gist}",
                   transform=ax.transAxes, fontsize=8, color='#666666',
                   ha='left', va='bottom', fontfamily='monospace')

        progress = show_up_to / max(n - 1, 1)
        ax.text(0.5, 0.98,
               f"Snapshot {show_up_to + 1}/{n}   •   Position: path-dependent (r=0.77)   •   Structure: path-invariant",
               transform=ax.transAxes, fontsize=9, color='#888888',
               ha='center', va='top', fontfamily='monospace')

        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.set_xticks([])
        ax.set_yticks([])

        ax.set_title("Identity Basin — CCS Trajectory",
                     fontsize=14, fontweight='bold', color='white',
                     fontfamily='monospace', pad=10)

        frame_path = os.path.join(frame_dir, f"frame_{frame_idx:04d}.png")
        plt.savefig(frame_path, dpi=120, facecolor='#0a0a1a',
                    bbox_inches='tight', pad_inches=0.3)
        plt.close()

        if (frame_idx + 1) % 20 == 0:
            print(f"    Rendered {frame_idx + 1}/{total_frames} frames")

    cmd = [
        "ffmpeg", "-y", "-framerate", str(fps),
        "-i", os.path.join(frame_dir, "frame_%04d.png"),
        "-vf", "pad=ceil(iw/2)*2:ceil(ih/2)*2",
        "-c:v", "libx264", "-pix_fmt", "yuv420p",
        "-crf", "23", "-preset", "medium",
        str(output_path)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        print(f"  ffmpeg error: {result.stderr[-200:]}")

    for f in Path(frame_dir).glob("*.png"):
        f.unlink()

    print(f"  Animation → {output_path}")


def main():
    print("=== Attractor Basin Visualization ===\n")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    snapshots = load_snapshots()
    print(f"Loaded {len(snapshots)} CCS snapshots")

    entity_tiers = get_entity_tiers(snapshots)
    core = [n for n, (t, _) in entity_tiers.items() if t == "core"]
    print(f"Entity tiers: {len(core)} core, "
          f"{len([t for t in entity_tiers.values() if t[0]=='stable'])} stable, "
          f"{len([t for t in entity_tiers.values() if t[0]=='coupled'])} coupled")

    print(f"\nEmbedding {len(snapshots)} snapshots...")
    embeddings = []
    for i, snap in enumerate(snapshots):
        text = serialize_ccs(snap["ccs"])
        emb = embed(text)
        embeddings.append(emb)
        if (i + 1) % 20 == 0:
            print(f"  Embedded {i + 1}/{len(snapshots)}")

    X = np.array(embeddings)
    print(f"\nRunning t-SNE (perplexity={min(15, len(snapshots)-1)})...")
    perp = min(15, len(snapshots) - 1)
    tsne = TSNE(n_components=2, perplexity=perp, random_state=42,
                max_iter=1000, learning_rate='auto', init='pca')
    coords = tsne.fit_transform(X)

    print(f"\nRendering static basin map...")
    render_static_basin(coords, snapshots, entity_tiers,
                       OUT_DIR / "identity_basin.png")

    print(f"\nRendering animation ({len(snapshots)} + 30 frames)...")
    render_animation(coords, snapshots, entity_tiers,
                    OUT_DIR / "identity_trajectory.mp4", fps=6)

    print(f"\nDone. Outputs in {OUT_DIR}/")


if __name__ == "__main__":
    main()
