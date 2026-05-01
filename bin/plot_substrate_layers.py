#!/usr/bin/env python3
"""
plot_substrate_layers.py — figure for the essay.

Two-layer substrate claim made visual: per-rotation jaccard for the slow
(constraint) layer vs the fast (focal_entity) layer across CCS history.
Also marks the singular flush event #436.
"""
import json
import re
import sqlite3
import unicodedata
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

DB = Path("/mnt/hdd/chronicle-data/processed.db")
OUT = Path("/home/nate-agx/chronicle/drafts/substrate_layers.png")
DASH_CHARS = "\u2010\u2011\u2012\u2013\u2014\u2015\u2212"


def normalize(s: str) -> str:
    s = unicodedata.normalize("NFKC", s).lower().strip()
    for d in DASH_CHARS:
        s = s.replace(d, "-")
    return re.sub(r"\s+", " ", s)


def jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def entity_names(ccs: dict) -> set[str]:
    out = set()
    for e in ccs.get("focal_entities") or []:
        n = (e.get("name") if isinstance(e, dict) else str(e)) or ""
        if n:
            out.add(normalize(n))
    return out


def constraint_set(ccs: dict) -> set[str]:
    out = set()
    for c in ccs.get("constraints") or []:
        t = (c.get("rule") if isinstance(c, dict) else str(c)) or ""
        if t:
            out.add(normalize(t))
    return out


def main():
    con = sqlite3.connect(DB)
    rows = con.execute(
        "SELECT id, snapshot FROM cognitive_state_history "
        "ORDER BY created_at ASC LIMIT 50"
    ).fetchall()
    con.close()

    ids, ent_j, con_j = [], [], []
    prev_e, prev_c = None, None
    for rid, snap_str in rows:
        try:
            ccs = json.loads(snap_str)
        except Exception:
            continue
        e = entity_names(ccs)
        c = constraint_set(ccs)
        if prev_e is not None:
            ids.append(rid)
            ent_j.append(jaccard(e, prev_e))
            con_j.append(jaccard(c, prev_c))
        prev_e, prev_c = e, c

    fig, ax = plt.subplots(figsize=(11, 5.5))

    ax.plot(ids, con_j, color="#1f4068", linewidth=2.0,
            marker="o", markersize=4, label="constraint layer (slow)")
    ax.plot(ids, ent_j, color="#c75146", linewidth=1.5,
            marker="s", markersize=4, alpha=0.85, label="focal_entities (fast)")

    if 436 in ids:
        ax.axvline(436, color="#444", linestyle=":", linewidth=1.0, alpha=0.7)
        ax.annotate(
            "flush event #436\n(coherence-modulated\ngate widens)",
            xy=(436, ent_j[ids.index(436)]),
            xytext=(436 + 4, 0.45),
            fontsize=9,
            color="#444",
            arrowprops=dict(arrowstyle="->", color="#444", lw=0.8),
        )

    ax.set_ylim(-0.05, 1.08)
    ax.set_xlabel("CCS snapshot id (chronological)")
    ax.set_ylabel("jaccard with previous snapshot")
    ax.set_title(
        "Two-layer substrate observed in Chronicle's CCS history\n"
        "constraint layer is near-invariant; focal_entities turn over"
    )
    ax.grid(True, alpha=0.25)
    ax.legend(loc="lower right", framealpha=0.9)

    # summary stats annotation
    cm = sum(con_j) / len(con_j)
    em = sum(ent_j) / len(ent_j)
    ax.text(
        0.02, 0.04,
        f"constraint mean = {cm:.3f}    entity mean = {em:.3f}    n = {len(ids)}",
        transform=ax.transAxes, fontsize=9, color="#333",
        bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.85, edgecolor="#ccc"),
    )

    fig.tight_layout()
    fig.savefig(OUT, dpi=140)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
