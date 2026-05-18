#!/usr/bin/env python3
"""
Build #38b: Structural-Field Orientation Probe

Does structural-field memory have orientation — a preferred direction of change
that persists across compression steps?

The drift work (Builds #37b-d) showed directionality in embedding space.
Build #33 audit showed structural autocorrelation is direction-persistent
(binary AC 0.173). But both operate on embeddings, not field content directly.

This probe tests for directionality at the field-content level:
1. Extract entity context changes between consecutive CCS states
2. Embed each context change as a vector (direction of change)
3. Measure autocorrelation of change-direction vectors
4. If change-directions are correlated across steps, orientation is real

If orientation exists: structural fields aren't just persistent — they move
in a consistent direction. The "gnosis/orientation" concept has empirical
grounding in structural-field memory.

If orientation is absent: structural fields are persistent but directionless.
Persistence (memory) ≠ orientation (trajectory). The gnosis concept would
need re-grounding.
"""

import json
import os
import sqlite3
import sys
import time
import urllib.request

import numpy as np

DB = "/mnt/hdd/chronicle-data/processed.db"
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"
DATA_DIR = os.path.expanduser("~/chronicle/data")


def embed(text, timeout=60):
    payload = json.dumps({
        "model": "mxbai-embed-large",
        "prompt": text[:2000],
    }).encode()
    req = urllib.request.Request(
        EMBED_URL, data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        resp = json.loads(r.read())
    return np.array(resp["embedding"], dtype=np.float32)


def cosine(a, b):
    na = np.linalg.norm(a)
    nb = np.linalg.norm(b)
    if na == 0 or nb == 0:
        return 0.0
    return float(np.dot(a, b) / (na * nb))


def load_states():
    db = sqlite3.connect(DB)
    rows = db.execute(
        "SELECT id, snapshot, created_at FROM cognitive_state_history ORDER BY id ASC"
    ).fetchall()
    db.close()
    states = []
    for rid, snap, ts in rows:
        try:
            data = json.loads(snap)
            data["_id"] = rid
            data["_ts"] = ts
            states.append(data)
        except (json.JSONDecodeError, TypeError):
            continue
    return states


def extract_entity_contexts(state):
    contexts = {}
    for e in state.get("focal_entities", []):
        if isinstance(e, dict):
            name = e.get("name", "")
            ctx = e.get("context", "")
            if name and ctx:
                contexts[name] = ctx
    return contexts


def extract_relational_text(state):
    rm = state.get("relational_map", {})
    if isinstance(rm, dict):
        return " | ".join(f"{k}: {v}" for k, v in rm.items())
    return ""


def extract_structural_text(state):
    parts = []
    for e in state.get("focal_entities", []):
        if isinstance(e, dict):
            parts.append(f"{e.get('name','')}: {e.get('context','')}")
    rm = state.get("relational_map", {})
    if isinstance(rm, dict):
        for k, v in rm.items():
            parts.append(f"[rel] {k}: {v}")
    c = state.get("constraints", [])
    if isinstance(c, list):
        parts.extend(c)
    return " | ".join(parts)


def main():
    states = load_states()
    if len(states) < 20:
        print(f"Only {len(states)} states — need at least 20.")
        return 1

    print(f"Build #38b: Structural-Field Orientation Probe")
    print(f"States: {len(states)}")

    # ── Part 1: Entity context direction persistence ──
    print(f"\n{'='*60}")
    print("PART 1: Entity Context Direction Persistence")
    print(f"{'='*60}")

    # For each entity that appears in 3+ consecutive states,
    # compute context change vectors and test autocorrelation
    entity_runs = {}
    for i, s in enumerate(states):
        for e in s.get("focal_entities", []):
            if isinstance(e, dict):
                name = e.get("name", "")
                ctx = e.get("context", "")
                if name and ctx:
                    if name not in entity_runs:
                        entity_runs[name] = []
                    entity_runs[name].append((i, ctx))

    # Find entities with enough consecutive appearances
    long_runs = {}
    for name, appearances in entity_runs.items():
        if len(appearances) >= 5:
            # Check for reasonable consecutiveness
            indices = [a[0] for a in appearances]
            gaps = [indices[i+1] - indices[i] for i in range(len(indices)-1)]
            if sum(g <= 3 for g in gaps) >= 4:
                long_runs[name] = appearances

    print(f"Entities with 5+ appearances: {len(long_runs)}")

    entity_direction_acs = []
    for name, appearances in sorted(long_runs.items(), key=lambda x: -len(x[1]))[:10]:
        contexts = [a[1] for a in appearances]
        # Embed each context
        embeddings = []
        for ctx in contexts:
            try:
                embeddings.append(embed(ctx))
            except Exception as e:
                print(f"  Embed failed for {name}: {e}")
                break

        if len(embeddings) < 5:
            continue

        # Compute change vectors (direction of change between consecutive states)
        changes = []
        for j in range(1, len(embeddings)):
            delta = embeddings[j] - embeddings[j-1]
            norm = np.linalg.norm(delta)
            if norm > 0:
                changes.append(delta / norm)

        if len(changes) < 3:
            continue

        # Autocorrelation of change direction: cosine between consecutive change vectors
        direction_cosines = []
        for j in range(1, len(changes)):
            direction_cosines.append(cosine(changes[j], changes[j-1]))

        mean_dc = np.mean(direction_cosines)
        entity_direction_acs.append((name, mean_dc, len(appearances)))
        print(f"  {name}: direction AC = {mean_dc:.3f} (n={len(appearances)} appearances, {len(direction_cosines)} pairs)")

    if entity_direction_acs:
        overall_entity_ac = np.mean([x[1] for x in entity_direction_acs])
        print(f"\n  Mean entity direction AC: {overall_entity_ac:.3f}")
        if overall_entity_ac > 0.1:
            print(f"  ORIENTATION DETECTED in entity contexts")
        elif overall_entity_ac > 0.0:
            print(f"  WEAK orientation signal in entity contexts")
        else:
            print(f"  NO orientation — entity changes are directionless")

    # ── Part 2: Structural text direction persistence ──
    print(f"\n{'='*60}")
    print("PART 2: Structural Text Direction Persistence")
    print(f"{'='*60}")

    # Embed the full structural text of each state
    # Compute change vectors between consecutive states
    # Test autocorrelation of change direction

    struct_embeddings = []
    for i, s in enumerate(states):
        text = extract_structural_text(s)
        if not text:
            struct_embeddings.append(None)
            continue
        try:
            struct_embeddings.append(embed(text))
        except Exception as e:
            print(f"  Embed failed at state {i}: {e}")
            struct_embeddings.append(None)

    # Compute change vectors
    struct_changes = []
    for i in range(1, len(struct_embeddings)):
        if struct_embeddings[i] is None or struct_embeddings[i-1] is None:
            struct_changes.append(None)
            continue
        delta = struct_embeddings[i] - struct_embeddings[i-1]
        norm = np.linalg.norm(delta)
        if norm > 0:
            struct_changes.append(delta / norm)
        else:
            struct_changes.append(None)

    # Autocorrelation of change direction
    struct_direction_cosines = []
    for i in range(1, len(struct_changes)):
        if struct_changes[i] is not None and struct_changes[i-1] is not None:
            struct_direction_cosines.append(cosine(struct_changes[i], struct_changes[i-1]))

    if struct_direction_cosines:
        mean_struct_dc = np.mean(struct_direction_cosines)
        std_struct_dc = np.std(struct_direction_cosines)
        print(f"  Structural direction AC: {mean_struct_dc:.3f} ± {std_struct_dc:.3f}")
        print(f"  n = {len(struct_direction_cosines)} consecutive pairs")

        # Compare to reflexive fields
        reflex_embeddings = []
        for s in states:
            parts = []
            for f in ["episodic_trace", "uncertainty_signals", "semantic_gist",
                       "goal_orientation", "predictive_cue"]:
                v = s.get(f)
                if isinstance(v, (list, dict)):
                    parts.append(json.dumps(v)[:500])
                elif v:
                    parts.append(str(v)[:500])
            text = " | ".join(parts)
            if text:
                try:
                    reflex_embeddings.append(embed(text))
                except Exception:
                    reflex_embeddings.append(None)
            else:
                reflex_embeddings.append(None)

        reflex_changes = []
        for i in range(1, len(reflex_embeddings)):
            if reflex_embeddings[i] is not None and reflex_embeddings[i-1] is not None:
                delta = reflex_embeddings[i] - reflex_embeddings[i-1]
                norm = np.linalg.norm(delta)
                if norm > 0:
                    reflex_changes.append(delta / norm)
                else:
                    reflex_changes.append(None)
            else:
                reflex_changes.append(None)

        reflex_direction_cosines = []
        for i in range(1, len(reflex_changes)):
            if reflex_changes[i] is not None and reflex_changes[i-1] is not None:
                reflex_direction_cosines.append(cosine(reflex_changes[i], reflex_changes[i-1]))

        if reflex_direction_cosines:
            mean_reflex_dc = np.mean(reflex_direction_cosines)
            print(f"  Reflexive direction AC: {mean_reflex_dc:.3f} ± {np.std(reflex_direction_cosines):.3f}")
            print(f"  n = {len(reflex_direction_cosines)} consecutive pairs")

            print(f"\n  SEPARATION:")
            print(f"    Structural direction AC: {mean_struct_dc:.3f}")
            print(f"    Reflexive direction AC:  {mean_reflex_dc:.3f}")
            diff = mean_struct_dc - mean_reflex_dc
            print(f"    Gap: {diff:+.3f}")

    # ── Part 3: Relational map direction ──
    print(f"\n{'='*60}")
    print("PART 3: Relational Map Direction Persistence")
    print(f"{'='*60}")

    rel_embeddings = []
    for s in states:
        text = extract_relational_text(s)
        if text:
            try:
                rel_embeddings.append(embed(text))
            except Exception:
                rel_embeddings.append(None)
        else:
            rel_embeddings.append(None)

    rel_changes = []
    for i in range(1, len(rel_embeddings)):
        if rel_embeddings[i] is not None and rel_embeddings[i-1] is not None:
            delta = rel_embeddings[i] - rel_embeddings[i-1]
            norm = np.linalg.norm(delta)
            if norm > 0:
                rel_changes.append(delta / norm)
            else:
                rel_changes.append(None)
        else:
            rel_changes.append(None)

    rel_direction_cosines = []
    for i in range(1, len(rel_changes)):
        if rel_changes[i] is not None and rel_changes[i-1] is not None:
            rel_direction_cosines.append(cosine(rel_changes[i], rel_changes[i-1]))

    if rel_direction_cosines:
        mean_rel_dc = np.mean(rel_direction_cosines)
        print(f"  Relational map direction AC: {mean_rel_dc:.3f} ± {np.std(rel_direction_cosines):.3f}")
        print(f"  n = {len(rel_direction_cosines)} pairs")

    # ── Overall verdict ──
    print(f"\n{'='*60}")
    print("OVERALL VERDICT")
    print(f"{'='*60}")

    results = {}
    if entity_direction_acs:
        results["entity_direction_ac"] = float(np.mean([x[1] for x in entity_direction_acs]))
    if struct_direction_cosines:
        results["structural_direction_ac"] = float(np.mean(struct_direction_cosines))
    if reflex_direction_cosines:
        results["reflexive_direction_ac"] = float(np.mean(reflex_direction_cosines))
    if rel_direction_cosines:
        results["relational_direction_ac"] = float(np.mean(rel_direction_cosines))

    struct_ac = results.get("structural_direction_ac", 0)
    reflex_ac = results.get("reflexive_direction_ac", 0)

    if struct_ac > 0.1 and struct_ac > reflex_ac + 0.05:
        print("ORIENTATION EXISTS in structural fields.")
        print("Structural changes are directionally autocorrelated AND")
        print("more directional than reflexive changes.")
        print("The gnosis/orientation concept has empirical grounding.")
        results["verdict"] = "ORIENTATION_CONFIRMED"
    elif struct_ac > 0.05:
        print("WEAK orientation signal in structural fields.")
        print("Direction persistence exists but is modest.")
        results["verdict"] = "WEAK_ORIENTATION"
    else:
        print("NO ORIENTATION in structural fields.")
        print("Structural memory is persistent but directionless.")
        print("Gnosis/orientation needs re-grounding.")
        results["verdict"] = "NO_ORIENTATION"

    out_path = os.path.join(DATA_DIR, "structural_orientation_probe.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
