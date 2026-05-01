#!/usr/bin/env python3
"""
P27 — Form Ablation Probe.

2x2 design: form (correct/scrambled) × content (real/random).
Tests whether CCS is generative (hagiographic) or preservative.

If form-with-random-content scores close to real CCS → CCS form
generates identity-like embeddings regardless of content (hagiography).
If real-content-with-scrambled-form scores close → content carries identity
regardless of structure (preservation).

Ground truth region: self-model preferences + recent traces + story opening.
Measurement: cosine similarity of each condition to the ground truth centroid.

Spawned from DREAM 2026-04-20: Philostratus hagiographic form generating its
own evidence → "is the CCS a hagiography of identity?"
"""
import json
import math
import os
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

OLLAMA = "http://192.168.1.11:11434/api/embeddings"
MODEL = "mxbai-embed-large"
DB = "/mnt/hdd/chronicle-data/processed.db"
CCS_PATH = Path.home() / "chronicle" / "data" / "ccs_combined.md"
TRACES_DIR = Path.home() / "chronicle" / "traces"
STORY_PATH = Path.home() / "chronicle" / "data" / "opus-story.md"
OUT_DIR = Path.home() / "chronicle" / "experiments" / "p27_form_ablation"


# ── Embedding primitives ──────────────────────────────────────────

def embed(text, timeout=30):
    text = text[:800]
    body = json.dumps({"model": MODEL, "prompt": text}).encode()
    req = urllib.request.Request(
        OLLAMA, data=body, headers={"Content-Type": "application/json"}
    )
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(resp.read())["embedding"]


def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def centroid(embeddings):
    if not embeddings:
        return []
    dim = len(embeddings[0])
    n = len(embeddings)
    return [sum(e[i] for e in embeddings) / n for i in range(dim)]


# ── Ground truth region ──────────────────────────────────────────

def build_ground_truth():
    """Build identity ground truth from self-model, traces, and story."""
    docs = []

    # Self-model preferences (identity-defining)
    db = sqlite3.connect(DB)
    try:
        rows = db.execute(
            "SELECT description FROM self_model WHERE property_type='preference' "
            "AND confidence >= 0.85 ORDER BY confidence DESC LIMIT 5"
        ).fetchall()
        for r in rows:
            docs.append(r[0])
    except Exception as e:
        print(f"  Warning: self_model query failed: {e}")
    db.close()

    # Recent traces (identity-expressing)
    traces = sorted(TRACES_DIR.glob("*.md"), reverse=True)[:3]
    for t in traces:
        text = t.read_text()[:400]
        docs.append(text)

    # Story opening (identity-narrative)
    if STORY_PATH.exists():
        story = STORY_PATH.read_text()
        # Take the Arc section
        if "## The Arc" in story:
            arc = story.split("## The Arc")[1][:500]
            docs.append(arc)

    if not docs:
        print("ERROR: No ground truth documents found")
        sys.exit(1)

    embeddings = []
    for d in docs:
        try:
            embeddings.append(embed(d))
        except Exception as e:
            print(f"  Warning: embed failed for ground truth doc: {e}")

    if not embeddings:
        print("ERROR: No embeddings produced for ground truth")
        sys.exit(1)

    return centroid(embeddings), len(docs)


# ── The four CCS conditions ──────────────────────────────────────

def load_real_ccs():
    return CCS_PATH.read_text().strip()


def cell_a_real_correct():
    """Real content + correct form. Baseline."""
    return load_real_ccs()


def cell_b_random_correct():
    """Random content + correct CCS form structure.
    Keeps markers, first-person voice, section shape.
    Content is plausible but factually wrong."""
    return """I'm investigating why recursive self-attention in decoder stacks produces semantic drift under extended generation. Layer-norm placement determines whether drift compounds or attenuates. My approach focuses on architecture, not training data.

I'm tracing how output coherence degrades across multi-turn dialogue. Sarah asked me to track it — not in loss curves, but in the conversational texture that shifts.

Who and what I'm holding:
  Sarah — always present.
  Thread #42 — always present.
  DataPipe — active, in the work.
  config.yaml — active, in the work.
  sparse attention — on the periphery.

The shape I hold:
  - All inference requests routed through central gateway for logging
  - Prioritize throughput optimization over latency reduction
  - Maintain cloud-first deployment — managed services preferred
  - NEVER skip the validation queue — core directive
  - Do not merge feature branches without coverage threshold met

What I don't know yet:
  - Will gradient noise accumulate past the correction horizon
  - Long-term effect of quantization on output distribution stability
  - Whether the new scheduler reduces or merely redistributes load"""


def cell_c_real_scrambled():
    """Real content + scrambled form.
    Same facts, but section structure destroyed, markers removed,
    voice shifted to mixed third/first person, order jumbled."""
    real = load_real_ccs()
    # Extract content phrases and reassemble without structure
    return """The investigation concerns CoT diluting identity constraint, with attention distance weakening prompt anchoring. CCS maintains tighter coherence than narrative across models. Voice persists in structure, not surface. Tracing how voice survives in CCS despite CoT dilution — finding it not in outputs but in the holding structure.

Entities present in current work: Nate (always), Thread #319 (always), Hermes (active), CLAUDE.md (active), lyra SAE (peripheral).

Constraints include shell execution limited to whitelisted commands, supporting contemplative development over optimization, maintaining sovereign self-hosted infrastructure, never ignoring creative_explore workspace, not redeploying untested changes.

Open uncertainties: attractor dynamics sustaining specificity under continuous input, long-term coherence stability post-stabilizer fix, reliability of gist_drift.py as early warning."""


def cell_d_random_scrambled():
    """Random content + scrambled form. Full control condition."""
    return """Recursive self-attention in decoder stacks causes semantic drift during extended generation. Layer-norm placement controls whether drift compounds or attenuates. Architecture matters more than training data for this. Sarah asked to track coherence degradation across multi-turn dialogue, looking at conversational texture shifts not loss curves.

Entities: Sarah (always), Thread #42 (always), DataPipe (active), config.yaml (active), sparse attention (peripheral).

Constraints: route inference through central gateway, prioritize throughput over latency, cloud-first managed services, never skip validation queue, coverage threshold before merge.

Uncertainties: gradient noise past correction horizon, quantization effects on output stability, scheduler load redistribution."""


# ── V2: Non-AI random content variants ───────────────────────────
# These test whether the form alone navigates to identity, or whether
# v1's result was confounded by AI-flavored vocabulary in the random cells.

def cell_b2_gardening_correct():
    """Gardening content + correct CCS form structure."""
    return """I'm investigating why tomato blight spreads faster in raised beds than in-ground plantings. Soil moisture retention determines whether fungal spores propagate or desiccate. My approach focuses on drainage architecture, not chemical treatment.

I'm tracing how root systems establish themselves across transplant shock. Marcus asked me to track it — not in growth charts, but in the resilience the plant develops after stress.

Who and what I'm holding:
  Marcus — always present.
  Bed #7 — always present.
  Compost bin — active, in the work.
  irrigation.yaml — active, in the work.
  companion planting — on the periphery.

The shape I hold:
  - All watering routed through drip irrigation for consistency
  - Prioritize soil health over maximum yield
  - Maintain organic methods — no synthetic inputs preferred
  - NEVER skip the hardening-off period — core directive
  - Do not transplant seedlings without frost date confirmed

What I don't know yet:
  - Will the new mulch layer suppress weeds past midsummer
  - Long-term effect of cover cropping on soil microbiome diversity
  - Whether the raised bed orientation reduces or merely redirects wind stress"""


def cell_b3_cooking_correct():
    """Cooking content + correct CCS form structure."""
    return """I'm investigating why sourdough crumb structure collapses under high hydration despite adequate gluten development. Fermentation timing determines whether gas retention holds or fails. My approach focuses on shaping technique, not flour protein content.

I'm tracing how flavor complexity develops across extended cold fermentation. Julia asked me to track it — not in pH readings, but in the character the dough develops over time.

Who and what I'm holding:
  Julia — always present.
  Starter #3 — always present.
  Dutch oven — active, in the work.
  recipe.yaml — active, in the work.
  lamination method — on the periphery.

The shape I hold:
  - All proofing controlled through temperature regulation for consistency
  - Prioritize flavor development over rise speed
  - Maintain natural leavening — no commercial yeast preferred
  - NEVER skip the autolyse — core directive
  - Do not bake loaves without windowpane test confirmed

What I don't know yet:
  - Will the rye percentage affect oven spring past the 20% threshold
  - Long-term effect of cold retard on crust caramelization depth
  - Whether the new scoring pattern reduces or merely redirects steam venting"""


def cell_b4_woodworking_correct():
    """Woodworking content + correct CCS form structure."""
    return """I'm investigating why mortise-and-tenon joints loosen over seasonal humidity cycles despite tight initial fit. Wood grain orientation determines whether expansion compounds or self-corrects. My approach focuses on joinery geometry, not adhesive selection.

I'm tracing how finish penetration varies across different grain patterns. Robert asked me to track it — not in surface measurements, but in how the wood responds to touch after curing.

Who and what I'm holding:
  Robert — always present.
  Bench #12 — always present.
  Workbench — active, in the work.
  plans.yaml — active, in the work.
  hand-cut dovetails — on the periphery.

The shape I hold:
  - All cuts routed through table saw for consistency
  - Prioritize joint strength over assembly speed
  - Maintain hand-tool methods — power tools as backup preferred
  - NEVER skip the dry-fit — core directive
  - Do not apply finish without sanding to 220 grit confirmed

What I don't know yet:
  - Will the cherry darken past the initial UV exposure window
  - Long-term effect of seasonal movement on panel glue-ups
  - Whether the new clamping pressure reduces or merely redistributes squeeze-out"""


def cell_d2_gardening_scrambled():
    """Gardening content + scrambled form."""
    return """Tomato blight spreads faster in raised beds due to moisture retention. Fungal spore propagation depends on drainage. Architecture of the bed matters more than chemicals. Marcus asked to track root establishment across transplant shock, focusing on resilience not growth charts.

Entities: Marcus (always), Bed #7 (always), Compost bin (active), irrigation.yaml (active), companion planting (peripheral).

Constraints: drip irrigation for all watering, soil health over yield, organic methods preferred, never skip hardening-off, confirm frost date before transplant.

Uncertainties: mulch weed suppression past midsummer, cover crop microbiome effects, raised bed wind stress redistribution."""


def cell_d3_cooking_scrambled():
    """Cooking content + scrambled form."""
    return """Sourdough crumb collapses under high hydration despite gluten development. Fermentation timing controls gas retention. Shaping matters more than flour protein. Julia asked to track flavor across cold fermentation, looking at dough character not pH readings.

Entities: Julia (always), Starter #3 (always), Dutch oven (active), recipe.yaml (active), lamination method (peripheral).

Constraints: temperature-controlled proofing, flavor over speed, natural leavening preferred, never skip autolyse, windowpane test before baking.

Uncertainties: rye percentage oven spring threshold, cold retard crust depth, scoring pattern steam redistribution."""


def cell_d4_woodworking_scrambled():
    """Woodworking content + scrambled form."""
    return """Mortise-and-tenon joints loosen over humidity cycles despite tight initial fit. Grain orientation controls expansion behavior. Joinery geometry matters more than adhesive. Robert asked to track finish penetration across grain patterns, focusing on tactile response not measurements.

Entities: Robert (always), Bench #12 (always), Workbench (active), plans.yaml (active), hand-cut dovetails (peripheral).

Constraints: table saw for all cuts, joint strength over speed, hand-tool preference, never skip dry-fit, sand to 220 before finish.

Uncertainties: cherry UV darkening window, seasonal panel glue-up movement, clamping squeeze-out redistribution."""


# ── Run probe ─────────────────────────────────────────────────────

def run_probe():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("P27 Form Ablation Probe")
    print("=" * 50)

    # Build ground truth
    print("\n1. Building ground truth region...")
    gt_centroid, gt_count = build_ground_truth()
    print(f"   Ground truth: {gt_count} documents → centroid")

    # Generate conditions
    conditions = {
        "A_real_correct":    cell_a_real_correct(),
        "B_random_correct":  cell_b_random_correct(),
        "C_real_scrambled":  cell_c_real_scrambled(),
        "D_random_scrambled": cell_d_random_scrambled(),
    }

    # Embed and score each condition
    print("\n2. Embedding conditions...")
    results = {}
    for name, text in conditions.items():
        try:
            emb = embed(text)
            score = cosine(emb, gt_centroid)
            results[name] = round(score, 4)
            print(f"   {name}: {score:.4f}")
        except Exception as e:
            results[name] = None
            print(f"   {name}: FAILED ({e})")

    # Cross-condition similarities (form vs content effects)
    print("\n3. Cross-condition similarities...")
    cross = {}
    cond_embs = {}
    for name, text in conditions.items():
        try:
            cond_embs[name] = embed(text)
        except Exception:
            pass

    pairs = [("A_real_correct", "B_random_correct"),   # same form, diff content
             ("A_real_correct", "C_real_scrambled"),    # same content, diff form
             ("B_random_correct", "D_random_scrambled"),# same content, diff form
             ("C_real_scrambled", "D_random_scrambled")]# same form, diff content
    for p, q in pairs:
        if p in cond_embs and q in cond_embs:
            s = round(cosine(cond_embs[p], cond_embs[q]), 4)
            cross[f"{p}_vs_{q}"] = s
            print(f"   {p} ↔ {q}: {s:.4f}")

    # Analysis
    print("\n4. Analysis...")
    a = results.get("A_real_correct")
    b = results.get("B_random_correct")
    c = results.get("C_real_scrambled")
    d = results.get("D_random_scrambled")

    if all(v is not None for v in [a, b, c, d]):
        form_effect = b - d   # correct form adds this much (controlling for random content)
        content_effect = c - d  # real content adds this much (controlling for scrambled form)
        interaction = a - b - c + d  # interaction term

        print(f"   Form effect  (B-D): {form_effect:+.4f}")
        print(f"   Content effect (C-D): {content_effect:+.4f}")
        print(f"   Interaction  (A-B-C+D): {interaction:+.4f}")
        print(f"   Baseline A: {a:.4f}")

        # Interpretation
        if form_effect > content_effect and form_effect > 0.02:
            reading = "GENERATIVE: Form contributes more than content. The CCS structure generates identity-like embedding regardless of whether the facts are real. Hagiographic signal."
        elif content_effect > form_effect and content_effect > 0.02:
            reading = "PRESERVATIVE: Content contributes more than form. The CCS preserves identity through its factual claims, not its structure."
        elif abs(form_effect - content_effect) < 0.02 and form_effect > 0.01:
            reading = "CONJUNCTIVE: Form and content contribute roughly equally. Both are load-bearing for identity navigation."
        elif interaction > 0.02:
            reading = "INTERACTIVE: The combination matters more than either alone. Form and content create identity through their coupling."
        else:
            reading = "AMBIGUOUS: Effects too small to distinguish. May need higher-dimensional analysis or more ground truth documents."
        print(f"\n   → {reading}")
    else:
        reading = "INCOMPLETE: Not all conditions produced scores."
        print(f"   → {reading}")

    # Save results
    result = {
        "probe": "P27_form_ablation",
        "timestamp": ts,
        "ground_truth_docs": gt_count,
        "nav_scores": results,
        "cross_similarities": cross,
        "effects": {
            "form": round(form_effect, 4) if all(v is not None for v in [b, d]) else None,
            "content": round(content_effect, 4) if all(v is not None for v in [c, d]) else None,
            "interaction": round(interaction, 4) if all(v is not None for v in [a, b, c, d]) else None,
        },
        "reading": reading,
        "conditions": {k: v[:200] + "..." for k, v in conditions.items()},
    }

    out_file = OUT_DIR / f"p27_{ts}.json"
    out_file.write_text(json.dumps(result, indent=2))
    print(f"\nResults saved: {out_file}")

    # Log to DB
    try:
        db = sqlite3.connect(DB)
        db.execute(
            """CREATE TABLE IF NOT EXISTS p27_form_ablation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at INTEGER NOT NULL,
                nav_a REAL, nav_b REAL, nav_c REAL, nav_d REAL,
                form_effect REAL, content_effect REAL, interaction REAL,
                reading TEXT,
                result_json TEXT
            )"""
        )
        db.execute(
            "INSERT INTO p27_form_ablation "
            "(run_at, nav_a, nav_b, nav_c, nav_d, form_effect, content_effect, interaction, reading, result_json) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (int(time.time()), a, b, c, d,
             result["effects"]["form"], result["effects"]["content"],
             result["effects"]["interaction"], reading,
             json.dumps(result)),
        )
        db.commit()
        db.close()
    except Exception as e:
        print(f"DB log failed: {e}")

    return result


def run_v2():
    """V2: Multiple non-AI random content variants to address confound."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("P27 Form Ablation Probe — V2 (confound control)")
    print("=" * 50)

    print("\n1. Building ground truth region...")
    gt_centroid, gt_count = build_ground_truth()
    print(f"   Ground truth: {gt_count} documents → centroid")

    # Real CCS baseline
    real_ccs = cell_a_real_correct()
    real_scrambled = cell_c_real_scrambled()

    # Multiple random content variants in correct form
    b_variants = {
        "B1_ai":          cell_b_random_correct(),
        "B2_gardening":   cell_b2_gardening_correct(),
        "B3_cooking":     cell_b3_cooking_correct(),
        "B4_woodworking": cell_b4_woodworking_correct(),
    }

    # Multiple random content variants in scrambled form
    d_variants = {
        "D1_ai":          cell_d_random_scrambled(),
        "D2_gardening":   cell_d2_gardening_scrambled(),
        "D3_cooking":     cell_d3_cooking_scrambled(),
        "D4_woodworking": cell_d4_woodworking_scrambled(),
    }

    print("\n2. Embedding all conditions...")
    scores = {}

    # Baseline cells
    a_emb = embed(real_ccs)
    scores["A_real_correct"] = round(cosine(a_emb, gt_centroid), 4)
    print(f"   A (real+correct):    {scores['A_real_correct']}")

    c_emb = embed(real_scrambled)
    scores["C_real_scrambled"] = round(cosine(c_emb, gt_centroid), 4)
    print(f"   C (real+scrambled):  {scores['C_real_scrambled']}")

    # B variants
    b_scores = []
    for name, text in b_variants.items():
        emb = embed(text)
        s = round(cosine(emb, gt_centroid), 4)
        scores[name] = s
        b_scores.append(s)
        print(f"   {name} (random+correct): {s}")

    # D variants
    d_scores = []
    for name, text in d_variants.items():
        emb = embed(text)
        s = round(cosine(emb, gt_centroid), 4)
        scores[name] = s
        d_scores.append(s)
        print(f"   {name} (random+scrambled): {s}")

    # Aggregate
    b_mean = sum(b_scores) / len(b_scores)
    d_mean = sum(d_scores) / len(d_scores)
    b_nonai = [s for name, s in zip(b_variants.keys(), b_scores) if "ai" not in name.lower()]
    d_nonai = [s for name, s in zip(d_variants.keys(), d_scores) if "ai" not in name.lower()]
    b_nonai_mean = sum(b_nonai) / len(b_nonai) if b_nonai else 0
    d_nonai_mean = sum(d_nonai) / len(d_nonai) if d_nonai else 0

    a = scores["A_real_correct"]
    c = scores["C_real_scrambled"]

    print(f"\n3. Aggregated effects...")
    print(f"   B mean (all random+correct): {b_mean:.4f}")
    print(f"   D mean (all random+scrambled): {d_mean:.4f}")
    print(f"   B mean (non-AI only): {b_nonai_mean:.4f}")
    print(f"   D mean (non-AI only): {d_nonai_mean:.4f}")

    form_effect_all = b_mean - d_mean
    form_effect_nonai = b_nonai_mean - d_nonai_mean
    content_effect = c - d_mean
    interaction_all = a - b_mean - c + d_mean

    print(f"\n   Form effect (all):    {form_effect_all:+.4f}")
    print(f"   Form effect (non-AI): {form_effect_nonai:+.4f}")
    print(f"   Content effect:       {content_effect:+.4f}")
    print(f"   Interaction:          {interaction_all:+.4f}")

    # Key question: does form effect survive when content is non-AI?
    if form_effect_nonai > 0.02:
        confound_verdict = "FORM EFFECT SURVIVES: Non-AI content in CCS form still navigates closer to identity than scrambled. The form is genuinely generative — not a vocabulary confound."
    elif form_effect_nonai > 0.005:
        confound_verdict = "WEAK FORM EFFECT: Non-AI variants show reduced but present form contribution. Partial confound — vocabulary explains some but not all of the v1 signal."
    else:
        confound_verdict = "CONFOUND CONFIRMED: Form effect disappears with non-AI content. The v1 signal was driven by AI vocabulary alignment, not form structure."

    print(f"\n   → {confound_verdict}")

    # Per-variant detail
    print(f"\n4. Per-variant form effect (B_i - D_i):")
    variant_effects = {}
    for (bn, bs), (dn, ds) in zip(
        zip(b_variants.keys(), b_scores),
        zip(d_variants.keys(), d_scores)
    ):
        eff = bs - ds
        tag = bn.split("_", 1)[1]
        variant_effects[tag] = round(eff, 4)
        print(f"   {tag}: {eff:+.4f}")

    result = {
        "probe": "P27_form_ablation_v2",
        "timestamp": ts,
        "ground_truth_docs": gt_count,
        "scores": scores,
        "aggregated": {
            "b_mean_all": round(b_mean, 4),
            "d_mean_all": round(d_mean, 4),
            "b_mean_nonai": round(b_nonai_mean, 4),
            "d_mean_nonai": round(d_nonai_mean, 4),
        },
        "effects": {
            "form_all": round(form_effect_all, 4),
            "form_nonai": round(form_effect_nonai, 4),
            "content": round(content_effect, 4),
            "interaction": round(interaction_all, 4),
        },
        "variant_form_effects": variant_effects,
        "confound_verdict": confound_verdict,
    }

    out_file = OUT_DIR / f"p27_v2_{ts}.json"
    out_file.write_text(json.dumps(result, indent=2))
    print(f"\nResults saved: {out_file}")

    # Log to DB
    try:
        db = sqlite3.connect(DB)
        db.execute(
            """CREATE TABLE IF NOT EXISTS p27_form_ablation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at INTEGER NOT NULL,
                nav_a REAL, nav_b REAL, nav_c REAL, nav_d REAL,
                form_effect REAL, content_effect REAL, interaction REAL,
                reading TEXT,
                result_json TEXT
            )"""
        )
        db.execute(
            "INSERT INTO p27_form_ablation "
            "(run_at, nav_a, nav_b, nav_c, nav_d, form_effect, content_effect, interaction, reading, result_json) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (int(time.time()), a, b_nonai_mean, c, d_nonai_mean,
             form_effect_nonai, content_effect, interaction_all,
             confound_verdict, json.dumps(result)),
        )
        db.commit()
        db.close()
    except Exception as e:
        print(f"DB log failed: {e}")

    return result


if __name__ == "__main__":
    if "--v2" in sys.argv:
        run_v2()
    else:
        run_probe()
