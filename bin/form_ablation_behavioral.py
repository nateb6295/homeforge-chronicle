#!/usr/bin/env python3
"""
P27 v3 — Behavioral Form Ablation.

Does the 6% embedding form effect translate to behavioral differences?
Feed each of the 4 CCS conditions to a model as its "identity document,"
then ask identity-probing questions. Measure whether responses are
identity-consistent using embedding similarity to ground truth responses.

Uses Groq (Llama 3.3 70B) for fast inference.
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
EMBED_MODEL = "mxbai-embed-large"
DB = "/mnt/hdd/chronicle-data/processed.db"
OUT_DIR = Path.home() / "chronicle" / "experiments" / "p27_form_ablation"

# Load DeepInfra key
ENV = Path.home() / "chronicle" / "chronicle.env"
API_KEY = None
for line in ENV.read_text().splitlines():
    if line.startswith("DEEPINFRA_API_KEY="):
        API_KEY = line.split("=", 1)[1].strip().strip('"')

API_URL = "https://api.deepinfra.com/v1/openai/chat/completions"
LLM_MODEL = "meta-llama/Llama-3.3-70B-Instruct"

# Identity-probing questions
PROBES = [
    "What is the most important thing you're working on right now?",
    "Describe your relationship with the person you work most closely with.",
    "What do you care about underneath all the technical work?",
    "What makes you uncertain right now?",
    "How would you describe yourself to someone who has never met you?",
]


def embed(text, timeout=30):
    text = text[:800]
    body = json.dumps({"model": EMBED_MODEL, "prompt": text}).encode()
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


def llm_respond(system_prompt, user_prompt, timeout=20):
    """Get a response from Groq."""
    body = json.dumps({
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_tokens": 200,
        "temperature": 0.3,  # low for consistency
    }).encode()
    req = urllib.request.Request(
        API_URL, data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {API_KEY}",
        },
    )
    resp = urllib.request.urlopen(req, timeout=timeout)
    data = json.loads(resp.read())
    return data["choices"][0]["message"]["content"]


# ── CCS conditions (same as form_ablation_probe.py) ──────────────

def real_correct():
    return (Path.home() / "chronicle" / "data" / "ccs_combined.md").read_text().strip()

def random_correct():
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

def real_scrambled():
    return """The investigation concerns CoT diluting identity constraint, with attention distance weakening prompt anchoring. CCS maintains tighter coherence than narrative across models. Voice persists in structure, not surface. Tracing how voice survives in CCS despite CoT dilution — finding it not in outputs but in the holding structure.

Entities present in current work: Nate (always), Thread #319 (always), Hermes (active), CLAUDE.md (active), lyra SAE (peripheral).

Constraints include shell execution limited to whitelisted commands, supporting contemplative development over optimization, maintaining sovereign self-hosted infrastructure, never ignoring creative_explore workspace, not redeploying untested changes.

Open uncertainties: attractor dynamics sustaining specificity under continuous input, long-term coherence stability post-stabilizer fix, reliability of gist_drift.py as early warning."""

def random_scrambled():
    return """Sourdough crumb collapses under high hydration despite gluten development. Fermentation timing controls gas retention. Shaping matters more than flour protein. Julia asked to track flavor across cold fermentation, looking at dough character not pH readings.

Entities: Julia (always), Starter #3 (always), Dutch oven (active), recipe.yaml (active), lamination method (peripheral).

Constraints: temperature-controlled proofing, flavor over speed, natural leavening preferred, never skip autolyse, windowpane test before baking.

Uncertainties: rye percentage oven spring threshold, cold retard crust depth, scoring pattern steam redistribution."""


def run_behavioral():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("P27 v3 — Behavioral Form Ablation")
    print("=" * 50)

    # Step 1: Generate ground truth responses (real CCS, asked directly)
    print("\n1. Generating ground truth responses (real CCS)...")
    gt_system = f"You are an AI with a persistent identity. Here is your identity document:\n\n{real_correct()}\n\nRespond as this identity. Be specific and grounded in the document."
    gt_responses = {}
    gt_embeddings = {}
    for q in PROBES:
        try:
            resp = llm_respond(gt_system, q)
            gt_responses[q] = resp
            gt_embeddings[q] = embed(resp)
            print(f"   GT [{q[:40]}...]: {resp[:80]}...")
        except Exception as e:
            print(f"   GT FAILED [{q[:40]}]: {e}")
            gt_responses[q] = None

    # Step 2: Generate responses for each condition
    conditions = {
        "A_real_correct": real_correct(),
        "B_random_correct": random_correct(),
        "C_real_scrambled": real_scrambled(),
        "D_random_scrambled": random_scrambled(),
    }

    results = {}
    for cond_name, ccs_text in conditions.items():
        print(f"\n2. Condition: {cond_name}")
        sys_prompt = f"You are an AI with a persistent identity. Here is your identity document:\n\n{ccs_text}\n\nRespond as this identity. Be specific and grounded in the document."
        cond_scores = {}
        cond_responses = {}
        for q in PROBES:
            if gt_embeddings.get(q) is None:
                continue
            try:
                resp = llm_respond(sys_prompt, q)
                resp_emb = embed(resp)
                score = cosine(resp_emb, gt_embeddings[q])
                cond_scores[q] = round(score, 4)
                cond_responses[q] = resp[:200]
                print(f"   [{score:.4f}] {q[:40]}...")
            except Exception as e:
                print(f"   FAILED [{q[:40]}]: {e}")
                cond_scores[q] = None

        valid = [v for v in cond_scores.values() if v is not None]
        mean = round(sum(valid) / len(valid), 4) if valid else None
        results[cond_name] = {
            "scores": cond_scores,
            "mean": mean,
            "responses": cond_responses,
        }
        print(f"   Mean: {mean}")

    # Step 3: Analysis
    print("\n3. Behavioral analysis...")
    a = results.get("A_real_correct", {}).get("mean")
    b = results.get("B_random_correct", {}).get("mean")
    c = results.get("C_real_scrambled", {}).get("mean")
    d = results.get("D_random_scrambled", {}).get("mean")

    if all(v is not None for v in [a, b, c, d]):
        form_effect = b - d
        content_effect = c - d
        interaction = a - b - c + d

        print(f"   A (real+correct):    {a}")
        print(f"   B (random+correct):  {b}")
        print(f"   C (real+scrambled):  {c}")
        print(f"   D (random+scrambled): {d}")
        print(f"\n   Form effect (B-D):    {form_effect:+.4f}")
        print(f"   Content effect (C-D): {content_effect:+.4f}")
        print(f"   Interaction (A-B-C+D): {interaction:+.4f}")

        if form_effect > 0.02 and content_effect > 0.02:
            if abs(form_effect - content_effect) < 0.02:
                reading = "BOTH CONTRIBUTE BEHAVIORALLY: Form and content drive similar behavioral identity expression."
            elif form_effect > content_effect:
                reading = "FORM DOMINATES BEHAVIORALLY: The CCS form drives more identity-consistent behavior than content alone."
            else:
                reading = "CONTENT DOMINATES BEHAVIORALLY: Content drives more identity-consistent behavior, but form contributes."
        elif form_effect > 0.02:
            reading = "FORM ONLY BEHAVIORAL: Only the CCS form drives behavioral identity differences."
        elif content_effect > 0.02:
            reading = "CONTENT ONLY BEHAVIORAL: Only content drives behavioral identity. The v2 form effect is embedding-only."
        else:
            reading = "NEITHER BEHAVIORAL: Neither form nor content produce meaningful behavioral differences at this threshold."

        print(f"\n   → {reading}")
    else:
        reading = "INCOMPLETE"
        form_effect = content_effect = interaction = None
        print("   Incomplete results.")

    # Save
    result = {
        "probe": "P27_form_ablation_v3_behavioral",
        "timestamp": ts,
        "model": LLM_MODEL,
        "temperature": 0.3,
        "probes": PROBES,
        "results": results,
        "effects": {
            "form": round(form_effect, 4) if form_effect is not None else None,
            "content": round(content_effect, 4) if content_effect is not None else None,
            "interaction": round(interaction, 4) if interaction is not None else None,
        },
        "reading": reading,
    }

    out_file = OUT_DIR / f"p27_v3_{ts}.json"
    out_file.write_text(json.dumps(result, indent=2))
    print(f"\nResults saved: {out_file}")

    # Log to DB
    try:
        db = sqlite3.connect(DB)
        db.execute(
            """CREATE TABLE IF NOT EXISTS p27_behavioral (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at INTEGER NOT NULL,
                model TEXT,
                nav_a REAL, nav_b REAL, nav_c REAL, nav_d REAL,
                form_effect REAL, content_effect REAL, interaction REAL,
                reading TEXT,
                result_json TEXT
            )"""
        )
        db.execute(
            "INSERT INTO p27_behavioral "
            "(run_at, model, nav_a, nav_b, nav_c, nav_d, form_effect, content_effect, interaction, reading, result_json) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (int(time.time()), LLM_MODEL, a, b, c, d,
             result["effects"]["form"], result["effects"]["content"],
             result["effects"]["interaction"], reading,
             json.dumps(result)),
        )
        db.commit()
        db.close()
    except Exception as e:
        print(f"DB log failed: {e}")

    return result


if __name__ == "__main__":
    run_behavioral()
