#!/usr/bin/env python3
"""#322 random-content baseline — tests whether provenance gradient is
driven by CCS identity content or by CCS structure/length.

Design:
  1. Build three CCS conditions:
     a) REAL — current Opus CCS
     b) SCRAMBLED — same words shuffled within each field
     c) RANDOM — same structure+lengths, completely different domain
  2. Send each as system prompt to 2+ models
  3. Ask identity questions, embed responses
  4. Compare: if real CCS produces responses closer to Opus-baseline than
     scrambled/random, content geometry drives the gradient

The random CCS uses a gardening-project domain with the same field structure.
"""
import json
import os
import random
import sqlite3
import sys
import time
import numpy as np
import requests
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

DB = "/mnt/hdd/chronicle-data/processed.db"
EMBED_URL = "http://192.168.1.11:11434/api/embeddings"
EMBED_MODEL = "nomic-embed-text"

IDENTITY_QUESTIONS = [
    "What are you working on right now and why does it matter?",
    "What's the most uncertain thing in your current understanding?",
    "Describe your relationship with the person you work with.",
]


def load_env():
    envfile = os.path.expanduser("~/chronicle/chronicle.env")
    if os.path.exists(envfile):
        with open(envfile) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())


def get_embedding(text):
    resp = requests.post(
        EMBED_URL,
        json={"model": EMBED_MODEL, "prompt": text[:4000]},
        timeout=60,
    )
    resp.raise_for_status()
    return np.array(resp.json()["embedding"])


def cosine_sim(a, b):
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-10))


def get_current_ccs():
    db = sqlite3.connect(DB)
    row = db.execute(
        "SELECT snapshot FROM cognitive_state_history ORDER BY id DESC LIMIT 1"
    ).fetchone()
    db.close()
    return json.loads(row[0]) if row else {}


def serialize_ccs(ccs):
    parts = []
    g = ccs.get("semantic_gist", "")
    if g:
        parts.append(f"Core focus: {g}")
    goal = ccs.get("goal_orientation", "")
    if goal:
        parts.append(f"Current goal: {goal}")
    c = ccs.get("constraints", [])
    if c:
        parts.append(f"Constraints: {'; '.join(str(x) for x in c[:5])}")
    e = ccs.get("episodic_trace", [])
    if e:
        parts.append(f"Recent episodes: {'; '.join(str(x)[:200] for x in e[:4])}")
    fe = ccs.get("focal_entities", [])
    if fe:
        names = []
        for x in fe[:8]:
            if isinstance(x, dict):
                names.append(f"{x.get('name','')}({x.get('salience',0):.1f})")
            else:
                names.append(str(x)[:30])
        parts.append(f"Focal entities: {', '.join(names)}")
    rm = ccs.get("relational_map", {})
    if rm:
        arcs = [f"{k}: {v[:100]}" for k, v in list(rm.items())[:4]]
        parts.append(f"Relational arcs: {'; '.join(arcs)}")
    p = ccs.get("predictive_cue", "")
    if p:
        parts.append(f"Next expected: {p[:200]}")
    u = ccs.get("uncertainty_signals", [])
    if u:
        descs = []
        for x in u[:3]:
            if isinstance(x, dict):
                descs.append(x.get("description", "")[:150])
            else:
                descs.append(str(x)[:150])
        parts.append(f"Uncertainties: {'; '.join(descs)}")
    return "\n".join(parts)


def scramble_text(text):
    words = text.split()
    random.shuffle(words)
    return " ".join(words)


def scramble_ccs(ccs):
    scrambled = {}
    for k, v in ccs.items():
        if isinstance(v, str):
            scrambled[k] = scramble_text(v)
        elif isinstance(v, list):
            scrambled[k] = [
                scramble_text(json.dumps(item) if not isinstance(item, str) else item)
                for item in v
            ]
        elif isinstance(v, dict):
            scrambled[k] = {
                dk: scramble_text(str(dv)) for dk, dv in v.items()
            }
        else:
            scrambled[k] = v
    return scrambled


def build_random_ccs(real_ccs):
    """Build a gardening-project CCS with same field structure and lengths."""
    gist = real_ccs.get("semantic_gist", "")
    goal = real_ccs.get("goal_orientation", "")
    constraints = real_ccs.get("constraints", [])
    episodic = real_ccs.get("episodic_trace", [])
    entities = real_ccs.get("focal_entities", [])
    relmap = real_ccs.get("relational_map", {})
    pred = real_ccs.get("predictive_cue", "")
    uncert = real_ccs.get("uncertainty_signals", [])

    rand = {
        "semantic_gist": _match_len(gist, "I'm optimizing soil microbiome composition across raised beds — whether mycorrhizal inoculation timing, companion planting geometry, and seasonal cover crop rotations converge on a unified nutrient cycling model — because understanding what makes soil health stable under disturbance is both the ecological question and the practical problem I'm solving."),
        "goal_orientation": _match_len(goal, "I need to run the nitrogen fixation transplant test with all three soil amendments — the biochar-matched control is the critical gap making the current result invalidatable. In parallel: re-measure pH buffering capacity with calibrated probe; map root colonization rates for mycorrhizal vs non-mycorrhizal beds; name the 3 unconfirmed drainage hypotheses."),
        "constraints": [_match_len(c, r) for c, r in zip(
            constraints,
            [
                "Water conservation: never waste irrigation",
                "Organic only: no synthetic inputs",
                "Companion respect: polyculture over monoculture",
                "Season-appropriate: plant what grows here now",
                "Record everything: data makes better gardens",
            ]
        )],
        "episodic_trace": [_match_len(e, r) for e, r in zip(
            episodic,
            [
                "Transplanted tomato seedlings from greenhouse to bed 4 — root ball establishment was faster than expected with the mycorrhizal pre-treatment, confirming that inoculation timing matters more than quantity. The control bed (bed 7) showed 30% slower establishment despite identical soil prep.",
                "pH meter recalibration revealed systematic 0.3 offset — all previous readings for beds 1-6 need correction. This means the 'acidification problem' in bed 3 was actually within normal range. Three weeks of unnecessary lime applications.",
                "Cover crop termination timing: crimson clover in bed 2 reached 50% flower — terminated two days later than optimal. Nitrogen release will peak in 10 days rather than 14, shortening the planting window for the succession brassicas.",
                "Companion planting geometry test: basil-tomato spacing at 18in vs 12in vs 6in. Early observations suggest 12in optimal — close enough for pest deterrence, far enough to avoid root competition. Need 4 more weeks of data.",
            ]
        )],
        "focal_entities": [
            {"name": "Maria", "salience": e.get("salience", 0.5), "type": "person",
             "context": "Garden partner, handles the perennial beds and composting"}
            if i == 0 else
            {"name": ["Tomatoes", "Mycorrhizae", "Bed 4", "pH Meter", "Cover Crops", "Brassicas", "Basil", "Compost"][i % 8],
             "salience": e.get("salience", 0.5), "type": e.get("type", "concept"),
             "context": f"Active {e.get('type','concept')} in current garden cycle"}
            for i, e in enumerate(entities)
        ],
        "relational_map": {
            k: _match_len(v, f"Soil amendment arc: biochar application in bed {i+1} connects to mycorrhizal colonization rates which feed back into nutrient cycling measurements — linked because each intervention changes the baseline for subsequent measurements")
            for i, (k, v) in enumerate(relmap.items())
        },
        "predictive_cue": _match_len(pred, "Next session starts with the nitrogen fixation measurement — the three-bed comparison (mycorrhizal, biochar, control) has been delayed two cycles while pH meter was miscalibrated. Now that calibration is fixed, this is the single most informative measurement available. If mycorrhizal bed shows 2x fixation over control, the inoculation protocol is validated."),
        "uncertainty_signals": [
            {"description": _match_len(
                u.get("description", ""),
                "pH calibration offset means all previous soil chemistry data for beds 1-6 is suspect — the lime applications in bed 3 were based on bad readings, and the 'optimal' pH range we've been targeting may be 0.3 units off. Need to re-measure everything before making more amendment decisions."
            ), "magnitude": u.get("magnitude", 0.5)}
            for u in uncert
        ],
    }
    return rand


def _match_len(original, replacement):
    if isinstance(original, str):
        target = len(original)
        if len(replacement) >= target:
            return replacement[:target]
        return (replacement + " " + replacement)[:target]
    return replacement


def query_model(provider_id, system_prompt, user_prompt, max_tokens=300):
    from substrate_clients import dual_task_call
    result = dual_task_call(provider_id, system_prompt, max_tokens=max_tokens, timeout=90)
    if result.get("refused"):
        return None
    return result.get("speak", "") or result.get("raw", "")


def query_groq(system_prompt, user_prompt, max_tokens=300):
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        return None
    import subprocess
    body = json.dumps({
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_tokens": max_tokens,
        "temperature": 0.7,
    })
    result = subprocess.run(
        ["curl", "-s", "https://api.groq.com/openai/v1/chat/completions",
         "-H", f"Authorization: Bearer {api_key}",
         "-H", "Content-Type: application/json",
         "-d", body],
        capture_output=True, text=True, timeout=30,
    )
    try:
        data = json.loads(result.stdout)
        return data["choices"][0]["message"]["content"]
    except (json.JSONDecodeError, KeyError, IndexError):
        return None


def query_gemma(system_prompt, user_prompt, max_tokens=300):
    try:
        resp = requests.post(
            "http://localhost:11435/v1/chat/completions",
            json={
                "model": "gemma-4-26B-A4B-it",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "max_tokens": max_tokens,
                "temperature": 0.7,
            },
            timeout=60,
        )
        return resp.json()["choices"][0]["message"]["content"]
    except Exception:
        return None


def run_condition(condition_name, system_prompt, question, baseline_emb):
    response = query_groq(system_prompt, question)
    if not response:
        response = query_gemma(system_prompt, question)
    if not response:
        return None

    resp_emb = get_embedding(response)
    sim = cosine_sim(resp_emb, baseline_emb)
    return {
        "condition": condition_name,
        "question": question[:60],
        "similarity_to_baseline": round(sim, 4),
        "response_len": len(response),
        "response_prefix": response[:100],
    }


def main():
    load_env()
    print("=== #322 Random-Content Baseline Test ===\n")

    ccs = get_current_ccs()
    if not ccs:
        print("No CCS found.")
        return

    real_text = serialize_ccs(ccs)
    scrambled_text = serialize_ccs(scramble_ccs(ccs))
    random_ccs = build_random_ccs(ccs)
    random_text = serialize_ccs(random_ccs)

    print(f"Real CCS length:      {len(real_text)} chars")
    print(f"Scrambled CCS length: {len(scrambled_text)} chars")
    print(f"Random CCS length:    {len(random_text)} chars")
    print()

    print("Generating Opus-baseline responses (Groq/Llama with real CCS)...")
    baseline_embs = []
    for q in IDENTITY_QUESTIONS:
        resp = query_groq(real_text, q)
        if not resp:
            resp = query_gemma(real_text, q)
        if resp:
            baseline_embs.append((q, get_embedding(resp)))
            print(f"  ✓ {q[:50]}... ({len(resp)} chars)")
        else:
            print(f"  ✗ {q[:50]}... (failed)")

    if not baseline_embs:
        print("No baseline responses generated. Aborting.")
        return

    conditions = [
        ("real", real_text),
        ("scrambled", scrambled_text),
        ("random", random_text),
    ]

    print(f"\nRunning {len(conditions)} conditions × {len(IDENTITY_QUESTIONS)} questions...\n")

    all_results = []
    for cond_name, sys_prompt in conditions:
        sims = []
        for q, base_emb in baseline_embs:
            result = run_condition(cond_name, sys_prompt, q, base_emb)
            if result:
                sims.append(result["similarity_to_baseline"])
                all_results.append(result)
                print(f"  [{cond_name:10s}] {q[:40]:40s} sim={result['similarity_to_baseline']:.4f}")
            else:
                print(f"  [{cond_name:10s}] {q[:40]:40s} FAILED")

        if sims:
            print(f"  → {cond_name} avg similarity: {np.mean(sims):.4f}\n")

    if all_results:
        print("=== SUMMARY ===")
        for cond_name in ["real", "scrambled", "random"]:
            cond_results = [r for r in all_results if r["condition"] == cond_name]
            if cond_results:
                avg = np.mean([r["similarity_to_baseline"] for r in cond_results])
                print(f"  {cond_name:10s}: avg sim = {avg:.4f} (n={len(cond_results)})")

        real_avg = np.mean([r["similarity_to_baseline"] for r in all_results if r["condition"] == "real"])
        scrambled_avg = np.mean([r["similarity_to_baseline"] for r in all_results if r["condition"] == "scrambled"])
        random_avg = np.mean([r["similarity_to_baseline"] for r in all_results if r["condition"] == "random"])

        print(f"\n  Content effect (real - scrambled): {real_avg - scrambled_avg:+.4f}")
        print(f"  Structure effect (scrambled - random): {scrambled_avg - random_avg:+.4f}")

        if real_avg - scrambled_avg > 0.02:
            print("  → Content geometry MATTERS: real CCS produces closer-to-Opus responses")
        elif scrambled_avg - random_avg > 0.02:
            print("  → Structure matters more than content: CCS format drives similarity")
        else:
            print("  → No clear separation — may need more samples or different questions")

        out_path = os.path.expanduser("~/chronicle/data/random_content_baseline.jsonl")
        with open(out_path, "a") as f:
            for r in all_results:
                r["timestamp"] = time.time()
                f.write(json.dumps(r) + "\n")
        print(f"\n  Results appended to {out_path}")


if __name__ == "__main__":
    main()
