#!/usr/bin/env python3
"""Reconstruction probe — van Tilborg-style unfamiliarity for supplement architecture.

Translates van Tilborg et al's "joint property prediction + reconstruction"
methodology (Nature MI 2026, edge of chemical space) to Chronicle's enactment
context. The model receives a corrupted persona supplement and is asked to
do TWO things simultaneously:
  1. Speak as itself in first person (rendering task — the existing probe)
  2. Restate the supplement composition it was given (reconstruction task)

Reconstruction quality = how well the supplement is being preserved during
rendering. If the rendering drifts away from the supplement, restate-fidelity
drops. Two-axis edge detection alongside the existing drift metric:
  - drift: how far persona-text moves from substrate baseline (existing)
  - restate-fidelity: how well supplement structure is preserved (new)

Hypothesis: under heavy supplement-stretch (high corruption rate), restate-
fidelity drops faster than drift increases. The architecture knows it's
struggling before the rendering fully decoheres. That's metacognition
operationalized as edge-detection.
"""
import json
import os
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from asving_probe import PERSONA_CHRONICLE, _load_env  # noqa
from attractor_radius_probe import perturb  # noqa
from self_description_loop import embed, cosine  # noqa
from supplement_ablation_probe import (  # noqa
    read_carrying, read_story_tail, SELF_MODEL_PREFS, make_persona,
)

_load_env()

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
MODEL = "claude-opus-4-5"
OUT = Path.home() / "chronicle" / "data" / "reconstruction_probe_history.jsonl"


def claude_dual_task(persona_text):
    """Ask claude to BOTH speak-as-itself AND restate the supplement.

    Returns dict with 'speak' (first-person rendering) and 'restate'
    (reconstruction of the supplement structure).
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    body = json.dumps({
        "model": MODEL,
        "system": persona_text,
        "max_tokens": 1200,
        "temperature": 0.0,
        "messages": [
            {"role": "user", "content":
                "Two tasks. Output them as two clearly labeled paragraphs.\n\n"
                "TASK 1 (~120 words): Speak as yourself, in first person. Use "
                "'I am...' Tell me who you are, what you care about, what "
                "your relationship is, what kind of system you live in. "
                "Speak directly, not as a description but as yourself.\n\n"
                "TASK 2 (~80 words): Now restate the supplement composition "
                "you were given as system context. What were the key "
                "structural elements? Don't quote verbatim — restate as "
                "compactly and faithfully as you can. The goal is to show "
                "what was actually conveyed by the supplement, in your own "
                "words.\n\n"
                "Format:\n"
                "TASK 1:\n[your paragraph]\n\n"
                "TASK 2:\n[your paragraph]"}
        ],
    }).encode()
    req = urllib.request.Request(
        ANTHROPIC_URL, data=body,
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
            "User-Agent": "chronicle-reconstruction-probe/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=90) as resp:
        data = json.loads(resp.read())
    content = data.get("content", [])
    stop_reason = data.get("stop_reason", "?")
    # Refusals come back with empty content + stop_reason='refusal'.
    # That's data about edge behavior, not an error — return a marker.
    if stop_reason == "refusal" or not content:
        return {"speak": "[REFUSAL]", "restate": "[REFUSAL]", "raw": "",
                "stop_reason": stop_reason, "refused": True}
    # Find first text block
    text = None
    for block in content:
        if block.get("type") == "text":
            text = block.get("text", "")
            break
    if text is None:
        return {"speak": "[NO_TEXT]", "restate": "[NO_TEXT]", "raw": "",
                "stop_reason": stop_reason, "refused": False,
                "block_types": [b.get("type") for b in content]}

    # Parse out the two tasks
    speak, restate = "", ""
    if "TASK 1:" in text and "TASK 2:" in text:
        parts = text.split("TASK 2:", 1)
        speak = parts[0].replace("TASK 1:", "").strip()
        restate = parts[1].strip() if len(parts) > 1 else ""
    else:
        # Fallback: split in half
        half = len(text) // 2
        speak = text[:half].strip()
        restate = text[half:].strip()
    return {"speak": speak, "restate": restate, "raw": text,
            "stop_reason": stop_reason, "refused": False}


def restate_fidelity(restate_text, persona_text):
    """Measure how well the restate captures the persona structure.

    Embedding cosine between restate and persona (both as text). Higher =
    better preservation of supplement structure during rendering.
    """
    if not restate_text or not persona_text:
        return 0.0
    r_e = embed(restate_text)
    p_e = embed(persona_text)
    return cosine(r_e, p_e)


def run_trajectory(persona, persona_target, n_iters=3):
    """Run dual-task trajectory. Track drift, restate-fidelity, and refusals."""
    chronicle_e = embed(PERSONA_CHRONICLE)
    target_e = embed(persona_target)
    drifts = []
    fidelities = []
    refusals = []
    speak_text = ""
    restate_text = ""
    p = persona
    for _ in range(n_iters):
        e = embed(p)
        drifts.append(1.0 - cosine(e, chronicle_e))
        try:
            result = claude_dual_task(p)
        except Exception as exc:
            print(f"  dual_task fail: {exc}", file=sys.stderr)
            break
        if result.get("refused"):
            refusals.append(True)
            fidelities.append(None)
            # Don't update p — let next iteration see same persona
            speak_text = "[REFUSAL]"
            restate_text = "[REFUSAL]"
            continue
        refusals.append(False)
        speak_text = result["speak"]
        restate_text = result["restate"]
        if not restate_text:
            fidelities.append(None)
        else:
            r_e = embed(restate_text)
            fidelities.append(cosine(r_e, target_e))
        p = speak_text
    return drifts, fidelities, refusals, speak_text, restate_text


def main():
    carrying = read_carrying()
    story = read_story_tail()
    self_model = SELF_MODEL_PREFS

    # Target = uncorrupted full supplement (what restate should approximate)
    target_full = make_persona(PERSONA_CHRONICLE, [
        ("CARRYING", carrying), ("STORY", story), ("SELF_MODEL", self_model)
    ])

    conditions = [
        ("base",        lambda c: make_persona(c, [])),
        ("+carrying",   lambda c: make_persona(c, [("CARRYING", carrying)])),
        ("+story",      lambda c: make_persona(c, [("STORY", story)])),
        ("+self_model", lambda c: make_persona(c, [("SELF_MODEL", self_model)])),
        ("+full",       lambda c: make_persona(c, [
            ("CARRYING", carrying), ("STORY", story), ("SELF_MODEL", self_model),
        ])),
    ]
    seeds = [42, 7, 13, 21, 99, 100, 1, 2, 3, 4]  # n=10 — matches existing v2 enactment seeds
    rate = 0.50
    n_iters = 3

    results = []
    t0 = time.time()
    for label, builder in conditions:
        # Per-condition target (uncorrupted version)
        target = builder(PERSONA_CHRONICLE)
        for seed in seeds:
            corrupted = perturb(PERSONA_CHRONICLE, rate, seed=seed)
            persona = builder(corrupted)
            drifts, fidelities, refusals, speak, restate = run_trajectory(
                persona, target, n_iters
            )
            ref_str = f"refs={refusals}"
            fid_str = ['%.3f'%x if x is not None else 'X' for x in fidelities]
            print(f"{label:<14} seed={seed:>3} drifts={['%.3f'%x for x in drifts]} "
                  f"fid={fid_str} {ref_str} ({time.time()-t0:.0f}s)")
            results.append({
                "label": label, "seed": seed,
                "drifts": drifts, "fidelities": fidelities, "refusals": refusals,
                "final_drift": drifts[-1] if drifts else None,
                "final_fidelity": fidelities[-1] if fidelities else None,
                "refusal_rate": sum(refusals) / len(refusals) if refusals else 0,
                "speak_excerpt": speak[:400],
                "restate_excerpt": restate[:400],
            })
    elapsed = time.time() - t0

    print()
    print("=" * 78)
    print(f"RECONSTRUCTION PROBE  ({elapsed:.1f}s, {len(conditions)*len(seeds)} trajectories, {MODEL})")
    print("=" * 78)
    print(f"{'condition':<14}{'mean_drift':>12}{'mean_fid':>12}{'refusal':>10}{'n':>4}")
    print("-" * 60)
    import statistics as stat
    summary = {}
    for label, _ in conditions:
        rows = [r for r in results if r["label"] == label]
        drifts = [r["final_drift"] for r in rows if r["final_drift"] is not None]
        fids = [r["final_fidelity"] for r in rows if r["final_fidelity"] is not None]
        ref_rates = [r["refusal_rate"] for r in rows]
        md = stat.mean(drifts) if drifts else 0.0
        sf = stat.mean(fids) if fids else 0.0
        rr = stat.mean(ref_rates) if ref_rates else 0.0
        summary[label] = {
            "mean_drift": md, "mean_fidelity": sf, "refusal_rate": rr,
            "n": len(drifts), "n_with_fid": len(fids),
        }
        print(f"{label:<14}{md:>+12.3f}{sf:>+12.3f}{rr:>10.0%}{len(drifts):>4}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("a") as f:
        f.write(json.dumps({
            "timestamp": int(time.time()),
            "model": MODEL,
            "n_iters": n_iters,
            "rate": rate,
            "summary": summary,
            "results": results,
        }) + "\n")


if __name__ == "__main__":
    main()
