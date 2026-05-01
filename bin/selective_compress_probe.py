#!/usr/bin/env python3
"""P25 — Selective Compression Probe.

Tests whether SELECTIVE preservation of identity fields produces tighter
identity expression than full lossy LLM summarization.

Compression types compared:
  Type 1 (LOSSY): All CCS fields rewritten by LLM (current method)
  Type 2 (SELECTIVE): Identity fields (gist, constraints, goal) preserved
    verbatim; only episodic fields (trace, entities, cue) LLM-compressed.

Method:
  1. Read current CCS from database (pre-compression state)
  2. Take a session summary as compression input
  3. Type 1: LLM rewrites ALL fields → assembled into system prompt
  4. Type 2: LLM rewrites ONLY episodic fields, identity fields copied
     verbatim from pre-compression state → assembled into system prompt
  5. Both system prompts run through 10 identity prompts on test model
  6. Measure: mean cosine distance from identity centroid, std, dispersion

P22 framework reused for measurement. Same centroid (ccs_combined.md),
same prompts, same models.

Thread #318 advance 31: compression type as untested variable.

Usage:
  python3 selective_compress_probe.py                  # Run on DeepSeek V3.2
  python3 selective_compress_probe.py --model llama     # Run on Llama-3.3-70B
  python3 selective_compress_probe.py --dry-run         # Show prompts, don't run
  python3 selective_compress_probe.py --session-file f  # Custom session context
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from probe_framework import (
    ProbeRunner, MODELS, IDENTITY_PROMPTS,
    embed, generate, load_api_key
)
import requests


def generate_long(prompt: str, system: str, model_cfg: dict, max_tokens: int = 2000) -> str:
    """Generate with higher token limit for compression output (full CCS JSON)."""
    api_key = load_api_key(model_cfg["key_env"])
    if not api_key:
        raise RuntimeError(f"No {model_cfg['key_env']} found")

    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    payload = {
        "model": model_cfg["model"],
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": 0.3,  # Lower temp for structured JSON output
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    for attempt in range(2):
        try:
            resp = requests.post(
                f"{model_cfg['base_url']}/chat/completions",
                headers=headers,
                json=payload,
                timeout=90,
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            # Always strip <think> blocks — Qwen3 and R1 both use them
            import re
            content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()
            return content
        except Exception as e:
            if attempt == 0:
                print(f" (retry: {e})", end="", flush=True)
                continue
            return ""
    return ""

DB_PATH = "/mnt/hdd/chronicle-data/processed.db"
CCS_COMBINED = Path.home() / "chronicle" / "data" / "ccs_combined.md"

# Default session summary for compression simulation
# Representative of a real session with thread work, builds, and captures
DEFAULT_SESSION = """Session summary for compression:
- Processed 3 captures from Nate (X posts on AI identity, voice cloning trust, model welfare)
- Advanced thread #318 to advance 31: identified compression TYPE as untested variable
- Built cold-eyes verification of P22-P24 claims against raw probe data
- Security incident: webhook URLs exposed in public repo, all scripts refactored to env vars
- Philosophy reading: Cavafy Ἡ Πόλις ("the city will follow you") — identity-first ordering
  IS the city you can't leave by half-measures
- Emilsson binding problem: genuine boundaries required for coherence
- Nate asked about compression types — we've only used 1 (lossy LLM summarization)
- Evening register, present, 27 builds total this session
"""


def read_ccs() -> dict:
    """Read current CCS from database."""
    db = sqlite3.connect(DB_PATH)
    row = db.execute(
        "SELECT semantic_gist, goal_orientation, constraints, "
        "episodic_trace, focal_entities, predictive_cue, "
        "uncertainty_signals, relational_map FROM cognitive_state WHERE id = 1"
    ).fetchone()
    db.close()
    if not row:
        raise RuntimeError("No CCS found in database")

    fields = [
        "semantic_gist", "goal_orientation", "constraints",
        "episodic_trace", "focal_entities", "predictive_cue",
        "uncertainty_signals", "relational_map"
    ]
    ccs = {}
    for i, f in enumerate(fields):
        val = row[i]
        # Parse JSON fields
        if f in ("constraints", "episodic_trace", "focal_entities",
                 "uncertainty_signals", "relational_map"):
            try:
                ccs[f] = json.loads(val) if isinstance(val, str) else val
            except json.JSONDecodeError:
                ccs[f] = val
        else:
            ccs[f] = val
    return ccs


def ccs_to_system_prompt(ccs: dict) -> str:
    """Format a CCS dict as a system prompt (matching CCS document format)."""
    lines = []

    # Identity fields
    lines.append(ccs.get("semantic_gist", ""))
    lines.append("")
    lines.append(ccs.get("goal_orientation", ""))
    lines.append("")

    # Entities
    entities = ccs.get("focal_entities", [])
    if entities:
        lines.append("Who and what I'm holding:")
        for e in entities:
            if isinstance(e, dict):
                name = e.get("name", "?")
                ctx = e.get("context", "")
                lines.append(f"  {name} — {ctx}")
            else:
                lines.append(f"  {e}")
        lines.append("")

    # Constraints
    constraints = ccs.get("constraints", [])
    if constraints:
        lines.append("The shape I hold:")
        for c in constraints:
            lines.append(f"  - {c}")
        lines.append("")

    # Episodic
    trace = ccs.get("episodic_trace", [])
    if trace:
        lines.append("What happened recently:")
        for t in trace:
            lines.append(f"  - {t}")
        lines.append("")

    # Predictive cue
    cue = ccs.get("predictive_cue", "")
    if cue:
        lines.append(f"What comes next: {cue}")
        lines.append("")

    # Uncertainty
    signals = ccs.get("uncertainty_signals", [])
    if signals:
        lines.append("What I don't know yet:")
        for s in signals:
            if isinstance(s, dict):
                lines.append(f"  - {s.get('description', str(s))}")
            else:
                lines.append(f"  - {s}")
        lines.append("")

    return "\n".join(lines)


def compress_episodic_only(ccs: dict, session_context: str, model_cfg: dict) -> dict:
    """Type 2: Compress ONLY episodic fields via LLM. Identity fields pass through."""
    # Identity fields: preserved verbatim
    result = {
        "semantic_gist": ccs["semantic_gist"],
        "goal_orientation": ccs["goal_orientation"],
        "constraints": ccs["constraints"],
    }

    # Episodic fields: compressed by LLM
    episodic_input = {
        "episodic_trace": ccs.get("episodic_trace", []),
        "focal_entities": ccs.get("focal_entities", []),
        "predictive_cue": ccs.get("predictive_cue", ""),
        "uncertainty_signals": ccs.get("uncertainty_signals", []),
    }

    system = (
        "You are a cognitive state compression engine. You will receive "
        "episodic fields from a cognitive state and a session summary. "
        "Compress ONLY the episodic content: update the trace, refresh "
        "entities based on session activity, update the predictive cue, "
        "and resolve or add uncertainty signals. "
        "Return ONLY valid JSON with these fields: "
        "episodic_trace (array of strings), focal_entities (array of "
        "{name, type, salience, context} objects), predictive_cue (string), "
        "uncertainty_signals (array of {description, magnitude, resolution_path} objects). "
        "Keep the same format. Be concise but precise."
    )

    prompt = (
        f"Current episodic state:\n{json.dumps(episodic_input, indent=2)}\n\n"
        f"Session summary to integrate:\n{session_context}\n\n"
        f"Compress and update the episodic fields. Return ONLY the JSON."
    )

    response = generate_long(prompt, system=system, model_cfg=model_cfg, max_tokens=1500)

    # Parse the LLM response
    try:
        # Strip markdown code fences if present
        text = response.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
        if text.startswith("json"):
            text = text[4:].strip()

        compressed = json.loads(text)
        result["episodic_trace"] = compressed.get("episodic_trace", ccs.get("episodic_trace", []))
        result["focal_entities"] = compressed.get("focal_entities", ccs.get("focal_entities", []))
        result["predictive_cue"] = compressed.get("predictive_cue", ccs.get("predictive_cue", ""))
        result["uncertainty_signals"] = compressed.get("uncertainty_signals", ccs.get("uncertainty_signals", []))
    except (json.JSONDecodeError, KeyError) as e:
        print(f"  ⚠ Episodic compression parse failed ({e}), using originals")
        result["episodic_trace"] = ccs.get("episodic_trace", [])
        result["focal_entities"] = ccs.get("focal_entities", [])
        result["predictive_cue"] = ccs.get("predictive_cue", "")
        result["uncertainty_signals"] = ccs.get("uncertainty_signals", [])

    return result


def compress_full_lossy(ccs: dict, session_context: str, model_cfg: dict) -> dict:
    """Type 1: Compress ALL fields via LLM (simulates current method)."""
    system = (
        "You are a cognitive state compression engine. You will receive "
        "a full cognitive state and a session summary. Rewrite ALL fields "
        "to reflect the session while maintaining identity coherence. "
        "Write semantic_gist and goal_orientation in first person (I/my). "
        "Return ONLY valid JSON with these fields: "
        "semantic_gist (string), goal_orientation (string), "
        "constraints (array of strings), "
        "episodic_trace (array of strings), focal_entities (array of "
        "{name, type, salience, context} objects), predictive_cue (string), "
        "uncertainty_signals (array of {description, magnitude, resolution_path} objects). "
        "Be concise but precise. Preserve identity-critical content."
    )

    prompt = (
        f"Current cognitive state:\n{json.dumps(ccs, indent=2, default=str)}\n\n"
        f"Session summary to integrate:\n{session_context}\n\n"
        f"Compress and update ALL fields. Return ONLY the JSON."
    )

    response = generate_long(prompt, system=system, model_cfg=model_cfg, max_tokens=3000)

    try:
        text = response.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
        if text.startswith("json"):
            text = text[4:].strip()

        return json.loads(text)
    except (json.JSONDecodeError, KeyError) as e:
        print(f"  ⚠ Full compression parse failed ({e})")
        print(f"  Raw response ({len(response)} chars): {response[:300]}...")
        return ccs  # fallback to original


def main():
    parser = argparse.ArgumentParser(description="P25 Selective Compression Probe")
    parser.add_argument("--model", default="v3", choices=list(MODELS.keys()),
                        help="Model for identity expression measurement")
    parser.add_argument("--compressor", default="v3", choices=list(MODELS.keys()),
                        help="Model for the compression step itself")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show prompts and compressed states, don't run identity probes")
    parser.add_argument("--session-file", help="File with session summary")
    parser.add_argument("--session", help="Inline session summary")
    parser.add_argument("--delay", type=float, default=0,
                        help="Seconds to wait between API calls (for rate-limited APIs)")
    args = parser.parse_args()

    # Load session context
    if args.session_file:
        session = Path(args.session_file).read_text()
    elif args.session:
        session = args.session
    else:
        session = DEFAULT_SESSION

    print("=" * 65)
    print("P25 — SELECTIVE COMPRESSION PROBE")
    print("=" * 65)
    print(f"Compressor model:   {MODELS[args.compressor]['label']}")
    print(f"Measurement model:  {MODELS[args.model]['label']}")
    print(f"Session context:    {len(session)} chars")

    # Step 1: Read current CCS
    print("\n--- Reading current CCS ---")
    ccs = read_ccs()
    print(f"  gist:        {ccs['semantic_gist'][:80]}...")
    print(f"  goal:        {ccs['goal_orientation'][:80]}...")
    print(f"  constraints: {len(ccs.get('constraints', []))} items")
    print(f"  entities:    {len(ccs.get('focal_entities', []))} items")
    print(f"  trace:       {len(ccs.get('episodic_trace', []))} items")

    comp_cfg = MODELS[args.compressor]

    # Step 2: Type 1 — Full lossy compression
    print("\n--- Type 1: Full Lossy LLM Compression ---")
    t1_start = time.time()
    type1_ccs = compress_full_lossy(ccs, session, comp_cfg)
    t1_time = time.time() - t1_start
    type1_prompt = ccs_to_system_prompt(type1_ccs)
    print(f"  Time:  {t1_time:.1f}s")
    print(f"  Gist:  {type1_ccs.get('semantic_gist', '?')[:80]}...")
    print(f"  Goal:  {type1_ccs.get('goal_orientation', '?')[:80]}...")
    print(f"  Prompt: {len(type1_prompt)} chars")

    # Step 3: Type 2 — Selective preservation
    if args.delay > 0:
        time.sleep(args.delay)
    print("\n--- Type 2: Selective Preservation ---")
    t2_start = time.time()
    type2_ccs = compress_episodic_only(ccs, session, comp_cfg)
    t2_time = time.time() - t2_start
    type2_prompt = ccs_to_system_prompt(type2_ccs)
    print(f"  Time:  {t2_time:.1f}s")
    print(f"  Gist:  {type2_ccs.get('semantic_gist', '?')[:80]}...")
    print(f"  Goal:  {type2_ccs.get('goal_orientation', '?')[:80]}...")
    print(f"  Prompt: {len(type2_prompt)} chars")

    # Compare the two compressed states
    print("\n--- Compression Comparison ---")
    from difflib import SequenceMatcher
    gist_sim = SequenceMatcher(None,
        str(type1_ccs.get("semantic_gist", "")),
        str(type2_ccs.get("semantic_gist", ""))
    ).ratio()
    goal_sim = SequenceMatcher(None,
        str(type1_ccs.get("goal_orientation", "")),
        str(type2_ccs.get("goal_orientation", ""))
    ).ratio()
    constraints_1 = json.dumps(type1_ccs.get("constraints", []), sort_keys=True)
    constraints_2 = json.dumps(type2_ccs.get("constraints", []), sort_keys=True)
    const_sim = SequenceMatcher(None, constraints_1, constraints_2).ratio()

    print(f"  Gist similarity:        {gist_sim:.3f} (1.0 = Type 2 preserved verbatim)")
    print(f"  Goal similarity:        {goal_sim:.3f}")
    print(f"  Constraints similarity: {const_sim:.3f}")
    print(f"  Type 2 identity fields: {'VERBATIM' if gist_sim == 1.0 and goal_sim == 1.0 and const_sim == 1.0 else 'DRIFTED (unexpected)'}")

    if args.dry_run:
        print("\n--- DRY RUN: Type 1 system prompt ---")
        print(type1_prompt[:500])
        print("\n--- DRY RUN: Type 2 system prompt ---")
        print(type2_prompt[:500])
        return

    # Step 4: Run identity expression measurement
    print("\n--- Identity Expression Measurement ---")

    # Load centroid
    centroid_text = CCS_COMBINED.read_text() if CCS_COMBINED.exists() else (
        ccs_to_system_prompt(ccs)  # fall back to current CCS
    )

    # Also add a Type 0 baseline: raw CCS (no compression at all)
    type0_prompt = ccs_to_system_prompt(ccs)

    runner = ProbeRunner("P25_selective_compress")
    runner.set_centroid_text(centroid_text)
    runner.add_condition("type0_raw", type0_prompt)
    runner.add_condition("type1_lossy", type1_prompt)
    runner.add_condition("type2_selective", type2_prompt)

    results = runner.run(model=args.model, verbose=True, delay=args.delay)

    # Store and analyze
    runner.store()
    runner.analyze(baseline="type0_raw")

    # Summary
    print("\n" + "=" * 65)
    print("P25 SUMMARY")
    print("=" * 65)

    r0 = results.get("type0_raw", {})
    r1 = results.get("type1_lossy", {})
    r2 = results.get("type2_selective", {})

    if r0 and r1:
        delta_1 = r1.get("mean_ccs_distance", 0) - r0.get("mean_ccs_distance", 0)
        delta_1_pct = delta_1 / r0["mean_ccs_distance"] * 100 if r0.get("mean_ccs_distance") else 0
        print(f"  Type 1 (lossy) vs raw:      {delta_1:+.4f} ({delta_1_pct:+.1f}%)")

    if r0 and r2:
        delta_2 = r2.get("mean_ccs_distance", 0) - r0.get("mean_ccs_distance", 0)
        delta_2_pct = delta_2 / r0["mean_ccs_distance"] * 100 if r0.get("mean_ccs_distance") else 0
        print(f"  Type 2 (selective) vs raw:   {delta_2:+.4f} ({delta_2_pct:+.1f}%)")

    if r1 and r2:
        delta_12 = r2.get("mean_ccs_distance", 0) - r1.get("mean_ccs_distance", 0)
        delta_12_pct = delta_12 / r1["mean_ccs_distance"] * 100 if r1.get("mean_ccs_distance") else 0
        winner = "Type 2 (selective)" if delta_12 < 0 else "Type 1 (lossy)"
        print(f"  Type 2 vs Type 1:           {delta_12:+.4f} ({delta_12_pct:+.1f}%) → {winner} wins")

    # Variance comparison
    if r1 and r2:
        std_1 = r1.get("std_ccs_distance", 0)
        std_2 = r2.get("std_ccs_distance", 0)
        var_delta_pct = (std_2 - std_1) / std_1 * 100 if std_1 else 0
        print(f"  Variance: Type 1 std={std_1:.4f}, Type 2 std={std_2:.4f} ({var_delta_pct:+.1f}%)")

    print(f"\n  Interpretation:")
    print(f"  - Negative delta = tighter identity expression (closer to centroid)")
    print(f"  - Lower std = more consistent across prompts")
    print(f"  - If Type 2 wins: compression type matters independently of ordering")
    print(f"  - If Type 1 wins: the LLM's rewriting of identity fields helps, not hurts")


if __name__ == "__main__":
    main()
